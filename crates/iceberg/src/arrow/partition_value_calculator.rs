// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Partition value calculation for Iceberg tables.
//!
//! This module provides utilities for calculating partition values from record batches
//! based on a partition specification.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};

use super::record_batch_projector::{RecordBatchProjector, parquet_field_id};
use super::schema::schema_to_arrow_schema;
use super::type_to_arrow_type;
use crate::spec::{PartitionSpec, Schema, StructType, Type};
use crate::transform::{BoxedTransformFunction, create_transform_function};
use crate::{Error, ErrorKind, Result};

/// Calculator for partition values in Iceberg tables.
///
/// Source columns are pulled by cached position when the runtime batch's
/// shape matches the iceberg-derived arrow schema (modulo metadata), and by
/// runtime `PARQUET:field_id` lookup otherwise. The fallback exists because
/// optimizer passes like DataFusion's projection unification can reshape the
/// input batch out from under cached positions.
#[derive(Debug)]
pub struct PartitionValueCalculator {
    source_field_ids: Vec<i32>,
    cached_projector: RecordBatchProjector,
    expected_arrow_schema: SchemaRef,
    transform_functions: Vec<BoxedTransformFunction>,
    partition_type: StructType,
    partition_arrow_type: DataType,
}

impl PartitionValueCalculator {
    /// Create a new PartitionValueCalculator.
    ///
    /// # Arguments
    ///
    /// * `partition_spec` - The partition specification
    /// * `table_schema` - The Iceberg table schema
    ///
    /// # Returns
    ///
    /// Returns a new `PartitionValueCalculator` instance or an error if initialization fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The partition spec is unpartitioned
    /// - Transform function creation fails
    pub fn try_new(partition_spec: &PartitionSpec, table_schema: &Schema) -> Result<Self> {
        if partition_spec.is_unpartitioned() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot create partition calculator for unpartitioned table",
            ));
        }

        // Create transform functions for each partition field
        let transform_functions: Vec<BoxedTransformFunction> = partition_spec
            .fields()
            .iter()
            .map(|pf| create_transform_function(&pf.transform))
            .collect::<Result<Vec<_>>>()?;

        // Extract source field IDs for projection
        let source_field_ids: Vec<i32> = partition_spec
            .fields()
            .iter()
            .map(|pf| pf.source_id)
            .collect();

        let expected_arrow_schema = Arc::new(schema_to_arrow_schema(table_schema)?);
        let cached_projector = RecordBatchProjector::new(
            expected_arrow_schema.clone(),
            &source_field_ids,
            parquet_field_id,
            |_| true,
        )?;

        let partition_type = partition_spec.partition_type(table_schema)?;
        let partition_arrow_type = type_to_arrow_type(&Type::Struct(partition_type.clone()))?;

        Ok(Self {
            source_field_ids,
            cached_projector,
            expected_arrow_schema,
            transform_functions,
            partition_type,
            partition_arrow_type,
        })
    }

    /// Get the partition type as an Iceberg StructType.
    pub fn partition_type(&self) -> &StructType {
        &self.partition_type
    }

    /// Get the partition type as an Arrow DataType.
    pub fn partition_arrow_type(&self) -> &DataType {
        &self.partition_arrow_type
    }

    /// Calculate partition values for a record batch.
    ///
    /// This method:
    /// 1. Projects the source columns from the batch
    /// 2. Applies partition transforms to each source column
    /// 3. Constructs a StructArray containing the partition values
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch to calculate partition values for
    ///
    /// # Returns
    ///
    /// Returns an ArrayRef containing a StructArray of partition values, or an error if calculation fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Column projection fails
    /// - Transform application fails
    /// - StructArray construction fails
    pub fn calculate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let source_columns = if shapes_match(&self.expected_arrow_schema, batch.schema_ref()) {
            self.cached_projector.project_column(batch.columns())?
        } else {
            RecordBatchProjector::project_columns_by_field_id(batch, &self.source_field_ids)?
        };

        // Get expected struct fields for the result
        let expected_struct_fields = match &self.partition_arrow_type {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Expected partition type must be a struct",
                ));
            }
        };

        // Apply transforms to each source column
        let mut partition_values = Vec::with_capacity(self.transform_functions.len());
        for (source_column, transform_fn) in source_columns.iter().zip(&self.transform_functions) {
            let partition_value = transform_fn.transform(source_column.clone())?;
            partition_values.push(partition_value);
        }

        // Construct the StructArray
        let struct_array = StructArray::try_new(expected_struct_fields, partition_values, None)
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to create partition struct array: {e}"),
                )
            })?;

        Ok(Arc::new(struct_array))
    }
}

/// Compare two arrow schemas while ignoring `Field` metadata (recursively
/// for struct types). `DataType` itself carries no metadata, so primitive
/// data types fall through to derived equality.
fn shapes_match(expected: &ArrowSchema, actual: &ArrowSchema) -> bool {
    fn fields_match(a: &Field, b: &Field) -> bool {
        a.name() == b.name()
            && a.is_nullable() == b.is_nullable()
            && match (a.data_type(), b.data_type()) {
                (DataType::Struct(af), DataType::Struct(bf)) => {
                    af.len() == bf.len() && af.iter().zip(bf).all(|(x, y)| fields_match(x, y))
                }
                (x, y) => x == y,
            }
    }
    let af = expected.fields();
    let bf = actual.fields();
    af.len() == bf.len() && af.iter().zip(bf).all(|(x, y)| fields_match(x, y))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::Field;

    use super::*;
    use crate::spec::{NestedField, PartitionSpecBuilder, PrimitiveType, Transform};

    #[test]
    fn test_partition_calculator_identity_transform() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpecBuilder::new(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();

        // Verify partition type
        assert_eq!(calculator.partition_type().fields().len(), 1);
        assert_eq!(calculator.partition_type().fields()[0].name, "id_partition");

        // Hand-built batch without field-id metadata: shape matches the
        // iceberg schema, so the cached-position fast path applies.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ])
        .unwrap();

        // Calculate partition values
        let result = calculator.calculate(&batch).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        let id_partition = struct_array
            .column_by_name("id_partition")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(id_partition.value(0), 10);
        assert_eq!(id_partition.value(1), 20);
        assert_eq!(id_partition.value(2), 30);
    }

    /// When shapes diverge, source columns must be resolved by field id, not
    /// by cached position.
    #[test]
    fn test_partition_calculator_runtime_field_id_fallback() {
        use std::collections::HashMap;

        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

        let field_id_meta = |id: i32| -> HashMap<String, String> {
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
        };

        // Iceberg has 3 fields. Partition source is `value` (id=2).
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "tag", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpecBuilder::new(Arc::new(table_schema.clone()))
            .add_partition_field("value", "value_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();

        // Shape-different runtime batch: 2 columns reordered so that
        // positional lookup for source id 2 (iceberg position 1) would pick
        // the wrong column.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("tag", DataType::Utf8, false).with_metadata(field_id_meta(3)),
            Field::new("value", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ])
        .unwrap();

        let result = calculator.calculate(&batch).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let value_partition = struct_array
            .column_by_name("value_partition")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(value_partition.value(0), 100);
        assert_eq!(value_partition.value(1), 200);
        assert_eq!(value_partition.value(2), 300);
    }

    #[test]
    fn test_partition_calculator_unpartitioned_error() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpecBuilder::new(Arc::new(table_schema.clone()))
            .build()
            .unwrap();

        let result = PartitionValueCalculator::try_new(&partition_spec, &table_schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unpartitioned table")
        );
    }
}
