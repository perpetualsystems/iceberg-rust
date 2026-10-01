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

//! Partition value projection for Iceberg tables.

use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StructArray, make_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ColumnarValue, ExecutionPlan};
use iceberg::arrow::{
    FieldMatchMode, PROJECTED_PARTITION_VALUE_COLUMN, PartitionValueCalculator,
    schema_to_arrow_schema, strip_metadata_from_schema,
};
use iceberg::spec::PartitionSpec;
use iceberg::table::Table;

use crate::to_datafusion_error;

/// Extends an ExecutionPlan with partition value calculations for Iceberg tables.
///
/// This function takes an input ExecutionPlan and extends it with an additional column
/// containing calculated partition values based on the table's partition specification.
/// For unpartitioned tables, returns the original plan unchanged.
///
/// # Arguments
/// * `input` - The input ExecutionPlan to extend
/// * `table` - The Iceberg table with partition specification
///
/// # Returns
/// * `Ok(Arc<dyn ExecutionPlan>)` - Extended plan with partition values column
/// * `Err` - If partition spec is not found or transformation fails
pub fn project_with_partition(
    input: Arc<dyn ExecutionPlan>,
    table: &Table,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    project_with_partition_with_match_mode(input, table, FieldMatchMode::Id)
}

/// Extends an ExecutionPlan with partition value calculations for Iceberg tables
/// using the requested partition-source matching mode.
pub fn project_with_partition_with_match_mode(
    input: Arc<dyn ExecutionPlan>,
    table: &Table,
    match_mode: FieldMatchMode,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let metadata = table.metadata();
    let partition_spec = metadata.default_partition_spec();
    let table_schema = metadata.current_schema();

    if partition_spec.is_unpartitioned() {
        return Ok(input);
    }

    let input_schema = input.schema();

    // Validate that input_schema matches the Iceberg table schema
    // Strip metadata from both schemas before comparison to ignore metadata differences
    let expected_arrow_schema =
        schema_to_arrow_schema(table_schema.as_ref()).map_err(to_datafusion_error)?;
    let input_schema_cleaned =
        strip_metadata_from_schema(&input_schema).map_err(to_datafusion_error)?;
    let expected_schema_cleaned =
        strip_metadata_from_schema(&expected_arrow_schema).map_err(to_datafusion_error)?;

    if input_schema_cleaned != expected_schema_cleaned {
        return Err(DataFusionError::Plan(format!(
            "Input schema does not match Iceberg table schema.\n\
             Expected schema: {expected_schema_cleaned}\n\
             Input schema: {input_schema_cleaned}"
        )));
    }

    let calculator = PartitionValueCalculator::try_new_with_match_mode(
        partition_spec.as_ref(),
        table_schema.as_ref(),
        match_mode,
    )
    .map_err(to_datafusion_error)?;

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(input_schema.fields().len() + 1);

    for (index, field) in input_schema.fields().iter().enumerate() {
        let column_expr = Arc::new(Column::new(field.name(), index));
        projection_exprs.push((column_expr, field.name().clone()));
    }

    let partition_expr = Arc::new(PartitionExpr::try_new(
        calculator,
        partition_spec.clone(),
        input_schema.clone(),
    )?);
    projection_exprs.push((partition_expr, PROJECTED_PARTITION_VALUE_COLUMN.to_string()));

    let projection = ProjectionExec::try_new(projection_exprs, input)?;
    Ok(Arc::new(projection))
}

/// PhysicalExpr implementation for partition value calculation
#[derive(Debug, Clone)]
pub struct PartitionExpr {
    calculator: Arc<PartitionValueCalculator>,
    partition_spec: Arc<PartitionSpec>,
    inputs: Vec<Arc<dyn PhysicalExpr>>,
    // Maps each partition field to its source expression; transforms may share a source.
    source_indices: Vec<usize>,
}

impl PartitionExpr {
    /// Bind partition sources to the input schema before DataFusion rewrites projections.
    /// Only partition sources become children, including field access for nested sources.
    /// Matching follows the calculator's name or field-ID policy at binding time.
    ///
    /// # Errors
    /// Returns an error if a partition source cannot be resolved in the input schema.
    pub fn try_new(
        calculator: PartitionValueCalculator,
        partition_spec: Arc<PartitionSpec>,
        input_schema: Arc<ArrowSchema>,
    ) -> DFResult<Self> {
        let paths = calculator
            .source_field_paths(input_schema.clone())
            .map_err(to_datafusion_error)?;
        let mut inputs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut source_indices = Vec::with_capacity(paths.len());
        let mut unique_paths = Vec::new();
        for path in paths {
            if let Some(index) = unique_paths.iter().position(|existing| existing == &path) {
                source_indices.push(index);
                continue;
            }
            let (&root, nested) = path
                .split_first()
                .ok_or_else(|| DataFusionError::Plan("Empty partition source path".to_string()))?;
            let field = input_schema.fields().get(root).ok_or_else(|| {
                DataFusionError::Plan(
                    "Partition source index is outside the input schema".to_string(),
                )
            })?;
            let mut input: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), root));
            if !nested.is_empty() {
                input = Arc::new(PartitionFieldExpr {
                    input,
                    path: nested.to_vec(),
                });
                // Validate the nested path while the binding schema is available.
                input.data_type(&input_schema)?;
            }
            source_indices.push(inputs.len());
            inputs.push(input);
            unique_paths.push(path);
        }
        Ok(Self {
            calculator: Arc::new(calculator),
            partition_spec,
            inputs,
            source_indices,
        })
    }
}

// Rewrites share the calculator and spec, but may compute different source values.
impl PartialEq for PartitionExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.calculator, &other.calculator)
            && Arc::ptr_eq(&self.partition_spec, &other.partition_spec)
            && self.inputs == other.inputs
            && self.source_indices == other.source_indices
    }
}

impl Eq for PartitionExpr {}

impl PhysicalExpr for PartitionExpr {
    fn data_type(&self, _input_schema: &ArrowSchema) -> DFResult<DataType> {
        Ok(self.calculator.partition_arrow_type().clone())
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> DFResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<DFResult<Vec<_>>>()?;
        let source_columns = self
            .source_indices
            .iter()
            .map(|&index| inputs[index].clone())
            .collect::<Vec<_>>();
        let array = self
            .calculator
            .calculate_from_columns(&source_columns)
            .map_err(to_datafusion_error)?;
        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        if children.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(format!(
                "PartitionExpr expected {} children, got {}",
                self.inputs.len(),
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            inputs: children,
            ..self.as_ref().clone()
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_names: Vec<String> = self
            .partition_spec
            .fields()
            .iter()
            .map(|pf| format!("{}({})", pf.transform, pf.name))
            .collect();
        write!(f, "iceberg_partition_values[{}]", field_names.join(", "))
    }
}

impl std::fmt::Display for PartitionExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_names: Vec<&str> = self
            .partition_spec
            .fields()
            .iter()
            .map(|pf| pf.name.as_str())
            .collect();
        write!(f, "iceberg_partition_values({})", field_names.join(", "))
    }
}

impl std::hash::Hash for PartitionExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Include rewritten inputs as well as the shared calculator and spec.
        Arc::as_ptr(&self.calculator).hash(state);
        Arc::as_ptr(&self.partition_spec).hash(state);
        self.inputs.hash(state);
        self.source_indices.hash(state);
    }
}

/// Extract a nested partition source while preserving nulls from every parent
/// struct, as Iceberg's RecordBatchProjector does. DataFusion's get_field only
/// returns the child array and does not propagate those parent nulls.
#[derive(Debug, Clone, Eq)]
struct PartitionFieldExpr {
    input: Arc<dyn PhysicalExpr>,
    path: Vec<usize>,
}

impl PartialEq for PartitionFieldExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.path == other.path
    }
}

impl std::hash::Hash for PartitionFieldExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.path.hash(state);
    }
}

impl PhysicalExpr for PartitionFieldExpr {
    fn data_type(&self, input_schema: &ArrowSchema) -> DFResult<DataType> {
        let mut data_type = self.input.data_type(input_schema)?;
        for &index in &self.path {
            let DataType::Struct(fields) = data_type else {
                return Err(DataFusionError::Plan(
                    "Partition source path requires a struct".to_string(),
                ));
            };
            data_type = fields
                .get(index)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "Partition source index is outside the struct".to_string(),
                    )
                })?
                .data_type()
                .clone();
        }
        Ok(data_type)
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> DFResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let mut array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        let mut nulls = array.logical_nulls();
        for &index in &self.path {
            let nested = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Partition source path requires a struct".to_string(),
                    )
                })?;
            array = nested
                .columns()
                .get(index)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Partition source index is outside the struct".to_string(),
                    )
                })?
                .clone();
            nulls = NullBuffer::union(nulls.as_ref(), array.logical_nulls().as_ref());
        }
        Ok(ColumnarValue::Array(make_array(
            array.to_data().into_builder().nulls(nulls).build()?,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let [input]: [Arc<dyn PhysicalExpr>; 1] = children.try_into().map_err(|_| {
            DataFusionError::Plan("PartitionFieldExpr expected one child".to_string())
        })?;
        Ok(Arc::new(Self {
            input,
            path: self.path.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for PartitionFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "iceberg_partition_field({}, {:?})",
            self.input, self.path
        )
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_plan::empty::EmptyExec;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Transform, Type};
    use iceberg::test_utils::test_runtime;

    use super::*;

    #[test]
    fn test_partition_calculator_basic() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();

        // Verify partition type
        assert_eq!(calculator.partition_type().fields().len(), 1);
        assert_eq!(calculator.partition_type().fields()[0].name, "id_partition");
    }

    #[test]
    fn test_partition_expr_with_projection() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = Arc::new(
            PartitionSpec::builder(Arc::new(table_schema.clone()))
                .add_partition_field("id", "id_partition", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let input = Arc::new(EmptyExec::new(arrow_schema.clone()));

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();

        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(arrow_schema.fields().len() + 1);
        for (i, field) in arrow_schema.fields().iter().enumerate() {
            let column_expr = Arc::new(Column::new(field.name(), i));
            projection_exprs.push((column_expr, field.name().clone()));
        }

        let partition_expr = Arc::new(
            PartitionExpr::try_new(calculator, partition_spec, arrow_schema.clone()).unwrap(),
        );
        projection_exprs.push((partition_expr, PROJECTED_PARTITION_VALUE_COLUMN.to_string()));

        let projection = ProjectionExec::try_new(projection_exprs, input).unwrap();
        let result = Arc::new(projection);

        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "name");
        assert_eq!(result.schema().field(2).name(), "_partition");
    }

    #[test]
    fn test_partition_expr_evaluate() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "a", "b", "c",
            ])),
        ])
        .unwrap();

        let partition_spec = Arc::new(partition_spec);
        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();
        let partition_type = calculator.partition_arrow_type().clone();
        let expr =
            PartitionExpr::try_new(calculator, partition_spec, arrow_schema.clone()).unwrap();

        assert_eq!(expr.data_type(&arrow_schema).unwrap(), partition_type);
        assert!(!expr.nullable(&arrow_schema).unwrap());

        let result = expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Array(array) => {
                let struct_array = Array::as_any(array.as_ref())
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let id_partition = struct_array
                    .column_by_name("id_partition")
                    .unwrap()
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(id_partition.value(0), 10);
                assert_eq!(id_partition.value(1), 20);
                assert_eq!(id_partition.value(2), 30);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_nested_partition() {
        let address_struct = StructType::new(vec![
            NestedField::required(3, "street", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "city", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "address", Type::Struct(address_struct)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("address.city", "city_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields), false),
        ]));

        let street_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "123 Main St",
            "456 Oak Ave",
        ]));
        let city_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "New York",
            "Los Angeles",
        ]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("street", DataType::Utf8, false)),
                street_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("city", DataType::Utf8, false)),
                city_array as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(struct_array),
        ])
        .unwrap();

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();
        let array = calculator.calculate(&batch).unwrap();

        let struct_array = Array::as_any(array.as_ref())
            .downcast_ref::<StructArray>()
            .unwrap();
        let city_partition = struct_array
            .column_by_name("city_partition")
            .unwrap()
            .as_ref()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(city_partition.value(0), "New York");
        assert_eq!(city_partition.value(1), "Los Angeles");
    }

    #[test]
    fn test_schema_validation_matching_schemas() {
        use iceberg::TableIdent;
        use iceberg::io::FileIO;
        use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};

        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = PartitionSpec::builder(table_schema.clone())
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder()
            .build(&table_schema)
            .unwrap();

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            (*table_schema).clone(),
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();

        // Create Arrow schema matching the table schema
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let input = Arc::new(EmptyExec::new(arrow_schema));

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location("/test/metadata.json")
            .runtime(test_runtime())
            .build()
            .unwrap();

        let result = project_with_partition(input, &table);
        assert!(result.is_ok(), "Schema validation should pass");
    }

    #[test]
    fn test_schema_validation_mismatched_schemas() {
        use iceberg::TableIdent;
        use iceberg::io::FileIO;
        use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};

        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = PartitionSpec::builder(table_schema.clone())
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder()
            .build(&table_schema)
            .unwrap();

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            (*table_schema).clone(),
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();

        // Create Arrow schema with different field name (mismatched)
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("different_name", DataType::Utf8, false), // Wrong field name
        ]));

        let input = Arc::new(EmptyExec::new(arrow_schema));

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location("/test/metadata.json")
            .runtime(test_runtime())
            .build()
            .unwrap();

        let result = project_with_partition(input, &table);
        assert!(
            result.is_err(),
            "Schema validation should fail for mismatched schemas"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Input schema does not match Iceberg table schema")
        );
    }

    #[test]
    fn test_schema_validation_with_metadata_differences() {
        use std::collections::HashMap;

        use iceberg::TableIdent;
        use iceberg::io::FileIO;
        use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};

        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = PartitionSpec::builder(table_schema.clone())
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder()
            .build(&table_schema)
            .unwrap();

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            (*table_schema).clone(),
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();

        // Create Arrow schema with metadata (should be ignored in comparison)
        let mut metadata = HashMap::new();
        metadata.insert("extra".to_string(), "metadata".to_string());

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(metadata.clone()),
            Field::new("name", DataType::Utf8, false).with_metadata(metadata),
        ]));

        let input = Arc::new(EmptyExec::new(arrow_schema));

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location("/test/metadata.json")
            .runtime(test_runtime())
            .build()
            .unwrap();

        let result = project_with_partition(input, &table);
        assert!(
            result.is_ok(),
            "Schema validation should pass even with metadata differences"
        );
    }

    /// ProjectionPushdown must rewrite partition sources when fusing a NULL-filling
    /// alignment projection into the partition projection.
    #[tokio::test]
    async fn test_partition_expr_survives_projection_unification() {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::common::ScalarValue;
        use datafusion::config::ConfigOptions;
        use datafusion::datasource::{MemTable, TableProvider};
        use datafusion::physical_expr::expressions::Literal;
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
        use datafusion::prelude::SessionContext;
        use iceberg::TableIdent;
        use iceberg::io::FileIO;
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Transform, Type,
        };

        // Iceberg schema: [a, b, c, d] with partition source `c` (id=3).
        // Column `b` will be NULL-filled by the upstream alignment projection.
        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "c", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::required(4, "d", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = PartitionSpec::builder(table_schema.clone())
            .add_partition_field("c", "c_day", Transform::Day)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = SortOrder::builder().build(&table_schema).unwrap();

        let table_metadata = TableMetadataBuilder::new(
            (*table_schema).clone(),
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap();

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location("/test/metadata.json".to_string())
            .runtime(test_runtime())
            .build()
            .unwrap();

        // Source plan: 3 columns [a, c, d] carrying PARQUET:field_id, as the
        // parquet read path produces in production.
        let field_id_meta = |id: i32| -> HashMap<String, String> {
            HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])
        };
        let source_arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(field_id_meta(1)),
            Field::new(
                "c",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            )
            .with_metadata(field_id_meta(3)),
            Field::new(
                "d",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            )
            .with_metadata(field_id_meta(4)),
        ]));

        // c values: Day(c) = [0, 1]. d values: Day(d) = [100, 200].
        // Distinct enough to tell the two apart in the assertion.
        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let batch = RecordBatch::try_new(source_arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![0, MICROS_PER_DAY]).with_timezone("+00:00"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![100 * MICROS_PER_DAY, 200 * MICROS_PER_DAY])
                    .with_timezone("+00:00"),
            ),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(source_arrow_schema, vec![vec![batch]]).unwrap();
        let source_plan = mem_table.scan(&ctx.state(), None, &[], None).await.unwrap();

        // NULL-filling alignment projection — what DataFusion's INSERT
        // analyzer emits to coerce a source to the table schema.
        let null_b: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Utf8(None)));
        let aligned_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (null_b, "b".to_string()),
            (Arc::new(Column::new("c", 1)), "c".to_string()),
            (Arc::new(Column::new("d", 2)), "d".to_string()),
        ];
        let aligned_plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(aligned_exprs, source_plan).unwrap());

        let unoptimized_plan = project_with_partition(aligned_plan, &table).unwrap();

        let optimized_plan = ProjectionPushdown::new()
            .optimize(unoptimized_plan, &ConfigOptions::default())
            .unwrap();

        // Precondition: the two ProjectionExecs must actually have fused;
        // otherwise this regression test is vacuous.
        assert!(
            !optimized_plan.children()[0].is::<ProjectionExec>(),
            "ProjectionPushdown did not fuse the two ProjectionExecs",
        );

        let results = datafusion::physical_plan::collect(optimized_plan, ctx.task_ctx())
            .await
            .unwrap();
        let partitions = extract_c_day_partitions(&results);

        // The rewritten source must select c, not the old positional column d.
        assert_eq!(partitions, vec![0, 1]);
    }

    /// Name mode must follow event_time AS c when projection fusion removes
    /// the alias, even without target PARQUET:field_id metadata.
    #[tokio::test]
    async fn test_partition_expr_name_mode_survives_projection_unification_without_field_ids() {
        use std::collections::HashMap;

        use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::common::ScalarValue;
        use datafusion::config::ConfigOptions;
        use datafusion::datasource::{MemTable, TableProvider};
        use datafusion::physical_expr::expressions::Literal;
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
        use datafusion::prelude::SessionContext;
        use iceberg::TableIdent;
        use iceberg::io::FileIO;
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Transform, Type,
        };

        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "c", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::required(4, "d", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = PartitionSpec::builder(table_schema.clone())
            .add_partition_field("c", "c_day", Transform::Day)
            .unwrap()
            .build()
            .unwrap();
        let sort_order = SortOrder::builder().build(&table_schema).unwrap();
        let table_metadata = TableMetadataBuilder::new(
            (*table_schema).clone(),
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location("/test/metadata.json".to_string())
            .runtime(test_runtime())
            .build()
            .unwrap();

        let source_arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            ),
            Field::new(
                "d",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            ),
        ]));

        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let batch = RecordBatch::try_new(source_arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![0, MICROS_PER_DAY]).with_timezone("+00:00"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![100 * MICROS_PER_DAY, 200 * MICROS_PER_DAY])
                    .with_timezone("+00:00"),
            ),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(source_arrow_schema, vec![vec![batch]]).unwrap();
        let source_plan = mem_table.scan(&ctx.state(), None, &[], None).await.unwrap();

        let null_b: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Utf8(None)));
        let aligned_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (null_b, "b".to_string()),
            (Arc::new(Column::new("event_time", 1)), "c".to_string()),
            (Arc::new(Column::new("d", 2)), "d".to_string()),
        ];
        let aligned_plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(aligned_exprs, source_plan).unwrap());

        let unoptimized_plan =
            project_with_partition_with_match_mode(aligned_plan, &table, FieldMatchMode::Name)
                .unwrap();
        let optimized_plan = ProjectionPushdown::new()
            .optimize(unoptimized_plan, &ConfigOptions::default())
            .unwrap();

        assert!(
            !optimized_plan.children()[0].is::<ProjectionExec>(),
            "ProjectionPushdown did not fuse the two ProjectionExecs",
        );

        let results = datafusion::physical_plan::collect(optimized_plan, ctx.task_ctx())
            .await
            .unwrap();
        let partitions = extract_c_day_partitions(&results);

        assert_eq!(partitions, vec![0, 1]);
    }

    /// Exercise the actual optimizer, with a partition-only output so unrelated
    /// dependencies cannot hide behind the table's passthrough columns.
    async fn optimize_partition_projection(
        batch: RecordBatch,
        projection: Vec<(Arc<dyn PhysicalExpr>, String)>,
        table_schema: &Schema,
        spec: Arc<PartitionSpec>,
        mode: FieldMatchMode,
    ) -> Vec<RecordBatch> {
        use datafusion::config::ConfigOptions;
        use datafusion::datasource::{MemTable, TableProvider};
        use datafusion::physical_expr::utils::collect_columns;
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
        use datafusion::prelude::SessionContext;

        let ctx = SessionContext::new();
        let source = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .unwrap()
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();
        let aligned: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(projection, source).unwrap());
        let calculator =
            PartitionValueCalculator::try_new_with_match_mode(&spec, table_schema, mode).unwrap();
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(PartitionExpr::try_new(calculator, spec, aligned.schema()).unwrap());
        // All cases below partition on one source, sometimes with multiple transforms.
        assert_eq!(expr.children().len(), 1);
        assert_eq!(collect_columns(&expr).len(), 1);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![(expr, PROJECTED_PARTITION_VALUE_COLUMN.to_string())],
                aligned,
            )
            .unwrap(),
        );
        let expected = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();
        let optimized = ProjectionPushdown::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        assert!(optimized.as_ref().is::<ProjectionExec>());
        assert!(
            !optimized.children()[0].is::<ProjectionExec>(),
            "projections must fuse"
        );
        let partition = &optimized.downcast_ref::<ProjectionExec>().unwrap().expr()[0].expr;
        assert!(
            collect_columns(partition)
                .iter()
                .all(|column| column.name() != "unrelated"),
            "unrelated input must not be a dependency"
        );
        let actual = datafusion::physical_plan::collect(optimized, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(actual, expected);
        actual
    }

    #[tokio::test]
    async fn test_partition_sources_rewritten_by_projection_optimizer() {
        use datafusion::arrow::array::{Date32Array, Int64Array, TimestampMicrosecondArray};
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::physical_expr::expressions::CastExpr;

        const DAY: i64 = 86_400_000_000;
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let table_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::required(2, "unrelated", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let spec = Arc::new(
            PartitionSpec::builder(Arc::new(table_schema.clone()))
                .add_partition_field("timestamp", "c_day", Transform::Day)
                .unwrap()
                .add_partition_field("timestamp", "original", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        // The source is reordered, and has no column named `timestamp`.
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("unrelated", DataType::Int32, false),
                Field::new("event_time", timestamp_type.clone(), true),
                Field::new("micros", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![99; 4])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(DAY),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(2 * DAY),
                    Some(3 * DAY),
                    Some(4 * DAY),
                    None,
                ])),
            ],
        )
        .unwrap();
        let cases = [
            (
                Arc::new(Column::new("event_time", 1)) as Arc<dyn PhysicalExpr>,
                vec![Some(-1), Some(0), Some(1), None],
            ),
            (
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("micros", 2)),
                    timestamp_type,
                    None,
                )),
                vec![Some(2), Some(3), Some(4), None],
            ),
            (
                Arc::new(Literal::new(ScalarValue::TimestampMicrosecond(
                    Some(5 * DAY),
                    None,
                ))),
                vec![Some(5); 4],
            ),
        ];
        for mode in [FieldMatchMode::Name, FieldMatchMode::Id] {
            for (source, expected) in &cases {
                let result = optimize_partition_projection(
                    batch.clone(),
                    vec![
                        (source.clone(), "timestamp".to_string()),
                        (
                            Arc::new(Column::new("unrelated", 0)),
                            "unrelated".to_string(),
                        ),
                    ],
                    &table_schema,
                    spec.clone(),
                    mode,
                )
                .await;
                let partitions = Array::as_any(result[0].column(0).as_ref())
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let days = Array::as_any(partitions.column(0).as_ref())
                    .downcast_ref::<Date32Array>()
                    .unwrap();
                assert_eq!(days.iter().collect::<Vec<_>>(), *expected);
                let original = Array::as_any(partitions.column(1).as_ref())
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                assert_eq!(
                    original
                        .iter()
                        .map(|v| v.map(|v| v.div_euclid(DAY) as i32))
                        .collect::<Vec<_>>(),
                    *expected
                );
            }
        }
    }

    #[tokio::test]
    async fn test_nested_partition_sources_rewritten_by_projection_optimizer() {
        use std::collections::HashMap;

        use datafusion::arrow::buffer::NullBuffer;

        let table_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "payload",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(
                            2,
                            "inner",
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(
                                    3,
                                    "value",
                                    Type::Primitive(PrimitiveType::Int),
                                )
                                .into(),
                            ])),
                        )
                        .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let spec = Arc::new(
            PartitionSpec::builder(Arc::new(table_schema.clone()))
                .add_partition_field("payload.inner.value", "value", Transform::Truncate(10))
                .unwrap()
                .build()
                .unwrap(),
        );
        let metadata = |id: i32| {
            HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])
        };
        for mode in [FieldMatchMode::Name, FieldMatchMode::Id] {
            // ID matching resolves renamed nested fields and a reordered leaf.
            let (inner_name, value_name) = match mode {
                FieldMatchMode::Name => ("inner", "value"),
                FieldMatchMode::Id => ("renamed_inner", "renamed_value"),
            };
            let leaf_fields = Fields::from(vec![
                Field::new("sibling", DataType::Int32, true).with_metadata(metadata(4)),
                Field::new(value_name, DataType::Int32, true).with_metadata(metadata(3)),
            ]);
            let inner = StructArray::new(
                leaf_fields.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![999; 4])),
                    Arc::new(Int32Array::from(vec![Some(19), Some(29), Some(39), None])),
                ],
                Some(NullBuffer::from(vec![true, false, true, true])),
            );
            let outer_fields = Fields::from(vec![
                Field::new(inner_name, DataType::Struct(leaf_fields), true)
                    .with_metadata(metadata(2)),
            ]);
            let outer = StructArray::new(
                outer_fields.clone(),
                vec![Arc::new(inner)],
                Some(NullBuffer::from(vec![true, true, false, true])),
            );
            let batch = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("unrelated", DataType::Int32, false).with_metadata(metadata(5)),
                    Field::new("event", DataType::Struct(outer_fields), true)
                        .with_metadata(metadata(1)),
                ])),
                vec![Arc::new(Int32Array::from(vec![99; 4])), Arc::new(outer)],
            )
            .unwrap();
            let result = optimize_partition_projection(
                batch,
                vec![
                    (
                        Arc::new(Column::new("unrelated", 0)),
                        "unrelated".to_string(),
                    ),
                    (Arc::new(Column::new("event", 1)), "payload".to_string()),
                ],
                &table_schema,
                spec.clone(),
                mode,
            )
            .await;
            let partitions = Array::as_any(result[0].column(0).as_ref())
                .downcast_ref::<StructArray>()
                .unwrap();
            let values = Array::as_any(partitions.column(0).as_ref())
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.iter().collect::<Vec<_>>(), vec![
                Some(10),
                None,
                None,
                None
            ]);
        }
    }

    #[test]
    fn test_partition_expr_replacement_identity() {
        use std::collections::HashSet;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let spec = Arc::new(
            PartitionSpec::builder(Arc::new(schema.clone()))
                .add_partition_field("id", "id", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        let calculator = PartitionValueCalculator::try_new(&spec, &schema).unwrap();
        let expr = Arc::new(
            PartitionExpr::try_new(
                calculator,
                spec,
                Arc::new(schema_to_arrow_schema(&schema).unwrap()),
            )
            .unwrap(),
        );
        assert!(expr.clone().with_new_children(vec![]).is_err());
        assert!(
            expr.clone()
                .with_new_children(vec![expr.inputs[0].clone(); 2])
                .is_err()
        );
        let unchanged = expr.clone().with_new_children(expr.inputs.clone()).unwrap();
        let rewritten = expr
            .clone()
            .with_new_children(vec![Arc::new(Literal::new(ScalarValue::Int32(Some(42))))])
            .unwrap();
        let original: Arc<dyn PhysicalExpr> = expr;
        assert!(original.eq(&unchanged));
        assert!(!original.eq(&rewritten));
        let mut expressions = HashSet::new();
        expressions.insert(original);
        expressions.insert(unchanged);
        expressions.insert(rewritten);
        assert_eq!(expressions.len(), 2);
    }

    fn extract_c_day_partitions(batches: &[RecordBatch]) -> Vec<i32> {
        use datafusion::arrow::array::{Array, Date32Array, StructArray};

        let mut out = vec![];
        for batch in batches {
            let partition_idx = batch
                .schema()
                .index_of(PROJECTED_PARTITION_VALUE_COLUMN)
                .expect("_partition column missing from output");
            let struct_array = Array::as_any(batch.column(partition_idx).as_ref())
                .downcast_ref::<StructArray>()
                .expect("_partition should be a StructArray");
            let c_day = Array::as_any(
                struct_array
                    .column_by_name("c_day")
                    .expect("c_day field missing from _partition struct")
                    .as_ref(),
            )
            .downcast_ref::<Date32Array>()
            .expect("c_day should be Date32 (Day transform output)");
            for i in 0..c_day.len() {
                out.push(c_day.value(i));
            }
        }
        out
    }
}
