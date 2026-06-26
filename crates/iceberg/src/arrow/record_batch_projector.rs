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

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray, make_array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema::schema_to_arrow_schema;
use crate::error::Result;
use crate::spec::Schema as IcebergSchema;
use crate::{Error, ErrorKind};

/// Help to project specific field from `RecordBatch`` according to the fields id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordBatchProjector {
    // A vector of vectors, where each inner vector represents the index path to access a specific field in a nested structure.
    // E.g. [[0], [1, 2]] means the first field is accessed directly from the first column,
    // while the second field is accessed from the second column and then from its third subcolumn (second column must be a struct column).
    field_indices: Vec<Vec<usize>>,
    // The schema reference after projection. This schema is derived from the original schema based on the given field IDs.
    projected_schema: SchemaRef,
}

/// Read a `PARQUET:field_id` from `field`'s metadata, parsing it as `i64`.
///
/// Returns `Ok(None)` when the field has no field-id metadata, and an error
/// when the metadata is present but unparseable.
pub(crate) fn parquet_field_id(field: &Field) -> Result<Option<i64>> {
    let Some(value) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Ok(None);
    };
    let id = value.parse::<i32>().map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Failed to parse field id")
            .with_context("value", value)
            .with_source(e)
    })?;
    Ok(Some(id as i64))
}

impl RecordBatchProjector {
    /// Init ArrowFieldProjector
    ///
    /// This function will iterate through the field and fetch the field from the original schema according to the field ids.
    /// The function to fetch the field id from the field is provided by `field_id_fetch_func`, return None if the field need to be skipped.
    /// This function will iterate through the nested fields if the field is a struct, `searchable_field_func` can be used to control whether
    /// iterate into the nested fields.
    pub(crate) fn new<F1, F2>(
        original_schema: SchemaRef,
        field_ids: &[i32],
        field_id_fetch_func: F1,
        searchable_field_func: F2,
    ) -> Result<Self>
    where
        F1: Fn(&Field) -> Result<Option<i64>>,
        F2: Fn(&Field) -> bool,
    {
        let mut field_indices = Vec::with_capacity(field_ids.len());
        let mut fields = Vec::with_capacity(field_ids.len());
        for &id in field_ids {
            let mut field_index = vec![];
            let field = Self::fetch_field_index(
                original_schema.fields(),
                &mut field_index,
                id as i64,
                &field_id_fetch_func,
                &searchable_field_func,
            )?
            .ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "Field not found")
                    .with_context("field_id", id.to_string())
            })?;
            fields.push(field.clone());
            field_indices.push(field_index);
        }
        let delete_arrow_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            field_indices,
            projected_schema: delete_arrow_schema,
        })
    }

    /// Create RecordBatchProjector using Iceberg schema.
    ///
    /// This constructor converts the Iceberg schema to Arrow schema with field ID metadata,
    /// then uses the standard field ID lookup for projection.
    ///
    /// # Arguments
    /// * `iceberg_schema` - The Iceberg schema for field ID mapping  
    /// * `target_field_ids` - The field IDs to project
    pub fn from_iceberg_schema(
        iceberg_schema: Arc<IcebergSchema>,
        target_field_ids: &[i32],
    ) -> Result<Self> {
        let arrow_schema_with_ids = Arc::new(schema_to_arrow_schema(&iceberg_schema)?);
        Self::new(
            arrow_schema_with_ids,
            target_field_ids,
            parquet_field_id,
            |_| true,
        )
    }

    /// Create RecordBatchProjector by resolving fields from Arrow field names.
    ///
    /// Each name may be either a top-level field name or a dot-separated path
    /// through nested struct fields.
    pub(crate) fn new_by_names(original_schema: SchemaRef, field_names: &[String]) -> Result<Self> {
        let mut field_indices = Vec::with_capacity(field_names.len());
        let mut fields = Vec::with_capacity(field_names.len());
        for name in field_names {
            let mut field_index = vec![];
            let field =
                Self::fetch_field_index_by_name(original_schema.fields(), &mut field_index, name)?
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "Field not found")
                            .with_context("field_name", name.clone())
                    })?;
            fields.push(field.clone());
            field_indices.push(field_index);
        }
        let projected_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            field_indices,
            projected_schema,
        })
    }

    fn fetch_field_index<F1, F2>(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        target_field_id: i64,
        field_id_fetch_func: &F1,
        searchable_field_func: &F2,
    ) -> Result<Option<FieldRef>>
    where
        F1: Fn(&Field) -> Result<Option<i64>>,
        F2: Fn(&Field) -> bool,
    {
        for (pos, field) in fields.iter().enumerate() {
            let id = field_id_fetch_func(field)?;
            if let Some(id) = id
                && target_field_id == id
            {
                index_vec.push(pos);
                return Ok(Some(field.clone()));
            }
            if let DataType::Struct(inner) = field.data_type()
                && searchable_field_func(field)
                && let Some(res) = Self::fetch_field_index(
                    inner,
                    index_vec,
                    target_field_id,
                    field_id_fetch_func,
                    searchable_field_func,
                )?
            {
                index_vec.push(pos);
                return Ok(Some(res));
            }
        }
        Ok(None)
    }

    fn fetch_field_index_by_name(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        target_field_name: &str,
    ) -> Result<Option<FieldRef>> {
        let mut path = target_field_name.split('.');
        let Some(first) = path.next() else {
            return Ok(None);
        };
        Self::fetch_field_index_by_name_path(fields, index_vec, first, path)
    }

    fn fetch_field_index_by_name_path<'a>(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        target_name: &str,
        mut remaining_path: impl Iterator<Item = &'a str> + Clone,
    ) -> Result<Option<FieldRef>> {
        for (pos, field) in fields.iter().enumerate() {
            if field.name() != target_name {
                continue;
            }

            let Some(next_name) = remaining_path.next() else {
                index_vec.push(pos);
                return Ok(Some(field.clone()));
            };

            let DataType::Struct(inner) = field.data_type() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot resolve nested field by name through non-struct field",
                )
                .with_context("field_name", field.name().to_string()));
            };

            if let Some(res) = Self::fetch_field_index_by_name_path(
                inner,
                index_vec,
                next_name,
                remaining_path.clone(),
            )? {
                index_vec.push(pos);
                return Ok(Some(res));
            }

            return Ok(None);
        }
        Ok(None)
    }

    /// Return the reference of projected schema
    pub(crate) fn projected_schema_ref(&self) -> &SchemaRef {
        &self.projected_schema
    }

    /// Do projection with record batch
    pub(crate) fn project_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.projected_schema.clone(),
            self.project_column(batch.columns())?,
        )
        .map_err(|err| Error::new(ErrorKind::DataInvalid, format!("{err}")))
    }

    /// Do projection with columns
    pub fn project_column(&self, batch: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        self.field_indices
            .iter()
            .map(|index_vec| Self::get_column_by_field_index(batch, index_vec))
            .collect::<Result<Vec<_>>>()
    }

    /// Project columns by looking up each target field id in the runtime
    /// batch's `PARQUET:field_id` metadata. Returns an error if any target
    /// field id is missing from the batch's schema.
    pub fn project_columns_by_field_id(
        batch: &RecordBatch,
        target_field_ids: &[i32],
    ) -> Result<Vec<ArrayRef>> {
        let projector = Self::new(batch.schema(), target_field_ids, parquet_field_id, |_| true)?;
        projector.project_column(batch.columns())
    }

    /// Project columns by looking up each target field name in the runtime
    /// batch's Arrow schema. Names may be dot-separated paths through nested
    /// struct fields.
    pub fn project_columns_by_name(
        batch: &RecordBatch,
        target_field_names: &[String],
    ) -> Result<Vec<ArrayRef>> {
        let projector = Self::new_by_names(batch.schema(), target_field_names)?;
        projector.project_column(batch.columns())
    }

    pub(crate) fn get_column_by_field_index(
        batch: &[ArrayRef],
        field_index: &[usize],
    ) -> Result<ArrayRef> {
        let mut rev_iterator = field_index.iter().rev();
        let mut array = batch[*rev_iterator.next().unwrap()].clone();
        let mut null_buffer = array.logical_nulls();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot convert Array to StructArray",
                ))?
                .column(*idx)
                .clone();
            null_buffer = NullBuffer::union(null_buffer.as_ref(), array.logical_nulls().as_ref());
        }
        Ok(make_array(
            array.to_data().into_builder().nulls(null_buffer).build()?,
        ))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};

    use crate::arrow::record_batch_projector::RecordBatchProjector;
    use crate::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use crate::{Error, ErrorKind};

    #[test]
    fn test_record_batch_projector_nested_level() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];
        let fields = vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let field_id_fetch_func = |field: &Field| match field.name().as_str() {
            "field1" => Ok(Some(1)),
            "field2" => Ok(Some(2)),
            "inner_field1" => Ok(Some(3)),
            "inner_field2" => Ok(Some(4)),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field id not found")),
        };
        let projector =
            RecordBatchProjector::new(schema.clone(), &[1, 3], field_id_fetch_func, |_| true)
                .unwrap();

        assert_eq!(projector.field_indices.len(), 2);
        assert_eq!(projector.field_indices[0], vec![0]);
        assert_eq!(projector.field_indices[1], vec![0, 1]);

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let inner_int_array = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let inner_string_array = Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef;
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(inner_fields[0].clone()),
                inner_int_array as ArrayRef,
            ),
            (
                Arc::new(inner_fields[1].clone()),
                inner_string_array as ArrayRef,
            ),
        ])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![int_array, struct_array]).unwrap();

        let projected_batch = projector.project_batch(batch).unwrap();
        assert_eq!(projected_batch.num_columns(), 2);
        let projected_int_array = projected_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let projected_inner_int_array = projected_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(projected_int_array.values(), &[1, 2, 3]);
        assert_eq!(projected_inner_int_array.values(), &[4, 5, 6]);
    }

    #[test]
    fn test_field_not_found() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];

        let fields = vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let field_id_fetch_func = |field: &Field| match field.name().as_str() {
            "field1" => Ok(Some(1)),
            "field2" => Ok(Some(2)),
            "inner_field1" => Ok(Some(3)),
            "inner_field2" => Ok(Some(4)),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field id not found")),
        };
        let projector =
            RecordBatchProjector::new(schema.clone(), &[1, 5], field_id_fetch_func, |_| true);

        assert!(projector.is_err());
    }

    #[test]
    fn test_field_not_reachable() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];

        let fields = vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let field_id_fetch_func = |field: &Field| match field.name().as_str() {
            "field1" => Ok(Some(1)),
            "field2" => Ok(Some(2)),
            "inner_field1" => Ok(Some(3)),
            "inner_field2" => Ok(Some(4)),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field id not found")),
        };
        let projector =
            RecordBatchProjector::new(schema.clone(), &[3], field_id_fetch_func, |_| false);
        assert!(projector.is_err());

        let projector =
            RecordBatchProjector::new(schema.clone(), &[3], field_id_fetch_func, |_| true);
        assert!(projector.is_ok());
    }

    #[test]
    fn test_record_batch_projector_name_lookup_top_level_and_nested() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];
        let fields = vec![
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
            Field::new("field1", DataType::Int32, false),
        ];
        let schema = Arc::new(Schema::new(fields));
        let field_names = vec!["field1".to_string(), "field2.inner_field2".to_string()];

        let projector = RecordBatchProjector::new_by_names(schema.clone(), &field_names).unwrap();

        assert_eq!(projector.field_indices.len(), 2);
        assert_eq!(projector.field_indices[0], vec![1]);
        assert_eq!(projector.field_indices[1], vec![1, 0]);
        assert_eq!(projector.projected_schema_ref().fields().len(), 2);
        assert_eq!(projector.projected_schema_ref().field(0).name(), "field1");
        assert_eq!(
            projector.projected_schema_ref().field(1).name(),
            "inner_field2"
        );

        let inner_int_array = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let inner_string_array = Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef;
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(inner_fields[0].clone()),
                inner_int_array as ArrayRef,
            ),
            (
                Arc::new(inner_fields[1].clone()),
                inner_string_array as ArrayRef,
            ),
        ])) as ArrayRef;
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![struct_array, int_array]).unwrap();

        let projected_batch = projector.project_batch(batch).unwrap();
        assert_eq!(projected_batch.num_columns(), 2);
        let projected_int_array = projected_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let projected_inner_string_array = projected_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(projected_int_array.values(), &[1, 2, 3]);
        assert_eq!(projected_inner_string_array.value(0), "x");
        assert_eq!(projected_inner_string_array.value(1), "y");
        assert_eq!(projected_inner_string_array.value(2), "z");
    }

    #[test]
    fn test_project_columns_by_name_uses_runtime_schema_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
        ])
        .unwrap();
        let field_names = vec!["a".to_string(), "b".to_string()];

        let columns = RecordBatchProjector::project_columns_by_name(&batch, &field_names).unwrap();

        assert_eq!(columns.len(), 2);
        let a = columns[0].as_any().downcast_ref::<Int32Array>().unwrap();
        let b = columns[1].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.values(), &[10, 20, 30]);
        assert_eq!(b.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_project_columns_by_name_preserves_nested_nulls() {
        let inner_fields = Fields::from(vec![Field::new("inner_field", DataType::Int32, true)]);
        let fields = vec![Field::new(
            "field2",
            DataType::Struct(inner_fields.clone()),
            true,
        )];
        let schema = Arc::new(Schema::new(fields));
        let inner_int_array = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let nulls = NullBuffer::from(vec![true, false, true]);
        let struct_array = Arc::new(StructArray::new(
            inner_fields,
            vec![inner_int_array],
            Some(nulls),
        )) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![struct_array]).unwrap();
        let field_names = vec!["field2.inner_field".to_string()];

        let columns = RecordBatchProjector::project_columns_by_name(&batch, &field_names).unwrap();

        assert_eq!(columns.len(), 1);
        let projected = columns[0].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(projected.len(), 3);
        assert_eq!(projected.value(0), 1);
        assert!(projected.is_null(1));
        assert!(projected.is_null(2));
    }

    #[test]
    fn test_name_field_not_found() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field1",
            DataType::Int32,
            false,
        )]));
        let field_names = vec!["missing".to_string()];

        let projector = RecordBatchProjector::new_by_names(schema, &field_names);

        assert!(projector.is_err());
        assert!(
            projector
                .unwrap_err()
                .to_string()
                .contains("Field not found")
        );
    }

    #[test]
    fn test_nested_name_field_not_found() {
        let inner_fields = vec![Field::new("inner_field1", DataType::Int32, false)];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field2",
            DataType::Struct(Fields::from(inner_fields)),
            false,
        )]));
        let field_names = vec!["field2.missing".to_string()];

        let projector = RecordBatchProjector::new_by_names(schema, &field_names);

        assert!(projector.is_err());
        assert!(
            projector
                .unwrap_err()
                .to_string()
                .contains("Field not found")
        );
    }

    #[test]
    fn test_nested_name_through_non_struct_field() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field1",
            DataType::Int32,
            false,
        )]));
        let field_names = vec!["field1.inner_field".to_string()];

        let projector = RecordBatchProjector::new_by_names(schema, &field_names);

        assert!(projector.is_err());
        assert!(
            projector
                .unwrap_err()
                .to_string()
                .contains("Cannot resolve nested field by name through non-struct field")
        );
    }

    #[test]
    fn test_from_iceberg_schema() {
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "age", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let projector =
            RecordBatchProjector::from_iceberg_schema(Arc::new(iceberg_schema), &[1, 3]).unwrap();

        assert_eq!(projector.field_indices.len(), 2);
        assert_eq!(projector.projected_schema_ref().fields().len(), 2);
        assert_eq!(projector.projected_schema_ref().field(0).name(), "id");
        assert_eq!(projector.projected_schema_ref().field(1).name(), "age");
    }
}
