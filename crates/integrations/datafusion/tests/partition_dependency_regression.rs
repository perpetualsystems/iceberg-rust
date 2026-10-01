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

//! SQL insert regression for partition sources rewritten by projection fusion.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::IcebergCatalogProvider;

async fn run_case(select: &str, expected: &[i32], disable_projection_pushdown: bool) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    dir.path().display().to_string(),
                )]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("test".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "part", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "part", Transform::Identity)
        .unwrap()
        .build();
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("dest".to_string())
                .schema(schema)
                .partition_spec(spec)
                .properties(HashMap::new())
                .build(),
        )
        .await
        .unwrap();

    let ctx = if disable_projection_pushdown {
        let defaults = SessionContext::new().state();
        let rules = defaults
            .physical_optimizers()
            .iter()
            .filter(|rule| rule.name() != "ProjectionPushdown")
            .cloned()
            .collect();
        SessionContext::new_with_state(
            SessionStateBuilder::new_with_default_features()
                .with_physical_optimizer_rules(rules)
                .build(),
        )
    } else {
        SessionContext::new()
    };
    ctx.register_catalog(
        "ice",
        Arc::new(
            IcebergCatalogProvider::try_new(catalog.clone())
                .await
                .unwrap(),
        ),
    );
    let input_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("left_value", DataType::Int32, true),
        Field::new("right_value", DataType::Int32, true),
    ]));
    let input = RecordBatch::try_new(input_schema.clone(), vec![
        Arc::new(Int32Array::from(vec![1, 2])),
        Arc::new(Int32Array::from(vec![101, 202])),
    ])
    .unwrap();
    ctx.register_table(
        "src",
        Arc::new(MemTable::try_new(input_schema, vec![vec![input]]).unwrap()),
    )
    .unwrap();

    let sql = format!("INSERT INTO ice.test.dest {select}");
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    println!(
        "CASE {sql}; disable_projection_pushdown={disable_projection_pushdown}\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    collect(plan, ctx.task_ctx()).await.unwrap();
    let table = catalog
        .load_table(&TableIdent::new(namespace, "dest".to_string()))
        .await
        .unwrap();
    let files: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    println!(
        "stored_partitions={:?}",
        files
            .iter()
            .map(|file| file.partition())
            .collect::<Vec<_>>()
    );
    let all = ctx
        .sql("SELECT part FROM ice.test.dest ORDER BY part")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values: Vec<i32> = all
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        values, expected,
        "full scan should return the inserted values"
    );
    let filter = expected
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let filtered = ctx
        .sql(&format!(
            "SELECT part FROM ice.test.dest WHERE part IN ({filter}) ORDER BY part"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let filtered_values: Vec<i32> = filtered
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    println!("full_scan={values:?}; filtered_scan={filtered_values:?}; expected={expected:?}");
    filtered_values == expected
}

#[tokio::test]
async fn partition_dependencies_sql_insert() {
    let cases: [(&str, &[i32]); 3] = [
        (
            "SELECT left_value AS part, right_value AS value FROM src",
            &[1, 2],
        ),
        (
            "SELECT right_value AS part, left_value AS value FROM src",
            &[101, 202],
        ),
        (
            "SELECT left_value + 10 AS part, right_value AS value FROM src",
            &[11, 12],
        ),
    ];
    let mut failures = Vec::new();
    for disable in [false, true] {
        for (sql, expected) in cases {
            if !run_case(sql, expected, disable).await {
                failures.push((sql, disable));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "partition pruning lost inserted rows: {failures:?}"
    );
}
