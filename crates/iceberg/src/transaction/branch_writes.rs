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

//! Tests for committing snapshots to a branch other than `main`.

use std::sync::Arc;

use crate::spec::{DataFile, MAIN_BRANCH, Snapshot, SnapshotReference, SnapshotRetention};
use crate::table::Table;
use crate::transaction::tests::{
    append_files, apply_updates_to_table, collect_alive_files, make_data_file,
    make_v2_minimal_table,
};
use crate::transaction::{Transaction, TransactionAction};
use crate::{ErrorKind, TableRequirement, TableUpdate};

const STAGING: &str = "staging";

async fn append_to_branch_updates(
    table: &Table,
    branch: &str,
    files: Vec<DataFile>,
) -> Vec<TableUpdate> {
    let action = Transaction::new(table)
        .fast_append()
        .add_data_files(files)
        .to_branch(branch);
    Arc::new(action).commit(table).await.unwrap().take_updates()
}

async fn append_to_branch(table: Table, branch: &str, files: Vec<DataFile>) -> Table {
    let updates = append_to_branch_updates(&table, branch, files).await;
    apply_updates_to_table(&table, &updates)
}

fn branch_head(table: &Table, branch: &str) -> Option<Arc<Snapshot>> {
    table.metadata().snapshot_for_ref(branch).cloned()
}

async fn alive(table: &Table, branch: &str) -> Vec<String> {
    let snap = branch_head(table, branch).expect("branch exists");
    collect_alive_files(&snap, table).await
}

fn ref_requirement(updates_requirements: &[TableRequirement], name: &str) -> Option<Option<i64>> {
    updates_requirements.iter().find_map(|r| match r {
        TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } if r#ref == name => {
            Some(*snapshot_id)
        }
        _ => None,
    })
}

#[tokio::test]
async fn test_refs_is_public_and_lists_main() {
    let table = make_v2_minimal_table();
    // The minimal table has no snapshots, so there are no refs.
    assert!(table.metadata().refs().is_empty());

    let table = append_files(table.clone(), vec![make_data_file(
        &table,
        "data/main.parquet",
        1,
    )])
    .await;
    let refs = table.metadata().refs();
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[MAIN_BRANCH].snapshot_id,
        table.metadata().current_snapshot_id().unwrap()
    );
}

#[tokio::test]
async fn test_commit_to_tag_is_rejected() {
    let table = make_v2_minimal_table();
    let table = append_files(table.clone(), vec![make_data_file(
        &table,
        "data/main.parquet",
        1,
    )])
    .await;
    let tagged_id = table.metadata().current_snapshot_id().unwrap();
    let table = apply_updates_to_table(&table, &[TableUpdate::SetSnapshotRef {
        ref_name: "v1".to_string(),
        reference: SnapshotReference::new(tagged_id, SnapshotRetention::Tag {
            max_ref_age_ms: None,
        }),
    }]);

    let action = Transaction::new(&table)
        .fast_append()
        .add_data_files(vec![make_data_file(&table, "data/f1.parquet", 2)])
        .to_branch("v1");
    let err = Arc::new(action).commit(&table).await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::DataInvalid);
    assert!(err.message().contains("is a tag"), "{err}");
}

#[tokio::test]
async fn test_append_to_missing_branch_creates_it_with_no_parent() {
    let table = make_v2_minimal_table();
    let f1 = make_data_file(&table, "data/f1.parquet", 10);

    let action = Transaction::new(&table)
        .fast_append()
        .add_data_files(vec![f1])
        .to_branch(STAGING);
    let mut commit = Arc::new(action).commit(&table).await.unwrap();
    let requirements = commit.take_requirements();
    let updates = commit.take_updates();

    let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
        panic!("expected AddSnapshot first");
    };
    assert_eq!(snapshot.parent_snapshot_id(), None);
    assert!(matches!(
        &updates[1],
        TableUpdate::SetSnapshotRef { ref_name, reference }
            if ref_name == STAGING && reference.snapshot_id == snapshot.snapshot_id()
    ));
    // The ref must not exist yet.
    assert_eq!(ref_requirement(&requirements, STAGING), Some(None));
    assert_eq!(ref_requirement(&requirements, MAIN_BRANCH), None);

    let after = apply_updates_to_table(&table, &updates);
    assert!(branch_head(&after, STAGING).is_some());
    assert!(branch_head(&after, MAIN_BRANCH).is_none());
    assert_eq!(after.metadata().current_snapshot_id(), None);
}

#[tokio::test]
async fn test_append_to_existing_branch_leaves_main_unchanged() {
    let table = make_v2_minimal_table();
    let main_file = make_data_file(&table, "data/main.parquet", 1);
    let table = append_files(table, vec![main_file]).await;
    let main_head = table.metadata().current_snapshot_id();
    assert!(main_head.is_some());

    // First write creates the branch with no parent, so it does not inherit
    // the files of `main`.
    let table = append_to_branch(table.clone(), STAGING, vec![make_data_file(
        &table,
        "data/s1.parquet",
        2,
    )])
    .await;
    let s1 = branch_head(&table, STAGING).unwrap();
    assert_eq!(s1.parent_snapshot_id(), None);
    assert_eq!(alive(&table, STAGING).await, vec!["data/s1.parquet"]);

    // Second write extends the branch head.
    let updates = append_to_branch_updates(&table, STAGING, vec![make_data_file(
        &table,
        "data/s2.parquet",
        3,
    )])
    .await;
    let action = Transaction::new(&table)
        .fast_append()
        .add_data_files(vec![make_data_file(&table, "data/s3.parquet", 3)])
        .to_branch(STAGING);
    let requirements = Arc::new(action)
        .commit(&table)
        .await
        .unwrap()
        .take_requirements();
    assert_eq!(
        ref_requirement(&requirements, STAGING),
        Some(Some(s1.snapshot_id()))
    );

    let table = apply_updates_to_table(&table, &updates);
    let s2 = branch_head(&table, STAGING).unwrap();
    assert_eq!(s2.parent_snapshot_id(), Some(s1.snapshot_id()));
    assert_eq!(s2.summary().additional_properties["total-data-files"], "2");
    assert_eq!(alive(&table, STAGING).await, vec![
        "data/s1.parquet",
        "data/s2.parquet"
    ]);

    // `main` did not move and keeps its own files.
    assert_eq!(table.metadata().current_snapshot_id(), main_head);
    assert_eq!(alive(&table, MAIN_BRANCH).await, vec!["data/main.parquet"]);
}

#[tokio::test]
async fn test_main_append_after_branch_write_ignores_branch() {
    let table = make_v2_minimal_table();
    let table = append_to_branch(table.clone(), STAGING, vec![make_data_file(
        &table,
        "data/s1.parquet",
        2,
    )])
    .await;

    // A plain append still targets `main` and sees an empty base.
    let f = make_data_file(&table, "data/m1.parquet", 1);
    let table = append_files(table, vec![f]).await;
    let main = table.metadata().current_snapshot().unwrap();
    assert_eq!(main.parent_snapshot_id(), None);
    assert_eq!(alive(&table, MAIN_BRANCH).await, vec!["data/m1.parquet"]);
    assert_eq!(alive(&table, STAGING).await, vec!["data/s1.parquet"]);
}

#[tokio::test]
async fn test_stale_branch_head_fails_requirement() {
    let table = make_v2_minimal_table();
    let table = append_to_branch(table.clone(), STAGING, vec![make_data_file(
        &table,
        "data/s1.parquet",
        2,
    )])
    .await;

    // Two writers plan against the same branch head.
    let action = Transaction::new(&table)
        .fast_append()
        .add_data_files(vec![make_data_file(&table, "data/a.parquet", 1)])
        .to_branch(STAGING);
    let stale_requirements = Arc::new(action)
        .commit(&table)
        .await
        .unwrap()
        .take_requirements();
    for r in &stale_requirements {
        r.check(Some(table.metadata())).unwrap();
    }

    // The other writer wins.
    let advanced = append_to_branch(table.clone(), STAGING, vec![make_data_file(
        &table,
        "data/b.parquet",
        1,
    )])
    .await;

    let failed = stale_requirements
        .iter()
        .any(|r| r.check(Some(advanced.metadata())).is_err());
    assert!(failed, "stale branch head must fail the requirement");
}

#[tokio::test]
async fn test_retry_rereads_target_branch_head() {
    // The transaction retry re-applies the action on a refreshed table. The
    // action must then use the refreshed head of the branch, not `main`.
    let table = make_v2_minimal_table();
    let table = append_to_branch(table.clone(), STAGING, vec![make_data_file(
        &table,
        "data/s1.parquet",
        2,
    )])
    .await;
    let main_file = make_data_file(&table, "data/m1.parquet", 1);
    let table = append_files(table, vec![main_file]).await;

    let action = Arc::new(
        Transaction::new(&table)
            .fast_append()
            .add_data_files(vec![make_data_file(&table, "data/a.parquet", 1)])
            .to_branch(STAGING),
    );
    // Another writer advances the branch before the retry.
    let advanced = append_to_branch(table.clone(), STAGING, vec![make_data_file(
        &table,
        "data/b.parquet",
        1,
    )])
    .await;

    let mut second = action.commit(&advanced).await.unwrap();
    let requirements = second.take_requirements();
    let updates = second.take_updates();
    let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
        panic!("expected AddSnapshot first");
    };
    let advanced_head = branch_head(&advanced, STAGING).unwrap().snapshot_id();
    assert_eq!(snapshot.parent_snapshot_id(), Some(advanced_head));
    assert_eq!(
        ref_requirement(&requirements, STAGING),
        Some(Some(advanced_head))
    );
}

#[tokio::test]
async fn test_merge_append_to_branch() {
    let table = make_v2_minimal_table();
    let updates = Arc::new(
        Transaction::new(&table)
            .merge_append()
            .add_data_files(vec![make_data_file(&table, "data/s1.parquet", 2)])
            .to_branch(STAGING),
    )
    .commit(&table)
    .await
    .unwrap()
    .take_updates();
    let table = apply_updates_to_table(&table, &updates);
    assert_eq!(alive(&table, STAGING).await, vec!["data/s1.parquet"]);
    assert!(branch_head(&table, MAIN_BRANCH).is_none());
}

#[tokio::test]
async fn test_rewrite_files_on_branch() {
    let table = make_v2_minimal_table();
    let s1 = make_data_file(&table, "data/s1.parquet", 2);
    let s2 = make_data_file(&table, "data/s2.parquet", 3);
    let main_file = make_data_file(&table, "data/main.parquet", 1);
    let table = append_files(table, vec![main_file]).await;
    let table = append_to_branch(table, STAGING, vec![s1.clone(), s2.clone()]).await;
    let main_head = table.metadata().current_snapshot_id();

    let merged = make_data_file(&table, "data/merged.parquet", 5);
    let updates = Arc::new(
        Transaction::new(&table)
            .rewrite_files()
            .delete_files(vec![s1, s2])
            .add_files(vec![merged])
            .to_branch(STAGING),
    )
    .commit(&table)
    .await
    .unwrap()
    .take_updates();
    let table = apply_updates_to_table(&table, &updates);

    assert_eq!(alive(&table, STAGING).await, vec!["data/merged.parquet"]);
    assert_eq!(table.metadata().current_snapshot_id(), main_head);
    assert_eq!(alive(&table, MAIN_BRANCH).await, vec!["data/main.parquet"]);
}

#[tokio::test]
async fn test_delete_files_on_branch() {
    let table = make_v2_minimal_table();
    let s1 = make_data_file(&table, "data/s1.parquet", 2);
    let s2 = make_data_file(&table, "data/s2.parquet", 3);
    let table = append_to_branch(table, STAGING, vec![s1.clone(), s2]).await;

    let updates = Arc::new(
        Transaction::new(&table)
            .delete_files()
            .delete_file(s1)
            .validate_files_exist()
            .to_branch(STAGING),
    )
    .commit(&table)
    .await
    .unwrap()
    .take_updates();
    let table = apply_updates_to_table(&table, &updates);

    assert_eq!(alive(&table, STAGING).await, vec!["data/s2.parquet"]);
    assert!(branch_head(&table, MAIN_BRANCH).is_none());
}
