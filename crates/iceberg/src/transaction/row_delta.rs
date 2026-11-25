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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, MAIN_BRANCH, ManifestEntry, ManifestFile, Operation,
};
use crate::table::Table;
use crate::transaction::commit_ids::MergingActionState;
use crate::transaction::merging_state::MergingState;
use crate::transaction::snapshot::merging::{SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::util::available_parallelism;
use crate::{Error, ErrorKind};

/// Action to append equality-delete and position-delete files to a table as a new
/// `Operation::Delete` snapshot.
///
/// Each commit runs the full merging snapshot producer filter+merge pipeline on
/// existing manifests and writes the supplied delete files into a new
/// `ManifestContent::Deletes` manifest.
///
/// Java analog: `org.apache.iceberg.BaseRowDelta` (`RowDelta` API).
///
/// # Unimplemented Java API
///
/// The following methods from `RowDelta` are not yet implemented (tracked in
/// <https://github.com/apache/iceberg-rust/issues/2203>):
///
/// - `addRows` / `add_data_files` — append data files alongside deletes
/// - `removeRows` — register existing data files for rewrite
/// - `removeDeletes` — drop existing delete files
/// - `validateFromSnapshot` — conflict detection starting snapshot
/// - `validateNoConflictingDeleteFiles` — reject new overlapping deletes
/// - `validateNoConflictingDataFiles` — reject new overlapping data files
/// - `validateDataFilesExistWhenDataFileReferenced` — referenced file existence check
/// - DV (deletion vector) support
#[derive(Clone)]
pub struct RowDeltaAction {
    added_delete_files: Vec<DataFile>,

    snapshot_properties: HashMap<String, String>,
    target_branch: String,
    manifest_read_concurrency: usize,
    manifest_write_concurrency: usize,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            added_delete_files: vec![],

            snapshot_properties: HashMap::default(),
            target_branch: MAIN_BRANCH.to_string(),
            manifest_read_concurrency: num_cpus,
            manifest_write_concurrency: std::cmp::max(1, num_cpus / 4),
        }
    }

    /// Add equality-delete or position-delete files to this snapshot.
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(files);
        self
    }

    /// Commit the new snapshot to `name` instead of `main`. A branch that does
    /// not exist yet is created with no parent. The commit fails if `name` is a
    /// tag.
    pub fn to_branch(mut self, name: impl Into<String>) -> Self {
        self.target_branch = name.into();
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, props: HashMap<String, String>) -> Self {
        self.snapshot_properties = props;
        self
    }

    /// Override the number of manifests read concurrently during the filter pass.
    pub fn with_manifest_read_concurrency(mut self, n: usize) -> Self {
        self.manifest_read_concurrency = std::cmp::max(1, n);
        self
    }

    /// Override the number of manifest bins written concurrently during the merge pass.
    pub fn with_manifest_write_concurrency(mut self, n: usize) -> Self {
        self.manifest_write_concurrency = std::cmp::max(1, n);
        self
    }

    fn validate_added_delete_files(&self) -> Result<()> {
        for f in &self.added_delete_files {
            if f.content_type() == DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "RowDelta does not accept data files; use add_data_files: {}",
                        f.file_path
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    type State = MergingActionState;
    fn new_state(&self) -> Self::State {
        Self::State::default()
    }
    async fn cleanup(
        self,
        state: Self::State,
        table: &Table,
        status: crate::transaction::action::CommitStatus,
    ) {
        state.cleanup(table, status).await;
    }
    async fn commit(&self, attempt: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        if table.metadata().format_version() == FormatVersion::V1 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "RowDelta is not supported for V1 tables",
            ));
        }

        self.validate_added_delete_files()?;

        if self.added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RowDelta requires at least one delete file",
            ));
        }

        let state = attempt.merging_state(
            table,
            self.manifest_read_concurrency,
            self.manifest_write_concurrency,
        )?;

        let op = RowDeltaOperation {
            added_delete_files: self.added_delete_files.clone(),
            state: state.clone(),
        };

        let snapshot_producer = SnapshotProducer::new(
            table,
            attempt.ids.snapshot_id(table),
            attempt.ids.commit_uuid(),
            self.snapshot_properties.clone(),
            vec![],
        )
        .with_target_branch(self.target_branch.clone())
        .with_attempt(attempt.next_attempt());

        let result = snapshot_producer
            .commit(op, state, &mut attempt.added_manifests)
            .await?;
        Ok(attempt.record_attempt(result))
    }
}

struct RowDeltaOperation {
    added_delete_files: Vec<DataFile>,
    state: Arc<MergingState>,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        Operation::Delete
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.base_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?;

        let current_manifests: Vec<ManifestFile> = manifest_list.entries().to_vec();
        self.state
            .filter_manifests(snapshot_produce, current_manifests)
            .await
    }

    fn added_delete_entries(&self) -> &[DataFile] {
        &self.added_delete_files
    }

    fn has_pending_content(&self) -> bool {
        !self.added_delete_files.is_empty()
    }
}

#[cfg(test)]
mod tests {

    use crate::spec::{DataContentType, ManifestContentType, ManifestStatus};
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::tests::{
        apply_updates_to_table, commit_uuid_from, make_file_with_content, make_v2_minimal_table,
        snapshot_id_from,
    };
    use crate::transaction::{Transaction, TransactionAction};

    #[tokio::test]
    async fn test_row_delta_rejects_data_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let df = make_file_with_content(&table, "data/x.parquet", DataContentType::Data);
        let action = tx.row_delta().add_delete_files(vec![df]);
        assert!(commit_with_fresh_state(action, &table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta_adds_delete_manifest() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let eq_delete = make_file_with_content(
            &table,
            "deletes/eq1.parquet",
            DataContentType::EqualityDeletes,
        );
        let action = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = commit_with_fresh_state(action, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        let deletes_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|e| e.content == ManifestContentType::Deletes)
            .collect();

        assert_eq!(deletes_manifests.len(), 1, "expected one deletes manifest");

        let manifest = table
            .manifest_reader()
            .read(deletes_manifests[0])
            .await
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(manifest.entries()[0].status(), ManifestStatus::Added);
    }

    #[tokio::test]
    async fn test_row_delta_equality_delete() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let eq_delete = make_file_with_content(
            &table,
            "deletes/eq1.parquet",
            DataContentType::EqualityDeletes,
        );
        let pos_delete = make_file_with_content(
            &table,
            "deletes/pos1.parquet",
            DataContentType::PositionDeletes,
        );
        let action = tx.row_delta().add_delete_files(vec![eq_delete, pos_delete]);
        let updates = commit_with_fresh_state(action, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        let deletes_count: usize = manifest_list
            .entries()
            .iter()
            .filter(|e| e.content == ManifestContentType::Deletes)
            .count();
        assert_eq!(deletes_count, 1);
    }

    #[tokio::test]
    async fn test_occ_retry_stable_ids() {
        let table = make_v2_minimal_table();
        let eq_delete = make_file_with_content(
            &table,
            "deletes/eq1.parquet",
            DataContentType::EqualityDeletes,
        );

        let action = Transaction::new(&table)
            .row_delta()
            .add_delete_files(vec![eq_delete]);
        let mut action_state = action.new_state();

        let updates1 = action
            .commit(&mut action_state, &table)
            .await
            .unwrap()
            .take_updates();
        let updates2 = action
            .commit(&mut action_state, &table)
            .await
            .unwrap()
            .take_updates();

        assert_eq!(
            snapshot_id_from(&updates1),
            snapshot_id_from(&updates2),
            "snapshot_id must be stable across OCC retries"
        );
        assert_eq!(
            commit_uuid_from(&updates1),
            commit_uuid_from(&updates2),
            "commit_uuid must be stable across OCC retries"
        );
    }
}
