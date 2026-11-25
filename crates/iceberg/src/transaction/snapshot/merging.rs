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

use std::collections::{HashMap, HashSet};
use std::future::Future;

use uuid::Uuid;

use crate::error::Result;
use crate::spec::snapshot_summary::{MANIFESTS_CREATED, MANIFESTS_KEPT, MANIFESTS_REPLACED};
use crate::spec::{
    DataContentType, DataFile, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpec, SnapshotRef,
    Summary,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind};

#[derive(Default)]
pub(crate) struct AddedManifestState {
    counter: u64,
    cached: Option<(FormatVersion, Vec<ManifestFile>)>,
}

pub(crate) struct CommitResult {
    pub commit: ActionCommit,
    /// Manifest paths written into the new snapshot. Pass to `clean_uncommitted`
    /// to delete orphaned prior-attempt residuals. See `RewriteFilesAction` for usage.
    pub committed_manifest_paths: HashSet<String>,
}

/// Set the per-commit manifest activity counters on `summary` from the final
/// manifest list and the action's replaced count.
///
/// Java analog: `org.apache.iceberg.SnapshotProducer::commitSummary`.
fn inject_manifest_summary_keys(
    summary: &mut Summary,
    manifests: &[ManifestFile],
    snapshot_id: i64,
    replaced_count: u64,
) {
    let mut created = 0u64;
    let mut kept = 0u64;
    for m in manifests {
        if m.added_snapshot_id == snapshot_id {
            created += 1;
        } else {
            kept += 1;
        }
    }
    // Do not overwrite values that may have been explicitly set by an action
    // like RewriteManifestsAction. Only insert counters if they are absent.
    summary
        .additional_properties
        .entry(MANIFESTS_CREATED.to_string())
        .or_insert_with(|| created.to_string());
    summary
        .additional_properties
        .entry(MANIFESTS_KEPT.to_string())
        .or_insert_with(|| kept.to_string());
    summary
        .additional_properties
        .entry(MANIFESTS_REPLACED.to_string())
        .or_insert_with(|| replaced_count.to_string());
}

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # When it accomplishes
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the target branch head should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method returns empty for all current
///    implementations; see `added_delete_entries` for V2 delete files.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the target branch head should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;

    /// Returns delete files (equality/position deletes) to be written into a new
    /// `ManifestContent::Deletes` manifest for this snapshot. Default: empty.
    fn added_delete_entries(&self) -> &[DataFile] {
        &[]
    }

    /// Returns `true` when the operation has content to commit even if
    /// `added_data_files` is empty (e.g. `DeleteFilesAction` rewrites existing
    /// manifests without adding new data). Default: `false`.
    fn has_pending_content(&self) -> bool {
        false
    }
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
        _new_manifest_paths: &HashSet<String>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(manifests)
    }
}

/// Hook that a snapshot-producing action can use to transform the manifest list
/// after it's been assembled (carry-forward + new added + new delete) but
/// before it's written into the snapshot's manifest list. The merging snapshot
/// producer uses this hook to bin-pack siblings via `MergingState::merge_manifests`.
pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
        new_manifest_paths: &HashSet<String>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;

    /// Number of manifests this process replaced as part of the transformation.
    /// Surfaces as the snapshot summary's `manifests-replaced` key. Default 0
    /// for processes that don't replace anything.
    fn replaced_manifests_count(&self) -> u64 {
        0
    }
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    /// Branch that the new snapshot is committed to. Every base-snapshot read
    /// and the commit requirement use this ref. Defaults to `main`.
    target_branch: String,
    added_data_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    /// Explicit data sequence number for added manifest entries. When `Some`, all entries written
    /// in the added data manifest carry this sequence number instead of inheriting from the new
    /// snapshot. Used by `RewriteFilesAction::data_sequence_number()` to preserve the
    /// original sequence number of compacted files when equality deletes are present.
    data_sequence_number: Option<i64>,
    attempt: u64,
}

impl<'a> SnapshotProducer<'a> {
    pub(crate) fn new(
        table: &'a Table,
        snapshot_id: i64,
        commit_uuid: Uuid,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
    ) -> Self {
        Self {
            table,
            snapshot_id,
            commit_uuid,
            snapshot_properties,
            target_branch: MAIN_BRANCH.to_string(),
            added_data_files,
            removed_data_files: vec![],
            data_sequence_number: None,
            attempt: 0,
        }
    }

    pub(crate) fn with_attempt(mut self, attempt: u64) -> Self {
        self.attempt = attempt;
        self
    }

    pub(crate) fn with_target_branch(mut self, branch: String) -> Self {
        self.target_branch = branch;
        self
    }

    /// Head snapshot of the target branch. `None` when the branch does not
    /// exist yet, which means the new snapshot has no parent.
    pub(crate) fn base_snapshot(&self) -> Option<&SnapshotRef> {
        self.table.metadata().snapshot_for_ref(&self.target_branch)
    }

    pub(crate) fn with_removed_data_files(mut self, files: Vec<DataFile>) -> Self {
        self.removed_data_files = files;
        self
    }

    pub(crate) fn with_data_sequence_number(mut self, seq_num: i64) -> Self {
        self.data_sequence_number = Some(seq_num);
        self
    }

    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub(crate) fn commit_uuid(&self) -> Uuid {
        self.commit_uuid
    }

    pub(crate) fn validate_added_data_files(&self) -> Result<()> {
        for data_file in &self.added_data_files {
            if data_file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            super::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    /// Reject the commit if any ancestor since `starting_snapshot_id` (or all
    /// ancestors when `None`) added a delete file that targets one of the
    /// replaced paths.
    ///
    /// Equality deletes are ignored when the producer has a
    /// `data_sequence_number` set — the rewrite preserves the original sequence
    /// number, so equality deletes at higher sequence numbers continue to apply
    /// correctly and don't conflict.
    ///
    /// Java analog: `org.apache.iceberg.MergingSnapshotProducer::validateNoNewDeletesForDataFiles`.
    pub(crate) async fn validate_no_new_deletes_for_data_files(
        &self,
        starting_snapshot_id: Option<i64>,
        replaced_data_files: &HashSet<String>,
    ) -> Result<()> {
        let metadata = self.table.metadata();
        if metadata.format_version() == FormatVersion::V1 {
            return Ok(());
        }
        let Some(parent) = self.base_snapshot() else {
            return Ok(());
        };
        if replaced_data_files.is_empty() {
            return Ok(());
        }

        let ignore_equality_deletes = self.data_sequence_number.is_some();

        let (starting_seq_num, final_starting_snapshot_id) = match starting_snapshot_id {
            Some(id) => match metadata.snapshot_by_id(id) {
                Some(s) => (s.sequence_number(), Some(id)),
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot determine history between starting snapshot {} and the last known ancestor",
                            id
                        ),
                    ));
                }
            },
            None => (0, None),
        };

        let mut current = Some(parent.clone());
        while let Some(snap) = current {
            if Some(snap.snapshot_id()) == final_starting_snapshot_id {
                break;
            }

            let manifest_list = self.table.manifest_list_reader(&snap).load().await?;

            for ml_entry in manifest_list.entries() {
                if ml_entry.content != ManifestContentType::Deletes {
                    continue;
                }
                // Only delete manifests this snapshot itself added — older
                // ones were already in scope at `starting_snapshot_id`.
                if ml_entry.added_snapshot_id != snap.snapshot_id() {
                    continue;
                }

                let manifest = self.table.object_cache().get_manifest(ml_entry).await?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let f = &entry.data_file;
                    match f.content_type() {
                        DataContentType::PositionDeletes => match f.referenced_data_file() {
                            Some(ref_path) => {
                                if replaced_data_files.contains(&ref_path) {
                                    return Err(Error::new(
                                        ErrorKind::DataInvalid,
                                        format!(
                                            "Cannot commit, found new position delete \
                                                 for replaced data file: {ref_path}"
                                        ),
                                    ));
                                }
                            }
                            None => {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Cannot commit, found new position delete file {} \
                                             missing referenced_data_file hint",
                                        f.file_path
                                    ),
                                ));
                            }
                        },
                        DataContentType::EqualityDeletes => {
                            if !ignore_equality_deletes
                                && entry.sequence_number().unwrap_or(0) > starting_seq_num
                            {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Cannot commit, found new equality delete file \
                                         {} added at sequence {} since starting sequence \
                                         {starting_seq_num}",
                                        f.file_path,
                                        entry.sequence_number().unwrap_or(0)
                                    ),
                                ));
                            }
                        }
                        DataContentType::Data => {}
                    }
                }
            }

            current = snap
                .parent_snapshot_id()
                .and_then(|pid| metadata.snapshot_by_id(pid))
                .cloned();
        }

        Ok(())
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        super::validate_files_not_live(self.table, &self.target_branch, &self.added_data_files)
            .await
    }

    /// Validates that every data file in `files` is still alive in the current
    /// snapshot. This ensures that a concurrent rewrite or delete hasn't already
    /// removed the files being replaced.
    ///
    /// Java analog: `MergingSnapshotProducer.validateDataFilesExist`
    pub(crate) async fn validate_data_files_exist(&self, files: &HashSet<String>) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        let Some(current_snapshot) = self.base_snapshot() else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot validate file existence on a table with no snapshots",
            ));
        };

        let manifest_list = self
            .table
            .manifest_list_reader(current_snapshot)
            .load()
            .await?;

        let mut found_files = HashSet::new();
        for manifest_list_entry in manifest_list.entries() {
            let manifest = self
                .table
                .object_cache()
                .get_manifest(manifest_list_entry)
                .await?;
            for entry in manifest.entries() {
                if entry.is_alive() && files.contains(entry.file_path()) {
                    found_files.insert(entry.file_path().to_string());
                }
            }
        }

        let missing: Vec<&str> = files
            .iter()
            .map(|s| s.as_str())
            .filter(|p| !found_files.contains(*p))
            .collect();

        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot commit, files being replaced are no longer alive in the head of branch '{}': {}",
                    self.target_branch,
                    missing.join(", ")
                ),
            ));
        }

        Ok(())
    }

    fn new_manifest_writer(
        &self,
        content: ManifestContentType,
        state: &mut AddedManifestState,
    ) -> Result<ManifestWriter> {
        let counter = state.counter;
        state.counter += 1;
        let path = super::manifest_path(self.table, self.commit_uuid, counter)?;
        super::new_manifest_writer(
            self.table,
            path,
            self.snapshot_id,
            content,
            self.table.metadata().current_schema().clone(),
            self.table.metadata().default_partition_spec().clone(),
        )
    }

    /// Build a manifest writer using the table's encryption configuration.
    pub(crate) fn manifest_writer_builder(
        &self,
        path: &str,
        partition_spec: PartitionSpec,
    ) -> Result<ManifestWriterBuilder> {
        let output_file = self.table.file_io().new_output(path)?;
        let schema = self.table.metadata().current_schema().clone();
        match self.table.encryption_manager() {
            Some(em) => ManifestWriterBuilder::new_from_encrypted(
                em.encrypt(output_file),
                Some(self.snapshot_id),
                schema,
                partition_spec,
            ),
            None => Ok(ManifestWriterBuilder::new(
                output_file,
                Some(self.snapshot_id),
                schema,
                partition_spec,
            )),
        }
    }

    async fn write_entries_as_manifest(
        &self,
        files: &[DataFile],
        content: ManifestContentType,
        data_seq_num: Option<i64>,
        state: &mut AddedManifestState,
    ) -> Result<ManifestFile> {
        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let entries = files.iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file.clone());
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else if let Some(seq_num) = data_seq_num {
                // Explicitly set sequence number instead of inheriting from the new snapshot.
                // Required when compacting files that predate an equality delete: the compacted
                // files must retain the original sequence number so equality deletes with a higher
                // sequence number continue to apply.
                builder.sequence_number(seq_num).build()
            } else {
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(content, state)?;
        for entry in entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn added_manifests(
        &self,
        deletes: &[DataFile],
        state: &mut AddedManifestState,
    ) -> Result<Vec<ManifestFile>> {
        let version = self.table.metadata().format_version();
        if let Some((cached_version, manifests)) = &state.cached
            && *cached_version == version
        {
            return Ok(manifests.clone());
        }
        let mut manifests = Vec::new();
        if !self.added_data_files.is_empty() {
            manifests.push(
                self.write_entries_as_manifest(
                    &self.added_data_files,
                    ManifestContentType::Data,
                    self.data_sequence_number,
                    state,
                )
                .await?,
            );
        }
        if !deletes.is_empty() {
            manifests.push(
                self.write_entries_as_manifest(deletes, ManifestContentType::Deletes, None, state)
                    .await?,
            );
        }
        state.cached = Some((version, manifests.clone()));
        Ok(manifests)
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
        state: &mut AddedManifestState,
    ) -> Result<Vec<ManifestFile>> {
        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.snapshot_properties.is_empty()
            && !snapshot_produce_operation.has_pending_content()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files or added snapshot properties found when write a manifest file",
            ));
        }

        let mut manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let added = self
            .added_manifests(snapshot_produce_operation.added_delete_entries(), state)
            .await?;
        let new_manifest_paths = added
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect();
        manifests.extend(added);
        manifest_process
            .process_manifests(self, manifests, &new_manifest_paths)
            .await
    }

    // Returns a `Summary` of the current snapshot
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        super::build_summary(
            self.table,
            &self.target_branch,
            snapshot_produce_operation.operation(),
            self.snapshot_properties.clone(),
            self.added_data_files
                .iter()
                .chain(snapshot_produce_operation.added_delete_entries()),
            &self.removed_data_files,
            snapshot_produce_operation.operation() == Operation::Overwrite,
        )
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction
    /// along with the set of committed manifest paths for `clean_uncommitted`.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        self,
        snapshot_produce_operation: OP,
        process: MP,
        state: &mut AddedManifestState,
    ) -> Result<CommitResult> {
        if let Some(existing) = self.table.metadata().refs().get(&self.target_branch)
            && !existing.is_branch()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot commit a snapshot to '{}': it is a tag, not a branch",
                    self.target_branch
                ),
            ));
        }

        let manifest_list_path = super::manifest_list_path(
            self.table,
            self.snapshot_id,
            self.attempt,
            self.commit_uuid,
        )?;

        let mut summary = self.summary(&snapshot_produce_operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process, state)
            .await?;

        let committed_manifest_paths: HashSet<String> = new_manifests
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();

        inject_manifest_summary_keys(
            &mut summary,
            &new_manifests,
            self.snapshot_id,
            process.replaced_manifests_count(),
        );

        let commit = super::commit_snapshot(
            self.table,
            &self.target_branch,
            self.snapshot_id,
            manifest_list_path,
            summary,
            new_manifests,
        )
        .await?;
        Ok(CommitResult {
            commit,
            committed_manifest_paths,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct, TableProperties,
    };
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::Transaction;
    use crate::transaction::tests::{apply_updates_to_table, make_v1_table, make_v2_minimal_table};

    fn data_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn validate_no_new_deletes_short_circuits_for_v1() {
        let table = make_v1_table();
        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/x.parquet")],
        );
        let mut replaced = HashSet::new();
        replaced.insert("data/replaced.parquet".to_string());

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("V1 must short-circuit");
    }

    #[tokio::test]
    async fn validate_no_new_deletes_short_circuits_for_empty_table() {
        let table = make_v2_minimal_table();
        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/x.parquet")],
        );
        let mut replaced = HashSet::new();
        replaced.insert("data/replaced.parquet".to_string());

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("empty-snapshot table must short-circuit");
    }

    #[tokio::test]
    async fn validate_no_new_deletes_short_circuits_for_empty_replaced_set() {
        let table = make_v2_minimal_table();
        let f = data_file(&table, "data/x.parquet");
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/y.parquet")],
        );
        let replaced = HashSet::new();

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("empty replaced set must short-circuit");
    }

    #[tokio::test]
    async fn validate_no_new_deletes_passes_when_no_delete_manifests() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");
        let f2 = data_file(&table, "data/f2.parquet");
        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(vec![f1.clone(), f2.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/compacted.parquet")],
        )
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("table without delete manifests must pass");

        // A path that never existed also passes — no false positives.
        let mut other = HashSet::new();
        other.insert("data/never-existed.parquet".to_string());
        producer
            .validate_no_new_deletes_for_data_files(None, &other)
            .await
            .expect("path not present in any ancestor still passes");
    }

    #[tokio::test]
    async fn test_validate_rejects_when_planning_snapshot_expired() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let first_snapshot_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Try to rewrite f1, but pass a NON-EXISTENT starting_snapshot_id
        // (simulating it was expired)
        let expired_id = first_snapshot_id - 100;
        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/compacted.parquet")],
        )
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        let result = producer
            .validate_no_new_deletes_for_data_files(Some(expired_id), &replaced)
            .await;

        assert!(
            result.is_err(),
            "Expired planning snapshot must be rejected"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot determine history between starting snapshot"),
            "Error message should mention history determination failure"
        );
    }

    #[tokio::test]
    async fn test_validate_real_equality_delete_conflict_still_rejected() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let snap_a_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Add an equality delete (Snapshot B)
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let delete = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = commit_with_fresh_state(delete, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 3. Try to rewrite f1, starting from Snapshot A.
        // It SHOULD reject because Snapshot B added a delete since A.
        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/compacted.parquet")],
        )
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        let result = producer
            .validate_no_new_deletes_for_data_files(Some(snap_a_id), &replaced)
            .await;

        assert!(result.is_err(), "Real conflict must still be rejected");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("found new equality delete file"),
            "Error message should mention new equality delete"
        );
    }

    #[tokio::test]
    async fn test_validate_rejects_when_position_delete_targets_replaced_file() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let snap_a_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Add a position delete (Snapshot B)
        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some(f1.file_path.clone()))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let delete = tx.row_delta().add_delete_files(vec![pos_delete]);
        let updates = commit_with_fresh_state(delete, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 3. Try to rewrite f1, starting from Snapshot A.
        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/compacted.parquet")],
        )
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        let result = producer
            .validate_no_new_deletes_for_data_files(Some(snap_a_id), &replaced)
            .await;

        assert!(result.is_err(), "New position delete must be rejected");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("found new position delete for replaced data file"),
            "Error message should mention new position delete"
        );
    }

    #[tokio::test]
    async fn test_validate_equality_delete_boundary_sequence_number() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 2. Add an equality delete (Snapshot B)
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let add_delete = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = commit_with_fresh_state(add_delete, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // snap_b is the current snapshot — planning STARTS from snap_b so the
        // validator's loop short-circuits immediately (current == starting).
        // This tests that a delete present at planning time does not trigger rejection.
        let snap_b_id = table.metadata().current_snapshot_id().unwrap();

        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/compacted.parquet")],
        )
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        producer
            .validate_no_new_deletes_for_data_files(Some(snap_b_id), &replaced)
            .await
            .expect("equality delete already present at planning time must pass");
    }

    #[tokio::test]
    async fn test_validate_ignore_equality_deletes_branch() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = commit_with_fresh_state(append, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let snap_a_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Add an equality delete (Snapshot B)
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let delete = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = commit_with_fresh_state(delete, &table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 3. Try to rewrite f1, starting from Snapshot A, with ignore_equality_deletes = true.
        // This is safe when the rewrite preserves the original sequence number.
        let producer = SnapshotProducer::new(
            &table,
            crate::transaction::snapshot::generate_snapshot_id(&table),
            Uuid::now_v7(),
            HashMap::new(),
            vec![data_file(&table, "data/compacted.parquet")],
        )
        .with_removed_data_files(vec![f1.clone()])
        .with_data_sequence_number(1); // Setting this triggers ignore_equality_deletes = true

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        producer
            .validate_no_new_deletes_for_data_files(Some(snap_a_id), &replaced)
            .await
            .expect("should pass when ignore_equality_deletes is true and Some(seq) provided");

        // 4. Test the case where ignore_equality_deletes = true BUT no data_sequence_number is provided
        // (This shouldn't happen via RewriteFilesAction, but we test the validator's internal logic)
        // Currently SnapshotProducer doesn't expose a way to set ignore_equality_deletes directly,
        // but it's derived from data_sequence_number.is_some().
    }

    #[tokio::test]
    async fn encrypted_rewrite_preserves_survivors_with_and_without_manifest_merging() {
        for merge_enabled in [false, true] {
            let table = crate::test_utils::make_encrypted_table().await;
            let tx = Transaction::new(&table);
            let updates = commit_with_fresh_state(
                tx.update_table_properties()
                    .set(
                        TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED.to_string(),
                        merge_enabled.to_string(),
                    )
                    .set(
                        TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_MERGE_COUNT.to_string(),
                        "1".to_string(),
                    ),
                &table,
            )
            .await
            .unwrap()
            .take_updates();
            let table = apply_updates_to_table(&table, &updates);
            let file = |name: &str| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!("memory:///table/data/{name}.parquet"))
                    .file_format(DataFileFormat::Parquet)
                    .partition(Struct::empty())
                    .record_count(100)
                    .file_size_in_bytes(4096)
                    .partition_spec_id(table.metadata().default_partition_spec_id())
                    .build()
                    .unwrap()
            };
            let replaced = file("old");
            let survivor = file("keep");
            let replacement = file("new");
            let tx = Transaction::new(&table);
            let updates = commit_with_fresh_state(
                tx.merge_append()
                    .add_data_files([replaced.clone(), survivor.clone()]),
                &table,
            )
            .await
            .unwrap()
            .take_updates();
            let table = apply_updates_to_table(&table, &updates);
            let tx = Transaction::new(&table);
            let updates = commit_with_fresh_state(
                tx.rewrite_files()
                    .delete_files([replaced])
                    .add_files([replacement.clone()]),
                &table,
            )
            .await
            .unwrap()
            .take_updates();
            let table = apply_updates_to_table(&table, &updates);
            let snapshot = table.metadata().current_snapshot().unwrap();
            assert!(snapshot.encryption_key_id().is_some());
            let manifests = table.manifest_list_reader(snapshot).load().await.unwrap();
            assert_eq!(manifests.entries().len(), if merge_enabled { 1 } else { 2 });
            let mut paths = Vec::new();
            for manifest in manifests.entries() {
                assert!(
                    manifest.key_metadata.is_some(),
                    "maintenance must preserve encryption"
                );
                let decoded = table.manifest_reader().read(manifest).await.unwrap();
                paths.extend(
                    decoded
                        .entries()
                        .iter()
                        .filter(|entry| entry.is_alive())
                        .map(|entry| entry.file_path().to_string()),
                );
            }
            paths.sort();
            let mut expected = vec![
                survivor.file_path().to_string(),
                replacement.file_path().to_string(),
            ];
            expected.sort();
            assert_eq!(paths, expected);
        }
    }
}
