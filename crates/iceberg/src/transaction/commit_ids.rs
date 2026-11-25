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

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use uuid::Uuid;

use super::action::CommitStatus;
use super::merging_state::MergingState;
use super::snapshot::merging::{AddedManifestState, CommitResult};
use crate::Result;
use crate::table::Table;
use crate::transaction::ActionCommit;

/// Snapshot id and commit UUID held behind `OnceLock`s so they survive OCC retries.
///
/// Each merging action holds one `CommitIds`. The first `commit()` call generates
/// both values; subsequent retries observe the same pair, which is the contract the
/// catalog requires for idempotent retries (matching path planning, manifest list
/// filenames, and snapshot identity across attempts).
#[derive(Default)]
pub(crate) struct CommitIds {
    snapshot_id: OnceLock<i64>,
    commit_uuid: OnceLock<Uuid>,
}

impl CommitIds {
    pub(crate) fn snapshot_id(&self, table: &Table) -> i64 {
        *self
            .snapshot_id
            .get_or_init(|| crate::transaction::snapshot::generate_snapshot_id(table))
    }

    pub(crate) fn commit_uuid(&self) -> Uuid {
        *self.commit_uuid.get_or_init(Uuid::now_v7)
    }
}

#[derive(Default)]
pub(crate) struct MergingActionState {
    pub(crate) ids: CommitIds,
    pub(crate) added_manifests: AddedManifestState,
    retained_paths: HashSet<String>,
    attempt: u64,
    merging: Option<Arc<MergingState>>,
}

impl MergingActionState {
    pub(crate) fn next_attempt(&mut self) -> u64 {
        let current = self.attempt;
        self.attempt += 1;
        current
    }

    pub(crate) fn record_attempt(&mut self, result: CommitResult) -> ActionCommit {
        self.retained_paths = result.committed_manifest_paths;
        result.commit
    }

    pub(crate) async fn cleanup(mut self, table: &Table, status: CommitStatus) {
        match status {
            CommitStatus::Committed => {}
            CommitStatus::Failed => self.retained_paths.clear(),
            CommitStatus::Unknown => return,
        }
        if let Some(state) = self.merging {
            state
                .clean_uncommitted(table.file_io(), &self.retained_paths)
                .await;
        }
    }

    pub(crate) fn merging_state(
        &mut self,
        table: &Table,
        reads: usize,
        writes: usize,
    ) -> Result<Arc<MergingState>> {
        if let Some(state) = &self.merging {
            return Ok(state.clone());
        }
        let state = Arc::new(MergingState::from_table(table, reads, writes)?);
        self.merging = Some(state.clone());
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::CommitIds;
    use crate::transaction::tests::make_v2_minimal_table;

    #[test]
    fn snapshot_id_is_idempotent() {
        let table = make_v2_minimal_table();
        let ids = CommitIds::default();
        assert_eq!(ids.snapshot_id(&table), ids.snapshot_id(&table));
    }

    #[test]
    fn commit_uuid_is_idempotent() {
        let ids = CommitIds::default();
        assert_eq!(ids.commit_uuid(), ids.commit_uuid());
    }
}
