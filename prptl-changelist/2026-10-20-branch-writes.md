# 2026-10-20 Branch Writes

## Baseline

- Apache baseline: `v0.11.0-rc.2` (fork tag `perpetual-v21`).
- Change: 6 commits, `b9263ef..b77be65`, on top of `perpetual-v21`.
- Upstream has no branch writes and no open proposal. The API matches
  Java `SnapshotUpdate.toBranch`.

## What the Change Adds

- `to_branch(name)` on these actions: fast append, merge append, rewrite
  files, row delta and delete files. The default target is `main`.
- A target branch that does not exist is created with no parent.
- A target that is a tag is rejected with `ErrorKind::DataInvalid`.
- `TableMetadata::refs()` is public. It returns all branches and tags by name.
- `SnapshotProducer` has a `target_branch` field, `with_target_branch()`,
  `base_snapshot()` and `base_snapshot_id()`.
- A commit that moves an existing branch resets its retention. This is the
  same as upstream `main` today. The change does not set retention.
- `rewrite_manifests` and `expire_snapshots` are not changed. They work on
  `main` only.
- Public API snapshot `crates/iceberg/public-api.txt` is regenerated.

## Hook Points

| File | Hook |
| --- | --- |
| `spec/table_metadata.rs` | `refs()` accessor. |
| `transaction/snapshot.rs` | `target_branch` field and builder. `base_snapshot()` and `base_snapshot_id()`. All base reads (parent, manifest carry-forward, alive-file check, summary, parent id) use them. Tag check. `SetSnapshotRef` and `RefSnapshotIdMatch` use the target branch. |
| `transaction/append.rs` | `target_branch` field and `to_branch()`. Passes the branch to `SnapshotProducer`. Reads the base snapshot through `base_snapshot()`. |
| `transaction/merge_append.rs` | Field, `to_branch()`, pass-through to `SnapshotProducer`. |
| `transaction/rewrite_files.rs` | Field, `to_branch()`, pass-through to `SnapshotProducer`. |
| `transaction/row_delta.rs` | Field, `to_branch()`, pass-through to `SnapshotProducer`. |
| `transaction/delete_files.rs` | Field, `to_branch()`, pass-through to `SnapshotProducer`. |
| `transaction/branch_writes.rs` | New test-only module. Registered in `transaction/mod.rs`. |

## Invariant

Commit-path code reads the base snapshot through
`SnapshotProducer::base_snapshot()`. It never reads
`table_metadata.current_snapshot()` or `MAIN_BRANCH`.

The tests in `crates/iceberg/src/transaction/branch_writes.rs` must pass after
every upstream sync.

Upstream code that reads `current_snapshot()` on the commit path does not
cause a compile error. It makes a branch write use the `main` head. The tests
and the guard check find this.

## Known Sync Hotspot

`snapshot.rs` has about 10 conflict hunks on each sync. One fork edit is in
an existing conflict: the previous-snapshot read for the summary
(`let previous_snapshot = self.base_snapshot();`). Upstream has
`table_metadata.current_snapshot()` at this place.

If you take the upstream side, branch handling stops. The code still compiles.
Keep the fork side: `self.base_snapshot()`. Then run the checks below.

`append.rs` has one small hunk (`current_snapshot()` replaced by
`base_snapshot()`). Check it in the same way.

## Guard Check

Run this command after each sync, before you run the unit tests:

```sh
dev/check_branch_write_guard.sh
```

The script fails when non-test code in `crates/iceberg/src/transaction/*.rs`
calls `current_snapshot()` or uses `MAIN_BRANCH`. It skips `branch_writes.rs`,
`use` lines, comments and inline test modules.

The script has an allowlist. Each entry is a file and the exact line text:

- `target_branch: MAIN_BRANCH.to_string(),` in the five actions and in
  `snapshot.rs`. This is the default target.
- `expire_snapshots.rs`: the `main` check when it selects refs to expire, and
  the `RefSnapshotIdMatch` requirement. Expire works on `main` only.
- `rewrite_manifests.rs`: the `current_snapshot()` read and two
  `MAIN_BRANCH` requirements. Rewrite manifests works on `main` only.

If a new line needs `current_snapshot()` or `MAIN_BRANCH` for a good reason,
add it to the allowlist in the script and write the reason there.

Then run the branch-write tests:

```sh
cargo test -p iceberg --lib transaction::branch_writes
```

The fork does not change the upstream workflows in `.github/`, so no CI job
runs the guard. Run it by hand in the sync steps.
