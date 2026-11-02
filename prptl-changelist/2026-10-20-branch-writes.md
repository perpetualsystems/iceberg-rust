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
find this. Each `base_snapshot()` call site has a test that fails if the call
reads the `main` head.

## Known Sync Hotspot

`snapshot.rs` has about 10 conflict hunks on each sync. One fork edit is in
an existing conflict: the previous-snapshot read for the summary
(`let previous_snapshot = self.base_snapshot();`). Upstream has
`table_metadata.current_snapshot()` at this place.

If you take the upstream side, branch handling stops. The code still compiles.
Keep the fork side: `self.base_snapshot()`. Then run the tests below.

`append.rs` has one small hunk (`current_snapshot()` replaced by
`base_snapshot()`). Check it in the same way.

## Main-Only Actions

These actions do not have `to_branch(name)`. They read `current_snapshot()` or
use `MAIN_BRANCH` on purpose:

- `expire_snapshots.rs`: the `main` check when it selects refs to expire, and
  the `RefSnapshotIdMatch` requirement.
- `rewrite_manifests.rs`: the `current_snapshot()` read and two `MAIN_BRANCH`
  requirements.

## Sync Check

Run the branch-write tests after each sync:

```sh
cargo test -p iceberg --lib transaction::branch_writes
```

If upstream adds a new `current_snapshot()` read to the commit path of an
action that has `to_branch(name)`, add a test for it to `branch_writes.rs`.
