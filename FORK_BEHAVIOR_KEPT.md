# Fork Behavior Kept After Upstream Merge

## Scope

This document records the behavioral choices made in merge commit
`d5157f31` (`upstream/main` at `6077a42e`). It compares the committed tree
with Apache's parent, not merely the textual merge conflicts.

The fork still has approximately 9,150 net lines that differ from Apache
across 47 runtime files. Most of that code is intentional transaction
functionality rather than merge residue.

## Retained Fork Behavior

### Transaction maintenance actions

The following fork-only APIs and their supporting implementation remain:

- `Transaction::merge_append`: appends files and runs the manifest
  filter/merge pipeline to keep manifest counts bounded.
- `Transaction::rewrite_files`: atomically replaces data files for compaction,
  with validation, residual manifests, manifest merging, rollback cleanup, and
  stable identifiers over OCC retries.
- `Transaction::rewrite_manifests`: rewrites manifests across partitions while
  preserving sequence-number semantics and omitting tombstones.
- `Transaction::row_delta`: writes equality or positional delete manifests.
- `Transaction::delete_files`: marks existing data files deleted, with optional
  existence validation and accurate snapshot summaries.

The implementation is centered in:

- `crates/iceberg/src/transaction/{manifest_filter,manifest_merge,merging_state}.rs`
- `crates/iceberg/src/transaction/{merge_append,rewrite_files,rewrite_manifests,row_delta,delete_files}.rs`
- `crates/iceberg/src/transaction/commit_ids.rs`
- `crates/iceberg/src/prptl_utils/bin_packing.rs`

Apache's new `ExpireSnapshotsAction` and `UpdateSchemaAction` were retained
alongside those fork actions. The fork transaction producer was adapted to
Apache's split manifest-list modules, writer futures, current runtime
requirements, and computed-summary precedence. In particular, user snapshot
properties can no longer override computed file/manifest metrics.

### Arrow schema override

The fork keeps a caller-provided Arrow schema path for both reads and writes:

- `TableScanBuilder::with_arrow_schema`
- `ArrowReaderBuilder::with_arrow_schema`
- `ParquetWriterBuilder::with_arrow_schema`

The reader only adopts override fields whose Parquet physical layout is
compatible with the file. This supports Arrow layout choices such as
`Utf8View` without incorrectly decoding an `Int32` file as `Int64` after an
Iceberg type promotion. The record-batch transformer still handles real type
casts after decoding.

Apache's runtime propagation, name mappings, field-ID projection, INT96
coercion, page-index filtering, and delete-loader behavior remain in place.

### Scan task metadata and incremental scans

`FileScanTask` still carries manifest-derived planning metadata:

- `column_sizes`
- `split_offsets`
- `lower_bounds`
- `upper_bounds`
- `sort_order_id`

Those fields are populated in `scan/context.rs` and have builder defaults so
existing callers are source-compatible. They are useful to downstream
planners that need ordering, split, or bounds information without rereading a
manifest.

The fork's incremental scan API was also retained:

- `TableScanBuilder::from_snapshot_id`
- `TableScanBuilder::to_snapshot_id`

It validates ancestry, selects added data files from append/overwrite
snapshots only, filters out replaced files, and rejects delete manifests. It
was adapted to Apache's current runtime/channel pipeline and asynchronous
`DeleteFileIndex` rather than retaining the old direct collection pipeline.

### Partition and DataFusion behavior

The fork continues to resolve partition source columns by Iceberg field ID and
can resolve partition values by runtime name when necessary. This avoids
incorrect matches where projected or reordered Arrow fields share a data type.

Fork DataFusion behavior retained after its 54.0 integration includes the
custom Iceberg physical plans, timestamp predicate pushdown, scan/write Arrow
schema propagation, and the public table-provider construction path. The
plans were updated for DataFusion 54's `ExecutionPlan` trait and Apache's
borrowed location-generator API.

### Writer and storage behavior

The fork keeps:

- `RowGroupFlushable` through Parquet, rolling, and data-file writers.
- Explicit data-file `sort_order_id` stamping.
- S3-family path normalization, including `s3a://`.

## Apache Behavior Adopted

The merge takes Apache's current implementations for:

- Runtime ownership and propagation through tables, catalogs, scans, and
  delete loading.
- Name-mapping-aware scans and reader projection.
- The asynchronous delete-file index, including its lost-wakeup protection.
- Encrypted manifest-list reading, KMS/catalog wiring, and encrypted-file
  primitives.
- The split manifest-list reader/writer modules and current manifest writer
  interfaces.
- REST endpoint discovery, catalog behavior, transaction expiry/schema APIs,
  Python bindings, DataFusion 54, OpenDAL 0.57, release tooling, and CI.

The old fork-only pure-stream scan implementation was not carried forward.
Apache's runtime/channel pipeline supersedes it, and the existing deadlock
scan test passes on the merged implementation.

## Cleanup Candidates

These items are now unnecessary or should be treated as migration debt.

| Priority | Candidate | Why it can be removed or replaced | Suggested action |
| --- | --- | --- | --- |
| High | Per-action `key_metadata` fields and `set_key_metadata` methods in the fork maintenance actions | Apache currently rejects encrypted writes, and `SnapshotProducer` rejects these values. The methods cannot successfully produce encrypted manifests. | Remove the setters and plumbing, or replace them with Apache's encrypted-output workflow when encrypted writes are implemented. |
| High | `Snapshot::load_manifest_list` compatibility helper | Apache's `Table::manifest_list_reader` handles encrypted manifest lists. The helper is retained only because fork actions still call the legacy plain-file API. | Migrate fork actions to `Table::manifest_list_reader`, then delete the helper. This is required before those actions can participate in encrypted-table support. |
| Medium | Local `ancestors_between` in `scan/context.rs` | Apache now has `crate::util::snapshot::ancestors_between` with the same inclusive/exclusive traversal contract. | Use the shared utility and delete the local copy. |
| Medium | `prptl_utils` module boundary | It is still needed because Apache has no `ListPacker` equivalent, but the module name advertises its temporary origin. | Keep the algorithm; consider moving it to a normal `util::bin_packing` module when the fork-only namespace is no longer useful. |
| Low | Compatibility builder defaults on the five `FileScanTask` planning fields | They are intentional for source compatibility, but they make omission indistinguishable from an unavailable manifest metric. | Keep unless a future major version explicitly makes scan metadata mandatory. |

## Not Cleanup Candidates

The transaction action stack, bin-packing implementation, Arrow schema
override, partition field-ID resolution, incremental scan bounds, row-group
flush API, and scan metadata are not duplicated by Apache main. Removing them
would remove fork behavior, not merely simplify the merge.

## Validation Performed

After the merge:

- `cargo check --workspace -q` passed.
- `cargo test -p iceberg --lib -- --test-threads=1` passed: 1,501 tests.
- The transaction subset passed: 160 tests.
- The scan subset passed: 38 tests.
- The Arrow reader subset passed: 51 tests.

The restored incremental-scan path does not yet have a dedicated merged-tree
test. Add focused tests for append/overwrite inclusion, replace exclusion,
delete-manifest rejection, and ancestor validation before changing that path.
