# 2026-07-10 Upstream Main Integration

## Commit Range

- Fork parent before the merge: `113e11843000422a748fde5fe9afebb39ab9b067`
  (`perpetual-main`).
- Apache parent integrated: `6077a42ea061f73a561c26880993f84fafb1ba0f`
  (`apache/iceberg-rust:main`).
- Merge commit: `d5157f31d9c469d0896abd2d3a17eef0537ccd6d`.
- Follow-up reconciliation range: `d5157f31..f5ee3ef4`.

This changelist records the resulting behavior after integrating Apache main
and resolving the required compatibility and semantic regressions.

## Retained Fork Behavior

### Transaction maintenance actions

The fork retains these APIs and their supporting implementation:

- `Transaction::merge_append`, which appends files through the manifest
  filter/merge pipeline to keep manifest counts bounded.
- `Transaction::rewrite_files`, which atomically replaces data files for
  compaction with validation, residual manifests, rollback cleanup, and stable
  identifiers across OCC retries.
- `Transaction::rewrite_manifests`, which rewrites manifests across partitions
  while preserving sequence-number semantics and omitting tombstones.
- `Transaction::row_delta`, which writes equality or positional delete
  manifests.
- `Transaction::delete_files`, which marks existing data files deleted with
  optional existence validation and accurate snapshot summaries.

The implementation remains in
`crates/iceberg/src/transaction/{manifest_filter,manifest_merge,merging_state}.rs`,
`{merge_append,rewrite_files,rewrite_manifests,row_delta,delete_files}.rs`,
`commit_ids.rs`, and `crates/iceberg/src/prptl_utils/bin_packing.rs`.

### Arrow schema override

The fork retains caller-provided Arrow schemas for reads and writes through:

- `TableScanBuilder::with_arrow_schema`
- `ArrowReaderBuilder::with_arrow_schema`
- `ParquetWriterBuilder::with_arrow_schema`

Only physically compatible override fields are adopted during Parquet decoding;
real Iceberg type promotion remains the record-batch transformer's job.

### Scan task metadata and incremental scans

`FileScanTask` continues to expose manifest-derived `column_sizes`,
`split_offsets`, `lower_bounds`, `upper_bounds`, and `sort_order_id` metadata.
The fork also retains bounded incremental scans through
`TableScanBuilder::from_snapshot_id` and `to_snapshot_id`, including ancestry
validation, replacement filtering, and delete-manifest rejection.

### Partition and DataFusion behavior

Partition source columns continue to resolve by Iceberg field ID, with runtime
name resolution where projection rewrites remove field-ID metadata. The fork's
custom Iceberg DataFusion physical plans, timestamp predicate pushdown,
scan/write Arrow-schema propagation, and public table-provider construction
path remain in place.

### Writer and storage behavior

The fork retains `RowGroupFlushable` through Parquet, rolling, and data-file
writers; explicit data-file `sort_order_id` stamping; and S3-family path
normalization including `s3a://`.

## Fork Code Removed As Unneeded

The following fork-specific compatibility code was removed in favor of Apache
main equivalents:

- `Snapshot::load_manifest_list` was removed. Callers now use Apache's
  `Table::manifest_list_reader(snapshot).load()`.
- The local `ancestors_between` helper in `scan/context.rs` was removed in
  favor of Apache's `util::snapshot::ancestors_between`.
- The duplicate OpenDAL S3 `tests` module introduced during conflict resolution
  was removed; the retained upstream module covers the test behavior.
- The merge had accidentally restored Apache's obsolete empty-insert path. The
  fork no longer carries that regression: it now uses Apache's
  `IcebergCommitExec` behavior, returning one `count = 0` row without creating
  a snapshot.

## Apache Behavior Adopted

The integration adopts Apache's current runtime propagation, name-mapping-aware
scans, asynchronous delete-file index, encrypted manifest-list reading and KMS
wiring, split manifest-list modules, REST endpoint discovery, transaction
expiry and schema APIs, DataFusion 54, OpenDAL 0.57, Python binding updates,
and CI/release tooling.

The old fork-only pure-stream scan pipeline was not retained. Apache's
runtime/channel pipeline supersedes it.

## Remaining Deliberate Debt

- Per-action `key_metadata` setters remain unusable while Apache blocks
  encrypted writes. Remove them or replace them with the encrypted-output
  workflow when encrypted writes are supported.
- `prptl_utils` remains because Apache has no `ListPacker` equivalent. The
  algorithm is required, although its temporary module name can later move to
  a general utility namespace.
- Compatibility defaults on `FileScanTask` metadata fields remain intentional
  for source compatibility.

## Verification

`make test` completed successfully after the reconciliation: 1,969 tests
passed, 0 skipped.
