# November 24, 2026 rebase onto Apache main

The upstream baseline is Apache Iceberg Rust commit `90e454aa51863afd0ad70dd92cf0a2aed1995e07`.
The consolidated fork changes come from `perpetual-main` at
`df49dff54ac4b6994fd29a8fb34a53594227471d`. The previous Apache baseline
was `v0.11.0-rc.2`. Earlier fork commits included squashed Apache imports.

## Core extensions

- Branch-targeted appends and maintenance transactions, including file
  rewrites, manifest rewrites, merge append, deletes, and row deltas. The
  merging actions use Apache's per-execution state for catalog retries.
- Bounded incremental scans, planning-time split offsets, and caller-supplied
  Arrow schemas for readers and writers.
- Partition projection by name, explicit row group flushing, sort order
  stamping on data files, and native Parquet Variant writes.
- Configurable HTTP client options for OpenDAL storage.

## Downstream API changes

Apache now develops the DataFusion provider in
[apache/datafusion-iceberg](https://github.com/apache/datafusion-iceberg).
The core workspace follows that split, including the removal of the Python
`IcebergDataFusionTable` binding. The Phantom migration is a separate change.

For per-column scan statistics, request columns with
`TableScanBuilder::with_column_stats` and read them with
`FileScanTask::column_stats`. Apache also supplies `FileScanTask::sort_order_id`.
The canonical Arrow schema for unshredded Variant uses `Binary` for its
storage fields. The native Parquet round-trip test covers data supplied as
`BinaryView`.

Apache's transaction layer reserves physical cleanup of discarded retry
manifests for its terminal cleanup hook. That hook is pending upstream.
