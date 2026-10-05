# 2026-10-01 Partition Expression Dependencies

## Assessment

The fork fixes `PartitionExpr` so DataFusion can see and rewrite the expressions
that provide Iceberg partition values. The hidden dependencies are an upstream
integration defect: ordinary SQL inserts can succeed while recording incorrect
partition values, causing subsequent filtered reads to omit the inserted rows.

This follows the [DataFusion 55 upgrade](2026-09-30-datafusion-55.md) on
`aaron/datafusion-55`, based on `e321f638834958d4799b62fe8b63ed3d41ff9a2e`.
The earlier upgrade assessment covered dependency and API compatibility; this
entry records the subsequent correctness investigation and fork fix.

## Simba Context

Simba uses the fork's `FieldMatchMode::Name` to resolve partition sources when
input batches do not carry usable target Iceberg field IDs. For example, an
input projection exposes `event_time AS timestamp`, and the table partitions on
`day(timestamp)`. If DataFusion merges that projection into the partition
projection, the calculator can receive the original batch, which has
`event_time` but no `timestamp`, and fail with `Field not found: timestamp`.

The fork fix supersedes the Simba adapter at
`data-plane/phantom/src/iceberg/catalog_table_provider/partition_expr.rs`
which exposed every input column as a child and reconstructed the original
batch schema before invoking Iceberg's expression. That approach can retain
unrelated columns and duplicate expression evaluation. Partition dependency
tracking belongs in the fork's DataFusion integration; Simba uses that
implementation as part of the migration.

The exact migration change that first exposed the failure has not been
isolated. Both DataFusion 54 and 55 traverse declared expression children;
this investigation does not establish that projection merging or the defect
originated in DataFusion 55.

## Upstream Ownership and Cause

The current upstream owner is **`apache/datafusion-iceberg`**. The integration
previously lived inside `apache/iceberg-rust` and remains bundled in this fork
under `crates/integrations/datafusion`.

The upstream `PartitionExpr` returns no expressions from `children()`, ignores
`with_new_children()` replacements, and evaluates its calculator against the
runtime batch. DataFusion therefore cannot discover or rewrite its column
dependencies when merging projections. DataFusion's rewrite is valid for the
dependencies declared by the expression. The core Iceberg calculator expects
appropriately aligned inputs; the integration must preserve that contract
across DataFusion rewrites.

Upstream's calculator caches source positions from the table schema. Unlike
the fork's name matching, it can survive a same-position alias but silently use
the wrong values after a reorder or derived expression. The name-matching
extension changes the observed symptom; it is not required to trigger the
underlying defect.

The inspected upstream change making `PartitionExpr` reconstructible retains
its spec and schema for reconstruction, but still declares no children. It
does not fix this optimizer interaction.

## Upstream Reproduction

Reproduced with unchanged upstream production code and dependency lockfile:

- `apache/datafusion-iceberg` commit
  [`a2bc9427d0659591b5f8122b90fec710fe2f5de6`](https://github.com/apache/datafusion-iceberg/commit/a2bc9427d0659591b5f8122b90fec710fe2f5de6).
- Its pinned `apache/iceberg-rust` dependency:
  [`8cb2adeddb4f8da1ca7bd86ca303337f011c12e2`](https://github.com/apache/iceberg-rust/commit/8cb2adeddb4f8da1ca7bd86ca303337f011c12e2).
- DataFusion 55.1.0, Arrow/Parquet 59.3.0, and Rust 1.98.1.

The test uses the standard `IcebergCatalogProvider`, an in-memory source table,
local file storage, and ordinary `INSERT INTO ... SELECT ...` SQL. The failing
cases use default optimizer settings and pass upstream's input-schema
validation. No Simba code, fork extensions, or custom physical expressions are
involved.

The source columns are `left_value = [1, 2]` and `right_value = [101, 202]`.
Each case creates a fresh destination with columns `(part, value)` and identity
partitioning on `part`:

| Source expression for `part` | Written values | Stored partition values | Filtered read for written values |
| --- | --- | --- | --- |
| `left_value AS part` | `[1, 2]` | `[1, 2]` | `[1, 2]` |
| `right_value AS part`, with `left_value AS value` | `[101, 202]` | `[1, 2]` | No rows |
| `left_value + 10 AS part` | `[11, 12]` | `[1, 2]` | No rows |

For example, upstream successfully executes:

```sql
INSERT INTO ice.test.dest
SELECT right_value AS part, left_value AS value FROM src;

SELECT part FROM ice.test.dest ORDER BY part;
-- Returns 101, 202.

SELECT part FROM ice.test.dest WHERE part IN (101, 202) ORDER BY part;
-- Incorrectly returns no rows.
```

The stored partition values were inspected through the table's file scan tasks.
The full scan confirms the intended row values were written; incorrect
partition metadata causes the filtered scan to prune those files. Disabling
only the `ProjectionPushdown` physical optimizer rule makes all three cases
pass and preserves the separate input and partition projections. The same six
cases, including those controls, pass with the fork fix.

The portable regression is retained in
[`partition_dependency_regression.rs`](../crates/integrations/datafusion/tests/partition_dependency_regression.rs).
The upstream copy uses the `datafusion_iceberg` import instead of the fork's
`iceberg_datafusion` import. Run the fork version with:

```sh
cargo test -p iceberg-datafusion --test partition_dependency_regression --locked -- --nocapture
```

## Fork Fix and Compatibility

The implementation:

- Binds partition sources against the input schema before optimization, using
  the calculator's existing name or field-ID matching policy.
- Exposes only partition-source expressions as children and honors replacements,
  including derived expressions and scalar literals. Multiple transforms of
  the same source share one child evaluation.
- Applies Iceberg's existing transform functions to the evaluated source arrays
  through `PartitionValueCalculator::calculate_from_columns`, without rebuilding
  the entire original input batch.
- Represents nested source access as a rewritable expression that propagates
  nulls from every parent struct, matching Iceberg's existing projector behavior.
- Includes rewritten children in expression equality and hashing, and rejects
  replacement lists with the wrong number of children.

Direct callers must replace `PartitionExpr::new(calculator, spec)` with
`PartitionExpr::try_new(calculator, spec, input_schema)?`. The public
`project_with_partition` and `project_with_partition_with_match_mode` signatures
are unchanged. Core and integration public API snapshots have been updated.
The calculator's existing `calculate(batch)` interface and matching behavior
remain available to other callers.

The Simba migration updates its direct constructor call and uses the fork's
implementation. Downstream Simba changes and validation are tracked separately
from this fork change. The fix does not repair partition metadata already
written incorrectly.

## Verification and Follow-up

- DataFusion unit tests: 95 passed, including projection-optimizer regressions
  for aliases, reordering, derived and scalar inputs, unrelated columns,
  repeated sources, nested nulls, and field-ID matching.
- Existing DataFusion integration tests: 9 passed, including partitioned and
  nested inserts.
- New SQL insert regression: passed all six cases with the fork fix; failed
  the two expected cases against upstream.
- Partition calculator tests: 8 passed, including source-count validation,
  empty arrays, and existing matching behavior.
- DataFusion doctests: 1 passed, 5 ignored by the existing suite.
- Clippy for core and integration, plus the new regression target, with
  warnings denied: passed. Formatting and whitespace checks: passed.

Temporarily restoring the old expression's hidden-child and runtime-batch
behavior also reproduced the missing-field failure in the fork's alias test.

An upstream report and fix belong in `apache/datafusion-iceberg`; no upstream
issue or pull request was filed during this work. The reproduction establishes
upstream impact at the recorded revisions, not the first affected release or
whether any existing Perpetual tables contain incorrect partition metadata.
