#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Perpetual fork guard for branch writes (see prptl-changelist/).
#
# Fails when non-test code in crates/iceberg/src/transaction/*.rs calls
# `current_snapshot()` or uses `MAIN_BRANCH`, unless the line is in the
# allowlist below. Commit-path code must read the base snapshot through
# `SnapshotProducer::base_snapshot()`.
#
# Skipped: branch_writes.rs, `use` declarations, and everything from the first
# inline test module (`#[cfg(test)]` followed by `mod name {`) to end of file.
#
# Usage: dev/check_branch_write_guard.sh [transaction-dir]

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dir="${1:-$root/crates/iceberg/src/transaction}"

# Allowlist: "<file>|<exact trimmed line>|<reason>".
# A line is matched on its trimmed text, so line-number drift does not matter.
allowlist='
append.rs|target_branch: MAIN_BRANCH.to_string(),|default target branch
delete_files.rs|target_branch: MAIN_BRANCH.to_string(),|default target branch
merge_append.rs|target_branch: MAIN_BRANCH.to_string(),|default target branch
rewrite_files.rs|target_branch: MAIN_BRANCH.to_string(),|default target branch
row_delta.rs|target_branch: MAIN_BRANCH.to_string(),|default target branch
snapshot.rs|target_branch: MAIN_BRANCH.to_string(),|default target branch
expire_snapshots.rs|if ref_name == MAIN_BRANCH|main is never expired as a ref
expire_snapshots.rs|r#ref: MAIN_BRANCH.to_string(),|requirement on main
rewrite_manifests.rs|let Some(snapshot) = table.metadata().current_snapshot() else {|rewrites main only
rewrite_manifests.rs|r#ref: MAIN_BRANCH.to_string(),|rewrites main only
'

fail=0
for f in "$dir"/*.rs; do
  name="$(basename "$f")"
  [ "$name" = "branch_writes.rs" ] && continue
  hits="$(awk '
    { line = $0; sub(/^[ \t]+/, "", line); sub(/[ \t]+$/, "", line) }
    pending_cfg {
      if (line ~ /^mod [A-Za-z0-9_]+ *\{$/) exit
      pending_cfg = 0
    }
    line == "#[cfg(test)]" { pending_cfg = 1; next }
    line ~ /^\/\// { next }
    in_use { if (line ~ /;$/) in_use = 0; next }
    line ~ /^(pub(\([a-z]+\))? )?use / { if (line !~ /;$/) in_use = 1; next }
    line ~ /current_snapshot\(\)|MAIN_BRANCH/ { print NR "\t" line }
  ' "$f")"
  [ -z "$hits" ] && continue
  while IFS=$'\t' read -r num text; do
    if printf '%s\n' "$allowlist" |
      awk -F'|' -v n="$name" -v t="$text" '$1==n && $2==t {f=1} END {exit !f}'; then
      continue
    fi
    echo "branch-write guard: $name:$num: $text" >&2
    fail=1
  done <<< "$hits"
done

if [ "$fail" -ne 0 ]; then
  echo "Use SnapshotProducer::base_snapshot() instead, or add the line to the" >&2
  echo "allowlist in dev/check_branch_write_guard.sh with a reason." >&2
  exit 1
fi
echo "branch-write guard: ok"
