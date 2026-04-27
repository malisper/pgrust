Goal:
Diagnose partition_prune regression diff from .context/attachments/pasted_text_2026-04-27_10-47-47.txt.

Key decisions:
The first real mismatch is planner pruning, not formatting. List partition pruning evaluates AND clauses per-clause, so partitions with multiple list values can survive even when no single value satisfies the full conjunction. NULL list values are also treated as order-comparable for <> and inequalities, which keeps NULL partitions for SQL comparisons that cannot be true on NULL. Range default partitions are conservative for non-exact constraints, so defaults/subpartitions remain for inequalities already fully covered by non-default range siblings.

Implemented explicit list-partition value checks for non-default list partitions so AND/OR are applied to the same concrete partition value. Strict comparison pruning now never matches NULL partition values or NULL comparison constants. Constant ScalarArrayOp pruning now handles IN/NOT IN style ANY/ALL comparisons through the same list-value path.

Added PostgreSQL-style default range pruning for single-key range partitions by turning full-key inequalities into query intervals and pruning default only when non-default sibling ranges fully cover that interval. Added a pruning-only own-partition-bound guard for partitioned children before child expansion; it maps simple parent partition keys to child columns by name/type so direct queries of partitioned children with different column order can be rejected without adding executor-visible quals.

Files touched:
.codex/task-notes/partition-prune-failures.md
src/backend/optimizer/partition_prune.rs
src/backend/optimizer/path/allpaths.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet partition_prune
scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test partition_prune --timeout 120 --port 55434

Remaining:
The focused Rust tests and cargo check pass. The full partition_prune regression still fails, but the targeted list AND/OR, NULL comparison, range-default, direct subpartition self-bound, and constant IN/NOT IN cases are fixed or semantically improved. Latest diff copied to /tmp/diffs/partition_prune.after-default-subpart-in-v2.diff. Remaining failures are mostly plan-shape/display mismatches plus broader unsupported pruning cases outside this fix.
