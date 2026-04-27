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

Goal:
Fix the next `partition_prune` clusters: EXPLAIN single-survivor Append/display noise, hash partition pruning, and boolean pruning operators.

Key decisions:
Hash pruning must use the partition opclass support proc, not pgrust's default hash helper. PostgreSQL's regression uses custom SQL hash support functions (`part_hashint4_noop`, `part_hashtext_length`) to make expected remainders stable. The planner now evaluates built-in extended hash support and those lightweight immutable SQL support functions; unknown support functions remain conservative.

Single-child Appends are still represented internally, but setrefs now rewrites the surviving child scan display alias to the parent alias. This matches PostgreSQL's `Seq Scan on child parent_alias` shape for one surviving partition without changing executor behavior.

Boolean pruning now recognizes pruning-only truth constraints for bare bool partition keys, `NOT key`, `key = bool`, `key <> bool`, and `NOT (key = bool)`, including simple `partition by list ((not a))` expression keys. Range bool `IS NOT TRUE/FALSE` pruning is tightened for explicit bool range bounds while default ranges remain conservative.

Files touched:
src/backend/commands/explain.rs
src/backend/executor/nodes.rs
src/backend/optimizer/inherit.rs
src/backend/optimizer/partition_prune.rs
src/backend/optimizer/setrefs.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet partition_prune
scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test partition_prune --timeout 120 --port 55435

Remaining:
Latest regression run: 490/750 matched, 4397 diff lines. Hash pruning now chooses the same hp/hp_prefix physical partitions as PostgreSQL; remaining hp differences are alias numbering and filter ordering. Boolean pruning for `not a = false`, `partition by list ((not a))`, and bool range `NOT` cases is fixed; remaining boolean differences are mostly display (`NOT a` vs `a = false`, `IS UNKNOWN` vs `IS NULL`, boolean array literal spelling) and alias numbering. Larger remaining clusters are unsupported PREPARE/runtime pruning, regex scalar-array operators, collation/type rendering (`bpchar`, `COLLATE`), multi-child Append alias numbering, and unrelated DDL/operator syntax gaps.
