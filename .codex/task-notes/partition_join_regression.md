Goal:
Run the partition_join regression test, write diffs to /tmp/diffs, and diagnose the main failures.

Key decisions:
Used a dedicated Cargo target dir because the shared target was blocked by other regression jobs.
Used port 56433 after 5433 and 55433 were already occupied.
Copied the primary diff and output to /tmp/diffs/partition_join.diff and /tmp/diffs/partition_join.out.

Files touched:
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/mod.rs
- src/backend/optimizer/path/subquery_prune.rs
- src/backend/optimizer/tests.rs
- .codex/task-notes/partition_join_regression.md

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-target-partition-join scripts/run_regression.sh --test partition_join --results-dir /tmp/diffs/partition_join --timeout 180 --port 56433 --skip-build

Remaining:
partition_join fails: 481/614 queries matched, 133 mismatched, 5575 diff lines.
Main failure areas are partitionwise join plan shape/costing, lateral partition joins timing out, unsupported TABLESAMPLE, and at least one wrong-result projection/slot mapping case in multi-way partition joins.

Lateral timeout detail:
The timed-out query keeps an unused subquery output expression `least(t1.a,t2.a,t3.a)`.
pgrust treats that as an outer dependency, producing a lateral Nested Loop Left Join with `Memoize Cache Key: t1.a`.
Removing only that unused `least(...)` target makes pgrust choose Hash Left Join and return the expected 12 rows immediately.
PostgreSQL has `remove_unused_subquery_outputs` in optimizer/path/allpaths.c for this class of issue.

Fix:
Added planner-side pruning for unused subquery target expressions in `src/backend/optimizer/path/subquery_prune.rs`.
It preserves target positions but replaces unused, non-volatile target expressions with NULL before planning the subquery.
Added `unused_lateral_subquery_output_does_not_parameterize_join` in `src/backend/optimizer/tests.rs`.
Validation:
- `CARGO_TARGET_DIR=/tmp/pgrust-target-partition-join scripts/cargo_isolated.sh test --lib --quiet unused_lateral_subquery_output_does_not_parameterize_join`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-partition-join scripts/cargo_isolated.sh check`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-partition-join scripts/run_regression.sh --test partition_join --results-dir /tmp/diffs/partition_join_fix --timeout 180 --port 56633`
Result:
The former lateral timeout now returns the expected 12 rows. `partition_join` still fails overall, but improved from 481/614 to 482/614 query matches and has no statement-timeout failure.
Final rerun after refactor:
`CARGO_TARGET_DIR=/tmp/pgrust-target-partition-join scripts/run_regression.sh --test partition_join --results-dir /tmp/diffs/partition_join_fix --timeout 180 --port 56633`
Result: FAIL, 482/614 queries matched, 132 mismatched, 5547 diff lines, Timed out: 0.
