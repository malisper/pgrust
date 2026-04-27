Goal:
Merge the updated origin/perf-optimization into malisper/regress-104032 and preserve the select_distinct non-parallel fixes.

Key decisions:
Resolved EXPLAIN conflicts by combining upstream sibling-scan alias and XID group-key display with local IncrementalSort, hidden projection, and DISTINCT expression display behavior.
Resolved executor aggregate EXPLAIN conflicts by keeping upstream disabled/XID formatting and local duplicate group-key suppression.
Adjusted child indentation for the new upstream prefix formatter so hidden plan nodes do not widen psql output separators.

Files touched:
src/backend/commands/explain.rs
src/backend/executor/nodes.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet explain_hash_distinct_group_key_uses_distinct_expr -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet planner_preserves_distinct_before_final_order_projection -- --nocapture
scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-merge2-no-run cargo test --no-run --message-format json-render-diagnostics --lib --locked
scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1

Remaining:
select_distinct still has only the accepted parallel-plan diff set: 100/105 queries matched, 90 diff lines.
