Goal:
Merge origin/perf-optimization into malisper/regress-104032 and keep select_distinct at the known parallel-only diff state.

Key decisions:
Resolved the setrefs conflict by keeping allowed PARAM_EXEC validation and adding IncrementalSort validation.
Avoided caching the pre-DISTINCT projection as a final upper relation, then forced the real final projection to restore output labels.
Rendered HashAggregate group keys through hidden computed projections so DISTINCT expression display stays PostgreSQL-like.

Files touched:
src/backend/optimizer/setrefs.rs
src/backend/optimizer/plan/planner.rs
src/backend/commands/explain.rs
src/backend/optimizer/tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet planner_preserves_distinct_before_final_order_projection -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet explain_hash_distinct_group_key_uses_distinct_expr -- --nocapture
CARGO_TARGET_DIR=/tmp/pgrust-target-merge-no-run cargo test --no-run --message-format json-render-diagnostics --lib --locked
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1

Remaining:
select_distinct still has the accepted parallel-plan diffs: 100/105 queries matched, 90 diff lines.
