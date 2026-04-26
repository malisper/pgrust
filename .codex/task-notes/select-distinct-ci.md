Goal:
Fix CI failures reported after the select_distinct DISTINCT-planning PR.

Key decisions:
Keep non-parallel select_distinct behavior aligned with PostgreSQL while leaving parallel plan diffs out of scope. Preserve InitPlan display when projections own subplans. Use catalog relnames to avoid qualifying unaliased sort keys while keeping explicit aliases.

Files touched:
src/backend/commands/explain.rs
src/backend/optimizer/plan/planner.rs
src/backend/executor/tests.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
targeted CI failure filters from attached logs
scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1

Remaining:
select_distinct still has only the accepted parallel-plan diffs.
