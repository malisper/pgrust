Goal:
Fix CI failures reported after the select_distinct DISTINCT-planning PR.

Key decisions:
Keep non-parallel select_distinct behavior aligned with PostgreSQL while leaving parallel plan diffs out of scope. Preserve InitPlan display when projections own subplans. Use catalog relnames to avoid qualifying unaliased sort keys while keeping explicit aliases.
For the second CI follow-up, charge aggregate paths for grouping expressions as well as transition expressions so two-key hash DISTINCT no longer ties sorted Unique on a one-row min/max rewrite. Keep the min/max rewrite test focused on the presence of Unique, because a final trivial Projection may sit above it after setrefs.

Files touched:
src/backend/commands/explain.rs
src/backend/optimizer/plan/planner.rs
src/backend/executor/tests.rs
src/pgrust/database_tests.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
targeted CI failure filters from attached logs
scripts/cargo_isolated.sh test --lib --quiet backend::optimizer::tests:: -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet explain -- --nocapture
scripts/cargo_isolated.sh test --lib --quiet distinct -- --nocapture
scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1

Remaining:
select_distinct still has only the accepted parallel-plan diffs.
