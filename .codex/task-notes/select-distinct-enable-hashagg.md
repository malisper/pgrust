Goal:
Diagnose and fix the select_distinct regression diff around SELECT DISTINCT with LIMIT.

Key decisions:
The semantic mismatch came from SET enable_hashagg TO OFF being accepted but not wired into PlannerConfig. SELECT DISTINCT with LIMIT is lowered to grouping, so the planner still chose HashAggregate and returned insertion-order groups instead of sorted groups. Added enable_hashagg to PlannerConfig and omitted hashed aggregate paths when it is disabled, falling back to sorted grouping.

Files touched:
src/include/nodes/pathnodes.rs
src/pgrust/session.rs
src/backend/optimizer/plan/planner.rs
src/backend/optimizer/tests.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet disabled_hashagg_uses_sorted_grouping_strategy
scripts/cargo_isolated.sh test --lib --quiet set_guc_to_default_resets_runtime_value
scripts/cargo_isolated.sh test --lib --quiet disabled_hashagg_keeps_distinct_limit_sorted
scripts/run_regression.sh --test select_distinct --timeout 180 --port 5543 --jobs 1

Remaining:
The normal parallel regression harness still fails before the test while staging the post_create_index base on create_index with an existing create_index/GIN issue. Running select_distinct with --jobs 1 avoids isolated base staging and reaches the target test. The latest select_distinct rerun fails with 90/105 queries matched and 290 diff lines. The previous LIMIT 10 row-order mismatch is gone; remaining differences are plan-shape/display gaps.
