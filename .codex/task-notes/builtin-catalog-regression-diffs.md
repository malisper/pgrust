Goal:
Diagnose /tmp/diffs regression failures around builtin catalog/function/operator
coverage, then fix the multirangetypes dynamic range aggregate failure.

Key decisions:
Grouped failures by root cause rather than per-hunk. Main groups: dynamic aggregate
OIDs not visible at execution, polymorphic function binding/default/variadic issues,
geometry operator/opclass planner coverage, AM reloption/default-opclass gaps, BRIN
detoast during index build, and SQL/JSON formatting/deparse/runtime gaps.

Files touched:
.codex/task-notes/builtin-catalog-regression-diffs.md
crates/pgrust_catalog_data/src/builtin_ranges.rs
crates/pgrust_catalog_data/src/pg_aggregate.rs
crates/pgrust_executor/src/aggregate.rs
crates/pgrust_analyze/src/lib.rs
src/backend/utils/cache/visible_catalog.rs
src/backend/utils/cache/lsyscache.rs
src/pgrust/database_tests.rs

Tests run:
Read targeted diff files under /tmp/diffs.
scripts/cargo_isolated.sh test -p pgrust_catalog_data --lib --quiet synthetic_range_aggregate_rows_cover_dynamic_aggregate_proc_oids
scripts/cargo_isolated.sh test -p pgrust_executor --lib --quiet dynamic_range_aggregate_oids_use_builtin_runtime_selection
scripts/cargo_isolated.sh test --lib --quiet dynamic_range_aggregates_work_for_custom_range_and_multirange
scripts/run_regression.sh --test multirangetypes --timeout 120 --port 55433 --results-dir /tmp/pgrust-regression-multirangetypes-55433

Remaining:
Follow-up patches for polymorphic binding, geometry/opclass planner behavior, BRIN
detoast during index build, and SQL/JSON formatting/deparse/runtime gaps.
