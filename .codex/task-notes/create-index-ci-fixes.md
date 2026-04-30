Goal:
Fix CI failures reported after merging perf-optimization into create_index work.

Key decisions:
- Keep OR-to-SAOP planner support and update the stale index-matrix test to assert the new indexed plan.
- Preserve original OR predicates for selectivity/recheck when using transformed scalar-array index quals.
- Use clause-list selectivity for bitmap candidates so extended dependency stats apply.
- Prefer narrower btree indexes for equal order-only pathkeys when choosing presorted paths.

Files touched:
- src/backend/commands/explain.rs
- src/backend/optimizer/bestpath.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/plan/planner.rs
- src/backend/utils/sql_deparse.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet explain_geometry_sort_keys_render_sql_function_names
- scripts/cargo_isolated.sh test --lib --quiet partial_index_catalog_persists_predicate_and_pg_get_indexdef_renders_where
- scripts/cargo_isolated.sh test --lib --quiet stats_ext_dependencies_use_postgres_selectivity_formula
- scripts/cargo_isolated.sh test --lib --quiet index_matrix_order_only_uses_forward_index_scan
- scripts/cargo_isolated.sh test --lib --quiet index_matrix_or_predicate_uses_scalar_array_index_scan
- scripts/cargo_isolated.sh test --lib --quiet index_matrix_
- scripts/cargo_isolated.sh check

Remaining:
- Existing unrelated unreachable-pattern warnings remain during check/tests.
