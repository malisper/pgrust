Goal:
Fix core PostgreSQL `window` regression failures while leaving SQL-language
functions with `BEGIN ATOMIC`/`WINDOW` and custom aggregate window execution as
follow-up scope.

Key decisions:
- Added parsed/analyzed `EXCLUDE` frame clauses and implemented executor
  exclusion for aggregate/value window functions.
- Matched PostgreSQL-visible peer ordering by using an unstable SQL sort path
  instead of synthetic tie keys for visible sorts.
- View deparse now renders window functions, function RTEs, frame exclusions,
  and verbose interval literals; temp `CREATE OR REPLACE VIEW` no longer leaves
  missing rewrite rules on replacement failure.
- ROWS/GROUPS frame offsets are coerced to int8 internally but deparsed without
  exposing the analyzer-added cast. Current-level variables in ROWS/GROUPS
  offsets now error like PostgreSQL.
- Fixed `count() OVER ()` rejection, VALUES common-type gaps for interval and
  numeric unknown literals, timestamp `generate_series` interval-step coercion,
  and explicit `bool::integer`.

Files touched:
- Parser/AST/analyzer: `src/backend/parser/gram.pest`,
  `src/backend/parser/gram.rs`, `src/include/nodes/parsenodes.rs`,
  `src/include/nodes/primnodes.rs`, `src/backend/parser/analyze/*`.
- Planner/rewrite/explain threading: `src/backend/optimizer/*`,
  `src/backend/rewrite/mod.rs`, `src/backend/rewrite/views.rs`,
  `src/backend/commands/explain.rs`.
- Executor/runtime: `src/backend/executor/window.rs`,
  `src/backend/executor/nodes.rs`, `src/backend/executor/expr_casts.rs`,
  `src/backend/executor/mod.rs`.
- Temp view catalog handling: `src/pgrust/database/commands/create.rs`,
  `drop.rs`, `execute.rs`, `src/pgrust/database/temp.rs`,
  `src/pgrust/session.rs`.
- Tests: `src/backend/parser/tests.rs`, `src/backend/executor/tests.rs`,
  `src/pgrust/database_tests.rs`.

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check` (passes; pre-existing warning in
  `src/bin/query_repl.rs` about unreachable `ReindexIndex` pattern)
- Focused tests:
  `window_frame_exclusion`,
  `pg_sql_sort_by_matches_postgres`,
  `build_plan_rejects_explicit_empty_count_window_call`,
  `build_plan_rejects_over_for_non_window_function`,
  `build_plan_rejects_rows_frame_offsets_with_variables`,
  `analyze_values_common_type_preserves_unknown_literal_targets`,
  `select_sql_bool_to_integer_cast`,
  `pg_get_viewdef_renders_window_functions_and_function_rtes`,
  `create_or_replace_temp_window_view_keeps_rewrite_rule`.
- Reduced current-binary window regression:
  `PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh --jobs 1 --port 55433 --schedule /tmp/diffs/window-only.schedule --test window --timeout 240 --results-dir /tmp/diffs/window-raleigh-v5`
  => `FAIL (303/388 queries matched, 1408 diff lines)`.
  This was before the final `generate_series(...) OVER ()` error-text patch;
  the focused test covers that change.

Remaining:
- Full default harness currently needs `create_index` base/dependency handling
  to get a comparable full `window` run; reduced schedule was used for current
  semantic verification.
- Remaining window diffs are mainly out-of-scope SQL function/custom aggregate
  sections, EXPLAIN/planner shape/name differences, error LINE/HINT formatting,
  function `OVER` error text, and some remaining peer/order/type coercion cases.
