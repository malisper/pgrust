Goal:
Fix remaining `window` regression failures from
`/tmp/pgrust-regression-diffs-2026-05-01T2044Z/window.diff`.

Key decisions:
Use assignment-cast compatibility for SQL `RETURNS TABLE` validation, allow
cataloged `CREATE FUNCTION ... WINDOW` rows, normalize grouped window function
arguments through default/named-argument resolution, prefer exact polymorphic
support proc lookup before concrete runtime-coercion retry, and keep planner
display sort keys in window-qualified EXPLAIN contexts.

Files touched:
`src/pgrust/database/commands/create.rs`
`src/backend/parser/analyze/agg_output.rs`
`src/backend/commands/explain.rs`
`src/pgrust/database_tests.rs`

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh test --lib --quiet create_sql_function_window_frames_keep_unbounded_keyword`
`scripts/cargo_isolated.sh test --lib --quiet create_window_function_named_and_default_args_work_in_grouped_query`
`scripts/cargo_isolated.sh test --lib --quiet polymorphic_moving_aggregate_window_frames_work`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --test window --jobs 1 --results-dir /tmp/pgrust-window-fix`

Remaining:
None for the targeted `window` regression.
