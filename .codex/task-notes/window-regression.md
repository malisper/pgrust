Goal:
Implement the first pass of fixes for the upstream `window` regression.

Key decisions:
Kept behavior changes in parser/analyzer/catalog/executor instead of expected
file edits or harness normalization. Used an isolated Cargo target for final
regression reruns to avoid unrelated shared-target locks.

Files touched:
`.codex/task-notes/window-regression.md`
`src/include/nodes/parsenodes.rs`
`src/backend/parser/gram.rs`
`src/pgrust/database/commands/create.rs`
`src/pgrust/session.rs`
`src/backend/parser/analyze/functions.rs`
`src/backend/parser/analyze/expr.rs`
`src/backend/parser/analyze/coerce.rs`
`src/backend/parser/analyze/scope.rs`
`src/backend/parser/analyze/window.rs`
`src/backend/rewrite/views.rs`
`src/backend/executor/sqlfunc.rs`
`src/backend/executor/agg.rs`
`src/backend/executor/expr_agg_support.rs`
`src/backend/executor/window.rs`
`src/backend/executor/expr_string.rs`
`src/backend/executor/exec_expr.rs`
`src/include/catalog/pg_proc.rs`
`src/include/nodes/primnodes.rs`
`src/backend/parser/tests.rs`
`src/pgrust/database/commands/drop.rs`

Tests run:
`scripts/run_regression.sh --test window --results-dir /tmp/pgrust_window_regress`
failed before SQL execution because port 5433 was occupied.
`scripts/run_regression.sh --test window --skip-build --results-dir /tmp/pgrust_window_regress --port 55433`
errored because worker port 55434 was occupied.
`scripts/run_regression.sh --test window --skip-build --results-dir /tmp/pgrust_window_regress --port 61233 --jobs 1`
ran to completion: 298/388 queries matched, 90 mismatched, 1451 diff lines.
`cargo fmt`
`CARGO_TARGET_DIR=/tmp/pgrust-window-target-chicago scripts/cargo_isolated.sh test --lib --quiet inline_sql_function_preserves_unbounded_window_frame_keyword`
passed.
`CARGO_TARGET_DIR=/tmp/pgrust-window-target-chicago scripts/run_regression.sh --test window --results-dir /tmp/pgrust_window_regress_impl4 --port 61273 --jobs 1`
ran to completion: 315/388 queries matched, 73 mismatched, 1201 diff lines.
`CARGO_TARGET_DIR=/tmp/pgrust-window-target-chicago scripts/cargo_isolated.sh test --lib --quiet parse_drop_function_statement_with_multiple_names`
passed.
`CARGO_TARGET_DIR=/tmp/pgrust-window-target-chicago scripts/cargo_isolated.sh test --lib --quiet parse_create_function_statement_with_window_clause`
passed.
`CARGO_TARGET_DIR=/tmp/pgrust-window-target-chicago scripts/run_regression.sh --test window --results-dir /tmp/pgrust_window_regress_impl8 --port 61313 --jobs 1`
ran to completion: 324/388 queries matched, 64 mismatched, 1102 diff lines.

Remaining:
Current diff artifacts copied to `/tmp/diffs/window-diff/window.diff`,
`/tmp/diffs/window-output/window.out`, and `/tmp/diffs/window-summary.json`.
Fixed or improved: `CREATE FUNCTION ... WINDOW`, internal `window_nth_value`
mapping with named/default args, SQL function `UNBOUNDED` frame keyword
substitution, `pg_temp` function creation/lookup, polymorphic aggregate support
function validation, custom moving aggregate runtime, `quote_nullable(text)`,
interval `sum`, `timestamptz - timestamptz` inference in `VALUES`,
multi-item `DROP FUNCTION`, SQL set-function inlining for the `pg_temp.f`
window body, view deparse for unambiguous function aliases like `i(i)`, and
RANGE offset text for varchar order keys.
Remaining: exact RANGE/GROUPS error caret/hints, several SQL-visible window
context errors, custom moving aggregate FILTER/volatile edge cases, exact
`pg_temp.f` EXPLAIN subquery shape, and planner shape/order drift.
