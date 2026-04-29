Goal:
Close PL/pgSQL context diffs where direct expression failures were missing expression frames.

Key decisions:
Store the original PL/pgSQL expression text on compiled expressions. Add expression context only for direct expression failures without existing SQL/PL context, so nested function errors do not gain extra frames. Declared cursor argument failures use a parameter-order context like `42/0 AS p1, 77 AS p2`.

Files touched:
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_expression_errors_include_expression_context
scripts/cargo_isolated.sh test --lib --quiet plpgsql_declared_cursor_arg_errors_include_argument_context
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-expression-context

Remaining:
Regression is 2235/2271 matched, 426 diff lines. Remaining clusters include anyarray inference, RAISE compile context/line numbers, SQLSTATE compile-time query context, composite unknown/varchar mismatch, nonstandard-string warnings, planner EXPLAIN output, and transition tables.
