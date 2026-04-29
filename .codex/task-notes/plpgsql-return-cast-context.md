Goal:
Close PL/pgSQL scalar return cast context diffs.

Key decisions:
Wrap errors from scalar return coercion with PostgreSQL-style `while casting return value to function's return type` context. Leave expression evaluation errors unchanged so `RETURN 1/0` still reports expression/RETURN context.

Files touched:
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_cast_errors_include_cast_context
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet plpgsql
CARGO_INCREMENTAL=0 scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-return-cast-context

Remaining:
Regression is 2237/2271 matched, 408 diff lines. `cast_invoker(-1)` still misses the inner SQL-function context and has a top-level LINE pointer; composite unknown/varchar and transition tables remain larger semantic clusters.
