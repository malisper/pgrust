Goal:
Add PostgreSQL-style PL/pgSQL context for functions that reach end without
RETURN.

Key decisions:
Wrap the implicit end-of-function missing-return error with a function-only
context line, matching PostgreSQL's context text without a statement line.

Files touched:
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_missing_return_reports_function_context
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-missing-return-context

Remaining:
Regression rerun reported 2194/2271 matched and 906 diff lines, but this run
has unrelated transition-table statement-timeout noise. The targeted
missing_return_expr context hunk is clear.
