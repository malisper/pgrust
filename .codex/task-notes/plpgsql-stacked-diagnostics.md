Goal:
Close PL/pgSQL stacked diagnostics diffs around exception message, SQLSTATE, detail, hint, context, and object-name fields.

Key decisions:
Added an executor DiagnosticError variant for PostgreSQL-style error object fields without changing existing DetailedError call sites.
Stored exception context and diagnostic fields in PL/pgSQL handler state.
Filled GET STACKED DIAGNOSTICS items and rejected stacked diagnostics outside exception handlers.
Added SQL statement context around PERFORM errors so PG_EXCEPTION_CONTEXT includes the SQL frame.

Files touched:
src/backend/executor/mod.rs
src/backend/executor/expr_casts.rs
src/backend/libpq/pqformat.rs
src/backend/tcop/postgres.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-stacked-diagnostics-rerun

Remaining:
Clean rerun baseline is 2165/2271 matched, 1263 diff lines.
The invalid /tmp/diffs/plpgsql-stacked-diagnostics run had transient early index/catalog failures and should not be used as baseline.
Remaining nearby diagnostics issue: SQLSTATE condition handler case still falls through to division_by_zero instead of raising substitute message.
