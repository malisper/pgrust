Goal:
Fix qualified `regclass` lookup errors so missing schemas report `schema "..." does not exist`.

Key decisions:
Use schema-aware lookup errors for runtime text-to-regclass paths and literal regclass binding. Keep missing relations as relation errors when the schema exists or the name is unqualified.

Files touched:
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_reg.rs
src/backend/parser/analyze/expr.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet regclass_cast_reports_missing_schema_for_qualified_name
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet regclass_cast
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet plpgsql
PGRUST_STATEMENT_TIMEOUT=10 CARGO_INCREMENTAL=0 scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-regclass-schema-10s

Remaining:
The target `error2('nonexistent.stuffs')` hunk is fixed. The 10s regression summary was contaminated by transition-table statement timeouts and reported 2236/2271, 403 diff lines instead of a clean semantic count.
