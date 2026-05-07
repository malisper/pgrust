Goal:
Reduce remaining PostgreSQL `plpgsql` regression diffs after the anyarray fix.

Key decisions:
PostgreSQL behavior is the reference for PL/pgSQL compile/runtime context, composite return coercion, `regclass` lookup, forced parallel EXPLAIN, and SQL-function cast contexts.
Create-time PL/pgSQL compile contexts should be SQL-visible `ExecError::WithContext`, not only `ParseError::WithContext`.
`regclass` missing qualified schema must be handled in both analyzer literal folding and executor runtime casts.
PL/pgSQL return coercion must use catalog SQL cast functions when the assignment cast is function-backed.

Files touched:
crates/pgrust_analyze/src/expr.rs
crates/pgrust_commands/src/explain_verbose.rs
crates/pgrust_expr/src/backend/executor/expr_reg.rs
crates/pgrust_optimizer/src/setrefs.rs
crates/pgrust_plpgsql/src/compiled.rs
crates/pgrust_plpgsql/src/lib.rs
src/backend/executor/mod.rs
src/backend/tcop/postgres.rs
src/pgrust/database/commands/create.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/mod.rs

Tests run:
cargo fmt
git diff --check
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo check
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet regclass_cast_reports_missing_schema_for_qualified_name
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_named_composite_return_row_requires_exact_field_types
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_return_expression_errors_include_expression_context
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_raise
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_where_current_of_updates_cursor_row
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_raise_placeholder_mismatch_fails_at_create_time
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" cargo test --lib --quiet plpgsql_return_assignment_cast_preserves_sql_function_context
PGRUST_STATEMENT_TIMEOUT=10 CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-buffalo-anyarray" scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 180 --port 55519 --results-dir "/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-plpgsql-after-gather-output"

Remaining:
Latest `plpgsql` regression: 2256/2271 matched, 200 diff lines. Latest copied diff is `/tmp/diffs/plpgsql.diff`.
First remaining hunk is missing nonstandard-string warnings in PL/pgSQL function creation/runtime. Other remaining gaps include PL/pgSQL internal-query caret formatting, declaration initializer binding order, conflict-test runtime context, FOREACH over composite arrays, lateral EXPLAIN formatting, and transition table named tuplestore support.
