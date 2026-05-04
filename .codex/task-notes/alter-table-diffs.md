Goal:
Implement the first PostgreSQL-aligned stored-expression deparse pass for defaults,
CHECK constraints, pg_get_expr, pg_get_constraintdef, psql describe, and
information_schema surfaces that show up in ALTER TABLE diffs.
Key decisions:
Canonicalize new stored defaults and CHECK constraints by binding and rendering
Expr trees instead of preserving raw SQL. Keep legacy normalization as fallback.
Do not do the full Expr::Cast metadata refactor in this slice; that remains a
larger cross-crate change.
For ADD COLUMN with a default that rewrites existing tuples, validate pending
NOT NULL against the rewritten tuple, matching PostgreSQL's ATRewriteTable
ordering. Skip the later old-row scan when that rewrite already validated rows.
Files touched:
crates/pgrust_analyze/src/create_table.rs
crates/pgrust_rewrite/src/views.rs
src/backend/executor/exec_expr.rs
src/backend/tcop/postgres.rs
src/pgrust/database/commands/alter_column_type.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/ddl.rs
src/pgrust/database_tests.rs
Tests run:
scripts/cargo_isolated.sh test --lib --quiet stored_expression_deparse_matches_pg_for_checks_and_defaults
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_reads_old_rows_with_null_or_default
scripts/run_regression.sh --test alter_table
scripts/run_regression.sh --port 55433 --test alter_table
scripts/run_regression.sh --port 56433 --test alter_table
scripts/run_regression.sh --port 57433 --test alter_table
Remaining:
alter_table still fails for unrelated existing semantic/catalog diffs. The
default random()::integer hunk is fixed. CHECK casts after ALTER COLUMN TYPE now
rewrite conbin for simple comparison constraints. Remaining visible
alter_table diffs are unrelated inheritance, domain cascade, storage, locking,
and view/explain differences.
The atacc1 volatile-default ADD COLUMN NOT NULL error is fixed; latest
alter_table run is 1577/1678 matched with 892 diff lines, and the diff was
copied to /tmp/diffs/alter_table.diff.
