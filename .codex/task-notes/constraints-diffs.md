Goal:
Fix constraints regression diffs around invalid NOT NULL table rewrite and COMMENT ON domain constraint ownership.

Key decisions:
Preserve nulls for unvalidated NOT NULL constraints when rewriting rows for added volatile/serial defaults.
Check domain type ownership before COMMENT ON CONSTRAINT ON DOMAIN writes pg_description.

Files touched:
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/ddl.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_reads_old_rows_with_null_or_default
scripts/cargo_isolated.sh test --lib --quiet comment_on_domain_constraint_requires_type_owner
scripts/run_regression.sh --test constraints --jobs 1 --timeout 300 --port 61237 --skip-build

Remaining:
None.
