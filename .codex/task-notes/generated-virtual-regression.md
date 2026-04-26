Goal:
Fix metadata-focused generated_virtual regression differences.

Key decisions:
Implemented metadata parity for the early generated_virtual catalog queries: full information_schema.columns shape, generated expression/nullability/default fields, table/view population, and information_schema.column_column_usage.

Persisted generated attrdef-to-source-column pg_depend rows and used them for column_column_usage. Added generated source-column DROP COLUMN restrict/cascade metadata behavior for same-table generated dependents.

Fixed create-table and ALTER TABLE INHERIT generated-column metadata conflicts, parent/child generated-kind details, inherited not-null constraint names, and moving/merging inherited column notices.

Remaining regression diffs are dominated by areas intentionally out of scope for this pass: generated-expression diagnostic text/carets, non-DEFAULT generated DML error text, old.* / new.* RETURNING aliases, MERGE RETURNING, generated-column view DML handling, COPY generated-column semantics, tableoid/system-column generated expressions, partitions, triggers, privileges, and planner expansion for virtual generated columns.

Files touched:
src/backend/catalog/pg_depend.rs
src/backend/parser/analyze/create_table.rs
src/backend/parser/analyze/create_table_inherits.rs
src/backend/parser/analyze/system_views.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/utils/cache/system_view_registry.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/drop_column.rs
src/pgrust/database/commands/inheritance.rs
src/pgrust/database/ddl.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh check
PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet information_schema
PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet generated_inheritance_metadata_conflicts_and_not_null_names_match_parent
PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet generated_column_source_drop_uses_dependency_metadata_for_restrict_and_cascade
PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_drop_column_statement
PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet drop_table_cascade_notice_uses_visible_search_path_name
CARGO_TARGET_DIR=/tmp/pgrust-target-buffalo-generated scripts/run_regression.sh --skip-build --port 5545 --timeout 300 --test generated_virtual

Remaining:
generated_virtual still fails 87/131 queries matched, 1462 diff lines. Latest default-harness diff copied to /tmp/diffs/generated_virtual.buffalo.default.diff.
