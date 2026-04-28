Goal:
Close publication regression parity gaps after sequence/EXCEPT and pg_publication_tables support.

Key decisions:
Added replica identity AST/executor/catalog support for DEFAULT/FULL/NOTHING/USING INDEX and wired publication UPDATE/DELETE enforcement through static and row-time checks. Kept the PostgreSQL `WHERE false` no-op exception by skipping static missing-RI checks for constant-false predicates.
Tightened publication validation for duplicate filters/column lists, system and virtual generated columns in column lists, user-defined row filter types, schema/column-list mixes, view publication errors, virtual generated user functions, virtual generated SET EXPRESSION on published tables, and DROP COLUMN dependencies on publication column lists/filters.
Fixed pg_publication_rel membership replacement by deleting all old dependency rows and keying pg_publication/pg_publication_rel/pg_publication_namespace persistence by OID.

Files touched:
.codex/task-notes/publication-regression.md
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/generated.rs
src/backend/catalog/persistence.rs
src/backend/catalog/pg_depend.rs
src/backend/catalog/store/heap.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/commands/alter_column_default.rs
src/pgrust/database/commands/drop_column.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/publication.rs
src/pgrust/database/ddl.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet virtual_generated
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication_column_list_blocks_drop_column
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication_rejects_views_with_publication_error
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication_column_list_rejects_schema_publication_mix
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication_update_requires_replica_identity_even_without_rows
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test publication --timeout 120 --jobs 1 --port 55441 --results-dir /tmp/pgrust-publication-validation-55441 failed: 629/710 queries matched, 745 diff lines.

Remaining:
Largest remaining clusters are partitioned UPDATE support/order of publication checks, psql describe queries that still hit unsupported internal-language function execution, CREATE/DROP COLLATION plus user collation row-filter validation, CURRENT_SCHEMA/schema-name behavior, ON CONFLICT on partitioned tables, MERGE RI checks, database/owner permission text drift, and parser caret/text drift around schema WHERE and invalid object-list errors.
