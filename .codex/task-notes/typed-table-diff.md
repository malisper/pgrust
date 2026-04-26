Goal:
Implement typed-table support needed by the `typed_table` regression diffs from `.context/attachments/pasted_text_2026-04-26_08-27-09.txt`.

Key decisions:
Added `pg_class.reloftype`/`of_type_oid` metadata through catalog state, relcache, temp relations, dependencies, and psql describe shims.
Parser/AST now accepts `CREATE TABLE ... OF`, typed column options, `ALTER TABLE ... OF/NOT OF`, and composite `ALTER TYPE ... ATTRIBUTE` actions.
`CREATE TABLE OF` expands visible columns from standalone composite types and applies options by matching existing attribute names.
Typed-table DDL guards reject column mutation, inheritance, and partition attachment.
`DROP TYPE ... CASCADE` now drops typed-table/function dependents for the regression path, and SQL-function inlining can pass typed-table rows as `ROW(...)::composite_type`.

Files touched:
Catalog/cache metadata, parser grammar/AST/tests, create-table lowering, typed-table command execution, DDL guard modules, type command cascade handling, SQL-function literal rendering, psql describe/error-position shims, and database tests.

Tests run:
`scripts/cargo_isolated.sh check` with `PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-muscat-typed PGRUST_TARGET_SLOT=0`
`scripts/cargo_isolated.sh test --lib --quiet parse_create_table_of_with_typed_column_options`
`scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_of_and_not_of`
`scripts/cargo_isolated.sh test --lib --quiet parse_alter_type_composite_attribute_actions`
`scripts/cargo_isolated.sh test --lib --quiet typed_table_create_alter_of_and_composite_attribute_cascade`
`scripts/cargo_isolated.sh test --lib --quiet typed_table_rows_coerce_to_composite_type_and_drop_type_cascades`
`CARGO_TARGET_DIR=/tmp/pgrust-target-regress-muscat-typed scripts/run_regression.sh --test typed_table --jobs 1 --timeout 120 --port 55459`
`git diff --check`

Remaining:
`typed_table` passes. Full `alter_table` was not run locally because it is broad and has many unrelated surfaces.
Composite `ALTER TYPE ... ATTRIBUTE ... CASCADE` updates descriptors for typed tables; full PostgreSQL storage rewrites and expression-field dependency tracking are still broader follow-up work.
