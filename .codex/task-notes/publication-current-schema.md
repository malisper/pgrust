Goal:
Fix publication regression differences around CURRENT_SCHEMA, quoted schema names, and schema-qualified quoted table names.

Key decisions:
Preserve quoted schema/table identifiers through CREATE SCHEMA and CREATE TABLE creation paths.
Resolve publication CURRENT_SCHEMA from the non-temp effective search path and report the PostgreSQL CURRENT_SCHEMA-specific empty-path error.
Keep FOR TABLE CURRENT_SCHEMA as a syntax error only when TABLE is explicit; bare continuation CURRENT_SCHEMA still reports invalid table name.
Try exact catalog namespace/relation syscache keys before folded keys for quoted-name lookup compatibility.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/commands/schemacmds.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/utils/cache/lsyscache.rs
src/pgrust/database/catalog_access.rs
src/pgrust/database/commands/publication.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_publication_current_schema_depends_on_target_mode
scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
scripts/cargo_isolated.sh test --lib --quiet publication_parser_reports_invalid_mixed_object_names
scripts/cargo_isolated.sh test --lib --quiet current_schema_publications_preserve_quoted_schema_and_table_names
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test publication --timeout 60 --results-dir /tmp/diffs/publication-current-schema

Remaining:
publication regression still times out before reaching the CURRENT_SCHEMA block in the full file; use focused unit coverage or a shorter harness slice for this block.
