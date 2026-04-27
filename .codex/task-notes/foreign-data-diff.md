Goal:
Add FDW support slices needed by the `foreign_data` regression.

Key decisions:
Implemented the real catalog-backed path for foreign servers, user mappings, and
foreign tables instead of more output rewrites. Kept narrow `:HACK:` parser
lowerings for `ALTER/DROP/COMMENT ON FOREIGN TABLE` where existing table
machinery can be reused. `DROP FOREIGN TABLE` lowers to a flagged table drop so
ordinary `DROP TABLE` still performs relkind checks.

Files touched:
Catalog descriptors/bootstrap/indexing/cache/storage for:
- `pg_foreign_server`
- `pg_user_mapping`
- `pg_foreign_table`

Parser/AST/command routing for:
- `CREATE/ALTER/DROP SERVER`
- `CREATE/ALTER/DROP USER MAPPING`
- `CREATE FOREIGN TABLE ... SERVER ... OPTIONS`
- flagged `DROP FOREIGN TABLE`

Runtime behavior:
- persists foreign server/table/user mapping rows
- validates `postgresql_fdw_validator` server and user-mapping options
- cascades FDW/server drops through dependent user mappings and foreign tables
- deletes `pg_foreign_table` rows when the relation is dropped
- expands FDW option arrays through `pg_options_to_table`
- exposes `pg_user_mappings` and information_schema FDW views
- supports `COMMENT ON SERVER`
- stores `GRANT/REVOKE USAGE` ACLs for foreign data wrappers and servers

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet parse_foreign_data_wrapper_statements`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_catalogs_track_servers_mappings_and_tables`
- `scripts/cargo_isolated.sh test --lib --quiet copy_freeze_rejects_foreign_tables`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55434 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results`
- `scripts/cargo_isolated.sh test --lib --quiet pg_options_to_table_expands_foreign_data_options`
- `scripts/cargo_isolated.sh test --lib --quiet pg_user_mappings_view_reports_servers_users_and_visible_options`
- `scripts/cargo_isolated.sh test --lib --quiet information_schema_foreign_data_views_report_catalog_rows`
- `scripts/cargo_isolated.sh test --lib --quiet comment_on_server_uses_pg_description_rows`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_usage_grant_revoke_updates_acl_views`
- `scripts/cargo_isolated.sh test --lib --quiet parse_grant_usage_on_foreign`
- `scripts/cargo_isolated.sh test --lib --quiet parse_revoke_all_on_foreign_data_wrapper_statement`
- `scripts/run_regression.sh --skip-build --port 55436 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-fdw-acl`
- `scripts/cargo_isolated.sh test --lib --quiet concat_right_and_quote_functions_are_available_to_sql`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_usage_controls_server_mapping_and_table_creation`
- `scripts/run_regression.sh --skip-build --port 55439 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-priv-funcs`
- `scripts/cargo_isolated.sh test --lib --quiet import_foreign_schema_requires_fdw_handler`
- `scripts/run_regression.sh --skip-build --port 55440 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-import`
- `scripts/run_regression.sh --skip-build --port 55441 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-pg-roles`

Remaining:
`foreign_data` still fails, but improved to 284/539 matching queries. Biggest
remaining groups:
- `pg_catalog.pg_table_is_visible` and `pg_catalog.pg_get_partkeydef`
- FDW dependency reporting for handler functions and owners
- full `IMPORT FOREIGN SCHEMA` callback behavior beyond missing-handler errors
- foreign table DDL/partition forms and psql helper functions
- Notices and exact PostgreSQL error/caret wording
