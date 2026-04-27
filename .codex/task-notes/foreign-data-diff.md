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
- exposes minimal `pg_roles`, `pg_table_is_visible`, and `pg_get_partkeydef`
  catalog helper behavior needed by FDW describe/privilege queries
- stores and alters foreign table/column FDW options, including
  `ALTER FOREIGN TABLE ... OPTIONS`, add-column options, and column option
  changes through `pg_attribute.attfdwoptions`
- rejects unsupported primary key, unique, foreign key, and exclusion
  constraints on foreign tables before creating catalog rows
- emits missing-relation notices for `ALTER TABLE/FOREIGN TABLE IF EXISTS`
  paths that share the table lookup helpers
- lets superusers reassign foreign server owners without the target role
  needing FDW usage, and blocks `DROP ROLE` while FDW/server ownership or ACL
  dependencies still reference the role
- supports `COMMENT ON FOREIGN TABLE` through relation descriptions and emits
  missing/duplicate FDW, server, and user-mapping notices plus FDW
  handler/validator change warnings
- preserves `pg_attribute.attfdwoptions` when relation descriptors are rebuilt
  through relcache/syscache paths, and displays foreign column FDW options plus
  column comments in psql `\d`/`\d+` describe fast paths
- reports FDW handler/validator dependencies when `DROP FUNCTION` targets a
  function referenced by `pg_foreign_data_wrapper`
- checks FDW handler/validator function signatures when resolving support
  functions, including PostgreSQL-style missing-function messages such as
  `bar(text[], oid)`
- preserves foreign-data wrapper option order during `ALTER FOREIGN DATA
  WRAPPER ... OPTIONS`
- matches PostgreSQL's `CREATE USER MAPPING` error priority by resolving an
  explicit role before checking the server
- quotes `user` as a keyword through `quote_ident`, fixing psql FDW option
  rendering for user mappings

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
- `scripts/cargo_isolated.sh test --lib --quiet pg_get_partkeydef_and_pg_table_is_visible_use_catalog`
- `scripts/run_regression.sh --skip-build --port 55442 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-helper-funcs`
- `scripts/cargo_isolated.sh test --lib --quiet parse_foreign_data_wrapper_statements`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_catalogs_track_servers_mappings_and_tables`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55443 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-column-alter`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_tables_reject_unsupported_constraints`
- `scripts/run_regression.sh --skip-build --port 55444 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-foreign-constraints`
- `scripts/cargo_isolated.sh test --lib --quiet parse_foreign_data_wrapper_statements`
- `scripts/cargo_isolated.sh test --lib --quiet alter_foreign_table_if_exists_reports_missing_relation_notice`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_tables_reject_unsupported_constraints`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55445 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-alter-missing-notices`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_dependencies_block_role_drop`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_usage_controls_server_mapping_and_table_creation`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55446 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-role-deps`
- `scripts/cargo_isolated.sh test --lib --quiet comment_on_foreign_table_uses_relation_description`
- `scripts/run_regression.sh --skip-build --port 55447 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-comment-ft`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_if_exists_notices_and_alter_warnings`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55448 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-notices`
- `scripts/cargo_isolated.sh test --lib --quiet psql_describe_columns_query_reports_foreign_column_options_and_comments`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_catalogs_track_servers_mappings_and_tables`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55451 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-relcache-fdw-options`
- `scripts/cargo_isolated.sh test --lib --quiet drop_function_reports_foreign_data_wrapper_dependency`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55452 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-fdw-function-deps`
- `scripts/cargo_isolated.sh test --lib --quiet fdw_function_lookup_errors_include_expected_signature`
- `scripts/cargo_isolated.sh test --lib --quiet drop_function_reports_foreign_data_wrapper_dependency`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_if_exists_notices_and_alter_warnings`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55453 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-fdw-proc-signatures`
- `scripts/cargo_isolated.sh test --lib --quiet alter_fdw_options_preserves_existing_order`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55454 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-fdw-option-order`
- `scripts/cargo_isolated.sh test --lib --quiet create_user_mapping_reports_missing_role_before_missing_server`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55455 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-user-mapping-error-order`
- `scripts/cargo_isolated.sh test --lib --quiet concat_right_and_quote_functions_are_available_to_sql`
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55456 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results-quote-user-option`

Remaining:
`foreign_data` still fails, but improved to 399/539 matching queries and 1436
diff lines in the latest run. Biggest
remaining groups:
- owner dependency reporting beyond the handled FDW function dependencies
- full `IMPORT FOREIGN SCHEMA` callback behavior beyond missing-handler errors
- foreign table DDL/partition forms and partition catalog state
- psql describe output compatibility beyond the helper-function lookups
- Notices and exact PostgreSQL error/caret wording
