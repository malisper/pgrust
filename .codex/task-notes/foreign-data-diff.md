Goal:
Add the first FDW support slice needed by the `foreign_data` regression.

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

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet parse_foreign_data_wrapper_statements`
- `scripts/cargo_isolated.sh test --lib --quiet foreign_data_catalogs_track_servers_mappings_and_tables`
- `scripts/cargo_isolated.sh test --lib --quiet copy_freeze_rejects_foreign_tables`
- `CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server`
- `scripts/run_regression.sh --skip-build --port 55434 --test foreign_data --jobs 1 --timeout 240 --results-dir /tmp/pgrust-foreign-data-results`

Remaining:
`foreign_data` still fails, but improved to 240/539 matching queries. Biggest
remaining groups:
- `pg_options_to_table` set-returning function for `\dew+`, `\des+`, `\deu+`
- `pg_user_mappings` and information_schema FDW views
- `GRANT/REVOKE USAGE ON FOREIGN DATA WRAPPER/SERVER` ACL behavior
- `COMMENT ON SERVER`
- FDW dependency reporting for handler functions and owners
- `IMPORT FOREIGN SCHEMA`
- Notices and exact PostgreSQL error/caret wording
