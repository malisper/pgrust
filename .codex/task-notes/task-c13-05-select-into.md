Goal:
Fix TASK-C13-05 for the select_into regression: SELECT INTO command shape, CTAS/SELECT INTO output, CTAS column metadata, EXPLAIN CTAS EXECUTE, disallowed SELECT INTO errors, and the default-privilege owner INSERT denial case.

Key decisions:
Lower top-level SELECT INTO under EXPLAIN to CreateTableAs, matching the existing top-level SELECT INTO lowering. Resolve prepared EXECUTE inside EXPLAIN CREATE TABLE AS in the session layer before database execution. Validate CTAS column aliases before table creation. Preserve explicit relation ACLs for owners when default privileges create an ACL that omits INSERT.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/include via parser tests only: src/backend/parser/tests.rs
src/backend/catalog/object_address.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust/manado-v1 PGRUST_TARGET_SLOT=0 scripts/cargo_isolated.sh check
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust/manado-v1 PGRUST_TARGET_SLOT=0 scripts/cargo_isolated.sh test --lib --quiet explain_analyze_create_table_as_execute_uses_prepared_select
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust/manado-v1 PGRUST_TARGET_SLOT=0 scripts/cargo_isolated.sh test --lib --quiet create_table_as_rejects_too_many_column_aliases_before_create
env -u CARGO_TARGET_DIR scripts/run_regression.sh --test select_into --port 62123 --results-dir /tmp/pgrust-task-c13-05-select-into

Remaining:
select_into passes 70/70. The ALTER DEFAULT PRIVILEGES path is still a narrow compatibility shim for table ACL items; full pg_default_acl-backed default-privilege state remains future work.
