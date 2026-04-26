Goal:
Reduce the upstream PostgreSQL `create_table` regression diff in early, low-risk slices, then close the partition validation and describe-output slice.

Key decisions:
Keep `unknown` catalog support as a text-shaped pseudo-type identity and reject it, plus `cstring`, where PostgreSQL rejects pseudo-type columns/attributes.
Validate invalid DEFAULT expressions during CREATE TABLE lowering before catalog creation.
Add narrow `CREATE UNLOGGED TABLE`, CTAS reloptions/IF NOT EXISTS duplicate handling, SQL PREPARE/EXECUTE/DEALLOCATE for SELECT, and CTAS EXECUTE support.
Repair savepoint rollback for catalog DDL by snapshotting MVCC-visible catalog rows and replaying catalog row deletes/inserts at ROLLBACK TO.
Add targeted error-position mapping for fixed CREATE TABLE and partition-bound diagnostics, leaving several exact caret/truncation differences for a later protocol pass.
Parse bare `CREATE TABLE ... WITH OIDS` so the syntax error points at `OIDS`, not `WITH`.
Register and execute catalog helpers needed by this regression slice: `pg_relation_filenode`, `pg_filenode_relation`, `pg_catalog.pg_get_partkeydef`, and `pg_catalog.pg_get_partition_constraintdef`.
Validate partition key expressions and bounds before catalog creation for the regression-covered missing/system/pseudo/opclass/aggregate/window/subquery/SRF/constant cases.
Serialize partition bounds and add targeted deparsing for `pg_get_expr(relpartbound, oid)`, `pg_get_partition_constraintdef(oid)`, and psql describe partition footers.
Keep psql describe shims narrow and marked as compatibility work; they now report `relispartition`, avoid false rewrite-rule lookups, hide partitioned parents from plain `Inherits:`, and sort partition children like psql.

Files touched:
.codex/task-notes/create-table-regression-diff.md
src/backend/catalog/namespace.rs
src/backend/catalog/store.rs
src/backend/commands/partition.rs
src/backend/executor/driver.rs
src/backend/executor/exec_expr.rs
src/backend/optimizer/constfold.rs
src/backend/optimizer/mod.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/analyze/create_table.rs
src/backend/parser/analyze/create_table_inherits.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/partition.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs
src/bin/query_repl.rs
src/include/catalog/bootstrap.rs
src/include/catalog/pg_opclass.rs
src/include/catalog/pg_opfamily.rs
src/include/catalog/pg_proc.rs
src/include/catalog/pg_type.rs
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
src/pgrust/database/catalog_access.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/matview.rs
src/pgrust/database/commands/sequence.rs
src/pgrust/database/commands/typecmds.rs
src/pgrust/database/txn.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet builtin_types_include_unknown
scripts/cargo_isolated.sh test --lib --quiet lower_create_table_rejects_unknown_pseudotype_columns
scripts/cargo_isolated.sh test --lib --quiet lower_create_table_rejects_invalid_default_expressions
scripts/cargo_isolated.sh test --lib --quiet create_type_rejects_pseudotype_attributes
scripts/cargo_isolated.sh test --lib --quiet create_table_as_execute_uses_prepared_select
scripts/cargo_isolated.sh test --lib --quiet create_unlogged_table_sets_catalog_persistence
scripts/cargo_isolated.sh test --lib --quiet create_table_as_if_not_exists_skips_before_planning_query
scripts/cargo_isolated.sh test --lib --quiet rollback_to_savepoint_restores_catalog_effects
scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_set_statement
scripts/cargo_isolated.sh test --lib --quiet resolve_scalar_function_uses_pg_proc_and_filters_non_scalar_rows
scripts/cargo_isolated.sh test --lib --quiet builtin_scalar_helpers_have_proc_oid_mappings
scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_create_table
scripts/cargo_isolated.sh test --lib --quiet create_table_partition_validation_matches_postgres_messages
scripts/cargo_isolated.sh test --lib --quiet partition_bound_validation_and_catalog_describe_helpers
scripts/cargo_isolated.sh test --lib --quiet psql_describe_tableinfo_query_reports_partition_without_rules
scripts/cargo_isolated.sh test --lib --quiet psql_describe_inherits_query_excludes_partitioned_parent
scripts/run_regression.sh --test create_table --results-dir /tmp/pgrust-create-table-regression-final --timeout 240 --port 55837

Remaining:
Current complete create_table regression: 232/330 queries matched, 642 diff lines; copied to `/tmp/diffs/create_table.diff`.
The fixed partition slice now covers most requested validation order/messages, bound validation, partition persistence wording, partition child catalog visibility, partition list ordering, and psql `Partition of`/constraint footers for simple list/range/hash/default cases.
Remaining prominent causes: exact LINE/caret rendering for some parser and partition-bound errors; partition key dependencies for functions/domains and DROP ... CASCADE behavior; rowtype and whole-row partition keys; default partition row revalidation before accepting overlapping partitions; hash modulus DETAIL text; inherited partition constraint/locality propagation and related ALTER TABLE DROP CONSTRAINT support; collation handling in partition definitions; expression-key bound values are serialized/displayed as text literals in some `abs(...)` range cases; user-defined operator class DDL; COMMENT/obj_description support for partitioned tables; array partition values; active-query guard for CREATE TABLE ... PARTITION OF inside triggers; volatile partition-bound expressions; partitioned-table column drop and dependent index propagation.
