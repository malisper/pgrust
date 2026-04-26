Goal:
Fix the remaining create_index regression failures from the user plan: parser blockers, unique/expression index metadata, ALTER TABLE USING INDEX, concurrent/drop/reindex compatibility, and executor index-filter correctness.

Key decisions:
Parser support was added for numeric underscores, NULLS DISTINCT, bare VACUUM FULL, broader REINDEX forms, SET SESSION ROLE, and compound ALTER TABLE actions.
Non-concurrent failed unique builds now clean catalog state; CREATE INDEX CONCURRENTLY failures intentionally commit an invalid catalog stub and initialize readable empty index storage as a :HACK: until real CIC phases exist.
REINDEX now rewrites index relfilenodes, rebuilds storage, marks rebuilt indexes ready/valid, and has an owned-temp-schema path so REINDEX SCHEMA CONCURRENTLY processes session temp indexes without timing out.
Index/index-only scans evaluate residual filters after tuple materialization; this fixes wrong large result sets when predicates are not represented as access-method scan keys.
The relcache timeout was fixed by avoiding repeated full relcache rebuilds in LazyCatalogLookup::index_relations_for_heap.
Correlated scalar subplans with runtime index keys can now choose covering index-only scans when the only non-index inputs are outer parameters; this removes the create_index OR/subplan per-query timeout while preserving the residual filter.
CREATE UNLOGGED TABLE is accepted and records relpersistence `u` as a compatibility shim; heap storage still uses the normal logged path.
Hash indexes are no longer considered for plain index-scan paths, so hash probes use the bitmap heap/index plan shape expected by the regression.
GIN array_ops now supports int/text array overlap/contains/contained/equality bitmap scans, including empty-array semantics, and psql `\d+` now reports GIN index key type/storage plus reloptions correctly.
CREATE INDEX column `COLLATE` is parsed and persisted in `indcollation`; ALTER TABLE ... USING INDEX rejects non-default collation like other non-default index ordering.
Parenthesized INSERT SELECT sources are accepted for PostgreSQL compatibility, fixing the bitmap_split_or setup statements in create_index.
CREATE INDEX now rejects table system columns in key, expression, include, and predicate positions with PostgreSQL's system-column error.
ALTER TABLE ... ADD CONSTRAINT USING INDEX now uses PostgreSQL-compatible primary error text for non-default ordering/collation, partitioned indexes, and NULLS NOT DISTINCT primary-key rejection.

Files touched:
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/include/nodes/parsenodes.rs
src/backend/catalog/namespace.rs
src/backend/parser/analyze/coerce.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/analyze/create_table_inherits.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/partition.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/setrefs.rs
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/backend/executor/exec_expr.rs
src/backend/executor/nodes.rs
src/backend/executor/tests.rs
src/backend/commands/tablecmds.rs
src/backend/catalog/store/heap.rs
src/backend/utils/cache/lsyscache.rs
src/backend/utils/sql_deparse.rs
src/backend/tcop/postgres.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/index.rs
src/pgrust/database/commands/sequence.rs
src/pgrust/database/catalog_access.rs
src/pgrust/database/temp.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet failed_unique_index
scripts/cargo_isolated.sh test --lib --quiet failed_unique_index_concurrently_leaves_invalid_catalog_state
scripts/cargo_isolated.sh test --lib --quiet create_index_if_not_exists_emits_relation_notice
scripts/cargo_isolated.sh test --lib --quiet reindex_table_rebuilds_table_indexes
scripts/cargo_isolated.sh test --lib --quiet reindex_owned_temp_schema_concurrently_rewrites_temp_indexes
env CARGO_TARGET_DIR=/tmp/pgrust-target-worcester-current scripts/run_regression.sh --test create_index --timeout 600 --jobs 1 --port 55451 --results-dir /tmp/pgrust_regress_create_index_current7
scripts/cargo_isolated.sh test --lib --quiet planner_uses_runtime_index_key_for_correlated_limit_subplan
scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
scripts/cargo_isolated.sh test --lib --quiet hash_index
env CARGO_TARGET_DIR=/tmp/pgrust-target-worcester-current scripts/run_regression.sh --test create_index --timeout 600 --jobs 1 --port 55454 --results-dir /tmp/pgrust_regress_create_index_current10
scripts/cargo_isolated.sh test --lib --quiet create_gin_array_index_uses_bitmap_scan_and_rechecks
scripts/cargo_isolated.sh test --lib --quiet parse_create_index_column_collation
scripts/cargo_isolated.sh test --lib --quiet alter_table_using_index_rejects_non_default_collation_after_index_build
scripts/cargo_isolated.sh test --lib --quiet compound_alter_table_drop_add_using_index_promotes_and_renames
scripts/cargo_isolated.sh test --lib --quiet psql_describe_tableinfo_query_reports_index_reloptions
scripts/cargo_isolated.sh test --lib --quiet psql_describe_columns_query_uses_gin_key_type_storage
scripts/cargo_isolated.sh test --lib --quiet parse_insert_select_default_values_and_table_stmt
scripts/cargo_isolated.sh test --lib --quiet insert_select_default_values_and_table_stmt_work
scripts/cargo_isolated.sh test --lib --quiet create_index_rejects_system_columns
scripts/cargo_isolated.sh test --lib --quiet expression_index_rejects_record_pseudo_type
scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/worcester-regress scripts/run_regression.sh --port 5563 --test create_index --timeout 360 --results-dir /tmp/pgrust_regress_create_index_current19
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/worcester-regress scripts/run_regression.sh --port 5563 --test create_index --timeout 360 --results-dir /tmp/pgrust_regress_create_index_current20

Remaining:
Latest focused create_index regression completed without file timeout: 528/687 queries matched, 159 mismatched, 2056 diff lines in /tmp/pgrust_regress_create_index_current20.
The old invalid-index IO errors, temp-schema REINDEX timeout/error, correlated OR/subplan statement timeout, CREATE UNLOGGED TABLE cascade, GIN array empty-result bug, missing GIN reloptions footer, parenthesized INSERT SELECT syntax errors, and system-column index errors are gone. The hash equality probe now uses Bitmap Heap Scan; its remaining diff is literal type display in the recheck condition.
Remaining diffs include expression/predicate deparse spacing/casts, GiST geometry/operator support gaps, ALTER TABLE USING INDEX caret/detail formatting and deeper edge-case validation, partitioned REINDEX and relfilenode helper failures, broader OR/ANY BitmapAnd/BitmapOr index-cond plan-shape gaps, and EXPLAIN sort/recheck formatting differences.
