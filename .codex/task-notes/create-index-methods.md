Goal:
Fix the remaining create_index regression failures from the user plan: parser blockers, unique/expression index metadata, ALTER TABLE USING INDEX, concurrent/drop/reindex compatibility, and executor index-filter correctness.

Key decisions:
Parser support was added for numeric underscores, NULLS DISTINCT, bare VACUUM FULL, broader REINDEX forms, SET SESSION ROLE, and compound ALTER TABLE actions.
Non-concurrent failed unique builds now clean catalog state; CREATE INDEX CONCURRENTLY failures intentionally commit an invalid catalog stub and initialize readable empty index storage as a :HACK: until real CIC phases exist.
REINDEX now rewrites index relfilenodes, rebuilds storage, marks rebuilt indexes ready/valid, and has an owned-temp-schema path so REINDEX SCHEMA CONCURRENTLY processes session temp indexes without timing out.
Index/index-only scans evaluate residual filters after tuple materialization; this fixes wrong large result sets when predicates are not represented as access-method scan keys.
The relcache timeout was fixed by avoiding repeated full relcache rebuilds in LazyCatalogLookup::index_relations_for_heap.

Files touched:
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/include/nodes/parsenodes.rs
src/backend/parser/analyze/coerce.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/functions.rs
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/backend/executor/exec_expr.rs
src/backend/executor/nodes.rs
src/backend/commands/tablecmds.rs
src/backend/catalog/store/heap.rs
src/backend/utils/cache/lsyscache.rs
src/backend/tcop/postgres.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/index.rs
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

Remaining:
Latest focused create_index regression completed without file timeout: 506/687 queries matched, 181 mismatched, 2243 diff lines in /tmp/pgrust_regress_create_index_current7.
The old invalid-index IO errors and temp-schema REINDEX timeout/error are gone. Failed concurrent unique indexes remain invalid after the first failed REINDEX and become valid after deleting duplicates plus successful REINDEX. After that regression run, REINDEX duplicate-key errors were remapped to "could not create unique index ... is duplicated" wording and focused tests/check passed, but the full regression was not rerun again.
Remaining diffs include expression/predicate deparse spacing/casts, GiST/GIN/hash/UNLOGGED gaps, ALTER TABLE USING INDEX edge-case wording/validation, partitioned REINDEX and relfilenode helper failures, a correlated-subquery per-query timeout in the OR/subplan planner area, and broader OR/ANY bitmap/index-cond plan-shape gaps.
