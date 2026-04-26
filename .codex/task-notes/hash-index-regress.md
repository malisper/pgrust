Goal:
Investigate and fix hash_index regression diffs for unique expression index build, ALTER INDEX SET fillfactor, and hash fillfactor range errors.

Key decisions:
Removed the stale manual expression-index build path for btree/hash so AM builds use IndexBuildKeyProjector consistently.
Added a narrow ALTER INDEX ... SET (...) parser/executor no-op shim with a :HACK: comment, matching existing ALTER TABLE SET behavior.
Changed hash fillfactor range validation to return PostgreSQL-shaped message/detail.
Added the missing PostgreSQL textcat(text,text) builtin/catalog row so create_index base setup can build functional indexes on textcat(f1,f2).
Added PostgreSQL abs overloads for int/float/numeric so expression index column types match execution, especially abs(float8).
Adjusted UPDATE index maintenance so unchanged unique keys do not self-conflict, while non-unique indexes still receive a new entry for the updated heap TID.

Files touched:
src/pgrust/database/commands/index.rs
src/backend/parser/gram.rs
src/include/nodes/parsenodes.rs
src/backend/executor/driver.rs
src/pgrust/database/commands/execute.rs
src/pgrust/session.rs
src/bin/query_repl.rs
src/backend/parser/tests.rs
src/pgrust/database_tests.rs
src/backend/parser/analyze/functions.rs
src/backend/executor/exec_expr.rs
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs
src/include/access/amapi.rs
src/backend/access/index/unique.rs
src/backend/access/heap/heaptoast.rs
src/backend/access/brin/brin.rs
src/backend/catalog/indexing.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/pgrust/database/commands/alter_column_type.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet parse_alter_index_set_statement
scripts/cargo_isolated.sh test --lib --quiet unique_expression_index_build_accepts_distinct_existing_rows
scripts/cargo_isolated.sh test --lib --quiet hash_index_fillfactor_out_of_range_uses_postgres_error_shape
scripts/cargo_isolated.sh test --lib --quiet functional_textcat_index_builds_and_enforces_uniqueness
scripts/cargo_isolated.sh test --lib --quiet unique_expression_index_update_allows_unchanged_key
scripts/cargo_isolated.sh test --lib --quiet unique_abs_float_index_update_allows_new_distinct_key
scripts/run_regression.sh --test hash_index --timeout 60
scripts/run_regression.sh --test hash_index --timeout 60 --ignore-deps
CARGO_TARGET_DIR=/tmp/pgrust-target-manila-hash scripts/run_regression.sh --test hash_index --timeout 60
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/manila-v3/2 scripts/run_regression.sh --test hash_index --timeout 60
CARGO_TARGET_DIR=/tmp/pgrust-target-manila-hash scripts/run_regression.sh --test hash_index --timeout 180 --port 55436 --jobs 1
git diff --check

Remaining:
The first regression run did not reach hash_index because post_create_index base setup failed on textcat(f1,f2); the missing textcat builtin has been fixed and covered by a focused test.
Default parallel isolated regression still attempts to build post_create_index from create_index first; that setup is slow/fails in unrelated later create_index/reindex sections. Direct hash_index validation with --jobs 1 passes 100/100 queries.
