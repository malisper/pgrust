Goal:
Fix GitHub regression failures for password, reindex_catalog, and lock without changing expected files.

Key decisions:
Downloaded aggregate artifact from GitHub Actions run 25182432089 into /tmp/pgrust-regression-25182432089 and analyzed the focused diff/output/status files. Wrote the summary report to /tmp/pgrust-regression-failures-25182432089.md.
Implemented PostgreSQL-compatible role password normalization for the regression surface, including real SCRAM-SHA-256 generation, accepted MD5/SCRAM pre-encrypted secrets, role rename MD5 invalidation, password GUC validation, and notice/detail/hint delivery.
Resolved bootstrapped system catalog tables and indexes through lookup_any_relation and rebuilt system catalog indexes through the bootstrap index path, including pg_shdescription_o_c_index.
Extended LOCK parsing/execution for ONLY/* targets, inheritance recursion, view dependency recursion, security-invoker checks, ALTER ROLE ... SET search_path compatibility, and test_atomic_ops().
Fixed transaction table-lock cleanup to release all repeated lock counts for the backend at COMMIT/ROLLBACK/abort paths.

Files touched:
Cargo.toml
Cargo.lock
crates/pgrust_sql_grammar/src/gram.pest
src/backend/catalog/indexing.rs
src/backend/catalog/roles.rs
src/backend/commands/rolecmds.rs
src/backend/executor/exec_expr.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs
src/backend/utils/cache/lsyscache.rs
src/include/catalog/indexing.rs
src/include/catalog/pg_proc.rs
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/index.rs
src/pgrust/database/commands/role.rs
src/pgrust/session.rs
.codex/task-notes/regression-password-reindex-lock.md

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/bismarck/53 PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=53 scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/bismarck/53 PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=53 scripts/cargo_isolated.sh test --lib --quiet parse_lock_table_statement_modes
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/bismarck/53 PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=53 scripts/cargo_isolated.sh test --lib --quiet parse_alter_role_set_config_statement
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/bismarck/53 PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=53 scripts/run_regression.sh --test password --jobs 1 --timeout 180 --results-dir /tmp/diffs/bismarck-password
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/bismarck/53 PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=53 scripts/run_regression.sh --test reindex_catalog --jobs 1 --timeout 180 --results-dir /tmp/diffs/bismarck-reindex_catalog
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/bismarck/53 PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=53 scripts/run_regression.sh --test lock --jobs 1 --timeout 180 --results-dir /tmp/diffs/bismarck-lock

Remaining:
All three requested regression files pass locally. Remaining risk is limited to broader suite interactions outside the requested focused files.
