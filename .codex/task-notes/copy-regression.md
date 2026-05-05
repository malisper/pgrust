Goal:
Diagnose copy regression diff where partitioned COPY rows disappear after COPY FREEZE error and ROLLBACK.
Key decisions:
PostgreSQL makes transactional TRUNCATE safe by switching to a new relfilenode while preserving the old relfilenode until commit/abort. pgrust already restored the catalog relfilenode on ROLLBACK, but invalidated dirty buffers for the old relfilenode before they were durable, so rollback pointed back to an empty on-disk table. The fix flushes the old relation buffers before invalidating them during transactional TRUNCATE, and marks COPY fast-path errors as transaction failures in the server protocol.
Files touched:
src/backend/tcop/postgres.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database_tests.rs
Tests run:
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=3 scripts/cargo_isolated.sh test --lib --quiet truncate_partitioned_copy_freeze_error_rollback
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=3 scripts/run_regression.sh --test copy --timeout 120 --port 60123
Remaining:
None.
