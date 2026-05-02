Goal:
Implement PostgreSQL-compatible system columns for combocid coverage, especially cmin and ctid behavior in /tmp/pgrust-regression-diffs-2026-05-01T2044Z/combocid.diff.

Key decisions:
Added cmin/cmax system-column binding and raw command-id evaluation. Kept ctid on the existing tuple-id path.
Separated heap-visible command ids from pgrust's internal catalog command counter so cmin/cmax match PostgreSQL command-counter behavior.
Kept catalog scans on the internal command counter and apply heap command ids only for non-bootstrap heap storage, so same-transaction DDL catalog rows stay visible.
Threaded heap command ids through CREATE TABLE AS / materialized view heap writes, including PL/pgSQL SPI execution.
Added local combo-cid forward and reverse maps so heap visibility can decode cmin/cmax pairs while a transaction is active.
Updated heap visibility and unique-index probing to treat parent/subtransaction xids in the current snapshot as own transactions.
Skipped add-column domain-default validation for non-domain columns so serial backfill does not evaluate nextval during validation.

Files touched:
src/include/nodes/primnodes.rs
src/include/nodes/execnodes.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/constraints.rs
src/backend/executor/exec_expr.rs
src/backend/executor/nodes.rs
src/backend/commands/explain.rs
src/backend/commands/tablecmds.rs
src/backend/access/heap/heapam.rs
src/backend/access/heap/heapam_visibility.rs
src/backend/access/index/unique.rs
src/backend/access/transam/xact.rs
src/backend/utils/time/snapmgr.rs
src/include/catalog/bootstrap.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/matview.rs
src/pgrust/database/commands/typecmds.rs
src/pgrust/session.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh check
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh build --bin pgrust_server
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/run_regression.sh --test combocid --timeout 120 --port 57900 --skip-build --results-dir /tmp/pgrust_regress_combocid_cmin
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet create_table_as_is_visible_in_same_txn_before_commit
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet materialized_view_with_no_data_refreshes_and_rejects_writes
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet plpgsql_create_materialized_view_executes_spi_statement
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet alter_reloptions_hold_pg_compatible_relation_locks
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_serial_backfills_existing_rows_and_keeps_sequence_advancing
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet drop_table_is_transactional
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet relation_rename_accepts_alter_table_and_index_object_kind_mismatch
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet create_table_as_toasted_relation_is_visible_before_commit
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/run_regression.sh --test combocid --timeout 120 --port 57900 --skip-build --results-dir /tmp/pgrust_regress_combocid_ci_fix

Remaining:
None for combocid / attached CI failures; focused regression passes all 62 queries.
