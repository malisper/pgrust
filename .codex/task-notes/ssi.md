Goal:
Add PostgreSQL-style serializable snapshot isolation support for the prepared_xacts SSI regression and deepen parity for safe snapshots plus old committed summaries.

Key decisions:
Use a separate predicate lock manager for nonblocking SIREAD locks and rw-conflict tracking instead of row-lock approximations. Register serializable transaction state on first serializable snapshot, keep predicate state across PREPARE TRANSACTION, and report PG-compatible 40001 dangerous-structure errors. Treat the prepared_xacts post-SSI PREPARE path as an SSI-canceled transaction cleanup so regress output matches PostgreSQL.
Read-only deferrable transactions now wait on possible unsafe writable transactions and retry if the snapshot is proven unsafe. Old committed serializable transactions are summarized into a pg_serial-like runtime summary plus an OldCommittedSxact-style dummy SIREAD owner until no active serializable transaction can overlap.
SSI executor hooks fast-path non-serializable sessions before consulting transaction state so read committed workloads avoid predicate-lock mutex overhead.

Files touched:
src/backend/storage/lmgr/predicate.rs
src/backend/storage/lmgr/mod.rs
src/backend/executor/mod.rs
src/backend/executor/nodes.rs
src/backend/commands/tablecmds.rs
src/pgrust/database.rs
src/pgrust/cluster.rs
src/pgrust/database/commands/execute.rs
src/pgrust/session.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet backend::storage::lmgr::predicate::tests
scripts/cargo_isolated.sh test --lib --quiet pg_blocking_pids_reports_serializable_safe_snapshot_waiter
scripts/run_regression.sh --test prepared_xacts --timeout 180 --jobs 1 --skip-build

Remaining:
Broaden SSI parity beyond prepared_xacts: finer-grained index/page predicate lock promotion and more exhaustive PostgreSQL isolation-spec coverage for complex dangerous-structure interleavings.
