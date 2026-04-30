Goal:
Implement PostgreSQL-shaped durable 2PC for prepared_xacts and isolate any non-2PC regression gaps.

Key decisions:
PREPARE TRANSACTION, COMMIT PREPARED, and ROLLBACK PREPARED are transaction-control statements, not SQL PREPARE statements. Prepared xacts live in a global registry and durable pg_twophase files keyed by xid. Prepared transaction locks move to a pseudo client owner and are restored on reopen. Table locks now keep per-holder acquisition counts so a command-level unlock cannot release a transaction-level lock. SELECT FOR UPDATE/SHARE NOWAIT carries a PostgreSQL-style wait policy through row marks and returns SQLSTATE 55P03.

Files touched:
Parser/AST, session transaction routing, prepared xact registry/recovery/WAL, pg_prepared_xacts SRF/GUC handling, table/row/advisory lock transfer/restore, serialization derives, and focused database/parser tests.

Tests run:
TMPDIR="/Volumes/OSCOO PSSD/tmp-austin-v5" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/austin-v5-target" CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=cc RUSTC_WRAPPER= cargo check --lib
TMPDIR="/Volumes/OSCOO PSSD/tmp-austin-v5" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/austin-v5-target" CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=cc RUSTC_WRAPPER= cargo test --lib --quiet parse_select_for_update_nowait_clause
TMPDIR="/Volumes/OSCOO PSSD/tmp-austin-v5" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/austin-v5-target" CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=cc RUSTC_WRAPPER= cargo test --lib --quiet repeated_holder_lock_requires_matching_unlocks
TMPDIR="/Volumes/OSCOO PSSD/tmp-austin-v5" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/austin-v5-target" CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=cc RUSTC_WRAPPER= cargo test --lib --quiet prepared_transaction_
TMPDIR="/Volumes/OSCOO PSSD/tmp-austin-v5" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/austin-v5-target" CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=cc RUSTC_WRAPPER= cargo build --bin pgrust_server
TMPDIR="/Volumes/OSCOO PSSD/tmp-austin-v5" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/austin-v5-target" CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=cc RUSTC_WRAPPER= scripts/run_regression.sh --test prepared_xacts --skip-build --port 25436 --results-dir /tmp/diffs/prepared_xacts-2pc-built7

Remaining:
prepared_xacts is at 88/96 matched. The remaining diff is isolated to PostgreSQL SSI/predicate locking: the serializable write-skew INSERT should fail with SQLSTATE 40001, but pgrust allows it and leaves regress_foo5 prepared, causing downstream gid/count and DROP TABLE cleanup differences.
