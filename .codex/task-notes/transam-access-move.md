Goal:
Move transam/WAL/checkpoint/control-file core from root `pgrust` into `pgrust_access`
while keeping root compatibility paths and root-owned AM redo callbacks.

Key decisions:
- `pgrust_access::transam` now owns xact, CLOG, xlog, xloginsert, xlogreader,
  xlogrecovery, controlfile, and checkpoint modules.
- Root `src/backend/access/transam/*` files are compatibility shims.
- Recovery in `pgrust_access` accepts `AccessRedoServices`; root installs AM redo
  callbacks until btree/GiST/GIN/hash redo modules move.
- SQL-facing checkpoint display helpers stay in root; checkpoint config/stats
  types moved to access and are re-exported.

Files touched:
- `crates/pgrust_access/src/transam/*`
- `crates/pgrust_access/src/crc32c.rs`
- `src/backend/access/transam/*`
- `src/backend/utils/misc/checkpoint.rs`
- `crates/pgrust_access/Cargo.toml`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check -p pgrust_access --message-format short`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh check --features lz4 --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet xact`
- `scripts/cargo_isolated.sh test --lib --quiet clog`
- `scripts/cargo_isolated.sh test --lib --quiet xlog`
- `scripts/cargo_isolated.sh test --lib --quiet recovery`
- `scripts/cargo_isolated.sh test --lib --quiet checkpoint`
- `scripts/cargo_isolated.sh test --lib --quiet durable_prepared_transaction_survives_reopen_then_finishes`
- `scripts/cargo_isolated.sh test --lib --quiet create_database_clones_template1_and_persists_across_reopen`
- `scripts/cargo_isolated.sh test --lib --quiet heap`
- `scripts/cargo_isolated.sh test --lib --quiet toast`
- `scripts/cargo_isolated.sh test --lib --quiet index`
- `scripts/cargo_isolated.sh test --lib --quiet concurrent_indexed_updates_and_deletes_keep_index_results_correct`
- `scripts/cargo_isolated.sh test --lib --quiet catalog`

Remaining:
- Full `index` filter timed out once in
  `concurrent_indexed_updates_and_deletes_keep_index_results_correct`; the same
  test passed when rerun alone.
- BRIN runtime and AM redo modules remain root-owned.
- `pgrust_access::transam::xlog::wal_segment_path_for_lsn` is test-only and still
  warns as dead code in non-test access builds.
