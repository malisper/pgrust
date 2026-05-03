Goal:
Move heap/table/TOAST runtime toward `pgrust_access`.

Key decisions:
- Moved heap scan/write/fetch/update/delete, MVCC visibility, visibility map,
  pruning, and vacuum into `pgrust_access::heap`.
- Added `AccessTransactionServices` snapshot/combo-cid hooks so heap code no
  longer imports the root `TransactionManager`.
- Kept root heap modules as `:HACK:` compatibility shims and waiter adapters.
- Moved TOAST tuple compression/externalization planning into
  `pgrust_access::table::toast_helper`; root supplies the callback that stores
  external chunks and updates the TOAST index.
- Moved portable TOAST detoast reconstruction, chunk tuple parsing, chunk row
  construction, pointer extraction, and relation descriptor helpers into
  `pgrust_access`; root now only scans/inserts/deletes heap chunks and updates
  the TOAST index through compatibility adapters.
- Added a default TOAST chunk fetch hook to `AccessToastServices` for future
  service-backed detoast call sites.

Files touched:
- `crates/pgrust_access/src/heap/*`
- `crates/pgrust_access/src/table/*`
- `crates/pgrust_access/src/services.rs`
- `crates/pgrust_core/src/transam.rs`
- `src/backend/access/heap/*`
- `src/backend/access/common/detoast.rs`
- `src/backend/access/table/toast_helper.rs`
- `src/backend/access/services.rs`
- `src/backend/access/transam/xact.rs`
- small call-site deref updates in catalog/analyze/executor code

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh check --features lz4 --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet heap`
- `scripts/cargo_isolated.sh test --lib --quiet toast`
- `scripts/cargo_isolated.sh test --lib --quiet vacuum`
- `scripts/cargo_isolated.sh test --lib --quiet index`
- `scripts/cargo_isolated.sh test --lib --quiet btree`
- `scripts/cargo_isolated.sh test --lib --quiet sequence`
- `scripts/cargo_isolated.sh test --lib --quiet catalog`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/cargo_isolated.sh test --lib --quiet executor` failed from disk
  exhaustion after hundreds of tests; reran
  `extended_numeric_columns_round_trip_through_storage` by itself successfully
  to confirm a representative storage executor test still passes.
- boundary checks for root imports in `pgrust_access` and access imports in
  `pgrust_storage`

Remaining:
- Move root-owned TOAST chunk scan/delete/index-maintenance orchestration behind
  explicit services when executor/table command boundaries are thinner.
- Move BRIN runtime when unpaused.
- Move transam/WAL/checkpoint/recovery last.
