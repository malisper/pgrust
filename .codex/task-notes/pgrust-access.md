Goal:
Extract storage and portable access format code toward `pgrust_storage` and `pgrust_access`.

Key decisions:
- Created `pgrust_storage` first so access code can depend on storage without depending on root `pgrust`.
- `BufferPool` now accepts a `WalSink` trait object; root `WalWriter` implements it.
- Moved storage/page/smgr/fsm/sync plus buffer internals into `pgrust_storage`; root storage paths are compatibility shims.
- Created `pgrust_access` for portable tuple/page/index/toast format definitions and tuple serialization logic; root include/access paths are compatibility shims.
- Kept root `Snapshot` as the root-owned type for now because MVCC visibility methods still depend on root `TransactionManager`.
- Did not move runtime heap/index/transam/lmgr code yet; those need `AccessScalarServices` and transaction/storage adapters first.

Files touched:
- `crates/pgrust_storage/**`
- `crates/pgrust_access/**`
- `crates/pgrust_core/src/{storage,transam,interrupts,catalog,lib}.rs`
- root storage/include/access shim modules
- `src/backend/access/transam/xlog.rs`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet heap`
- `scripts/cargo_isolated.sh test --lib --quiet index`
- `scripts/cargo_isolated.sh test --lib --quiet toast`
- `scripts/cargo_isolated.sh test --lib --quiet catalog`

Remaining:
- Add `AccessScalarServices` and runtime context traits.
- Move `lmgr`/transaction wait state after deciding whether it belongs in storage or access.
- Move transam/WAL/checkpoint after Snapshot/TransactionManager can cross the crate boundary cleanly.
- Move heap/index runtime modules and native AM implementations behind root service adapters.
