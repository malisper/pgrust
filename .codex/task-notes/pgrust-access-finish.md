Goal:
Finish the `pgrust_access` split by moving access runtime behind explicit service boundaries while keeping root as orchestration/shim code.

Key decisions:
- Start with scalar-dependent AM support before heap/transam moves.
- Added `AccessError`, `AccessResult`, and service traits in `pgrust_access`.
- Root adapter is `RootAccessServices`; it maps scalar comparisons/range/multirange/network helpers, GIN JSONB extraction, and geometry helpers back to existing executor code.
- Kept `pgrust_access` independent of root, parser/analyze/optimizer, executor, PL/pgSQL, and `pgrust_expr`.
- GIN JSONB and GiST support modules now live in `pgrust_access` behind scalar service hooks.
- Moved portable index scan descriptor state and generic scan stub setup/reset into `pgrust_access`; root keeps old paths as shims.
- Root `snapmgr::Snapshot` now re-exports `pgrust_core::Snapshot`; heap visibility helpers are a root extension trait until heap runtime moves.
- Moved portable BRIN/index AM catalog validation into `pgrust_access`; root only supplies AM-handler existence through a shim callback.
- Moved lock-manager code into `pgrust_storage`; root `storage::lmgr` is now compatibility shims. `TransactionWaiter` uses a `TransactionStatusLookup` trait so storage does not depend on access/transam.
- Moved pure index key projection/coercion into `pgrust_access::index::buildkeys`; root still owns expression-index and partial-index evaluation.
- `TableLockManager::has_locks_for_client` is public because root integration tests call it through the moved storage crate, where `#[cfg(test)]` would only apply to storage's own tests.
- Added access runtime service traits for interrupts, transaction lookup/waiting, heap fetch/visible scans, and WAL record logging.
- Moved portable AM result/unique-check types into `pgrust_access::access::amapi`; root `include::access::amapi` re-exports them while keeping root-owned callback contexts.
- Added root runtime bridge implementations for interrupt/transaction/heap services and a root index-build projection service wrapper.
- Routed hash index build heap scanning and key projection through `AccessHeapServices` and `AccessIndexServices` as the first runtime path using the new boundary.
- Routed btree, BRIN, GIN, GiST, and SP-GiST build heap scans/key projection through the same service boundary.
- Routed btree unique probing through `AccessHeapServices` and `AccessTransactionServices`; access errors now preserve interrupt reasons explicitly.

Files touched:
- `crates/pgrust_access/src/error.rs`
- `crates/pgrust_access/src/services.rs`
- `crates/pgrust_access/src/access/amapi.rs`
- `crates/pgrust_access/src/nbtree/{mod.rs,nbtcompare.rs,nbtpreprocesskeys.rs,tuple.rs}`
- `crates/pgrust_access/src/brin/{mod.rs,minmax.rs,tuple.rs}`
- `crates/pgrust_access/src/brin/validate.rs`
- `crates/pgrust_access/src/gin/{mod.rs,jsonb_ops.rs}`
- `crates/pgrust_access/src/gist/{mod.rs,support/*,tuple.rs}`
- `crates/pgrust_access/src/spgist/{mod.rs,support.rs,quad_box.rs,tuple.rs}`
- `crates/pgrust_access/src/access/relscan.rs`
- `crates/pgrust_access/src/index/{mod.rs,genam.rs}`
- `crates/pgrust_access/src/index/amvalidate.rs`
- `crates/pgrust_access/src/index/buildkeys.rs`
- `src/backend/access/services.rs`
- `src/backend/access/brin/brin.rs`
- `src/backend/access/gin/gin.rs`
- `src/backend/access/gist/build.rs`
- `src/backend/access/hash/mod.rs`
- `src/backend/access/nbtree/nbtree.rs`
- `src/backend/access/spgist/build.rs`
- `src/backend/access/index/genam.rs`
- `src/backend/access/index/amvalidate.rs`
- `src/backend/access/index/buildkeys.rs`
- `src/backend/access/index/unique.rs`
- `src/backend/access/brin/validate.rs`
- `src/include/access/amapi.rs`
- `crates/pgrust_core/src/transam.rs`
- `src/backend/commands/tablecmds.rs`
- `src/backend/utils/time/snapmgr.rs`
- `src/backend/access/heap/heapam_visibility.rs`
- `crates/pgrust_storage/src/lmgr/*`
- `src/backend/storage/lmgr/*`
- `src/backend/utils/misc/interrupts.rs`
- Root compatibility shims under `src/backend/access/{nbtree,brin,gin,gist}/...`

Tests run:
- `cargo fmt --all`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- `scripts/cargo_isolated.sh check --features lz4 --message-format short`
- `scripts/cargo_isolated.sh test --lib --quiet btree`
- `scripts/cargo_isolated.sh test --lib --quiet large_text_index_keys_use_inline_compression`
- `scripts/cargo_isolated.sh test --lib --quiet brin`
- `scripts/cargo_isolated.sh test --lib --quiet gist`
- `scripts/cargo_isolated.sh test --lib --quiet spgist`
- `scripts/cargo_isolated.sh test --lib --quiet hash`
- `scripts/cargo_isolated.sh test --lib --quiet btree`
- `scripts/cargo_isolated.sh test --lib --quiet brin`
- `scripts/cargo_isolated.sh test --lib --quiet gist`
- `scripts/cargo_isolated.sh test --lib --quiet spgist`
- `scripts/cargo_isolated.sh test --lib --quiet form_and_read_index_image_roundtrips_entry_pages`
- `scripts/cargo_isolated.sh test --lib --quiet form_index_pages_records_pending_tail_free_space`
- `scripts/cargo_isolated.sh test --lib --quiet jsonb_ops_extracts_object_keys_and_array_strings_as_keys`
- `scripts/cargo_isolated.sh test --lib --quiet jsonb_ops_empty_container_emits_empty_item`
- `scripts/cargo_isolated.sh test --lib --quiet index`
- `scripts/cargo_isolated.sh test --lib --quiet hash`
- Boundary checks for `crates/pgrust_access/src` root imports and `crates/pgrust_storage/src` access imports.
- Note: `scripts/cargo_isolated.sh test --lib --quiet gin` also matches unrelated names containing "gin" (for example "planning") and currently hits an unrelated root analyze-services failure; use focused GIN test names instead.

Remaining:
- Use the new runtime service traits from moved index runtime paths instead of passing root contexts directly.
- Wire `AccessToastServices` into detoast/TOAST runtime paths.
- Move index runtime only after expression/partial index projection is represented by `AccessIndexServices`.
- Move lock/transam/WAL/checkpoint and heap/table runtime in separate slices; those need storage/runtime traits and careful recovery byte-preservation checks.
