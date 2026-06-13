//! Seams for the radix-tree storage template (`src/include/lib/radixtree.h`),
//! as instantiated by `access/common/tidstore.c` for the `BlocktableEntry`
//! value type (`RT_PREFIX local_ts` / `shared_ts`).
//!
//! `radixtree.h` is a generic container template that `tidstore.c` `#include`s
//! twice — once for a backend-local (process-heap) tree and once for a
//! DSA-shared tree guarded by the tree's own LWLock. That container — node
//! kinds / size classes / the grow-insert-search-iterate walk / the
//! embedded-value tagging / the DSA-shared flavor — is its own unit
//! (`backend-lib-radixtree`), not part of `tidstore.c`'s own logic. Until that
//! owner lands, every op here is a [`seam_core::seam!`] loud panic; the owner
//! installs them from its `init_seams()`. There is no silent fallback: nothing
//! fabricates a tree, an entry, or an iteration result.
//!
//! ## Identity and value wire format
//!
//! A live tree is named by the [`TidStore`] handle `tidstore.c` threads (its
//! `id` is the tree's runtime identity); an in-progress iteration is named by
//! a [`TidStoreIterHandle`]. The radix *value* is a `BlocktableEntry`, crossed
//! as the `Vec<bitmapword>` byte image `tidstore.c` packs/unpacks
//! (`wire[0]` = the packed `header` slot, `wire[1..]` = the bitmap `words`),
//! mirroring the C in-memory layout where the header occupies one
//! pointer-sized slot immediately followed by `words[]`.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use types_core::BlockNumber;
use types_dsa::{DsaHandle, DsaPointer};
use types_error::PgResult;
use types_nodes::bitmapset::bitmapword;
use types_vacuum::vacuumlazy::{TidStore, TidStoreIterHandle};

// ---- creation / teardown ----------------------------------------------------

seam_core::seam!(
    /// `BumpContextCreate`/`AllocSetContextCreate("TID storage", ...)` +
    /// `local_ts_create(rt_context)` — create a backend-local radix tree in a
    /// fresh "TID storage" memory context with the given block sizing (the
    /// `max_bytes`-derived policy is computed by the caller in `tidstore.c`);
    /// `insert_only` selects the bump context. Returns the tree's runtime
    /// [`TidStore`] identity.
    pub fn radixtree_create_local(
        min_context_size: usize,
        init_block_size: usize,
        max_block_size: usize,
        insert_only: bool,
    ) -> PgResult<TidStore>
);
seam_core::seam!(
    /// `dsa_create_ext(tranche_id, dsa_init_size, dsa_max_size)` +
    /// `shared_ts_create(area, tranche_id)` — create a DSA-shared radix tree.
    /// The segment sizes are the `max_bytes`-derived policy the caller computes
    /// in `tidstore.c`.
    pub fn radixtree_create_shared(
        dsa_init_size: usize,
        dsa_max_size: usize,
        tranche_id: i32,
    ) -> PgResult<TidStore>
);
seam_core::seam!(
    /// `dsa_attach(area_handle)` + `shared_ts_attach(area, handle)` — attach to
    /// an existing shared tree; returns this backend's [`TidStore`] identity.
    pub fn radixtree_attach(area_handle: DsaHandle, handle: DsaPointer) -> PgResult<TidStore>
);
seam_core::seam!(
    /// `shared_ts_detach(tree)` + `dsa_detach(area)` — detach a shared tree and
    /// free backend-local resources (no-op effect on the shared data).
    pub fn radixtree_detach(ts: TidStore) -> PgResult<()>
);
seam_core::seam!(
    /// `shared_ts_free`/`local_ts_free` + context delete / `dsa_detach` — fully
    /// destroy the tree, returning all memory.
    pub fn radixtree_free(ts: TidStore) -> PgResult<()>
);

// ---- shared-tree locking -----------------------------------------------------

seam_core::seam!(
    /// `shared_ts_lock_exclusive` (`Some(true)`), `shared_ts_lock_share`
    /// (`Some(false)`), or `shared_ts_unlock` (`None`) — take/release the
    /// shared tree's LWLock. Only ever invoked for a shared tree.
    pub fn radixtree_lock(ts: TidStore, exclusive: Option<bool>) -> PgResult<()>
);

// ---- per-key set / find ------------------------------------------------------

seam_core::seam!(
    /// `local_ts_set`/`shared_ts_set(tree, blkno, page)` — store (replacing any
    /// existing) the `BlocktableEntry` `wire` image at key `blkno`.
    pub fn radixtree_set(ts: TidStore, blkno: BlockNumber, wire: Vec<bitmapword>) -> PgResult<()>
);
seam_core::seam!(
    /// `local_ts_find`/`shared_ts_find(tree, blkno)` — the `BlocktableEntry`
    /// wire image at `blkno`, or `None` if absent.
    pub fn radixtree_find(ts: TidStore, blkno: BlockNumber) -> PgResult<Option<Vec<bitmapword>>>
);

// ---- iteration ---------------------------------------------------------------

seam_core::seam!(
    /// `local_ts_begin_iterate`/`shared_ts_begin_iterate(tree)` — start a
    /// forward (ascending-key) iteration; returns its runtime handle.
    pub fn radixtree_begin_iterate(ts: TidStore) -> PgResult<TidStoreIterHandle>
);
seam_core::seam!(
    /// `local_ts_iterate_next`/`shared_ts_iterate_next(iter, &key)` — the next
    /// `(blkno, entry-wire-image)`, or `None` at end.
    pub fn radixtree_iterate_next(
        iter: TidStoreIterHandle,
    ) -> PgResult<Option<(BlockNumber, Vec<bitmapword>)>>
);
seam_core::seam!(
    /// `local_ts_end_iterate`/`shared_ts_end_iterate(iter)` — finish iteration.
    pub fn radixtree_end_iterate(iter: TidStoreIterHandle) -> PgResult<()>
);

// ---- accounting --------------------------------------------------------------

seam_core::seam!(
    /// `local_ts_memory_usage`/`shared_ts_memory_usage(tree)` — current
    /// footprint of the tree in bytes.
    pub fn radixtree_memory_usage(ts: TidStore) -> PgResult<usize>
);
seam_core::seam!(
    /// `shared_ts_get_handle(tree)` — the `dsa_pointer` handle naming the shared
    /// tree for `TidStoreAttach`. Only ever invoked for a shared tree.
    pub fn radixtree_get_handle(ts: TidStore) -> PgResult<DsaPointer>
);
seam_core::seam!(
    /// `ts->area` — the `dsa_handle` of the DSA area the shared tree lives in
    /// (`TidStoreGetDSA`). Only ever invoked for a shared tree.
    pub fn radixtree_get_dsa(ts: TidStore) -> PgResult<DsaHandle>
);
