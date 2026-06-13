#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
// SCAFFOLD: family signatures with `todo!()` bodies are not yet all wired into
// each other; the unused-helper warnings clear as the bodies land.
#![allow(dead_code)]

//! `backend/utils/cache/catcache.c` — the system-catalog tuple cache
//! (idiomatic port). SCAFFOLD: family module layout with signatures and
//! `todo!()` bodies; the seam wiring is in place so consumers link.
//!
//! # Representation
//!
//! The live cache state is the **free-listed index arena** in
//! [`types_cache::backend_utils_cache_catcache`] — the cache owns its tuples
//! and lists in slot vectors and every cross-reference (bucket chains, list
//! membership, `c_list`/`my_cache` back-links, the in-progress stack) is an
//! arena index, not a pointer. The whole subsystem state
//! ([`CatCacheArena`]) is a per-backend [`thread_local!`], mirroring the C
//! file-scope `CacheHdr` + `SysCache[]` + `catcache_in_progress_stack`.
//!
//! # Families
//!
//! * [`core_compute`] — the node-independent hash/equality fast functions plus
//!   `GetCCHashEqFuncs`, `CatalogCacheComputeHashValue`, `CatalogCacheCompareTuple`.
//! * [`graph_machinery`] — `InitCatCache`, the rehash routines,
//!   `CatalogCacheCreateEntry`, `CatCacheRemoveCTup`/`CatCacheRemoveCList`,
//!   `CatCacheInvalidate`, `ResetCatalogCache(s)(Ext)`,
//!   `CatalogCacheFlushCatalog`, and the in-progress stack.
//! * [`init_meta`] — `CatalogCacheInitializeCache` / `…Conditional` /
//!   `InitCatCachePhase2` / `IndexScanOK`, via the relcache seams.
//! * [`search_path`] — `SearchCatCache1..4`/`Internal`/`Miss` + release.
//! * [`list_path`] — `SearchCatCacheList`/`_miss` + release.
//! * [`inval_support`] — `PrepareToInvalidateCacheTuple`.
//! * [`wiring`] — installs every outward catcache seam.

extern crate alloc;

use core::cell::RefCell;
use std::thread_local;

use types_cache::backend_utils_cache_catcache::{CacheIdx, CatCacheArena};

pub mod core_compute;
pub mod graph_machinery;
pub mod init_meta;
pub mod inval_support;
pub mod list_path;
pub mod search_path;
pub mod wiring;

/* ===========================================================================
 * Process-global subsystem state (C file-scope `CacheHdr`/`SysCache[]`/
 * `catcache_in_progress_stack`).
 * ======================================================================== */

thread_local! {
    static ARENA: RefCell<CatCacheArena> = RefCell::new(CatCacheArena::default());
}

/// Run `f` with mutable access to the catcache arena.
pub(crate) fn with_arena<R>(f: impl FnOnce(&mut CatCacheArena) -> R) -> R {
    ARENA.with(|a| f(&mut a.borrow_mut()))
}

/// Find a registered cache by its syscache id (`SysCache[id]`).
pub(crate) fn find_cache_by_id(arena: &CatCacheArena, cache_id: i32) -> Option<CacheIdx> {
    arena
        .caches
        .iter()
        .position(|c| c.id == cache_id)
        .map(CacheIdx)
}

/// Install every outward catcache seam (called once from `seams-init`).
pub fn init_seams() {
    wiring::init_seams();
}
