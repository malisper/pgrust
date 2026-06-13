//! Cache initialization + metadata (`catcache.c`):
//! `CatalogCacheInitializeCache`, `ConditionalCatalogCacheInitializeCache`,
//! `InitCatCachePhase2`, `IndexScanOK`, and the small per-cache metadata reads
//! exposed as outward seams (`cache_nkeys`, `cache_relisshared`,
//! `cache_tupdesc_is_valid`, `with_cache_tupdesc`).
//!
//! `CatalogCacheInitializeCache` reaches the relcache (catalog relname /
//! relisshared / column types / tupdesc) — routed through the relcache seams
//! (panic until relcache lands). `IndexScanOK` depends on
//! `IsBootstrapProcessingMode` / `criticalRelcachesBuilt`.

use types_cache::backend_utils_cache_catcache::{CacheIdx, CatCacheArena};
use types_error::PgResult;
use types_tuple::heaptuple::TupleDescData;

/// `CatalogCacheInitializeCache(cache)` — final init: load the tupdesc /
/// relname / relisshared (via the relcache seam) and set up per-key fast-kind
/// selection.
pub(crate) fn catalog_cache_initialize_cache(
    _arena: &mut CatCacheArena,
    _cache_idx: CacheIdx,
) -> PgResult<()> {
    todo!("catcache::init_meta::catalog_cache_initialize_cache")
}

/// `ConditionalCatalogCacheInitializeCache(cache)`.
pub(crate) fn conditional_initialize(
    _arena: &mut CatCacheArena,
    _cache_idx: CacheIdx,
) -> PgResult<()> {
    todo!("catcache::init_meta::conditional_initialize")
}

/// `InitCatCachePhase2(cache, touch_index)` — finish init; the index touch is a
/// relcache-warming side effect (relcache owns `index_open`), routed via seam.
pub fn InitCatCachePhase2(_cache_id: i32, _touch_index: bool) -> PgResult<()> {
    todo!("catcache::init_meta::InitCatCachePhase2")
}

/// `IndexScanOK(cache)` — bootstrap-time index-safe predicate (depends on
/// `IsBootstrapProcessingMode`/`criticalRelcachesBuilt`); seamed.
pub fn IndexScanOK(_cache_id: i32) -> PgResult<bool> {
    todo!("catcache::init_meta::IndexScanOK")
}

/* ----------------------------------------------------------------------------
 * Per-cache metadata reads exposed as outward seams.
 * ------------------------------------------------------------------------- */

/// Read `cache->cc_nkeys` (the `Assert(SysCache[id]->cc_nkeys == N)` checks of
/// `SearchSysCacheN`).
pub fn cache_nkeys(_cache_id: i32) -> i32 {
    todo!("catcache::init_meta::cache_nkeys")
}

/// Read `cache->cc_relisshared`.
pub fn cache_relisshared(_cache_id: i32) -> bool {
    todo!("catcache::init_meta::cache_relisshared")
}

/// `PointerIsValid(cache->cc_tupdesc)` — whether phase-2 init has run.
pub fn cache_tupdesc_is_valid(_cache_id: i32) -> bool {
    todo!("catcache::init_meta::cache_tupdesc_is_valid")
}

/// Read access to `cache->cc_tupdesc`: runs `f` with a borrow of the
/// descriptor. Panics if not loaded (callers check `cache_tupdesc_is_valid` /
/// run phase 2 first, as `SysCacheGetAttr` does).
pub fn with_cache_tupdesc(_cache_id: i32, _f: &mut dyn FnMut(&TupleDescData<'_>)) {
    todo!("catcache::init_meta::with_cache_tupdesc")
}
