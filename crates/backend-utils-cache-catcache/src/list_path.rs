//! The partial-key list search path (`catcache.c`): `SearchCatCacheList`,
//! `SearchCatCacheList`'s miss/build body, and `ReleaseCatCacheList`.
//!
//! In the owned model the list search returns the member tuples as a
//! `PgVec<FormedTuple>` (each member copied into the caller's `mcx`, in scan
//! order); release of the C `CatCList` refcount is folded into the copy, as for
//! the single searches. The create-in-progress stack guards against a
//! concurrent invalidation arriving mid-build.

use mcx::{Mcx, PgVec};
use types_cache::backend_utils_cache_catcache::{CacheIdx, ClIdx, CtIdx};
use types_cache::SysCacheKey;
use types_datum::Datum;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::FormedTuple;

/// One probe outcome from the list-bucket scan.
pub(crate) enum ListProbe {
    Hit(CacheIdx, ClIdx),
    Miss(CacheIdx, u32, usize),
}

/// `SearchCatCacheList(cache, nkeys, v1, v2, v3)` — build (or hit) the list of
/// all tuples matching a partial key, returning member copies in `mcx`.
pub fn search_cat_cache_list<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _nkeys: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
    _v3: SysCacheKey<'_>,
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    todo!("catcache::list_path::search_cat_cache_list")
}

/// Build the list on a miss: scan the catalog, reuse/create member entries,
/// then assemble and link the `CatCList`. Mirrors the C `PG_TRY` body with the
/// in-progress stack guarding concurrent invalidation.
pub(crate) fn search_cat_cache_list_miss<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _cache_idx: CacheIdx,
    _nkeys: i32,
    _l_hash_value: u32,
    _l_hash_index: usize,
    _arguments: [Datum; 4],
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    todo!("catcache::list_path::search_cat_cache_list_miss")
}

/// Undo temporary member refcounts on a list-build error (the C `PG_CATCH`
/// loop): drop each member's temp ref, removing now-dead unreferenced entries.
pub(crate) fn undo_member_refs(_cache_idx: CacheIdx, _members: &[CtIdx]) -> PgResult<()> {
    todo!("catcache::list_path::undo_member_refs")
}

/// Copy a live list's members into `mcx` as `FormedTuple`s, in scan order.
pub(crate) fn build_list_members<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_idx: CacheIdx,
    _cl_idx: ClIdx,
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    todo!("catcache::list_path::build_list_members")
}

/// `ReleaseCatCacheList(list)` — decrement the list refcount, removing it if
/// now dead and unreferenced.
pub(crate) fn release_cat_cache_list(_cache_id: i32, _cl_idx: ClIdx) -> PgResult<()> {
    todo!("catcache::list_path::release_cat_cache_list")
}
