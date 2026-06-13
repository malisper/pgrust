//! The single-tuple search hot path (`catcache.c`):
//! `SearchCatCache`/`SearchCatCache1..4`, `SearchCatCacheInternal`,
//! `SearchCatCacheMiss`, `GetCatCacheHashValue`, and `ReleaseCatCache`.
//!
//! In the owned model a search returns a *copy* of the matched tuple as a
//! [`FormedTuple`] allocated in the caller's `mcx`; the owner drops its
//! reference before returning, so there is no caller-visible release seam (the
//! C `&ct->tuple` borrow + later `ReleaseCatCache` collapse into the copy).
//! `ReleaseCatCache` remains as an internal arena operation paired with the
//! resource-owner bookkeeping. A cache miss (a negative entry) is `Ok(None)`.
//!
//! The catalog scan on a miss (`table_open` + `systable_*` + detoast + key
//! extraction) crosses the genam/heaptoast/relcache substrate seams, returning
//! [`FetchedCatalogTuple`] carriers; resource-owner enlarge/remember/forget
//! cross the resowner seams.

use mcx::Mcx;
use types_cache::backend_utils_cache_catcache::{CacheIdx, CtIdx, FetchedCatalogTuple};
use types_cache::SysCacheKey;
use types_datum::Datum;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::FormedTuple;

/// One probe outcome from the bucket scan.
pub(crate) enum ProbeResult {
    HitPositive(CacheIdx, CtIdx),
    HitNegative,
    Miss(CacheIdx, u32, usize),
}

/// `SearchCatCacheInternal` — the bucket-probe hot path; on a miss delegates to
/// [`search_cat_cache_miss`]. Returns `None` for a negative hit (C `NULL`), or
/// a copy of the matched tuple in `mcx`.
pub(crate) fn search_cat_cache_internal<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _nkeys: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
    _v3: SysCacheKey<'_>,
    _v4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache_internal")
}

/// `SearchCatCacheMiss` — scan the catalog (via seam), enter a positive entry,
/// else a negative entry (unless bootstrap mode). The C stale-retry loop is
/// folded into the scan seam, which returns fresh, flattened tuples.
pub(crate) fn search_cat_cache_miss<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _cache_idx: CacheIdx,
    _nkeys: i32,
    _hash_value: u32,
    _hash_index: usize,
    _arguments: [Datum; 4],
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache_miss")
}

/// Copy a live positive cache tuple into `mcx` as a `FormedTuple` (the C
/// `&ct->tuple` borrow, materialized as the owned copy callers keep).
pub(crate) fn build_formed_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_idx: CacheIdx,
    _ct_idx: CtIdx,
) -> PgResult<FormedTuple<'mcx>> {
    todo!("catcache::search_path::build_formed_tuple")
}

/* ----------------------------------------------------------------------------
 * Outward seam shapes — `SearchCatCache{,1..4}` and `GetCatCacheHashValue`.
 * ------------------------------------------------------------------------- */

/// `SearchCatCache(cache, v1, v2, v3, v4)`.
pub fn search_cat_cache<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
    _v3: SysCacheKey<'_>,
    _v4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache")
}

/// `SearchCatCache1(cache, v1)`.
pub fn search_cat_cache_1<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _v1: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache_1")
}

/// `SearchCatCache2(cache, v1, v2)`.
pub fn search_cat_cache_2<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache_2")
}

/// `SearchCatCache3(cache, v1, v2, v3)`.
pub fn search_cat_cache_3<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
    _v3: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache_3")
}

/// `SearchCatCache4(cache, v1, v2, v3, v4)`.
pub fn search_cat_cache_4<'mcx>(
    _mcx: Mcx<'mcx>,
    _cache_id: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
    _v3: SysCacheKey<'_>,
    _v4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    todo!("catcache::search_path::search_cat_cache_4")
}

/// `GetCatCacheHashValue(cache, v1..v4)` — initialize the cache if needed and
/// hash the search keys.
pub fn get_cat_cache_hash_value(
    _cache_id: i32,
    _v1: SysCacheKey<'_>,
    _v2: SysCacheKey<'_>,
    _v3: SysCacheKey<'_>,
    _v4: SysCacheKey<'_>,
) -> PgResult<u32> {
    todo!("catcache::search_path::get_cat_cache_hash_value")
}

/* ----------------------------------------------------------------------------
 * ReleaseCatCache (internal arena operation; the copy collapses caller-side
 * release, but the owner still decrements after building the copy).
 * ------------------------------------------------------------------------- */

/// `ReleaseCatCache(tuple)` — decrement the refcount grabbed by a successful
/// search, forget the resource-owner reference, and remove the entry if it is
/// now dead and unreferenced.
pub(crate) fn release_cat_cache(_cache_id: i32, _ct_idx: CtIdx) -> PgResult<()> {
    todo!("catcache::search_path::release_cat_cache")
}

/// Reuse-or-create a member entry from a fetched tuple (shared with the list
/// path); returns the entry's arena index.
pub(crate) fn reuse_or_create_entry(
    _cache_idx: CacheIdx,
    _fetched: &FetchedCatalogTuple,
) -> PgResult<CtIdx> {
    todo!("catcache::search_path::reuse_or_create_entry")
}
