//! Seam declarations for the `backend-utils-cache-catcache` unit
//! (`utils/cache/catcache.c`), which owns the per-cache `CatCache` control
//! blocks (the `SysCache[]` array contents) and their entries.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! A `CatCache *` argument crosses these seams as the syscache's integer
//! cache id (`0 <= cache_id < SysCacheSize`) — the owner resolves the id to
//! its control block. C's `SearchCatCache*` return a refcounted pointer into
//! the cache that the caller must `ReleaseCatCache`; in the owned model each
//! search returns a *copy* of the entry's tuple allocated in the caller's
//! `mcx`, the owner dropping its reference before returning, so release is
//! dropping the copy and no release seam exists. `Err` carries the C
//! `ereport(ERROR)` surface of the underlying catalog scan plus OOM from the
//! copy; a cache miss (a negative entry) is `Ok(None)`.

use mcx::{Mcx, PgVec};
use cache::SysCacheKey;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::heaptuple::FormedTuple;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `InitCatCache(id, reloid, indexoid, nkeys, key, nbuckets)`
    /// (catcache.c): allocate and initialize a cache's control block in
    /// `CacheMemoryContext`. Returns whether the cache was created (the C
    /// non-NULL pointer test in `InitCatalogCache`); `Err` carries OOM.
    pub fn init_cat_cache(
        id: i32,
        reloid: Oid,
        indexoid: Oid,
        nkeys: i32,
        key: [i32; 4],
        nbuckets: i32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `InitCatCachePhase2(cache, touch_index)` (catcache.c): finish
    /// initializing a cache's control block (loads the catalog's tupdesc;
    /// with `touch_index`, also opens the supporting index once).
    pub fn init_cat_cache_phase2(cache_id: i32, touch_index: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `SearchCatCache(cache, v1, v2, v3, v4)` (catcache.c). The cache's
    /// `cc_nkeys` determines how many keys are used; unused slots are
    /// `SysCacheKey::UNUSED`.
    pub fn search_cat_cache<'mcx>(
        mcx: Mcx<'mcx>,
        cache_id: i32,
        v1: SysCacheKey<'_>,
        v2: SysCacheKey<'_>,
        v3: SysCacheKey<'_>,
        v4: SysCacheKey<'_>,
    ) -> PgResult<Option<FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchCatCache1(cache, v1)` (catcache.c).
    pub fn search_cat_cache_1<'mcx>(
        mcx: Mcx<'mcx>,
        cache_id: i32,
        v1: SysCacheKey<'_>,
    ) -> PgResult<Option<FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchCatCache2(cache, v1, v2)` (catcache.c).
    pub fn search_cat_cache_2<'mcx>(
        mcx: Mcx<'mcx>,
        cache_id: i32,
        v1: SysCacheKey<'_>,
        v2: SysCacheKey<'_>,
    ) -> PgResult<Option<FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchCatCache3(cache, v1, v2, v3)` (catcache.c).
    pub fn search_cat_cache_3<'mcx>(
        mcx: Mcx<'mcx>,
        cache_id: i32,
        v1: SysCacheKey<'_>,
        v2: SysCacheKey<'_>,
        v3: SysCacheKey<'_>,
    ) -> PgResult<Option<FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchCatCache4(cache, v1, v2, v3, v4)` (catcache.c).
    pub fn search_cat_cache_4<'mcx>(
        mcx: Mcx<'mcx>,
        cache_id: i32,
        v1: SysCacheKey<'_>,
        v2: SysCacheKey<'_>,
        v3: SysCacheKey<'_>,
        v4: SysCacheKey<'_>,
    ) -> PgResult<Option<FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `GetCatCacheHashValue(cache, v1, v2, v3, v4)` (catcache.c): the hash
    /// value the cache would use for these keys (exposed to inval.c via
    /// `GetSysCacheHashValue`).
    pub fn get_cat_cache_hash_value(
        cache_id: i32,
        v1: SysCacheKey<'_>,
        v2: SysCacheKey<'_>,
        v3: SysCacheKey<'_>,
        v4: SysCacheKey<'_>,
    ) -> PgResult<u32>
);

seam_core::seam!(
    /// `SearchCatCacheList(cache, nkeys, v1, v2, v3)` (catcache.c): all
    /// member tuples matching the partial key, in index order — each member
    /// copied into `mcx` (release of the C `CatCList` refcount is folded in,
    /// as for the single searches).
    pub fn search_cat_cache_list<'mcx>(
        mcx: Mcx<'mcx>,
        cache_id: i32,
        nkeys: i32,
        v1: SysCacheKey<'_>,
        v2: SysCacheKey<'_>,
        v3: SysCacheKey<'_>,
    ) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `CatCacheInvalidate(cache, hashValue)` (catcache.c): mark matching
    /// entries (and any matching cat-lists) dead. Infallible in C.
    pub fn cat_cache_invalidate(cache_id: i32, hash_value: u32)
);

seam_core::seam!(
    /// Read `cache->cc_nkeys` (the `Assert(SysCache[id]->cc_nkeys == N)`
    /// checks of `SearchSysCacheN`).
    pub fn cache_nkeys(cache_id: i32) -> i32
);

seam_core::seam!(
    /// Read `cache->cc_relisshared`. May be a temporary `false` until the
    /// cache's first search completes phase-2 initialization (see
    /// `SearchSysCacheLocked1`).
    pub fn cache_relisshared(cache_id: i32) -> bool
);

seam_core::seam!(
    /// `PointerIsValid(cache->cc_tupdesc)` — whether the cache's tuple
    /// descriptor has been loaded (phase-2 initialization done).
    pub fn cache_tupdesc_is_valid(cache_id: i32) -> bool
);

seam_core::seam!(
    /// Read access to `cache->cc_tupdesc` (which lives in
    /// `CacheMemoryContext`): runs `f` with a borrow of the descriptor.
    /// Panics if the descriptor is not loaded — callers check
    /// [`cache_tupdesc_is_valid`] / run phase 2 first, as
    /// `SysCacheGetAttr` does.
    pub fn with_cache_tupdesc(cache_id: i32, f: &mut dyn FnMut(&TupleDescData<'_>))
);

seam_core::seam!(
    /// `ResetCatalogCachesExt(debug_discard)` (catcache.c): blow away every
    /// entry of every catcache (the `InvalidateSystemCachesExtended` reset).
    /// `Err` carries any error raised by a downstream invalidation callback.
    pub fn reset_catalog_caches_ext(debug_discard: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `SysCacheInvalidate(cacheId, hashValue)` (syscache.c → catcache.c):
    /// invalidate the matching entries of one catcache (the catcache arm of
    /// `LocalExecuteInvalidationMessage`). `Err` carries callback errors.
    pub fn syscache_invalidate(cache_id: i32, hash_value: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogCacheFlushCatalog(catId)` (catcache.c): flush all catcaches
    /// derived from one catalog (the `SHAREDINVALCATALOG_ID` arm). It calls
    /// the registered syscache callbacks as needed. `Err` carries callback
    /// errors.
    pub fn catalog_cache_flush_catalog(cat_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareToInvalidateCacheTuple(relation, tuple, newtuple, function,
    /// context)` (catcache.c): for each catcache the (old and new) tuple
    /// belongs to, the C code invokes `function(cacheId, hashValue, dbId,
    /// context)`. The owned model instead returns one
    /// [`PrepareToInvalidateCacheTuple`] request per invocation, in the same
    /// order, allocated in `mcx`. `Err` carries OOM / the C `elog(ERROR,
    /// "bogus call to PrepareToInvalidateCacheTuple")`.
    /// `tuple_data` / `newtuple_data` are the respective tuples' user-data
    /// areas (`(char *) t_data + t_hoff`), threaded alongside the header-only
    /// [`HeapTupleData`] so the cache-key columns can be deformed (the bytes
    /// live in `FormedTuple.data` at every caller).
    pub fn prepare_to_invalidate_cache_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &rel::RelationData<'_>,
        tuple: &types_tuple::HeapTupleData<'_>,
        tuple_data: &[u8],
        newtuple: Option<&types_tuple::HeapTupleData<'_>>,
        newtuple_data: Option<&[u8]>,
    ) -> PgResult<PgVec<'mcx, types_storage::PrepareToInvalidateCacheTuple>>
);
