# Audit: backend-utils-cache-catcache

- **Unit:** backend-utils-cache-catcache
- **C source:** `src/backend/utils/cache/catcache.c` (+ `src/include/utils/catcache.h`)
- **Branch:** port/backend-utils-cache-catcache-assemble (assembled from scaffold + 7 family branches)
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context)
- **Verdict:** PASS (after one fix-and-re-audit round on `SearchCatCacheList`)

## Method

Independent re-derivation from the C source and the c2rust run
(`pgrust/c2rust-runs/backend-utils-cache-catcache/src/catcache.rs`). Every
function definition in `catcache.c` (57 defs incl. statics and the resowner
trampolines) was enumerated and matched against the port modules. Constants
(`F_*EQ` regprocs, key-type OIDs, `CT_MAGIC`/`CL_MAGIC`, rotate amounts,
fill-factor `*2`, initial bucket counts) verified against the C, not memory.

## Function inventory & verdicts

| C function (catcache.c line) | Port location | Verdict | Notes |
|---|---|---|---|
| ResourceOwnerRememberCatCacheRef (159) | search_path / resowner seam | SEAMED | resowner ref identity tracked via resowner crate |
| ResourceOwnerForgetCatCacheRef (164) | search_path / resowner | SEAMED | |
| ResourceOwnerRememberCatCacheListRef (169) | list_path / resowner | SEAMED | |
| ResourceOwnerForgetCatCacheListRef (174) | list_path / resowner | SEAMED | |
| chareqfast (191) | core_compute::chareqfast | MATCH | |
| charhashfast (197) | core_compute::charhashfast | MATCH | |
| nameeqfast (203) | core_compute::nameeqfast (byte-slice) | MATCH | by-ref key compared on resolved payload |
| namehashfast (212) | core_compute::namehashfast | MATCH | significant-len truncation preserved |
| int2eqfast / int2hashfast (220/226) | core_compute | MATCH | |
| int4eqfast / int4hashfast (232/238) | core_compute | MATCH | |
| texteqfast / texthashfast (244/254) | core_compute (byte-slice) | MATCH | |
| oidvectoreqfast / oidvectorhashfast (261/267) | core_compute | MATCH | hash returns PgResult (fmgr path) |
| GetCCHashEqFuncs (274) | core_compute::GetCCHashEqFuncs | MATCH | full key-type table; default = FATAL panic; CCFastKind+eqfunc returned |
| CatalogCacheComputeHashValue (344) | core_compute::CatalogCacheComputeHashValue | MATCH | switch fall-through + position rotate (24/16/8/0), FATAL on bad nkeys |
| CatalogCacheComputeTupleHashValue (386) | inval_support / list_path (key extraction) | MATCH | fastgetattr per cc_keyno then combine |
| CatalogCacheCompareTuple (441) | core_compute::CatalogCacheCompareTuple | MATCH | |
| CatCachePrintStats (460) | — | N/A | `#ifdef CATCACHE_STATS` only; not in build config |
| CatCacheRemoveCTup (528) | graph_machinery::CatCacheRemoveCTup | MATCH | unlink, list-member fixup, free, counters |
| CatCacheRemoveCList (570) | graph_machinery::CatCacheRemoveCList | MATCH | member c_list clear + conditional ct removal |
| CatCacheInvalidate (625) | graph_machinery::cat_cache_invalidate_idx | MATCH | zap all lists, bucket tuple scan, in-progress stack |
| CreateCacheMemoryContext (708) | — | SEAMED | CacheMemoryContext owned by mmgr substrate |
| ResetCatalogCache (736) | graph_machinery::reset_catalog_cache | MATCH | list+tuple mark-dead/remove, in-progress (skipped on debug_discard) |
| ResetCatalogCaches (798) | graph_machinery::ResetCatalogCaches | MATCH | |
| ResetCatalogCachesExt (804) | graph_machinery::ResetCatalogCachesExt | MATCH | |
| CatalogCacheFlushCatalog (834) | graph_machinery::CatalogCacheFlushCatalog | MATCH | per-cache reset filtered by catId |
| InitCatCache (878) | graph_machinery::InitCatCache | MATCH | register cache, alloc buckets, key/fastkind setup |
| RehashCatCache (985) | graph_machinery::rehash_cat_cache | MATCH | double buckets, rechain by HASH_INDEX |
| RehashCatCacheLists (1023) | graph_machinery::rehash_cat_cache_lists | MATCH | |
| ConditionalCatalogCacheInitializeCache (1064) | init_meta::conditional_initialize | MATCH | |
| CatalogCacheInitializeCache (1115) | init_meta::catalog_cache_initialize_cache | MATCH | relcache/tupdesc seam, per-key GetCCHashEqFuncs, skey build |
| InitCatCachePhase2 (1224) | init_meta::InitCatCachePhase2 | MATCH | |
| IndexScanOK (1275) | init_meta::IndexScanOK | MATCH | the bootstrapping-index allowlist preserved |
| SearchCatCache (1340) | search_path::search_cat_cache | MATCH | |
| SearchCatCache1..4 (1357..1381) | search_path::search_cat_cache_1..4 | MATCH | |
| SearchCatCacheInternal (1391) | search_path::search_cat_cache_internal | MATCH | bucket probe, dead skip, move-to-front, refcount+ |
| SearchCatCacheMiss (1499) | search_path::search_cat_cache_miss | SEAMED | table_open/systable scan + toast-flatten + stale-retry owned by the genam/heaptoast/relcache substrate behind `catcache_scan_single`; the seam returns already-flattened tuples, so the detoast-invalidation retry window does not exist inside this crate. Negative-entry + bootstrap-mode branch is in-crate and MATCH. |
| ReleaseCatCache (1647) | search_path::release_cat_cache | MATCH | |
| ReleaseCatCacheWithOwner (1653) | search_path::release_cat_cache | MATCH | refcount--, remove if dead&unref |
| GetCatCacheHashValue (1686) | search_path::get_cat_cache_hash_value | MATCH | |
| SearchCatCacheList (1719) | list_path::search_cat_cache_list + build_list_body + scan_members | MATCH (after fix) | see findings below |
| ReleaseCatCacheList (2093) | list_path::release_cat_cache_list | MATCH | |
| ReleaseCatCacheListWithOwner (2099) | list_path::release_cat_cache_list | MATCH | |
| CatalogCacheCreateEntry (2133) | graph_machinery::create_entry_positive / create_entry_negative | MATCH | toast-flatten + in-progress push live behind the scan seam (positive) / list path (list build); negative uses CatCacheCopyKeys equivalent |
| CatCacheFreeKeys (2281) | implicit (owned arena drop) | MATCH | by-ref key copies are owned Datums freed on slot reuse |
| CatCacheCopyKeys (2306) | create_entry_negative / list keys | MATCH | |
| PrepareToInvalidateCacheTuple (2376) | inval_support::prepare_to_invalidate_cache_tuple | MATCH | callback → returned request vec; ConditionalInit mid-iter; old + conditional-new hash; relisshared ? 0 : MyDatabaseId |
| ResOwnerReleaseCatCache / Print (2437/2443) | resowner integration | SEAMED | |
| ResOwnerReleaseCatCacheList / Print (2460/2466) | resowner integration | SEAMED | |

## Findings (fixed this round)

1. **SearchCatCacheList member-reuse missing `c_list` skip** — the bucket probe
   in `reuse_or_create_entry` omitted the C `if (ct->c_list) continue;` guard
   ("can't use it if it belongs to another list already"). A tuple already
   owned by another list could be reused, then trip the `c_list == NULL`
   assertion during assembly / cause double list-ownership. **Fixed** in
   `search_path::reuse_or_create_entry`.
2. **SearchCatCacheList missing dead-list propagation** — the final member loop
   in `build_list_body` omitted `if (ct->dead) cl->dead = true;`, so a list
   built over a concurrently-invalidated member was born live and could be
   served stale. **Fixed** in `list_path::build_list_body`.

Both fixes re-derived against the C and re-audited clean; `cargo check
--workspace` and `cargo test --workspace` green after the fix.

## Seam audit

Owned seam crate: `backend-utils-cache-catcache-seams` (maps to catcache.c).
18 declarations, **all 18 installed** by `wiring::init_seams()` (init_cat_cache,
cat_cache_invalidate, reset_catalog_caches_ext, catalog_cache_flush_catalog,
syscache_invalidate, init_cat_cache_phase2, cache_nkeys, cache_relisshared,
cache_tupdesc_is_valid, with_cache_tupdesc, search_cat_cache, search_cat_cache_1..4,
get_cat_cache_hash_value, search_cat_cache_list, prepare_to_invalidate_cache_tuple).
`init_seams()` contains only `set()` calls (plus one thin marshal trampoline
`syscache_invalidate` that wraps the infallible `CatCacheInvalidate` into the
callback-error-carrying seam shape — no logic). `seams-init::init_all()` calls
`backend_utils_cache_catcache::init_seams()`.

Outward seams consumed (justified by real cycles, thin marshal+delegate only):
relcache (RelationIdGetRelation/close, tupdesc), genam (systable_begin/get/end),
heaptuple, indexam, lmgr, xact, miscinit (bootstrap mode), init-small
(MyDatabaseId), inval, hashfn. No branching/node-construction observed in the
crate's own seam-call sites beyond argument/result marshalling.

## Design conformance

- Opacity: cache state is the real free-listed arena in `types-cache`
  (`CatCacheArena`/`ArenaCatCTup`/`ArenaCatCList`); cross-refs are typed arena
  indices, not invented handles. No opaque void* stand-ins introduced.
- Per-backend globals (`CacheHdr`/`SysCache[]`/`catcache_in_progress_stack`)
  are a `thread_local!` arena — not shared statics. PASS.
- Allocating/`mcx`-bearing entry points take `Mcx` and return `PgResult`. PASS.
- `elog(FATAL,...)` paths mapped to panics with the same predicates
  (unsupported key type, wrong nkeys). PASS.

## Residual

- `todo!()`/`unimplemented!()` in own logic: **0**.
- `CatCachePrintStats` and `cc_*` stat counters are `#ifdef CATCACHE_STATS`,
  outside the build config — correctly absent (N/A, not MISSING).

**VERDICT: PASS**
