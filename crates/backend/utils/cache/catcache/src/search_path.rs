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
use cache::catcache::{
    CacheIdx, CatKey, CtIdx, FetchedCatalogTuple, ItemPointer,
};
use cache::SysCacheKey;
use types_core::Oid;
// Catcache key slots cross as `CatKey` (the owned form of C's `Datum keys[]`):
// by-value keys carry the scalar word (C's `cur_skey[i].sk_argument = vN`), and
// by-reference keys (name/text/oidvector) carry their resolved payload bytes —
// the pointer-bearing `Datum` C's search key already holds.
use types_error::PgResult;
use types_tuple::heaptuple::FormedTuple;

use crate::core_compute::{
    CatalogCacheCompareTuple, CatalogCacheComputeHashValue, HASH_INDEX,
};
use crate::graph_machinery::{self, ct_bucket_move_head, CatCacheRemoveCTup};
use crate::{find_cache_by_id, with_arena};
use crate::init_meta;
use crate::list_path;

/* ----------------------------------------------------------------------------
 * Substrate seams the single-search path calls out to.
 *
 * `SearchCatCacheMiss` opens the catalog (`table_open`), runs the index/heap
 * scan (`systable_beginscan`/`systable_getnext`), detoasts + flattens each
 * matched tuple, and extracts its key columns via the cache's `cc_tupdesc`.
 * That whole fetch+flatten+extract step is owned by the genam/heaptoast/
 * relcache substrate; it crosses this seam, returning [`FetchedCatalogTuple`]
 * carriers (at most one, for a unique single-tuple cache). Panics until the
 * substrate lands.
 *
 * The cached tuple bytes are reconstituted into an owned [`FormedTuple`] (the
 * C `&ct->tuple` borrow, materialized as `heap_copytuple` into the caller's
 * `mcx`) by the heaptuple substrate, which owns on-disk header decoding.
 *
 * A by-reference search key (`name`/`text`/`oidvector`) crosses as the bytes
 * carried in [`SysCacheKey`]; in C those already are a pointer-bearing `Datum`.
 * Reconstituting that key `Datum` for the hash/compare core is owned by the
 * datum/heaptuple substrate.
 * ------------------------------------------------------------------------- */

seam_core::seam!(
    /// `table_open(cache->cc_reloid, AccessShareLock)` +
    /// `systable_beginscan(rel, cc_indexoid, IndexScanOK(cache), NULL, nkeys,
    /// cur_skey)` + `systable_getnext` loop + detoast/flatten + key extraction
    /// (`catcache.c` `SearchCatCacheMiss`). Returns the matched catalog tuples
    /// (at most one for a single-tuple cache) as [`FetchedCatalogTuple`]
    /// carriers. `Err` carries the scan's `ereport(ERROR)` surface.
    pub fn catcache_scan_single(
        cache_id: i32,
        nkeys: i32,
        arguments: &[CatKey; 4],
    ) -> PgResult<alloc::vec::Vec<FetchedCatalogTuple>>
);

seam_core::seam!(
    /// `heap_copytuple(&ct->tuple)` into `mcx` (the materialized
    /// `&ct->tuple` callers keep): decode the cached on-disk header bytes and
    /// build an owned [`FormedTuple`]. Owned by the heaptuple substrate.
    pub fn catcache_form_cached_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        t_len: u32,
        t_self: ItemPointer,
        t_tableoid: Oid,
        t_data: &[u8],
    ) -> PgResult<FormedTuple<'mcx>>
);

/// One probe outcome from the bucket scan.
pub(crate) enum ProbeResult {
    HitPositive(CacheIdx, CtIdx),
    HitNegative,
    Miss(CacheIdx, u32, usize),
}

/// Resolve a [`SysCacheKey`] to the [`CatKey`] the hash/compare core consumes.
/// By-value keys keep their scalar word (the C `sk_argument = vN`); by-reference
/// keys carry their resolved payload bytes (in C the search key arrives as a
/// pointer-bearing `Datum` into that payload).
#[inline]
fn key_to_catkey(key: SysCacheKey<'_>) -> CatKey {
    match key {
        SysCacheKey::Value(d) => CatKey::Scalar(d),
        SysCacheKey::Str(s) => CatKey::ByRef(s.as_bytes().to_vec()),
        SysCacheKey::Bytes(b) => CatKey::ByRef(b.to_vec()),
    }
}

/// Build the `arguments[CATCACHE_MAXKEYS]` array from the search keys (the C
/// `cur_skey[i].sk_argument = vN` setup).
#[inline]
fn build_arguments(
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
    v4: SysCacheKey<'_>,
) -> [CatKey; 4] {
    [
        key_to_catkey(v1),
        key_to_catkey(v2),
        key_to_catkey(v3),
        key_to_catkey(v4),
    ]
}

/// `SearchCatCacheInternal` — the bucket-probe hot path; on a miss delegates to
/// [`search_cat_cache_miss`]. Returns `None` for a negative hit (C `NULL`), or
/// a copy of the matched tuple in `mcx`.
pub(crate) fn search_cat_cache_internal<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    nkeys: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
    v4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let arguments = build_arguments(v1, v2, v3, v4);

    /*
     * one-time startup overhead for each cache (the C
     * `if (unlikely(cache->cc_tupdesc == NULL))
     *      CatalogCacheInitializeCache(cache);`).
     */
    let cache_idx = with_arena(|arena| {
        find_cache_by_id(arena, cache_id).expect("SearchCatCacheInternal: unknown cache id")
    });
    // The cache-init path opens the catalog relation, which loads the relcache
    // and re-enters the catcache (SearchSysCache); it must therefore run without
    // the arena borrow held. catalog_cache_initialize_cache takes its own short
    // scoped borrows.
    if !with_arena(|arena| arena.caches[cache_idx.0].initialized) {
        init_meta::catalog_cache_initialize_cache(cache_idx)?;
    }

    /*
     * Find the hash bucket in which to look for the tuple, then scan it.
     */
    let probe = with_arena(|arena| -> PgResult<ProbeResult> {
        let cache = &arena.caches[cache_idx.0];
        let kinds: alloc::vec::Vec<cache::catcache::CCFastKind> = cache
            .cc_fastkind
            .iter()
            .take(nkeys as usize)
            .map(|k| k.expect("SearchCatCacheInternal: fastkind not set"))
            .collect();
        let hash_value = CatalogCacheComputeHashValue(&kinds, nkeys, &arguments)?;
        let hash_index = HASH_INDEX(hash_value, cache.cc_nbuckets);

        /*
         * scan the hash bucket until we find a match or exhaust our tuples
         *
         * Note: it's okay to use dlist_foreach here, even though we modify the
         * dlist within the loop, because we don't continue the loop afterwards.
         */
        let bucket = cache.cc_bucket[hash_index].clone();
        let mut found: Option<(CtIdx, bool)> = None;
        for &ct_idx in bucket.iter() {
            let ct = cache.tuples[ct_idx.0]
                .as_ref()
                .expect("bucket references a free slot");

            if ct.dead {
                continue; /* ignore dead entries */
            }

            if ct.hash_value != hash_value {
                continue; /* quickly skip entry if wrong hash val */
            }

            if !CatalogCacheCompareTuple(&kinds, nkeys, &ct.keys, &arguments)? {
                continue;
            }

            found = Some((ct_idx, ct.negative));
            break;
        }

        let Some((ct_idx, negative)) = found else {
            return Ok(ProbeResult::Miss(cache_idx, hash_value, hash_index));
        };

        /*
         * We found a match in the cache.  Move it to the front of the list
         * for its hashbucket, in order to speed subsequent searches.  (The
         * most frequently accessed elements in any hashbucket will tend to
         * be near the front of the hashbucket's list.)
         */
        let cache = &mut arena.caches[cache_idx.0];
        ct_bucket_move_head(&mut cache.cc_bucket[hash_index], ct_idx);

        /*
         * If it's a positive entry, bump its refcount and return it. If it's
         * negative, we can report failure to the caller.
         */
        if !negative {
            /*
             * We want to return a positive entry, so bump its refcount and
             * remember the reference (the latter folds into the immediate
             * release the owned model performs after copying the tuple).
             */
            cache.tuples[ct_idx.0].as_mut().unwrap().refcount += 1;
            Ok(ProbeResult::HitPositive(cache_idx, ct_idx))
        } else {
            Ok(ProbeResult::HitNegative)
        }
    })?;

    match probe {
        ProbeResult::HitPositive(cache_idx, ct_idx) => {
            trace::trace!(
                trace::Category::Catcache,
                "SearchCatCache cache_id={} nkeys={} HIT",
                cache_id,
                nkeys
            );
            let tuple = build_formed_tuple(mcx, cache_idx, ct_idx)?;
            /*
             * The owned model returns a copy; the borrowed reference's life is
             * entirely within this call, so we drop it now (the collapsed
             * `ReleaseCatCache`).
             */
            release_cat_cache(cache_id, ct_idx)?;
            Ok(Some(tuple))
        }
        ProbeResult::HitNegative => {
            trace::trace!(
                trace::Category::Catcache,
                "SearchCatCache cache_id={} nkeys={} HIT-NEGATIVE",
                cache_id,
                nkeys
            );
            Ok(None)
        }
        ProbeResult::Miss(cache_idx, hash_value, hash_index) => {
            trace::trace!(
                trace::Category::Catcache,
                "SearchCatCache cache_id={} nkeys={} MISS (hash={:#x})",
                cache_id,
                nkeys,
                hash_value
            );
            search_cat_cache_miss(
                mcx, cache_id, cache_idx, nkeys, hash_value, hash_index, arguments,
            )
        }
    }
}

/// `SearchCatCacheMiss` — scan the catalog (via seam), enter a positive entry,
/// else a negative entry (unless bootstrap mode). The C stale-retry loop is
/// folded into the scan seam, which returns fresh, flattened tuples.
pub(crate) fn search_cat_cache_miss<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    cache_idx: CacheIdx,
    nkeys: i32,
    hash_value: u32,
    hash_index: usize,
    arguments: [CatKey; 4],
) -> PgResult<Option<FormedTuple<'mcx>>> {
    /*
     * Tuple was not found in cache, so we have to try to retrieve it directly
     * from the relation.  If found, we will add it to the cache; if not found,
     * we will add a negative cache entry instead.
     *
     * The C `table_open` + `systable_beginscan` + `systable_getnext` loop with
     * its stale-retry is owned by the genam/heaptoast/relcache substrate and
     * crosses the scan seam, which returns the matched, flattened tuples (at
     * most one for a single-tuple cache).
     */
    let fetched = catcache_scan_single::call(cache_id, nkeys, &arguments)?;

    if let Some(fetched) = fetched.into_iter().next() {
        /*
         * We found a tuple, so add it to the cache.
         */
        let ct_idx = with_arena(|arena| {
            graph_machinery::create_entry_positive(
                arena, cache_idx, fetched, hash_value, hash_index,
            )
        })?;

        /*
         * immediately bump the refcount and remember the reference (folded into
         * the immediate release below), then build the caller copy.
         */
        with_arena(|arena| {
            arena.caches[cache_idx.0].tuples[ct_idx.0]
                .as_mut()
                .unwrap()
                .refcount += 1;
        });
        let tuple = build_formed_tuple(mcx, cache_idx, ct_idx)?;
        release_cat_cache(cache_id, ct_idx)?;
        return Ok(Some(tuple));
    }

    /*
     * If tuple was not found, we need to build a negative cache entry
     * containing a fake tuple.  The fake tuple has the correct key columns,
     * but nulls everything else.
     *
     * In bootstrap mode, we don't build negative entries, because the cache
     * invalidation mechanism isn't alive and can't clear them if the tuple
     * gets created later.  (Bootstrap doesn't do UPDATEs, so it doesn't need
     * cache inval for that.)
     */
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(None);
    }

    with_arena(|arena| {
        graph_machinery::create_entry_negative(
            arena, cache_idx, arguments, hash_value, hash_index,
        )
    })?;

    Ok(None)
}

/// Copy a live positive cache tuple into `mcx` as a `FormedTuple` (the C
/// `&ct->tuple` borrow, materialized as the owned copy callers keep).
pub(crate) fn build_formed_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    cache_idx: CacheIdx,
    ct_idx: CtIdx,
) -> PgResult<FormedTuple<'mcx>> {
    let (t_len, t_self, t_tableoid, t_data) = with_arena(|arena| {
        let ct = arena.caches[cache_idx.0].tuples[ct_idx.0]
            .as_ref()
            .expect("build_formed_tuple: free slot");
        (ct.t_len, ct.t_self, ct.t_tableoid, ct.t_data.clone())
    });
    catcache_form_cached_tuple::call(mcx, t_len, t_self, t_tableoid, &t_data)
}

/* ----------------------------------------------------------------------------
 * Outward seam shapes — `SearchCatCache{,1..4}` and `GetCatCacheHashValue`.
 * ------------------------------------------------------------------------- */

/// `SearchCatCache(cache, v1, v2, v3, v4)`.
pub fn search_cat_cache<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
    v4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    /* Make sure we're in an xact, even if this ends up being a cache hit */
    // (Assert(IsTransactionState()) — checked by the caller's xact state.)
    let nkeys = with_arena(|arena| {
        let idx = find_cache_by_id(arena, cache_id).expect("SearchCatCache: unknown cache id");
        arena.caches[idx.0].cc_nkeys
    });
    search_cat_cache_internal(mcx, cache_id, nkeys, v1, v2, v3, v4)
}

/// `SearchCatCache1(cache, v1)`.
pub fn search_cat_cache_1<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    v1: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Assert(cache->cc_nkeys == 1)
    search_cat_cache_internal(
        mcx,
        cache_id,
        1,
        v1,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SearchCatCache2(cache, v1, v2)`.
pub fn search_cat_cache_2<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Assert(cache->cc_nkeys == 2)
    search_cat_cache_internal(
        mcx,
        cache_id,
        2,
        v1,
        v2,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SearchCatCache3(cache, v1, v2, v3)`.
pub fn search_cat_cache_3<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Assert(cache->cc_nkeys == 3)
    search_cat_cache_internal(mcx, cache_id, 3, v1, v2, v3, SysCacheKey::UNUSED)
}

/// `SearchCatCache4(cache, v1, v2, v3, v4)`.
pub fn search_cat_cache_4<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
    v4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Assert(cache->cc_nkeys == 4)
    search_cat_cache_internal(mcx, cache_id, 4, v1, v2, v3, v4)
}

/// `GetCatCacheHashValue(cache, v1..v4)` — initialize the cache if needed and
/// hash the search keys.
pub fn get_cat_cache_hash_value(
    cache_id: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
    v4: SysCacheKey<'_>,
) -> PgResult<u32> {
    let arguments = build_arguments(v1, v2, v3, v4);

    /*
     * one-time startup overhead for each cache
     */
    let cache_idx = with_arena(|arena| {
        find_cache_by_id(arena, cache_id).expect("GetCatCacheHashValue: unknown cache id")
    });
    // Init opens the catalog relation (relcache re-entrancy); must run without
    // the arena borrow held.
    if !with_arena(|arena| arena.caches[cache_idx.0].initialized) {
        init_meta::catalog_cache_initialize_cache(cache_idx)?;
    }

    with_arena(|arena| {
        /*
         * calculate the hash value
         */
        let cache = &arena.caches[cache_idx.0];
        let nkeys = cache.cc_nkeys;
        let kinds: alloc::vec::Vec<cache::catcache::CCFastKind> = cache
            .cc_fastkind
            .iter()
            .take(nkeys as usize)
            .map(|k| k.expect("GetCatCacheHashValue: fastkind not set"))
            .collect();
        CatalogCacheComputeHashValue(&kinds, nkeys, &arguments)
    })
}

/* ----------------------------------------------------------------------------
 * ReleaseCatCache (internal arena operation; the copy collapses caller-side
 * release, but the owner still decrements after building the copy).
 * ------------------------------------------------------------------------- */

/// `ReleaseCatCache(tuple)` — decrement the refcount grabbed by a successful
/// search, forget the resource-owner reference, and remove the entry if it is
/// now dead and unreferenced.
pub(crate) fn release_cat_cache(cache_id: i32, ct_idx: CtIdx) -> PgResult<()> {
    with_arena(|arena| {
        let cache_idx =
            find_cache_by_id(arena, cache_id).expect("ReleaseCatCache: unknown cache id");
        let cache = &mut arena.caches[cache_idx.0];
        let ct = cache.tuples[ct_idx.0]
            .as_mut()
            .expect("ReleaseCatCache: free slot");

        // Assert(ct->ct_magic == CT_MAGIC);
        // Assert(ct->refcount > 0);
        ct.refcount -= 1;

        /*
         * ResourceOwnerForgetCatCacheRef collapses in the owned model: the
         * borrowed reference never escaped this call.
         */

        let dead = ct.dead;
        let refcount = ct.refcount;
        let in_list = !ct.c_list.is_none();

        // #ifndef CATCACHE_FORCE_RELEASE: the debug-discard path is omitted.
        if dead && refcount == 0 && !in_list {
            CatCacheRemoveCTup(arena, cache_idx, ct_idx);
        }
    });
    Ok(())
}

/// Reuse-or-create a member entry from a fetched tuple (shared with the list
/// path); returns the entry's arena index.
pub(crate) fn reuse_or_create_entry(
    cache_idx: CacheIdx,
    fetched: &FetchedCatalogTuple,
) -> PgResult<CtIdx> {
    /*
     * See if there's an existing entry that matches the new tuple (an entry
     * for the same key with the same `t_self`); if so, reuse it and bump its
     * refcount.  Otherwise create a fresh positive entry.  This mirrors the
     * member-reuse loop of `SearchCatCacheList`.
     */
    with_arena(|arena| {
        let hash_value = {
            let cache = &arena.caches[cache_idx.0];
            let nkeys = cache.cc_nkeys;
            let kinds: alloc::vec::Vec<cache::catcache::CCFastKind> =
                cache
                    .cc_fastkind
                    .iter()
                    .take(nkeys as usize)
                    .map(|k| k.expect("reuse_or_create_entry: fastkind not set"))
                    .collect();
            CatalogCacheComputeHashValue(&kinds, nkeys, &fetched.keys)?
        };

        let cache = &arena.caches[cache_idx.0];
        let hash_index = HASH_INDEX(hash_value, cache.cc_nbuckets);
        let bucket = cache.cc_bucket[hash_index].clone();
        for &ct_idx in bucket.iter() {
            let ct = cache.tuples[ct_idx.0].as_ref().unwrap();
            if ct.dead || ct.negative {
                continue;
            }
            if ct.hash_value != hash_value {
                continue;
            }
            if ct.t_self != fetched.t_self {
                continue; /* not same tuple */
            }
            /*
             * Found a match, but can't use it if it belongs to another list
             * already (C: `if (ct->c_list) continue;`).
             */
            if !ct.c_list.is_none() {
                continue;
            }
            /* Found a match — reuse it (bump refcount). */
            arena.caches[cache_idx.0].tuples[ct_idx.0]
                .as_mut()
                .unwrap()
                .refcount += 1;
            return Ok(ct_idx);
        }

        /* No match — create a new positive entry and bump its refcount. */
        let ct_idx = graph_machinery::create_entry_positive(
            arena,
            cache_idx,
            fetched.clone(),
            hash_value,
            hash_index,
        )?;
        arena.caches[cache_idx.0].tuples[ct_idx.0]
            .as_mut()
            .unwrap()
            .refcount += 1;
        Ok(ct_idx)
    })
}

/* ----------------------------------------------------------------------------
 * Substrate seam bodies — `SearchCatCacheMiss`'s catalog scan + the cached-tuple
 * copy + the by-reference search-key reconstitution. These are the genam /
 * heaptuple / relcache-facing halves the catcache crate declares (above) and
 * installs from `wiring::init_seams` (it depends on genam-seams, the relcache
 * seams, and the real heaptuple crate, so the scan path closes here without a
 * dependency cycle: neither genam nor heaptuple depends back on the catcache).
 * ------------------------------------------------------------------------- */

/// `catcache_scan_single` — `SearchCatCacheMiss`'s catalog scan
/// (`catcache.c`): `table_open(cache->cc_reloid, AccessShareLock)` +
/// `systable_beginscan(cache->cc_indexoid, IndexScanOK(cache), nkeys, cur_skey)`
/// + the `systable_getnext` loop (`break` after the first match — a single-tuple
/// cache has at most one) + `heap_copytuple`/detoast/flatten + key extraction,
/// returning the matched tuple as a [`FetchedCatalogTuple`].
///
/// The C `do { ... } while (stale)` retry (a tuple going stale during
/// `CatalogCacheCreateEntry`'s detoast) is folded into this seam: the relcache
/// scan returns the flattened tuple eagerly, so the carrier the catcache caches
/// is already a consistent post-detoast image and no second scan is needed.
fn catcache_scan_single_impl(
    cache_id: i32,
    nkeys: i32,
    arguments: &[CatKey; 4],
) -> PgResult<alloc::vec::Vec<FetchedCatalogTuple>> {
    use cache::catcache::CATCACHE_MAXKEYS;
    // The canonical by-value Datum the keystone-owned `ScanKeyData.sk_argument`
    // carries (the per-search scalar word crosses into its by-value arm).
    use types_tuple::heaptuple::Datum as DatumV;
    use genam_seams as genam;

    let cache_idx = with_arena(|arena| {
        find_cache_by_id(arena, cache_id).expect("catcache_scan_single: unknown cache id")
    });

    // Read the per-cache scan inputs (reloid, indexoid, scankey template) out of
    // the arena up front. Phase-2 init has already populated cc_skey for all
    // cc_nkeys (the caller ran CatalogCacheInitializeCache before this).
    let (cc_reloid, cc_indexoid, cc_fastkind, skey_tmpl) = with_arena(|arena| {
        let cache = &arena.caches[cache_idx.0];
        (
            cache.cc_reloid,
            cache.cc_indexoid,
            cache.cc_fastkind,
            skey_template(cache),
        )
    });

    // The catalog scan + its result tuples live in a scratch context (C's
    // CurrentMemoryContext during the scan); the bytes we keep are copied into
    // owned `FetchedCatalogTuple`s, so the scratch is dropped when the scan ends.
    // Declared before `cur_skey` so it outlives the by-reference `sk_argument`s
    // that borrow it.
    let scratch = mcx::MemoryContext::new("SearchCatCacheMiss scan");
    let scan_mcx = scratch.mcx();
    let mut cur_skey: [types_scan::scankey::ScanKeyData<'_>;
        cache::catcache::CATCACHE_MAXKEYS] =
        core::array::from_fn(|i| skey_tmpl[i].clone());

    // relation = table_open(cache->cc_reloid, AccessShareLock);
    // The catalog must be locked (AccessShareLock) before the scan: genam's
    // systable_beginscan re-opens the heap relation with NoLock, asserting the
    // caller already holds the lock (the C `table_open(cc_reloid, AccessShareLock)`).
    let relation = common_relation::relation_open(
        scan_mcx,
        cc_reloid,
        types_storage::lock::AccessShareLock,
    )?;

    // memcpy(cur_skey, cache->cc_skey, sizeof(ScanKeyData) * nkeys);
    // cur_skey[0..nkeys].sk_argument = v1..vN;
    // A by-value key becomes the canonical by-value `Datum` word; a by-reference
    // key (name/text/oidvector) becomes a `Datum::ByRef` over the payload bytes
    // (the pointer-bearing `Datum` C's scan key already carries), allocated in
    // the scan's scratch context. genam's scankey decode (genam:204) handles both.
    //
    // In C the search-key `Datum` the caller passed is already framed for the
    // type the index opclass compares against (`CStringGetTextDatum`/
    // `NameGetDatum`/...), so the index AM's comparator (`texteq`/`nameeq`/
    // `oidvectoreq`, reached via fmgr) reads it correctly. The catcache's owned
    // `CatKey::ByRef` instead stores the *resolved* in-memory payload the
    // computational-core fast functions consume (name: NUL-significant bytes;
    // text: the `VARDATA_ANY` header-less image; oidvector: the `Oid` element
    // bytes). The catalog scan crosses the fmgr by-ref lane, so each by-reference
    // argument must be re-framed into the on-disk `Datum` image the index
    // comparator expects, keyed on the column's `CCFastKind`:
    //   * Name  -> a raw NAMEDATALEN (64-byte) NUL-padded buffer (NameData), no
    //     varlena header (`arg_name` reads the fixed buffer; framing it as a
    //     varlena would make `nameout`/`namecmp` read a stray header byte).
    //   * Text  -> a 4-byte-header (`VARATT_IS_4B_U`) varlena image so the fmgr
    //     `texteq` adapter's `VARDATA_ANY` strips the header back to the payload
    //     (a header-less raw image makes the adapter misread the first data byte
    //     as a length header and drop bytes -> the scan never matches).
    //   * OidVector -> already the contiguous `Oid` element image the comparator
    //     deforms; passed through verbatim.
    for i in 0..(nkeys as usize) {
        cur_skey[i].sk_argument = match &arguments[i] {
            CatKey::Scalar(w) => DatumV::ByVal(w.as_usize()),
            CatKey::ByRef(bytes) => {
                let image = frame_byref_scankey_arg(cc_fastkind[i], bytes);
                DatumV::ByRef(mcx::slice_in(scan_mcx, &image)?)
            }
        };
    }

    // scandesc = systable_beginscan(relation, cache->cc_indexoid,
    //                               IndexScanOK(cache), NULL, nkeys, cur_skey);
    let index_ok = init_meta::IndexScanOK(cache_id)?;
    let mut guard = genam::systable_beginscan::call(
        &relation,
        cc_indexoid,
        index_ok,
        None,
        &cur_skey[..nkeys as usize],
    )?;

    // while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) { ...; break; }
    // — a single-tuple cache assumes only one match, so we stop after the first.
    let mut out: alloc::vec::Vec<FetchedCatalogTuple> = alloc::vec::Vec::new();
    if let Some(ntp) = genam::systable_getnext::call(scan_mcx, guard.desc_mut())? {
        let fetched = list_path::build_fetched(scan_mcx, cache_idx, &ntp)?;
        out.push(fetched);
        // break; /* assume only one match */
    }

    // systable_endscan(scandesc); table_close(relation, AccessShareLock);
    guard.end()?;
    relation.close(types_storage::lock::AccessShareLock)?;

    let _ = CATCACHE_MAXKEYS;
    Ok(out)
}

/// `catcache_form_cached_tuple` — `heap_copytuple(&ct->tuple)` into `mcx`: decode
/// the cached tuple's full on-disk byte image (header + null bitmap + pad + user
/// data, the bytes [`list_path::build_fetched`] stored) back into an owned
/// [`FormedTuple`], preserving the source tuple's `t_self`/`t_tableOid`.
fn catcache_form_cached_tuple_impl<'mcx>(
    mcx: Mcx<'mcx>,
    t_len: u32,
    t_self: ItemPointer,
    t_tableoid: Oid,
    t_data: &[u8],
) -> PgResult<FormedTuple<'mcx>> {
    let ip = types_tuple::heaptuple::ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData::new(t_self.block),
        ip_posid: t_self.offset,
    };
    heaptuple::heap_copytuple_from_disk_image(
        mcx, t_len, ip, t_tableoid, t_data,
    )
}

/// Re-frame a by-reference catcache search key (the in-memory `CatKey::ByRef`
/// payload the computational-core fast functions consume) into the on-disk
/// `Datum` image the catalog index opclass comparator expects across the fmgr
/// by-ref lane. The framing is determined by the key column's [`CCFastKind`]:
///
///  * [`CCFastKind::Name`] — a raw NAMEDATALEN-wide (64-byte) NUL-padded
///    `NameData` buffer (no varlena header). The stored payload is the
///    NUL-significant bytes (`<= NAMEDATALEN`); pad it back out to the fixed
///    width so `nameeq`/`btnamecmp` read a proper `Name`.
///  * [`CCFastKind::Text`] — a 4-byte-header uncompressed (`VARATT_IS_4B_U`)
///    varlena image: `SET_VARSIZE(buf, len + VARHDRSZ)` (low two bits `00`,
///    little/native-endian `(len + VARHDRSZ) << 2`) over the header, then the
///    payload bytes; `texteq`'s `VARDATA_ANY` strips it back.
///  * Other kinds (`OidVector` and the scalar kinds, which never reach here as
///    a `ByRef`) pass through verbatim — the `Oid` element image the
///    `oidvectoreq` comparator deforms.
fn frame_byref_scankey_arg(
    kind: Option<cache::catcache::CCFastKind>,
    bytes: &[u8],
) -> alloc::vec::Vec<u8> {
    use cache::catcache::CCFastKind;
    const NAMEDATALEN: usize = 64;
    const VARHDRSZ: usize = 4;
    match kind {
        Some(CCFastKind::Name) => {
            // Raw fixed-width NameData: copy the significant bytes (capped at
            // NAMEDATALEN) into a zeroed 64-byte buffer, NUL-padded.
            let mut buf = alloc::vec![0u8; NAMEDATALEN];
            let n = bytes.len().min(NAMEDATALEN);
            buf[..n].copy_from_slice(&bytes[..n]);
            buf
        }
        Some(CCFastKind::Text) => {
            // 4-byte-header uncompressed varlena: SET_VARSIZE(buf, len+VARHDRSZ).
            let total = bytes.len() + VARHDRSZ;
            let tagged = (total as u32) << 2; // VARATT_IS_4B_U: low bits 00.
            let mut buf = alloc::vec::Vec::with_capacity(total);
            buf.extend_from_slice(&tagged.to_ne_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        _ => bytes.to_vec(),
    }
}

/// `memcpy(cur_skey, cache->cc_skey, sizeof(cur_skey))` — the scankey template
/// the scan stamps its per-search arguments into (the `cc_skey` slots are all
/// populated by phase-2 init by the time a miss scans). `sk_argument` is reset
/// to a fresh by-value NULL (immediately overwritten by the stamping loop),
/// which decouples the returned key's `'mcx` from the borrowed cache.
fn skey_template<'a>(
    cache: &cache::catcache::ArenaCatCache,
) -> [types_scan::scankey::ScanKeyData<'a>;
    cache::catcache::CATCACHE_MAXKEYS] {
    use types_tuple::heaptuple::Datum as DatumV;
    core::array::from_fn(|i| match &cache.cc_skey[i] {
        Some(k) => types_scan::scankey::ScanKeyData {
            sk_flags: k.sk_flags,
            sk_attno: k.sk_attno,
            sk_strategy: k.sk_strategy,
            sk_subtype: k.sk_subtype,
            sk_collation: k.sk_collation,
            sk_func: k.sk_func.clone(),
            sk_argument: DatumV::null(),
            sk_subkeys: None,
        },
        None => types_scan::scankey::ScanKeyData::empty(),
    })
}

/// Install the three substrate seams the catcache miss path calls (declared
/// above). Called from [`crate::wiring::init_seams`].
pub(crate) fn install_substrate_seams() {
    catcache_scan_single::set(catcache_scan_single_impl);
    catcache_form_cached_tuple::set(catcache_form_cached_tuple_impl);
}
