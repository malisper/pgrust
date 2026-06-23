//! The partial-key list search path (`catcache.c`): `SearchCatCacheList`,
//! `SearchCatCacheList`'s miss/build body, and `ReleaseCatCacheList`.
//!
//! In the owned model the list search returns the member tuples as a
//! `PgVec<FormedTuple>` (each member copied into the caller's `mcx`, in scan
//! order); release of the C `CatCList` refcount is folded into the copy, as for
//! the single searches. The create-in-progress stack guards against a
//! concurrent invalidation arriving mid-build.

extern crate alloc;

use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use cache::catcache::{
    ArenaCatCList, CacheIdx, ClIdx, CtIdx, FetchedCatalogTuple, ItemPointer, CATCACHE_MAXKEYS,
    CL_MAGIC,
};
use cache::SysCacheKey;
use cache::catcache::CatKey;
// Bare-word machine-word `Datum` (`datum::Datum`), aliased `ScalarWord`:
// the by-value scalar word a `CatKey::Scalar` carries (C's by-value key `Datum`).
use datum::Datum as ScalarWord;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ScanKeyData.sk_argument` carries.
use types_tuple::heaptuple::Datum as DatumV;
use types_error::PgResult;
use types_scan::scankey::ScanKeyData;
use types_tuple::heaptuple::{FormedTuple, Datum};

use genam_seams as genam;

use crate::core_compute::{
    CatalogCacheCompareTuple, CatalogCacheComputeHashValue, HASH_INDEX,
};
use crate::graph_machinery::{
    cl_alloc, cl_bucket_move_head, cl_bucket_push_head, rehash_cat_cache_lists,
    CatCacheRemoveCList, CatCacheRemoveCTup,
};
use crate::{find_cache_by_id, with_arena};

/// One probe outcome from the list-bucket scan.
pub(crate) enum ListProbe {
    Hit(CacheIdx, ClIdx),
    Miss(CacheIdx, u32, usize),
}

/// Resolve a [`SysCacheKey`] into the [`CatKey`] stored in the entry's `keys[]`
/// and passed to the hash and comparison functions. C passes a bare `Datum`;
/// by-value keys keep the scalar word, by-reference keys (name/text/oidvector)
/// carry their resolved payload bytes (the pointer-bearing `Datum` C holds).
#[inline]
fn key_datum(k: SysCacheKey<'_>) -> CatKey {
    match k {
        SysCacheKey::Value(d) => CatKey::Scalar(d),
        SysCacheKey::Str(s) => CatKey::ByRef(s.as_bytes().to_vec()),
        SysCacheKey::Bytes(b) => CatKey::ByRef(b.to_vec()),
    }
}

/// `SearchCatCacheList(cache, nkeys, v1, v2, v3)` — build (or hit) the list of
/// all tuples matching a partial key, returning member copies in `mcx`.
pub fn search_cat_cache_list<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    nkeys: i32,
    v1: SysCacheKey<'_>,
    v2: SysCacheKey<'_>,
    v3: SysCacheKey<'_>,
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    // Datum arguments[CATCACHE_MAXKEYS];  arguments[0..2] = v1..v3, rest unused.
    let arguments: [CatKey; CATCACHE_MAXKEYS] = [
        key_datum(v1),
        key_datum(v2),
        key_datum(v3),
        CatKey::scalar_null(),
    ];

    // one-time startup overhead to lookup index info
    // if (unlikely(cache->cc_tupdesc == NULL)) CatalogCacheInitializeCache(cache);
    // This opens the catalog relation (relcache re-entrancy into the catcache),
    // so it must run without the arena borrow held.
    let cache_idx = with_arena(|arena| {
        find_cache_by_id(arena, cache_id).expect("SearchCatCacheList: unknown cache id")
    });
    if !with_arena(|arena| arena.caches[cache_idx.0].initialized) {
        crate::init_meta::catalog_cache_initialize_cache(cache_idx)?;
    }

    // Validate nkeys and compute the list hash value + bucket under the arena.
    let (cache_idx, l_hash_value, l_hash_index) = with_arena(|arena| -> PgResult<_> {
        let cache = &arena.caches[cache_idx.0];

        // Assert(nkeys > 0 && nkeys < cache->cc_nkeys);
        assert!(nkeys > 0 && nkeys < cache.cc_nkeys);

        // find the hash value for the supplied keys
        let kinds = fastkinds(cache);
        let l_hash_value = CatalogCacheComputeHashValue(&kinds, nkeys, &arguments)?;

        // scan the cache for a matching list, if cc_nlbuckets is nonzero
        let l_hash_index = if cache.cc_nlbuckets > 0 {
            HASH_INDEX(l_hash_value, cache.cc_nlbuckets)
        } else {
            0
        };

        Ok((cache_idx, l_hash_value, l_hash_index))
    })?;

    // Probe the list bucket for a live, matching list (the hot path).
    let hit = with_arena(|arena| -> PgResult<Option<ClIdx>> {
        let cache = &arena.caches[cache_idx.0];
        if cache.cc_nlbuckets == 0 {
            return Ok(None);
        }
        let kinds = fastkinds(cache);

        // dlist_foreach(iter, &cache->cc_lbucket[lHashIndex])  (front -> back)
        let bucket = cache.cc_lbucket[l_hash_index].clone();
        for cl_idx in bucket {
            let cl = arena.caches[cache_idx.0].lists[cl_idx.0]
                .as_ref()
                .expect("live list bucket entry");

            if cl.dead {
                continue; // ignore dead entries
            }
            if cl.hash_value != l_hash_value {
                continue; // quickly skip entries with different hashes
            }
            // see if the cached list matches the requested keys
            if cl.nkeys as i32 != nkeys {
                continue;
            }
            if !CatalogCacheCompareTuple(&kinds, nkeys, &cl.keys, &arguments)? {
                continue;
            }
            return Ok(Some(cl_idx));
        }
        Ok(None)
    })?;

    if let Some(cl_idx) = hit {
        // We found a matching list.  Move the list to the front of the list
        // for its hashbucket, so as to speed subsequent searches.  (We do not
        // move the members to the fronts of their hashbucket lists, however,
        // since there's no point in that unless they are searched for
        // individually.)
        with_arena(|arena| {
            cl_bucket_move_head(
                &mut arena.caches[cache_idx.0].cc_lbucket[l_hash_index],
                cl_idx,
            );

            // Bump the list's refcount and stamp it as touched this transaction
            // (in C: ResourceOwnerEnlarge + cl->refcount++ +
            // ResourceOwnerRememberCatCacheListRef). C does NOT assert
            // refcount > 0 here: an unreferenced-but-live cached list correctly
            // sits at refcount 0 between uses, so the bump on a cache hit starts
            // from 0. (The `refcount > 0` assert belongs to ReleaseCatCacheList,
            // which runs after this bump.)
            let cl = arena.caches[cache_idx.0].lists[cl_idx.0]
                .as_mut()
                .expect("live list");
            cl.refcount += 1;
        });

        // Materialize the member copies into the caller's mcx, then drop the
        // reference we just took (the owned model folds ReleaseCatCacheList
        // into the copy).
        let members = build_list_members(mcx, cache_idx, cl_idx)?;
        release_cat_cache_list(cache_id, cl_idx)?;
        return Ok(members);
    }

    // List was not found in cache, so we have to build it by reading the
    // relation.  For each matching tuple found in the relation, use an existing
    // cache entry if possible, else build a new one.
    search_cat_cache_list_miss(
        mcx,
        cache_id,
        cache_idx,
        nkeys,
        l_hash_value,
        l_hash_index,
        arguments,
    )
}

/// Build the list on a miss: scan the catalog, reuse/create member entries,
/// then assemble and link the `CatCList`. Mirrors the C `PG_TRY` body with the
/// in-progress stack guarding concurrent invalidation.
pub(crate) fn search_cat_cache_list_miss<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    cache_idx: CacheIdx,
    nkeys: i32,
    l_hash_value: u32,
    l_hash_index: usize,
    arguments: [CatKey; 4],
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    // Read the per-cache scan inputs (reloid, indexoid, scankeys, nbuckets,
    // nkeys) out of the arena up front.
    let (cc_reloid, cc_indexoid, cc_nkeys, cc_nbuckets, skey_tmpl) =
        with_arena(|arena| {
            let cache = &arena.caches[cache_idx.0];
            (
                cache.cc_reloid,
                cache.cc_indexoid,
                cache.cc_nkeys,
                cache.cc_nbuckets,
                // memcpy(cur_skey, cache->cc_skey, sizeof(cur_skey));
                skey_template(cache),
            )
        });

    // CatCInProgress in_progress_ent;  push it (list = true) before we start.
    // The unwinding (pop + cleanup of half-built members) is handled below on
    // every error return via the scope guard.
    let depth = with_arena(|arena| {
        crate::graph_machinery::push_in_progress(arena, cache_idx, l_hash_value, true)
    });

    // The PG_TRY body. We collect the member tuple handles (each holding a
    // temporary refcount) into `ctlist`; on any error we undo those refs (the
    // PG_CATCH body) before re-throwing.
    let result = build_list_body(
        cache_id,
        cache_idx,
        nkeys,
        l_hash_value,
        l_hash_index,
        cc_reloid,
        cc_indexoid,
        cc_nkeys,
        cc_nbuckets,
        &skey_tmpl,
        arguments,
    );

    let cl_idx = match result {
        Ok(cl_idx) => cl_idx,
        Err((members, err)) => {
            // PG_CATCH(): drop the temp refs we took on each member, freeing any
            // entries that have now become unreferenced.
            undo_member_refs(cache_idx, &members)?;
            // catcache_in_progress_stack = save_in_progress; (pop)
            with_arena(|arena| {
                let _dead = crate::graph_machinery::pop_in_progress(arena);
                assert_eq!(arena.in_progress.len(), depth - 1);
            });
            return Err(err);
        }
    };

    // catcache_in_progress_stack = save_in_progress; (pop, success path)
    with_arena(|arena| {
        let _dead = crate::graph_machinery::pop_in_progress(arena);
        assert_eq!(arena.in_progress.len(), depth - 1);
    });

    // Bump the new list's refcount (the search holds a reference), then
    // materialize the member copies and drop the reference (folding
    // ReleaseCatCacheList into the copy as the single searches do).
    with_arena(|arena| {
        let cl = arena.caches[cache_idx.0].lists[cl_idx.0]
            .as_mut()
            .expect("freshly built list");
        cl.refcount += 1;
    });

    let members = build_list_members(mcx, cache_idx, cl_idx)?;
    release_cat_cache_list(cache_id, cl_idx)?;
    Ok(members)
}

/// The `PG_TRY` body of `SearchCatCacheList`: scan the catalog, reuse/create
/// each member entry (holding a temp ref), then assemble and link the
/// `CatCList`. On error, returns the partially-built member list so the caller
/// can run the `PG_CATCH` undo.
#[allow(clippy::type_complexity)]
fn build_list_body(
    cache_id: i32,
    cache_idx: CacheIdx,
    nkeys: i32,
    l_hash_value: u32,
    l_hash_index: usize,
    cc_reloid: types_core::Oid,
    cc_indexoid: types_core::Oid,
    cc_nkeys: i32,
    cc_nbuckets: i32,
    skey_tmpl: &[ScanKeyData<'_>; CATCACHE_MAXKEYS],
    arguments: [CatKey; 4],
) -> Result<ClIdx, (Vec<CtIdx>, types_error::PgError)> {
    // ctlist = NIL; nmembers = 0; ordered = false;
    let mut ctlist: Vec<CtIdx> = Vec::new();
    let mut ordered = false;

    // Scan the table for matching entries.  If an invalidation arrives
    // mid-build, we loop back here to retry (C wraps the scan in
    //   do { ... } while (in_progress_ent.dead); ).
    loop {
        // If we are retrying, release the temp refcounts on any items created on
        // the previous iteration.  We dare not try to free them if they're now
        // unreferenced, since an error while doing that would result in the
        // PG_CATCH below doing extra refcount decrements.  Besides, we'll likely
        // re-adopt those items in the next iteration, so it's not worth
        // complicating matters to try to get rid of them.
        //   foreach(ctlist_item, ctlist) {
        //       ct = lfirst(ctlist_item);
        //       Assert(ct->c_list == NULL);
        //       Assert(ct->refcount > 0);
        //       ct->refcount--;
        //   }
        if !ctlist.is_empty() {
            with_arena(|arena| {
                for &ct_idx in &ctlist {
                    let ct = arena.caches[cache_idx.0].tuples[ct_idx.0]
                        .as_mut()
                        .expect("member tuple");
                    assert!(ct.c_list.is_none());
                    assert!(ct.refcount > 0);
                    ct.refcount -= 1;
                }
            });
        }
        // Reset ctlist in preparation for new try; in_progress_ent.dead = false.
        ctlist.clear();
        with_arena(|arena| {
            crate::graph_machinery::reset_in_progress_top_dead(arena);
        });

        // Run the catalog scan, populating ctlist with member handles each
        // holding a temp refcount. A scan/relation error is mapped into the
        // PG_CATCH return. (The repo's `scan_members` does table_open /
        // beginscan / the loop / endscan / table_close per call, so a restart
        // re-opens and re-scans the relation from scratch — the same observable
        // "retry the whole scan" outcome as the C do/while.)
        match scan_members(
            cache_id,
            cache_idx,
            nkeys,
            cc_reloid,
            cc_indexoid,
            cc_nbuckets,
            skey_tmpl,
            &arguments,
            &mut ctlist,
            &mut ordered,
        ) {
            Ok(()) => {}
            Err(err) => return Err((ctlist, err)),
        }

        // } while (in_progress_ent.dead);  — restart if a concurrent
        // invalidation marked the in-progress list build dead during the scan.
        let dead = with_arena(|arena| crate::graph_machinery::in_progress_top_dead(arena));
        if !dead {
            break;
        }
    }

    // Now we can build the CatCList entry.  First we need a dummy tuple
    // containing the key values...  (in the owned arena the list simply records
    // the key datums directly). Allocate it in the cache's slots.
    let nmembers = ctlist.len();

    let assemble = with_arena(|arena| -> PgResult<ClIdx> {
        // oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
        //
        // cl = (CatCList *) palloc(...); plus the members[] array.
        let mut cl = ArenaCatCList {
            cl_magic: CL_MAGIC,
            hash_value: l_hash_value,
            // cl->cache_elem is linked into the bucket below.
            // cl->refcount = 0; will be bumped by the caller (the search ref).
            refcount: 0,
            dead: false,
            ordered,
            nkeys: nkeys as i16,
            // c_list / my_cache:
            my_cache: cache_idx,
            n_members: nmembers as i32,
            members: ctlist.clone(),
            // keys[] = the search arguments (CatCacheCopyKeys in C).
            keys: arguments,
        };
        // The fields not in the literal above default to their `CL_MAGIC`-correct
        // values; set them explicitly for clarity above.
        let _ = &mut cl;

        // Before linking we may need to grow / first-allocate the list buckets:
        // if (cache->cc_nlbuckets == 0) -> allocate; then if too full, rehash.
        let cache = &mut arena.caches[cache_idx.0];
        if cache.cc_nlbuckets == 0 {
            // CatCacheInitializeListBuckets: start with 16 list buckets.
            cache.cc_nlbuckets = 16;
            cache.cc_lbucket = alloc::vec![Vec::new(); cache.cc_nlbuckets as usize];
        }

        // cl->my_cache = cache; (already set)
        let cl_idx = cl_alloc(cache, cl);

        // dlist_push_head(&cache->cc_lbucket[lHashIndex], &cl->cache_elem);
        // recompute the index against the (possibly freshly allocated) bucket
        // count.
        let idx = HASH_INDEX(l_hash_value, cache.cc_nlbuckets);
        let _ = l_hash_index; // computed against the pre-grow count in the probe
        cl_bucket_push_head(&mut cache.cc_lbucket[idx], cl_idx);

        // cache->cc_nlist++;
        cache.cc_nlist += 1;

        // Finally, set each member tuple's c_list pointer to the new list, and
        // drop the temporary refcount we held during the build (it is now
        // accounted for by the list's ownership). The C code does:
        //   foreach(ctlc, ctlist) {
        //       ct = lfirst(ctlc);
        //       Assert(ct->c_list == NULL);
        //       ct->c_list = cl;
        //       /* release the temporary refcount we used while building */
        //       ct->refcount--;
        //       /* mark list dead if any members already dead */
        //       if (ct->dead)
        //           cl->dead = true;
        //   }
        let mut any_member_dead = false;
        for &ct_idx in &ctlist {
            let ct = arena.caches[cache_idx.0].tuples[ct_idx.0]
                .as_mut()
                .expect("member tuple");
            assert!(ct.c_list.is_none());
            ct.c_list = cl_idx;
            ct.refcount -= 1;
            /* mark list dead if any members already dead */
            if ct.dead {
                any_member_dead = true;
            }
        }
        if any_member_dead {
            arena.caches[cache_idx.0].lists[cl_idx.0]
                .as_mut()
                .expect("freshly created list")
                .dead = true;
        }

        // If the list bucket is now overfull, double it.
        // if (cache->cc_nlist > cache->cc_nlbuckets * 2) RehashCatCacheLists(cache);
        let cache = &mut arena.caches[cache_idx.0];
        if cache.cc_nlist > cache.cc_nlbuckets * 2 {
            rehash_cat_cache_lists(cache)?;
        }

        Ok(cl_idx)
    });

    let _ = (cc_nkeys, cache_id);

    match assemble {
        Ok(cl_idx) => Ok(cl_idx),
        Err(err) => Err((ctlist, err)),
    }
}

/// The catalog scan portion of the list build: open the catalog, set the
/// scankeys, scan, and for each matching tuple reuse-or-create a cache member
/// entry holding a temporary refcount. Mirrors the C `systable_beginscan` loop.
#[allow(clippy::too_many_arguments)]
fn scan_members(
    cache_id: i32,
    cache_idx: CacheIdx,
    nkeys: i32,
    cc_reloid: types_core::Oid,
    cc_indexoid: types_core::Oid,
    cc_nbuckets: i32,
    skey_tmpl: &[ScanKeyData<'_>; CATCACHE_MAXKEYS],
    arguments: &[CatKey; 4],
    ctlist: &mut Vec<CtIdx>,
    ordered: &mut bool,
) -> PgResult<()> {
    // The catalog scan and its result tuples live in a scratch context (C's
    // CurrentMemoryContext during the scan); the key datums and tuple bytes we
    // keep are copied out into owned `FetchedCatalogTuple`s, so the scratch is
    // dropped when the scan ends.
    let scratch = mcx::MemoryContext::new("SearchCatCacheList scan");
    let scan_mcx = scratch.mcx();

    // relation = table_open(cache->cc_reloid, AccessShareLock);
    // The catalog must be locked (AccessShareLock) before the scan: genam's
    // systable_beginscan re-opens the heap relation with NoLock, asserting the
    // caller already holds the lock (the C `table_open(cc_reloid,
    // AccessShareLock)`). Mirrors the single-search path (search_path.rs); a
    // bare relcache lookup (`relation_id_get_relation`) does NOT take the lock
    // and trips the `finish_open` lock-held-by-me assertion.
    let relation = common_relation::relation_open(
        scan_mcx,
        cc_reloid,
        types_storage::lock::AccessShareLock,
    )?;

    // memcpy(cur_skey, cache->cc_skey, sizeof(cur_skey));
    // cur_skey[0..nkeys].sk_argument = v1..vN;  (only the first nkeys are used)
    // The per-search scankeys are built fresh in the scan scratch (their
    // by-reference `sk_argument`s borrow it), from the cache's scankey template.
    // By-value keys become the canonical by-value `Datum` word; by-reference keys
    // (name/text/oidvector) become a `Datum::ByRef` over the payload bytes (the
    // pointer-bearing `Datum` C's scan key already carries).
    let mut cur_skey: [ScanKeyData<'_>; CATCACHE_MAXKEYS] =
        core::array::from_fn(|i| skey_tmpl[i].clone());
    for i in 0..(nkeys as usize) {
        cur_skey[i].sk_argument = match &arguments[i] {
            CatKey::Scalar(w) => DatumV::ByVal(w.as_usize()),
            CatKey::ByRef(bytes) => DatumV::ByRef(mcx::slice_in(scan_mcx, bytes)?),
        };
    }

    // scandesc = systable_beginscan(relation, cache->cc_indexoid,
    //                               IndexScanOK(cache), NULL, nkeys, cur_skey);
    let index_ok = crate::init_meta::IndexScanOK(cache_id)?;
    let mut guard = genam::systable_beginscan::call(
        &relation,
        cc_indexoid,
        index_ok,
        None,
        &cur_skey[..nkeys as usize],
    )?;

    // ordered = (scandesc->irel != NULL);
    *ordered = index_ok;

    // while (HeapTupleIsValid(ntp = systable_getnext(scandesc)) &&
    //        !in_progress_ent.dead)
    //
    // The `&& !in_progress_ent.dead` guard terminates the scan early if a
    // concurrent invalidation marked the in-progress list build dead (e.g. via
    // ResetCatalogCache / CatCacheInvalidate marking the top in-progress entry,
    // or CatalogCacheCreateEntry failing because the tuple went stale during
    // toast access). The caller's do/while then restarts the scan from scratch.
    loop {
        let ntp = genam::systable_getnext::call(scan_mcx, guard.desc_mut())?;
        let ntp = match ntp {
            Some(t) => t,
            None => break,
        };

        // `&& !in_progress_ent.dead`: the C `while` short-circuits — getnext is
        // evaluated first, then the dead flag is checked, and a dead in-progress
        // entry exits the loop *without* processing this tuple. (The flag is the
        // top of the create-in-progress stack we pushed in
        // `search_cat_cache_list_miss`; a concurrent invalidation — including a
        // tuple going stale during toast access mid-getnext — marks it.)
        if with_arena(|arena| crate::graph_machinery::in_progress_top_dead(arena)) {
            break;
        }

        // Build the carrier the entry-reuse helper consumes (the C inline
        // CatalogCacheComputeTupleHashValue + key extraction).
        let fetched = build_fetched(scan_mcx, cache_idx, &ntp)?;

        // Reuse an existing cache entry, or create a new one. The shared helper
        // (owned by the single-search path) does the bucket probe by t_self and
        // creates a positive entry if absent.
        let ct_idx = crate::search_path::reuse_or_create_entry(cache_idx, &fetched)?;

        // We have to bump the member's refcount "temporarily" while we build the
        // list, so as to ensure it survives even if some other set of cache
        // entries gets discarded.  The refcount is transferred to the list (and
        // the temp ref released) once the list is fully built.
        with_arena(|arena| {
            let ct = arena.caches[cache_idx.0].tuples[ct_idx.0]
                .as_mut()
                .expect("member tuple");
            ct.refcount += 1;
        });

        // ctlist = lappend(ctlist, ct); nmembers++;
        ctlist.push(ct_idx);
    }

    // systable_endscan(scandesc); table_close(relation, AccessShareLock);
    guard.end()?;
    relation.close(types_storage::lock::AccessShareLock)?;

    let _ = cc_nbuckets;
    Ok(())
}

/// Build a [`FetchedCatalogTuple`] from a scanned tuple, extracting the key
/// columns via the cache's tupdesc (`cc_tupdesc`) and `cc_keyno`. This is the
/// owned-model expression of the C inline `CatalogCacheComputeTupleHashValue`
/// key extraction the single-search and list-search paths share.
pub(crate) fn build_fetched(
    scan_mcx: Mcx<'_>,
    cache_idx: CacheIdx,
    ntp: &FormedTuple<'_>,
) -> PgResult<FetchedCatalogTuple> {
    // Read t_self / t_len / t_tableOid off the scanned tuple.
    let t_self = ItemPointer {
        block: ntp.tuple.t_self.ip_blkid.block_number(),
        offset: ntp.tuple.t_self.ip_posid,
    };
    let t_tableoid = ntp.tuple.t_tableOid;

    // Extract the key datums via the cache's tupdesc + cc_keyno.
    let (cache_id, cc_keyno, cc_nkeys, cc_fastkind) = with_arena(|arena| {
        let cache = &arena.caches[cache_idx.0];
        (cache.id, cache.cc_keyno, cache.cc_nkeys, cache.cc_fastkind)
    });

    // `CatalogCacheCreateEntry` (catcache.c:2164): if the tuple has any
    // out-of-line toasted fields, expand them in-line before caching. This both
    // saves cycles on later use and — crucially — protects against the toast
    // tuples being freed before a (slightly stale) catcache entry tries to fetch
    // them. Without this, the cached `t_data` keeps an external TOAST pointer and
    // a later deform of that column reads a TOAST pointer where an inline varlena
    // is expected (`invalid varlena TOAST tag`). We flatten under the cache
    // tupdesc; the flattened tuple then feeds both the cached image serialization
    // and the key extraction below.
    let has_external = ntp
        .tuple
        .t_data
        .as_ref()
        .map(|h| {
            (h.t_infomask & types_tuple::heaptuple::HEAP_HASEXTERNAL) != 0
        })
        .unwrap_or(false);

    let mut flat_err: Option<types_error::PgError> = None;
    let mut flattened: Option<FormedTuple<'_>> = None;
    if has_external {
        crate::init_meta::with_cache_tupdesc(cache_id, &mut |tupdesc| {
            match heaptoast_seams::toast_flatten_tuple::call(
                scan_mcx, ntp, tupdesc,
            ) {
                Ok(ft) => flattened = Some(ft),
                Err(e) => flat_err = Some(e),
            }
        });
        if let Some(e) = flat_err {
            return Err(e);
        }
    }
    // The tuple feeding the cached image + key extraction: the flattened copy if
    // we had externals, else the scanned tuple verbatim.
    let dtp: &FormedTuple<'_> = flattened.as_ref().unwrap_or(ntp);

    let t_len = dtp.tuple.t_len;

    // The cached `t_data` is the tuple's full contiguous on-disk image
    // (the C `memcpy(dtp->t_data)` of header + null bitmap + pad + user data),
    // so the entry copy (`catcache_form_cached_tuple` = `heap_copytuple`) can
    // rebuild a complete `FormedTuple` later. Serialize the (possibly flattened)
    // tuple via the heaptuple disk-image codec rather than keeping only the
    // user-data bytes (which would lose the header).
    let t_data: alloc::vec::Vec<u8> = {
        let img = heaptuple::heap_tuple_to_disk_image(scan_mcx, dtp)?;
        img.iter().copied().collect()
    };

    let mut keys: [CatKey; CATCACHE_MAXKEYS] = core::array::from_fn(|_| CatKey::scalar_null());

    // heap_deform_tuple(tuple, cc_tupdesc, values, isnull); then for each key
    // column i (0..cc_nkeys), keys[i] = values[cc_keyno[i] - 1]. This is the C
    // `CatCacheCopyKeys`: by-value keys keep the scalar word, by-reference keys
    // (name/text/oidvector) `datumCopy` their payload bytes into the entry.
    let mut extract_err: Option<types_error::PgError> = None;
    crate::init_meta::with_cache_tupdesc(cache_id, &mut |tupdesc| {
        let res = heaptuple::heap_deform_tuple(
            scan_mcx,
            &dtp.tuple,
            tupdesc,
            &dtp.data,
        );
        match res {
            Ok(deformed) => {
                for i in 0..(cc_nkeys as usize) {
                    // cc_keyno is 1-based (an AttrNumber).
                    let attno = cc_keyno[i];
                    let col = &deformed[(attno - 1) as usize];
                    keys[i] = match &col.0 {
                        // By-value key: the scalar word is the key Datum.
                        Datum::ByVal(d) => CatKey::Scalar(ScalarWord::from_usize(*d)),
                        // By-reference key (name/text/oidvector): own a copy of
                        // the attribute's *canonical* payload bytes. The deformed
                        // image is the full on-disk varlena (for `text`); the fast
                        // hash/compare functions consume the header-LESS
                        // `VARDATA_ANY` payload (the same bare bytes a by-name
                        // search key carries), so strip the varlena header for
                        // `text` here. `name` (NUL-truncated by the fast fns) and
                        // `oidvector` (Oid element bytes) pass through verbatim.
                        Datum::ByRef(b) => {
                            let kind = cc_fastkind[i]
                                .expect("build_fetched: cc_fastkind set by phase-2 init");
                            CatKey::ByRef(
                                crate::core_compute::canonicalize_byref_key(kind, b),
                            )
                        }
                        // The non-flat payload kinds are not catcache key types
                        // (GetCCHashEqFuncs rejects them); a by-value placeholder
                        // is never read for them.
                        Datum::Cstring(_)
                        | Datum::Composite(_)
                        | Datum::Expanded(_)
                        | Datum::Internal(_) => CatKey::scalar_null(),
                    };
                }
            }
            Err(e) => extract_err = Some(e),
        }
    });
    if let Some(e) = extract_err {
        return Err(e);
    }

    Ok(FetchedCatalogTuple {
        t_len,
        t_self,
        t_tableoid,
        t_data,
        keys,
    })
}

/// Undo temporary member refcounts on a list-build error (the C `PG_CATCH`
/// loop): drop each member's temp ref, removing now-dead unreferenced entries.
pub(crate) fn undo_member_refs(cache_idx: CacheIdx, members: &[CtIdx]) -> PgResult<()> {
    // foreach(ctlc, ctlist) {
    //     ct = lfirst(ctlc);
    //     Assert(ct->refcount > 0);
    //     ct->refcount--;
    //     if (ct->refcount == 0 && (ct->c_list == NULL || ct->c_list->refcount == 0))
    //         CatCacheRemoveCTup(ct->my_cache, ct);
    // }
    with_arena(|arena| {
        for &ct_idx in members {
            let ct = arena.caches[cache_idx.0].tuples[ct_idx.0]
                .as_mut()
                .expect("member tuple");
            assert!(ct.refcount > 0);
            ct.refcount -= 1;
            let refcount = ct.refcount;
            let c_list = ct.c_list;
            let dead = ct.dead;

            // The temp ref dropping to zero only matters for entries not yet
            // owned by a (live) list. (During the PG_CATCH path no list exists.)
            let list_unref = if c_list.is_none() {
                true
            } else {
                arena.caches[cache_idx.0].lists[c_list.0]
                    .as_ref()
                    .map(|cl| cl.refcount == 0)
                    .unwrap_or(true)
            };
            if refcount == 0 && (dead || list_unref) {
                CatCacheRemoveCTup(arena, cache_idx, ct_idx);
            }
        }
    });
    Ok(())
}

/// Copy a live list's members into `mcx` as `FormedTuple`s, in scan order.
pub(crate) fn build_list_members<'mcx>(
    mcx: Mcx<'mcx>,
    cache_idx: CacheIdx,
    cl_idx: ClIdx,
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    // Snapshot the member handles, then copy each member's cached tuple into the
    // caller's mcx as a FormedTuple (the C callers walk cl->members[i] and read
    // &member->tuple; the owned model hands back owned copies).
    let members = with_arena(|arena| {
        arena.caches[cache_idx.0].lists[cl_idx.0]
            .as_ref()
            .expect("live list")
            .members
            .clone()
    });

    let mut out: PgVec<'mcx, FormedTuple<'mcx>> = mcx::vec_with_capacity_in(mcx, members.len())?;
    for ct_idx in members {
        let formed = crate::search_path::build_formed_tuple(mcx, cache_idx, ct_idx)?;
        out.push(formed);
    }
    Ok(out)
}

/// `ReleaseCatCacheList(list)` — decrement the list refcount, removing it if
/// now dead and unreferenced.
pub(crate) fn release_cat_cache_list(cache_id: i32, cl_idx: ClIdx) -> PgResult<()> {
    with_arena(|arena| {
        let cache_idx = find_cache_by_id(arena, cache_id)
            .expect("ReleaseCatCacheList: unknown cache id");

        // Assert(cl->cl_magic == CL_MAGIC);
        // ResourceOwnerForgetCatCacheListRef(...); (folded into the owned copy)
        let cl = arena.caches[cache_idx.0].lists[cl_idx.0]
            .as_mut()
            .expect("live list");
        assert_eq!(cl.cl_magic, CL_MAGIC);

        // Assert(cl->refcount > 0);
        assert!(cl.refcount > 0);
        cl.refcount -= 1;
        let now = cl.refcount;
        let dead = cl.dead;

        // if (cl->refcount == 0 && (cl->dead || ...)) CatCacheRemoveCList(...)
        // In C the second clause is `#ifdef CLOBBER_FREED_MEMORY` debug discard;
        // the production condition removes a dead list once unreferenced.
        if now == 0 && dead {
            CatCacheRemoveCList(arena, cache_idx, cl_idx);
        }
    });
    Ok(())
}

/* ----------------------------------------------------------------------------
 * Small local helpers bridging the arena cache fields the core/compute family
 * consumes.
 * ------------------------------------------------------------------------- */

/// The per-key fast-kind slice (`cache->cc_hashfunc[]`/`cc_fastequal[]`), as the
/// `core_compute` hash/compare functions expect. Built from the cache's
/// phase-2-populated `cc_fastkind`.
fn fastkinds(
    cache: &cache::catcache::ArenaCatCache,
) -> Vec<cache::catcache::CCFastKind> {
    cache
        .cc_fastkind
        .iter()
        .take(cache.cc_nkeys as usize)
        .map(|k| k.expect("cc_fastkind populated by phase-2 init"))
        .collect()
}

/// `memcpy(cur_skey, cache->cc_skey, sizeof(cur_skey))` — the scankey template
/// the scan stamps its arguments into. `cc_skey` slots are `None` until phase-2
/// build; by then all `cc_nkeys` slots are populated.
fn skey_template<'a>(
    cache: &cache::catcache::ArenaCatCache,
) -> [ScanKeyData<'a>; CATCACHE_MAXKEYS] {
    core::array::from_fn(|i| {
        // C memcpy's the whole template, but `sk_argument` is immediately
        // overwritten by the per-search arguments (see the stamping loop), so
        // the copied-out template carries a fresh NULL by-value argument. This
        // decouples the returned key's `'mcx` from the borrowed cache (the
        // canonical `Datum`'s by-value arm is lifetime-free).
        match &cache.cc_skey[i] {
            Some(k) => ScanKeyData {
                sk_flags: k.sk_flags,
                sk_attno: k.sk_attno,
                sk_strategy: k.sk_strategy,
                sk_subtype: k.sk_subtype,
                sk_collation: k.sk_collation,
                sk_func: k.sk_func.clone(),
                sk_argument: DatumV::null(),
                sk_subkeys: None,
            },
            None => ScanKeyData::empty(),
        }
    })
}
