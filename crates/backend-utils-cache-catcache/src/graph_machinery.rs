//! The cache-graph machinery (`catcache.c`): cache registration
//! (`InitCatCache`), the rehash routines, entry creation
//! (`CatalogCacheCreateEntry`), entry/list removal
//! (`CatCacheRemoveCTup`/`CatCacheRemoveCList`), invalidation
//! (`CatCacheInvalidate`), the resets (`ResetCatalogCache(s)(Ext)` /
//! `CatalogCacheFlushCatalog`), and the create-in-progress stack.

extern crate alloc;

use alloc::vec::Vec;

use alloc::string::String;

use types_cache::backend_utils_cache_catcache::{
    ArenaCatCList, ArenaCatCTup, ArenaCatCache, CacheIdx, CatCInProgress, CatCacheArena, ClIdx,
    CtIdx, FetchedCatalogTuple, CATCACHE_MAXKEYS, CT_MAGIC,
};
use types_core::Oid;
use types_datum::Datum;
use types_error::{PgError, PgResult};

use crate::core_compute::HASH_INDEX;
use crate::{find_cache_by_id, with_arena};

/* ----------------------------------------------------------------------------
 * Bucket primitives — the intrusive-dlist operations over arena indices.
 * A bucket is `Vec<idx>`, front = head.
 * ------------------------------------------------------------------------- */

/// `dlist_push_head(bucket, &elem)`.
#[inline]
pub(crate) fn ct_bucket_push_head(bucket: &mut Vec<CtIdx>, idx: CtIdx) {
    bucket.insert(0, idx);
}

/// `dlist_move_head(bucket, &elem)` — move an existing member to the front.
pub(crate) fn ct_bucket_move_head(bucket: &mut Vec<CtIdx>, idx: CtIdx) {
    if let Some(pos) = bucket.iter().position(|&e| e == idx) {
        bucket.remove(pos);
        bucket.insert(0, idx);
    }
}

/// `dlist_delete(&elem)` for a tuple in its bucket.
pub(crate) fn ct_bucket_delete(bucket: &mut Vec<CtIdx>, idx: CtIdx) {
    if let Some(pos) = bucket.iter().position(|&e| e == idx) {
        bucket.remove(pos);
    }
}

#[inline]
pub(crate) fn cl_bucket_push_head(bucket: &mut Vec<ClIdx>, idx: ClIdx) {
    bucket.insert(0, idx);
}

pub(crate) fn cl_bucket_move_head(bucket: &mut Vec<ClIdx>, idx: ClIdx) {
    if let Some(pos) = bucket.iter().position(|&e| e == idx) {
        bucket.remove(pos);
        bucket.insert(0, idx);
    }
}

pub(crate) fn cl_bucket_delete(bucket: &mut Vec<ClIdx>, idx: ClIdx) {
    if let Some(pos) = bucket.iter().position(|&e| e == idx) {
        bucket.remove(pos);
    }
}

/* ----------------------------------------------------------------------------
 * Arena slot allocation/free (`palloc`/`pfree` of a `CatCTup`/`CatCList`).
 * ------------------------------------------------------------------------- */

/// Allocate a tuple slot, reusing a free slot if available.
pub(crate) fn ct_alloc(cache: &mut ArenaCatCache, ct: ArenaCatCTup) -> CtIdx {
    if let Some(slot) = cache.ct_freelist.pop() {
        cache.tuples[slot] = Some(ct);
        CtIdx(slot)
    } else {
        cache.tuples.push(Some(ct));
        CtIdx(cache.tuples.len() - 1)
    }
}

/// Free a tuple slot (the C `pfree(ct)`).
pub(crate) fn ct_free(cache: &mut ArenaCatCache, idx: CtIdx) {
    cache.tuples[idx.0] = None;
    cache.ct_freelist.push(idx.0);
}

/// Allocate a list slot.
pub(crate) fn cl_alloc(cache: &mut ArenaCatCache, cl: ArenaCatCList) -> ClIdx {
    if let Some(slot) = cache.cl_freelist.pop() {
        cache.lists[slot] = Some(cl);
        ClIdx(slot)
    } else {
        cache.lists.push(Some(cl));
        ClIdx(cache.lists.len() - 1)
    }
}

/// Free a list slot (the C `pfree(cl)`).
pub(crate) fn cl_free(cache: &mut ArenaCatCache, idx: ClIdx) {
    cache.lists[idx.0] = None;
    cache.cl_freelist.push(idx.0);
}

/* ----------------------------------------------------------------------------
 * CatCacheRemoveCTup / CatCacheRemoveCList (catcache.c).
 * ------------------------------------------------------------------------- */

/// `CatCacheRemoveCTup(cache, ct)` — unlink and delete a cached tuple; if it
/// belongs to a list, the list is deleted too.
pub(crate) fn CatCacheRemoveCTup(arena: &mut CatCacheArena, cache_idx: CacheIdx, ct_idx: CtIdx) {
    let (c_list, hash_value) = {
        let cache = &arena.caches[cache_idx.0];
        let ct = cache.tuples[ct_idx.0].as_ref().expect("live CatCTup");
        debug_assert_eq!(ct.refcount, 0);
        debug_assert_eq!(ct.my_cache, cache_idx);
        (ct.c_list, ct.hash_value)
    };

    if !c_list.is_none() {
        /*
         * The cleanest way to handle this is to call CatCacheRemoveCList,
         * which will recurse back to me, and the recursive call will do the
         * work.  Set the "dead" flag to make sure it does recurse.
         */
        arena.caches[cache_idx.0].tuples[ct_idx.0]
            .as_mut()
            .unwrap()
            .dead = true;
        CatCacheRemoveCList(arena, cache_idx, c_list);
        return; /* nothing left to do */
    }

    /* delink from linked list */
    {
        let cache = &mut arena.caches[cache_idx.0];
        let nbuckets = cache.cc_nbuckets;
        let bi = HASH_INDEX(hash_value, nbuckets);
        ct_bucket_delete(&mut cache.cc_bucket[bi], ct_idx);
    }

    /*
     * In C, negative entries free their by-ref keys (CatCacheFreeKeys); normal
     * entries' keys point into the tuple data freed with the CatCTup.  In the
     * arena form the key datums are plain words owned by the slot, so freeing
     * the slot (the C `pfree(ct)`) covers both cases.
     */
    ct_free(&mut arena.caches[cache_idx.0], ct_idx);

    arena.caches[cache_idx.0].cc_ntup -= 1;
    arena.ch_ntup -= 1;
}

/// `CatCacheRemoveCList(cache, cl)` — unlink and delete a list, removing any
/// now-unreferenced dead member tuples.
pub(crate) fn CatCacheRemoveCList(arena: &mut CatCacheArena, cache_idx: CacheIdx, cl_idx: ClIdx) {
    let (members, hash_value) = {
        let cache = &arena.caches[cache_idx.0];
        let cl = cache.lists[cl_idx.0].as_ref().expect("live CatCList");
        debug_assert_eq!(cl.refcount, 0);
        debug_assert_eq!(cl.my_cache, cache_idx);
        (cl.members.clone(), cl.hash_value)
    };

    /* delink from member tuples (C iterates `for (i = n_members; --i >= 0;)`) */
    for &ct_idx in members.iter().rev() {
        let (dead, refcount) = {
            let cache = &mut arena.caches[cache_idx.0];
            let ct = cache.tuples[ct_idx.0].as_mut().expect("live member");
            debug_assert_eq!(ct.c_list, cl_idx);
            ct.c_list = ClIdx::NONE;
            (ct.dead, ct.refcount)
        };
        /* if the member is dead and now has no references, remove it */
        if dead && refcount == 0 {
            CatCacheRemoveCTup(arena, cache_idx, ct_idx);
        }
    }

    /* delink from linked list */
    {
        let cache = &mut arena.caches[cache_idx.0];
        let nlbuckets = cache.cc_nlbuckets;
        let bi = HASH_INDEX(hash_value, nlbuckets);
        cl_bucket_delete(&mut cache.cc_lbucket[bi], cl_idx);
    }

    /* free associated column data (the C `pfree(cl)`) */
    cl_free(&mut arena.caches[cache_idx.0], cl_idx);

    arena.caches[cache_idx.0].cc_nlist -= 1;
}

/* ----------------------------------------------------------------------------
 * CatCacheInvalidate (catcache.c) — inval.c-only.
 * ------------------------------------------------------------------------- */

/// `CatCacheInvalidate(cache, hashValue)` — zap entries matching `hashValue`
/// (positive or negative) and *all* lists.
pub(crate) fn cat_cache_invalidate_idx(
    arena: &mut CatCacheArena,
    cache_idx: CacheIdx,
    hash_value: u32,
) {
    /*
     * We don't bother to check whether the cache has finished initialization
     * yet; if not, there will be no entries in it so no problem.
     *
     * Invalidate *all* CatCLists in this cache; it's too hard to tell which
     * searches might still be correct, so just zap 'em all.
     */
    let nlbuckets = arena.caches[cache_idx.0].cc_nlbuckets;
    for i in 0..nlbuckets as usize {
        let snapshot: Vec<ClIdx> = arena.caches[cache_idx.0].cc_lbucket[i].clone();
        for cl_idx in snapshot {
            /* entry may already have been removed by a prior recursion */
            if !arena.caches[cache_idx.0].cc_lbucket[i]
                .iter()
                .any(|&e| e == cl_idx)
            {
                continue;
            }
            let refcount = arena.caches[cache_idx.0].lists[cl_idx.0]
                .as_ref()
                .map(|cl| cl.refcount)
                .unwrap_or(0);
            if refcount > 0 {
                if let Some(cl) = arena.caches[cache_idx.0].lists[cl_idx.0].as_mut() {
                    cl.dead = true;
                }
            } else {
                CatCacheRemoveCList(arena, cache_idx, cl_idx);
            }
        }
    }

    /* inspect the proper hash bucket for tuple matches */
    let nbuckets = arena.caches[cache_idx.0].cc_nbuckets;
    let bi = HASH_INDEX(hash_value, nbuckets);
    let snapshot: Vec<CtIdx> = arena.caches[cache_idx.0].cc_bucket[bi].clone();
    for ct_idx in snapshot {
        if !arena.caches[cache_idx.0].cc_bucket[bi]
            .iter()
            .any(|&e| e == ct_idx)
        {
            continue;
        }
        let ct = match arena.caches[cache_idx.0].tuples[ct_idx.0].as_ref() {
            Some(ct) => ct,
            None => continue,
        };
        if ct.hash_value != hash_value {
            continue;
        }
        let ct_refcount = ct.refcount;
        let c_list = ct.c_list;
        let list_refcount = if c_list.is_none() {
            0
        } else {
            arena.caches[cache_idx.0].lists[c_list.0]
                .as_ref()
                .map(|cl| cl.refcount)
                .unwrap_or(0)
        };
        if ct_refcount > 0 || list_refcount > 0 {
            if let Some(ct) = arena.caches[cache_idx.0].tuples[ct_idx.0].as_mut() {
                ct.dead = true;
            }
            /* list, if any, was marked dead above */
            debug_assert!(
                c_list.is_none()
                    || arena.caches[cache_idx.0].lists[c_list.0]
                        .as_ref()
                        .map(|cl| cl.dead)
                        .unwrap_or(true)
            );
        } else {
            CatCacheRemoveCTup(arena, cache_idx, ct_idx);
        }
        /* could be multiple matches, so keep looking! */
    }

    /* Also invalidate any entries that are being built */
    for e in arena.in_progress.iter_mut() {
        if e.cache == cache_idx && (e.list || e.hash_value == hash_value) {
            e.dead = true;
        }
    }
}

/// `CatCacheInvalidate(SysCache[cacheId], hashValue)` — the inval.c entry
/// point, addressed by cache id (infallible in C).
pub fn CatCacheInvalidate(cache_id: i32, hash_value: u32) {
    with_arena(|arena| {
        if let Some(cache_idx) = find_cache_by_id(arena, cache_id) {
            cat_cache_invalidate_idx(arena, cache_idx, hash_value);
        }
    });
}

/* ----------------------------------------------------------------------------
 * ResetCatalogCache(s)(Ext) / CatalogCacheFlushCatalog (catcache.c).
 * ------------------------------------------------------------------------- */

/// `ResetCatalogCache(cache, debug_discard)` — reset one cache to empty.
pub(crate) fn reset_catalog_cache(
    arena: &mut CatCacheArena,
    cache_idx: CacheIdx,
    debug_discard: bool,
) {
    /* Remove each list in this cache, or at least mark it dead */
    let nlbuckets = arena.caches[cache_idx.0].cc_nlbuckets;
    for i in 0..nlbuckets as usize {
        let snapshot: Vec<ClIdx> = arena.caches[cache_idx.0].cc_lbucket[i].clone();
        for cl_idx in snapshot {
            if !arena.caches[cache_idx.0].cc_lbucket[i]
                .iter()
                .any(|&e| e == cl_idx)
            {
                continue;
            }
            let refcount = arena.caches[cache_idx.0].lists[cl_idx.0]
                .as_ref()
                .map(|cl| cl.refcount)
                .unwrap_or(0);
            if refcount > 0 {
                if let Some(cl) = arena.caches[cache_idx.0].lists[cl_idx.0].as_mut() {
                    cl.dead = true;
                }
            } else {
                CatCacheRemoveCList(arena, cache_idx, cl_idx);
            }
        }
    }

    /* Remove each tuple in this cache, or at least mark it dead */
    let nbuckets = arena.caches[cache_idx.0].cc_nbuckets;
    for i in 0..nbuckets as usize {
        let snapshot: Vec<CtIdx> = arena.caches[cache_idx.0].cc_bucket[i].clone();
        for ct_idx in snapshot {
            if !arena.caches[cache_idx.0].cc_bucket[i]
                .iter()
                .any(|&e| e == ct_idx)
            {
                continue;
            }
            let ct = match arena.caches[cache_idx.0].tuples[ct_idx.0].as_ref() {
                Some(ct) => ct,
                None => continue,
            };
            let ct_refcount = ct.refcount;
            let c_list = ct.c_list;
            let list_refcount = if c_list.is_none() {
                0
            } else {
                arena.caches[cache_idx.0].lists[c_list.0]
                    .as_ref()
                    .map(|cl| cl.refcount)
                    .unwrap_or(0)
            };
            if ct_refcount > 0 || list_refcount > 0 {
                if let Some(ct) = arena.caches[cache_idx.0].tuples[ct_idx.0].as_mut() {
                    ct.dead = true;
                }
                /* list, if any, was marked dead above */
                debug_assert!(
                    c_list.is_none()
                        || arena.caches[cache_idx.0].lists[c_list.0]
                            .as_ref()
                            .map(|cl| cl.dead)
                            .unwrap_or(true)
                );
            } else {
                CatCacheRemoveCTup(arena, cache_idx, ct_idx);
            }
        }
    }

    /* Also invalidate any entries that are being built */
    if !debug_discard {
        for e in arena.in_progress.iter_mut() {
            if e.cache == cache_idx {
                e.dead = true;
            }
        }
    }
}

/// `ResetCatalogCaches()`.
pub fn ResetCatalogCaches() -> PgResult<()> {
    ResetCatalogCachesExt(false)
}

/// `ResetCatalogCachesExt(debug_discard)`.
pub fn ResetCatalogCachesExt(debug_discard: bool) -> PgResult<()> {
    with_arena(|arena| {
        let n = arena.caches.len();
        for i in 0..n {
            reset_catalog_cache(arena, CacheIdx(i), debug_discard);
        }
    });
    Ok(())
}

/// `CatalogCacheFlushCatalog(catId)` — flush every cache on `catId` and fire
/// its syscache callbacks (via seam).
pub fn CatalogCacheFlushCatalog(cat_id: Oid) -> PgResult<()> {
    /*
     * Collect the (cache, id) targets first; firing the syscache callbacks
     * reaches inval.c through its seam and could re-enter the arena, so we must
     * not hold the borrow across the callback.
     */
    let to_flush: Vec<(CacheIdx, i32)> = with_arena(|arena| {
        let mut v = Vec::new();
        for (i, cache) in arena.caches.iter().enumerate() {
            /* Does this cache store tuples of the target catalog? */
            if cache.cc_reloid == cat_id {
                v.push((CacheIdx(i), cache.id));
            }
        }
        v
    });
    for (cache_idx, id) in to_flush {
        /* Yes, so flush all its contents */
        with_arena(|arena| reset_catalog_cache(arena, cache_idx, false));
        /* Tell inval.c to call syscache callbacks for this cache */
        backend_utils_cache_inval_seams::call_syscache_callbacks::call(id, 0)?;
    }
    Ok(())
}

/* ----------------------------------------------------------------------------
 * InitCatCache / Rehash (catcache.c).
 * ------------------------------------------------------------------------- */

/// `InitCatCache(id, reloid, indexoid, nkeys, key, nbuckets)` — allocate and
/// partially initialize a cache, registering it in the header. Returns whether
/// the cache was created (the C non-NULL pointer test).
pub fn InitCatCache(
    id: i32,
    reloid: Oid,
    indexoid: Oid,
    nkeys: i32,
    key: &[i32],
    nbuckets: i32,
) -> PgResult<bool> {
    /*
     * nbuckets must be a power of two.  We check this via Assert rather than a
     * full runtime check because the values come from constant tables.
     */
    debug_assert!(nbuckets > 0 && (nbuckets & -nbuckets) == nbuckets);

    /*
     * Initialize the cache's relation information and some of the new cache's
     * other internal fields.  But don't open the relation yet (the C only
     * partially initializes here; phase 2 loads the tupdesc).
     */
    let mut cc_keyno = [0i32; CATCACHE_MAXKEYS];
    for i in 0..nkeys as usize {
        debug_assert!(key[i] != 0); /* AttributeNumberIsValid */
        cc_keyno[i] = key[i];
    }

    /*
     * Allocate the bucket array.  Zeroing initializes each dlist header in C;
     * here every bucket starts as an empty Vec.
     */
    let mut cc_bucket: Vec<Vec<CtIdx>> = Vec::new();
    if cc_bucket.try_reserve(nbuckets as usize).is_err() {
        return Err(PgError::error("out of memory"));
    }
    for _ in 0..nbuckets {
        cc_bucket.push(Vec::new());
    }

    let cache = ArenaCatCache {
        id,
        cc_relname: String::from("(not known yet)"),
        cc_reloid: reloid,
        cc_indexoid: indexoid,
        cc_relisshared: false, /* temporary */
        cc_ntup: 0,
        cc_nlist: 0,
        cc_nbuckets: nbuckets,
        cc_nlbuckets: 0,
        cc_nkeys: nkeys,
        cc_keyno,
        cc_fastkind: [None; CATCACHE_MAXKEYS],
        cc_skey: core::array::from_fn(|_| None),
        initialized: false,
        cc_bucket,
        /*
         * Many catcaches never receive any list searches.  Therefore, we don't
         * allocate the cc_lbuckets till we get a list search.
         */
        cc_lbucket: Vec::new(),
        tuples: Vec::new(),
        ct_freelist: Vec::new(),
        lists: Vec::new(),
        cl_freelist: Vec::new(),
    };

    with_arena(|arena| {
        /*
         * The C `slist_push_head` puts the new cache at the head; we append and
         * rely on id lookup, since head-vs-tail only affects the (debug) stats
         * walk and the reset iteration order (which is order-insensitive).
         */
        arena.caches.push(cache);
    });

    /* C returns the non-NULL CatCache pointer; the seam reports "created". */
    Ok(true)
}

/// `RehashCatCache(cp)` — double the tuple bucket count, relinking all entries.
pub(crate) fn rehash_cat_cache(cache: &mut ArenaCatCache) -> PgResult<()> {
    /* Allocate a new, larger, hash table. */
    let newnbuckets = cache.cc_nbuckets * 2;
    let mut newbucket: Vec<Vec<CtIdx>> = Vec::new();
    if newbucket.try_reserve(newnbuckets as usize).is_err() {
        return Err(PgError::error("out of memory"));
    }
    for _ in 0..newnbuckets {
        newbucket.push(Vec::new());
    }

    /*
     * Move all entries from old hash table to new.  The C iterates each old
     * bucket front-to-back and dlist_push_heads into the new; iterating old
     * front-to-back and push_head here reproduces the exact resulting order.
     */
    for i in 0..cache.cc_nbuckets as usize {
        let old = core::mem::take(&mut cache.cc_bucket[i]);
        for ct_idx in old {
            let hv = cache.tuples[ct_idx.0]
                .as_ref()
                .ok_or_else(|| PgError::error("rehash_cat_cache: dangling tuple in bucket"))?
                .hash_value;
            let hi = HASH_INDEX(hv, newnbuckets);
            ct_bucket_push_head(&mut newbucket[hi], ct_idx);
        }
    }

    /* Switch to the new array. */
    cache.cc_nbuckets = newnbuckets;
    cache.cc_bucket = newbucket;
    Ok(())
}

/// `RehashCatCacheLists(cp)` — double the list bucket count.
pub(crate) fn rehash_cat_cache_lists(cache: &mut ArenaCatCache) -> PgResult<()> {
    /* Allocate a new, larger, hash table. */
    let newnbuckets = cache.cc_nlbuckets * 2;
    let mut newbucket: Vec<Vec<ClIdx>> = Vec::new();
    if newbucket.try_reserve(newnbuckets as usize).is_err() {
        return Err(PgError::error("out of memory"));
    }
    for _ in 0..newnbuckets {
        newbucket.push(Vec::new());
    }

    /* Move all entries from old hash table to new. */
    for i in 0..cache.cc_nlbuckets as usize {
        let old = core::mem::take(&mut cache.cc_lbucket[i]);
        for cl_idx in old {
            let hv = cache.lists[cl_idx.0]
                .as_ref()
                .ok_or_else(|| PgError::error("rehash_cat_cache_lists: dangling list in bucket"))?
                .hash_value;
            let hi = HASH_INDEX(hv, newnbuckets);
            cl_bucket_push_head(&mut newbucket[hi], cl_idx);
        }
    }

    /* Switch to the new array. */
    cache.cc_nlbuckets = newnbuckets;
    cache.cc_lbucket = newbucket;
    Ok(())
}

/* ----------------------------------------------------------------------------
 * CatalogCacheCreateEntry (catcache.c).
 * ------------------------------------------------------------------------- */

/// `CatalogCacheCreateEntry` for a *positive* entry, from a fetched catalog
/// tuple. Returns the new tuple's arena index, linked into the bucket with
/// refcount 0.
pub(crate) fn create_entry_positive(
    arena: &mut CatCacheArena,
    cache_idx: CacheIdx,
    fetched: FetchedCatalogTuple,
    hash_value: u32,
    hash_index: usize,
) -> PgResult<CtIdx> {
    /*
     * The C copies the (already-detoasted) tuple into the cache context, then
     * extracts the keys.  The fetch seam has already flattened the tuple and
     * extracted the key datums, so we build the CatCTup directly.
     */
    let ct = ArenaCatCTup {
        ct_magic: CT_MAGIC,
        hash_value,
        keys: fetched.keys,
        refcount: 0, /* for the moment */
        dead: false,
        negative: false, /* ntp != NULL */
        t_len: fetched.t_len,
        t_self: fetched.t_self,
        t_tableoid: fetched.t_tableoid,
        t_data: fetched.t_data,
        c_list: ClIdx::NONE,
        my_cache: cache_idx,
    };
    let ct_idx = ct_alloc(&mut arena.caches[cache_idx.0], ct);

    /* add it to the cache's linked list and counts */
    ct_bucket_push_head(&mut arena.caches[cache_idx.0].cc_bucket[hash_index], ct_idx);
    arena.caches[cache_idx.0].cc_ntup += 1;
    arena.ch_ntup += 1;

    /* enlarge if the hash table has become too full (fill factor > 2) */
    maybe_rehash(&mut arena.caches[cache_idx.0])?;
    Ok(ct_idx)
}

/// `CatalogCacheCreateEntry` for a *negative* entry (key present, no tuple).
pub(crate) fn create_entry_negative(
    arena: &mut CatCacheArena,
    cache_idx: CacheIdx,
    arguments: [Datum; CATCACHE_MAXKEYS],
    hash_value: u32,
    hash_index: usize,
) -> PgResult<CtIdx> {
    /* Set up keys for a negative cache entry (the C CatCacheCopyKeys). */
    let ct = ArenaCatCTup {
        ct_magic: CT_MAGIC,
        hash_value,
        keys: arguments,
        refcount: 0,
        dead: false,
        negative: true, /* ntp == NULL */
        t_len: 0,
        t_self: types_cache::backend_utils_cache_catcache::ItemPointer::default(),
        t_tableoid: Oid::default(),
        t_data: Vec::new(),
        c_list: ClIdx::NONE,
        my_cache: cache_idx,
    };
    let ct_idx = ct_alloc(&mut arena.caches[cache_idx.0], ct);

    ct_bucket_push_head(&mut arena.caches[cache_idx.0].cc_bucket[hash_index], ct_idx);
    arena.caches[cache_idx.0].cc_ntup += 1;
    arena.ch_ntup += 1;

    maybe_rehash(&mut arena.caches[cache_idx.0])?;
    Ok(ct_idx)
}

/// The C `if (cache->cc_ntup > cache->cc_nbuckets * 2) RehashCatCache(cache)`.
pub(crate) fn maybe_rehash(cache: &mut ArenaCatCache) -> PgResult<()> {
    if cache.cc_ntup > cache.cc_nbuckets * 2 {
        rehash_cat_cache(cache)?;
    }
    Ok(())
}

/* ----------------------------------------------------------------------------
 * create-in-progress stack helpers.
 * ------------------------------------------------------------------------- */

/// Push a create-in-progress entry; returns its stack depth (for the matching
/// pop's `Assert`).
pub(crate) fn push_in_progress(
    arena: &mut CatCacheArena,
    cache_idx: CacheIdx,
    hash_value: u32,
    list: bool,
) -> usize {
    /*
     * C prepends `in_progress_ent` to `catcache_in_progress_stack` (top =
     * head).  Here the Vec's last element is the top.  We return the new stack
     * depth so the matching pop can `Assert` it removes the same entry.
     */
    arena.in_progress.push(CatCInProgress {
        cache: cache_idx,
        hash_value,
        list,
        dead: false,
    });
    arena.in_progress.len()
}

/// Pop the top create-in-progress entry, returning whether it was marked dead.
pub(crate) fn pop_in_progress(arena: &mut CatCacheArena) -> bool {
    /* C asserts `catcache_in_progress_stack == &in_progress_ent` before pop. */
    arena
        .in_progress
        .pop()
        .expect("pop_in_progress: empty in-progress stack")
        .dead
}

/// Helper kept available for the search families: run `with_arena` against the
/// subsystem state. (Re-exported so siblings need not import the crate root.)
pub(crate) fn arena<R>(f: impl FnOnce(&mut CatCacheArena) -> R) -> R {
    with_arena(f)
}
