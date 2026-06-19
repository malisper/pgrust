//! `utils/cache/syscache.c` — system cache management routines.
//!
//! A thin dispatch layer over the catalog cache (catcache.c): each
//! `SearchSysCache*` / `Init*` / `GetSysCacheHashValue` / `SearchSysCacheList`
//! / `SysCacheInvalidate` entry point delegates to the matching catcache
//! routine, addressed across the cycle-breaking catcache seams by integer
//! cache id (the C `SysCache[cacheId]->...` indirection).
//!
//! Tuple-ownership model: C's `SearchSysCache*` return a refcounted pointer
//! into the cache that must be `ReleaseSysCache`d; here every search returns
//! a *copy* of the cached tuple allocated in the caller's `mcx` (the catcache
//! owner drops its reference before returning), so [`ReleaseSysCache`] simply
//! consumes the copy and the `heap_copytuple` convenience wrappers reduce to
//! returning the already-owned copy's clone. The C distinction between "cache
//! copy, do not modify/free" and "your modifiable copy" disappears: every
//! returned tuple is the caller's.
//!
//! Per-backend mutable state (the file-scope statics: `CacheInitialized`, the
//! sorted OID lists, the `PointerIsValid(SysCache[id])` initialization flags)
//! lives in a `thread_local!`, matching the one-backend-one-thread model.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use std::cell::RefCell;

use backend_access_common_heaptuple::{
    getmissingattr, heap_attisnull, heap_copytuple, heap_getsysattr, nocachegetattr,
};
use backend_storage_lmgr_lock_seams as lock_seams;
use backend_utils_cache_catcache_seams as catcache_seams;
use backend_utils_cache_inval_seams as inval_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use mcx::{vec_with_capacity_in, McxOwned, Mcx, MemoryContext, PgVec};
use types_cache::SysCacheKey;
use types_core::{AttrNumber, Oid, OidIsValid, InvalidOid};
use types_error::{PgError, PgResult};
use types_storage::lock::{
    InplaceUpdateTupleLock, DEFAULT_LOCKMETHOD, LOCKMODE, LOCKTAG, LOCKTAG_TUPLE,
};
use types_tuple::backend_access_common_heaptuple::{Datum, DeformedColumn, FormedTuple};
// `types_datum::Datum` (the bare-word shim) survives only at the unmigrated
// cross-crate contract edge `SysCacheKey::Value`'s search-key word (C:
// `Datum key1..key4`), audited `types-cache` vocabulary not in this batch.
use types_datum::Datum as KeyDatum;
use types_tuple::heaptuple::{
    HeapTupleHeaderGetNatts, ItemPointerData, TupleDescData,
};

pub mod cacheinfo;
mod projections;

pub use cacheinfo::*;

/* ---------------------------------------------------------------------------
 * Per-backend module state (the syscache.c file-scope statics)
 * ------------------------------------------------------------------------- */

/// The syscache.c file-scope statics. `sys_cache_exists[id]` mirrors
/// `PointerIsValid(SysCache[cacheId])` (the `CatCache` control blocks
/// themselves are owned by the catcache provider).
struct SysCacheState<'mcx> {
    mcx: Mcx<'mcx>,
    /// `static bool CacheInitialized`.
    cache_initialized: bool,
    /// `PointerIsValid(SysCache[id])` per cache id.
    sys_cache_exists: [bool; SysCacheSize],
    /// `static Oid SysCacheRelationOid[]` + size: sorted, de-duplicated OIDs
    /// of tables that have caches on them.
    sys_cache_relation_oid: PgVec<'mcx, Oid>,
    /// `static Oid SysCacheSupportingRelOid[]` + size: sorted, de-duplicated
    /// OIDs of tables and indexes used by caches.
    sys_cache_supporting_rel_oid: PgVec<'mcx, Oid>,
}

mcx::bind!(SysCacheStateTy => SysCacheState<'mcx>);

thread_local! {
    static STATE: RefCell<Option<McxOwned<SysCacheStateTy>>> = const { RefCell::new(None) };
}

/// Run `f` over the backend-local state, creating it (with its owning
/// context, the analog of these statics living in `CacheMemoryContext`) on
/// first use.
fn with_state<R>(f: impl for<'mcx> FnOnce(&mut SysCacheState<'mcx>) -> R) -> R {
    STATE.with(|s| {
        let mut slot = s.borrow_mut();
        if slot.is_none() {
            let owned = McxOwned::<SysCacheStateTy>::try_new(
                MemoryContext::new("SysCache OID lists"),
                |mcx| {
                    Ok(SysCacheState {
                        mcx,
                        cache_initialized: false,
                        sys_cache_exists: [false; SysCacheSize],
                        sys_cache_relation_oid: PgVec::new_in(mcx),
                        sys_cache_supporting_rel_oid: PgVec::new_in(mcx),
                    })
                },
            )
            .expect("allocating the empty syscache state cannot fail");
            *slot = Some(owned);
        }
        slot.as_mut().unwrap().with_mut(f)
    })
}

/// `Assert(cacheId >= 0 && cacheId < SysCacheSize &&
/// PointerIsValid(SysCache[cacheId]))`.
fn assert_cache_exists(cache_id: i32) {
    debug_assert!(
        cache_id >= 0
            && (cache_id as usize) < SysCacheSize
            && with_state(|st| st.sys_cache_exists[cache_id as usize]),
        "syscache id {cache_id} out of range or not initialized"
    );
}

/* ---------------------------------------------------------------------------
 * InitCatalogCache / InitCatalogCachePhase2
 * ------------------------------------------------------------------------- */

/// `InitCatalogCache` — initialize the caches.
///
/// Note that no database access is done here; we only allocate memory and
/// initialize the cache structure. Interrogation of the database to complete
/// initialization of a cache happens upon first use of that cache.
pub fn InitCatalogCache() -> PgResult<()> {
    with_state(|st| -> PgResult<()> {
        debug_assert!(!st.cache_initialized);

        // SysCacheRelationOidSize = SysCacheSupportingRelOidSize = 0;
        let mut relation_oid: PgVec<'_, Oid> = vec_with_capacity_in(st.mcx, SysCacheSize)?;
        let mut supporting_rel_oid: PgVec<'_, Oid> =
            vec_with_capacity_in(st.mcx, SysCacheSize * 2)?;

        for (cache_id, desc) in cacheinfo.iter().enumerate() {
            // Assert that every enumeration value defined in syscache.h has
            // been populated in the cacheinfo array.
            debug_assert!(OidIsValid(desc.reloid));
            debug_assert!(OidIsValid(desc.indoid));
            // .nbuckets and .key[] are checked by InitCatCache().

            let created = catcache_seams::init_cat_cache::call(
                cache_id as i32,
                desc.reloid,
                desc.indoid,
                desc.nkeys,
                desc.key,
                desc.nbuckets,
            )?;
            if !created {
                return Err(PgError::error(format!(
                    "could not initialize cache {} ({})",
                    desc.reloid, cache_id
                )));
            }
            st.sys_cache_exists[cache_id] = true;

            // Accumulate data for OID lists, too. Both pushes stay within the
            // capacity reserved above.
            relation_oid.push(desc.reloid);
            supporting_rel_oid.push(desc.reloid);
            supporting_rel_oid.push(desc.indoid);

            // see comments for RelationInvalidatesSnapshotsOnly
            debug_assert!(!RelationInvalidatesSnapshotsOnly(desc.reloid));
        }

        debug_assert!(relation_oid.len() <= SysCacheSize);
        debug_assert!(supporting_rel_oid.len() <= SysCacheSize * 2);

        // Sort and de-dup OID arrays, so we can use binary search
        // (qsort(oid_compare) + qunique; Oid is unsigned so the unsigned
        // `Ord` is exactly `pg_cmp_u32`).
        relation_oid.sort_unstable();
        relation_oid.dedup();
        supporting_rel_oid.sort_unstable();
        supporting_rel_oid.dedup();

        st.sys_cache_relation_oid = relation_oid;
        st.sys_cache_supporting_rel_oid = supporting_rel_oid;
        st.cache_initialized = true;
        Ok(())
    })
}

/// `InitCatalogCachePhase2` — finish initializing the caches (including
/// database access).
///
/// This is *not* essential; normally we allow syscaches to be initialized on
/// first use. However, it is useful as a mechanism to preload the relcache
/// with entries for the most-commonly-used system catalogs, so we invoke it
/// when we need to write a new relcache init file.
pub fn InitCatalogCachePhase2() -> PgResult<()> {
    debug_assert!(with_state(|st| st.cache_initialized));

    for cache_id in 0..SysCacheSize {
        catcache_seams::init_cat_cache_phase2::call(cache_id as i32, true)?;
    }
    Ok(())
}

/* ---------------------------------------------------------------------------
 * SearchSysCache family
 * ------------------------------------------------------------------------- */

/// `SearchSysCache` — a layer on top of `SearchCatCache` that does the
/// initialization and key-setting for you.
///
/// Returns a copy of the cached tuple if one is found (see the module docs on
/// the ownership model), `None` if not.
pub fn SearchSysCache<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    assert_cache_exists(cacheId);

    pgrust_trace::trace!(pgrust_trace::Category::Syscache, "SearchSysCache cacheId={}", cacheId);
    catcache_seams::search_cat_cache::call(mcx, cacheId, key1, key2, key3, key4)
}

/// `SearchSysCache1`.
pub fn SearchSysCache1<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    assert_cache_exists(cacheId);
    debug_assert_eq!(catcache_seams::cache_nkeys::call(cacheId), 1);

    pgrust_trace::trace!(pgrust_trace::Category::Syscache, "SearchSysCache1 cacheId={}", cacheId);
    catcache_seams::search_cat_cache_1::call(mcx, cacheId, key1)
}

/// `SearchSysCache2`.
pub fn SearchSysCache2<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    assert_cache_exists(cacheId);
    debug_assert_eq!(catcache_seams::cache_nkeys::call(cacheId), 2);

    pgrust_trace::trace!(pgrust_trace::Category::Syscache, "SearchSysCache2 cacheId={}", cacheId);
    catcache_seams::search_cat_cache_2::call(mcx, cacheId, key1, key2)
}

/// `SearchSysCache3`.
pub fn SearchSysCache3<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    assert_cache_exists(cacheId);
    debug_assert_eq!(catcache_seams::cache_nkeys::call(cacheId), 3);

    pgrust_trace::trace!(pgrust_trace::Category::Syscache, "SearchSysCache3 cacheId={}", cacheId);
    catcache_seams::search_cat_cache_3::call(mcx, cacheId, key1, key2, key3)
}

/// `SearchSysCache4`.
pub fn SearchSysCache4<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    assert_cache_exists(cacheId);
    debug_assert_eq!(catcache_seams::cache_nkeys::call(cacheId), 4);

    pgrust_trace::trace!(pgrust_trace::Category::Syscache, "SearchSysCache4 cacheId={}", cacheId);
    catcache_seams::search_cat_cache_4::call(mcx, cacheId, key1, key2, key3, key4)
}

/// `ReleaseSysCache` — release a previously grabbed reference count on a
/// tuple. In the owned model the search returned a copy, so releasing is
/// consuming it (the cache-side refcount was dropped inside the search).
pub fn ReleaseSysCache(tuple: FormedTuple<'_>) {
    pgrust_trace::trace!(pgrust_trace::Category::Syscache, "ReleaseSysCache");
    drop(tuple);
}

/* ---------------------------------------------------------------------------
 * SearchSysCacheLocked1
 * ------------------------------------------------------------------------- */

/// `SearchSysCacheLocked1` — combine [`SearchSysCache1`] with acquiring a
/// `LOCKTAG_TUPLE` at mode `InplaceUpdateTupleLock`. A tool for complying
/// with the README.tuplock section "Locking to write inplace-updated tables";
/// the C contract ("after heap_update(), UnlockTuple and ReleaseSysCache")
/// is the returned [`lock_seams::LockGuard`]: the lock stays held for as
/// long as the guard lives and is released on its drop.
///
/// `my_database_id` is C's `MyDatabaseId` (globals.c), passed explicitly —
/// no ambient-global seams.
///
/// Since inplace updates may happen just before our LockTuple(), we must
/// return content acquired after LockTuple() of the TID we return; in the
/// happy case this takes two fetches, one to determine the TID to lock and
/// another to get the content and confirm the TID didn't change. (See the C
/// comment for the GRANT/CLUSTER/VACUUM interleaving this defeats.)
///
/// The returned tuple may be the subject of an uncommitted update, so this
/// doesn't prevent the "tuple concurrently updated" error.
pub fn SearchSysCacheLocked1<'mcx>(
    mcx: Mcx<'mcx>,
    my_database_id: Oid,
    cacheId: i32,
    key1: SysCacheKey<'_>,
) -> PgResult<Option<(lock_seams::LockGuard, FormedTuple<'mcx>)>> {
    let mut tid = item_pointer_invalid(); // ItemPointerSetInvalid(&tid)
    let mut held: Option<lock_seams::LockGuard> = None;
    loop {
        let lockmode: LOCKMODE = InplaceUpdateTupleLock;

        let tuple = SearchSysCache1(mcx, cacheId, key1)?;
        let tup = if let Some(guard) = held.take() {
            debug_assert!(item_pointer_is_valid(&tid));
            match tuple {
                None => {
                    // C releases explicitly here; propagate that surface.
                    guard.release()?;
                    return Ok(None);
                }
                Some(tup) if item_pointer_equals(&tid, &tup.tuple.t_self) => {
                    return Ok(Some((guard, tup)));
                }
                Some(tup) => {
                    // TID changed under us: release and retry with the new TID.
                    guard.release()?;
                    tup
                }
            }
        } else {
            match tuple {
                None => return Ok(None),
                Some(tup) => tup,
            }
        };

        tid = tup.tuple.t_self;
        ReleaseSysCache(tup);

        // Do like LockTuple(rel, &tid, lockmode). While cc_relisshared won't
        // change from one iteration to another, it may have been a temporary
        // "false" until our first SearchSysCache1().
        let dboid = if catcache_seams::cache_relisshared::call(cacheId) {
            InvalidOid
        } else {
            my_database_id
        };
        let mut tag = LOCKTAG::default();
        set_locktag_tuple(
            &mut tag,
            dboid,
            cacheinfo[cacheId as usize].reloid,
            tid.ip_blkid.block_number(),
            tid.ip_posid,
        );
        held = Some(lock_seams::lock_acquire(&tag, lockmode, false, false)?);

        // If an inplace update just finished, ensure we process the syscache
        // inval.
        //
        // If a heap_update() call just released its LOCKTAG_TUPLE, we'll
        // probably find the old tuple and reach "tuple concurrently updated".
        // If that heap_update() aborts, our LOCKTAG_TUPLE blocks inplace
        // updates while our caller works.
        inval_seams::accept_invalidation_messages::call()?;
    }
}

/* ---------------------------------------------------------------------------
 * Copy / Exists / Oid convenience wrappers
 * ------------------------------------------------------------------------- */

/// `SearchSysCacheCopy` — `SearchSysCache` plus (if successful) a modifiable
/// copy of the entry, the original released before returning.
pub fn SearchSysCacheCopy<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let tuple = SearchSysCache(mcx, cacheId, key1, key2, key3, key4)?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let newtuple = heap_copytuple(mcx, Some(&tup))?;
    ReleaseSysCache(tup);
    Ok(newtuple)
}

/// `SearchSysCacheLockedCopy1` — meld [`SearchSysCacheLocked1`] with
/// [`SearchSysCacheCopy`]. The C contract ("after heap_update(),
/// UnlockTuple and heap_freetuple") is the returned guard + owned copy.
pub fn SearchSysCacheLockedCopy1<'mcx>(
    mcx: Mcx<'mcx>,
    my_database_id: Oid,
    cacheId: i32,
    key1: SysCacheKey<'_>,
) -> PgResult<Option<(lock_seams::LockGuard, FormedTuple<'mcx>)>> {
    let tuple = SearchSysCacheLocked1(mcx, my_database_id, cacheId, key1)?;
    let Some((guard, tup)) = tuple else {
        return Ok(None);
    };
    let newtuple = heap_copytuple(mcx, Some(&tup))?;
    ReleaseSysCache(tup);
    Ok(newtuple.map(|t| (guard, t)))
}

/// `SearchSysCacheExists` — probe whether a tuple can be found. No lock is
/// retained on the syscache entry.
pub fn SearchSysCacheExists(
    mcx: Mcx<'_>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<bool> {
    let tuple = SearchSysCache(mcx, cacheId, key1, key2, key3, key4)?;
    match tuple {
        None => Ok(false),
        Some(tup) => {
            ReleaseSysCache(tup);
            Ok(true)
        }
    }
}

/// `GetSysCacheOid` — `SearchSysCache` returning the OID in the `oidcol`
/// column of the found tuple, or `InvalidOid` if no tuple could be found. No
/// lock is retained on the syscache entry.
pub fn GetSysCacheOid(
    mcx: Mcx<'_>,
    cacheId: i32,
    oidcol: AttrNumber,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Oid> {
    let tuple = SearchSysCache(mcx, cacheId, key1, key2, key3, key4)?;
    let Some(tup) = tuple else {
        return Ok(InvalidOid);
    };
    // heap_getattr(tuple, oidcol, SysCache[cacheId]->cc_tupdesc, &isNull)
    let (value, is_null) = getattr_with_cache_tupdesc(mcx, cacheId, &tup, oidcol as i32)?;
    debug_assert!(!is_null); // columns used as oids should never be NULL
    let result = match &value {
        Datum::ByVal(_) => value.as_oid(),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error("GetSysCacheOid: oid column is not by-value"))
        }
    };
    ReleaseSysCache(tup);
    Ok(result)
}

/* ---------------------------------------------------------------------------
 * pg_attribute attisdropped-aware helpers
 * ------------------------------------------------------------------------- */

/// `Anum_pg_attribute_attisdropped` (`catalog/pg_attribute.h`).
const Anum_pg_attribute_attisdropped: i32 = 17;

/// Read `((Form_pg_attribute) GETSTRUCT(tuple))->attisdropped`.
fn attisdropped(mcx: Mcx<'_>, cacheId: i32, tup: &FormedTuple<'_>) -> PgResult<bool> {
    let (value, is_null) = SysCacheGetAttr(mcx, cacheId, tup, Anum_pg_attribute_attisdropped)?;
    debug_assert!(!is_null);
    match &value {
        Datum::ByVal(_) => Ok(value.as_bool()),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("attisdropped is not by-value")),
    }
}

/// `SearchSysCacheAttName` — `SearchSysCache` on the ATTNAME cache, except
/// that it returns `None` if the found attribute is marked `attisdropped`.
/// Convenient for callers that want to act as though dropped attributes don't
/// exist.
pub fn SearchSysCacheAttName<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attname: &str,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let tuple = SearchSysCache2(
        mcx,
        ATTNAME,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Str(attname),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    if attisdropped(mcx, ATTNAME, &tup)? {
        ReleaseSysCache(tup);
        return Ok(None);
    }
    Ok(Some(tup))
}

/// `SearchSysCacheCopyAttName` — an attisdropped-aware [`SearchSysCacheCopy`].
pub fn SearchSysCacheCopyAttName<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attname: &str,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let tuple = SearchSysCacheAttName(mcx, relid, attname)?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let newtuple = heap_copytuple(mcx, Some(&tup))?;
    ReleaseSysCache(tup);
    Ok(newtuple)
}

/// `SearchSysCacheExistsAttName` — an attisdropped-aware
/// [`SearchSysCacheExists`].
pub fn SearchSysCacheExistsAttName(mcx: Mcx<'_>, relid: Oid, attname: &str) -> PgResult<bool> {
    let tuple = SearchSysCacheAttName(mcx, relid, attname)?;
    match tuple {
        None => Ok(false),
        Some(tup) => {
            ReleaseSysCache(tup);
            Ok(true)
        }
    }
}

/// `SearchSysCacheAttNum` — `SearchSysCache` on the ATTNUM cache, except that
/// it returns `None` if the found attribute is marked `attisdropped`.
pub fn SearchSysCacheAttNum<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let tuple = SearchSysCache2(
        mcx,
        ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    if attisdropped(mcx, ATTNUM, &tup)? {
        ReleaseSysCache(tup);
        return Ok(None);
    }
    Ok(Some(tup))
}

/// `SearchSysCacheCopyAttNum` — an attisdropped-aware [`SearchSysCacheCopy`].
pub fn SearchSysCacheCopyAttNum<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let tuple = SearchSysCacheAttNum(mcx, relid, attnum)?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let newtuple = heap_copytuple(mcx, Some(&tup))?;
    ReleaseSysCache(tup);
    Ok(newtuple)
}

/* ---------------------------------------------------------------------------
 * Attribute extraction
 * ------------------------------------------------------------------------- */

/// `heap_getattr(tup, attnum, tupleDesc, isnull)` (`access/htup_details.h`),
/// composed from the heaptuple unit's pieces: system attributes via
/// `heap_getsysattr`, attributes past the tuple's natts via `getmissingattr`,
/// NULLs via the bitmap check, everything else via `nocachegetattr`.
pub(crate) fn heap_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    attnum: i32,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<DeformedColumn<'mcx>> {
    if attnum <= 0 {
        return heap_getsysattr(mcx, &tup.tuple, attnum);
    }
    let header = tup
        .tuple
        .t_data
        .as_ref()
        .expect("heap_getattr: tuple has no t_data");
    if attnum > HeapTupleHeaderGetNatts(header) as i32 {
        return getmissingattr(mcx, tupdesc, attnum);
    }
    if heap_attisnull(&tup.tuple, attnum, Some(tupdesc)) {
        return Ok((Datum::null(), true));
    }
    Ok((nocachegetattr(mcx, &tup.tuple, attnum, tupdesc, &tup.data)?, false))
}

/// `heap_getattr` against `SysCache[cacheId]->cc_tupdesc` (borrowed through
/// the catcache seam's callback shape).
fn getattr_with_cache_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<DeformedColumn<'mcx>> {
    let mut result: Option<PgResult<DeformedColumn<'mcx>>> = None;
    catcache_seams::with_cache_tupdesc::call(cacheId, &mut |tupdesc| {
        result = Some(heap_getattr(mcx, tup, attnum, tupdesc));
    });
    result.expect("with_cache_tupdesc did not invoke its callback")
}

/// `SysCacheGetAttr` — given a tuple previously fetched by `SearchSysCache`,
/// extract a specific attribute; returns `(value, is_null)`.
///
/// Usually only used for attributes that could be NULL or variable length;
/// note it is legal to use a `cacheId` referencing a different cache for the
/// same catalog the tuple was fetched from.
pub fn SysCacheGetAttr<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    tup: &FormedTuple<'_>,
    attributeNumber: i32,
) -> PgResult<DeformedColumn<'mcx>> {
    // We just need to get the TupleDesc out of the cache entry, and then we
    // can apply heap_getattr(). Normally the cache control data is already
    // valid (because the caller recently fetched the tuple via this same
    // cache), but there are cases where we have to initialize the cache here.
    if cacheId < 0
        || (cacheId as usize) >= SysCacheSize
        || !with_state(|st| st.sys_cache_exists[cacheId as usize])
    {
        return Err(PgError::error(format!("invalid cache ID: {cacheId}")));
    }
    if !catcache_seams::cache_tupdesc_is_valid::call(cacheId) {
        catcache_seams::init_cat_cache_phase2::call(cacheId, false)?;
        debug_assert!(catcache_seams::cache_tupdesc_is_valid::call(cacheId));
    }

    getattr_with_cache_tupdesc(mcx, cacheId, tup, attributeNumber)
}

/// `SysCacheGetAttrNotNull` — as [`SysCacheGetAttr`], but the attribute
/// cannot be NULL; errors otherwise.
pub fn SysCacheGetAttrNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    tup: &FormedTuple<'_>,
    attributeNumber: i32,
) -> PgResult<Datum<'mcx>> {
    let (attr, isnull) = SysCacheGetAttr(mcx, cacheId, tup, attributeNumber)?;

    if isnull {
        let relname = lsyscache_seams::get_rel_name::call(mcx, cacheinfo[cacheId as usize].reloid)?
            .map(|s| s.as_str().to_owned())
            .unwrap_or_default();
        let mut colname = String::new();
        catcache_seams::with_cache_tupdesc::call(cacheId, &mut |tupdesc| {
            colname = String::from_utf8_lossy(
                tupdesc.attrs[(attributeNumber - 1) as usize].attname.name_str(),
            )
            .into_owned();
        });
        return Err(PgError::error(format!(
            "unexpected null value in cached tuple for catalog {relname} column {colname}"
        )));
    }

    Ok(attr)
}

/* ---------------------------------------------------------------------------
 * Hash / list / invalidate
 * ------------------------------------------------------------------------- */

/// `GetSysCacheHashValue` — the hash value that would be used for a tuple in
/// the specified cache with the given search keys.
///
/// Exposed because the hash value appears in cache invalidation operations,
/// so places outside the catcache code need to compute it.
pub fn GetSysCacheHashValue(
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<u32> {
    if cacheId < 0
        || (cacheId as usize) >= SysCacheSize
        || !with_state(|st| st.sys_cache_exists[cacheId as usize])
    {
        return Err(PgError::error(format!("invalid cache ID: {cacheId}")));
    }

    catcache_seams::get_cat_cache_hash_value::call(cacheId, key1, key2, key3, key4)
}

/// `GetSysCacheHashValue1` (`utils/syscache.h` convenience macro).
pub fn GetSysCacheHashValue1(cacheId: i32, key1: SysCacheKey<'_>) -> PgResult<u32> {
    GetSysCacheHashValue(cacheId, key1, SysCacheKey::UNUSED, SysCacheKey::UNUSED, SysCacheKey::UNUSED)
}

/// `GetSysCacheHashValue2` (`utils/syscache.h` convenience macro).
pub fn GetSysCacheHashValue2(
    cacheId: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
) -> PgResult<u32> {
    GetSysCacheHashValue(cacheId, key1, key2, SysCacheKey::UNUSED, SysCacheKey::UNUSED)
}

/// `SearchSysCacheList` — list-search interface: all member tuples matching
/// the partial key, copied into `mcx` in index order (C: a `struct catclist`
/// to be `ReleaseSysCacheList`d; dropping the vector is the release).
pub fn SearchSysCacheList<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    nkeys: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    if cacheId < 0
        || (cacheId as usize) >= SysCacheSize
        || !with_state(|st| st.sys_cache_exists[cacheId as usize])
    {
        return Err(PgError::error(format!("invalid cache ID: {cacheId}")));
    }

    catcache_seams::search_cat_cache_list::call(mcx, cacheId, nkeys, key1, key2, key3)
}

/// `SearchSysCacheList1` (`utils/syscache.h` convenience macro).
pub fn SearchSysCacheList1<'mcx>(
    mcx: Mcx<'mcx>,
    cacheId: i32,
    key1: SysCacheKey<'_>,
) -> PgResult<PgVec<'mcx, FormedTuple<'mcx>>> {
    SearchSysCacheList(mcx, cacheId, 1, key1, SysCacheKey::UNUSED, SysCacheKey::UNUSED)
}

/// `SysCacheInvalidate` — invalidate entries in the specified cache, given a
/// hash value. Only quasi-public: it should only be used by inval.c.
pub fn SysCacheInvalidate(cacheId: i32, hashValue: u32) -> PgResult<()> {
    if cacheId < 0 || (cacheId as usize) >= SysCacheSize {
        return Err(PgError::error(format!("invalid cache ID: {cacheId}")));
    }

    // if this cache isn't initialized yet, no need to do anything
    if !with_state(|st| st.sys_cache_exists[cacheId as usize]) {
        return Ok(());
    }

    catcache_seams::cat_cache_invalidate::call(cacheId, hashValue);
    Ok(())
}

/* ---------------------------------------------------------------------------
 * Relation membership predicates
 * ------------------------------------------------------------------------- */

/// `RelationInvalidatesSnapshotsOnly`.
///
/// Certain relations that do not have system caches send snapshot
/// invalidation messages in lieu of catcache messages, for the benefit of
/// `GetCatalogSnapshot()` (which can then reuse its existing MVCC snapshot
/// for scanning one of those catalogs if no invalidation has been received).
///
/// Relations that have syscaches need not (and must not) be listed here: the
/// catcache invalidation messages also flush the snapshot. If you add a
/// syscache for one of these relations, remove it from this list.
pub fn RelationInvalidatesSnapshotsOnly(relid: Oid) -> bool {
    /// `DbRoleSettingRelationId` (`catalog/pg_db_role_setting.h`).
    const DbRoleSettingRelationId: Oid = 2964;
    /// `DependRelationId` (`catalog/pg_depend.h`).
    const DependRelationId: Oid = 2608;
    /// `SharedDependRelationId` (`catalog/pg_shdepend.h`).
    const SharedDependRelationId: Oid = 1214;
    /// `DescriptionRelationId` (`catalog/pg_description.h`).
    const DescriptionRelationId: Oid = 2609;
    /// `SharedDescriptionRelationId` (`catalog/pg_shdescription.h`).
    const SharedDescriptionRelationId: Oid = 2396;
    /// `SecLabelRelationId` (`catalog/pg_seclabel.h`).
    const SecLabelRelationId: Oid = 3596;
    /// `SharedSecLabelRelationId` (`catalog/pg_shseclabel.h`).
    const SharedSecLabelRelationId: Oid = 3592;

    matches!(
        relid,
        DbRoleSettingRelationId
            | DependRelationId
            | SharedDependRelationId
            | DescriptionRelationId
            | SharedDescriptionRelationId
            | SecLabelRelationId
            | SharedSecLabelRelationId
    )
}

/// `RelationHasSysCache` — test whether a relation has a system cache.
pub fn RelationHasSysCache(relid: Oid) -> bool {
    with_state(|st| oid_binary_search(&st.sys_cache_relation_oid, relid))
}

/// `RelationSupportsSysCache` — test whether a relation supports a system
/// cache, ie it is either a cached table or the index used for a cache.
pub fn RelationSupportsSysCache(relid: Oid) -> bool {
    with_state(|st| oid_binary_search(&st.sys_cache_supporting_rel_oid, relid))
}

/// The C binary search of `RelationHasSysCache` / `RelationSupportsSysCache`
/// (`int low/high` widened so the empty-array `high = -1` cannot underflow).
fn oid_binary_search(arr: &[Oid], relid: Oid) -> bool {
    let mut low: i64 = 0;
    let mut high: i64 = arr.len() as i64 - 1;

    while low <= high {
        let middle = low + (high - low) / 2;
        let v = arr[middle as usize];
        if v == relid {
            return true;
        }
        if v < relid {
            low = middle + 1;
        } else {
            high = middle - 1;
        }
    }
    false
}

/* ---------------------------------------------------------------------------
 * Item-pointer / locktag helpers (the C macros the Locked1 loop uses)
 * ------------------------------------------------------------------------- */

/// `ItemPointerSetInvalid` — `InvalidBlockNumber` block, posid 0
/// (`InvalidOffsetNumber`).
fn item_pointer_invalid() -> ItemPointerData {
    ItemPointerData::new(0xffff_ffff, 0)
}

/// `ItemPointerIsValid(ptr)` — posid is a valid `OffsetNumber`.
fn item_pointer_is_valid(ptr: &ItemPointerData) -> bool {
    ptr.ip_posid != 0
}

/// `ItemPointerEquals(p1, p2)`.
fn item_pointer_equals(p1: &ItemPointerData, p2: &ItemPointerData) -> bool {
    p1.ip_blkid.block_number() == p2.ip_blkid.block_number() && p1.ip_posid == p2.ip_posid
}

/// `SET_LOCKTAG_TUPLE(tag, dboid, reloid, blocknum, offnum)`
/// (`storage/lock.h`).
fn set_locktag_tuple(tag: &mut LOCKTAG, dboid: Oid, reloid: Oid, blocknum: u32, offnum: u16) {
    tag.locktag_field1 = dboid;
    tag.locktag_field2 = reloid;
    tag.locktag_field3 = blocknum;
    tag.locktag_field4 = offnum;
    tag.locktag_type = LOCKTAG_TUPLE;
    tag.locktag_lockmethodid = DEFAULT_LOCKMETHOD;
}

/* ---------------------------------------------------------------------------
 * Seam installation
 * ------------------------------------------------------------------------- */

/// Install this unit's inward seams (the caller-shaped projected-row lookups
/// in `backend-utils-cache-syscache-seams`).
pub fn init_seams() {
    // functioncmds.c (CreateFunction) looks up a language by name via the
    // LANGNAME syscache; syscache.c is its real owner.
    backend_commands_functioncmds_seams::lookup_language_by_name::set(|langname| {
        projections::lookup_language_by_name(&langname)
    });
    // pg_proc.c (ProcedureCreate) probes for a pre-existing definition via the
    // PROCNAMEARGSNSP syscache; syscache.c is its real owner.
    backend_catalog_pg_proc_seams::search_proc_name_args_nsp::set(
        projections::search_proc_name_args_nsp,
    );
    // fmgr_sql_validator's PROCOID read (pg_proc.c:851); syscache.c is its owner.
    backend_catalog_pg_proc_seams::search_proc_oid_sql::set(projections::search_proc_oid_sql);
    backend_utils_cache_syscache_seams::lookup_enum_by_oid::set(projections::lookup_enum_by_oid);
    backend_utils_cache_syscache_seams::lookup_enum_by_typoid_name::set(
        projections::lookup_enum_by_typoid_name,
    );
    backend_utils_cache_syscache_seams::search_relation_relam::set(projections::search_relation_relam);
    backend_utils_cache_syscache_seams::search_am_handler::set(projections::search_am_handler);
    backend_utils_cache_syscache_seams::search_rewrite_oid::set(projections::search_rewrite_oid);
    backend_utils_cache_syscache_seams::search_relation_reloftype::set(projections::search_relation_reloftype);
    backend_utils_cache_syscache_seams::cast_by_source_target::set(projections::cast_by_source_target);
    backend_utils_cache_syscache_seams::search_opclass::set(projections::search_opclass);
    backend_utils_cache_syscache_seams::search_amop_list::set(projections::search_amop_list);
    backend_utils_cache_syscache_seams::amop_by_strategy_full::set(projections::amop_by_strategy_full);
    backend_utils_cache_syscache_seams::search_amproc_list::set(projections::search_amproc_list);
    backend_utils_cache_syscache_seams::search_amproc_list2::set(projections::search_amproc_list2);
    backend_utils_cache_syscache_seams::search_opclass_list_by_am::set(
        projections::search_opclass_list_by_am,
    );
    backend_utils_cache_syscache_seams::pg_operator_form::set(projections::pg_operator_form);
    backend_utils_cache_syscache_seams::oper_catlist3::set(projections::oper_catlist3);
    backend_utils_cache_syscache_seams::oper_catlist1::set(projections::oper_catlist1);
    backend_utils_cache_syscache_seams::oper_row_by_oid::set(projections::oper_row_by_oid);
    backend_commands_vacuum_seams::search_syscache_class::set(projections::search_syscache_class);
    backend_optimizer_util_clauses_seams::get_func_form::set(projections::get_func_form);
    backend_optimizer_util_clauses_seams::get_func_sql_body::set(projections::get_func_sql_body);
    backend_optimizer_util_clauses_seams::fetch_function_defaults::set(
        projections::fetch_function_defaults,
    );
    backend_utils_cache_syscache_seams::amop_list_by_opr::set(projections::amop_list_by_opr);
    backend_utils_cache_syscache_seams::oper_oprcode::set(projections::oper_oprcode);
    backend_utils_cache_syscache_seams::oper_oprcom::set(projections::oper_oprcom);
    backend_utils_cache_syscache_seams::oper_input_types::set(projections::oper_input_types);
    backend_utils_cache_syscache_seams::proc_isstrict::set(projections::proc_isstrict);
    backend_utils_cache_syscache_seams::proc_cost_rows::set(projections::proc_cost_rows);
    backend_utils_cache_syscache_seams::proc_argdefaults::set(projections::proc_argdefaults);
    backend_utils_cache_syscache_seams::open_partrel_tuple::set(projections::open_partrel_tuple);
    backend_utils_cache_syscache_seams::agg_row_by_oid::set(projections::agg_row_by_oid);
    backend_utils_cache_syscache_seams::agg_form_by_oid::set(projections::agg_form_by_oid);
    // prepagg.c `preprocess_aggref`'s bundled pg_aggregate read + transtype
    // resolution + GetAggInitVal — installed here (the syscache / type-IO owner)
    // since prepagg can't reach this layer without a dependency cycle.
    backend_optimizer_prep_prepagg_seams::get_agg_catalog_info::set(
        projections::get_agg_catalog_info,
    );
    backend_utils_cache_syscache_seams::foreign_data_wrapper_options::set(
        projections::foreign_data_wrapper_options,
    );
    backend_utils_cache_syscache_seams::foreign_server_options::set(
        projections::foreign_server_options,
    );
    backend_utils_cache_syscache_seams::user_mapping_options_by_oid::set(
        projections::user_mapping_options_by_oid,
    );
    backend_utils_cache_syscache_seams::foreign_data_wrapper_form::set(
        projections::foreign_data_wrapper_form,
    );
    backend_utils_cache_syscache_seams::foreign_server_form::set(projections::foreign_server_form);
    backend_utils_cache_syscache_seams::foreign_data_wrapper_oid_by_name::set(
        projections::foreign_data_wrapper_oid_by_name,
    );
    // proclang.c's syscache legs: the LANGNAME OID-by-name lookup and the
    // writable LANGNAME tuple for the create-vs-replace decision. The
    // `get_language_oid` wrapper (with the missing-language error) lives in the
    // proclang owner.
    backend_utils_cache_syscache_seams::language_oid_by_name::set(projections::language_oid_by_name);
    backend_utils_cache_syscache_seams::language_tuple_by_name::set(
        projections::language_tuple_by_name,
    );
    backend_utils_cache_syscache_seams::foreign_server_oid_by_name::set(
        projections::foreign_server_oid_by_name,
    );
    backend_utils_cache_syscache_seams::foreign_table_server_by_relid::set(
        projections::foreign_table_server_by_relid,
    );
    backend_utils_cache_syscache_seams::foreign_table_form::set(projections::foreign_table_form);
    backend_utils_cache_syscache_seams::user_mapping_form::set(projections::user_mapping_form);
    backend_utils_cache_syscache_seams::attribute_fdwoptions::set(
        projections::attribute_fdwoptions,
    );
    backend_utils_cache_syscache_seams::relation_supports_syscache::set(
        projections::relation_supports_syscache,
    );
    backend_utils_cache_syscache_seams::init_catalog_cache_phase2::set(
        projections::init_catalog_cache_phase2,
    );
    backend_utils_cache_syscache_seams::search_syscache_exists_reloid::set(
        projections::search_syscache_exists_reloid,
    );
    // functioncmds.c reaches `SearchSysCacheExists3(PROCNAMEARGSNSP, ...)`
    // through the functioncmds-seams `function_exists_in_namespace` channel
    // (AlterFunction / RenameFunction name-collision probe). syscache.c owns the
    // PROCNAMEARGSNSP cache, so it installs the probe.
    backend_commands_functioncmds_seams::function_exists_in_namespace::set(
        projections::function_exists_in_namespace,
    );
    backend_utils_cache_syscache_seams::rel_relkind::set(projections::rel_relkind);
    backend_utils_cache_syscache_seams::rel_relhastriggers::set(
        projections::rel_relhastriggers,
    );
    backend_utils_cache_syscache_seams::pg_proc_form::set(projections::pg_proc_form);
    backend_utils_cache_syscache_seams::pg_type_form::set(projections::pg_type_form);
    backend_utils_cache_syscache_seams::type_form::set(projections::type_form);
    backend_utils_cache_syscache_seams::search_type_attr_info::set(
        projections::search_type_attr_info,
    );
    backend_utils_cache_syscache_seams::get_opclass_oid::set(projections::get_opclass_oid);
    backend_utils_cache_syscache_seams::search_syscache_attname::set(
        projections::search_syscache_attname,
    );
    backend_utils_cache_syscache_seams::lookup_authid_by_oid::set(projections::lookup_authid_by_oid);
    backend_utils_cache_syscache_seams::authid_rolname::set(projections::authid_rolname);
    backend_utils_cache_syscache_seams::get_namespace_oid_cached::set(
        projections::get_namespace_oid_cached,
    );
    backend_utils_cache_syscache_seams::relname_relid::set(projections::relname_relid);
    backend_utils_cache_syscache_seams::lookup_authid_by_name::set(
        projections::lookup_authid_by_name,
    );
    backend_utils_cache_syscache_seams::lookup_authmem_by_keys::set(
        projections::lookup_authmem_by_keys,
    );
    backend_utils_cache_syscache_seams::lookup_authmem_list_by_role::set(
        projections::lookup_authmem_list_by_role,
    );
    backend_utils_cache_syscache_seams::lookup_language::set(projections::lookup_language);
    backend_utils_cache_syscache_seams::pg_index_has_predicate::set(
        projections::pg_index_has_predicate,
    );
    backend_utils_cache_syscache_seams::pg_index_tid_and_hasexprs::set(
        projections::pg_index_tid_and_hasexprs,
    );
    backend_utils_cache_syscache_seams::pg_index_exprs_text::set(
        projections::pg_index_exprs_text,
    );
    backend_utils_cache_syscache_seams::pg_index_pred_text::set(
        projections::pg_index_pred_text,
    );
    backend_utils_cache_syscache_seams::pg_class_relpartbound_text::set(
        projections::pg_class_relpartbound_text,
    );
    backend_utils_cache_syscache_seams::pg_class_relpartbound_text_direct::set(
        projections::pg_class_relpartbound_text,
    );
    backend_utils_cache_syscache_seams::search_pg_index_info::set(
        projections::search_pg_index_info,
    );
    backend_utils_cache_syscache_seams::rule_tuple_by_relname::set(
        projections::rule_tuple_by_relname,
    );
    backend_utils_cache_syscache_seams::class_relkind_namespace::set(
        projections::class_relkind_namespace,
    );
    backend_utils_cache_syscache_seams::collation_qualified_name::set(
        projections::collation_qualified_name,
    );
    backend_utils_cache_syscache_seams::search_pg_class_full_form::set(
        projections::search_pg_class_full_form,
    );
    backend_utils_cache_syscache_seams::database_syscache_hash_value::set(
        projections::database_syscache_hash_value,
    );
    // pg_locale.c catalog-read seams (the locale-relevant pg_database /
    // pg_collation columns + the MyDatabaseId / get_namespace_name the
    // version-mismatch path needs).
    backend_utils_adt_pg_locale_catalog_seams::database_locale_row::set(
        projections::database_locale_row,
    );
    backend_utils_adt_pg_locale_catalog_seams::collation_locale_row::set(
        projections::collation_locale_row,
    );
    backend_utils_adt_pg_locale_catalog_seams::my_database_id::set(|| {
        backend_utils_init_small_seams::my_database_id::call()
    });
    backend_utils_adt_pg_locale_catalog_seams::get_namespace_name::set(|nspid| {
        let scratch = mcx::MemoryContext::new("get_namespace_name (pg_locale)");
        let name = backend_utils_cache_lsyscache_seams::get_namespace_name::call(
            scratch.mcx(),
            nspid,
        )?
        .map(|s| s.as_str().to_string());
        Ok(name)
    });
    backend_utils_cache_syscache_seams::get_syscache_hash_value_constroid::set(
        projections::get_syscache_hash_value_constroid,
    );
    backend_utils_cache_syscache_seams::get_syscache_hash_value_typeoid::set(
        projections::get_syscache_hash_value_typeoid,
    );
    backend_utils_cache_syscache_seams::get_syscache_hash_value_oid::set(
        projections::get_syscache_hash_value_oid,
    );
    backend_utils_cache_syscache_seams::opfamily_namespace_method_name::set(
        projections::opfamily_namespace_method_name,
    );
    backend_utils_cache_syscache_seams::proc_row_by_oid::set(projections::proc_row_by_oid);
    backend_utils_cache_syscache_seams::proc_catlist::set(projections::proc_catlist);
    backend_utils_cache_syscache_seams::proc_arg_attrs::set(projections::proc_arg_attrs);
    backend_utils_cache_syscache_seams::proc_compile_row::set(projections::proc_compile_row);
    backend_utils_cache_syscache_seams::search_constraint_form_by_oid::set(
        projections::search_constraint_form_by_oid,
    );
    backend_utils_cache_syscache_seams::fetch_relchecks::set(projections::fetch_relchecks);
    backend_utils_cache_syscache_seams::search_syscache_copy_pg_class_tuple::set(
        projections::search_syscache_copy_pg_class_tuple,
    );
    backend_utils_cache_syscache_seams::search_syscache_copy_pg_class::set(
        projections::search_syscache_copy_pg_class,
    );
    backend_utils_cache_syscache_seams::search_syscache_copy_pg_index::set(
        projections::search_syscache_copy_pg_index,
    );
    backend_utils_cache_syscache_seams::lookup_proc::set(projections::lookup_proc);
    backend_utils_cache_syscache_seams::lookup_proc_result_info::set(
        projections::lookup_proc_result_info,
    );
    backend_utils_cache_syscache_seams::pg_index_indclass::set(projections::pg_index_indclass);
    backend_utils_cache_syscache_seams::read_constraint_form::set(
        projections::read_constraint_form,
    );
    backend_utils_cache_syscache_seams::get_conkey_array::set(projections::get_conkey_array);
    backend_utils_cache_syscache_seams::heap_get_conkey::set(projections::heap_get_conkey);
    backend_utils_cache_syscache_seams::deconstruct_fk_arrays::set(
        projections::deconstruct_fk_arrays,
    );
    backend_utils_cache_syscache_seams::search_constraint_tuple_by_oid::set(
        projections::search_constraint_tuple_by_oid,
    );
    // amutils.c SQL-level property reporting: the pg_class / pg_index catalog
    // projections `indexam_property` reads.
    backend_utils_adt_amutils_seams::index_relation::set(projections::amutils_index_relation);
    backend_utils_adt_amutils_seams::index_form::set(projections::amutils_index_form);

    // ACL/owner catalog-row projections (the aclmask/aclcheck F0 keystone).
    backend_utils_cache_syscache_seams::pg_class_owner_acl::set(projections::pg_class_owner_acl);
    backend_utils_cache_syscache_seams::pg_attribute_owner_acl::set(
        projections::pg_attribute_owner_acl,
    );
    // pg_attribute fixed-width form + nullable attoptions (lsyscache
    // get_atttype/get_attgenerated/get_atttypetypmodcoll + get_attoptions).
    backend_utils_cache_syscache_seams::pg_attribute_form::set(projections::pg_attribute_form);
    backend_utils_cache_syscache_seams::pg_attribute_attoptions::set(
        projections::pg_attribute_attoptions,
    );
    backend_utils_cache_syscache_seams::pg_namespace_owner_acl::set(
        projections::pg_namespace_owner_acl,
    );
    backend_utils_cache_syscache_seams::pg_type_owner_acl::set(projections::pg_type_owner_acl);
    backend_utils_cache_syscache_seams::object_owner_acl::set(projections::object_owner_acl);
    backend_utils_cache_syscache_seams::parameter_acl_by_name::set(
        projections::parameter_acl_by_name,
    );
    backend_utils_cache_syscache_seams::parameter_acl_by_oid::set(
        projections::parameter_acl_by_oid,
    );

    // pg_statistic (STATRELATTINH) — the selfuncs / lsyscache statistics path.
    backend_utils_cache_syscache_seams::search_statrelattinh::set(
        projections::search_statrelattinh,
    );
    backend_utils_cache_syscache_seams::release_stats_tuple::set(
        projections::release_stats_tuple,
    );
    backend_utils_cache_syscache_seams::pg_statistic_stanullfrac::set(
        projections::pg_statistic_stanullfrac,
    );
    backend_utils_cache_syscache_seams::pg_statistic_stadistinct::set(
        projections::pg_statistic_stadistinct,
    );
    backend_utils_cache_syscache_seams::pg_statistic_slot_meta::set(
        projections::pg_statistic_slot_meta,
    );
    backend_utils_cache_syscache_seams::syscache_get_attr_not_null_statistic::set(
        projections::syscache_get_attr_not_null_statistic,
    );
    backend_utils_cache_syscache_seams::pg_statistic_stawidth::set(
        projections::pg_statistic_stawidth,
    );

    // ---- batch 2026-06-17: additional caller-shaped syscache projections ----
    use backend_utils_cache_syscache_seams as s;
    // SearchSysCacheExists* probes.
    s::reloid_exists::set(projections::reloid_exists);
    s::tablespace_exists::set(projections::tablespace_exists);
    s::auth_oid_exists::set(projections::auth_oid_exists);
    s::namespace_name_exists::set(projections::namespace_name_exists);
    s::procoid_exists::set(projections::procoid_exists);
    s::operoid_exists::set(projections::operoid_exists);
    s::typeoid_exists::set(projections::typeoid_exists);
    s::colloid_exists::set(projections::colloid_exists);
    s::tsconfigoid_exists::set(projections::tsconfigoid_exists);
    s::tsdictoid_exists::set(projections::tsdictoid_exists);
    s::namespaceoid_exists::set(projections::namespaceoid_exists);
    s::type_exists::set(projections::type_exists);
    s::statext_exists::set(projections::statext_exists);
    s::ts_parser_exists::set(projections::ts_parser_exists);
    s::ts_dict_exists::set(projections::ts_dict_exists);
    s::ts_template_exists::set(projections::ts_template_exists);
    s::ts_config_exists::set(projections::ts_config_exists);
    s::opfamily_exists::set(projections::opfamily_exists);
    s::opclass_exists::set(projections::opclass_exists);
    s::amop_search_exists::set(projections::amop_search_exists);
    // GetSysCacheOid* probes.
    s::get_type_oid::set(projections::get_type_oid);
    s::get_opfamily_oid::set(projections::get_opfamily_oid);
    s::amop_oid::set(projections::amop_oid);
    s::amproc_oid::set(projections::amproc_oid);
    s::get_conversion_oid_cached::set(projections::get_conversion_oid_cached);
    s::get_statext_oid::set(projections::get_statext_oid);
    s::get_ts_parser_oid_cached::set(projections::get_ts_parser_oid_cached);
    s::get_ts_dict_oid_cached::set(projections::get_ts_dict_oid_cached);
    s::get_ts_template_oid_cached::set(projections::get_ts_template_oid_cached);
    s::get_ts_config_oid_cached::set(projections::get_ts_config_oid_cached);
    s::get_collation_oid_by_name_enc_nsp::set(projections::get_collation_oid_by_name_enc_nsp);
    s::get_am_oid_by_name::set(projections::get_am_oid_by_name);
    s::cast_oid::set(projections::cast_oid);
    s::get_publication_oid_syscache::set(projections::get_publication_oid_syscache);
    s::get_subscription_oid_syscache::set(projections::get_subscription_oid_syscache);
    // name lookups.
    s::search_type_name::set(projections::search_type_name);
    s::search_namespace_name::set(projections::search_namespace_name);
    s::search_am_name::set(projections::search_am_name);
    s::am_name::set(projections::am_name);
    s::rel_name::set(projections::rel_name);
    s::language_name::set(projections::language_name);
    s::constraint_name::set(projections::constraint_name);
    s::event_trigger_name::set(projections::event_trigger_name);
    s::get_publication_name_syscache::set(projections::get_publication_name_syscache);
    s::get_subscription_name_syscache::set(projections::get_subscription_name_syscache);
    s::search_attnum_attname::set(projections::search_attnum_attname);
    s::parameter_acl_name::set(projections::parameter_acl_name);
    // (namespace, name) -> CatalogObjectName.
    s::relation_namespace_and_name::set(projections::relation_namespace_and_name);
    s::type_namespace_and_name::set(projections::type_namespace_and_name);
    s::collation_namespace_and_name::set(projections::collation_namespace_and_name);
    s::conversion_namespace_and_name::set(projections::conversion_namespace_and_name);
    s::statext_namespace_and_name::set(projections::statext_namespace_and_name);
    s::ts_parser_namespace_and_name::set(projections::ts_parser_namespace_and_name);
    s::ts_dict_namespace_and_name::set(projections::ts_dict_namespace_and_name);
    s::ts_template_namespace_and_name::set(projections::ts_template_namespace_and_name);
    s::ts_config_namespace_and_name::set(projections::ts_config_namespace_and_name);
    // (namespace, owner, name) namespace rows.
    s::namespace_owner_row_by_name::set(projections::namespace_owner_row_by_name);
    s::namespace_owner_row_by_oid::set(projections::namespace_owner_row_by_oid);
    // scalar / small-tuple field projections.
    s::search_attname_attnum::set(projections::search_attname_attnum);
    s::search_attnum_attisdropped::set(projections::search_attnum_attisdropped);
    s::att_get_attnotnull::set(projections::att_get_attnotnull);
    s::search_relation_rls_flags::set(projections::search_relation_rls_flags);
    s::search_authid_rolsuper::set(projections::search_authid_rolsuper);
    s::database_datdba::set(projections::database_datdba);
    s::collation_isdeterministic::set(projections::collation_isdeterministic);
    s::collation_any_encoding_row::set(projections::collation_any_encoding_row);
    s::oper_exact::set(projections::oper_exact);
    s::constraint_type_index::set(projections::constraint_type_index);
    s::constraint_relid::set(projections::constraint_relid);
    s::constraint_identity::set(projections::constraint_identity);
    s::transform_funcs::set(projections::transform_funcs);
    s::transform_type_lang::set(projections::transform_type_lang);
    s::user_mapping_user_server::set(projections::user_mapping_user_server);
    s::statext_get_relid::set(projections::statext_get_relid);
    s::statext_namespace::set(projections::statext_namespace);
    s::statext_search_tuple::set(projections::statext_search_tuple);
    s::statext_data_search_tuple::set(projections::statext_data_search_tuple);
    s::search_seqrelid::set(projections::search_seqrelid);
    s::pg_type_default::set(projections::pg_type_default);
    s::search_pg_proc_fastpath::set(projections::search_pg_proc_fastpath);
    // Cross-crate install: funccache's pg_proc projection is a syscache catalog
    // read, so the syscache owner installs it into the funccache seam crate.
    backend_utils_cache_funccache_seams::search_proc_compile_info::set(
        projections::search_proc_compile_info,
    );
    s::fetch_class_reloptions::set(projections::fetch_class_reloptions);
    s::aggregate_tuple_by_fnoid::set(projections::aggregate_tuple_by_fnoid);
    s::publication_rel_pub_rel::set(projections::publication_rel_pub_rel);
    s::publication_rel_ids::set(projections::publication_rel_ids);
    s::publication_namespace_pub_nsp::set(projections::publication_namespace_pub_nsp);
    s::publication_namespace_ids::set(projections::publication_namespace_ids);
    // pg_index / pg_range / pg_opclass / pg_class extra projections.
    s::index_isclustered::set(projections::index_isclustered);
    s::index_get_relid::set(projections::index_get_relid);
    s::index_get_indisprimary::set(projections::index_get_indisprimary);
    s::pg_index_flags::set(projections::pg_index_flags);
    s::pg_range_form::set(projections::pg_range_form);
    s::pg_range_fields::set(projections::pg_range_fields);
    s::pg_range_rngtypid_of_multirange::set(projections::pg_range_rngtypid_of_multirange);
    s::pg_opclass_form::set(projections::pg_opclass_form);
    s::pg_opclass_keytype::set(projections::pg_opclass_keytype);
    s::opclass_namespace_method_name::set(projections::opclass_namespace_method_name);
    s::pg_class_extra::set(projections::pg_class_extra);
    s::rel_relispartition::set(projections::rel_relispartition);
    s::rel_namespace::set(projections::rel_namespace);
    s::search_partrelid_partdefid::set(projections::search_partrelid_partdefid);
    // pg_amop strategy/sortfamily + pg_proc projections.
    s::amop_by_opr_purpose::set(projections::amop_by_opr_purpose);
    s::amop_by_opr_purpose_family::set(projections::amop_by_opr_purpose_family);
    s::proc_proargnames_isnull::set(projections::proc_proargnames_isnull);
    // follow-on simple projections.
    s::collation_name::set(projections::collation_name);
    s::lookup_pg_class_by_relid::set(projections::lookup_pg_class_by_relid);
    // inval.c GETSTRUCT projections over a caller-supplied catalog tuple.
    s::pg_class_shape::set(projections::pg_class_shape);
    s::pg_attribute_attrelid::set(projections::pg_attribute_attrelid);
    s::pg_index_indexrelid::set(projections::pg_index_indexrelid);
    s::pg_constraint_fk_target::set(projections::pg_constraint_fk_target);
    s::search_am_by_name::set(projections::search_am_by_name);
    s::auth_members_of_member::set(projections::auth_members_of_member);

    // collationcmds.c (CREATE/ALTER COLLATION) syscache reads — pg_collation /
    // pg_database / pg_namespace projections, installed into its seam crate here.
    {
        use backend_commands_collationcmds_seams as cc;
        cc::collation_row_by_oid::set(projections::collation_row_by_oid);
        cc::collation_name_enc_nsp_exists::set(projections::collation_name_enc_nsp_exists);
        cc::namespace_exists::set(projections::namespace_exists);
        cc::database_locale_for_default_collation::set(
            projections::database_locale_for_default_collation,
        );
    }
    // dbcommands.c `have_createdb_privilege` reads pg_authid.rolcreatedb.
    backend_commands_dbcommands_seams::user_rolcreatedb::set(|_mcx, roleid| {
        projections::search_authid_rolcreatedb(roleid)
    });

    // plancache's InitPlanCache resolves the integer SysCacheIdentifier for the
    // caches it hooks; map its small enum to the genbki cache ids.
    backend_utils_cache_syscache_pc_seams::syscache_id::set(syscache_id_for);
}

/// Map plancache's `SysCacheId` to the genbki `SysCacheIdentifier` integer
/// (`catalog/syscache_ids.h`).
fn syscache_id_for(which: types_plancache::SysCacheId) -> types_error::PgResult<i32> {
    use types_plancache::SysCacheId;
    Ok(match which {
        SysCacheId::ProcOid => cacheinfo::PROCOID,
        SysCacheId::TypeOid => cacheinfo::TYPEOID,
        SysCacheId::NamespaceOid => cacheinfo::NAMESPACEOID,
        SysCacheId::OperOid => cacheinfo::OPEROID,
        SysCacheId::AmOpOpId => cacheinfo::AMOPOPID,
        SysCacheId::ForeignServerOid => cacheinfo::FOREIGNSERVEROID,
        SysCacheId::ForeignDataWrapperOid => cacheinfo::FOREIGNDATAWRAPPEROID,
    })
}
