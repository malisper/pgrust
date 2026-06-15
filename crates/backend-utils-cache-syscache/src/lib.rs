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

    catcache_seams::search_cat_cache_4::call(mcx, cacheId, key1, key2, key3, key4)
}

/// `ReleaseSysCache` — release a previously grabbed reference count on a
/// tuple. In the owned model the search returned a copy, so releasing is
/// consuming it (the cache-side refcount was dropped inside the search).
pub fn ReleaseSysCache(tuple: FormedTuple<'_>) {
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
        Datum::ByRef(_) => {
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
        Datum::ByRef(_) => Err(PgError::error("attisdropped is not by-value")),
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
fn heap_getattr<'mcx>(
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
    backend_utils_cache_syscache_seams::search_relation_relam::set(projections::search_relation_relam);
    backend_utils_cache_syscache_seams::search_relation_reloftype::set(projections::search_relation_reloftype);
    backend_utils_cache_syscache_seams::cast_by_source_target::set(projections::cast_by_source_target);
    backend_utils_cache_syscache_seams::search_opclass::set(projections::search_opclass);
    backend_utils_cache_syscache_seams::search_amop_list::set(projections::search_amop_list);
    backend_utils_cache_syscache_seams::search_amproc_list::set(projections::search_amproc_list);
}
