#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod cacheinfo;
mod projections;
#[cfg(test)]
mod tests;

use core::cell::RefCell;

use catcache::{CatCTuple};
use datum::Datum;
use mcx::Mcx;
use types_core::{uint16, uint32, Oid};
use types_error::{PgError, PgResult};
use types_storage::lock::{InplaceUpdateTupleLock, LOCKTAG};
use types_tuple::{ItemPointerData, ItemPointerEquals, ItemPointerIsValid};

pub use cacheinfo::*;
pub use catcache::CatCKey as SysCacheKey;

struct RelOidArrays {
    initialized: bool,
    relation_oids: [Oid; SYS_CACHE_SIZE],
    n_relation: usize,
    supporting_oids: [Oid; SYS_CACHE_SIZE * 2],
    n_supporting: usize,
}

thread_local! {
    static ARRAYS: RefCell<RelOidArrays> = const {
        RefCell::new(RelOidArrays {
            initialized: false,
            relation_oids: [0; SYS_CACHE_SIZE],
            n_relation: 0,
            supporting_oids: [0; SYS_CACHE_SIZE * 2],
            n_supporting: 0,
        })
    };
}

fn sort_unique(arr: &mut [Oid]) -> usize {
    arr.sort_unstable();
    let mut n = 0;
    for i in 0..arr.len() {
        if i == 0 || arr[i] != arr[i - 1] {
            arr[n] = arr[i];
            n += 1;
        }
    }
    n
}

/// `InitCatalogCache()` — register all 85 caches with the catcache and build
/// the sorted, de-duplicated relation-OID lookup arrays.
pub fn InitCatalogCache() -> PgResult<()> {
    ARRAYS.with(|cell| -> PgResult<()> {
        let mut a = cell.borrow_mut();
        assert!(!a.initialized, "InitCatalogCache called twice");
        for (id, desc) in CACHEINFO.iter().enumerate() {
            debug_assert!(desc.reloid != 0 && desc.indoid != 0);
            debug_assert!(!RelationInvalidatesSnapshotsOnly(desc.reloid));
            catcache::InitCatCache(
                id as i32,
                desc.reloid,
                desc.indoid,
                desc.nkeys,
                &desc.key,
                desc.nbuckets,
            )?;
            a.relation_oids[id] = desc.reloid;
            let n = a.n_supporting;
            a.supporting_oids[n] = desc.reloid;
            a.supporting_oids[n + 1] = desc.indoid;
            a.n_supporting += 2;
        }
        a.n_relation = sort_unique(&mut a.relation_oids);
        let n = a.n_supporting;
        a.n_supporting = sort_unique(&mut a.supporting_oids[..n]);
        a.initialized = true;
        Ok(())
    })
}

/// `InitCatalogCachePhase2()`.
pub fn InitCatalogCachePhase2() -> PgResult<()> {
    for id in 0..SYS_CACHE_SIZE {
        catcache::InitCatCachePhase2(id as i32, true)?;
    }
    Ok(())
}

#[inline]
fn check_cache_id(cache_id: i32) {
    assert!(
        (0..SYS_CACHE_SIZE as i32).contains(&cache_id),
        "invalid cache ID: {cache_id}"
    );
}

pub fn SearchSysCache(
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    check_cache_id(cache_id);
    catcache::SearchCatCache(cache_id, key1, key2, key3, key4)
}

pub fn SearchSysCache1(cache_id: i32, key1: SysCacheKey<'_>) -> PgResult<Option<CatCTuple>> {
    check_cache_id(cache_id);
    catcache::SearchCatCache1(cache_id, key1)
}

pub fn SearchSysCache2(
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    check_cache_id(cache_id);
    catcache::SearchCatCache2(cache_id, key1, key2)
}

pub fn SearchSysCache3(
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    check_cache_id(cache_id);
    catcache::SearchCatCache3(cache_id, key1, key2, key3)
}

pub fn SearchSysCache4(
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    check_cache_id(cache_id);
    catcache::SearchCatCache4(cache_id, key1, key2, key3, key4)
}

pub fn ReleaseSysCache(tuple: CatCTuple) {
    catcache::ReleaseCatCache(tuple);
}

/// `SearchSysCacheLocked1` — SearchSysCache1 + LOCKTAG_TUPLE at
/// InplaceUpdateTupleLock, looped until the locked TID is the returned TID.
pub fn SearchSysCacheLocked1(cache_id: i32, key1: SysCacheKey<'_>) -> PgResult<Option<CatCTuple>> {
    check_cache_id(cache_id);
    let mut tid = ItemPointerData::invalid();
    let mut tag = LOCKTAG::default();
    loop {
        let lockmode = InplaceUpdateTupleLock;
        let tuple = SearchSysCache1(cache_id, key1)?;
        if ItemPointerIsValid(&tid) {
            let Some(tuple) = tuple else {
                lock_seams::lock_release::call(tag, lockmode, false)?;
                return Ok(None);
            };
            if ItemPointerEquals(&tid, &tuple.tuple().t_self) {
                return Ok(Some(tuple));
            }
            lock_seams::lock_release::call(tag, lockmode, false)?;
            tid = tuple.tuple().t_self;
            ReleaseSysCache(tuple);
        } else {
            let Some(tuple) = tuple else {
                return Ok(None);
            };
            tid = tuple.tuple().t_self;
            ReleaseSysCache(tuple);
        }

        let dbid = if catcache::cache_relisshared(cache_id) {
            0
        } else {
            init_small::globals::MyDatabaseId()
        };
        let reloid = CACHEINFO[cache_id as usize].reloid;
        tag = LOCKTAG::tuple(
            dbid,
            reloid,
            types_tuple::ItemPointerGetBlockNumber(&tid) as uint32,
            types_tuple::ItemPointerGetOffsetNumber(&tid) as uint16,
        );
        lock_seams::lock_acquire_extended::call(tag, lockmode, false, false, true, false)?;
        inval::local::AcceptInvalidationMessages()?;
    }
}

/// `SearchSysCacheCopy` — modifiable copy in `mcx`; cache ref released.
pub fn SearchSysCacheCopy<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Option<heaptuple::HeapTuple<'mcx>>> {
    let Some(tuple) = SearchSysCache(cache_id, key1, key2, key3, key4)? else {
        return Ok(None);
    };
    let copy = heaptuple::heap_copytuple(mcx, &tuple.tuple())?;
    ReleaseSysCache(tuple);
    Ok(Some(copy))
}

pub fn SearchSysCacheExists(
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<bool> {
    let Some(tuple) = SearchSysCache(cache_id, key1, key2, key3, key4)? else {
        return Ok(false);
    };
    ReleaseSysCache(tuple);
    Ok(true)
}

/// `GetSysCacheOid(cacheId, oidcol, ...)` — `InvalidOid` on a miss.
pub fn GetSysCacheOid(
    cache_id: i32,
    oidcol: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<Oid> {
    let Some(tuple) = SearchSysCache(cache_id, key1, key2, key3, key4)? else {
        return Ok(0);
    };
    let (d, isnull) = SysCacheGetAttr(cache_id, &tuple, oidcol)?;
    debug_assert!(!isnull);
    let result = d.as_oid();
    ReleaseSysCache(tuple);
    Ok(result)
}

const ANUM_PG_ATTRIBUTE_ATTISDROPPED: i32 = 17;

/// `SearchSysCacheAttName(relid, attname)` — `None` for a dropped column too.
pub fn SearchSysCacheAttName(relid: Oid, attname: &str) -> PgResult<Option<CatCTuple>> {
    let Some(tuple) = SearchSysCache2(
        ATTNAME,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Str(attname),
    )?
    else {
        return Ok(None);
    };
    let (d, _) = SysCacheGetAttr(ATTNAME, &tuple, ANUM_PG_ATTRIBUTE_ATTISDROPPED)?;
    if d.as_bool() {
        ReleaseSysCache(tuple);
        return Ok(None);
    }
    Ok(Some(tuple))
}

/// `SearchSysCacheAttNum(relid, attnum)`.
pub fn SearchSysCacheAttNum(relid: Oid, attnum: i16) -> PgResult<Option<CatCTuple>> {
    let Some(tuple) = SearchSysCache2(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?
    else {
        return Ok(None);
    };
    let (d, _) = SysCacheGetAttr(ATTNUM, &tuple, ANUM_PG_ATTRIBUTE_ATTISDROPPED)?;
    if d.as_bool() {
        ReleaseSysCache(tuple);
        return Ok(None);
    }
    Ok(Some(tuple))
}

/// `SysCacheGetAttr(cacheId, tup, attributeNumber, &isNull)`.
pub fn SysCacheGetAttr(cache_id: i32, tup: &CatCTuple, attnum: i32) -> PgResult<(Datum, bool)> {
    check_cache_id(cache_id);
    let tupdesc = match catcache::cache_tupdesc(cache_id) {
        Some(td) => td,
        None => {
            catcache::InitCatCachePhase2(cache_id, false)?;
            catcache::cache_tupdesc(cache_id).expect("phase-2 init left no tupdesc")
        }
    };
    let mut isnull = false;
    // SAFETY: the pinned tuple's image is a valid heap tuple of this cache's
    // own catalog descriptor.
    let d = unsafe { types_tuple::heap_getattr(&tup.tuple(), attnum, tupdesc, &mut isnull) };
    Ok((d, isnull))
}

/// `SysCacheGetAttrNotNull`.
pub fn SysCacheGetAttrNotNull(cache_id: i32, tup: &CatCTuple, attnum: i32) -> PgResult<Datum> {
    let (d, isnull) = SysCacheGetAttr(cache_id, tup, attnum)?;
    if isnull {
        return Err(notnull_error(cache_id, attnum));
    }
    Ok(d)
}

#[track_caller]
#[cold]
fn notnull_error(cache_id: i32, attnum: i32) -> Box<PgError> {
    let reloid = CACHEINFO[cache_id as usize].reloid;
    PgError::error(format!(
        "unexpected null value in cached tuple for catalog {reloid} column {attnum}"
    ))
    .into()
}

pub fn GetSysCacheHashValue(
    cache_id: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
    key4: SysCacheKey<'_>,
) -> PgResult<u32> {
    check_cache_id(cache_id);
    catcache::GetCatCacheHashValue(cache_id, key1, key2, key3, key4)
}

pub fn SearchSysCacheList(
    cache_id: i32,
    nkeys: i32,
    key1: SysCacheKey<'_>,
    key2: SysCacheKey<'_>,
    key3: SysCacheKey<'_>,
) -> PgResult<catcache::CatCListRef> {
    check_cache_id(cache_id);
    catcache::SearchCatCacheList(cache_id, nkeys, key1, key2, key3)
}

pub fn SearchSysCacheList1(cache_id: i32, key1: SysCacheKey<'_>) -> PgResult<catcache::CatCListRef> {
    SearchSysCacheList(cache_id, 1, key1, SysCacheKey::UNUSED, SysCacheKey::UNUSED)
}

pub fn ReleaseSysCacheList(list: catcache::CatCListRef) {
    catcache::ReleaseCatCacheList(list);
}

/// `SysCacheInvalidate(cacheId, hashValue)`.
pub fn SysCacheInvalidate(cache_id: i32, hash_value: uint32) {
    check_cache_id(cache_id);
    // No-op before InitCatalogCache (CatCacheInvalidate skips unregistered).
    catcache::CatCacheInvalidate(cache_id, hash_value);
}

// Catalogs with no syscache that send snapshot invalidations instead
// (syscache.c): pg_db_role_setting, pg_depend, pg_shdepend, pg_description,
// pg_shdescription, pg_seclabel, pg_shseclabel.
const SNAPSHOT_ONLY_RELIDS: [Oid; 7] = [2964, 2608, 1214, 2609, 2396, 3596, 3592];

pub fn RelationInvalidatesSnapshotsOnly(relid: Oid) -> bool {
    SNAPSHOT_ONLY_RELIDS.contains(&relid)
}

pub fn RelationHasSysCache(relid: Oid) -> bool {
    ARRAYS.with(|cell| {
        let a = cell.borrow();
        debug_assert!(a.initialized);
        a.relation_oids[..a.n_relation].binary_search(&relid).is_ok()
    })
}

pub fn RelationSupportsSysCache(relid: Oid) -> bool {
    ARRAYS.with(|cell| {
        let a = cell.borrow();
        debug_assert!(a.initialized);
        a.supporting_oids[..a.n_supporting].binary_search(&relid).is_ok()
    })
}

pub fn init_seams() {
    projections::install();
    syscache_seams::relation_has_sys_cache::set(RelationHasSysCache);
    syscache_seams::relation_supports_sys_cache::set(RelationSupportsSysCache);
    syscache_seams::init_catalog_cache_phase2::set(InitCatalogCachePhase2);
}

pub fn init_seams_pg_statistic_only() {
    projections::install_pg_statistic();
}
