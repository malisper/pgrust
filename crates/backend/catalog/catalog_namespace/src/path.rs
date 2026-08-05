use std::cell::{Cell, RefCell};

use mcx::{slice_in, vec_with_capacity_in, McxOwned, MemoryContext};
use mcx::{Mcx, PgHashMap, PgString, PgVec};
use types_core::{InvalidOid, Oid, PG_CATALOG_NAMESPACE};
use types_error::PgResult;

use cache_syscache::cacheinfo::{AUTHMEMROLEMEM, AUTHOID, DATABASEOID, NAMESPACEOID};

use crate::lookup::get_namespace_oid;
use crate::{
    with_path_state, OidIsValid, ACTIVE_PATH_GENERATION, BASE_CREATION_NAMESPACE,
    BASE_SEARCH_PATH_VALID, BASE_TEMP_CREATION_PENDING, MY_TEMP_NAMESPACE,
    NAMESPACE_SEARCH_PATH, NAMESPACE_USER,
};

const ACL_USAGE: u64 = 1 << 8;
const ACLCHECK_OK: i32 = 0;

const SPCACHE_RESET_THRESHOLD: usize = 256;

// Offsets into an SpCache pool; the pools are append-only until the wholesale
// cache reset, so spans stay valid for the entry's whole life (C: entry lists
// palloc'd in SearchPathCacheContext). len == 0 carries C's NIL double
// meaning: "never computed" and "legitimately empty" both recompute.
#[derive(Clone, Copy)]
struct Span {
    off: u32,
    len: u32,
}

const NIL: Span = Span { off: 0, len: 0 };

#[derive(Clone, Copy)]
struct SpEntry {
    name: Span,
    roleid: Oid,
    oidlist: Span,
    final_path: Span,
    first_ns: Oid,
    temp_missing: bool,
    force_recompute: bool,
}

pub(crate) struct SpCache<'mcx> {
    names: PgVec<'mcx, u8>,
    oids: PgVec<'mcx, Oid>,
    entries: PgVec<'mcx, SpEntry>,
    by_role: PgHashMap<'mcx, Oid, PgHashMap<'mcx, PgString<'mcx>, u32>>,
    // LastSearchPathCacheEntry.
    last: Option<u32>,
}

impl SpCache<'_> {
    fn name_of(&self, e: &SpEntry) -> &[u8] {
        &self.names[e.name.off as usize..(e.name.off + e.name.len) as usize]
    }

    fn oid_span(&self, s: Span) -> &[Oid] {
        &self.oids[s.off as usize..(s.off + s.len) as usize]
    }
}

fn append_oids<'mcx>(
    mcx: Mcx<'mcx>,
    pool: &mut PgVec<'mcx, Oid>,
    src: &[Oid],
) -> PgResult<Span> {
    let off = pool.len();
    pool.try_reserve(src.len()).map_err(|_| mcx.oom(src.len() * 4))?;
    for &o in src {
        pool.push(o);
    }
    Ok(Span { off: off as u32, len: src.len() as u32 })
}

mcx::bind!(SpCacheTy => SpCache<'mcx>);

thread_local! {
    static SPCACHE: RefCell<Option<McxOwned<SpCacheTy>>> = const { RefCell::new(None) };
    static SEARCH_PATH_CACHE_VALID: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn invalidate_search_path_cache() {
    SEARCH_PATH_CACHE_VALID.with(|c| c.set(false));
}

fn spcache_created() -> bool {
    SPCACHE.with(|s| s.borrow().is_some())
}

fn with_spcache<R>(f: impl for<'mcx> FnOnce(&mut SpCache<'mcx>) -> R) -> R {
    SPCACHE.with(|slot| {
        let mut s = slot.borrow_mut();
        s.as_mut().expect("spcache used before spcache_init").with_mut(f)
    })
}

fn with_spcache_mcx<R>(
    f: impl for<'mcx> FnOnce(Mcx<'mcx>, &mut SpCache<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    SPCACHE.with(|slot| {
        let mut s = slot.borrow_mut();
        s.as_mut().expect("spcache used before spcache_init").with_mut_mcx(f)
    })
}

fn spcache_init() -> PgResult<()> {
    SPCACHE.with(|slot| {
        {
            let s = slot.borrow();
            if let Some(cache) = s.as_ref() {
                if SEARCH_PATH_CACHE_VALID.with(Cell::get)
                    && cache.with(|c| c.entries.len()) < SPCACHE_RESET_THRESHOLD
                {
                    return Ok(());
                }
            }
        }

        SEARCH_PATH_CACHE_VALID.with(|c| c.set(false));
        BASE_SEARCH_PATH_VALID.with(|c| c.set(false));

        // Dropping the old McxOwned frees its context wholesale (C:
        // MemoryContextReset of SearchPathCacheContext).
        *slot.borrow_mut() = None;
        let fresh = McxOwned::try_new(
            MemoryContext::new("search_path processing cache"),
            |m| {
                Ok(SpCache {
                    names: vec_with_capacity_in(m, 256)?,
                    oids: vec_with_capacity_in(m, 64)?,
                    entries: vec_with_capacity_in(m, 16)?,
                    by_role: PgHashMap::with_capacity_in(4, m),
                    last: None,
                })
            },
        )?;
        *slot.borrow_mut() = Some(fresh);
        SEARCH_PATH_CACHE_VALID.with(|c| c.set(true));
        Ok(())
    })
}

fn spcache_lookup(searchPath: &str, roleid: Oid) -> bool {
    with_spcache(|c| {
        if let Some(i) = c.last {
            let e = c.entries[i as usize];
            if e.roleid == roleid && c.name_of(&e) == searchPath.as_bytes() {
                return true;
            }
        }
        match c.by_role.get(&roleid).and_then(|m| m.get(searchPath)) {
            Some(&i) => {
                c.last = Some(i);
                true
            }
            None => false,
        }
    })
}

fn spcache_insert(searchPath: &str, roleid: Oid) -> PgResult<u32> {
    with_spcache_mcx(|m, c| {
        if let Some(i) = c.last {
            let e = c.entries[i as usize];
            if e.roleid == roleid && c.name_of(&e) == searchPath.as_bytes() {
                return Ok(i);
            }
        }
        if let Some(&i) = c.by_role.get(&roleid).and_then(|inner| inner.get(searchPath)) {
            c.last = Some(i);
            return Ok(i);
        }

        let idx = c.entries.len() as u32;
        let name_off = c.names.len() as u32;
        mcx::vec_append_bytes(&mut c.names, searchPath.as_bytes())?;
        c.entries.try_reserve(1).map_err(|_| m.oom(core::mem::size_of::<SpEntry>()))?;
        c.entries.push(SpEntry {
            name: Span { off: name_off, len: searchPath.len() as u32 },
            roleid,
            oidlist: NIL,
            final_path: NIL,
            first_ns: InvalidOid,
            temp_missing: false,
            force_recompute: false,
        });
        let key = PgString::from_str_in(searchPath, m)?;
        c.by_role
            .entry(roleid)
            .or_insert_with(|| PgHashMap::with_capacity_in(4, m))
            .insert(key, idx);
        c.last = Some(idx);
        Ok(idx)
    })
}

fn database_encoding() -> wchar::pg_enc {
    // SQL_ASCII until mbutils arms it (backend_startup precedent).
    if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    }
}

fn preprocessNamespacePath(searchPath: &str, roleid: Oid) -> PgResult<(Vec<Oid>, bool)> {
    let scratch = MemoryContext::new("preprocessNamespacePath");
    let mcx = scratch.mcx();
    let Some(namelist) = varlena::split_identifier_string(mcx, searchPath, b',', database_encoding())?
    else {
        return Err(Box::new(
            types_error::PgError::error("invalid list syntax")
                .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR),
        ));
    };

    let mut oidlist: Vec<Oid> = Vec::new();
    let mut temp_missing = false;
    for curname in &namelist {
        if curname == "$user" {
            if let Some(rname) = syscache_seams::lookup_authid_rolname::call(mcx, roleid)? {
                let namespaceId = get_namespace_oid(rname.as_str(), true)?;
                if OidIsValid(namespaceId)
                    && aclchk_seams::object_aclcheck::call(
                        types_core::catalog::NAMESPACE_RELATION_ID,
                        namespaceId,
                        roleid,
                        ACL_USAGE,
                    )? == ACLCHECK_OK
                {
                    oidlist.push(namespaceId);
                }
            }
        } else if curname == "pg_temp" {
            let mtn = MY_TEMP_NAMESPACE.with(Cell::get);
            if OidIsValid(mtn) {
                oidlist.push(mtn);
            } else if oidlist.is_empty() {
                temp_missing = true;
            }
        } else {
            let namespaceId = get_namespace_oid(curname, true)?;
            if OidIsValid(namespaceId)
                && aclchk_seams::object_aclcheck::call(
                    types_core::catalog::NAMESPACE_RELATION_ID,
                    namespaceId,
                    roleid,
                    ACL_USAGE,
                )? == ACLCHECK_OK
            {
                oidlist.push(namespaceId);
            }
        }
    }

    Ok((oidlist, temp_missing))
}

fn finalNamespacePath(oidlist: &[Oid]) -> (Vec<Oid>, Oid) {
    let mut finalPath: Vec<Oid> = Vec::with_capacity(oidlist.len() + 2);

    for &namespaceId in oidlist {
        if !finalPath.contains(&namespaceId) {
            finalPath.push(namespaceId);
        }
    }

    let firstNS = finalPath.first().copied().unwrap_or(InvalidOid);

    if !finalPath.contains(&PG_CATALOG_NAMESPACE) {
        finalPath.insert(0, PG_CATALOG_NAMESPACE);
    }
    let mtn = MY_TEMP_NAMESPACE.with(Cell::get);
    if OidIsValid(mtn) && !finalPath.contains(&mtn) {
        finalPath.insert(0, mtn);
    }

    (finalPath, firstNS)
}

fn cachedNamespacePath<R>(
    searchPath: &str,
    roleid: Oid,
    consume: impl FnOnce(&SpEntry, &[Oid]) -> PgResult<R>,
) -> PgResult<R> {
    spcache_init()?;
    let idx = spcache_insert(searchPath, roleid)? as usize;

    let need_oidlist = with_spcache(|c| c.entries[idx].oidlist.len == 0);
    if need_oidlist {
        // Computed outside the spcache borrow: it reads the syscache and can
        // reenter namespace state.
        let (oidlist, temp_missing) = preprocessNamespacePath(searchPath, roleid)?;
        with_spcache_mcx(|m, c| {
            let span = append_oids(m, &mut c.oids, &oidlist)?;
            let e = &mut c.entries[idx];
            e.oidlist = span;
            e.temp_missing = temp_missing;
            Ok(())
        })?;
    }

    with_spcache_mcx(|m, c| {
        if c.entries[idx].final_path.len == 0 || c.entries[idx].force_recompute {
            // Transient owned list: the pool cannot be read and grown at once.
            let (final_path, first_ns) = finalNamespacePath(c.oid_span(c.entries[idx].oidlist));
            let span = append_oids(m, &mut c.oids, &final_path)?;
            let e = &mut c.entries[idx];
            e.final_path = span;
            e.first_ns = first_ns;
            e.force_recompute = false;
        }
        let e = c.entries[idx];
        consume(&e, c.oid_span(e.final_path))
    })
}

pub(crate) fn recomputeNamespacePath() -> PgResult<()> {
    let roleid = miscinit_seams::get_user_id::call();

    if BASE_SEARCH_PATH_VALID.with(Cell::get) && NAMESPACE_USER.with(Cell::get) == roleid {
        return Ok(());
    }

    let search_path = NAMESPACE_SEARCH_PATH.with(|s| s.borrow().clone()).unwrap_or_default();
    let pathChanged = cachedNamespacePath(&search_path, roleid, |entry, final_path| {
        with_path_state(|ps| {
            let changed = !(BASE_CREATION_NAMESPACE.with(Cell::get) == entry.first_ns
                && BASE_TEMP_CREATION_PENDING.with(Cell::get) == entry.temp_missing
                && ps.base_search_path.as_slice() == final_path);
            if changed {
                ps.base_search_path = slice_in(ps.mcx, final_path)?;
                BASE_CREATION_NAMESPACE.with(|c| c.set(entry.first_ns));
                BASE_TEMP_CREATION_PENDING.with(|c| c.set(entry.temp_missing));
            }
            Ok(changed)
        })
    })?;

    BASE_SEARCH_PATH_VALID.with(|c| c.set(true));
    NAMESPACE_USER.with(|c| c.set(roleid));

    if pathChanged {
        ACTIVE_PATH_GENERATION.with(|c| c.set(c.get() + 1));
    }
    Ok(())
}

pub fn check_search_path(mcx: Mcx<'_>, newval: &str) -> PgResult<bool> {
    let use_cache = spcache_created();
    let mut roleid = InvalidOid;

    if use_cache {
        spcache_init()?;
        roleid = miscinit_seams::get_user_id::call();
        if spcache_lookup(newval, roleid) {
            return Ok(true);
        }
    }

    if varlena::split_identifier_string(mcx, newval, b',', database_encoding())?.is_none() {
        guc_seams::guc_check_errdetail::call("List syntax is invalid.".to_string());
        return Ok(false);
    }

    if use_cache {
        spcache_insert(newval, roleid)?;
    }
    Ok(true)
}

pub fn assign_search_path(_newval: Option<&str>) {
    BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
}

pub(crate) fn check_search_path_hook(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    let scratch = MemoryContext::new("check_search_path");
    check_search_path(scratch.mcx(), newval.as_deref().unwrap_or(""))
}

pub(crate) fn assign_search_path_hook(
    newval: Option<&str>,
    _extra: Option<&guc_tables::GucHookExtra>,
) {
    assign_search_path(newval);
}

pub fn InitializeSearchPath() -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        with_path_state(|ps| -> PgResult<()> {
            ps.base_search_path = slice_in(ps.mcx, &[PG_CATALOG_NAMESPACE])?;
            Ok(())
        })?;
        BASE_CREATION_NAMESPACE.with(|c| c.set(PG_CATALOG_NAMESPACE));
        BASE_TEMP_CREATION_PENDING.with(|c| c.set(false));
        BASE_SEARCH_PATH_VALID.with(|c| c.set(true));
        NAMESPACE_USER.with(|c| c.set(miscinit_seams::get_user_id::call()));
        ACTIVE_PATH_GENERATION.with(|c| c.set(c.get() + 1));
    } else {
        // Registered once per thread: a retained pool standby (wretain) runs
        // InitializeSearchPath once per claim, and the callback tables are
        // fixed-capacity.
        thread_local! {
            static CALLBACKS_REGISTERED: std::cell::Cell<bool> =
                const { std::cell::Cell::new(false) };
        }
        if !CALLBACKS_REGISTERED.get() {
            inval::invalidate::CacheRegisterSyscacheCallback(
                NAMESPACEOID,
                InvalidationCallback,
                datum::Datum::null(),
            )?;
            inval::invalidate::CacheRegisterSyscacheCallback(
                AUTHOID,
                InvalidationCallback,
                datum::Datum::null(),
            )?;
            inval::invalidate::CacheRegisterSyscacheCallback(
                AUTHMEMROLEMEM,
                InvalidationCallback,
                datum::Datum::null(),
            )?;
            inval::invalidate::CacheRegisterSyscacheCallback(
                DATABASEOID,
                InvalidationCallback,
                datum::Datum::null(),
            )?;
            CALLBACKS_REGISTERED.set(true);
        }

        BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
        SEARCH_PATH_CACHE_VALID.with(|c| c.set(false));
    }
    Ok(())
}

fn InvalidationCallback(_arg: datum::Datum, _cacheid: i32, _hashvalue: u32) {
    BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
    SEARCH_PATH_CACHE_VALID.with(|c| c.set(false));
}

pub fn fetch_search_path<'mcx>(
    mcx: Mcx<'mcx>,
    includeImplicit: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    recomputeNamespacePath()?;

    if BASE_TEMP_CREATION_PENDING.with(Cell::get) {
        crate::AccessTempTableNamespace(mcx, true)?;
        recomputeNamespacePath()?;
    }

    let mut result = with_path_state(|ps| slice_in(mcx, &ps.base_search_path))?;
    if !includeImplicit {
        let creation = BASE_CREATION_NAMESPACE.with(Cell::get);
        while !result.is_empty() && result[0] != creation {
            result.remove(0);
        }
    }
    Ok(result)
}

/// C `fetch_search_path_array`: fills `sarray` (temp namespace excluded) and
/// returns the would-be count, which may exceed `sarray.len()`.
pub fn fetch_search_path_array(sarray: &mut [Oid]) -> PgResult<usize> {
    recomputeNamespacePath()?;

    let mtn = MY_TEMP_NAMESPACE.with(Cell::get);
    Ok(with_path_state(|ps| {
        let mut count = 0;
        for &namespaceId in ps.base_search_path.iter() {
            if namespaceId == mtn {
                continue;
            }
            if count < sarray.len() {
                sarray[count] = namespaceId;
            }
            count += 1;
        }
        count
    }))
}

pub struct SearchPathMatcher<'mcx> {
    pub schemas: PgVec<'mcx, Oid>,
    pub addCatalog: bool,
    pub addTemp: bool,
    // Zero is valid input ("not known equal to the active path").
    pub generation: u64,
}

pub fn GetSearchPathMatcher<'mcx>(mcx: Mcx<'mcx>) -> PgResult<SearchPathMatcher<'mcx>> {
    recomputeNamespacePath()?;

    let creation = BASE_CREATION_NAMESPACE.with(Cell::get);
    let mtn = MY_TEMP_NAMESPACE.with(Cell::get);
    with_path_state(|ps| {
        let path = ps.base_search_path.as_slice();
        let mut skip = 0;
        let mut addTemp = false;
        let mut addCatalog = false;
        while skip < path.len() && path[skip] != creation {
            if path[skip] == mtn {
                addTemp = true;
            } else {
                debug_assert_eq!(path[skip], PG_CATALOG_NAMESPACE);
                addCatalog = true;
            }
            skip += 1;
        }
        Ok(SearchPathMatcher {
            schemas: slice_in(mcx, &path[skip..])?,
            addCatalog,
            addTemp,
            generation: ACTIVE_PATH_GENERATION.with(Cell::get),
        })
    })
}

pub fn CopySearchPathMatcher<'mcx>(
    mcx: Mcx<'mcx>,
    path: &SearchPathMatcher<'_>,
) -> PgResult<SearchPathMatcher<'mcx>> {
    Ok(SearchPathMatcher {
        schemas: slice_in(mcx, &path.schemas)?,
        addCatalog: path.addCatalog,
        addTemp: path.addTemp,
        generation: path.generation,
    })
}

pub fn SearchPathMatchesCurrentEnvironment(path: &mut SearchPathMatcher<'_>) -> PgResult<bool> {
    recomputeNamespacePath()?;

    let generation = ACTIVE_PATH_GENERATION.with(Cell::get);
    if path.generation == generation {
        return Ok(true);
    }

    let matches = with_path_state(|ps| {
        let active = ps.base_search_path.as_slice();
        let mut lc = 0;
        if path.addTemp {
            if lc < active.len() && active[lc] == MY_TEMP_NAMESPACE.with(Cell::get) {
                lc += 1;
            } else {
                return false;
            }
        }
        if path.addCatalog {
            if lc < active.len() && active[lc] == PG_CATALOG_NAMESPACE {
                lc += 1;
            } else {
                return false;
            }
        }
        let cur = active.get(lc).copied().unwrap_or(InvalidOid);
        if BASE_CREATION_NAMESPACE.with(Cell::get) != cur {
            return false;
        }
        for &sch in path.schemas.iter() {
            if lc < active.len() && active[lc] == sch {
                lc += 1;
            } else {
                return false;
            }
        }
        lc == active.len()
    });
    if !matches {
        return Ok(false);
    }

    path.generation = generation;
    Ok(true)
}
