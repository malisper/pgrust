use types_core::Oid;
use types_error::{error, PgError, PgResult};
use types_storage::lock::AccessShareLock;
use types_tuple::TupleDescData;

use crate::compute::get_cc_hash_eq_funcs;
use crate::with_state;

const AMNAME: i32 = 1;
const AMOID: i32 = 2;
const AUTHMEMMEMROLE: i32 = 8;
const AUTHNAME: i32 = 10;
const AUTHOID: i32 = 11;
const DATABASEOID: i32 = 21;
const INDEXRELID: i32 = 34;
const OIDOID: Oid = 26;

/// `CatalogCacheInitializeCache`. Runs with no state borrow held:
/// `table_open` reaches the relcache, which re-enters the catcache.
pub(crate) fn catalog_cache_initialize_cache(cache_id: i32) -> PgResult<()> {
    let (reloid, nkeys, keyno) = with_state(|st| {
        let c = st.cache(cache_id);
        (c.cc_reloid, c.cc_nkeys, c.cc_keyno)
    });

    let scratch = mcx::MemoryContext::new("CatalogCacheInitializeCache");
    let relation = table::table_open(scratch.mcx(), reloid, AccessShareLock)?;

    with_state(|st| -> PgResult<()> {
        let mcx = st.mcx;
        // Set once for the backend's life, never rebuilt (catalog schemas
        // are immutable), so the leak below is honest.
        let copied: TupleDescData<'_> = tupdesc::CreateTupleDescCopyConstr(mcx, relation.descr())?;
        // Justified bare Box: the droppy descriptor header cannot live in a
        // no-drop arena; the leak is C's never-freed CacheMemoryContext copy.
        let leaked: &mut TupleDescData<'_> = Box::leak(Box::new(copied));
        // SAFETY: the header is leaked and its inner allocations live in the
        // ManuallyDrop'd, never-reset CacheMemoryContext (crate::STATE); it
        // is written once here and no path frees or rebuilds it, so
        // extending to 'static is sound.
        let td: &'static TupleDescData<'static> =
            unsafe { core::mem::transmute::<&TupleDescData<'_>, &'static TupleDescData<'static>>(leaked) };

        let cache = st.cache_mut(cache_id);
        cache.cc_tupdesc = Some(td);
        cache.cc_relisshared = relation.rd_rel.relisshared;
        cache.cc_relname = Some(mcx::PgString::from_str_in(relation.name(), mcx)?);

        for i in 0..nkeys as usize {
            let keytype: Oid = if keyno[i] > 0 {
                let a = td.attr((keyno[i] - 1) as usize);
                debug_assert!(a.attnotnull);
                a.atttypid
            } else if keyno[i] < 0 {
                return Err(PgError::new(error::FATAL, "sys attributes are not supported in caches").into());
            } else {
                OIDOID
            };
            let (kind, eqfunc) = get_cc_hash_eq_funcs(keytype);
            let cache = st.cache_mut(cache_id);
            cache.cc_kind[i] = kind;
            cache.cc_eqfunc[i] = eqfunc;
        }
        st.cache_mut(cache_id).initialized = true;
        Ok(())
    })?;

    table::table_close(relation, AccessShareLock)?;
    Ok(())
}

/// `InitCatCachePhase2(cache, touchindex)`.
pub fn InitCatCachePhase2(cache_id: i32, touch_index: bool) -> PgResult<()> {
    if !with_state(|st| st.cache(cache_id).initialized) {
        catalog_cache_initialize_cache(cache_id)?;
    }
    if touch_index && cache_id != AMOID && cache_id != AMNAME {
        let (reloid, indexoid) = with_state(|st| {
            let c = st.cache(cache_id);
            (c.cc_reloid, c.cc_indexoid)
        });
        /* lock the catalog before the index: deadlock avoidance */
        lmgr_seams::lock_relation_oid::call(reloid, AccessShareLock)?;
        let scratch = mcx::MemoryContext::new("InitCatCachePhase2");
        let idesc = indexam::index_open(scratch.mcx(), indexoid, AccessShareLock)?;
        #[cfg(debug_assertions)]
        {
            let idx = idesc.rd_index.as_ref().expect("index_open returned a non-index");
            debug_assert!(idx.indisunique && idx.indimmediate);
        }
        indexam::index_close(idesc, AccessShareLock)?;
        lmgr_seams::unlock_relation_oid::call(reloid, AccessShareLock)?;
    }
    Ok(())
}

/// `IndexScanOK(cache)`.
pub(crate) fn IndexScanOK(cache_id: i32) -> bool {
    match cache_id {
        INDEXRELID => relcache_seams::critical_relcaches_built::call(),
        AMOID | AMNAME => false,
        AUTHNAME | AUTHOID | AUTHMEMMEMROLE | DATABASEOID => {
            relcache_seams::critical_shared_relcaches_built::call()
        }
        _ => true,
    }
}

pub fn cache_nkeys(cache_id: i32) -> i32 {
    with_state(|st| st.cache(cache_id).cc_nkeys)
}

pub fn cache_relisshared(cache_id: i32) -> bool {
    with_state(|st| st.cache(cache_id).cc_relisshared)
}

/// `cache->cc_tupdesc` (`None` ⇔ phase-2 init has not run).
pub fn cache_tupdesc(cache_id: i32) -> Option<&'static TupleDescData<'static>> {
    with_state(|st| st.cache(cache_id).cc_tupdesc)
}
