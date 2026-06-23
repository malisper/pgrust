//! Cache initialization + metadata (`catcache.c`):
//! `CatalogCacheInitializeCache`, `ConditionalCatalogCacheInitializeCache`,
//! `InitCatCachePhase2`, `IndexScanOK`, and the small per-cache metadata reads
//! exposed as outward seams (`cache_nkeys`, `cache_relisshared`,
//! `cache_tupdesc_is_valid`, `with_cache_tupdesc`).
//!
//! `CatalogCacheInitializeCache` reaches the relcache (catalog relname /
//! relisshared / column types / tupdesc) — routed through `relation_open`
//! (`table_open`) and the relcache seams. `IndexScanOK` depends on
//! `criticalRelcachesBuilt` / `criticalSharedRelcachesBuilt`, read through the
//! relcache owner's seams (panic until relcache lands).
//!
//! The arena (`cache::catcache`) deliberately does
//! not store `cc_tupdesc` (which in C lives in `CacheMemoryContext`): it lives
//! in this module's [`TUPDESC_STORE`] — a backend-local `CacheMemoryContext`
//! analog — and the arena records only [`ArenaCatCache::initialized`]. The
//! `with_cache_tupdesc` / `cache_tupdesc_is_valid` seams read it from there.

use core::cell::RefCell;
use std::thread_local;

use mcx::{McxOwned, MemoryContext, PgBox, PgVec};
use cache::catcache::CacheIdx;
use types_core::Oid;
use types_core::catalog::{C_COLLATION_OID, OIDOID};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, InvalidOid};
use types_error::PgResult;
use types_error::pg_error::PgError;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::AccessShareLock;
use types_tuple::heaptuple::TupleDescData;

use crate::core_compute;
use crate::{find_cache_by_id, with_arena};

/* ----------------------------------------------------------------------------
 * `SysCacheIdentifier` values (`utils/syscache.h`, generated). Only the small
 * set referenced by `IndexScanOK` / the `ConditionalCatalogCacheInitializeCache`
 * assert is mirrored here; the catcache crate is below `syscache`.
 * ------------------------------------------------------------------------- */
const AMNAME: i32 = 1;
const AMOID: i32 = 2;
const ATTNUM: i32 = 7;
const AUTHMEMMEMROLE: i32 = 8;
const AUTHNAME: i32 = 10;
const AUTHOID: i32 = 11;
const DATABASEOID: i32 = 21;
const INDEXRELID: i32 = 34;
const TYPEOID: i32 = 82;

/* ----------------------------------------------------------------------------
 * The per-cache tuple-descriptor store — the `CacheMemoryContext`-resident
 * `cc_tupdesc` array, kept out of the arena (the descriptor owns its own
 * allocations and is not part of the index graph). Keyed by syscache id.
 * ------------------------------------------------------------------------- */

struct TupdescStore<'mcx> {
    /// The store's own `CacheMemoryContext` handle (where the descriptors and
    /// the vector live).
    mcx: mcx::Mcx<'mcx>,
    /// `(cache->id, cache->cc_tupdesc)` pairs.
    descs: PgVec<'mcx, (i32, PgBox<'mcx, TupleDescData<'mcx>>)>,
}

impl<'mcx> TupdescStore<'mcx> {
    fn new(mcx: mcx::Mcx<'mcx>) -> Self {
        TupdescStore { mcx, descs: PgVec::new_in(mcx) }
    }
}

mcx::bind!(TupdescStoreTy => TupdescStore<'mcx>);

thread_local! {
    /// The `CacheMemoryContext` analog co-owning every cache's `cc_tupdesc`.
    /// `None` until the first cache completes phase-2 init.
    static TUPDESC_STORE: RefCell<Option<McxOwned<TupdescStoreTy>>> =
        const { RefCell::new(None) };
}

/// Run `f` with mutable access to the tupdesc store, creating it on first use.
fn with_tupdesc_store<R>(f: impl for<'mcx> FnOnce(&mut TupdescStore<'mcx>) -> R) -> R {
    TUPDESC_STORE.with(|s| {
        {
            let mut slot = s.borrow_mut();
            if slot.is_none() {
                // C: the catcache tuple descriptors live in `CacheMemoryContext`.
                // Create this store's context as a child of the process
                // `CacheMemoryContext` so it shows up as one of its children in
                // pg_get_backend_memory_contexts() (faithful tree shape).
                let owned = McxOwned::<TupdescStoreTy>::try_new(
                    relcache_seams::cache_memory_context::call()
                        .context()
                        .new_child("CatCacheContext"),
                    |mcx| Ok(TupdescStore::new(mcx)),
                )
                .expect("allocating the empty catcache tupdesc store cannot fail");
                *slot = Some(owned);
            }
        }
        let mut slot = s.borrow_mut();
        slot.as_mut().unwrap().with_mut(f)
    })
}

/// `CatalogCacheInitializeCache(cache)` — final init: load the tupdesc /
/// relname / relisshared (via the relcache) and set up per-key fast-kind
/// selection.
///
/// IMPORTANT: this must NOT be called while the catcache `with_arena` borrow is
/// held — `relation_open` (below) loads the relcache, which itself performs
/// catcache lookups (`SearchSysCache`/`cache_nkeys`) that re-enter `with_arena`.
/// All arena access here is therefore done through short scoped borrows that are
/// released before `relation_open`.
pub(crate) fn catalog_cache_initialize_cache(cache_idx: CacheIdx) -> PgResult<()> {
    // relation = table_open(cache->cc_reloid, AccessShareLock);
    let (cc_reloid, cc_nkeys, cc_keyno) = with_arena(|arena| {
        let cache = &arena.caches[cache_idx.0];
        (cache.cc_reloid, cache.cc_nkeys, cache.cc_keyno)
    });

    // table_open + the descriptor copy use CurrentMemoryContext / the cache
    // context respectively; the relation copy lives in a scratch context.
    let scratch = MemoryContext::new("CatalogCacheInitializeCache");
    let relation =
        common_relation::relation_open(scratch.mcx(), cc_reloid, AccessShareLock)?;

    // copy the relcache's tuple descriptor to permanent cache storage:
    //     tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));
    // and stash it in the CacheMemoryContext-analog store keyed by id.
    let cache_id = with_arena(|arena| arena.caches[cache_idx.0].id);
    with_tupdesc_store(|store| -> PgResult<()> {
        let mcx = store.mcx;
        // CreateTupleDescCopyConstr(RelationGetDescr(relation)) into the store.
        let copied = tupdesc::CreateTupleDescCopyConstr(mcx, &relation.rd_att)?;
        let boxed = mcx::alloc_in(mcx, copied)?;
        // Replace any stale entry for this id (a cache is initialized once,
        // but a reset can clear and reload it).
        store.descs.retain(|(id, _)| *id != cache_id);
        store
            .descs
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<(i32, PgBox<'_, TupleDescData<'_>>)>()))?;
        store.descs.push((cache_id, boxed));
        Ok(())
    })?;

    // save the relation's name and relisshared flag, too.
    with_arena(|arena| {
        let cache = &mut arena.caches[cache_idx.0];
        // cc_relname = pstrdup(RelationGetRelationName(relation));
        cache.cc_relname.clear();
        cache.cc_relname.push_str(relation.name());
        // cc_relisshared = RelationGetForm(relation)->relisshared;
        cache.cc_relisshared = relation.rd_rel.relisshared;
    });

    // We need the descriptor again for the per-key type reads; read attribute
    // types out of the just-copied store descriptor.
    // initialize cache's key information
    for i in 0..cc_nkeys as usize {
        let keytype: Oid;

        if cc_keyno[i] > 0 {
            // Form_pg_attribute attr = TupleDescAttr(tupdesc, cc_keyno[i] - 1);
            let attr_idx = (cc_keyno[i] - 1) as usize;
            let (atttypid, attnotnull) = with_tupdesc_store(|store| {
                let (_, td) = store
                    .descs
                    .iter()
                    .find(|(id, _)| *id == cache_id)
                    .expect("tupdesc just stored");
                let a = td.attr(attr_idx);
                (a.atttypid, a.attnotnull)
            });
            keytype = atttypid;
            // cache key columns should always be NOT NULL
            debug_assert!(attnotnull);
        } else {
            if cc_keyno[i] < 0 {
                // elog(FATAL, "sys attributes are not supported in caches");
                return Err(PgError::new(
                    types_error::error::FATAL,
                    "sys attributes are not supported in caches",
                ));
            }
            keytype = OIDOID;
        }

        // GetCCHashEqFuncs(keytype, &cc_hashfunc[i], &eqfunc, &cc_fastequal[i]);
        let (fastkind, eqfunc) = core_compute::GetCCHashEqFuncs(keytype)?;

        // Build the scankey template slot. The scan owner re-resolves the
        // comparison procedure with fmgr_info; we stamp only the eqfunc OID
        // (fmgr_info_cxt(eqfunc, &cc_skey[i].sk_func, CacheMemoryContext)).
        let mut skey = ScanKeyData::empty();
        skey.sk_func = FmgrInfo { fn_oid: eqfunc, ..Default::default() };
        // sk_attno suitably for HeapKeyTest() and heap scans.
        skey.sk_attno = cc_keyno[i] as AttrNumber;
        // sk_strategy --- always standard equality.
        skey.sk_strategy = BTEqualStrategyNumber;
        skey.sk_subtype = InvalidOid;
        // A catcache key requiring a collation must be C collation.
        skey.sk_collation = C_COLLATION_OID;
        with_arena(|arena| {
            let cache = &mut arena.caches[cache_idx.0];
            cache.cc_fastkind[i] = Some(fastkind);
            cache.cc_skey[i] = Some(skey);
        });
    }

    // mark this cache fully initialized (cc_tupdesc = tupdesc)
    with_arena(|arena| arena.caches[cache_idx.0].initialized = true);

    // return to the caller's memory context and close the rel
    //     table_close(relation, AccessShareLock);
    relation.close(AccessShareLock)?;

    Ok(())
}

/// `ConditionalCatalogCacheInitializeCache(cache)` — call
/// `CatalogCacheInitializeCache` if not yet done.
///
/// Must NOT be called while the catcache `with_arena` borrow is held (see
/// [`catalog_cache_initialize_cache`]); it does its own short scoped borrows.
pub(crate) fn conditional_initialize(cache_idx: CacheIdx) -> PgResult<()> {
    // #ifdef USE_ASSERT_CHECKING block (diagnostic-only in C).
    #[cfg(debug_assertions)]
    {
        let (cache_id, initialized) =
            with_arena(|arena| (arena.caches[cache_idx.0].id, arena.caches[cache_idx.0].initialized));
        if !(cache_id == TYPEOID || cache_id == ATTNUM)
            || transam_xact_seams::is_transaction_state::call()
        {
            relcache_seams::assert_could_get_relation::call();
        } else {
            debug_assert!(initialized);
        }
    }

    // if (unlikely(cache->cc_tupdesc == NULL)) CatalogCacheInitializeCache(cache);
    if !with_arena(|arena| arena.caches[cache_idx.0].initialized) {
        catalog_cache_initialize_cache(cache_idx)?;
    }
    Ok(())
}

/// `InitCatCachePhase2(cache, touch_index)` — finish init; the index touch is a
/// relcache-warming side effect (relcache owns `index_open`).
pub fn InitCatCachePhase2(cache_id: i32, touch_index: bool) -> PgResult<()> {
    // ConditionalCatalogCacheInitializeCache(cache);
    let cache_idx = with_arena(|arena| {
        find_cache_by_id(arena, cache_id)
            .unwrap_or_else(|| panic!("InitCatCachePhase2: cache id {cache_id} not registered"))
    });
    // conditional_initialize must run without the arena borrow held (relcache
    // re-entrancy); it takes its own short scoped borrows.
    conditional_initialize(cache_idx)?;
    let (cc_reloid, cc_indexoid) = with_arena(|arena| {
        let cache = &arena.caches[cache_idx.0];
        (cache.cc_reloid, cache.cc_indexoid)
    });

    if touch_index && cache_id != AMOID && cache_id != AMNAME {
        // We must lock the underlying catalog before opening the index to
        // avoid deadlock, since index_open could possibly result in reading
        // this same catalog.
        //     LockRelationOid(cache->cc_reloid, AccessShareLock);
        lmgr_seams::lock_relation_oid::call(cc_reloid, AccessShareLock)?
            .keep();

        // idesc = index_open(cache->cc_indexoid, AccessShareLock);
        let scratch = MemoryContext::new("InitCatCachePhase2");
        let idesc = indexam_seams::index_open::call(
            scratch.mcx(),
            cc_indexoid,
            AccessShareLock,
        )?;

        // While we've got the index open, check that it's unique (and not just
        // deferrable-unique): catch thinkos in new catcache definitions.
        //     Assert(idesc->rd_index->indisunique && idesc->rd_index->indimmediate);
        #[cfg(debug_assertions)]
        {
            let idx = idesc.rd_index.as_ref().expect("index_open returned a non-index");
            debug_assert!(idx.indisunique && idx.indimmediate);
        }

        // index_close(idesc, AccessShareLock);
        idesc.close(AccessShareLock)?;
        // UnlockRelationOid(cache->cc_reloid, AccessShareLock);
        lmgr_seams::unlock_relation_oid::call(cc_reloid, AccessShareLock)?;
    }

    Ok(())
}

/// `IndexScanOK(cache)` — bootstrap-time index-safe predicate (depends on
/// `criticalRelcachesBuilt` / `criticalSharedRelcachesBuilt`, read via the
/// relcache owner's seams).
pub fn IndexScanOK(cache_id: i32) -> PgResult<bool> {
    match cache_id {
        // INDEXRELID: force all pg_index searches to heap scans until the
        // critical relcaches are built (else infinite recursion).
        INDEXRELID => {
            if !relcache_seams::critical_relcaches_built::call() {
                return Ok(false);
            }
        }

        // AMOID / AMNAME: always heap-scan pg_am (it's tiny; and we *must* do
        // this when initially building critical relcache entries).
        AMOID | AMNAME => return Ok(false),

        // Authentication lookups occurring before the shared relcache has
        // collected entries for shared indexes.
        AUTHNAME | AUTHOID | AUTHMEMMEMROLE | DATABASEOID => {
            if !relcache_seams::critical_shared_relcaches_built::call() {
                return Ok(false);
            }
        }

        _ => {}
    }

    // Normal case, allow index scan.
    Ok(true)
}

/* ----------------------------------------------------------------------------
 * Per-cache metadata reads exposed as outward seams.
 * ------------------------------------------------------------------------- */

/// Read `cache->cc_nkeys` (the `Assert(SysCache[id]->cc_nkeys == N)` checks of
/// `SearchSysCacheN`).
pub fn cache_nkeys(cache_id: i32) -> i32 {
    with_arena(|arena| {
        let cache_idx = find_cache_by_id(arena, cache_id)
            .unwrap_or_else(|| panic!("cache_nkeys: cache id {cache_id} not registered"));
        arena.caches[cache_idx.0].cc_nkeys
    })
}

/// Read `cache->cc_relisshared`.
pub fn cache_relisshared(cache_id: i32) -> bool {
    with_arena(|arena| {
        let cache_idx = find_cache_by_id(arena, cache_id)
            .unwrap_or_else(|| panic!("cache_relisshared: cache id {cache_id} not registered"));
        arena.caches[cache_idx.0].cc_relisshared
    })
}

/// `PointerIsValid(cache->cc_tupdesc)` — whether phase-2 init has run.
pub fn cache_tupdesc_is_valid(cache_id: i32) -> bool {
    with_arena(|arena| {
        let cache_idx = find_cache_by_id(arena, cache_id)
            .unwrap_or_else(|| panic!("cache_tupdesc_is_valid: cache id {cache_id} not registered"));
        arena.caches[cache_idx.0].initialized
    })
}

/// Read access to `cache->cc_tupdesc`: runs `f` with a borrow of the
/// descriptor. Panics if not loaded (callers check `cache_tupdesc_is_valid` /
/// run phase 2 first, as `SysCacheGetAttr` does).
///
/// IMPORTANT: `f` is *not* run while the `TUPDESC_STORE` `RefCell` borrow is
/// held. `f` is typically `heap_deform_tuple` over a catalog tuple; deforming a
/// TOASTed catalog tuple re-enters the catcache (TOAST fetch → index scan →
/// relcache build → `SearchSysCache`), which calls `with_cache_tupdesc` again.
/// Holding the store borrow across that re-entrant call trips
/// "RefCell already borrowed" (the C `cc_tupdesc` is a bare pointer with no such
/// guard). So we resolve the descriptor pointer under a short borrow, release
/// the borrow, then call `f`. The descriptor lives in the CacheMemoryContext
/// analog (`TUPDESC_STORE`'s `McxOwned`), which persists for the whole backend
/// life and is never freed or relocated once a cache is initialized, so the
/// pointer stays valid across `f`.
pub fn with_cache_tupdesc(cache_id: i32, f: &mut dyn FnMut(&TupleDescData<'_>)) {
    // Resolve a stable pointer to the stored TupleDescData under a short borrow.
    // The store's `'mcx` is an HRTB that can't escape the closure, so we erase it
    // to `'static` for the carrier pointer and reattach a fresh lifetime below.
    let td_ptr: *const TupleDescData<'static> = with_tupdesc_store(|store| {
        let (_, td) = store
            .descs
            .iter()
            .find(|(id, _)| *id == cache_id)
            .unwrap_or_else(|| {
                panic!("with_cache_tupdesc: cc_tupdesc for cache id {cache_id} not loaded")
            });
        // The PgBox's target is pinned in the store's MemoryContext for the
        // backend's life; a raw pointer outlives this borrow safely.
        (&**td as *const TupleDescData<'_>) as *const TupleDescData<'static>
    });
    // SAFETY: `td_ptr` points into the CacheMemoryContext-analog store, which is
    // never freed or moved for an initialized cache (see the doc comment). The
    // borrow that produced it has been released, so `f` may freely re-enter the
    // catcache (and `with_cache_tupdesc`) without a double-borrow panic. The
    // `'static` is a carrier only; `f` accepts any lifetime (`&TupleDescData<'_>`).
    let td: &TupleDescData<'static> = unsafe { &*td_ptr };
    f(td);
}
