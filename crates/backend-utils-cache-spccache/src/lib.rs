//! `utils/cache/spccache.c` — tablespace cache management.
//!
//! We cache the parsed version of spcoptions for each tablespace to avoid
//! needing to reparse on every lookup. Right now, there doesn't appear to be
//! a measurable performance gain from doing this, but that might change in
//! the future as we add more options.
//!
//! The C `static HTAB *TableSpaceCacheHash` (in `CacheMemoryContext`) is a
//! `thread_local!` map with its own owning context, per the per-backend
//! model. C's `get_tablespace` returns a pointer into the cache that must not
//! be stored; the only field callers read is `opts`, so this port's
//! `get_tablespace` returns that by value.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use backend_access_common_reloptions_seams as reloptions_seams;
use backend_optimizer_path_costsize_seams as costsize_seams;
use backend_storage_buffer_bufmgr_seams as bufmgr_seams;
use backend_utils_cache_inval_seams as inval_seams;
use backend_utils_cache_syscache as syscache;
use backend_utils_init_small_seams as globals_seams;
use mcx::{McxOwned, Mcx, MemoryContext, PgHashMap};
use types_cache::SysCacheKey;
use types_core::{InvalidOid, Oid};
use types_datum::Datum;
use types_error::{PgError, PgResult};
use types_reloptions::TableSpaceOpts;
use types_tuple::backend_access_common_heaptuple::TupleValue;

/// `Anum_pg_tablespace_spcoptions` (`catalog/pg_tablespace.h`).
const Anum_pg_tablespace_spcoptions: i32 = 5;

struct TableSpaceCache<'mcx> {
    mcx: Mcx<'mcx>,
    /// `TableSpaceCacheEntry` keyed by tablespace oid; the value is the
    /// entry's `opts` (`None` mirroring the C NULL).
    hash: PgHashMap<'mcx, Oid, Option<TableSpaceOpts>>,
}

mcx::bind!(TableSpaceCacheTy => TableSpaceCache<'mcx>);

thread_local! {
    /// `static HTAB *TableSpaceCacheHash = NULL;`
    static TABLESPACE_CACHE: RefCell<Option<McxOwned<TableSpaceCacheTy>>> =
        const { RefCell::new(None) };
}

/// `InvalidateTableSpaceCacheCallback` — flush all cache entries when
/// pg_tablespace is updated.
///
/// When pg_tablespace is updated, we must flush the cache entry at least for
/// that tablespace. Currently, we just flush them all. This is quick and
/// easy and doesn't cost much, since there shouldn't be terribly many
/// tablespaces, nor do we expect them to be frequently modified.
fn InvalidateTableSpaceCacheCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    TABLESPACE_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if let Some(owned) = slot.as_mut() {
            // C: hash_seq over every entry, pfree(spc->opts) + HASH_REMOVE;
            // the payload here is by-value, so clearing the map is the whole
            // job.
            owned.with_mut(|cache| cache.hash.clear());
        }
    });
}

/// `InitializeTableSpaceCache` — initialize the tablespace cache and watch
/// for invalidation events.
fn InitializeTableSpaceCache() -> PgResult<()> {
    let owned =
        McxOwned::<TableSpaceCacheTy>::try_new(MemoryContext::new("TableSpace cache"), |mcx| {
            Ok(TableSpaceCache { mcx, hash: PgHashMap::new_in(mcx) })
        })?;
    TABLESPACE_CACHE.with(|cell| {
        *cell.borrow_mut() = Some(owned);
    });

    // Watch for invalidation events.
    inval_seams::cache_register_syscache_callback::call(
        syscache::TABLESPACEOID,
        InvalidateTableSpaceCacheCallback,
        Datum::null(),
    )
}

/// `get_tablespace` — fetch the cached entry for a specified tablespace OID,
/// reading pg_tablespace on a cache miss.
fn get_tablespace(mut spcid: Oid) -> PgResult<Option<TableSpaceOpts>> {
    // Since spcid is always from a pg_class tuple, InvalidOid implies the
    // default.
    if spcid == InvalidOid {
        spcid = globals_seams::my_database_tablespace::call();
    }

    // Find existing cache entry, if any.
    let initialized = TABLESPACE_CACHE.with(|cell| cell.borrow().is_some());
    if !initialized {
        InitializeTableSpaceCache()?;
    }
    let cached = TABLESPACE_CACHE
        .with(|cell| cell.borrow().as_ref().and_then(|owned| owned.with(|s| s.hash.get(&spcid).copied())));
    if let Some(spc) = cached {
        return Ok(spc);
    }

    // Not found in TableSpace cache. Check catcache. If we don't find a
    // valid HeapTuple, it must mean someone has managed to request tablespace
    // details for a non-existent tablespace. We'll just treat that case as if
    // no options were specified.
    let opts = {
        let scratch = MemoryContext::new("tablespace cache lookup");
        let mcx = scratch.mcx();
        let tp = syscache::SearchSysCache1(
            mcx,
            syscache::TABLESPACEOID,
            SysCacheKey::Value(Datum::from_oid(spcid)),
        )?;
        match tp {
            None => None,
            Some(tup) => {
                let (datum, is_null) = syscache::SysCacheGetAttr(
                    mcx,
                    syscache::TABLESPACEOID,
                    &tup,
                    Anum_pg_tablespace_spcoptions,
                )?;
                let opts = if is_null {
                    None
                } else {
                    let bytes = match &datum {
                        TupleValue::ByRef(b) => &b[..],
                        TupleValue::ByVal(_) => {
                            return Err(PgError::error("spcoptions datum is not by-reference"))
                        }
                    };
                    Some(reloptions_seams::tablespace_reloptions::call(bytes, false)?)
                };
                syscache::ReleaseSysCache(tup);
                opts
            }
        }
    };

    // Now create the cache entry. It's important to do this only after
    // reading the pg_tablespace entry, since doing so could cause a cache
    // flush.
    TABLESPACE_CACHE.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let owned = slot
            .as_mut()
            .ok_or_else(|| PgError::error("get_tablespace: TableSpaceCache not initialized"))?;
        owned.with_mut(|cache| -> PgResult<()> {
            cache
                .hash
                .try_reserve(1)
                .map_err(|_| cache.mcx.oom(core::mem::size_of::<Option<TableSpaceOpts>>()))?;
            cache.hash.insert(spcid, opts);
            Ok(())
        })
    })?;
    Ok(opts)
}

/// `get_tablespace_page_costs` — return random and/or sequential page costs
/// for a given tablespace.
///
/// This value is not locked by the transaction, so it may be changed while a
/// SELECT that has used these values for planning is still executing. The C
/// nullable out-pointers are optional mutable borrows.
#[allow(clippy::neg_cmp_op_on_partial_ord)] // the C predicate, complemented exactly
pub fn get_tablespace_page_costs(
    spcid: Oid,
    spc_random_page_cost: Option<&mut f64>,
    spc_seq_page_cost: Option<&mut f64>,
) -> PgResult<()> {
    let spc = get_tablespace(spcid)?;
    // Assert(spc != NULL) — get_tablespace always produces an entry; `spc`
    // here is the entry's opts (None == C spc->opts == NULL).

    if let Some(out) = spc_random_page_cost {
        // C: `!spc->opts || spc->opts->random_page_cost < 0` selects the GUC
        // (written as the exact complement so NaN behaves identically).
        *out = match &spc {
            Some(opts) if !(opts.random_page_cost < 0.0) => opts.random_page_cost,
            _ => costsize_seams::random_page_cost::call(),
        };
    }

    if let Some(out) = spc_seq_page_cost {
        *out = match &spc {
            Some(opts) if !(opts.seq_page_cost < 0.0) => opts.seq_page_cost,
            _ => costsize_seams::seq_page_cost::call(),
        };
    }

    Ok(())
}

/// `get_tablespace_io_concurrency`.
///
/// This value is not locked by the transaction, so it may be changed while a
/// SELECT that has used these values for planning is still executing.
pub fn get_tablespace_io_concurrency(spcid: Oid) -> PgResult<i32> {
    let spc = get_tablespace(spcid)?;
    Ok(match spc {
        Some(opts) if opts.effective_io_concurrency >= 0 => opts.effective_io_concurrency,
        _ => bufmgr_seams::effective_io_concurrency::call(),
    })
}

/// `get_tablespace_maintenance_io_concurrency`.
pub fn get_tablespace_maintenance_io_concurrency(spcid: Oid) -> PgResult<i32> {
    let spc = get_tablespace(spcid)?;
    Ok(match spc {
        Some(opts) if opts.maintenance_io_concurrency >= 0 => opts.maintenance_io_concurrency,
        _ => bufmgr_seams::maintenance_io_concurrency::call(),
    })
}

/// This crate declares no inward seams (callers depend on it directly).
pub fn init_seams() {}
