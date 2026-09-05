// attoptcache.c: parsed AttributeOpts cached per (attrelid, attnum) in a
// session-lifetime hash (C: AttoptCacheHash in CacheMemoryContext), flushed
// wholesale by InvalidateAttoptCacheCallback on every ATTNUM syscache
// invalidation (attoptcache.c:53-124). get_attribute_options
// (attoptcache.c:130-177) consults the hash first and only parses the
// pg_attribute.attoptions image on a miss; the caller gets its own copy
// (C palloc's one; AttributeOpts is Copy here).
use core::cell::{Cell, RefCell};
use std::collections::HashMap;

use datum::Datum;
use mcx::Mcx;
use reloptions::AttributeOpts;
use types_core::Oid;
use types_error::PgResult;

// syscache id of ATTNUM (syscache_ids.h / cacheinfo.rs).
const ATTNUM: i32 = 7;

thread_local! {
    // C's per-backend AttoptCacheHash: None = "no options" for a known key
    // (C caches an entry with opts == NULL for those too).
    static ATTOPT_CACHE: RefCell<HashMap<(Oid, i16), Option<AttributeOpts>>> =
        RefCell::new(HashMap::new());
    static CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

// InvalidateAttoptCacheCallback (attoptcache.c:53-77): C flushes every entry
// regardless of the hash value (the callback is keyed on the cache, not the
// tuple), so a flush is a clear.
pub fn InvalidateAttoptCacheCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    ATTOPT_CACHE.with(|c| c.borrow_mut().clear());
}

// InitializeAttoptCache (attoptcache.c:97-124): register the syscache
// callback once, on first use.
fn initialize_attopt_cache() -> PgResult<()> {
    if CALLBACK_REGISTERED.with(|c| c.get()) {
        return Ok(());
    }
    inval::invalidate::CacheRegisterSyscacheCallback(
        ATTNUM,
        InvalidateAttoptCacheCallback,
        Datum::null(),
    )?;
    CALLBACK_REGISTERED.with(|c| c.set(true));
    Ok(())
}

// get_attribute_options (attoptcache.c:130). A missing attribute reads as
// "no options specified", as does a null attoptions column.
pub fn get_attribute_options(
    mcx: Mcx<'_>,
    attrelid: Oid,
    attnum: i16,
) -> PgResult<Option<AttributeOpts>> {
    initialize_attopt_cache()?;
    if let Some(hit) = ATTOPT_CACHE.with(|c| c.borrow().get(&(attrelid, attnum)).copied()) {
        return Ok(hit);
    }
    let opts = parse_attribute_options(mcx, attrelid, attnum)?;
    ATTOPT_CACHE.with(|c| c.borrow_mut().insert((attrelid, attnum), opts));
    Ok(opts)
}

fn parse_attribute_options(
    mcx: Mcx<'_>,
    attrelid: Oid,
    attnum: i16,
) -> PgResult<Option<AttributeOpts>> {
    let Some(attopts) = syscache_seams::pg_attribute_attoptions::call(mcx, attrelid, attnum)?
    else {
        return Ok(None);
    };
    let Some(datum) = attopts else {
        return Ok(None);
    };
    let p = datum.as_usize() as *const u8;
    // SAFETY: not-null attoptions datum off the syscache projection — a live
    // varlena image readable through its varsize_any extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    reloptions::attribute_reloptions(mcx, Some(image), false)
}

#[cfg(test)]
mod tests;
