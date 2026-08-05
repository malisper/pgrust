use std::cell::RefCell;
use std::mem::ManuallyDrop;

use mcx::{Mcx, MemoryContext, PgHashMap};
use types_core::{catalog::FirstGenbkiObjectId, InvalidOid, Oid};
use types_error::PgResult;

pub fn is_builtin(object_id: Oid) -> bool {
    object_id < FirstGenbkiObjectId
}

type ShippableCacheKey = (Oid, Oid, Oid);

thread_local! {
    static SHIPPABLE_CACHE: RefCell<Option<ManuallyDrop<PgHashMap<'static, ShippableCacheKey, bool>>>> =
        const { RefCell::new(None) };
}

fn invalidate_shippable_cache_callback(_arg: datum::Datum, _cacheid: i32, _hashvalue: u32) {
    SHIPPABLE_CACHE.with(|cell| {
        if let Some(map) = cell.borrow_mut().as_mut() {
            map.clear();
        }
    });
}

fn lookup_shippable(
    mcx: Mcx<'_>,
    object_id: Oid,
    class_id: Oid,
    shippable_extensions: &[Oid],
) -> PgResult<bool> {
    let extension_oid = pg_depend::getExtensionOfObject(mcx, class_id, object_id)?;
    Ok(extension_oid != InvalidOid && shippable_extensions.contains(&extension_oid))
}

pub fn is_shippable(
    mcx: Mcx<'_>,
    object_id: Oid,
    class_id: Oid,
    serverid: Oid,
    shippable_extensions: &[Oid],
) -> PgResult<bool> {
    if is_builtin(object_id) {
        return Ok(true);
    }
    if shippable_extensions.is_empty() {
        return Ok(false);
    }

    let key: ShippableCacheKey = (object_id, class_id, serverid);
    let cached = SHIPPABLE_CACHE.with(|cell| -> PgResult<Option<bool>> {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            // Backend-lifetime table (C: hash_create at first use); flushed
            // wholesale when pg_foreign_server changes (extensions option).
            let cache_mcx = Box::leak(Box::new(MemoryContext::new("Shippability cache"))).mcx();
            inval::invalidate::CacheRegisterSyscacheCallback(
                cache_syscache::cacheinfo::FOREIGNSERVEROID,
                invalidate_shippable_cache_callback,
                datum::Datum::null(),
            )?;
            *slot = Some(ManuallyDrop::new(PgHashMap::with_capacity_in(256, cache_mcx)));
        }
        Ok(slot.as_ref().unwrap().get(&key).copied())
    })?;

    if let Some(shippable) = cached {
        return Ok(shippable);
    }

    // C enters the hash entry only after the lookup: the catalog probes may
    // fire the invalidation callback mid-flight.
    let shippable = lookup_shippable(mcx, object_id, class_id, shippable_extensions)?;
    SHIPPABLE_CACHE.with(|cell| {
        if let Some(map) = cell.borrow_mut().as_mut() {
            map.insert(key, shippable);
        }
    });
    Ok(shippable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_boundary() {
        assert!(is_builtin(0));
        assert!(is_builtin(9999));
        assert!(!is_builtin(10000));
        assert!(!is_builtin(16384));
    }
}
