#![allow(non_snake_case)]

use std::cell::Cell;

use cache_syscache::cacheinfo::AUTHOID;
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use types_core::{catalog::BOOTSTRAP_SUPERUSERID, InvalidOid, Oid};
use types_error::PgResult;

const ANUM_PG_AUTHID_ROLSUPER: i32 = 3;

thread_local! {
    static LAST_ROLEID: Cell<Oid> = const { Cell::new(InvalidOid) };
    static LAST_ROLEID_IS_SUPER: Cell<bool> = const { Cell::new(false) };
    static ROLEID_CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

pub fn superuser() -> PgResult<bool> {
    superuser_arg(miscinit::GetUserId())
}

pub fn superuser_arg(roleid: Oid) -> PgResult<bool> {
    let last = LAST_ROLEID.get();
    if last != InvalidOid && last == roleid {
        return Ok(LAST_ROLEID_IS_SUPER.get());
    }

    // Escape path in case you deleted all your users.
    if !init_small::globals::IsUnderPostmaster() && roleid == BOOTSTRAP_SUPERUSERID {
        return Ok(true);
    }

    let result = match SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? {
        Some(tuple) => {
            let rolsuper =
                SysCacheGetAttrNotNull(AUTHOID, &tuple, ANUM_PG_AUTHID_ROLSUPER)?.as_bool();
            ReleaseSysCache(tuple);
            rolsuper
        }
        None => false,
    };

    if !ROLEID_CALLBACK_REGISTERED.get() {
        inval::invalidate::CacheRegisterSyscacheCallback(AUTHOID, RoleidCallback, Datum::null())?;
        ROLEID_CALLBACK_REGISTERED.set(true);
    }

    LAST_ROLEID.set(roleid);
    LAST_ROLEID_IS_SUPER.set(result);

    Ok(result)
}

fn RoleidCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    LAST_ROLEID.set(InvalidOid);
}

pub fn init_seams() {
    superuser_seams::superuser::set(superuser);
    superuser_seams::superuser_arg::set(superuser_arg);
}

#[cfg(test)]
mod tests;
