//! Port of `src/backend/utils/misc/superuser.c`.
//!
//! `superuser()`/`superuser_arg()` answer whether a role has superuser
//! privilege, keeping a one-entry cache of the last queried role that is
//! flushed by a `pg_authid` syscache-invalidation callback. The cache and the
//! callback-registered flag are per-backend `static` globals in C, so they are
//! `thread_local!` here.

use std::cell::Cell;

use ::inval_seams::cache_register_syscache_callback as inval_seam;
use ::syscache_seams::search_authid_rolsuper;
use ::miscinit_seams::get_user_id;
use ::init_small::globals::IsUnderPostmaster;
use types_core::{Oid, BOOTSTRAP_SUPERUSERID, INVALID_OID};
use ::datum::Datum;
use ::types_error::PgResult;
use ::types_syscache::AUTHOID;

thread_local! {
    /// `static Oid last_roleid` — `InvalidOid` means the cache is not valid.
    static LAST_ROLEID: Cell<Oid> = const { Cell::new(INVALID_OID) };
    /// `static bool last_roleid_is_super`.
    static LAST_ROLEID_IS_SUPER: Cell<bool> = const { Cell::new(false) };
    /// `static bool roleid_callback_registered`.
    static ROLEID_CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

/// `superuser(void)` — does the current user have superuser privilege?
pub fn superuser() -> PgResult<bool> {
    superuser_arg(get_user_id::call())
}

/// `superuser_arg(roleid)` — does `roleid` have superuser privilege?
pub fn superuser_arg(roleid: Oid) -> PgResult<bool> {
    // Quick out for cache hit.
    let last = LAST_ROLEID.with(Cell::get);
    if OidIsValid(last) && last == roleid {
        return Ok(LAST_ROLEID_IS_SUPER.with(Cell::get));
    }

    // Special escape path in case you deleted all your users.
    if !IsUnderPostmaster() && roleid == BOOTSTRAP_SUPERUSERID {
        return Ok(true);
    }

    // OK, look up the information in pg_authid. Report "not superuser" for
    // invalid roleids (`!HeapTupleIsValid`).
    let result = search_authid_rolsuper::call(roleid)?.unwrap_or(false);

    // If first time through, set up callback for cache flushes.
    if !ROLEID_CALLBACK_REGISTERED.with(Cell::get) {
        inval_seam::call(AUTHOID, RoleidCallback, Datum::null())?;
        ROLEID_CALLBACK_REGISTERED.with(|c| c.set(true));
    }

    // Cache the result for next time.
    LAST_ROLEID.with(|c| c.set(roleid));
    LAST_ROLEID_IS_SUPER.with(|c| c.set(result));

    Ok(result)
}

/// `RoleidCallback` — syscache invalidation callback that invalidates our local
/// cache in case the role's superuserness changed.
pub fn RoleidCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    LAST_ROLEID.with(|c| c.set(INVALID_OID));
}

/// `OidIsValid(objectId)` — `(objectId) != InvalidOid`.
#[inline]
fn OidIsValid(object_id: Oid) -> bool {
    object_id != INVALID_OID
}
