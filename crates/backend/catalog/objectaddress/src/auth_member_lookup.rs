//! `pg_auth_members` by-oid projection used by `getObjectDescription` /
//! `getObjectIdentityParts` for `OCLASS_DEFACL`/role-membership entries
//! (objectaddress.c).
//!
//! There is no by-oid system cache for `pg_auth_members`, so the C fetch
//! `table_open(AuthMemRelationId) +
//! systable_beginscan(AuthMemOidIndexId, oid = authmemid) +
//! GETSTRUCT(Form_pg_auth_members)` is reproduced here with the shared
//! [`crate::resolve::get_catalog_object_by_oid`] oid-index scan followed by
//! `heap_getattr` on the fixed-width `member` / `roleid` columns. Mirrors
//! [`crate::trigger_lookup`].
//!
//! Unlike the trigger projection the seam carries no `mcx` (its result is two
//! `Oid`s, which are `Copy`), so the scan runs inside a private transient
//! [`::mcx::MemoryContext`] whose allocations are reclaimed when it drops — the
//! C call's `CurrentMemoryContext` churn.

use ::mcx::MemoryContext;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_storage::lock::AccessShareLock;

use crate::consts::{
    Anum_pg_auth_members_member, Anum_pg_auth_members_oid, Anum_pg_auth_members_roleid,
    AuthMemRelationId,
};
use crate::resolve::get_catalog_object_by_oid;

/// `table_open(AuthMemRelationId) + systable_beginscan(AuthMemOidIndexId,
/// oid = authmemid) + GETSTRUCT(Form_pg_auth_members)` projected to
/// `(member, roleid)`. `Ok(None)` on a scan miss (the C `!HeapTupleIsValid`).
pub fn auth_member_member_role(authmemid: Oid) -> PgResult<Option<(Oid, Oid)>> {
    let ctx = MemoryContext::new("auth_member_lookup");
    let mcx = ctx.mcx();

    // table_open(AuthMemRelationId, AccessShareLock);
    let catalog = common_relation_seams::relation_open::call(
        mcx,
        AuthMemRelationId,
        AccessShareLock,
    )?;

    // tup = get_catalog_object_by_oid(catalog, Anum_pg_auth_members_oid, authmemid);
    let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_auth_members_oid, authmemid)?;

    let result = match objtup {
        None => None,
        Some(tup) => {
            // GETSTRUCT(tup): member (oid) and roleid (oid) are fixed-width
            // NOT-NULL columns, so heap_getattr always yields a value.
            let member = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_auth_members_member as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!(
                        "null member for pg_auth_members entry {authmemid}"
                    )));
                }
            };
            let roleid = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_auth_members_roleid as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!(
                        "null roleid for pg_auth_members entry {authmemid}"
                    )));
                }
            };
            Some((member, roleid))
        }
    };

    // table_close(catalog, AccessShareLock);
    catalog.close(AccessShareLock)?;
    Ok(result)
}
