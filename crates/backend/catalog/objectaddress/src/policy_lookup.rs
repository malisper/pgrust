//! `pg_policy` by-oid projection used by `getObjectDescription` for
//! `OCLASS_POLICY` (objectaddress.c 3990). There is no `POLICYOID` system
//! cache, so the C by-oid fetch
//! `table_open(PolicyRelationId) + systable_beginscan(PolicyOidIndexId,
//! oid = polid) + GETSTRUCT(Form_pg_policy)` is reproduced here with the shared
//! [`crate::resolve::get_catalog_object_by_oid`] oid-index scan followed by
//! `heap_getattr` on the fixed `polrelid` / `polname` columns. Mirrors
//! [`crate::trigger_lookup`].

use ::mcx::{Mcx, PgString};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_storage::lock::AccessShareLock;

use crate::consts::{
    Anum_pg_policy_oid, Anum_pg_policy_polname, Anum_pg_policy_polrelid, PolicyRelationId,
};
use crate::resolve::get_catalog_object_by_oid;

/// `(polrelid, NameStr(polname))` projection — `getObjectDescription`
/// (objectaddress.c 3990). The name is copied into `mcx`. `Ok(None)` on a
/// scan miss (the C `!HeapTupleIsValid(tup)`).
pub fn policy_relid_name<'mcx>(
    mcx: Mcx<'mcx>,
    polid: Oid,
) -> PgResult<Option<(Oid, PgString<'mcx>)>> {
    // table_open(PolicyRelationId, AccessShareLock);
    let catalog = common_relation_seams::relation_open::call(
        mcx,
        PolicyRelationId,
        AccessShareLock,
    )?;

    // tup = get_catalog_object_by_oid(catalog, Anum_pg_policy_oid, polid);
    let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_policy_oid, polid)?;

    let result = match objtup {
        None => None,
        Some(tup) => {
            // GETSTRUCT(tup): polrelid (oid) and polname (name) are fixed-width
            // NOT-NULL columns, so heap_getattr always yields a value.
            let polrelid = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_policy_polrelid as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!("null polrelid for policy {polid}")));
                }
            };
            let polname = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_policy_polname as i32,
                &catalog.rd_att,
            )? {
                Some(d) => crate::fmgr_sql::datum_get_name(&d),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!("null polname for policy {polid}")));
                }
            };
            Some((polrelid, PgString::from_str_in(&polname, mcx)?))
        }
    };

    // table_close(catalog, AccessShareLock);
    catalog.close(AccessShareLock)?;
    Ok(result)
}
