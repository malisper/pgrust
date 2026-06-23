//! `pg_trigger` by-oid projections used by `getObjectDescription` /
//! `getObjectIdentityParts` for `OCLASS_TRIGGER` (objectaddress.c 1610, 3818).
//!
//! There is no `TRIGGEROID` system cache, so the by-oid fetch the C does as
//! `table_open(TriggerRelationId) + systable_beginscan(TriggerOidIndexId,
//! oid = trigid) + GETSTRUCT(Form_pg_trigger)` is reproduced here with the
//! shared [`crate::resolve::get_catalog_object_by_oid`] scan (the genam
//! oid-index lookup) followed by `heap_getattr` on the fixed `tgrelid` /
//! `tgname` columns. Mirrors [`crate::rewrite_lookup`].

use ::mcx::{Mcx, PgString};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_storage::lock::AccessShareLock;

use crate::consts::{
    Anum_pg_trigger_oid, Anum_pg_trigger_tgname, Anum_pg_trigger_tgrelid, TriggerRelationId,
};
use crate::resolve::get_catalog_object_by_oid;

/// Fetch the `pg_trigger` tuple for `trigid` and project `(tgrelid, tgname)`.
/// `Ok(None)` on a scan miss (the C `!HeapTupleIsValid(tup)`). Shared core for
/// the two seam orderings.
fn trigger_lookup<'mcx>(mcx: Mcx<'mcx>, trigid: Oid) -> PgResult<Option<(Oid, String)>> {
    // table_open(TriggerRelationId, AccessShareLock);
    let catalog = common_relation_seams::relation_open::call(
        mcx,
        TriggerRelationId,
        AccessShareLock,
    )?;

    // tup = get_catalog_object_by_oid(catalog, Anum_pg_trigger_oid, trigid);
    let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_trigger_oid, trigid)?;

    let result = match objtup {
        None => None,
        Some(tup) => {
            // GETSTRUCT(tup): tgrelid (oid) and tgname (name) are fixed-width
            // NOT-NULL columns, so heap_getattr always yields a value.
            let tgrelid = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_trigger_tgrelid as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!("null tgrelid for trigger {trigid}")));
                }
            };
            let tgname = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_trigger_tgname as i32,
                &catalog.rd_att,
            )? {
                Some(d) => crate::fmgr_sql::datum_get_name(&d),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!("null tgname for trigger {trigid}")));
                }
            };
            Some((tgrelid, tgname))
        }
    };

    // table_close(catalog, AccessShareLock);
    catalog.close(AccessShareLock)?;
    Ok(result)
}

/// `(tgrelid, NameStr(tgname))` projection — `getObjectDescription`
/// (objectaddress.c 1610). The name is copied into `mcx`.
pub fn trigger_relid_name<'mcx>(
    mcx: Mcx<'mcx>,
    trigid: Oid,
) -> PgResult<Option<(Oid, PgString<'mcx>)>> {
    match trigger_lookup(mcx, trigid)? {
        None => Ok(None),
        Some((tgrelid, tgname)) => Ok(Some((tgrelid, PgString::from_str_in(&tgname, mcx)?))),
    }
}

/// `(NameStr(tgname), tgrelid)` projection — `getObjectIdentityParts`
/// (objectaddress.c 3818). The name is copied into `mcx`.
pub fn trigger_name_relid<'mcx>(
    mcx: Mcx<'mcx>,
    trigid: Oid,
) -> PgResult<Option<(PgString<'mcx>, Oid)>> {
    match trigger_lookup(mcx, trigid)? {
        None => Ok(None),
        Some((tgrelid, tgname)) => Ok(Some((PgString::from_str_in(&tgname, mcx)?, tgrelid))),
    }
}
