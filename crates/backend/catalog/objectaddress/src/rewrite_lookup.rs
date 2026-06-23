//! `pg_rewrite` by-oid projections used by `getObjectDescription` /
//! `getObjectIdentityParts` for `OCLASS_REWRITE` (objectaddress.c 1592, 3782).
//!
//! There is no `RULEOID` system cache (rules are cached only by
//! `(ev_class, rulename)` via `RULERELNAME`), so the by-oid fetch the C does as
//! `table_open(RewriteRelationId) + systable_beginscan(RewriteOidIndexId,
//! oid = ruleid) + GETSTRUCT(Form_pg_rewrite)` is reproduced here with the
//! shared [`crate::resolve::get_catalog_object_by_oid`] scan (the genam
//! oid-index lookup) followed by `heap_getattr` on the fixed `ev_class` /
//! `rulename` columns.

use ::mcx::{Mcx, PgString};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_storage::lock::AccessShareLock;

use crate::consts::{Anum_pg_rewrite_oid, Anum_pg_rewrite_rulename, RewriteRelationId};
use crate::resolve::get_catalog_object_by_oid;

/// `Anum_pg_rewrite_ev_class` (pg_rewrite.h): the rule's event relation.
const Anum_pg_rewrite_ev_class: i16 = 3;

/// Fetch the `pg_rewrite` tuple for `ruleid` and project `(ev_class, rulename)`.
/// `Ok(None)` on a scan miss (the C `!HeapTupleIsValid(tup)`). Shared core for
/// the two seam orderings.
fn rewrite_lookup<'mcx>(mcx: Mcx<'mcx>, ruleid: Oid) -> PgResult<Option<(Oid, String)>> {
    // table_open(RewriteRelationId, AccessShareLock);
    let catalog = common_relation_seams::relation_open::call(
        mcx,
        RewriteRelationId,
        AccessShareLock,
    )?;

    // tup = get_catalog_object_by_oid(catalog, Anum_pg_rewrite_oid, ruleid);
    let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_rewrite_oid, ruleid)?;

    let result = match objtup {
        None => None,
        Some(tup) => {
            // GETSTRUCT(tup): ev_class (oid) and rulename (name) are fixed-width
            // NOT-NULL columns, so heap_getattr always yields a value.
            let ev_class = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_rewrite_ev_class as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!("null ev_class for rule {ruleid}")));
                }
            };
            let rulename = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_rewrite_rulename as i32,
                &catalog.rd_att,
            )? {
                Some(d) => crate::fmgr_sql::datum_get_name(&d),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!("null rulename for rule {ruleid}")));
                }
            };
            Some((ev_class, rulename))
        }
    };

    // table_close(catalog, AccessShareLock);
    catalog.close(AccessShareLock)?;
    Ok(result)
}

/// `(ev_class, NameStr(rulename))` projection — `getObjectDescription`
/// (objectaddress.c 1592). The name is copied into `mcx`.
pub fn rewrite_class_name<'mcx>(
    mcx: Mcx<'mcx>,
    ruleid: Oid,
) -> PgResult<Option<(Oid, PgString<'mcx>)>> {
    match rewrite_lookup(mcx, ruleid)? {
        None => Ok(None),
        Some((ev_class, rulename)) => Ok(Some((ev_class, PgString::from_str_in(&rulename, mcx)?))),
    }
}

/// `(NameStr(rulename), ev_class)` projection — `getObjectIdentityParts` /
/// `RemoveRewriteRuleById` (objectaddress.c 3782, rewriteRemove.c 47). The name
/// is copied into `mcx`.
pub fn rewrite_name_evclass<'mcx>(
    mcx: Mcx<'mcx>,
    ruleid: Oid,
) -> PgResult<Option<(PgString<'mcx>, Oid)>> {
    match rewrite_lookup(mcx, ruleid)? {
        None => Ok(None),
        Some((ev_class, rulename)) => Ok(Some((PgString::from_str_in(&rulename, mcx)?, ev_class))),
    }
}
