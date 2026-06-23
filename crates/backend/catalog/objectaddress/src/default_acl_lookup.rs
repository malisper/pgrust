//! `pg_default_acl` by-oid projection used by `getObjectDescription` for
//! `OCLASS_DEFACL` (objectaddress.c 3761). There is no `DEFACLOID` system
//! cache, so the C by-oid fetch
//! `table_open(DefaultAclRelationId) +
//! systable_beginscan(DefaultAclOidIndexId, oid = defaclid) +
//! GETSTRUCT(Form_pg_default_acl)` is reproduced here with the shared
//! [`crate::resolve::get_catalog_object_by_oid`] oid-index scan followed by
//! `heap_getattr` on the fixed `defaclrole` / `defaclnamespace` /
//! `defaclobjtype` columns. Mirrors [`crate::auth_member_lookup`].

use mcx::MemoryContext;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_storage::lock::AccessShareLock;

use syscache_seams::DefaultAclDescRow;

use crate::consts::{
    Anum_pg_default_acl_defaclnamespace, Anum_pg_default_acl_defaclobjtype,
    Anum_pg_default_acl_defaclrole, Anum_pg_default_acl_oid, DefaultAclRelationId,
};
use crate::resolve::get_catalog_object_by_oid;

/// `(defaclrole, defaclnamespace, defaclobjtype)` projection —
/// `getObjectDescription` (objectaddress.c 3761). `Ok(None)` on a scan miss
/// (the C `!HeapTupleIsValid(tup)`).
pub fn default_acl_row(defaclid: Oid) -> PgResult<Option<DefaultAclDescRow>> {
    let ctx = MemoryContext::new("default_acl_lookup");
    let mcx = ctx.mcx();

    // table_open(DefaultAclRelationId, AccessShareLock);
    let catalog = common_relation_seams::relation_open::call(
        mcx,
        DefaultAclRelationId,
        AccessShareLock,
    )?;

    // tup = get_catalog_object_by_oid(catalog, Anum_pg_default_acl_oid, defaclid);
    let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_default_acl_oid, defaclid)?;

    let result = match objtup {
        None => None,
        Some(tup) => {
            // GETSTRUCT(tup): defaclrole/defaclnamespace (oid) and defaclobjtype
            // (char) are fixed-width NOT-NULL columns.
            let defaclrole = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_default_acl_defaclrole as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!(
                        "null defaclrole for default ACL {defaclid}"
                    )));
                }
            };
            let defaclnamespace = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_default_acl_defaclnamespace as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_oid(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!(
                        "null defaclnamespace for default ACL {defaclid}"
                    )));
                }
            };
            let defaclobjtype = match crate::fmgr_sql::heap_getattr(
                mcx,
                &tup,
                Anum_pg_default_acl_defaclobjtype as i32,
                &catalog.rd_att,
            )? {
                Some(d) => d.as_char(),
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(PgError::error(format!(
                        "null defaclobjtype for default ACL {defaclid}"
                    )));
                }
            };
            Some(DefaultAclDescRow {
                defaclrole,
                defaclnamespace,
                defaclobjtype,
            })
        }
    };

    // table_close(catalog, AccessShareLock);
    catalog.close(AccessShareLock)?;
    Ok(result)
}
