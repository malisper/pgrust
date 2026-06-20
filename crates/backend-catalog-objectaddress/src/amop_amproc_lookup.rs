//! `pg_amop` / `pg_amproc` by-oid projections used by `getObjectDescription`
//! for `OCLASS_AMOP` (objectaddress.c 3229) and `OCLASS_AMPROC`
//! (objectaddress.c 3294). There are no `AMOPOID` / `AMPROCOID` system caches,
//! so the C by-oid fetch
//! `table_open(...) + systable_beginscan(...OidIndexId, oid = amopid) +
//! GETSTRUCT(Form_pg_amop/amproc)` is reproduced here with the shared
//! [`crate::resolve::get_catalog_object_by_oid`] oid-index scan followed by
//! `heap_getattr` on the fixed columns. Mirrors [`crate::trigger_lookup`].

use mcx::MemoryContext;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_storage::lock::AccessShareLock;

use backend_utils_cache_syscache_seams::{AmopDescriptionRow, AmprocDescriptionRow};

use crate::consts::{
    AccessMethodOperatorRelationId, AccessMethodProcedureRelationId, Anum_pg_amop_amopfamily,
    Anum_pg_amop_amoplefttype, Anum_pg_amop_amopopr, Anum_pg_amop_amoprighttype,
    Anum_pg_amop_amopstrategy, Anum_pg_amop_oid, Anum_pg_amproc_amproc,
    Anum_pg_amproc_amprocfamily, Anum_pg_amproc_amproclefttype, Anum_pg_amproc_amprocnum,
    Anum_pg_amproc_amprocrighttype, Anum_pg_amproc_oid,
};
use crate::resolve::get_catalog_object_by_oid;

/// `(amopfamily, amopstrategy, amoplefttype, amoprighttype, amopopr)` projection
/// — `getObjectDescription` (objectaddress.c 3229). `Ok(None)` on a scan miss.
pub fn amop_description_row(amopid: Oid) -> PgResult<Option<AmopDescriptionRow>> {
    let ctx = MemoryContext::new("amop_lookup");
    let mcx = ctx.mcx();
    {
        let catalog = backend_access_common_relation_seams::relation_open::call(
            mcx,
            AccessMethodOperatorRelationId,
            AccessShareLock,
        )?;

        let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_amop_oid, amopid)?;

        let result = match objtup {
            None => None,
            Some(tup) => {
                let get = |attnum: i16, name: &str| -> PgResult<Oid> {
                    match crate::fmgr_sql::heap_getattr(mcx, &tup, attnum as i32, &catalog.rd_att)? {
                        Some(d) => Ok(d.as_oid()),
                        None => Err(PgError::error(format!("null {name} for amop {amopid}"))),
                    }
                };
                let amopfamily = get(Anum_pg_amop_amopfamily, "amopfamily")?;
                let amoplefttype = get(Anum_pg_amop_amoplefttype, "amoplefttype")?;
                let amoprighttype = get(Anum_pg_amop_amoprighttype, "amoprighttype")?;
                let amopopr = get(Anum_pg_amop_amopopr, "amopopr")?;
                let amopstrategy = match crate::fmgr_sql::heap_getattr(
                    mcx,
                    &tup,
                    Anum_pg_amop_amopstrategy as i32,
                    &catalog.rd_att,
                )? {
                    Some(d) => d.as_i16(),
                    None => {
                        catalog.close(AccessShareLock)?;
                        return Err(PgError::error(format!(
                            "null amopstrategy for amop {amopid}"
                        )));
                    }
                };
                Some(AmopDescriptionRow {
                    amopfamily,
                    amopstrategy,
                    amoplefttype,
                    amoprighttype,
                    amopopr,
                })
            }
        };

        catalog.close(AccessShareLock)?;
        Ok(result)
    }
}

/// `(amprocfamily, amprocnum, amproclefttype, amprocrighttype, amproc)`
/// projection — `getObjectDescription` (objectaddress.c 3294). `Ok(None)` on a
/// scan miss.
pub fn amproc_description_row(amprocid: Oid) -> PgResult<Option<AmprocDescriptionRow>> {
    let ctx = MemoryContext::new("amproc_lookup");
    let mcx = ctx.mcx();
    {
        let catalog = backend_access_common_relation_seams::relation_open::call(
            mcx,
            AccessMethodProcedureRelationId,
            AccessShareLock,
        )?;

        let objtup = get_catalog_object_by_oid(mcx, &catalog, Anum_pg_amproc_oid, amprocid)?;

        let result = match objtup {
            None => None,
            Some(tup) => {
                let get = |attnum: i16, name: &str| -> PgResult<Oid> {
                    match crate::fmgr_sql::heap_getattr(mcx, &tup, attnum as i32, &catalog.rd_att)? {
                        Some(d) => Ok(d.as_oid()),
                        None => Err(PgError::error(format!("null {name} for amproc {amprocid}"))),
                    }
                };
                let amprocfamily = get(Anum_pg_amproc_amprocfamily, "amprocfamily")?;
                let amproclefttype = get(Anum_pg_amproc_amproclefttype, "amproclefttype")?;
                let amprocrighttype = get(Anum_pg_amproc_amprocrighttype, "amprocrighttype")?;
                let amproc = get(Anum_pg_amproc_amproc, "amproc")?;
                let amprocnum = match crate::fmgr_sql::heap_getattr(
                    mcx,
                    &tup,
                    Anum_pg_amproc_amprocnum as i32,
                    &catalog.rd_att,
                )? {
                    Some(d) => d.as_i16(),
                    None => {
                        catalog.close(AccessShareLock)?;
                        return Err(PgError::error(format!(
                            "null amprocnum for amproc {amprocid}"
                        )));
                    }
                };
                Some(AmprocDescriptionRow {
                    amprocfamily,
                    amprocnum,
                    amproclefttype,
                    amprocrighttype,
                    amproc,
                })
            }
        };

        catalog.close(AccessShareLock)?;
        Ok(result)
    }
}
