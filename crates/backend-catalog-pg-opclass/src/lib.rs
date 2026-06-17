//! `GetDefaultOpClass` (commands/indexcmds.c) — the default operator class for
//! a type in a given access method.
//!
//! `GetDefaultOpClass` is reached through the `lsyscache` convenience surface
//! (`get_default_opclass`), which routes the whole computation through this
//! unit's `backend_catalog_pg_opclass_seams::get_default_opclass` seam (the
//! "home divergence" the seam crate documents). It opens `pg_opclass`,
//! `systable_beginscan`s `OpclassAmNameNspIndexId` filtered to the access
//! method, and resolves the unique exact / binary-compatible /
//! preferred-compatible default via `getBaseType`, `TypeCategory`,
//! `IsBinaryCoercible` and `IsPreferredType`.
//!
//! Although the C lives in `indexcmds.c`, none of `GetDefaultOpClass`'s helpers
//! reach the (unported) ALTER/CREATE INDEX spine, so it ports cleanly as its
//! own leaf unit and installs the `pg_opclass` seam.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table::{table_close, table_open};
use backend_parser_coerce::{IsBinaryCoercible, IsPreferredType, TypeCategory};
use backend_utils_adt_format_type::format_type_be_str;
use backend_utils_cache_lsyscache::type_::get_base_type;
use mcx::MemoryContext;
use types_catalog::opclasscmds_catalog::{
    Anum_pg_opclass_oid, Anum_pg_opclass_opcdefault, Anum_pg_opclass_opcintype,
    Anum_pg_opclass_opcmethod, OpclassAmNameNspIndexId, OperatorClassRelationId,
};
use types_core::fmgr::F_OIDEQ;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::AccessShareLock;
use types_tuple::backend_access_common_heaptuple::Datum;

/// `GetDefaultOpClass(type_id, am_id)` (commands/indexcmds.c) — the default
/// operator class OID for `type_id` in access method `am_id`, or `InvalidOid`
/// when there is no unambiguous default.
///
/// We scan through all the opclasses available for the access method, looking
/// for one that is marked default and matches the target type (either exactly
/// or binary-compatibly, but prefer an exact match). We could find more than
/// one binary-compatible match; if just one is for a preferred type, use that
/// one, otherwise we fail (the preferred-type special case is a kluge for
/// varchar). If we find more than one exact match, then someone put bogus
/// entries in pg_opclass.
pub fn GetDefaultOpClass(mut type_id: Oid, am_id: Oid) -> PgResult<Oid> {
    let mut result: Oid = InvalidOid;
    let mut nexact: i32 = 0;
    let mut ncompatible: i32 = 0;
    let mut ncompatiblepreferred: i32 = 0;

    // If it's a domain, look at the base type instead.
    type_id = get_base_type(type_id)?;
    let tcategory = TypeCategory(type_id)?;

    // Scan temporaries land in a scratch context dropped at the end.
    let scratch = MemoryContext::new("GetDefaultOpClass scan");
    let smcx = scratch.mcx();

    let rel = table_open(smcx, OperatorClassRelationId, AccessShareLock)?;

    // ScanKeyInit(&skey[0], Anum_pg_opclass_opcmethod, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(am_id));
    let mut skey = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_opclass_opcmethod,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(am_id),
    )?;

    let mut scan = genam_seams::systable_beginscan::call(
        &rel,
        OpclassAmNameNspIndexId,
        true,
        None,
        &skey,
    )?;

    while let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? {
        // Form_pg_opclass opclass = (Form_pg_opclass) GETSTRUCT(tup);
        let row = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let opcdefault = row[(Anum_pg_opclass_opcdefault - 1) as usize].0.as_bool();
        // ignore altogether if not a default opclass
        if !opcdefault {
            continue;
        }
        let opcintype = row[(Anum_pg_opclass_opcintype - 1) as usize].0.as_oid();
        let opc_oid = row[(Anum_pg_opclass_oid - 1) as usize].0.as_oid();

        if opcintype == type_id {
            nexact += 1;
            result = opc_oid;
        } else if nexact == 0 && IsBinaryCoercible(type_id, opcintype)? {
            if IsPreferredType(tcategory, opcintype)? {
                ncompatiblepreferred += 1;
                result = opc_oid;
            } else if ncompatiblepreferred == 0 {
                ncompatible += 1;
                result = opc_oid;
            }
        }
    }

    scan.end()?;
    table_close(rel, AccessShareLock)?;
    drop(scratch);

    // raise error if pg_opclass contains inconsistent data
    if nexact > 1 {
        let type_name = format_type_be_str(type_id)?;
        return Err(PgError::error(alloc::format!(
            "there are multiple default operator classes for data type {type_name}"
        )));
    }

    if nexact == 1
        || ncompatiblepreferred == 1
        || (ncompatiblepreferred == 0 && ncompatible == 1)
    {
        return Ok(result);
    }

    Ok(InvalidOid)
}

/// Install this unit's seams.
pub fn init_seams() {
    backend_catalog_pg_opclass_seams::get_default_opclass::set(GetDefaultOpClass);
}
