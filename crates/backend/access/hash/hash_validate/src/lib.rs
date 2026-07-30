// hashvalidate.c — hashvalidate + hashadjustmembers.
#![allow(non_snake_case)]

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opclass_for_family_datatype, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_core::{InvalidOid, Oid, BOOLOID, HASH_AM_OID, INT4OID, INT8OID};
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_hash::hashpage::{HASHEXTENDED_PROC, HASHOPTIONS_PROC, HASHSTANDARD_PROC};
use types_relscan::OpFamilyMember;
use types_scan::scankey::{HTEqualStrategyNumber, HTMaxStrategyNumber};

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "hashvalidate"))
}

pub fn hashvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("hashvalidate");
    let mcx = ctx.mcx();
    let mut result = true;

    let shape = syscache_seams::lookup_pg_opclass_shape::call(opclassoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator class {opclassoid}"));
    let opfamilyoid = shape.opcfamily;
    let opcintype = shape.opcintype;
    let opclassname_data = syscache_seams::pg_opclass_opcname::call(opclassoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator class {opclassoid}"));
    let opclassname =
        core::str::from_utf8(opclassname_data.name_str()).unwrap_or("").to_string();

    let opfamilyname = lsyscache::get_opfamily_name(mcx, opfamilyoid, false)?
        .expect("opfamily name")
        .as_str()
        .to_string();

    let (oprlist, opr_ordered) = syscache_seams::lookup_pg_amop_rows::call(mcx, opfamilyoid)?;
    let (proclist, proc_ordered) = syscache_seams::lookup_pg_amproc_rows::call(mcx, opfamilyoid)?;

    let mut hashabletypes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);

    // Check individual support functions.
    for procform in proclist.iter() {
        // All hash functions should be registered with matching left/right
        // types.
        if procform.amproclefttype != procform.amprocrighttype {
            info(format!(
                "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                opfamilyname,
                "hash",
                adt_regproc::format_procedure(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        let ok = match procform.amprocnum as u16 {
            HASHSTANDARD_PROC => check_amproc_signature(
                procform.amproc,
                INT4OID,
                true,
                1,
                1,
                &[procform.amproclefttype],
            )?,
            HASHEXTENDED_PROC => check_amproc_signature(
                procform.amproc,
                INT8OID,
                true,
                2,
                2,
                &[procform.amproclefttype, INT8OID],
            )?,
            HASHOPTIONS_PROC => check_amoptsproc_signature(procform.amproc)?,
            _ => {
                info(format!(
                    "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                    opfamilyname,
                    "hash",
                    adt_regproc::format_procedure(mcx, procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue;
            }
        };

        if !ok {
            info(format!(
                "operator family \"{}\" of access method {} contains function {} with wrong signature for support number {}",
                opfamilyname,
                "hash",
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }

        if ok
            && matches!(procform.amprocnum as u16, HASHSTANDARD_PROC | HASHEXTENDED_PROC)
            && !hashabletypes.contains(&procform.amproclefttype)
        {
            hashabletypes.push(procform.amproclefttype);
        }
    }

    // Check individual operators.
    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 || oprform.amopstrategy as u16 > HTMaxStrategyNumber {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                "hash",
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // Hash doesn't support ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH || oprform.amopsortfamily != InvalidOid {
            info(format!(
                "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                opfamilyname,
                "hash",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }

        if !check_amop_signature(
            oprform.amopopr,
            BOOLOID,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                opfamilyname,
                "hash",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }

        if !hashabletypes.contains(&oprform.amoplefttype)
            || !hashabletypes.contains(&oprform.amoprighttype)
        {
            info(format!(
                "operator family \"{}\" of access method {} lacks support function for operator {}",
                opfamilyname,
                "hash",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Check for inconsistent groups of operators/functions.
    let grouplist =
        identify_opfamily_groups(mcx, &oprlist, opr_ordered, &proclist, proc_ordered)?;
    let mut opclassgroup = None;
    for thisgroup in grouplist.iter() {
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(*thisgroup);
        }

        // A hash function without an operator is an incomplete set.
        if thisgroup.operatorset != (1u64 << HTEqualStrategyNumber) {
            info(format!(
                "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                opfamilyname,
                "hash",
                format_type::format_type_be(thisgroup.lefttype)?,
                format_type::format_type_be(thisgroup.righttype)?
            ))?;
            result = false;
        }
    }

    if opclassgroup.is_none() {
        info(format!(
            "operator class \"{}\" of access method {} is missing operator(s)",
            opclassname, "hash"
        ))?;
        result = false;
    }

    // Missing cross-type operators are not fatal, but built-in hash
    // opfamilies must be complete.
    if grouplist.len() != hashabletypes.len() * hashabletypes.len() {
        info(format!(
            "operator family \"{}\" of access method {} is missing cross-type operator(s)",
            opfamilyname, "hash"
        ))?;
        result = false;
    }

    Ok(result)
}

pub fn hashadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    let mut opclassoid = opclassoid;
    let mut opcintype = if opclassoid != InvalidOid {
        // During CREATE OPERATOR CLASS, CCI to see the pg_opclass row.
        xact::CommandCounterIncrement()?;
        lsyscache::get_opclass_input_type(opclassoid)?
    } else {
        InvalidOid
    };

    for op in operators.iter_mut().chain(functions.iter_mut()) {
        if op.is_func && op.number as u16 != HASHSTANDARD_PROC {
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else if op.lefttype != op.righttype {
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else {
            if op.lefttype != opcintype {
                opcintype = op.lefttype;
                opclassoid =
                    opclass_for_family_datatype(HASH_AM_OID, opfamilyoid, opcintype)?;
            }
            if opclassoid != InvalidOid {
                op.ref_is_hard = true;
                op.ref_is_family = false;
                op.refobjid = opclassoid;
            } else {
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
        }
    }
    Ok(())
}
