//! nbtvalidate.c — btree opclass validator and member-adjust hook.
#![allow(non_snake_case)]

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opclass_for_family_datatype, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_core::{
    InvalidOid, Oid, BOOLOID, BTREE_AM_OID, INT4OID, INTERNALOID, OIDOID, VOIDOID,
};
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_nbtree::page::{
    BTEQUALIMAGE_PROC, BTINRANGE_PROC, BTOPTIONS_PROC, BTORDER_PROC, BTSKIPSUPPORT_PROC,
    BTSORTSUPPORT_PROC,
};
use types_relscan::OpFamilyMember;
use types_scan::scankey::{
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber, BTMaxStrategyNumber,
};

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "btvalidate"))
}

pub fn btvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("btvalidate");
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

    // Check individual support functions.
    for procform in proclist.iter() {
        let ok = match procform.amprocnum as u16 {
            BTORDER_PROC => check_amproc_signature(
                procform.amproc,
                INT4OID,
                true,
                2,
                2,
                &[procform.amproclefttype, procform.amprocrighttype],
            )?,
            BTSORTSUPPORT_PROC => {
                check_amproc_signature(procform.amproc, VOIDOID, true, 1, 1, &[INTERNALOID])?
            }
            BTINRANGE_PROC => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                true,
                5,
                5,
                &[
                    procform.amproclefttype,
                    procform.amproclefttype,
                    procform.amprocrighttype,
                    BOOLOID,
                    BOOLOID,
                ],
            )?,
            BTEQUALIMAGE_PROC => {
                check_amproc_signature(procform.amproc, BOOLOID, true, 1, 1, &[OIDOID])?
            }
            BTOPTIONS_PROC => check_amoptsproc_signature(procform.amproc)?,
            BTSKIPSUPPORT_PROC => {
                check_amproc_signature(procform.amproc, VOIDOID, true, 1, 1, &[INTERNALOID])?
            }
            _ => {
                info(format!(
                    "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                    opfamilyname,
                    "btree",
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
                "btree",
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 || oprform.amopstrategy as u16 > BTMaxStrategyNumber {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                "btree",
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }
        if oprform.amoppurpose != AMOP_SEARCH || oprform.amopsortfamily != InvalidOid {
            info(format!(
                "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                opfamilyname,
                "btree",
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
                "btree",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Check for inconsistent groups of operators/functions.
    let grouplist =
        identify_opfamily_groups(mcx, &oprlist, opr_ordered, &proclist, proc_ordered)?;
    let mut usefulgroups = 0usize;
    let mut opclassgroup = None;
    let mut familytypes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    for thisgroup in grouplist.iter() {
        // A lone in_range function's RHS type doesn't represent a supported
        // type pair.
        if thisgroup.operatorset == 0 && thisgroup.functionset == (1u64 << BTINRANGE_PROC) {
            continue;
        }
        usefulgroups += 1;

        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(*thisgroup);
        }

        if !familytypes.contains(&thisgroup.lefttype) {
            familytypes.push(thisgroup.lefttype);
        }
        if !familytypes.contains(&thisgroup.righttype) {
            familytypes.push(thisgroup.righttype);
        }

        // sortsupport, in_range, and equalimage are optional.
        if thisgroup.operatorset
            != ((1u64 << BTLessStrategyNumber)
                | (1u64 << BTLessEqualStrategyNumber)
                | (1u64 << BTEqualStrategyNumber)
                | (1u64 << BTGreaterEqualStrategyNumber)
                | (1u64 << BTGreaterStrategyNumber))
        {
            info(format!(
                "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                opfamilyname,
                "btree",
                format_type::format_type_be(thisgroup.lefttype)?,
                format_type::format_type_be(thisgroup.righttype)?
            ))?;
            result = false;
        }
        if thisgroup.functionset & (1u64 << BTORDER_PROC) == 0 {
            info(format!(
                "operator family \"{}\" of access method {} is missing support function for types {} and {}",
                opfamilyname,
                "btree",
                format_type::format_type_be(thisgroup.lefttype)?,
                format_type::format_type_be(thisgroup.righttype)?
            ))?;
            result = false;
        }
    }

    if opclassgroup.is_none() {
        info(format!(
            "operator class \"{}\" of access method {} is missing operator(s)",
            opclassname, "btree"
        ))?;
        result = false;
    }

    if usefulgroups != familytypes.len() * familytypes.len() {
        info(format!(
            "operator family \"{}\" of access method {} is missing cross-type operator(s)",
            opfamilyname, "btree"
        ))?;
        result = false;
    }

    Ok(result)
}

pub fn btadjustmembers(
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
        if op.is_func && op.number as u16 != BTORDER_PROC {
            // Optional support proc: always a soft family dependency.
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else if op.lefttype != op.righttype {
            // Cross-type: always a soft family dependency.
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else {
            if op.lefttype != opcintype {
                opcintype = op.lefttype;
                opclassoid =
                    opclass_for_family_datatype(BTREE_AM_OID, opfamilyoid, opcintype)?;
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
