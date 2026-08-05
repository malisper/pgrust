// ginvalidate.c — ginvalidate (ginadjustmembers ported in amapi).
#![allow(non_snake_case)]

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_core::{InvalidOid, Oid, BOOLOID, CHAROID, INT2OID, INT4OID, INTERNALOID};
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use gin_vocab::{
    GINNProcs, GIN_COMPARE_PARTIAL_PROC, GIN_COMPARE_PROC, GIN_CONSISTENT_PROC,
    GIN_EXTRACTQUERY_PROC, GIN_EXTRACTVALUE_PROC, GIN_OPTIONS_PROC, GIN_TRICONSISTENT_PROC,
};

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "ginvalidate"))
}

pub fn ginvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("ginvalidate");
    let mcx = ctx.mcx();
    let mut result = true;

    let shape = syscache_seams::lookup_pg_opclass_shape::call(opclassoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator class {opclassoid}"));
    let opfamilyoid = shape.opcfamily;
    let opcintype = shape.opcintype;
    let opckeytype = if shape.opckeytype != InvalidOid { shape.opckeytype } else { opcintype };
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
        if procform.amproclefttype != procform.amprocrighttype {
            info(format!(
                "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                opfamilyname,
                "gin",
                adt_regproc::format_procedure(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        if procform.amproclefttype != opcintype {
            continue;
        }

        let ok = match procform.amprocnum as u16 {
            GIN_COMPARE_PROC => check_amproc_signature(
                procform.amproc,
                INT4OID,
                false,
                2,
                2,
                &[opckeytype, opckeytype],
            )?,
            GIN_EXTRACTVALUE_PROC => check_amproc_signature(
                procform.amproc,
                INTERNALOID,
                false,
                2,
                3,
                &[opcintype, INTERNALOID, INTERNALOID],
            )?,
            GIN_EXTRACTQUERY_PROC => check_amproc_signature(
                procform.amproc,
                INTERNALOID,
                false,
                5,
                7,
                &[
                    opcintype, INTERNALOID, INT2OID, INTERNALOID, INTERNALOID, INTERNALOID,
                    INTERNALOID,
                ],
            )?,
            GIN_CONSISTENT_PROC => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                false,
                6,
                8,
                &[
                    INTERNALOID, INT2OID, opcintype, INT4OID, INTERNALOID, INTERNALOID,
                    INTERNALOID, INTERNALOID,
                ],
            )?,
            GIN_COMPARE_PARTIAL_PROC => check_amproc_signature(
                procform.amproc,
                INT4OID,
                false,
                4,
                4,
                &[opckeytype, opckeytype, INT2OID, INTERNALOID],
            )?,
            GIN_TRICONSISTENT_PROC => check_amproc_signature(
                procform.amproc,
                CHAROID,
                false,
                7,
                7,
                &[
                    INTERNALOID, INT2OID, opcintype, INT4OID, INTERNALOID, INTERNALOID,
                    INTERNALOID,
                ],
            )?,
            GIN_OPTIONS_PROC => check_amoptsproc_signature(procform.amproc)?,
            _ => {
                info(format!(
                    "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                    opfamilyname,
                    "gin",
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
                "gin",
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 || oprform.amopstrategy > 63 {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                "gin",
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // gin doesn't support ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH || oprform.amopsortfamily != InvalidOid {
            info(format!(
                "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                opfamilyname,
                "gin",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }

        // Check operator signature --- same for all gin strategies.
        if !check_amop_signature(
            oprform.amopopr,
            BOOLOID,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                opfamilyname,
                "gin",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions.
    let grouplist = identify_opfamily_groups(mcx, &oprlist, opr_ordered, &proclist, proc_ordered)?;
    let mut opclassgroup = None;
    for thisgroup in grouplist.iter() {
        // Remember the group exactly matching the test opclass.
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(*thisgroup);
        }

        // There is not a lot we can do to check the operator sets, since each
        // GIN opclass is more or less a law unto itself, and some contain
        // only operators that are binary-compatible with the opclass
        // datatype (meaning that empty operator sets can be OK). That case
        // also means that we shouldn't insist on nonempty function sets
        // except for the opclass's own group.
    }

    // Check that the originally-named opclass is complete.
    for i in 1..=GINNProcs as u16 {
        if let Some(g) = opclassgroup {
            if g.functionset & (1u64 << i) != 0 {
                continue;
            }
        }
        if i == GIN_COMPARE_PROC || i == GIN_COMPARE_PARTIAL_PROC || i == GIN_OPTIONS_PROC {
            continue;
        }
        if i == GIN_CONSISTENT_PROC || i == GIN_TRICONSISTENT_PROC {
            continue; // don't need both, see check below loop
        }
        info(format!(
            "operator class \"{}\" of access method {} is missing support function {}",
            opclassname, "gin", i
        ))?;
        result = false;
    }
    let has_consistent = opclassgroup
        .map(|g| g.functionset & (1u64 << GIN_CONSISTENT_PROC) != 0)
        .unwrap_or(false);
    let has_triconsistent = opclassgroup
        .map(|g| g.functionset & (1u64 << GIN_TRICONSISTENT_PROC) != 0)
        .unwrap_or(false);
    if !has_consistent && !has_triconsistent {
        info(format!(
            "operator class \"{}\" of access method {} is missing support function {} or {}",
            opclassname, "gin", GIN_CONSISTENT_PROC, GIN_TRICONSISTENT_PROC
        ))?;
        result = false;
    }

    Ok(result)
}
