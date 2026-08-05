// gistvalidate.c — gistvalidate (gistadjustmembers ported in amapi).
#![allow(non_snake_case)]

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opfamily_can_sort_type, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_core::{
    InvalidOid, Oid, ANYOID, BOOLOID, FLOAT8OID, INT2OID, INT4OID, INTERNALOID, OIDOID, VOIDOID,
};
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_gist::{
    GISTNProcs, GIST_COMPRESS_PROC, GIST_CONSISTENT_PROC, GIST_DECOMPRESS_PROC,
    GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_FETCH_PROC, GIST_OPTIONS_PROC, GIST_PENALTY_PROC,
    GIST_PICKSPLIT_PROC, GIST_SORTSUPPORT_PROC, GIST_TRANSLATE_CMPTYPE_PROC, GIST_UNION_PROC,
};

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "gistvalidate"))
}

pub fn gistvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("gistvalidate");
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
                "gist",
                adt_regproc::format_procedure(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        if procform.amproclefttype != opcintype {
            continue;
        }

        let ok = match procform.amprocnum as u16 {
            GIST_CONSISTENT_PROC => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                false,
                5,
                5,
                &[INTERNALOID, opcintype, INT2OID, OIDOID, INTERNALOID],
            )?,
            GIST_UNION_PROC => check_amproc_signature(
                procform.amproc,
                opckeytype,
                false,
                2,
                2,
                &[INTERNALOID, INTERNALOID],
            )?,
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_FETCH_PROC => {
                check_amproc_signature(procform.amproc, INTERNALOID, true, 1, 1, &[INTERNALOID])?
            }
            GIST_PENALTY_PROC => check_amproc_signature(
                procform.amproc,
                INTERNALOID,
                true,
                3,
                3,
                &[INTERNALOID, INTERNALOID, INTERNALOID],
            )?,
            GIST_PICKSPLIT_PROC => check_amproc_signature(
                procform.amproc,
                INTERNALOID,
                true,
                2,
                2,
                &[INTERNALOID, INTERNALOID],
            )?,
            GIST_EQUAL_PROC => check_amproc_signature(
                procform.amproc,
                INTERNALOID,
                false,
                3,
                3,
                &[opckeytype, opckeytype, INTERNALOID],
            )?,
            GIST_DISTANCE_PROC => check_amproc_signature(
                procform.amproc,
                FLOAT8OID,
                false,
                5,
                5,
                &[INTERNALOID, opcintype, INT2OID, OIDOID, INTERNALOID],
            )?,
            GIST_OPTIONS_PROC => check_amoptsproc_signature(procform.amproc)?,
            GIST_SORTSUPPORT_PROC => {
                check_amproc_signature(procform.amproc, VOIDOID, true, 1, 1, &[INTERNALOID])?
            }
            GIST_TRANSLATE_CMPTYPE_PROC => {
                check_amproc_signature(procform.amproc, INT2OID, true, 1, 1, &[INT4OID])?
                    && procform.amproclefttype == ANYOID
                    && procform.amprocrighttype == ANYOID
            }
            _ => {
                info(format!(
                    "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                    opfamilyname,
                    "gist",
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
                "gist",
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                "gist",
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // GiST supports ORDER BY operators; they must have a matching
        // distance proc, and their result must match the claimed btree
        // opfamily.
        let op_rettype = if oprform.amoppurpose != AMOP_SEARCH {
            if lsyscache::get_opfamily_proc(
                opfamilyoid,
                oprform.amoplefttype,
                oprform.amoplefttype,
                GIST_DISTANCE_PROC as i16,
            )? == InvalidOid
            {
                info(format!(
                    "operator family \"{}\" of access method {} contains unsupported ORDER BY specification for operator {}",
                    opfamilyname,
                    "gist",
                    adt_regproc::format_operator(mcx, oprform.amopopr)?
                ))?;
                result = false;
            }
            let op_rettype = lsyscache::get_op_rettype(oprform.amopopr)?;
            if !opfamily_can_sort_type(oprform.amopsortfamily, op_rettype)? {
                info(format!(
                    "operator family \"{}\" of access method {} contains incorrect ORDER BY opfamily specification for operator {}",
                    opfamilyname,
                    "gist",
                    adt_regproc::format_operator(mcx, oprform.amopopr)?
                ))?;
                result = false;
            }
            op_rettype
        } else {
            BOOLOID
        };

        if !check_amop_signature(
            oprform.amopopr,
            op_rettype,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with wrong signature",
                opfamilyname,
                "gist",
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
        // GiST opclass is more or less a law unto itself, and some contain
        // only operators that are binary-compatible with the opclass
        // datatype (meaning that empty operator sets can be OK). That case
        // also means that we shouldn't insist on nonempty function sets
        // except for the opclass's own group.
    }

    // Check that the originally-named opclass is complete.
    for i in 1..=GISTNProcs as u16 {
        if let Some(g) = opclassgroup {
            if g.functionset & (1u64 << i) != 0 {
                continue;
            }
        }
        if i == GIST_DISTANCE_PROC
            || i == GIST_FETCH_PROC
            || i == GIST_COMPRESS_PROC
            || i == GIST_DECOMPRESS_PROC
            || i == GIST_OPTIONS_PROC
            || i == GIST_SORTSUPPORT_PROC
            || i == GIST_TRANSLATE_CMPTYPE_PROC
        {
            continue;
        }
        info(format!(
            "operator class \"{}\" of access method {} is missing support function {}",
            opclassname, "gist", i
        ))?;
        result = false;
    }

    Ok(result)
}
