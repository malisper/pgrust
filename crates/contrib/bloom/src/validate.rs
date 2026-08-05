//! blvalidate.c. Message texts are C's exactly (they differ from core AMs').

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_bloom::{BLOOM_HASH_PROC, BLOOM_NPROC, BLOOM_NSTRATEGIES, BLOOM_OPTIONS_PROC};
use types_core::{InvalidOid, Oid, BOOLOID, INT4OID};
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "blvalidate"))
}

pub fn blvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("blvalidate");
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

    for procform in proclist.iter() {
        if procform.amproclefttype != procform.amprocrighttype {
            info(format!(
                "bloom opfamily {} contains support procedure {} with cross-type registration",
                opfamilyname,
                adt_regproc::format_procedure(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        // Signatures are only checkable within the specific opclass (opckeytype).
        if procform.amproclefttype != opcintype {
            continue;
        }

        let ok = match procform.amprocnum as u16 {
            BLOOM_HASH_PROC => {
                check_amproc_signature(procform.amproc, INT4OID, false, 1, 1, &[opckeytype])?
            }
            BLOOM_OPTIONS_PROC => check_amoptsproc_signature(procform.amproc)?,
            _ => {
                info(format!(
                    "bloom opfamily {} contains function {} with invalid support number {}",
                    opfamilyname,
                    adt_regproc::format_procedure(mcx, procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue; // don't want additional message
            }
        };

        if !ok {
            info(format!(
                "bloom opfamily {} contains function {} with wrong signature for support number {}",
                opfamilyname,
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 || oprform.amopstrategy > BLOOM_NSTRATEGIES as i16 {
            info(format!(
                "bloom opfamily {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        if oprform.amoppurpose != AMOP_SEARCH || oprform.amopsortfamily != InvalidOid {
            info(format!(
                "bloom opfamily {} contains invalid ORDER BY specification for operator {}",
                opfamilyname,
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
                "bloom opfamily {} contains operator {} with wrong signature",
                opfamilyname,
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    let grouplist = identify_opfamily_groups(mcx, &oprlist, opr_ordered, &proclist, proc_ordered)?;
    let mut opclassgroup = None;
    for thisgroup in grouplist.iter() {
        // Last group matching the test opclass wins; empty operator sets can
        // be OK (each bloom opclass is a law unto itself — C comment).
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(*thisgroup);
        }
    }

    // The originally-named opclass must be complete.
    for i in 1..=BLOOM_NPROC {
        if let Some(g) = opclassgroup {
            if g.functionset & (1u64 << i) != 0 {
                continue; // got it
            }
        }
        if i == BLOOM_OPTIONS_PROC {
            continue; // optional method
        }
        info(format!(
            "bloom opclass {} is missing support function {}",
            opclassname, i
        ))?;
        result = false;
    }

    Ok(result)
}
