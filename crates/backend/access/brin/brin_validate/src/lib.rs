// brin_validate.c — brinvalidate.
#![allow(non_snake_case)]

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_brin::{
    BRIN_PROCNUM_ADDVALUE, BRIN_PROCNUM_CONSISTENT, BRIN_PROCNUM_OPCINFO, BRIN_PROCNUM_UNION,
};
use types_core::{InvalidOid, Oid, BOOLOID, INT4OID, INTERNALOID};
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};

const BRIN_PROCNUM_OPTIONS: u16 = 5;
const BRIN_MANDATORY_NPROCS: u16 = 4;
const BRIN_FIRST_OPTIONAL_PROCNUM: i16 = 11;
const BRIN_LAST_OPTIONAL_PROCNUM: i16 = 15;

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "brinvalidate"))
}

pub fn brinvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("brinvalidate");
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

    let mut allfuncs: u64 = 0;
    let mut allops: u64 = 0;

    for procform in proclist.iter() {
        let ok = match procform.amprocnum as u16 {
            BRIN_PROCNUM_OPCINFO => {
                check_amproc_signature(procform.amproc, INTERNALOID, true, 1, 1, &[INTERNALOID])?
            }
            BRIN_PROCNUM_ADDVALUE => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                true,
                4,
                4,
                &[INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID],
            )?,
            BRIN_PROCNUM_CONSISTENT => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                true,
                3,
                4,
                &[INTERNALOID, INTERNALOID, INTERNALOID, INT4OID],
            )?,
            BRIN_PROCNUM_UNION => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                true,
                3,
                3,
                &[INTERNALOID, INTERNALOID, INTERNALOID],
            )?,
            BRIN_PROCNUM_OPTIONS => check_amoptsproc_signature(procform.amproc)?,
            _ => {
                if procform.amprocnum < BRIN_FIRST_OPTIONAL_PROCNUM
                    || procform.amprocnum > BRIN_LAST_OPTIONAL_PROCNUM
                {
                    info(format!(
                        "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                        opfamilyname,
                        "brin",
                        adt_regproc::format_procedure(mcx, procform.amproc)?,
                        procform.amprocnum
                    ))?;
                    result = false;
                    continue;
                }
                // Can't check signatures of optional procs, so assume OK.
                true
            }
        };

        if !ok {
            info(format!(
                "operator family \"{}\" of access method {} contains function {} with wrong signature for support number {}",
                opfamilyname,
                "brin",
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }

        allfuncs |= 1u64 << procform.amprocnum;
    }

    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 || oprform.amopstrategy > 63 {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                "brin",
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        } else if oprform.amoplefttype == oprform.amoprighttype {
            // Only non-cross-type strategy numbers feed the completeness
            // check; cross-type operators may use unique strategies.
            allops |= 1u64 << oprform.amopstrategy;
        }

        if oprform.amoppurpose != AMOP_SEARCH || oprform.amopsortfamily != InvalidOid {
            info(format!(
                "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                opfamilyname,
                "brin",
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
                "brin",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    let grouplist =
        identify_opfamily_groups(mcx, &oprlist, opr_ordered, &proclist, proc_ordered)?;
    let mut opclassgroup = None;
    for thisgroup in grouplist.iter() {
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(*thisgroup);
        }

        // Cross-type pairs with no support functions at all get a pass.
        if thisgroup.functionset == 0 && thisgroup.lefttype != thisgroup.righttype {
            continue;
        }

        if thisgroup.operatorset != allops {
            info(format!(
                "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                opfamilyname,
                "brin",
                format_type::format_type_be(thisgroup.lefttype)?,
                format_type::format_type_be(thisgroup.righttype)?
            ))?;
            result = false;
        }
        if thisgroup.functionset != allfuncs {
            info(format!(
                "operator family \"{}\" of access method {} is missing support function(s) for types {} and {}",
                opfamilyname,
                "brin",
                format_type::format_type_be(thisgroup.lefttype)?,
                format_type::format_type_be(thisgroup.righttype)?
            ))?;
            result = false;
        }
    }

    if opclassgroup.is_none()
        || opclassgroup.map(|g| g.operatorset != allops).unwrap_or(false)
    {
        info(format!(
            "operator class \"{}\" of access method {} is missing operator(s)",
            opclassname, "brin"
        ))?;
        result = false;
    }
    for i in 1..=BRIN_MANDATORY_NPROCS {
        if let Some(g) = opclassgroup {
            if g.functionset & (1u64 << i) != 0 {
                continue;
            }
        }
        info(format!(
            "operator class \"{}\" of access method {} is missing support function {}",
            opclassname, "brin", i
        ))?;
        result = false;
    }

    Ok(result)
}
