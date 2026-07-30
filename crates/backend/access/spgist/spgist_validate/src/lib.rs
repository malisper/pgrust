// spgvalidate.c — spgvalidate + spgadjustmembers.
#![allow(non_snake_case)]

use elog::ereport;
use index_amvalidate::{
    check_amop_signature, check_amoptsproc_signature, check_amproc_signature,
    identify_opfamily_groups, opfamily_can_sort_type, AMOP_SEARCH,
};
use mcx::MemoryContext;
use types_core::{InvalidOid, Oid, BOOLOID, INTERNALOID, VOIDOID};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_fmgr::LocalFcinfo;
use types_relscan::OpFamilyMember;
use types_spgist::{
    spgConfigIn, spgConfigOut, SPGISTNProc, SPGIST_CHOOSE_PROC, SPGIST_COMPRESS_PROC,
    SPGIST_CONFIG_PROC, SPGIST_INNER_CONSISTENT_PROC, SPGIST_LEAF_CONSISTENT_PROC,
    SPGIST_OPTIONS_PROC, SPGIST_PICKSPLIT_PROC,
};

fn info(msg: String) -> PgResult<()> {
    ereport(INFO)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "spgvalidate"))
}

pub fn spgvalidate(opclassoid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("spgvalidate");
    let mcx = ctx.mcx();
    let mut result = true;

    let shape = syscache_seams::lookup_pg_opclass_shape::call(opclassoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator class {opclassoid}"));
    let opfamilyoid = shape.opcfamily;
    let opcintype = shape.opcintype;
    let opckeytype = shape.opckeytype;
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
    let mut grouplist =
        identify_opfamily_groups(mcx, &oprlist, opr_ordered, &proclist, proc_ordered)?;

    let mut configOutLefttype = InvalidOid;
    let mut configOutRighttype = InvalidOid;
    let mut configOutLeafType = InvalidOid;

    for procform in proclist.iter() {
        if procform.amproclefttype != procform.amprocrighttype {
            info(format!(
                "operator family \"{}\" of access method {} contains support function {} with different left and right input types",
                opfamilyname,
                "spgist",
                adt_regproc::format_procedure(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        let ok = match procform.amprocnum as u16 {
            SPGIST_CONFIG_PROC => {
                let ok = check_amproc_signature(
                    procform.amproc,
                    VOIDOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?;
                let config_in = spgConfigIn { attType: procform.amproclefttype };
                let mut config_out = spgConfigOut::default();
                call_config_proc(procform.amproc, &config_in, &mut config_out)?;

                configOutLefttype = procform.amproclefttype;
                configOutRighttype = procform.amprocrighttype;

                configOutLeafType = if opckeytype != InvalidOid {
                    opckeytype
                } else {
                    procform.amproclefttype
                };

                if config_out.leafType != InvalidOid && configOutLeafType != config_out.leafType {
                    info(format!(
                        "SP-GiST leaf data type {} does not match declared type {}",
                        format_type::format_type_be(config_out.leafType)?,
                        format_type::format_type_be(configOutLeafType)?
                    ))?;
                    result = false;
                    configOutLeafType = config_out.leafType;
                }

                // Same leaf and attribute type: compress is not required, so
                // pre-set its functionset bit for the group check below.
                if configOutLeafType == config_in.attType {
                    for group in grouplist.iter_mut() {
                        if group.lefttype == procform.amproclefttype
                            && group.righttype == procform.amprocrighttype
                        {
                            group.functionset |= 1u64 << SPGIST_COMPRESS_PROC;
                            break;
                        }
                    }
                }
                ok
            }
            SPGIST_CHOOSE_PROC | SPGIST_PICKSPLIT_PROC | SPGIST_INNER_CONSISTENT_PROC => {
                check_amproc_signature(
                    procform.amproc,
                    VOIDOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?
            }
            SPGIST_LEAF_CONSISTENT_PROC => check_amproc_signature(
                procform.amproc,
                BOOLOID,
                true,
                2,
                2,
                &[INTERNALOID, INTERNALOID],
            )?,
            SPGIST_COMPRESS_PROC => {
                if configOutLefttype != procform.amproclefttype
                    || configOutRighttype != procform.amprocrighttype
                {
                    false
                } else {
                    check_amproc_signature(
                        procform.amproc,
                        configOutLeafType,
                        true,
                        1,
                        1,
                        &[procform.amproclefttype],
                    )?
                }
            }
            SPGIST_OPTIONS_PROC => check_amoptsproc_signature(procform.amproc)?,
            _ => {
                info(format!(
                    "operator family \"{}\" of access method {} contains function {} with invalid support number {}",
                    opfamilyname,
                    "spgist",
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
                "spgist",
                adt_regproc::format_procedure(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    for oprform in oprlist.iter() {
        if oprform.amopstrategy < 1 || oprform.amopstrategy > 63 {
            info(format!(
                "operator family \"{}\" of access method {} contains operator {} with invalid strategy number {}",
                opfamilyname,
                "spgist",
                adt_regproc::format_operator(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // spgist supports ORDER BY operators; their result must match the
        // claimed btree opfamily.
        let op_rettype = if oprform.amoppurpose != AMOP_SEARCH {
            let op_rettype = lsyscache::get_op_rettype(oprform.amopopr)?;
            if !opfamily_can_sort_type(oprform.amopsortfamily, op_rettype)? {
                info(format!(
                    "operator family \"{}\" of access method {} contains invalid ORDER BY specification for operator {}",
                    opfamilyname,
                    "spgist",
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
                "spgist",
                adt_regproc::format_operator(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    let mut opclassgroup = None;
    for thisgroup in grouplist.iter() {
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(*thisgroup);
        }

        if thisgroup.operatorset == 0 {
            info(format!(
                "operator family \"{}\" of access method {} is missing operator(s) for types {} and {}",
                opfamilyname,
                "spgist",
                format_type::format_type_be(thisgroup.lefttype)?,
                format_type::format_type_be(thisgroup.righttype)?
            ))?;
            result = false;
        }

        if thisgroup.lefttype != thisgroup.righttype {
            continue;
        }

        for i in 1..=SPGISTNProc as u16 {
            if thisgroup.functionset & (1u64 << i) != 0 {
                continue;
            }
            if i == SPGIST_OPTIONS_PROC {
                continue;
            }
            info(format!(
                "operator family \"{}\" of access method {} is missing support function {} for type {}",
                opfamilyname,
                "spgist",
                i,
                format_type::format_type_be(thisgroup.lefttype)?
            ))?;
            result = false;
        }
    }

    if opclassgroup.is_none() {
        info(format!(
            "operator class \"{}\" of access method {} is missing operator(s)",
            opclassname, "spgist"
        ))?;
        result = false;
    }

    Ok(result)
}

// OidFunctionCall2(configproc, &configIn, &configOut).
fn call_config_proc(
    proc_oid: Oid,
    config_in: &spgConfigIn,
    config_out: &mut spgConfigOut,
) -> PgResult<()> {
    let mut flinfo = fmgr_seams::fmgr_info::call(proc_oid)?;
    let scratch = MemoryContext::new("spgvalidate config call");
    let mut frame: LocalFcinfo<2> = LocalFcinfo::fresh(InvalidOid);
    // SAFETY: scratch outlives the frame and the config output consumption.
    unsafe { frame.set_result_mcx(scratch.mcx()) };
    frame.set_arg(0, datum::Datum::from_usize(config_in as *const spgConfigIn as usize));
    frame.set_arg(1, datum::Datum::from_usize(config_out as *mut spgConfigOut as usize));
    flinfo.invoke(&mut frame)?;
    Ok(())
}

pub fn spgadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    // Operator members never get hard dependencies (their opfamily connection
    // depends only on what the support functions think).
    for op in operators.iter_mut() {
        op.ref_is_hard = false;
        op.ref_is_family = true;
        op.refobjid = opfamilyoid;
    }

    for op in functions.iter_mut() {
        match op.number as u16 {
            SPGIST_CONFIG_PROC
            | SPGIST_CHOOSE_PROC
            | SPGIST_PICKSPLIT_PROC
            | SPGIST_INNER_CONSISTENT_PROC
            | SPGIST_LEAF_CONSISTENT_PROC => {
                op.ref_is_hard = true;
            }
            SPGIST_COMPRESS_PROC | SPGIST_OPTIONS_PROC => {
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
            _ => {
                return Err(Box::new(
                    PgError::error(format!(
                        "support function number {} is invalid for access method {}",
                        op.number, "spgist"
                    ))
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
                ));
            }
        }
    }
    Ok(())
}
