//! `src/backend/access/spgist/spgvalidate.c` (PostgreSQL 18.3) — the opclass
//! validator and member-precheck for the SP-GiST access method.
//!
//! Two entry points: [`spgvalidate`] (the `amvalidate` callback) and
//! [`spgadjustmembers`] (the `amadjustmembers` callback). The body is pure
//! orchestration over the system catalogs (syscache lookups, `amvalidate.c`
//! signature checks, opfamily-group identification) plus one call into each
//! opclass' `config` support procedure (the C `OidFunctionCall2(amproc, &cfgin,
//! &cfgout)`), reached through the SP-GiST core's typed `spg_config` dispatch
//! seam keyed on the proc OID. All other genuinely-external substrate is reached
//! through the per-owner function-pointer seams, exactly as the sibling
//! `backend-access-brin-validate` / `-nbt-validate` / `-hashvalidate` validators
//! do. The control flow, branch order, message text and SQLSTATE are ported 1:1.
//!
//! `spgvalidate` returns `PgResult`, the owned-model carrier for a C
//! `ereport(ERROR)`; every `INFO` diagnostic goes through the error subsystem's
//! seam and never raises (in C, `errfinish` returns for `INFO`; the validator
//! records `result = false` and keeps going).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{Oid, OidIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use ::opclass::AMOP_SEARCH;
use ::spgist::{
    spgConfigIn, spgConfigOut, SPGISTNProc, SPGIST_CHOOSE_PROC, SPGIST_COMPRESS_PROC,
    SPGIST_CONFIG_PROC, SPGIST_INNER_CONSISTENT_PROC, SPGIST_LEAF_CONSISTENT_PROC,
    SPGIST_OPTIONS_PROC, SPGIST_PICKSPLIT_PROC,
};

// The opclass-form / member-row mirror types carried across the syscache seams
// live in `types-hash` (shared by every AM validator); re-export them.
pub use ::hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};

use amvalidate_seams as amvalidate_seams;
use spg_core_seams as spg_core_seams;
use format_type_seams as format_type_seams;
use regproc_seams as regproc_seams;
use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use error_seams as error_seams;

// ===========================================================================
// Constants from catalog/pg_type.h.
// ===========================================================================

/// `BOOLOID` (pg_type.h).
const BOOLOID: Oid = 16;
/// `VOIDOID` (pg_type.h).
const VOIDOID: Oid = 2278;
/// `INTERNALOID` (pg_type.h).
const INTERNALOID: Oid = 2281;

/// `ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg(...)))`.
///
/// INFO never raises in C (`errfinish` returns); the validator records
/// `result = false` and keeps going.
fn report_info(msg: String) -> PgResult<()> {
    error_seams::ereport::call(
        PgError::new(INFO, msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

// ===========================================================================
// spgvalidate (spgvalidate.c:37)
// ===========================================================================

/// `spgvalidate(opclassoid)` — validator for an SP-GiST opclass.
///
/// Some checks cover the whole opfamily and are therefore redundant when
/// checking each opclass in a family, but they don't run long enough to matter,
/// so the duplication is accepted rather than complicating the amvalidate API
/// (as in C). `mcx` is the translation of the C current context every catalog
/// projection and work list is palloc'd in; everything allocated here drops on
/// return (C: `ReleaseCatCacheList` / `ReleaseSysCache` plus context cleanup).
pub fn spgvalidate(mcx: Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
    let mut result = true;

    // Fetch opclass information.
    let classform = match syscache_seams::search_opclass::call(mcx, opclassoid)? {
        Some(form) => form,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for operator class {opclassoid}"
            )))
        }
    };
    let opfamilyoid = classform.opcfamily;
    let opcintype = classform.opcintype;
    let opckeytype = classform.opckeytype;
    let opclassname = classform.opcname;

    // Fetch opfamily information.
    let opfamilyname = lsyscache_seams::get_opfamily_name::call(mcx, opfamilyoid, false)?
        .expect("get_opfamily_name(missing_ok = false) returned no name");

    // Fetch all operators and support functions of the opfamily.
    let oprlist = syscache_seams::search_amop_list::call(mcx, opfamilyoid)?;
    let proclist = syscache_seams::search_amproc_list::call(mcx, opfamilyoid)?;

    // grouplist = identify_opfamily_groups(oprlist, proclist). Built up-front
    // (as in C) because the config-proc branch below mutates a group's
    // functionset. Project the owned rows to the fields the helper reads.
    let mut amv_oprlist: PgVec<'_, types_amvalidate::AmopRow> =
        vec_with_capacity_in(mcx, oprlist.len())?;
    for r in &oprlist {
        amv_oprlist.push(types_amvalidate::AmopRow {
            amoplefttype: r.amoplefttype,
            amoprighttype: r.amoprighttype,
            amopstrategy: r.amopstrategy,
        });
    }
    let mut amv_proclist: PgVec<'_, types_amvalidate::AmprocRow> =
        vec_with_capacity_in(mcx, proclist.len())?;
    for r in &proclist {
        amv_proclist.push(types_amvalidate::AmprocRow {
            amproclefttype: r.amproclefttype,
            amprocrighttype: r.amprocrighttype,
            amprocnum: r.amprocnum,
        });
    }
    let mut grouplist =
        amvalidate_seams::identify_opfamily_groups::call(mcx, &amv_oprlist, &amv_proclist)?;

    // configOut*: tracked across the config branch for the compress-proc check.
    let mut config_out_lefttype = Oid::default();
    let mut config_out_righttype = Oid::default();
    let mut config_out_leaf_type = Oid::default();

    // Check individual support functions.
    for procform in &proclist {
        let ok;

        // All SP-GiST support functions should be registered with matching
        // left/right types.
        if procform.amproclefttype != procform.amprocrighttype {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method spgist contains support function {} with different left and right input types",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        // Check procedure numbers and function signatures.
        match procform.amprocnum as i32 {
            SPGIST_CONFIG_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    VOIDOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?;

                // configIn.attType = procform->amproclefttype; memset(&configOut).
                let configIn = spgConfigIn {
                    attType: procform.amproclefttype,
                };
                let mut configOut = spgConfigOut::default();

                // OidFunctionCall2(procform->amproc, &configIn, &configOut) —
                // the opclass config support proc, dispatched by OID.
                spg_core_seams::spg_config::call(procform.amproc, &configIn, &mut configOut)?;

                config_out_lefttype = procform.amproclefttype;
                config_out_righttype = procform.amprocrighttype;

                // Default leaf type is opckeytype or input type.
                if OidIsValid(opckeytype) {
                    config_out_leaf_type = opckeytype;
                } else {
                    config_out_leaf_type = procform.amproclefttype;
                }

                // If some other leaf datum type is specified, warn.
                if OidIsValid(configOut.leafType) && config_out_leaf_type != configOut.leafType {
                    report_info(format!(
                        "SP-GiST leaf data type {} does not match declared type {}",
                        format_type_seams::format_type_be::call(mcx, configOut.leafType)?,
                        format_type_seams::format_type_be::call(mcx, config_out_leaf_type)?
                    ))?;
                    result = false;
                    config_out_leaf_type = configOut.leafType;
                }

                // When leaf and attribute types are the same, the compress
                // function is not required; set the corresponding bit in the
                // matching group's functionset for the later consistency check.
                if config_out_leaf_type == configIn.attType {
                    for group in grouplist.iter_mut() {
                        if group.lefttype == procform.amproclefttype
                            && group.righttype == procform.amprocrighttype
                        {
                            group.functionset |= 1u64 << SPGIST_COMPRESS_PROC;
                            break;
                        }
                    }
                }
            }
            SPGIST_CHOOSE_PROC | SPGIST_PICKSPLIT_PROC | SPGIST_INNER_CONSISTENT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    VOIDOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?;
            }
            SPGIST_LEAF_CONSISTENT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?;
            }
            SPGIST_COMPRESS_PROC => {
                if config_out_lefttype != procform.amproclefttype
                    || config_out_righttype != procform.amprocrighttype
                {
                    ok = false;
                } else {
                    ok = amvalidate_seams::check_amproc_signature::call(
                        procform.amproc,
                        config_out_leaf_type,
                        true,
                        1,
                        1,
                        &[procform.amproclefttype],
                    )?;
                }
            }
            SPGIST_OPTIONS_PROC => {
                ok = amvalidate_seams::check_amoptsproc_signature::call(procform.amproc)?;
            }
            _ => {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method spgist contains function {} with invalid support number {}",
                    regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method spgist contains function {} with wrong signature for support number {}",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in &oprlist {
        let op_rettype;

        // TODO: Check that only allowed strategy numbers exist.
        if oprform.amopstrategy < 1 || oprform.amopstrategy > 63 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method spgist contains operator {} with invalid strategy number {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // spgist supports ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH {
            // ... and operator result must match the claimed btree opfamily.
            op_rettype = lsyscache_seams::get_op_rettype::call(oprform.amopopr)?;
            if !amvalidate_seams::opfamily_can_sort_type::call(oprform.amopsortfamily, op_rettype)?
            {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method spgist contains invalid ORDER BY specification for operator {}",
                    regproc_seams::format_operator::call(mcx, oprform.amopopr)?
                ))?;
                result = false;
            }
        } else {
            op_rettype = BOOLOID;
        }

        // Check operator signature --- same for all spgist strategies.
        if !amvalidate_seams::check_amop_signature::call(
            oprform.amopopr,
            op_rettype,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method spgist contains operator {} with wrong signature",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions.
    let mut opclassgroup: Option<&types_amvalidate::OpFamilyOpFuncGroup> = None;

    for thisgroup in &grouplist {
        // Remember the group exactly matching the test opclass.
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(thisgroup);
        }

        // Complain if there are any datatype pairs with functions but no
        // operators. (Best we can do for now to detect missing operators.)
        if thisgroup.operatorset == 0 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method spgist is missing operator(s) for types {} and {}",
                format_type_seams::format_type_be::call(mcx, thisgroup.lefttype)?,
                format_type_seams::format_type_be::call(mcx, thisgroup.righttype)?
            ))?;
            result = false;
        }

        // Complain if we're missing functions for any datatype, remembering that
        // SP-GiST doesn't use cross-type support functions.
        if thisgroup.lefttype != thisgroup.righttype {
            continue;
        }

        for i in 1..=SPGISTNProc {
            if (thisgroup.functionset & (1u64 << i)) != 0 {
                continue; // got it
            }
            if i == SPGIST_OPTIONS_PROC {
                continue; // optional method
            }
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method spgist is missing support function {i} for type {}",
                format_type_seams::format_type_be::call(mcx, thisgroup.lefttype)?
            ))?;
            result = false;
        }
    }

    // Check that the originally-named opclass is supported. (If the group is
    // there, we already checked it adequately above.)
    if opclassgroup.is_none() {
        report_info(format!(
            "operator class \"{opclassname}\" of access method spgist is missing operator(s)"
        ))?;
        result = false;
    }

    // ReleaseCatCacheList / ReleaseSysCache: the owned lists drop here.

    Ok(result)
}

// ===========================================================================
// spgadjustmembers (spgvalidate.c:322)
// ===========================================================================

/// `OpFamilyMember` (`amapi.h`). Canonical definition lives in `opclass`;
/// re-exported here so this crate names the same type (no duplicate definition).
/// `spgadjustmembers` reads only `number` and rewrites the
/// hard/family/`refobjid` dependency flags; the remaining C fields are present
/// but untouched here.
pub use ::opclass::OpFamilyMember;

/// `spgadjustmembers(opfamilyoid, opclassoid, operators, functions)`
/// (spgvalidate.c:322) — prechecking for adding operators/functions to an
/// SP-GiST opfamily, rewriting each member's dependency shape in place.
///
/// `opclassoid` is unused by the SP-GiST policy (it leaves required-support-fn
/// dependencies pointing at the whole opfamily); kept to mirror the C signature.
pub fn spgadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    // Operator members of an SP-GiST opfamily should never have hard
    // dependencies; make all of them soft, pointing at the opfamily.
    for op in operators.iter_mut() {
        op.ref_is_hard = false;
        op.ref_is_family = true;
        op.refobjid = opfamilyoid;
    }

    // Required support functions should have hard dependencies (left pointing at
    // the opfamily); optional ones become soft family dependencies.
    for op in functions.iter_mut() {
        match op.number {
            SPGIST_CONFIG_PROC
            | SPGIST_CHOOSE_PROC
            | SPGIST_PICKSPLIT_PROC
            | SPGIST_INNER_CONSISTENT_PROC
            | SPGIST_LEAF_CONSISTENT_PROC => {
                // Required support function.
                op.ref_is_hard = true;
            }
            SPGIST_COMPRESS_PROC | SPGIST_OPTIONS_PROC => {
                // Optional, so force a soft family dependency.
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
            _ => {
                return Err(PgError::error(format!(
                    "support function number {} is invalid for access method spgist",
                    op.number
                ))
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
            }
        }
    }

    Ok(())
}

/// This crate owns no inward seams (it is a leaf consumed by the SP-GiST handler
/// / `opclasscmds` `CREATE OPERATOR CLASS` validation path), mirroring the
/// sibling AM validators. The empty `init_seams()` keeps the `seams-init`
/// recurrence guard satisfied.
pub fn init_seams() {}
