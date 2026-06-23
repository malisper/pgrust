//! `src/backend/access/gin/ginvalidate.c` (PostgreSQL 18.3) — the opclass
//! validator for GIN.
//!
//! Two entry points: [`ginvalidate`] (the `amvalidate` callback) and
//! [`ginadjustmembers`] (the `amadjustmembers` callback). Both return
//! `PgResult`; every `ereport(INFO)` diagnostic goes through the error
//! subsystem's seam and never raises. The fatal `elog(ERROR)` "cache lookup
//! failed" is the `Err` of the syscache `search_opclass` lookup.
//!
//! The validation logic — the `switch` over support-function numbers, the
//! per-procedure / per-operator signature checks, the group-consistency
//! `functionset` bit manipulation, and the missing-support-function
//! cross-checks — is reproduced exactly, including the `(uint64) 1 << i` vs
//! `1 << GIN_CONSISTENT_PROC` shift widths. The catalog substrate (syscache,
//! lsyscache, regproc, amvalidate signature checks) is reached through the
//! shared AM-validator seams, exactly as the sibling `hashvalidate` port.

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{Oid, OidIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};

pub use ::hash::backend_access_hash_hashvalidate::{
    AmopRow, AmprocRow, OpFamilyMember, OpFamilyOpFuncGroup, OpclassForm,
};

use amvalidate_seams as amvalidate_seams;
use regproc_seams as regproc_seams;
use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use error_seams as error_seams;

// GIN support function numbers (access/gin.h:24..31).
/// `GIN_COMPARE_PROC`.
pub const GIN_COMPARE_PROC: i16 = 1;
/// `GIN_EXTRACTVALUE_PROC`.
pub const GIN_EXTRACTVALUE_PROC: i16 = 2;
/// `GIN_EXTRACTQUERY_PROC`.
pub const GIN_EXTRACTQUERY_PROC: i16 = 3;
/// `GIN_CONSISTENT_PROC`.
pub const GIN_CONSISTENT_PROC: i16 = 4;
/// `GIN_COMPARE_PARTIAL_PROC`.
pub const GIN_COMPARE_PARTIAL_PROC: i16 = 5;
/// `GIN_TRICONSISTENT_PROC`.
pub const GIN_TRICONSISTENT_PROC: i16 = 6;
/// `GIN_OPTIONS_PROC`.
pub const GIN_OPTIONS_PROC: i16 = 7;
/// `GINNProcs`.
pub const GINNProcs: i16 = 7;

/// `AMOP_SEARCH` (pg_amop.h): `'s'`.
const AMOP_SEARCH: i8 = b's' as i8;

// Built-in type OIDs referenced by the support-function signature checks
// (catalog/pg_type_d.h).
const BOOLOID: Oid = 16;
const CHAROID: Oid = 18;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const INTERNALOID: Oid = 2281;

/// `ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg(...)))`.
/// INFO never raises in C; the validator records `result = false` and keeps
/// going.
fn report_info(msg: String) -> PgResult<()> {
    error_seams::ereport::call(
        PgError::new(INFO, msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

/// `ginvalidate(opclassoid)` (ginvalidate.c:30): validator for a GIN opclass.
pub fn ginvalidate(mcx: Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
    let mut result = true;

    // Fetch opclass information.
    //
    // C: classtup = SearchSysCache1(CLAOID, ...); if (!HeapTupleIsValid)
    //    elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
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
    // C: opckeytype = classform->opckeytype; if (!OidIsValid(opckeytype))
    //    opckeytype = opcintype;
    let mut opckeytype = classform.opckeytype;
    if !OidIsValid(opckeytype) {
        opckeytype = opcintype;
    }
    let opclassname = classform.opcname;

    // Fetch opfamily information.
    let opfamilyname = lsyscache_seams::get_opfamily_name::call(mcx, opfamilyoid, false)?
        .expect("get_opfamily_name(missing_ok = false) returned no name");

    // Fetch all operators and support functions of the opfamily.
    let oprlist = syscache_seams::search_amop_list::call(mcx, opfamilyoid)?;
    let proclist = syscache_seams::search_amproc_list::call(mcx, opfamilyoid)?;

    // Check individual support functions.
    for procform in &proclist {
        let ok;

        // All GIN support functions should be registered with matching
        // left/right types.
        if procform.amproclefttype != procform.amprocrighttype {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gin contains support function {} with different left and right input types",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?
            ))?;
            result = false;
        }

        // We can't check signatures except within the specific opclass, since
        // we need to know the associated opckeytype in many cases.
        if procform.amproclefttype != opcintype {
            continue;
        }

        // Check procedure numbers and function signatures.
        match procform.amprocnum {
            GIN_COMPARE_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INT4OID,
                    false,
                    2,
                    2,
                    &[opckeytype, opckeytype],
                )?;
            }
            GIN_EXTRACTVALUE_PROC => {
                // Some opclasses omit nullFlags.
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    false,
                    2,
                    3,
                    &[opcintype, INTERNALOID, INTERNALOID],
                )?;
            }
            GIN_EXTRACTQUERY_PROC => {
                // Some opclasses omit nullFlags and searchMode.
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    false,
                    5,
                    7,
                    &[
                        opcintype,
                        INTERNALOID,
                        INT2OID,
                        INTERNALOID,
                        INTERNALOID,
                        INTERNALOID,
                        INTERNALOID,
                    ],
                )?;
            }
            GIN_CONSISTENT_PROC => {
                // Some opclasses omit queryKeys and nullFlags.
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    false,
                    6,
                    8,
                    &[
                        INTERNALOID,
                        INT2OID,
                        opcintype,
                        INT4OID,
                        INTERNALOID,
                        INTERNALOID,
                        INTERNALOID,
                        INTERNALOID,
                    ],
                )?;
            }
            GIN_COMPARE_PARTIAL_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INT4OID,
                    false,
                    4,
                    4,
                    &[opckeytype, opckeytype, INT2OID, INTERNALOID],
                )?;
            }
            GIN_TRICONSISTENT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    CHAROID,
                    false,
                    7,
                    7,
                    &[
                        INTERNALOID,
                        INT2OID,
                        opcintype,
                        INT4OID,
                        INTERNALOID,
                        INTERNALOID,
                        INTERNALOID,
                    ],
                )?;
            }
            GIN_OPTIONS_PROC => {
                ok = amvalidate_seams::check_amoptsproc_signature::call(procform.amproc)?;
            }
            _ => {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method gin contains function {} with invalid support number {}",
                    regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gin contains function {} with wrong signature for support number {}",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in &oprlist {
        // TODO (as in C): Check that only allowed strategy numbers exist.
        if oprform.amopstrategy < 1 || oprform.amopstrategy > 63 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gin contains operator {} with invalid strategy number {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // gin doesn't support ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH || OidIsValid(oprform.amopsortfamily) {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gin contains invalid ORDER BY specification for operator {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }

        // Check operator signature --- same for all gin strategies.
        if !amvalidate_seams::check_amop_signature::call(
            oprform.amopopr,
            BOOLOID,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gin contains operator {} with wrong signature",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions. The validator's
    // owned rows are projected to the fields identify_opfamily_groups reads.
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
    let grouplist =
        amvalidate_seams::identify_opfamily_groups::call(mcx, &amv_oprlist, &amv_proclist)?;

    let mut opclassgroup: Option<&OpFamilyOpFuncGroup> = None;
    for thisgroup in &grouplist {
        // Remember the group exactly matching the test opclass.
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(thisgroup);
        }

        // There is not a lot we can do to check the operator sets, since each
        // GIN opclass is more or less a law unto itself, and some contain only
        // operators that are binary-compatible with the opclass datatype
        // (meaning that empty operator sets can be OK). That case also means
        // that we shouldn't insist on nonempty function sets except for the
        // opclass's own group.
    }

    // Check that the originally-named opclass is complete.
    //
    // C: for (i = 1; i <= GINNProcs; i++) and tests
    //    (opclassgroup->functionset & (((uint64) 1) << i)) != 0
    for i in 1..=GINNProcs {
        if let Some(group) = opclassgroup {
            if (group.functionset & ((1u64) << i)) != 0 {
                continue; // got it
            }
        }
        if i == GIN_COMPARE_PROC || i == GIN_COMPARE_PARTIAL_PROC || i == GIN_OPTIONS_PROC {
            continue; // optional method
        }
        if i == GIN_CONSISTENT_PROC || i == GIN_TRICONSISTENT_PROC {
            continue; // don't need both, see check below loop
        }
        report_info(format!(
            "operator class \"{opclassname}\" of access method gin is missing support function {i}"
        ))?;
        result = false;
    }

    // C: if (!opclassgroup ||
    //        ((opclassgroup->functionset & (1 << GIN_CONSISTENT_PROC)) == 0 &&
    //         (opclassgroup->functionset & (1 << GIN_TRICONSISTENT_PROC)) == 0))
    // Note: here C uses the *int* shift `(1 << ...)`, not the `(uint64)` cast
    // above; reproduced via the `i32` shift widened to `u64`.
    let missing_consistent = match opclassgroup {
        Some(group) => {
            (group.functionset & (1i32 << GIN_CONSISTENT_PROC) as u64) == 0
                && (group.functionset & (1i32 << GIN_TRICONSISTENT_PROC) as u64) == 0
        }
        None => true,
    };
    if missing_consistent {
        report_info(format!(
            "operator class \"{opclassname}\" of access method gin is missing support function {GIN_CONSISTENT_PROC} or {GIN_TRICONSISTENT_PROC}"
        ))?;
        result = false;
    }

    // ReleaseCatCacheList / ReleaseSysCache: the owned lists drop here.

    Ok(result)
}

/// `ginadjustmembers(opfamilyoid, opclassoid, operators, functions)`
/// (ginvalidate.c:268): prechecking function for adding operators/functions to
/// a GIN opfamily.
pub fn ginadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    // Operator members of a GIN opfamily should never have hard dependencies,
    // since their connection to the opfamily depends only on what the support
    // functions think, and that can be altered. For consistency, we make all
    // soft dependencies point to the opfamily, though a soft dependency on the
    // opclass would work as well in the CREATE OPERATOR CLASS case.
    for op in operators.iter_mut() {
        op.ref_is_hard = false;
        op.ref_is_family = true;
        op.refobjid = opfamilyoid;
    }

    // Required support functions should have hard dependencies. Preferably
    // those are just dependencies on the opclass, but if we're in ALTER
    // OPERATOR FAMILY, we leave the dependency pointing at the whole opfamily.
    // (Given that GIN opclasses generally don't share opfamilies, it seems
    // unlikely to be worth working harder.)
    for op in functions.iter_mut() {
        match op.number {
            GIN_EXTRACTVALUE_PROC | GIN_EXTRACTQUERY_PROC => {
                // Required support function.
                op.ref_is_hard = true;
            }
            GIN_COMPARE_PROC
            | GIN_CONSISTENT_PROC
            | GIN_COMPARE_PARTIAL_PROC
            | GIN_TRICONSISTENT_PROC
            | GIN_OPTIONS_PROC => {
                // Optional, so force it to be a soft family dependency.
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
            _ => {
                return Err(PgError::error(format!(
                    "support function number {} is invalid for access method gin",
                    op.number
                ))
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
            }
        }
    }

    Ok(())
}
