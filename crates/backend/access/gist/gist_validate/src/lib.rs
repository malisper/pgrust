//! `src/backend/access/gist/gistvalidate.c` (PostgreSQL 18.3) — the opclass
//! validator for the GiST access method.
//!
//! Two entry points: [`gistvalidate`] (the `amvalidate` callback) and
//! [`gistadjustmembers`] (the `amadjustmembers` callback). `gistvalidate`
//! returns `PgResult`, the owned-model carrier for a C `ereport(ERROR)`; every
//! INFO diagnostic goes through the error subsystem's seam and never raises (in
//! C, `errfinish` returns for `INFO`, the validator records `result = false`
//! and keeps going).
//!
//! The body is pure orchestration over the system catalogs (syscache lookups,
//! `amvalidate.c` signature checks, opfamily-group identification). All of that
//! genuinely-external substrate
//! (syscache/catalog/`amvalidate`/`lsyscache`/`regproc`/`format_type`) is
//! reached through the per-owner function-pointer seams, exactly as the sibling
//! `backend-access-brin-validate` / `backend-access-nbt-validate` validators
//! do. The control flow, branch order, message text and SQLSTATE are ported 1:1
//! from C.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use ::opclass::AMOP_SEARCH;

// The opclass-form / member-row mirror types carried across the syscache seams
// live in `types-hash` (shared by every AM validator); re-export so callers and
// tests can name them through this crate as well.
pub use ::hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};

use amvalidate_seams as amvalidate_seams;
use regproc_seams as regproc_seams;
use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use error_seams as error_seams;

use gist::{
    GISTNProcs, GIST_COMPRESS_PROC, GIST_CONSISTENT_PROC, GIST_DECOMPRESS_PROC, GIST_DISTANCE_PROC,
    GIST_EQUAL_PROC, GIST_FETCH_PROC, GIST_OPTIONS_PROC, GIST_PENALTY_PROC, GIST_PICKSPLIT_PROC,
    GIST_SORTSUPPORT_PROC, GIST_TRANSLATE_CMPTYPE_PROC, GIST_UNION_PROC,
};

// ===========================================================================
// Constants from catalog/pg_type.h
// ===========================================================================

/// `BOOLOID` (pg_type.h).
const BOOLOID: Oid = 16;
/// `INT2OID` (pg_type.h).
const INT2OID: Oid = 21;
/// `INT4OID` (pg_type.h).
const INT4OID: Oid = 23;
/// `OIDOID` (pg_type.h).
const OIDOID: Oid = 26;
/// `FLOAT8OID` (pg_type.h).
const FLOAT8OID: Oid = 701;
/// `ANYOID` (pg_type.h).
const ANYOID: Oid = 2276;
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
// gistvalidate (gistvalidate.c:31)
// ===========================================================================

/// `gistvalidate(opclassoid)` — validator for a GiST opclass.
///
/// The C signature is `bool gistvalidate(Oid)`; here the `elog(ERROR, "cache
/// lookup failed for operator class %u")` path (and any error raised by the
/// catalog substrate, including OOM) travels on the `Err` channel. `mcx` is the
/// translation of the C current context every catalog projection and the work
/// lists are palloc'd in; everything allocated here drops on return (C:
/// `ReleaseCatCacheList` / `ReleaseSysCache` plus context cleanup).
pub fn gistvalidate(mcx: Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
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
        // All GiST support functions should be registered with matching
        // left/right types.
        if procform.amproclefttype != procform.amprocrighttype {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gist contains support function {} with different left and right input types",
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
        let ok;
        match procform.amprocnum as i32 {
            GIST_CONSISTENT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    false,
                    5,
                    5,
                    &[INTERNALOID, opcintype, INT2OID, OIDOID, INTERNALOID],
                )?;
            }
            GIST_UNION_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    opckeytype,
                    false,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?;
            }
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_FETCH_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                )?;
            }
            GIST_PENALTY_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    true,
                    3,
                    3,
                    &[INTERNALOID, INTERNALOID, INTERNALOID],
                )?;
            }
            GIST_PICKSPLIT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    true,
                    2,
                    2,
                    &[INTERNALOID, INTERNALOID],
                )?;
            }
            GIST_EQUAL_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    false,
                    3,
                    3,
                    &[opckeytype, opckeytype, INTERNALOID],
                )?;
            }
            GIST_DISTANCE_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    FLOAT8OID,
                    false,
                    5,
                    5,
                    &[INTERNALOID, opcintype, INT2OID, OIDOID, INTERNALOID],
                )?;
            }
            GIST_OPTIONS_PROC => {
                ok = amvalidate_seams::check_amoptsproc_signature::call(procform.amproc)?;
            }
            GIST_SORTSUPPORT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    VOIDOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                )?;
            }
            GIST_TRANSLATE_CMPTYPE_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INT2OID,
                    true,
                    1,
                    1,
                    &[INT4OID],
                )? && procform.amproclefttype == ANYOID
                    && procform.amprocrighttype == ANYOID;
            }
            _ => {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method gist contains function {} with invalid support number {}",
                    regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gist contains function {} with wrong signature for support number {}",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in &oprlist {
        // TODO: Check that only allowed strategy numbers exist.
        if oprform.amopstrategy < 1 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gist contains operator {} with invalid strategy number {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // GiST supports ORDER BY operators.
        let op_rettype;
        if oprform.amoppurpose != AMOP_SEARCH {
            // ... but must have matching distance proc.
            if !OidIsValid(lsyscache_seams::get_opfamily_proc::call(
                opfamilyoid,
                oprform.amoplefttype,
                oprform.amoplefttype,
                GIST_DISTANCE_PROC as i16,
            )?) {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method gist contains unsupported ORDER BY specification for operator {}",
                    regproc_seams::format_operator::call(mcx, oprform.amopopr)?
                ))?;
                result = false;
            }
            // ... and operator result must match the claimed btree opfamily.
            op_rettype = lsyscache_seams::get_op_rettype::call(oprform.amopopr)?;
            if !amvalidate_seams::opfamily_can_sort_type::call(oprform.amopsortfamily, op_rettype)?
            {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method gist contains incorrect ORDER BY opfamily specification for operator {}",
                    regproc_seams::format_operator::call(mcx, oprform.amopopr)?
                ))?;
                result = false;
            }
        } else {
            // Search operators must always return bool.
            op_rettype = BOOLOID;
        }

        // Check operator signature.
        if !amvalidate_seams::check_amop_signature::call(
            oprform.amopopr,
            op_rettype,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method gist contains operator {} with wrong signature",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions. In C this passes
    // the same CatCLists; here the validator's owned rows are projected to the
    // fields identify_opfamily_groups reads.
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

    let mut opclassgroup: Option<&types_amvalidate::OpFamilyOpFuncGroup> = None;

    for thisgroup in &grouplist {
        // Remember the group exactly matching the test opclass.
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(thisgroup);
        }

        // There is not a lot we can do to check the operator sets, since each
        // GiST opclass is more or less a law unto itself, and some contain only
        // operators that are binary-compatible with the opclass datatype
        // (meaning that empty operator sets can be OK). That case also means
        // that we shouldn't insist on nonempty function sets except for the
        // opclass's own group.
    }

    // Check that the originally-named opclass is complete.
    for i in 1..=GISTNProcs {
        if let Some(group) = opclassgroup {
            if (group.functionset & (1u64 << i)) != 0 {
                continue; // got it
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
            continue; // optional methods
        }
        report_info(format!(
            "operator class \"{opclassname}\" of access method gist is missing support function {i}"
        ))?;
        result = false;
    }

    // ReleaseCatCacheList / ReleaseSysCache: the owned lists drop here.

    Ok(result)
}

// ===========================================================================
// gistadjustmembers (gistvalidate.c:287)
// ===========================================================================

/// `OpFamilyMember` (`amapi.h`), mutated in place by [`gistadjustmembers`].
/// Canonical definition lives in `opclass`; re-exported here so this
/// crate names the same type (no duplicate definition).
pub use ::opclass::OpFamilyMember;

/// `gistadjustmembers(opfamilyoid, opclassoid, operators, functions)` —
/// prechecking function for adding operators/functions to a GiST opfamily.
///
/// Operator members of a GiST opfamily should never have hard dependencies,
/// since their connection to the opfamily depends only on what the support
/// functions think, and that can be altered. For consistency, we make all soft
/// dependencies point to the opfamily, though a soft dependency on the opclass
/// would work as well in the CREATE OPERATOR CLASS case.
///
/// Required support functions should have hard dependencies. Preferably those
/// are just dependencies on the opclass, but if we're in ALTER OPERATOR FAMILY,
/// we leave the dependency pointing at the whole opfamily. (Given that GiST
/// opclasses generally don't share opfamilies, it seems unlikely to be worth
/// working harder.)
///
/// In C the member lists are `List *` of `OpFamilyMember *`; here they are
/// mutable slices. The C uses `opclassoid` only for the comment about the
/// CREATE OPERATOR CLASS case; the body never reads it.
pub fn gistadjustmembers(
    opfamilyoid: Oid,
    _opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    // Operator members: always a soft family dependency.
    for op in operators.iter_mut() {
        op.ref_is_hard = false;
        op.ref_is_family = true;
        op.refobjid = opfamilyoid;
    }

    // Support functions.
    for op in functions.iter_mut() {
        match op.number as i32 {
            GIST_CONSISTENT_PROC | GIST_UNION_PROC | GIST_PENALTY_PROC | GIST_PICKSPLIT_PROC
            | GIST_EQUAL_PROC => {
                // Required support function — hard dependency (left as set by
                // the caller; matches C which leaves ref_is_hard at its default
                // true / only flips the optional procs to soft).
                op.ref_is_hard = true;
            }
            GIST_COMPRESS_PROC | GIST_DECOMPRESS_PROC | GIST_DISTANCE_PROC | GIST_FETCH_PROC
            | GIST_OPTIONS_PROC | GIST_SORTSUPPORT_PROC | GIST_TRANSLATE_CMPTYPE_PROC => {
                // Optional, so force it to be a soft family dependency.
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
            _ => {
                return Err(PgError::error(format!(
                    "support function number {} is invalid for access method gist",
                    op.number
                ))
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
            }
        }
    }

    Ok(())
}

/// This crate owns no inward seams (it is a leaf consumed by the GiST handler /
/// `opclasscmds` `CREATE OPERATOR CLASS` validation path). The empty
/// `init_seams()` mirrors the sibling `backend-access-brin-validate` /
/// `backend-access-nbt-validate` validators and keeps the `seams-init`
/// recurrence guard satisfied.
pub fn init_seams() {}
