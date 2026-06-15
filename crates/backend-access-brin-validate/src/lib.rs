//! `src/backend/access/brin/brin_validate.c` (PostgreSQL 18.3) — the opclass
//! validator for the BRIN access method.
//!
//! One entry point: [`brinvalidate`] (the `amvalidate` callback). It returns
//! `PgResult`, the owned-model carrier for a C `ereport(ERROR)`; every INFO
//! diagnostic goes through the error subsystem's seam and never raises (in C,
//! `errfinish` returns for `INFO`, the validator records `result = false` and
//! keeps going).
//!
//! The body is pure orchestration over the system catalogs (syscache lookups,
//! `amvalidate.c` signature checks, opfamily-group identification). All of that
//! genuinely-external substrate
//! (syscache/catalog/`amvalidate`/`lsyscache`/`regproc`/`format_type`) is
//! reached through the per-owner function-pointer seams, exactly as the sibling
//! `backend-access-nbt-validate` / `backend-access-hashvalidate` validators do.
//! The control flow, branch order, message text and SQLSTATE are ported 1:1
//! from C.
//!
//! BRIN, unlike btree/hash, has no `amadjustmembers` callback, so this crate
//! only carries `brinvalidate`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_opclass::AMOP_SEARCH;

// The opclass-form / member-row mirror types carried across the syscache seams
// live in `types-hash` (shared by every AM validator); re-export so callers and
// tests can name them through this crate as well.
pub use types_hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};

use backend_access_index_amvalidate_seams as amvalidate_seams;
use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_adt_regproc_seams as regproc_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_error_seams as error_seams;

// ===========================================================================
// Constants from access/brin_internal.h, catalog/pg_type.h
// ===========================================================================

/// `BRIN_PROCNUM_OPCINFO` (`access/brin_internal.h`) — support function 1.
const BRIN_PROCNUM_OPCINFO: i16 = 1;
/// `BRIN_PROCNUM_ADDVALUE` (`access/brin_internal.h`) — support function 2.
const BRIN_PROCNUM_ADDVALUE: i16 = 2;
/// `BRIN_PROCNUM_CONSISTENT` (`access/brin_internal.h`) — support function 3.
const BRIN_PROCNUM_CONSISTENT: i16 = 3;
/// `BRIN_PROCNUM_UNION` (`access/brin_internal.h`) — support function 4.
const BRIN_PROCNUM_UNION: i16 = 4;
/// `BRIN_MANDATORY_NPROCS` (`access/brin_internal.h`) — the count of mandatory
/// support procs (1..=4).
const BRIN_MANDATORY_NPROCS: i16 = 4;
/// `BRIN_PROCNUM_OPTIONS` (`access/brin_internal.h`) — support function 5
/// (optional opclass-options routine).
const BRIN_PROCNUM_OPTIONS: i16 = 5;
/// `BRIN_FIRST_OPTIONAL_PROCNUM` (`access/brin_internal.h`).
const BRIN_FIRST_OPTIONAL_PROCNUM: i16 = 11;
/// `BRIN_LAST_OPTIONAL_PROCNUM` (`access/brin_internal.h`).
const BRIN_LAST_OPTIONAL_PROCNUM: i16 = 15;

/// `BOOLOID` (pg_type.h).
const BOOLOID: Oid = 16;
/// `INT4OID` (pg_type.h).
const INT4OID: Oid = 23;
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
// brinvalidate (brin_validate.c:36)
// ===========================================================================

/// `brinvalidate(opclassoid)` — validator for a BRIN opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family. But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API (as in C).
///
/// The C signature is `bool brinvalidate(Oid)`; here the `elog(ERROR, "cache
/// lookup failed for operator class %u")` path (and any error raised by the
/// catalog substrate, including OOM) travels on the `Err` channel. `mcx` is the
/// translation of the C current context every catalog projection and the work
/// lists are palloc'd in; everything allocated here drops on return (C:
/// `ReleaseCatCacheList` / `ReleaseSysCache` plus context cleanup).
pub fn brinvalidate(mcx: Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
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
    let opclassname = classform.opcname;

    // Fetch opfamily information.
    let opfamilyname = lsyscache_seams::get_opfamily_name::call(mcx, opfamilyoid, false)?
        .expect("get_opfamily_name(missing_ok = false) returned no name");

    // Fetch all operators and support functions of the opfamily.
    let oprlist = syscache_seams::search_amop_list::call(mcx, opfamilyoid)?;
    let proclist = syscache_seams::search_amproc_list::call(mcx, opfamilyoid)?;

    let mut allfuncs: u64 = 0;
    let mut allops: u64 = 0;

    // Check individual support functions.
    for procform in &proclist {
        let ok;

        // Check procedure numbers and function signatures.
        match procform.amprocnum {
            BRIN_PROCNUM_OPCINFO => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INTERNALOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                )?;
            }
            BRIN_PROCNUM_ADDVALUE => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    true,
                    4,
                    4,
                    &[INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID],
                )?;
            }
            BRIN_PROCNUM_CONSISTENT => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    true,
                    3,
                    4,
                    &[INTERNALOID, INTERNALOID, INTERNALOID, INT4OID],
                )?;
            }
            BRIN_PROCNUM_UNION => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    true,
                    3,
                    3,
                    &[INTERNALOID, INTERNALOID, INTERNALOID],
                )?;
            }
            BRIN_PROCNUM_OPTIONS => {
                ok = amvalidate_seams::check_amoptsproc_signature::call(procform.amproc)?;
            }
            _ => {
                // Complain if it's not a valid optional proc number.
                if procform.amprocnum < BRIN_FIRST_OPTIONAL_PROCNUM
                    || procform.amprocnum > BRIN_LAST_OPTIONAL_PROCNUM
                {
                    report_info(format!(
                        "operator family \"{opfamilyname}\" of access method brin contains function {} with invalid support number {}",
                        regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                        procform.amprocnum
                    ))?;
                    result = false;
                    continue; // omit bad proc numbers from allfuncs
                }
                // Can't check signatures of optional procs, so assume OK.
                ok = true;
            }
        }

        if !ok {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method brin contains function {} with wrong signature for support number {}",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }

        // Track all valid procedure numbers seen in opfamily.
        allfuncs |= 1u64 << procform.amprocnum;
    }

    // Check individual operators.
    for oprform in &oprlist {
        // Check that only allowed strategy numbers exist.
        if oprform.amopstrategy < 1 || oprform.amopstrategy > 63 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method brin contains operator {} with invalid strategy number {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        } else {
            // The set of operators supplied varies across BRIN opfamilies. Our
            // plan is to identify all operator strategy numbers used in the
            // opfamily and then complain about datatype combinations that are
            // missing any operator(s). However, consider only numbers that
            // appear in some non-cross-type case, since cross-type operators may
            // have unique strategies. (This is not a great heuristic, in
            // particular an erroneous number used in a cross-type operator will
            // not get noticed; but the core BRIN opfamilies are messy enough to
            // make it necessary.)
            if oprform.amoplefttype == oprform.amoprighttype {
                allops |= 1u64 << oprform.amopstrategy;
            }
        }

        // brin doesn't support ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH || OidIsValid(oprform.amopsortfamily) {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method brin contains invalid ORDER BY specification for operator {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }

        // Check operator signature --- same for all brin strategies.
        if !amvalidate_seams::check_amop_signature::call(
            oprform.amopopr,
            BOOLOID,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method brin contains operator {} with wrong signature",
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

        // Some BRIN opfamilies expect cross-type support functions to exist, and
        // some don't. We don't know exactly which are which, so if we find a
        // cross-type operator for which there are no support functions at all,
        // let it pass. (Don't expect that all operators exist for such
        // cross-type cases, either.)
        if thisgroup.functionset == 0 && thisgroup.lefttype != thisgroup.righttype {
            continue;
        }

        // Else complain if there seems to be an incomplete set of either
        // operators or support functions for this datatype pair.
        if thisgroup.operatorset != allops {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method brin is missing operator(s) for types {} and {}",
                format_type_seams::format_type_be::call(mcx, thisgroup.lefttype)?,
                format_type_seams::format_type_be::call(mcx, thisgroup.righttype)?
            ))?;
            result = false;
        }
        if thisgroup.functionset != allfuncs {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method brin is missing support function(s) for types {} and {}",
                format_type_seams::format_type_be::call(mcx, thisgroup.lefttype)?,
                format_type_seams::format_type_be::call(mcx, thisgroup.righttype)?
            ))?;
            result = false;
        }
    }

    // Check that the originally-named opclass is complete.
    if opclassgroup.is_none() || opclassgroup.unwrap().operatorset != allops {
        report_info(format!(
            "operator class \"{opclassname}\" of access method brin is missing operator(s)"
        ))?;
        result = false;
    }
    for i in 1..=BRIN_MANDATORY_NPROCS {
        if let Some(group) = opclassgroup {
            if (group.functionset & (1i64 << i) as u64) != 0 {
                continue; // got it
            }
        }
        report_info(format!(
            "operator class \"{opclassname}\" of access method brin is missing support function {i}"
        ))?;
        result = false;
    }

    // ReleaseCatCacheList / ReleaseSysCache: the owned lists drop here.

    Ok(result)
}

/// This crate owns no inward seams (it is a leaf consumed by the BRIN handler /
/// `opclasscmds` `CREATE OPERATOR CLASS` validation path). The empty
/// `init_seams()` mirrors the sibling `backend-access-nbt-validate` /
/// `backend-access-hashvalidate` validators and keeps the `seams-init`
/// recurrence guard satisfied.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
