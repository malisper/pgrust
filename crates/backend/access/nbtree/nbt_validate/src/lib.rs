//! `src/backend/access/nbtree/nbtvalidate.c` (PostgreSQL 18.3) — the opclass
//! validator for the btree access method.
//!
//! Two entry points: [`btvalidate`] (the `amvalidate` callback) and
//! [`btadjustmembers`] (the `amadjustmembers` callback). Both return
//! `PgResult`, the owned-model carrier for a C `ereport(ERROR)`; every INFO
//! diagnostic goes through the error subsystem's seam and never raises.
//!
//! The body of both functions is pure orchestration over the system catalogs
//! (syscache lookups, `amvalidate.c` signature checks, opfamily-group
//! identification) and the dependency-recording helpers used during `CREATE
//! OPERATOR CLASS`. All of that genuinely-external substrate
//! (syscache/catalog/`amvalidate`/`lsyscache`/`regproc`/`format_type`) is
//! reached through the per-owner function-pointer seams. The control flow,
//! branch order, message text and SQLSTATE are ported 1:1 from C.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_nbtree::{BTMaxStrategyNumber, BTORDER_PROC, BTOPTIONS_PROC};

// The opclass-form / member-row mirror types carried across the syscache seams
// live in `types-hash` (shared by every AM validator); re-export so callers and
// tests can name them through this crate as well.
pub use hash::backend_access_hash_hashvalidate::{OpclassForm, AmopRow, AmprocRow};

use amvalidate_seams as amvalidate_seams;
use transam_xact_seams as xact_seams;
use format_type_seams as format_type_seams;
use regproc_seams as regproc_seams;
use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use error_seams as error_seams;

// ===========================================================================
// Constants from access/nbtree.h, access/stratnum.h, catalog/pg_am.h,
// catalog/pg_amop.h, catalog/pg_type.h
// ===========================================================================

/// `BTSORTSUPPORT_PROC` (`access/nbtree.h`) — support function 2, the
/// sortsupport routine.
const BTSORTSUPPORT_PROC: i16 = 2;
/// `BTINRANGE_PROC` (`access/nbtree.h`) — support function 3, the in_range
/// window-frame routine.
const BTINRANGE_PROC: i16 = 3;
/// `BTEQUALIMAGE_PROC` (`access/nbtree.h`) — support function 4, the
/// "equalimage" routine.
const BTEQUALIMAGE_PROC: i16 = 4;
/// `BTSKIPSUPPORT_PROC` (`access/nbtree.h`) — support function 6, the skip
/// support routine.
const BTSKIPSUPPORT_PROC: i16 = 6;

/// `BTLessStrategyNumber` (`access/stratnum.h`).
const BTLessStrategyNumber: u16 = 1;
/// `BTLessEqualStrategyNumber` (`access/stratnum.h`).
const BTLessEqualStrategyNumber: u16 = 2;
/// `BTEqualStrategyNumber` (`access/stratnum.h`).
const BTEqualStrategyNumber: u16 = 3;
/// `BTGreaterEqualStrategyNumber` (`access/stratnum.h`).
const BTGreaterEqualStrategyNumber: u16 = 4;
/// `BTGreaterStrategyNumber` (`access/stratnum.h`).
const BTGreaterStrategyNumber: u16 = 5;

/// `AMOP_SEARCH` (pg_amop.h) — `'s'`.
const AMOP_SEARCH: i8 = b's' as i8;

/// `BTREE_AM_OID` (pg_am.h) — the btree access method's pg_am row.
const BTREE_AM_OID: Oid = 403;

// Built-in catalog OIDs referenced by the checks (pg_type.h).
/// `INT4OID`.
const INT4OID: Oid = 23;
/// `BOOLOID`.
const BOOLOID: Oid = 16;
/// `VOIDOID`.
const VOIDOID: Oid = 2278;
/// `OIDOID`.
const OIDOID: Oid = 26;
/// `INTERNALOID`.
const INTERNALOID: Oid = 2281;

/// `OpFamilyMember` (`amapi.h`), mutated in place by [`btadjustmembers`].
/// Canonical definition lives in `opclass`; re-exported here so this
/// crate names the same type (no duplicate definition).
pub use opclass::OpFamilyMember;

/// `list_append_unique_oid(list, oid)` — append `oid` if not already present.
/// Fallible: C's `lappend_oid` pallocs in the list's context.
fn list_append_unique_oid(list: &mut PgVec<'_, Oid>, oid: Oid) -> PgResult<()> {
    if !list.contains(&oid) {
        let mcx = *list.allocator();
        list.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
        list.push(oid);
    }
    Ok(())
}

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
// btvalidate (nbtvalidate.c:39)
// ===========================================================================

/// `btvalidate(opclassoid)` — validator for a btree opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family. But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API (as in C).
///
/// The C signature is `bool btvalidate(Oid)`; here the `elog(ERROR, "cache
/// lookup failed for operator class %u")` path (and any error raised by the
/// catalog substrate, including OOM) travels on the `Err` channel. `mcx` is
/// the translation of the C current context every catalog projection and the
/// work lists are palloc'd in; everything allocated here drops on return
/// (C: `ReleaseCatCacheList` / `ReleaseSysCache` plus context cleanup).
pub fn btvalidate(mcx: Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
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

    // Check individual support functions.
    for procform in &proclist {
        let ok;

        // Check procedure numbers and function signatures.
        match procform.amprocnum {
            BTORDER_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INT4OID,
                    true,
                    2,
                    2,
                    &[procform.amproclefttype, procform.amprocrighttype],
                )?;
            }
            BTSORTSUPPORT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    VOIDOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                )?;
            }
            BTINRANGE_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
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
                )?;
            }
            BTEQUALIMAGE_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    BOOLOID,
                    true,
                    1,
                    1,
                    &[OIDOID],
                )?;
            }
            n if n as u16 == BTOPTIONS_PROC => {
                ok = amvalidate_seams::check_amoptsproc_signature::call(procform.amproc)?;
            }
            BTSKIPSUPPORT_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    VOIDOID,
                    true,
                    1,
                    1,
                    &[INTERNALOID],
                )?;
            }
            _ => {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method btree contains function {} with invalid support number {}",
                    regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method btree contains function {} with wrong signature for support number {}",
                regproc_seams::format_procedure::call(mcx, procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }
    }

    // Check individual operators.
    for oprform in &oprlist {
        // Check that only allowed strategy numbers exist.
        if oprform.amopstrategy < 1 || oprform.amopstrategy > BTMaxStrategyNumber as i16 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method btree contains operator {} with invalid strategy number {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // btree doesn't support ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH || OidIsValid(oprform.amopsortfamily) {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method btree contains invalid ORDER BY specification for operator {}",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }

        // Check operator signature --- same for all btree strategies.
        if !amvalidate_seams::check_amop_signature::call(
            oprform.amopopr,
            BOOLOID,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method btree contains operator {} with wrong signature",
                regproc_seams::format_operator::call(mcx, oprform.amopopr)?
            ))?;
            result = false;
        }
    }

    // Now check for inconsistent groups of operators/functions. In C this
    // passes the same CatCLists; here the validator's owned rows are projected
    // to the fields identify_opfamily_groups reads.
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

    let mut usefulgroups: usize = 0;
    let mut opclassgroup: Option<&types_amvalidate::OpFamilyOpFuncGroup> = None;
    // list_append_unique_oid over lefttype/righttype.
    let mut familytypes: PgVec<'_, Oid> = PgVec::new_in(mcx);

    for thisgroup in &grouplist {
        // It is possible for an in_range support function to have a RHS type
        // that is otherwise irrelevant to the opfamily --- for instance, SQL
        // requires the datetime_ops opclass to have range support with an
        // interval offset. So, if this group appears to contain only an
        // in_range function, ignore it: it doesn't represent a pair of
        // supported types.
        if thisgroup.operatorset == 0 && thisgroup.functionset == (1u64 << BTINRANGE_PROC) {
            continue;
        }

        // Else count it as a relevant group.
        usefulgroups += 1;

        // Remember the group exactly matching the test opclass.
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(thisgroup);
        }

        // Identify all distinct data types handled in this opfamily. This
        // implementation is O(N^2), but there aren't likely to be enough types
        // in the family for it to matter.
        list_append_unique_oid(&mut familytypes, thisgroup.lefttype)?;
        list_append_unique_oid(&mut familytypes, thisgroup.righttype)?;

        // Complain if there seems to be an incomplete set of either operators
        // or support functions for this datatype pair. The sortsupport,
        // in_range, and equalimage functions are considered optional.
        if thisgroup.operatorset
            != ((1u64 << BTLessStrategyNumber)
                | (1u64 << BTLessEqualStrategyNumber)
                | (1u64 << BTEqualStrategyNumber)
                | (1u64 << BTGreaterEqualStrategyNumber)
                | (1u64 << BTGreaterStrategyNumber))
        {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method btree is missing operator(s) for types {} and {}",
                format_type_seams::format_type_be::call(mcx, thisgroup.lefttype)?,
                format_type_seams::format_type_be::call(mcx, thisgroup.righttype)?
            ))?;
            result = false;
        }
        if (thisgroup.functionset & (1u64 << BTORDER_PROC)) == 0 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method btree is missing support function for types {} and {}",
                format_type_seams::format_type_be::call(mcx, thisgroup.lefttype)?,
                format_type_seams::format_type_be::call(mcx, thisgroup.righttype)?
            ))?;
            result = false;
        }
    }

    // Check that the originally-named opclass is supported.
    // (if group is there, we already checked it adequately above)
    if opclassgroup.is_none() {
        report_info(format!(
            "operator class \"{opclassname}\" of access method btree is missing operator(s)"
        ))?;
        result = false;
    }

    // Complain if the opfamily doesn't have entries for all possible
    // combinations of its supported datatypes. While missing cross-type
    // operators are not fatal, they do limit the planner's ability to derive
    // additional qual clauses from equivalence classes, so it seems reasonable
    // to insist that all built-in btree opfamilies be complete.
    if usefulgroups != (familytypes.len() * familytypes.len()) {
        report_info(format!(
            "operator family \"{opfamilyname}\" of access method btree is missing cross-type operator(s)"
        ))?;
        result = false;
    }

    // ReleaseCatCacheList / ReleaseSysCache: the owned lists drop here.

    Ok(result)
}

// ===========================================================================
// btadjustmembers (nbtvalidate.c:287)
// ===========================================================================

/// `btadjustmembers` — prechecking function for adding operators/functions to a
/// btree opfamily.
///
/// Btree operators and comparison support functions are always "loose" members
/// of the opfamily if they are cross-type. If they are not cross-type, we
/// prefer to tie them to the appropriate opclass ... but if the user hasn't
/// created one, we can't do that, and must fall back to using the opfamily
/// dependency. (We mustn't force creation of an opclass in such a case, as
/// leaving an incomplete opclass laying about would be bad. Throwing an error
/// is another undesirable alternative.)
///
/// Optional support functions are always "loose" family members.
///
/// In C the member lists are `List *` of `OpFamilyMember *`; here they are
/// mutable slices, iterated operators-then-functions in the
/// `list_concat_copy(operators, functions)` order.
pub fn btadjustmembers(
    opfamilyoid: Oid,
    mut opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    // To avoid repeated lookups, we remember the most recently used opclass's
    // input type.
    let mut opcintype;
    if OidIsValid(opclassoid) {
        // During CREATE OPERATOR CLASS, need CCI to see the pg_opclass row.
        xact_seams::command_counter_increment::call()?;
        opcintype = lsyscache_seams::get_opclass_input_type::call(opclassoid)?;
    } else {
        opcintype = InvalidOid;
    }

    // We handle operators and support functions almost identically, so rather
    // than duplicate this code block, just join the lists.
    for op in operators.iter_mut().chain(functions.iter_mut()) {
        if op.is_func && op.number != BTORDER_PROC as i32 {
            // Optional support proc, so always a soft family dependency.
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else if op.lefttype != op.righttype {
            // Cross-type, so always a soft family dependency.
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else {
            // Not cross-type; is there a suitable opclass?
            if op.lefttype != opcintype {
                // Avoid repeating this expensive lookup, even if it fails.
                opcintype = op.lefttype;
                opclassoid = amvalidate_seams::opclass_for_family_datatype::call(
                    BTREE_AM_OID,
                    opfamilyoid,
                    opcintype,
                )?;
            }
            if OidIsValid(opclassoid) {
                // Hard dependency on opclass.
                op.ref_is_hard = true;
                op.ref_is_family = false;
                op.refobjid = opclassoid;
            } else {
                // We're stuck, so make a soft dependency on the opfamily.
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's seam implementations. This crate declares no inward
/// seams: the btree AM handler sets `amvalidate: None` and reaches `btvalidate`
/// by name (it is a soft-error validator, not the raw `fn(Oid) -> bool` ABI
/// pointer), so nothing depends on this crate across a cycle. There is nothing
/// to `set()`; the hook keeps `seams-init` wiring uniform.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
