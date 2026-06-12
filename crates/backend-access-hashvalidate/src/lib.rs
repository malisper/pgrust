//! `src/backend/access/hash/hashvalidate.c` (PostgreSQL 18.3) — the opclass
//! validator for the hash access method.
//!
//! Two entry points: [`hashvalidate`] (the `amvalidate` callback) and
//! [`hashadjustmembers`] (the `amadjustmembers` callback). Both return
//! `PgResult`, the owned-model carrier for a C `ereport(ERROR)`; every INFO
//! diagnostic goes through the error subsystem's seam and never raises.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{Oid, OidIsValid, InvalidOid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, INFO};
use types_hash::hash::{
    HASHEXTENDED_PROC, HASHOPTIONS_PROC, HASHSTANDARD_PROC, HTEqualStrategyNumber,
    HTMaxStrategyNumber,
};

pub use types_hash::backend_access_hash_hashvalidate::{
    AmopRow, AmprocRow, OpFamilyMember, OpFamilyOpFuncGroup, OpclassForm,
};

use backend_access_index_amvalidate_seams as amvalidate_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_adt_regproc_seams as regproc_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_error_seams as error_seams;

/// `AMOP_SEARCH` (pg_amop.h) — `'s'`.
const AMOP_SEARCH: i8 = b's' as i8;

// Built-in catalog OIDs referenced by the checks.
/// `INT4OID` (pg_type.h).
const INT4OID: Oid = 23;
/// `INT8OID` (pg_type.h).
const INT8OID: Oid = 20;
/// `BOOLOID` (pg_type.h).
const BOOLOID: Oid = 16;
/// `HASH_AM_OID` (pg_am.h) — the hash access method's pg_am row.
const HASH_AM_OID: Oid = 405;

/// `list_member_oid(list, oid)`.
fn list_member_oid(list: &[Oid], oid: Oid) -> bool {
    list.contains(&oid)
}

/// `list_append_unique_oid(list, oid)` — fallible: C's `lappend_oid` pallocs
/// in the list's context.
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
// hashvalidate (hashvalidate.c:40)
// ===========================================================================

/// `hashvalidate(opclassoid)` — validator for a hash opclass.
///
/// Some of the checks done here cover the whole opfamily, and therefore are
/// redundant when checking each opclass in a family. But they don't run long
/// enough to be much of a problem, so we accept the duplication rather than
/// complicate the amvalidate API (as in C).
///
/// The C signature is `bool hashvalidate(Oid)`; here the `elog(ERROR, "cache
/// lookup failed for operator class %u")` path (and any error raised by the
/// catalog substrate, including OOM) travels on the `Err` channel. `mcx` is
/// the translation of the C current context every catalog projection and the
/// work lists are palloc'd in; everything allocated here drops on return
/// (C: `ReleaseCatCacheList` / `ReleaseSysCache` plus context cleanup).
pub fn hashvalidate(mcx: Mcx<'_>, opclassoid: Oid) -> PgResult<bool> {
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

    let mut hashabletypes: PgVec<'_, Oid> = PgVec::new_in(mcx);

    // Check individual support functions.
    for procform in &proclist {
        let ok;

        // All hash functions should be registered with matching left/right
        // types.
        if procform.amproclefttype != procform.amprocrighttype {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash contains support function {} with different left and right input types",
                regproc_seams::format_procedure::call(procform.amproc)?
            ))?;
            result = false;
        }

        // Check procedure numbers and function signatures.
        match procform.amprocnum {
            n if n as u16 == HASHSTANDARD_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INT4OID,
                    true,
                    1,
                    1,
                    &[procform.amproclefttype],
                )?;
            }
            n if n as u16 == HASHEXTENDED_PROC => {
                ok = amvalidate_seams::check_amproc_signature::call(
                    procform.amproc,
                    INT8OID,
                    true,
                    2,
                    2,
                    &[procform.amproclefttype, INT8OID],
                )?;
            }
            n if n as u16 == HASHOPTIONS_PROC => {
                ok = amvalidate_seams::check_amoptsproc_signature::call(procform.amproc)?;
            }
            _ => {
                report_info(format!(
                    "operator family \"{opfamilyname}\" of access method hash contains function {} with invalid support number {}",
                    regproc_seams::format_procedure::call(procform.amproc)?,
                    procform.amprocnum
                ))?;
                result = false;
                continue; // don't want additional message
            }
        }

        if !ok {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash contains function {} with wrong signature for support number {}",
                regproc_seams::format_procedure::call(procform.amproc)?,
                procform.amprocnum
            ))?;
            result = false;
        }

        // Remember which types we can hash.
        if ok
            && (procform.amprocnum as u16 == HASHSTANDARD_PROC
                || procform.amprocnum as u16 == HASHEXTENDED_PROC)
        {
            list_append_unique_oid(&mut hashabletypes, procform.amproclefttype)?;
        }
    }

    // Check individual operators.
    for oprform in &oprlist {
        // Check that only allowed strategy numbers exist.
        if oprform.amopstrategy < 1 || oprform.amopstrategy > HTMaxStrategyNumber as i16 {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash contains operator {} with invalid strategy number {}",
                regproc_seams::format_operator::call(oprform.amopopr)?,
                oprform.amopstrategy
            ))?;
            result = false;
        }

        // hash doesn't support ORDER BY operators.
        if oprform.amoppurpose != AMOP_SEARCH || OidIsValid(oprform.amopsortfamily) {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash contains invalid ORDER BY specification for operator {}",
                regproc_seams::format_operator::call(oprform.amopopr)?
            ))?;
            result = false;
        }

        // Check operator signature --- same for all hash strategies.
        if !amvalidate_seams::check_amop_signature::call(
            oprform.amopopr,
            BOOLOID,
            oprform.amoplefttype,
            oprform.amoprighttype,
        )? {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash contains operator {} with wrong signature",
                regproc_seams::format_operator::call(oprform.amopopr)?
            ))?;
            result = false;
        }

        // There should be relevant hash functions for each datatype.
        if !list_member_oid(&hashabletypes, oprform.amoplefttype)
            || !list_member_oid(&hashabletypes, oprform.amoprighttype)
        {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash lacks support function for operator {}",
                regproc_seams::format_operator::call(oprform.amopopr)?
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

    let mut opclassgroup: Option<&types_amvalidate::OpFamilyOpFuncGroup> = None;
    for thisgroup in &grouplist {
        // Remember the group exactly matching the test opclass.
        if thisgroup.lefttype == opcintype && thisgroup.righttype == opcintype {
            opclassgroup = Some(thisgroup);
        }

        // Complain if there seems to be an incomplete set of operators for
        // this datatype pair (implying that we have a hash function but no
        // operator).
        if thisgroup.operatorset != (1u64 << HTEqualStrategyNumber) {
            report_info(format!(
                "operator family \"{opfamilyname}\" of access method hash is missing operator(s) for types {} and {}",
                format_type_seams::format_type_be::call(thisgroup.lefttype)?,
                format_type_seams::format_type_be::call(thisgroup.righttype)?
            ))?;
            result = false;
        }
    }

    // Check that the originally-named opclass is supported.
    // (if group is there, we already checked it adequately above)
    if opclassgroup.is_none() {
        report_info(format!(
            "operator class \"{opclassname}\" of access method hash is missing operator(s)"
        ))?;
        result = false;
    }

    // Complain if the opfamily doesn't have entries for all possible
    // combinations of its supported datatypes. While missing cross-type
    // operators are not fatal, it seems reasonable to insist that all
    // built-in hash opfamilies be complete.
    if grouplist.len() != hashabletypes.len() * hashabletypes.len() {
        report_info(format!(
            "operator family \"{opfamilyname}\" of access method hash is missing cross-type operator(s)"
        ))?;
        result = false;
    }

    // ReleaseCatCacheList / ReleaseSysCache: the owned lists drop here.

    Ok(result)
}

// ===========================================================================
// hashadjustmembers (hashvalidate.c:262)
// ===========================================================================

/// `hashadjustmembers` — prechecking function for adding operators/functions
/// to a hash opfamily.
///
/// Hash operators and required support functions are always "loose" members of
/// the opfamily if they are cross-type. If they are not cross-type, we prefer
/// to tie them to the appropriate opclass ... but if the user hasn't created
/// one, we can't do that, and must fall back to using the opfamily dependency.
/// (We mustn't force creation of an opclass in such a case, as leaving an
/// incomplete opclass laying about would be bad. Throwing an error is another
/// undesirable alternative.)
///
/// This behavior results in a bit of a dump/reload hazard, in that the order
/// of restoring objects could affect what dependencies we end up with.
/// pg_dump's existing behavior will preserve the dependency choices in most
/// cases, but not if a cross-type operator has been bound tightly into an
/// opclass. That's a mistake anyway, so silently "fixing" it isn't awful.
///
/// Optional support functions are always "loose" family members.
///
/// In C the member lists are `List *` of `OpFamilyMember *`; here they are
/// mutable slices, iterated operators-then-functions in the
/// `list_concat_copy(operators, functions)` order.
pub fn hashadjustmembers(
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
        if op.is_func && op.number as u16 != HASHSTANDARD_PROC {
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
                    HASH_AM_OID,
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
/// seams (the hash AM handler can depend on it directly without a cycle), so
/// there is nothing to `set()`; the hook keeps `seams-init` wiring uniform.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
