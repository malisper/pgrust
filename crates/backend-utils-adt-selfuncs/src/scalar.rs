//! The scalar equality / ndistinct estimation cores of selfuncs.c —
//! `var_eq_const`, `var_eq_non_const`, `get_variable_numdistinct`, and the
//! `statistic_proc_security_check` helper, plus the `eqsel`/`neqsel` dispatch.
//!
//! These operate on an already-resolved [`VariableStatData`] (filled by
//! [`crate::examine`]). The arithmetic is a 1:1 port of selfuncs.c; the
//! `pg_statistic` field reads (`stadistinct`/`stanullfrac` via `GETSTRUCT`)
//! cross the [`stats_tuple_stadistinct`] / `stats_tuple_stanullfrac` boundary
//! that this crate owns (and that panics until the syscache keystone lands —
//! `examine_simple_variable` cannot yet produce a non-NULL `statsTuple`).

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid};
use types_datum::datum::Datum;
use types_datum::NullableDatum;
use types_error::PgResult;
use types_pathnodes::PlannerInfo;
use types_selfuncs::{
    StatsTuple, VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES,
};

use backend_utils_cache_lsyscache_seams as lsc;
use backend_utils_fmgr_fmgr_seams as fmgr;

use crate::{
    clamp_probability, clamp_row_est, BOOLOID, DEFAULT_NUM_DISTINCT, RTE_VALUES,
    SELF_ITEM_POINTER_ATTRIBUTE_NUMBER, STATISTIC_KIND_MCV, TABLE_OID_ATTRIBUTE_NUMBER,
};

/* ---------------------------------------------------------------------------
 * pg_statistic Form readers (owned by this unit; GETSTRUCT over the syscache
 * tuple). Blocked on the syscache/RTE keystone — examine_simple_variable cannot
 * yet acquire a real statsTuple, so these are never reached with a live tuple.
 * ------------------------------------------------------------------------- */

/// `((Form_pg_statistic) GETSTRUCT(statsTuple))->stanullfrac` (pg_statistic.h):
/// the fraction of NULLs in the column. The `Form_pg_statistic` fixed area is
/// read off the syscache-pinned `pg_statistic` tuple
/// ([`crate::examine::examine_simple_variable`] obtained it via
/// `search_statrelattinh`); the projection is owned by the syscache unit.
pub(crate) fn stats_tuple_stanullfrac(stats_tuple: StatsTuple) -> f32 {
    backend_utils_cache_syscache_seams::pg_statistic_stanullfrac::call(stats_tuple)
}

/// `((Form_pg_statistic) GETSTRUCT(statsTuple))->stadistinct` (pg_statistic.h),
/// used by [`get_variable_numdistinct`]. Read off the syscache-pinned
/// `pg_statistic` tuple via the syscache unit's projection.
pub(crate) fn stats_tuple_stadistinct(stats_tuple: StatsTuple) -> f32 {
    backend_utils_cache_syscache_seams::pg_statistic_stadistinct::call(stats_tuple)
}

/// Seam body for `stats_tuple_stanullfrac`.
pub fn seam_stats_tuple_stanullfrac(stats_tuple: StatsTuple) -> f32 {
    stats_tuple_stanullfrac(stats_tuple)
}

/* ---------------------------------------------------------------------------
 * statistic_proc_security_check (selfuncs.c:6228)
 * ------------------------------------------------------------------------- */

/// `statistic_proc_security_check(vardata, func_oid)` (selfuncs.c) — whether it
/// is safe to apply the support function `func_oid` to this variable's
/// statistics. 1:1 with the C body.
pub(crate) fn statistic_proc_security_check(
    vardata: &VariableStatData,
    func_oid: Oid,
) -> PgResult<bool> {
    if vardata.acl_ok {
        return Ok(true); // have SELECT privs and no securityQuals
    }

    if func_oid == InvalidOid {
        return Ok(false);
    }

    if lsc::get_func_leakproof::call(func_oid)? {
        return Ok(true);
    }

    // C ereports DEBUG2 ("not using statistics because function ... is not
    // leakproof"); a DEBUG2 elog is not a control-flow event, so we elide the
    // log line and just return false, as the C does after the ereport.
    Ok(false)
}

/// Seam body for `statistic_proc_security_check`.
pub fn seam_statistic_proc_security_check(
    vardata: &VariableStatData,
    func_oid: Oid,
) -> PgResult<bool> {
    statistic_proc_security_check(vardata, func_oid)
}

/* ---------------------------------------------------------------------------
 * get_variable_numdistinct (selfuncs.c:6257)
 * ------------------------------------------------------------------------- */

/// `get_variable_numdistinct(vardata, &isdefault)` (selfuncs.c) — estimate the
/// number of distinct values of a variable. Returns `(ndistinct, isdefault)`,
/// where `isdefault` is the C out-parameter (true when the estimate fell back
/// on `DEFAULT_NUM_DISTINCT`). 1:1 with the C body.
pub(crate) fn get_variable_numdistinct(
    root: &PlannerInfo,
    vardata: &VariableStatData,
) -> (f64, bool) {
    let mut isdefault = false;
    let stadistinct: f64;
    let mut stanullfrac = 0.0f64;

    // Determine the stadistinct value to use.
    if let Some(stats_tuple) = vardata.stats_tuple {
        // Use the pg_statistic entry.
        stadistinct = stats_tuple_stadistinct(stats_tuple) as f64;
        stanullfrac = stats_tuple_stanullfrac(stats_tuple) as f64;
    } else if vardata.vartype == BOOLOID {
        // Special-case boolean columns: presumably, two distinct values.
        stadistinct = 2.0;
    } else if vardata
        .rel
        .map(|relid| root.rel(relid).rtekind == RTE_VALUES)
        .unwrap_or(false)
    {
        // A column of a VALUES RTE is assumed unique.
        stadistinct = -1.0; // unique (and all non null)
    } else {
        // We don't keep statistics for system columns, but in some cases we can
        // infer distinctness anyway.
        stadistinct = match var_attno(root, vardata) {
            Some(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER) => -1.0, // unique (and all non null)
            Some(TABLE_OID_ATTRIBUTE_NUMBER) => 1.0,          // only 1 value
            _ => 0.0,                                         // means "unknown"
        };
    }

    // If there is a unique index, DISTINCT or GROUP-BY clause for the variable,
    // assume it is unique no matter what pg_statistic says; however, we'd better
    // still believe the null-fraction statistic.
    let stadistinct = if vardata.isunique {
        -1.0 * (1.0 - stanullfrac)
    } else {
        stadistinct
    };

    // If we had an absolute estimate, use that.
    if stadistinct > 0.0 {
        return (clamp_row_est(stadistinct), isdefault);
    }

    // Otherwise we need to get the relation size; punt if not available.
    let ntuples = match vardata.rel {
        None => {
            isdefault = true;
            return (DEFAULT_NUM_DISTINCT, isdefault);
        }
        Some(relid) => root.rel(relid).tuples,
    };
    if ntuples <= 0.0 {
        isdefault = true;
        return (DEFAULT_NUM_DISTINCT, isdefault);
    }

    // If we had a relative estimate, use that.
    if stadistinct < 0.0 {
        return (clamp_row_est(-stadistinct * ntuples), isdefault);
    }

    // With no data, estimate ndistinct = ntuples if the table is small, else use
    // default.
    if ntuples < DEFAULT_NUM_DISTINCT {
        return (clamp_row_est(ntuples), isdefault);
    }

    isdefault = true;
    (DEFAULT_NUM_DISTINCT, isdefault)
}

/// `((Var *) vardata->var)->varattno` when `vardata->var` is a `Var`, else
/// `None` — the system-column branch of [`get_variable_numdistinct`].
fn var_attno(root: &PlannerInfo, vardata: &VariableStatData) -> Option<i16> {
    root.node(vardata.var).as_var().map(|v| v.varattno)
}

/* ---------------------------------------------------------------------------
 * var_eq_const (selfuncs.c:298)
 * ------------------------------------------------------------------------- */

/// `var_eq_const(vardata, oproid, collation, constval, constisnull, varonleft,
/// negate)` (selfuncs.c) — equality selectivity for the `var = const` case.
/// 1:1 with the C body.
pub(crate) fn var_eq_const<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    vardata: &VariableStatData,
    oproid: Oid,
    collation: Oid,
    constval: &types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    constisnull: bool,
    varonleft: bool,
    negate: bool,
) -> PgResult<f64> {
    // If the constant is NULL, assume operator is strict and return zero.
    if constisnull {
        return Ok(0.0);
    }

    let mut nullfrac = 0.0f64;
    // Grab the nullfrac for use below. Note we allow use of nullfrac regardless
    // of security check.
    if let Some(stats_tuple) = vardata.stats_tuple {
        nullfrac = stats_tuple_stanullfrac(stats_tuple) as f64;
    }

    let mut selec: f64;

    // If we matched the var to a unique index, DISTINCT or GROUP-BY clause,
    // assume there is exactly one match regardless of anything else.
    if vardata.isunique
        && vardata
            .rel
            .map(|relid| root.rel(relid).tuples >= 1.0)
            .unwrap_or(false)
    {
        selec = 1.0 / root.rel(vardata.rel.unwrap()).tuples;
    } else if vardata.stats_tuple.is_some() && {
        // C assigns opfuncoid = get_opcode(oproid) inside the && condition.
        statistic_proc_security_check(vardata, lsc::get_opcode::call(oproid)?)?
    } {
        let opfuncoid = lsc::get_opcode::call(oproid)?;
        let stats_tuple = vardata.stats_tuple.unwrap();

        // C declares an AttStatsSlot `sslot`; get_attstatsslot fills it (and
        // returns true) when an MCV slot exists, else leaves it zeroed (returns
        // false). The `if (match) ... else ...` below runs in BOTH cases, so an
        // absent slot is modeled as an empty (nvalues == nnumbers == 0) slot.
        let mut matched = false;
        let mut match_index = 0usize;
        let slot_opt = lsc::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;
        // (values, numbers) of the MCV slot, empty when no slot is present.
        let empty_v: &[Datum] = &[];
        let empty_n: &[f32] = &[];
        let mcv_values: &[Datum] = slot_opt.as_ref().map(|s| s.values.as_slice()).unwrap_or(empty_v);
        let mcv_numbers: &[f32] = slot_opt.as_ref().map(|s| s.numbers.as_slice()).unwrap_or(empty_n);

        // The constant crosses the operator's `function_call_invoke` boundary as
        // the bare ABI word. A by-value scalar IS its word; a by-reference value
        // (text/name/varchar/numeric) compared against ACTUAL MCV slot values is
        // the selfuncs by-reference value-carrier follow-on (WALL 1ai): the MCV
        // slot values (`mcv_values`) are themselves bare pointer words from the
        // C-shaped `pg_statistic` tuple, so comparing a `ByRef` const against them
        // needs the canonical by-reference fmgr lane threaded through
        // `get_attstatsslot` — out of this lane's scope. The extraction is
        // deferred to actual use: on a fresh cluster the MCV slot is EMPTY
        // (no `ANALYZE`), so the loop body never runs and a by-reference constant
        // (e.g. `WHERE relname = 'pg_type'`) flows through to the no-MCV-match
        // branch below without ever needing the bare word.
        let constval_word = |c: &types_tuple::backend_access_common_heaptuple::Datum<'_>| -> Datum {
            match c {
                types_tuple::backend_access_common_heaptuple::Datum::ByVal(w) => {
                    Datum::from_usize(*w)
                }
                _ => panic!(
                    "var_eq_const: by-reference constant compared against MCV slot \
                     values requires the selfuncs by-reference value carrier (WALL 1ai)"
                ),
            }
        };
        // Is the constant "=" to any of the column's most common values?
        for i in 0..mcv_values.len() {
            // be careful to apply operator right way 'round
            let (arg0, arg1) = if varonleft {
                (mcv_values[i], constval_word(constval))
            } else {
                (constval_word(constval), mcv_values[i])
            };
            let (fresult, isnull) = fmgr::function_call_invoke::call(
                opfuncoid,
                collation,
                &[NullableDatum::value(arg0), NullableDatum::value(arg1)],
            )?;
            if !isnull && fresult.as_bool() {
                matched = true;
                match_index = i;
                break;
            }
        }

        if matched {
            // Constant is "=" to this common value.
            selec = mcv_numbers[match_index] as f64;
        } else {
            // Comparison is against a constant that is neither NULL nor any of
            // the common values. Its selectivity cannot be more than this:
            let mut sumcommon = 0.0f64;
            for &n in mcv_numbers.iter() {
                sumcommon += n as f64;
            }
            selec = 1.0 - sumcommon - nullfrac;
            selec = clamp_probability(selec);

            // and in fact it's probably a good deal less.
            let (numdistinct, _isdefault) = get_variable_numdistinct(root, vardata);
            let otherdistinct = numdistinct - mcv_numbers.len() as f64;
            if otherdistinct > 1.0 {
                selec /= otherdistinct;
            }

            // Another cross-check: selectivity shouldn't be more than the least
            // common "most common value".
            let nnumbers = mcv_numbers.len();
            if nnumbers > 0 && selec > mcv_numbers[nnumbers - 1] as f64 {
                selec = mcv_numbers[nnumbers - 1] as f64;
            }
        }
    } else {
        // No ANALYZE stats available, so make a guess using estimated number of
        // distinct values and assuming they are equally common.
        let (numdistinct, _isdefault) = get_variable_numdistinct(root, vardata);
        selec = 1.0 / numdistinct;
    }

    // now adjust if we wanted <> rather than =
    if negate {
        selec = 1.0 - selec - nullfrac;
    }

    // result should be in range, but make sure...
    Ok(clamp_probability(selec))
}

/* ---------------------------------------------------------------------------
 * var_eq_non_const (selfuncs.c:473)
 * ------------------------------------------------------------------------- */

/// `var_eq_non_const(vardata, oproid, collation, other, varonleft, negate)`
/// (selfuncs.c) — equality selectivity for the `var = not-a-Const` case. 1:1
/// with the C body. (`oproid`/`collation`/`varonleft` are part of the C
/// signature but not read in this path; kept to preserve the signature.)
#[allow(unused_variables)]
pub(crate) fn var_eq_non_const<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    vardata: &VariableStatData,
    oproid: Oid,
    collation: Oid,
    varonleft: bool,
    negate: bool,
) -> PgResult<f64> {
    let mut nullfrac = 0.0f64;
    if let Some(stats_tuple) = vardata.stats_tuple {
        nullfrac = stats_tuple_stanullfrac(stats_tuple) as f64;
    }

    let mut selec: f64;

    if vardata.isunique
        && vardata
            .rel
            .map(|relid| root.rel(relid).tuples >= 1.0)
            .unwrap_or(false)
    {
        selec = 1.0 / root.rel(vardata.rel.unwrap()).tuples;
    } else if let Some(stats_tuple) = vardata.stats_tuple {
        // Search is for a value we do not know a priori but assume non-NULL.
        selec = 1.0 - nullfrac;
        let (ndistinct, _isdefault) = get_variable_numdistinct(root, vardata);
        if ndistinct > 1.0 {
            selec /= ndistinct;
        }

        // Cross-check: selectivity should never be more than the most common
        // value's.
        if let Some(sslot) = lsc::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_NUMBERS,
        )? {
            if !sslot.numbers.is_empty() && selec > sslot.numbers[0] as f64 {
                selec = sslot.numbers[0] as f64;
            }
        }
    } else {
        let (ndistinct, _isdefault) = get_variable_numdistinct(root, vardata);
        selec = 1.0 / ndistinct;
    }

    if negate {
        selec = 1.0 - selec - nullfrac;
    }

    Ok(clamp_probability(selec))
}
