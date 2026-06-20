//! The merge-join scan selectivity and hash-bucket statistics estimators of
//! selfuncs.c — `mergejoinscansel`, its stats-range subroutines
//! (`get_variable_range` / `get_stats_slot_range`), and
//! `estimate_hash_bucket_stats`.
//!
//! These are the selfuncs.c functions the cost model
//! (`backend-optimizer-path-costsize`) reaches through the
//! `mergejoinscansel` / `estimate_hash_bucket_stats` seams to cost merge joins
//! and hash joins. Both call [`crate::examine::examine_variable`], which needs
//! the planner-run RTE store and the `&mut PlannerInfo` node arena, so the seams
//! are re-signed to carry `run` / `&mut root` (the cost call sites already have
//! both) and the estimator opens its own per-call `MemoryContext` for the
//! detoasted-stats allocations (the result is a scalar, matching C running these
//! in the planner's context).
//!
//! `get_variable_range` is ported from its stats-only body (the C
//! `get_actual_variable_range` index probe is `#ifdef NOT_USED` there and is
//! *not* reached). The C `datumCopy` of slot values is a plain `Datum` move in
//! this value model: the per-call `MemoryContext` outlives the estimate, so the
//! min/max Datums stay valid for the `scalarineqsel` calls that consume them.

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo};
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_selfuncs::{
    AttStatsSlot, VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES,
};

use backend_utils_cache_lsyscache_seams as lsc;
use backend_utils_fmgr_fmgr_seams as fmgr;

use crate::examine::{examine_variable, release_variable_stats};
use crate::ineq::scalarineqsel;
use crate::scalar::{
    get_variable_numdistinct, statistic_proc_security_check, stats_tuple_stanullfrac,
};
use crate::{clamp_probability, clamp_row_est, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV};

use types_selfuncs::DEFAULT_INEQ_SEL;

/* ---------------------------------------------------------------------------
 * CompareType (access/cmptype.h) and StrategyNumber (access/stratnum.h) the
 * merge-join range setup translates between. BTREE_AM_OID (pg_am.dat) is the
 * fast-path access method whose strategy numbers equal the compare types.
 * ------------------------------------------------------------------------- */

/// `COMPARE_INVALID` (cmptype.h).
const COMPARE_INVALID: i32 = 0;
/// `COMPARE_LT` (cmptype.h) — `BTLessStrategyNumber`.
const COMPARE_LT: i32 = 1;
/// `COMPARE_LE` (cmptype.h) — `BTLessEqualStrategyNumber`.
const COMPARE_LE: i32 = 2;
/// `COMPARE_GE` (cmptype.h) — `BTGreaterEqualStrategyNumber`.
const COMPARE_GE: i32 = 4;
/// `COMPARE_GT` (cmptype.h) — `BTGreaterStrategyNumber`.
const COMPARE_GT: i32 = 5;

/// `BTREE_AM_OID` (pg_am.dat) — the b-tree access method OID.
const BTREE_AM_OID: Oid = 403;

/// `IndexAmTranslateCompareType(cmptype, amoid, opfamily, missing_ok=true)`
/// (access/index/amapi.c) — translate a [`CompareType`](COMPARE_LT) into the
/// access method's `StrategyNumber`. The common case is b-tree, whose strategy
/// numbers equal the compare types (the C fast path); any other access method
/// dispatches to the AM's `amtranslatecmptype` routine, which is unported.
fn index_am_translate_compare_type(cmptype: i32, amoid: Oid, opfamily: Oid) -> i16 {
    // shortcut for common case (b-tree): strategy number == compare type
    if amoid == BTREE_AM_OID && cmptype > COMPARE_INVALID && cmptype <= COMPARE_GT {
        return cmptype as i16;
    }
    // For non-btree AMs C calls GetIndexAmRoutineByAmId(amoid)->amtranslatecmptype,
    // the index AM's compare-type<->strategy callback. That index-AM-routine
    // dispatch is unported; missing_ok = true would return InvalidStrategy, but
    // we cannot faithfully decide that without the routine, so panic loudly.
    let _ = opfamily;
    panic!(
        "selfuncs: IndexAmTranslateCompareType for non-btree access method {amoid} is unported — \
         it dispatches to the AM's amtranslatecmptype routine (GetIndexAmRoutineByAmId), which has \
         no owner; reached from mergejoinscansel for a non-btree merge opfamily"
    )
}

/* ---------------------------------------------------------------------------
 * get_stats_slot_range (selfuncs.c) — scan an AttStatsSlot for min/max values.
 * ------------------------------------------------------------------------- */

/// `get_stats_slot_range(sslot, opfuncoid, opproc, collation, typLen, typByVal,
/// &min, &max, &have_data)` (selfuncs.c) — update `(min, max, have_data)`
/// according to the values in `sslot`, ordering them by `opfuncoid` (a "<"
/// comparison proc). 1:1 with the C body; `opproc` caching is folded into a
/// single up-front resolution since `function_call_invoke_datum` re-resolves by
/// OID.
///
/// The values cross the ordering proc's fmgr boundary as their canonical images
/// (`canon[i]` for a by-reference element type — `name`/`text`/`bytea`/`numeric`
/// — whose bare `sslot->values` offset is non-dereferenceable; the bare word
/// for a pass-by-value element). The resulting `(min, max)` are likewise
/// canonical `DatumV`, so the consuming `scalarineqsel` comparisons hold the
/// real referent bytes for a by-reference column.
#[allow(clippy::too_many_arguments)]
fn get_stats_slot_range<'mcx>(
    mcx: Mcx<'mcx>,
    sslot: &AttStatsSlot<'mcx>,
    canon: Option<&mcx::PgVec<'mcx, DatumV<'mcx>>>,
    opfuncoid: Oid,
    collation: Oid,
    min: &mut Option<DatumV<'mcx>>,
    max: &mut Option<DatumV<'mcx>>,
    have_data: &mut bool,
) -> PgResult<()> {
    let mut tmin = min.clone();
    let mut tmax = max.clone();
    let mut have = *have_data;

    // Scan all the slot's values. The C `datumCopy(tmin/tmax)` write-back is a
    // `clone_in` in this value model (the per-call mcx arena holding the slot
    // values outlives the estimate); tmin/tmax start as `*min`/`*max`, so
    // writing them back unconditionally matches the C `found_t*` guards.
    for i in 0..sslot.values.len() {
        let v = crate::ineq::slot_value_canon(&sslot.values, canon, i, mcx)?;
        if !have {
            tmin = Some(v.clone_in(mcx)?);
            tmax = Some(v);
            *have_data = true;
            have = true;
            continue;
        }

        // opproc(values[i], tmin) — value < tmin ?
        let (lt_min, lt_min_null) = fmgr::function_call_invoke_datum::call(
            mcx,
            opfuncoid,
            collation,
            &[v.clone_in(mcx)?, tmin.as_ref().unwrap().clone_in(mcx)?],
            &[],
            None,
        )?;
        if !lt_min_null && lt_min.as_bool() {
            tmin = Some(v.clone_in(mcx)?);
        }

        // opproc(tmax, values[i]) — tmax < value ?
        let (gt_max, gt_max_null) = fmgr::function_call_invoke_datum::call(
            mcx,
            opfuncoid,
            collation,
            &[tmax.as_ref().unwrap().clone_in(mcx)?, v.clone_in(mcx)?],
            &[],
            None,
        )?;
        if !gt_max_null && gt_max.as_bool() {
            tmax = Some(v);
        }
    }

    *min = tmin;
    *max = tmax;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * get_variable_range (selfuncs.c) — extract the min/max of a variable from
 * its pg_statistic histogram / MCV data.
 * ------------------------------------------------------------------------- */

/// `get_variable_range(root, vardata, sortop, collation, &min, &max)`
/// (selfuncs.c) — find the smallest and largest values of the variable as
/// ordered by `sortop`, returning `Some((min, max))` (C `true`) or `None` (C
/// `false`, no usable stats). The `#ifdef NOT_USED` `get_actual_variable_range`
/// index probe is not compiled in, so this is purely a stats read. 1:1 with the
/// C body.
fn get_variable_range<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData,
    sortop: Oid,
    collation: Oid,
) -> PgResult<Option<(DatumV<'mcx>, DatumV<'mcx>)>> {
    let stats_tuple = match vardata.stats_tuple {
        None => return Ok(None), // no stats available, so default result
        Some(t) => t,
    };

    // If we can't apply the sortop to the stats data, just fail.
    let opfuncoid = lsc::get_opcode::call(sortop)?;
    if !statistic_proc_security_check(vardata, opfuncoid)? {
        return Ok(None);
    }

    let mut tmin: Option<DatumV<'mcx>> = None;
    let mut tmax: Option<DatumV<'mcx>> = None;
    let mut have_data = false;

    // If there is a histogram with the ordering we want, grab the first and
    // last values. The slot values cross as their canonical images (`canon[i]`
    // for a by-reference element type whose bare offset is non-dereferenceable);
    // the C datumCopy is a `clone_in` here (the per-call mcx arena holding
    // sslot.values outlives this estimate).
    if let Some(sslot) = lsc::get_attstatsslot::call(
        mcx,
        stats_tuple,
        STATISTIC_KIND_HISTOGRAM,
        sortop,
        ATTSTATSSLOT_VALUES,
    )? {
        if sslot.stacoll == collation && !sslot.values.is_empty() {
            let canon = crate::ineq::slot_canon_values(
                mcx,
                stats_tuple,
                STATISTIC_KIND_HISTOGRAM,
                sortop,
                sslot.valuetype,
            )?;
            tmin = Some(crate::ineq::slot_value_canon(
                &sslot.values,
                canon.as_ref(),
                0,
                mcx,
            )?);
            tmax = Some(crate::ineq::slot_value_canon(
                &sslot.values,
                canon.as_ref(),
                sslot.values.len() - 1,
                mcx,
            )?);
            have_data = true;
        }
    }

    // Otherwise, if there is a histogram with some other ordering, scan it and
    // get the min and max values according to the ordering we want.
    if !have_data {
        if let Some(sslot) = lsc::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )? {
            let canon = crate::ineq::slot_canon_values(
                mcx,
                stats_tuple,
                STATISTIC_KIND_HISTOGRAM,
                InvalidOid,
                sslot.valuetype,
            )?;
            get_stats_slot_range(
                mcx,
                &sslot,
                canon.as_ref(),
                opfuncoid,
                collation,
                &mut tmin,
                &mut tmax,
                &mut have_data,
            )?;
        }
    }

    // If we have most-common-values info, look for extreme MCVs. This is needed
    // even with a histogram (the histogram excludes the MCVs). If we *only* have
    // MCVs, proceed only if they represent the whole table.
    let mcv_flags = if have_data {
        ATTSTATSSLOT_VALUES
    } else {
        ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS
    };
    if let Some(sslot) =
        lsc::get_attstatsslot::call(mcx, stats_tuple, STATISTIC_KIND_MCV, InvalidOid, mcv_flags)?
    {
        let mut use_mcvs = have_data;
        if !have_data {
            let mut sumcommon = 0.0f64;
            for i in 0..sslot.numbers.len() {
                sumcommon += sslot.numbers[i] as f64;
            }
            let nullfrac = stats_tuple_stanullfrac(stats_tuple) as f64;
            if sumcommon + nullfrac > 0.99999 {
                use_mcvs = true;
            }
        }

        if use_mcvs {
            let canon = crate::ineq::slot_canon_values(
                mcx,
                stats_tuple,
                STATISTIC_KIND_MCV,
                InvalidOid,
                sslot.valuetype,
            )?;
            get_stats_slot_range(
                mcx,
                &sslot,
                canon.as_ref(),
                opfuncoid,
                collation,
                &mut tmin,
                &mut tmax,
                &mut have_data,
            )?;
        }
    }

    if have_data {
        Ok(Some((tmin.unwrap(), tmax.unwrap())))
    } else {
        Ok(None)
    }
}

/* ---------------------------------------------------------------------------
 * mergejoinscansel (selfuncs.c) — scan selectivity of a merge join.
 * ------------------------------------------------------------------------- */

/// The `(leftstart, leftend, rightstart, rightend)` outputs of
/// [`mergejoinscansel`].
type ScanSel = (f64, f64, f64, f64);

/// `mergejoinscansel(root, clause, opfamily, cmptype, nulls_first, &leftstart,
/// &leftend, &rightstart, &rightend)` (selfuncs.c) — estimate the fraction of
/// each merge-join input that will be scanned, before the first match
/// (`*start`) and before the join terminates (`*end`). Returns the four
/// fractions as a tuple. 1:1 with the C body.
pub fn mergejoinscansel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    clause: NodeId,
    opfamily: Oid,
    cmptype: i32,
    nulls_first: bool,
) -> PgResult<ScanSel> {
    // Set default results if we can't figure anything out.
    let mut leftstart = 0.0f64;
    let mut leftend = 1.0f64;
    let mut rightstart = 0.0f64;
    let mut rightend = 1.0f64;

    // Deconstruct the merge clause.
    let clause_expr = root.node(clause).clone();
    let op = match &clause_expr {
        Expr::OpExpr(op) => op,
        // !is_opclause(clause) — "shouldn't happen"; return the defaults.
        _ => return Ok((leftstart, leftend, rightstart, rightend)),
    };
    let opno = op.opno;
    let collation = op.inputcollid;
    let left = match op.args.first() {
        Some(l) => l.clone(),
        None => return Ok((leftstart, leftend, rightstart, rightend)),
    };
    let right = match op.args.get(1) {
        // !right — "shouldn't happen"; return the defaults.
        Some(r) => r.clone(),
        None => return Ok((leftstart, leftend, rightstart, rightend)),
    };

    // Look for stats for the inputs.
    let left_id = root.alloc_node(left);
    let right_id = root.alloc_node(right);
    let leftvar = examine_variable(mcx, run, root, left_id, 0)?;
    let rightvar = examine_variable(mcx, run, root, right_id, 0)?;

    let opmethod = lsc::get_opfamily_method::call(opfamily)?;

    // Extract the operator's declared left/right datatypes.
    let (_op_strategy, op_lefttype, op_righttype) =
        match lsc::get_op_opfamily_properties::call(opno, opfamily, false)? {
            Some(t) => t,
            None => {
                // get_op_opfamily_properties(missing_ok=false) elog(ERROR)s in C;
                // PgResult Err carries that. None can only mean missing_ok=true,
                // which we did not pass — treat as fail for safety.
                release_variable_stats(leftvar);
                release_variable_stats(rightvar);
                return Ok((leftstart, leftend, rightstart, rightend));
            }
        };

    // Look up the various operators we need. If we don't find them all, it
    // probably means the opfamily is broken, but we just fail silently. We
    // expect pg_statistic histograms are sorted by '<' regardless of sort dir.
    let isgt;
    let (lsortop, rsortop, lstatop, rstatop, ltop, leop, revltop, revleop);

    match cmptype {
        x if x == COMPARE_LT => {
            isgt = false;
            let ltstrat = index_am_translate_compare_type(COMPARE_LT, opmethod, opfamily);
            let lestrat = index_am_translate_compare_type(COMPARE_LE, opmethod, opfamily);
            if op_lefttype == op_righttype {
                // easy case
                ltop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, ltstrat)?;
                leop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, lestrat)?;
                lsortop = ltop;
                rsortop = ltop;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = ltop;
                revleop = leop;
            } else {
                ltop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, ltstrat)?;
                leop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, lestrat)?;
                lsortop =
                    lsc::get_opfamily_member::call(opfamily, op_lefttype, op_lefttype, ltstrat)?;
                rsortop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_righttype, ltstrat)?;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_lefttype, ltstrat)?;
                revleop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_lefttype, lestrat)?;
            }
        }
        x if x == COMPARE_GT => {
            // descending-order case
            isgt = true;
            let ltstrat = index_am_translate_compare_type(COMPARE_LT, opmethod, opfamily);
            let gtstrat = index_am_translate_compare_type(COMPARE_GT, opmethod, opfamily);
            let gestrat = index_am_translate_compare_type(COMPARE_GE, opmethod, opfamily);
            if op_lefttype == op_righttype {
                // easy case
                ltop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, gtstrat)?;
                leop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, gestrat)?;
                lsortop = ltop;
                rsortop = ltop;
                lstatop =
                    lsc::get_opfamily_member::call(opfamily, op_lefttype, op_lefttype, ltstrat)?;
                rstatop = lstatop;
                revltop = ltop;
                revleop = leop;
            } else {
                ltop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, gtstrat)?;
                leop = lsc::get_opfamily_member::call(opfamily, op_lefttype, op_righttype, gestrat)?;
                lsortop =
                    lsc::get_opfamily_member::call(opfamily, op_lefttype, op_lefttype, gtstrat)?;
                rsortop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_righttype, gtstrat)?;
                lstatop =
                    lsc::get_opfamily_member::call(opfamily, op_lefttype, op_lefttype, ltstrat)?;
                rstatop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_righttype, ltstrat)?;
                revltop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_lefttype, gtstrat)?;
                revleop =
                    lsc::get_opfamily_member::call(opfamily, op_righttype, op_lefttype, gestrat)?;
            }
        }
        _ => {
            // shouldn't get here — fail (release stats and return defaults).
            release_variable_stats(leftvar);
            release_variable_stats(rightvar);
            return Ok((leftstart, leftend, rightstart, rightend));
        }
    }

    if !OidIsValid(lsortop)
        || !OidIsValid(rsortop)
        || !OidIsValid(lstatop)
        || !OidIsValid(rstatop)
        || !OidIsValid(ltop)
        || !OidIsValid(leop)
        || !OidIsValid(revltop)
        || !OidIsValid(revleop)
    {
        // insufficient info in catalogs
        release_variable_stats(leftvar);
        release_variable_stats(rightvar);
        return Ok((leftstart, leftend, rightstart, rightend));
    }

    // Try to get ranges of both inputs. (`isgt` swaps max/min.)
    let (leftmin, leftmax, rightmin, rightmax);
    if !isgt {
        let (lmin, lmax) = match get_variable_range(mcx, &leftvar, lstatop, collation)? {
            Some(t) => t,
            None => {
                release_variable_stats(leftvar);
                release_variable_stats(rightvar);
                return Ok((leftstart, leftend, rightstart, rightend));
            }
        };
        let (rmin, rmax) = match get_variable_range(mcx, &rightvar, rstatop, collation)? {
            Some(t) => t,
            None => {
                release_variable_stats(leftvar);
                release_variable_stats(rightvar);
                return Ok((leftstart, leftend, rightstart, rightend));
            }
        };
        leftmin = lmin;
        leftmax = lmax;
        rightmin = rmin;
        rightmax = rmax;
    } else {
        // need to swap the max and min
        let (lmax, lmin) = match get_variable_range(mcx, &leftvar, lstatop, collation)? {
            Some(t) => t,
            None => {
                release_variable_stats(leftvar);
                release_variable_stats(rightvar);
                return Ok((leftstart, leftend, rightstart, rightend));
            }
        };
        let (rmax, rmin) = match get_variable_range(mcx, &rightvar, rstatop, collation)? {
            Some(t) => t,
            None => {
                release_variable_stats(leftvar);
                release_variable_stats(rightvar);
                return Ok((leftstart, leftend, rightstart, rightend));
            }
        };
        leftmin = lmin;
        leftmax = lmax;
        rightmin = rmin;
        rightmax = rmax;
    }

    // The fraction of the left variable that will be scanned is the fraction
    // that's <= the right-side maximum value. Only believe non-default
    // estimates.
    // The range endpoints are the canonical value images that `get_variable_range`
    // distilled (the real referent bytes for a by-reference column type), so the
    // consuming `scalarineqsel` comparisons carry them across the fmgr boundary.
    let rightmax_v = rightmax;
    let leftmax_v = leftmax;
    let rightmin_v = rightmin;
    let leftmin_v = leftmin;
    let selec = scalarineqsel(
        mcx, root, leop, isgt, true, collation, &leftvar, &rightmax_v, op_righttype,
    )?;
    if selec != DEFAULT_INEQ_SEL {
        leftend = selec;
    }

    // And similarly for the right variable.
    let selec = scalarineqsel(
        mcx, root, revleop, isgt, true, collation, &rightvar, &leftmax_v, op_lefttype,
    )?;
    if selec != DEFAULT_INEQ_SEL {
        rightend = selec;
    }

    // Only one of the two "end" fractions can really be less than 1.0; believe
    // the smaller estimate and reset the other to 1.0. Equal => believe neither.
    if leftend > rightend {
        leftend = 1.0;
    } else if leftend < rightend {
        rightend = 1.0;
    } else {
        leftend = 1.0;
        rightend = 1.0;
    }

    // The fraction of the left variable scanned before the first join pair is
    // the fraction that's < the right-side minimum value.
    let selec = scalarineqsel(
        mcx, root, ltop, isgt, false, collation, &leftvar, &rightmin_v, op_righttype,
    )?;
    if selec != DEFAULT_INEQ_SEL {
        leftstart = selec;
    }

    // And similarly for the right variable.
    let selec = scalarineqsel(
        mcx, root, revltop, isgt, false, collation, &rightvar, &leftmin_v, op_lefttype,
    )?;
    if selec != DEFAULT_INEQ_SEL {
        rightstart = selec;
    }

    // Only one of the two "start" fractions can really be more than zero;
    // believe the larger estimate and reset the other to 0.0. Equal => neither.
    if leftstart < rightstart {
        leftstart = 0.0;
    } else if leftstart > rightstart {
        rightstart = 0.0;
    } else {
        leftstart = 0.0;
        rightstart = 0.0;
    }

    // If the sort order is nulls-first, skip over any nulls too. These were not
    // counted by scalarineqsel, and can be added regardless of belief. Clamp.
    if nulls_first {
        if let Some(t) = leftvar.stats_tuple {
            let nullfrac = stats_tuple_stanullfrac(t) as f64;
            leftstart += nullfrac;
            leftstart = clamp_probability(leftstart);
            leftend += nullfrac;
            leftend = clamp_probability(leftend);
        }
        if let Some(t) = rightvar.stats_tuple {
            let nullfrac = stats_tuple_stanullfrac(t) as f64;
            rightstart += nullfrac;
            rightstart = clamp_probability(rightstart);
            rightend += nullfrac;
            rightend = clamp_probability(rightend);
        }
    }

    // Disbelieve start >= end, just in case that can happen.
    if leftstart >= leftend {
        leftstart = 0.0;
        leftend = 1.0;
    }
    if rightstart >= rightend {
        rightstart = 0.0;
        rightend = 1.0;
    }

    release_variable_stats(leftvar);
    release_variable_stats(rightvar);

    Ok((leftstart, leftend, rightstart, rightend))
}

/* ---------------------------------------------------------------------------
 * estimate_hash_bucket_stats (selfuncs.c) — MCV freq + bucket-size fraction.
 * ------------------------------------------------------------------------- */

/// `estimate_hash_bucket_stats(root, hashkey, nbuckets, &mcv_freq,
/// &bucketsize_frac)` (selfuncs.c) — estimate the most-common-value frequency
/// and the per-bucket size fraction for a hash join's hash key. Returns
/// `(mcv_freq, bucketsize_frac)`. 1:1 with the C body.
pub fn estimate_hash_bucket_stats<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    hashkey: NodeId,
    nbuckets: f64,
) -> PgResult<(f64, f64)> {
    let vardata = examine_variable(mcx, run, root, hashkey, 0)?;

    // Look up the frequency of the most common value, if available.
    let mut mcv_freq = 0.0f64;

    if let Some(stats_tuple) = vardata.stats_tuple {
        if let Some(sslot) = lsc::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_NUMBERS,
        )? {
            // The first MCV stat is for the most common value.
            if !sslot.numbers.is_empty() {
                mcv_freq = sslot.numbers[0] as f64;
            }
        }
    }

    // Get number of distinct values.
    let (mut ndistinct, isdefault) = get_variable_numdistinct(root, &vardata);

    // If ndistinct isn't real, punt. We normally return 0.1, but if mcv_freq is
    // known to be even higher than that, use it instead.
    if isdefault {
        let bucketsize_frac = mcv_freq.max(0.1);
        release_variable_stats(vardata);
        return Ok((mcv_freq, bucketsize_frac));
    }

    // Get fraction that are null.
    let stanullfrac = match vardata.stats_tuple {
        Some(t) => stats_tuple_stanullfrac(t) as f64,
        None => 0.0,
    };

    // Compute avg freq of all distinct data values in raw relation.
    let avgfreq = (1.0 - stanullfrac) / ndistinct;

    // Adjust ndistinct to account for restriction clauses (assume the data
    // distribution is affected uniformly by the restriction clauses).
    if let Some(relid) = vardata.rel {
        let (rows, tuples) = {
            let rel = root.rel(relid);
            (rel.rows, rel.tuples)
        };
        if tuples > 0.0 {
            ndistinct *= rows / tuples;
            ndistinct = clamp_row_est(ndistinct);
        }
    }

    // Initial estimate of bucketsize fraction is 1/nbuckets as long as the
    // number of buckets is less than the expected number of distinct values;
    // otherwise it is 1/ndistinct.
    let mut estfract = if ndistinct > nbuckets {
        1.0 / nbuckets
    } else {
        1.0 / ndistinct
    };

    // Adjust estimated bucketsize upward to account for skewed distribution.
    if avgfreq > 0.0 && mcv_freq > avgfreq {
        estfract *= mcv_freq / avgfreq;
    }

    // Clamp bucketsize to sane range (a little above zero, since zero isn't a
    // very sane result).
    if estfract < 1.0e-6 {
        estfract = 1.0e-6;
    } else if estfract > 1.0 {
        estfract = 1.0;
    }

    release_variable_stats(vardata);

    Ok((mcv_freq, estfract))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- index_am_translate_compare_type btree fast path ----

    #[test]
    fn translate_compare_type_btree_is_identity() {
        // For the btree AM, the strategy number equals the compare type.
        assert_eq!(
            index_am_translate_compare_type(COMPARE_LT, BTREE_AM_OID, 0),
            COMPARE_LT as i16
        );
        assert_eq!(
            index_am_translate_compare_type(COMPARE_LE, BTREE_AM_OID, 0),
            COMPARE_LE as i16
        );
        assert_eq!(
            index_am_translate_compare_type(COMPARE_GE, BTREE_AM_OID, 0),
            COMPARE_GE as i16
        );
        assert_eq!(
            index_am_translate_compare_type(COMPARE_GT, BTREE_AM_OID, 0),
            COMPARE_GT as i16
        );
    }

    #[test]
    #[should_panic(expected = "non-btree access method")]
    fn translate_compare_type_non_btree_panics() {
        // A non-btree access method dispatches into the unported AM routine.
        let _ = index_am_translate_compare_type(COMPARE_LT, 405 /* hash AM */, 0);
    }

    #[test]
    #[should_panic(expected = "non-btree access method")]
    fn translate_compare_type_btree_out_of_range_panics() {
        // Even for btree, a compare type outside (INVALID, GT] falls through to
        // the AM routine path (e.g. COMPARE_OVERLAP = 7).
        let _ = index_am_translate_compare_type(7, BTREE_AM_OID, 0);
    }
}
