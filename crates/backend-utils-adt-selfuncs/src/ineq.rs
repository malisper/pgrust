//! The inequality / histogram / MCV selectivity cores of selfuncs.c —
//! `mcv_selectivity`, `histogram_selectivity`, `ineq_histogram_selectivity`,
//! and `scalarineqsel`.
//!
//! All operate over a resolved [`VariableStatData`]. The MCV/histogram scans
//! invoke the operator's comparison proc by hand (`function_call_invoke`, which
//! reports the callee's `isnull` so a NULL result does not abort). The two
//! deep statics `ineq_histogram_selectivity` relies on — `get_actual_variable_range`
//! (an index endpoint probe) and `convert_to_scalar` (per-type scalar
//! conversion) — are private to selfuncs.c and depend on unported index-AM /
//! type-dispatch machinery; they are kept structurally and panic when reached.

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid};
use types_datum::datum::Datum;
use types_datum::NullableDatum;
use types_error::PgResult;
use types_pathnodes::PlannerInfo;
use types_selfuncs::{VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES};

use backend_utils_cache_lsyscache_seams as lsc;
use backend_utils_fmgr_fmgr_seams as fmgr;

use crate::scalar::{get_variable_numdistinct, statistic_proc_security_check};
use crate::{
    clamp_probability, SELF_ITEM_POINTER_ATTRIBUTE_NUMBER, STATISTIC_KIND_HISTOGRAM,
    STATISTIC_KIND_MCV,
};
use types_selfuncs::DEFAULT_INEQ_SEL;

/* ---------------------------------------------------------------------------
 * mcv_selectivity (selfuncs.c:739) — INSTALLED seam.
 * ------------------------------------------------------------------------- */

/// `mcv_selectivity(vardata, opproc, collation, constval, varOnLeft,
/// &sumcommon)` (selfuncs.c) — fraction of the MCV population satisfying
/// `VAR OP CONST` (or `CONST OP VAR`), and the total MCV fraction. Returns
/// `(mcv_selec, sumcommon)`. The C `FmgrInfo *opproc` crosses as `opproc_oid`
/// (the owner re-resolves). 1:1 with the C body.
pub(crate) fn mcv_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData,
    opproc_oid: Oid,
    collation: Oid,
    constval: Datum,
    var_on_left: bool,
) -> PgResult<(f64, f64)> {
    let mut mcv_selec = 0.0f64;
    let mut sumcommon = 0.0f64;

    if let Some(stats_tuple) = vardata.stats_tuple {
        if statistic_proc_security_check(vardata, opproc_oid)? {
            if let Some(sslot) = lsc::get_attstatsslot::call(
                mcx,
                stats_tuple,
                STATISTIC_KIND_MCV,
                InvalidOid,
                ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
            )? {
                for i in 0..sslot.values.len() {
                    // be careful to apply operator right way 'round
                    let (arg0, arg1) = if var_on_left {
                        (sslot.values[i], constval)
                    } else {
                        (constval, sslot.values[i])
                    };
                    let (fresult, isnull) = fmgr::function_call_invoke::call(
                        opproc_oid,
                        collation,
                        &[NullableDatum::value(arg0), NullableDatum::value(arg1)],
                    )?;
                    if !isnull && fresult.as_bool() {
                        mcv_selec += sslot.numbers[i] as f64;
                    }
                    sumcommon += sslot.numbers[i] as f64;
                }
            }
        }
    }

    Ok((mcv_selec, sumcommon))
}

/// Seam body for `mcv_selectivity`.
pub fn seam_mcv_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData,
    opproc_oid: Oid,
    collation: Oid,
    constval: Datum,
    var_on_left: bool,
) -> PgResult<(f64, f64)> {
    mcv_selectivity(mcx, vardata, opproc_oid, collation, constval, var_on_left)
}

/* ---------------------------------------------------------------------------
 * histogram_selectivity (selfuncs.c:830)
 * ------------------------------------------------------------------------- */

/// `histogram_selectivity(vardata, opproc, collation, constval, varOnLeft,
/// min_hist_size, n_skip, &hist_size)` (selfuncs.c) — the fraction of the
/// variable's histogram entries satisfying the predicate. Returns
/// `(result, hist_size)`; `result` is `-1.0` when there is no histogram or it
/// is smaller than `min_hist_size`. 1:1 with the C body.
pub(crate) fn histogram_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData,
    opproc_oid: Oid,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
    min_hist_size: i32,
    n_skip: i32,
) -> PgResult<(f64, i32)> {
    debug_assert!(n_skip >= 0);
    debug_assert!(min_hist_size > 2 * n_skip);

    let mut hist_size = 0i32;
    let mut result = -1.0f64;

    if let Some(stats_tuple) = vardata.stats_tuple {
        if statistic_proc_security_check(vardata, opproc_oid)? {
            if let Some(sslot) = lsc::get_attstatsslot::call(
                mcx,
                stats_tuple,
                STATISTIC_KIND_HISTOGRAM,
                InvalidOid,
                ATTSTATSSLOT_VALUES,
            )? {
                let nvalues = sslot.values.len() as i32;
                hist_size = nvalues;
                if nvalues >= min_hist_size {
                    let mut nmatch = 0i32;
                    let mut i = n_skip;
                    while i < nvalues - n_skip {
                        let (arg0, arg1) = if varonleft {
                            (sslot.values[i as usize], constval)
                        } else {
                            (constval, sslot.values[i as usize])
                        };
                        let (fresult, isnull) = fmgr::function_call_invoke::call(
                            opproc_oid,
                            collation,
                            &[NullableDatum::value(arg0), NullableDatum::value(arg1)],
                        )?;
                        if !isnull && fresult.as_bool() {
                            nmatch += 1;
                        }
                        i += 1;
                    }
                    result = (nmatch as f64) / ((nvalues - 2 * n_skip) as f64);
                } else {
                    result = -1.0;
                }
            }
        }
    }

    Ok((result, hist_size))
}

/* ---------------------------------------------------------------------------
 * convert_to_scalar / get_actual_variable_range — private statics of
 * selfuncs.c that ineq_histogram_selectivity drives.
 *
 * `convert_to_scalar` (the per-type scalar-conversion dispatch) is now ported
 * faithfully in [`crate::convert`] — its by-value legs are computed in full and
 * its by-reference legs panic with a precise blocker (the bare-word `Datum`
 * model cannot dereference a histogram-bin value of a pass-by-reference type;
 * see that module's header).
 *
 * `get_actual_variable_range` (the index endpoint probe) stays structural and
 * panics when reached; the precise blocker is documented below.
 * ------------------------------------------------------------------------- */

/// `get_actual_variable_range(root, vardata, sortop, collation, &min, &max)`
/// (selfuncs.c) — try to replace a histogram endpoint with the column's true
/// current min/max via an index endpoint probe.
///
/// BLOCKED (documented STOP; this lane owns `backend-utils-adt-selfuncs` only).
/// `get_actual_variable_endpoint` (selfuncs.c:6770) deliberately bypasses the
/// executor's plan-state layer: it sets up a transient `AllocSetContext`, opens
/// the table + index with `NoLock`, makes a *bare* `table_slot_create` slot
/// (no `EState`), builds an `IS NOT NULL` `ScanKeyData[1]`, and then runs
/// `index_beginscan` + `index_rescan(scan, scankeys, 1, NULL, 0)` +
/// `index_getnext_tid` + (`VM_ALL_VISIBLE` / `index_fetch_heap`) +
/// `index_deform_tuple(xs_itup, ...)` directly on the raw `IndexScanDesc`, under
/// an `InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(heap))`
/// transient snapshot. Two sub-dependencies are genuinely unavailable from this
/// crate and live in other owners:
///
/// 1. There is NO raw `index_rescan(scan, ScanKey[], nkeys, orderbys, n)` seam.
///    The `backend-access-index-indexam-seams` owner exposes `index_rescan`
///    only over a full executor `IndexOnlyScanState` / `IndexScanState` /
///    `BitmapIndexScanState` node (it reads the node's `*_ScanKeys` arrays and
///    `*_ScanDesc`). `get_actual_variable_endpoint` has no plan-state node — it
///    drives a bare scan descriptor with a locally-built scankey array — so the
///    node-shaped seams cannot express it.
///
/// 2. `index_fetch_heap` / `index_getnext_slot` require an `EStateData` + a
///    `SlotId` from the executor tuple-table pool, but the endpoint probe runs
///    *without* an executor (its slot is a standalone `table_slot_create` slot,
///    its memory a private transient context). There is no executor-free
///    heap-fetch-into-bare-slot form of these seams.
///
/// Lifting this requires the indexam-seams owner to add a raw-scankey
/// `index_rescan` form and an executor-free `index_fetch_heap` (taking a
/// standalone slot), plus the transient `SnapshotNonVacuumable` setup wired
/// through — all out of this crate's lane. (Substrate that DOES exist:
/// `IndexOptInfo` carries `sortopfamily`/`indpred`/`hypothetical`/`canreturn`/
/// `indexcollations`/`reverse_sort`/`relam`; `index_beginscan`,
/// `index_getnext_tid`, the `xs_want_itup`/`xs_itup`/`xs_itupdesc`/`xs_recheck`
/// scan-desc fields, and `GlobalVisTestFor` are all present.)
///
/// Separately, this code path is not even reached in the current single-user
/// boot: with `search_statrelattinh` uninstalled, `examine_simple_variable`
/// cannot pin a `pg_statistic` `statsTuple` for a relation column, so
/// `ineq_histogram_selectivity` takes the stats-absent default-estimate path
/// and never calls `get_actual_variable_range`.
fn get_actual_variable_range(
    _root: &PlannerInfo,
    _vardata: &VariableStatData,
    _sortop: Oid,
    _collation: Oid,
    _min: Option<&mut Datum>,
    _max: Option<&mut Datum>,
) -> bool {
    panic!(
        "selfuncs: get_actual_variable_range is blocked — the index endpoint probe \
         (get_actual_variable_endpoint) needs a raw-scankey index_rescan(scan, ScanKey[], ...) \
         form and an executor-free index_fetch_heap over a bare table_slot_create slot under a \
         transient SnapshotNonVacuumable, none of which the node-shaped indexam-seams expose \
         (out-of-lane: backend-access-index-indexam owner)"
    )
}

/// `convert_to_scalar(...)` (selfuncs.c) — the faithful per-type
/// scalar-conversion dispatch, ported in [`crate::convert`].
use crate::convert::convert_to_scalar;

/* ---------------------------------------------------------------------------
 * ineq_histogram_selectivity (selfuncs.c:1048)
 * ------------------------------------------------------------------------- */

/// `ineq_histogram_selectivity(root, vardata, opoid, opproc, isgt, iseq,
/// collation, constval, consttype)` (selfuncs.c) — selectivity of an
/// inequality from the histogram. Returns the histogram selectivity, or `-1.0`
/// if no usable histogram. 1:1 with the C body (the deep statics
/// `get_actual_variable_range` / `convert_to_scalar` panic — see above).
pub(crate) fn ineq_histogram_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    vardata: &VariableStatData,
    opoid: Oid,
    opproc_oid: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    constval: Datum,
    consttype: Oid,
) -> PgResult<f64> {
    let mut hist_selec = -1.0f64;

    if let Some(stats_tuple) = vardata.stats_tuple {
        if statistic_proc_security_check(vardata, opproc_oid)? {
            if let Some(mut sslot) = lsc::get_attstatsslot::call(
                mcx,
                stats_tuple,
                STATISTIC_KIND_HISTOGRAM,
                InvalidOid,
                ATTSTATSSLOT_VALUES,
            )? {
                let nvalues = sslot.values.len() as i32;
                let staop = sslot.staop;
                let stacoll = sslot.stacoll;

                if nvalues > 1
                    && stacoll == collation
                    && lsc::comparison_ops_are_compatible::call(staop, opoid)?
                {
                    // Binary search for the right end of the histogram bin
                    // containing the comparison value.
                    let histfrac: f64;
                    let mut lobound = 0i32; // first possible slot to search
                    let mut hibound = nvalues; // last+1 slot to search
                    let mut have_end = false;

                    if nvalues == 2 {
                        // get_actual_variable_range mutates sslot.values[0/1]
                        let (v0, v1) = (sslot.values[0], sslot.values[1]);
                        let mut min = v0;
                        let mut max = v1;
                        have_end = get_actual_variable_range(
                            root,
                            vardata,
                            staop,
                            collation,
                            Some(&mut min),
                            Some(&mut max),
                        );
                        sslot.values[0] = min;
                        sslot.values[1] = max;
                    }

                    while lobound < hibound {
                        let probe = (lobound + hibound) / 2;

                        if probe == 0 && nvalues > 2 {
                            let mut min = sslot.values[0];
                            have_end = get_actual_variable_range(
                                root,
                                vardata,
                                staop,
                                collation,
                                Some(&mut min),
                                None,
                            );
                            sslot.values[0] = min;
                        } else if probe == nvalues - 1 && nvalues > 2 {
                            let mut max = sslot.values[probe as usize];
                            have_end = get_actual_variable_range(
                                root,
                                vardata,
                                staop,
                                collation,
                                None,
                                Some(&mut max),
                            );
                            sslot.values[probe as usize] = max;
                        }

                        let res = fmgr::function_call2_coll::call(
                            opproc_oid,
                            collation,
                            sslot.values[probe as usize],
                            constval,
                        )?;
                        let mut ltcmp = res.as_bool();
                        if isgt {
                            ltcmp = !ltcmp;
                        }
                        if ltcmp {
                            lobound = probe + 1;
                        } else {
                            hibound = probe;
                        }
                    }

                    if lobound <= 0 {
                        // Constant is below the lower histogram boundary.
                        histfrac = 0.0;
                    } else if lobound >= nvalues {
                        // Constant is above the upper histogram boundary.
                        histfrac = 1.0;
                    } else {
                        // values[i-1] <= constant <= values[i].
                        let i = lobound;
                        let mut eq_selec = 0.0f64;
                        let binfrac: f64;

                        if i == 1 || isgt == iseq {
                            // Estimate the selectivity of "x = constval".
                            let (mut otherdistinct, _isdefault) =
                                get_variable_numdistinct(root, vardata);

                            // Subtract off the number of known MCVs.
                            if let Some(mcvslot) = lsc::get_attstatsslot::call(
                                mcx,
                                stats_tuple,
                                STATISTIC_KIND_MCV,
                                InvalidOid,
                                ATTSTATSSLOT_NUMBERS,
                            )? {
                                otherdistinct -= mcvslot.numbers.len() as f64;
                            }

                            if otherdistinct > 1.0 {
                                eq_selec = 1.0 / otherdistinct;
                            }
                        }

                        // Convert the constant and the two nearest bin
                        // boundaries to a uniform scale and interpolate.
                        let (ok, val, low, high) = convert_to_scalar(
                            constval,
                            consttype,
                            collation,
                            sslot.values[(i - 1) as usize],
                            sslot.values[i as usize],
                            vardata.vartype,
                        );
                        if ok {
                            if high <= low {
                                binfrac = 0.5;
                            } else if val <= low {
                                binfrac = 0.0;
                            } else if val >= high {
                                binfrac = 1.0;
                            } else {
                                let bf = (val - low) / (high - low);
                                if bf.is_nan() || bf < 0.0 || bf > 1.0 {
                                    binfrac = 0.5;
                                } else {
                                    binfrac = bf;
                                }
                            }
                        } else {
                            binfrac = 0.5;
                        }

                        let mut hf = (i - 1) as f64 + binfrac;
                        hf /= (nvalues - 1) as f64;

                        // First bin is slightly narrower; rescale.
                        if i == 1 {
                            hf += eq_selec * (1.0 - binfrac);
                        }

                        // "<", ">=" need to subtract eq_selec.
                        if isgt == iseq {
                            hf -= eq_selec;
                        }

                        histfrac = hf;
                    }

                    hist_selec = if isgt { 1.0 - histfrac } else { histfrac };

                    if have_end {
                        hist_selec = clamp_probability(hist_selec);
                    } else {
                        let cutoff = 0.01 / ((nvalues - 1) as f64);
                        if hist_selec < cutoff {
                            hist_selec = cutoff;
                        } else if hist_selec > 1.0 - cutoff {
                            hist_selec = 1.0 - cutoff;
                        }
                    }
                } else if nvalues > 1 {
                    // Histogram present but not sorted the way we want:
                    // brute-force count.
                    let mut nmatch = 0i32;
                    for i in 0..nvalues {
                        let res = fmgr::function_call_invoke::call(
                            opproc_oid,
                            collation,
                            &[
                                NullableDatum::value(sslot.values[i as usize]),
                                NullableDatum::value(constval),
                            ],
                        )?;
                        if !res.1 && res.0.as_bool() {
                            nmatch += 1;
                        }
                    }
                    hist_selec = (nmatch as f64) / (nvalues as f64);

                    let cutoff = 0.01 / ((nvalues - 1) as f64);
                    if hist_selec < cutoff {
                        hist_selec = cutoff;
                    } else if hist_selec > 1.0 - cutoff {
                        hist_selec = 1.0 - cutoff;
                    }
                }
            }
        }
    }

    Ok(hist_selec)
}

/* ---------------------------------------------------------------------------
 * scalarineqsel (selfuncs.c:587)
 * ------------------------------------------------------------------------- */

/// `scalarineqsel(root, operator, isgt, iseq, collation, vardata, constval,
/// consttype)` (selfuncs.c) — selectivity of a scalar inequality. 1:1 with the
/// C body. The CTID special case reads `vardata->rel->pages`/`tuples`.
pub(crate) fn scalarineqsel<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    operator: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    vardata: &VariableStatData,
    constval: Datum,
    consttype: Oid,
) -> PgResult<f64> {
    let stats_tuple = match vardata.stats_tuple {
        None => {
            // No stats. If the variable is CTID, estimate from the table size.
            if let Some(var) = root.node(vardata.var).as_var() {
                if var.varattno == SELF_ITEM_POINTER_ATTRIBUTE_NUMBER {
                    if let Some(relid) = vardata.rel {
                        let rel = root.rel(relid);
                        // If the relation's empty, include all of it.
                        if rel.pages == 0 {
                            return Ok(1.0);
                        }

                        // A TID Datum word is an ItemPointer; the block number /
                        // offset extraction is part of the unported tid.c
                        // ItemPointer decode over the raw constval word.
                        let _ = constval;
                        panic!(
                            "selfuncs: scalarineqsel CTID density estimate is unported — it \
                             decodes the constval word as an ItemPointer \
                             (ItemPointerGetBlockNumberNoCheck / \
                             ItemPointerGetOffsetNumberNoCheck), which the planner-stats path \
                             cannot reach yet"
                        );
                    }
                }
            }
            // no stats available, so default result
            return Ok(DEFAULT_INEQ_SEL);
        }
        Some(t) => t,
    };

    let opproc_oid = lsc::get_opcode::call(operator)?;

    // MCV contribution.
    let (mcv_selec, sumcommon) =
        mcv_selectivity(mcx, vardata, opproc_oid, collation, constval, true)?;

    // Histogram contribution.
    let hist_selec = ineq_histogram_selectivity(
        mcx, root, vardata, operator, opproc_oid, isgt, iseq, collation, constval, consttype,
    )?;

    // Merge MCV and histogram, knowing the histogram covers only non-null
    // values not in the MCV list.
    let stanullfrac = crate::scalar::stats_tuple_stanullfrac(stats_tuple) as f64;
    let mut selec = 1.0 - stanullfrac - sumcommon;

    if hist_selec >= 0.0 {
        selec *= hist_selec;
    } else {
        // No histogram but there are non-MCV values: assume half match.
        selec *= 0.5;
    }

    selec += mcv_selec;

    Ok(clamp_probability(selec))
}
