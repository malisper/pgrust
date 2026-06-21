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
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_pathnodes::PlannerInfo;
use types_selfuncs::{VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES};

use backend_utils_cache_lsyscache_seams as lsc;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_access_index_indexam_seams as idxseam;
use backend_access_index_amapi_seams as amapi;
use backend_access_common_relation_seams as relseam;
use backend_optimizer_path_indxpath_seams as ix;
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_scan::sdir::{BackwardScanDirection, ForwardScanDirection, ScanDirection};
use types_storage::lock::NoLock;
use crate::RELKIND_PARTITIONED_TABLE;

use crate::scalar::{get_variable_numdistinct, statistic_proc_security_check};
use crate::{
    clamp_probability, SELF_ITEM_POINTER_ATTRIBUTE_NUMBER, STATISTIC_KIND_HISTOGRAM,
    STATISTIC_KIND_MCV,
};
use types_selfuncs::DEFAULT_INEQ_SEL;

/// `COMPARE_LT` (cmptype.h) — `BTLessStrategyNumber`.
const COMPARE_LT: i32 = 1;
/// `COMPARE_GT` (cmptype.h) — `BTGreaterStrategyNumber`.
const COMPARE_GT: i32 = 5;

/// The canonical (value-carrying) image of one statistics-slot value, for the
/// operator-comparison fmgr boundary. The shared `AttStatsSlot.values` carries a
/// by-reference element (`name`/`text`/`bytea`/`numeric`) only as a
/// non-dereferenceable in-buffer offset, so a by-reference column type's slot
/// values are re-decoded by value via `get_attstatsslot_value_datums`
/// (`canon[i]`, aligned 1:1 with the bare slot). A pass-by-value element is the
/// bare word wrapped as `ByVal` (no separate canonical array is fetched).
pub(crate) fn slot_value_canon<'mcx>(
    bare: &[Datum],
    canon: Option<&mcx::PgVec<'mcx, DatumV<'mcx>>>,
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<DatumV<'mcx>> {
    match canon {
        Some(c) => c[i].clone_in(mcx),
        None => Ok(DatumV::ByVal(bare[i].as_usize())),
    }
}

/// Like [`slot_value_canon`], but consults a per-position endpoint override
/// first. `ineq_histogram_selectivity` may replace the first/last histogram
/// boundary with the column's *actual* current min/max (an index endpoint
/// probe via `get_actual_variable_range`); C overwrites `sslot.values[i]` in
/// place, and this returns the overriding canonical value when one is recorded
/// for position `i`, else falls back to the recorded histogram bound.
fn hist_value_at<'mcx>(
    bare: &[Datum],
    canon: Option<&mcx::PgVec<'mcx, DatumV<'mcx>>>,
    overrides: &[Option<DatumV<'mcx>>],
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<DatumV<'mcx>> {
    match &overrides[i] {
        Some(v) => v.clone_in(mcx),
        None => slot_value_canon(bare, canon, i, mcx),
    }
}

/// Fetch the canonical value-carrying images of a statistics slot's `stavalues`
/// array when the element type is pass-by-reference (so the bare-word
/// `AttStatsSlot.values` offsets cannot be dereferenced at the fmgr boundary).
/// Returns `None` for a pass-by-value element type — the bare word is the value.
pub(crate) fn slot_canon_values<'mcx>(
    mcx: Mcx<'mcx>,
    stats_tuple: types_selfuncs::StatsTuple,
    reqkind: i32,
    reqop: Oid,
    valuetype: Oid,
) -> PgResult<Option<mcx::PgVec<'mcx, DatumV<'mcx>>>> {
    if lsc::get_typbyval::call(valuetype)? {
        return Ok(None);
    }
    lsc::get_attstatsslot_value_datums::call(mcx, stats_tuple, reqkind, reqop)
}

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
    constval: &DatumV<'mcx>,
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
                let canon = slot_canon_values(
                    mcx,
                    stats_tuple,
                    STATISTIC_KIND_MCV,
                    InvalidOid,
                    sslot.valuetype,
                )?;
                for i in 0..sslot.values.len() {
                    // be careful to apply operator right way 'round. Both the MCV
                    // slot value and the constant cross the operator's fmgr
                    // boundary as their canonical images (by-value word OR
                    // by-reference referent) via the by-reference-capable
                    // `function_call_invoke_datum` lane, so a
                    // `name`/`text`/`bytea`/`numeric` MCV value and constant are
                    // compared correctly.
                    let mcv = slot_value_canon(&sslot.values, canon.as_ref(), i, mcx)?;
                    let (arg0, arg1) = if var_on_left {
                        (mcv, constval.clone_in(mcx)?)
                    } else {
                        (constval.clone_in(mcx)?, mcv)
                    };
                    let (fresult, isnull) = fmgr::function_call_invoke_datum::call(
                        mcx,
                        opproc_oid,
                        collation,
                        &[arg0, arg1],
                        &[],
                        None,
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
    constval: &DatumV<'mcx>,
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
    constval: &DatumV<'mcx>,
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
                    let canon = slot_canon_values(
                        mcx,
                        stats_tuple,
                        STATISTIC_KIND_HISTOGRAM,
                        InvalidOid,
                        sslot.valuetype,
                    )?;
                    let mut nmatch = 0i32;
                    let mut i = n_skip;
                    while i < nvalues - n_skip {
                        let hval = slot_value_canon(&sslot.values, canon.as_ref(), i as usize, mcx)?;
                        let (arg0, arg1) = if varonleft {
                            (hval, constval.clone_in(mcx)?)
                        } else {
                            (constval.clone_in(mcx)?, hval)
                        };
                        let (fresult, isnull) = fmgr::function_call_invoke_datum::call(
                            mcx,
                            opproc_oid,
                            collation,
                            &[arg0, arg1],
                            &[],
                            None,
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

/// The endpoints `get_actual_variable_range` extracted from an index probe, as
/// canonical value images. `have_data` mirrors C's boolean return; `min`/`max`
/// are filled only for the endpoints the caller requested AND that were found.
pub(crate) struct ActualVariableRange<'mcx> {
    pub have_data: bool,
    pub min: Option<DatumV<'mcx>>,
    pub max: Option<DatumV<'mcx>>,
}

/// `get_actual_variable_range(root, vardata, sortop, collation, &min, &max)`
/// (selfuncs.c:6581) — try to identify the variable's *actual* current min
/// and/or max by finding a suitable btree index and fetching its low/high
/// endpoint via an index-only-scan probe. Returns `have_data = true` (C `true`)
/// with the requested endpoints filled, or `have_data = false` (C `false`) when
/// there is no suitable index or the probe gives up.
///
/// The bare-scan endpoint extraction (`get_actual_variable_endpoint`) lives in
/// `backend-access-index-indexam` (it owns the scan-descriptor primitives) and
/// is reached through the `get_actual_variable_endpoint` seam; this function is
/// the index-selection driver (the part that is selfuncs's own logic). `want_min`
/// / `want_max` correspond to C's non-NULL `min` / `max` pointers.
#[allow(clippy::too_many_arguments)]
fn get_actual_variable_range<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    vardata: &VariableStatData,
    sortop: Oid,
    collation: Oid,
    want_min: bool,
    want_max: bool,
) -> PgResult<ActualVariableRange<'mcx>> {
    let none = ActualVariableRange { have_data: false, min: None, max: None };

    // No hope if no relation or it doesn't have indexes.
    let relid = match vardata.rel {
        None => return Ok(none),
        Some(r) => r,
    };
    let rel = root.rel(relid);
    if rel.indexlist.is_empty() {
        return Ok(none);
    }

    // If it has indexes it must be a plain relation.
    // Assert(rte->rtekind == RTE_RELATION).
    let rte = planner_rt_fetch(run, root, rel.relid);
    debug_assert_eq!(
        rte.rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RELATION
    );
    let rte_relid = rte.relid;
    let rte_relkind = rte.relkind;

    // ignore partitioned tables. Any indexes here are not real indexes.
    if rte_relkind == RELKIND_PARTITIONED_TABLE as i8 {
        return Ok(none);
    }

    // Search through the indexes to see if any match our problem.
    for index in rel.indexlist.iter() {
        // Ignore non-ordering indexes.
        if index.sortopfamily.is_empty() {
            continue;
        }
        // Ignore partial indexes (we only want stats covering the whole rel).
        if !index.indpred.is_empty() {
            continue;
        }
        // Hypothetical indexes (from a get_relation_info hook) aren't real.
        if index.hypothetical {
            continue;
        }
        // get_actual_variable_endpoint uses the index-only-scan machinery, so
        // ignore indexes that can't use it on their first column.
        if !index.canreturn[0] {
            continue;
        }
        // The first index column must match the desired variable, sortop, and
        // collation --- but we can use a descending-order index.
        if collation != index.indexcollations[0] {
            continue; // test first 'cause it's cheapest
        }
        if !ix::match_index_to_operand::call(root, vardata.var, 0, index) {
            continue;
        }
        let strategy = lsc::get_op_opfamily_strategy::call(sortop, index.sortopfamily[0])?;
        let compare = amapi::index_am_translate_strategy::call(
            strategy,
            index.relam,
            index.sortopfamily[0],
            true,
        )?;
        let indexscandir = if compare == COMPARE_LT {
            if index.reverse_sort[0] {
                BackwardScanDirection
            } else {
                ForwardScanDirection
            }
        } else if compare == COMPARE_GT {
            if index.reverse_sort[0] {
                ForwardScanDirection
            } else {
                BackwardScanDirection
            }
        } else {
            // index doesn't match the sortop
            continue;
        };

        // Found a suitable index to extract data from.
        // Open the table and index so we can read from them. We should already
        // have some type of lock on each; table_open/index_open with NoLock.
        let heap_rel = relseam::relation_open::call(mcx, rte_relid, NoLock)?;
        let index_rel = idxseam::index_open::call(mcx, index.indexoid, NoLock)?;

        // get_typlenbyval(vardata->atttype, &typLen, &typByVal).
        let (typ_len, typ_byval) = lsc::get_typlenbyval::call(vardata.atttype)?;

        let mut have_data = false;
        let mut min_out: Option<DatumV<'mcx>> = None;
        let mut max_out: Option<DatumV<'mcx>> = None;

        // If min is requested ...
        if want_min {
            match idxseam::get_actual_variable_endpoint::call(
                mcx,
                heap_rel.alias(),
                index_rel.alias(),
                indexscandir,
                typ_len,
                typ_byval,
            )? {
                Some(v) => {
                    min_out = Some(v);
                    have_data = true;
                }
                None => have_data = false,
            }
        } else {
            // If min not requested, still want to fetch max.
            have_data = true;
        }

        // If max is requested, and we didn't already fail ...
        if want_max && have_data {
            // scan in the opposite direction; all else is the same.
            let revdir = match indexscandir {
                ForwardScanDirection => BackwardScanDirection,
                BackwardScanDirection => ForwardScanDirection,
                d => d,
            };
            match idxseam::get_actual_variable_endpoint::call(
                mcx,
                heap_rel.alias(),
                index_rel.alias(),
                revdir,
                typ_len,
                typ_byval,
            )? {
                Some(v) => max_out = Some(v),
                None => have_data = false,
            }
        }

        // index_close(indexRel, NoLock); table_close(heapRel, NoLock). The owned
        // Relation handles close on drop (NoLock abort-path closer), matching
        // C's explicit NoLock close here.
        drop(index_rel);
        drop(heap_rel);

        // And we're done (C breaks out of the index loop unconditionally).
        return Ok(ActualVariableRange { have_data, min: min_out, max: max_out });
    }

    Ok(none)
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
/// if no usable histogram. 1:1 with the C body (`get_actual_variable_range`
/// declines gracefully — see above — so the recorded histogram bound is used).
#[allow(clippy::too_many_arguments)]
pub(crate) fn ineq_histogram_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    vardata: &VariableStatData,
    opoid: Oid,
    opproc_oid: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    constval: &DatumV<'mcx>,
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
                // Canonical value-carrying images of the histogram bin values
                // (by-reference element types cannot cross the operator fmgr
                // boundary as the bare offset words).
                let canon = slot_canon_values(
                    mcx,
                    stats_tuple,
                    STATISTIC_KIND_HISTOGRAM,
                    InvalidOid,
                    sslot.valuetype,
                )?;

                // `get_actual_variable_range` may replace the first / last
                // histogram boundary with the column's *actual* current min/max
                // (an index endpoint probe). C overwrites `sslot.values[0/...]`
                // in place; here those positions are tracked as canonical-value
                // overrides consulted by [`hist_value_at`] (the bare slot words
                // remain untouched, so `convert_to_scalar`'s by-value scaling
                // below still reads them — only the comparison value is the
                // refined endpoint).
                let mut endpoint_override: Vec<Option<DatumV<'mcx>>> =
                    (0..nvalues).map(|_| None).collect();

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
                        // get_actual_variable_range overwrites values[0] (min) and
                        // values[1] (max).
                        let range = get_actual_variable_range(
                            mcx, run, root, vardata, staop, collation, true, true,
                        )?;
                        have_end = range.have_data;
                        if let Some(v) = range.min {
                            endpoint_override[0] = Some(v);
                        }
                        if let Some(v) = range.max {
                            endpoint_override[1] = Some(v);
                        }
                    }

                    while lobound < hibound {
                        let probe = (lobound + hibound) / 2;

                        if probe == 0 && nvalues > 2 {
                            let range = get_actual_variable_range(
                                mcx, run, root, vardata, staop, collation, true, false,
                            )?;
                            have_end = range.have_data;
                            if let Some(v) = range.min {
                                endpoint_override[0] = Some(v);
                            }
                        } else if probe == nvalues - 1 && nvalues > 2 {
                            let range = get_actual_variable_range(
                                mcx, run, root, vardata, staop, collation, false, true,
                            )?;
                            have_end = range.have_data;
                            if let Some(v) = range.max {
                                endpoint_override[probe as usize] = Some(v);
                            }
                        }

                        // Both the histogram bin value and the constant cross
                        // the by-reference-capable lane, so a
                        // by-ref `text`/`name`/`bytea` comparison is correct.
                        let bin = hist_value_at(
                            &sslot.values,
                            canon.as_ref(),
                            &endpoint_override,
                            probe as usize,
                            mcx,
                        )?;
                        let res = fmgr::function_call2_coll_datum::call(
                            mcx,
                            opproc_oid,
                            collation,
                            bin,
                            constval.clone_in(mcx)?,
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
                        // boundaries to a uniform scale and interpolate. The
                        // bare-word `convert_to_scalar` reads the constant and
                        // the bin boundary values as machine words: a
                        // pass-by-value type scales precisely. For a
                        // pass-by-reference element type (`canon.is_some()`) the
                        // bin boundaries are non-dereferenceable in-buffer
                        // `deconstruct_array` offsets (the `convert.rs` string /
                        // bytea / numeric conversion keystone), so the scalar
                        // conversion cannot run; that is exactly C's
                        // `convert_to_scalar` "don't know how to convert"
                        // (`ok == false`) outcome, which yields the bin midpoint
                        // `binfrac = 0.5`. The bin itself was located by the real
                        // by-reference comparisons in the binary search above, so
                        // only the intra-bin interpolation degrades to the
                        // midpoint.
                        if canon.is_some() {
                            binfrac = 0.5;
                        } else {
                            let const_word = match constval {
                                DatumV::ByVal(w) => Datum::from_usize(*w),
                                _ => Datum::from_usize(0),
                            };
                            // For a by-value type, an endpoint refined by
                            // get_actual_variable_range replaces the bare slot
                            // word (C overwrites `sslot.values[i]`); read the
                            // override word when present, else the bare slot.
                            let low_word = match &endpoint_override[(i - 1) as usize] {
                                Some(DatumV::ByVal(w)) => Datum::from_usize(*w),
                                _ => sslot.values[(i - 1) as usize],
                            };
                            let high_word = match &endpoint_override[i as usize] {
                                Some(DatumV::ByVal(w)) => Datum::from_usize(*w),
                                _ => sslot.values[i as usize],
                            };
                            let (ok, val, low, high) = convert_to_scalar(
                                const_word,
                                consttype,
                                collation,
                                low_word,
                                high_word,
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
                        let bin = slot_value_canon(&sslot.values, canon.as_ref(), i as usize, mcx)?;
                        let res = fmgr::function_call_invoke_datum::call(
                            mcx,
                            opproc_oid,
                            collation,
                            &[bin, constval.clone_in(mcx)?],
                            &[],
                            None,
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn scalarineqsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    operator: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    vardata: &VariableStatData,
    constval: &DatumV<'mcx>,
    consttype: Oid,
) -> PgResult<f64> {
    let stats_tuple = match vardata.stats_tuple {
        None => {
            // No stats. If the variable is CTID, estimate from the table size.
            if let Some(var) = root.node(vardata.var).as_var() {
                if var.varattno == SELF_ITEM_POINTER_ATTRIBUTE_NUMBER {
                    if let Some(relid) = vardata.rel {
                        let rel = root.rel(relid);
                        // If the relation's empty, we're going to include all of
                        // it. (Mostly to avoid divide-by-zero below.)
                        if rel.pages == 0 {
                            return Ok(1.0);
                        }

                        // itemptr = (ItemPointer) DatumGetPointer(constval);
                        // The TID Datum crosses by reference as the verbatim
                        // 6-byte ItemPointerData image (BlockIdData{bi_hi, bi_lo}
                        // + uint16 ip_posid), exactly as tid.c's
                        // return_itempointer writes it.
                        let image = constval.as_ref_bytes();
                        let bi_hi = u16::from_ne_bytes([image[0], image[1]]);
                        let bi_lo = u16::from_ne_bytes([image[2], image[3]]);
                        let off = u16::from_ne_bytes([image[4], image[5]]);
                        // ItemPointerGetBlockNumberNoCheck(itemptr)
                        let mut block = (((bi_hi as u32) << 16) | bi_lo as u32) as f64;
                        // ItemPointerGetOffsetNumberNoCheck(itemptr)
                        let offset = off as f64;

                        let pages = rel.pages as f64;
                        let tuples = rel.tuples;

                        // Determine the average number of tuples per page
                        // (density). The last page is, on average, half full, so
                        // give it half weight.
                        let mut density = tuples / (pages - 0.5);

                        // If target is the last page, use half the density.
                        if block >= pages - 1.0 {
                            density *= 0.5;
                        }

                        // Use the density to estimate how far into the page the
                        // itemptr is likely to be, and add that fraction of a
                        // whole block (never more than a whole block).
                        if density > 0.0 {
                            block += (offset / density).min(1.0);
                        }

                        // Convert relative block number to selectivity; again the
                        // last page has only half weight.
                        let mut selec = block / (pages - 0.5);

                        // The calculation so far gave us a selectivity for "<=".
                        // We'll have one fewer tuple for "<" and one additional
                        // for ">=" (the latter reversed below), so subtract one
                        // tuple in both cases; identified by iseq == isgt.
                        if iseq == isgt && tuples >= 1.0 {
                            selec -= 1.0 / tuples;
                        }

                        // Reverse the selectivity for the ">", ">=" cases.
                        if isgt {
                            selec = 1.0 - selec;
                        }

                        return Ok(clamp_probability(selec));
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
        mcx, run, root, vardata, operator, opproc_oid, isgt, iseq, collation, constval, consttype,
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
