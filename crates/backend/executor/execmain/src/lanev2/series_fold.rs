//! Plain-agg fold over a `generate_series` FunctionScan (issue #83) — the
//! consumer half of `nodefunctionscan::series`.
//!
//! `SELECT sum(i) FROM generate_series(1, 100000000) t(i)` plans as
//! `Agg(AGG_PLAIN) → FunctionScan`, and every row of it used to travel the
//! long way round: `ExecMakeTableFunctionResult` drains the SRF into a
//! `Tuplestore` (100M heap tuples, spilled to disk past `work_mem`), then
//! each `exec_proc_node` pull reads one tuple back out and runs the per-row
//! transition program over it. The store buys backward scans, rescans and
//! `WITH ORDINALITY` — none of which a one-pass plain aggregate uses.
//!
//! This drive removes it for exactly that shape: the generator
//! (`nodefunctionscan::series_open`) stages [`SERIES_BATCH`] values straight
//! into a lane column and `lanefold::fold_batch` folds the batch into the
//! node's single pergroup array — the SAME kernel, over the same
//! [`lanefold::LaneCols`] face, that the heap plain-fold feed
//! (`agg_plain_fold_drive`) runs. No tuplestore, no tuple, no slot, no
//! per-row expression call.
//!
//! # Byte-identity
//!
//! The whole argument is that this drive is the plain-fold feed with a
//! different column source, so the two ways an answer could change are the
//! ROWS and the FOLD:
//!
//!   * ROWS — the generator emits the SRF's own `next()` sequence with the
//!     SRF's own argument evaluation and its NULL/step-zero contracts
//!     (series.rs states the full identity argument, including the
//!     `pgstat` refusal). Same values, same order, same errors, same point
//!     in the pull.
//!   * FOLD — `fold_batch` over a dense all-selected, no-null batch is the
//!     admitted transitions' accumulation in row order, exactly as on a heap
//!     lane. The admission below pins the one thing a synthetic lane can get
//!     wrong that a staged heap batch cannot: the batch has ONE column, so
//!     every read the plan performs must be column 0 at the generator's own
//!     datum width ([`series_plan_admits`]).
//!
//! Refusal is always safe: nothing has been consumed at any refusal point
//! (recognition is pure — see the `series_kind`-before-`series_open` order
//! in [`try_own_plain_agg_over_function_scan`]), so the caller falls through
//! to the UNCHANGED `exec_agg` over `exec_function_scan` path.
//!
//! Knobs: `PGRUST_LANE_V2_SERIESFOLD` — default ON (this drive IS the fix
//! for #83); the permanent `=0`/`off` spelling is the kill switch and
//! byte-restores the store path. Knob-OFF ticks NOTHING and costs one
//! relaxed byte load + compare, resolved through the `#[cold]` tail
//! (the se2-cost law).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::lanefold::{LaneCols, LaneKind, LanePlan, LaneWidth};
use ::nodefunctionscan::{FunctionScanState, SeriesKind};
use ::types_error::PgResult;

use super::stats::{self, RefuseReason, ShapeClass};

/// Values staged per fold call. Unlike the heap feeds (whose batch IS a
/// page, so `SOA_MAX_ROWS` is a storage fact) the series lane is synthetic
/// and picks its own grain: large enough that the per-batch bitmap build,
/// interrupt check and `fold_batch` dispatch amortize away, small enough
/// that the staged column stays L1-resident.
const SERIES_BATCH: usize = 1024;
const SERIES_BM_WORDS: usize = SERIES_BATCH / 64;

/// `PGRUST_LANE_V2_SERIESFOLD` (default ON; R-KNOBS registry spelling): the
/// series-fold gate. 0 = unresolved, 1 = OFF, 2 = ON. Same AtomicU8 idiom as
/// the other lane knobs (OFF-first relaxed byte load, `#[cold]`-outlined
/// resolve, same-process test lever).
static SERIESFOLD: AtomicU8 = AtomicU8::new(0);

#[inline]
fn seriesfold_enabled() -> bool {
    match SERIESFOLD.load(Relaxed) {
        1 => false,
        2 => true,
        _ => seriesfold_resolve(),
    }
}

#[cold]
#[inline(never)]
fn seriesfold_resolve() -> bool {
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_SERIESFOLD").as_deref(),
        Ok("0") | Ok("off")
    );
    SERIESFOLD.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// The staged series column, in the shape `lanefold` reads lanes through.
/// One column (the scan's single output attribute), never null, every row
/// selected — so the fold's null and selection handling collapse to the
/// dense case exactly as they do on an all-visible unqualified heap batch.
struct SeriesCols {
    values: [Datum; SERIES_BATCH],
    isnull: [bool; SERIES_BATCH],
    n: usize,
}

impl SeriesCols {
    fn new() -> SeriesCols {
        SeriesCols { values: [Datum::null(); SERIES_BATCH], isnull: [false; SERIES_BATCH], n: 0 }
    }
}

impl LaneCols for SeriesCols {
    #[inline(always)]
    fn col_values(&self, c: usize) -> &[Datum] {
        debug_assert_eq!(c, 0, "series admission pins every lane read to column 0");
        &self.values[..self.n]
    }

    #[inline(always)]
    fn col_isnull(&self, c: usize) -> &[bool] {
        debug_assert_eq!(c, 0, "series admission pins every lane read to column 0");
        &self.isnull[..self.n]
    }
}

/// The lane read width the generator's datum encoding serves.
fn series_lane_width(kind: SeriesKind) -> LaneWidth {
    match kind {
        SeriesKind::Int4 => LaneWidth::I32,
        SeriesKind::Int8 => LaneWidth::I64,
    }
}

/// Fold-plan admission for a ONE-column, never-null, fully-selected lane.
///
/// A staged heap batch carries every column the plan classified against the
/// outer descriptor; this batch carries one. So the plan may read nothing
/// else, and it must read that one at the width the generator writes:
///
///   * `guarded` — the data-level proofs (`check_guards`) demote a batch to
///     the checked per-row program, and this drive has no per-row program to
///     demote to (it never builds a slot). Refused, so `sum(i + 1)`-style
///     transforms keep today's path rather than risk a fold C would have
///     raised on.
///   * `resid` — a residual transition is a per-row program by definition.
///   * `filters` — a `FILTER (WHERE ...)` predicate reads a second lane in
///     the general case; refused wholesale rather than admitted per shape.
///   * two-argument kinds (`FRegrAccum`, `Count2`) read `col2`; refused for
///     the same reason.
///   * `cols` — `classify` records EVERY lane column a plan reads (value
///     lanes, `col2`, filter columns), so `cols ⊆ {0}` is the complete
///     "reads nothing but the series" test.
///   * `width` — a `CountStar` reads no lane (its `width` field is not a
///     column's), every other kind reads `col` at `t.width`. A mismatch
///     would reinterpret the datum word, so it is an exact-equality gate,
///     not a size comparison.
///
/// The width check is belt-and-braces (the scan's output column type IS the
/// overload's return type, and `classify_var` already matched the Var's
/// type), and it is the one gate that turns a future overload's datum
/// encoding into a refusal instead of a wrong sum.
fn series_plan_admits(plan: &LanePlan<'_>, width: LaneWidth) -> bool {
    if plan.guarded || !plan.resid.is_empty() || !plan.filters.is_empty() {
        return false;
    }
    if plan.cols.iter().any(|&c| c != 0) {
        return false;
    }
    plan.trans.iter().all(|t| match t.kind {
        LaneKind::CountStar => true,
        LaneKind::FRegrAccum | LaneKind::Count2 => false,
        _ => t.col == 0 && t.width == width,
    })
}

/// Try to let the lane own an `AGG_PLAIN` `Agg` over a `generate_series`
/// `FunctionScan` by folding straight off the generator. `Some(result)` =
/// the lane drove this call; `None` = refused (the caller falls through to
/// the unchanged per-tuple `exec_agg` over `exec_function_scan`).
pub(crate) fn try_own_plain_agg_over_function_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    fs: &mut ::mcx::PgBox<'mcx, FunctionScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Knob first (OFF ticks nothing), then the dynamic per-call gates in the
    // host template's order: EPQ is the first dynamic refusal (§4.2).
    if !seriesfold_enabled() {
        return Ok(None);
    }
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::FunctionScan, RefuseReason::Epq);
        return Ok(None);
    }
    // EXPLAIN ANALYZE: the drive reports no per-node row counts. (Under
    // instrumentation the child arrives wrapped, so this arm is already
    // unreachable — the gate is the explicit statement of why.)
    if estate.es_instrument != 0 {
        stats::tick_refused(ShapeClass::FunctionScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    // No runtime-direction gate: pulls are forward-invariant below the run
    // seam (deletion-prep B1 — the `Backward` refusal row is a tombstone),
    // and the PLAN-level fact this drive actually needs — a
    // backward-capable scan, whose store IS its random-access buffer — is
    // refused structurally by `series_kind`'s `EXEC_FLAG_BACKWARD` arm.
    //
    // Agg side: batch-drainable AGG_PLAIN with a classified fold plan and no
    // pending initplan params (the plain fold drive's own gate, verbatim).
    if !::nodeagg::agg_plain_fold_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // Scan side. PURE recognition, and it MUST precede the plan admission's
    // refusal points as well as its own: `series_open` evaluates the SRF's
    // argument expressions, so nothing may refuse after it (a volatile
    // argument would be evaluated twice — once here, once by the store path).
    let Some(kind) = ::nodefunctionscan::series_kind(&**fs) else {
        stats::tick_refused(ShapeClass::FunctionScan, RefuseReason::SeriesShape);
        return Ok(None);
    };
    let width = series_lane_width(kind);
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold-admissible node has a plan");
        if !series_plan_admits(plan, width) {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::SeriesShape);
            return Ok(None);
        }
    }
    // exec_agg's top-of-call guard: the one result row is out; a drained agg
    // stays drained until a rescan clears `agg_done`.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // One OWNED tick per lane-owned build event, on both classes it engages
    // (aggbuild counts builds; functionscan counts feeds) — the
    // agg-over-subqueryscan cadence. Past this point the drive is committed.
    stats::tick_owned(ShapeClass::AggBuild);
    stats::tick_owned(ShapeClass::FunctionScan);
    super::lane_trace("plainagg series fold armed");

    let mut feed = ::nodefunctionscan::series_open(&mut **fs, estate, kind)?;
    // initialize_aggregates (delegated): fresh initval pergroups; a rescan
    // re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    series_fold_drive(agg, &mut feed)?;
    // Retrieve (delegated): finalize + HAVING + project — one row (or none,
    // when a var-free HAVING rejects it), setting `agg_done`.
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// The build feed: generate → fold, to exhaustion, inside one
/// `exec_proc_node` call (the plain-fold drive's own shape).
///
/// Outlined so the staged column — ~9 KiB of lane — lives only on the drive's
/// frame, never on the dispatch path's.
#[inline(never)]
fn series_fold_drive<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    feed: &mut ::nodefunctionscan::SeriesFeed,
) -> PgResult<()> {
    let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold-admissible node has a plan");
    let aggcx = ::nodeagg::agg_aggcontext(agg);
    let base = ::nodeagg::agg_plain_pergroup_base(agg);
    let mut cols = SeriesCols::new();
    loop {
        let n = feed.next_batch(&mut cols.values);
        if n == 0 {
            break;
        }
        // The store fill checked interrupts per produced row; a batch is the
        // drive's row-group, so a cancel lands within one batch.
        ::postgres_seams::check_for_interrupts::call()?;
        cols.n = n;
        let nwords = n.div_ceil(64);
        let mut rows = [0u64; SERIES_BM_WORDS];
        rows[..nwords].fill(!0u64);
        if n % 64 != 0 {
            rows[nwords - 1] = (1u64 << (n % 64)) - 1;
        }
        // SAFETY: `base` is the node's once-allocated single-group pergroup
        // array covering every transno (`agg_plain_build_begin` just wrote
        // it); every selected row carries a live lane value for the plan's
        // only column (the generator filled `0..n` and admission proved the
        // plan reads nothing else, at this exact width); AvgAccum pergroups
        // hold the catalog's {0,0} int8[2] transarray, datum-copied at
        // initialize_aggregates; Int128AvgAccum pergroups are NULL or hold
        // the aggcontext state the fold chain installed, and `aggcx` is that
        // same aggcontext; the plan is unguarded (admission), so there is no
        // proof obligation to discharge; no column carries a dict-code view.
        unsafe {
            ::lanefold::fold_batch(plan, &cols, &rows[..nwords], n, base, aggcx)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trans(kind: LaneKind, col: u16, width: LaneWidth) -> ::lanefold::LaneTrans {
        ::lanefold::LaneTrans {
            kind,
            col,
            col2: col,
            width,
            res_width: width,
            fconv: ::lanefold::FloatConv::None,
            fconv2: ::lanefold::FloatConv::None,
            filter: ::lanefold::NO_FILTER,
            addend: 0,
            mulk: 1,
            divk: 1,
            transno: 0,
        }
    }

    // The lanefold-tests idiom: a leaked context, so the plans borrow for
    // 'static and the test needs no estate.
    fn leaked_mcx() -> ::mcx::Mcx<'static> {
        Box::leak(Box::new(::mcx::MemoryContext::new("seriesfold-test"))).mcx()
    }

    fn plan_of(
        mcx: ::mcx::Mcx<'static>,
        trs: &[::lanefold::LaneTrans],
        cols: &[u16],
    ) -> LanePlan<'static> {
        let mut plan = ::lanefold::empty_plan(mcx);
        for t in trs {
            plan.trans.push(*t);
            // `cse_skip` is parallel to `trans` (no CSE group here).
            plan.cse_skip.push(false);
        }
        for c in cols {
            plan.cols.push(*c);
        }
        plan
    }

    /// `series_plan_admits` is the whole safety argument for a one-column
    /// synthetic lane, so pin each refusal separately.
    #[test]
    fn plan_admission() {
        let mcx = leaked_mcx();
        let sum4 = [trans(LaneKind::Sum, 0, LaneWidth::I32)];

        // sum(i) over an int4 series: the target shape.
        let p = plan_of(mcx, &sum4, &[0]);
        assert!(series_plan_admits(&p, LaneWidth::I32));
        // ... and the SAME plan read at the int8 encoding must refuse: the
        // datum word would be reinterpreted.
        assert!(!series_plan_admits(&p, LaneWidth::I64));

        // count(*) reads no lane at all, so any encoding serves it.
        let p = plan_of(mcx, &[trans(LaneKind::CountStar, 0, LaneWidth::I32)], &[]);
        assert!(series_plan_admits(&p, LaneWidth::I64));

        // A second column cannot exist on this batch.
        let p = plan_of(mcx, &[trans(LaneKind::Sum, 1, LaneWidth::I32)], &[1]);
        assert!(!series_plan_admits(&p, LaneWidth::I32));

        // Guarded / residual plans need a per-row program to demote to.
        let mut p = plan_of(mcx, &sum4, &[0]);
        p.guarded = true;
        assert!(!series_plan_admits(&p, LaneWidth::I32));
        let mut p = plan_of(mcx, &sum4, &[0]);
        p.resid.push(0);
        assert!(!series_plan_admits(&p, LaneWidth::I32));

        // Two-argument kinds read `col2`.
        let p = plan_of(mcx, &[trans(LaneKind::Count2, 0, LaneWidth::I32)], &[0]);
        assert!(!series_plan_admits(&p, LaneWidth::I32));
    }

    /// The staged batch's bitmap must select exactly the staged rows: a set
    /// bit past `n` folds a stale datum, a cleared bit inside `n` drops a
    /// row. Mirrors the drive's tail-word build.
    #[test]
    fn batch_bitmap_selects_exactly_n() {
        for n in [1usize, 63, 64, 65, 511, SERIES_BATCH] {
            let nwords = n.div_ceil(64);
            let mut rows = [0u64; SERIES_BM_WORDS];
            rows[..nwords].fill(!0u64);
            if n % 64 != 0 {
                rows[nwords - 1] = (1u64 << (n % 64)) - 1;
            }
            let sel: u32 = rows[..nwords].iter().map(|w| w.count_ones()).sum();
            assert_eq!(sel as usize, n, "n = {n}");
        }
    }
}
