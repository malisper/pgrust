//! sorted-sink arm — the ORDERED-GROUPED runtime aggregation arm: a
//! SERIAL-plan sort-free GroupAggregate (`Agg(AGG_SORTED) → pgrcolumnar
//! SeqScan`, the clustered/footer-sorted bank shape) executed as one runtime
//! ParallelSink at DOP N on the M1 pinned-RG machinery.
//!
//! Shape: morsel claims on a group-key-clustered store are contiguous
//! granule ranges, locally ordered, but a group can SPAN claim boundaries.
//! Each ACCEPT claim drives the SERIAL sorted-fold kernels (boundary
//! detection on the staged key lanes + one `fold_batch` per group run — the
//! lane-v2-sortedfold drive, narrowed) over its range:
//!
//!  * COMPLETE interior groups finalize+HAVING+project WORKER-side through
//!    the node's own `agg_sorted_emit` and are captured as self-contained
//!    rows (`SortedEmitAcc` — byref outputs deep-copied into the claim's
//!    arena);
//!  * the claim's two EDGE groups cross as `RuntimePartial` boundary
//!    partials (runtime_partial.rs — plain Rust integers, exact
//!    order-insensitive combine) plus their raw key datum words; a claim
//!    with zero interior boundaries exports ONE spanning partial.
//!
//! SEAL (single-threaded, last-worker-out) collects every Local's claim
//! records sorted by range start (the shared-cursor claims tile the granule
//! space exactly once, so record order IS table order). The LEADER stitches
//! adjacent claims on Completed: key-equal edge partials combine
//! (`agg_runtime_combine_into`); each completed boundary group installs into
//! the leader's own Agg node (`agg_sorted_stitch_begin` + absorb) and
//! finalizes through the node's own `agg_sorted_emit` — the serial code
//! path, byte-identically. Emission order = claim range order = store order
//! = group-key order: the AGG_SORTED pathkey contract is preserved.
//!
//! Admission (fail-closed; every refusal = today's serial arm):
//!  * `PGRUST_RUNTIME=1` + `SET pgrust.runtime_agg_pool = <dop>` (the agg
//!    pool's ordered face) + the lane master switch +
//!    `PGRUST_RUNTIME_AGG_SORTED` kill;
//!  * sorted-FOLD admissible (AGG_SORTED, no gsets/merge/internal sorts,
//!    classified fold plan, representational grouping equality), lane-
//!    comparable by-value keys, unprojected fusible pgrcolumnar SeqScan whose
//!    staging arms (PREWHERE for qualled scans);
//!  * no vguards, no residual transitions; every transition kind
//!    runtime-partial combinable (AvgAccum/Int128/Count/Sum/byval folds —
//!    Str/Bp/F min-max refuse). GUARDED plans (the real length()-agg charlen mb
//!    guard) ADMIT: the claim drive re-proves guards per window from zone
//!    answers and refuses fail-closed on a Demote verdict (no checked
//!    per-row program exists in the narrow drive);
//!  * projection + HAVING reference ONLY grouping columns and aggregates
//!    (the boundary-group representative is reconstructed from key datums;
//!    a func-dep non-key Var would need the group's true first tuple);
//!  * every output column byval / varlena / fixed byref (the capture's
//!    deep-copy vocabulary);
//!  * the M1 session/binder gate set verbatim; granule floor.
//!
//! Memory (R3): captured rows + partials are metered per Local against
//! `work_mem × hash_mem_multiplier`; a crossing records a BUDGET REFUSAL,
//! aborts the RG, and the leader falls back to the serial arm (R5
//! whole-attempt rerun — nothing consumed twice). No spill arm in v1.
//!
//! Claims opt into WHOLE-BOUNDARY sizing (`whole_boundary_claims`): a claim
//! is one dict-epoch row group — mid-RG splits duplicate dict decompress +
//! memo per worker (runtime-drive-scaling law), and fewer, RG-aligned claims
//! also minimize boundary partials. `PGRUST_RUNTIME_AGG_SORTED_SPLIT=1`
//! restores duration-sized sub-RG claims (diagnostic).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::datum::Datum;
use ::executils::EStateData;
use ::nodeagg::runtime_partial::RuntimePartial;
use ::nodeagg::sink::sink_shape_error;
use ::nodeagg::sortedsink::{SortedByrefSpec, SortedEmitAcc, SortedEmitSeg};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, SortedFoldKeys, SORTED_FOLD_MAX_KEYS};

// ---------------------------------------------------------------------------
// Claim records + the sink.
// ---------------------------------------------------------------------------

/// One exported edge group: the raw key datum words (widened comparison via
/// the same width masks as the boundary compare) + the self-contained
/// partial states.
struct BoundaryPartial {
    key: [(u64, bool); SORTED_FOLD_MAX_KEYS],
    partial: RuntimePartial,
}

/// One claim's output. Cases:
///  * no surviving rows: `first`/`last` None, `interior` None;
///  * >=1 rows, zero interior boundaries: `spanning` with `first` = the one
///    open partial (`last` None);
///  * >=1 boundaries: `first` = the left edge partial, `interior` = the
///    captured complete groups (may be empty), `last` = the right edge
///    (open) partial.
struct ClaimRec {
    start: u64,
    end: u64,
    first: Option<BoundaryPartial>,
    interior: Option<SortedEmitSeg>,
    last: Option<BoundaryPartial>,
    spanning: bool,
}

#[derive(Default)]
pub(super) struct SortedAggLocal {
    recs: Vec<ClaimRec>,
    /// Retained capture bytes (R3 metering against `sink.budget`).
    bytes: usize,
}

struct SortedAggSink {
    /// Per-Local memory envelope (work_mem × hash_mem_multiplier).
    budget: usize,
    nkeys: usize,
    key_lens: [i16; SORTED_FOLD_MAX_KEYS],
    /// SEAL output: every Local's claim records, sorted by range start.
    collected: Mutex<Option<Vec<ClaimRec>>>,
    rg: OnceLock<runtime::WeakRgHandle>,
    failed: AtomicBool,
    error: Mutex<Option<Box<PgError>>>,
    budget_refused: AtomicBool,
}

impl SortedAggSink {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn refuse_budget(&self) {
        self.budget_refused.store(true, Ordering::SeqCst);
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

enum AcceptFail {
    /// Budget/shape refusal — RG abort → serial whole-attempt rerun (R5).
    Budget,
    Error(Box<PgError>),
}

impl From<Box<PgError>> for AcceptFail {
    fn from(e: Box<PgError>) -> AcceptFail {
        AcceptFail::Error(e)
    }
}

impl runtime::ParallelSink for SortedAggSink {
    type Local = SortedAggLocal;

    fn fork(&self, _worker: usize) -> SortedAggLocal {
        SortedAggLocal::default()
    }

    fn accept_local(
        &self,
        local: &mut SortedAggLocal,
        worker: usize,
        range: runtime::MorselRange,
    ) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let r =
            catch_unwind(AssertUnwindSafe(|| accept_morsel_body(self, local, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(AcceptFail::Budget)) => {
                mark_self_errored();
                self.refuse_budget();
            }
            Ok(Err(AcceptFail::Error(e))) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime sorted-agg sink worker panicked").into(),
                );
            }
        }
    }

    /// SEAL (single-threaded, last-worker-out): collect every Local's claim
    /// records in range order — the leader's stitch input.
    fn seal(&self, locals: &mut [SortedAggLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let mut all: Vec<ClaimRec> = Vec::new();
        for l in locals.iter_mut() {
            all.append(&mut l.recs);
        }
        all.sort_unstable_by_key(|r| r.start);
        *self.collected.lock().unwrap_or_else(|p| p.into_inner()) = Some(all);
    }

    fn partitions(&self) -> u64 {
        // The stitch is leader-side (it needs the leader's Agg node for the
        // boundary finalize); the combine phase has nothing to do.
        1
    }

    fn combine(&self, _part: u64, _worker: usize, _locals: &[SortedAggLocal]) {}

    fn finalize(&self, _locals: &[SortedAggLocal]) {}
}

// ---------------------------------------------------------------------------
// Shared engagement payload + worker executor.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract — runtime_agg precedent).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeAggSortedShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    refused: AtomicUsize,
    started: AtomicUsize,
    exited: AtomicUsize,
    sink: Arc<SortedAggSink>,
    query_id: AtomicU64,
    /// M2 inc-1 standing channel: the live board entry, held for the
    /// PRIVATE_SHUTDOWN standing join (standing_channel, scan discipline).
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
}

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
    keys: SortedFoldKeys,
    spec: SortedByrefSpec,
    natts: usize,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

// ---------------------------------------------------------------------------
// Worker drive: one accept morsel = one claim of the ordered drive.
// ---------------------------------------------------------------------------

fn accept_morsel_body(
    sink: &SortedAggSink,
    local: &mut SortedAggLocal,
    _worker: usize,
    range: runtime::MorselRange,
) -> Result<(), AcceptFail> {
    WORKER_EXEC.with(|cell| -> Result<(), AcceptFail> {
        let mut b = cell.borrow_mut();
        let Some(ex) = b.as_mut() else {
            return Err(AcceptFail::Error(Box::new(PgError::new(
                ERROR,
                "runtime sorted-agg morsel without a bound executor",
            ))));
        };
        let (qd, keys) = (ex.qd, ex.keys);
        let (spec, natts) = (&ex.spec, ex.natts);
        crate::querydesc::with_qd(qd, |q| {
            let x = q.exec.as_mut().expect("runtime sorted-agg worker executor state");
            x.with_mut(|d| -> Result<(), AcceptFail> {
                let estate = &mut d.estate;
                let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                else {
                    return Err(AcceptFail::Error(sink_shape_error(
                        "sorted worker plan root is not an Agg",
                    )));
                };
                let aps = &mut **aps;
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                    return Err(AcceptFail::Error(sink_shape_error(
                        "sorted worker outer node is not a SeqScan",
                    )));
                };
                ::nodeseqscan::seq_scan_set_morsel_range(ss, estate, range.start, range.end)?;
                // R3 envelope: the per-batch capture check inside
                // drive_claim sees the Local's ALREADY-RETAINED bytes, so a
                // whole-boundary claim cannot balloon past the budget before
                // the post-claim check (review finding, inc-1b).
                let rec = drive_claim(
                    &mut aps.agg,
                    ss,
                    &keys,
                    spec,
                    natts,
                    range,
                    sink.budget.saturating_sub(local.bytes),
                    estate,
                )?;
                local.bytes += rec.interior.as_ref().map_or(0, |s| s.bytes());
                if local.bytes > sink.budget {
                    return Err(AcceptFail::Budget);
                }
                local.recs.push(rec);
                Ok(())
            })
        })
    })
}

/// Export the OPEN group as a boundary partial (states + raw key words).
fn export_open_group(
    agg: &::nodeagg::AggStateData<'_>,
    cur_key: &[(Datum, bool); SORTED_FOLD_MAX_KEYS],
    nkeys: usize,
) -> PgResult<BoundaryPartial> {
    let mut partial = RuntimePartial::default();
    ::nodeagg::runtime_partial::agg_sorted_export_partial_into(agg, &mut partial)?;
    let mut key = [(0u64, false); SORTED_FOLD_MAX_KEYS];
    for k in 0..nkeys {
        key[k] = (cur_key[k].0.as_u64(), cur_key[k].1);
    }
    Ok(BoundaryPartial { key, partial })
}

/// The narrow ordered claim drive: the serial sorted-fold walk (bitmap mode
/// only — every complexity the big drive hosts is refused at admission or
/// demotes this ATTEMPT to the serial rerun), with the claim's two edge
/// groups exported instead of emitted.
#[allow(clippy::too_many_arguments)]
fn drive_claim<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    keys: &SortedFoldKeys,
    spec: &SortedByrefSpec,
    natts: usize,
    range: runtime::MorselRange,
    budget_left: usize,
    estate: &mut EStateData<'mcx>,
) -> Result<ClaimRec, AcceptFail> {
    let nkeys = keys.n;
    let mut rec = ClaimRec {
        start: range.start,
        end: range.end,
        first: None,
        interior: None,
        last: None,
        spanning: false,
    };
    let mut acc: Option<SortedEmitAcc> = None;
    let mut cur_key = [(Datum::null(), false); SORTED_FOLD_MAX_KEYS];
    let mut group_open = false;
    let mut first_done = false;
    // Belt: no pending boundary tuple may survive a previous claim.
    if ::nodeagg::agg_sorted_have_pending(agg) {
        return Err(AcceptFail::Error(sink_shape_error(
            "sorted claim drive entered with a pending boundary tuple",
        )));
    }
    // Granule length-stats meta-fold (lane-v2-lenfooter), the serial fold
    // drive's between-windows arm threaded into the claim: whole INTERIOR
    // granules of the OPEN group are answered from v7 footer metadata with
    // no decode. Sound inside a claim because the scan is range-bound
    // (seq_scan_set_morsel_range) and granule_meta_peek stops at the
    // claim's range_end — coverage stays exact and disjoint. Byte-identity
    // is the serial arm's own argument verbatim (fold_granule_meta is
    // bit-equal to fold_batch over the granule's selection), and the
    // claim's edge-group PARTIALS read the same pergroup states the fold
    // mutates.
    let meta = super::sorted_fold_meta_ctx(agg, ss, keys);
    loop {
        // Between windows: consume whole interior granules of the open
        // group from footer metadata first (the serial drive's order).
        if group_open {
            if let Some(mf) = &meta {
                super::sorted_fold_meta_granules(agg, ss, keys, &cur_key, mf, estate)?;
            }
        }
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Per-batch capture budget (R3): a whole-boundary claim's interior
        // capture must not balloon past the Local's remaining envelope —
        // refuse mid-claim (serial rerun), never exhaust memory first.
        if acc.as_ref().is_some_and(|a| a.bytes() > budget_left) {
            return Err(AcceptFail::Budget);
        }
        let nwords = (n as usize).div_ceil(64);
        // Fail-closed window verdicts: fallback rows and non-datum-ready key
        // lanes are shapes the narrow drive refuses — RG abort → serial
        // rerun (never a partial fold).
        {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .ok_or_else(|| sink_shape_error("sorted claim drive without a staged SoA"))?;
            if soa.fallback_words()[..nwords].iter().any(|&w| w != 0) {
                return Err(AcceptFail::Budget);
            }
            for &(c, _) in &keys.cols[..nkeys] {
                if !soa.col_datum_ready(c as usize) {
                    return Err(AcceptFail::Budget);
                }
            }
        }
        let mut sel = [u64::MAX; ::exectuples::SOA_BM_WORDS];
        match ::nodeseqscan::seq_scan_batch_qual_sel(ss) {
            Some(s) => sel[..nwords].copy_from_slice(&s[..nwords]),
            None => {
                if ss.ss.qual.is_some() {
                    // Qualled scan without a bitmap verdict this window
                    // (requal/scalar tail) — the narrow drive refuses.
                    return Err(AcceptFail::Budget);
                }
            }
        }
        if n % 64 != 0 {
            sel[nwords - 1] &= (1u64 << (n % 64)) - 1;
        }
        // GUARDED plans (the real length()-agg class — charlen multibyte guard on
        // avg(length(url))): the serial fold's per-window guard re-proof,
        // verbatim (zone minmax answers first, then check_guards over the
        // selected non-fallback rows). A Demote verdict is a fail-closed
        // REFUSAL here (the narrow drive hosts no checked per-row program —
        // serial rerun raises C's error at C's row if the data is bad).
        {
            let guarded = ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| p.guarded);
            if guarded {
                let mut zmm = [(0u16, (0i64, 0i64)); 8];
                let mut nz = 0usize;
                {
                    let plan = ::nodeagg::agg_lanefold_plan(agg)
                        .ok_or_else(|| sink_shape_error("guard proof without a plan"))?;
                    for g in plan.guards.iter() {
                        if nz == zmm.len() {
                            break;
                        }
                        if let Some(mm) =
                            ::nodeseqscan::seq_scan_window_value_minmax(ss, g.col as usize)
                        {
                            zmm[nz] = (g.col, mm);
                            nz += 1;
                        }
                    }
                }
                let plan = ::nodeagg::agg_lanefold_plan(agg)
                    .ok_or_else(|| sink_shape_error("guard proof without a plan"))?;
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .ok_or_else(|| sink_shape_error("guard proof without a staged SoA"))?;
                // Proof domain: the selection (fallback words are all zero —
                // checked above); PREWHERE lane sel is a superset of the
                // fold's touched rows when present (the serial discipline).
                let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
                match ::nodeseqscan::seq_scan_batch_lane_sel(ss) {
                    Some(ls) => rows[..nwords].copy_from_slice(&ls[..nwords]),
                    None => rows[..nwords].copy_from_slice(&sel[..nwords]),
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                if rows[..nwords].iter().any(|&w| w != 0) {
                    // SAFETY: proof rows are staged non-fallback selected
                    // rows with live deformed lane values (the completing
                    // deform filled every prefix column, vguard columns
                    // included — the staging prefix chains plan.vguards;
                    // len-staged columns skip their proofs inside
                    // check_guards).
                    let demote = unsafe {
                        ::lanefold::check_guards(plan, soa, &rows[..nwords], |c| {
                            zmm[..nz].iter().find(|e| e.0 == c).map(|e| e.1)
                        }) == ::lanefold::GuardCheck::Demote
                    };
                    if demote {
                        return Err(AcceptFail::Budget);
                    }
                }
            }
        }
        let mut run = [0u64; ::exectuples::SOA_BM_WORDS];
        let mut run_any = false;
        macro_rules! flush_run {
            () => {
                if run_any {
                    let plan = ::nodeagg::agg_lanefold_plan(agg)
                        .ok_or_else(|| sink_shape_error("sorted claim drive without a plan"))?;
                    let aggcx = ::nodeagg::agg_aggcontext(agg);
                    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .ok_or_else(|| sink_shape_error("claim drive lost its SoA"))?;
                    // SAFETY: pergroup contract identical to the serial
                    // sorted fold's flush_run (once-allocated current-group
                    // array, initialize_aggregates rewrote it at group
                    // begin); run rows are selected non-fallback rows with
                    // live deformed lane values for every plan column; the
                    // plan is UNGUARDED with no vguards (admission) so no
                    // re-proof is needed; Str min/max kinds are refused so
                    // no dict-code views arise.
                    unsafe {
                        ::lanefold::fold_batch(
                            plan,
                            &super::CodesCols { inner: soa, codes: &[] },
                            &run[..nwords],
                            n as usize,
                            ::nodeagg::agg_sorted_pergroup_base(agg),
                            aggcx,
                        )?;
                    }
                    run[..nwords].fill(0);
                    run_any = false;
                }
            };
        }
        let mut i = 0u32;
        while i < n {
            // Phase A (staged reads only): extend the open group's run to
            // the next event — a group boundary or window end. Skipped rows
            // are qual rejections (the bitmap IS the verdict).
            let boundary = {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .ok_or_else(|| sink_shape_error("claim drive lost its SoA"))?;
                let mut key_vals: [&[Datum]; SORTED_FOLD_MAX_KEYS] =
                    [&[]; SORTED_FOLD_MAX_KEYS];
                let mut key_nulls: [&[bool]; SORTED_FOLD_MAX_KEYS] =
                    [&[]; SORTED_FOLD_MAX_KEYS];
                for k in 0..nkeys {
                    key_vals[k] = soa.col_values(keys.cols[k].0 as usize);
                    key_nulls[k] = soa.col_isnull(keys.cols[k].0 as usize);
                }
                let mut boundary = None;
                let mut j = i;
                while j < n {
                    if sel[(j / 64) as usize] & (1u64 << (j % 64)) == 0 {
                        j += 1;
                        continue;
                    }
                    if !group_open {
                        boundary = Some(j);
                        break;
                    }
                    let same = (0..nkeys).all(|k| {
                        let (cv, cn) = cur_key[k];
                        let jn = key_nulls[k][j as usize];
                        if cn || jn {
                            cn && jn
                        } else {
                            super::sorted_key_datum_eq(
                                key_vals[k][j as usize],
                                cv,
                                keys.cols[k].1,
                            )
                        }
                    });
                    if !same {
                        boundary = Some(j);
                        break;
                    }
                    run[(j / 64) as usize] |= 1u64 << (j % 64);
                    run_any = true;
                    j += 1;
                }
                boundary
            };
            // Phase B: fold the accumulated run, then the boundary event.
            flush_run!();
            let Some(j) = boundary else {
                i = n;
                continue;
            };
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, j)? {
                if !group_open {
                    ::nodeagg::agg_sorted_group_begin(agg, estate, Some(slot))?;
                    ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
                    group_open = true;
                } else {
                    // Group END: save the boundary row first (the pull
                    // loop's order), then EXPORT (first) or finalize+emit+
                    // capture (interior), then begin the next group from
                    // the pending tuple.
                    ::nodeagg::agg_sorted_save_pending(agg, estate, slot)?;
                    if !first_done {
                        rec.first = Some(export_open_group(agg, &cur_key, nkeys)?);
                        first_done = true;
                    } else if let Some(row) = ::nodeagg::agg_sorted_emit(agg, estate)? {
                        let a = acc.get_or_insert_with(|| SortedEmitAcc::new(natts));
                        let s = estate.slot_mut(row);
                        let sb = s.base_mut();
                        // SAFETY: the projected row's non-null byref datums
                        // are live images (just projected); `spec` mirrors
                        // the result tupledesc (admission).
                        unsafe {
                            a.push_row(
                                &sb.tts_values[..natts],
                                &sb.tts_isnull[..natts],
                                spec,
                            )?;
                        }
                    }
                    ::nodeagg::agg_sorted_group_begin(agg, estate, None)?;
                    ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
                    group_open = true;
                }
            }
            i = j + 1;
        }
    }
    // Claim end: drop the scan slot's pin (end-of-stream parity) and export
    // the open edge group.
    {
        let mcx = estate.es_query_cxt;
        ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
    }
    if group_open {
        let p = export_open_group(agg, &cur_key, nkeys)?;
        if !first_done {
            rec.first = Some(p);
            rec.spanning = true;
        } else {
            rec.last = Some(p);
        }
    }
    rec.interior = acc.map(SortedEmitAcc::finish);
    Ok(rec)
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else {
        lane_trace("runtime-agg-sorted: post-task-park without a private payload");
        return;
    };
    let Ok(payload) = private.downcast::<RuntimeAggSortedShared>() else { return };
    // Every LAUNCHED helper bumps `exited` exactly once, on EVERY exit
    // path. HOOK-frame placement (the scan arm's law): the standing driver
    // reuses helper_drive and must NOT bump — standing exits ride the
    // board's claimed/detached accounting.
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(&payload)));
    if r.is_err() {
        payload
            .sink
            .fail(PgError::new(ERROR, "runtime sorted-agg helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The standing driver (M2 inc-1, parallel::set_standing_driver): the
/// POST_TASK_PARK body minus the ExitBump; exit-committed unwinds (FATAL)
/// rethrow to the gang glue (a terminated worker must die).
fn standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeAggSortedShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(&payload)));
    if let Err(unwind) = r {
        payload
            .sink
            .fail(PgError::new(ERROR, "runtime sorted-agg standing executor panicked").into());
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            shared.parallel_leader_proc_number,
        ));
        if parallel::standing::is_exit_unwind(&*unwind) {
            std::panic::resume_unwind(unwind);
        }
        return;
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(payload: &Arc<RuntimeAggSortedShared>) {
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-agg-sorted: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-agg-sorted: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-agg-sorted: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let lane = std::cell::RefCell::new(Some(lane));
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg, &mut lane.borrow_mut())
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                if !payload.sink.budget_refused.load(Ordering::SeqCst) {
                    payload.sink.fail(e);
                }
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!(
                    "runtime-agg-sorted: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeAggSortedShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    lane: &mut Option<runtime::ExternalLane>,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _end = super::standing_channel::drive_pool_serve(&payload.rt, local, rg, lane);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // Route the binder through its transaction-ABORT unbind (a released
        // executor may still hold registered snapshots — the runtime_agg F1
        // discipline, verbatim).
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime sorted-agg worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

/// Build + ARM this helper's executor over the shared worker PlannedStmt.
/// Divergence from the leader's admission is an ERROR (the leader proved the
/// shape; a worker that cannot reproduce it must not silently build
/// something else).
fn build_worker_exec(payload: &Arc<RuntimeAggSortedShared>) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        if let Some(stale) = cell.borrow_mut().take() {
            crate::querydesc::release_query_desc_seam(stale.qd);
        }
        // SAFETY: leader-arena pstmt, alive until DestroyParallelContext
        // joins this helper (SendConst contract).
        let pstmt: &PlannedStmt<'_> = unsafe { &*payload.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &payload.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let armed = (|| -> PgResult<(SortedFoldKeys, SortedByrefSpec, usize)> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime sorted-agg worker ExecutorStart");
                x.with_mut(|d| -> PgResult<(SortedFoldKeys, SortedByrefSpec, usize)> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(sink_shape_error("sorted worker plan root is not an Agg"));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(sink_shape_error(
                            "sorted worker outer node is not a SeqScan",
                        ));
                    };
                    arm_sorted_build(&mut aps.agg, ss, estate)
                })
            })
        })();
        match armed {
            Ok((keys, spec, natts)) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    errored: std::cell::Cell::new(false),
                    keys,
                    spec,
                    natts,
                });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

/// The worker's build arm: the serial sorted-fold decide's own admission +
/// staging sequence, re-checked (divergence = error).
fn arm_sorted_build<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(SortedFoldKeys, SortedByrefSpec, usize)> {
    if !sorted_arm_shape_ok(agg) {
        return Err(sink_shape_error("worker sorted-fold shape diverged from the leader's"));
    }
    let Some(keys) = super::sorted_fold_key_cols(agg, ss) else {
        return Err(sink_shape_error("worker sorted key shape diverged from the leader's"));
    };
    if !arm_sorted_staging(agg, ss, &keys, estate)? {
        return Err(sink_shape_error("worker sorted staging refused"));
    }
    let Some(spec) = ::nodeagg::sortedsink::agg_sorted_result_byref_spec(agg) else {
        return Err(sink_shape_error("worker result byref spec diverged from the leader's"));
    };
    let natts = spec.len();
    Ok((keys, spec, natts))
}

/// The structural fold-shape gate shared by leader admission and worker
/// re-check: sorted-FOLD admissible, unguarded classified plan with no
/// vguards and no residuals, every transition runtime-partial combinable.
fn sorted_arm_shape_ok(agg: &::nodeagg::AggStateData<'_>) -> bool {
    ::nodeagg::agg_sorted_fold_admissible(agg)
        // GUARDED plans admit — including vguard/uguard (varlena inline-form
        // + UTF-8 countability) obligations, which EVERY length/str lane
        // carries (the bank's real avg(length(URL)) class): the claim drive
        // runs the serial fold's exact `check_guards` per window (len-staged
        // columns skip their proofs there) and REFUSES fail-closed on a
        // Demote verdict (no checked per-row program exists in the narrow
        // drive). Residual transitions still refuse; str/bp/f min-max kinds
        // refuse through the runtime-partial admission below.
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| p.resid.is_empty())
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::runtime_partial::agg_runtime_partial_admissible(agg)
}

/// The serial fold decide's staging arm, verbatim (PREWHERE for qualled
/// scans, the offset-free columnar arm otherwise; prefix widened to fold +
/// key columns; length lanes armed). `false` = staging refused.
fn arm_sorted_staging<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    keys: &SortedFoldKeys,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let plan = ::nodeagg::agg_lanefold_plan(agg)
        .ok_or_else(|| sink_shape_error("sorted staging arm without a fold plan"))?;
    let mut maxcol = 0i32;
    for &c in plan.cols.iter().chain(plan.vguards.iter()) {
        maxcol = maxcol.max(c as i32);
    }
    for &(c, _) in &keys.cols[..keys.n] {
        maxcol = maxcol.max(c as i32);
    }
    let prefix = maxcol + 1;
    let armed = if ss.ss.qual.is_some() {
        ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix)?
    } else {
        ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None)
    };
    if !armed || ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
        return Ok(false);
    }
    super::arm_fold_len_lanes(agg, ss);
    Ok(true)
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        let Some(ex) = cell.borrow_mut().take() else { return Ok(()) };
        if clean {
            let r = crate::execmain::executor_finish_seam(ex.qd)
                .and_then(|()| crate::execmain::executor_end_seam(ex.qd));
            match r {
                Ok(()) => {
                    crate::querydesc::free_query_desc_seam(ex.qd);
                    Ok(())
                }
                Err(e) => {
                    crate::querydesc::release_query_desc_seam(ex.qd);
                    Err(e)
                }
            }
        } else {
            crate::querydesc::release_query_desc_seam(ex.qd);
            Ok(())
        }
    })
}

fn private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeAggSortedShared>() else { return };
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    if let Some(rg) = &rg {
        rg.abort();
    }
    // Standing channel (M2 inc-1): complete the standing join on leader
    // unwind paths (standing_channel::shutdown_standing_join).
    super::standing_channel::shutdown_standing_join(&payload.standing, rg.as_ref(), &|rg| {
        drain_rg(payload.rt, rg)
    });
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_agg_sorted_main",
            worker_main,
        );
        parallel::register_parallel_post_task_park(post_task_park);
        parallel::register_parallel_private_shutdown(private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SORTED_MIN_GRANULES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64)
    })
}

fn split_claims() -> bool {
    static B: OnceLock<bool> = OnceLock::new();
    crate::once_val(&B, || {
        matches!(std::env::var("PGRUST_RUNTIME_AGG_SORTED_SPLIT").as_deref(), Ok("1"))
    })
}

/// GL-Q2829-FIX-1 auto-arm (`PGRUST_RUNTIME_AGG_SORTED_AUTO`, DEFAULT ON
/// since the string-agg-with-HAVING-class flip — Michael, 2026-07-22:
/// "4 sounds good" to the auto-arm default; `=0|off` disarms, restoring
/// the bench-GUC-only read byte-exactly): the ordered-grouped arm
/// resolves its DOP through the router's agg-class resolution (bench GUC
/// verbatim when set; else engine=runtime arms at `pgrust.runtime_dop`)
/// — the hashed sink's exact arming — instead of the bench-GUC-only read.
/// Closes the stock-defaults engagement hole on the suppressed presorted
/// grouped class: the m5 carve deletes the exchange frame expecting the
/// runtime engine to own the serial-shaped plan, but the ordered face
/// never armed without an explicit `SET pgrust.runtime_agg_pool`, so the
/// plan ran the SERIAL sorted-fold drive (measured 6-13x off the engaged
/// arm on the class's composed shape, laptop 10M + fleet 100M born-RED
/// legs). The arm's own kill (`PGRUST_RUNTIME_AGG_SORTED`) and every
/// admission gate below are unchanged — auto-arm only widens the DOP
/// source, and every refusal still lands the serial arm byte-identically.
fn sorted_auto_enabled() -> bool {
    static B: OnceLock<bool> = OnceLock::new();
    crate::once_val(&B, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_SORTED_AUTO").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Non-group Var reference census over the Agg's plan targetlist + qual:
/// `true` when every OUTER Var outside an Aggref subtree is a grouping
/// column (the boundary-group representative is then exactly reconstructible
/// from key datums). Fail-closed: any walk surprise refuses.
fn proj_qual_group_vars_only(agg: &::nodeagg::AggStateData<'_>) -> bool {
    use ::types_nodes::primnodes::Var;
    struct VarCensus<'a> {
        group: &'a [i16],
        bad: bool,
    }
    impl<'mcx> ::nodes_core::NodeWalker<'mcx> for VarCensus<'_> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if self.bad {
                return Ok(true);
            }
            match node.node_tag() {
                NodeTag::T_Var => {
                    let v = node.as_variant::<Var>().expect("tagged Var");
                    // OUTER, same-level, grouping-column Vars only: the
                    // boundary representative reconstructs exactly those.
                    // Anything else (upper-level/correlated/other-relation
                    // vars — mostly unreachable past the binder/param
                    // gates, but fail-closed) refuses.
                    if v.varlevelsup != 0
                        || v.varno != ::types_nodes::primnodes::OUTER_VAR
                        || !self.group.contains(&v.varattno)
                    {
                        self.bad = true;
                        return Ok(true);
                    }
                    Ok(false)
                }
                // Aggref subtrees are transition inputs — finalize never
                // reads the representative through them.
                NodeTag::T_Aggref => Ok(false),
                _ => ::nodes_core::expression_tree_walker(node, self),
            }
        }
    }
    let group = ::nodeagg::agg_plan_group_cols(agg);
    let mut w = VarCensus { group, bad: false };
    let plan = &agg.plan.plan;
    for n in &plan.targetlist {
        if ::nodes_core::expression_tree_walker(n, &mut w).is_err() {
            return false;
        }
    }
    for n in &plan.qual {
        if ::nodes_core::NodeWalker::visit(&mut w, n).is_err() {
            return false;
        }
    }
    !w.bad
}

/// The ordered-grouped runtime arm. `false` = not engaged (caller falls
/// through to the serial sorted decide, byte-identically — nothing was
/// consumed). `true` = the stitched ordered result was adopted; the caller
/// drains it through `agg_sorted_sink_emit_next`.
pub(super) fn try_engage_sortedagg_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // Auto-arm (fn doc at sorted_auto_enabled): knob-ON resolves DOP through
    // the router's agg-class arming; knob-OFF keeps the bench-GUC-only read.
    let dop = if sorted_auto_enabled() {
        super::router::arm_dop(super::router::ArmClass::Agg)
    } else {
        ::guc_tables::runtime_pool::runtime_agg_pool_dop()
    };
    if dop <= 0
        || !::guc_tables::runtime_pool::runtime_agg_sorted_env_ok()
        || !runtime::runtime_enabled()
    {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };

    fn refuse(why: &'static str) {
        lane_trace(&format!("runtime-agg-sorted: refused ({why})"));
    }
    // EXPLAIN ANALYZE refuses in v1 (the serial EA arms keep their exact
    // instrument surfaces).
    if estate.es_instrument != 0 {
        refuse("explain analyze (v1)");
        return Ok(false);
    }
    if estate.es_epq_active {
        return Ok(false);
    }
    // --- Plan/shape gates (fail-closed). The caller proved pgrcolumnar +
    // seq_scan_fusible before the choice memo; re-checked cheaply here.
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        refuse("scan not fusible cbstore");
        return Ok(false);
    }
    if ss.ss.ps_ProjInfo.is_some() {
        refuse("projected scan");
        return Ok(false);
    }
    if !sorted_arm_shape_ok(agg) {
        // Leg-resolved refusal trace (fleet diagnosis channel).
        if !::nodeagg::agg_sorted_fold_admissible(agg) {
            refuse("sorted fold shape (fold admission)");
        } else if let Some(p) = ::nodeagg::agg_lanefold_plan(agg) {
            if !p.resid.is_empty() || ::nodeagg::agg_lanefold_has_resid(agg) {
                refuse("sorted fold shape (residual transitions)");
            } else {
                refuse("sorted fold shape (runtime-partial kinds)");
            }
        } else {
            refuse("sorted fold shape (no fold plan)");
        }
        return Ok(false);
    }
    let Some(keys) = super::sorted_fold_key_cols(agg, ss) else {
        refuse("key shape");
        return Ok(false);
    };
    if !proj_qual_group_vars_only(agg) {
        refuse("non-group column reference in proj/qual");
        return Ok(false);
    }
    let Some(spec) = ::nodeagg::sortedsink::agg_sorted_result_byref_spec(agg) else {
        refuse("result column class");
        return Ok(false);
    };
    let natts = spec.len();
    // The staging the workers will arm — proven on the leader's own scan
    // (same table, same plan; the serial decide arms the same shape on
    // fallback, so this is idempotent with the refusal path).
    if !arm_sorted_staging(agg, ss, &keys, estate)? {
        refuse("staging");
        return Ok(false);
    }
    // --- Session/binder gates (the M1 set, verbatim).
    if super::runtime_in_parallel_machinery(ss) {
        refuse("in parallel mode");
        return Ok(false);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse("extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refuse("no planned stmt");
        return Ok(false);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse("exec params");
        return Ok(false);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refuse("non-MVCC snapshot");
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refuse("binder policy");
        return Ok(false);
    }
    let Some(root) = leader_pstmt.planTree else {
        refuse("no plan tree");
        return Ok(false);
    };
    let Some(agg_node) = super::runtime_agg::find_agg_node(root, agg.plan) else {
        refuse("agg node not in plan tree");
        return Ok(false);
    };
    if agg.plan.plan.lefttree.map(Node::node_tag) != Some(NodeTag::T_SeqScan) {
        refuse("scan child shape");
        return Ok(false);
    }
    // --- Geometry.
    let Some((total_granules, starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        refuse("granule geometry unavailable (no columnar part)");
        return Ok(false);
    };
    if total_granules < min_granules().max(2 * dop as u64) {
        refuse("granule floor");
        return Ok(false);
    }
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill: PGRUST_RUNTIME_ELASTIC_DOP=0).
    let dop = super::runtime_scan::elastic_dop(dop, total_granules);
    // --- Engage.
    let budget = ::execgrouping::get_hash_memory_limit() as usize;
    let mut key_lens = [0i16; SORTED_FOLD_MAX_KEYS];
    for k in 0..keys.n {
        key_lens[k] = keys.cols[k].1;
    }
    let sink = Arc::new(SortedAggSink {
        budget,
        nkeys: keys.n,
        key_lens,
        collected: Mutex::new(None),
        rg: OnceLock::new(),
        failed: AtomicBool::new(false),
        error: Mutex::new(None),
        budget_refused: AtomicBool::new(false),
    });
    engage(agg, estate, rt, dop, total_granules, starts, agg_node, sink, &keys, &spec, natts)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    agg_node: Node<'mcx>,
    sink: Arc<SortedAggSink>,
    keys: &SortedFoldKeys,
    spec: &SortedByrefSpec,
    natts: usize,
) -> PgResult<bool> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;
    let payload = Arc::new(RuntimeAggSortedShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (runtime_agg precedent).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        sink: Arc::clone(&sink),
        query_id: AtomicU64::new(0),
        standing: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        agg, estate, rt, dop, total_granules, starts, &payload, &sink, keys, spec, natts,
    );
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

/// This arm's standing-channel constants (M2 inc-1; see
/// standing_channel::StandingArm — sinks_gate: PGRUST_RUNTIME_POOLBIND_SINKS).
static STANDING_ARM: super::standing_channel::StandingArm =
    super::standing_channel::StandingArm {
        label: "runtime-agg-sorted",
        died: "runtime sorted-agg standing executors exited before completing the aggregation",
        sinks_gate: true,
    };

/// Shared post-outcome tail (standing and launched channels): worker-phase
/// errors rethrow PLAIN; budget refusals take the R5 whole-attempt serial
/// rerun; an unexplained abort surfaces the pending interrupt or reports;
/// completed-but-nobody-participated falls back serially.
fn finish_outcome(
    payload: &Arc<RuntimeAggSortedShared>,
    sink: &Arc<SortedAggSink>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
    if let Some(e) = sink.take_error() {
        return Err(e);
    }
    if sink.budget_refused.load(Ordering::SeqCst) {
        lane_trace("runtime-agg-sorted: budget refusal — falling back to the serial arm");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(EngageOutcome::Fallback);
    }
    if outcome == runtime::RgOutcome::Aborted {
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "runtime sorted-agg pipeline aborted")));
    }
    if payload.started.load(Ordering::SeqCst) == 0 {
        return Ok(EngageOutcome::Fallback);
    }
    Ok(EngageOutcome::Completed)
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeAggSortedShared>,
    sink: &Arc<SortedAggSink>,
    keys: &SortedFoldKeys,
    spec: &SortedByrefSpec,
    natts: usize,
) -> PgResult<bool> {
    let pcxt =
        parallel::CreateParallelContext("postgres", "pgrust_runtime_agg_sorted_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        // Standing driver dispatch (M2 inc-1): deferred_bind false — this
        // arm binds EAGERLY (with_query_task_binding); the standing serve
        // re-establishes visibility up front and evicts parked sticky.
        parallel::set_standing_driver(pcxt, parallel::standing::StandingDriver {
            drive: standing_driver,
            deferred_bind: false,
        });
        // M2 inc-2: the POOL-DB channel — built BEFORE submit (the bound
        // descriptor must ride the submission: publication keys the
        // pool-visible active bit off it); sinks_gate: POOLBIND_SINKS=0
        // retires this channel with the gang's. None = plain pinned
        // submit, inc-1 byte-exactly.
        let pool = super::standing_channel::try_pool_channel(
            payload.pcxt_shared.get().expect("pcxt shared set above"),
            dop,
            /* sinks_gate */ true,
        );


        let source = Arc::new(SortedGranuleSource { starts, whole: !split_claims() });
        let runtime::SinkTaskSets { accept, combine, probe: _probe } = runtime::sink_tasksets(
            Arc::clone(sink),
            source,
            rt.nthreads(),
            0,
        );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let qid = NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64;
        payload.query_id.store(qid, Ordering::SeqCst);
        let spec = runtime::QuerySpec { query_id: qid, tasksets: vec![accept, combine] };
        // rg-set-BEFORE-publish (M2 inc-3 rung 3): every serve-visible rg
        // cell is stored by on_rg before the bound submission can become
        // pool-visible — no "rg gone" refusal churn window.
        let set_rg = |rg: &runtime::RgHandle| {
            payload
                .rg
                .set(rg.downgrade())
                .unwrap_or_else(|_| unreachable!("rg set once"));
            sink.rg
                .set(rg.downgrade())
                .unwrap_or_else(|_| unreachable!("sink rg set once"));
        };
        let (rg, waiter) = match &pool {
            Some((_, descriptor)) => {
                rt.submit_pinned_bound(spec, 0, descriptor.clone(), set_rg)
            }
            None => {
                let (rg, waiter) = rt.submit_pinned(spec);
                set_rg(&rg);
                (rg, waiter)
            }
        };
        *mut_submitted = Some(rg.clone());

        // M2 inc-1: STANDING engagement first — no worker launch, one
        // binder bind per participant; fallback leaves the RG untouched
        // for the launched path below.
        match super::standing_channel::standing_wait(
            &STANDING_ARM,
            super::standing_channel::StandingLeader {
                // M2 inc-2: the pool-db board attached at submit (None =
                // gang-first, inc-1 exactly).
                pool: pool.as_ref().map(|(entry, _)| Arc::clone(entry)),
                shared: payload.pcxt_shared.get().expect("pcxt shared set above"),
                slot: &payload.standing,
                started: &payload.started,
                refused: &payload.refused,
                take_error: &|| sink.take_error(),
                drain: &|rg| drain_rg(rt, rg),
                census: "",
            },
            dop,
            total_granules,
            &rg,
            &waiter,
        )? {
            super::standing_channel::StandingWait::Done(outcome) => {
                return finish_outcome(payload, sink, outcome);
            }
            super::standing_channel::StandingWait::Fallback => {}
        }

        // M2 inc-3 rung 4: the launched-bgworker fallback is DELETED — a
        // board decline goes straight to the serial arm (pool → gang →
        // serial; the NOLAUNCH posture made permanent). Cause attribution
        // ticks the nolaunch-serial floor row inside the shared helper.
        super::standing_channel::launched_fallback_retired(&STANDING_ARM);
        drain_rg(rt, &rg);
        Ok(EngageOutcome::Fallback)
    })(&mut submitted);

    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    match outcome {
        EngageOutcome::Fallback => {
            stats::tick_engaged(STANDING_ARM.label, stats::EngageChannel::Serial);
            lane_trace("runtime-agg-sorted: fallback to serial arm");
            Ok(false)
        }
        EngageOutcome::Completed => {
            let recs = sink
                .collected
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
                .ok_or_else(|| sink_shape_error("completed sorted sink published nothing"))?;
            let segs = stitch_and_finalize(
                agg,
                estate,
                recs,
                total_granules,
                sink,
                keys,
                spec,
                natts,
            )?;
            let groups: usize = segs.iter().map(|s| s.nrows).sum();
            lane_trace(&format!("runtime-agg-sorted: complete, groups={groups}"));
            ::nodeagg::sortedsink::agg_sorted_sink_adopt(agg, segs, natts);
            stats::tick_owned(ShapeClass::AggBuild);
            Ok(true)
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive (runtime_agg's
/// hardened drain, verbatim).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-agg-sorted: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-agg-sorted: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}

// ---------------------------------------------------------------------------
// Leader stitch: combine adjacent edge partials, finalize boundary groups
// through the node's own emit seam, splice with the captured interiors.
// ---------------------------------------------------------------------------

/// Width-masked key equality over exported raw key words — exactly the
/// boundary compare's verdict (NULL keys group together).
fn stitch_key_eq(
    a: &[(u64, bool); SORTED_FOLD_MAX_KEYS],
    b: &[(u64, bool); SORTED_FOLD_MAX_KEYS],
    nkeys: usize,
    lens: &[i16; SORTED_FOLD_MAX_KEYS],
) -> bool {
    (0..nkeys).all(|k| {
        let (av, an) = a[k];
        let (bv, bn) = b[k];
        if an || bn {
            an && bn
        } else {
            super::sorted_key_datum_eq(Datum::from_u64(av), Datum::from_u64(bv), lens[k])
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn stitch_and_finalize<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    recs: Vec<ClaimRec>,
    total_granules: u64,
    sink: &SortedAggSink,
    keys: &SortedFoldKeys,
    spec: &SortedByrefSpec,
    natts: usize,
) -> PgResult<Vec<SortedEmitSeg>> {
    // Coverage law: the claims tile [0, total) exactly once (shared-cursor
    // CAS); a gap/overlap is an engagement bug, never silent.
    let mut expect = 0u64;
    for r in &recs {
        if r.start != expect {
            return Err(sink_shape_error("sorted claim coverage gap"));
        }
        expect = r.end;
    }
    if expect != total_granules {
        return Err(sink_shape_error("sorted claim coverage short"));
    }
    let nkeys = sink.nkeys;
    // Representative reconstruction inputs: key datums at their 0-based
    // outer columns, everything else NULL (admission proved proj/qual read
    // only group columns). The minimal tuple forms directly into
    // persort.first_slot inside the stitch seam — no per-engagement scratch
    // slot exists (an extra estate slot per re-engagement would leak across
    // rescans).
    let mut key_cols = [0u16; SORTED_FOLD_MAX_KEYS];
    for k in 0..nkeys {
        key_cols[k] = keys.cols[k].0;
    }
    let mut segs: Vec<SortedEmitSeg> = Vec::new();
    let mut bacc: Option<SortedEmitAcc> = None;
    let mut open: Option<BoundaryPartial> = None;

    macro_rules! emit_boundary {
        ($p:expr) => {{
            let p: BoundaryPartial = $p;
            let mut kd = [(Datum::null(), false); SORTED_FOLD_MAX_KEYS];
            for k in 0..nkeys {
                kd[k] = (Datum::from_u64(p.key[k].0), p.key[k].1);
            }
            ::nodeagg::sortedsink::agg_sorted_stitch_begin_keys(
                agg,
                estate,
                &kd[..nkeys],
                &key_cols[..nkeys],
            )?;
            ::nodeagg::runtime_partial::agg_sorted_absorb_partial(agg, &p.partial)?;
            if let Some(row) = ::nodeagg::agg_sorted_emit(agg, estate)? {
                let a = bacc.get_or_insert_with(|| SortedEmitAcc::new(natts));
                let s = estate.slot_mut(row);
                let sb = s.base_mut();
                // SAFETY: freshly projected row; spec mirrors the result
                // tupledesc (admission).
                unsafe {
                    a.push_row(&sb.tts_values[..natts], &sb.tts_isnull[..natts], spec)?;
                }
            }
        }};
    }

    for rec in recs {
        let ClaimRec { first, interior, last, spanning, .. } = rec;
        if first.is_none() {
            // No surviving rows in this claim: the open group persists
            // across it (the store is key-clustered; survivors of one key
            // are contiguous, so a fully-filtered claim cannot separate
            // same-key survivors).
            continue;
        }
        if spanning {
            let p = first.expect("spanning claim has a partial");
            match open.as_mut() {
                Some(o) if stitch_key_eq(&o.key, &p.key, nkeys, &sink.key_lens) => {
                    ::nodeagg::runtime_partial::agg_runtime_combine_into(
                        agg, &mut o.partial, &p.partial,
                    )?;
                }
                Some(_) => {
                    let o = open.take().expect("checked Some");
                    emit_boundary!(o);
                    open = Some(p);
                }
                None => open = Some(p),
            }
            continue;
        }
        // Non-spanning: resolve the left edge...
        let pf = first.expect("non-spanning claim with rows has a first partial");
        match open.take() {
            Some(mut o) if stitch_key_eq(&o.key, &pf.key, nkeys, &sink.key_lens) => {
                ::nodeagg::runtime_partial::agg_runtime_combine_into(
                    agg, &mut o.partial, &pf.partial,
                )?;
                emit_boundary!(o);
            }
            Some(o) => {
                emit_boundary!(o);
                emit_boundary!(pf);
            }
            None => emit_boundary!(pf),
        }
        // ...then the captured interior (flush pending boundary rows first
        // to preserve order)...
        if let Some(seg) = interior {
            if seg.nrows > 0 {
                if let Some(a) = bacc.take() {
                    if !a.is_empty() {
                        segs.push(a.finish());
                    }
                }
                segs.push(seg);
            }
        }
        // ...and the right edge stays open for the next claim.
        open = Some(last.ok_or_else(|| {
            sink_shape_error("non-spanning sorted claim without an open right edge")
        })?);
    }
    if let Some(o) = open.take() {
        emit_boundary!(o);
    }
    if let Some(a) = bacc.take() {
        if !a.is_empty() {
            segs.push(a.finish());
        }
    }
    Ok(segs)
}

// ---------------------------------------------------------------------------
// Morsel source: whole-boundary claims over the pgrcolumnar granule geometry.
// ---------------------------------------------------------------------------

/// Granule-addressed morsel source (runtime_agg's copy) that opts into
/// WHOLE-BOUNDARY claims: one claim = one dict-epoch row group (the
/// runtime-drive-scaling law — mid-RG splits duplicate dict decompress+memo
/// per worker, and RG-aligned claims minimize boundary partials).
struct SortedGranuleSource {
    starts: Vec<u64>,
    whole: bool,
}

impl runtime::MorselSource for SortedGranuleSource {
    fn total_granules(&self) -> u64 {
        self.starts.last().copied().unwrap_or(0)
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        match self.starts.binary_search(&start) {
            Ok(i) => self.starts.get(i + 1).copied().unwrap_or_else(|| self.total_granules()),
            Err(i) => self.starts.get(i).copied().unwrap_or_else(|| self.total_granules()),
        }
    }

    fn startup_c0(&self) -> u64 {
        2
    }

    fn whole_boundary_claims(&self) -> bool {
        self.whole
    }
}

/// The engaged leader's per-pull emit step (dispatched from the sorted-agg
/// drive once the choice memo says SortedSink).
pub(super) fn sorted_sink_emit_step<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<::executils::ExecSlotId>> {
    ::nodeagg::sortedsink::agg_sorted_sink_emit_next(agg, estate)
}
