//! M2 RUNTIME AGGREGATION SINK — the parallel GROUP BY engagement
//! (docs/design/m2-sinks.md §2 donor A, notes/m2-agg-sink.md).
//!
//! Shape (phase 1): a SERIAL-plan hashed Agg (AGGSPLIT_SIMPLE) over an
//! unprojected cbstore SeqScan, K2 single-int-key compact class, byval
//! whitelist transitions with catalog combine functions, identity emit —
//! executed as one runtime ParallelSink (ACCEPT + COMBINE task sets) at
//! DOP N on the M1 pinned-RG machinery. The plan surface stays the serial
//! plan; engagement is FORCED/explicit:
//!
//!   PGRUST_RUNTIME=1  (pool spawned at postmaster start, M0 kill switch)
//!   SET pgrust.runtime_agg_pool = <dop>   (never consulted by the planner)
//!
//! Execution model:
//!  * LEADER: admission (fail-closed — every refusal is the serial arm,
//!    byte-identically) → parallel context + query-task binding policy →
//!    submit a PINNED RG with the sink's two task sets → launch N helpers →
//!    park (WaitForParallelWorkersToFinish-shaped loop). On completion it
//!    adopts the published per-bucket EmitBufs and becomes a pure emitter.
//!  * HELPERS (bound, at POST_TASK_PARK): build a thread-local executor
//!    over the worker PlannedStmt (root = the Agg subtree), arm the SINK
//!    build (staging + K2 + compact table under the sink cap), then drive
//!    the pinned RG: ACCEPT morsels run the narrow ranged drain below
//!    (survivor collect → compact batch probe → whole-batch fold — the
//!    serial lane's own kernels over the claimed granule range); COMBINE
//!    morsels merge one radix bucket across all sealed Locals and
//!    finalize+project it (paremit) into a self-contained EmitBuf.
//!  * Local discipline (R3/R5): the worker's compact table lives in its
//!    sink Local between morsels (lend/reclaim by move); at the sink cap it
//!    flushes into a radix-partitioned SinkRun; table + run bytes are
//!    budgeted against `work_mem × hash_mem_multiplier` per Local — a
//!    crossing records a BUDGET REFUSAL, aborts the RG, and the leader
//!    falls back to the serial arm (whole-attempt rerun; nothing consumed
//!    twice).
//!
//! WFIN markers (M0 acceptance instrument contract): emitted by the
//! runtime's generic sched.rs channel under `PGRUST_MORSEL_MARKERS=1` —
//! `MORSEL|WFIN|qid=..|pipe=..|worker=..|t_us=..|tasks=..|task_avg_us=..`
//! per (worker, task set); pipe = task-set index (0 = ACCEPT, 1 = COMBINE).
//! The arm's own duplicate emitter was removed at m2-integration: with the
//! sched channel armed, double emission (different time bases) garbled the
//! instrument parser's spread verdicts.

use core::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::EStateData;
use ::nodeagg::sink::{
    sink_build_emit_plan, sink_combine_bucket, sink_emit_bucket, sink_partition_remainder,
    sink_resolve_combines, SinkCombineFn, SinkEmitBuf, SinkEmitPlan, SinkKeySpec,
    SinkLocalView, SinkPart, SinkRun, SinkTableHandle, SINK_NBUCKETS,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, ScanFeedShape, ScanK2Scratch};

// ---------------------------------------------------------------------------
// The sink: ParallelSink impl + engagement-shared control state.
// ---------------------------------------------------------------------------

/// Per-worker sink Local: flushed runs + the owned compact table between
/// morsels (+ its SEAL partition).
#[derive(Default)]
pub(super) struct AggSinkLocal {
    runs: Vec<SinkRun>,
    run_bytes: usize,
    table: Option<SinkTableHandle>,
    part: Option<SinkPart>,
}

/// Which worker drain feeds the sink build.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SinkDrain {
    /// Unprojected scan, K2 single-int-key batch probe.
    K2,
    /// Unprojected scan, packed multi-int composite key (Mk car, q41/q42
    /// class) — `scan_mk_batch` per staged batch, fail-closed off-compact.
    /// Int components only: the packed image is value-derived, so worker
    /// tables merge on the canonical key words verbatim (no per-worker
    /// intern state, no numeric pack legality mid-build).
    Mk,
    /// Projected scan, expr-key feed (Arith/TsTrunc/Reduced kinds) —
    /// `exprkey_sink_batch` per staged batch, fail-closed off-compact.
    ExprKey,
}

struct AggSink {
    drain: SinkDrain,
    /// Reduced-key shape (worker arm re-derives and must match; the emit
    /// plan's Derived columns came from it). None = single-key.
    red: Option<::nodeagg::RedShape>,
    /// Packed multi-key shape (SinkDrain::Mk; worker arm re-derives and
    /// must match — the emit plan's MultiComp columns came from it).
    mk: Option<::nodeagg::MkShape>,
    cap: u32,
    /// Per-Local budget: work_mem × hash_mem_multiplier (R3 envelope).
    budget: usize,
    key_words: usize,
    state_bytes: usize,
    width: u8,
    /// Any byref state class present (PolyInt128/AvgInt8): the drain's
    /// budget accounting adds the aggcontext subtree (states live there).
    byref_states: bool,
    combines: Vec<SinkCombineFn>,
    emit: SinkEmitPlan,
    /// 256 per-bucket outputs; slot b is written only by the combine task
    /// that claimed partition b (single writer by the sink contract).
    out_emit: Vec<UnsafeCell<SinkEmitBuf>>,
    /// finalize's published output (leader consumes after completion).
    published: Mutex<Option<SinkPublished>>,
    /// TRUE TABLE ADOPT (dop1-tax2 inc-1) shape gate, fixed at construction:
    /// every emit column byval AND no byref combine state class — a byref
    /// transvalue points into a WORKER aggcontext, which dies with the
    /// helpers; byref shapes keep the EmitBuf arms (whose arena copy is what
    /// makes them self-contained).
    adopt_shape: bool,
    /// Seal-time hand-off: the single sealed Local's table + SEAL partition.
    /// Set only when the LIVE seal census admits (exactly one sealed Local,
    /// zero flushed runs, adopt_shape) — every combine claim then no-ops and
    /// finalize publishes the table wholesale (the ledger's literal "adopt
    /// its table (pointer hand-off)").
    adopted: Mutex<Option<(SinkTableHandle, SinkPart)>>,
    /// Lock-free mirror of `adopted` for the per-claim combine check
    /// (written once at SEAL, which happens-before every combine claim).
    adopted_flag: AtomicBool,
    /// Abort/observability control (shared with the engagement payload).
    rg: OnceLock<runtime::WeakRgHandle>,
    failed: AtomicBool,
    error: Mutex<Option<Box<PgError>>>,
    /// A Local crossed its memory budget: not an error — the leader falls
    /// back to the serial arm (R5 whole-attempt rerun).
    budget_refused: AtomicBool,
    /// Combine-phase retained CONTENT bytes (the per-bucket emit buffers,
    /// summed across claims) — the m2-integration R3 accounting for the
    /// merged RESULT, checked against the ADMITTED envelope (forked Locals
    /// × per-Local budget; see the check site). Crossing = budget refusal.
    /// LIFETIME NOTE: this sink object is strictly per-engagement
    /// (constructed in try_engage_hashagg_runtime); if sink regeneration
    /// (the M1+ re-publish regime sink.rs documents) ever reuses one sink
    /// across generations, this counter — like the distinct arm's
    /// merged_bytes — must reset at re-publish or regenerated engagements
    /// double-count.
    combined_bytes: AtomicUsize,
}

// SAFETY: out_emit cells are written only by the exclusive claimer of their
// partition (the runtime's exactly-once combine claim) and read only by
// finalize, which happens-after every combine by last-worker-out.
unsafe impl Sync for AggSink {}

/// What finalize hands the leader.
enum SinkPublished {
    /// Combine-materialized per-bucket EmitBufs (the general arm).
    Emit(Vec<SinkEmitBuf>),
    /// TRUE TABLE ADOPT: the single sealed Local's whole table + SEAL
    /// partition — no partition merge ran, no re-insert, no EmitBuf
    /// materialization; the leader drains the table directly.
    Table(SinkTableHandle, SinkPart),
}

impl AggSink {
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

    /// Shared combine tail (merge and pass-through arms): meter the RETAINED
    /// emit buffer against the admitted envelope (R3, m2-integration audit —
    /// the emit buffers are the merged result, held until the leader
    /// drains; the union is bounded by the admitted Locals' content, so a
    /// crossing is a real accounting surprise → budget refusal, fail-closed)
    /// and store it in the claimed partition's slot.
    fn retain_bucket(&self, part: u64, buf: SinkEmitBuf, nlocals: usize) -> PgResult<()> {
        let retained = buf.bytes();
        let total = self.combined_bytes.fetch_add(retained, Ordering::Relaxed) + retained;
        if total > self.budget.saturating_mul(nlocals.max(1)) {
            self.refuse_budget();
            return Ok(());
        }
        // SAFETY: partition `part` is claimed exactly once (runtime
        // contract); this is its single writer.
        unsafe { *self.out_emit[part as usize].get() = buf };
        Ok(())
    }
}

impl runtime::ParallelSink for AggSink {
    type Local = AggSinkLocal;

    fn fork(&self, _worker: usize) -> AggSinkLocal {
        AggSinkLocal::default()
    }

    fn accept_local(&self, local: &mut AggSinkLocal, _worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| accept_morsel_body(self, local, range)));
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
                self.fail(PgError::new(ERROR, "runtime agg sink worker panicked").into());
            }
        }
    }

    /// SEAL: partition every Local's remainder table (single-threaded by
    /// the last-worker-out protocol; counting sort, one pass per Local).
    fn seal(&self, locals: &mut [AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        for l in locals.iter_mut() {
            l.part = l.table.as_ref().map(|t| sink_partition_remainder(t.table()));
            // R3 accounting (m2-integration audit): the SEAL index is
            // per-Local retained memory that lives through the whole combine
            // phase — charge it like a run. Crossing = budget refusal (R5
            // whole-attempt rerun), never an error.
            if let Some(p) = &l.part {
                l.run_bytes += p.bytes();
                let table_mem = l.table.as_ref().map_or(0, |t| t.table().mem_used());
                if l.run_bytes + table_mem > self.budget {
                    self.refuse_budget();
                    return;
                }
            }
        }
        // TRUE TABLE ADOPT decision (dop1-tax2 inc-1) — LIVE STATE at SEAL:
        // the sealed-Local census is final here (last-worker-out; a widened
        // engagement forked ≥2 Locals and takes the merge/pass-through
        // combine arms). Exactly one sealed Local with zero flushed runs
        // and an all-byval shape: hand its table + SEAL partition to
        // finalize wholesale. Every combine claim then no-ops (nothing to
        // merge, nothing to materialize) and the leader drains the table
        // directly — forming rows only for boundary-cut survivors. Memory
        // is already charged (the loop above); nothing new is retained.
        if self.adopt_shape {
            if let [l] = locals {
                if l.runs.is_empty() && l.table.is_some() && l.part.is_some() {
                    let t = l.table.take().expect("checked Some");
                    let p = l.part.take().expect("checked Some");
                    *self.adopted.lock().unwrap_or_else(|g| g.into_inner()) = Some((t, p));
                    self.adopted_flag.store(true, Ordering::SeqCst);
                }
            }
        }
    }

    fn partitions(&self) -> u64 {
        SINK_NBUCKETS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        // TRUE TABLE ADOPT: seal took the single Local's table — there is
        // nothing to merge and nothing to materialize; finalize publishes
        // the table itself. (Set at SEAL, which happens-before every
        // combine claim; SeqCst pairs with the seal store.)
        if self.adopted_flag.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<()> {
            // SINGLE-LOCAL PASS-THROUGH (dop1-tax fix 3): exactly one sealed
            // Local and zero flushed runs — the merged bucket table would be
            // a verbatim re-insert of the Local's rows, so emit straight
            // from its table through the SEAL partition index (no 256-way
            // rebuild, no double insert; byte-identical order by
            // construction — see sink_emit_bucket_passthrough). LIVE-STATE
            // decision: a widened engagement (≥2 Locals) or a flushed Local
            // takes the merge arm below; no plan/DOP special-casing.
            if let [l] = locals {
                if l.runs.is_empty() {
                    if let (Some(t), Some(p)) = (&l.table, &l.part) {
                        let buf = ::nodeagg::sink::sink_emit_bucket_passthrough(
                            &self.emit,
                            t.table(),
                            p,
                            part as usize,
                        )?;
                        return self.retain_bucket(part, buf, locals.len());
                    }
                }
            }
            // Std-collections audit note: this views Vec is a per-claim
            // allocation, but the combine morsel space is a FIXED 256
            // partitions x dop-sized views — bounded per engagement,
            // independent of data volume (accepted; a borrowed view cannot
            // be retained across claims without lifetime erasure).
            let views: Vec<SinkLocalView<'_>> = locals
                .iter()
                .map(|l| SinkLocalView {
                    runs: &l.runs,
                    remainder: match (&l.table, &l.part) {
                        (Some(t), Some(p)) => Some((t.table(), p)),
                        _ => None,
                    },
                })
                .collect();
            let merged = sink_combine_bucket(
                part as usize,
                self.key_words,
                self.state_bytes,
                &views,
                &self.combines,
            )?;
            let buf = sink_emit_bucket(&self.emit, &merged)?;
            self.retain_bucket(part, buf, locals.len())
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "runtime agg sink combine panicked").into())
            }
        }
    }

    /// Publish: the adopted table (TRUE TABLE ADOPT — the pointer hand-off)
    /// or the 256 emit buffers, moved out (O(partitions), the §6 contract).
    /// Locals drop with the plumbing right after.
    fn finalize(&self, _locals: &[AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        if self.adopted_flag.load(Ordering::SeqCst) {
            if let Some((t, p)) =
                self.adopted.lock().unwrap_or_else(|g| g.into_inner()).take()
            {
                *self.published.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(SinkPublished::Table(t, p));
                return;
            }
            // Unreachable by construction (flag implies content); fall
            // through fail-closed to the buf publish (empty → leader errors
            // on "published nothing"-class checks rather than wedging).
        }
        let bufs: Vec<SinkEmitBuf> = self
            .out_emit
            .iter()
            .map(|c| {
                // SAFETY: all combine claims settled (last-worker-out);
                // finalize is the single reader.
                unsafe { std::mem::take(&mut *c.get()) }
            })
            .collect();
        *self.published.lock().unwrap_or_else(|p| p.into_inner()) = Some(SinkPublished::Emit(bufs));
    }
}

enum AcceptFail {
    Budget,
    Error(Box<PgError>),
}

impl From<Box<PgError>> for AcceptFail {
    fn from(e: Box<PgError>) -> AcceptFail {
        AcceptFail::Error(e)
    }
}

// ---------------------------------------------------------------------------
// Worker-side executor (helper thread-local) + the narrow ranged drain.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim — runtime_scan precedent).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeAggShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    refused: AtomicUsize,
    started: AtomicUsize,
    sink: Arc<AggSink>,
    query_id: AtomicU64,
}

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
    /// Per-worker reusable drain scratch.
    k2s: ScanK2Scratch,
    idxs: Vec<u32>,
    groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    /// ExprKey drain state (SinkDrain::ExprKey only): the worker's own
    /// decide + the spill-replay stage slot.
    xk: Option<Box<super::ExprKeyState>>,
    stage_slot: Option<::executils::ExecSlotId>,
    /// Mk drain state (SinkDrain::Mk only): the worker's own armed shape +
    /// the reusable pack scratch.
    mk: Option<super::ScanMk>,
    mks: super::MkScratch,
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

/// One accept morsel: position the worker's scan on the claimed granule
/// range, lend the Local's table to the executor, run the narrow drain,
/// reclaim the table.
fn accept_morsel_body(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    range: runtime::MorselRange,
) -> Result<(), AcceptFail> {
    WORKER_EXEC.with(|cell| -> Result<(), AcceptFail> {
        let mut b = cell.borrow_mut();
        let Some(ex) = b.as_mut() else {
            return Err(AcceptFail::Error(Box::new(PgError::new(
                ERROR,
                "runtime agg morsel without a bound executor",
            ))));
        };
        let WorkerExec { qd, k2s, idxs, groups, xk, stage_slot, mk, mks, .. } = ex;
        let (k2s, idxs, groups) = (&mut *k2s, &mut *idxs, &mut *groups);
        let (xk, stage_slot) = (&mut *xk, &mut *stage_slot);
        let (mk, mks) = (&mut *mk, &mut *mks);
        crate::querydesc::with_qd(*qd, |q| {
            let x = q.exec.as_mut().expect("runtime agg worker executor state");
            x.with_mut(|d| -> Result<(), AcceptFail> {
                let estate = &mut d.estate;
                let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                else {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker plan root is not an Agg",
                    ))));
                };
                let aps = &mut **aps;
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker outer node is not a SeqScan",
                    ))));
                };
                // train-12 composition: the heap lane generalized the
                // positioner to AM-dispatched seq_scan_set_morsel_range
                // (PgResult<()>); this arm admits only cbstore scans (its
                // admission requires cb granule geometry), so the former
                // not-cbstore false branch is unreachable by construction.
                ::nodeseqscan::seq_scan_set_morsel_range(
                    ss,
                    estate,
                    range.start,
                    range.end,
                )?;
                // Lend the Local's table to the executor for this range
                // (first morsel: the armed table is already in place).
                if let Some(t) = local.table.take() {
                    ::nodeagg::sink::agg_sink_put_table(&mut aps.agg, t);
                }
                let drained = sink_drain_range(
                    sink, local, &mut aps.agg, ss, k2s, idxs, groups, xk, stage_slot, mk, mks,
                    estate,
                );
                // Reclaim on EVERY path — the Local owns the table between
                // morsels and at SEAL.
                if let Some(t) = ::nodeagg::sink::agg_sink_take_table(&mut aps.agg) {
                    local.table = Some(t);
                }
                drained
            })
        })
    })
}

/// The narrow sink drain over the positioned claim: per staged page batch —
/// cap-flush check, survivor collection, canonical key gather, compact
/// batch probe (never the C table), whole-batch fold.
#[allow(clippy::too_many_arguments)]
fn sink_drain_range<'mcx>(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    k2s: &mut ScanK2Scratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    xk: &mut Option<Box<super::ExprKeyState>>,
    stage_slot: &mut Option<::executils::ExecSlotId>,
    mk: &mut Option<super::ScanMk>,
    mks: &mut super::MkScratch,
    estate: &mut EStateData<'mcx>,
) -> Result<(), AcceptFail> {
    let key_col = match sink.drain {
        // Unused: the expr-key feed derives keys; the mk feed packs its own.
        SinkDrain::ExprKey | SinkDrain::Mk => 0,
        SinkDrain::K2 => ::nodeagg::agg_hash_staged_probe_col(agg).ok_or_else(|| {
            AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker build lost its staged key column",
            ))
        })? as usize,
    };
    loop {
        // Bounded-Local discipline: flush BEFORE the batch (no group pointer
        // held across this point), budget-check table + runs.
        if let Some(run) = ::nodeagg::sink::agg_sink_flush_if_due(agg, sink.cap) {
            local.run_bytes += run.bytes();
            local.runs.push(run);
            let aggctx = if sink.byref_states {
                ::nodeagg::sink::agg_sink_aggctx_mem(agg)
            } else {
                0
            };
            if local.run_bytes + ::nodeagg::sink::agg_sink_table_mem(agg) + aggctx
                > sink.budget
            {
                return Err(AcceptFail::Budget);
            }
        }
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        if sink.drain == SinkDrain::ExprKey {
            // Expr-key feed: keys derived per batch; fail-closed inside the
            // adapter (range guard / per-row route / compact disarm).
            let xk = xk.as_deref_mut().ok_or_else(|| {
                AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "expr-key drain without a worker decide",
                ))
            })?;
            super::exprkey::exprkey_sink_batch(
                agg, ss, xk, stage_slot, idxs, groups, n, estate,
            )?;
            continue;
        }
        // Fail-closed: a fallback row has no staged key — the sink cannot
        // route it (no C-table leg exists here).
        let all_lane = ::nodeseqscan::seq_scan_batch_soa(ss)
            .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
        if !all_lane {
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "fallback rows in a sink accept batch",
            )));
        }
        if sink.drain == SinkDrain::Mk {
            // The serial lane's own packed multi-key batch (survivors →
            // pack pre-pass → mk1/mk2 compact probe → whole-batch fold).
            // Under the sink cap the compact backstop ERRORS instead of
            // migrating, and Int-only components never hit the numeric
            // pack-legality demote — a `false` here is a contract breach.
            let mk = mk.as_ref().ok_or_else(|| {
                AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "mk drain without a worker shape",
                ))
            })?;
            if !super::scan_mk_batch(agg, ss, mk, mks, idxs, groups, n, estate)? {
                return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "worker mk feed demoted mid-build",
                )));
            }
            continue;
        }
        let ScanK2Scratch { rows, keys, knull, .. } = k2s;
        super::scan_collect_survivors(ss, estate, n, rows)?;
        keys.clear();
        knull.clear();
        {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("sink drain requires the armed SoA");
            let (kv, kn) = (soa.col_values(key_col), soa.col_isnull(key_col));
            for &i in rows.iter() {
                keys.push(kv[i as usize]);
                knull.push(kn[i as usize]);
            }
        }
        if !::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)? {
            // The compact table migrated (backstop) — unexportable. The
            // sink-mode backstop errors before this; belt-and-braces.
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker compact table disarmed mid-build",
            )));
        }
        idxs.clear();
        idxs.extend_from_slice(rows);
        let soa =
            ::nodeseqscan::seq_scan_batch_soa(ss).expect("sink drain requires the armed SoA");
        // SAFETY: every probed row is non-fallback (all-lane batch), so the
        // SoA lanes carry valid deformed values for every plan column; the
        // plan is unguarded (sink admission); each pergroup was installed by
        // the compact probe within this batch (agg_fold_staged contract).
        unsafe { super::agg_fold_staged(agg, soa, idxs, groups)? };
    }
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn runtime_agg_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_agg_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else {
        // F1 observability: a context with NO private payload can never be
        // driven by any arm — trace it (foreign-payload downcast misses stay
        // silent below: every arm's hook runs for every worker by design).
        lane_trace("runtime-agg: post-task-park without a private payload");
        return;
    };
    let Ok(payload) = private.downcast::<RuntimeAggShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.sink.fail(PgError::new(ERROR, "runtime agg helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeAggShared>) {
    let _ = shared;
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-agg: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-agg: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-agg: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg)
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                // Budget refusals are NOT query errors (the leader falls
                // back to the serial arm); the Err only routed the binder
                // through its abort-side cleanup.
                if !payload.sink.budget_refused.load(Ordering::SeqCst) {
                    payload.sink.fail(e);
                }
                // F1 liveness (the wedge mechanism): a helper that errored
                // BEFORE joining the drive (build_worker_exec failure) has
                // aborted the RG via fail()/refuse_budget() — but an aborted
                // PINNED RG still needs a driver to run invalidate/finalize/
                // complete, or the leader's waiter parks forever. Drive the
                // closed generation to completion here (pure protocol
                // cleanup, the drain_rg discipline); post-drive errors find
                // it already complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!("runtime-agg: helper bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    // WFIN markers: emitted by the runtime's generic channel (sched.rs,
    // PGRUST_MORSEL_MARKERS=1) — one line per (worker, task set). The arm's
    // own duplicate emitter was removed at m2-integration: with the sched
    // channel armed the double emission (different time bases) garbled the
    // instrument parser's spread verdicts.
}

fn drive_bound(
    payload: &Arc<RuntimeAggShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // A released (not finished) executor may still hold registered
        // snapshots — the binder's NORMAL unbind asserts a cleared xmin, so
        // route through its transaction-ABORT path by returning an error
        // (observed live: snapmgr xmin assertion at worker slot teardown
        // after a budget refusal). The real error (if any) was recorded
        // first (fail() is first-wins); budget refusals record none and
        // helper_drive swallows this marker.
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime agg worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

/// Build + SINK-ARM this helper's executor over the shared worker
/// PlannedStmt. Divergence from the leader's admission is an ERROR (the
/// leader proved the shape; a worker that cannot reproduce it must not
/// silently build something else).
fn build_worker_exec(payload: &Arc<RuntimeAggShared>) -> PgResult<()> {
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
        let armed = (|| -> PgResult<ArmedDrain> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime agg worker ExecutorStart");
                x.with_mut(|d| -> PgResult<ArmedDrain> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime agg worker plan root is not an Agg",
                        )));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime agg worker outer node is not a SeqScan",
                        )));
                    };
                    arm_sink_build(&payload.sink, &mut aps.agg, ss, estate)
                })
            })
        })();
        match armed {
            Ok(drain) => {
                let (xk, mk) = match drain {
                    ArmedDrain::K2 => (None, None),
                    ArmedDrain::ExprKey(xk) => (Some(xk), None),
                    ArmedDrain::Mk(mk) => (None, Some(mk)),
                };
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    errored: std::cell::Cell::new(false),
                    k2s: ScanK2Scratch::default(),
                    idxs: Vec::new(),
                    groups: Vec::new(),
                    xk,
                    stage_slot: None,
                    mk,
                    mks: super::MkScratch::default(),
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

/// The worker arm's drain-specific state (see [`arm_sink_build`]).
enum ArmedDrain {
    K2,
    ExprKey(Box<super::ExprKeyState>),
    Mk(super::ScanMk),
}

/// The worker's sink-build arm: the serial lane's own staging + key-shape +
/// compact arm sequence, under the sink cap, with every admission the leader
/// proved re-checked (divergence = error). Returns the ExprKey drain's
/// worker decide (None for the K2 drain).
fn arm_sink_build<'mcx>(
    sink: &AggSink,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ArmedDrain> {
    let shape_err = ::nodeagg::sink::sink_shape_error;
    let plan_ok = ::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| !p.guarded && p.vguards.is_empty() && p.resid.is_empty());
    if !plan_ok || ::nodeagg::agg_lanefold_has_resid(agg) {
        return Err(shape_err("worker fold plan diverged from the leader's"));
    }
    if sink.drain == SinkDrain::ExprKey {
        // The worker's own decide (same plan tree — same census result).
        let Some(xk) = super::exprkey::decide_exprkey(agg, ss, estate) else {
            return Err(shape_err("worker expr-key decide diverged from the leader's"));
        };
        if xk.sink_refused() {
            return Err(shape_err("worker expr-key decide starts refused"));
        }
        let kind = xk.sink_key_kind();
        ::nodeagg::sink::agg_sink_set_cap(agg, sink.cap);
        match (&sink.red, kind) {
            (None, Some(None)) => {
                if ::nodeagg::agg_hash_compact_try_arm(agg) != ::nodeagg::CompactArm::Armed {
                    return Err(shape_err("worker compact arm refused under the sink cap"));
                }
            }
            (Some(shape), Some(Some(wshape))) => {
                if wshape.width != shape.width || wshape.keys.len() != shape.keys.len() {
                    return Err(shape_err("worker reduced shape diverged from the leader's"));
                }
                if ::nodeagg::agg_hash_compact_try_arm_reduced(agg, wshape)
                    != ::nodeagg::CompactArm::Armed
                {
                    return Err(shape_err("worker reduced arm refused under the sink cap"));
                }
            }
            _ => return Err(shape_err("worker expr-key kind diverged from the leader's")),
        }
        let spec_ok = match ::nodeagg::sink::agg_sink_key_spec(agg) {
            Some(SinkKeySpec::Single { width }) => sink.red.is_none() && width == sink.width,
            Some(SinkKeySpec::Reduced(sh)) => {
                sink.red.as_ref().is_some_and(|r| r.width == sh.width) && sh.width == sink.width
            }
            // The expr-key drain never arms a Multi table (Mk rides its own
            // drain; the decide refuses Multi kinds).
            Some(SinkKeySpec::Multi(_)) | None => false,
        };
        if !spec_ok {
            return Err(shape_err("worker key spec diverged from the leader's"));
        }
        if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
            return Err(shape_err("worker state layout diverged from the leader's"));
        }
        return Ok(ArmedDrain::ExprKey(xk));
    }
    super::arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
    if sink.drain == SinkDrain::Mk {
        // Packed multi-key arm: the same decide the leader probed, this time
        // arming the compact table under the sink cap. Every divergence from
        // the leader's snapshot is an error (the sink's combine + emit plans
        // were built off that exact shape).
        ::nodeagg::sink::agg_sink_set_cap(agg, sink.cap);
        let Some(mk) = super::scan_mk_shape(agg, ss, estate) else {
            return Err(shape_err("worker mk shape diverged from the leader's"));
        };
        if mk.dict_att.is_some() || ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some()
        {
            return Err(shape_err("dict component on a sink mk worker"));
        }
        let lshape = sink
            .mk
            .as_ref()
            .ok_or_else(|| shape_err("mk drain without a leader shape"))?;
        if &mk.shape != lshape {
            return Err(shape_err("worker mk shape diverged from the leader's"));
        }
        match ::nodeagg::sink::agg_sink_key_spec(agg) {
            Some(SinkKeySpec::Multi(sh)) if &sh == lshape => {}
            _ => return Err(shape_err("worker key spec diverged from the leader's")),
        }
        if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
            return Err(shape_err("worker state layout diverged from the leader's"));
        }
        return Ok(ArmedDrain::Mk(mk));
    }
    if super::scan_k2_shape(agg, ss, estate).is_none() {
        return Err(shape_err("worker K2 shape diverged from the leader's"));
    }
    if ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some() {
        return Err(shape_err("dict-group staging on a sink worker"));
    }
    ::nodeagg::sink::agg_sink_set_cap(agg, sink.cap);
    if ::nodeagg::agg_hash_compact_try_arm(agg) != ::nodeagg::CompactArm::Armed {
        return Err(shape_err("worker compact arm refused under the sink cap"));
    }
    match ::nodeagg::sink::agg_sink_key_spec(agg) {
        Some(SinkKeySpec::Single { width }) if width == sink.width => {}
        _ => return Err(shape_err("worker key spec diverged from the leader's")),
    }
    if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
        return Err(shape_err("worker state layout diverged from the leader's"));
    }
    Ok(ArmedDrain::K2)
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

fn runtime_agg_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeAggShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_agg_main",
            runtime_agg_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_agg_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_agg_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

/// Env override for the sink flush cap (entries); None = budget-derived.
fn sink_cap_override() -> Option<u32> {
    static N: OnceLock<Option<u32>> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_AGG_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|&c| c >= 1024)
    })
}

/// Sink flush cap (worker table bound, entries) — BUDGET-DERIVED (dop1-tax
/// inc-3b). The fixed 64K exchange-class cap forced ~17 flush cycles on
/// q36@10M at DOP1 (~1.1M groups), keeping the single-Local pass-through
/// permanently dormant and re-inserting every group at combine. The cap is
/// now the entry count whose compact-table estimate fills HALF the
/// per-Local budget (the compact spill gate's own arithmetic:
/// 16+8+state+16 bytes/entry), floored at the 64K class — at default
/// work_mem it degenerates to ~the old cap (tranche behavior preserved);
/// under the matched-memory protocol (1GB) a q36-class Local holds all its
/// groups, never flushes, and the pass-through fires. Width-INDEPENDENT:
/// each Local is budget-bounded exactly as before (runs held the same
/// bytes the larger live table now holds — the R3 envelope arithmetic is
/// unchanged, and the seal/accept budget checks still refuse crossings).
/// PGRUST_RUNTIME_AGG_CAP overrides to a fixed cap (the A/B arm; 65536 =
/// the old behavior).
fn sink_cap_for(state_bytes: usize, budget: usize, ngroups_limit: u64) -> u32 {
    if let Some(c) = sink_cap_override() {
        return c;
    }
    let entry = 16u64 + 8 + state_bytes as u64 + 16;
    // BOTH admission bounds (compact_admission / agg_hash_compact_sink_
    // admissible): capped-numgroups must satisfy est_bytes <= budget/2 AND
    // numgroups <= ngroups_limit/2 — a cap above either manufactures
    // refusals the fixed 64K cap never hit (round-3 battery: count-only
    // high-NDV shapes flipped admit->refuse because the mem-derived cap
    // 74898 crossed ngroups_limit/2 ~73.7k at default work_mem). The 64K
    // floor keeps heavy-state shapes exactly at the old cap (their old
    // verdict, admit or refuse, is reproduced verbatim).
    let mem_bound = (budget as u64 / 2) / entry.max(1);
    let cap = mem_bound.min(ngroups_limit / 2);
    cap.clamp(1 << 16, u32::MAX as u64 / 2) as u32
}

/// Engagement floor (granules) — below it helper launches are pure overhead.
fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_AGG_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// Find the Node of `agg.plan` inside the leader's plan tree (worker pstmts
/// root at the Agg subtree; the Agg need not be the leader plan's root).
fn find_agg_node<'mcx>(
    root: Node<'mcx>,
    target: *const ::types_nodes::plannodes::Agg<'mcx>,
) -> Option<Node<'mcx>> {
    if let Some(a) = root.as_agg() {
        if core::ptr::eq(a, target) {
            return Some(root);
        }
    }
    let plan = root.as_plan()?;
    if let Some(l) = plan.lefttree {
        if let Some(n) = find_agg_node(l, target) {
            return Some(n);
        }
    }
    if let Some(r) = plan.righttree {
        if let Some(n) = find_agg_node(r, target) {
            return Some(n);
        }
    }
    None
}

/// The runtime aggregation-sink arm. `false` = not engaged (caller falls
/// through to the serial build, byte-identically — nothing was consumed).
/// `true` = the published parallel result was adopted; every retrieve path
/// drains it through `agg_hash_retrieve`'s sink branch.
pub(super) fn try_engage_hashagg_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: Option<&super::ExprKeyState>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_agg_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };

    // --- Plan shape gates (fail-closed).
    let refuse = |why: &str| {
        lane_trace(&format!("runtime-agg: refused ({why})"));
    };
    if !::nodeagg::sink::agg_sink_plan_shape_ok(agg) {
        refuse("plan shape");
        return Ok(false);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refuse("instrumented/EPQ");
        return Ok(false);
    }
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(ss) {
        refuse("scan not fusible cbstore");
        return Ok(false);
    }
    // Unprojected K2 class only in phase 1 (exprkey/Reduced/Multi are the
    // next cars); scan projection means the key is computed — refuse.
    let plan_ok = ::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| !p.guarded && p.vguards.is_empty() && p.resid.is_empty());
    if !plan_ok || ::nodeagg::agg_lanefold_has_resid(agg) {
        refuse("fold plan guarded/varlena/residual");
        return Ok(false);
    }
    // Budget triple (hoisted above the shape decide): the leader-side
    // cap-aware mk probe below and the sink construction must see the SAME
    // budget-derived cap (dop1-tax inc-3b — sink_cap_for replaces the fixed
    // 64K cap everywhere a cap is decided).
    let Some(state_bytes) = ::nodeagg::sink::agg_sink_state_bytes(agg) else {
        return Ok(false);
    };
    let Some(budget) = ::nodeagg::sink::agg_sink_hash_mem_limit(agg) else {
        return Ok(false);
    };
    let Some(ngroups_limit) = ::nodeagg::sink::agg_sink_ngroups_limit(agg) else {
        return Ok(false);
    };
    // Drain mode: projected scans take the expr-key feed (Arith/TsTrunc/
    // Reduced kinds — the lane's decide already ran and is memoized in
    // `xk`); unprojected scans take the K2 single-int-key batch probe or
    // (Mk car) the packed multi-int composite feed.
    let (drain, red, mk, width);
    if ss.ss.ps_ProjInfo.is_some() {
        let Some(xk) = xk else {
            refuse("projected scan without an expr-key decide");
            return Ok(false);
        };
        if xk.sink_refused() {
            refuse("expr-key decide refused");
            return Ok(false);
        }
        let Some(kind) = xk.sink_key_kind() else {
            refuse("expr-key kind (dict/multi cars)");
            return Ok(false);
        };
        drain = SinkDrain::ExprKey;
        mk = None;
        match kind {
            None => {
                let Some(w) = ::nodeagg::sink::agg_sink_key_width(agg) else {
                    refuse("key width");
                    return Ok(false);
                };
                red = None;
                width = w;
            }
            Some(shape) => {
                width = shape.width;
                red = Some(shape);
            }
        }
    } else {
        // The staging arm (idempotent — the serial fold feed re-arms the
        // same shape on fallback) + the K2 single-key / Mk packed decide.
        super::arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
        if super::scan_k2_shape(agg, ss, estate).is_some() {
            if ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some() {
                refuse("dict-group staging");
                return Ok(false);
            }
            let Some(w) = ::nodeagg::sink::agg_sink_key_width(agg) else {
                refuse("key width");
                return Ok(false);
            };
            drain = SinkDrain::K2;
            red = None;
            mk = None;
            width = w;
        } else if let Some(probe) = {
            // Cap-aware probe: the worker arms under the sink cap (bounded
            // table + flush discipline), so the leader's spill-estimate gate
            // must see the same capped group count — the K2 leader has no
            // estimate gate at all for exactly this reason. The cap is
            // cleared right after: the leader's own executor may still run
            // the SERIAL build (refusal / budget fallback / rescan), which
            // must never see sink mode.
            ::nodeagg::sink::agg_sink_set_cap(
                agg,
                sink_cap_for(state_bytes, budget, ngroups_limit),
            );
            let probe = super::scan_mk_probe(agg, ss, estate);
            ::nodeagg::sink::agg_sink_clear_cap(agg);
            probe
        } {
            // Mk car: packed multi-INT composite keys only. Intern ids are
            // per-worker (never canonical across Locals — the C2 car needs a
            // shared vocabulary), Numeric packs carry a mid-batch legality
            // demote (phase 2), and nullable images are heap-source-only.
            if probe.dict_att.is_some()
                || probe.shape.nullable
                || probe
                    .shape
                    .comps
                    .iter()
                    .any(|c| !matches!(c.kind, ::nodeagg::MkCompKind::Int { .. }))
            {
                refuse("mk component kind (C2/numeric cars)");
                return Ok(false);
            }
            drain = SinkDrain::Mk;
            red = None;
            mk = Some(probe.shape);
            // Unused by the Mk drain: per-component widths ride the emit
            // plan's MultiComp columns.
            width = 8;
        } else {
            refuse("K2/Mk shape");
            return Ok(false);
        }
    }
    // Combine + identity-emit qualification (fail-closed; catalog access).
    let Some(combines) = sink_resolve_combines(agg)? else {
        refuse("combine whitelist");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    };
    let key_spec = match (&red, &mk) {
        (Some(shape), _) => SinkKeySpec::Reduced(shape.clone()),
        (None, Some(shape)) => SinkKeySpec::Multi(shape.clone()),
        (None, None) => SinkKeySpec::Single { width },
    };
    let Some(emit) = sink_build_emit_plan(agg, &key_spec) else {
        refuse("identity emit");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    };
    // F1 root cause (chaos-battery): the WORKER arm re-runs the compact
    // spill-eligibility gate under the sink cap with the leader's restored
    // work_mem — at small work_mem (<=256kB on 16k-group shapes) EVERY
    // worker refused ("worker compact arm refused under the sink cap"),
    // erroring pre-drive and stranding the pinned RG nobody would ever
    // drain. The leader runs the SAME numbers, so admission must refuse
    // here, fail-closed to the serial arm, before anything launches.
    if !::nodeagg::agg_hash_compact_sink_admissible(
        agg,
        sink_cap_for(state_bytes, budget, ngroups_limit),
    ) {
        refuse("worker compact arm would refuse under the sink budget");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    }
    // --- Session/binder gates (the M1 set, verbatim).
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refuse("in parallel mode");
        return Ok(false);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse("extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(false) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse("exec params");
        return Ok(false);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
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
    // Worker plan root: the Agg subtree's Node in the leader plan tree.
    let Some(root) = leader_pstmt.planTree else { return Ok(false) };
    let Some(agg_node) = find_agg_node(root, agg.plan) else {
        refuse("agg node not in plan tree");
        return Ok(false);
    };
    // The Agg's scan child must be the SeqScan (no intermediate nodes).
    if agg.plan.plan.lefttree.map(Node::node_tag) != Some(NodeTag::T_SeqScan) {
        refuse("scan child shape");
        return Ok(false);
    }

    // --- Geometry.
    let Some((total_granules, starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(false);
    };
    if total_granules < min_granules().max(2 * dop as u64) {
        refuse("granule floor");
        return Ok(false);
    }

    // --- Engage.
    let key_words = mk.as_ref().map_or(1, |s| if s.two_words { 2 } else { 1 });
    let byref_states = ::nodeagg::sink::sink_combines_byref(&combines);
    // TABLE-ADOPT shape gate: byval emit columns AND byval combine states —
    // the adopted table's rows must be self-contained past helper teardown
    // (a byref transvalue points into a worker aggcontext).
    let adopt_shape =
        ::nodeagg::sink::sink_emit_plan_all_byval(&emit) && !byref_states;
    let sink = Arc::new(AggSink {
        drain,
        red,
        mk,
        cap: sink_cap_for(state_bytes, budget, ngroups_limit),
        budget,
        key_words,
        state_bytes,
        byref_states,
        width,
        combines,
        emit,
        out_emit: (0..SINK_NBUCKETS).map(|_| UnsafeCell::new(SinkEmitBuf::default())).collect(),
        published: Mutex::new(None),
        adopt_shape,
        adopted: Mutex::new(None),
        adopted_flag: AtomicBool::new(false),
        rg: OnceLock::new(),
        failed: AtomicBool::new(false),
        error: Mutex::new(None),
        budget_refused: AtomicBool::new(false),
        combined_bytes: AtomicUsize::new(0),
    });
    engage(agg, estate, rt, dop, total_granules, starts, agg_node, sink)
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
    sink: Arc<AggSink>,
) -> PgResult<bool> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;
    let payload = Arc::new(RuntimeAggShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (runtime_scan precedent).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        sink: Arc::clone(&sink),
        query_id: AtomicU64::new(0),
    });

    xact::EnterParallelMode();
    let engaged =
        engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload, &sink);
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeAggShared>,
    sink: &Arc<AggSink>,
) -> PgResult<bool> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_agg_main", dop)?;
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

        // The sink's two task sets over the cbstore granule geometry.
        let source = Arc::new(CbstoreGranuleSource { starts });
        let runtime::SinkTaskSets { accept, combine, probe: _probe } = runtime::sink_tasksets(
            Arc::clone(sink),
            source,
            rt.nthreads(),
            0,
        );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let qid = NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64;
        payload.query_id.store(qid, Ordering::SeqCst);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: qid,
            tasksets: vec![accept, combine],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        sink.rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("sink rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-agg: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-agg: engaged dop={launched} granules={total_granules}"
        ));

        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                lane_trace(&format!("runtime-agg: all {refused} helpers refused the bind"));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            // LIVENESS backstop (m1 helper-death fix 5cf96f83d, ported from
            // runtime_scan.rs — F1 defect layer 2a): every launched helper's
            // task has ENDED (normal hook exit keeps BGWH_STARTED until
            // after the drive, so this cannot trip mid-drive) yet the RG is
            // incomplete — helpers died or returned without a channel
            // message and without driving. Nothing claimed => clean serial
            // fallback; claimed => reap if possible and surface a real
            // error.
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-agg: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg(rt, &rg);
                if let Some(e) = sink.take_error() {
                    return Err(e);
                }
                if sink.budget_refused.load(Ordering::SeqCst) {
                    lane_trace(
                        "runtime-agg: budget refusal — falling back to the serial arm",
                    );
                    stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
                    return Ok(EngageOutcome::Fallback);
                }
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime agg helpers exited before completing the aggregation",
                )));
            }
            // A raised cancel disposition (statement_timeout /
            // pg_cancel_backend) surfaces from the latch quantum (F1 defect
            // layer 2b): abort + drain the RG, then propagate — exactly the
            // CFI branch above.
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
        };

        if let Some(e) = sink.take_error() {
            return Err(e);
        }
        if sink.budget_refused.load(Ordering::SeqCst) {
            // R5 degrade: whole-attempt rerun on the serial arm.
            lane_trace("runtime-agg: budget refusal — falling back to the serial arm");
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
            return Ok(EngageOutcome::Fallback);
        }
        if outcome == runtime::RgOutcome::Aborted {
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "runtime agg pipeline aborted")));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(EngageOutcome::Fallback);
        }
        Ok(EngageOutcome::Completed)
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
            lane_trace("runtime-agg: fallback to serial arm");
            Ok(false)
        }
        EngageOutcome::Completed => {
            let published = sink
                .published
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
                .ok_or_else(|| {
                    ::nodeagg::sink::sink_shape_error("completed sink published nothing")
                })?;
            let natts = sink.emit.cols.len();
            match published {
                SinkPublished::Emit(bufs) => {
                    let rows = ::nodeagg::sink::sink_emit_rows(&bufs);
                    lane_trace(&format!("runtime-agg: complete, groups={rows}"));
                    ::nodeagg::sink::agg_sink_adopt_emit(agg, bufs, natts);
                }
                SinkPublished::Table(table, part) => {
                    let rows = part.idx.len() + usize::from(part.has_null);
                    lane_trace(&format!(
                        "runtime-agg: complete (table adopt), groups={rows}"
                    ));
                    ::nodeagg::sink::agg_sink_adopt_table(
                        agg,
                        table,
                        part,
                        sink.emit.clone(),
                    );
                }
            }
            Ok(true)
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive
/// (abort/fallback paths) — cleanup driving, not leader execution
/// (runtime_scan's hardened drain, verbatim; F1 port). True = the RG
/// completed. False = it could not be completed (a participant died holding
/// an unsettled pin): the RG and its slot are deliberately LEAKED and the
/// caller must surface an error rather than wait forever — the previous
/// unbounded `loop {{ acquire }} + drive_pinned` shape could itself wedge
/// on exactly the helper-death cases this lane fixes.
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    // Bounded lane wait (~2s): helper drives settle within a morsel.
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-agg: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-agg: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}

/// Granule-addressed morsel source over one cbstore part's geometry
/// (runtime_scan's source, module-local copy — claims never cross a
/// row-group/dict-epoch edge).
struct CbstoreGranuleSource {
    starts: Vec<u64>,
}

impl runtime::MorselSource for CbstoreGranuleSource {
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
}
