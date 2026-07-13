//! M2 DISTINCT SINK — parallel exact-DISTINCT / COUNT(DISTINCT) on the
//! morsel runtime (docs/design/m2-sinks.md §3 donor B re-homed;
//! docs/design/parallelism-redesign-2026-07.md §2.2/§5-M2).
//!
//! Shape: the SERIAL-plan grouped distinct pipeline `Agg(AGG_SORTED) ← Sort
//! ← SeqScan(cbstore)` (the ClickBench Q9/Q10 class), executed as one
//! SealedParallelSink on the runtime: ACCEPT (granule-morsel scan →
//! PREWHERE → per-worker `PdBuilder` partial: compact int group keys,
//! (acc,count) vocab words, exact `DistinctSet`s) → SEAL (parallel
//! per-worker freeze into `PdHandedTable`s) → COMBINE (256 group-partition
//! bucket-claim merges — disjoint partitions, single writer per output
//! cell) → finalize (concatenate buckets, publish). The parked leader
//! adopts the merged result through the UNCHANGED serial emit tail
//! (`agg_hashgroup_adopt_merged` → hashgroup emit): groups in the plan
//! Sort's prefix order, byte-identical to the serial arm by the donor's
//! identity argument (exact representational set equality;
//! order-insensitive-exact transitions; count/sum reassociation
//! unobservable).
//!
//! vs the Gather-era donor (pardistinct): the registry/handoff, the leader's
//! own partial, the stray-row queue drain, and the `spent` flag are all
//! GONE (no tuple queues exist); the vocabulary refusal is DROPPED — the
//! Q10 companion-agg shape rides the sink (the donor's refusal priced the
//! per-row vocab accept against the fused classic GatherMerge drives, a
//! comparison that no longer exists here).
//!
//! Budget law (m2-sinks.md R3/R5, phase 1): each Local gets the derived
//! `worker_budget` (C-parity per participant; participants = launched
//! helpers ≤ dop, so the memory envelope is the plan-shaped one, never
//! nthreads-shaped). A worker CROSSING its budget has no degrade target
//! under the runtime (no queues) — the sink emits nothing until finalize,
//! so the arm aborts the RG and the leader FALLS BACK TO THE SERIAL ARM
//! rerun: exact, nothing consumed, bounded memory at every arm.
//!
//! Engagement layering (all cheap; absent = today's serial path, byte- and
//! perf-identical): PGRUST_RUNTIME=1 (pool spawned) + SET
//! pgrust.runtime_distinct_pool = <dop> (falling back to
//! pgrust.runtime_scan_pool — the lane's booked instrument vocabulary) +
//! PGRUST_RUNTIME_DISTINCT != 0 (arm kill switch, decoupled from the scan
//! arm's at m2-integration); see guc_tables::runtime_pool for the reconciled
//! three-arm surface. The plan surface stays the serial plan; EXPLAIN
//! unchanged; instrumented runs refuse (EXPLAIN ANALYZE stays C-exact).

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::nodeagg::{
    pd_concat_buckets, pd_empty_grouped_table, pd_merge_bucket, PdFeed, PdHandedTable, PdMerged,
    PdSinkLocal, PdSpec, PD_SINK_GROUP_PARTS,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, trace_feed};
use super::{drain_pipeline, BatchSink, SeqScanFilterProject, SeqScanSource, Sink, SinkFeed};

// ---------------------------------------------------------------------------
// Shared state: the parallel context's private payload AND the sink body
// (one struct, one Arc — the runtime_scan discipline).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeDistinctShared {
    rt: &'static Arc<runtime::Runtime>,
    /// Weak: the RG's task sets hold this struct as their sink — a strong
    /// handle here would leak the cycle.
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// The leader-derived build recipe (plain data; helpers fork Locals
    /// from it in-process — no DSM transfer).
    spec: Arc<PdSpec>,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// First worker-phase error (the entry-phase errors ride the ordinary
    /// parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    /// Set when any worker recorded an error (fast skip for later morsels).
    failed: AtomicBool,
    /// A worker budget crossed mid-accept: NOT an error — the RG aborts and
    /// the leader falls back to the serial arm (m2-sinks.md R5 phase 1).
    crossed: AtomicBool,
    /// Combine-phase retained CONTENT bytes (merged bucket outputs, summed
    /// across claims) — m2-integration R3 accounting for the merged RESULT,
    /// checked against the ADMITTED envelope (forked Locals × worker_budget;
    /// see the check site for why not one worker_budget). Crossing = the
    /// same `crossed` fallback.
    merged_bytes: AtomicUsize,
    /// Combine output cells, one per group partition. Single writer each:
    /// partition p is claimed exactly once by the combine task set.
    out: Vec<UnsafeCell<Option<PdMerged<'static>>>>,
    /// The published merged result (finalize writes, the leader takes).
    merged: Mutex<Option<PdMerged<'static>>>,
}

// SAFETY: (i) each `out` cell has a single writer — the sink contract
// visits every partition exactly once — and is read only by `finalize`,
// which the runtime's last-worker-out orders after every combine; (ii) the
// PdMerged values held in `out`/`merged` are never-spilled bucket-merge
// outputs (owned plain data — the PdHandedTable self-contained-buffer
// argument); (iii) every other member is Send/Sync by composition.
unsafe impl Send for RuntimeDistinctShared {}
unsafe impl Sync for RuntimeDistinctShared {}

impl RuntimeDistinctShared {
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

    /// Budget crossing: no degrade target under the runtime — abort the RG;
    /// the leader observes `crossed` and reruns the serial arm.
    fn cross(&self) {
        self.crossed.store(true, Ordering::SeqCst);
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

    fn take_merged(&self) -> Option<PdMerged<'static>> {
        self.merged.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

// ---------------------------------------------------------------------------
// The SealedParallelSink implementation. accept_local/seal are INFALLIBLE BY
// CONTRACT: errors and panics are caught, recorded (first wins), and turn
// into an RG abort — the runtime protocol never sees an unwind.
// ---------------------------------------------------------------------------

impl runtime::SealedParallelSink for RuntimeDistinctShared {
    type Local = PdSinkLocal;
    type Sealed = PdHandedTable;

    fn fork(&self, _worker: usize) -> PdSinkLocal {
        PdSinkLocal::new(Arc::clone(&self.spec), self.spec.worker_budget)
    }

    fn accept_local(&self, local: &mut PdSinkLocal, _worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            // Already aborting: drain the claim without work.
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(local, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime distinct worker panicked in a morsel").into(),
                );
            }
        }
    }

    fn seal(&self, _worker: usize, local: PdSinkLocal) -> PdHandedTable {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return pd_empty_grouped_table(&self.spec);
        }
        let r = catch_unwind(AssertUnwindSafe(|| local.freeze()));
        match r {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => {
                self.fail(e);
                pd_empty_grouped_table(&self.spec)
            }
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "runtime distinct worker panicked in seal").into());
                pd_empty_grouped_table(&self.spec)
            }
        }
    }

    fn partitions(&self) -> u64 {
        PD_SINK_GROUP_PARTS
    }

    fn combine(&self, part: u64, sealed: &[PdHandedTable]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| {
            pd_merge_bucket(&self.spec, sealed, part as usize)
        }));
        match r {
            Ok(m) => {
                // R3 accounting (m2-integration audit): the merged bucket is
                // RETAINED until the leader adopts — meter it against the
                // ADMITTED engagement envelope (forked Locals x per-Local
                // budget: the merged union is bounded by the sum of the
                // sealed tables' content, so this trips only on real
                // overhead/accounting surprises — fail-closed, visible).
                // NOT one worker_budget: the union legitimately exceeds a
                // single Local's budget (the q9@100M rt1-crosses/rt2-fits
                // booked behavior). Crossing takes the same bounded fallback
                // as an accept-phase crossing.
                let b = m.mem_bytes();
                let total = self.merged_bytes.fetch_add(b, Ordering::Relaxed) + b;
                if total > self.spec.worker_budget.saturating_mul(sealed.len().max(1)) {
                    self.cross();
                    return;
                }
                // SAFETY: partition `part` is handed to this claimer alone
                // (sink contract); finalize reads happen-after every combine.
                unsafe { *self.out[part as usize].get() = Some(m) };
            }
            Err(_panic) => {
                self.fail(
                    PgError::new(ERROR, "runtime distinct worker panicked in combine").into(),
                );
            }
        }
    }

    fn finalize(&self, _sealed: &[PdHandedTable]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        // SAFETY: single-threaded under last-worker-out, after every combine.
        let buckets: Vec<PdMerged<'static>> = self
            .out
            .iter()
            .filter_map(|c| unsafe { (*c.get()).take() })
            .collect();
        let merged = pd_concat_buckets(buckets);
        *self.merged.lock().unwrap_or_else(|p| p.into_inner()) = Some(merged);
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: thread-local executor + the accept morsel body.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    /// Per-helper detoast scratch context (reset per row when a bytes set
    /// detoasts into per-tuple memory).
    tmp: EcxtId,
    reset_tmp: bool,
    /// THIS helper contributed an error (take the release/abort teardown).
    errored: std::cell::Cell<bool>,
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

/// The per-morsel accept feed: rows into the worker's `PdSinkLocal`. A
/// budget crossing flips `crossed` and drops the remainder of the morsel
/// (the RG is aborting; nothing is emitted anywhere).
struct PdAcceptSink<'a> {
    local: &'a mut PdSinkLocal,
    tmp: EcxtId,
    reset_tmp: bool,
    crossed: bool,
}

impl<'mcx> Sink<'mcx> for PdAcceptSink<'_> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        if self.crossed {
            return Ok(SinkFeed::NeedMore);
        }
        let crossed = self.local.accept(estate, tuple, self.tmp)? == PdFeed::Crossed;
        if self.reset_tmp {
            estate.reset_expr_context(self.tmp);
        }
        if crossed {
            self.crossed = true;
        }
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for PdAcceptSink<'_> {}

impl RuntimeDistinctShared {
    fn morsel_body(&self, local: &mut PdSinkLocal, range: runtime::MorselRange) -> PgResult<()> {
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime distinct morsel without a bound executor",
                )));
            };
            let (tmp, reset_tmp) = (ex.tmp, ex.reset_tmp);
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime distinct worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = distinct_worker_scan(d.planstate.as_mut())?;
                    if !::nodeseqscan::seq_scan_cb_set_granule_range(
                        ss,
                        estate,
                        range.start,
                        range.end,
                    )? {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime distinct worker scan is not cbstore",
                        )));
                    }
                    let mut sink =
                        PdAcceptSink { local, tmp, reset_tmp, crossed: false };
                    let fed = drain_pipeline(
                        ss,
                        &mut SeqScanSource,
                        &mut SeqScanFilterProject,
                        &mut sink,
                        estate,
                    );
                    let crossed = sink.crossed;
                    fed?;
                    if crossed {
                        trace_feed(
                            "runtime distinct worker budget crossed; aborting to serial fallback",
                        );
                        self.cross();
                    }
                    Ok(())
                })
            })
        })
    }
}

/// The worker plan tree is the SCAN SUBTREE alone (workers never run the
/// Agg or the Sort — accept_local drives scan → PREWHERE → project into the
/// PdBuilder; the worker pstmt's planTree is the SeqScan node).
fn distinct_worker_scan<'a, 'mcx>(
    planstate: Option<&'a mut crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = planstate else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime distinct worker plan is not a SeqScan root",
        )));
    };
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Helper entry + POST_TASK_PARK drive (the runtime_scan ceremony, with this
// arm's payload type; the hook registries are multi-registrant and every
// hook no-ops on foreign payloads).
// ---------------------------------------------------------------------------

fn runtime_distinct_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_distinct_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeDistinctShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime distinct helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeDistinctShared>) {
    let _ = shared;
    let Some(target) = payload.pcxt_shared.get() else { return };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return };
    let Some(lane) = payload.rt.acquire_external_lane() else {
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
                payload.fail(e);
            } else {
                lane_trace(&format!(
                    "runtime-distinct: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeDistinctShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // m2-integration port of the agg lane's binder abort-path fix (also
        // applied to the scan arm): a released (not finished) executor may
        // still hold registered snapshots — the binder's NORMAL unbind
        // asserts a cleared xmin, so route through its transaction-ABORT
        // path by returning an error. The real error was recorded first
        // (fail() is first-wins), so this marker never surfaces; budget
        // crossings do not set the errored flag and keep their serial
        // fallback path.
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime distinct worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

fn build_worker_exec(payload: &Arc<RuntimeDistinctShared>) -> PgResult<()> {
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
        let armed = (|| -> PgResult<EcxtId> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime distinct worker ExecutorStart");
                x.with_mut(|d| -> PgResult<EcxtId> {
                    let estate = &mut d.estate;
                    let ss = distinct_worker_scan(d.planstate.as_mut())?;
                    super::arm_scan_staging(
                        ss,
                        estate,
                        super::ScanFeedShape::RowFeed {
                            ctx: "runtime distinct worker feed",
                            stitch: true,
                        },
                    )?;
                    Ok(estate.exec_assign_expr_context())
                })
            })
        })();
        match armed {
            Ok(tmp) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    tmp,
                    reset_tmp: payload.spec.any_bytes_set(),
                    errored: std::cell::Cell::new(false),
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

fn runtime_distinct_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeDistinctShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_distinct_main",
            runtime_distinct_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_distinct_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_distinct_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side engagement. Arming layering (kill switch + DOP option + lane
// master) lives in guc_tables::runtime_pool::runtime_distinct_pool_dop —
// the reconciled three-arm surface (PGRUST_RUNTIME_DISTINCT is this arm's
// dedicated kill; the scan arm's kill no longer disarms it).
// ---------------------------------------------------------------------------

/// Refusal diagnosis trace (PGRUST_LANE_V2_TRACE only; emitted only once the
/// arm is ARMED — dop set + runtime on — so unarmed sessions stay silent).
#[cold]
fn refused(reason: &str) {
    lane_trace(&format!("runtime-distinct: refused ({reason})"));
}

/// The runtime distinct-sink arm, probed from the sorted-agg narrow branch
/// (set-mode already armed by the caller — the last-refusal ordering law is
/// satisfied there). `None` = refused or fell back (nothing consumed; the
/// serial arms run byte-identically). `Some(row)` = the arm owns the node
/// (merged result adopted; emit chain active).
pub(super) fn try_own_sorted_distinct_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    rd_shape_refused: &mut bool,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_distinct_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // Static shape refusal memo: the plan-shape gates below cannot flip for
    // this node; skip the whole probe (incl. spec derivation) on re-pulls.
    if *rd_shape_refused {
        return Ok(None);
    }
    lane_trace("runtime-distinct: probed");

    // --- Shape + session gates (fail-closed; every refusal is the serial arm).
    let crate::procnode::PlanStateNode::SeqScan(ss) = outer else {
        refused("outer not SeqScan");
        return Ok(None);
    };
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(ss) {
        refused("scan not fusible/cbstore");
        return Ok(None);
    }
    // Instrumented runs refuse the sink (EXPLAIN ANALYZE stays C-exact) —
    // the caller's seam does not gate instrumentation for the serial arms.
    if estate.es_instrument != 0 || estate.es_epq_active {
        refused("instrumented/epq");
        return Ok(None);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refused("already in parallel machinery");
        return Ok(None);
    }
    // Agg-side admission: the hash-grouped arm's integer-key/exact-set
    // vocabulary and its density economics (a refusal falls back to the
    // serial arms, byte-identically). Vocab shapes (Q10 companions) are
    // ADMITTED — see the module doc.
    if !::nodeagg::agg_hashgroup_admissible(agg)
        || !::nodeagg::agg_hashgroup_economical(
            agg,
            super::pardistinct_force(),
            sort.plan.plan.plan_rows,
        )
    {
        refused("hashgroup admission/economics");
        return Ok(None);
    }
    let Some(order) = super::hashgroup_order_spec(agg, sort.plan, k) else {
        refused("order spec");
        *rd_shape_refused = true;
        return Ok(None);
    };
    let Some(desc) = outer_desc.as_ref() else {
        refused("no outer desc");
        return Ok(None);
    };
    let Some(spec) = ::nodeagg::pd_derive_spec(agg, desc) else {
        refused("spec derivation");
        *rd_shape_refused = true;
        return Ok(None);
    };
    if spec.max_att > desc.natts {
        refused("att bound");
        *rd_shape_refused = true;
        return Ok(None);
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused("extern params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused("exec params");
        return Ok(None);
    }
    // Plan shape below the Agg: exactly THIS Sort → SeqScan (the workers
    // receive the SCAN SUBTREE as their pstmt — the Agg need not be the
    // plan root, so ORDER BY/LIMIT above it, the real CB q9/q10 shape,
    // stays engageable).
    let Some(sort_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if sort_node.node_tag() != NodeTag::T_Sort
        || !std::ptr::eq(sort_node.as_sort().expect("Sort tag"), sort.plan)
    {
        refused("agg child not this Sort");
        *rd_shape_refused = true;
        return Ok(None);
    }
    let Some(scan_node) = sort.plan.plan.lefttree else { return Ok(None) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused("sort child not SeqScan");
        *rd_shape_refused = true;
        return Ok(None);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused("parallel-unsafe scan exprs");
        *rd_shape_refused = true;
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refused("non-MVCC snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refused("binder policy sources");
        return Ok(None);
    }

    // --- Geometry: enough granules to be worth a gang.
    let Some((total_granules, starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(None);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused("granule floor");
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    engage(agg, estate, rt, dop, total_granules, starts, spec, order, scan_node)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    spec: Arc<PdSpec>,
    order: Vec<::nodeagg::HashGroupOrderKey>,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    // The worker pstmt carries ONLY the scan subtree (ExecSerializePlan's
    // fragment-transfer shape; the helpers drive scan → PREWHERE → project
    // into their PdBuilder Locals — no Agg, no Sort).
    let pstmt = crate::execparallel::build_worker_pstmt(estate, scan_node)?;

    let payload = Arc::new(RuntimeDistinctShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path.
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        spec: Arc::clone(&spec),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        crossed: AtomicBool::new(false),
        merged_bytes: AtomicUsize::new(0),
        out: (0..PD_SINK_GROUP_PARTS as usize).map(|_| UnsafeCell::new(None)).collect(),
        merged: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged =
        engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload, spec, order);
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
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeDistinctShared>,
    spec: Arc<PdSpec>,
    order: Vec<::nodeagg::HashGroupOrderKey>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_distinct_main", dop)?;
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

        // Submit the pinned RG (accept → freeze → combine) before launch.
        let source = Arc::new(super::runtime_scan::CbstoreGranuleSource { starts });
        let runtime::SealedSinkTaskSets { accept, freeze, combine, probe: _probe } =
            runtime::sealed_sink_tasksets(
                Arc::clone(payload),
                source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![accept, freeze, combine],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-distinct: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-distinct: engaged dop={launched} granules={total_granules} vocab={} sets={}",
            spec.vocab.len(),
            spec.sets.len()
        ));

        // Submit-and-park (the WaitForParallelWorkersToFinish shape).
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
                lane_trace(&format!(
                    "runtime-distinct: all {refused} helpers refused the bind"
                ));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            parallel::wait_parallel_finish_quantum();
        };

        if let Some(e) = payload.take_error() {
            lane_trace(&format!(
                "runtime-distinct: worker-phase error: {}",
                e.message()
            ));
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            if payload.crossed.load(Ordering::SeqCst) {
                // Worker budget crossed: bounded-memory refusal — rerun the
                // serial arm (nothing was emitted; the leader's scan is
                // untouched).
                lane_trace("runtime-distinct: worker budget crossed; serial fallback");
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
                return Ok(EngageOutcome::Fallback);
            }
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime distinct pipeline aborted",
            )));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(EngageOutcome::Fallback);
        }
        Ok(EngageOutcome::Completed)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // parallel context is destroyed and this frame's arena can unwind.
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
            lane_trace("runtime-distinct: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let Some(merged) = payload.take_merged() else {
                // Completed with participants but no published result: a
                // protocol violation, never silently wrong output.
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime distinct completed without a merged result",
                )));
            };
            stats::tick_owned(ShapeClass::AggBuild);
            lane_trace(&format!(
                "runtime-distinct: complete, groups={}",
                merged.ngroups
            ));
            trace_feed("runtime distinct sink adopt + hashgroup emit engaged");
            ::nodeagg::agg_hashgroup_adopt_merged(
                agg,
                estate,
                merged.into_lt(),
                &spec.vocab,
                order,
            )?;
            Ok(Some(super::hashgroup_emit(agg, estate)?))
        }
    }
}

/// Reap a pinned RG no helper will drive (abort/fallback paths) — protocol
/// cleanup driving, not leader work execution (§2.5).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) {
    rg.abort();
    let lane = loop {
        if let Some(l) = rt.acquire_external_lane() {
            break l;
        }
        std::thread::yield_now();
    };
    let mut local = lane.local();
    let _ = rt.drive_pinned(&mut local, rg);
}
