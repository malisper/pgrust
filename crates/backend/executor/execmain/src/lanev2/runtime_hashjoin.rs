//! M3 RUNTIME HASH JOIN — the shared-build hash join on the morsel runtime
//! (docs/design/m3-joins.md; parent plan parallelism-redesign-2026-07 §5-M3).
//!
//! Shape (phase 1): a SERIAL-plan plain Agg over a HashJoin over two
//! lane-fusible cbstore SeqScans, executed as THREE runtime task sets:
//!
//!   [0] BUILD-ACCEPT   inner-scan granules → filter/project → per-worker
//!                      JoinBuildLocal (materialize + count; sink accept)
//!   [1] BUILD-COMBINE  256 partitions, deps=[0] — partitioned single-writer
//!                      table construction; finalize publishes the frozen
//!                      table (the ParallelSink pair via sink_tasksets)
//!   [2] PROBE          outer-scan granules, deps=[1] — per row: hash → tag
//!                      → chain → recheck → joinqual/otherqual → null-fill
//!                      arms → the plain-agg partial absorb (M1's
//!                      runtime_partial tail)
//!
//! Engagement layering (identical to M1/M2): PGRUST_RUNTIME=1 +
//! `SET pgrust.runtime_hashjoin_pool = <dop>` + lane master switch, with
//! `PGRUST_RUNTIME_HASHJOIN=0` as the dedicated arm kill. The plan surface
//! stays the serial plan; every refusal falls through to the serial arms
//! byte-identically (nothing consumed).
//!
//! Join types (phase 1): INNER / LEFT / SEMI / ANTI — fully probe-local.
//! The right-fill family (match flags + FILL task set) is inc-3.
//!
//! Ordering contract (Michael's 2026-07-13 directive): order-insensitive
//! emission is the baseline; the probe feeds an order-insensitive plain-agg
//! partial tail in phase 1, and the gates use tie-normalized comparison.
//!
//! Memory (§6): admission sizes the build with the C combined envelope
//! (`exec_choose_hash_table_size_full(try_combined_hash_mem=true)`);
//! nbatch > 1 refuses at admission. Runtime enforcement: the shared
//! JoinBudget; a crossing records a refusal, aborts the RG, and the leader
//! falls back to the SERIAL arm (R5 whole-attempt rerun — never an error).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ::executils::{EStateData, ExecSlotId};
use ::nodeagg::runtime_partial::{
    agg_runtime_combine, agg_runtime_export_partial_into, agg_runtime_partial_admissible,
    exec_agg_runtime_partials, RuntimePartial,
};
use ::nodehashjoin::shared_build::{
    freeze, BudgetExceeded, CombinePlan, FrozenJoinTable, JoinBudget, JoinBuildLocal, PARTITIONS,
};
use ::nodehashjoin::shared_exec::{
    shared_build_accept, shared_fill_partition, shared_join_admissible, shared_probe_outer,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::runtime_scan::{exprs_parallel_safe, CbstoreGranuleSource};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible};

// ---------------------------------------------------------------------------
// Shared state: parallel-context private payload + probe task-set work body.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim — the M1 arm's discipline).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeHjShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    pins_base: usize,
    refused: AtomicUsize,
    started: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// §6 envelope crossing: abort → LEADER FALLBACK (serial rerun), not an
    /// error (R5). Set before the abort; checked on the Aborted outcome.
    budget_refused: AtomicBool,
    /// Per-ordinal cumulative probe partials (M1 overwrite discipline).
    partials: Vec<Mutex<Option<RuntimePartial>>>,
    /// The build sink (the ParallelSink of task sets [0]/[1]).
    sink: OnceLock<Arc<JoinBuildSink>>,
}

impl RuntimeHjShared {
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

    fn table(&self) -> Option<Arc<FrozenJoinTable>> {
        self.sink.get().and_then(|s| s.table.lock().unwrap_or_else(|p| p.into_inner()).clone())
    }
}

// ---------------------------------------------------------------------------
// The build sink: ParallelSink over the shared_build core. accept_local
// drives the bound helper's INNER scan over the claimed granule range;
// combine/finalize are pure core calls (no executor).
// ---------------------------------------------------------------------------

pub(super) struct JoinBuildSink {
    budget: Arc<JoinBudget>,
    /// Lazily planned at first combine (the SEAL happens inside the sink
    /// plumbing; the sink sees the sealed Locals only at combine time).
    plan: Mutex<Option<Arc<CombinePlan>>>,
    /// Published at finalize; the probe task set (deps=[combine]) reads it.
    table: Mutex<Option<Arc<FrozenJoinTable>>>,
    shared: Weak<RuntimeHjShared>,
}

impl JoinBuildSink {
    fn fail(&self, e: Box<PgError>) {
        if let Some(s) = self.shared.upgrade() {
            s.fail(e);
        }
    }

    fn failed(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| s.failed.load(Ordering::SeqCst))
    }

    /// The lazily-built combine plan (first combine wins the build; the
    /// mutex is held only for the plan/lookup, never across a partition).
    fn plan_for(&self, locals: &[JoinBuildLocal]) -> Option<Arc<CombinePlan>> {
        let mut g = self.plan.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(p) = g.as_ref() {
            return Some(Arc::clone(p));
        }
        match CombinePlan::plan(locals, &self.budget) {
            Ok(p) => {
                let p = Arc::new(p);
                *g = Some(Arc::clone(&p));
                Some(p)
            }
            Err(BudgetExceeded) => {
                drop(g);
                lane_trace("runtime-hashjoin: REFUSED (envelope crossed at seal) — serial rerun");
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget();
                }
                None
            }
        }
    }
}

impl runtime::ParallelSink for JoinBuildSink {
    type Local = JoinBuildLocal;

    fn fork(&self, worker: usize) -> JoinBuildLocal {
        JoinBuildLocal::new(worker, Arc::clone(&self.budget))
    }

    fn accept_local(&self, local: &mut JoinBuildLocal, _worker: usize, range: runtime::MorselRange) {
        if self.failed() {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| build_morsel_body(local, range)));
        match r {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                // §6 envelope crossing mid-accept: refusal, not error.
                lane_trace("runtime-hashjoin: REFUSED (envelope crossed in build) — serial rerun");
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget();
                }
            }
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a build morsel")
                        .into(),
                );
            }
        }
    }

    fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[JoinBuildLocal]) {
        if self.failed() {
            return;
        }
        if let Some(plan) = self.plan_for(locals) {
            plan.combine_partition(part, locals);
        }
    }

    fn finalize(&self, locals: &[JoinBuildLocal]) {
        if self.failed() {
            return;
        }
        // Zero-granule inner side: no combine morsel ran (empty partition
        // space never happens — PARTITIONS is fixed — but a fully-refused
        // plan slot can be absent after refuse_budget).
        let Some(plan) = self.plan_for(locals) else { return };
        *self.table.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(Arc::new(freeze(plan, locals)));
    }
}

// ---------------------------------------------------------------------------
// Worker-side executor (TLS): the whole serial Agg→HashJoin→scans subtree,
// built once per bound helper; build morsels position the INNER scan, probe
// morsels the OUTER scan.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static HJ_WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
    /// The probe payload for the currently-driving helper (set for the
    /// drive's duration; run_morsel bodies read it for the frozen table).
    static HJ_PAYLOAD: std::cell::RefCell<Option<Arc<RuntimeHjShared>>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    HJ_WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// Split the worker plan tree into (agg, hj_state, outer scan, hash state,
/// inner scan) and run `f`. All field borrows are disjoint.
fn with_join_tree<'a, 'mcx, R>(
    estate: &'a mut EStateData<'mcx>,
    planstate: &'a mut Option<crate::procnode::PlanStateNode<'mcx>>,
    f: impl FnOnce(
        &mut EStateData<'mcx>,
        &mut ::nodeagg::AggStateData<'mcx>,
        &mut ::nodehashjoin::HashJoinState<'mcx>,
        &mut ::nodeseqscan::SeqScanState<'mcx>,
        &mut ::nodehash::HashState<'mcx>,
        &mut ::nodeseqscan::SeqScanState<'mcx>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    let Some(crate::procnode::PlanStateNode::Agg(aps)) = planstate.as_mut() else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker plan is not a plain Agg root",
        )));
    };
    let aps: &mut crate::procnode::AggPlanState<'mcx> = aps;
    let crate::procnode::PlanStateNode::HashJoin(hjn) = &mut aps.outer else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker outer node is not a HashJoin",
        )));
    };
    let hjn: &mut crate::procnode::HashJoinNode<'mcx> = hjn;
    let crate::procnode::PlanStateNode::SeqScan(outer_ss) = &mut *hjn.outer else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker probe child is not a SeqScan",
        )));
    };
    let hash: &mut crate::procnode::HashSubNode<'mcx> = &mut hjn.hash;
    let crate::procnode::PlanStateNode::SeqScan(inner_ss) = &mut *hash.child else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker build child is not a SeqScan",
        )));
    };
    f(estate, &mut aps.agg, &mut hjn.state, outer_ss, &mut hash.state, inner_ss)
}

fn with_worker_exec<R>(
    ctx: &'static str,
    f: impl for<'mcx> FnOnce(
        &mut EStateData<'mcx>,
        &mut Option<crate::procnode::PlanStateNode<'mcx>>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    HJ_WORKER_EXEC.with(|cell| {
        let b = cell.borrow();
        let Some(ex) = b.as_ref() else {
            return Err(Box::new(PgError::new(ERROR, ctx)));
        };
        crate::querydesc::with_qd(ex.qd, |q| {
            let x = q.exec.as_mut().expect("runtime hash-join worker executor state");
            x.with_mut(|d| f(&mut d.estate, &mut d.planstate))
        })
    })
}

/// One BUILD-ACCEPT morsel: position the inner scan on the claimed granule
/// range and materialize every surviving row into the Local. Ok(false) =
/// envelope crossed (refusal, not error).
fn build_morsel_body(local: &mut JoinBuildLocal, range: runtime::MorselRange) -> PgResult<bool> {
    with_worker_exec("runtime hash-join build morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, _agg, _hj, _outer_ss, hstate, inner_ss| {
            // train-12 composition: AM-dispatched positioner (heap lane
            // rename); this arm admits only cbstore scans by construction.
            ::nodeseqscan::seq_scan_set_morsel_range(
                inner_ss,
                estate,
                range.start,
                range.end,
            )?;
            local.begin_run(range.start);
            let mut crossed = false;
            loop {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(inner_ss, estate)?;
                if n == 0 {
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(inner_ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    break;
                }
                ::postgres_seams::check_for_interrupts::call()?;
                for i in 0..n {
                    if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(inner_ss, estate, i)? {
                        if shared_build_accept(hstate, estate, slot, local)?.is_err() {
                            crossed = true;
                            break;
                        }
                    }
                }
                if crossed {
                    break;
                }
            }
            local.end_run();
            Ok(!crossed)
        })
    })
}

/// One PROBE morsel: position the outer scan and stream every surviving
/// outer row against the frozen table into the plain-agg absorb; export the
/// cumulative partial (M1 overwrite discipline — the worker's last export
/// precedes its settle).
fn probe_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let Some(table) = payload.table() else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join probe ran without a published table",
        )));
    };
    with_worker_exec("runtime hash-join probe morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, outer_ss, hstate, _inner_ss| {
            // train-12 composition: AM-dispatched positioner (heap lane
            // rename); this arm admits only cbstore scans by construction.
            ::nodeseqscan::seq_scan_set_morsel_range(
                outer_ss,
                estate,
                range.start,
                range.end,
            )?;
            loop {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(outer_ss, estate)?;
                if n == 0 {
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(outer_ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    break;
                }
                ::postgres_seams::check_for_interrupts::call()?;
                for i in 0..n {
                    let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(outer_ss, estate, i)?
                    else {
                        continue;
                    };
                    shared_probe_outer(
                        hj,
                        hstate,
                        estate,
                        &table,
                        slot,
                        &mut |_hj, estate, out| ::nodeagg::agg_plain_build_accept(agg, estate, out),
                    )?;
                }
            }
            let slot = worker - payload.pins_base;
            {
                // train-12 composition: the m2 std-audit replaced the
                // per-export Vec allocation with export-into (retained
                // capacity); overwrite discipline is preserved — the export
                // rewrites the slot's partial in place.
                let mut g = payload.partials[slot].lock().unwrap_or_else(|p| p.into_inner());
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for RuntimeHjShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let payload = HJ_PAYLOAD.with(|c| c.borrow().clone());
        let Some(payload) = payload else {
            self.fail(PgError::new(ERROR, "runtime hash-join probe without a bound payload").into());
            return;
        };
        let r = catch_unwind(AssertUnwindSafe(|| probe_morsel_body(&payload, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a probe morsel")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self) {
        // Partials are installed per morsel; the leader combines after
        // completion (M1 discipline).
    }
}

/// The FILL task set's work (right-fill family only, deps=[probe]):
/// never-matched build tuples of one partition, null-extended, into the
/// same plain-agg tail. The probe set's last-worker-out completion is the
/// match-flag visibility barrier.
struct FillWork(Arc<RuntimeHjShared>);

fn fill_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let Some(table) = payload.table() else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join fill ran without a published table",
        )));
    };
    with_worker_exec("runtime hash-join fill morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, _outer_ss, hstate, _inner_ss| {
            for part in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                shared_fill_partition(
                    hj,
                    hstate,
                    estate,
                    &table,
                    part,
                    &mut |_hj, estate, out| ::nodeagg::agg_plain_build_accept(agg, estate, out),
                )?;
            }
            // Cumulative partial export (same slot as the probe morsels —
            // the worker's agg accumulates across both phases; overwrite
            // discipline keeps the last export authoritative).
            let slot = worker - payload.pins_base;
            {
                // train-12 composition: the m2 std-audit replaced the
                // per-export Vec allocation with export-into (retained
                // capacity); overwrite discipline is preserved — the export
                // rewrites the slot's partial in place.
                let mut g = payload.partials[slot].lock().unwrap_or_else(|p| p.into_inner());
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for FillWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.0.failed.load(Ordering::SeqCst) {
            return;
        }
        let payload = &self.0;
        let r = catch_unwind(AssertUnwindSafe(|| fill_morsel_body(payload, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                payload.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                payload.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a fill morsel")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self) {}
}

/// The fill set's morsel space: one claim per partition (the sink
/// plumbing's PartitionSource shape, re-stated here — it is private to
/// runtime::sink).
struct FillPartitionSource;

impl runtime::MorselSource for FillPartitionSource {
    fn total_granules(&self) -> u64 {
        PARTITIONS as u64
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(PARTITIONS as u64)
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn runtime_hj_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_hj_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeHjShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime hash-join helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeHjShared>) {
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
                    "runtime-hashjoin: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeHjShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    HJ_PAYLOAD.with(|c| *c.borrow_mut() = Some(Arc::clone(payload)));
    let _outcome = payload.rt.drive_pinned(local, rg);
    HJ_PAYLOAD.with(|c| *c.borrow_mut() = None);
    let self_errored =
        HJ_WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    teardown_worker_exec(!self_errored)
}

fn build_worker_exec(payload: &Arc<RuntimeHjShared>) -> PgResult<()> {
    HJ_WORKER_EXEC.with(|cell| -> PgResult<()> {
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
        let armed = (|| -> PgResult<()> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime hash-join worker ExecutorStart");
                x.with_mut(|d| {
                    with_join_tree(&mut d.estate, &mut d.planstate, |estate, agg, hj, outer_ss, hstate, inner_ss| {
                        if !agg_runtime_partial_admissible(agg) {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime hash-join worker fold plan diverged from the leader's",
                            )));
                        }
                        if !shared_join_admissible(hj, hstate) {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime hash-join worker join shape diverged from the leader's",
                            )));
                        }
                        // Per-row drive staging on both scans (the census
                        // RowFeed shape: PREWHERE bitmap when kernel-shaped,
                        // stitched tiers on; per-row emits re-check quals).
                        super::arm_scan_staging(
                            outer_ss,
                            estate,
                            super::ScanFeedShape::RowFeed {
                                ctx: "runtime hash-join probe feed",
                                stitch: true,
                            },
                        )?;
                        super::arm_scan_staging(
                            inner_ss,
                            estate,
                            super::ScanFeedShape::RowFeed {
                                ctx: "runtime hash-join build feed",
                                stitch: true,
                            },
                        )?;
                        ::nodeagg::agg_plain_build_begin(agg, estate)?;
                        Ok(())
                    })
                })
            })
        })();
        match armed {
            Ok(()) => {
                *cell.borrow_mut() =
                    Some(WorkerExec { qd, errored: std::cell::Cell::new(false) });
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
    HJ_WORKER_EXEC.with(|cell| -> PgResult<()> {
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

fn runtime_hj_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeHjShared>() else { return };
    payload.abort_rg();
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_hashjoin_main",
            runtime_hj_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_hj_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_hj_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// The runtime hash-join arm. `None` = not engaged (caller falls through to
/// the serial arms byte-identically — nothing was consumed). `Some(row)` =
/// the plain agg's one finalized result row.
pub(super) fn try_own_agg_over_hash_join_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_hashjoin_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };

    // --- Node shape: HashJoin over two lane-fusible cbstore SeqScans; a
    // fresh (untouched) join; phase-1 join types; subplan/param-free exprs.
    let crate::procnode::PlanStateNode::SeqScan(outer_ss) = &mut *hj.outer else {
        return Ok(None);
    };
    let hash = &mut *hj.hash;
    let crate::procnode::PlanStateNode::SeqScan(inner_ss) = &mut *hash.child else {
        return Ok(None);
    };
    if !shared_join_admissible(&hj.state, &hash.state) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        return Ok(None);
    }
    if !::nodehashjoin::lane_join_untouched(&hj.state, &hash.state) {
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(None);
    }
    if !seq_scan_fusible(outer_ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(outer_ss) {
        return Ok(None);
    }
    if !seq_scan_fusible(inner_ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(inner_ss) {
        return Ok(None);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        return Ok(None);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        return Ok(None);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        return Ok(None);
    }
    // Agg must be the plan root; its child the HashJoin; the join's children
    // the two scans (the worker pstmt transfers the whole root subtree).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else { return Ok(None) };
    if !std::ptr::eq(root_agg, agg.plan) {
        return Ok(None);
    }
    let Some(join_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if join_node.node_tag() != NodeTag::T_HashJoin {
        return Ok(None);
    }
    let join_plan = join_node.as_hash_join().expect("HashJoin tag");
    let Some(outer_plan) = join_plan.join.plan.lefttree else { return Ok(None) };
    let Some(hash_plan_node) = join_plan.join.plan.righttree else { return Ok(None) };
    if outer_plan.node_tag() != NodeTag::T_SeqScan
        || hash_plan_node.node_tag() != NodeTag::T_Hash
    {
        return Ok(None);
    }
    let hash_plan = hash_plan_node.as_hash().expect("Hash tag");
    let Some(inner_plan) = hash_plan.plan.lefttree else { return Ok(None) };
    if inner_plan.node_tag() != NodeTag::T_SeqScan {
        return Ok(None);
    }
    // Parallel-safety walk over everything that runs on helpers.
    let outer_scan_plan = outer_plan.as_seq_scan().expect("SeqScan tag");
    let inner_scan_plan = inner_plan.as_seq_scan().expect("SeqScan tag");
    if !exprs_parallel_safe(outer_scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(outer_scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(inner_scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(inner_scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(join_plan.hashclauses.iter())?
        || !exprs_parallel_safe(join_plan.join.joinqual.iter())?
        || !exprs_parallel_safe(join_plan.join.plan.qual.iter())?
        || !exprs_parallel_safe(join_plan.join.plan.targetlist.iter())?
    {
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        return Ok(None);
    }

    // --- Envelope sizing (§6): C's combined-budget rule; nbatch>1 refuses.
    let (_, nbatch, _, space_allowed) = ::nodehash::exec_choose_hash_table_size_full(
        hash_plan.plan.plan_rows,
        hash_plan.plan.plan_width,
        false, // useskew: C PHJ parity — no skew in parallel
        true,  // try_combined_hash_mem: pooled participant budget
        dop,
    );
    if nbatch > 1 {
        lane_trace("runtime-hashjoin: REFUSED (estimated nbatch > 1) — serial arm");
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        return Ok(None);
    }

    // --- Geometry: the probe side pays the gang; the build side may be a
    // small dimension table (any nonzero geometry admits).
    let Some((outer_granules, outer_starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(outer_ss, estate)?
    else {
        return Ok(None);
    };
    let Some((_inner_granules, inner_starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(inner_ss, estate)?
    else {
        return Ok(None);
    };
    if outer_granules < min_granules().max(2 * dop as u64) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // Right-fill family (RIGHT/FULL/RIGHT_ANTI) adds the FILL task set.
    let fill_inner = matches!(
        join_plan.join.jointype,
        ::types_nodes::JoinType::JOIN_RIGHT
            | ::types_nodes::JoinType::JOIN_FULL
            | ::types_nodes::JoinType::JOIN_RIGHT_ANTI
    );

    engage(
        agg,
        estate,
        rt,
        dop,
        outer_granules,
        outer_starts,
        inner_starts,
        space_allowed,
        fill_inner,
    )
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    outer_granules: u64,
    outer_starts: Vec<u64>,
    inner_starts: Vec<u64>,
    space_allowed: usize,
    fill_inner: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let agg_node = estate.es_plannedstmt.and_then(|p| p.planTree).expect("gated above");
    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;

    let payload = Arc::new(RuntimeHjShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (the M1 SendConst discipline).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        pins_base: rt.nthreads(),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        budget_refused: AtomicBool::new(false),
        partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        sink: OnceLock::new(),
    });
    let sink = Arc::new(JoinBuildSink {
        budget: JoinBudget::new(space_allowed),
        plan: Mutex::new(None),
        table: Mutex::new(None),
        shared: Arc::downgrade(&payload),
    });
    payload.sink.set(Arc::clone(&sink)).unwrap_or_else(|_| unreachable!("sink set once"));

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        agg,
        estate,
        rt,
        dop,
        outer_granules,
        outer_starts,
        inner_starts,
        &payload,
        sink,
        fill_inner,
    );
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
    outer_granules: u64,
    outer_starts: Vec<u64>,
    inner_starts: Vec<u64>,
    payload: &Arc<RuntimeHjShared>,
    sink: Arc<JoinBuildSink>,
    fill_inner: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_hashjoin_main", dop)?;
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

        // Three task sets: the build sink pair + the probe pipeline.
        let runtime::SinkTaskSets { accept, combine, probe: _sink_probe } =
            runtime::sink_tasksets(
                sink,
                Arc::new(CbstoreGranuleSource {
                    starts: Arc::new(inner_starts),
                    // This arm feeds claims straight into set_granule_range
                    // (single-epoch contract); it does not subdivide
                    // multi-epoch claims — never coalesce.
                    coalesce: false,
                }),
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        let probe = runtime::TaskSetSpec {
            source: Arc::new(CbstoreGranuleSource {
                starts: Arc::new(outer_starts),
                // As above: straight set_granule_range feed, never coalesce.
                coalesce: false,
            }),
            work: Arc::clone(payload) as Arc<dyn runtime::TaskSetWork>,
            deps: vec![1],
        };
        let mut tasksets = vec![accept, combine, probe];
        if fill_inner {
            // Right-fill family: the unmatched-build walk, after the probe
            // barrier (deps=[2] — the match-flag visibility edge).
            tasksets.push(runtime::TaskSetSpec {
                source: Arc::new(FillPartitionSource),
                work: Arc::new(FillWork(Arc::clone(payload))),
                deps: vec![2],
            });
        }
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets,
        });
        payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-hashjoin: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-hashjoin: engaged dop={launched} outer_granules={outer_granules}"
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
                lane_trace(&format!(
                    "runtime-hashjoin: all {refused} helpers refused the bind"
                ));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-hashjoin: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg(rt, &rg);
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime hash-join helpers exited before completing the join",
                )));
            }
            parallel::wait_parallel_finish_quantum();
        };

        if payload.budget_refused.load(Ordering::SeqCst) {
            // §6/R5: envelope crossing — whole-attempt rerun on the serial
            // arm. Drop any recorded secondary errors (the abort races
            // in-flight morsels); nothing was consumed on the leader.
            let _ = payload.take_error();
            lane_trace("runtime-hashjoin: envelope refusal — falling back to the serial arm");
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            return Ok(EngageOutcome::Fallback);
        }
        if let Some(e) = payload.take_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "runtime hash-join pipeline aborted")));
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
            lane_trace("runtime-hashjoin: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let parts: Vec<RuntimePartial> = payload
                .partials
                .iter()
                .filter_map(|m| m.lock().unwrap_or_else(|p| p.into_inner()).take())
                .collect();
            let combined = agg_runtime_combine(agg, &parts)?;
            stats::tick_owned(ShapeClass::Join);
            lane_trace(&format!("runtime-hashjoin: complete, partials={}", parts.len()));
            Ok(Some(exec_agg_runtime_partials(agg, estate, &combined)?))
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG nobody will drive (the M1 drain
/// discipline: cleanup driving, not leader work execution).
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
        lane_trace("runtime-hashjoin: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-hashjoin: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}
