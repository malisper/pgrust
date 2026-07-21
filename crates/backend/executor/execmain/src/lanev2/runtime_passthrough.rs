//! World-B parallel PASSTHROUGH arm — Stage 1: the per-worker producer body.
//!
//! Fork of the agg arm's worker side (`runtime_scan.rs`) for a bare
//! `SeqScan`-rooted plan that STREAMS rows through the funnel instead of
//! folding into a partial. Each bgworker (bound by the parallel-context
//! ceremony — Stage 2) builds a per-worker `QueryDesc` from the leader-arena
//! `SeqScan` pstmt, and for each claimed morsel block-range runs
//! `SeqScanSource → qual/project → RowEmitSink` (the World-A serial push
//! island body, `RootAdapter` → `RowEmitSink`), blocking on a full ring under
//! the K-standby permit. `finalize` marks every ring done so the leader drain
//! reaches EOF.
//!
//! The runtime morsel cursor divides the blocks (each `run_morsel(range)`
//! claims a block range); `SeqScanSource::position` sets the worker's scan to
//! exactly that range (`seq_scan_set_morsel_range`) — no shared PG
//! `ParallelBlockTableScanDesc` is involved.
//!
//! STAGE 1 SCOPE: the producer body only. The leader ceremony that creates the
//! parallel context, launches the bound workers, and runs the concurrent drain
//! is Stage 2 (`engage_passthrough`); the `execute_plan` gated hook is Stage 3.
//! Kill-switch gated (`PGRUST_RUNTIME_ROW_FUNNEL`), default OFF — no call site
//! yet, so `dead_code` is allowed until Stage 3 wires it.
#![allow(dead_code)]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;

use runtime::{DrainStep, RowFunnel};

use super::batch_source::{BatchGranuleSource, SeqScanSource};
use super::row_emit::{MinImage, RowEmitSink};

/// `*const PlannedStmt` shipped to the bound worker threads. The pstmt lives in
/// the leader arena and outlives every worker (DestroyParallelContext joins
/// them before the arena unwinds — the same SendConst contract as the agg arm).
struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: leader-arena pstmt, immutable, alive until the ceremony joins every
// worker; workers only read it.
unsafe impl Send for SendConstPstmt {}
unsafe impl Sync for SendConstPstmt {}

/// Shared work body of the passthrough taskset (the funnel producer side).
pub(super) struct PassthroughShared {
    rt: &'static Arc<runtime::Runtime>,
    /// Weak: the RG's taskset holds this as its work; a strong handle would
    /// leak the cycle. Upgrade fails only after the leader dropped its
    /// handles, when nothing executes morsels.
    rg: OnceLock<runtime::WeakRgHandle>,
    /// The parallel context's shared binder target (set right after
    /// InitializeParallelDSM, before any worker launches — Stage 2).
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    /// The worker `SeqScan` pstmt (the ORIGINAL serial plan tree — a bare scan,
    /// since route_to is not flipped and the planner made no parallel plan).
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// The row-emit funnel: one ring per worker index, drained by the leader.
    funnel: Arc<RowFunnel<MinImage>>,
    /// Workers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Workers that bound and built their executor.
    started: AtomicUsize,
    /// Launched workers that have EXITED their drive frame (liveness reap).
    exited: AtomicUsize,
    /// First worker-phase error.
    error: Mutex<Option<Box<PgError>>>,
    /// Set when any worker recorded an error (fast skip for later morsels).
    failed: AtomicBool,
}

impl PassthroughShared {
    pub(super) fn new(
        rt: &'static Arc<runtime::Runtime>,
        pstmt: *const PlannedStmt<'static>,
        query_text: String,
        eflags: i32,
        funnel: Arc<RowFunnel<MinImage>>,
    ) -> Arc<PassthroughShared> {
        Arc::new(PassthroughShared {
            rt,
            rg: OnceLock::new(),
            pcxt_shared: OnceLock::new(),
            pstmt: SendConstPstmt(pstmt),
            query_text,
            eflags,
            funnel,
            refused: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            exited: AtomicUsize::new(0),
            error: Mutex::new(None),
            failed: AtomicBool::new(false),
        })
    }

    pub(super) fn set_rg(&self, rg: runtime::WeakRgHandle) {
        let _ = self.rg.set(rg);
    }

    pub(super) fn set_pcxt_shared(&self, shared: Arc<parallel::ParallelShared>) {
        let _ = self.pcxt_shared.set(shared);
    }

    pub(super) fn funnel(&self) -> &Arc<RowFunnel<MinImage>> {
        &self.funnel
    }

    pub(super) fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        // Abort the RG so the leader drain observes completion (Aborted) and
        // the producers stop; close demand so a parked producer wakes too.
        self.funnel.close_demand();
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }
}

/// Per-worker (thread-local) executor state: a fresh `QueryDesc` over the
/// leader-arena pstmt plus this worker's `RowEmitSink` (bound to its ring).
struct WorkerExecPt {
    qd: ::types_portal::QueryDescHandle,
    sink: RowEmitSink,
}

thread_local! {
    static WORKER_EXEC_PT: std::cell::RefCell<Option<WorkerExecPt>> =
        const { std::cell::RefCell::new(None) };
}

impl runtime::TaskSetWork for PassthroughShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) || self.funnel.demand_closed() {
            // Already aborting or LIMIT satisfied: drop the claim without work
            // (aborted/closed generations need not execute every granule).
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "passthrough worker panicked in a morsel").into());
            }
        }
    }

    fn finalize(&self) {
        // Streaming taskset: nothing to combine. Publish producers-done so the
        // leader drain reaches EOF once every ring is also drained.
        self.funnel.mark_all_done();
    }
}

impl PassthroughShared {
    /// Build this worker's executor once (first claimed morsel of the drive).
    fn ensure_worker_exec(&self, worker: usize) -> PgResult<()> {
        if WORKER_EXEC_PT.with(|cell| cell.borrow().is_some()) {
            return Ok(());
        }
        WORKER_EXEC_PT.with(|cell| -> PgResult<()> {
            if let Some(stale) = cell.borrow_mut().take() {
                crate::querydesc::release_query_desc_seam(stale.qd);
            }
            // SAFETY: leader-arena pstmt, alive until the ceremony joins this
            // worker (SendConst contract).
            let pstmt: &PlannedStmt<'_> = unsafe { &*self.pstmt.0 };
            let qd = crate::querydesc::create_query_desc_seam(
                pstmt,
                &self.query_text,
                Some(::snapmgr::GetActiveSnapshot()),
                None,
                ::types_dest::CommandDest::None,
                ::types_portal::ParamListHandle::NULL,
                ::types_portal::QueryEnvHandle::NULL,
                0,
            )?;
            let built = (|| -> PgResult<()> {
                crate::execmain::executor_start_seam(qd, self.eflags)?;
                // The worker plan root must be a bare SeqScan (the eligibility
                // gate, Stage 3, guarantees it; check defensively here).
                crate::querydesc::with_qd(qd, |q| {
                    let x = q.exec.as_mut().expect("passthrough worker ExecutorStart");
                    x.with_mut(|d| -> PgResult<()> {
                        match d.planstate.as_ref() {
                            Some(crate::procnode::PlanStateNode::SeqScan(_)) => Ok(()),
                            _ => Err(Box::new(PgError::new(
                                ERROR,
                                "passthrough worker plan root is not a bare SeqScan",
                            ))),
                        }
                    })
                })?;
                Ok(())
            })();
            if let Err(e) = built {
                crate::querydesc::release_query_desc_seam(qd);
                return Err(e);
            }
            let sink = RowEmitSink::new(self.funnel.producer(worker), None);
            *cell.borrow_mut() = Some(WorkerExecPt { qd, sink });
            Ok(())
        })
    }

    fn morsel_body(&self, worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        self.ensure_worker_exec(worker)?;
        WORKER_EXEC_PT.with(|cell| {
            let mut b = cell.borrow_mut();
            let ex = b.as_mut().expect("passthrough morsel without a bound executor");
            let qd = ex.qd;
            let sink = &mut ex.sink;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("passthrough worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "passthrough worker plan root is not a bare SeqScan",
                        )));
                    };
                    let mut src = SeqScanSource::new(&mut *ss);
                    // Heap sources have no interior boundaries → one positioned
                    // range (`Segments::whole`); the segment loop matches the
                    // fold arm's `drive_claim_segments` shape.
                    let mut segs = runtime::Segments::whole(range.start..range.end);
                    while let Some(seg) = segs.next() {
                        src.position(estate, seg)?;
                        if !emit_drain(sink, &mut src, estate)? {
                            // Demand closed (LIMIT): stop this claim.
                            break;
                        }
                        if segs.more() && (self.failed.load(Ordering::SeqCst)
                            || self.funnel.demand_closed())
                        {
                            break;
                        }
                    }
                    Ok(())
                })
            })
        })
    }
}

/// Drive one positioned segment: `next_batch` → per surviving row `emit(i)` →
/// `RowEmitSink::emit_blocking` (materialize + blocking push). Returns `false`
/// iff demand closed (LIMIT) — the caller stops. Mirrors the fold drain's
/// batch loop with the sink swapped for the funnel producer.
fn emit_drain<'a, 'mcx>(
    sink: &mut RowEmitSink,
    src: &mut SeqScanSource<'a, 'mcx>,
    estate: &mut ::executils::EStateData<'mcx>,
) -> PgResult<bool> {
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's pin (fold-drain parity).
            if let Some(b) = src.seq_scan_bridge() {
                let mcx = estate.es_query_cxt;
                ::exectuples::exec_clear_tuple(estate.slot_mut(b.ss.ss_ScanTupleSlot), mcx);
            }
            return Ok(true);
        }
        ::postgres_seams::check_for_interrupts::call()?;
        for i in 0..n {
            if let Some(slot) = src.emit(estate, i)? {
                if !sink.emit_blocking(slot, estate)? {
                    return Ok(false);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stage 2: bgworker main + registration + the leader ceremony.
// ---------------------------------------------------------------------------

/// Release this worker's thread-local executor. `clean` = finish/end/free (a
/// drive that completed); else release (mid-batch executor on an error path) —
/// the agg arm's `teardown_worker_exec` discipline.
fn teardown_worker_exec_pt(clean: bool) -> PgResult<()> {
    WORKER_EXEC_PT.with(|cell| -> PgResult<()> {
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

/// The bound-context worker body: lease a lane, drive the pinned RG (claims
/// morsels → `run_morsel` → produce into the ring), then tear down. Errors
/// recorded payload-side (the leader rethrows PLAIN). Mirrors the agg arm's
/// `helper_drive_entry` minus the fold/instrument specifics.
fn helper_drive_entry_pt(payload: &Arc<PassthroughShared>) -> PgResult<()> {
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return Ok(()) };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let mut local = lane.local();
    payload.started.fetch_add(1, Ordering::SeqCst);
    let _outcome = payload.rt.drive_pinned(&mut local, &rg);
    let self_errored = payload.failed.load(Ordering::SeqCst);
    let teardown = teardown_worker_exec_pt(!self_errored);
    if let Err(e) = teardown {
        payload.fail(e);
        return Err(Box::new(PgError::new(
            ERROR,
            "passthrough worker failed (see leader error)",
        )));
    }
    if self_errored {
        return Err(Box::new(PgError::new(
            ERROR,
            "passthrough worker failed (see leader error)",
        )));
    }
    Ok(())
}

/// Registered bgworker entrypoint (`pgrust_runtime_passthrough_main`).
fn runtime_passthrough_worker_main(shared: &parallel::ParallelShared) -> PgResult<()> {
    let Some(private) = shared.private() else { return Ok(()) };
    let Ok(payload) = private.downcast::<PassthroughShared>() else { return Ok(()) };
    // Every launched helper bumps `exited` exactly once on every exit path
    // (the leader's liveness reap counts these against `launched`).
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive_entry_pt(&payload)));
    let outcome = match r {
        Ok(o) => o,
        Err(unwind) => {
            payload.fail(PgError::new(ERROR, "passthrough helper panicked").into());
            let _ = teardown_worker_exec_pt(false);
            if parallel::standing::is_exit_unwind(&*unwind) {
                latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                    shared.parallel_leader_proc_number,
                ));
                std::panic::resume_unwind(unwind);
            }
            Err(Box::new(PgError::new(
                ERROR,
                "passthrough worker failed (see leader error)",
            )))
        }
    };
    // Wake the parked/looping leader: completion/refusal/error re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
    outcome
}

fn ensure_passthrough_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_passthrough_main",
            runtime_passthrough_worker_main,
        );
    });
}

/// Abort + drain a pinned RG to completion (the teardown-tail / error path):
/// close demand so any parked producer wakes and settles, then drive the RG
/// down via a leader-acquired external lane. Bounded; returns whether drained.
fn drain_rg_pt(
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    rg: &runtime::RgHandle,
) -> bool {
    rg.abort();
    funnel.close_demand();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else { return false };
    let mut local = lane.local();
    rt.try_drain_pinned(&mut local, rg, 4000).is_some()
}

pub(super) enum PassthroughEngageOutcome {
    /// The parallel path could not run (no workers, all refused); the caller
    /// runs the serial arm.
    Fallback,
    /// The scan completed through the funnel; `.0` rows were emitted.
    Completed(u64),
}

/// The leader ceremony (Stage 2): create the parallel context, submit the
/// pinned passthrough RG, launch bound workers, then run the funnel drain
/// CONCURRENTLY as a pure consumer (woven into the WaitForParallelWorkers-shaped
/// loop; the drain never parks, so the message/liveness poll always runs and a
/// producer parked on a full ring is freed within one bounded quantum).
/// `emit_row` receives each drained row image and returns `false` to stop
/// (client stop); `limit` closes demand once satisfied (LIMIT).
#[allow(clippy::too_many_arguments)]
pub(super) fn engage_passthrough(
    rt: &'static Arc<runtime::Runtime>,
    pstmt: *const PlannedStmt<'static>,
    query_text: &str,
    eflags: i32,
    dop: i32,
    source: Arc<dyn runtime::MorselSource>,
    ring_cap: usize,
    limit: Option<u64>,
    emit_row: impl FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<PassthroughEngageOutcome> {
    ensure_passthrough_hooks_registered();
    let funnel: Arc<RowFunnel<MinImage>> =
        RowFunnel::new(rt.nthreads() + runtime::MAX_EXTERNAL_LANES, ring_cap);
    let payload =
        PassthroughShared::new(rt, pstmt, query_text.to_string(), eflags, Arc::clone(&funnel));

    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_passthrough_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;
    let funnel_body = Arc::clone(&funnel);

    let body = (move |mut_submitted: &mut Option<runtime::RgHandle>,
                      mut emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>|
          -> PgResult<PassthroughEngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        if parallel::nworkers(pcxt) <= 0 {
            return Ok(PassthroughEngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload.set_pcxt_shared(parallel::shared_for(pcxt));
        parallel::set_private(pcxt, Arc::clone(&payload) as _);

        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(&payload) as _;
        static NEXT_QID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned_with_affinity(
            runtime::QuerySpec {
                query_id: NEXT_QID.fetch_add(1, Ordering::SeqCst) as u64,
                tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
            },
            0,
        );
        payload.set_rg(rg.downgrade());
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            drain_rg_pt(rt, &funnel_body, &rg);
            return Ok(PassthroughEngageOutcome::Fallback);
        }

        let mut drain = funnel_body.drain();
        let mut emitted: u64 = 0;
        let mut stop_emitting = false;
        let mut all_exited_seen = false;

        // Non-blocking drain pass: emit every currently-available row, freeing
        // producers parked on full rings. Never parks (so the poll below runs).
        let mut pump = |drain: &mut runtime::FunnelDrain<MinImage>,
                        emitted: &mut u64,
                        stop: &mut bool,
                        emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>|
         -> PgResult<()> {
            loop {
                match drain.next() {
                    DrainStep::Row(img) => {
                        if *stop {
                            drop(img);
                            continue;
                        }
                        let cont = emit_row(img)?;
                        *emitted += 1;
                        if !cont || limit.is_some_and(|n| *emitted >= n) {
                            *stop = true;
                            funnel_body.close_demand();
                        }
                    }
                    DrainStep::Idle | DrainStep::Eof => return Ok(()),
                }
            }
        };

        let outcome = loop {
            if let Err(e) = pump(&mut drain, &mut emitted, &mut stop_emitting, emit_row) {
                rg.abort();
                drain_rg_pt(rt, &funnel_body, &rg);
                return Err(e);
            }
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg_pt(rt, &funnel_body, &rg);
                return Err(e);
            }
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                rg.abort();
                drain_rg_pt(rt, &funnel_body, &rg);
                return Ok(PassthroughEngageOutcome::Fallback);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                rg.abort();
                let drained = drain_rg_pt(rt, &funnel_body, &rg);
                if payload.started.load(Ordering::SeqCst) == 0 && drained {
                    return Ok(PassthroughEngageOutcome::Fallback);
                }
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "passthrough helpers exited before completing the scan",
                )));
            }
            if payload.exited.load(Ordering::SeqCst) >= launched as usize {
                if all_exited_seen && waiter.try_wait().is_none() {
                    rg.abort();
                    drain_rg_pt(rt, &funnel_body, &rg);
                    continue;
                }
                all_exited_seen = true;
            }
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg_pt(rt, &funnel_body, &rg);
                return Err(e);
            }
        };

        // Post-completion tail: finalize marked every ring done, so drain the
        // buffered remainder to EOF.
        pump(&mut drain, &mut emitted, &mut stop_emitting, emit_row)?;

        if let Some(e) = payload.take_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "passthrough pipeline aborted")));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(PassthroughEngageOutcome::Fallback);
        }
        Ok(PassthroughEngageOutcome::Completed(emitted))
    })(&mut submitted, &mut { emit_row });

    // Teardown tail: a submitted RG must be COMPLETE before DestroyParallelContext.
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg_pt(rt, &funnel, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;
    Ok(outcome)
}
