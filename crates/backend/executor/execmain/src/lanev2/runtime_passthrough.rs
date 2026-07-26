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
    /// Leader-producer mode: the leader's caller-worker lane ordinal (its
    /// `run_morsel` worker index). Set before the caller drive starts;
    /// `run_morsel` takes the NON-PARKING leader emit path for this index.
    leader_lane: OnceLock<usize>,
    /// Leader-producer mode: the leader's produced images. The leader NEVER
    /// parks producing (it is the drainer — a park would self-deadlock), so
    /// its emit is stash-append instead of `push_blocking`; the pump drains
    /// the stash to the wire alongside the rings. Bounded by ONE claim's rows
    /// (the drain-first claim gate ends the leader's task at the next claim
    /// boundary once the stash is non-empty). Leader-thread-only in practice;
    /// the Mutex is uncontended shape (Send/Sync for the work body).
    leader_stash: Mutex<Vec<MinImage>>,
    /// W2a inc-2: when set, workers write the target THEMSELVES over private
    /// block runs (write_blockrun.rs) instead of ring-emitting; the rings
    /// stay empty and the leader only joins + sums counts.
    blockrun: Option<Arc<super::write_blockrun::BlockRunShared>>,
}

impl PassthroughShared {
    pub(super) fn new(
        rt: &'static Arc<runtime::Runtime>,
        pstmt: *const PlannedStmt<'static>,
        query_text: String,
        eflags: i32,
        funnel: Arc<RowFunnel<MinImage>>,
        blockrun: Option<Arc<super::write_blockrun::BlockRunShared>>,
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
            leader_lane: OnceLock::new(),
            leader_stash: Mutex::new(Vec::new()),
            blockrun,
        })
    }

    fn set_leader_lane(&self, lane: usize) {
        let _ = self.leader_lane.set(lane);
    }

    fn leader_stash_empty(&self) -> bool {
        self.leader_stash.lock().unwrap_or_else(|p| p.into_inner()).is_empty()
    }

    /// Drain the leader stash (pump side; leader thread only).
    fn take_leader_stash(&self, into: &mut Vec<MinImage>) {
        let mut g = self.leader_stash.lock().unwrap_or_else(|p| p.into_inner());
        into.append(&mut g);
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
/// leader-arena pstmt plus this worker's `RowEmitSink` (bound to its ring)
/// and, in block-run mode, its direct-write half (target handle + W1 buffer
/// + run-fed bistate).
struct WorkerExecPt {
    qd: ::types_portal::QueryDescHandle,
    sink: RowEmitSink,
    write: Option<super::write_blockrun::WorkerWriteState>,
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
            // Block-run mode: build the direct-write half BEFORE the first
            // row is produced; a refusal here (target open / needs_wal
            // disagreement) errors the worker, which fails the statement —
            // shape-level fallback happened at the leader's admission, so a
            // mid-flight surprise must never silently change the write path.
            let write = match self.blockrun.as_ref() {
                Some(shared) => match super::write_blockrun::WorkerWriteState::begin(shared) {
                    Ok(ws) => Some(ws),
                    Err(e) => {
                        crate::querydesc::release_query_desc_seam(qd);
                        return Err(e);
                    }
                },
                None => None,
            };
            let sink = RowEmitSink::new(self.funnel.producer(worker));
            *cell.borrow_mut() = Some(WorkerExecPt { qd, sink, write });
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
            let write = &mut ex.write;
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
                    // Leader-producer mode: the leader's claims emit into the
                    // stash (non-parking); workers emit through their rings.
                    let stash = (self.leader_lane.get() == Some(&worker))
                        .then_some(&self.leader_stash);
                    // Heap sources have no interior boundaries → one positioned
                    // range (`Segments::whole`); the segment loop matches the
                    // fold arm's `drive_claim_segments` shape.
                    let mut segs = runtime::Segments::whole(range.start..range.end);
                    while let Some(seg) = segs.next() {
                        src.position(estate, seg)?;
                        let cont = match write.as_mut() {
                            // W2a inc-2: rows land in THIS worker's run-fed
                            // write buffer; nothing rides the ring.
                            Some(ws) => emit_drain_write(ws, &mut src, estate)?,
                            None => emit_drain(sink, &mut src, estate, stash)?,
                        };
                        if !cont {
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
/// the sink emit — `RowEmitSink::emit_blocking` (materialize + blocking push)
/// for workers, `RowEmitSink::emit_stash` (materialize + non-parking stash
/// append) for the leader-producer (`stash` = Some). Returns `false` iff
/// demand closed (LIMIT) — the caller stops. Mirrors the fold drain's batch
/// loop with the sink swapped for the funnel producer.
/// W2a inc-2 twin of `emit_drain`: each surviving row goes to the worker's
/// OWN write state (run-fed W1 multi-insert) instead of the funnel ring.
/// Write engagements never close demand, so this only returns `false` on the
/// abort path (`close_demand` from a failing sibling — checked by the caller
/// via `failed` at segment boundaries; the emit itself cannot observe it).
fn emit_drain_write<'a, 'mcx>(
    ws: &mut super::write_blockrun::WorkerWriteState,
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
                ws.receive(estate, slot)?;
            }
        }
    }
}

fn emit_drain<'a, 'mcx>(
    sink: &mut RowEmitSink,
    src: &mut SeqScanSource<'a, 'mcx>,
    estate: &mut ::executils::EStateData<'mcx>,
    stash: Option<&std::sync::Mutex<Vec<MinImage>>>,
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
                let cont = match stash {
                    Some(s) => sink.emit_stash(slot, estate, s)?,
                    None => sink.emit_blocking(slot, estate)?,
                };
                if !cont {
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
        let Some(mut ex) = cell.borrow_mut().take() else { return Ok(()) };
        // Block-run SEAL half (W2a inc-2): the clean path flushes the W1
        // tail into this worker's run and publishes the count — a seal error
        // is a worker error (the leader re-checks after the gang join, so a
        // post-RG-completion failure still fails the statement). The error
        // path abandons: unflushed copies drop, the aborted transaction
        // kills the flushed pages with the relfilenode.
        let seal = match ex.write.take() {
            Some(ws) if clean => ws.seal(),
            Some(ws) => {
                ws.abandon();
                Ok(())
            }
            None => Ok(()),
        };
        if clean && seal.is_ok() {
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
            seal
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
///
/// `pure_drain` (W0.1, GL-W0-1 lever 1): the leader NEVER alternates into
/// producing, even when leader-producer mode is on. Write dests set this —
/// the leader IS the writer there, and every leader morsel stalls the write
/// drain (GL-W0-1 measured Gather beating the funnel 1.3-2.4x on writes with
/// the producing leader; wire drains keep the GL-FUNNEL-2 default). Producer-
/// count parity falls back to DOP+1 admission (funnel_plus1), unchanged.
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
    pure_drain: bool,
    blockrun: Option<Arc<super::write_blockrun::BlockRunShared>>,
    emit_row: impl FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<PassthroughEngageOutcome> {
    // Block-run engagements are pure-drain by construction (the leader owns
    // finalize only; a producing leader would need its own write state and
    // the who-finalizes analysis — explicitly W2b).
    debug_assert!(blockrun.is_none() || pure_drain);
    ensure_passthrough_hooks_registered();
    let funnel: Arc<RowFunnel<MinImage>> =
        RowFunnel::new(rt.nthreads() + runtime::MAX_EXTERNAL_LANES, ring_cap);
    // Review fix #2 (leader mid-drive wake): producers SET THE LEADER'S LATCH
    // on every push / done / full-ring park, so the drain below wakes
    // immediately instead of at the bounded recheck quantum (which stays as a
    // backstop only — without this, every ring-full batch cost a full quantum,
    // and a non-expiring quantum would deadlock). Captured on the leader
    // thread BEFORE any producer starts; SetLatch on an already-set latch is a
    // cheap flag test, and set-after-push ordering against the loop's
    // check-then-wait makes it lost-wake-free (standard latch protocol).
    let leader_proc = init_small::globals::MyProcNumber();
    funnel.set_wake_hook(Box::new(move || {
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(leader_proc));
    }));
    let payload = PassthroughShared::new(
        rt,
        pstmt,
        query_text.to_string(),
        eflags,
        Arc::clone(&funnel),
        blockrun,
    );

    // EnterParallelMode brackets the parallel-context lifetime
    // (CreateParallelContext asserts it; the agg arm's engage discipline,
    // runtime_scan.rs). The hook engages from a SERIAL plan, so the executor's
    // own use_parallel_mode bracket never ran. An error RETURN exits the mode
    // below (the context is destroyed on every return path first); an error
    // UNWIND aborts the transaction, which destroys live contexts and resets
    // the mode (AtEOXact_Parallel — the Gather discipline).
    ::xact::EnterParallelMode();
    let r =
        engage_passthrough_inner(rt, &funnel, &payload, dop, source, limit, pure_drain, emit_row);
    ::xact::ExitParallelMode();
    r
}

/// Everything between Enter/ExitParallelMode: create the context, submit the
/// pinned RG, launch, run the leader drain loop, tear down. On ANY return path
/// the parallel context is destroyed (workers joined) and the RG completed and
/// drained before the caller's arena can unwind.
#[allow(clippy::too_many_arguments)]
fn engage_passthrough_inner(
    rt: &'static Arc<runtime::Runtime>,
    funnel: &Arc<RowFunnel<MinImage>>,
    payload: &Arc<PassthroughShared>,
    dop: i32,
    source: Arc<dyn runtime::MorselSource>,
    limit: Option<u64>,
    pure_drain: bool,
    emit_row: impl FnMut(MinImage) -> PgResult<bool>,
) -> PgResult<PassthroughEngageOutcome> {
    // W0.1: a pure-drain engagement (write dests) suppresses leader-producer
    // mode entirely — the DOP+1 arm below supplies the +1 producer instead.
    let leader_mode = funnel_leader_mode() && !pure_drain;
    // GL-FUNNEL-2 producer-count parity vs classic Gather (whose leader
    // participates, giving N+1 producers at DOP N): leader-producer mode makes
    // the funnel leader the +1 (gang stays N); otherwise DOP+1 admission
    // launches one extra gang producer (default ON; _PLUS1=0 restores N).
    let launch_n = if leader_mode {
        dop
    } else if funnel_plus1() {
        dop + 1
    } else {
        dop
    };
    let pcxt =
        parallel::CreateParallelContext("postgres", "pgrust_runtime_passthrough_main", launch_n)?;
    let mut submitted: Option<runtime::RgHandle> = None;
    let funnel_body = Arc::clone(funnel);

    let body = (move |mut_submitted: &mut Option<runtime::RgHandle>,
                      mut emit_row: &mut dyn FnMut(MinImage) -> PgResult<bool>|
          -> PgResult<PassthroughEngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        if parallel::nworkers(pcxt) <= 0 {
            return Ok(PassthroughEngageOutcome::Fallback);
        }
        // Install the REAL session policy (not default): for the wire path
        // every probed flag is false by the hook's gates (identical encoding);
        // for W0 write dests `pending_invalidations` rides along so a parked-
        // helper adoption (never used by this launched-only ceremony today)
        // would fail-closed refuse in validate() rather than bind blind.
        parallel::InstallQueryTaskBinding(pcxt, parallel::query_task_policy_probe())?;
        payload.set_pcxt_shared(parallel::shared_for(pcxt));
        parallel::set_private(pcxt, Arc::clone(payload) as _);

        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(payload) as _;
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

        // Non-blocking drain pass: emit every currently-available row (worker
        // rings round-robin, then the leader-producer stash), freeing
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
                    DrainStep::Idle | DrainStep::Eof => break,
                }
            }
            // Leader-producer stash (empty in every other mode): one lock per
            // pump pass, bounded by one leader claim's rows.
            if !payload.leader_stash_empty() {
                let mut sb: Vec<MinImage> = Vec::new();
                payload.take_leader_stash(&mut sb);
                for img in sb {
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
            }
            Ok(())
        };

        // GL-FUNNEL-2 increment 2 — LEADER-PRODUCER MODE: the leader drives
        // the SAME pinned RG through the sanctioned caller-worker machinery
        // (`drive_with_duties_parked`), alternating drain passes with morsel
        // production. Fail-closed: lane exhaustion falls through to the
        // pure-drain loop below.
        //
        // INVARIANT RE-PROOF for the producing leader (the funnel.rs
        // invariant #4 pure-drain argument no longer applies verbatim):
        // 1. DRAIN-FIRST at claim granularity: `claim_duty` admits a claim
        //    ONLY when every ring AND the stash read empty; it also runs at
        //    every claim boundary of a leader task, ending the task as soon
        //    as anything is drainable. The leader is "busy producing" only
        //    when there was nothing to drain, for at most ONE claim.
        // 2. NO SELF-DEADLOCK: the leader's emit is `emit_stash` — a
        //    non-parking append — never `push_blocking`. The leader can
        //    therefore never park as a producer; its only waits are the
        //    bounded `idle_park` latch quantum (armed drain-waiter flag →
        //    any worker push sets the latch) and never-while-holding-work.
        // 3. WORKERS PARKED ON FULL RINGS ARE FREED IN BOUNDED TIME: a
        //    leader claim is bounded (sizer-bounded block range); after it,
        //    control returns to the step loop whose `duty` pumps every ring.
        //    No wait cycle exists: workers wait only on the leader's pump,
        //    which the leader reaches after bounded work; the leader waits
        //    only on the latch with a bounded quantum + wake hook.
        // 4. LAST-WORKER-OUT / GENERATION UNCHANGED: the leader participates
        //    through the ordinary external-lane pinned-step machinery
        //    (worker_step_pinned) — pin board, fin_counter, generation join
        //    all standard; `finalize` (mark_all_done) may run on the leader,
        //    which is sound (atomic stores + wakes).
        // 5. STASH BOUND: one claim's rows (drain-first gate #1) — the same
        //    order of buffering as Gather's per-worker tuple queue.
        // 6. ERROR DISCIPLINE: a duty/park error aborts + drains the RG
        //    inside the caller drive (CallerWorker contract) before Err
        //    surfaces; worker errors abort the RG via payload.fail and
        //    surface from the post-completion take_error, as in pure-drain
        //    mode. The all-refused/all-stopped liveness reaps are NOT needed
        //    here: the leader itself drives the RG to completion even if
        //    every gang worker refuses or dies without error (their error
        //    path still aborts the RG through the message channel duty).
        if leader_mode {
            if let Some(mut cw) = runtime::CallerWorker::enter(rt) {
                // run_morsel receives the PIN-BOARD worker index, which for an
                // external lane is `nthreads + lane ordinal` (lib.rs worker
                // index space) — NOT the bare lane ordinal. The first smoke of
                // this mode hung on exactly that mismatch: the leader took the
                // worker push_blocking path and parked on its own full ring.
                payload.set_leader_lane(rt.nthreads() + cw.lane_ordinal());
                // The leader is a real participant: count it started so the
                // post-completion "started == 0 → Fallback" can never rerun a
                // scan whose rows the leader already emitted.
                payload.started.fetch_add(1, Ordering::SeqCst);
                let drive = {
                    let pump = &mut pump;
                    let drain = &mut drain;
                    let emitted = &mut emitted;
                    let stop_emitting = &mut stop_emitting;
                    let mut duty = || -> PgResult<()> {
                        // Waiter-flag pattern, caller form: arm, then pump
                        // (the sweep); a push after the arm sets the latch.
                        funnel_body.arm_drain_wait();
                        pump(drain, emitted, stop_emitting, emit_row)?;
                        ::postgres_seams::check_for_interrupts::call()?;
                        parallel::ProcessParallelMessages()?;
                        Ok(())
                    };
                    let mut claim_duty = || {
                        !funnel_body.demand_closed()
                            && funnel_body.all_rings_empty()
                            && payload.leader_stash_empty()
                    };
                    let mut idle_park =
                        || -> PgResult<()> { parallel::wait_parallel_finish_quantum() };
                    cw.drive_with_duties_parked(
                        rt,
                        &rg,
                        &mut duty,
                        &mut claim_duty,
                        &mut idle_park,
                    )
                };
                let outcome = match drive {
                    Ok(o) => o,
                    Err(e) => {
                        // CallerWorker discipline: the RG is already aborted
                        // AND drained. Release the leader's own executor
                        // (mid-batch possible) and surface.
                        let _ = teardown_worker_exec_pt(false);
                        return Err(e);
                    }
                };
                // Tear down the leader's thread-local executor (built iff the
                // leader claimed at least one morsel; no-op otherwise) BEFORE
                // returning — the session thread must never carry it into a
                // later query.
                let self_errored = payload.failed.load(Ordering::SeqCst);
                teardown_worker_exec_pt(!self_errored)?;
                // Post-completion tail: drain rings + stash to EOF.
                pump(&mut drain, &mut emitted, &mut stop_emitting, emit_row)?;
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                if outcome == runtime::RgOutcome::Aborted {
                    ::postgres_seams::check_for_interrupts::call()?;
                    return Err(Box::new(PgError::new(
                        ERROR,
                        "passthrough pipeline aborted",
                    )));
                }
                return Ok(PassthroughEngageOutcome::Completed(emitted));
            }
            // Lanes exhausted: fall through to pure-drain (fail-closed).
        }

        let outcome = loop {
            // The waiter-flag wait pattern, LATCH form (funnel.rs protocol
            // doc): ARM the drain waiter flag, THEN pump. A push ordered after
            // the arm-fence sees the flag and sets the leader's latch (the
            // wake hook), so the WaitLatch quantum below returns immediately;
            // one ordered before it is drained by this pump. Re-armed every
            // iteration (the waking push consumes the flag).
            funnel_body.arm_drain_wait();
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
            drain_rg_pt(rt, funnel, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;
    // SEAL BARRIER error recheck (W2a inc-2, general for every mode): a
    // worker's post-RG-completion teardown (block-run tail flush, executor
    // finish) can record an error AFTER the leader's drain loop took the
    // completed outcome. DestroyParallelContext joined every worker above,
    // so any late error is visible now — a Completed with a failed seal must
    // fail the statement, never under-write silently.
    if let Some(e) = payload.take_error() {
        return Err(e);
    }
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Stage 3: the gated execute_plan hook.
// ---------------------------------------------------------------------------

/// Stage-4 smoke observability: cumulative (engaged, completed) counters —
/// `engaged` bumps when every eligibility gate passed and the ceremony was
/// entered; `completed` bumps when the funnel answered the run. The e2e smoke
/// asserts engagement positively (byte-identical rows alone cannot distinguish
/// the funnel from the serial loop) and asserts NON-engagement on the
/// fail-closed paths (count-limited refusal). Diagnostic-only.
static PT_ENGAGED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static PT_COMPLETED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn funnel_engagements() -> (u64, u64) {
    (
        PT_ENGAGED.load(Ordering::SeqCst),
        PT_COMPLETED.load(Ordering::SeqCst),
    )
}

/// Degree of parallelism for the funnel (`PGRUST_RUNTIME_ROW_FUNNEL_DOP`,
/// default 2). The funnel is experimental/gated, so DOP is a knob rather than
/// the planner's estimate.
fn funnel_dop() -> i32 {
    std::env::var("PGRUST_RUNTIME_ROW_FUNNEL_DOP")
        .ok()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .filter(|&d| d > 0)
        .unwrap_or(2)
}

/// GL-FUNNEL-2 increment 1 — DOP+1 admission (default ON;
/// `PGRUST_RUNTIME_ROW_FUNNEL_PLUS1=0` restores N producers): launch N+1 gang
/// producers at funnel DOP N, matching classic Gather's (N+1)/N producer count
/// (its leader participates) while the funnel leader stays a pure drain — the
/// structural deficit GL-FUNNEL-1 measured as most of the ≤1GB funnel/gather
/// gap. Superseded (not stacked) by leader-producer mode below: with the
/// leader producing, the gang stays at N (leader IS the +1).
fn funnel_plus1() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_ROW_FUNNEL_PLUS1").map_or(true, |v| v.trim() != "0")
    })
}

/// Leader-producer mode — THE DEFAULT ENGAGEMENT CONFIG as of the GL-FUNNEL-4
/// flip (`PGRUST_RUNTIME_ROW_FUNNEL_LEADER=0` restores the pure-drain leader
/// with DOP+1 admission): the leader alternates drain passes with producing
/// morsels through the sanctioned caller-worker machinery
/// (`runtime::CallerWorker::drive_with_duties_parked`) — invariant analysis at
/// the engage site. It dominated DOP+1 at every measured exp point (GL-2) and
/// carried the GL-4 decider (0.905/0.917 vs Gather) while using one fewer
/// gang slot.
fn funnel_leader_mode() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_ROW_FUNNEL_LEADER").is_ok_and(|v| v.trim() == "0")
    })
}

/// FLOORGUARD gate 3 predicate — the EMIT-FRACTION band, in WHOLE-PLAN units.
///
/// UNIT CONTRACT (pinned by the unit tests below): `plan_rows` must be the
/// planner's whole-plan post-qual output estimate and `reltuples` the full
/// table's analyzed tuple count, so the ratio is the TRUE emit fraction —
/// the unit the GL-FUNNEL ladder banked the band in (GL-1 measured
/// emitted/scanned on whole serial-shaped plans; the GL-5 knee sweep's
/// selectivity axis is the same unit) and the unit operators tune
/// `PGRUST_RUNTIME_ROW_FUNNEL_EMIT_MAX_PCT` in.
///
/// A PARALLEL FRAGMENT's `plan_rows` is NOT in that unit: the planner
/// divides a partial path's row estimate by `get_parallel_divisor(workers)`
/// (workers plus the leader's damped contribution) at costing time, so a
/// fragment handed here would price `true_fraction / divisor` against the
/// full `reltuples` — a 33%-emit qual read as 8.3% at 4 planned workers
/// (divisor 4.0), 10.75% at 3 (3.1), 13.9% at 2 (2.4): the admission
/// enabler of the worker-side duplication bug. The `Plan` node does not
/// carry `parallel_workers`, so the divisor is not derivable from the
/// fragment and exact un-division is impossible; the scale-consistent form
/// is therefore to REFUSE per-participant estimates outright
/// (`parallel_aware`, fail-closed — categorical, even at the band-disabling
/// 100 setting) and compare only whole-plan estimates. For the one node
/// shape this gate ever admits (a bare heap `SeqScan` top node),
/// `parallel_aware` is exactly the divided-estimate marker: the planner
/// builds partial seqscan paths parallel-aware, and a serial seqscan path
/// is never divided.
///
/// Also fail-closed on missing stats (never-analyzed `reltuples <= 0`) and
/// on a non-positive estimate: an unproven fraction refuses to the serial
/// loop.
fn floorguard_emit_band_admits(
    parallel_aware: bool,
    plan_rows: f64,
    reltuples: f64,
    emit_max_pct: f64,
) -> bool {
    if parallel_aware {
        // Per-participant scale — never comparable against full reltuples;
        // refuse before any ratio forms (and regardless of the knob).
        return false;
    }
    if emit_max_pct >= 100.0 {
        return true;
    }
    if reltuples <= 0.0 || plan_rows <= 0.0 {
        return false;
    }
    plan_rows / reltuples <= emit_max_pct / 100.0
}

/// World-B gated hook (Stage 3): when the row funnel is armed (default ON
/// since the GL-FUNNEL-4 flip; `PGRUST_RUNTIME_ROW_FUNNEL=0` kills) and the
/// plan is a lane-ownable bare passthrough `SeqScan`, run it in parallel through
/// the funnel and stream the rows to `dest`. Returns `true` iff it handled the
/// whole run (the caller then skips the serial per-tuple loop); `false` = not
/// eligible / fell back (the caller runs the serial loop, byte-identically).
pub(crate) fn try_passthrough_funnel<'mcx, 'd>(
    estate: &mut ::executils::EStateData<'mcx>,
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    number_tuples: u64,
    dest: &mut ::tcop_dest::DestReceiver<'d>,
) -> PgResult<bool> {
    // Kill switch (default OFF) — the cheap first test.
    if !super::row_emit::row_funnel_enabled() {
        return Ok(false);
    }
    // CRITICAL (review fix #1): the funnel is a COMPLETE-DRAIN engine only. A
    // count-limited run (extended-protocol Execute(portal, max_rows) — and any
    // suspendable portal cadence) must NOT engage: the funnel would emit
    // `number_tuples` rows, close demand, and destroy the parallel context;
    // the portal then SUSPENDS and the next Execute would re-engage a FRESH
    // funnel that rescans from block 0 → duplicated rows. A resumable funnel
    // is future work; fail closed to the serial loop, whose per-tuple state
    // survives suspend/resume.
    if number_tuples != 0 {
        return Ok(false);
    }
    // W0 funnel-into-writer (parallel-writes design §4; write_funnel.rs): a
    // write DestReceiver (IntoRel/TransientRel — CTAS / SELECT INTO / matview
    // datafill) engages only under its own kill switch, and fail-closes to the
    // serial loop otherwise. Non-write dests: unchanged rules.
    let write_dest = match super::write_funnel::classify_write_dest(dest.mydest()) {
        super::write_funnel::WriteDestVerdict::NotWrite => false,
        super::write_funnel::WriteDestVerdict::Admit => true,
        super::write_funnel::WriteDestVerdict::Refuse => return Ok(false),
    };
    // Fail-closed gates: no EPQ recheck, no junk filter, no cursor/SPI cadence,
    // and no instrumented run (review fix #3b: EXPLAIN ANALYZE node
    // instrumentation would silently read zero — the workers' per-node
    // instrumentation is never merged back on this arm).
    if estate.es_epq_active
        || estate.es_junkFilter.is_some()
        || estate.es_lane_cursor_parked
        || estate.es_cursor_run_budget.is_some()
        || estate.es_spi_run_budget.is_some()
        || estate.es_instrument != 0
    {
        return Ok(false);
    }
    // Not from within parallel machinery (the sibling arms' gate; every
    // runtime engagement carries it). A legacy Gather WORKER re-enters
    // execute_plan with its serial-shaped fragment (the serialized plan
    // clears parallelModeNeeded, so the caller's !use_parallel_mode guard
    // does not cover it): engaging here would (a) full-scan the relation
    // through a private granule map instead of the shared parallel scan
    // descriptor — every participant emits the complete result, and a
    // write destination persists the (workers+1)x duplication — and (b)
    // nest LaunchParallelWorkers inside the worker, whose
    // BecomeLockGroupLeader corrupts the in-flight lock-group membership
    // (debug builds assert at the stale lockGroupLeader). A leader already
    // in parallel mode must equally not stack a second context.
    if super::runtime_in_parallel_role() {
        return Ok(false);
    }
    // The runtime pool must be live.
    let Some(rt) = runtime::global() else { return Ok(false) };
    if !runtime::runtime_enabled() {
        return Ok(false);
    }
    // Top node must be a bare SeqScan.
    if !matches!(&*planstate, crate::procnode::PlanStateNode::SeqScan(_)) {
        return Ok(false);
    }
    // Plan + result descriptor (immutable planstate borrow, released before the
    // mutable scan-source build below).
    let Some(pstmt_ref) = estate.es_plannedstmt else { return Ok(false) };
    let Some(plan_node) = pstmt_ref.planTree else { return Ok(false) };
    let Some(plan) = plan_node.as_plan() else { return Ok(false) };
    if !plan.parallel_safe {
        return Ok(false);
    }
    // FLOORGUARD (GL-FUNNEL-4 flip band; GL-FUNNEL-1 recipe, all fail-closed):
    // 1. QUAL REQUIRED — bare no-qual passthroughs measured 1.55–2.3x losses
    //    (the drain ceiling); never admissible.
    //    W0 write dests are EXEMPT from the wire band (this gate and the
    //    emit-fraction gate below): those price the funnel against the serial
    //    WIRE emit, whereas a write drain's per-row baseline is a heap insert
    //    (WAL + extension + TOAST) — bulk (high-emit, unqualed) CTAS is the
    //    canonical write shape. Write admission is guarded by its own kill
    //    switch (default OFF) until the W0 fleet ladder prices a write band.
    if plan.qual.is_nil() && !write_dest {
        return Ok(false);
    }
    // 2. DOP >= 2 (no DOP-1 arm was ever measured; DOP2 already wins 0.59–0.75
    //    in-region).
    let dop = funnel_dop();
    if dop < 2 {
        return Ok(false);
    }
    let desc = planstate.exec_get_result_type(plan)?;

    // Binder policy: a shape the query-task binder would refuse must not launch.
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable {
        return Ok(false);
    }
    // Pending uncommitted-DDL invalidations: refused (parity with every other
    // arm's probe) EXCEPT for W0 write dests, whose statement SELF-CREATES the
    // target before the SELECT runs — the flag is unconditionally true there.
    // Admission is sound for the LAUNCHED-ONLY ceremony below: launched gang
    // workers bind through `parallel::parallel_worker_body`, whose
    // pending-invals arm skips the warm claim, runs InvalidateSystemCaches,
    // and notes the abort-poison taint (the shipped matview-datafill /
    // legacy-Gather precedent); the leader-producer sees its own state
    // natively. The parked-helper binder (`validate()`) still refuses these
    // targets — the InstallQueryTaskBinding policy below carries the real
    // probe so any future adoption path fail-closes.
    if policy.pending_invalidations && !write_dest {
        return Ok(false);
    }

    let pstmt: *const PlannedStmt<'static> =
        pstmt_ref as *const PlannedStmt<'mcx> as *const PlannedStmt<'static>;
    let query_text = estate.es_sourceText.unwrap_or("");
    let eflags = estate.es_top_eflags;
    let wire_mcx = estate.es_query_cxt;

    // Morsel source (heap block geometry): mutable scan-source borrow.
    let source: Arc<dyn runtime::MorselSource> = {
        let crate::procnode::PlanStateNode::SeqScan(ss) = planstate else {
            return Ok(false);
        };
        if !::nodeseqscan::seq_scan_is_heap(ss) {
            return Ok(false);
        }
        // FLOORGUARD gate 3: EMIT-FRACTION band, WHOLE-PLAN units only.
        // Admit only when the estimated TRUE emitted/scanned fraction is
        // inside the proven-win band (default 10% — GL-1's recipe: proven
        // region <=0.4%, GL-5 knee sweep prices the boundary;
        // PGRUST_RUNTIME_ROW_FUNNEL_EMIT_MAX_PCT overrides, 100 disables).
        // `floorguard_emit_band_admits` (unit-pinned above) owns the scale
        // semantics: a parallel FRAGMENT's plan_rows is the planner's
        // per-participant (divisor-divided) estimate — never comparable
        // against full reltuples — so parallel_aware refuses categorically;
        // the in-parallel-machinery gate above makes fragments unreachable
        // today, and this keeps the band itself fragment-aware for any
        // future engagement site handed a partial plan. Fail-closed on
        // missing stats (never-analyzed reltuples <= 0).
        let emit_max_pct = std::env::var("PGRUST_RUNTIME_ROW_FUNNEL_EMIT_MAX_PCT")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|p| *p > 0.0)
            .unwrap_or(10.0);
        let reltuples = ss
            .ss
            .ss_currentRelation
            .as_ref()
            .map(|rel| rel.rd_rel.reltuples as f64)
            .unwrap_or(0.0);
        // W0 (writes-stack): write destinations bypass the emit band — every
        // row is "emitted" into the writer by construction, so the band is
        // meaningless there; the divisor member's whole-plan-units law
        // (floorguard_emit_band_admits: fragment estimates refuse
        // categorically, stats-less fail-closed) governs read emits only.
        if !write_dest
            && !floorguard_emit_band_admits(
                plan.parallel_aware,
                plan.plan_rows,
                reltuples,
                emit_max_pct,
            )
        {
            return Ok(false);
        }
        let Some(map) = SeqScanSource::new(&mut *ss).granule_map(estate)? else {
            return Ok(false);
        };
        Arc::new(runtime::GranuleMapSource::new(Arc::new(map), false, false))
    };
    let total_granules = source.total_granules();
    // Tiny-input floor: a gang is only worth it above a small block count.
    if total_granules < (2 * dop as u64).max(2) {
        return Ok(false);
    }

    // Complete-drain only (the count-limited refusal above): no LIMIT bound
    // rides the engagement. `engage_passthrough` keeps its `limit` parameter
    // for the future resumable-funnel increment (and the runtime e2e tests).
    let limit = None;
    debug_assert_eq!(number_tuples, 0, "count-limited runs are refused above");

    // Wire slot: a Minimal slot carrying the result descriptor; the drain
    // stores each image into it and hands it to `dest.receive_slot`.
    let mut wire_slot = ::exectuples::make_tuple_table_slot(
        wire_mcx,
        ::types_slot::TupleSlotKind::MinimalTuple,
        Some(desc),
    );

    PT_ENGAGED.fetch_add(1, Ordering::SeqCst);
    if write_dest {
        super::write_funnel::note_engaged(total_granules);
    }
    // W2a inc-2 (worker-direct block-run writes; PGRUST_W2A_BLOCKRUN default
    // OFF — write_blockrun.rs): STRUCTURAL admission against the started-up
    // receiver. Armed => workers write their own private runs and the drain
    // below sees zero rows; refused => increments 0/1 exactly as before.
    let blockrun = if write_dest {
        super::write_blockrun::try_arm(dest)
    } else {
        None
    };
    // W2a inc-1 (pop-K batched write drain; PGRUST_W2A_DRAIN_BATCH default
    // OFF — write_funnel.rs): write-dest rows collect into a K-image batch
    // and flush through `flush_write_batch` (one receiver dispatch + one
    // slot clear + K image frees per batch, W1 buffer fed directly). Write
    // receivers never stop early, so the batched arm returns Ok(true)
    // unconditionally; the tail remainder flushes after the engage returns.
    // Inert when inc-2 owns the run (no rows reach the drain).
    let batch_writes = write_dest && super::write_funnel::w2a_drain_batch_enabled();
    let mut batch: Vec<MinImage> = if batch_writes {
        Vec::with_capacity(super::write_funnel::DRAIN_BATCH_CAP)
    } else {
        Vec::new()
    };
    let outcome = engage_passthrough(
        rt,
        pstmt,
        query_text,
        eflags,
        dop,
        source,
        super::row_emit::DEFAULT_RING_CAP,
        limit,
        // W0.1: write dests drain PURE — the leader is the writer (inc-2:
        // the leader is only the finalizer; workers write).
        write_dest,
        blockrun.clone(),
        |img: MinImage| -> PgResult<bool> {
            if batch_writes {
                batch.push(img);
                if batch.len() >= super::write_funnel::DRAIN_BATCH_CAP {
                    // Lifetime bridge as the per-row path below: the batch
                    // flush only borrows the slot during the call.
                    let slot: &mut ::types_slot::SlotData<'d> = unsafe {
                        &mut *(&mut wire_slot as *mut ::types_slot::SlotData<'mcx>)
                            .cast::<::types_slot::SlotData<'d>>()
                    };
                    let slot_mcx: ::mcx::Mcx<'d> =
                        unsafe { std::mem::transmute::<::mcx::Mcx<'mcx>, ::mcx::Mcx<'d>>(wire_mcx) };
                    super::write_funnel::flush_write_batch(&mut batch, dest, slot, slot_mcx)?;
                }
                return Ok(true);
            }
            // SAFETY: `wire_slot` is a Minimal slot; `img` owns the bytes and
            // outlives this store+receive (dropped at the end of the call).
            unsafe {
                ::exectuples::exec_store_minimal_tuple_ptr(&mut wire_slot, wire_mcx, img.as_mtup_ptr());
            }
            // Lifetime bridge at the dest seam (as in TuplestoreBatchSink): the
            // receiver only reads datums out during the call and retains no
            // borrow, so re-tagging the slot to the dest's lifetime is sound.
            let slot: &mut ::types_slot::SlotData<'d> = unsafe {
                &mut *(&mut wire_slot as *mut ::types_slot::SlotData<'mcx>)
                    .cast::<::types_slot::SlotData<'d>>()
            };
            let cont = dest.receive_slot(slot)?;
            // Clear the borrowed pointer before freeing the image (the original
            // 'mcx-typed slot; the re-tagged `slot` alias's borrow ends above).
            ::exectuples::exec_clear_tuple(&mut wire_slot, wire_mcx);
            drop(img);
            Ok(cont)
        },
    )?;

    // W2a inc-1 tail: engage returns only after the drain reached EOF, so
    // the remainder (< one batch) flushes here; an engage Err dropped the
    // batch with the closure (images freed, statement aborts — nothing
    // half-buffered survives).
    if batch_writes && !batch.is_empty() {
        let slot: &mut ::types_slot::SlotData<'d> = unsafe {
            &mut *(&mut wire_slot as *mut ::types_slot::SlotData<'mcx>)
                .cast::<::types_slot::SlotData<'d>>()
        };
        let slot_mcx: ::mcx::Mcx<'d> =
            unsafe { std::mem::transmute::<::mcx::Mcx<'mcx>, ::mcx::Mcx<'d>>(wire_mcx) };
        super::write_funnel::flush_write_batch(&mut batch, dest, slot, slot_mcx)?;
    }

    match outcome {
        PassthroughEngageOutcome::Completed(n) => {
            PT_COMPLETED.fetch_add(1, Ordering::SeqCst);
            // Block-run completion: the drain saw zero rows by design; the
            // statement's row count is the workers' sealed sum (read after
            // the gang join inside engage). The zero-drain invariant is a
            // release-reachable witness: any ring leakage double-counts and
            // fails the e2e ground-truth/parity legs.
            let n = match blockrun.as_ref() {
                Some(br) => {
                    debug_assert_eq!(n, 0, "block-run engagement drained rows");
                    let rows = br.rows.load(Ordering::SeqCst);
                    super::lane_trace(&format!(
                        "blockrun: completed rows={rows} writers={} pages={} drained={n}",
                        br.writers.load(Ordering::SeqCst),
                        br.allocator.claimed_pages(),
                    ));
                    for run in br.allocator.run_map() {
                        super::lane_trace(&format!(
                            "blockrun: run start={} len={}",
                            run.start, run.len
                        ));
                    }
                    n + rows
                }
                None => n,
            };
            if write_dest {
                super::write_funnel::note_completed(n);
            }
            estate.es_processed = n;
            Ok(true)
        }
        PassthroughEngageOutcome::Fallback => Ok(false),
    }
}

/// Unit pins for FLOORGUARD gate 3 (`floorguard_emit_band_admits`): the
/// knob's unit is the TRUE (whole-plan) emit fraction, and per-participant
/// fragment estimates never reach the ratio. The fragment cells reproduce
/// the measured mis-admission enabler exactly: divisors below are
/// `get_parallel_divisor(w)` = w + max(0, 1 - 0.3*w) (leader participation
/// damping) for w = 2, 3, 4 planned workers.
#[cfg(test)]
mod floorguard_emit_band_tests {
    use super::floorguard_emit_band_admits;

    const T: f64 = 60_000.0; // analyzed reltuples

    #[test]
    fn whole_plan_band_boundary() {
        // Just-under, at, and just-over the default 10% band — the admitted
        // set at the boundary is unchanged by the scale fix (<= admits).
        assert!(floorguard_emit_band_admits(false, 5_999.0, T, 10.0));
        assert!(floorguard_emit_band_admits(false, 6_000.0, T, 10.0));
        assert!(!floorguard_emit_band_admits(false, 6_001.0, T, 10.0));
        // A 33%-emit whole-plan estimate is far out of band.
        assert!(!floorguard_emit_band_admits(false, 19_800.0, T, 10.0));
    }

    #[test]
    fn knob_unit_is_true_emit_percent() {
        // Residual exposure 3: operators tune the knob in TRUE emit percent.
        // A 33% whole-plan shape admits at 50, refuses at 20 — the knob
        // brackets the true fraction, not a divided one.
        let rows_33pct = 0.33 * T;
        assert!(floorguard_emit_band_admits(false, rows_33pct, T, 50.0));
        assert!(!floorguard_emit_band_admits(false, rows_33pct, T, 20.0));
        // 100 disables the band entirely (whole-plan shapes only).
        assert!(floorguard_emit_band_admits(false, T, T, 100.0));
    }

    #[test]
    fn fragment_estimates_refuse_categorically() {
        // A 33%-TRUE-emit qual as a parallel fragment sees plan_rows divided
        // by the parallel divisor. The dop4 cell is the RED WITNESS: its
        // mis-scaled ratio (8.25%) sits INSIDE the 10% band — the pre-fix
        // expression `plan_rows / reltuples <= 0.10` admitted it.
        let whole = 0.33 * T; // 19_800 true emitted rows
        let dop4 = whole / 4.0; // divisor 4.0 -> apparent  8.25%
        let dop3 = whole / 3.1; // divisor 3.1 -> apparent 10.65%
        let dop2 = whole / 2.4; // divisor 2.4 -> apparent 13.75%
        assert!(dop4 / T <= 0.10, "red-witness cell must sit inside the band mis-scaled");
        assert!(!floorguard_emit_band_admits(true, dop4, T, 10.0));
        assert!(!floorguard_emit_band_admits(true, dop3, T, 10.0));
        assert!(!floorguard_emit_band_admits(true, dop2, T, 10.0));
        // Even a fragment whose TRUE fraction is under the band refuses:
        // the divisor is not recoverable from the Plan node, so no exact
        // true fraction exists to admit on — fail-closed to the serial loop.
        let under_whole = 0.05 * T;
        assert!(!floorguard_emit_band_admits(true, under_whole / 4.0, T, 10.0));
        // Categorical: the band-disabling knob setting does not re-admit
        // fragments.
        assert!(!floorguard_emit_band_admits(true, dop4, T, 100.0));
    }

    #[test]
    fn missing_stats_fail_closed() {
        // Never-analyzed (reltuples -1 / 0) and non-positive estimates refuse.
        assert!(!floorguard_emit_band_admits(false, 100.0, -1.0, 10.0));
        assert!(!floorguard_emit_band_admits(false, 100.0, 0.0, 10.0));
        assert!(!floorguard_emit_band_admits(false, 0.0, T, 10.0));
        // Band disabled skips the stats check (pre-fix behavior, preserved).
        assert!(floorguard_emit_band_admits(false, 100.0, -1.0, 100.0));
    }
}
