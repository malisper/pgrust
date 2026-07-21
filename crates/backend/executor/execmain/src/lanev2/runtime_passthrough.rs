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

use runtime::RowFunnel;

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
        pstmt: *const PlannedStmt<'static>,
        query_text: String,
        eflags: i32,
        funnel: Arc<RowFunnel<MinImage>>,
    ) -> Arc<PassthroughShared> {
        Arc::new(PassthroughShared {
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
            self.started.fetch_add(1, Ordering::SeqCst);
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
