//! Caller-as-worker TYPED SKELETON (single-executor migration Phase 0.1,
//! WS-B): the session thread as worker #0 — it leases an external pin-board
//! lane and drives its OWN resource group through the ordinary pinned-step
//! machinery, pumping a caller-supplied DUTY (CFI + ProcessParallelMessages
//! + latch quantum — the pieces of the arms' bespoke wait loops the runtime
//! crate cannot depend on, injected as a callback per the ratified contract
//! shape 1d) between steps and at Idle transitions.
//!
//! THIS REVERSES THE RATIFIED §2.5 LAW (rg.rs: leaders submit-and-park, NO
//! leader execution) — deliberately, and only as a compiling skeleton:
//! under the cores-permit cap a participating leader displaces a pool
//! worker, which is exactly why §2.5 rejected it; the admission ledger's
//! core budget (crate::ledger) is what makes it sound, because the caller's
//! grant is counted like any worker's. NO production caller exists at this
//! increment: no arm, no execmain path, nothing constructs a CallerWorker
//! outside tests. Production wiring is per-arm surgery on six bespoke wait
//! loops + standing_wait + vacuum's round ceremony, each with its own
//! error/EA/liveness coupling — the TODO ledger in notes/se-ws-b-ledger.md
//! itemizes it.
//!
//! Duty cadence honesty: the duty runs BETWEEN scheduler steps (a step is
//! one task = one or more claims) and before parking — claim-boundary-exact
//! duty pumping needs a hook inside run_task's claim loop and is deferred
//! with the production wiring. Cancel latency is therefore one task
//! (~t_max), already far under the arms' 100ms park quantum.

use std::sync::Arc;

use types_error::PgError;

use crate::sched::Step;
use crate::{ExternalLane, RgHandle, RgOutcome, Runtime, WorkerLocal};

/// Caller-as-worker (migration doc §0.1). SKELETON at this increment:
/// types + the duty seam; NO production caller.
pub struct CallerWorker {
    lane: ExternalLane,
    local: WorkerLocal,
}

impl CallerWorker {
    /// Lease an external lane for the session thread. None = lanes
    /// exhausted (the caller falls back to submit-and-park, fail-closed —
    /// the same refusal discipline as every external participant).
    pub fn enter(rt: &Arc<Runtime>) -> Option<CallerWorker> {
        let lane = rt.acquire_external_lane()?;
        let local = lane.local();
        Some(CallerWorker { lane, local })
    }

    /// Pin-board lane ordinal this caller occupies (EA partials indexing:
    /// lane-indexed partial vecs need the leader's own index).
    pub fn lane_ordinal(&self) -> usize {
        self.lane.ordinal()
    }

    /// Drive the caller's own RG on the session thread, running `duty` at
    /// every step boundary and Idle transition. Err from duty aborts the
    /// RG and DRAINS it before returning (the drain_rg discipline: the
    /// caller keeps stepping until the abort completes through the
    /// ordinary protocol, so no pin, grant, or finalization obligation is
    /// stranded), then surfaces the duty's error.
    ///
    /// Permit discipline: the caller holds an execution permit across each
    /// step and releases it around parks — it displaces a pool worker
    /// exactly as §2.5 predicted, which is why this stays skeleton-only
    /// until the ledger counts it (module doc).
    pub fn drive_with_duty(
        &mut self,
        rt: &Runtime,
        rg: &RgHandle,
        duty: &mut dyn FnMut() -> Result<(), Box<PgError>>,
    ) -> Result<RgOutcome, Box<PgError>> {
        let mut failed: Option<Box<PgError>> = None;
        let outcome = loop {
            if let Some(outcome) = rg.try_outcome() {
                rt.sched.stat_flush_all(&mut self.local);
                self.local.wfin_flush_all();
                break outcome;
            }
            if failed.is_none() {
                if let Err(e) = duty() {
                    // Duty failure ON the session thread (no payload.fail
                    // indirection): abort, then fall through to the drain.
                    rg.abort();
                    failed = Some(e);
                    continue;
                }
            }
            let epoch = rt.park_epoch();
            rt.execution_permits().acquire();
            let step = rt.sched.worker_step_pinned(&mut self.local, &rg.rg);
            rt.execution_permits().release();
            match step {
                Step::Ran => {}
                Step::Retry => std::thread::yield_now(),
                Step::Idle => {
                    if rg.try_outcome().is_some() {
                        continue;
                    }
                    // Epoch captured before the step: a publish/completion/
                    // ledger wake between the failed pick and this park is
                    // never lost.
                    rt.park(epoch);
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        };
        match failed {
            Some(e) => Err(e),
            None => Ok(outcome),
        }
    }
}
