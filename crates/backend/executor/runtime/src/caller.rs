//! Caller-as-worker TYPED SKELETON (single-executor migration Phase 0.1,
//! WS-B): the session thread as worker #0 — it leases an external pin-board
//! lane and drives its OWN resource group through the ordinary pinned-step
//! machinery, pumping a caller-supplied DUTY (CFI + ProcessParallelMessages
//! plus the latch quantum — the pieces of the arms' bespoke wait loops the
//! runtime crate cannot depend on, injected as a callback per the ratified
//! contract shape 1d) between steps and at Idle transitions.
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

    /// Read-only view of this caller's scheduling bookkeeping (C2 inc-4,
    /// WS-S wave 3: WFIN observability parity — execmain's emit_wfin
    /// reads the drive accumulators off the participant's WorkerLocal
    /// after the drive, exactly as the helper drive frames do).
    pub fn worker_local(&self) -> &WorkerLocal {
        &self.local
    }

    /// C1 (wave 2, WS-O): [`CallerWorker::drive_with_duty`] plus a LIGHT
    /// claim-boundary duty installed for the drive's extent (sched.rs
    /// CALLER_DUTY): `claim_duty` runs at EVERY claim boundary of tasks
    /// this caller executes — return false to end the current task at the
    /// boundary (the Budget path) and fall back to the step loop, where
    /// the full error-carrying `duty` runs. The light duty must be CHEAP
    /// and non-erroring (flag checks, bounded liveness probes — the gang
    /// all-stopped detection lives here per the contract adjudication);
    /// error semantics stay with `duty`.
    pub fn drive_with_duties(
        &mut self,
        rt: &Runtime,
        rg: &RgHandle,
        duty: &mut dyn FnMut() -> Result<(), Box<PgError>>,
        claim_duty: &mut dyn FnMut() -> bool,
    ) -> Result<RgOutcome, Box<PgError>> {
        // SAFETY (CallerDutyCtx contract): the pointer is consumed only by
        // run_task on THIS thread while the guard lives, and the guard
        // (unwind-safe RAII) drops before this frame returns — the &mut
        // borrow never escapes the drive. The transmute erases ONLY the
        // reference lifetime (identical fat-pointer layout); the guard's
        // scope is what makes the erasure sound.
        let duty_ptr: *mut (dyn FnMut() -> bool + 'static) = unsafe {
            core::mem::transmute::<
                *mut (dyn FnMut() -> bool + '_),
                *mut (dyn FnMut() -> bool + 'static),
            >(claim_duty as *mut _)
        };
        let _duty_ctx = crate::sched::CallerDutyCtx::set(duty_ptr);
        self.drive_with_duty(rt, rg, duty)
    }

    /// C2 (wave 3, WS-S inc-3): [`CallerWorker::drive_with_duties`] with a
    /// caller-supplied IDLE PARK replacing the runtime eventcount park.
    /// While the caller would otherwise block on the ParkLot (C1's honest
    /// limit: parked-at-Idle duties do not pump), `idle_park` runs a
    /// BOUNDED caller-owned wait (production: the latch/WaitEventSet
    /// quantum the submit-and-park loop uses, cancel-disposition-carrying)
    /// and control returns to the step loop, where the full `duty` pumps.
    /// Cancel latency in the Idle window moves from next-publish-wake to
    /// the caller's own wait primitive; runtime-internal wakes are bounded
    /// by the park's quantum plus an epoch pre-check (zero when a publish
    /// already landed between the failed step and the park).
    ///
    /// Error discipline: an `idle_park` error is a duty error — abort,
    /// drain through the ordinary protocol, surface. During the drain a
    /// repeat park error is IGNORED (the first error already aborted; the
    /// bounded park still paces the drain instead of busy-spinning).
    ///
    /// Caller contract (upheld by the production wiring): `idle_park`
    /// must be BOUNDED — an unbounded wait here re-creates the C1 park
    /// without the eventcount's lost-wakeup guarantee.
    pub fn drive_with_duties_parked(
        &mut self,
        rt: &Runtime,
        rg: &RgHandle,
        duty: &mut dyn FnMut() -> Result<(), Box<PgError>>,
        claim_duty: &mut dyn FnMut() -> bool,
        idle_park: &mut dyn FnMut() -> Result<(), Box<PgError>>,
    ) -> Result<RgOutcome, Box<PgError>> {
        // SAFETY: identical to drive_with_duties — the erased-lifetime
        // pointer is consumed only by run_task on THIS thread while the
        // RAII guard lives, and the guard drops before this frame returns.
        let duty_ptr: *mut (dyn FnMut() -> bool + 'static) = unsafe {
            core::mem::transmute::<
                *mut (dyn FnMut() -> bool + '_),
                *mut (dyn FnMut() -> bool + 'static),
            >(claim_duty as *mut _)
        };
        let _duty_ctx = crate::sched::CallerDutyCtx::set(duty_ptr);
        self.drive_loop(rt, rg, duty, Some(idle_park))
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
        self.drive_loop(rt, rg, duty, None)
    }

    /// The one caller step loop (C1 == `idle_park: None`, byte-for-byte
    /// the WS-O drive; C2 == `Some`, the inc-3 latch-bridged park). Kept
    /// private so the two public faces stay the knob arms.
    fn drive_loop(
        &mut self,
        rt: &Runtime,
        rg: &RgHandle,
        duty: &mut dyn FnMut() -> Result<(), Box<PgError>>,
        mut idle_park: Option<&mut dyn FnMut() -> Result<(), Box<PgError>>>,
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
                // Loom-aware yield (WS-S wave-3: the caller loop is now
                // loom-modeled — loom must see the Retry spin as a
                // scheduling point or the model livelocks; std arm is the
                // identical production yield).
                #[cfg(not(loom))]
                Step::Retry => std::thread::yield_now(),
                #[cfg(loom)]
                Step::Retry => loom::thread::yield_now(),
                Step::Idle => {
                    if rg.try_outcome().is_some() {
                        continue;
                    }
                    match idle_park.as_deref_mut() {
                        // C1: epoch captured before the step — a publish/
                        // completion/ledger wake between the failed pick
                        // and this park is never lost.
                        None => rt.park(epoch),
                        // C2 inc-3: bounded caller-owned park. The epoch
                        // pre-check skips the wait when a wake already
                        // landed; otherwise the park's own bound (latch
                        // quantum) caps the lost-wakeup window that the
                        // eventcount would have closed exactly.
                        Some(park) => {
                            if rt.park_epoch() == epoch {
                                let parked = park();
                                if failed.is_none() {
                                    if let Err(e) = parked {
                                        // Park failure == duty failure
                                        // (cancel raised at the latch):
                                        // abort, then drain below.
                                        rg.abort();
                                        failed = Some(e);
                                    }
                                }
                            }
                        }
                    }
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        };
        // WS-O inc-2 pin-board assert (debug-only accessor): a drive that
        // returned settled the caller's pin.
        debug_assert!(
            rt.debug_pin_settled(&self.local),
            "caller pin unsettled after its drive"
        );
        match failed {
            Some(e) => Err(e),
            None => Ok(outcome),
        }
    }
}
