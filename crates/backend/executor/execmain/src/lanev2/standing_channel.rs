//! M2 inc-1 — the STANDING engagement channel shared by the runtime arms
//! (m2-pool-binding inc-3's leader loop, hoisted out of runtime_scan when
//! the sink arms joined the channel; scratchpad/night/
//! m2-pool-binding-scope.md §3 inc-1, notes/m2-pool-binding.md).
//!
//! One helper owns the leader-side submit-and-park loop against the
//! standing gang board (`parallel::standing`): publish the engagement,
//! then poll completion + interrupts + participation counters. Every exit
//! path closes the board entry and waits for claimed participants to
//! detach (the arena-lifetime join — detach is Drop-guaranteed on the
//! workers). `Fallback` returns with the RG UNTOUCHED so the caller's
//! launched-gang path takes over (fail-closed layering: standing →
//! launched → serial).
//!
//! Kill-switch layering: PGRUST_RUNTIME_POOLBIND=0 kills the standing
//! module wholesale (`parallel::standing::try_engage` refuses — scan arm
//! included); PGRUST_RUNTIME_POOLBIND_SINKS=0 retires ONLY the sink arms'
//! standing engagement (agg / agg_sorted / sort / hashjoin / distinct /
//! plaindistinct — `StandingArm::sinks_gate`), restoring their
//! launched-gang ceremony exactly. The scan arm ships ungated here (its
//! inc-3 semantics unchanged).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::types_error::{PgError, PgResult, ERROR};

use super::lane_trace;

pub(super) enum StandingWait {
    /// The RG reached an outcome under standing participation.
    Done(runtime::RgOutcome),
    /// Standing path unavailable or refused with the RG UNTOUCHED —
    /// take the launched path.
    Fallback,
}

/// Per-arm constants for the shared wait loop.
pub(super) struct StandingArm {
    /// Trace prefix ("runtime-agg", …) — the channel's lane_trace lines
    /// ("<label>: engaged standing dop=…", the tranche legs' grep surface).
    pub label: &'static str,
    /// Error text when claimed participants all detached with the RG
    /// incomplete and no recorded error (a worker died outside every catch
    /// layer).
    pub died: &'static str,
    /// inc-1 sink gate: PGRUST_RUNTIME_POOLBIND_SINKS=0 restores this
    /// arm's launched-gang engagement (false on the scan arm — its
    /// standing channel predates this increment and keeps its own gates).
    pub sinks_gate: bool,
}

/// The leader-side hooks the shared loop needs from an arm's payload.
pub(super) struct StandingLeader<'a> {
    /// The engagement's binder target (payload.pcxt_shared, already set).
    pub shared: &'a Arc<parallel::ParallelShared>,
    /// The payload's board-entry slot: held across the wait so the arm's
    /// PRIVATE_SHUTDOWN hook can complete the standing join (abort + drain
    /// + await detach) on leader unwind paths that never reach this loop's
    /// own cleanup — the launched path gets the same guarantee from
    /// DestroyParallelContext's worker-exit wait.
    pub slot: &'a Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    /// Participants that bound and entered the drive.
    pub started: &'a AtomicUsize,
    /// Payload-side refusals (bind/lane refusals inside the arm's
    /// helper_drive; the board's own pre-driver refusals are added).
    pub refused: &'a AtomicUsize,
    /// First recorded worker-phase error (the arm's payload/sink slot).
    pub take_error: &'a dyn Fn() -> Option<Box<PgError>>,
    /// Abort + BOUNDED drain of the pinned RG (the arm's drain_rg): the
    /// leader's protocol cleanup, which completes the RG and releases
    /// workers parked in their drives.
    pub drain: &'a dyn Fn(&runtime::RgHandle) -> bool,
    /// Arm-specific engagement census appended to the "engaged standing"
    /// trace line (the launched line's shape witnesses — "nbatch=8
    /// (spill)", "full", "bound=10" — so the tranche legs' grep surfaces
    /// hold on both channels). Empty = no suffix (the scan arm's line
    /// stays byte-identical to inc-3).
    pub census: &'a str,
}

/// First-claim deadline: parked standing workers wake in microseconds, so
/// an unclaimed engagement after this long means the gang is dead/busy —
/// fall back to the launched path (correctness never depends on this).
fn standing_claim_deadline() -> std::time::Duration {
    // DST P2 (contract §1.3, census erratum (a)): this deadline is
    // BEHAVIORAL (changes execution path) — its clock is pg_clock, below.
    static MS: OnceLock<u64> = OnceLock::new();
    std::time::Duration::from_millis(crate::once_val(&MS, || {
        std::env::var("PGRUST_RUNTIME_GANG_CLAIM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100)
    }))
}

/// PGRUST_RUNTIME_POOLBIND_SINKS=0 — the inc-1 kill switch (sink arms
/// only; see the module doc's layering).
fn standing_sinks_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        std::env::var("PGRUST_RUNTIME_POOLBIND_SINKS").map_or(true, |v| v.trim() != "0")
    })
}

/// The standing channel's submit-and-park (the scan arm's standing_wait,
/// verbatim, parameterized per arm): publish the engagement, then poll
/// completion + interrupts + participation counters. Every exit path
/// closes the board entry and waits for claimed participants to detach.
///
/// The caller must have submitted the pinned RG already (a straggler that
/// claims right as we close simply drives the same RG) and must be inside
/// its Enter/ExitParallelMode bracket with pcxt_shared set.
pub(super) fn standing_wait(
    arm: &StandingArm,
    leader: StandingLeader<'_>,
    dop: i32,
    granules: u64,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
) -> PgResult<StandingWait> {
    if arm.sinks_gate && !standing_sinks_enabled() {
        return Ok(StandingWait::Fallback);
    }
    parallel::gtrace("l.publish.begin");
    let engaged = parallel::standing::try_engage(leader.shared, dop.max(0) as usize);
    parallel::gtrace("l.publish.end");
    let Some(entry) = engaged else {
        return Ok(StandingWait::Fallback);
    };
    // Leader-unwind containment: the arm's PRIVATE_SHUTDOWN hook completes
    // the standing join if this frame never reaches one of its own cleanup
    // paths (each of which takes the slot back first).
    *leader.slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&entry));
    let take_slot = || {
        leader.slot.lock().unwrap_or_else(|p| p.into_inner()).take();
    };
    let t0 = pg_clock::MonoStamp::now();
    let census = if leader.census.is_empty() {
        String::new()
    } else {
        format!(" {}", leader.census)
    };
    let mut traced = false;
    loop {
        if let Some(o) = waiter.try_wait() {
            take_slot();
            parallel::gtrace("l.close.begin");
            parallel::standing::close_and_await(&entry);
            parallel::gtrace("l.close.end");
            if !traced {
                lane_trace(&format!(
                    "{}: engaged standing dop={} granules={granules}{census}",
                    arm.label,
                    entry.claimed()
                ));
            }
            return Ok(StandingWait::Done(o));
        }
        if let Err(e) = ::postgres_seams::check_for_interrupts::call() {
            // Order matters: abort THEN drain (the leader's protocol
            // cleanup is what completes the RG and releases workers parked
            // in their drives) THEN await detach.
            rg.abort();
            (leader.drain)(rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(e);
        }
        let claimed = entry.claimed();
        if !traced && claimed > 0 {
            lane_trace(&format!(
                "{}: engaged standing dop={claimed} granules={granules}{census}",
                arm.label
            ));
            traced = true;
        }
        let started = leader.started.load(Ordering::SeqCst);
        let refused = entry.refused() + leader.refused.load(Ordering::SeqCst);
        // Nobody will participate: every ticket-holder refused pre-bind or
        // at the bind/lane stage, before any granule was claimed.
        if started == 0 && refused >= entry.tickets() {
            lane_trace(&format!(
                "{}: standing refused ({refused} refusals) — launched fallback",
                arm.label
            ));
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Ok(StandingWait::Fallback);
        }
        // Nothing driving and nothing pending within the deadline: gang
        // dead/busy (claimed==0) OR a smaller-than-tickets gang whose every
        // claimant exited pre-drive without reaching the refusal counters'
        // tickets floor above (started==0, detached>=claimed>0). Either
        // way no granule was consumed; the launched path takes over. A
        // straggler that claims right as we close simply drives the same
        // RG (morsel claims are atomic; its partial combines like any
        // participant's) — close_and_await bounds on its drive.
        if started == 0
            && entry.detached() >= claimed
            && std::time::Duration::from_nanos(t0.elapsed_ns()) > standing_claim_deadline()
        {
            lane_trace(&format!(
                "{}: standing claim deadline — launched fallback",
                arm.label
            ));
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Ok(StandingWait::Fallback);
        }
        // Participants all detached yet the RG is incomplete and no error
        // was recorded: a worker died outside every catch layer (detach is
        // Drop-guaranteed, so this is reachable only through that needle).
        if claimed > 0 && started > 0 && entry.detached() >= claimed {
            if let Some(o) = waiter.try_wait() {
                take_slot();
                parallel::standing::close_and_await(&entry);
                return Ok(StandingWait::Done(o));
            }
            if let Some(e) = (leader.take_error)() {
                rg.abort();
                (leader.drain)(rg);
                take_slot();
                parallel::standing::close_and_await(&entry);
                return Err(e);
            }
            rg.abort();
            (leader.drain)(rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(Box::new(PgError::new(ERROR, arm.died)));
        }
        // F1 PgResult propagation (train-12 composition seam): a raised
        // cancel disposition (statement_timeout / pg_cancel_backend)
        // surfaces from the latch quantum — the standing-loop mirror of
        // the launched path's F1 defect layer 2b branch and the CFI
        // branch above (abort THEN drain THEN close-and-await).
        if let Err(e) = parallel::wait_parallel_finish_quantum() {
            rg.abort();
            (leader.drain)(rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(e);
        }
    }
}

/// PRIVATE_SHUTDOWN completion of the standing join (every arm): a leader
/// unwind (error/panic between publish and standing_wait's own cleanup)
/// reaches the arm's shutdown hook through DestroyParallelContext with
/// claimed workers possibly still driving. Complete the RG (drain releases
/// drives parked on the aborted generation) and hold the frame until every
/// participant detached — the leader arena must outlive their SendConst
/// refs. UNCONDITIONAL on the rg upgrade: a dead weak handle still leaves
/// the board entry occupied (every future try_engage would refuse and
/// parked workers would wedge against an entry nobody removes).
pub(super) fn shutdown_standing_join(
    slot: &Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    rg: Option<&runtime::RgHandle>,
    drain: &dyn Fn(&runtime::RgHandle) -> bool,
) {
    let entry = slot.lock().unwrap_or_else(|p| p.into_inner()).take();
    if let Some(entry) = entry {
        if let Some(rg) = rg {
            if rg.try_outcome().is_none() {
                // The arm's drain_rg aborts (idempotent) and drives
                // protocol cleanup.
                drain(rg);
            }
        }
        parallel::standing::close_and_await(&entry);
    }
}
