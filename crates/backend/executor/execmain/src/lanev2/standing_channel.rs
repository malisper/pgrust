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
//! workers). `Fallback` returns with the RG UNTOUCHED and the caller goes
//! straight to its serial arm (M2 inc-3 rung 4: the launched-bgworker
//! fallback is DELETED — the engagement ladder is pool → gang → serial,
//! the `PGRUST_RUNTIME_NOLAUNCH` posture made permanent per Michael's
//! rung-4 rulings, notes/m2-inc3-rung4.md §4a).
//!
//! Kill-switch layering: PGRUST_RUNTIME_POOLBIND=0 kills the standing
//! module wholesale (`parallel::standing::try_engage` refuses — scan arm
//! included); PGRUST_RUNTIME_POOLBIND_SINKS=0 retires ONLY the sink arms'
//! standing engagement (agg / agg_sorted / sort / hashjoin / distinct /
//! plaindistinct — `StandingArm::sinks_gate`). Post rung-4 both restore
//! the SERIAL arm, never a bgworker launch (the kill's restore target
//! shrank exactly like the D1 precedent: correct, slower, never an error).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::types_error::{PgError, PgResult, ERROR};

use super::lane_trace;

pub(super) enum StandingWait {
    /// The RG reached an outcome under standing participation.
    Done(runtime::RgOutcome),
    /// Standing path unavailable or refused with the RG UNTOUCHED —
    /// the caller drains the RG and falls back to its serial arm
    /// (rung 4: there is no launched path to take).
    Fallback,
}

/// Per-arm constants for the shared wait loop.
pub(super) struct StandingArm {
    /// Trace prefix ("runtime-agg", …) — the channel's lane_trace lines
    /// ("<label>: engaged standing dop=…", the tranche legs' grep surface).
    /// The dop printed is the POST-CLOSE claim census (true participation);
    /// it is emitted at completion, never mid-claim (GL-HJMB-1 esc. B).
    pub label: &'static str,
    /// Error text when claimed participants all detached with the RG
    /// incomplete and no recorded error (a worker died outside every catch
    /// layer).
    pub died: &'static str,
    /// inc-1 sink gate: PGRUST_RUNTIME_POOLBIND_SINKS=0 retires this
    /// arm's board engagement — post rung-4 that means the serial arm
    /// (false on the scan arm — its standing channel predates this
    /// increment and keeps its own gates).
    pub sinks_gate: bool,
}

/// The leader-side hooks the shared loop needs from an arm's payload.
pub(super) struct StandingLeader<'a> {
    /// The engagement's binder target (payload.pcxt_shared, already set).
    pub shared: &'a Arc<parallel::ParallelShared>,
    /// M2 inc-2: the POOL-DB engagement board the arm attached to its
    /// submission (`try_pool_channel` + `Runtime::submit_pinned_bound`).
    /// Some ⇒ standing_wait waits on the POOL channel first; its fallback
    /// (closed board, deadline, all-refused) then tries the gang channel
    /// with the RG untouched. None ⇒ inc-1 exactly.
    pub pool: Option<Arc<parallel::standing::StandingEngagement>>,
    /// The payload's board-entry slot: held across the wait so the arm's
    /// PRIVATE_SHUTDOWN hook can complete the standing join (abort + drain
    /// + await detach) on leader unwind paths that never reach this loop's
    /// own cleanup.
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
    /// trace line (shape witnesses — "nbatch=8
    /// (spill)", "full", "bound=10" — so the tranche legs' grep surfaces
    /// hold on both channels). Empty = no suffix (the scan arm's line
    /// stays byte-identical to inc-3).
    pub census: &'a str,
}

/// First-claim deadline: parked standing workers wake in microseconds, so
/// an unclaimed engagement after this long means the gang is dead/busy —
/// fall back to the serial arm (correctness never depends on this).
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

/// M2 inc-3 rung 4 (m2-inc3-scope.md §5 rung 4, DELETION LANDED): the
/// launched-bgworker fallback is gone from the pool-channel arms — when
/// the board channels decline (`StandingWait::Fallback`), the arm goes
/// straight to its serial arm. This helper is the arms' shared
/// fallthrough: the trace line keeps the rung-4 posture grep surface
/// ("launched fallback retired") and the `engage-<arm>-nolaunch-serial`
/// floor row keeps the cause attribution (one tick = one engagement the
/// deletion serialized — the R2-cliff watch; the arm-tail `serial` tick
/// still fires, outcome census unchanged). The floors' witness machinery
/// (stats rows + scripts/nolaunch-floors-witness.sh) retires at Phase-5
/// D5, not here. `PGRUST_RUNTIME_NOLAUNCH` is now INERT — accepted, read
/// by nothing; the posture it armed is the only behavior. Launched-ONLY
/// vehicles (funnel passthrough, nlindex) keep their own ceremonies and
/// migrate in their own increments; caller modes C1/C2 retired WITH this
/// path (Michael ruling D-1), the C3 seam stays board-shaped.
pub(super) fn launched_fallback_retired(arm: &StandingArm) {
    lane_trace(&format!(
        "{}: launched fallback retired — serial fallback",
        arm.label
    ));
    super::stats::tick_engaged(arm.label, super::stats::EngageChannel::NolaunchSerial);
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
    // GL-SLEASE-1: a leased session leader is about to PARK while pool/gang
    // workers execute its query — give the lease permit up for the wait
    // span (its own workers need it; a parked leader holding one steals a
    // worker-width from every engagement). Re-acquired on every exit path
    // by the guard's drop. No-op when unleased.
    let _lease_yield = crate::execmain::serial_lease_yield_for_engagement();
    // M2 inc-2: POOL-DB channel first — the per-RG board the arm attached
    // at submit (publication set the pool-visible active bit; parked pool
    // workers rode the publish wake). Same wait loop, same leader
    // contract; a fallback closes the board (late pool picks refuse
    // through the closed flag + skip cache) and the gang channel takes
    // over with the RG untouched.
    if let Some(pool) = &leader.pool {
        *leader.slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(pool));
        match wait_engaged(arm, &leader, pool, "pooldb", "standing", dop, granules, rg, waiter)?
        {
            StandingWait::Done(o) => return Ok(StandingWait::Done(o)),
            StandingWait::Fallback => {}
        }
    }
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
    wait_engaged(arm, &leader, &entry, "standing", "serial", dop, granules, rg, waiter)
}

/// The engaged wait loop (both channels): poll completion + interrupts +
/// participation counters against ONE board entry; every exit path closes
/// the entry and waits for claimed participants to detach. `channel`
/// parameterizes the trace surface ("standing" keeps the inc-3/inc-1 lines
/// byte-identical; "pooldb" is the M2 inc-2 grep surface); `next` names the
/// fallback channel in the refusal/deadline lines.
#[allow(clippy::too_many_arguments)]
fn wait_engaged(
    arm: &StandingArm,
    leader: &StandingLeader<'_>,
    entry: &Arc<parallel::standing::StandingEngagement>,
    channel: &str,
    next: &str,
    dop: i32,
    granules: u64,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
) -> PgResult<StandingWait> {
    let _ = dop;
    let entry = Arc::clone(entry);
    let take_slot = || {
        leader.slot.lock().unwrap_or_else(|p| p.into_inner()).take();
    };
    let t0 = pg_clock::MonoStamp::now();
    let census = if leader.census.is_empty() {
        String::new()
    } else {
        format!(" {}", leader.census)
    };
    // M2 inc-3 rung-2 fallback-floor accounting: one tick per engagement
    // OUTCOME on this channel (the two Done exits below). Fallback and
    // error exits tick nothing here — nolaunch-serial/serial tick at the
    // arm.
    let floor_channel = if channel == "pooldb" {
        super::stats::EngageChannel::PoolDb
    } else {
        super::stats::EngageChannel::Gang
    };
    loop {
        if let Some(o) = waiter.try_wait() {
            take_slot();
            parallel::gtrace("l.close.begin");
            parallel::standing::close_and_await(&entry);
            parallel::gtrace("l.close.end");
            // Engagement trace at COMPLETION only (GL-HJMB-1 escalation B):
            // the old mid-loop trace printed the claim count at first
            // observation — a race with worker wakeup that reported dop=1/2
            // on healthy full-dop engagements and poisoned every dop pin
            // grepped from it. Post-close claimed() is the true
            // participation census (the arms' "complete, partials=N" line
            // agrees with it by construction).
            lane_trace(&format!(
                "{}: engaged {channel} dop={} granules={granules}{census}",
                arm.label,
                entry.claimed()
            ));
            super::stats::tick_engaged(arm.label, floor_channel);
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
        let started = leader.started.load(Ordering::SeqCst);
        let refused = entry.refused() + leader.refused.load(Ordering::SeqCst);
        // Nobody will participate: every ticket-holder refused pre-bind or
        // at the bind/lane stage, before any granule was claimed.
        if started == 0 && refused >= entry.tickets() {
            lane_trace(&format!(
                "{}: {channel} refused ({refused} refusals) — {next} fallback",
                arm.label
            ));
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Ok(StandingWait::Fallback);
        }
        // COUNTER READ ORDER (m2-inc3 rung-1 finding, the hashjoin pooldb
        // died-needle misfire): both detach-comparing branches below MUST
        // read `detached` BEFORE re-reading `claimed`. Every detach is
        // preceded by its claim, so detached(t1) <= claims-as-of(t1) <=
        // claimed(t2) for t1 < t2 — a claim landing between the two reads
        // can only RAISE claimed, never manufacture detached >= claimed.
        // The old order (claimed captured at loop top, detached fresh)
        // tore under the pool channel's pre-bind refusal churn (helpers
        // claiming and detaching on "rg gone" — payload.rg is set only
        // AFTER submit publishes): the stale-low claimed met the churn-
        // inflated detached while a LATER claimer was healthily mid-drive
        // (started>0), and the needle killed a live engagement. A real
        // all-detached death is stable, so reading it one iteration later
        // loses no liveness.
        let detached = entry.detached();
        let claimed_now = entry.claimed();
        // Nothing driving and nothing pending within the deadline: gang
        // dead/busy (claimed==0) OR a smaller-than-tickets gang whose every
        // claimant exited pre-drive without reaching the refusal counters'
        // tickets floor above (started==0, detached>=claimed>0). Either
        // way no granule was consumed; the next channel (gang after pool;
        // serial after gang) takes over. A
        // straggler that claims right as we close simply drives the same
        // RG (morsel claims are atomic; its partial combines like any
        // participant's) — close_and_await bounds on its drive.
        if started == 0
            && detached >= claimed_now
            && std::time::Duration::from_nanos(t0.elapsed_ns()) > standing_claim_deadline()
        {
            lane_trace(&format!(
                "{}: {channel} claim deadline — {next} fallback",
                arm.label
            ));
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Ok(StandingWait::Fallback);
        }
        // Participants all detached yet the RG is incomplete and no error
        // was recorded: a worker died outside every catch layer (detach is
        // Drop-guaranteed, so this is reachable only through that needle).
        // detached-before-claimed read order per the block comment above.
        //
        // POOL-QOS yield-kind split: voluntary serve-yields are detaches
        // too, but HEALTHY ones (the board's last-active guard keeps >=1
        // participant, and yielded capacity refills through the concurrent
        // ticket cap) — subtract them. Read order law: `yielded` is read
        // AFTER `detached` (every yield bumps both under one lock, yielded
        // first), so `terminal` can only UNDER-count — the needle waits
        // instead of killing a live engagement; a real death stabilizes
        // terminal > 0 by the next iteration.
        let yielded = entry.yielded();
        let terminal = detached.saturating_sub(yielded);
        if claimed_now > 0 && started > 0 && detached >= claimed_now && terminal > 0 {
            if let Some(o) = waiter.try_wait() {
                take_slot();
                parallel::standing::close_and_await(&entry);
                super::stats::tick_engaged(arm.label, floor_channel);
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
        // PRE-CLAIM phase (GL-ZSTALL-1): before anything has claimed, the
        // only wakes the leader can rely on are pre-claim refusals — and a
        // worker set that never visits the board (all parked blocked on a
        // database mismatch, or busy elsewhere) sends none. Bound the park
        // by the claim deadline so the fallback branch above fires ON the
        // deadline instead of at the next MQ-recheck quantum (~1 s). Once
        // any claim exists, claim-holder progress (DetachGuard's
        // count-then-wake) drives the loop and the plain quantum is right.
        let preclaim_bound_ms = (started == 0 && claimed_now == 0).then(|| {
            let deadline = standing_claim_deadline();
            let elapsed = std::time::Duration::from_nanos(t0.elapsed_ns());
            i64::try_from(deadline.saturating_sub(elapsed).as_millis())
                .unwrap_or(i64::MAX)
                .max(1)
        });
        // F1 PgResult propagation (train-12 composition seam): a raised
        // cancel disposition (statement_timeout / pg_cancel_backend)
        // surfaces from the latch quantum — the F1 defect layer 2b
        // branch's standing-loop mirror, same protocol as the CFI
        // branch above (abort THEN drain THEN close-and-await).
        let quantum = match preclaim_bound_ms {
            Some(ms) => parallel::wait_parallel_finish_quantum_bounded(ms),
            None => parallel::wait_parallel_finish_quantum(),
        };
        if let Err(e) = quantum {
            rg.abort();
            (leader.drain)(rg);
            take_slot();
            parallel::standing::close_and_await(&entry);
            return Err(e);
        }
    }
}

/// GL-SEATLIFT-1 arming switch: PGRUST_RUNTIME_POOL_SEATLIFT, armed iff
/// exactly `1`/`on` (DEFAULT OFF — the measured-increment posture; the
/// scatter-accept knob's spelling convention).
fn seatlift_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_RUNTIME_POOL_SEATLIFT").ok().as_deref().map(str::trim),
            Some("1") | Some("on")
        )
    })
}

/// GL-SEATLIFT-1 band ceiling (exclusive): PGRUST_RUNTIME_POOL_SEATLIFT_MAXDOP,
/// default 12 — the GL-RADIX-2 v2p1 diagnostic's derived boundary (the +1
/// seat wins d4/d8, HURTS at d16; the ladder probes the boundary through
/// this override).
fn seatlift_maxdop() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_POOL_SEATLIFT_MAXDOP")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(12)
    })
}

/// GL-SEATLIFT-1 ticket count (pure; the unit-pinned law): sink admissions
/// in the band `2 <= dop < maxdop` publish dop+1 tickets when armed;
/// everything else — scan-gate channels, dop==1, at/over the ceiling,
/// disarmed — publishes dop exactly (knob-OFF byte-identity is structural).
/// dop==1 is EXCLUDED by design: the sink arms' single-Local pass-through
/// contract keys off dop==1 ⇒ exactly one partial.
fn seatlift_tickets(dop: usize, sinks_gate: bool, armed: bool, maxdop: usize) -> usize {
    if armed && sinks_gate && (2..maxdop).contains(&dop) {
        dop + 1
    } else {
        dop
    }
}

/// M2 inc-2: build the POOL-DB channel for one engagement — the per-RG
/// board plus the runtime bound descriptor. The descriptor MUST ride the
/// submission (`Runtime::submit_pinned_bound` — the pool-visible active
/// bit keys off it at publication); the returned entry goes into
/// `StandingLeader::pool` so standing_wait waits the pool phase first.
/// None = channel unavailable (PGRUST_RUNTIME_POOLDB off, POOLBIND off,
/// sinks gate, no driver, no pool identity wiring, lock-group failure) —
/// submit plain-pinned and the layering is inc-1 exactly. Call AFTER
/// set_standing_driver (the serve dispatches through it).
///
/// GL-SEATLIFT-1 (dop-banded +1 pool seat on sink admissions, DEFAULT
/// OFF): the pool channel PARKS its leader, so an engagement fields one
/// fewer accept thread than the hybrid's participating-leader plan — the
/// measured d4/d8 residual on grouped sinks (~6% of the α≈1 gap is the
/// seat; GL-RADIX-2 v2p1 diagnostic). Armed sink admissions in the band
/// publish ONE extra ticket: the extra pool worker claims it and drives an
/// ordinary external lane (sink Locals are sized nthreads +
/// MAX_EXTERNAL_LANES, and the combine consumes claimed() partials by
/// construction — an extra participant is inside every existing
/// invariant). Composes with rung-3 sticky: the extra seat is a normal
/// serve, so retention/eviction see nothing new.
pub(super) fn try_pool_channel(
    shared: &Arc<parallel::ParallelShared>,
    dop: i32,
    sinks_gate: bool,
) -> Option<(Arc<parallel::standing::StandingEngagement>, runtime::BoundDescriptor)> {
    if sinks_gate && !standing_sinks_enabled() {
        return None;
    }
    let dop = dop.max(0) as usize;
    let tickets = seatlift_tickets(dop, sinks_gate, seatlift_enabled(), seatlift_maxdop());
    let entry = parallel::standing::try_engage_pool(shared, tickets)?;
    if tickets != dop {
        // The ladder's arming witness (grep surface): one line per lifted
        // admission, emitted only after the channel actually engaged.
        lane_trace(&format!("lanev2: pool seatlift +1 seat (dop={dop} tickets={tickets})"));
    }
    let payload: Arc<dyn std::any::Any + Send + Sync> = Arc::clone(&entry) as _;
    Some((entry, runtime::BoundDescriptor {
        serve: pooldb_serve,
        payload,
        // POOL-QOS: the engagement's requested width feeds the interactive
        // demand ledger (unmet width draws demoted serve-yields).
        width: dop.max(0) as u32,
    }))
}

/// POOL-QOS (GL-POOLDB-1 mitigation): the arms' driver-body drive. On a
/// POOL serve the nested drive may YIELD at a morsel boundary toward
/// waiting interactive width (`Runtime::drive_pinned_yieldable`; the grant
/// callback is the board's last-active-guarded exit, and the pending
/// detach settles in serve_ticket after the driver's teardown). Returns
/// None on a yield — the arm's teardown runs exactly as on completion:
/// the participant simply stops participating; its partials live in the
/// leader-arena per-worker slots and seal cross-worker per the elastic
/// protocol (the loom-modeled accepter≠sealer invariant). Gang serves
/// keep the plain drive byte-identically (pooldb0 is the
/// letter's control arm).
pub(super) fn drive_pool_serve(
    rt: &runtime::Runtime,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    lane: &mut Option<runtime::ExternalLane>,
) -> Option<runtime::RgOutcome> {
    if parallel::standing::serving_on_pool() {
        let end = rt.drive_pinned_yieldable(local, rg, &mut || {
            parallel::standing::try_grant_yield_current()
        });
        if end.is_none() {
            // Yielded: PARK this participant's lane until the RG completes
            // — its per-lane sink slot may hold mid-accept state that a
            // rejoining serve must not inherit (the FREEZE set seals it
            // cross-worker later, the elastic accepter≠sealer invariant).
            if let Some(l) = lane.take() {
                rt.park_external_lane(l, rg);
            }
        }
        end
    } else {
        Some(rt.drive_pinned(local, rg))
    }
}

#[cfg(test)]
mod seatlift_tests {
    use super::seatlift_tickets;

    /// GL-SEATLIFT-1 band law: +1 iff armed ∧ sink ∧ 2 <= dop < maxdop.
    #[test]
    fn seatlift_band_law() {
        // Armed, sink, default ceiling 12: the band.
        assert_eq!(seatlift_tickets(1, true, true, 12), 1, "dop1 pass-through excluded");
        assert_eq!(seatlift_tickets(2, true, true, 12), 3);
        assert_eq!(seatlift_tickets(4, true, true, 12), 5, "d4 win cell lifts");
        assert_eq!(seatlift_tickets(8, true, true, 12), 9, "d8 win cell lifts");
        assert_eq!(seatlift_tickets(11, true, true, 12), 12, "band edge inclusive");
        assert_eq!(seatlift_tickets(12, true, true, 12), 12, "ceiling exclusive");
        assert_eq!(seatlift_tickets(16, true, true, 12), 16, "d16 no-harm: never lifted");
        // Scan-gate channels never lift.
        assert_eq!(seatlift_tickets(4, false, true, 12), 4);
        // Disarmed (the default): byte-identity — dop exactly, every cell.
        for d in 0..20 {
            assert_eq!(seatlift_tickets(d, true, false, 12), d);
            assert_eq!(seatlift_tickets(d, false, false, 12), d);
        }
        // Ceiling override probes the boundary.
        assert_eq!(seatlift_tickets(12, true, true, 16), 13);
        assert_eq!(seatlift_tickets(0, true, true, 12), 0, "dop0 refused upstream, untouched");
    }
}

/// The bound descriptor's serve: parallel::standing::pool_serve mapped
/// onto the runtime's verdict (the parallel crate cannot name runtime
/// types — the dependency points the other way; this arm-side adapter
/// closes the loop, the StandingDriver fn-pointer precedent).
fn pooldb_serve(payload: &Arc<dyn std::any::Any + Send + Sync>) -> runtime::BoundServe {
    let served = match parallel::standing::pool_serve(payload) {
        parallel::standing::PoolServe::Served => runtime::BoundServe::Served,
        parallel::standing::PoolServe::Refused => runtime::BoundServe::Refused,
        parallel::standing::PoolServe::Closed => runtime::BoundServe::Closed,
    };
    // M2 inc-3 rung 3: refresh the runtime's session-residue hint — a
    // Served engagement may have PARKED a sticky session retention on this
    // thread (pool_sticky posture); the hint feeds the scheduler's
    // unbound-work eviction gate and the affinity tiebreak. Refreshed on
    // every verdict (an eager sink serve EVICTS a prior retention, so the
    // hint self-corrects to false). A FATAL unwind skips this note — the
    // thread is dying and its TLS dies with it.
    runtime::note_session_residue(parallel::sticky_parked());
    served
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
