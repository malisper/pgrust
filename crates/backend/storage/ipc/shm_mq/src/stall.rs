// MQ stall self-report (notes/parallel-repeat-wedge-2026-07-12.md): a
// once-observed production wedge left one parallel worker permanently deaf to
// latch wakes — the GatherMerge leader blocked in MessageQueueReceive on the
// deaf worker's queue and the other workers blocked in MessageQueueSend as
// backpressure — and never reproduced under a harness. So the blocking MQ
// wait sites self-diagnose the next natural occurrence: a wait that exceeds
// the threshold without a latch wake elogs ONE LOG line carrying the queue
// counters and both endpoints' latch + wakeup-registry state (the decoded
// evidence says a lost pid->wakeup-pipe mapping is the broken link), then
// KEEPS WAITING unchanged — no self-heal, the report must not destroy the
// wedge evidence.
//
// Cost when not stalled: nothing on any path that doesn't block; a blocked
// wait arms its deadline with one clock read and sleeps with a timeout
// instead of forever. PGRUST_MQ_STALL_REPORT_MS overrides the 60 s default
// (<= 0 disables; diagnostics knob, read once).
//
// RECHECK CADENCE (fix-pardistinct-contention, 2026-07-13): the original
// detector kept waiting untouched after its one report, which made every
// LOST latch wake cost exactly one threshold (the wait un-wedged only when
// the next WaitLatch entry saw a stuck is_set) and made a SECOND lost wake
// on the same blocked operation a PERMANENT hang (post-report the sleep
// reverted to infinite). Production hit this repeatedly (a dict-int-key grouped top-n's
// default plan: 173 s for a 0.3 s query = wedge timeouts, S3 job
// flatprof-1783927152; the same 60 s-cadence signature on two
// pods in notes/batchemit-lane.md). The blocked sleeps now time out every
// PGRUST_MQ_RECHECK_MS (default 1000, <= 0 restores the old infinite
// behavior) and RETURN to the caller as a spurious wake — every wait site
// is a recheck loop (ResetLatch + re-poll progress state), so a lost wake
// now costs at most one recheck period instead of 60 s / forever. The
// one-shot LOG report still fires when total blocked time crosses the
// report threshold, so the wedge evidence is preserved.

use std::sync::OnceLock;

use elog::ereport;
use init_small::globals::{MyProcPid, MyProcNumber};
use latch::{ResetLatch, WaitLatch};
use types_error::{ErrorLocation, PgResult, LOG};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::ShmMq;

const DEFAULT_THRESHOLD_MS: i64 = 60_000;
const DEFAULT_RECHECK_MS: i64 = 1_000;

fn threshold_ms() -> i64 {
    static THRESHOLD: OnceLock<i64> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("PGRUST_MQ_STALL_REPORT_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(DEFAULT_THRESHOLD_MS)
    })
}

/// The process-wide blocked-wait recheck cadence (PGRUST_MQ_RECHECK_MS,
/// default 1000 ms, <= 0 disables) — pub for latch-wait sites whose flag
/// mix (WL_POSTMASTER_DEATH) cannot reuse wait_latch_reporting.
pub fn recheck_ms() -> i64 {
    static RECHECK: OnceLock<i64> = OnceLock::new();
    *RECHECK.get_or_init(|| {
        std::env::var("PGRUST_MQ_RECHECK_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(DEFAULT_RECHECK_MS)
    })
}

fn now_ms() -> i64 {
    // DST P2 (contract §1.3): stall detection on the one monotonic authority.
    pg_clock::mono_ms()
}

/// One logical blocked operation's stall clock. The owner keeps it across
/// wake -> recheck -> re-block loop iterations (a latch wake without progress
/// does not restart the clock) and resets it on progress; the report fires at
/// most once per stall.
pub struct StallDetector {
    threshold_ms: i64,
    recheck_ms: i64,
    start_ms: Option<i64>,
    reported: bool,
}

impl StallDetector {
    pub fn new() -> Self {
        Self::with_thresholds(threshold_ms(), recheck_ms())
    }

    pub fn with_threshold(threshold_ms: i64) -> Self {
        // Test/diagnostic constructor: report threshold only, no recheck
        // cadence (the original report-then-wait-on shape).
        Self::with_thresholds(threshold_ms, 0)
    }

    pub fn with_thresholds(threshold_ms: i64, recheck_ms: i64) -> Self {
        StallDetector { threshold_ms, recheck_ms, start_ms: None, reported: false }
    }

    /// `new()` with the recheck cadence additionally capped at `cap_ms`
    /// (floored at 1 ms): wait sites whose ONLY guaranteed progress source
    /// is their own deadline — a wake may legitimately never come — bound
    /// every sleep to that deadline instead of the process-wide cadence.
    /// With the cadence knob disabled (<= 0) the cap alone bounds the
    /// sleep: such a site cannot afford an infinite park.
    pub fn new_capped(cap_ms: i64) -> Self {
        let cap = cap_ms.max(1);
        let recheck = recheck_ms();
        let capped = if recheck > 0 { recheck.min(cap) } else { cap };
        Self::with_thresholds(threshold_ms(), capped)
    }

    fn active(&self) -> bool {
        self.threshold_ms > 0 && !self.reported
    }

    /// True = a timed-out sleep should return to the caller as a spurious
    /// wake (the caller rechecks its progress condition), bounding the cost
    /// of a lost latch wake to one recheck period.
    fn recheck_enabled(&self) -> bool {
        self.recheck_ms > 0
    }

    /// Timeout (ms) for the next latch sleep; None = sleep forever (both
    /// mechanisms disabled). The report deadline arms at the first block;
    /// the recheck cadence caps every sleep.
    fn next_timeout_ms(&mut self, now: i64) -> Option<i64> {
        let deadline = if self.active() {
            let start = *self.start_ms.get_or_insert(now);
            Some((start + self.threshold_ms - now).max(1))
        } else {
            None
        };
        let recheck = self.recheck_enabled().then_some(self.recheck_ms);
        match (deadline, recheck) {
            (Some(d), Some(r)) => Some(d.min(r)),
            (Some(d), None) => Some(d),
            (None, r) => r,
        }
    }

    /// The sleep timed out: Some(waited_ms) exactly once, when the total
    /// blocked time crosses the threshold — the caller reports then.
    fn note_timeout(&mut self, now: i64) -> Option<i64> {
        let start = self.start_ms?;
        if !self.active() {
            return None;
        }
        let waited = now - start;
        if waited < self.threshold_ms {
            return None;
        }
        self.reported = true;
        Some(waited)
    }

    /// Progress: disarm and allow a future stall of the same operation to
    /// report again.
    pub fn reset(&mut self) {
        self.start_ms = None;
        self.reported = false;
    }
}

impl Default for StallDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Core reporting sleep: WaitLatch until the latch fires OR the detector's
/// recheck cadence elapses. A timed-out sleep past the report threshold
/// emits the caller's report (once per stall); with the recheck cadence
/// enabled every timeout RETURNS to the caller as a spurious wake — all
/// wait sites are recheck loops, so a lost latch wake costs at most one
/// recheck period. With both knobs disabled this is exactly
/// WaitLatch(latch, forever).
pub fn wait_latch_reporting(
    latch: LatchHandle,
    wait_event_info: u32,
    detector: &mut StallDetector,
    report: &mut dyn FnMut(i64),
) -> PgResult<()> {
    loop {
        let rc = match detector.next_timeout_ms(now_ms()) {
            Some(timeout) => WaitLatch(
                Some(latch),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                timeout,
                wait_event_info,
            )?,
            None => WaitLatch(
                Some(latch),
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                wait_event_info,
            )?,
        };
        if rc & WL_LATCH_SET != 0 {
            return Ok(());
        }
        // Timed out. Do NOT ResetLatch here: a set that raced the timeout
        // must survive for the recheck. Report once per stall.
        if let Some(waited_ms) = detector.note_timeout(now_ms()) {
            report(waited_ms);
        }
        if detector.recheck_enabled() {
            // Spurious-wake return: the caller re-polls its progress
            // condition, which un-wedges every lost-wake class (including
            // the ones where is_set was never stuck on).
            return Ok(());
        }
    }
}

/// `wait_on_my_latch` with the stall self-report armed (same ResetLatch +
/// CHECK_FOR_INTERRUPTS tail).
pub fn wait_on_my_latch_reporting(
    my_latch: Option<LatchHandle>,
    wait_event_info: u32,
    detector: &mut StallDetector,
    report: &mut dyn FnMut(i64),
) -> PgResult<()> {
    let latch = my_latch.expect("shm_mq blocking operation requires MyLatch");
    wait_latch_reporting(latch, wait_event_info, detector, report)?;
    ResetLatch(latch);
    postgres_seams::check_for_interrupts::call()?;
    Ok(())
}

// One endpoint's wake-path state: PGPROC latch bits plus whether the
// published waker handle can still route a SetLatch to its owner (the
// waker word in the latch replaced the pid-keyed wakeup registry).
// "registry=MISSING"/"registry=STALE(...)" on a live blocked endpoint is
// the deaf-worker signature.
fn describe_endpoint(procno: Option<types_core::ProcNumber>) -> String {
    let Some(procno) = procno else { return "procno=none".into() };
    if !lmgr_proc_seams::proc_latch::is_installed() {
        return format!("procno={procno} (latch state unavailable)");
    }
    let latch = latch::latch_ref(LatchHandle::proc(procno));
    let pid = latch.owner_pid.load(std::sync::atomic::Ordering::Relaxed);
    let is_set = latch.is_set.load(std::sync::atomic::Ordering::Relaxed);
    let sleeping = latch.maybe_sleeping.load(std::sync::atomic::Ordering::Relaxed);
    let registry = format!(
        "registry={}",
        waiter::describe_word(latch.waker.load(std::sync::atomic::Ordering::Acquire))
    );
    format!(
        "procno={procno} pid={pid} latch_set={is_set} latch_sleeping={sleeping} {registry}"
    )
}

/// Queue + both endpoints, one line's worth.
pub fn describe_queue(mq: &ShmMq) -> String {
    format!(
        "queue={:p} bytes_written={} bytes_read={} detached={} receiver=[{}] sender=[{}]",
        mq as *const ShmMq,
        mq.bytes_written(),
        mq.bytes_read(),
        mq.detached(),
        describe_endpoint(mq.get_receiver()),
        describe_endpoint(mq.get_sender()),
    )
}

/// The single LOG line for a stalled shm_mq send/receive wait. `role` names
/// the wait site ("receive", "send", "send-ledger-full"); `pending` is the
/// handle-side un-consumed/un-flushed byte (or slot) count.
pub fn report_queue_stall(mq: &ShmMq, role: &str, pending: usize, waited_ms: i64) {
    let msg = format!(
        "shm_mq stall self-report: role={role} waited_ms={waited_ms} my_pid={} my_procno={} pending={pending} {}",
        MyProcPid(),
        MyProcNumber(),
        describe_queue(mq),
    );
    // LOG never unwinds; the wait continues untouched either way.
    let _ = ereport(LOG)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "shm_mq_stall_report"));
}

/// LOG channel for layered wait sites' stall reports (the Gather leader wait
/// lives in execmain, which has no elog dependency).
pub fn log_stall_report(msg: String) {
    let _ = ereport(LOG)
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "mq_stall_report"));
}

/// Default "the caller left the park loop" gap: four recheck periods.
const DEFAULT_PARK_GAP_MS: i64 = 4_000;

/// The monotonic clock the stall machinery runs on, for owners that keep a
/// [`ParkStallClock`] across calls.
pub fn park_now_ms() -> i64 {
    now_ms()
}

/// A stall clock for a park loop whose wait quantum is a SEPARATE CALL —
/// `parallel::wait_parallel_finish_quantum` and friends, where the loop body
/// lives in one crate and the latch sleep in another, so the caller cannot
/// hold a `StallDetector` across iterations the way `gather_readnext` does.
///
/// GL-SYNCWEDGE-1: the quantum used to build a fresh `StallDetector` on every
/// call. `start_ms` therefore re-armed once per 1000 ms recheck, the 60 s
/// report deadline could never mature, and a leader that parked for 12m53s in
/// production emitted NOT ONE log line. This type keeps the detector across
/// quanta and only re-arms when the owner has been away from the loop longer
/// than `gap_ms` — i.e. when it did real work rather than re-polling a
/// progress condition that never changed.
pub struct ParkStallClock {
    detector: StallDetector,
    last_entry_ms: Option<i64>,
    gap_ms: i64,
}

impl ParkStallClock {
    pub fn new() -> Self {
        Self::with_params(StallDetector::new(), DEFAULT_PARK_GAP_MS)
    }

    pub fn with_params(detector: StallDetector, gap_ms: i64) -> Self {
        ParkStallClock { detector, last_entry_ms: None, gap_ms }
    }

    /// Call at the top of every park quantum. Re-arms the episode when the
    /// gap since the previous quantum says the owner left the loop.
    pub fn enter(&mut self, now_ms: i64) {
        let fresh = match self.last_entry_ms {
            None => true,
            Some(prev) => now_ms.saturating_sub(prev) > self.gap_ms,
        };
        if fresh {
            self.detector.reset();
        }
        self.last_entry_ms = Some(now_ms);
    }

    /// The detector to hand to [`wait_latch_reporting`].
    pub fn detector(&mut self) -> &mut StallDetector {
        &mut self.detector
    }

    /// Test/diagnostic view of the wrapped one-shot report predicate.
    pub fn note_timeout(&mut self, now_ms: i64) -> Option<i64> {
        self.detector.note_timeout(now_ms)
    }
}

impl Default for ParkStallClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod stall_tests {
    use super::{ParkStallClock, StallDetector};

    // The GL-SYNCWEDGE-1 defect, stated executably: a park loop that builds a
    // FRESH detector for every 1000 ms quantum re-arms `start_ms` each lap, so
    // the 60 s report deadline never matures. 12m53s of parking, zero reports.
    // This is exactly what parallel::wait_on_my_latch did.
    #[test]
    fn fresh_detector_per_quantum_can_never_report() {
        let mut reports = 0;
        let mut now = 0i64;
        for _ in 0..900 {
            // 900 laps x 1000 ms = 15 minutes parked.
            let mut d = StallDetector::with_thresholds(60_000, 1_000);
            let timeout = d.next_timeout_ms(now).expect("a bounded sleep");
            now += timeout;
            if d.note_timeout(now).is_some() {
                reports += 1;
            }
        }
        assert_eq!(reports, 0, "the defect: a per-quantum detector is structurally silent");
        assert_eq!(now, 900_000);
    }

    // The fix: one clock kept across quanta reports exactly once, at the
    // threshold, and keeps the recheck cadence afterwards.
    #[test]
    fn park_clock_kept_across_quanta_reports_exactly_once() {
        let mut clock =
            ParkStallClock::with_params(StallDetector::with_thresholds(60_000, 1_000), 4_000);
        let mut reports = Vec::new();
        let mut now = 0i64;
        for _ in 0..900 {
            clock.enter(now);
            let timeout = clock.detector().next_timeout_ms(now).expect("a bounded sleep");
            assert!(timeout <= 1_000, "the recheck cadence must still cap every sleep");
            now += timeout;
            if let Some(waited) = clock.note_timeout(now) {
                reports.push(waited);
            }
        }
        assert_eq!(reports.len(), 1, "exactly one report per stall episode");
        assert_eq!(reports[0], 60_000);
        assert_eq!(now, 900_000);
    }

    // Leaving the loop and coming back later is a NEW episode: the previous
    // episode's elapsed time must not carry over (otherwise a long-lived
    // session's accumulated parking would fire one bogus report and then go
    // permanently silent).
    #[test]
    fn park_clock_rearms_after_the_owner_leaves_the_loop() {
        let mut clock =
            ParkStallClock::with_params(StallDetector::with_thresholds(60_000, 1_000), 4_000);
        // Episode 1: parks 59 s, completes, no report.
        let mut now = 0i64;
        for _ in 0..59 {
            clock.enter(now);
            now += clock.detector().next_timeout_ms(now).expect("bounded");
            assert_eq!(clock.note_timeout(now), None);
        }
        // The owner is away for 10 minutes (a different statement).
        now += 600_000;
        // Episode 2 must start its own 60 s clock, not report on lap one.
        clock.enter(now);
        now += clock.detector().next_timeout_ms(now).expect("bounded");
        assert_eq!(clock.note_timeout(now), None);
        // ...and it does report 60 s into episode 2.
        let episode2_start = now - 1_000;
        while now - episode2_start < 60_000 {
            clock.enter(now);
            now += clock.detector().next_timeout_ms(now).expect("bounded");
        }
        assert_eq!(clock.note_timeout(now), Some(now - episode2_start));
    }

    // A quantum gap SHORTER than the away-threshold is the same episode: the
    // caller's progress poll between sleeps must not reset the clock.
    #[test]
    fn park_clock_treats_a_poll_gap_as_the_same_episode() {
        let mut clock =
            ParkStallClock::with_params(StallDetector::with_thresholds(10_000, 1_000), 4_000);
        let mut now = 0i64;
        let mut reports = 0;
        for _ in 0..20 {
            clock.enter(now);
            now += clock.detector().next_timeout_ms(now).expect("bounded");
            now += 3_000; // slow poll work, still under the 4 s away-threshold
            if clock.note_timeout(now).is_some() {
                reports += 1;
            }
        }
        assert_eq!(reports, 1);
    }

    #[test]
    fn arms_at_first_block_and_holds_deadline() {
        let mut d = StallDetector::with_threshold(60_000);
        assert_eq!(d.next_timeout_ms(1_000), Some(60_000));
        // Re-blocking later without progress keeps the original deadline.
        assert_eq!(d.next_timeout_ms(31_000), Some(30_000));
        assert_eq!(d.next_timeout_ms(60_999), Some(1));
        // Past the deadline the sleep still gets a positive timeout.
        assert_eq!(d.next_timeout_ms(61_500), Some(1));
    }

    #[test]
    fn reports_once_per_stall() {
        let mut d = StallDetector::with_threshold(60_000);
        assert_eq!(d.next_timeout_ms(0), Some(60_000));
        // Early timeout (clock skew/rounding): not yet a report.
        assert_eq!(d.note_timeout(59_000), None);
        // Threshold crossed: exactly one report, with the waited time.
        assert_eq!(d.note_timeout(60_010), Some(60_010));
        // Stall persists: no second report, and the wait goes untimed.
        assert_eq!(d.note_timeout(180_000), None);
        assert_eq!(d.next_timeout_ms(180_000), None);
    }

    #[test]
    fn timeout_without_arming_never_reports() {
        let mut d = StallDetector::with_threshold(60_000);
        assert_eq!(d.note_timeout(100_000), None);
    }

    #[test]
    fn reset_rearms_for_a_new_stall() {
        let mut d = StallDetector::with_threshold(1_000);
        assert_eq!(d.next_timeout_ms(0), Some(1_000));
        assert_eq!(d.note_timeout(1_000), Some(1_000));
        d.reset();
        assert_eq!(d.next_timeout_ms(5_000), Some(1_000));
        assert_eq!(d.note_timeout(6_000), Some(1_000));
    }

    #[test]
    fn disabled_threshold_stays_inert() {
        let mut d = StallDetector::with_threshold(0);
        assert_eq!(d.next_timeout_ms(0), None);
        assert_eq!(d.note_timeout(100_000), None);
        let mut d = StallDetector::with_threshold(-5);
        assert_eq!(d.next_timeout_ms(0), None);
    }

    #[test]
    fn recheck_cadence_caps_every_sleep() {
        let mut d = StallDetector::with_thresholds(60_000, 1_000);
        assert!(d.recheck_enabled());
        // Far from the report deadline: the recheck cadence wins.
        assert_eq!(d.next_timeout_ms(0), Some(1_000));
        assert_eq!(d.next_timeout_ms(30_000), Some(1_000));
        // Near the deadline: the report deadline wins.
        assert_eq!(d.next_timeout_ms(59_500), Some(500));
        // Report fires on total blocked time, once.
        assert_eq!(d.note_timeout(60_010), Some(60_010));
        assert_eq!(d.note_timeout(120_000), None);
        // Post-report the cadence continues (no infinite sleep).
        assert_eq!(d.next_timeout_ms(120_000), Some(1_000));
    }

    #[test]
    fn recheck_without_threshold_never_reports() {
        let mut d = StallDetector::with_thresholds(0, 1_000);
        assert_eq!(d.next_timeout_ms(0), Some(1_000));
        assert_eq!(d.note_timeout(100_000), None);
    }

    /// new_capped: the cap wins under the cadence, the cadence wins under
    /// the cap, and a degenerate cap floors at 1 ms — a deadline-owning
    /// wait site can never sleep past its deadline OR forever. (Env-default
    /// cadence assumed: the test binary runs without the MQ knobs set.)
    #[test]
    fn capped_bounds_every_sleep_to_the_deadline() {
        let mut d = StallDetector::new_capped(100);
        assert_eq!(d.next_timeout_ms(0), Some(100));
        let mut d = StallDetector::new_capped(5_000_000);
        assert_eq!(d.next_timeout_ms(0), Some(1_000));
        let mut d = StallDetector::new_capped(0);
        assert_eq!(d.next_timeout_ms(0), Some(1));
        let mut d = StallDetector::new_capped(-7);
        assert_eq!(d.next_timeout_ms(0), Some(1));
    }
}
