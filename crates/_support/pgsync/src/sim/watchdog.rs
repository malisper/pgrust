//! THE watchdog — permit-holder-blocked-outside-interception (contract
//! §3.2; design §4). ONE detector, exactly; do not add detectors.
//!
//! An UNREGISTERED monitor thread (outside the model, plain std — its
//! nondeterminism is confined here and never reaches the SCHEDOP stream)
//! watches the handoff heartbeat (seq counter + wall stamp). No handoff for
//! N wall-seconds (default 30 s, `PGRUST_SIM_WATCHDOG_S`; corpora finish in
//! wall-milliseconds) while the permit is HELD and live slots exist ⇒ dump
//! {holder vpid/name, its last shim event with its `#[track_caller]`
//! Location — the dump NAMES the blocked site symbolically, all slot
//! states, SCHEDOP tail} and abort. That dump is the empirical-worklist
//! feed: the named site is the next site to shim (contract §2.4).
//!
//! The one wedge shape the model admits is the holder blocked where the
//! scheduler cannot see — a census gap, FFI, an allocator lock. A
//! deterministic deadlock is NOT a watchdog case (the scheduler's pick
//! fails immediately and reports; sched.rs).
//!
//! This file deliberately owns ALL wall-clock usage of the sim scheduler
//! (std::time::Instant) so the determinism-lint surface stays one row wide.

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use super::sched::{Scheduler, WatchdogSink};

/// Wall milliseconds since the process's watchdog epoch. Watchdog-only food
/// (heartbeat stamps + stall arithmetic); never logged into SCHEDOP lines.
pub(crate) fn wall_ms() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Start the monitor for `sched`. Holds only a Weak — the monitor dies when
/// the scheduler is dropped (unit instances) or at process end (the global).
pub(crate) fn start(sched: &Arc<Scheduler>) {
    let weak = Arc::downgrade(sched);
    // Deliberately outside the spawn door: the monitor must never be a slot
    // (it has to observe a wedged schedule from outside the model).
    let _ = std::thread::Builder::new()
        .name("pgsync-sim-watchdog".to_string())
        .spawn(move || run(weak));
}

fn run(weak: Weak<Scheduler>) {
    let mut last_seq = u64::MAX;
    let mut last_change_ms = wall_ms();
    loop {
        let Some(sched) = weak.upgrade() else { return };
        let poll_ms = sched.cfg.watchdog_poll_ms.max(5);
        let timeout_ms = sched.cfg.watchdog_timeout_ms.max(1);
        // The monitor's own wait is a REAL sleep by design: it measures wall
        // stalls of the virtualized world and so must live outside it.
        std::thread::sleep(std::time::Duration::from_millis(poll_ms));
        let seq = sched.heartbeat_seq.load(Ordering::Relaxed);
        let now = wall_ms();
        if seq != last_seq {
            last_seq = seq;
            last_change_ms = now;
            continue;
        }
        let stalled = now.saturating_sub(last_change_ms);
        if stalled < timeout_ms {
            continue;
        }
        // Heartbeat flat past the threshold: establish the wedge (permit
        // held + a Running holder) via try_lock — never block behind the
        // model's own state lock.
        let Some(report) = sched.try_dump_wedge(stalled) else {
            continue;
        };
        match &sched.cfg.watchdog_sink {
            WatchdogSink::AbortProcess => {
                eprintln!("{report}");
                std::process::abort();
            }
            WatchdogSink::Capture(buf) => {
                *buf.lock().unwrap_or_else(|e| e.into_inner()) = Some(report);
                // Captured (red-unit sink): re-arm rather than abort so the
                // test can observe and unwedge.
                last_change_ms = now;
            }
        }
    }
}
