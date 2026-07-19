//! PERMIT-S1 WS-DEMO corpora (pgrust extension, `--cfg pgrust_sim` builds
//! ONLY; sim-harness domain, zero product surface): the two no-boot demo
//! corpora the permit-scheduler step-1 contract's e2e drives
//! (`scripts/permit-sched-e2e.sh`):
//!
//! * **P2 plant** (`PGRUST_PERMIT_PLANT=<iters>`): a DELIBERATE lost-update
//!   read-modify-write race — two threads each do `iters` increments of a
//!   shared counter with the read and the write under SEPARATE lock
//!   acquisitions and a yield in the window. The final value names the
//!   interleaving: `2*iters` = no loss, less = lost updates. Under the
//!   permit scheduler each lock/unlock/yield is a scheduling touch, so the
//!   outcome is a deterministic function of PGRUST_SIM_SEED and the e2e's
//!   N-seed sweep provably finds BOTH outcomes and replays each from its
//!   seed. Emits exactly one grep-stable line:
//!   `PLANT threads=2 iters=N final=V expected=E lost=L`
//!
//! * **P4 watchdog red** (`PGRUST_PERMIT_RED=1`): an INTENTIONALLY
//!   unshimmed blocking site — thread A takes a RAW `std::sync::Mutex`
//!   (never a pgsync type; that rawness IS the fixture) and parks while
//!   holding it; thread B then blocks on that raw mutex. Under the permit
//!   scheduler B blocks OUTSIDE interception while holding the permit —
//!   the exact (and only) wedge shape THE watchdog exists to catch; the
//!   harness asserts the dump names this file. Red-battery discipline: a
//!   fallback timer FAILS the fixture (exit 3, `RED-FALLBACK` line) if no
//!   watchdog kills the process first.
//!
//! WIRED AT COMPOSE (dst/permit-s1): lock ops are `pgsync::…` (scheduler
//! touches), corpus threads register — main via the spawn door
//! (`register_self`), children via `pgsync::thread` (child-side synthetic
//! registration behind the parent-side spawn fence) — so the schedule is a
//! seeded function of `PGRUST_SIM_SEED` under `PGRUST_SIM_SCHED=1`. The
//! RED's raw `std::sync::Mutex` stays raw FOREVER — it is the fixture (its
//! rawsync allowlist row names it). With the scheduler off everything here
//! degrades to plain std (scaffold mode, exactly the pre-compose behavior).

// PERMIT-S1 wiring point (WS-SYNC): FLIPPED at compose. The RED keeps its
// raw std::sync::Mutex (fully-qualified below — the fixture).
use pgsync::{Condvar, Mutex};
use std::sync::Arc;

/// Env-dispatched corpus entry: called by PostgresSimNetMain FIRST; returns
/// only if no demo-corpus env is set (the normal session path proceeds).
pub(crate) fn maybe_run_demo_corpus_from_env() {
    if let Ok(v) = std::env::var("PGRUST_PERMIT_PLANT") {
        let iters: u64 = v.parse().expect("PGRUST_PERMIT_PLANT=<iters>");
        run_plant(iters);
    }
    if std::env::var("PGRUST_PERMIT_RED").is_ok_and(|v| v == "1") {
        run_watchdog_red();
    }
}

// ---------------------------------------------------------------------------
// P2: the planted lost-update RMW race.
// ---------------------------------------------------------------------------

fn run_plant(iters: u64) -> ! {
    // In-model driver: register main at the spawn door (no-op when the
    // scheduler is off) so the children's registrations are program-ordered
    // (spawn fence) and the joins below are hooked parks.
    let _door = pgsync::sim::spawn_door::register_self("plant-main");
    let ctr = Arc::new(Mutex::new(0u64));
    let mut handles = Vec::new();
    for t in 0..2u32 {
        let ctr = Arc::clone(&ctr);
        handles.push(
            pgsync::thread::Builder::new()
                .name(format!("plant-{t}"))
                .spawn(move || {
                    for _ in 0..iters {
                        // THE RACE: read under one acquisition …
                        let seen = *ctr.lock().unwrap_or_else(|e| e.into_inner());
                        // … window (under the permit scheduler the seeded
                        // preemption dial at the surrounding lock/unlock
                        // touches opens it; a plain yield otherwise) …
                        std::thread::yield_now();
                        // … write under a SECOND acquisition. An
                        // interleaved peer increment in the window is
                        // overwritten: the classic lost update.
                        *ctr.lock().unwrap_or_else(|e| e.into_inner()) = seen + 1;
                    }
                })
                .expect("spawn plant thread"),
        );
    }
    for h in handles {
        h.join().expect("plant thread panicked");
    }
    let final_v = *ctr.lock().unwrap_or_else(|e| e.into_inner());
    let expected = 2 * iters;
    println!(
        "PLANT threads=2 iters={iters} final={final_v} expected={expected} lost={}",
        expected - final_v
    );
    std::process::exit(0)
}

// ---------------------------------------------------------------------------
// P5 (PERMIT-S2 F2): the wpool 2-worker demo.
// ---------------------------------------------------------------------------

/// Drive the REAL launch_backend wpool through its (now-doored) spawn site.
/// Boot half must have completed on THIS thread — the standby prelude
/// restores the postmaster GUC snapshot. Sequence: `maintain()` spawns
/// `max_parallel_workers` pooled standbys (parent-side door registration,
/// child-side gate); one virtual-time park lets every standby run its
/// prelude to the recv() park (virtual time advances only when nothing is
/// runnable — the deterministic rendezvous); `flush()` retires them
/// (hooked channel-drop wakes); the drain loop rides TimedParks until every
/// standby exit dropped its population charge. One grep-stable line:
/// `WPOOL target=N spawned_population=N drained=0`. The e2e (P5) asserts
/// both standby registrations + grants in the SCHEDOP stream and x3
/// same-seed byte-identity. With the scheduler off this degrades to real
/// sleeps over OS scheduling (scaffold mode) — same line, no SCHEDOP.
pub(crate) fn run_wpool_demo() -> ! {
    // NO register_self here: this runs on the boot/driver thread, which
    // PostgresSimNetMain already registered ("simnet-main"). A second
    // registration would rebind TLS to a fresh slot and park waiting for
    // its grant while the OLD slot still holds the permit — a
    // self-inflicted watchdog wedge (found live at PERMIT-S2 bring-up).
    let target = init_small::globals::max_parallel_workers();
    // Seam-routed (tcop -> launch_backend directly is a package cycle
    // through autovacuum -> commands_vacuum -> explain).
    postmaster_seams::wpool_maintain::call();
    let spawned = postmaster_seams::wpool_population::call();
    // Rendezvous: when this park returns, every standby has either parked
    // in recv() or exited.
    pgsync::thread::sleep(std::time::Duration::from_millis(10));
    postmaster_seams::wpool_flush::call();
    let mut polls = 0u32;
    while postmaster_seams::wpool_population::call() > 0 {
        pgsync::thread::sleep(std::time::Duration::from_millis(1));
        polls += 1;
        assert!(polls < 60_000, "wpool demo: standbys never drained");
    }
    println!("WPOOL target={target} spawned_population={spawned} drained=0");
    std::process::exit(0)
}

// ---------------------------------------------------------------------------
// P6 (PERMIT-S5): the rtpool 2-worker demo.
// ---------------------------------------------------------------------------

/// Drive the REAL launch_backend rtpool (the PGRUST_RUNTIME=1 runtime worker
/// pool) through its (now-doored) spawn sites — the runtime-mode analogue of
/// [`run_wpool_demo`]. Requires `PGRUST_RUNTIME=1`; the harness pins
/// `PGRUST_RUNTIME_WORKERS`/`PGRUST_RUNTIME_STANDBYS` so the pool size (and
/// so the SCHEDOP stream) is box-independent. With `PGRUST_RUNTIME_BGJOBS=1`
/// the bgjobs dispatcher rides the same start and registers at ITS door.
///
/// Sequence: `rtpool_start` spawns the pool (parent-side door registration
/// per worker, child-side gate) → one virtual-time park is the rendezvous
/// (every worker has run its prelude to the runtime eventcount park) →
/// `rtpool_stop` (stop flag + wake_all, hooked wakes) → the drain loop rides
/// TimedParks until every worker exit dropped its population charge. One
/// grep-stable line: `RTPOOL start=N spawned_population=N drained=0`. The
/// e2e (P6) asserts the worker/dispatcher door registrations + per-vpid
/// grants in SCHEDOP and x3 same-seed byte-identity. The dispatcher (when
/// armed) is process-lifetime and stays parked at exit — deterministic under
/// the same seed. With the scheduler off this degrades to real sleeps over
/// OS scheduling (scaffold mode) — same line, no SCHEDOP.
pub(crate) fn run_rtpool_demo() -> ! {
    // NO register_self: runs on the boot/driver thread, already registered
    // as "simnet-main" (the W-S2-1 one-door law — now a symbolic assert).
    let started = postmaster_seams::rtpool_start::call();
    assert!(started >= 0, "rtpool demo requires PGRUST_RUNTIME=1 (start returned {started})");
    // Rendezvous: when this park returns, every worker is at its eventcount
    // park (virtual time advances only when nothing is runnable).
    pgsync::thread::sleep(std::time::Duration::from_millis(10));
    let spawned = postmaster_seams::rtpool_population::call();
    postmaster_seams::rtpool_stop::call();
    let mut polls = 0u32;
    while postmaster_seams::rtpool_population::call() > 0 {
        pgsync::thread::sleep(std::time::Duration::from_millis(1));
        polls += 1;
        assert!(polls < 60_000, "rtpool demo: workers never drained");
    }
    println!("RTPOOL start={started} spawned_population={spawned} drained=0");
    std::process::exit(0)
}

// ---------------------------------------------------------------------------
// P4: the watchdog red fixture (intentionally unshimmed blocking).
// ---------------------------------------------------------------------------

fn run_watchdog_red() -> ! {
    // In-model victim: THIS thread (B) registers, so when it blocks on the
    // raw mutex below it is the PERMIT HOLDER blocked outside interception —
    // the one wedge shape THE watchdog exists to catch. No-op when the
    // scheduler is off (scaffold mode: fallback timer trips).
    let _door = pgsync::sim::spawn_door::register_self("red-main");
    // Fallback FAIL timer (red-battery discipline: the fixture FAILS if the
    // watchdog does not fire). Plain unregistered std thread, wall clock —
    // exactly like the watchdog itself, outside the model.
    let fallback_s: u64 = std::env::var("PGRUST_PERMIT_RED_FALLBACK_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(fallback_s));
        println!("RED-FALLBACK no watchdog fired after {fallback_s}s");
        std::process::exit(3);
    });

    // gate: A signals it holds the raw mutex; released is never signalled.
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    // THE RAW LOCK (the fixture): never migrates to pgsync.
    let raw = Arc::new(std::sync::Mutex::new(()));

    let a = {
        let gate = Arc::clone(&gate);
        let raw = Arc::clone(&raw);
        // In-model holder (pgsync spawn: registered behind the spawn fence):
        // A parks forever in a SHIM wait while holding the raw mutex, so the
        // schedule stays live until B reaches the raw lock.
        pgsync::thread::Builder::new()
            .name("red-holder".into())
            .spawn(move || {
                let _held = raw.lock().unwrap_or_else(|e| e.into_inner());
                let (lock, cv) = &*gate;
                let mut holding = lock.lock().unwrap_or_else(|e| e.into_inner());
                *holding = true;
                cv.notify_all();
                // Park FOREVER while holding the raw mutex (under the permit
                // scheduler this wait is a shim park that hands off).
                loop {
                    holding = cv.wait(holding).unwrap_or_else(|e| e.into_inner());
                    let _ = &holding;
                }
            })
            .expect("spawn red-holder")
    };

    // Wait until A holds the raw mutex.
    {
        let (lock, cv) = &*gate;
        let mut holding = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !*holding {
            holding = cv.wait(holding).unwrap_or_else(|e| e.into_inner());
        }
    }

    // B (this thread): block on the RAW mutex — outside interception, with
    // the permit once the scheduler exists. sim_sched_demo.rs RED SITE:
    let _stuck = raw.lock().unwrap_or_else(|e| e.into_inner());
    // Unreachable while A parks forever; the watchdog (or fallback) ends us.
    let _ = a;
    unreachable!("red fixture: raw mutex was released");
}
