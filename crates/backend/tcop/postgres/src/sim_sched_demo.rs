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
//! Lock imports are `std::sync` TODAY (pgsync does not exist yet at this
//! branch's base). PERMIT-S1 wiring point: when dst/permit-s1-sync lands,
//! the PLANT's imports flip to `pgsync::…` (one-line swap below) so its
//! ops are scheduler touches; the RED's raw `std::sync::Mutex` stays raw
//! FOREVER — it is the fixture. DST-REVIEW: this module is cfg(pgrust_sim)
//! (invisible to the determinism-lint scanner per the simnet-lane trap);
//! when WS-SYNC's `rawsync` category lands, this file's rows belong in the
//! `# PERMIT-S1-DEMO` allowlist band as the named red fixture.

// PERMIT-S1 wiring point (WS-SYNC): flip to `use pgsync::{Mutex, Condvar};`
// and `use pgsync::thread;` for the PLANT. The RED keeps raw std::sync.
use std::sync::{Arc, Condvar, Mutex};

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
    let ctr = Arc::new(Mutex::new(0u64));
    let mut handles = Vec::new();
    for t in 0..2u32 {
        let ctr = Arc::clone(&ctr);
        handles.push(
            std::thread::Builder::new()
                .name(format!("plant-{t}"))
                .spawn(move || {
                    for _ in 0..iters {
                        // THE RACE: read under one acquisition …
                        let seen = *ctr.lock().unwrap_or_else(|e| e.into_inner());
                        // … window (a scheduling touch under the permit
                        // scheduler; a plain yield today) …
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
// P4: the watchdog red fixture (intentionally unshimmed blocking).
// ---------------------------------------------------------------------------

fn run_watchdog_red() -> ! {
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
        std::thread::Builder::new()
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
