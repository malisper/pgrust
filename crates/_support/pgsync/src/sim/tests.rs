//! WS-CORE unit battery (contract §3.4 G2/G3 + the §3 EXIT toy corpus).
//!
//! Instance discipline: every test constructs its own [`Scheduler`] (private
//! seed, private Instance clock) so the battery is parallel-safe and every
//! SCHEDOP assertion is byte-deterministic. Ops go through the SAME
//! [`Router`] the sim wrappers use (`enter` binds TLS; trait calls route to
//! the test's instance). Deterministic-ordering trick used throughout: the
//! FIRST registered slot takes the first grant (the bootstrap handoff runs
//! at its registration, when it is the only runnable slot), so "A acts
//! before B" needs no seed pinning.

use core::time::Duration;
use std::collections::VecDeque;
use std::panic::Location;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::hooks::{OpClass, Vpid};
use super::sched::{
    router, ClockMode, FailAction, PickAlgo, Scheduler, SchedulerConfig, WatchdogSink,
    UNREGISTERED_VPID,
};
use super::watchdog;

fn plock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn test_cfg(seed: u64) -> SchedulerConfig {
    SchedulerConfig {
        seed,
        ..SchedulerConfig::default()
    }
}

/// A wrapper-shaped test channel: wait/wake list owned by the wrapper,
/// wakee = seeded pick, would-block = block_on in a predicate loop —
/// exactly the pattern pgsync's sim wrappers use. Race-free under the
/// permit: every check-then-park sequence is quantum-atomic because
/// block_on is the only yield point.
struct SimChan {
    q: Mutex<VecDeque<u64>>,
    waiters: Mutex<Vec<Vpid>>,
}

impl SimChan {
    fn new() -> Self {
        SimChan {
            q: Mutex::new(VecDeque::new()),
            waiters: Mutex::new(Vec::new()),
        }
    }

    #[track_caller]
    fn send(&self, v: u64) {
        let site = Location::caller();
        let r = router();
        plock(&self.q).push_back(v);
        let wakee = {
            let mut ws = plock(&self.waiters);
            if ws.is_empty() {
                None
            } else {
                let i = r.pick_waiter(site, ws.len());
                Some(ws.remove(i))
            }
        };
        if let Some(t) = wakee {
            r.wake(t, site);
        }
        r.touch(site, OpClass::ChanSend);
    }

    #[track_caller]
    fn recv(&self) -> u64 {
        let site = Location::caller();
        let r = router();
        loop {
            if let Some(v) = plock(&self.q).pop_front() {
                return v;
            }
            plock(&self.waiters).push(r.current_vpid());
            r.block_on(site, OpClass::ChanRecv);
        }
    }

    /// recv with a (relative) virtual-time deadline.
    #[track_caller]
    fn recv_timeout(&self, dur: Duration) -> Option<u64> {
        let site = Location::caller();
        let r = router();
        loop {
            if let Some(v) = plock(&self.q).pop_front() {
                return Some(v);
            }
            plock(&self.waiters).push(r.current_vpid());
            if r.timed_park(site, dur) {
                // Deadline expiry: withdraw the (still-registered) wait.
                let me = r.current_vpid();
                plock(&self.waiters).retain(|w| *w != me);
                return None;
            }
        }
    }
}

// --- G2: picker determinism ------------------------------------------------

#[test]
fn picker_determinism_fixed_entropy() {
    let picks = |seed: u64| -> Vec<usize> {
        let sched = Scheduler::new(test_cfg(seed));
        let vpid = sched.register_self("picker");
        let site = Location::caller();
        let r = router();
        let v: Vec<usize> = (0..32).map(|_| r.pick_waiter(site, 10)).collect();
        r.exit(vpid);
        v
    };
    let a = std::thread::spawn(move || picks(0x5EED)).join().unwrap();
    let b = std::thread::spawn(move || picks(0x5EED)).join().unwrap();
    let c = std::thread::spawn(move || picks(0xBEEF)).join().unwrap();
    assert_eq!(a, b, "same seed => same pick stream");
    assert_ne!(a, c, "different seed => different pick stream");
    assert!(a.iter().all(|&i| i < 10));
}

// --- G2: would-block handoff per op class -----------------------------------

#[test]
fn would_block_handoff_per_op_class() {
    for kind in [
        OpClass::MutexLock,
        OpClass::RwRead,
        OpClass::RwWrite,
        OpClass::CondWait,
        OpClass::ChanRecv,
        OpClass::ChanSend,
        OpClass::SemAcquire,
        OpClass::BarrierWait,
        OpClass::OnceInit,
        OpClass::Park,
    ] {
        let sched = Scheduler::new(test_cfg(1));
        // A registered first => A takes the first grant.
        let a = sched.register(101, "a");
        let b = sched.register(102, "b");
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let (oa, ob) = (order.clone(), order.clone());
        let (sa, sb) = (sched.clone(), sched.clone());
        let ta = std::thread::spawn(move || {
            sa.enter(a);
            let site = Location::caller();
            plock(&oa).push("a-blocks");
            router().block_on(site, kind);
            plock(&oa).push("a-woken");
            router().exit(101);
        });
        let tb = std::thread::spawn(move || {
            sb.enter(b);
            let site = Location::caller();
            plock(&ob).push("b-runs");
            router().wake(101, site);
            router().exit(102);
        });
        ta.join().unwrap();
        tb.join().unwrap();
        assert_eq!(
            *plock(&order),
            vec!["a-blocks", "b-runs", "a-woken"],
            "mandatory handoff on {kind:?}"
        );
        let log = sched.dump_log();
        assert!(
            log.contains("Block site=") && log.contains(&format!("kind={}", kind.as_str())),
            "SCHEDOP records the {kind:?} block:\n{log}"
        );
    }
}

// --- G2: virtual-time advance ------------------------------------------------

#[test]
fn virtual_time_advances_to_earliest_deadline_when_idle() {
    let sched = Scheduler::new(test_cfg(2));
    let a = sched.register(101, "sleeper");
    let s = sched.clone();
    let t = std::thread::spawn(move || {
        s.enter(a);
        let site = Location::caller();
        let r1 = router().timed_park(site, Duration::from_nanos(1_234));
        let r2 = router().timed_park(site, Duration::from_nanos(3_766));
        router().exit(101);
        (r1, r2)
    });
    let (r1, r2) = t.join().unwrap();
    assert!(r1, "nothing else runnable: the deadline expired");
    assert!(r2);
    assert_eq!(sched.now_ns(), 5_000, "advanced exactly to each deadline");
    let log = sched.dump_log();
    assert!(log.contains("Advance site=- now=1234 woke=1"), "{log}");
    assert!(log.contains("Advance site=- now=5000 woke=1"), "{log}");
}

#[test]
fn timed_park_woken_before_deadline_does_not_advance() {
    let sched = Scheduler::new(test_cfg(3));
    let ch = Arc::new(SimChan::new());
    let a = sched.register(101, "rx");
    let b = sched.register(102, "tx");
    let (sa, sb) = (sched.clone(), sched.clone());
    let (ca, cb) = (ch.clone(), ch.clone());
    let ta = std::thread::spawn(move || {
        let _ = &ca;
        sa.enter(a);
        let got = ca.recv_timeout(Duration::from_millis(1));
        router().exit(101);
        got
    });
    let tb = std::thread::spawn(move || {
        sb.enter(b);
        cb.send(42);
        router().exit(102);
    });
    let got = ta.join().unwrap();
    tb.join().unwrap();
    assert_eq!(got, Some(42));
    assert_eq!(sched.now_ns(), 0, "no advance: the wake beat the deadline");
    assert!(!sched.dump_log().contains("Advance"), "{}", sched.dump_log());
}

// --- G2: never-satisfied-predicate ceiling ------------------------------------

#[test]
fn virtual_time_ceiling_is_a_run_bound() {
    let mut cfg = test_cfg(4);
    cfg.virtual_ceiling_ns = Some(10_000);
    let sched = Scheduler::new(cfg);
    let a = sched.register(101, "spinner");
    let s = sched.clone();
    let t = std::thread::spawn(move || {
        s.enter(a);
        let site = Location::caller();
        // Never-satisfied predicate: re-park forever; every park times out
        // and re-arms further out until the ceiling trips.
        loop {
            let _ = router().timed_park(site, Duration::from_nanos(4_000));
        }
    });
    let err = t.join().expect_err("ceiling must trip");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "non-string panic".into());
    assert!(msg.contains("SCHEDCEILING"), "{msg}");
    assert!(msg.contains("seed=4"), "report carries the seed: {msg}");
}

// --- G2: deterministic-deadlock report -----------------------------------------

#[test]
fn deterministic_deadlock_reports_slots_and_seed() {
    let sched = Scheduler::new(test_cfg(5));
    let a = sched.register(101, "loner");
    let s = sched.clone();
    let t = std::thread::spawn(move || {
        s.enter(a);
        // Nobody will ever wake this: all live slots parked in shims, no
        // timed sleeper => immediate pick-fail report, NOT a watchdog case.
        router().block_on(Location::caller(), OpClass::MutexLock);
    });
    let err = t.join().expect_err("deadlock must be reported");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "non-string panic".into());
    assert!(msg.contains("SCHEDDEADLOCK"), "{msg}");
    assert!(msg.contains("seed=5"), "{msg}");
    assert!(msg.contains("vpid=101"), "{msg}");
    assert!(msg.contains("blocked(mutex-lock)"), "{msg}");
    assert!(msg.contains("SCHEDOP tail:"), "{msg}");
}

// --- G2: TLS-teardown rules under churn ----------------------------------------

#[test]
fn tls_teardown_rules_under_thread_churn() {
    let sched = Scheduler::new(test_cfg(6));
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let ev = events.clone();
    sched.register_teardown_hook(move |vpid| {
        plock(&ev).push(format!("teardown:{vpid}"));
    });

    // Rule 3: a joiner wakes on deregister, post-dating shared teardown
    // (predicate-loop join, the pgsync::thread wrapper protocol).
    let done = Arc::new(AtomicBool::new(false));
    let j = sched.register(201, "joiner");
    let t = sched.register(202, "target");
    let (sj, st) = (sched.clone(), sched.clone());
    let (ej, et) = (events.clone(), events.clone());
    let (dj, dt) = (done.clone(), done.clone());
    let tj = std::thread::spawn(move || {
        sj.enter(j);
        assert_eq!(super::current_vpid(), Some(201), "rule 2: identity is the vpid");
        let site = Location::caller();
        while !dj.load(Ordering::SeqCst) {
            router().block_on(site, OpClass::Join);
        }
        plock(&ej).push("joiner-woke".into());
        router().exit(201);
    });
    let tt = std::thread::spawn(move || {
        st.enter(t);
        assert_eq!(super::current_vpid(), Some(202));
        plock(&et).push("target-ran".into());
        dt.store(true, Ordering::SeqCst);
        router().exit(202);
    });
    tj.join().unwrap();
    tt.join().unwrap();
    assert_eq!(
        *plock(&events),
        vec![
            "target-ran".to_string(),
            "teardown:202".to_string(), // rule 1: inside the final quantum
            "joiner-woke".to_string(),  // rule 3: join wake post-dates teardown
            "teardown:201".to_string(),
        ]
    );

    // Churn: fresh vpids get fresh slots; teardown runs once per thread.
    for vpid in [301u32, 302, 303] {
        let slot = sched.register(vpid, "churn");
        let s = sched.clone();
        std::thread::spawn(move || {
            s.enter(slot);
            assert_eq!(super::current_vpid(), Some(vpid));
            router().exit(vpid);
        })
        .join()
        .unwrap();
    }
    let evs = plock(&events).clone();
    for vpid in [301, 302, 303] {
        assert_eq!(
            evs.iter().filter(|e| **e == format!("teardown:{vpid}")).count(),
            1,
            "exactly one teardown per churned thread: {evs:?}"
        );
    }

    // Rule 2 negative: slot identity is the vpid — re-registering one
    // (live or exited) is a caller bug and panics.
    let dup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sched.register(301, "dup")
    }));
    let msg = match dup {
        Err(p) => p
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_else(|| "non-string panic".into()),
        Ok(_) => panic!("duplicate vpid registration must panic"),
    };
    assert!(msg.contains("duplicate vpid"), "{msg}");
}

// --- G3: the watchdog red unit ---------------------------------------------------

#[test]
fn watchdog_red_names_the_unshimmed_site() {
    let captured = Arc::new(Mutex::new(None::<String>));
    let mut cfg = test_cfg(7);
    cfg.watchdog_timeout_ms = 150;
    cfg.watchdog_poll_ms = 20;
    cfg.watchdog_sink = WatchdogSink::Capture(captured.clone());
    let sched = Scheduler::new(cfg);
    watchdog::start(&sched);

    // The unshimmed block: a RAW std::sync::Mutex held across a quantum.
    // (The raw lock below is the DELIBERATE red fixture — test-only, and the
    // whole battery is cfg(test), outside the determinism lint's prod scan.)
    let raw = Arc::new(Mutex::new(()));
    let wedge_guard = raw.lock().unwrap();

    let a = sched.register(401, "wedger");
    let s = sched.clone();
    let r = raw.clone();
    let expected_site = Arc::new(Mutex::new(String::new()));
    let es = expected_site.clone();
    let t = std::thread::spawn(move || {
        s.enter(a);
        // The holder's LAST SHIM EVENT — the site the dump must name.
        let site = Location::caller();
        *plock(&es) = format!("{}:{}", site.file(), site.line());
        router().touch(site, OpClass::CondNotify);
        // Permit held; now block where the scheduler cannot see.
        let _g = r.lock().unwrap();
        router().exit(401);
    });

    // Wait (wall time, generously) for the watchdog to fire and capture.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let report = loop {
        if let Some(r) = plock(&captured).clone() {
            break r;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "watchdog did not fire within 20s (red battery: this test FAILS if the watchdog stays silent)"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    // Unwedge and drain the thread before asserting.
    drop(wedge_guard);
    t.join().unwrap();

    assert!(
        report.contains("permit-holder-blocked-outside-interception"),
        "{report}"
    );
    assert!(report.contains("vpid=401"), "{report}");
    let site = plock(&expected_site).clone();
    assert!(
        report.contains(&site),
        "the dump NAMES the blocked site symbolically (want {site}):\n{report}"
    );
    assert!(report.contains("SCHEDOP tail:"), "{report}");
}

// --- EXIT: the seeded 2-thread toy corpus ----------------------------------------

/// Two registered threads ping-pong 3 messages through wrapper-shaped
/// channels under the permit (with seeded preemption on), then exit. Returns
/// the full SCHEDOP stream.
fn run_toy_corpus(seed: u64) -> String {
    let mut cfg = test_cfg(seed);
    cfg.preempt_p = 0.25;
    let sched = Scheduler::new(cfg);
    let ab = Arc::new(SimChan::new());
    let ba = Arc::new(SimChan::new());
    let a = sched.register(101, "toy:a");
    let b = sched.register(102, "toy:b");
    let (sa, sb) = (sched.clone(), sched.clone());
    let (ab_a, ba_a) = (ab.clone(), ba.clone());
    let (ab_b, ba_b) = (ab.clone(), ba.clone());
    let ta = std::thread::spawn(move || {
        sa.enter(a);
        for i in 1..=3u64 {
            ab_a.send(i);
            let ack = ba_a.recv();
            assert_eq!(ack, i * 10);
        }
        router().exit(101);
    });
    let tb = std::thread::spawn(move || {
        sb.enter(b);
        for _ in 1..=3 {
            let v = ab_b.recv();
            ba_b.send(v * 10);
        }
        router().exit(102);
    });
    ta.join().unwrap();
    tb.join().unwrap();
    sched.dump_log()
}

#[test]
fn toy_corpus_same_seed_byte_identical_schedop() {
    let r1 = run_toy_corpus(0xC0FFEE);
    let r2 = run_toy_corpus(0xC0FFEE);
    let r3 = run_toy_corpus(0xC0FFEE);
    assert_eq!(r1, r2, "same seed => byte-identical SCHEDOP stream");
    assert_eq!(r2, r3, "same seed => byte-identical SCHEDOP stream (x3)");
    assert!(r1.starts_with("SCHEDOP 0 "), "stream starts at seq 0:\n{r1}");
    assert!(r1.contains("Exit"), "corpus ran to completion:\n{r1}");
    // The stream is dense: seq numbers are exactly 0..n with no gaps.
    let n = r1.lines().count();
    for (i, line) in r1.lines().enumerate() {
        assert!(
            line.starts_with(&format!("SCHEDOP {i} ")),
            "dense seq at line {i}/{n}: {line}"
        );
    }
}

#[test]
fn toy_corpus_seeds_diversify_schedules() {
    let logs: Vec<String> = (0..8).map(|s| run_toy_corpus(0xA000 + s)).collect();
    let distinct: std::collections::HashSet<&String> = logs.iter().collect();
    assert!(
        distinct.len() >= 2,
        "8 seeds with preemption on must produce at least 2 distinct schedules"
    );
}

// --- PgClock-arm smoke (the global scheduler's clock mode) -----------------------

#[test]
fn pgclock_mode_timed_park_advances_the_sim_clock() {
    // Parallel-tolerant assertions only: this instance drives the
    // PROCESS-WIDE SimClock (frozen-mode default never advances by itself,
    // and advance_ns is legal in any mode), so other tests may also move it.
    let mut cfg = test_cfg(9);
    cfg.clock = ClockMode::PgClock;
    let sched = Scheduler::new(cfg);
    let a = sched.register(501, "pgclock");
    let s = sched.clone();
    let before = pg_clock::mono_ns();
    let t = std::thread::spawn(move || {
        s.enter(a);
        let expired = router().timed_park(Location::caller(), Duration::from_nanos(10_000));
        router().exit(501);
        expired
    });
    assert!(t.join().unwrap(), "nothing else runnable: deadline expired");
    assert!(
        pg_clock::mono_ns() >= before + 10_000,
        "the driven-mode lever moved mono past the deadline"
    );
}

// --- hooks are neutral off-model --------------------------------------------------

#[test]
fn unregistered_threads_fall_through() {
    // No registration (and no global scheduler: PGRUST_SIM_SCHED unset in
    // unit runs): the Router must be inert — this is what keeps
    // scheduler-off sim runs byte-identical to today.
    let site = Location::caller();
    let r = router();
    assert_eq!(super::current_vpid(), None);
    assert_eq!(r.current_vpid(), UNREGISTERED_VPID);
    r.block_on(site, OpClass::MutexLock); // yields, returns
    r.touch(site, OpClass::MutexUnlock);
    r.wake(12345, site);
    assert_eq!(r.pick_waiter(site, 4), 0);
    assert!(r.timed_park(site, Duration::from_millis(1)), "real-sleep fallback");
    r.exit(12345);
}

// --- config default sanity ------------------------------------------------------

#[test]
fn instance_defaults_are_panicky_and_preemption_off() {
    let cfg = SchedulerConfig::default();
    assert_eq!(cfg.fail, FailAction::Panic);
    assert_eq!(cfg.preempt_p, 0.0);
    assert_eq!(cfg.clock, ClockMode::Instance);
}

// --- BLOCKING-1: Condvar check-before-park under guard-drop preemption -------
// Adversarial-review BLOCKING-1 (2026-07-18, WS-SYNC fixer f0ca1b5e7):
// `Condvar::wait`'s guard drop embeds unlock hooks (pick/wake/touch — a
// preemption point). A seeded preemption there lets the notifier run, PICK
// the enqueued waiter out of the wait list, and fire a `wake` that targets a
// RUNNABLE (pre-park) thread — which the scheduler DROPS by protocol
// (`Wake … prev=runnable`). Pre-fix the waiter then parked forever on a
// consumed wait entry (phantom deadlock); post-fix (check-before-park in
// wait/wait_timeout) it returns without parking.
//
// Unlike the rest of this battery, these tests drive the REAL pgsync sim
// wrappers (crate::Mutex / crate::Condvar) — hooks installed (ROUTER),
// preempt_p = 1.0 so the guard-drop touch ALWAYS preempts. A seed sweep
// covers both legs of the seeded handoff at that touch:
//   - handoff -> notifier: the BLOCKING-1 interleaving (wake dropped on the
//     pre-park waiter; the log shows `Wake … target=101 prev=runnable` and
//     NO cond-wait park — the waiter was consumed before ever parking);
//   - handoff -> waiter: the benign park-then-notify order.
// EVERY seed must complete: a reintroduced lost wake becomes a deterministic
// deadlock => FailAction::Panic => joined-thread panic => test failure (the
// wait_timeout leg would instead be rescued by idle virtual-time advance, so
// it additionally asserts the wait did NOT time out).

use crate::{Condvar as PgCondvar, Mutex as PgMutex};

fn install_router_once() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| super::hooks::install(&super::sched::ROUTER));
}

/// One seeded run of the two-thread wait/notify corpus over the real
/// wrappers. Returns the SCHEDOP log.
fn cond_blocking1_run(seed: u64, timed: bool) -> String {
    install_router_once();
    let mut cfg = test_cfg(seed);
    cfg.preempt_p = 1.0; // every touch preempts — the guard-drop touch included
    let sched = Scheduler::new(cfg);
    // Waiter registered first => takes the first grant (battery convention).
    let w = sched.register(101, "waiter");
    let n = sched.register(102, "notifier");
    let pair = Arc::new((PgMutex::new(false), PgCondvar::new()));
    let (pw, pn) = (pair.clone(), pair.clone());
    let (sw, sn) = (sched.clone(), sched.clone());
    let tw = std::thread::spawn(move || {
        sw.enter(w);
        let (m, cv) = &*pw;
        let mut g = m.lock().unwrap_or_else(|e| e.into_inner());
        while !*g {
            if timed {
                // Far-future virtual deadline: expiry would mean the wake
                // was lost and only idle-advance rescued us — the pre-fix
                // failure shape for this leg. Assert against it.
                let (g2, res) = cv
                    .wait_timeout(g, Duration::from_secs(3600))
                    .unwrap_or_else(|e| e.into_inner());
                assert!(
                    !res.timed_out(),
                    "seed {seed}: wait_timeout expired — the notify was lost"
                );
                g = g2;
            } else {
                g = cv.wait(g).unwrap_or_else(|e| e.into_inner());
            }
        }
        drop(g);
        router().exit(101);
    });
    let tn = std::thread::spawn(move || {
        sn.enter(n);
        let (m, cv) = &*pn;
        let mut g = m.lock().unwrap_or_else(|e| e.into_inner());
        *g = true;
        // Notify while HOLDING the mutex: when the waiter was preempted at
        // its guard-drop touch, this pick consumes it PRE-PARK and the wake
        // targets a runnable thread (the dropped-wake window).
        cv.notify_one();
        drop(g);
        router().exit(102);
    });
    // Join the NOTIFIER first: a reintroduced lost wake deadlocks at the
    // notifier's exit handoff (FailAction::Panic on ITS thread) while the
    // waiter is parked forever — joining the waiter first would hang the
    // harness instead of failing it.
    tn.join().expect("notifier completed (a lost wake deadlock-panics here)");
    tw.join().expect("waiter completed");
    sched.dump_log()
}

/// The dropped-wake signature: a wake aimed at the pre-park waiter. Only the
/// Condvar guard-drop window can produce it in this corpus (the mutex path
/// has no hook between its wait-list enqueue and `block_on`).
fn dropped_wake_on_waiter(log: &str) -> bool {
    log.lines()
        .any(|l| l.contains(" Wake ") && l.contains("target=101") && l.contains("prev=runnable"))
}

#[test]
fn condvar_wait_survives_pre_park_notify_consumption() {
    let mut blocking1_leg_seen = false;
    for seed in 0..16 {
        let log = cond_blocking1_run(seed, false);
        if dropped_wake_on_waiter(&log) {
            blocking1_leg_seen = true;
            // The waiter was consumed before ever parking: no cond-wait
            // park may appear (check-before-park returned immediately).
            assert!(
                !log.contains("kind=cond-wait"),
                "seed {seed}: dropped wake yet the waiter parked on the condvar:\n{log}"
            );
        }
    }
    assert!(
        blocking1_leg_seen,
        "16-seed sweep never produced the BLOCKING-1 interleaving \
         (guard-drop preempt -> pre-park notify): the corpus lost its teeth"
    );
}

#[test]
fn condvar_wait_timeout_survives_pre_park_notify_consumption() {
    let mut blocking1_leg_seen = false;
    for seed in 0..16 {
        let log = cond_blocking1_run(seed, true);
        if dropped_wake_on_waiter(&log) {
            blocking1_leg_seen = true;
            assert!(
                !log.contains("kind=cond-wait"),
                "seed {seed}: dropped wake yet the waiter timed-parked on the condvar:\n{log}"
            );
        }
    }
    assert!(
        blocking1_leg_seen,
        "16-seed sweep never produced the BLOCKING-1 interleaving on the \
         wait_timeout leg: the corpus lost its teeth"
    );
}

// --- PCT priorities (PERMIT-S2 step 1; Burckhardt et al. ASPLOS 2010) -----------

/// Two registered threads each run `iters` non-blocking touches (sends into
/// a never-received SimChan) under PCT and exit. Returns the SCHEDOP stream.
fn run_pct_touch_corpus(seed: u64, depth: u32, steps: u64, iters: u64) -> String {
    let mut cfg = test_cfg(seed);
    cfg.algo = PickAlgo::Pct { depth, steps };
    let sched = Scheduler::new(cfg);
    let chan = Arc::new(SimChan::new());
    let a = sched.register(101, "pct:a");
    let b = sched.register(102, "pct:b");
    let (sa, sb) = (sched.clone(), sched.clone());
    let (ca, cb) = (chan.clone(), chan.clone());
    let ta = std::thread::spawn(move || {
        sa.enter(a);
        for i in 0..iters {
            ca.send(i);
        }
        router().exit(101);
    });
    let tb = std::thread::spawn(move || {
        sb.enter(b);
        for i in 0..iters {
            cb.send(100 + i);
        }
        router().exit(102);
    });
    ta.join().unwrap();
    tb.join().unwrap();
    sched.dump_log()
}

fn preempt_count(log: &str) -> usize {
    log.lines().filter(|l| l.contains("preempt=1")).count()
}

/// Depth 1 = zero change points: the highest-priority runnable slot runs to
/// its exit uninterrupted. The only possible preemption is the initial
/// priority sort-out (the first-granted slot losing to a higher-priority
/// peer at its first touch), so preempt=1 appears AT MOST ONCE — and a seed
/// sweep must produce both priority orders (the priorities are seeded).
#[test]
fn pct_depth1_highest_priority_runs_uninterrupted() {
    let mut orders = std::collections::HashSet::new();
    for seed in 0..16u64 {
        let log = run_pct_touch_corpus(seed, 1, 64, 6);
        let n = preempt_count(&log);
        assert!(n <= 1, "seed {seed}: depth-1 PCT preempted {n} times:\n{log}");
        orders.insert(n);
    }
    assert_eq!(
        orders.len(),
        2,
        "16 seeds must produce both priority orders (0- and 1-preempt runs)"
    );
}

/// Depth 2 over a tiny step budget: the single change point fires while
/// both threads are live in some seeds, dropping the running thread below
/// its peer — a second preemption the depth-1 corpus can never produce.
/// Every seed replays byte-identically.
#[test]
fn pct_change_point_preempts_and_replays() {
    let mut cp_preempt_seen = false;
    for seed in 0..16u64 {
        let l1 = run_pct_touch_corpus(seed, 2, 8, 6);
        let l2 = run_pct_touch_corpus(seed, 2, 8, 6);
        assert_eq!(l1, l2, "seed {seed}: PCT schedule must replay byte-identically");
        if preempt_count(&l1) >= 2 {
            cp_preempt_seen = true;
        }
    }
    assert!(
        cp_preempt_seen,
        "16-seed depth-2 sweep never showed a change-point preemption"
    );
}

/// The planted lost-update RMW shape (P2's plant, in-unit): read under one
/// touch-delimited window, write under another. PCT with d=2 must find both
/// outcomes across a seed sweep (a depth-2 bug: one forced preemption in
/// the window), and each outcome must replay from its seed.
#[test]
fn pct_finds_depth2_lost_update_in_seed_sweep() {
    fn run(seed: u64) -> u64 {
        let mut cfg = test_cfg(seed);
        cfg.algo = PickAlgo::Pct { depth: 2, steps: 64 };
        let sched = Scheduler::new(cfg);
        let chan = Arc::new(SimChan::new());
        let ctr = Arc::new(Mutex::new(0u64));
        let a = sched.register(101, "rmw:a");
        let b = sched.register(102, "rmw:b");
        let mut handles = Vec::new();
        for (slot, vpid) in [(a, 101u32), (b, 102u32)] {
            let (s, c, v) = (sched.clone(), chan.clone(), ctr.clone());
            handles.push(std::thread::spawn(move || {
                s.enter(slot);
                for i in 0..4u64 {
                    let seen = *plock(&v);
                    c.send(i); // the window: a non-blocking touch
                    *plock(&v) = seen + 1;
                }
                router().exit(vpid);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let f = *plock(&ctr);
        assert!(f <= 8);
        f
    }
    let finals: Vec<u64> = (0..64u64).map(run).collect();
    let lost0 = finals.iter().filter(|f| **f == 8).count();
    let lost_n = finals.iter().filter(|f| **f < 8).count();
    assert!(
        lost0 > 0 && lost_n > 0,
        "64-seed PCT sweep must find both outcomes (no-loss {lost0} / lost {lost_n})"
    );
    // Replay: one representative seed per outcome, x2 identical.
    let s0 = (0..64u64).find(|s| finals[*s as usize] == 8).unwrap();
    let sn = (0..64u64).find(|s| finals[*s as usize] < 8).unwrap();
    assert_eq!(run(s0), finals[s0 as usize], "no-loss seed replays");
    assert_eq!(run(sn), finals[sn as usize], "lost seed replays");
}
