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
    router, ClockMode, FailAction, Scheduler, SchedulerConfig, WatchdogSink, UNREGISTERED_VPID,
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
