//! Increment-2 synthetic-job e2e: deadline cadence, latch wake redirect,
//! exit retirement — over a real 1-worker runtime pool. No daemon is wired
//! (that is increments 3-4); these tests pin the dispatcher mechanism.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use runtime::{Runtime, RuntimeConfig, SizingParams, WorkerPool};
use types_storage::latch::Latch;

use crate::{BgJob, CycleOutcome, CycleReason, Dispatcher};

fn test_runtime() -> (Arc<Runtime>, WorkerPool) {
    let rt = Runtime::new(RuntimeConfig {
        workers: 1,
        standbys: 0,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).expect("pool spawn");
    (rt, pool)
}

struct TickJob {
    cadence: Duration,
    max_cycles: u64,
    cycles: AtomicU64,
    wakes: AtomicU64,
    latch: Option<&'static Latch>,
}

impl BgJob for TickJob {
    fn name(&self) -> &'static str {
        "tickjob"
    }

    fn latch(&self) -> Option<&'static Latch> {
        self.latch
    }

    fn run_cycle(&self, reason: CycleReason) -> CycleOutcome {
        // Cycle prologue analog: consume the wake (the real daemons
        // ResetLatch here, under the job envelope bind).
        if let Some(l) = self.latch {
            l.is_set.store(0, Ordering::SeqCst);
        }
        if reason == CycleReason::Wake {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
        let n = self.cycles.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= self.max_cycles {
            CycleOutcome::Exit
        } else {
            CycleOutcome::Sleep(self.cadence)
        }
    }
}

fn wait_until(deadline: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    pred()
}

/// Deadline cadence: a 20ms job ticks repeatedly and retires itself via
/// Exit after 4 cycles (first cycle = Startup, immediate).
#[test]
fn deadline_cycles_then_exit() {
    let (rt, _pool) = test_runtime();
    let d = Dispatcher::spawn(Arc::clone(&rt));
    let job = Arc::new(TickJob {
        cadence: Duration::from_millis(20),
        max_cycles: 4,
        cycles: AtomicU64::new(0),
        wakes: AtomicU64::new(0),
        latch: None,
    });
    let id = d.register(Arc::clone(&job) as Arc<dyn BgJob>);
    assert!(
        wait_until(Duration::from_secs(10), || d.is_exited(id)),
        "job must run its 4 cycles and retire (got {})",
        job.cycles.load(Ordering::SeqCst)
    );
    assert_eq!(job.cycles.load(Ordering::SeqCst), 4);
    // No further cycles after Exit.
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(job.cycles.load(Ordering::SeqCst), 4);
}

/// Latch wake redirect: a job parked on a LONG deadline is dispatched
/// promptly when its latch is set through the ordinary SetLatch path (the
/// production wake edge — no dispatcher API involved).
#[test]
fn latch_wake_dispatches_promptly() {
    let (rt, _pool) = test_runtime();
    let d = Dispatcher::spawn(Arc::clone(&rt));
    let l: &'static Latch = Box::leak(Box::new(Latch::new(true, 0)));
    let job = Arc::new(TickJob {
        cadence: Duration::from_secs(3600),
        max_cycles: u64::MAX,
        cycles: AtomicU64::new(0),
        wakes: AtomicU64::new(0),
        latch: Some(l),
    });
    let id = d.register(Arc::clone(&job) as Arc<dyn BgJob>);
    let _ = id;
    // Startup cycle runs, then the job idles on the hour-long deadline.
    assert!(wait_until(Duration::from_secs(10), || {
        job.cycles.load(Ordering::SeqCst) == 1
    }));
    // Give the dispatcher a pass to publish the idle waker, then wake
    // through the production latch path.
    std::thread::sleep(Duration::from_millis(30));
    latch::set_latch(l);
    assert!(
        wait_until(Duration::from_secs(10), || job.wakes.load(Ordering::SeqCst) >= 1),
        "latch set must dispatch a Wake cycle"
    );
    assert!(job.cycles.load(Ordering::SeqCst) >= 2);
}

/// Startup reason on the first cycle, Deadline afterward; poke() collapses
/// a long deadline.
#[test]
fn poke_collapses_deadline() {
    let (rt, _pool) = test_runtime();
    let d = Dispatcher::spawn(Arc::clone(&rt));
    struct ReasonJob {
        reasons: std::sync::Mutex<Vec<CycleReason>>,
        seen: AtomicUsize,
    }
    impl BgJob for ReasonJob {
        fn name(&self) -> &'static str {
            "reasonjob"
        }
        fn latch(&self) -> Option<&'static Latch> {
            None
        }
        fn run_cycle(&self, reason: CycleReason) -> CycleOutcome {
            self.reasons.lock().unwrap().push(reason);
            self.seen.fetch_add(1, Ordering::SeqCst);
            CycleOutcome::Sleep(Duration::from_secs(3600))
        }
    }
    let job = Arc::new(ReasonJob {
        reasons: std::sync::Mutex::new(Vec::new()),
        seen: AtomicUsize::new(0),
    });
    let id = d.register(Arc::clone(&job) as Arc<dyn BgJob>);
    assert!(wait_until(Duration::from_secs(10), || job.seen.load(Ordering::SeqCst) == 1));
    d.poke(id);
    assert!(
        wait_until(Duration::from_secs(10), || job.seen.load(Ordering::SeqCst) == 2),
        "poke must collapse the deadline"
    );
    let reasons = job.reasons.lock().unwrap();
    assert_eq!(reasons[0], CycleReason::Startup);
    assert_eq!(reasons[1], CycleReason::Deadline);
}

/// Maintenance cycles keep ticking while a foreground RG saturates the
/// pool — the §3.5 floor observed end-to-end through the dispatcher.
#[test]
fn cycles_run_under_foreground_load() {
    let (rt, _pool) = test_runtime();
    let d = Dispatcher::spawn(Arc::clone(&rt));

    // Foreground load: a long RG on the single worker.
    struct BusyWork;
    impl runtime::TaskSetWork for BusyWork {
        fn run_morsel(&self, _worker: usize, range: runtime::MorselRange) {
            // ~10µs per granule keeps the sizer honest (constant
            // throughput) and the whole RG at ~2s of wall — far longer
            // than the 5 fast maintenance cycles below.
            std::thread::sleep(Duration::from_micros(10 * (range.end - range.start)));
        }
        fn finalize(&self) {}
    }
    let (_fh, fg_waiter) = rt.submit(runtime::QuerySpec {
        query_id: 7,
        tasksets: vec![runtime::TaskSetSpec {
            source: Arc::new(runtime::SyntheticMorselSource::new(200_000)),
            work: Arc::new(BusyWork),
            deps: vec![],
        }],
    });

    let job = Arc::new(TickJob {
        cadence: Duration::from_millis(10),
        max_cycles: 5,
        cycles: AtomicU64::new(0),
        wakes: AtomicU64::new(0),
        latch: None,
    });
    let id = d.register(Arc::clone(&job) as Arc<dyn BgJob>);
    assert!(
        wait_until(Duration::from_secs(20), || d.is_exited(id)),
        "maintenance cycles must complete under foreground load (got {})",
        job.cycles.load(Ordering::SeqCst)
    );
    assert!(
        fg_waiter.try_wait().is_none(),
        "foreground RG should still be running when the 5 fast cycles finished"
    );
    let _ = fg_waiter.wait();
}

/// TWO concurrent latch jobs (the walwriter-migration shape: bgwriter +
/// walwriter on one dispatcher): waking job A's latch dispatches ONLY A —
/// B stays parked on its long deadline — and vice versa. Pins the per-job
/// wake routing the multi-job seat design depends on.
#[test]
fn two_latch_jobs_wake_independently() {
    let (rt, _pool) = test_runtime();
    let d = Dispatcher::spawn(Arc::clone(&rt));

    let mk = || -> (Arc<TickJob>, &'static Latch) {
        let l: &'static Latch = Box::leak(Box::new(Latch::new(true, 0)));
        (
            Arc::new(TickJob {
                cadence: Duration::from_secs(3600),
                max_cycles: u64::MAX,
                cycles: AtomicU64::new(0),
                wakes: AtomicU64::new(0),
                latch: Some(l),
            }),
            l,
        )
    };
    let (a, la) = mk();
    let (b, lb) = mk();
    let _ida = d.register(Arc::clone(&a) as Arc<dyn BgJob>);
    let _idb = d.register(Arc::clone(&b) as Arc<dyn BgJob>);

    // Both startup cycles run, then both idle on hour-long deadlines.
    assert!(wait_until(Duration::from_secs(10), || {
        a.cycles.load(Ordering::SeqCst) == 1 && b.cycles.load(Ordering::SeqCst) == 1
    }));
    std::thread::sleep(Duration::from_millis(30));

    // Wake A only.
    latch::set_latch(la);
    assert!(
        wait_until(Duration::from_secs(10), || a.wakes.load(Ordering::SeqCst) >= 1),
        "A's latch must dispatch A"
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(b.cycles.load(Ordering::SeqCst), 1, "B must not ride A's wake");

    // Wake B only.
    latch::set_latch(lb);
    assert!(
        wait_until(Duration::from_secs(10), || b.wakes.load(Ordering::SeqCst) >= 1),
        "B's latch must dispatch B"
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(a.cycles.load(Ordering::SeqCst), 2, "A must not ride B's wake");
}

/// Panic containment: a job whose cycle body panics is crash-retired
/// (crashed() hook fires once) while the dispatcher survives and other
/// jobs keep cycling.
#[test]
fn cycle_panic_crash_retires_job_dispatcher_survives() {
    let (rt, _pool) = test_runtime();
    let d = Dispatcher::spawn(Arc::clone(&rt));

    struct PanicJob {
        crashed: AtomicU64,
    }
    impl BgJob for PanicJob {
        fn name(&self) -> &'static str {
            "panicjob"
        }
        fn latch(&self) -> Option<&'static Latch> {
            None
        }
        fn crashed(&self) {
            self.crashed.fetch_add(1, Ordering::SeqCst);
        }
        fn run_cycle(&self, _reason: CycleReason) -> CycleOutcome {
            panic!("synthetic cycle panic");
        }
    }
    let bad = Arc::new(PanicJob { crashed: AtomicU64::new(0) });
    let bad_id = d.register(Arc::clone(&bad) as Arc<dyn BgJob>);

    let good = Arc::new(TickJob {
        cadence: Duration::from_millis(10),
        max_cycles: 3,
        cycles: AtomicU64::new(0),
        wakes: AtomicU64::new(0),
        latch: None,
    });
    let good_id = d.register(Arc::clone(&good) as Arc<dyn BgJob>);

    assert!(wait_until(Duration::from_secs(10), || d.is_exited(bad_id)));
    assert_eq!(bad.crashed.load(Ordering::SeqCst), 1, "crashed() exactly once");
    assert!(
        wait_until(Duration::from_secs(10), || d.is_exited(good_id)),
        "dispatcher must survive and keep cycling the good job (got {})",
        good.cycles.load(Ordering::SeqCst)
    );
    assert_eq!(good.cycles.load(Ordering::SeqCst), 3);
}
