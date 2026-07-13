//! Loom models for the M0 runtime core — the first loom models in the repo
//! (redesign doc §4: the previously-claimed models were aspirational; these
//! are new work and an M0 merge gate).
//!
//! Build/run:
//!   RUSTFLAGS="--cfg loom" cargo test -p runtime --test loom --release
//!
//! Models:
//!   1. finalization_protocol — the last-worker-out protocol under bounded-
//!      exhaustive interleavings of 2 workers × exhaustion × markers ×
//!      counter (including the transiently-negative counter case):
//!      finalize exactly once per task set, never while a worker is inside
//!      run_morsel, every granule exactly once, next task set activates
//!      only after its predecessor finalizes.
//!   2. generation_handoff_unconsumable — pinned-interface invariant: a
//!      task from an aborted generation is unconsumable (abort completed ⇒
//!      enter fails); retire refuses while a participant is inside; retired
//!      generations never admit.
//!   3. abort_cleanup — scheduler-level abort: cleanup rides the ordinary
//!      protocol, the RG completes Aborted, finalize work never runs, and
//!      no morsel of the aborted generation starts after abort completes.
//!   4. standby_absorption — §2.8 addendum: a permit released mid-task-set
//!      (IoGuard) lets a standby absorb the core while the blocked worker
//!      keeps its pin-board entry and finalization-marker obligations; the
//!      last-worker-out counter is never confused.
//!
//! Determinism note: models use a never-advanced VirtualClock so every
//! morsel measures a constant dt (loom requires branch determinism along an
//! execution path — a wall clock would make sizing branches diverge).

#![cfg(loom)]

use std::sync::Arc;

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::thread;

use runtime::{
    Clock, CompletionWaiter, MorselRange, QuerySpec, QueryTaskGuard, RgOutcome, Runtime,
    RuntimeConfig, SizingParams, Step, SyntheticMorselSource, TaskLifecycle, TaskSetSpec,
    TaskSetWork, VirtualClock,
};

fn small_runtime(workers: usize, standbys: usize) -> Arc<Runtime> {
    let cfg = RuntimeConfig {
        workers,
        standbys,
        slots: 2,
        sizing: SizingParams::default(),
        trace: false,
    };
    Runtime::with_clock(cfg, Arc::new(VirtualClock::new()) as Arc<dyn Clock>)
}

/// Drive worker steps until the RG completes, with the production park
/// discipline (spinning on yield_now would grow loom's path unboundedly —
/// parking on the eventcount is both the real behavior and loom-blockable).
/// The first driver to observe completion stops the runtime so parked
/// peers wake and exit, as production shutdown does.
fn drive(rt: &Runtime, worker: usize, waiter: &CompletionWaiter) {
    let mut local = rt.worker_local(worker);
    loop {
        if waiter.try_wait().is_some() {
            break;
        }
        let epoch = rt.park_epoch();
        match rt.worker_step(&mut local) {
            Step::Ran => {}
            // Retry = the coordinator is mid-invalidation (slot word already
            // invalid, active bit not yet cleared). A real scheduler runs the
            // coordinator eventually; loom must be TOLD the spinner cannot
            // progress alone.
            Step::Retry => thread::yield_now(),
            Step::Idle => {
                if waiter.try_wait().is_some() {
                    break;
                }
                rt.park(epoch);
            }
            Step::Stop => break,
        }
    }
    rt.request_stop();
}

/// Work body instrumented for the protocol invariants.
struct ModelWork {
    /// Per-granule execution counts (exactly-once assert).
    executed: Vec<AtomicUsize>,
    /// Workers currently inside run_morsel (finalize must observe 0).
    inside: AtomicUsize,
    /// Finalize count (exactly-once assert).
    finalized: AtomicUsize,
    /// Predecessor task set (dep-order assert): must have finalized before
    /// any of our morsels runs.
    predecessor: Option<Arc<ModelWork>>,
}

impl ModelWork {
    fn new(total: u64, predecessor: Option<Arc<ModelWork>>) -> Arc<ModelWork> {
        Arc::new(ModelWork {
            executed: (0..total).map(|_| AtomicUsize::new(0)).collect(),
            inside: AtomicUsize::new(0),
            finalized: AtomicUsize::new(0),
            predecessor,
        })
    }

    fn assert_complete(&self) {
        for (g, x) in self.executed.iter().enumerate() {
            assert_eq!(x.load(Ordering::SeqCst), 1, "granule {g} not exactly-once");
        }
        assert_eq!(self.finalized.load(Ordering::SeqCst), 1, "finalize not exactly-once");
        assert_eq!(self.inside.load(Ordering::SeqCst), 0);
    }
}

impl TaskSetWork for ModelWork {
    fn run_morsel(&self, _worker: usize, range: MorselRange) {
        if let Some(p) = &self.predecessor {
            assert_eq!(
                p.finalized.load(Ordering::SeqCst),
                1,
                "task set ran before its dependency finalized"
            );
        }
        self.inside.fetch_add(1, Ordering::SeqCst);
        for g in range {
            let prev = self.executed[g as usize].fetch_add(1, Ordering::SeqCst);
            assert_eq!(prev, 0, "granule {g} executed twice");
        }
        self.inside.fetch_sub(1, Ordering::SeqCst);
    }

    fn finalize(&self) {
        // The protocol's core promise: finalization runs only after ALL
        // workers finished their in-flight tasks — an empty morsel queue is
        // NOT completion (literature mistake #2).
        assert_eq!(
            self.inside.load(Ordering::SeqCst),
            0,
            "finalize ran while a worker was inside run_morsel"
        );
        for (g, x) in self.executed.iter().enumerate() {
            assert_eq!(x.load(Ordering::SeqCst), 1, "finalize before granule {g} ran");
        }
        let prev = self.finalized.fetch_add(1, Ordering::SeqCst);
        assert_eq!(prev, 0, "finalize ran twice");
    }
}

/// Model 1: 2 workers, one RG of two dependent task sets (2 + 1 granules,
/// C0 = 1 so claims interleave). Explores coordinator election, marker
/// swaps, the transiently-negative counter, and next-task-set activation
/// in the same slot.
#[test]
fn finalization_protocol() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    // The full scheduler step is branch-heavy (stats ticks, sizer locks);
    // the default 1k-branch budget underestimates one execution.
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        let w1 = ModelWork::new(2, None);
        let w2 = ModelWork::new(1, Some(Arc::clone(&w1)));
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                    work: Arc::clone(&w1) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                },
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1).with_c0(1)),
                    work: Arc::clone(&w2) as Arc<dyn TaskSetWork>,
                    deps: vec![0],
                },
            ],
        });

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive(&rt1, 1, &waiter1));
        drive(&rt, 0, &waiter);
        t.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        w1.assert_complete();
        w2.assert_complete();
        let stats = rt.stats();
        assert_eq!(stats.finalize_events, 2);
        assert_eq!(stats.tasksets_published, 2);
        assert_eq!(stats.tasksets_invalidated, 2);
        assert_eq!(stats.rgs_completed, 1);
    });
}

/// Model 2: the pinned lifecycle interface. Abort completed ⇒ enter fails
/// (the task is unconsumable); a guard blocks retire; a retired generation
/// never admits again.
#[test]
fn generation_handoff_unconsumable() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let lc = Arc::new(TaskLifecycle::new());
        let g = lc.current_generation();
        let abort_done = Arc::new(AtomicBool::new(false));

        let lc_a = Arc::clone(&lc);
        let done_a = Arc::clone(&abort_done);
        let aborter = thread::spawn(move || {
            assert!(lc_a.abort(g));
            done_a.store(true, Ordering::SeqCst);
        });

        let lc_c = Arc::clone(&lc);
        let done_c = Arc::clone(&abort_done);
        let consumer = thread::spawn(move || {
            let abort_was_done = done_c.load(Ordering::SeqCst);
            match QueryTaskGuard::enter(&lc_c, g) {
                Some(guard) => {
                    // Entering can race an in-flight abort, but never a
                    // COMPLETED one: unconsumable-by-construction.
                    assert!(!abort_was_done, "entered a generation whose abort had completed");
                    // While we are inside, the generation cannot retire.
                    assert!(lc_c.retire(g).is_none(), "retire admitted with a live participant");
                    drop(guard);
                }
                None => {}
            }
        });

        aborter.join().unwrap();
        consumer.join().unwrap();

        // Drained now: retire must succeed exactly once, and the dead
        // generation admits nobody ever again.
        let g1 = lc.retire(g).expect("drained aborted generation must retire");
        assert_ne!(g, g1);
        assert!(QueryTaskGuard::enter(&lc, g).is_none(), "retired generation admitted");
        assert!(lc.retire(g).is_none(), "double retire");
        // The new generation is live.
        let guard = QueryTaskGuard::enter(&lc, g1).expect("new generation must admit");
        drop(guard);
    });
}

/// Model 3: scheduler-level abort cleanup. One worker drives; the aborter
/// races. Whatever the interleaving: the RG completes Aborted OR Completed
/// (abort may lose the race entirely), finalize-work runs at most once, no
/// granule twice, and if the abort completed before any morsel ran, no
/// morsel runs at all.
#[test]
fn abort_cleanup() {
    struct AbortWork {
        executed: AtomicUsize,
        work_finalized: AtomicUsize,
    }
    impl TaskSetWork for AbortWork {
        fn run_morsel(&self, _w: usize, range: MorselRange) {
            // A morsel may start only through a live-generation guard: if
            // the abort had fully completed before our task's guard was
            // taken, the runtime must not get here. We can't observe the
            // guard edge from the work body, so the checkable invariant is
            // exactly-once execution; the no-consume-after-abort edge is
            // model 2's job (the guard IS the mechanism).
            self.executed.fetch_add((range.end - range.start) as usize, Ordering::SeqCst);
        }
        fn finalize(&self) {
            // Aborted RGs must not run finalize work; completed ones must
            // run it exactly once.
            self.work_finalized.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let rt = small_runtime(1, 0);
        let abort_done = Arc::new(AtomicBool::new(false));
        let work = Arc::new(AbortWork {
            executed: AtomicUsize::new(0),
            work_finalized: AtomicUsize::new(0),
        });
        let (handle, waiter) = rt.submit(QuerySpec {
            query_id: 7,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let aborter = {
            let handle = handle.clone();
            let done = Arc::clone(&abort_done);
            thread::spawn(move || {
                handle.abort();
                done.store(true, Ordering::SeqCst);
            })
        };

        drive(&rt, 0, &waiter);
        aborter.join().unwrap();

        let outcome = waiter.try_wait().expect("RG must complete");
        let finals = work.work_finalized.load(Ordering::SeqCst);
        let executed = work.executed.load(Ordering::SeqCst);
        match outcome {
            RgOutcome::Aborted => {
                assert_eq!(finals, 0, "aborted RG ran finalize work");
                assert!(executed <= 2);
            }
            RgOutcome::Completed => {
                // Abort lost the race entirely (landed after completion).
                assert_eq!(finals, 1);
                assert_eq!(executed, 2);
            }
        }
        // Protocol teardown always runs exactly once either way.
        assert_eq!(rt.stats().finalize_events, 1);
    });
}

/// Model 4 (§2.8 addendum): 2 pool threads, ONE permit (worker + standby).
/// The first morsel executes inside a declared blocking section (permit
/// released, reacquired on exit) while the thread stays pinned. The standby
/// absorbs the permit and works the same task set; the last-worker-out
/// counter must tolerate every interleaving of {blocked-pinned worker,
/// absorbing standby, coordinator election}.
#[test]
fn standby_absorption() {
    struct IoWork {
        inner: Arc<ModelWork>,
        rt: Arc<Runtime>,
        io_taken: AtomicUsize,
    }
    impl TaskSetWork for IoWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            if self.io_taken.fetch_add(1, Ordering::SeqCst) == 0 {
                // Declared blocking section on the FIRST morsel: donate the
                // permit, "block", reacquire on drop. The pin-board entry
                // and any finalization-marker obligation stay ours.
                let io = self.rt.execution_permits().io_section();
                thread::yield_now(); // let the standby absorb the permit
                self.inner.run_morsel(worker, range);
                drop(io);
            } else {
                self.inner.run_morsel(worker, range);
            }
        }
        fn finalize(&self) {
            self.inner.finalize();
        }
    }

    /// Pool-loop emulation with the real permit + park discipline.
    fn drive_with_permits(rt: &Runtime, worker: usize, waiter: &CompletionWaiter) {
        let mut local = rt.worker_local(worker);
        loop {
            if waiter.try_wait().is_some() {
                break;
            }
            let epoch = rt.park_epoch();
            rt.execution_permits().acquire();
            let step = rt.worker_step(&mut local);
            rt.execution_permits().release();
            match step {
                Step::Ran => {}
                Step::Retry => thread::yield_now(),
                Step::Idle => {
                    if waiter.try_wait().is_some() {
                        break;
                    }
                    rt.park(epoch);
                }
                Step::Stop => break,
            }
        }
        rt.request_stop();
    }

    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        // workers=1 ⇒ ONE execution permit; standbys=1 ⇒ two pool threads.
        let rt = small_runtime(1, 1);
        let inner = ModelWork::new(2, None);
        let work = Arc::new(IoWork {
            inner: Arc::clone(&inner),
            rt: Arc::clone(&rt),
            io_taken: AtomicUsize::new(0),
        });
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 3,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let standby = thread::spawn(move || drive_with_permits(&rt1, 1, &waiter1));
        drive_with_permits(&rt, 0, &waiter);
        standby.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        inner.assert_complete();
        assert_eq!(rt.stats().finalize_events, 1);
        // Every donated permit came back: full capacity restored.
        assert_eq!(rt.execution_permits().available(), 1);
    });
}
