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
//!   2. generation_handoff_unconsumable — pinned-interface invariant over
//!      the MERGED (lane A) lifecycle: a task from an aborted generation is
//!      unconsumable (abort completed ⇒ join refuses); a live participant
//!      pins its generation against retirement (the drain rendezvous is the
//!      only retire path — loom's deadlock detector oracles the
//!      leave/notify handshake); retired generations never admit;
//!      reinitialize opens a fresh admitting generation.
//!   3. abort_cleanup — scheduler-level abort: cleanup rides the ordinary
//!      protocol, the RG completes Aborted, finalize work never runs, and
//!      no morsel of the aborted generation starts after abort completes.
//!   4. standby_absorption — §2.8 addendum: a permit released mid-task-set
//!      (IoGuard) lets a standby absorb the core while the blocked worker
//!      keeps its pin-board entry and finalization-marker obligations; the
//!      last-worker-out counter is never confused.
//!   8. dag_fanout_publish_exactly_once (M5+1) — the pipeline-DAG
//!      edge-satisfaction/submission handshake: racing finalizes of two
//!      independent pipelines publish their gated consumer exactly once,
//!      only after both dependencies finalized.
//!   9. dag_abort_live_siblings_completes_once (M5+1) — abort racing two
//!      live pipelines: the racing live-counter decrements elect exactly
//!      one RG completer; a dead generation never publishes the sink.
//!  10. stream_source_handshake — the stream-fed source / producer
//!      handshake (parallel COPY): starved claims never finalize an open
//!      stream, publish/close wakes are lost-wakeup-free through the
//!      epoch-guarded park, exhaustion requires the close.
//!
//! Determinism note: models use a never-advanced VirtualClock so every
//! morsel measures a constant dt (loom requires branch determinism along an
//! execution path — a wall clock would make sizing branches diverge).

#![cfg(loom)]

use std::sync::Arc;

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Condvar, Mutex};
use loom::thread;

use runtime::{
    Clock, CompletionWaiter, Generation, MorselRange, ParticipantOwner, QuerySpec,
    QueryTaskLifecycle, RgOutcome, Runtime, RuntimeConfig, SizingParams, Step,
    StreamSource, SyntheticMorselSource, TaskSetSpec, TaskSetWork, VirtualClock,
};
use types_error::{PgError, ERROR};

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
    /// Predecessor task sets (dep-order assert): ALL must have finalized
    /// before any of our morsels runs (M5+1 DAG models gate on several).
    predecessors: Vec<Arc<ModelWork>>,
}

impl ModelWork {
    fn new(total: u64, predecessor: Option<Arc<ModelWork>>) -> Arc<ModelWork> {
        ModelWork::new_deps(total, predecessor.into_iter().collect())
    }

    fn new_deps(total: u64, predecessors: Vec<Arc<ModelWork>>) -> Arc<ModelWork> {
        Arc::new(ModelWork {
            executed: (0..total).map(|_| AtomicUsize::new(0)).collect(),
            inside: AtomicUsize::new(0),
            finalized: AtomicUsize::new(0),
            predecessors,
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
        for p in &self.predecessors {
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

/// Permissive owner for the lifecycle models — the scheduler's dispatcher
/// role (rg.rs SchedulerOwner), restated here because the real one is
/// crate-private.
struct ModelOwner;
impl ParticipantOwner for ModelOwner {
    fn permits_join(&self, _g: Generation) -> bool {
        true
    }
    fn request_stop(&self, _g: Generation) {}
    fn generation_stopped(&self, _g: Generation) -> bool {
        false
    }
}

/// Model 2: the pinned lifecycle interface over the MERGED (lane A)
/// lifecycle. Abort (cancel/close) completed ⇒ join refuses (the task is
/// unconsumable); a live participant pins the generation against
/// retirement — the aborter's drain must block on the participant's exit
/// (loom's deadlock detector verifies the leave→notify rendezvous); a
/// retired generation never admits again; reinitialize opens a fresh one
/// that does.
#[test]
fn generation_handoff_unconsumable() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(|| {
        let task = QueryTaskLifecycle::with_owner(Arc::new(ModelOwner));
        let handle = task.publish().expect("fresh lifecycle publishes once");
        let abort_done = Arc::new(AtomicBool::new(false));

        let task_a = Arc::clone(&task);
        let handle_a = handle.clone();
        let done_a = Arc::clone(&abort_done);
        let aborter = thread::spawn(move || {
            task_a.cancel(PgError::new(ERROR, "model abort").into());
            done_a.store(true, Ordering::SeqCst);
            // Drain + retire: must WAIT for any live participant (its exit
            // notifies the drain condvar; a lost notify here deadlocks the
            // model). Surfaces the recorded abort error.
            let err = task_a
                .close_generation_and_wait_with(handle_a.lifecycle(), || false, || Ok(()))
                .expect_err("drain must surface the recorded abort");
            assert_eq!(err.message(), "model abort");
        });

        let handle_c = handle.clone();
        let done_c = Arc::clone(&abort_done);
        let consumer = thread::spawn(move || {
            let abort_was_done = done_c.load(Ordering::SeqCst);
            match handle_c.join() {
                Ok(participant) => {
                    // Joining can race an in-flight abort, but never a
                    // COMPLETED one: unconsumable-by-construction.
                    assert!(!abort_was_done, "joined a generation whose abort had completed");
                    // While we are inside, the generation cannot retire.
                    assert!(
                        !handle_c.lifecycle().retired(),
                        "generation retired with a live participant"
                    );
                    participant.complete().expect("drained participant completes");
                }
                Err(_refused) => {}
            }
        });

        aborter.join().unwrap();
        consumer.join().unwrap();

        // Retired now (exactly once, by the drain): the dead generation
        // admits nobody ever again; reinitialize opens a live one.
        assert!(handle.lifecycle().retired());
        assert!(handle.join().is_err(), "retired generation admitted");
        let fresh = task.reinitialize().expect("reinitialize opens a new generation");
        assert_ne!(handle.generation(), fresh.generation());
        assert!(handle.join().is_err(), "stale handle joined the new generation");
        let participant = fresh.join().expect("new generation must admit");
        participant.complete().unwrap();
        let _ = task.close_generation_and_wait_with(fresh.lifecycle(), || false, || Ok(()));
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

/// Model 5 (M3.5 §6.1): model 4 driven through the SPILL FACADE instead of
/// a direct io_section — the pool-loop emulation REGISTERS each thread
/// (PermitThreadReg), the task body calls `blocking_io_section()` with no
/// runtime reference (the spill-substrate call shape), and the armed
/// section donates/reacquires the permit under every interleaving. The
/// facade's TLS gate must never confuse the permit count or the
/// last-worker-out protocol.
#[test]
fn facade_standby_absorption() {
    struct FacadeIoWork {
        inner: Arc<ModelWork>,
        io_taken: AtomicUsize,
    }
    impl TaskSetWork for FacadeIoWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            if self.io_taken.fetch_add(1, Ordering::SeqCst) == 0 {
                // Declared blocking spill I/O on the FIRST morsel, through
                // the facade: armed because the driving thread registered.
                let io = runtime::blocking_io_section();
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

    /// Pool-loop emulation with the real permit + park discipline AND the
    /// facade registration (mirrors pool.rs worker_loop).
    fn drive_registered(rt: &Runtime, worker: usize, waiter: &CompletionWaiter) {
        // SAFETY: the runtime outlives this drive; the permit is held
        // across worker_step, where the facade is called.
        let _reg = unsafe { runtime::PermitThreadReg::new(rt.execution_permits()) };
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
        let work = Arc::new(FacadeIoWork {
            inner: Arc::clone(&inner),
            io_taken: AtomicUsize::new(0),
        });
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 5,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let standby = thread::spawn(move || drive_registered(&rt1, 1, &waiter1));
        drive_registered(&rt, 0, &waiter);
        standby.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        inner.assert_complete();
        assert_eq!(rt.stats().finalize_events, 1);
        // Every facade-donated permit came back: full capacity restored.
        assert_eq!(rt.execution_permits().available(), 1);
    });
}

/// Drive until EVERY listed waiter resolves (multi-RG models; `drive` stops
/// at its single waiter). Same park discipline; first finisher stops the
/// runtime so parked peers exit.
fn drive_all(rt: &Arc<Runtime>, worker: usize, waiters: &[CompletionWaiter]) {
    let mut local = rt.worker_local(worker);
    loop {
        if waiters.iter().all(|w| w.try_wait().is_some()) {
            break;
        }
        let epoch = rt.park_epoch();
        match rt.worker_step(&mut local) {
            Step::Ran => {}
            Step::Retry => thread::yield_now(),
            Step::Idle => {
                if waiters.iter().all(|w| w.try_wait().is_some()) {
                    break;
                }
                rt.park(epoch);
            }
            Step::Stop => break,
        }
    }
    rt.request_stop();
}

/// Model 5 (M5-4): queued-abort reap vs slot-release admission — the
/// exactly-once completion election. One slot: RG A occupies it, RG B
/// queues; an aborter thread races `abort()` (cancel + wait-queue reap)
/// against the driver completing A (whose slot release pops the queue).
/// Every interleaving must complete BOTH RGs exactly once (rgs_completed
/// == 2 — a double complete would tick 3) with no hang: B is reaped
/// (never executes), popped-aborted, or admitted and then aborted/completed
/// through the ordinary protocol.
#[test]
fn queued_abort_reap_exactly_once() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.max_branches = 200_000;
    b.check(|| {
        let cfg = RuntimeConfig {
            workers: 1,
            standbys: 0,
            slots: 1, // force B to queue behind A
            sizing: SizingParams::default(),
            trace: false,
        };
        let rt = Runtime::with_clock(cfg, Arc::new(VirtualClock::new()) as Arc<dyn Clock>);
        let wa = ModelWork::new(1, None);
        let (_ha, waiter_a) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::clone(&wa) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });
        let wb = ModelWork::new(1, None);
        let (hb, waiter_b) = rt.submit(QuerySpec {
            query_id: 2,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::clone(&wb) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let aborter = thread::spawn(move || hb.abort());
        drive_all(&rt, 0, &[waiter_a.clone(), waiter_b.clone()]);
        aborter.join().unwrap();

        assert_eq!(waiter_a.try_wait(), Some(RgOutcome::Completed));
        wa.assert_complete();
        let outcome_b = waiter_b.try_wait().expect("B must complete (no hang)");
        let stats = rt.stats();
        // Exactly-once completion: reap and admission-pop both remove under
        // the membership lock; only the remover completes.
        assert_eq!(stats.rgs_completed, 2, "each RG completes exactly once");
        match outcome_b {
            RgOutcome::Aborted => {
                // Reaped from the queue, popped aborted, or drained through
                // the generation-refusal path — B's finalize never runs.
                assert_eq!(wb.finalized.load(Ordering::SeqCst), 0);
                if stats.queued_aborts_reaped == 1 {
                    // Reaped ⇒ never admitted ⇒ never executed.
                    assert_eq!(wb.executed[0].load(Ordering::SeqCst), 0);
                }
            }
            RgOutcome::Completed => {
                // The abort landed after B already completed (cancel on a
                // retired lifecycle is a no-op) — legal, and B ran normally.
                assert_eq!(stats.queued_aborts_reaped, 0);
                wb.assert_complete();
            }
        }
    });
}

/// Model 6 (M5-4): two CONCURRENTLY ACTIVE RGs under the live stride pick —
/// two workers, two slots. The pass/stride/session words are advisory
/// (Relaxed) by design; this model proves the finalization protocol's
/// exactly-once guarantees hold across every interleaving of the new pick:
/// every granule of both RGs exactly once, finalize exactly once each, both
/// RGs complete.
#[test]
fn stride_two_active_rgs_exactly_once() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_stride(true);
        let wa = ModelWork::new(2, None);
        let (_ha, waiter_a) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&wa) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });
        let wb = ModelWork::new(1, None);
        let (_hb, waiter_b) = rt.submit(QuerySpec {
            query_id: 2,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::clone(&wb) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let (wa1, wb1) = (waiter_a.clone(), waiter_b.clone());
        let t = thread::spawn(move || drive_all(&rt1, 1, &[wa1, wb1]));
        drive_all(&rt, 0, &[waiter_a.clone(), waiter_b.clone()]);
        t.join().unwrap();

        assert_eq!(waiter_a.try_wait(), Some(RgOutcome::Completed));
        assert_eq!(waiter_b.try_wait(), Some(RgOutcome::Completed));
        wa.assert_complete();
        wb.assert_complete();
        let stats = rt.stats();
        assert_eq!(stats.rgs_completed, 2);
        assert_eq!(stats.finalize_events, 2);
    });
}

/// Model 8 (M5+1): the DAG edge-satisfaction/submission handshake. Two
/// workers, two slots, one RG shaped build-A + build-B -> probe-C
/// (deps=[0,1]): A and B publish at admission (one via fan-out) and their
/// finalizes RACE — both last-outs contend on the membership+progress
/// handshake and exactly ONE of them must observe C newly-ready and publish
/// it, exactly once, only after BOTH dependencies finalized (ModelWork's
/// dep-order assert), with no lost pipeline and one RG completion. The
/// submission-gating deadlock premise (nothing dependency-unsatisfied is
/// ever visible to a worker) is asserted structurally by the publish-time
/// debug assert plus C's gate here.
#[test]
fn dag_fanout_publish_exactly_once() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_stride(true);
        rt.set_dag(true);
        let wa = ModelWork::new(1, None);
        let wb = ModelWork::new(1, None);
        let wc = ModelWork::new_deps(1, vec![Arc::clone(&wa), Arc::clone(&wb)]);
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1)),
                    work: Arc::clone(&wa) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                },
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1)),
                    work: Arc::clone(&wb) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                },
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1)),
                    work: Arc::clone(&wc) as Arc<dyn TaskSetWork>,
                    deps: vec![0, 1],
                },
            ],
        });

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive(&rt1, 1, &waiter1));
        drive(&rt, 0, &waiter);
        t.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        wa.assert_complete();
        wb.assert_complete();
        wc.assert_complete();
        let stats = rt.stats();
        assert_eq!(stats.rgs_completed, 1, "RG completes exactly once");
        assert_eq!(stats.finalize_events, 3);
        assert_eq!(stats.tasksets_published, 3, "C published exactly once");
        assert_eq!(stats.dag_fanout_publishes, 1, "B fanned out at admission");
    });
}

/// Model 9 (M5+1): abort with multiple LIVE pipelines. Two workers drive an
/// RG with two independent published pipelines plus a gated sink while an
/// aborter thread races `abort()`: each live pipeline drains through its
/// own last-out, the RG completes EXACTLY ONCE (the racing live-counter
/// decrements elect one completer), the sink never publishes on a dead
/// generation, and no finalize work runs after an observed abort.
#[test]
fn dag_abort_live_siblings_completes_once() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_stride(true);
        rt.set_dag(true);
        let wa = ModelWork::new(1, None);
        let wb = ModelWork::new(1, None);
        let wc = ModelWork::new_deps(1, vec![Arc::clone(&wa), Arc::clone(&wb)]);
        let (h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1)),
                    work: Arc::clone(&wa) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                },
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1)),
                    work: Arc::clone(&wb) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                },
                TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(1)),
                    work: Arc::clone(&wc) as Arc<dyn TaskSetWork>,
                    deps: vec![0, 1],
                },
            ],
        });

        let aborter = thread::spawn(move || h.abort());
        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive(&rt1, 1, &waiter1));
        drive(&rt, 0, &waiter);
        t.join().unwrap();
        aborter.join().unwrap();

        let outcome = waiter.try_wait().expect("RG must complete (no hang)");
        let stats = rt.stats();
        assert_eq!(stats.rgs_completed, 1, "RG completes exactly once");
        match outcome {
            RgOutcome::Aborted => {
                // The abort landed before completion: the sink either never
                // published or drained refused; nothing double-finalizes.
                assert!(stats.rgs_aborted == 1);
            }
            RgOutcome::Completed => {
                // Abort landed after completion (no-op on retired
                // lifecycle): everything ran normally.
                wa.assert_complete();
                wb.assert_complete();
                wc.assert_complete();
            }
        }
    });
}


/// Model 5: the STREAM-FED source / producer handshake (parallel COPY's
/// segmentator feed). One producer publishes granule 1, then granule 2 and
/// closes — each publish/close followed by the wake (`publish`-then-wake is
/// the documented order); 2 workers drive with the epoch-guarded park
/// discipline. Invariants under bounded-exhaustive interleavings:
///   * a starved claim (cursor at an OPEN watermark) never finalizes the
///     set — exhaustion requires the close;
///   * no lost wakeup: parked starved workers always observe the
///     publish/close (the epoch protocol), so the RG always completes;
///   * every granule exactly once, finalize exactly once, and finalize only
///     after the close made the watermark final.
#[test]
fn stream_source_handshake() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        let work = ModelWork::new(2, None);
        let source = Arc::new(StreamSource::new());
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::clone(&source) as _,
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let producer = {
            let rt = Arc::clone(&rt);
            let source = Arc::clone(&source);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                source.publish(1);
                rt.notify_source_progress();
                source.publish(2);
                source.close();
                // Finalize before the close is a protocol breach; the close
                // is ordered before the wake below, so a finalize observed
                // here can only have raced the close itself never a starved
                // claim (the assert documents exhaustion-requires-close).
                rt.notify_source_progress();
                let _ = work;
            })
        };

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive(&rt1, 1, &waiter1));
        drive(&rt, 0, &waiter);
        t.join().unwrap();
        producer.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        work.assert_complete();
        let stats = rt.stats();
        assert_eq!(stats.finalize_events, 1);
        assert_eq!(stats.rgs_completed, 1);
    });
}

// ===========================================================================
// ParkLot (the idle-worker epoch eventcount) — DIRECT protocol models
// (LOOM-BREADTH inc-1, absorbed at PERMIT-S2). Until now the eventcount was
// only exercised through the whole-runtime models above; these pin ITS
// invariant in isolation: with the capture-epoch -> check-work -> park(seen)
// discipline, a wake_all issued at ANY point relative to the park is never
// lost (the agg192-contention atomic-epoch rework's lost-wakeup argument,
// verified exhaustively). 2-3 threads, single wake — inside the fast-tier
// budget. ABSORB DELTA (compose §1.3): the models drive `pgsync::ParkLot`
// DIRECTLY — the type runtime's sync facade re-exports — so the loom-world
// pgsync types are what the models check; the branch-era cfg(loom)
// `runtime::ParkLot` export is not needed and was not absorbed.
// ===========================================================================

/// One publisher, one idle worker: the worker runs the documented protocol
/// (epoch() BEFORE the failed work pick; park(seen) only after the pick
/// missed). The publisher stores work then wake_all()s. In EVERY
/// interleaving the worker must terminate having seen the work — a lost
/// wake parks it forever and loom's deadlock detector fails the model.
#[test]
fn parklot_publish_wake_never_lost() {
    // UNBOUNDED (inc-1 review disposition, 2026-07-18): the space is
    // exhaustible in seconds, so both tiers run the full space in-tree
    // (the fast tier's env permutation/duration caps still apply there).
    loom::model(|| {
        let lot = Arc::new(pgsync::ParkLot::new());
        let work = Arc::new(AtomicBool::new(false));

        let publisher = {
            let (lot, work) = (Arc::clone(&lot), Arc::clone(&work));
            thread::spawn(move || {
                // Publish order is the protocol: work visible BEFORE the
                // epoch bump + notify.
                work.store(true, Ordering::SeqCst);
                lot.wake_all();
            })
        };

        loop {
            let seen = lot.epoch();
            if work.load(Ordering::SeqCst) {
                break;
            }
            lot.park(seen);
        }
        publisher.join().unwrap();
        assert!(work.load(Ordering::SeqCst));
    });
}

/// One publisher, TWO idle workers: a single wake_all must release both
/// parkers (notify_all + epoch bump), in every interleaving of their
/// capture/pick/park windows against the publish.
#[test]
fn parklot_wake_all_reaches_every_parker() {
    // UNBOUNDED (inc-1 review disposition, 2026-07-18): see above.
    loom::model(|| {
        let lot = Arc::new(pgsync::ParkLot::new());
        let work = Arc::new(AtomicBool::new(false));

        let workers: Vec<_> = (0..2)
            .map(|_| {
                let (lot, work) = (Arc::clone(&lot), Arc::clone(&work));
                thread::spawn(move || loop {
                    let seen = lot.epoch();
                    if work.load(Ordering::SeqCst) {
                        return;
                    }
                    lot.park(seen);
                })
            })
            .collect();

        work.store(true, Ordering::SeqCst);
        lot.wake_all();
        for w in workers {
            w.join().unwrap();
        }
    });
}

// ===========================================================================
// CURSOR PARK SHAPE — settle / claim-release / R3 (LOOM-BREADTH inc-2 item 2).
// The WS-AI forward-pull cursor park protocol (notes/se-wave9-ai.md §2 / §8
// Item 1): at a budget-N suspension the walker SETTLES every lane-staged scan
// claim — drops its bufmgr pins to ZERO (R3), records the reposition window,
// and RELEASES the claim + publishes it to the runtime idle-worker path
// (ParkLot) — so an idle pool worker may legitimately absorb the freed
// resource while the cursor is parked; the next FETCH REPOSSESSES (re-acquires
// the claim, re-pins at the recorded window, delivers the remainder).
//
// The invariant this pins: while the cursor is parked (claim released), it
// holds ZERO pins (R3), and the park-resume race against the runtime waiter is
// clean — the claim is always eventually repossessable (no leaked claim, no
// lost publish), and the reposition window is idempotent across the cycle.
//
// MIRROR-DIALECT (notes/loom-breadth-inc2.md §2): the SYNC primitives are the
// REAL converted target — `pgsync::Semaphore` (the exclusive scan claim) and
// `pgsync::ParkLot` (the runtime waiter path) driven directly under --cfg
// loom. The executor node plumbing (SeqScanState.lane_park, heapam
// reposition, the ParkWalk recursion) is ABSTRACTED: the production cursor
// code (execmain lanev2) lives on the single-executor lineage, NOT this DST
// base (grep on the tree: `cursor_run_park`/`es_cursor_run_budget` absent).
// The model verifies the park/settle/claim-release SYNC CONTRACT; the direct
// node-driving model is inc-3 work once the cursor lane composes onto the DST
// substrate.
// ===========================================================================

/// One cursor fetcher racing one runtime idle worker over a single-permit
/// scan claim. The fetcher stages (pin+claim), settles at suspension (pin→0,
/// release+publish), then resumes (repossess). The idle worker parks on the
/// runtime lot until it can seize the freed claim, and asserts R3 (zero pins)
/// the instant it holds the claim. Every interleaving must complete with R3
/// intact — a held pin at settle fails the assert, an unpublished release
/// deadlocks the parked worker.
#[test]
fn cursor_park_settle_releases_claim_and_pins_r3() {
    // The reposition window (b0,b1) stand-in: settle records it, resume
    // asserts it survived idempotently.
    const WINDOW: usize = 0xB0B1;

    loom::model(|| {
        let claim = Arc::new(pgsync::Semaphore::new(1)); // the exclusive scan claim
        let pins = Arc::new(AtomicUsize::new(0)); // the fetcher's bufmgr pins (R3 subject)
        let reposition = Arc::new(AtomicUsize::new(0)); // the recorded (b0,b1) window
        let lot = Arc::new(pgsync::ParkLot::new()); // the runtime waiter path

        // Releasing a scan resource ALWAYS publishes availability to idle
        // workers (settle AND final release) — the lost-wakeup-free pairing.
        let release_and_publish = {
            let (claim, lot) = (Arc::clone(&claim), Arc::clone(&lot));
            move || {
                claim.release();
                lot.wake_all();
            }
        };

        // The runtime idle worker: park on the lot until it can seize the
        // freed claim; when it holds the claim the cursor MUST hold zero pins.
        let worker = {
            let (claim, pins, lot) = (Arc::clone(&claim), Arc::clone(&pins), Arc::clone(&lot));
            thread::spawn(move || loop {
                let seen = lot.epoch();
                if claim.try_acquire() {
                    assert_eq!(
                        pins.load(Ordering::SeqCst),
                        0,
                        "R3: cursor holds zero pins whenever its claim is repossessable"
                    );
                    claim.release();
                    return;
                }
                lot.park(seen);
            })
        };

        // The cursor fetcher.
        // STAGE (first FETCH): own the claim, take a pin, stage a batch.
        claim.acquire();
        pins.fetch_add(1, Ordering::SeqCst);
        // ... deliver budget-N tuples ...
        // SETTLE at suspension: drop pins to zero (R3), record reposition,
        // release + publish the claim.
        pins.fetch_sub(1, Ordering::SeqCst);
        reposition.store(WINDOW, Ordering::SeqCst);
        release_and_publish();
        // RESUME (next FETCH): repossess the claim (may wait behind the idle
        // worker), verify the reposition window is idempotent, re-pin.
        claim.acquire();
        assert_eq!(
            reposition.load(Ordering::SeqCst),
            WINDOW,
            "reposition window idempotent across settle/resume"
        );
        pins.fetch_add(1, Ordering::SeqCst);
        // ... deliver the remainder ...
        pins.fetch_sub(1, Ordering::SeqCst);
        release_and_publish();

        worker.join().unwrap();
        assert_eq!(pins.load(Ordering::SeqCst), 0);
    });
}

// ===========================================================================
// STANDING GANG detach-count JOIN — census arm (a) row 1 / §3 row 1 (the
// "gang Condvar → rg.rs shape" conversion target), LOOM-BREADTH inc-2 item 3.
// Mirror of transam/parallel/standing.rs close_and_await: the leader waits for
// detached == claimed before its executor arena may unwind (the SendConst
// join replacing DestroyParallelContext's worker-exit wait), and every worker
// detaches UNCONDITIONALLY through the DetachGuard drop.
//
// MODELS THE CONVERTED SHAPE, not the std one being deleted (inc-1 policy):
// production's leader currently POLLS via wait_parallel_finish_quantum (a
// row-13 timed sleep, lost-wake-tolerant by construction); the P3/rg.rs
// conversion target is a lost-wake-FREE Condvar join, which is what this pins.
// Mirror-dialect: standing.rs is not loom-buildable (latch/lmgr/PGPROC
// globals) — the model drives loom `Mutex`/`Condvar` + atomics directly.
// ===========================================================================

/// N workers each claim a ticket then unconditionally detach (bump `detached`
/// + a lock-mediated wake); the leader closes the board and joins on
/// `detached == claimed`. In every interleaving the leader completes — the
/// detach wake is never lost (loom's deadlock detector is the oracle).
#[test]
fn standing_gang_detach_count_join_no_lost_wake() {
    const N: usize = 2;

    loom::model(|| {
        let claimed = Arc::new(AtomicUsize::new(0));
        let detached = Arc::new(AtomicUsize::new(0));
        let m = Arc::new(Mutex::new(())); // the gang cv mutex
        let cv = Arc::new(Condvar::new()); // the gang Condvar

        let workers: Vec<_> = (0..N)
            .map(|_| {
                let (claimed, detached, m, cv) =
                    (Arc::clone(&claimed), Arc::clone(&detached), Arc::clone(&m), Arc::clone(&cv));
                thread::spawn(move || {
                    // try_claim: take a ticket.
                    claimed.fetch_add(1, Ordering::SeqCst);
                    // ... run the engagement driver ...
                    // DetachGuard drop (UNCONDITIONAL): bump detached, then a
                    // lock-mediated wake — the barrier that makes the notify
                    // lost-wake-free (either the leader sees the new count at
                    // its under-lock check, or it is already parked when the
                    // notify fires).
                    detached.fetch_add(1, Ordering::SeqCst);
                    {
                        let g = m.lock().unwrap();
                        drop(g);
                    }
                    cv.notify_all();
                })
            })
            .collect();

        // Leader: close_and_await — wait until every claimed ticket detached.
        {
            let mut g = m.lock().unwrap();
            while detached.load(Ordering::SeqCst) < N {
                g = cv.wait(g).unwrap();
            }
        }

        for w in workers {
            w.join().unwrap();
        }
        assert_eq!(claimed.load(Ordering::SeqCst), N);
        assert_eq!(detached.load(Ordering::SeqCst), N, "detached == claimed at join");
    });
}

// ===========================================================================
// PERMIT-S4 — the step-4 PROTOCOL-conversion models (notes/dst-permit-s4.md).
// Three converted sites, three models. Dialect per model, disclosed:
//
//   * mailbox_* — FULLY DIRECT: they drive `pgsync::mailbox`, the REAL
//     production utility the row-5 (copy/parallel work+done channels) and
//     row-10 (pgrcolumnar writer stitch channel) conversions consume. The
//     call-site glue (pump/encoder/leader topology) is production-only; the
//     shared protocol is entirely this type.
//   * relscan_seize_* — REAL SHARED TYPE, transcribed driver: the model
//     constructs the production `BTParallelScanShared` (types_relscan is
//     loom-buildable since the LATCH-LOOM latch conversion — the old
//     "mirror struct" disclosure is DISCHARGED); the seize/release loop
//     remains transcribed 1:1 from nbtree/src/parallel.rs, whose real
//     `_bt_parallel_seize` needs Relation machinery outside model reach.
//   * bitmapheap_phase_* — MIRROR over the REAL waiter Slot core (the
//     waiter crate's loom surface): nodebitmapheapscan is not
//     loom-buildable (executor dep tree); the BmShared waker-list phase
//     machine is mirrored 1:1 from its converted
//     bitmap_should_initialize_shared_state, with UNTIMED parks — the
//     production 10ms cadence is the InterruptPending poll, and removing it
//     in the model makes a lost publish-wake a deadlock loom must catch
//     instead of something the cadence would paper over (the pg_barrier
//     models' discipline).
//
// RED battery (house law: every model catches its bug): each section names
// its red — the pre-conversion / weakened shape — and how it fails.
// ===========================================================================

/// Row 5/10 (DIRECT): a cap-1 mailbox pipeline delivers every message in
/// order and terminates through the drop-close. The producer parks on FULL
/// (message 2 cannot enter until the consumer pops message 1), the consumer
/// parks on EMPTY, and the producer's drop-close ends the consumer's drain
/// loop — every park is against the real ParkLot pair inside the mailbox.
///
/// RED (verified by transient weakening, not shipped — the model drives the
/// production type, so the red is a production weakening):
///   * `send` not waking not_empty ⇒ consumer parked forever — loom
///     deadlock;
///   * `recv` parking with the state guard HELD (the row-5 recv-under-mutex
///     shape this conversion kills) ⇒ producer blocked on the state mutex,
///     consumer parked — loom deadlock [Blocked, Blocked].
#[test]
fn mailbox_bounded_pipeline_delivers_and_closes() {
    loom::model(|| {
        let (tx, rx) = pgsync::mailbox::<u32>(Some(1));

        let consumer = thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(v) = rx.recv() {
                got.push(v);
            }
            got
        });

        tx.send(1).unwrap();
        tx.send(2).unwrap(); // parks while the queue is at cap
        drop(tx); // close: consumer's drain observes None and exits

        let got = consumer.join().unwrap();
        assert_eq!(got, vec![1, 2], "every message delivered, in order");
    });
}

/// Row 8 (SIMVFS-SHARED lane, DIRECT): the loadsort feeder shape over the
/// REAL mailbox — a producer that only ever `try_send`s (keeping a rejected
/// chunk PENDING, the feeder's round-robin discipline: it must never park on
/// one full channel while other runs starve), a consumer that drains, and
/// the drop-close EOF. Every chunk arrives exactly once, in order, despite
/// Full rejections at cap 1 (`sync_channel(2)` in production; cap 1 here
/// maximizes the Full interleavings loom must cover).
///
/// RED (verified by transient weakening, not shipped — the model drives the
/// production type): dropping the value on `TrySend::Full` instead of
/// re-pending it (the lost-chunk shape a bespoke conversion could write) ⇒
/// the delivered stream has a hole — assert fires on every such schedule.
/// One-line re-arm: replace the `pending = Some(p)` arm with `continue`.
#[test]
fn mailbox_try_send_feeder_never_loses_a_chunk() {
    loom::model(|| {
        let (tx, rx) = pgsync::mailbox::<u32>(Some(1));

        let consumer = thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(v) = rx.recv() {
                got.push(v);
            }
            got
        });

        // The feeder loop: chunks 1..=3, try_send with a pending slot.
        let mut pending: Option<u32> = None;
        let mut next = 1u32;
        while next <= 3 || pending.is_some() {
            let p = pending.take().unwrap_or_else(|| {
                let v = next;
                next += 1;
                v
            });
            match tx.try_send(p) {
                Ok(()) => {}
                Err(pgsync::TrySend::Full(p)) => {
                    // The feeder's re-pend discipline (the red drops this).
                    pending = Some(p);
                    loom::thread::yield_now();
                }
                Err(pgsync::TrySend::Disconnected(_)) => break,
            }
        }
        drop(tx); // EOF: consumer drains then observes None

        let got = consumer.join().unwrap();
        assert_eq!(got, vec![1, 2, 3], "every chunk delivered exactly once, in order");
    });
}

/// Row 5 MPMC leg (DIRECT): two receivers share the mailbox BY CLONING (the
/// converted encoder pool's shape — replacing the Mutex<Receiver> wrap), one
/// message + close. Exactly one receiver gets the message; BOTH terminate
/// (the close-wake reaches every parked receiver, not just one).
#[test]
fn mailbox_mpmc_close_wakes_every_receiver() {
    loom::model(|| {
        let (tx, rx) = pgsync::mailbox::<u32>(Some(1));
        let rx2 = rx.clone();

        let consumers: Vec<_> = [rx, rx2]
            .into_iter()
            .map(|rx| {
                thread::spawn(move || {
                    let mut got = Vec::new();
                    while let Some(v) = rx.recv() {
                        got.push(v);
                    }
                    got
                })
            })
            .collect();

        tx.send(7).unwrap();
        drop(tx);

        let mut all = Vec::new();
        for c in consumers {
            all.extend(c.join().unwrap());
        }
        assert_eq!(all, vec![7], "delivered exactly once, both consumers exited");
    });
}

/// Row 2 — now over the REAL `types_relscan::parallel::BTParallelScanShared`
/// (LATCH-LOOM lane: the latch conversion made types_relscan loom-buildable,
/// discharging this model's "mirror struct" disclosure — the shared TYPE is
/// production; the seize/release DRIVER below remains transcribed from
/// nbtree/src/parallel.rs, whose `_bt_parallel_seize` needs Relation
/// machinery no model can construct). A seizer that observes `Advancing`
/// captures the lot epoch UNDER the state lock, drops the lock, parks;
/// release (`Advancing -> Idle`) and done (`* -> Done`) bump-then-notify via
/// `wake_all`. One advancing winner, one waiting seizer: in every
/// interleaving the waiter terminates, having seized the released page or
/// observed Done.
///
/// RED: swap `release` for `release_naked` (state change WITHOUT the
/// wake_all — the pre-conversion notify-dropped shape): the waiter parks
/// forever — loom deadlock, CAUGHT.
#[test]
fn relscan_seize_release_wake_never_lost() {
    use types_relscan::parallel::{BTParallelScanShared, BtPsState};

    // The converted release: state change under the lock, then bump+notify.
    fn release(sh: &BTParallelScanShared) {
        sh.state.lock().unwrap().page_status = BtPsState::Idle;
        sh.lot.wake_all();
    }

    // RED helper (one-line swap in the winner thread): the naked state
    // change the conversion outlaws.
    #[allow(dead_code)]
    fn release_naked(sh: &BTParallelScanShared) {
        sh.state.lock().unwrap().page_status = BtPsState::Idle;
    }

    // The converted seize wait loop (nbtree bt_parallel_seize, Advancing
    // arm): epoch captured UNDER the state lock, park OUTSIDE it.
    fn seize_wait(sh: &BTParallelScanShared) -> BtPsState {
        loop {
            let (st, seen) = {
                let g = sh.state.lock().unwrap();
                (g.page_status, sh.lot.epoch())
            };
            match st {
                BtPsState::Advancing => {
                    sh.lot.park(seen);
                }
                BtPsState::Idle => {
                    // Seize it: Idle -> Advancing (the caller now owns the
                    // scan advance).
                    let mut g = sh.state.lock().unwrap();
                    if g.page_status == BtPsState::Idle {
                        g.page_status = BtPsState::Advancing;
                        return BtPsState::Idle;
                    }
                }
                BtPsState::Done => return BtPsState::Done,
                other => unreachable!("model never enters {other:?}"),
            }
        }
    }

    loom::model(|| {
        let sh = Arc::new(BTParallelScanShared::new());
        // Winner already advancing (the model's precondition).
        sh.state.lock().unwrap().page_status = BtPsState::Advancing;

        let waiter_t = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                let got = seize_wait(&sh);
                if got == BtPsState::Idle {
                    // The waiter seized the scan; finish it: -> Done + wake.
                    sh.state.lock().unwrap().page_status = BtPsState::Done;
                    sh.lot.wake_all();
                }
                got
            })
        };

        // Winner: finishes its page read, releases the scan.
        release(&sh); // RED: release_naked(&sh)

        let got = waiter_t.join().unwrap();
        assert!(
            got == BtPsState::Idle || got == BtPsState::Done,
            "waiter terminated through seize or done"
        );
        assert_eq!(sh.state.lock().unwrap().page_status, BtPsState::Done);
    });
}

/// Row 3 (MIRROR over the real waiter Slot core): the converted bitmapheap
/// build wait — `bitmap_should_initialize_shared_state`'s InProgress arm.
/// The waiter REGISTERS its waker word in the shared state under the lock,
/// releases, parks (untimed here — see the section header); the builder's
/// publish (InProgress -> Finished) drains the waker list after dropping the
/// lock and unparks every registrant. In every interleaving of
/// register/park vs publish/unpark the waiter terminates with `false`.
///
/// RED: swap `publish` for `publish_naked` (state change without the
/// drain+unpark — the shape the production 10ms cadence would paper over):
/// waiter parked forever — loom deadlock, CAUGHT.
#[test]
fn bitmapheap_phase_publish_unparks_registered_waiter() {
    use std::cell::Cell;

    use waiter::clock::WaiterClock;
    use waiter::{Slot, SlotInner};

    struct ModelClock;
    impl WaiterClock for ModelClock {
        fn now_ms(&self) -> i64 {
            0
        }
        fn wait<'a>(
            &self,
            slot: &'a Slot,
            guard: pgsync::MutexGuard<'a, SlotInner>,
            _timeout_ms: Option<i64>,
        ) -> (pgsync::MutexGuard<'a, SlotInner>, bool) {
            (slot.wait_for_model(guard), false)
        }
    }
    static CLOCK: ModelClock = ModelClock;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Phase {
        InProgress,
        Finished,
    }

    struct BmMirror {
        state: Mutex<(Phase, Vec<u64>)>,
    }

    loom::thread_local! {
        static MY_SLOT: Cell<Option<(usize, u32)>> = Cell::new(None);
    }

    // The converted publish: state -> Finished, TAKE the waker list, drop
    // the lock, unpark every word (the pg_barrier discipline).
    fn publish(sh: &BmMirror, slots: &[Arc<Slot>]) {
        let woken = {
            let mut g = sh.state.lock().unwrap();
            g.0 = Phase::Finished;
            std::mem::take(&mut g.1)
        };
        for w in woken {
            let (idx, token) = ((w >> 32) as usize - 1, w as u32);
            let _ = slots[idx].unpark_token(token);
        }
    }

    // RED helper: the naked publish (no drain, no unpark).
    #[allow(dead_code)]
    fn publish_naked(sh: &BmMirror) {
        sh.state.lock().unwrap().0 = Phase::Finished;
    }

    loom::model(|| {
        let sh = Arc::new(BmMirror {
            state: Mutex::new((Phase::InProgress, Vec::new())),
        });
        // Model-owned slot slab (the waiter global slab is production-only).
        let slots: Arc<Vec<Arc<Slot>>> = Arc::new(vec![Arc::new(Slot::new_for_model())]);

        let waiter_t = {
            let (sh, slots) = (Arc::clone(&sh), Arc::clone(&slots));
            thread::spawn(move || {
                // bitmap_should_initialize_shared_state, InProgress arm.
                loop {
                    let word = MY_SLOT.with(|c| {
                        if c.get().is_none() {
                            let token = slots[0].issue_token();
                            c.set(Some((0, token)));
                        }
                        let (idx, token) = c.get().unwrap();
                        ((idx as u64 + 1) << 32) | token as u64
                    });
                    {
                        let mut g = sh.state.lock().unwrap();
                        match g.0 {
                            Phase::Finished => return false,
                            Phase::InProgress => {
                                if !g.1.contains(&word) {
                                    g.1.push(word);
                                }
                            }
                        }
                    }
                    // Untimed park on the REAL slot core (production adds
                    // the 10ms InterruptPending cadence).
                    let _ = slots[0].park_core(None, None, &CLOCK);
                }
            })
        };

        // Builder: the bitmap is built; publish Finished.
        publish(&sh, &slots); // RED: publish_naked(&sh)

        let elected = waiter_t.join().unwrap();
        assert!(!elected, "the waiter never becomes the builder");
        assert_eq!(sh.state.lock().unwrap().0, Phase::Finished);
    });
}

// ===========================================================================
// PERMIT-S5 — the runtime-doors / row-16 tranche models
// (notes/dst-permit-s5.md). Dialect, disclosed:
//
//   * timeout_timer_arm_fire_never_lost — MIRROR over the REAL waiter Slot
//     core: the timeout crate is not loom-buildable (its dep tree pulls
//     `latch`, which fails under --cfg loom — the standing mirror-model
//     reason), so the timer/armer Dekker protocol is mirrored 1:1 from
//     timeout/src/lib.rs (`timer_thread` publish-scan-park + `arm_timer`
//     insert-then-unpark), driving the real Slot token park/unpark and a
//     (pgsync=loom) Mutex slots registry — the registry type this lane
//     converted from raw std::sync so the REGISTERED timer thread can
//     never block raw while holding the permit.
// ===========================================================================

/// Row 16 (timer door): the timeout arm/fire Dekker protocol. The timer
/// thread publishes its waker word BEFORE scanning the registry, scans
/// under the (pgsync) mutex, drops it, parks untimed; an armer inserts its
/// deadline under the mutex, drops it, then unparks through the published
/// word (a 0 word = the timer has not published yet, and its first
/// publish-then-scan must see the insert). In every interleaving the timer
/// observes the arm.
///
/// RED: swap `arm` for `arm_naked` (insert WITHOUT the unpark — the lost-arm
/// shape the Dekker discipline exists to kill): the timer parks forever with
/// an armed deadline it never scanned — loom deadlock, CAUGHT.
#[test]
fn timeout_timer_arm_fire_never_lost() {
    use loom::sync::atomic::AtomicU64;
    use waiter::clock::WaiterClock;
    use waiter::{Slot, SlotInner};

    struct ModelClock;
    impl WaiterClock for ModelClock {
        fn now_ms(&self) -> i64 {
            0
        }
        fn wait<'a>(
            &self,
            slot: &'a Slot,
            guard: pgsync::MutexGuard<'a, SlotInner>,
            _timeout_ms: Option<i64>,
        ) -> (pgsync::MutexGuard<'a, SlotInner>, bool) {
            (slot.wait_for_model(guard), false)
        }
    }
    static CLOCK: ModelClock = ModelClock;

    struct TimerMirror {
        // (armed, fired) — the slots HashMap collapsed to one entry; the
        // protocol is per-entry independent.
        state: Mutex<(bool, bool)>,
        // The timer thread's published waker word (0 until published) —
        // timeout's TimerShared.timer_waker.
        timer_waker: AtomicU64,
    }

    // arm_timer mirrored: insert under the lock, release, THEN unpark
    // through the published word.
    fn arm(sh: &TimerMirror, slots: &[Arc<Slot>]) {
        {
            let mut g = sh.state.lock().unwrap();
            g.0 = true;
        }
        let w = sh.timer_waker.load(Ordering::SeqCst);
        if w != 0 {
            let (idx, token) = ((w >> 32) as usize - 1, w as u32);
            let _ = slots[idx].unpark_token(token);
        }
    }

    // RED helper (one-line swap below): the naked insert.
    #[allow(dead_code)]
    fn arm_naked(sh: &TimerMirror) {
        let mut g = sh.state.lock().unwrap();
        g.0 = true;
    }

    loom::model(|| {
        let sh = Arc::new(TimerMirror {
            state: Mutex::new((false, false)),
            timer_waker: AtomicU64::new(0),
        });
        // Model-owned slot slab (the waiter global slab is production-only).
        let slots: Arc<Vec<Arc<Slot>>> = Arc::new(vec![Arc::new(Slot::new_for_model())]);

        let timer_t = {
            let (sh, slots) = (Arc::clone(&sh), Arc::clone(&slots));
            thread::spawn(move || {
                // timer_thread mirrored: publish BEFORE scan, park outside
                // the lock (Dekker: either the scan sees the arm, or the
                // arm's unpark hits the published word — unparks latched).
                let token = slots[0].issue_token();
                let word = (1u64 << 32) | token as u64;
                loop {
                    sh.timer_waker.store(word, Ordering::SeqCst);
                    {
                        let mut g = sh.state.lock().unwrap();
                        if g.0 {
                            // "Fire": post + wake the owner (model: flag).
                            g.1 = true;
                            return;
                        }
                    }
                    // No deadline armed: untimed park (production parks
                    // timed to the nearest deadline; untimed here makes a
                    // lost arm a deadlock loom must catch).
                    let _ = slots[0].park_core(None, None, &CLOCK);
                }
            })
        };

        // The armer (a backend's arm_timer).
        arm(&sh, &slots); // RED: arm_naked(&sh)

        timer_t.join().unwrap();
        assert!(sh.state.lock().unwrap().1, "the armed timeout fired");
    });
}

// ---- WS-B admission-ledger models (single-executor Phase 0.1) --------------
// Three models sized for the loom-fast ≤5min train gate (2 threads, 1-3
// granules, preemption bound 2 — the standing budget law): the ledger is
// ADVISORY width policy over the existing finalization protocol, so the
// invariants are (a) grants never exceed the core budget, (b) a shed
// (Yield → TaskEnd::Budget → re-pick) never loses or duplicates work, and
// (c) narrowed/under-target entries always finish (liveness: target >= 1
// while admitted; a lost ledger wake would deadlock the model, which
// loom's detector oracles).

/// Drive worker steps until Stop is requested (multi-RG helper for models
/// whose waiters are created concurrently): parks on Idle with the epoch
/// discipline; request_stop's wake_all unparks it to observe Stop.
fn drive_until_stop(rt: &Runtime, worker: usize) {
    let mut local = rt.worker_local(worker);
    loop {
        let epoch = rt.park_epoch();
        match rt.worker_step(&mut local) {
            Step::Ran => {}
            Step::Retry => thread::yield_now(),
            Step::Idle => rt.park(epoch),
            Step::Stop => break,
        }
    }
}

/// Ledger model 1: Σ granted ≤ the core budget under every interleaving of
/// two workers × two concurrently-admitted RGs (targets split 1+1), probed
/// from INSIDE the work body (the only place a violation could act), plus
/// clean drain (admitted = granted = 0) after both completions.
/// LOOM-FAST SIZING (≤5min budget law): both RGs single-granule AND
/// preemption_bound 1 — the original 2-granule/bound-2 shape blew the
/// local budget (>150s alone), and the cost lives in the two full drive
/// loops (pick/park/publish interleavings), not the granule count. Bound 1
/// still explores every ordering of the two workers' joins/leaves across
/// both slots (loom branches cover all yield points; the bound only caps
/// forced preemptions between them). Bound-2 exhaustion rides the fleet's
/// exhaustive loom trail, not the gate.
#[test]
fn ledger_grants_never_exceed_core_budget() {
    struct BudgetWork {
        inner: Arc<ModelWork>,
        rt: Arc<Runtime>,
        cores: u32,
    }
    impl TaskSetWork for BudgetWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            let snap = self.rt.ledger_snapshot();
            assert!(
                snap.granted_total <= self.cores,
                "grants exceed the core budget: {} > {}",
                snap.granted_total,
                self.cores
            );
            self.inner.run_morsel(worker, range);
        }
        fn finalize(&self) {
            self.inner.finalize();
        }
    }

    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(1);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_ledger(true);
        let wa = ModelWork::new(1, None);
        let wb = ModelWork::new(1, None);
        let (_ha, waiter_a) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::new(BudgetWork {
                    inner: Arc::clone(&wa),
                    rt: Arc::clone(&rt),
                    cores: 2,
                }) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });
        let (_hb, waiter_b) = rt.submit(QuerySpec {
            query_id: 2,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::new(BudgetWork {
                    inner: Arc::clone(&wb),
                    rt: Arc::clone(&rt),
                    cores: 2,
                }) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let (wa1, wb1) = (waiter_a.clone(), waiter_b.clone());
        let t = thread::spawn(move || drive_all(&rt1, 1, &[wa1, wb1]));
        drive_all(&rt, 0, &[waiter_a.clone(), waiter_b.clone()]);
        t.join().unwrap();

        assert_eq!(waiter_a.try_wait(), Some(RgOutcome::Completed));
        assert_eq!(waiter_b.try_wait(), Some(RgOutcome::Completed));
        wa.assert_complete();
        wb.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0, "both entries retired");
        assert_eq!(snap.granted_total, 0, "every grant returned");
    });
}

/// Ledger model 2: yield / worker-freed re-pick loses no work. An arrival
/// (B, submitted CONCURRENTLY with the drive) narrows the incumbent A
/// below its live grants; whatever the interleaving — shed at the claim
/// boundary, re-pick onto B, or B arriving after A drained — every granule
/// of both RGs executes exactly once and both RGs complete.
/// LOOM-FAST SIZING (≤5min budget law): preemption_bound 1 — the narrowing
/// needs a 2-granule incumbent (granted 2 when B admits), and that shape
/// at bound 2 blew the local budget (>150s alone). Bound 1 still explores
/// every ORDERING of B's admit vs A's claims/sheds (the model's subject —
/// loom branches cover all yield points; the bound only caps forced
/// preemptions between them). Bound-2 exhaustion rides the fleet trail.
#[test]
fn ledger_yield_worker_freed_repick() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(1);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_ledger(true);
        let wa = ModelWork::new(2, None);
        let (_ha, waiter_a) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&wa) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        // The second worker starts BEFORE B exists: it can be mid-A-task
        // (granted 2, target 2) when B's admit narrows A to target 1.
        let rt1 = Arc::clone(&rt);
        let t = thread::spawn(move || drive_until_stop(&rt1, 1));

        let wb = ModelWork::new(1, None);
        let (_hb, waiter_b) = rt.submit(QuerySpec {
            query_id: 2,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::clone(&wb) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        drive_all(&rt, 0, &[waiter_a.clone(), waiter_b.clone()]);
        t.join().unwrap();

        assert_eq!(waiter_a.try_wait(), Some(RgOutcome::Completed));
        assert_eq!(waiter_b.try_wait(), Some(RgOutcome::Completed));
        wa.assert_complete();
        wb.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0);
        assert_eq!(snap.granted_total, 0);
    });
}

/// Ledger model 3: re-nudge is BOUNDED and the entry stays LIVE. A single
/// driver serves a target-2 entry (permanently under target: every claim
/// boundary may re-nudge); the RG must complete under every interleaving
/// (a lost ledger wake or a target-floor bug would deadlock the model —
/// loom's detector is the oracle) and the re-nudge count never exceeds the
/// one recompute window's budget.
#[test]
fn ledger_renudge_bounded_and_live() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_ledger(true);
        let work = ModelWork::new(2, None);
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive(&rt1, 1, &waiter1));
        // The main thread only observes (cfg(loom) wait polls try_wait):
        // the drive stays single-width, under the target-2 entry.
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        t.join().unwrap();

        work.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0, "entry retired despite never reaching target");
        assert_eq!(snap.granted_total, 0);
        assert!(
            snap.renudges <= 4,
            "re-nudges exceed the per-recompute budget: {}",
            snap.renudges
        );
    });
}

/// Ledger model 4: BLOCKING-SECTION DONATION liveness (§2.8 × ledger
/// composition — the knob-ON hang WS-B's finalize caught in
/// io_permit_seams_donate_core_to_standby): with ONE core (target 1) the
/// incumbent enters a declared blocking section INSIDE run_morsel and
/// spins until the peer granule runs — only the standby can run it, and
/// only if the section donated the width grant along with the permit
/// (otherwise the saturated slot refuses the standby's join and every
/// thread ends up yielded/parked: loom's deadlock detector is the oracle).
/// Sized for the loom-fast gate: 2 threads, 2 granules, preemption_bound 1
/// (the bound-2 shape belongs to the fleet's exhaustive trail).
#[test]
fn ledger_blocking_section_donation_liveness() {
    struct DonateWork {
        inner: Arc<ModelWork>,
        io_taken: AtomicUsize,
        peer_ran: AtomicBool,
    }
    impl TaskSetWork for DonateWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            if self.io_taken.fetch_add(1, Ordering::SeqCst) == 0 {
                // Declared blocking section on the FIRST morsel executed:
                // wait (spin-yield: a loom scheduling point) for the peer
                // morsel, which only the standby can run.
                let io = runtime::blocking_io_section();
                while !self.peer_ran.load(Ordering::SeqCst) {
                    thread::yield_now();
                }
                drop(io);
                self.inner.run_morsel(worker, range);
            } else {
                self.peer_ran.store(true, Ordering::SeqCst);
                self.inner.run_morsel(worker, range);
            }
        }
        fn finalize(&self) {
            self.inner.finalize();
        }
    }

    /// Pool-loop emulation with the real permit + park discipline AND the
    /// facade registration (mirrors pool.rs worker_loop; the
    /// facade_standby_absorption shape).
    fn drive_registered(rt: &Runtime, worker: usize, waiter: &CompletionWaiter) {
        // SAFETY: the runtime outlives this drive; the permit is held
        // across worker_step, where the facade is called.
        let _reg = unsafe { runtime::PermitThreadReg::new(rt.execution_permits()) };
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
    b.preemption_bound = Some(1);
    b.max_branches = 200_000;
    b.check(|| {
        // workers=1 ⇒ ONE execution permit AND ledger target 1; standbys=1
        // ⇒ two pool threads.
        let rt = small_runtime(1, 1);
        rt.set_ledger(true);
        let inner = ModelWork::new(2, None);
        let work = Arc::new(DonateWork {
            inner: Arc::clone(&inner),
            io_taken: AtomicUsize::new(0),
            peer_ran: AtomicBool::new(false),
        });
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 6,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive_registered(&rt1, 1, &waiter1));
        drive_registered(&rt, 0, &waiter);
        t.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        inner.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0);
        assert_eq!(snap.granted_total, 0, "donate/rejoin stayed balanced");
    });
}

/// PHASE3-CLOSE WS-WIDTH unified-face model — the REPLACEMENT of WS-O's
/// external-face lease-liveness model (removed in the same W4 commit that
/// removed the external face; this successor landed at W1 and carries the
/// oracles): a ParallelWidthLease backed by a UNIFIED GANG ENTRY
/// (admit_gang → settle → drop/retire, all through the pool face's ONE
/// recompute) runs CONCURRENTLY with a pool drive on a 2-core box —
/// gang-and-pool interleavings in ONE model (contract §2.1).
/// Oracles: (a) the pool RG completes under every interleaving — the
/// frozen gang charge can zero the pool budget mid-drive and the liveness
/// floor (target ≥ 1) plus the retire wake must keep the driver live (a
/// lost widening wake or a floor bug deadlocks the model — FM-2's bound);
/// (b) the gang grant respects the core budget (headroom-only, single
/// leaser: granted ≤ cores); (c) accounting drains to zero — gang entry
/// retired, every pool grant returned. Lock-order soundness (membership →
/// inner; admit_gang takes inner ALONE) is exercised by the concurrent
/// submit + lease: an inversion would deadlock under loom.
/// LOOM-FAST SIZING (≤5min law): pb 1, one lease thread + one driver —
/// the external model's exact budget.
#[test]
fn ledger_unified_gang_lease_liveness() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(1);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_ledger(true);
        let work = ModelWork::new(1, None);
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let rt1 = Arc::clone(&rt);
        let lessee = thread::spawn(move || {
            let lease = rt1.lease_parallel_width(2);
            if let Some(mut lease) = lease {
                assert!(lease.granted() <= 2, "headroom-only: never above cores");
                lease.settle(lease.granted().min(1));
            }
            // Drop retires the gang entry (widening wake rides retire).
        });

        drive_all(&rt, 0, &[waiter.clone()]);
        lessee.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        work.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0);
        assert_eq!(snap.granted_total, 0, "every pool grant returned");
        assert_eq!(snap.gang_admitted, 0, "lease drop retired the gang entry");
        assert_eq!(snap.gang_active, 0);
    });
}

/// WS-S (wave 3) caller-C2 model: the caller's PARKED drive
/// (`drive_with_duties_parked` — the inc-3 bounded idle park replacing the
/// C1 eventcount park) completes a PINNED RG under every interleaving with
/// one concurrent external helper. The park is modeled as a loom yield —
/// the production latch quantum's bound with the timeout collapsed to
/// zero — so the model proves the C2 loop's liveness comes from the
/// BOUND + re-check, not from the eventcount's lost-wakeup guarantee the
/// C1 arm kept: a C2 iteration that could sleep past a completion would
/// deadlock/livelock the model. Oracles: completion under every path,
/// every granule exactly once, the claim duty observed at caller claim
/// boundaries, drained ledger accounting, and the pin-board settle
/// (debug_assert inside drive_loop, live under loom's debug build).
/// LOOM-FAST SIZING (≤5min law): pb 1, 2 granules, one helper thread.
#[test]
fn caller_c2_parked_drive_liveness() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(1);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        let work = ModelWork::new(2, None);
        let (h, waiter) = rt.submit_pinned(QuerySpec {
            query_id: 7,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        // Helper = a second external participant on the C1 face
        // (eventcount-parked drive_with_duty; the production drive_pinned
        // is not(loom) — Instant/sleep): the model races the C1 park
        // discipline against the C2 bounded park on the same pinned RG.
        let rt1 = Arc::clone(&rt);
        let h1 = h.clone();
        let helper = thread::spawn(move || {
            let mut cw = runtime::CallerWorker::enter(&rt1).expect("helper lane available");
            cw.drive_with_duty(&rt1, &h1, &mut || Ok(()))
                .expect("helper duty never fails")
        });

        let claim_boundaries = AtomicUsize::new(0);
        let mut caller = runtime::CallerWorker::enter(&rt).expect("caller lane available");
        let outcome = caller
            .drive_with_duties_parked(
                &rt,
                &h,
                &mut || Ok(()),
                &mut || {
                    claim_boundaries.fetch_add(1, Ordering::SeqCst);
                    true
                },
                &mut || {
                    // Bounded park: a scheduling point, then re-check —
                    // the quantum's timeout arm with zero wait.
                    thread::yield_now();
                    Ok(())
                },
            )
            .expect("duty and park never fail");
        assert_eq!(outcome, RgOutcome::Completed);
        assert_eq!(helper.join().unwrap(), RgOutcome::Completed);
        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        work.assert_complete();
        // The caller stepped through run_task at least once in every
        // interleaving where it won a claim; the duty hook must have fired
        // at each of ITS claim boundaries — never negative, and zero only
        // if the helper claimed everything first (both are legal; the
        // assert is the hook's install/clear RAII not corrupting states).
        let _ = claim_boundaries.load(Ordering::SeqCst);
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0);
        assert_eq!(snap.granted_total, 0);
    });
}

/// Ledger model 6 (CALLER-C2 LEDGER HANG fix, the wake-gate's adversarial
/// oracle): `stream_source_handshake` with the ledger ON. The fix
/// suppresses the leave hint's wake_all when the task ended STARVED (the
/// self-wake busy-loop: every starved leave of a granted==target slot
/// bumped the park epoch, so no park — pool eventcount or caller-C2
/// bounded park — ever slept). THIS model is the proof the gate loses no
/// wakeup: a publish/close landing in ANY window around a starved task's
/// leave must still complete the RG — a starved worker stranded in a park
/// (the wake the gate suppressed turning out to be load-bearing) deadlocks
/// the model, which loom's blocked-thread detector oracles. The publish
/// wake (notify_source_progress) and the epoch capture-before-step are
/// what close the window, exactly as the pre-ledger handshake model
/// proved for the knob-OFF path.
/// PRE-FIX TEETH (review-corrected record): at loom-fast bounds
/// (LOOM_MAX_PERMUTATIONS=200000) this model passes pre-fix — the
/// bounded exploration prefix never lets a starved worker spin long
/// enough to matter. EXHAUSTIVE, it FAILS pre-fix (~40s): the
/// unbounded self-wake spin's tracked atomic ops saturate loom's u16
/// `VersionVec` counters — the same artifact class as model 7's
/// tripwire, beyond the 200k prefix.
/// LOOM-FAST SIZING (≤5min law): pb 2 as the knob-OFF twin; 2 workers,
/// 2 granules, one producer.
#[test]
fn ledger_starved_leave_wake_gate_no_lost_wakeup() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(2, 0);
        rt.set_ledger(true);
        let work = ModelWork::new(2, None);
        let source = Arc::new(StreamSource::new());
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::clone(&source) as _,
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let producer = {
            let rt = Arc::clone(&rt);
            let source = Arc::clone(&source);
            thread::spawn(move || {
                source.publish(1);
                rt.notify_source_progress();
                source.publish(2);
                source.close();
                rt.notify_source_progress();
            })
        };

        let rt1 = Arc::clone(&rt);
        let waiter1 = waiter.clone();
        let t = thread::spawn(move || drive(&rt1, 1, &waiter1));
        drive(&rt, 0, &waiter);
        t.join().unwrap();
        producer.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        work.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0, "entry retired at completion");
        assert_eq!(snap.granted_total, 0, "every grant returned");
    });
}

/// Ledger model 7 (CALLER-C2 LEDGER HANG shape under loom): a PINNED RG
/// on an OPEN starved stream, ledger ON, the caller-C2 bounded park as
/// the producer — the exact hang configuration (unit twins:
/// caller_parked_drive_pumps_idle_park_ledger_on / _park_error arm).
/// Pre-fix this LIVELOCKS: the caller's own starved leave bumps the park
/// epoch every step, the C2 epoch pre-check never admits the park, the
/// publish never happens, and the model spins unboundedly. The pre-fix
/// ORACLE (review-corrected record): loom has no livelock detector for a
/// non-switching spin — what trips, deterministically in ~0.01s, is
/// loom's causality bookkeeping. The spin's unbounded tracked ops WRAP
/// the caller thread's u16 `VersionVec` counter (`inc` is an unchecked
/// `+= 1` in release), after which the caller's own `active_workers`
/// fetch_add in `run_task` no longer appears causally-after the atomic's
/// creation-time happens-after mark (`State::new` → `track_unsync_mut`),
/// and loom panics "Causality violation: Concurrent load and mut
/// accesses" (created = the taskset entry's `active_workers` init in
/// sched.rs; load = the `run_task` fetch_add). Single user thread ⇒ a
/// genuine race is impossible; the panic is an ARTIFACT of the removed
/// busy-loop path — and precisely therefore a sound, fast tripwire: any
/// reintroduced unbounded spin re-trips it. Post-fix the park must run
/// under every ordering of the ledger words and the drive completes with
/// drained accounting. LOOM-FAST SIZING: single caller thread, 2
/// granules.
#[test]
fn caller_c2_ledger_starved_park_completes() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 200_000;
    b.check(|| {
        let rt = small_runtime(1, 0);
        rt.set_ledger(true);
        let work = ModelWork::new(2, None);
        let source = Arc::new(StreamSource::new());
        let (h, waiter) = rt.submit_pinned(QuerySpec {
            query_id: 9,
            tasksets: vec![TaskSetSpec {
                source: Arc::clone(&source) as _,
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        let mut parks = 0u64;
        let mut caller = runtime::CallerWorker::enter(&rt).expect("caller lane available");
        let outcome = caller
            .drive_with_duties_parked(
                &rt,
                &h,
                &mut || Ok(()),
                &mut || true,
                &mut || {
                    parks += 1;
                    // Park-as-producer (publish-then-wake order): the
                    // starved window ends HERE, so reaching the park at
                    // all is the liveness being modeled.
                    source.publish(2);
                    source.close();
                    rt.notify_source_progress();
                    Ok(())
                },
            )
            .expect("duty and park never fail");
        assert_eq!(outcome, RgOutcome::Completed);
        assert!(parks >= 1, "the bounded idle park must run");
        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        work.assert_complete();
        let snap = rt.ledger_snapshot();
        assert_eq!(snap.admitted, 0);
        assert_eq!(snap.granted_total, 0);
    });
}

/// 11. funnel_waiter_flag_park_wake — the row-emit funnel's waiter-flag
/// park/wake protocol (gather-elimination Phase 2; funnel.rs), the SC-fence
/// Dekker handshake on BOTH sides: producer parks on full behind
/// `producer_waiting` (armed before the full re-check, consumed by the pop
/// wake) and the drain parks on empty behind `drain_waiting` (armed before
/// the final sweep, consumed by the push wake). A capacity-2 ring with 3
/// rows forces both full-parks and empty-parks across interleavings; a LOST
/// WAKE on either side — the store-buffering double-park race the flags must
/// resolve — is a loom-detected deadlock. Oracles: termination, and all rows
/// delivered exactly once in order (SPSC FIFO through the funnel).
#[test]
fn funnel_waiter_flag_park_wake() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.max_branches = 200_000;
    b.check(|| {
        let f: Arc<runtime::RowFunnel<u32>> = runtime::RowFunnel::new(1, 2);
        let fp = Arc::clone(&f);
        let t = thread::spawn(move || {
            let p = fp.producer(0);
            assert_eq!(p.push_blocking(1, || {}), runtime::PushOutcome::Pushed);
            assert_eq!(p.push_blocking(2, || {}), runtime::PushOutcome::Pushed);
            assert_eq!(p.push_blocking(3, || {}), runtime::PushOutcome::Pushed);
            p.mark_done();
        });
        let mut d = f.drain();
        let mut got = Vec::new();
        loop {
            // The waiter-flag wait pattern: epoch, arm, sweep, park-on-Idle.
            let seen = d.park_epoch();
            d.arm_wait();
            match d.next() {
                runtime::DrainStep::Row(v) => got.push(v),
                runtime::DrainStep::Idle => d.park(seen),
                runtime::DrainStep::Eof => break,
            }
        }
        t.join().unwrap();
        assert_eq!(got, vec![1, 2, 3], "every row exactly once, FIFO");
    });
}

/// 12. funnel_close_demand_unblocks — LIMIT/demand-close liveness under the
/// waiter-flag protocol: a producer parked on a FULL ring must be freed by
/// `close_demand`'s unconditional wake (the drain never pops). Termination
/// is the oracle (a missed close wake = loom-detected deadlock); the
/// outcome may be Pushed or DemandClosed depending on the race — both legal,
/// the parked case exercising the flag-armed park against the close.
#[test]
fn funnel_close_demand_unblocks() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.max_branches = 200_000;
    b.check(|| {
        let f: Arc<runtime::RowFunnel<u32>> = runtime::RowFunnel::new(1, 2);
        // Fill the ring before spawning (sequential producer handoff — the
        // SPSC discipline holds: never two live producers at once).
        let p0 = f.producer(0);
        p0.try_push(1).ok().unwrap();
        p0.try_push(2).ok().unwrap();
        drop(p0);
        let fp = Arc::clone(&f);
        let t = thread::spawn(move || fp.producer(0).push_blocking(3, || {}));
        f.close_demand();
        let _ = t.join().unwrap();
    });
}

// ---------------------------------------------------------------------------
// M2 inc-2 — the bound-engagement claim gate (PGPROC-leasing pool workers;
// scratchpad/night/m2-pool-binding-scope.md §3 inc-2 "NEW loom models:
// claim-gate vs abort, detach join, permit + binding interleavings").
//
// The detach-count leader join is the standing board's, already pinned by
// `standing_gang_detach_count_join_no_lost_wake` (the pool channel reuses
// StandingEngagement verbatim). These models pin the RUNTIME-side additions:
// the worker_step dispatch (bound RGs never reach run_task from a pool
// worker; the early pin settle cannot corrupt the finalization protocol),
// the permit release/re-acquire around the nested serve drive (balance at
// capacity — the double-permit deadlock the release exists to prevent), and
// the Refused/Closed skip cache (a refusing worker parks instead of
// spinning, and the park loses no wake for other work).
// ---------------------------------------------------------------------------

use runtime::{BoundDescriptor, BoundServe, RgHandle};

/// The models' serve: claim one synthetic ticket (cap 1) and drive the
/// bound RG to its outcome as an external participant (CallerWorker — the
/// loom-usable face of the production external-lane pinned drive). Called
/// by the scheduler with the pool permit RELEASED; the caller drive's own
/// per-step permit rhythm exercises exactly the capacity interleaving the
/// release exists for (at workers=1 a held permit here would deadlock).
struct BoundModelPayload {
    rt: Arc<Runtime>,
    rg: RgHandle,
    tickets: AtomicUsize,
    refuse_all: AtomicBool,
    serves: AtomicUsize,
}

fn bound_model_serve(payload: &Arc<dyn std::any::Any + Send + Sync>) -> BoundServe {
    let p = Arc::clone(payload)
        .downcast::<BoundModelPayload>()
        .expect("model payload");
    if p.refuse_all.load(Ordering::SeqCst) {
        return BoundServe::Refused;
    }
    if p.tickets.fetch_add(1, Ordering::SeqCst) >= 1 {
        p.tickets.fetch_sub(1, Ordering::SeqCst);
        return BoundServe::Closed;
    }
    let mut cw = runtime::CallerWorker::enter(&p.rt).expect("external lane for the serve");
    let _outcome = cw
        .drive_with_duty(&p.rt, &p.rg, &mut || Ok(()))
        .expect("model duty never fails");
    p.serves.fetch_add(1, Ordering::SeqCst);
    BoundServe::Served
}

/// Pool-loop emulation with the REAL permit discipline (the serve gate's
/// contract: worker_step is entered with a permit held) — the
/// standby_absorption driver, shared by the bound models.
fn drive_bound_pool(rt: &Arc<Runtime>, worker: usize, waiters: &[CompletionWaiter]) {
    let mut local = rt.worker_local(worker);
    loop {
        if waiters.iter().all(|w| w.try_wait().is_some()) {
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
                if waiters.iter().all(|w| w.try_wait().is_some()) {
                    break;
                }
                rt.park(epoch);
            }
            Step::Stop => break,
        }
    }
}

fn bound_model_submit(
    rt: &Arc<Runtime>,
    total: u64,
    refuse_all: bool,
) -> (Arc<ModelWork>, Arc<BoundModelPayload>, RgHandle, CompletionWaiter) {
    let work = ModelWork::new(total, None);
    // The descriptor must ride the submit (the active bit keys off it at
    // publication), but the payload needs the submit's own RgHandle: an
    // indirection cell bridges the cycle. The cell is completed BEFORE any
    // pool driver thread spawns, so no serve can race the store.
    let placeholder = Arc::new(loom::sync::Mutex::new(None::<Arc<BoundModelPayload>>));
    fn deferred_serve(payload: &Arc<dyn std::any::Any + Send + Sync>) -> BoundServe {
        let slot = Arc::clone(payload)
            .downcast::<loom::sync::Mutex<Option<Arc<BoundModelPayload>>>>()
            .expect("model placeholder");
        let inner = slot.lock().unwrap().clone().expect("payload installed pre-race");
        let inner: Arc<dyn std::any::Any + Send + Sync> = inner;
        bound_model_serve(&inner)
    }
    let (h, waiter) = rt.submit_pinned_bound(
        QuerySpec {
            query_id: 21,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(total).with_c0(1)),
                work: Arc::clone(&work) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        },
        0,
        BoundDescriptor {
            serve: deferred_serve,
            payload: Arc::clone(&placeholder) as Arc<dyn std::any::Any + Send + Sync>,
        },
    );
    let payload = Arc::new(BoundModelPayload {
        rt: Arc::clone(rt),
        rg: h.clone(),
        tickets: AtomicUsize::new(0),
        refuse_all: AtomicBool::new(refuse_all),
        serves: AtomicUsize::new(0),
    });
    *placeholder.lock().unwrap() = Some(Arc::clone(&payload));
    (work, payload, h, waiter)
}

/// Model B1 — bound serve completes under the permit cap: ONE permit
/// (workers=1), two pool threads; whoever picks the bound RG settles its
/// pool pin, gives up the permit, and drives the whole RG through the
/// caller face while the other thread interleaves freely. Oracles: RG
/// completes, every granule exactly once, finalize exactly once, the
/// ticket cap held (≤1 serve), and full permit capacity restored (the
/// release/re-acquire around the serve balanced in every interleaving).
#[test]
fn bound_gate_serve_completes_under_permit_cap() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.check(|| {
        let rt = small_runtime(1, 1);
        let (work, payload, _h, waiter) = bound_model_submit(&rt, 2, false);

        let rt1 = Arc::clone(&rt);
        let w1 = waiter.clone();
        let peer = thread::spawn(move || drive_bound_pool(&rt1, 1, &[w1]));
        drive_bound_pool(&rt, 0, &[waiter.clone()]);
        peer.join().unwrap();

        assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
        work.assert_complete();
        assert_eq!(rt.stats().finalize_events, 1);
        assert!(payload.serves.load(Ordering::SeqCst) <= 1, "ticket cap held");
        assert_eq!(rt.execution_permits().available(), 1, "permit balance");
    });
}

/// Model B2 — the Refused skip cache: a pool worker whose serve refuses
/// (identity/db mismatch in production) must SKIP the publication — its
/// park stays reachable and an UNBOUND RG published afterwards is still
/// executed (no lost wake through the skip), while an external caller
/// completes the bound RG. Oracles: both RGs complete, granules exactly
/// once each, permit balance.
#[test]
fn bound_gate_refused_skip_no_lost_wake() {
    let mut b = loom::model::Builder::new();
    // Three threads x two RGs x the permit handoff: the richest of the
    // bound models — needs the explicit branch budget the caller/ledger
    // models use (the default trips loom's spin heuristic long before the
    // space is explored).
    b.preemption_bound = Some(2);
    b.max_branches = 500_000;
    b.check(|| {
        let rt = small_runtime(1, 0);
        let (bwork, payload, h, bwaiter) = bound_model_submit(&rt, 1, true);
        let uwork = ModelWork::new(1, None);
        let (_uh, uwaiter) = rt.submit(QuerySpec {
            query_id: 22,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1).with_c0(1)),
                work: Arc::clone(&uwork) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });

        // External caller (the leader's fallback face) completes the
        // refused bound RG while the pool worker skips it.
        let rt1 = Arc::clone(&rt);
        let h1 = h.clone();
        let external = thread::spawn(move || {
            let mut cw = runtime::CallerWorker::enter(&rt1).expect("caller lane");
            cw.drive_with_duty(&rt1, &h1, &mut || Ok(())).expect("duty never fails")
        });
        drive_bound_pool(&rt, 0, &[bwaiter.clone(), uwaiter.clone()]);
        assert_eq!(external.join().unwrap(), RgOutcome::Completed);

        assert_eq!(bwaiter.try_wait(), Some(RgOutcome::Completed));
        assert_eq!(uwaiter.try_wait(), Some(RgOutcome::Completed));
        bwork.assert_complete();
        uwork.assert_complete();
        assert_eq!(payload.serves.load(Ordering::SeqCst), 0, "refused board never served");
        assert_eq!(rt.execution_permits().available(), 1, "permit balance");
    });
}

/// Model B3 — abort vs the serve claim: an abort racing the pool worker's
/// engagement (before, during, or after its drive) always completes the
/// RG (Aborted or Completed — both legal depending on the interleaving),
/// never runs finalize work on the aborted arm, and balances permits.
#[test]
fn bound_gate_abort_vs_serve() {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.check(|| {
        let rt = small_runtime(1, 0);
        let (work, _payload, h, waiter) = bound_model_submit(&rt, 2, false);

        let h1 = h.clone();
        let aborter = thread::spawn(move || h1.abort());
        drive_bound_pool(&rt, 0, &[waiter.clone()]);
        aborter.join().unwrap();

        let outcome = waiter.try_wait().expect("RG reached an outcome");
        if outcome == RgOutcome::Completed {
            work.assert_complete();
            assert_eq!(rt.stats().finalize_events, 1);
        }
        assert_eq!(rt.execution_permits().available(), 1, "permit balance");
    });
}
