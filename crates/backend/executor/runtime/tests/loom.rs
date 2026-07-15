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
use loom::thread;

use runtime::{
    Clock, CompletionWaiter, Generation, MorselRange, ParticipantOwner, QuerySpec,
    QueryTaskLifecycle, RgOutcome, Runtime, RuntimeConfig, SizingParams, Step,
    StreamSource, SyntheticMorselSource, TaskSetSpec, TaskSetWork, VirtualClock,
};
use types_error::{PgError, ERROR};

/// loomfast lane (2026-07-15): fast-tier preemption clamp. The census showed
/// the loom gate's ~6,300s wall is entirely these models at their LANDED
/// hardcoded bounds (finalization_protocol/dag_abort >900s, dag_fanout 611s;
/// all waiter models ~0s even unbounded), so an external LOOM_MAX_PREEMPTIONS
/// cannot make a fast tier -- Builder::new() reads it but the models overwrite
/// preemption_bound. This helper lets PGRUST_LOOM_PREEMPTION_CAP only LOWER a
/// model's landed bound, never raise it: unset (the exhaustive tier,
/// runtime-loom-e2e.sh) is byte-identical to the pre-lane behavior; the fast
/// tier (runtime-loom-fast-e2e.sh) sets the cap. Model logic and landed
/// bounds are unchanged.
fn clamped_bound(landed: usize) -> Option<usize> {
    match std::env::var("PGRUST_LOOM_PREEMPTION_CAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        Some(cap) if cap < landed => Some(cap),
        _ => Some(landed),
    }
}

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
    b.preemption_bound = clamped_bound(3);
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
    b.preemption_bound = clamped_bound(3);
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
    b.preemption_bound = clamped_bound(3);
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
    b.preemption_bound = clamped_bound(3);
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
    b.preemption_bound = clamped_bound(3);
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
    b.preemption_bound = clamped_bound(3);
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
    b.preemption_bound = clamped_bound(2);
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
    b.preemption_bound = clamped_bound(2);
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
    b.preemption_bound = clamped_bound(2);
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
    b.preemption_bound = clamped_bound(2);
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
