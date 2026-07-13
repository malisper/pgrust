//! M0 unit tests: deterministic virtual-time sizing, protocol end-to-end
//! over real threads, generation lifecycle, boundary clamping, kill switch.
//! (The exhaustive-interleaving finalization/generation models live in
//! tests/loom.rs under --cfg loom.)

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::*;

/// Work that records every executed granule exactly once via a shared
/// bitmap, advances a virtual clock by cost·granules, and counts finalizes.
struct SyntheticWork {
    clock: Option<Arc<VirtualClock>>,
    cost_per_granule_ns: u64,
    executed: Mutex<Vec<bool>>,
    finalizes: AtomicU64,
    claims: Mutex<Vec<Range<u64>>>,
}

impl SyntheticWork {
    fn new(total: u64, clock: Option<Arc<VirtualClock>>, cost: u64) -> Arc<SyntheticWork> {
        Arc::new(SyntheticWork {
            clock,
            cost_per_granule_ns: cost,
            executed: Mutex::new(vec![false; total as usize]),
            finalizes: AtomicU64::new(0),
            claims: Mutex::new(Vec::new()),
        })
    }

    fn assert_all_executed_once(&self) {
        let ex = self.executed.lock().unwrap();
        assert!(ex.iter().all(|&b| b), "every granule must execute exactly once");
    }
}

impl TaskSetWork for SyntheticWork {
    fn run_morsel(&self, _worker: usize, range: MorselRange) {
        {
            let mut ex = self.executed.lock().unwrap();
            for g in range.clone() {
                assert!(!ex[g as usize], "granule {g} executed twice");
                ex[g as usize] = true;
            }
        }
        self.claims.lock().unwrap().push(range.clone());
        if let Some(c) = &self.clock {
            c.advance(self.cost_per_granule_ns * (range.end - range.start));
        }
    }

    fn finalize(&self) {
        self.finalizes.fetch_add(1, Ordering::SeqCst);
    }
}

fn spec_one(work: &Arc<SyntheticWork>, source: Arc<dyn MorselSource>) -> QuerySpec {
    QuerySpec {
        query_id: 1,
        tasksets: vec![TaskSetSpec {
            source,
            work: Arc::clone(work) as Arc<dyn TaskSetWork>,
            deps: vec![],
        }],
    }
}

fn virtual_runtime(workers: usize, clock: &Arc<VirtualClock>) -> Arc<Runtime> {
    let mut cfg = RuntimeConfig::new(workers);
    cfg.slots = 8;
    Runtime::with_clock(cfg, Arc::clone(clock) as Arc<dyn Clock>)
}

// ---- sizing state machine (deterministic, virtual time) --------------------

/// Single worker, 1 µs/granule, t_max 2 ms: the startup ramp must be
/// 16,32,64,128,256,512 (Σ=1008 µs; the next doubling 2·512 > 2000−1008),
/// then Default-state morsels of T·t_max = 2000 granules, then the photo
/// finish (remaining < W·t_max) sized remaining/W.
#[test]
fn sizing_ramp_default_shutdown_trace() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    let total = 16_000u64;
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (_h, waiter) = rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));

    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);

    let claims = work.claims.lock().unwrap();
    let sizes: Vec<u64> = claims.iter().map(|r| r.end - r.start).collect();
    // Startup ramp (one task, exponential, budget-fitted):
    assert_eq!(&sizes[..6], &[16, 32, 64, 128, 256, 512]);
    // Default state: T seeded from the LAST ramp morsel = 512g/512µs = 1g/µs
    // ⇒ T·t_max = 2000 granules.
    assert_eq!(sizes[6], 2000);
    // EWMA on a constant-throughput source keeps T at 1 g/µs.
    assert_eq!(sizes[7], 2000);
    // Shutdown (W=1): remaining < 1·t_max ⇒ one final morsel of exactly the
    // remainder. Total must be preserved regardless.
    assert_eq!(sizes.iter().sum::<u64>(), total);
    let last = *sizes.last().unwrap();
    assert!(last < 2000, "photo-finish must shrink the final morsel, got {last}");

    let stats = rt.stats();
    assert!(stats.sizing_ramp >= 6);
    assert!(stats.sizing_default >= 1);
    assert!(stats.sizing_shutdown >= 1);
    assert_eq!(stats.finalize_events, 1);
    assert_eq!(stats.rgs_completed, 1);
}

/// EWMA (α = 0.8, recent-heavy: T' = 0.8·measured + 0.2·T): after the
/// throughput changes, the estimate must converge to the new measurement
/// geometrically (residual ×0.2 per observation).
#[test]
fn sizing_ewma_tracks_throughput_change() {
    let params = SizingParams { t_max_ns: 2_000_000, t_min_ns: 500_000 };
    let shared = crate::sizing::SizerShared::new();
    // Seed default state via a ramp task whose only morsel eats the whole
    // budget: T seeds SLOW (16 granules / 1.9 ms).
    let mut t = crate::sizing::TaskSizer::new(params, 16);
    let (c, d) = t.next_size(&shared, 1 << 40, 1);
    assert_eq!((c, d), (16, SizingDecision::Ramp));
    t.observe(&shared, 16, 1_900_000); // ramp ends, seeds T
    assert!(t.task_done());
    assert_eq!(shared.phase(), Phase::Default);
    let t0 = shared.throughput();
    let exact_seed = 16.0 / 1_900_000.0;
    assert!((t0 - exact_seed).abs() < 1e-12, "seed must be the last ramp morsel's throughput");

    // Follow-up tasks measure FASTER (4 µs per granule beats the seed's
    // ~119 µs per granule): EWMA pulls T up toward measured, 0.8 per step.
    let mut prev = t0;
    let measured = 1.0 / 4_000.0;
    for _ in 0..4 {
        let mut task = crate::sizing::TaskSizer::new(params, 16);
        let (size, d) = task.next_size(&shared, 1 << 40, 1);
        assert_eq!(d, SizingDecision::Default);
        assert!(size >= 1);
        task.observe(&shared, size, size * 4_000); // 4000 ns per granule
        assert!(task.task_done());
        let cur = shared.throughput();
        assert!(cur > prev, "EWMA must move toward faster measurements");
        let expect = 0.8 * measured + 0.2 * prev;
        assert!((cur - expect).abs() < 1e-12, "EWMA arithmetic must be exact");
        prev = cur;
    }
    assert!((prev - measured).abs() / measured < 0.05, "EWMA must converge (T={prev})");
}

/// Photo finish with W workers: once predicted remaining < W·t_max, sizes
/// become remaining/W (floored by t_min·T).
#[test]
fn sizing_photo_finish_divides_remainder() {
    let params = SizingParams { t_max_ns: 2_000_000, t_min_ns: 500_000 };
    let shared = crate::sizing::SizerShared::new();
    let mut t = crate::sizing::TaskSizer::new(params, 16);
    let _ = t.next_size(&shared, 1 << 40, 4);
    t.observe(&shared, 2000, 2_000_000); // seed T = 1 g/µs
    assert_eq!(shared.phase(), Phase::Default);

    // remaining 6000 granules = 6 ms < W(4)·t_max(2ms) = 8 ms ⇒ shutdown.
    let mut task = crate::sizing::TaskSizer::new(params, 16);
    let (size, d) = task.next_size(&shared, 6_000, 4);
    assert_eq!(d, SizingDecision::Shutdown);
    assert_eq!(size, 6_000 / 4); // remaining/W = 1500 > floor T·t_min = 500
    assert_eq!(shared.phase(), Phase::Shutdown);

    // Floor: tiny remainder still claims at least T·t_min granules.
    let mut task = crate::sizing::TaskSizer::new(params, 16);
    let (size, d) = task.next_size(&shared, 40, 4);
    assert_eq!(d, SizingDecision::Shutdown);
    assert_eq!(size, 500); // floor (claim path clamps to remaining)
}

// ---- boundary clamping ------------------------------------------------------

/// Claims never cross a hard (row-group / dictionary-epoch) boundary even
/// when the sizing state machine wants bigger morsels.
#[test]
fn claims_never_cross_boundaries() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    let total = 3_000u64;
    let every = 100u64;
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (_h, waiter) =
        rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::with_boundaries(total, every))));

    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();

    for r in work.claims.lock().unwrap().iter() {
        assert!(
            r.start / every == (r.end - 1) / every,
            "claim {r:?} crosses a boundary (every {every})"
        );
    }
}

// ---- protocol end-to-end over real threads ---------------------------------

/// Full pool: several RGs with dependency-DAG-ordered task sets, real
/// threads, standbys present. Every granule exactly once, finalize exactly
/// once per task set, deps respected, FIFO admission completes everything.
#[test]
fn pool_runs_dag_ordered_tasksets() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 4,
        standbys: 2,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    let order = Arc::new(Mutex::new(Vec::<(u64, usize)>::new()));

    struct OrderedWork {
        inner: Arc<SyntheticWork>,
        order: Arc<Mutex<Vec<(u64, usize)>>>,
        rg: u64,
        index: usize,
    }
    impl TaskSetWork for OrderedWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            self.inner.run_morsel(worker, range);
        }
        fn finalize(&self) {
            self.inner.finalize();
            self.order.lock().unwrap().push((self.rg, self.index));
        }
    }

    let mut waiters = Vec::new();
    let mut works: Vec<Vec<Arc<SyntheticWork>>> = Vec::new();
    for rg in 0..6u64 {
        let mut tasksets = Vec::new();
        let mut rg_works = Vec::new();
        // 0 ← 1 ← 2 chain (deps DAG-ordered).
        for index in 0..3usize {
            let total = 4_096u64;
            let inner = SyntheticWork::new(total, None, 0);
            rg_works.push(Arc::clone(&inner));
            tasksets.push(TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(total)),
                work: Arc::new(OrderedWork {
                    inner,
                    order: Arc::clone(&order),
                    rg,
                    index,
                }),
                deps: if index == 0 { vec![] } else { vec![index - 1] },
            });
        }
        works.push(rg_works);
        let (_h, waiter) = rt.submit(QuerySpec { query_id: rg, tasksets });
        waiters.push(waiter);
    }

    for w in &waiters {
        assert_eq!(w.wait(), RgOutcome::Completed);
    }
    pool.shutdown();

    for rg_works in &works {
        for w in rg_works {
            w.assert_all_executed_once();
            assert_eq!(w.finalizes.load(Ordering::SeqCst), 1, "finalize exactly once");
        }
    }
    // Dependency order within each RG.
    let order = order.lock().unwrap();
    for rg in 0..6u64 {
        let seq: Vec<usize> = order.iter().filter(|(g, _)| *g == rg).map(|&(_, i)| i).collect();
        assert_eq!(seq, vec![0, 1, 2], "task sets must finalize in dep order");
    }
    let stats = rt.stats();
    assert_eq!(stats.rgs_completed, 6);
    assert_eq!(stats.finalize_events, 18);
    assert_eq!(stats.tasksets_published, 18);
    assert_eq!(stats.tasksets_invalidated, 18);
}

/// Abort mid-flight: the waiter reports Aborted, no double-finalize, no
/// morsel of the aborted generation runs after its guard would fail, and
/// queued task sets never start.
#[test]
fn abort_completes_rg_as_aborted() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 4,
        standbys: 1,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    struct SlowWork {
        started: AtomicBool,
        second_ts_ran: Arc<AtomicBool>,
        mark_second: bool,
    }
    impl TaskSetWork for SlowWork {
        fn run_morsel(&self, _w: usize, _r: MorselRange) {
            if self.mark_second {
                self.second_ts_ran.store(true, Ordering::SeqCst);
            }
            self.started.store(true, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
        fn finalize(&self) {
            panic!("aborted RG must not run finalize work");
        }
    }

    let second_ts_ran = Arc::new(AtomicBool::new(false));
    let w0 = Arc::new(SlowWork {
        started: AtomicBool::new(false),
        second_ts_ran: Arc::clone(&second_ts_ran),
        mark_second: false,
    });
    let w1 = Arc::new(SlowWork {
        started: AtomicBool::new(false),
        second_ts_ran: Arc::clone(&second_ts_ran),
        mark_second: true,
    });
    let (handle, waiter) = rt.submit(QuerySpec {
        query_id: 9,
        tasksets: vec![
            TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1 << 20).with_c0(1)),
                work: w0.clone(),
                deps: vec![],
            },
            TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(1 << 20)),
                work: w1,
                deps: vec![0],
            },
        ],
    });

    while !w0.started.load(Ordering::SeqCst) {
        std::hint::spin_loop();
    }
    handle.abort();
    assert_eq!(waiter.wait(), RgOutcome::Aborted);
    assert!(!second_ts_ran.load(Ordering::SeqCst), "dependent task set ran after abort");
    pool.shutdown();

    let stats = rt.stats();
    assert_eq!(stats.rgs_aborted, 1);
    // Cleanup rode the ordinary protocol: the aborted set still finalized
    // (teardown event), but SlowWork::finalize (the work body) never ran —
    // it would have panicked.
    assert_eq!(stats.finalize_events, 1);
}

/// Aborting an RG that is still queued (no slot) completes it Aborted
/// without ever publishing its task sets.
#[test]
fn abort_while_queued_never_runs() {
    // Single slot forces queueing.
    let rt = Runtime::new(RuntimeConfig {
        workers: 2,
        standbys: 0,
        slots: 1,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    struct Busy {
        release: Arc<AtomicBool>,
    }
    impl TaskSetWork for Busy {
        fn run_morsel(&self, _w: usize, _r: MorselRange) {
            while !self.release.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_micros(10));
            }
        }
        fn finalize(&self) {}
    }

    let release = Arc::new(AtomicBool::new(false));
    let (_h1, wait1) = rt.submit(QuerySpec {
        query_id: 1,
        tasksets: vec![TaskSetSpec {
            source: Arc::new(SyntheticMorselSource::new(4)),
            work: Arc::new(Busy { release: Arc::clone(&release) }),
            deps: vec![],
        }],
    });

    let never = SyntheticWork::new(8, None, 0);
    let (h2, wait2) = rt.submit(spec_one(&never, Arc::new(SyntheticMorselSource::new(8))));
    assert!(wait2.try_wait().is_none(), "second RG must be queued (1 slot)");
    h2.abort();
    release.store(true, Ordering::SeqCst);

    assert_eq!(wait1.wait(), RgOutcome::Completed);
    assert_eq!(wait2.wait(), RgOutcome::Aborted);
    pool.shutdown();
    assert!(
        never.claims.lock().unwrap().is_empty(),
        "aborted-in-queue RG must never execute a morsel"
    );
}

// ---- generation-keyed lifecycle ---------------------------------------------

/// The pinned-interface semantics over the MERGED (lane A) lifecycle: a
/// closed (aborted) generation is unconsumable by construction, existing
/// participants drain, a retired generation never admits again, and only a
/// fresh generation (reinitialize) does. Mirrors the shim-era test through
/// the armed-participant API.
#[test]
fn lifecycle_semantics() {
    struct Owner;
    impl ParticipantOwner for Owner {
        fn permits_join(&self, _g: Generation) -> bool {
            true
        }
        fn request_stop(&self, _g: Generation) {}
        fn generation_stopped(&self, _g: Generation) -> bool {
            false
        }
    }

    let task = QueryTaskLifecycle::with_owner(Arc::new(Owner));
    let handle = task.publish().expect("fresh lifecycle publishes once");
    let g0 = handle.generation();

    // Normal join + operation.
    let participant = handle.join().expect("open generation admits");
    participant.run(|| Ok(())).expect("open generation runs operations");

    // Abort (cancel): new joins refuse immediately; the existing
    // participant's next operation refuses (its morsel-boundary signal) and
    // it drains. Idempotent: first recorded error wins.
    task.cancel(types_error::PgError::new(types_error::ERROR, "test abort").into());
    task.cancel(types_error::PgError::new(types_error::ERROR, "second abort dropped").into());
    assert!(handle.join().is_err(), "aborted generation admits nobody");
    assert!(participant.run(|| Ok(())).is_err(), "operations refuse after close");
    participant.complete().expect("drained participant completes");

    // Drain retires the generation and surfaces the FIRST recorded error.
    let err = task.close_and_wait().expect_err("abort error must surface");
    assert_eq!(err.message(), "test abort");
    assert!(handle.lifecycle().retired());

    // The dead generation stays unconsumable forever; publish refuses too
    // (single-publication rule) — reinitialize opens the next generation.
    assert!(handle.join().is_err());
    assert!(task.publish().is_err());
    let fresh = task.reinitialize().expect("reinitialize opens a new generation");
    assert_ne!(g0, fresh.generation());
    assert!(handle.join().is_err(), "stale handle cannot join the new generation");
    let participant = fresh.join().expect("new generation admits");
    participant.complete().unwrap();
    task.close_and_wait().unwrap();
}

// ---- permits / IoGuard ------------------------------------------------------

#[test]
fn io_guard_releases_and_reacquires() {
    let sem = Semaphore::new(2);
    sem.acquire();
    sem.acquire();
    assert_eq!(sem.available(), 0);
    {
        let _io = sem.io_section();
        // Declared blocking section: our permit is donatable.
        assert_eq!(sem.available(), 1);
        assert!(sem.try_acquire(), "standby can absorb the released permit");
        sem.release();
    }
    // Guard drop reacquired as ordinary contender.
    assert_eq!(sem.available(), 0);
    sem.release();
    sem.release();
    assert_eq!(sem.available(), 2);
}

/// Standby absorption end-to-end: 1 worker + 1 standby over 2 pool threads
/// and ONE permit. The worker's morsel enters an I/O section; completion of
/// the whole RG proves the standby could make progress on the released
/// permit while the blocked thread kept its pin (protocol unconfused —
/// exhaustive interleavings of this case live in the loom model).
#[test]
fn standby_absorbs_io_released_permit() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 1,
        standbys: 1,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });

    struct IoWork {
        rt: Mutex<Option<Arc<Runtime>>>,
        inner: Arc<SyntheticWork>,
        blocked_once: AtomicBool,
    }
    impl TaskSetWork for IoWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            if !self.blocked_once.swap(true, Ordering::SeqCst) {
                let rt = self.rt.lock().unwrap().clone().unwrap();
                let _io = rt.execution_permits().io_section();
                // Simulated blocking I/O: hold the section long enough for
                // the standby to claim morsels on the donated permit.
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            self.inner.run_morsel(worker, range);
        }
        fn finalize(&self) {
            self.inner.finalize();
        }
    }

    let total = 64u64;
    let inner = SyntheticWork::new(total, None, 0);
    let work = Arc::new(IoWork {
        rt: Mutex::new(Some(Arc::clone(&rt))),
        inner: Arc::clone(&inner),
        blocked_once: AtomicBool::new(false),
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let (_h, waiter) = rt.submit(QuerySpec {
        query_id: 1,
        tasksets: vec![TaskSetSpec {
            source: Arc::new(SyntheticMorselSource::new(total).with_c0(1)),
            work,
            deps: vec![],
        }],
    });
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    pool.shutdown();
    inner.assert_all_executed_once();
    assert_eq!(inner.finalizes.load(Ordering::SeqCst), 1);
}

// ---- kill switch / config ---------------------------------------------------

#[test]
fn kill_switch_defaults_off() {
    // The suite never sets PGRUST_RUNTIME; default must be OFF (M0: nothing
    // engages the runtime in production paths).
    assert!(!runtime_enabled());
}

#[test]
fn empty_rg_completes_immediately() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 1,
        standbys: 0,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });
    let (_h, waiter) = rt.submit(QuerySpec { query_id: 0, tasksets: vec![] });
    assert_eq!(waiter.wait(), RgOutcome::Completed);
}

#[test]
#[should_panic(expected = "DAG-ordered")]
fn forward_deps_rejected() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 1,
        standbys: 0,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });
    let w = SyntheticWork::new(1, None, 0);
    let _ = rt.submit(QuerySpec {
        query_id: 0,
        tasksets: vec![TaskSetSpec {
            source: Arc::new(SyntheticMorselSource::new(1)),
            work: w,
            deps: vec![0], // self/forward dep
        }],
    });
}
