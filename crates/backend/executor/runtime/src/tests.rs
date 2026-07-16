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

/// Claim-duration DOP scaling (tails192 #4): identity at W ≤ 32 (16-thread
/// behavior unchanged by construction), linear t_max ramp above, ×2.5 at
/// 191+, and the photo-finish floor t_min NEVER scales (end-game spread
/// posture preserved).
#[test]
fn sizing_dopscale_width_ramp() {
    let params = SizingParams { t_max_ns: 2_000_000, t_min_ns: 500_000 };
    // Identity band (includes the whole 16-core fleet + mt16 vectors).
    for w in [1u64, 4, 15, 16, 32] {
        let p = crate::sizing::dopscale_ramp(params, w);
        assert_eq!((p.t_max_ns, p.t_min_ns), (2_000_000, 500_000), "w={w} must be identity");
    }
    // Ramp: monotone in W, capped at ×2.5 from 191.
    let p96 = crate::sizing::dopscale_ramp(params, 96);
    let p191 = crate::sizing::dopscale_ramp(params, 191);
    let p256 = crate::sizing::dopscale_ramp(params, 256);
    assert!(p96.t_max_ns > 2_000_000 && p96.t_max_ns < p191.t_max_ns);
    assert_eq!(p191.t_max_ns, 5_000_000); // 2ms × 2.5
    assert_eq!(p256.t_max_ns, p191.t_max_ns, "cap at DOPSCALE_W1");
    // The photo-finish floor never scales.
    for p in [p96, p191, p256] {
        assert_eq!(p.t_min_ns, 500_000);
    }
    // Exact ramp arithmetic at w=112: frac=(112−32)/159, x=1+1.5·frac.
    let p112 = crate::sizing::dopscale_ramp(params, 112);
    let expect = (2_000_000f64 * (1.0 + 1.5 * (80.0 / 159.0))) as u64;
    assert_eq!(p112.t_max_ns, expect);
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

/// Whole-boundary claims (drive-scaling inc-2): an opted-in source's claims
/// each run boundary-to-boundary — per-epoch state is never split across
/// workers, regardless of what the duration-adaptive sizer wants.
#[test]
fn whole_boundary_claims_are_boundary_aligned() {
    struct WholeSource(SyntheticMorselSource);
    impl MorselSource for WholeSource {
        fn total_granules(&self) -> u64 {
            self.0.total_granules()
        }
        fn next_boundary_after(&self, start: u64) -> u64 {
            self.0.next_boundary_after(start)
        }
        fn startup_c0(&self) -> u64 {
            self.0.startup_c0()
        }
        fn whole_boundary_claims(&self) -> bool {
            true
        }
    }
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    let total = 3_000u64;
    let every = 100u64;
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (_h, waiter) = rt.submit(spec_one(
        &work,
        Arc::new(WholeSource(SyntheticMorselSource::with_boundaries(total, every))),
    ));

    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();

    for r in work.claims.lock().unwrap().iter() {
        assert!(
            r.start % every == 0 && (r.end % every == 0 || r.end == total),
            "claim {r:?} is not boundary-aligned (every {every})"
        );
        assert_eq!(r.end, ((r.start / every + 1) * every).min(total));
    }
}

/// Claim coalescing (dop1-tax fix 1): an opted-in whole-boundary source at
/// LOW live width claims SEVERAL epochs per morsel (default target 8 at one
/// worker) — but every claim stays boundary-ALIGNED, the Startup ramp stays
/// single-epoch (no stale-width giant claim can front-run a mid-query
/// widening: coalescing waits for the Default phase, by which point every
/// gang member's join is visible in active_workers), Shutdown (photo
/// finish) stays single-epoch, and every granule still executes exactly
/// once.
#[test]
fn coalesced_claims_are_boundary_aligned_and_multi_epoch() {
    struct CoalescingSource(SyntheticMorselSource);
    impl MorselSource for CoalescingSource {
        fn total_granules(&self) -> u64 {
            self.0.total_granules()
        }
        fn next_boundary_after(&self, start: u64) -> u64 {
            self.0.next_boundary_after(start)
        }
        fn startup_c0(&self) -> u64 {
            self.0.startup_c0()
        }
        fn whole_boundary_claims(&self) -> bool {
            true
        }
        fn coalesce_claims(&self) -> bool {
            true
        }
    }
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    let total = 20_000u64;
    let every = 100u64;
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (_h, waiter) = rt.submit(spec_one(
        &work,
        Arc::new(CoalescingSource(SyntheticMorselSource::with_boundaries(total, every))),
    ));

    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();

    let claims = work.claims.lock().unwrap();
    let mut multi = 0usize;
    for r in claims.iter() {
        assert!(
            r.start % every == 0 && (r.end % every == 0 || r.end == total),
            "claim {r:?} is not boundary-aligned (every {every})"
        );
        // Never past the coalescing target (default 8 epochs at 1 worker).
        assert!(
            r.end - r.start <= 8 * every,
            "claim {r:?} exceeds the coalescing target"
        );
        if r.end - r.start > every {
            multi += 1;
        }
    }
    // Startup-ramp claims precede any coalescing: the first claim is
    // exactly one epoch (the width signal is not yet trustworthy there).
    let first = &claims[0];
    assert_eq!((first.start, first.end), (0, every), "ramp claims must stay single-epoch");
    // Default-phase claims at one worker DID coalesce (the fix's point:
    // ~total/(8·every) Default claims instead of total/every).
    assert!(multi > 0, "no multi-epoch claim at DOP1 — coalescing never engaged");
    assert!(
        claims.len() < (total / every) as usize / 2,
        "coalescing must cut the claim count (got {})",
        claims.len()
    );
    // The photo-finish tail: the LAST claim is back to a single epoch
    // (Shutdown never coalesces).
    let last = claims.last().unwrap();
    assert!(
        last.end - last.start <= every,
        "shutdown claims must stay single-epoch, got {last:?}"
    );
}

/// Coalescing under a REAL pool (8 workers, real clock): whatever width the
/// claim path observes moment to moment, boundary alignment and
/// exactly-once execution hold — the widenability invariant's mechanical
/// half (the factor is a live active_workers read, re-evaluated per claim).
#[test]
fn coalesced_claims_with_pool_stay_exact() {
    struct CoalescingSource(SyntheticMorselSource);
    impl MorselSource for CoalescingSource {
        fn total_granules(&self) -> u64 {
            self.0.total_granules()
        }
        fn next_boundary_after(&self, start: u64) -> u64 {
            self.0.next_boundary_after(start)
        }
        fn whole_boundary_claims(&self) -> bool {
            true
        }
        fn coalesce_claims(&self) -> bool {
            true
        }
    }
    let rt = Runtime::new(RuntimeConfig {
        workers: 8,
        standbys: 2,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let total = 4_000u64;
    let every = 40u64;
    let work = SyntheticWork::new(total, None, 0);
    let (_h, waiter) = rt.submit(spec_one(
        &work,
        Arc::new(CoalescingSource(SyntheticMorselSource::with_boundaries(total, every))),
    ));
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();
    for r in work.claims.lock().unwrap().iter() {
        assert!(
            r.start % every == 0 && (r.end % every == 0 || r.end == total),
            "claim {r:?} is not boundary-aligned (every {every})"
        );
    }
    pool.shutdown();
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

// ---- spill blocking-section facade (M3.5 §6.1) --------------------------------

/// Unregistered thread: the facade is a no-op — no semaphore is touched
/// (there is none to touch) and the guard drop does nothing.
#[test]
fn blocking_facade_noop_off_pool_thread() {
    let s = crate::blocking_io_section();
    drop(s);
    // And nested/no-guard sequencing is harmless.
    let a = crate::blocking_io_section();
    let b = crate::blocking_io_section();
    drop(a);
    drop(b);
}

/// Registered thread holding a permit: the facade donates it for the
/// section and reacquires on drop; after the registration guard drops the
/// facade reverts to no-op.
#[test]
fn blocking_facade_arms_on_registered_thread() {
    let sem = Semaphore::new(1);
    sem.acquire();
    {
        // SAFETY: sem outlives the guard; we hold its permit.
        let _reg = unsafe { crate::blocking::PermitThreadReg::new(&sem) };
        {
            let _s = crate::blocking_io_section();
            assert_eq!(sem.available(), 1, "permit donated for the section");
            assert!(sem.try_acquire(), "standby can absorb it");
            sem.release();
        }
        assert_eq!(sem.available(), 0, "permit reacquired at section end");
    }
    // Registration gone: facade is a no-op again.
    {
        let _s = crate::blocking_io_section();
        assert_eq!(sem.available(), 0);
    }
    sem.release();
    assert_eq!(sem.available(), 1);
}

/// End-to-end through the pool: a task body calls the facade with NO
/// runtime reference (the spill-substrate call shape); the pool loop's
/// registration makes it donate the worker's permit, and the standby
/// absorbs it (the RG completes with 1 worker + 1 standby over one permit
/// while the first morsel blocks inside the section).
#[test]
fn blocking_facade_end_to_end_in_pool_task() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 1,
        standbys: 1,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });

    struct FacadeIoWork {
        inner: Arc<SyntheticWork>,
        blocked_once: AtomicBool,
    }
    impl TaskSetWork for FacadeIoWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            if !self.blocked_once.swap(true, Ordering::SeqCst) {
                let _io = crate::blocking_io_section();
                // Simulated blocking spill I/O: hold the section long
                // enough for the standby to claim morsels on the donated
                // permit.
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
    let work = Arc::new(FacadeIoWork {
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
fn kill_switch_defaults_on() {
    // The suite never sets PGRUST_RUNTIME; since the M5 boarding flip the
    // pool defaults ON (PGRUST_RUNTIME=0 is the kill switch — the OnceLock
    // cache makes the killed branch a boot-time property, exercised by the
    // fleet batteries that boot with the switch thrown).
    assert!(runtime_enabled());
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

// ---- M1 pinned submission + external participant drive ---------------------

/// Pinned RGs are invisible to the pool: external drivers execute every
/// granule exactly once, finalize exactly once, and the pool workers never
/// claim a morsel (their task counter stays zero).
#[test]
fn pinned_rg_runs_on_external_drivers_only() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 4,
        standbys: 1,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    // Real pool spun up — it must IGNORE the pinned RG.
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    let total = 10_000u64;
    let work = SyntheticWork::new(total, None, 0);
    let (handle, waiter) = rt.submit_pinned(spec_one(
        &work,
        Arc::new(SyntheticMorselSource::with_boundaries(total, 8)),
    ));

    let mut joins = Vec::new();
    for ordinal in 0..3usize {
        let rt2 = Arc::clone(&rt);
        let h = handle.clone();
        joins.push(std::thread::spawn(move || {
            let mut local = rt2.external_local(ordinal);
            rt2.drive_pinned(&mut local, &h)
        }));
    }
    for j in joins {
        assert_eq!(j.join().unwrap(), RgOutcome::Completed);
    }
    assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
    work.assert_all_executed_once();
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);
    // Boundary contract: no claim crosses a multiple of 8.
    for r in work.claims.lock().unwrap().iter() {
        assert_eq!(r.start / 8, (r.end - 1) / 8, "claim {r:?} crossed a boundary");
    }
    pool.shutdown();
    let stats = rt.stats();
    // Every task was claimed by an external lane, none by the pool: the
    // per-RG counter equals the global one (only this RG ran), and the pool
    // parked without work.
    assert_eq!(stats.rgs_completed, 1);
}

/// Aborting a pinned RG with NO external driver still completes once a
/// driver drains it (the leader's reap path: closed generation refuses
/// every join, so the drive runs pure protocol cleanup).
#[test]
fn pinned_rg_abort_reaped_by_driver() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 2,
        standbys: 1,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });
    let total = 1_000u64;
    let work = SyntheticWork::new(total, None, 0);
    let (handle, waiter) = rt.submit_pinned(spec_one(
        &work,
        Arc::new(SyntheticMorselSource::new(total)),
    ));
    handle.abort();
    assert_eq!(waiter.try_wait(), None, "nobody drove the pinned RG yet");
    let mut local = rt.external_local(0);
    assert_eq!(rt.drive_pinned(&mut local, &handle), RgOutcome::Aborted);
    assert_eq!(waiter.try_wait(), Some(RgOutcome::Aborted));
    // The closed generation refused every join: no morsel ran.
    assert!(work.claims.lock().unwrap().is_empty());
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 0, "finalize work must not run");
}

/// External-lane leases are process-exclusive: 64 concurrent leases exhaust
/// the mask, drop releases, and every lease maps to a distinct lane.
#[test]
fn external_lane_leases_are_exclusive() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 2,
        standbys: 0,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });
    let mut lanes = Vec::new();
    for _ in 0..MAX_EXTERNAL_LANES {
        lanes.push(rt.acquire_external_lane().expect("lane available"));
    }
    let mut seen: Vec<usize> = lanes.iter().map(|l| l.ordinal()).collect();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), MAX_EXTERNAL_LANES, "lanes must be distinct");
    assert!(rt.acquire_external_lane().is_none(), "mask exhausted");
    lanes.pop();
    let again = rt.acquire_external_lane().expect("released lane reusable");
    assert_eq!(again.ordinal(), MAX_EXTERNAL_LANES - 1);
}

/// DOP-192 readiness smoke: 192 concurrent EXTERNAL pinned drivers — every
/// helper leases a real lane (the production path, exercising the widened
/// multi-word lease mask past the old 64-lane ceiling) and drives one
/// pinned RG to completion. Threads multiplex on however many cores the
/// host has; correctness (exactly-once, single finalize) must hold.
#[test]
fn pinned_rg_192_external_drivers_exactly_once() {
    const DRIVERS: usize = 192;
    assert!(DRIVERS <= MAX_EXTERNAL_LANES, "test premise: lanes cover dop-192");
    let rt = Runtime::new(RuntimeConfig {
        workers: 4,
        standbys: 1,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let total = 50_000u64;
    let work = SyntheticWork::new(total, None, 0);
    let (handle, waiter) = rt.submit_pinned(spec_one(
        &work,
        Arc::new(SyntheticMorselSource::with_boundaries(total, 8)),
    ));
    let mut joins = Vec::new();
    for _ in 0..DRIVERS {
        let rt2 = Arc::clone(&rt);
        let h = handle.clone();
        joins.push(std::thread::spawn(move || {
            let lane = rt2.acquire_external_lane().expect("192 lanes must be available");
            let mut local = lane.local();
            rt2.drive_pinned(&mut local, &h)
        }));
    }
    for j in joins {
        assert_eq!(j.join().unwrap(), RgOutcome::Completed);
    }
    assert_eq!(waiter.try_wait(), Some(RgOutcome::Completed));
    work.assert_all_executed_once();
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);
}

/// DOP-192 readiness smoke: a 192-worker POOL (declared workers far above
/// the host's cores — the threads multiplex) runs a submitted RG
/// exactly-once. Pool sizing has no hard cap; this pins that at 192.
#[test]
fn pool_192_workers_exactly_once() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 192,
        standbys: 2,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let total = 50_000u64;
    let work = SyntheticWork::new(total, None, 0);
    let (_h, waiter) =
        rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);
    pool.shutdown();
}

/// DOP-192 readiness K-sweep: "K" here = the standby count (the §2.8
/// permit model; the M5 stride fields are inert). 64 standbys against 8
/// permits — far past the tested K=2 — must park cleanly and never break
/// exactly-once or finalize-once.
#[test]
fn pool_k64_standbys_exactly_once() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 8,
        standbys: 64,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let total = 50_000u64;
    let work = SyntheticWork::new(total, None, 0);
    let (_h, waiter) =
        rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);
    pool.shutdown();
}

// ---- M1 §2.9: uring worker-loop duties (rings, boundary reap, IoGuard seams)

/// Counting stand-ins for aio_uring's seam impls. aio_uring is not linked
/// into this test binary, so the slots are ours to install (once —
/// process-global; assertions below are deltas/floors because parallel
/// tests' pools also drive them).
mod uring_stub {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static RING_INITS: AtomicU64 = AtomicU64::new(0);
    pub static RING_TEARDOWNS: AtomicU64 = AtomicU64::new(0);
    pub static BOUNDARY_REAPS: AtomicU64 = AtomicU64::new(0);
    pub const RING_ID: i32 = 7;

    pub fn install() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            aio_seams::uring_worker_ring_init::set(|| {
                RING_INITS.fetch_add(1, Ordering::SeqCst);
                RING_ID
            });
            aio_seams::uring_worker_ring_teardown::set(|| {
                RING_TEARDOWNS.fetch_add(1, Ordering::SeqCst);
            });
            aio_seams::uring_boundary_reap::set(|| {
                BOUNDARY_REAPS.fetch_add(1, Ordering::SeqCst);
            });
        });
    }
}

/// Worker start creates + registers the ring with the runtime worker
/// struct; worker exit tears it down and clears the registration; the loop
/// boundary-reaps at task boundaries.
#[test]
fn worker_loop_ring_lifecycle_and_boundary_reap() {
    uring_stub::install();
    let inits0 = uring_stub::RING_INITS.load(Ordering::SeqCst);
    let teardowns0 = uring_stub::RING_TEARDOWNS.load(Ordering::SeqCst);

    let rt = Runtime::new(RuntimeConfig {
        workers: 2,
        standbys: 1,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    // Ring registration: every worker (standbys included) publishes its
    // ring id at loop entry.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while (0..rt.nthreads()).any(|w| rt.worker_ring(w).is_none()) {
        assert!(std::time::Instant::now() < deadline, "ring registration timed out");
        std::thread::yield_now();
    }
    for w in 0..rt.nthreads() {
        assert_eq!(rt.worker_ring(w), Some(uring_stub::RING_ID as u32));
    }
    assert!(uring_stub::RING_INITS.load(Ordering::SeqCst) >= inits0 + 3);

    // Boundary reaping: run an RG; every task boundary reaps, so the count
    // must move while the query executes.
    let reaps0 = uring_stub::BOUNDARY_REAPS.load(Ordering::SeqCst);
    let total = 4_096u64;
    let work = SyntheticWork::new(total, None, 0);
    let (_h, waiter) = rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    work.assert_all_executed_once();
    assert!(
        uring_stub::BOUNDARY_REAPS.load(Ordering::SeqCst) > reaps0,
        "task boundaries must reap"
    );

    // Worker exit: teardown runs, registration clears.
    pool.shutdown();
    assert!(uring_stub::RING_TEARDOWNS.load(Ordering::SeqCst) >= teardowns0 + 3);
    for w in 0..rt.nthreads() {
        assert_eq!(rt.worker_ring(w), None, "exit must clear the ring registration");
    }
}

/// The §2.8/§2.9 IoGuard seam pair: a pool worker blocking inside a task
/// donates its permit through `io_permit_release` (a standby absorbs the
/// core — with ONE permit, the second granule can only run while the first
/// holder is inside the released section; the test deadlocks if the wiring
/// is broken) and reacquires on `io_permit_reacquire`. Non-workers get
/// `false` (the no-permit contract).
#[test]
fn io_permit_seams_donate_core_to_standby() {
    uring_stub::install();
    let rt = Runtime::new(RuntimeConfig {
        workers: 1, // exactly one execution permit
        standbys: 1,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });

    struct BlockingIoWork {
        inner: Arc<SyntheticWork>,
        release_seen: Arc<AtomicBool>,
        tx: Mutex<std::sync::mpsc::Sender<()>>,
        rx: Mutex<std::sync::mpsc::Receiver<()>>,
        blocked_once: AtomicBool,
    }
    impl TaskSetWork for BlockingIoWork {
        fn run_morsel(&self, worker: usize, range: MorselRange) {
            if range.start == 0 && !self.blocked_once.swap(true, Ordering::SeqCst) {
                // Declared blocking section via the seams (what aio_uring's
                // genuinely-pending wait does): release, block, reacquire.
                assert!(
                    aio_seams::io_permit_release::call(),
                    "a permit-holding worker must be allowed to release"
                );
                self.release_seen.store(true, Ordering::SeqCst);
                // Blocked: the ONLY permit is free; granule 1 must run.
                self.rx
                    .lock()
                    .unwrap()
                    .recv()
                    .expect("peer granule must run while we block");
                aio_seams::io_permit_reacquire::call();
            } else {
                // The peer granule: only reachable on the donated permit.
                self.tx.lock().unwrap().send(()).unwrap();
            }
            self.inner.run_morsel(worker, range);
        }
        fn finalize(&self) {
            self.inner.finalize();
        }
    }

    let total = 2u64;
    let inner = SyntheticWork::new(total, None, 0);
    let release_seen = Arc::new(AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel();
    let work = Arc::new(BlockingIoWork {
        inner: Arc::clone(&inner),
        release_seen: Arc::clone(&release_seen),
        tx: Mutex::new(tx),
        rx: Mutex::new(rx),
        blocked_once: AtomicBool::new(false),
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let (_h, waiter) = rt.submit(QuerySpec {
        query_id: 9,
        tasksets: vec![TaskSetSpec {
            // c0=1: granule-sized morsels, so granules 0 and 1 are separate
            // claims and can land on different workers.
            source: Arc::new(SyntheticMorselSource::new(total).with_c0(1)),
            work,
            deps: vec![],
        }],
    });
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    pool.shutdown();
    inner.assert_all_executed_once();
    assert!(release_seen.load(Ordering::SeqCst));

    // Contract: a thread that is not a permit-holding pool worker must get
    // false (and must then NOT call reacquire).
    assert!(!aio_seams::io_permit_release::call());
}

// ---- M4 maintenance class (docs/design/m4-bgjobs.md §3.5) -------------------

/// The starvation floor, deterministically: a Maintenance RG submitted while
/// a Foreground RG occupies a LOWER slot index is picked at the very next
/// task boundary — one worker_step — and completes while the foreground RG
/// is still incomplete. Under pure FIFO pick it would wait for the whole
/// foreground RG.
#[test]
fn maintenance_preferred_over_foreground_fifo() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    let total = 16_000u64;
    let fg = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (_h, fg_waiter) = rt.submit(spec_one(&fg, Arc::new(SyntheticMorselSource::new(total))));

    let mut local = rt.worker_local(0);
    // One foreground task runs (slot 0 is the only active slot).
    assert_eq!(rt.worker_step(&mut local), Step::Ran);
    assert!(fg_waiter.try_wait().is_none(), "foreground must not be complete yet");

    // A job cycle arrives: single-granule task set, higher slot index.
    let mt = SyntheticWork::new(1, Some(Arc::clone(&clock)), 1_000);
    let (_mh, mt_waiter) = rt.submit_maintenance(spec_one(&mt, Arc::new(SyntheticMorselSource::new(1))));

    // The very next step must pick the maintenance slot (preference beats
    // the lower-index foreground slot) and drive it to completion.
    assert_eq!(rt.worker_step(&mut local), Step::Ran);
    assert_eq!(mt_waiter.try_wait(), Some(RgOutcome::Completed), "maintenance cycle must complete at the first boundary");
    assert!(fg_waiter.try_wait().is_none(), "foreground still has granules");
    mt.assert_all_executed_once();

    // Drain the foreground RG; nothing is lost.
    while fg_waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(fg_waiter.wait(), RgOutcome::Completed);
    fg.assert_all_executed_once();
    assert_eq!(rt.stats().rgs_completed, 2);
}

/// Queue overtake: with every slot busy, a Maintenance RG goes to the FRONT
/// of the wait queue — it is admitted before an earlier-queued Foreground RG.
#[test]
fn maintenance_overtakes_wait_queue() {
    let clock = Arc::new(VirtualClock::new());
    let mut cfg = RuntimeConfig::new(1);
    cfg.slots = 1;
    let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);

    let fg1 = SyntheticWork::new(64, Some(Arc::clone(&clock)), 1_000);
    let (_h1, w1) = rt.submit(spec_one(&fg1, Arc::new(SyntheticMorselSource::new(64))));
    let fg2 = SyntheticWork::new(64, Some(Arc::clone(&clock)), 1_000);
    let (_h2, w2) = rt.submit(spec_one(&fg2, Arc::new(SyntheticMorselSource::new(64))));
    let mt = SyntheticWork::new(1, Some(Arc::clone(&clock)), 1_000);
    let (_mh, mw) = rt.submit_maintenance(spec_one(&mt, Arc::new(SyntheticMorselSource::new(1))));

    let mut local = rt.worker_local(0);
    // Drive fg1 to completion; the released slot must admit the maintenance
    // RG (queue front), NOT fg2.
    while w1.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    while mw.try_wait().is_none() {
        assert!(w2.try_wait().is_none(), "fg2 must not complete before the overtaking maintenance RG");
        rt.worker_step(&mut local);
    }
    assert_eq!(mw.try_wait(), Some(RgOutcome::Completed));
    assert!(w2.try_wait().is_none(), "fg2 admitted after the maintenance RG");
    while w2.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    fg1.assert_all_executed_once();
    fg2.assert_all_executed_once();
    mt.assert_all_executed_once();
}

// ---- M5-4 stride/fair-share activation (m5-planner §3, inter-query §5.3) ----

/// Single-RG bit-identity anchor (in-crate half of the M5-4 proof; the
/// server-level half is regress-diff + select1 on fleet): with exactly one
/// active RG, the stride pick is FORCED — the claim sequence is identical
/// between stride ON and OFF, morsel for morsel.
#[test]
fn stride_single_rg_claim_sequence_identical() {
    let run = |stride: bool| -> Vec<Range<u64>> {
        let clock = Arc::new(VirtualClock::new());
        let rt = virtual_runtime(1, &clock);
        rt.set_stride(stride);
        let total = 16_000u64;
        let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
        let (_h, waiter) =
            rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        work.assert_all_executed_once();
        let claims = work.claims.lock().unwrap();
        claims.clone()
    };
    let on = run(true);
    let off = run(false);
    assert_eq!(on, off, "single-RG claim sequence must be identical under stride");
}

/// Kill switch: PGRUST_RUNTIME_STRIDE=0 (here per-instance set_stride(false))
/// restores the M0 FIFO pick — with two active RGs and one worker, RG A runs
/// to completion before RG B's first claim.
#[test]
fn stride_off_is_fifo_two_rgs() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(false);
    let a = SyntheticWork::new(8_000, Some(Arc::clone(&clock)), 1_000);
    let (_ha, wa) = rt.submit(spec_one(&a, Arc::new(SyntheticMorselSource::new(8_000))));
    let b = SyntheticWork::new(8_000, Some(Arc::clone(&clock)), 1_000);
    let (_hb, wb) = rt.submit(spec_one(&b, Arc::new(SyntheticMorselSource::new(8_000))));
    let mut local = rt.worker_local(0);
    while wa.try_wait().is_none() {
        rt.worker_step(&mut local);
        if wa.try_wait().is_none() {
            assert!(
                b.claims.lock().unwrap().is_empty(),
                "FIFO (stride off) must not interleave RG B before A completes"
            );
        }
    }
    while wb.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    a.assert_all_executed_once();
    b.assert_all_executed_once();
}

/// The K-sweep proportional-share gate at equal shares (§3.4 law, M5-4
/// scope): K concurrent equal-priority RGs on one deterministic worker —
/// after a fixed number of task boundaries with ALL RGs still active, each
/// RG's cpu_consumed_ns share error is within one task quantum of 1/K.
/// (Deterministic virtual clock: the numbers below are exact, not flaky —
/// they are the in-band evidence the close-out gate table cites.)
#[test]
fn stride_equal_share_k_sweep() {
    for k in [2usize, 3, 4, 8, 16] {
        let clock = Arc::new(VirtualClock::new());
        let mut cfg = RuntimeConfig::new(1);
        cfg.slots = 32;
        let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);
        rt.set_stride(true);
        let total = 400_000u64; // 400 ms of virtual CPU per RG — nobody finishes
        let mut works = Vec::new();
        let mut handles = Vec::new();
        let mut waiters = Vec::new();
        for q in 0..k {
            let w = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
            let (h, waiter) = rt.submit(QuerySpec {
                query_id: q as u64 + 1,
                tasksets: vec![TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(total)),
                    work: Arc::clone(&w) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                }],
            });
            works.push(w);
            handles.push(h);
            waiters.push(waiter);
        }
        // 20 task boundaries per RG at perfect fairness.
        let mut local = rt.worker_local(0);
        for _ in 0..(20 * k) {
            assert_eq!(rt.worker_step(&mut local), Step::Ran);
        }
        for w in &waiters {
            assert!(w.try_wait().is_none(), "K-sweep RGs must all still be active");
        }
        let cpus: Vec<u64> = handles.iter().map(|h| h.cpu_consumed_ns()).collect();
        let sum: u64 = cpus.iter().sum();
        let mean = sum as f64 / k as f64;
        let mut max_err = 0f64;
        for c in &cpus {
            max_err = max_err.max((*c as f64 - mean).abs() / mean);
        }
        // One task quantum ≈ t_max (2 ms) against ≈ 20 quanta ⇒ ≤ ~5%; the
        // startup ramp spends its smaller morsels equally across RGs.
        assert!(
            max_err <= 0.06,
            "K={k}: proportional-share error {max_err:.4} out of band; cpus={cpus:?}"
        );
        eprintln!("stride K-sweep K={k}: max share error {max_err:.4} (cpu ns: {cpus:?})");
        // Drain: abort everything and drive the cleanup to completion.
        for h in &handles {
            h.abort();
        }
        for w in &waiters {
            while w.try_wait().is_none() {
                rt.worker_step(&mut local);
            }
        }
    }
}

/// M5-5 decay trajectory + floor clamp (inter-query §5.4): a single RG's
/// priority follows p(q) = max(p_min, p0·λ^q) exactly as its consumed CPU
/// crosses decay-quantum boundaries, and clamps at the p_min floor (reached
/// at q=4 with the ratified λ=1/2, p_min=p0/16).
#[test]
fn decay_priority_trajectory_and_floor() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_decay(true);
    let qn = 4_000_000u64; // 4 ms CPU per decay quantum (test-tightened)
    rt.set_decay_quantum_ns(qn);
    assert_eq!(rt.p_min(), 625, "ratified default floor = p0/16");
    let total = 400_000u64; // long-lived: never finishes inside the loop
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (h, waiter) = rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
    assert_eq!(h.priority(), 10_000, "fresh RG holds p0");
    let mut local = rt.worker_local(0);
    let mut reached_floor_at_q = None;
    while h.cpu_consumed_ns() < 10 * qn {
        assert_eq!(rt.worker_step(&mut local), Step::Ran);
        let q = (h.cpu_consumed_ns() / qn).min(63) as i32;
        let expect = ((10_000f64 * 0.5f64.powi(q)) as u32).max(625);
        assert_eq!(
            h.priority(),
            expect,
            "after q={q} quanta ({}ns cpu) priority must be max(p_min, p0·λ^q)",
            h.cpu_consumed_ns()
        );
        if expect == 625 && reached_floor_at_q.is_none() {
            reached_floor_at_q = Some(q);
        }
    }
    assert_eq!(reached_floor_at_q, Some(4), "λ=1/2 floors p0/16 at exactly 4 quanta");
    assert_eq!(h.priority(), 625, "floor holds: later quanta never lower it further");
    h.abort();
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
}

/// M5-5 kill switch: decay OFF pins every RG at p0 regardless of consumed
/// CPU — the M5-4 equal-shares scheduler exactly.
#[test]
fn decay_off_keeps_p0() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_decay(false);
    rt.set_decay_quantum_ns(1_000_000);
    let total = 100_000u64;
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (h, waiter) = rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
        assert_eq!(h.priority(), 10_000, "decay off: priority never moves");
    }
    assert!(h.cpu_consumed_ns() >= 50_000_000, "consumed far past many quanta");
}

/// M5-5 single-RG bit-identity (re-proof at this increment): with one RG
/// the pick is forced before any pass/stride read, so decay ON vs OFF must
/// produce the identical claim sequence, morsel for morsel.
#[test]
fn decay_single_rg_claim_sequence_identical() {
    let run = |decay: bool| -> Vec<Range<u64>> {
        let clock = Arc::new(VirtualClock::new());
        let rt = virtual_runtime(1, &clock);
        rt.set_stride(true);
        rt.set_decay(decay);
        rt.set_decay_quantum_ns(1_000_000); // aggressive: many boundaries
        let total = 16_000u64;
        let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
        let (_h, waiter) =
            rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        work.assert_all_executed_once();
        let claims = work.claims.lock().unwrap();
        claims.clone()
    };
    assert_eq!(
        run(true),
        run(false),
        "single-RG claim sequence must be identical under decay"
    );
}

/// M5-5 K-sweep invariance: with decay ACTIVE (boundaries crossed many
/// times), K equal RGs consume equally, decay identically, and keep equal
/// shares — the M5-4 proportional-share gate holds through live decay.
#[test]
fn decay_equal_consumption_keeps_equal_shares() {
    for k in [2usize, 4, 8] {
        let clock = Arc::new(VirtualClock::new());
        let mut cfg = RuntimeConfig::new(1);
        cfg.slots = 32;
        let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);
        rt.set_stride(true);
        rt.set_decay(true);
        rt.set_decay_quantum_ns(2_000_000); // one boundary per ~task
        let total = 400_000u64;
        let mut handles = Vec::new();
        let mut waiters = Vec::new();
        for q in 0..k {
            let w = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
            let (h, waiter) = rt.submit(QuerySpec {
                query_id: q as u64 + 1,
                tasksets: vec![TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(total)),
                    work: Arc::clone(&w) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                }],
            });
            handles.push(h);
            waiters.push(waiter);
        }
        let mut local = rt.worker_local(0);
        for _ in 0..(20 * k) {
            assert_eq!(rt.worker_step(&mut local), Step::Ran);
        }
        for h in &handles {
            assert!(h.priority() < 10_000, "decay must have engaged (boundaries crossed)");
        }
        let cpus: Vec<u64> = handles.iter().map(|h| h.cpu_consumed_ns()).collect();
        let mean = cpus.iter().sum::<u64>() as f64 / k as f64;
        let mut max_err = 0f64;
        for c in &cpus {
            max_err = max_err.max((*c as f64 - mean).abs() / mean);
        }
        assert!(
            max_err <= 0.06,
            "K={k}: share error {max_err:.4} out of band under live decay; cpus={cpus:?}"
        );
        eprintln!("decay K-sweep K={k}: max share error {max_err:.4}");
        for h in &handles {
            h.abort();
        }
        for w in &waiters {
            while w.try_wait().is_none() {
                rt.worker_step(&mut local);
            }
        }
    }
}

/// M5-5 starvation bound + share skew (the §3.4 floor law, unit form): a
/// batch RG decayed to the p_min floor against a fresh p0 arrival must
/// (a) keep receiving service with a bounded gap between its tasks —
/// ≈ p0/p_min boundaries, the starvation bound the lock-wait edge relies
/// on — and (b) hold a CPU share ≈ p_min/(p_min+p0) = 1/17.
#[test]
fn decay_starvation_floor_share_skew() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_decay(true);
    rt.set_decay_quantum_ns(2_000_000);
    let total = 40_000_000u64; // 40 s virtual CPU: nobody finishes
    // Phase 1: batch B runs alone and decays to the floor.
    let b = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (hb, wb) = rt.submit(spec_one(&b, Arc::new(SyntheticMorselSource::new(total))));
    let mut local = rt.worker_local(0);
    while hb.priority() > rt.p_min() {
        assert_eq!(rt.worker_step(&mut local), Step::Ran);
    }
    assert_eq!(hb.priority(), 625);
    // Freeze further decay (quantum → ∞) so the fresh arrival keeps p0:
    // persistent adversarial skew, the §3.4 worst case.
    rt.set_decay_quantum_ns(u64::MAX);
    let a = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
    let (ha, wa) = rt.submit(spec_one(&a, Arc::new(SyntheticMorselSource::new(total))));
    assert_eq!(ha.priority(), 10_000);
    // Phase 2: 340 task boundaries ≈ 20 rotations of the 17-slot cycle.
    let b_start = hb.cpu_consumed_ns();
    let a_start = ha.cpu_consumed_ns();
    let mut last_b = b_start;
    let mut gap = 0u32;
    let mut max_gap = 0u32;
    const BOUNDARIES: u32 = 340;
    for _ in 0..BOUNDARIES {
        assert_eq!(rt.worker_step(&mut local), Step::Ran);
        let bc = hb.cpu_consumed_ns();
        if bc > last_b {
            last_b = bc;
            gap = 0;
        } else {
            gap += 1;
            max_gap = max_gap.max(gap);
        }
    }
    // (a) Starvation bound: the floor guarantees B a task at least every
    // ~p0/p_min = 16 boundaries; allow ramp slack.
    assert!(
        max_gap <= 24,
        "starvation bound violated: floor RG waited {max_gap} boundaries (p0/p_min=16)"
    );
    // (b) Proportional skew: B's share ≈ p_min/(p_min+p0) = 1/17 ≈ 0.0588.
    let b_cpu = (hb.cpu_consumed_ns() - b_start) as f64;
    let a_cpu = (ha.cpu_consumed_ns() - a_start) as f64;
    let b_share = b_cpu / (b_cpu + a_cpu);
    assert!(
        (0.035..=0.085).contains(&b_share),
        "floor share {b_share:.4} outside the ≈1/17 band (b={b_cpu} a={a_cpu})"
    );
    eprintln!("decay skew: floor share {b_share:.4}, max service gap {max_gap} boundaries");
    ha.abort();
    hb.abort();
    while wa.try_wait().is_none() || wb.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
}

/// M5-5 latency payoff (the §3.4 MLFQ effect, unit form): a fresh short
/// query submitted against a decayed 6-query batch background completes in
/// far fewer task boundaries with decay ON (batch at the floor: the short
/// query holds ~73% of the pool) than with decay OFF (equal shares: 1/7).
/// Runs at the RATIFIED constants (50ms quantum, λ=1/2, p_min=625): the
/// 20ms short query is sub-quantum by definition and never decays.
#[test]
fn decay_short_query_latency_under_batch_background() {
    let run = |decay: bool| -> u32 {
        let clock = Arc::new(VirtualClock::new());
        let rt = virtual_runtime(1, &clock);
        rt.set_stride(true);
        rt.set_decay(decay);
        let batch_total = 40_000_000u64;
        let mut batch = Vec::new();
        let mut batch_waiters = Vec::new();
        for q in 0..6u64 {
            let w = SyntheticWork::new(batch_total, Some(Arc::clone(&clock)), 1_000);
            let (h, wt) = rt.submit(QuerySpec {
                query_id: q + 1,
                tasksets: vec![TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(batch_total)),
                    work: Arc::clone(&w) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                }],
            });
            batch.push(h);
            batch_waiters.push(wt);
        }
        let mut local = rt.worker_local(0);
        // Warm the background past the decay horizon: each batch RG needs
        // 4 quanta = 200ms CPU to floor; 6 RGs × ~100 2ms-tasks ⇒ ~600
        // boundaries (virtual clock: instant). Decay ON: all six settle to
        // the floor; OFF: they stay at p0.
        for _ in 0..700 {
            assert_eq!(rt.worker_step(&mut local), Step::Ran);
        }
        if decay {
            for h in &batch {
                assert_eq!(h.priority(), 625, "background must sit at the floor");
            }
        }
        // Submit the short interactive query (≈ 10 tasks of work) and count
        // boundaries until it completes.
        let short = SyntheticWork::new(20_000, Some(Arc::clone(&clock)), 1_000);
        let (_hs, ws) = rt.submit(QuerySpec {
            query_id: 99,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(20_000)),
                work: Arc::clone(&short) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });
        let mut boundaries = 0u32;
        while ws.try_wait().is_none() {
            rt.worker_step(&mut local);
            boundaries += 1;
            assert!(boundaries < 10_000, "short query starved");
        }
        for h in &batch {
            h.abort();
        }
        for wt in &batch_waiters {
            while wt.try_wait().is_none() {
                rt.worker_step(&mut local);
            }
        }
        boundaries
    };
    let on = run(true);
    let off = run(false);
    assert!(
        on * 2 < off,
        "MLFQ effect missing: short query took {on} boundaries with decay vs {off} without"
    );
    eprintln!("decay latency: short query {on} boundaries (decay on) vs {off} (off)");
}

/// Session-affine stickiness as pick-tiebreaker (§5.2; ceremony-v2
/// mechanism): at EQUAL pass, a worker sticky-bound to RG B's leader session
/// picks B's slot over the lower-index equal-pass slot of RG A. Equal-pass
/// ONLY (the design's §10 default): once A's pass falls behind, A is picked
/// regardless of affinity.
#[test]
fn stride_session_affinity_tiebreak() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    let a = SyntheticWork::new(8_000, Some(Arc::clone(&clock)), 1_000);
    let (_ha, _wa) = rt.submit_with_affinity(
        spec_one(&a, Arc::new(SyntheticMorselSource::new(8_000))),
        7,
    );
    let b = SyntheticWork::new(8_000, Some(Arc::clone(&clock)), 1_000);
    let (_hb, _wb) = rt.submit_with_affinity(
        spec_one(&b, Arc::new(SyntheticMorselSource::new(8_000))),
        9,
    );
    let mut local = rt.worker_local(0);
    local.set_session_token(9);
    // Both slots joined at watermark 0: equal pass ⇒ affinity must win the
    // tie and claim from B first.
    assert_eq!(rt.worker_step(&mut local), Step::Ran);
    assert!(
        a.claims.lock().unwrap().is_empty() && !b.claims.lock().unwrap().is_empty(),
        "equal-pass pick must prefer the session-affine slot"
    );
    assert!(rt.stats().affinity_tiebreaks >= 1);
    // B's pass advanced past A's: the next pick is A (lowest pass beats
    // affinity — equal-pass-only tiebreak, no pass penalty).
    assert_eq!(rt.worker_step(&mut local), Step::Ran);
    assert!(
        !a.claims.lock().unwrap().is_empty(),
        "lower pass must beat session affinity (equal-pass-only tiebreak)"
    );
}

/// M5-4 slot-reclamation fix: an aborted RG still in the WAIT QUEUE
/// completes at abort time — promptly, with no slot freeing and no worker
/// stepping — instead of waiting for an unrelated slot release.
#[test]
fn queued_abort_reaped_promptly() {
    let clock = Arc::new(VirtualClock::new());
    let mut cfg = RuntimeConfig::new(1);
    cfg.slots = 1;
    let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);

    let a = SyntheticWork::new(64, Some(Arc::clone(&clock)), 1_000);
    let (_ha, wa) = rt.submit(spec_one(&a, Arc::new(SyntheticMorselSource::new(64))));
    let b = SyntheticWork::new(64, Some(Arc::clone(&clock)), 1_000);
    let (hb, wb) = rt.submit(spec_one(&b, Arc::new(SyntheticMorselSource::new(64))));
    assert!(wb.try_wait().is_none(), "B must be queued (1 slot)");

    // Abort the QUEUED RG: completion must be immediate — no worker has
    // stepped, no slot has freed.
    hb.abort();
    assert_eq!(
        wb.try_wait(),
        Some(RgOutcome::Aborted),
        "aborted queued RG must complete at abort time (slot-reclamation fix)"
    );
    let (submit_ns, first_ns, done_ns) = hb.service_times();
    assert!(submit_ns > 0 && done_ns >= submit_ns, "service timestamps must be recorded");
    assert_eq!(first_ns, 0, "a reaped queued RG was never serviced");
    assert!(b.claims.lock().unwrap().is_empty(), "reaped RG must never execute a morsel");

    let stats = rt.stats();
    assert_eq!(stats.queued_aborts_reaped, 1);
    assert_eq!(stats.rgs_aborted, 1);

    // The active RG is untouched.
    let mut local = rt.worker_local(0);
    while wa.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(wa.wait(), RgOutcome::Completed);
    a.assert_all_executed_once();
    // Idempotence: a second abort on the completed RG is a no-op.
    hb.abort();
    assert_eq!(rt.stats().queued_aborts_reaped, 1);
}

/// M4 Maintenance ≤ ~1-task bound RETEST under live stride (m5-planner §3.2
/// reconciliation): with several foreground RGs mid-flight at UNEQUAL
/// passes, a submitted maintenance cycle is still picked at the very next
/// task boundary — the preference is evaluated before the stride pick — and
/// its pass charges normally.
#[test]
fn maintenance_bound_survives_stride() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    let mut fgs = Vec::new();
    let mut fgw = Vec::new();
    for q in 0..3u64 {
        let w = SyntheticWork::new(64_000, Some(Arc::clone(&clock)), 1_000);
        let (_h, waiter) = rt.submit(QuerySpec {
            query_id: q + 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(64_000)),
                work: Arc::clone(&w) as Arc<dyn TaskSetWork>,
                deps: vec![],
            }],
        });
        fgs.push(w);
        fgw.push(waiter);
    }
    let mut local = rt.worker_local(0);
    // Let the stride pick rotate a few boundaries so passes are unequal.
    for _ in 0..5 {
        assert_eq!(rt.worker_step(&mut local), Step::Ran);
    }
    let mt = SyntheticWork::new(1, Some(Arc::clone(&clock)), 1_000);
    let (_mh, mw) =
        rt.submit_maintenance(spec_one(&mt, Arc::new(SyntheticMorselSource::new(1))));
    // ≤ 1 task boundary to cycle start AND completion (single-morsel body).
    assert_eq!(rt.worker_step(&mut local), Step::Ran);
    assert_eq!(
        mw.try_wait(),
        Some(RgOutcome::Completed),
        "maintenance cycle must complete at the first boundary under stride"
    );
    mt.assert_all_executed_once();
    // Drain the foreground RGs.
    for w in &fgw {
        while w.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
    }
    for f in &fgs {
        f.assert_all_executed_once();
    }
}

/// §3.5 submit→service instrument channel: timestamps are recorded and
/// ordered (submit ≤ first-service ≤ done on the scheduler clock), and the
/// per-RG CPU readback matches the virtual work exactly.
#[test]
fn service_times_and_cpu_readback() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    let total = 8_000u64;
    let cost = 1_000u64;
    let work = SyntheticWork::new(total, Some(Arc::clone(&clock)), cost);
    let (h, waiter) = rt.submit(spec_one(&work, Arc::new(SyntheticMorselSource::new(total))));
    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    let (submit_ns, first_ns, done_ns) = h.service_times();
    assert!(submit_ns > 0, "submit timestamp recorded");
    assert!(first_ns >= submit_ns, "first service after submit");
    assert!(done_ns >= first_ns, "done after first service");
    assert_eq!(h.cpu_consumed_ns(), total * cost, "exact CPU readback (virtual clock)");
}

/// Threaded fair-share smoke (the MULTI-arm shape, in-crate): K equal-share
/// RGs on a real pool, CPU shares sampled MID-FLIGHT (at completion the
/// totals are trivially equal — fixed work per RG). Sampling guard: shares
/// are only asserted if every RG is still active at the sample point, so
/// the test cannot flake on a fast machine; the band is deliberately loose
/// (fairness law's starvation direction) — exact error numbers come from
/// the deterministic K-sweep above.
#[test]
fn stride_threaded_pool_mid_flight_shares() {
    struct Spin {
        ns_per_granule: u64,
    }
    impl TaskSetWork for Spin {
        fn run_morsel(&self, _w: usize, range: MorselRange) {
            let n = range.end - range.start;
            let t0 = std::time::Instant::now();
            let budget = std::time::Duration::from_nanos(self.ns_per_granule * n);
            while t0.elapsed() < budget {
                std::hint::spin_loop();
            }
        }
        fn finalize(&self) {}
    }

    let mut cfg = RuntimeConfig::new(4);
    cfg.slots = 16;
    let rt = Runtime::new(cfg);
    rt.set_stride(true);
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    let k = 6usize;
    let mut handles = Vec::new();
    let mut waiters = Vec::new();
    for q in 0..k {
        let (h, w) = rt.submit(QuerySpec {
            query_id: q as u64 + 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(4_000)),
                work: Arc::new(Spin { ns_per_granule: 20_000 }),
                deps: vec![],
            }],
        });
        handles.push(h);
        waiters.push(w);
    }
    std::thread::sleep(std::time::Duration::from_millis(40));
    let all_active = waiters.iter().all(|w| w.try_wait().is_none());
    if all_active {
        let cpus: Vec<u64> = handles.iter().map(|h| h.cpu_consumed_ns()).collect();
        let mean = cpus.iter().sum::<u64>() as f64 / k as f64;
        eprintln!("stride threaded mid-flight shares (mean {mean:.0}): {cpus:?}");
        for c in &cpus {
            assert!(
                (*c as f64) >= 0.4 * mean && (*c as f64) <= 1.8 * mean,
                "mid-flight share out of loose band: {c} vs mean {mean:.0} ({cpus:?})"
            );
        }
    } else {
        eprintln!("stride threaded shares: sample point missed (fast machine) — band skipped");
    }
    for w in &waiters {
        assert_eq!(w.wait(), RgOutcome::Completed);
    }
    pool.shutdown();
}

/// M5-5 lock-wait-fairness (inter-query §6.3, the §3.4/§5.4 gate — CI
/// form): an adversarial priority-skew + contention workload that WOULD
/// starve a lock holder under unclamped decay, asserted bounded by p_min.
/// The lock-wait bound IS the holder's completion bound: whoever queues on
/// the holder's lock waits exactly until the holder's query completes and
/// releases it. Deterministic (virtual clock, one worker): a floor-decayed
/// holder with R remaining work against K fresh-p0 adversaries completes
/// within tasks(R) × (1 + K·p0/p_min) boundaries — and the bound tightens
/// proportionally when the floor is raised, proving p_min is the binding
/// constant (the C-legality window §3.4 relies on).
#[test]
fn lock_wait_fairness_pmin_bounds_holder() {
    let run = |p_min: u32| -> (u32, u32) {
        let clock = Arc::new(VirtualClock::new());
        let mut cfg = RuntimeConfig::new(1);
        cfg.slots = 32;
        let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);
        rt.set_stride(true);
        rt.set_decay(true);
        rt.set_p_min(p_min);
        rt.set_decay_quantum_ns(2_000_000);
        // The HOLDER: enough total work that ~20ms remains after it decays
        // to the floor (worst-case: it consumed heavily while holding).
        let holder_total = 40_000u64; // 40 ms virtual CPU
        let h_work = SyntheticWork::new(holder_total, Some(Arc::clone(&clock)), 1_000);
        let (hh, hw) =
            rt.submit(spec_one(&h_work, Arc::new(SyntheticMorselSource::new(holder_total))));
        let mut local = rt.worker_local(0);
        while hh.priority() > rt.p_min() {
            assert_eq!(rt.worker_step(&mut local), Step::Ran);
        }
        assert_eq!(hh.priority(), p_min);
        let consumed = hh.cpu_consumed_ns();
        let remaining_ns = holder_total * 1_000 - consumed;
        // Freeze decay so the adversaries KEEP p0 — sustained maximal skew.
        rt.set_decay_quantum_ns(u64::MAX);
        const K: u64 = 8;
        let adv_total = 40_000_000u64; // 40 s: adversaries never finish
        let mut adv = Vec::new();
        let mut adv_waiters = Vec::new();
        for q in 0..K {
            let w = SyntheticWork::new(adv_total, Some(Arc::clone(&clock)), 1_000);
            let (h, wt) = rt.submit(QuerySpec {
                query_id: 100 + q,
                tasksets: vec![TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(adv_total)),
                    work: Arc::clone(&w) as Arc<dyn TaskSetWork>,
                    deps: vec![],
                }],
            });
            adv.push(h);
            adv_waiters.push(wt);
        }
        // Drive until the holder completes (= the lock releases); count
        // boundaries — the deterministic "wall clock" of the wait.
        let mut boundaries = 0u32;
        while hw.try_wait().is_none() {
            assert_eq!(rt.worker_step(&mut local), Step::Ran);
            boundaries += 1;
            assert!(
                boundaries < 2_000_000,
                "holder starved outright at p_min={p_min} — the floor failed"
            );
        }
        assert_eq!(hw.wait(), RgOutcome::Completed);
        // Theory: holder runs 1 task per (1 + K·p0/p_min) boundaries.
        let holder_tasks = remaining_ns.div_ceil(2_000_000).max(1) as u32; // t_max tasks
        let rotation = 1 + (K as u32) * 10_000 / p_min;
        let bound = holder_tasks * rotation * 13 / 10 + rotation; // +30% ramp slack
        assert!(
            boundaries <= bound,
            "p_min={p_min}: lock-holder wait {boundaries} boundaries exceeds the \
             floor-derived bound {bound} (tasks={holder_tasks}, rotation={rotation})"
        );
        for h in &adv {
            h.abort();
        }
        for wt in &adv_waiters {
            while wt.try_wait().is_none() {
                rt.worker_step(&mut local);
            }
        }
        (boundaries, bound)
    };
    // Ratified floor: bounded.
    let (b625, bound625) = run(625);
    // Raised floor: the window tightens ~4x — p_min is the binding constant.
    let (b2500, bound2500) = run(2_500);
    assert!(
        b2500 * 3 < b625,
        "raising p_min 4x must shrink the lock-wait window ~4x (625: {b625}, 2500: {b2500})"
    );
    eprintln!(
        "lock-wait fairness: p_min=625 wait {b625}/{bound625} boundaries; \
         p_min=2500 wait {b2500}/{bound2500} — floor bounds the window"
    );
}

/// M5-5 §3.4 latency-fairness panel, MULTI arm (unit form; the fleet panel
/// script drives the SQL altitude): a controlled mix of short probes
/// against a saturating batch background on a REAL 4-worker pool, decay ON
/// vs OFF. Emits the M0ACCEPT|FAIR verdict line the m0-accept channel
/// greps. Gate: short-query p50/p95 under load approaches isolated (the
/// MLFQ effect); batch throughput floor guarded by completion.
#[test]
fn multi_fairness_panel_short_latency_under_batch() {
    struct Spin {
        ns_per_granule: u64,
    }
    impl TaskSetWork for Spin {
        fn run_morsel(&self, _w: usize, range: MorselRange) {
            let n = range.end - range.start;
            let t0 = std::time::Instant::now();
            let budget = std::time::Duration::from_nanos(self.ns_per_granule * n);
            while t0.elapsed() < budget {
                std::hint::spin_loop();
            }
        }
        fn finalize(&self) {}
    }
    // One probe = ~4 ms of work (sub-quantum: never decays).
    let submit_probe = |rt: &Arc<Runtime>, qid: u64| {
        rt.submit(QuerySpec {
            query_id: qid,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(200)),
                work: Arc::new(Spin { ns_per_granule: 20_000 }),
                deps: vec![],
            }],
        })
    };
    let percentile = |xs: &mut Vec<u64>, p: f64| -> u64 {
        xs.sort_unstable();
        xs[((xs.len() - 1) as f64 * p) as usize]
    };
    let run = |decay: bool| -> (u64, u64, u64, u64) {
        let mut cfg = RuntimeConfig::new(4);
        cfg.slots = 16;
        let rt = Runtime::new(cfg);
        rt.set_stride(true);
        rt.set_decay(decay);
        // Tight quantum so the batch decays within the test's horizon
        // (real clock; the ratified 50ms/quantum geometry scaled down).
        rt.set_decay_quantum_ns(5_000_000);
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        // Isolated baseline: 8 probes, quiet pool.
        let mut iso = Vec::new();
        for i in 0..8u64 {
            let (h, w) = submit_probe(&rt, 1000 + i);
            assert_eq!(w.wait(), RgOutcome::Completed);
            let (s, _f, d) = h.service_times();
            iso.push(d - s);
        }
        // Saturating batch background (~200 ms each); count env-tunable
        // for fleet calibration.
        let n_batch: u64 = std::env::var("M5_PANEL_BATCH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6);
        let mut batch = Vec::new();
        let mut batch_waiters = Vec::new();
        for q in 0..n_batch {
            let (h, w) = rt.submit(QuerySpec {
                query_id: q + 1,
                tasksets: vec![TaskSetSpec {
                    source: Arc::new(SyntheticMorselSource::new(10_000)),
                    work: Arc::new(Spin { ns_per_granule: 20_000 }),
                    deps: vec![],
                }],
            });
            batch.push(h);
            batch_waiters.push(w);
        }
        // Let the background consume past the decay horizon.
        std::thread::sleep(std::time::Duration::from_millis(60));
        // Under-load probes: 8 shorts, controlled spacing.
        let mut load = Vec::new();
        for i in 0..8u64 {
            let (h, w) = submit_probe(&rt, 2000 + i);
            assert_eq!(w.wait(), RgOutcome::Completed);
            let (s, _f, d) = h.service_times();
            load.push(d - s);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        // Batch background must still complete (throughput floor: no
        // starvation of the decayed class).
        for w in &batch_waiters {
            assert_eq!(w.wait(), RgOutcome::Completed);
        }
        pool.shutdown();
        (
            percentile(&mut iso, 0.5),
            percentile(&mut iso, 0.95),
            percentile(&mut load, 0.5),
            percentile(&mut load, 0.95),
        )
    };
    let (on_iso_p50, on_iso_p95, on_p50, on_p95) = run(true);
    let (off_iso_p50, _off_iso_p95, off_p50, off_p95) = run(false);
    let r50 = on_p50 as f64 / on_iso_p50.max(1) as f64;
    let r95 = on_p95 as f64 / on_iso_p95.max(1) as f64;
    let r50_off = off_p50 as f64 / off_iso_p50.max(1) as f64;
    let verdict = if r95 <= 8.0 && r50 <= 6.0 { "PASS" } else { "FAIL" };
    eprintln!(
        "M0ACCEPT|FAIR|arm=multi-unit|decay=on|short_p50_us={}|short_p95_us={}|iso_p50_us={}|\
         iso_p95_us={}|p50_ratio={r50:.2}|p95_ratio={r95:.2}|off_p50_ratio={r50_off:.2}|\
         off_p50_us={}|off_p95_us={}|verdict={verdict}",
        on_p50 / 1000,
        on_p95 / 1000,
        on_iso_p50 / 1000,
        on_iso_p95 / 1000,
        off_p50 / 1000,
        off_p95 / 1000,
    );
    // Loose CI bands (real threads; the calibrated band is the fleet
    // panel's): under-load short latency must stay within an order of
    // magnitude of isolated with decay ON, and must beat decay OFF on p50.
    assert_eq!(verdict, "PASS", "latency-fairness panel out of band");
    assert!(
        on_p50 <= off_p50.max(1) * 2,
        "decay ON p50 ({on_p50}ns) should not exceed 2x decay OFF p50 ({off_p50}ns)"
    );
}

// ---- M5+1 pipeline-DAG dispatch (m5-planner §3.6, increment 1) --------------
//
// Independent-subtree overlap: every dependency-satisfied task set publishes
// concurrently, each in its own slot. OFF (the default) is the sequential
// walk — the whole pre-existing suite runs with the switch off and is the
// byte-identity evidence; the tests here exercise ON.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DagEv {
    Claim(usize),
    Finalize(usize),
}

/// SyntheticWork wrapped with a shared event log (claim/finalize order —
/// the overlap and gating oracles).
struct DagWork {
    inner: Arc<SyntheticWork>,
    log: Arc<Mutex<Vec<DagEv>>>,
    index: usize,
}

impl TaskSetWork for DagWork {
    fn run_morsel(&self, w: usize, range: MorselRange) {
        self.log.lock().unwrap().push(DagEv::Claim(self.index));
        self.inner.run_morsel(w, range);
    }
    fn finalize(&self) {
        self.inner.finalize();
        self.log.lock().unwrap().push(DagEv::Finalize(self.index));
    }
}

/// Build a QuerySpec from (granules, deps) shapes over a shared event log.
fn dag_spec(
    qid: u64,
    clock: &Arc<VirtualClock>,
    log: &Arc<Mutex<Vec<DagEv>>>,
    shapes: &[(u64, &[usize])],
) -> (QuerySpec, Vec<Arc<SyntheticWork>>) {
    let mut tasksets = Vec::new();
    let mut works = Vec::new();
    for (i, (total, deps)) in shapes.iter().enumerate() {
        let inner = SyntheticWork::new(*total, Some(Arc::clone(clock)), 1_000);
        works.push(Arc::clone(&inner));
        tasksets.push(TaskSetSpec {
            source: Arc::new(SyntheticMorselSource::new(*total)),
            work: Arc::new(DagWork { inner, log: Arc::clone(log), index: i }),
            deps: deps.to_vec(),
        });
    }
    (QuerySpec { query_id: qid, tasksets }, works)
}

fn first_pos(log: &[DagEv], ev: DagEv) -> Option<usize> {
    log.iter().position(|e| *e == ev)
}

/// Admission publishes EVERY dependency-satisfied pipeline concurrently
/// (multi-build shape: two independent build sides + a gated probe): both
/// builds occupy slots before any worker steps; the probe is NOT submitted
/// (occupies nothing) until both builds finalize; a single worker's claims
/// INTERLEAVE the two builds (the overlap the increment buys).
#[test]
fn dag_admission_fans_out_and_gates() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_dag(true);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (spec, works) = dag_spec(
        1,
        &clock,
        &log,
        &[(6_000, &[]), (6_000, &[]), (2_000, &[0, 1])],
    );
    let (_h, waiter) = rt.submit(spec);

    // Both independent builds are published at admission; the gated probe
    // is not (submission gating: unmet deps ⇒ not submitted anywhere).
    let s = rt.stats();
    assert_eq!(s.tasksets_published, 2, "both ready pipelines publish at admission");
    assert_eq!(s.dag_fanout_publishes, 1, "second build fans out into its own slot");

    let mut local = rt.worker_local(0);
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    for w in &works {
        w.assert_all_executed_once();
        assert_eq!(w.finalizes.load(Ordering::SeqCst), 1);
    }
    let log = log.lock().unwrap();
    let fin0 = first_pos(&log, DagEv::Finalize(0)).unwrap();
    let fin1 = first_pos(&log, DagEv::Finalize(1)).unwrap();
    let c0 = first_pos(&log, DagEv::Claim(0)).unwrap();
    let c1 = first_pos(&log, DagEv::Claim(1)).unwrap();
    let c2 = first_pos(&log, DagEv::Claim(2)).unwrap();
    // Overlap: each build claims before the other finalizes (stride
    // alternation over the query's equal-pass slots).
    assert!(c0 < fin1 && c1 < fin0, "independent builds must interleave, got {log:?}");
    // Barrier: the probe's first claim strictly follows BOTH finalizes.
    assert!(c2 > fin0 && c2 > fin1, "probe ran before its build barriers, got {log:?}");
}

/// DAG ON with a dependency CHAIN (single live pipeline throughout — the
/// ClickBench shape class) produces the exact claim sequence the sequential
/// walk produces: the flatness anchor.
#[test]
fn dag_chain_claim_sequence_identical_to_off() {
    let run = |dag: bool| -> Vec<Vec<Range<u64>>> {
        let clock = Arc::new(VirtualClock::new());
        let rt = virtual_runtime(1, &clock);
        rt.set_stride(true);
        rt.set_dag(dag);
        let log = Arc::new(Mutex::new(Vec::new()));
        let (spec, works) =
            dag_spec(1, &clock, &log, &[(6_000, &[]), (4_000, &[0]), (2_000, &[1])]);
        let (_h, waiter) = rt.submit(spec);
        let mut local = rt.worker_local(0);
        while waiter.try_wait().is_none() {
            rt.worker_step(&mut local);
        }
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        works.iter().map(|w| w.claims.lock().unwrap().clone()).collect()
    };
    assert_eq!(run(true), run(false), "chain claims must be identical under DAG dispatch");
}

/// Slot-capacity deferral: 3 independent pipelines, 2 scheduler slots — the
/// third publishes only when one of the query's own pipelines finishes (the
/// RG always retains a slot; nothing strands, nothing deadlocks), and a
/// QUEUED second query still gets admitted when a slot frees mid-query.
#[test]
fn dag_capacity_defers_and_freed_slot_admits_queued() {
    let clock = Arc::new(VirtualClock::new());
    let mut cfg = RuntimeConfig::new(1);
    cfg.slots = 2;
    let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);
    rt.set_stride(true);
    rt.set_dag(true);
    let log = Arc::new(Mutex::new(Vec::new()));
    // Two long builds + a short one + a sink gated on all three.
    let (spec, works) = dag_spec(
        1,
        &clock,
        &log,
        &[(4_000, &[]), (4_000, &[]), (2_000, &[]), (1_000, &[0, 1, 2])],
    );
    let (_h, wa) = rt.submit(spec);
    assert_eq!(rt.stats().tasksets_published, 2, "capacity caps admission fan-out");
    assert!(rt.stats().dag_ready_deferred >= 1, "third pipeline defers on slot capacity");

    // A second, single-pipeline query queues behind the full slot array and
    // completes once a slot frees (FIFO admission is not starved by the
    // multi-pipeline query's deferred pipelines).
    let b = SyntheticWork::new(1_000, Some(Arc::clone(&clock)), 1_000);
    let (_hb, wb) = rt.submit(spec_one(&b, Arc::new(SyntheticMorselSource::new(1_000))));

    let mut local = rt.worker_local(0);
    while wa.try_wait().is_none() || wb.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(wa.wait(), RgOutcome::Completed);
    assert_eq!(wb.wait(), RgOutcome::Completed);
    for w in &works {
        w.assert_all_executed_once();
        assert_eq!(w.finalizes.load(Ordering::SeqCst), 1);
    }
    b.assert_all_executed_once();
}

/// §3.6 fairness law with pipeline-RGs live: the QUERY is the fair-share
/// principal — a query with TWO live pipelines gets the same aggregate CPU
/// share as single-pipeline peers (its slots advance ONE shared account),
/// not double. Deterministic K-sweep shape: K-1 single-pipeline queries plus
/// one two-pipeline query on one virtual-clock worker.
#[test]
fn dag_query_level_share_k_sweep() {
    for k in [2usize, 4, 8] {
        let clock = Arc::new(VirtualClock::new());
        let mut cfg = RuntimeConfig::new(1);
        cfg.slots = 32;
        let rt = Runtime::with_clock(cfg, Arc::clone(&clock) as Arc<dyn Clock>);
        rt.set_stride(true);
        rt.set_dag(true);
        let total = 400_000u64;
        let mut handles = Vec::new();
        let mut waiters = Vec::new();
        // Query 1: two independent pipelines (never finishes in-window).
        let log = Arc::new(Mutex::new(Vec::new()));
        let (spec, _works) =
            dag_spec(1, &clock, &log, &[(total, &[]), (total, &[]), (1, &[0, 1])]);
        let (h, w) = rt.submit(spec);
        handles.push(h);
        waiters.push(w);
        // Queries 2..=k: single pipeline.
        for q in 1..k {
            let w = SyntheticWork::new(total, Some(Arc::clone(&clock)), 1_000);
            let (h, waiter) =
                rt.submit(spec_one(&w, Arc::new(SyntheticMorselSource::new(total))));
            let _ = q;
            handles.push(h);
            waiters.push(waiter);
        }
        // 20 task boundaries per SLOT at perfect fairness (k+1 live slots).
        let mut local = rt.worker_local(0);
        for _ in 0..(20 * (k + 1)) {
            assert_eq!(rt.worker_step(&mut local), Step::Ran);
        }
        for w in &waiters {
            assert!(w.try_wait().is_none(), "K-sweep RGs must all still be active");
        }
        let cpus: Vec<u64> = handles.iter().map(|h| h.cpu_consumed_ns()).collect();
        let mean = cpus.iter().sum::<u64>() as f64 / k as f64;
        let mut max_err = 0f64;
        for c in &cpus {
            max_err = max_err.max((*c as f64 - mean).abs() / mean);
        }
        // The two-pipeline query's mirror staleness bounds its edge at ~one
        // task quantum per extra pipeline over the ~20-quanta window.
        assert!(
            max_err <= 0.12,
            "K={k}: query-level share error {max_err:.4} out of band; cpus={cpus:?} \
             (first query holds TWO live pipelines and must not get 2x)"
        );
        eprintln!("dag K-sweep K={k} (query 1 dual-pipeline): max share error {max_err:.4} (cpu ns: {cpus:?})");
        for h in &handles {
            h.abort();
        }
        for w in &waiters {
            while w.try_wait().is_none() {
                rt.worker_step(&mut local);
            }
        }
    }
}

/// Within-query dependency-depth pick (§3.6): at equal pass (two pipelines
/// published by one fan-out mirror the same account value), the pick
/// prefers the DEEPER pipeline even from a higher slot index.
#[test]
fn dag_depth_priority_pick() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_dag(true);
    // Query A occupies slot 0 briefly, then completes and frees it.
    let a = SyntheticWork::new(500, Some(Arc::clone(&clock)), 1_000);
    let (_ha, wa) = rt.submit(spec_one(&a, Arc::new(SyntheticMorselSource::new(500))));
    // Query B: root in slot 1; its finalize fans out S (shallow, depth 1)
    // into freed slot 0 and D (deep, depth 2) into retained slot 1 — both
    // mirroring the same account (equal pass).
    let log = Arc::new(Mutex::new(Vec::new()));
    let (spec, works) = dag_spec(
        2,
        &clock,
        &log,
        &[
            (1_000, &[]),      // 0 = R (root)
            (2_000, &[0]),     // 1 = S (shallow: only the sink above)
            (2_000, &[0]),     // 2 = D (deep: Z then the sink)
            (1_000, &[2]),     // 3 = Z
            (500, &[1, 3]),    // 4 = sink
        ],
    );
    let (_hb, wb) = rt.submit(spec);

    let mut local = rt.worker_local(0);
    // Step 1: equal pass across queries -> scan order picks A; A completes
    // and frees slot 0. Step 2: forced pick of B's root; it exhausts and
    // fans out D (retained slot 1) + S (freed slot 0) at equal pass.
    while wa.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    while first_pos(&log.lock().unwrap(), DagEv::Finalize(0)).is_none() {
        rt.worker_step(&mut local);
    }
    // Step 3: equal pass, same query, shallow in the LOWER slot — the
    // depth refinement must pick the deeper pipeline first.
    rt.worker_step(&mut local);
    {
        let log = log.lock().unwrap();
        let cs = first_pos(&log, DagEv::Claim(1));
        let cd = first_pos(&log, DagEv::Claim(2));
        assert!(
            cd.is_some() && cs.is_none(),
            "deeper pipeline must be picked first at equal pass, got {log:?}"
        );
    }
    assert!(rt.stats().dag_depth_picks >= 1, "depth refinement must have decided the pick");
    while wb.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(wb.wait(), RgOutcome::Completed);
    for w in &works {
        w.assert_all_executed_once();
    }
}

/// Abort with multiple LIVE pipelines: each published pipeline drains
/// through its own generation-refusal last-out; the LAST one completes the
/// RG (exactly once, Aborted); gated pipelines never publish; no finalize
/// work runs.
#[test]
fn dag_abort_with_live_siblings() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_dag(true);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (spec, works) = dag_spec(
        1,
        &clock,
        &log,
        &[(50_000, &[]), (50_000, &[]), (1_000, &[0, 1])],
    );
    let (h, waiter) = rt.submit(spec);
    let mut local = rt.worker_local(0);
    // Run a few tasks on the live builds, then abort mid-flight.
    for _ in 0..4 {
        assert_eq!(rt.worker_step(&mut local), Step::Ran);
    }
    h.abort();
    while waiter.try_wait().is_none() {
        rt.worker_step(&mut local);
    }
    assert_eq!(waiter.wait(), RgOutcome::Aborted);
    assert_eq!(rt.stats().rgs_aborted, 1);
    assert_eq!(rt.stats().rgs_completed, 1);
    for w in &works {
        assert_eq!(w.finalizes.load(Ordering::SeqCst), 0, "aborted RGs never run finalize work");
    }
    let log = log.lock().unwrap();
    assert!(
        first_pos(&log, DagEv::Claim(2)).is_none(),
        "the gated sink must never have published on an aborted RG"
    );
}

/// PINNED multi-slot drive (the arm execution mode): a pinned RG's external
/// driver picks among the RG's live pipelines DEEPEST-FIRST, and the whole
/// DAG completes through pinned stepping alone.
#[test]
fn dag_pinned_drive_prefers_deeper_pipeline() {
    let clock = Arc::new(VirtualClock::new());
    let rt = virtual_runtime(1, &clock);
    rt.set_stride(true);
    rt.set_dag(true);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (spec, works) = dag_spec(
        1,
        &clock,
        &log,
        &[
            (2_000, &[]),   // 0 = S (shallow: straight to sink)
            (2_000, &[]),   // 1 = D (deep: Z then sink)
            (1_000, &[1]),  // 2 = Z
            (500, &[0, 2]), // 3 = sink
        ],
    );
    let (h, waiter) = rt.submit_pinned(spec);
    assert_eq!(rt.stats().tasksets_published, 2, "pinned admission fans out too");
    let lane = rt.acquire_external_lane().expect("lane");
    let mut local = lane.local();
    assert_eq!(rt.drive_pinned(&mut local, &h), RgOutcome::Completed);
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    for w in &works {
        w.assert_all_executed_once();
        assert_eq!(w.finalizes.load(Ordering::SeqCst), 1);
    }
    let log = log.lock().unwrap();
    let cs = first_pos(&log, DagEv::Claim(0)).unwrap();
    let cd = first_pos(&log, DagEv::Claim(1)).unwrap();
    assert!(cd < cs, "pinned driver must start on the deeper pipeline, got {log:?}");
}

/// Threaded pool end-to-end over the three §3.6 win shapes (multi-build
/// join, UNION ALL, independent subqueries): completion, exactly-once
/// execution, dep-ordered finalizes, and real fan-out engagement.
#[test]
fn dag_pool_win_shapes_complete() {
    let shapes: [&[(u64, &[usize])]; 3] = [
        // multi-build: 3 independent builds + probe gated on all three
        &[(8_192, &[]), (8_192, &[]), (8_192, &[]), (8_192, &[0, 1, 2])],
        // UNION ALL: 4 independent branches + a concat sink
        &[(8_192, &[]), (8_192, &[]), (8_192, &[]), (8_192, &[]), (1_024, &[0, 1, 2, 3])],
        // independent subqueries: two 2-deep chains joining a final
        &[(8_192, &[]), (4_096, &[0]), (8_192, &[]), (4_096, &[2]), (2_048, &[1, 3])],
    ];
    for (si, shape) in shapes.iter().enumerate() {
        let rt = Runtime::new(RuntimeConfig {
            workers: 4,
            standbys: 2,
            slots: 16,
            sizing: SizingParams::default(),
            trace: false,
        });
        rt.set_dag(true);
        let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut tasksets = Vec::new();
        let mut works = Vec::new();
        for (i, (total, deps)) in shape.iter().enumerate() {
            let inner = SyntheticWork::new(*total, None, 0);
            works.push(Arc::clone(&inner));
            tasksets.push(TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(*total)),
                work: Arc::new(DagWork { inner, log: Arc::clone(&log), index: i }),
                deps: deps.to_vec(),
            });
        }
        let (_h, waiter) = rt.submit(QuerySpec { query_id: si as u64 + 1, tasksets });
        assert_eq!(waiter.wait(), RgOutcome::Completed);
        pool.shutdown();
        for w in &works {
            w.assert_all_executed_once();
            assert_eq!(w.finalizes.load(Ordering::SeqCst), 1);
        }
        // Gating oracle: every pipeline's first claim follows ALL its deps'
        // finalizes.
        let log = log.lock().unwrap();
        for (i, (_, deps)) in shape.iter().enumerate() {
            let ci = first_pos(&log, DagEv::Claim(i)).unwrap();
            for &d in deps.iter() {
                let fd = first_pos(&log, DagEv::Finalize(d)).unwrap();
                assert!(ci > fd, "shape {si}: pipeline {i} claimed before dep {d} finalized");
            }
        }
        assert!(rt.stats().dag_fanout_publishes >= 1, "shape {si} must fan out");
    }
}

/// Threaded-pool leg of the §3.4 fairness panel with pipeline-RGs live
/// (the stride_threaded_pool_mid_flight_shares instrument extended per
/// §3.6): K queries on a real pool, ONE of them holding TWO live
/// independent pipelines the whole window — mid-flight per-QUERY CPU shares
/// stay in the loose band (the dual-pipeline query must not take 2x).
#[test]
fn dag_threaded_pool_query_shares() {
    struct Spin {
        ns_per_granule: u64,
    }
    impl TaskSetWork for Spin {
        fn run_morsel(&self, _w: usize, range: MorselRange) {
            let n = range.end - range.start;
            let t0 = std::time::Instant::now();
            let budget = std::time::Duration::from_nanos(self.ns_per_granule * n);
            while t0.elapsed() < budget {
                std::hint::spin_loop();
            }
        }
        fn finalize(&self) {}
    }

    let mut cfg = RuntimeConfig::new(4);
    cfg.slots = 16;
    let rt = Runtime::new(cfg);
    rt.set_stride(true);
    rt.set_dag(true);
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();

    let k = 6usize;
    let mut handles = Vec::new();
    let mut waiters = Vec::new();
    // Query 1: TWO independent live pipelines (each half the work of a
    // single-pipeline peer, so equal aggregate demand) + a tiny gated sink.
    let (h, w) = rt.submit(QuerySpec {
        query_id: 1,
        tasksets: vec![
            TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2_000)),
                work: Arc::new(Spin { ns_per_granule: 20_000 }),
                deps: vec![],
            },
            TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(2_000)),
                work: Arc::new(Spin { ns_per_granule: 20_000 }),
                deps: vec![],
            },
            TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(64)),
                work: Arc::new(Spin { ns_per_granule: 1_000 }),
                deps: vec![0, 1],
            },
        ],
    });
    handles.push(h);
    waiters.push(w);
    for q in 1..k {
        let (h, w) = rt.submit(QuerySpec {
            query_id: q as u64 + 1,
            tasksets: vec![TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(4_000)),
                work: Arc::new(Spin { ns_per_granule: 20_000 }),
                deps: vec![],
            }],
        });
        handles.push(h);
        waiters.push(w);
    }
    std::thread::sleep(std::time::Duration::from_millis(40));
    let all_active = waiters.iter().all(|w| w.try_wait().is_none());
    if all_active {
        let cpus: Vec<u64> = handles.iter().map(|h| h.cpu_consumed_ns()).collect();
        let mean = cpus.iter().sum::<u64>() as f64 / k as f64;
        eprintln!("dag threaded query shares (mean {mean:.0}): {cpus:?}");
        for c in &cpus {
            assert!(
                (*c as f64) >= 0.4 * mean && (*c as f64) <= 1.8 * mean,
                "mid-flight QUERY share out of loose band: {c} vs mean {mean:.0} ({cpus:?}) \
                 — query 1 holds two live pipelines and must not take 2x"
            );
        }
    } else {
        eprintln!("dag threaded shares: sample point missed (fast machine) — band skipped");
    }
    for w in &waiters {
        assert_eq!(w.wait(), RgOutcome::Completed);
    }
    pool.shutdown();
}
// ---- stream-fed sources (parallel COPY's segmentator feed) -----------------

/// Stream source under a REAL pool: a producer publishes granules in bursts
/// (workers starve and park between bursts), then closes. Every granule
/// executes exactly once, claims are single-granule (one chunk per claim),
/// and the RG completes only after the close.
#[test]
fn stream_source_pool_exact() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 4,
        standbys: 2,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let total = 96u64;
    let work = SyntheticWork::new(total, None, 0);
    let source = Arc::new(StreamSource::new());
    let (_h, waiter) = rt.submit(spec_one(&work, Arc::clone(&source) as Arc<dyn MorselSource>));

    // Producer: bursts of 8 with pauses (parked-worker wake coverage).
    let psrc = Arc::clone(&source);
    let prt = Arc::clone(&rt);
    let producer = std::thread::spawn(move || {
        let mut upto = 0u64;
        while upto < total {
            upto = (upto + 8).min(total);
            psrc.publish(upto);
            prt.notify_source_progress();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        psrc.close();
        prt.notify_source_progress();
    });

    assert_eq!(waiter.wait(), RgOutcome::Completed);
    producer.join().unwrap();
    work.assert_all_executed_once();
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);
    for r in work.claims.lock().unwrap().iter() {
        assert_eq!(r.end - r.start, 1, "stream claims must be single-chunk, got {r:?}");
    }
    pool.shutdown();
}

/// An EMPTY stream (closed at watermark 0) completes with no morsels.
#[test]
fn stream_source_empty_completes() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 2,
        standbys: 1,
        slots: 4,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let work = SyntheticWork::new(0, None, 0);
    let source = Arc::new(StreamSource::new());
    let (_h, waiter) = rt.submit(spec_one(&work, Arc::clone(&source) as Arc<dyn MorselSource>));
    source.close();
    rt.notify_source_progress();
    assert_eq!(waiter.wait(), RgOutcome::Completed);
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 1);
    assert!(work.claims.lock().unwrap().is_empty());
    pool.shutdown();
}

/// Abort with STARVED (parked) workers on a never-closed stream: the
/// producer's error path closes + wakes; workers observe the abort at the
/// claim boundary and the RG completes Aborted with cleanup finalization
/// suppressed (finalize never runs on aborted RGs).
#[test]
fn stream_source_abort_wakes_starved_workers() {
    let rt = Runtime::new(RuntimeConfig {
        workers: 4,
        standbys: 2,
        slots: 8,
        sizing: SizingParams::default(),
        trace: false,
    });
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).unwrap();
    let work = SyntheticWork::new(16, None, 0);
    let source = Arc::new(StreamSource::new());
    let (h, waiter) = rt.submit(spec_one(&work, Arc::clone(&source) as Arc<dyn MorselSource>));
    source.publish(16);
    rt.notify_source_progress();
    // Wait until the published prefix drains and workers starve-park.
    while work.claims.lock().unwrap().iter().map(|r| r.end - r.start).sum::<u64>() < 16 {
        std::thread::yield_now();
    }
    // Producer error path: abort, then close + wake (the documented order).
    h.abort();
    source.close();
    rt.notify_source_progress();
    assert_eq!(waiter.wait(), RgOutcome::Aborted);
    assert_eq!(work.finalizes.load(Ordering::SeqCst), 0, "aborted RGs skip finalize");
    work.assert_all_executed_once();
    pool.shutdown();
}

/// WS-A batchsource inc-1: deterministic equivalence of the extracted
/// [`GranuleMap`]/[`GranuleMapSource`]/[`Segments`] geometry against the
/// pre-extraction sources' code, ported verbatim as oracles (behavior
/// preservation is the whole game — these tests pin the bit-for-bit claim).
mod granule_map_tests {
    use std::ops::Range;
    use std::sync::Arc;

    use crate::{GranuleMap, GranuleMapSource, MorselSource, Segments, SyntheticMorselSource};

    /// The legacy scan-arm source's boundary search
    /// (runtime_scan.rs `PgrcolumnarGranuleSource::next_boundary_after`),
    /// ported verbatim as the equivalence oracle.
    fn legacy_boundary_after(starts: &[u64], start: u64) -> u64 {
        let total = starts.last().copied().unwrap_or(0);
        match starts.binary_search(&start) {
            Ok(i) => starts.get(i + 1).copied().unwrap_or(total),
            Err(i) => starts.get(i).copied().unwrap_or(total),
        }
    }

    /// morsel_body's open-coded coalesced-claim segmentation (the
    /// pre-extraction loop, runtime_scan.rs), ported verbatim as the
    /// `segments()` oracle. `None` starts = the heap path (whole range).
    fn legacy_segments(starts: Option<&[u64]>, range: Range<u64>) -> Vec<Range<u64>> {
        let mut out = Vec::new();
        let mut seg = range.start;
        while seg < range.end {
            let seg_end = match starts {
                Some(starts) => {
                    let bound = match starts.binary_search(&seg) {
                        Ok(i) => starts[i + 1],
                        Err(i) => starts[i],
                    };
                    bound.min(range.end)
                }
                None => range.end,
            };
            out.push(seg..seg_end);
            seg = seg_end;
        }
        out
    }

    fn map_over(starts: &[u64]) -> GranuleMap {
        GranuleMap::with_boundaries(Arc::new(starts.to_vec()), 2)
    }

    /// Fixture with short interior RGs (reopen-append seals partial RGs):
    /// the map is a real prefix sum, never `rg * k`.
    const UNEVEN: &[u64] = &[0, 8, 11, 19, 24];

    #[test]
    fn boundary_after_matches_legacy_source() {
        for starts in [&[0u64, 8][..], UNEVEN, &[0]] {
            let map = map_over(starts);
            let total = starts.last().copied().unwrap();
            // Full sweep covers start-on-boundary and mid-RG starts.
            for s in 0..total {
                assert_eq!(
                    map.boundary_after(s),
                    legacy_boundary_after(starts, s),
                    "starts={starts:?} s={s}"
                );
                assert!(map.boundary_after(s) > s, "MorselSource contract");
                assert!(map.boundary_after(s) <= total, "MorselSource contract");
            }
        }
    }

    #[test]
    fn boundary_after_duplicate_starts_match_legacy() {
        // Zero-granule RGs would produce duplicate prefix-sum entries;
        // defensively pin that the guarded form answers exactly as the
        // legacy source did (behavior preservation, not endorsement).
        let starts: &[u64] = &[0, 4, 4, 9];
        let map = map_over(starts);
        for s in 0..9 {
            assert_eq!(map.boundary_after(s), legacy_boundary_after(starts, s), "s={s}");
        }
    }

    #[test]
    fn unbounded_map_is_boundary_free() {
        let map = GranuleMap::unbounded(37, 16);
        assert_eq!(map.total(), 37);
        assert_eq!(map.c0(), 16);
        assert_eq!(map.nbounds(), 0);
        for s in 0..37 {
            // The MorselSource trait default: no interior boundaries.
            assert_eq!(map.boundary_after(s), 37);
        }
        let segs: Vec<_> = map.segments(3..20).collect();
        assert_eq!(segs, vec![3..20], "boundary-free claims yield once");
    }

    #[test]
    fn with_boundaries_geometry_accessors() {
        let map = map_over(UNEVEN);
        assert_eq!(map.total(), 24);
        assert_eq!(map.c0(), 2);
        assert_eq!(map.nbounds(), 4, "nrgs = len - 1 (the LFIN channel)");
    }

    #[test]
    fn segments_match_morsel_body_oracle() {
        let map = map_over(UNEVEN);
        // Every legal claim shape: aligned/mid-RG starts, single-epoch,
        // coalesced multi-epoch, claims ending mid-RG.
        for start in 0..24u64 {
            for end in (start + 1)..=24 {
                let got: Vec<_> = map.segments(start..end).collect();
                let want = legacy_segments(Some(UNEVEN), start..end);
                assert_eq!(got, want, "claim {start}..{end}");
            }
        }
        // Heap path parity: Segments::whole == the oracle's None branch.
        let got: Vec<_> = Segments::whole(5..17).collect();
        assert_eq!(got, legacy_segments(None, 5..17));
    }

    #[test]
    fn segments_more_tracks_unyielded_work() {
        let map = map_over(UNEVEN);
        let mut segs = map.segments(2..20);
        let mut yielded = 2u64;
        while let Some(seg) = segs.next() {
            yielded = seg.end;
            assert_eq!(segs.more(), yielded < 20, "after seg {seg:?}");
        }
        assert_eq!(yielded, 20);
        assert!(!Segments::whole(0..0).more(), "empty claim has no work");
    }

    /// Epoch-alignment property test over seeded pseudo-random geometry
    /// (plain xorshift; no proptest dep — workspace law): every yielded
    /// segment is non-empty, consecutive, covers the claim exactly, and
    /// never crosses an interior hard boundary.
    #[test]
    fn segments_epoch_alignment_property() {
        let mut state = 0x9e3779b97f4a7c15u64;
        let mut next_rand = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound.max(1)
        };
        for _case in 0..200 {
            let nrgs = 1 + next_rand(12) as usize;
            let mut starts = vec![0u64];
            for _ in 0..nrgs {
                let rg_granules = 1 + next_rand(9);
                starts.push(starts.last().unwrap() + rg_granules);
            }
            let total = *starts.last().unwrap();
            let map = GranuleMap::with_boundaries(Arc::new(starts.clone()), 2);
            for _claim in 0..8 {
                let a = next_rand(total);
                let b = a + 1 + next_rand(total - a);
                let mut cursor = a;
                for seg in map.segments(a..b) {
                    assert!(seg.start < seg.end, "non-empty");
                    assert_eq!(seg.start, cursor, "consecutive");
                    assert!(
                        !starts.iter().any(|&s| seg.start < s && s < seg.end),
                        "segment {seg:?} crosses a hard boundary ({starts:?})"
                    );
                    cursor = seg.end;
                }
                assert_eq!(cursor, b, "claim covered exactly");
            }
        }
    }

    #[test]
    fn granule_map_source_delegates_and_passes_posture_through() {
        let map = Arc::new(map_over(UNEVEN));
        // Posture is EXPLICIT and independent of geometry: all four
        // combinations must read back exactly as constructed.
        for (whole, coalesce) in [(false, false), (true, false), (false, true), (true, true)] {
            let src = GranuleMapSource::new(Arc::clone(&map), whole, coalesce);
            assert_eq!(src.whole_boundary_claims(), whole);
            assert_eq!(src.coalesce_claims(), coalesce);
            assert_eq!(src.total_granules(), 24);
            assert_eq!(src.startup_c0(), 2);
            for s in 0..24 {
                assert_eq!(src.next_boundary_after(s), map.boundary_after(s));
            }
        }
        assert!(GranuleMapSource::new(map, false, false).stream_state().is_none());
    }

    #[test]
    fn granule_map_source_matches_synthetic_source_shape() {
        // A regular-boundary map answers exactly like the M0 synthetic
        // source with the same geometry (the scheduler-facing contract).
        let starts: Vec<u64> = (0..=3).map(|i| i * 8).collect();
        let map = Arc::new(GranuleMap::with_boundaries(Arc::new(starts), 16));
        let src = GranuleMapSource::new(map, false, false);
        let synth = SyntheticMorselSource::with_boundaries(24, 8);
        assert_eq!(src.total_granules(), synth.total_granules());
        assert_eq!(src.startup_c0(), synth.startup_c0());
        for s in 0..24 {
            assert_eq!(src.next_boundary_after(s), synth.next_boundary_after(s), "s={s}");
        }
    }
}
