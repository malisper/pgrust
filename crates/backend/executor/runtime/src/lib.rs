//! pgrust morsel runtime — M0 RUNTIME CORE.
//!
//! Design authority: docs/design/parallelism-redesign-2026-07.md (§2.1
//! runtime, §2.5 leaders submit-and-park / NO leader execution, §2.8
//! blocking-I/O permit model, §5 M0), notes/morsel-lit-review.md (Umbra
//! SIGMOD'21 mechanisms), docs/design/inter-query-scheduling.md (ratified
//! slot-array / stride shapes — INERT here, activated M5).
//!
//! What this crate is in M0: the complete runtime skeleton — worker pool
//! plumbing, ResourceGroup → TaskSet → Task scheduling structures, the
//! last-worker-out finalization protocol, duration-adaptive task sizing,
//! generation-keyed task lifecycle, RG completion waiters, stats — with NO
//! production caller. `PGRUST_RUNTIME=1` is the only way any of it runs and
//! it defaults OFF; the M0 merge gate is ZERO behavior change (regress
//! untouched-green, regress-diff byte-identical, off-path instruction cost
//! ≤0.001%).
//!
//! What it is deliberately NOT (yet):
//! - no leader execution path (§2.5 DECIDED: a participating leader only
//!   displaces a worker under the cores-permit cap; the deferred mechanism
//!   is documented in the design doc — do not build it here);
//! - no real pipelines (M1 wires cbstore scans through [`MorselSource`] /
//!   [`TaskSetWork`]); M0 exercises everything with synthetic sources;
//! - no stride/priority policy (fields present, single-RG FIFO behavior);
//! - no readahead (scan-side, M1).
//!
//! Thread accounting (§2.8): the pool is `cores + K` threads with exactly
//! `cores` execution permits. Permits cap RUNNING tasks; the K standbys
//! absorb permits released by declared blocking sections
//! ([`sync::IoGuard`], reserved in M0 for M1+ uring/spill/Waiter waits).
//!
//! MERGE PROVENANCE (m0-integration): the task lifecycle here is lane A's
//! (m0-harvest) design-donor extraction of the qualified scan-task lifecycle
//! — [`QueryTaskLifecycle`] / [`TaskHandle`] / [`TaskParticipant`] with
//! fail-closed [`ParticipantOwner`] admission and armed participant
//! outcomes. It replaced lane B's cfg(m0_harvest_lifecycle) interface shim
//! wholesale at merge; the scheduler (rg.rs / sched.rs) is the dispatcher
//! owner the donor demands.

mod clock;
mod lifecycle;
mod morsel;
mod rg;
mod sched;
mod sink;
mod sizing;
mod stats;
mod sync;
mod taskset;

#[cfg(not(loom))]
mod io;
#[cfg(not(loom))]
mod pool;

#[cfg(all(test, not(loom)))]
mod tests;

use std::sync::Arc;

pub use clock::{Clock, MonotonicClock, VirtualClock};
pub use lifecycle::{
    ForeignParticipationDisabled, Generation, LifecycleState, ParticipantOwner, QueryTaskLifecycle,
    TaskHandle, TaskLifecycle, TaskParticipant,
};
pub use morsel::{MorselRange, MorselSource, SyntheticMorselSource};
pub use rg::{
    CompletionWaiter, QuerySpec, RgHandle, RgOutcome, TaskSetSpec, TaskSetWork, WeakRgHandle,
};
pub use sched::{Step, WorkerLocal, DEFAULT_SLOTS, MAX_EXTERNAL_LANES};
pub use sink::{
    sealed_sink_tasksets, sink_tasksets, ParallelSink, SealedParallelSink, SealedSinkTaskSets,
    SinkProbe, SinkTaskSets,
};
pub use sizing::{Phase, SizingDecision, SizingParams, DEFAULT_T_MAX_NS, DEFAULT_T_MIN_NS, EWMA_ALPHA};
pub use stats::{RgStatsSnapshot, RuntimeStatsSnapshot};
pub use sync::{IoGuard, Semaphore};

#[cfg(not(loom))]
pub use pool::WorkerPool;

/// Pinned M0 interface, lane A's binder half: [`QueryTaskGuard`] is the
/// query-task binder's RAII bind/unbind of xact + snapshot + temp-namespace
/// for a foreign thread (re-exported from the `parallel` crate, where the
/// binder lives with its 9×5 fault matrix; drive it only through
/// [`with_query_task_binding`]). Production-only: loom models exercise the
/// lifecycle, not the binder.
#[cfg(not(loom))]
pub use parallel::{
    with_query_task_binding, InstallQueryTaskBinding, QueryTaskBindingGuard as QueryTaskGuard,
    QueryTaskBindingPolicy,
};

/// Kill switch (M0 deliverable 6): the runtime is OFF by default and nothing
/// in production paths engages it yet. `PGRUST_RUNTIME=1` enables; any other
/// value (or unset) disables. Read once.
#[cfg(not(loom))]
pub fn runtime_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PGRUST_RUNTIME").is_ok_and(|v| v == "1"))
}

/// Process-global runtime handle (M1): published once by the postmaster's
/// rtpool start (launch_backend::rtpool) so executor-side engagement can
/// reach the runtime without depending on the postmaster crates. None until
/// (and unless) the kill switch spawned the pool.
#[cfg(not(loom))]
static GLOBAL: std::sync::OnceLock<Arc<Runtime>> = std::sync::OnceLock::new();

#[cfg(not(loom))]
pub fn install_global(rt: Arc<Runtime>) -> &'static Arc<Runtime> {
    GLOBAL.get_or_init(|| rt)
}

#[cfg(not(loom))]
pub fn global() -> Option<&'static Arc<Runtime>> {
    GLOBAL.get()
}

#[derive(Clone, Copy, Debug)]
pub struct RuntimeConfig {
    /// Execution width = number of execution permits (= cores in
    /// production; §2.1 fixed pool sized to cores).
    pub workers: usize,
    /// Standby threads beyond `workers` (§2.8; absorb IoGuard permit
    /// releases). Total pool threads = workers + standbys.
    pub standbys: usize,
    /// Scheduler slots (≤ 128). Umbra's bound; smaller in loom models.
    pub slots: usize,
    pub sizing: SizingParams,
    /// eprintln lifecycle trace (PGRUST_RUNTIME_TRACE=1).
    pub trace: bool,
}

impl RuntimeConfig {
    pub fn new(workers: usize) -> RuntimeConfig {
        RuntimeConfig {
            workers,
            standbys: DEFAULT_STANDBYS,
            slots: DEFAULT_SLOTS,
            sizing: SizingParams::default(),
            trace: false,
        }
    }
}

/// Default K of §2.8 ("default small"): 2 standbys.
pub const DEFAULT_STANDBYS: usize = 2;

#[cfg(not(loom))]
impl RuntimeConfig {
    /// Production configuration. Knobs stay env vars for now — new real GUCs
    /// are barred by pg_settings byte-identity (the lane_pool precedent);
    /// t_max is "GUC-able" via PGRUST_RUNTIME_TMAX_US until the customized-
    /// option surface is chartered.
    pub fn from_env() -> RuntimeConfig {
        fn env_u64(name: &str) -> Option<u64> {
            std::env::var(name).ok().and_then(|v| v.parse().ok())
        }
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let workers = env_u64("PGRUST_RUNTIME_WORKERS").map(|n| n.max(1) as usize).unwrap_or(cores);
        let standbys =
            env_u64("PGRUST_RUNTIME_STANDBYS").map(|n| n as usize).unwrap_or(DEFAULT_STANDBYS);
        let t_max_ns =
            env_u64("PGRUST_RUNTIME_TMAX_US").map(|us| us.max(1) * 1_000).unwrap_or(DEFAULT_T_MAX_NS);
        let t_min_ns = env_u64("PGRUST_RUNTIME_TMIN_US")
            .map(|us| us.max(1) * 1_000)
            .unwrap_or_else(|| (t_max_ns / 4).max(1));
        RuntimeConfig {
            workers,
            standbys,
            slots: DEFAULT_SLOTS,
            sizing: SizingParams { t_max_ns, t_min_ns },
            trace: std::env::var("PGRUST_RUNTIME_TRACE").is_ok_and(|v| v == "1"),
        }
    }
}

/// The runtime: scheduling structures + the worker-step engine. Thread
/// spawning is layered above (see [`WorkerPool`] and launch_backend's
/// rtpool glue) so this type stays loom-drivable.
pub struct Runtime {
    sched: sched::Scheduler,
    config: RuntimeConfig,
    /// §2.9 ring registration: worker ordinal → io_uring ring id (None: no
    /// ring — uring unavailable, aio_uring not linked, or worker exited).
    /// Written by the worker loop at enter/exit; diagnostics + tests read.
    #[cfg(not(loom))]
    rings: std::sync::Mutex<Vec<Option<u32>>>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Arc<Runtime> {
        Self::with_clock(config, Arc::new(MonotonicClock::new()))
    }

    pub fn with_clock(config: RuntimeConfig, clock: Arc<dyn Clock>) -> Arc<Runtime> {
        let nthreads = config.workers + config.standbys;
        Arc::new(Runtime {
            sched: sched::Scheduler::new(
                nthreads,
                config.workers,
                config.slots,
                config.sizing,
                clock,
                config.trace,
            ),
            config,
            #[cfg(not(loom))]
            rings: std::sync::Mutex::new(vec![None; nthreads]),
        })
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Total pool threads (workers + standbys); sizes the pin board.
    pub fn nthreads(&self) -> usize {
        self.sched.nthreads()
    }

    /// Submit a query's resource group. The LEADER'S ONLY MOVES are this and
    /// parking on the returned waiter (§2.5: submit-and-park; no leader
    /// execution path exists, deliberately).
    pub fn submit(&self, spec: QuerySpec) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, false);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// Submit a PINNED resource group (M1 scan pipelines): executed only by
    /// external participant threads driving [`Runtime::drive_pinned`] — the
    /// query's own bound parallel helpers, which carry the session state the
    /// work needs. Pool workers never claim from pinned RGs (they cannot
    /// bind foreign query state yet; §2.3 db-pinned pool binding is the M2+
    /// retirement path, at which point pinned submission collapses into
    /// `submit`). Everything else — morsel cursor, adaptive sizing, the
    /// last-worker-out finalization protocol, abort drain, completion — is
    /// the ordinary runtime machinery.
    pub fn submit_pinned(&self, spec: QuerySpec) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, true);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// Per-thread bookkeeping for an external participant (pin-board lane
    /// `nthreads + ordinal`; at most [`sched::MAX_EXTERNAL_LANES`] lanes).
    /// Callers MUST hold the lane through [`Runtime::acquire_external_lane`]
    /// — pin-board lanes are a process-wide resource: two concurrent
    /// participants on one lane corrupt the finalization protocol's pins.
    pub fn external_local(&self, ordinal: usize) -> WorkerLocal {
        self.sched.external_local(ordinal)
    }

    /// Lease one external pin-board lane (process-wide; None = all
    /// [`MAX_EXTERNAL_LANES`] busy — the caller must refuse participation).
    /// The lease releases on drop; hold it across the whole drive. Lanes are
    /// a PROCESS resource: two concurrent participants on one lane would
    /// corrupt the finalization protocol's pins (publish-over-unsettled).
    pub fn acquire_external_lane(self: &Arc<Self>) -> Option<ExternalLane> {
        let mask = &self.sched.external_lanes;
        loop {
            let cur = mask.load(std::sync::atomic::Ordering::SeqCst);
            let free = !cur;
            if free == 0 {
                return None;
            }
            let bit = free.trailing_zeros() as usize;
            if mask
                .compare_exchange(
                    cur,
                    cur | (1u64 << bit),
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                return Some(ExternalLane { rt: Arc::clone(self), ordinal: bit });
            }
        }
    }

    /// Drive one pinned RG as an external participant until it completes.
    /// (Production-only: the loom models drive `worker_step` shapes and
    /// poll try_wait; the pinned driver's yield is a std thread op.)
    /// The participant obeys the execution-permit cap (a permit is held
    /// across each step, released before parking) and observes aborts at
    /// morsel boundaries like any pool worker. The WORK must not unwind
    /// (TaskSetWork::run_morsel is infallible by contract — implementations
    /// catch their own errors, record them, and abort the RG); a panic
    /// escaping a step would strand the participant's pin and wedge the
    /// finalization protocol.
    /// Bounded pinned drive for CLEANUP paths (abort drains): like
    /// [`Runtime::drive_pinned`] but gives up after `max_idle` consecutive
    /// idle observations (never parks — cleanup must not sleep on wakes a
    /// dead participant will never send). None = the RG could not be
    /// completed (a participant died holding an unsettled pin); the caller
    /// must treat the RG as leaked and error out loudly.
    #[cfg(not(loom))]
    pub fn try_drain_pinned(
        &self,
        local: &mut WorkerLocal,
        rg: &RgHandle,
        max_idle: u32,
    ) -> Option<RgOutcome> {
        let mut idle = 0u32;
        loop {
            if let Some(outcome) = rg.try_outcome() {
                return Some(outcome);
            }
            self.execution_permits().acquire();
            let step = self.sched.worker_step_pinned(local, &rg.rg);
            self.execution_permits().release();
            match step {
                Step::Ran => idle = 0,
                Step::Retry => std::thread::yield_now(),
                Step::Idle => {
                    idle += 1;
                    if idle >= max_idle {
                        return rg.try_outcome();
                    }
                    std::thread::sleep(std::time::Duration::from_micros(500));
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        }
    }

    #[cfg(not(loom))]
    pub fn drive_pinned(&self, local: &mut WorkerLocal, rg: &RgHandle) -> RgOutcome {
        loop {
            if let Some(outcome) = rg.try_outcome() {
                // WFIN marker channel: the drive is over — flush any
                // participation this driver still holds (a worker whose last
                // task did not observe exhaustion emits here).
                local.wfin_flush_all();
                return outcome;
            }
            let epoch = self.park_epoch();
            self.execution_permits().acquire();
            let step = self.sched.worker_step_pinned(local, &rg.rg);
            self.execution_permits().release();
            match step {
                Step::Ran => {}
                Step::Retry => std::thread::yield_now(),
                Step::Idle => {
                    if rg.try_outcome().is_some() {
                        continue;
                    }
                    self.park(epoch);
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        }
    }

    /// Per-thread scheduling bookkeeping; `worker` ∈ 0..nthreads().
    pub fn worker_local(&self, worker: usize) -> WorkerLocal {
        self.sched.worker_local(worker)
    }

    /// One scheduling decision + at most one task execution. Drivers: the
    /// pool worker loop, and the loom models. See the pool loop for the
    /// required epoch-capture/park discipline around `Step::Idle`.
    pub fn worker_step(&self, local: &mut WorkerLocal) -> Step {
        self.sched.worker_step(local)
    }

    /// The execution-permit semaphore (permits = workers). Task-executing
    /// threads hold a permit; declared blocking sections release it through
    /// [`Semaphore::io_section`].
    pub fn execution_permits(&self) -> &Semaphore {
        &self.sched.permits
    }

    /// Park-lot epoch; capture BEFORE worker_step, park on Idle.
    pub fn park_epoch(&self) -> u64 {
        self.sched.park.epoch()
    }

    pub fn park(&self, seen: u64) {
        sched_park(&self.sched, seen);
    }

    /// §2.9 ring registration: record `worker`'s ring id at loop enter
    /// (Some) / exit (None). Called by the worker loop only.
    #[cfg(not(loom))]
    pub(crate) fn register_worker_ring(&self, worker: usize, ring: Option<u32>) {
        let mut g = self.rings.lock().unwrap_or_else(|e| e.into_inner());
        g[worker] = ring;
    }

    /// The io_uring ring id registered by `worker`, if any.
    #[cfg(not(loom))]
    pub fn worker_ring(&self, worker: usize) -> Option<u32> {
        let g = self.rings.lock().unwrap_or_else(|e| e.into_inner());
        g.get(worker).copied().flatten()
    }

    /// Ask all workers to exit their loops (tests / shutdown).
    pub fn request_stop(&self) {
        self.sched.request_stop();
    }

    pub fn stats(&self) -> RuntimeStatsSnapshot {
        self.sched.snapshot()
    }
}

fn sched_park(sched: &sched::Scheduler, seen: u64) {
    stats::RuntimeStats::tick(&sched.stats.worker_parks);
    sched.park.park(seen);
}

/// RAII lease of one external pin-board lane (see
/// [`Runtime::acquire_external_lane`]).
pub struct ExternalLane {
    rt: Arc<Runtime>,
    ordinal: usize,
}

impl ExternalLane {
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }

    /// Fresh per-drive bookkeeping bound to this lane.
    pub fn local(&self) -> WorkerLocal {
        self.rt.external_local(self.ordinal)
    }
}

impl Drop for ExternalLane {
    fn drop(&mut self) {
        // Release the lane bit. The pin was settled by the drive (every
        // worker_step_pinned settles before returning), so the next lessee
        // starts from a clean pin.
        self.rt
            .sched
            .external_lanes
            .fetch_and(!(1u64 << self.ordinal), std::sync::atomic::Ordering::SeqCst);
    }
}
