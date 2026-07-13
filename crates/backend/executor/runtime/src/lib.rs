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

mod clock;
mod lifecycle;
mod morsel;
mod rg;
mod sched;
mod sizing;
mod stats;
mod sync;
mod taskset;

#[cfg(not(loom))]
mod pool;

#[cfg(all(test, not(loom)))]
mod tests;

use std::sync::Arc;

pub use clock::{Clock, MonotonicClock, VirtualClock};
pub use lifecycle::{Generation, QueryTaskGuard, TaskLifecycle};
pub use morsel::{MorselRange, MorselSource, SyntheticMorselSource};
pub use rg::{CompletionWaiter, QuerySpec, RgHandle, RgOutcome, TaskSetSpec, TaskSetWork};
pub use sched::{Step, WorkerLocal, DEFAULT_SLOTS};
pub use sizing::{Phase, SizingDecision, SizingParams, DEFAULT_T_MAX_NS, DEFAULT_T_MIN_NS, EWMA_ALPHA};
pub use stats::{RgStatsSnapshot, RuntimeStatsSnapshot};
pub use sync::{IoGuard, Semaphore};

#[cfg(not(loom))]
pub use pool::WorkerPool;

/// Kill switch (M0 deliverable 6): the runtime is OFF by default and nothing
/// in production paths engages it yet. `PGRUST_RUNTIME=1` enables; any other
/// value (or unset) disables. Read once.
#[cfg(not(loom))]
pub fn runtime_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PGRUST_RUNTIME").is_ok_and(|v| v == "1"))
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
        let rg = self.sched.submit(spec);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
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
