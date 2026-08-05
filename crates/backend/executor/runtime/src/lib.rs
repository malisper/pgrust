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
//! - no real pipelines (M1 wires pgrcolumnar scans through [`MorselSource`] /
//!   [`TaskSetWork`]); M0 exercises everything with synthetic sources;
//! - no stride/priority policy (fields present, single-RG FIFO behavior)
//!   — SUPERSEDED (M5-4): stride/fair-share at equal shares is live (see
//!   sched.rs module doc; PGRUST_RUNTIME_STRIDE=0 restores FIFO); decaying
//!   priorities + p_min stay M5-5;
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

mod blocking;
mod clock;
mod funnel;
mod ledger;

/// PHASE3-CLOSE WS-WIDTH: whether non-pool gangs (Gather/GatherMerge
/// bgworker gangs now; index gangs / CREATE INDEX / vacuum's parallel
/// index phase later, per morsel-runtime-v2 §4) lease width from the
/// admission ledger's pool face. Resolved by [`gang_width_face`] — ONE
/// OnceLock read (§2.6 knob-OFF budget pin: the gather startup path costs
/// ONE OnceLock bool read knob-OFF). The WS-O external arm and its knob
/// were retired by the W4 audited removal (the knob died with the body;
/// see notes/se-p3close-width.md §6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GangWidthFace {
    /// Knob off (the stock default): no lease, today's launch.
    Off,
    /// `PGRUST_RUNTIME_WIDTH_UNIFIED` — unified pool-face gang entries
    /// (requires `PGRUST_RUNTIME_LEDGER_V2` downstream; the fail-open
    /// ladder: face off / no runtime / ledger off / capacity exhausted →
    /// stock launch byte-exactly).
    Unified,
}

/// The one process-static gang-knob resolve (see [`GangWidthFace`]).
pub fn gang_width_face() -> GangWidthFace {
    static KNOB: std::sync::OnceLock<GangWidthFace> = std::sync::OnceLock::new();
    *KNOB.get_or_init(|| {
        if matches!(
            std::env::var("PGRUST_RUNTIME_WIDTH_UNIFIED").as_deref(),
            Ok("1") | Ok("on")
        ) {
            GangWidthFace::Unified
        } else {
            GangWidthFace::Off
        }
    })
}
mod lifecycle;
mod morsel;
mod rg;
mod sched;
mod sink;
mod sizing;
mod stats;
mod sync;
mod taskset;

// WS-S wave-3: caller.rs is loom-modeled from C2 on (the parked-drive
// liveness model) — the module is loom-clean (ParkLot/Semaphore come from
// the sync shim; the Retry yield is cfg-switched) and no longer gated.
mod caller;
#[cfg(not(loom))]
mod io;
#[cfg(not(loom))]
mod pool;

#[cfg(all(test, not(loom)))]
mod tests;

use std::sync::Arc;

pub use blocking::{blocking_io_section, BlockingIoSection, PermitThreadReg};
pub use clock::{Clock, MonotonicClock, VirtualClock};
pub use funnel::{
    DrainStep, FunnelDrain, FunnelProducer, PushOutcome, RowEmitWork, RowFunnel, SpscRing,
};
pub use ledger::{ArrivalNudge, ClaimVerdict, LedgerBudgets, LedgerSnapshot, WidthRequest};
pub use lifecycle::{
    ForeignParticipationDisabled, Generation, LifecycleState, ParticipantOwner, QueryTaskLifecycle,
    TaskHandle, TaskLifecycle, TaskParticipant,
};
pub use morsel::{MorselRange, MorselSource, StreamSource, SyntheticMorselSource};
pub use morsel::{GranuleMap, GranuleMapSource, Segments};
pub use rg::{
    BoundDescriptor, BoundServe, CompletionWaiter, QuerySpec, RgClass, RgHandle, RgOutcome,
    TaskSetSpec, TaskSetWork, WeakRgHandle,
};
pub use sched::{DriveLocal, Step, WorkerLocal, DEFAULT_SLOTS, MAX_EXTERNAL_LANES};
#[cfg(not(loom))]
pub use sched::install_qos_mem_probe;
pub use sink::{
    sealed_sink_tasksets, sink_tasksets, ParallelSink, SealedParallelSink, SealedSinkTaskSets,
    SinkProbe, SinkTaskSets,
};
pub use sizing::{Phase, SizingDecision, SizingParams, DEFAULT_T_MAX_NS, DEFAULT_T_MIN_NS, EWMA_ALPHA};
pub use stats::{RgStatsSnapshot, RuntimeStatsSnapshot};
pub use sync::{IoGuard, Semaphore};

pub use caller::CallerWorker;
#[cfg(not(loom))]
pub use pool::{worker_loop, WorkerPool};

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

/// Kill switch (M0 deliverable 6; DEFAULT FLIPPED at the M5 boarding —
/// docs/design/m5-planner.md §4.4 criteria met): the runtime pool is ON by
/// default. ONE authority (t34-config review, defect 3): this reads the
/// registered `pgrust.runtime` GUC's backing cell — the same cell the pool
/// spawn gate (launch_backend::rtpool) and the M5-3 planner probe key off —
/// so no reader can disagree with another. `PGRUST_RUNTIME=0` still kills:
/// it SEEDS the cell at boot (guc/store.rs
/// initialize_guc_options_from_environment, PGC_S_ENV_VAR); postgresql.conf
/// / -c can also set it. PGC_POSTMASTER scope: the value is fixed before any
/// executor reads it, and the cell read is one Relaxed atomic load (cheaper
/// than the env read this replaces; deliberately NOT cached so a pre-seed
/// early call cannot freeze a stale value).
#[cfg(not(loom))]
pub fn runtime_enabled() -> bool {
    guc_tables::backing::pgrust_runtime()
}

/// Process-global runtime handle (M1): published once by the postmaster's
/// rtpool start (launch_backend::rtpool) so executor-side engagement can
/// reach the runtime without depending on the postmaster crates. None until
/// (and unless) the kill switch spawned the pool.
#[cfg(not(loom))]
static GLOBAL: crate::sync::OnceLock<Arc<Runtime>> = crate::sync::OnceLock::new();

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
    /// Arc'd so submitted RGs can hold a Weak back-reference for the M5-4
    /// queued-abort reap (the RG never keeps the scheduler alive).
    sched: Arc<sched::Scheduler>,
    config: RuntimeConfig,
    /// §2.9 ring registration: worker ordinal → io_uring ring id (None: no
    /// ring — uring unavailable, aio_uring not linked, or worker exited).
    /// Written by the worker loop at enter/exit; diagnostics + tests read.
    #[cfg(not(loom))]
    rings: crate::sync::Mutex<Vec<Option<u32>>>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Arc<Runtime> {
        Self::with_clock(config, Arc::new(MonotonicClock::new()))
    }

    pub fn with_clock(config: RuntimeConfig, clock: Arc<dyn Clock>) -> Arc<Runtime> {
        let nthreads = config.workers + config.standbys;
        Arc::new(Runtime {
            sched: Arc::new(sched::Scheduler::new(
                nthreads,
                config.workers,
                config.slots,
                config.sizing,
                clock,
                config.trace,
            )),
            config,
            #[cfg(not(loom))]
            rings: crate::sync::Mutex::new(vec![None; nthreads]),
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
        let rg = self.sched.submit(spec, false, RgClass::Foreground, 0, None, None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// [`Runtime::submit`] with a leader-session affinity token (M5-4): the
    /// stride pick's equal-pass tiebreak prefers workers sticky-bound to
    /// this session (set theirs via [`WorkerLocal::set_session_token`]).
    /// Token 0 ⇔ no affinity ⇔ plain `submit`. INTEGRATION-TRAIN NOTE:
    /// [`QuerySpec`] deliberately does NOT carry the token (its literal
    /// constructors stay source-compatible across lanes); the M5-1 router
    /// should submit through this entry with the leader's session identity.
    pub fn submit_with_affinity(
        &self,
        spec: QuerySpec,
        session_token: u64,
    ) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, false, RgClass::Foreground, session_token, None, None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// [`Runtime::submit`] carrying an explicit [`WidthRequest`] (WS-B
    /// admission ledger, single-executor Phase 0.1). [`QuerySpec`] stays
    /// literally source-compatible (the integration-train law above) — new
    /// entries carry the width beside the spec; existing entries pass the
    /// no-information request. The request is ledger policy only: with the
    /// PGRUST_RUNTIME_LEDGER_V2 switch OFF (the default) it is recorded
    /// nowhere and behavior is byte-identical to [`Runtime::submit`].
    pub fn submit_with_width(
        &self,
        spec: QuerySpec,
        width: WidthRequest,
    ) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, false, RgClass::Foreground, 0, Some(width), None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// [`Runtime::submit_pinned_with_affinity`] carrying an explicit
    /// [`WidthRequest`] — see [`Runtime::submit_with_width`].
    pub fn submit_pinned_with_width(
        &self,
        spec: QuerySpec,
        session_token: u64,
        width: WidthRequest,
    ) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, true, RgClass::Foreground, session_token, Some(width), None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// WS-B ledger-policy toggle (tests / A-B). Production default is the
    /// PGRUST_RUNTIME_LEDGER_V2 kill switch, read once at construction —
    /// DEFAULT OFF at this increment. Toggle before submitting.
    pub fn set_ledger(&self, on: bool) {
        self.sched.set_ledger(on);
    }

    pub fn ledger_enabled(&self) -> bool {
        self.sched.ledger_enabled()
    }

    /// Admission-ledger instrument readback (tests / diagnostics).
    pub fn ledger_snapshot(&self) -> LedgerSnapshot {
        self.sched.ledger.snapshot()
    }

    /// Lease parallel width for a non-pool gang (Gather's bgworker
    /// helpers) through the admission ledger. `requested` is denominated
    /// in WORKERS — the LEADER IS NOT COUNTED (C parity;
    /// parallel_leader_participation rides free). THE STABLE CALLER FACE
    /// (PHASE3-CLOSE §2.1): backed by a unified pool-face gang entry
    /// (frozen, non-shedding, charged by the ONE recompute) — the ONLY
    /// leased path since the W4 removal of the WS-O external face.
    ///
    /// None = FAIL-OPEN: the ledger is OFF (`PGRUST_RUNTIME_LEDGER_V2`
    /// unset / per-instance toggle off) or the gang capacity (64) is
    /// exhausted — the caller keeps today's uncapped launch path exactly
    /// (never block, never error).
    ///
    /// Some(lease): launch at most [`ParallelWidthLease::granted`] workers.
    /// **The grant MAY BE 0 — the caller MUST have a serial path** (e.g.
    /// Gather's leader-local scan). The grant is a HEADROOM-ONLY, FROZEN
    /// ceiling (ledger.rs module doc); after launching, call
    /// [`ParallelWidthLease::settle`] with the live worker count so parked
    /// gang width is released (granted counts ACTIVE width — the
    /// composition rule). Dropping the lease retires the entry.
    pub fn lease_parallel_width(self: &Arc<Self>, requested: u32) -> Option<ParallelWidthLease> {
        let (id, granted) = self.sched.lease_gang_width(requested)?;
        Some(ParallelWidthLease { rt: Arc::clone(self), id, granted })
    }

    /// WS-B test hook (the set_decay_quantum_ns precedent): per-instance
    /// JOIN threshold so deterministic tests exercise sub-threshold
    /// admission. Production keeps PGRUST_RUNTIME_LEDGER_JOIN_US (default
    /// 0 = inert until the calibration lane lands a measured value).
    pub fn set_ledger_join_threshold_ns(&self, ns: u64) {
        self.sched.ledger.set_join_threshold_ns(ns);
    }

    /// M5-4 stride toggle (tests / A-B arms). Production default is the
    /// PGRUST_RUNTIME_STRIDE kill switch, read once at construction:
    /// ON ⇒ stride/fair-share pick; OFF ⇒ the M0 FIFO pick, byte-identical.
    pub fn set_stride(&self, on: bool) {
        self.sched.set_stride(on);
    }

    pub fn stride_enabled(&self) -> bool {
        self.sched.stride_enabled()
    }

    /// M5+1 pipeline-DAG dispatch toggle (tests / A-B arms). Production
    /// default is the `PGRUST_RUNTIME_PIPELINE_DAG` switch, read once at
    /// construction — DEFAULT OFF at this increment. ON ⇒ every
    /// dependency-satisfied task set of an RG is published concurrently,
    /// each in its own slot (independent-subtree overlap, m5-planner §3.6);
    /// OFF ⇒ the sequential one-slot task-set walk, byte-identical.
    pub fn set_dag(&self, on: bool) {
        self.sched.set_dag(on);
    }

    pub fn dag_enabled(&self) -> bool {
        self.sched.dag_enabled()
    }

    /// M5-5 decaying-priorities toggle (tests / A-B arms). Production
    /// default is the `PGRUST_RUNTIME_DECAY` kill switch, read once at
    /// construction — default ON. OFF ⇒ every RG stays at p0: the M5-4
    /// equal-shares scheduler exactly.
    pub fn set_decay(&self, on: bool) {
        self.sched.set_decay(on);
    }

    pub fn decay_enabled(&self) -> bool {
        self.sched.decay_enabled()
    }

    /// M5-5 test hook: tighten the consumed-CPU decay quantum so
    /// deterministic virtual-clock tests cross decay boundaries. Production
    /// keeps the ratified constant (50ms CPU).
    pub fn set_decay_quantum_ns(&self, ns: u64) {
        self.sched.set_decay_quantum_ns(ns);
    }

    /// M5-5 test hook: probe alternative starvation floors (adversarial
    /// skew tests). Production keeps the ratified default (p0/16 = 625).
    pub fn set_p_min(&self, p: u32) {
        self.sched.set_p_min(p);
    }

    pub fn p_min(&self) -> u32 {
        self.sched.p_min_value()
    }

    /// Submit a MAINTENANCE resource group (M4 background-job cycles,
    /// docs/design/m4-bgjobs.md §3.4/§3.5): pool-executed like `submit`, but
    /// preferred by the pick over foreground FIFO order and admitted ahead
    /// of the wait queue — the minimal starvation floor for GUC-cadence job
    /// deadlines. Cycle task sets are single-morsel by construction, so the
    /// preference diverts at most one worker for one cycle body.
    pub fn submit_maintenance(&self, spec: QuerySpec) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, false, RgClass::Maintenance, 0, None, None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// Submit a UTILITY resource group (Track-4 Q0/Q1, scratchpad/night/
    /// pool-qos-design.md §1/§3): bulk background compute — vacuum drivers,
    /// index build, COPY after their M4 migrations. Pool-executed with
    /// ordinary FIFO admission (never overtakes; NOT the Maintenance
    /// pick-preference class), but served at the utility stride weight
    /// (p_util, default = the ratified p_min floor: guaranteed nonzero
    /// share, ≥1/(1+K·p0/p_util) against K fresh queries — no starvation)
    /// and, under the WS-B ledger, admitted into the CAPPED utility budget
    /// tier: soft cap B_util (`PGRUST_RUNTIME_UTIL_PERMITS`, default
    /// cores/8) whenever foreground is admitted, full fair share on an
    /// idle pool, reclaim on foreground arrival bounded by one claim
    /// (should_continue's Yield). `width` carries the utility work's
    /// ceiling/footprint (ledger policy only; None = no-information).
    /// Kill switch `PGRUST_RUNTIME_UTIL_QOS=0` folds this to a plain
    /// foreground submit. NO CONSUMERS on this train — the surface lands
    /// ahead of M4.1 (first consumer, gated on M2 inc-2) so Track 3's
    /// QoS gate is discharged by the synthetic proofs in tests.rs.
    pub fn submit_utility(
        &self,
        spec: QuerySpec,
        width: Option<WidthRequest>,
    ) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, false, RgClass::Utility, 0, width, None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// Track-4 kill-switch toggle (tests / A-B arms). Production default is
    /// the PGRUST_RUNTIME_UTIL_QOS switch, read once at construction —
    /// default ON (inert without utility submissions). OFF ⇒
    /// [`Runtime::submit_utility`] behaves as plain [`Runtime::submit`].
    pub fn set_util_qos(&self, on: bool) {
        self.sched.set_util_qos(on);
    }

    pub fn util_qos_enabled(&self) -> bool {
        self.sched.util_qos_enabled()
    }

    /// Track-4 Q0 test hook (the set_p_min precedent): probe alternative
    /// utility stride weights. Production keeps
    /// PGRUST_RUNTIME_UTIL_PRIORITY (default = p_min = p0/16).
    pub fn set_p_util(&self, p: u32) {
        self.sched.set_p_util(p);
    }

    pub fn p_util(&self) -> u32 {
        self.sched.p_util_value()
    }

    /// Track-4 Q1 test hook (the set_ledger_join_threshold_ns precedent):
    /// per-instance utility permit budget B_util. Production keeps
    /// PGRUST_RUNTIME_UTIL_PERMITS (default max(1, cores/8)).
    pub fn set_util_permits(&self, n: u32) {
        self.sched.ledger.set_util_cores(n);
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
        self.submit_pinned_with_affinity(spec, 0)
    }

    /// [`Runtime::submit_pinned`] with a leader-session affinity token
    /// (M5-4 × M5-1 integration seam): pinned RGs bypass the stride pick
    /// today (pool workers never claim from them), so the token is inert
    /// but RECORDED on the RG's slot — when §2.3 db-pinned pool binding
    /// retires pinned submission into [`Runtime::submit`], the affinity
    /// plumbing is already in place and the equal-pass tiebreak engages
    /// with no arm-side change. Token 0 ⇔ no affinity.
    pub fn submit_pinned_with_affinity(
        &self,
        spec: QuerySpec,
        session_token: u64,
    ) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(spec, true, RgClass::Foreground, session_token, None, None, None);
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// M2 inc-2 (PGPROC-leasing pool workers): [`Runtime::submit_pinned_with_affinity`]
    /// CARRYING a bound-engagement descriptor — the scheduler's bindability
    /// gate. Unlike a plain pinned submission, publication SETS the
    /// pool-visible active bit: pool workers' picks reach the RG and each
    /// serves one engagement through `descriptor.serve` (ticket claim →
    /// leased-PGPROC identity → query-task bind → external-lane drive to an
    /// outcome → unbind → detach). External participants (launched/standing
    /// helpers, the leader's cleanup drains) drive it exactly as any pinned
    /// RG — the two channels compose on the ordinary morsel protocol.
    ///
    /// The descriptor must be attached AT SUBMIT (never retro-fitted): the
    /// active bit and the gate key off it at publication time.
    ///
    /// `on_rg` runs with the submission's RgHandle BEFORE the RG can become
    /// pool-visible (M2 inc-3 rung 3 — closing the rg-set-after-publish
    /// window): the serve dispatches into arm code that resolves the RG
    /// through a caller-owned cell, and publication used to race the
    /// caller's post-return store — pool picks landing inside the window
    /// paid a spurious claim + "rg gone" refusal + detach per worker, and a
    /// 1-ticket board's single spurious refusal met the leader's
    /// `refused >= tickets` needle and flipped the engagement to the
    /// fallback channel. Set every serve-visible cell here; the callback
    /// runs pre-admission on the submitting thread (keep it tiny — a store,
    /// no locks the serve path takes).
    pub fn submit_pinned_bound(
        &self,
        spec: QuerySpec,
        session_token: u64,
        descriptor: BoundDescriptor,
        on_rg: impl FnOnce(&RgHandle),
    ) -> (RgHandle, CompletionWaiter) {
        let mut on_rg = Some(on_rg);
        let mut call = |rg: &RgHandle| {
            if let Some(f) = on_rg.take() {
                f(rg);
            }
        };
        let rg = self.sched.submit(
            spec,
            true,
            RgClass::Foreground,
            session_token,
            None,
            Some(descriptor),
            Some(&mut call),
        );
        (RgHandle { rg: Arc::clone(&rg) }, CompletionWaiter { rg })
    }

    /// M4.1 (utility compute onto the pool — scratchpad/night/
    /// all-morsels-plan.md Track 3): [`Runtime::submit_pinned_bound`] at
    /// QoS class UTILITY with a width request — the composition the vacuum
    /// driver swap rides. The RG is pinned (the leader and any fallback
    /// helpers drive it externally), carries the bound-engagement
    /// descriptor (pool workers lease PGPROC identity and serve it through
    /// the inc-2 gate), is served at the utility stride weight (Q0), and
    /// its width flows into the ledger's CAPPED utility tier (Q1) so bulk
    /// background work cannot crowd foreground queries when the ledger is
    /// armed. The Track-4 kill switch (PGRUST_RUNTIME_UTIL_QOS=0) folds
    /// the class to Foreground at submit, exactly as for
    /// [`Runtime::submit_utility`].
    pub fn submit_pinned_bound_utility(
        &self,
        spec: QuerySpec,
        session_token: u64,
        width: Option<WidthRequest>,
        descriptor: BoundDescriptor,
    ) -> (RgHandle, CompletionWaiter) {
        let rg = self.sched.submit(
            spec,
            true,
            RgClass::Utility,
            session_token,
            width,
            Some(descriptor),
            None,
        );
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
        // Low-word-first scan over the multi-word lease mask (DOP-192:
        // MAX_EXTERNAL_LANES = 256 ⇒ 4 words). A word that fills under us
        // is skipped; a CAS failure retries the same word.
        for (wi, mask) in self.sched.external_lanes.iter().enumerate() {
            loop {
                let cur = mask.load(crate::sync::atomic::Ordering::SeqCst);
                let free = !cur;
                if free == 0 {
                    break; // word full — try the next word
                }
                let bit = free.trailing_zeros() as usize;
                if mask
                    .compare_exchange(
                        cur,
                        cur | (1u64 << bit),
                        crate::sync::atomic::Ordering::SeqCst,
                        crate::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return Some(ExternalLane { rt: Arc::clone(self), ordinal: wi * 64 + bit });
                }
            }
        }
        None
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
                self.sched.stat_flush_all(local);
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
                        self.sched.stat_flush_all(local);
                        return rg.try_outcome();
                    }
                    std::thread::sleep(std::time::Duration::from_micros(500));
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        }
    }

    /// BOUNDED-PARTICIPATION pinned drive (Q2 leader participation): run at
    /// most `max_tasks` tasks of `rg` and return — NEVER parks. The caller
    /// interleaves its own duties (interrupt checks, progress relays)
    /// between calls, so its cancel-observation cadence is bounded by one
    /// task (~t_max sizer budget) plus its own loop, independent of the
    /// total work — the long-unit law for session threads that also owe a
    /// polling duty. Some(outcome) = the RG completed; None = still
    /// running (a task budget elapsed, or the step went Idle: nothing
    /// claimable right now — starved stream / queued slot — or a bounded
    /// Retry spin expired). The second element counts tasks RUN this call —
    /// 0 with a None outcome means idle (callers may park on their own
    /// wait channel; a publish will not wake it, so keep that park bounded).
    /// Same permit discipline as [`Runtime::try_drain_pinned`] (per-step
    /// acquire/release).
    #[cfg(not(loom))]
    pub fn drive_pinned_tasks(
        &self,
        local: &mut WorkerLocal,
        rg: &RgHandle,
        max_tasks: u32,
    ) -> (Option<RgOutcome>, u32) {
        let mut ran = 0u32;
        let mut retries = 0u32;
        let flush = |s: &Self, local: &mut WorkerLocal| {
            s.sched.stat_flush_all(local);
            local.wfin_flush_all();
        };
        loop {
            if let Some(outcome) = rg.try_outcome() {
                flush(self, local);
                return (Some(outcome), ran);
            }
            if ran >= max_tasks {
                flush(self, local);
                return (None, ran);
            }
            self.execution_permits().acquire();
            let step = self.sched.worker_step_pinned(local, &rg.rg);
            self.execution_permits().release();
            match step {
                Step::Ran => {
                    ran += 1;
                    retries = 0;
                }
                Step::Retry => {
                    // Invalidated-slot windows are bounded; a stuck window
                    // returns to the caller's duty loop instead of spinning.
                    retries += 1;
                    if retries >= crate::sched::RETRY_PARK_AFTER {
                        flush(self, local);
                        return (None, ran);
                    }
                    std::thread::yield_now();
                }
                Step::Idle => {
                    flush(self, local);
                    return (None, ran);
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        }
    }

    /// POOL-QOS (GL-POOLDB-1 mitigation): [`Runtime::drive_pinned`] for a
    /// POOL SERVE's nested drive — identical STEP-V2 loop plus the three
    /// class-aware moves, all inert when `PGRUST_RUNTIME_POOL_QOS=0` or no
    /// interactive demand exists:
    ///
    ///   1. INTERACTIVE drives (the RG is still undecayed — the ratified
    ///      M5-5 constants make that "< 50ms consumed CPU") acquire their
    ///      permit through the semaphore's PRIORITY lane;
    ///   2. DEMOTED drives holding a permit release/re-acquire it at step
    ///      boundaries while priority waiters are parked (first-permit
    ///      latency for a fresh engagement ≈ one morsel, not one query);
    ///   3. DEMOTED drives YIELD THE WHOLE SERVE at a step boundary when
    ///      interactive demand is live and `should_yield` grants (the
    ///      board's last-active-guarded exit): the drive returns `None`
    ///      WITHOUT an outcome — the serve's thread frees to pick the
    ///      waiting interactive engagement; the yielded capacity refills
    ///      through the board's concurrent ticket cap.
    ///
    /// `should_yield` is the arm-side board grant (the runtime cannot name
    /// the parallel crate); once it returns true the drive exits
    /// immediately — a grant is always spent. Returns `Some(outcome)` when
    /// the RG completed under this participant, `None` on a yield.
    #[cfg(not(loom))]
    pub fn drive_pinned_yieldable(
        &self,
        local: &mut WorkerLocal,
        rg: &RgHandle,
        should_yield: &mut dyn FnMut() -> bool,
    ) -> Option<RgOutcome> {
        if !crate::sched::step_v2() || !crate::sched::pool_qos_enabled() {
            return Some(self.drive_pinned(local, rg));
        }
        let cprobe = crate::sched::cprobe_enabled();
        let mut held = false;
        let mut retries = 0u32;
        let outcome = loop {
            if let Some(outcome) = rg.try_outcome() {
                self.sched.stat_flush_all(local);
                local.wfin_flush_all();
                break Some(outcome);
            }
            // Class read per iteration: demotion mid-drive downgrades the
            // drive's posture at the next boundary (one Relaxed load).
            let interactive =
                rg.rg.priority.load(crate::sync::atomic::Ordering::Relaxed)
                    >= crate::sched::qos_interactive_p();
            let epoch = self.park_epoch();
            if !held {
                if interactive {
                    self.execution_permits().acquire_priority();
                    stats::RuntimeStats::tick(&self.sched.stats.qos_priority_acquires);
                } else {
                    self.execution_permits().acquire();
                }
                held = true;
            }
            let step = self.sched.worker_step_pinned(local, &rg.rg);
            match step {
                Step::Ran => {
                    retries = 0;
                    if !interactive {
                        // Memory governor (GL-CONCMEM-1): both QoS moves
                        // below multiply concurrently-live engagement
                        // working sets (round-robin progress keeps every
                        // partial state resident). Over the bar, hold them
                        // — this serve runs to completion (the pre-QoS
                        // shape, and the fastest shed) and the interleave
                        // resumes under the bar.
                        if crate::sched::qos_mem_over_bar() {
                            stats::RuntimeStats::tick(&self.sched.stats.qos_mem_holds);
                        } else {
                            // Move 3: yield the serve toward live interactive
                            // demand (board grant = the last-active guard).
                            if self.sched.qos_demand_live() && should_yield() {
                                stats::RuntimeStats::tick(&self.sched.stats.qos_yields);
                                self.sched.stat_flush_all(local);
                                local.wfin_flush_all();
                                break None;
                            }
                            // Move 2: hand the permit through the priority
                            // lane's deferral window, then take one back.
                            if self.execution_permits().priority_waiting() > 0 {
                                stats::RuntimeStats::tick(&self.sched.stats.qos_permit_defers);
                                self.execution_permits().release();
                                self.execution_permits().acquire();
                            }
                        }
                    }
                }
                Step::Retry => {
                    local.drive.steps_retry += 1;
                    retries += 1;
                    if retries >= crate::sched::RETRY_PARK_AFTER {
                        retries = 0;
                        self.execution_permits().release();
                        held = false;
                        local.drive.parks += 1;
                        self.park(epoch);
                    } else if cprobe {
                        let t0 = std::time::Instant::now();
                        std::thread::yield_now();
                        local.drive.retry_spin_ns += t0.elapsed().as_nanos() as u64;
                    } else {
                        std::thread::yield_now();
                    }
                }
                Step::Idle => {
                    retries = 0;
                    if rg.try_outcome().is_some() {
                        continue;
                    }
                    self.execution_permits().release();
                    held = false;
                    local.drive.parks += 1;
                    self.park(epoch);
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        };
        if held {
            self.execution_permits().release();
        }
        outcome
    }

    #[cfg(not(loom))]
    pub fn drive_pinned(&self, local: &mut WorkerLocal, rg: &RgHandle) -> RgOutcome {
        if !crate::sched::step_v2() {
            return self.drive_pinned_v1(local, rg);
        }
        // STEP-V2 loop (agg192-contention, 48xl finding #1): the permit is
        // held ACROSS steps (acquired once, released around parks and at
        // exit) instead of acquire/release per step — same law ("any
        // task-executing thread holds one": Retry/pick windows are a
        // superset of execution, exactly the pool loop's charter) with the
        // per-step global mutex+condvar traffic removed. Retry parks after a
        // bounded spin instead of spinning the whole invalidated-slot window
        // (seal / last-out / straggler tails): publish and completion both
        // wake_all, and the epoch is captured BEFORE the failing step, so
        // the park is lost-wakeup-free.
        let cprobe = crate::sched::cprobe_enabled();
        let mut held = false;
        let mut retries = 0u32;
        let outcome = loop {
            if let Some(outcome) = rg.try_outcome() {
                // Drive over — flush any WFIN/stats accumulation this
                // driver still holds (a worker whose last task did not
                // observe exhaustion emits here).
                self.sched.stat_flush_all(local);
                local.wfin_flush_all();
                break outcome;
            }
            let epoch = self.park_epoch();
            if !held {
                if cprobe {
                    let t0 = std::time::Instant::now();
                    self.execution_permits().acquire();
                    local.drive.permit_wait_ns += t0.elapsed().as_nanos() as u64;
                } else {
                    self.execution_permits().acquire();
                }
                held = true;
            }
            let step = self.sched.worker_step_pinned(local, &rg.rg);
            match step {
                Step::Ran => retries = 0,
                Step::Retry => {
                    local.drive.steps_retry += 1;
                    retries += 1;
                    if retries >= crate::sched::RETRY_PARK_AFTER {
                        retries = 0;
                        self.execution_permits().release();
                        held = false;
                        local.drive.parks += 1;
                        self.park(epoch);
                    } else if cprobe {
                        let t0 = std::time::Instant::now();
                        std::thread::yield_now();
                        local.drive.retry_spin_ns += t0.elapsed().as_nanos() as u64;
                    } else {
                        std::thread::yield_now();
                    }
                }
                Step::Idle => {
                    retries = 0;
                    if rg.try_outcome().is_some() {
                        continue;
                    }
                    self.execution_permits().release();
                    held = false;
                    local.drive.parks += 1;
                    self.park(epoch);
                }
                Step::Stop => unreachable!("pinned steps do not observe stop"),
            }
        };
        if held {
            self.execution_permits().release();
        }
        if cprobe {
            let d = &local.drive;
            eprintln!(
                "MORSEL|CPROBE|worker={}|tasks={}|morsels={}|busy_us={}|retry={}|retry_us={}|permit_us={}|cas_retry={}|lookups={}|parks={}",
                local.worker_id(),
                d.tasks,
                d.morsels,
                d.busy_ns / 1000,
                d.steps_retry,
                d.retry_spin_ns / 1000,
                d.permit_wait_ns / 1000,
                d.cas_retries,
                d.pinned_lookups,
                d.parks,
            );
        }
        outcome
    }

    /// The pre-STEP-V2 pinned drive (kill switch `PGRUST_RUNTIME_STEP_V2=0`):
    /// per-step permit acquire/release, unthrottled Retry yield-spin.
    #[cfg(not(loom))]
    fn drive_pinned_v1(&self, local: &mut WorkerLocal, rg: &RgHandle) -> RgOutcome {
        loop {
            if let Some(outcome) = rg.try_outcome() {
                self.sched.stat_flush_all(local);
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

    /// WS-O inc-2 DEBUG-ONLY accessor (contract-approved "pin-board
    /// assert"): `local`'s pin-board entry is settled. For post-drive
    /// `debug_assert!`s — a participant that returned from its drive must
    /// hold no pin (a violation = a stranded finalization obligation that
    /// would wedge the protocol). Never branch production behavior on it.
    pub fn debug_pin_settled(&self, local: &WorkerLocal) -> bool {
        self.sched.pin_settled(local.worker_id())
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

    /// GL-STMTTASK-2 change 3 (inline-execute, the Cilk work-first / Go
    /// run-on-arrival doctrine): borrow one execution seat WITHOUT waiting
    /// — the session thread is about to execute an admitted statement
    /// ITSELF instead of enqueuing it to the pool. None = no seat free
    /// right now (pool under load) — the caller takes the enqueue+park
    /// path; never blocks, so no lease/seat ordering deadlock exists. The
    /// seat is a REAL execution permit: while held, one fewer pool step
    /// can run — governed accounting, not a bypass.
    pub fn try_borrow_seat(self: &Arc<Self>) -> Option<InlineSeat> {
        if self.sched.permits.try_acquire() {
            Some(InlineSeat { rt: Arc::clone(self) })
        } else {
            None
        }
    }

    /// GL-STMTTASK-2 (wake elision): true ⇔ PGRUST_POOL_WAKE_SPINNER armed
    /// (and the step-v2 loop is live).
    pub fn wake_spinner_armed(&self) -> bool {
        sched::wake_spinner_enabled()
    }

    /// Total actual thread unparks the pool park lot performed (the wakeup
    /// program's unparks-per-statement counter; monotonic, process-wide).
    pub fn pool_unparks(&self) -> u64 {
        self.sched.park.unparks()
    }

    /// GL-STMTTASK-2 (wake elision): mark this pool worker as a SEARCHER
    /// (spin_enter). The pool loop arms it at loop start and re-arms after
    /// each Ran step; worker_step's claim points and the directed park
    /// consume it.
    pub fn spin_enter_worker(&self, local: &mut WorkerLocal) {
        debug_assert!(!local.spinning, "double spin_enter");
        self.sched.park.spin_enter();
        local.spinning = true;
    }

    /// GL-STMTTASK flip fix: drop the searcher mark before a BLOCKING
    /// loop-top permit acquire (a blocked thread is not searching — an
    /// elided wake against it strands work behind the block). Pairs with a
    /// spin_enter_worker re-arm after the acquire.
    pub fn spin_abandon_worker(&self, local: &mut WorkerLocal) {
        if local.spinning {
            local.spinning = false;
            self.sched.park.spin_abandon();
        }
    }

    /// GL-STMTTASK-2 (wake elision): directed park for the step-v2 pool
    /// loop — registers on the LIFO idle stack and consumes the worker's
    /// search-phase mark; on return the worker re-enters the search phase
    /// (mark re-armed here so the elision window never gaps).
    pub fn park_worker_directed(
        &self,
        seen: u64,
        parker: &Arc<pgsync::WorkerParker>,
        local: &mut WorkerLocal,
    ) {
        stats::RuntimeStats::tick(&self.sched.stats.worker_parks);
        let was_spinning = local.spinning;
        local.spinning = false;
        self.sched.park.park_worker(seen, parker, was_spinning);
        self.sched.park.spin_enter();
        local.spinning = true;
    }

    /// POOL-QOS observability: live interactive demand (unmet width of
    /// fresh bound engagements). Tests + diagnostics.
    pub fn qos_demand_live(&self) -> bool {
        self.sched.qos_demand_live()
    }

    /// POOL-QOS: park a yielded participant's external lane until its RG
    /// completes — a rejoining serve must never lease the departed
    /// participant's lane and inherit its mid-accept per-lane slot state
    /// (the sink Local↔executor pairing is per-lane). The RAII lease is
    /// consumed; the scheduler owns the eventual bit clear (swept at RG
    /// completion). NOTE: the forget leaks the lease's Arc<Runtime> strong
    /// count — bounded by the board yield budget and irrelevant to the
    /// process-lifetime production runtime; test runtimes that exercise
    /// yields leak their allocation at exit (accepted, documented).
    pub fn park_external_lane(&self, lane: ExternalLane, rg: &RgHandle) {
        let ordinal = lane.ordinal();
        std::mem::forget(lane);
        self.sched.park_lane_for_rg(&rg.rg, ordinal);
    }

    /// Stream-source producer wake: after `StreamSource::publish`/`close`
    /// (or any stream-fed [`MorselSource`]'s watermark advance), wake parked
    /// workers so starved claims re-check. Epoch-guarded parks make the
    /// publish-then-wake sequence lost-wakeup-free.
    pub fn notify_source_progress(&self) {
        self.sched.park.wake_all();
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

    /// Scheduler clock read, ns — the WFIN markers' shared time domain
    /// (worker morsel timestamps live on this clock; leader-side marks
    /// must too, or spreads would mix clock bases).
    pub fn now_ns(&self) -> u64 {
        self.sched.clock_now_ns()
    }
}

/// GL-STMTTASK-2 quantum yield: is THIS thread inside a declared blocking
/// section (permit donated)? The yield governor's double-release guard;
/// one TLS load.
#[cfg(not(loom))]
pub fn in_blocking_section() -> bool {
    crate::io::in_io_section()
}

fn sched_park(sched: &sched::Scheduler, seen: u64) {
    stats::RuntimeStats::tick(&sched.stats.worker_parks);
    sched.park.park(seen);
}

// ---------------------------------------------------------------------------
// M2 inc-3 rung 3 — SESSION RESIDUE on pool workers (sticky retention).
//
// A pool worker that finished a bound serve may PARK its session binding
// (the binder layer's sticky retention) so a re-engagement from the same
// session resumes in the sticky class instead of paying the full session
// bind. Between engagements a pool worker runs ORDINARY runtime work
// (maintenance cycles, unbound task sets), which must never execute over a
// retained session's identity/GUC view — so the scheduler EVICTS the
// residue at the last gate before unbound work runs (worker_step's unbound
// dispatch). The runtime never interprets the residue: the binder layer
// owns park/resume/evict; this seam is a thread-local hint plus an
// installed evictor (the POOL_GATE fn-pointer precedent — the binder crate
// cannot name runtime types).
//
// The hint is maintained by the serve adapter (arm side) after every bound
// serve; it is thread-local, so session threads / external drives never see
// it, and with no pool-sticky posture armed it is never set — the unbound
// hot path pays one thread-local read + branch.
// ---------------------------------------------------------------------------

thread_local! {
    static SESSION_RESIDUE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The installed residue evictor. Contract: restore the thread to a clean
/// (no retained session state) boundary or DO NOT RETURN (a failed session
/// restore must kill the thread — the glue owns respawn); returning with
/// residue still live would run unbound work over a foreign session view.
static RESIDUE_EVICTOR: crate::sync::OnceLock<fn()> = crate::sync::OnceLock::new();

/// Boot wiring (rtpool glue): install the sticky-residue evictor. Once;
/// later calls ignored.
pub fn install_session_residue_evictor(f: fn()) {
    let _ = RESIDUE_EVICTOR.set(f);
}

/// Serve-adapter side: record whether THIS pool thread parked a session
/// retention when its bound serve returned (true) or holds none (false).
/// Advisory for scheduling (the affinity tiebreak) and the eviction gate;
/// never a correctness input on its own — the binder revalidates its keys.
pub fn note_session_residue(parked: bool) {
    SESSION_RESIDUE.with(|c| c.set(parked));
}

/// True ⇔ this thread currently hints a parked session retention.
pub fn session_residue() -> bool {
    SESSION_RESIDUE.with(|c| c.get())
}

/// The unbound-work gate (called by the scheduler): evict any parked
/// session residue before ordinary (non-bound) task work may run on this
/// thread. No-op unless the hint is set; the evictor either restores a
/// clean boundary or kills the thread (see [`install_session_residue_evictor`]).
pub(crate) fn evict_session_residue_for_unbound_work() {
    SESSION_RESIDUE.with(|c| {
        if c.get() {
            c.set(false);
            if let Some(f) = RESIDUE_EVICTOR.get() {
                f();
            }
        }
    });
}

/// RAII width lease for a non-pool parallel gang (WS-O wave 2, retargeted
/// by PHASE3-CLOSE WS-WIDTH; see [`Runtime::lease_parallel_width`]).
/// Holds one unified pool-face gang entry: the grant is a frozen
/// headroom-only ceiling ([`ParallelWidthLease::granted`] may be 0 — the
/// caller must have a serial path); [`ParallelWidthLease::settle`] tracks
/// the ACTIVE width within it; drop retires the entry and returns the
/// width to the pool.
pub struct ParallelWidthLease {
    rt: Arc<Runtime>,
    id: usize,
    granted: u32,
}

impl ParallelWidthLease {
    /// The frozen grant ceiling (workers; the leader is not counted).
    /// **May be 0 — the caller MUST have a serial path.**
    pub fn granted(&self) -> u32 {
        self.granted
    }

    /// Engine-of-record for the §2.4 width mirror (post-W4 there is one
    /// leased engine; "fail-open" is the no-lease arm, printed by the
    /// mirror itself).
    pub fn engine(&self) -> &'static str {
        "unified"
    }

    /// Record the gang's ACTIVE width (launched/live workers ≤ granted;
    /// parked workers hold no grant). Callable repeatedly — launch,
    /// rescan relaunch, partial exit.
    pub fn settle(&mut self, active: u32) {
        debug_assert!(
            active <= self.granted,
            "settling above the frozen grant ({active} > {})",
            self.granted
        );
        self.rt.sched.settle_gang_width(self.id, active.min(self.granted));
    }
}

impl Drop for ParallelWidthLease {
    fn drop(&mut self) {
        self.rt.sched.retire_gang_width(self.id);
    }
}

/// RAII lease of one external pin-board lane (see
/// [`Runtime::acquire_external_lane`]).
/// GL-STMTTASK-2 change 3: an inline-execute seat — one execution permit
/// borrowed by a SESSION thread for the span of a statement it runs itself
/// (see [`Runtime::try_borrow_seat`]). RAII: the permit returns on drop,
/// on every exit path (completion, error, cancel unwind).
pub struct InlineSeat {
    rt: Arc<Runtime>,
}

impl Drop for InlineSeat {
    fn drop(&mut self) {
        self.rt.sched.permits.release();
    }
}

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
        self.rt.sched.external_lanes[self.ordinal / 64]
            .fetch_and(!(1u64 << (self.ordinal % 64)), crate::sync::atomic::Ordering::SeqCst);
    }
}
