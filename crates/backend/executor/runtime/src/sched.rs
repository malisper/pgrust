//! The scheduler core: 128-slot global array, morsel claiming, and the
//! LAST-WORKER-OUT FINALIZATION PROTOCOL (SIGMOD'21 §2.4 via
//! notes/morsel-lit-review.md §1.3; correctness-critical — modeled in
//! tests/loom.rs before any perf work, per redesign doc §2.1).
//!
//! # The finalization protocol, exactly
//!
//! Finishing a pipeline must (a) run its finalization AT MOST ONCE, and
//! (b) only after ALL workers finished their in-flight tasks — an empty
//! morsel queue does NOT mean the pipeline is done (literature mistake #2).
//!
//! 1. PUBLISH-TARGET-BEFORE-CLAIM: a worker stores the slot index it is
//!    about to work in the pin board BEFORE reading the slot word.
//! 2. EXHAUSTED → INVALIDATE: the first worker to find the task set's
//!    cursor exhausted (or its generation dead) CASes the slot word from
//!    valid to invalid; the CAS winner is the unique coordinator.
//! 3. MARK: the coordinator scans the pin board and swaps every entry still
//!    pinned to the dying slot (its own included) for a finalization
//!    marker, then adds the number of marked workers to the task set's
//!    finalization counter.
//! 4. SETTLE: every worker clears its pin after finishing its in-flight task;
//!    if it finds a marker it decrements the counter. The counter may go
//!    TRANSIENTLY NEGATIVE (marked workers can decrement before the
//!    coordinator's add lands).
//! 5. LAST OUT: whoever moves the counter to zero is provably the last
//!    worker out; it runs finalize, then activates the RG's next task set
//!    in the SAME slot (or completes the RG and admits the next queued RG).
//!
//! Safety note (why `settle`'s ownership lookup cannot dangle): the slot's
//! ownership entry is only replaced/cleared by LAST OUT, which requires the
//! counter to reach zero, which requires every marked worker's decrement —
//! so a marked worker always finds its task set still owned at settle time.
//! Conversely a worker whose pin was marked can never observe the slot's
//! NEXT occupant as valid: the next occupant is published by last-out,
//! which its own pending decrement blocks.
//!
//! Scheduling policy (M5-4, docs/design/inter-query-scheduling.md §5.3 /
//! docs/design/m5-planner.md §3): STRIDE / FAIR-SHARE at equal shares. The
//! pick is the lowest-`pass` active slot; each executed task advances its
//! slot's pass by `stride × cpu_ns` (stride = STRIDE1/priority; every RG
//! holds p_0 in M5-4, so shares are equal — decaying priorities + p_min are
//! M5-5). Equal-pass ties prefer the slot whose RG's leader session the
//! picking worker is sticky-bound to (§5.2 session-affine tiebreak,
//! equal-pass-only per the design's §10 default), then lowest index. The
//! M4 Maintenance preference is evaluated BEFORE the stride pick (§3.2
//! reconciliation) and maintenance passes charge normally, so background
//! cycles keep their ≤ ~1-task start bound without ever starving foreground.
//!
//! Invariants the activation preserves:
//! - ONE ACTIVE RG ⇒ the pick is forced (the only set bit) with no pass
//!   reads — provably today's FIFO pick; the single-query benchmark case is
//!   bit-identical by construction.
//! - `PGRUST_RUNTIME_STRIDE=0` (kill switch) restores the M0 FIFO
//!   lowest-index pick outright.
//! - Pass/stride/session words are Relaxed and ADVISORY: they order the
//!   pick, never execution safety — a stale pick revalidates through the
//!   slot word into Retry (same argument as the maintenance mask).
//!
//! PIPELINE-DAG DISPATCH (M5+1 increment 1, m5-planner §3.6 — independent-
//! subtree overlap ONLY; `PGRUST_RUNTIME_PIPELINE_DAG=1`, default OFF):
//! instead of walking an RG's task sets one slot at a time, publish EVERY
//! task set whose dependencies have all finalized, each in its own slot —
//! independent hash-build sides / subqueries / UNION ALL branches overlap. A
//! task set with unmet deps is NOT SUBMITTED: it occupies no slot, queues
//! nowhere, and no worker can wait on it — which is the increment's
//! structural deadlock argument (the wait graph over RUNNING entities has no
//! dependency edges at all; workers never hold-and-wait across task sets —
//! one claim, one morsel, permits donated on declared blocking). A
//! finishing pipeline's finalize marks its dep edge satisfied and publishes
//! the newly-ready pipelines: the deepest into its own (retained) slot,
//! the rest into free slots — capacity shortfalls defer (counted), and the
//! deferred pipeline re-publishes when one of the query's own pipelines
//! finishes, so the RG always holds ≥1 slot while work remains (progress
//! guaranteed without any cross-RG wait). Fairness: the QUERY is the
//! fair-share principal — all of an RG's slots advance ONE stride account
//! ([`ResourceGroup::pass_account`], the sum of their quanta) mirrored into
//! each slot's pass word (one-task-bounded staleness, advisory); within a
//! query at equal pass the pick prefers the DEEPER pipeline (§3.6
//! dependency-depth priority, ties by submission order). OFF (the kill
//! switch, this increment's default) is today's sequential walk,
//! byte-identical; ON with a single-pipeline RG degenerates to the same
//! pick and pass sequence by construction (the single-wide-pipeline flatness anchor).
//! Streaming across pipeline seams is inc-3 (evidence-gated, NOT here);
//! tail/ramp readahead overlap is inc-2 (also not here).

use std::collections::VecDeque;
use std::sync::Arc;

use crate::clock::Clock;
use crate::ledger::{AdmissionLedger, ClaimVerdict, LedgerBudgets, LedgerClass, WidthRequest};
use crate::morsel::MorselRange;
use crate::rg::{BoundDescriptor, BoundServe, QuerySpec, ResourceGroup, RgClass, RgOutcome};
use crate::sizing::{SizingDecision, SizingParams, TaskSizer};
use crate::stats::{RuntimeStats, RuntimeStatsSnapshot};
use crate::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use crate::sync::{lock, Mutex, ParkLot, Semaphore};
use crate::taskset::{PinBoard, Slot, TaskSetRt, WorkerMailbox};

/// Umbra's slot-array bound: 128 concurrently-active resource groups; later
/// arrivals wait in the FIFO queue.
pub const DEFAULT_SLOTS: usize = 128;

/// Stride fixed point (M5-4): `stride = STRIDE1 / priority`, and a task's
/// pass advance is `(cpu_ns × stride) >> PASS_SHIFT` — i.e. advance is
/// cpu_ns scaled by `(1 << (32 - 16)) / priority`. At p_0 = 10^4 every RG
/// advances ≈ 6.55 × cpu_ns: equal strides ⇒ equal shares, and u64 passes
/// have decades of headroom. M5-5's decayed priorities (p ≥ p_min) only
/// change the `stride_for` input.
const STRIDE1: u64 = 1 << 32;
const PASS_SHIFT: u32 = 16;

pub(crate) fn stride_for(priority: u32) -> u64 {
    STRIDE1 / priority.max(1) as u64
}

thread_local! {
    /// (scheduler, slot) while THIS thread is inside a ledger-JOINED
    /// `run_task` (set by `run_task_admitted`, cleared by [`GrantCtx`]'s
    /// drop — unwind-safe). Consulted by the declared-blocking-section
    /// entry points (io.rs permit seams, blocking.rs facade) so the width
    /// grant is donated and retaken ALONGSIDE the execution permit; empty
    /// on non-worker threads and under knob OFF, where both entry points
    /// no-op. Plain per-thread state (same pattern as blocking.rs's
    /// PERMIT_SEM), sound under loom.
    static LEDGER_GRANT: std::cell::Cell<Option<(*const Scheduler, usize)>> =
        const { std::cell::Cell::new(None) };
    /// WS-O C1 (wave 2): the caller-as-worker CLAIM-BOUNDARY duty hook —
    /// a LIGHT duty pumped inside run_task's claim loop while the SESSION
    /// thread drives its own RG (installed for the drive's extent by
    /// [`crate::CallerWorker::drive_with_duties`], RAII-cleared by
    /// [`CallerDutyCtx`]). Returning false ends the current task at the
    /// boundary (the TaskEnd::Budget path — the ledger-Yield shape) and
    /// control returns to the caller's STEP loop, where the full
    /// error-carrying duty runs. The light hook is where the gang
    /// all-stopped detection lives (contract adjudication: duty cadence
    /// alone is insufficient for C2 liveness — claim cadence bounds
    /// detection latency to one claim, not one task). Empty on pool
    /// workers and non-caller externals: one TL read per claim boundary.
    /// Same thread_local! block as LEDGER_GRANT (no TLS-census delta).
    static CALLER_DUTY: std::cell::Cell<Option<*mut dyn FnMut() -> bool>> =
        const { std::cell::Cell::new(None) };
}

/// RAII for CALLER_DUTY: cleared even if the drive frame unwinds (the
/// GrantCtx pattern).
pub(crate) struct CallerDutyCtx;

impl CallerDutyCtx {
    /// SAFETY contract (upheld by caller.rs): `duty` must outlive the
    /// returned guard, the guard must not escape the caller's drive frame,
    /// and the pointer is dereferenced only from THIS thread inside
    /// run_task's claim loop while the guard lives.
    pub(crate) fn set(duty: *mut dyn FnMut() -> bool) -> CallerDutyCtx {
        CALLER_DUTY.with(|c| c.set(Some(duty)));
        CallerDutyCtx
    }
}

impl Drop for CallerDutyCtx {
    fn drop(&mut self) {
        CALLER_DUTY.with(|c| c.set(None));
    }
}

/// RAII for LEDGER_GRANT: cleared even if the task body unwinds.
struct GrantCtx;

impl GrantCtx {
    fn set(sched: &Scheduler, slot: usize) -> GrantCtx {
        LEDGER_GRANT.with(|c| c.set(Some((sched as *const Scheduler, slot))));
        GrantCtx
    }
}

impl Drop for GrantCtx {
    fn drop(&mut self) {
        LEDGER_GRANT.with(|c| c.set(None));
    }
}

/// Blocking-section entry (§2.8 composition): donate the current task's
/// width grant along with the execution permit — the standby absorbing the
/// freed core must be joinable or a width-saturated slot deadlocks the
/// donation (ledger.rs `donate` doc). Called by io.rs `io_permit_release`
/// and blocking.rs `blocking_io_section` BEFORE the permit release; no-op
/// unless this thread is inside a ledger-joined task.
pub(crate) fn ledger_donate_current() {
    if let Some((sched, slot)) = LEDGER_GRANT.with(std::cell::Cell::get) {
        // SAFETY: set only for the extent of run_task_admitted on this
        // thread; the scheduler (owned by the Runtime the worker loop
        // holds) outlives every task run.
        let sched = unsafe { &*sched };
        if sched.ledger.donate(slot) > 0 {
            sched.park.wake_all();
        }
    }
}

/// Blocking-section exit: retake the grant (permit already reacquired by
/// the caller — grant follows permit, in both directions). Transient
/// overshoot over target resolves via Yield at the next claim boundary.
pub(crate) fn ledger_restore_current() {
    if let Some((sched, slot)) = LEDGER_GRANT.with(std::cell::Cell::get) {
        // SAFETY: as in ledger_donate_current.
        let sched = unsafe { &*sched };
        sched.ledger.rejoin(slot);
    }
}

/// M5-4 kill switch: `PGRUST_RUNTIME_STRIDE=0` restores the M0 FIFO
/// lowest-index pick (byte-identical to the pre-M5-4 scheduler). Default ON.
/// Read once; tests toggle per-instance via [`crate::Runtime::set_stride`].
fn stride_default() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_STRIDE").map_or(true, |v| v.trim() != "0"))
}

/// M5+1 pipeline-DAG dispatch switch: `PGRUST_RUNTIME_PIPELINE_DAG=1` (or
/// `on`) publishes every dependency-satisfied task set concurrently (see
/// module doc). DEFAULT OFF at this increment — off is today's sequential
/// walk, byte-identical. Read once; tests toggle per-instance via
/// [`crate::Runtime::set_dag`].
fn dag_default() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_PIPELINE_DAG").as_deref(), Ok("1") | Ok("on"))
    })
}

/// M5-5 decaying priorities (inter-query §5.4, MLFQ-via-stride): an RG's
/// priority decays with its CONSUMED CPU — `p(q) = max(p_min, p0·λ^q)`
/// where `q = cpu_consumed_ns / decay_quantum_ns`. New/short queries keep
/// p0 (interactive latency); long queries settle to the p_min fair-share
/// floor. Constants are ratified in-code (inter-query ruling 2: decay is
/// automatic, no user input); the env overrides below are TEST/CALIBRATION
/// knobs on the PGRUST_RUNTIME_STRIDE precedent, not product surface.
///
/// Ratified values: λ = 0.5 per 50ms-CPU quantum (one halving per 50ms of
/// consumed CPU — a sub-50ms interactive query never decays at all);
/// p_min = p0/16 = 625, i.e. a fully-decayed batch query holds ≥ 1/16 the
/// stride weight of a fresh arrival (reached after 4 quanta = 200ms CPU).
/// p_min is LOAD-BEARING for C-legality (inter-query §3.4): it bounds
/// lock-holder starvation to C's own nondeterministic window; the
/// lock-wait-fairness test validates exactly this floor.
const DECAY_LAMBDA_DEFAULT: f64 = 0.5;
const DECAY_QUANTUM_NS_DEFAULT: u64 = 50_000_000;
pub(crate) const P_MIN_DEFAULT: u32 = crate::rg::INITIAL_PRIORITY / 16;

/// POOL-QOS kill switch (GL-POOLDB-1 mitigation): `PGRUST_RUNTIME_POOL_QOS=0`
/// restores the pre-QoS pool exactly (no interactive demand, no serve
/// yields, no priority permits). Default ON — but the whole mechanism only
/// runs inside POOL serves' drives (the arms call the yieldable drive only
/// when serving on a pool thread), so the effective exposure rides the
/// PGRUST_RUNTIME_POOLDB layering.
pub(crate) fn pool_qos_enabled() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_POOL_QOS").map_or(true, |v| v.trim() != "0")
    })
}

// ---------------------------------------------------------------------------
// POOL-QOS memory governor (GL-CONCMEM-1): the interleave the QoS tier buys
// (demoted serves yielding threads / deferring permits toward interactive
// arrivals) multiplies CONCURRENTLY-LIVE engagement working sets — N clients'
// engagements progress round-robin with all their partial states resident,
// where a run-to-completion serve holds few big states at a time. At a
// bounded-memory posture that envelope is a container kill (the concurrent-
// window OOM class). The governor holds the QoS moves while the process is
// over a memory bar: demoted serves run to completion (the pre-QoS shape —
// also the fastest way to shed memory), and the interleave resumes as soon
// as the process drops back under the bar. Advisory scheduling only —
// results are unaffected; interactive first-permit latency degrades toward
// the pre-QoS posture exactly and only while memory is scarce.
//
// Basis: anonymous RSS from /proc/self/status (the OOM-class growth; shared
// memory excluded) when available, else the installed accounted-bytes probe
// (mcx global block bytes — boot glue installs it; this crate stays
// mcx-free). Bar: `PGRUST_RUNTIME_QOS_MEM_BAR_KB` (>0 arms at that many KB;
// `0`/`off` DISARMS the governor — the t35 kill spelling), else the
// BOOT-DERIVED MEMORY MODEL bar (the bar-derivation law, GL-MEMCEIL-1
// amendment): memory.max − page_cache_reserve − kernel_slack, on this
// governor's ANON basis; unbounded hosts stay disarmed (never pay a
// read). The model terms mirror memwatchdog::derived_memory_bar (the dep
// direction keeps this crate postmaster-free — keep the constants in
// lockstep; derivation + death-ledger table live there). The verdict is
// cached and refreshed at most every GOVERNOR_REFRESH_MS — the yield gate
// costs one Relaxed load between refreshes.
// ---------------------------------------------------------------------------

const GOVERNOR_REFRESH_MS: u64 = 64;
/// Model term (lockstep with memwatchdog::MEM_MODEL_PAGE_CACHE_RESERVE_PCT):
/// the reclaim-resistant file-backed share of memory.max at the kill
/// profile of record.
const MEM_MODEL_PAGE_CACHE_RESERVE_PCT: u64 = 31;
/// Model term (lockstep with memwatchdog::MEM_MODEL_KERNEL_SLACK_PCT):
/// kernel slab/sock overhead + the shed/error defense margin.
const MEM_MODEL_KERNEL_SLACK_PCT: u64 = 4;

#[cfg(not(loom))]
mod qos_governor {
    use super::{
        GOVERNOR_REFRESH_MS, MEM_MODEL_KERNEL_SLACK_PCT, MEM_MODEL_PAGE_CACHE_RESERVE_PCT,
    };
    use core::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

    /// Accounted-bytes fallback probe (installed by the pool boot glue).
    static PROBE: crate::sync::OnceLock<fn() -> usize> = crate::sync::OnceLock::new();
    /// Resolved bar in bytes; 0 = disarmed.
    static BAR: crate::sync::OnceLock<u64> = crate::sync::OnceLock::new();
    /// Cached verdict + last refresh (ms since the epoch Instant).
    static OVER: AtomicBool = AtomicBool::new(false);
    static LAST_MS: AtomicU64 = AtomicU64::new(0);

    fn epoch() -> &'static std::time::Instant {
        static T0: crate::sync::OnceLock<std::time::Instant> = crate::sync::OnceLock::new();
        T0.get_or_init(std::time::Instant::now)
    }

    /// `PGRUST_RUNTIME_QOS_MEM_BAR_KB` spelling (unit-pinned): `0`/`off`
    /// disarm; a positive integer arms at that many KB; unset/other = auto.
    pub(crate) fn bar_from_env(v: Option<&str>) -> Option<u64> {
        match v.map(str::trim) {
            Some("0") | Some("off") => Some(0),
            Some(s) => s.parse::<u64>().ok().filter(|&n| n > 0).map(|n| n * 1024),
            None => None,
        }
    }

    // The cgroup v2 memory limit for this process, if bounded (the
    // memwatchdog parse, self-contained: dep direction forbids importing
    // the postmaster crate here).
    fn cgroup_mem_max() -> Option<u64> {
        let s = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let rel = s.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
        let dir = std::path::Path::new("/sys/fs/cgroup").join(rel.trim_start_matches('/'));
        let read = |d: &std::path::Path| -> Option<u64> {
            let s = std::fs::read_to_string(d.join("memory.max")).ok()?;
            s.trim().parse().ok() // "max" (unbounded) parses Err -> None
        };
        read(&dir).or_else(|| read(std::path::Path::new("/sys/fs/cgroup")))
    }

    /// The boot-derived model bar (bar-derivation law):
    /// memory.max − page_cache_reserve − kernel_slack.
    fn model_bar(memory_max: u64) -> u64 {
        memory_max
            .saturating_sub(memory_max / 100 * MEM_MODEL_PAGE_CACHE_RESERVE_PCT)
            .saturating_sub(memory_max / 100 * MEM_MODEL_KERNEL_SLACK_PCT)
    }

    fn resolve_bar() -> u64 {
        let bar = match bar_from_env(
            std::env::var("PGRUST_RUNTIME_QOS_MEM_BAR_KB").ok().as_deref(),
        ) {
            Some(b) => b,
            None => cgroup_mem_max().map_or(0, model_bar),
        };
        if bar > 0 {
            // Posture witness (harvested-log law): the armed bar, once.
            eprintln!(
                "LOG:  pool-qos memory governor bar {} kB (PGRUST_RUNTIME_QOS_MEM_BAR_KB; model: memory.max - page_cache_reserve {MEM_MODEL_PAGE_CACHE_RESERVE_PCT}% - kernel_slack {MEM_MODEL_KERNEL_SLACK_PCT}%)",
                bar / 1024
            );
        }
        bar
    }

    /// Anonymous RSS (kB) from /proc/self/status; None off-Linux.
    fn proc_anon_kb() -> Option<u64> {
        let s = std::fs::read_to_string("/proc/self/status").ok()?;
        s.lines()
            .find_map(|l| l.strip_prefix("RssAnon:"))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
    }

    pub(crate) fn install(probe: fn() -> usize) {
        let _ = PROBE.set(probe);
        BAR.get_or_init(resolve_bar);
    }

    /// One Relaxed load between refreshes; a refresh is one /proc read (or
    /// one probe call off-Linux). Disarmed = one OnceLock read, always false.
    pub(crate) fn over_bar() -> bool {
        let bar = *BAR.get_or_init(resolve_bar);
        if bar == 0 {
            return false;
        }
        let now_ms = epoch().elapsed().as_millis() as u64;
        let last = LAST_MS.load(Relaxed);
        if now_ms.wrapping_sub(last) >= GOVERNOR_REFRESH_MS
            && LAST_MS.compare_exchange(last, now_ms, Relaxed, Relaxed).is_ok()
        {
            let usage = match proc_anon_kb() {
                Some(kb) => kb * 1024,
                None => PROBE.get().map_or(0, |p| p() as u64),
            };
            OVER.store(usage > bar, Relaxed);
        }
        OVER.load(Relaxed)
    }

}

/// Test face for the bar spelling (the tests module lives outside sched).
#[cfg(all(test, not(loom)))]
pub(crate) fn qos_governor_bar_from_env(v: Option<&str>) -> Option<u64> {
    qos_governor::bar_from_env(v)
}

/// Boot-glue install of the governor's accounted-bytes fallback probe (the
/// anon-RSS basis needs no install; off-Linux the probe is the basis).
/// Idempotent; also resolves the bar once so the first yield gate pays no
/// env/cgroup read.
#[cfg(not(loom))]
pub fn install_qos_mem_probe(probe: fn() -> usize) {
    qos_governor::install(probe);
}

/// The governor verdict consumed by the pool serve drive's QoS moves: hold
/// the interleave (yield/defer) while over the bar. Loom builds have no
/// governor (the yieldable drive is cfg(not(loom)) too).
#[cfg(not(loom))]
pub(crate) fn qos_mem_over_bar() -> bool {
    qos_governor::over_bar()
}

/// POOL-QOS interactive threshold: an RG whose CURRENT priority is at or
/// above this is INTERACTIVE-class (fresh / undecayed — the ratified M5-5
/// decay constants make this "consumed less than one 50ms-CPU quantum").
/// Default p0/2 = the first decay halving. Calibration knob on the
/// DECAY_LAMBDA precedent, not product surface.
pub(crate) fn qos_interactive_p() -> u32 {
    static V: crate::sync::OnceLock<u32> = crate::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_QOS_INTERACTIVE_P")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|p| *p > 0)
            .unwrap_or(crate::rg::INITIAL_PRIORITY / 2)
    })
}

/// M5-5 kill switch: `PGRUST_RUNTIME_DECAY=0` pins every RG at p0 (the
/// M5-4 equal-shares scheduler exactly). Default ON. Read once; tests
/// toggle per-instance via [`crate::Runtime::set_decay`].
fn decay_default() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_DECAY").map_or(true, |v| v.trim() != "0"))
}

fn decay_lambda_default() -> f64 {
    static V: crate::sync::OnceLock<f64> = crate::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DECAY_LAMBDA")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|l| *l > 0.0 && *l < 1.0)
            .unwrap_or(DECAY_LAMBDA_DEFAULT)
    })
}

fn decay_quantum_default() -> u64 {
    static V: crate::sync::OnceLock<u64> = crate::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DECAY_QUANTUM_US")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|q| *q > 0)
            .map(|us| us.saturating_mul(1000))
            .unwrap_or(DECAY_QUANTUM_NS_DEFAULT)
    })
}

/// WS-B admission-ledger switch (single-executor Phase 0.1):
/// `PGRUST_RUNTIME_LEDGER_V2=1` (or `on`) activates the ledger width policy.
/// DEFAULT OFF — off is today's scheduler, byte-identical (every touch point
/// is one cached-bool branch, zero new atomics on the hot paths). Read once;
/// tests toggle per instance via [`crate::Runtime::set_ledger`]. (The
/// stride/dag/decay switch pattern above.)
fn ledger_default() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_LEDGER_V2").as_deref(), Ok("1") | Ok("on"))
    })
}

fn p_min_default() -> u32 {
    static V: crate::sync::OnceLock<u32> = crate::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_PMIN")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|p| *p > 0 && *p <= crate::rg::INITIAL_PRIORITY)
            .unwrap_or(P_MIN_DEFAULT)
    })
}

/// Track-4 Q0: the UTILITY class stride weight p_util (pool-qos-design.md
/// §1.2). Default = the ratified p_min floor (p0/16 = 625): utility work
/// holds exactly the fully-decayed-batch share — the §3.4 C-legality bound
/// already argued for the floor — and, being at the floor, never decays
/// (the decay site's `priority > p_min` guard). Env override is a
/// calibration knob on the PGRUST_RUNTIME_PMIN precedent, not product
/// surface.
pub(crate) const P_UTIL_DEFAULT: u32 = P_MIN_DEFAULT;

fn p_util_default() -> u32 {
    static V: crate::sync::OnceLock<u32> = crate::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_UTIL_PRIORITY")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|p| *p > 0 && *p <= crate::rg::INITIAL_PRIORITY)
            .unwrap_or(P_UTIL_DEFAULT)
    })
}

/// Track-4 Q0/Q1 kill switch: `PGRUST_RUNTIME_UTIL_QOS=0` makes a Utility
/// submission behave as a plain Foreground one (p0 stride weight, standard
/// ledger tier) — the pre-QoS scheduler byte-identically. Default ON;
/// INERT either way until a consumer submits a Utility RG (none on this
/// train — M4.1 is the first, gated on M2 inc-2). Read once; tests toggle
/// per instance via [`crate::Runtime::set_util_qos`].
fn util_qos_default() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_UTIL_QOS").map_or(true, |v| v.trim() != "0"))
}

/// Pin-board lanes reserved for EXTERNAL participant threads (M1: the
/// query's bound parallel helpers driving `Runtime::drive_pinned`). External
/// lanes live above the pool's `nthreads` indexes; the finalization
/// protocol's coordinator scans the whole board, so external participants
/// carry marker obligations exactly like pool workers.
///
/// 256 (DOP-192 readiness): every gang helper leases one lane for the whole
/// drive, so this constant IS the per-process pinned-participation ceiling —
/// at the old 64, a dop-192 engagement silently refused 128+ helpers
/// (fail-closed non-participation). 256 = 192 helpers + vacuum drivers +
/// concurrent-query headroom; the lease mask is `EXTERNAL_LANE_WORDS` words.
pub const MAX_EXTERNAL_LANES: usize = 256;

/// Words of the external-lane lease bitmask (64 lanes per `AtomicU64`).
pub(crate) const EXTERNAL_LANE_WORDS: usize = MAX_EXTERNAL_LANES / 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Executed (part of) a task.
    Ran,
    /// Published against a slot that turned invalid; try again.
    Retry,
    /// No active slot; caller should park (capture the park epoch BEFORE
    /// calling worker_step, then park on it).
    Idle,
    /// Stop requested; worker loop should exit.
    Stop,
}

struct SlotEntry {
    seq: u64,
    ts: Arc<TaskSetRt>,
}

/// One claim attempt's outcome (stream sources add STARVED to the classic
/// claimed/exhausted pair).
enum Claim {
    Range(MorselRange),
    /// Stream source, cursor at an OPEN watermark: nothing claimable NOW but
    /// the set is not exhausted — the worker parks and the producer's
    /// publish/close wakes it.
    Starved,
    Exhausted,
}

/// How a task ended (run_task).
enum TaskEnd {
    /// The set's cursor is exhausted (or its generation dead): drive
    /// invalidation/finalization.
    Exhausted,
    /// Duration budget spent; more work remains.
    Budget,
    /// Stream starvation: no claimable granule and the stream is open. The
    /// worker should park (epoch-guarded — a publish during the task wakes
    /// the park immediately).
    Starved,
}

/// Membership state: slot ownership + the RG wait queue. One mutex, touched
/// only on membership events (publish/finalize/admit) and on worker cache
/// misses — never on the per-task hot path (slot-word seq revalidation hits
/// the thread-local cache).
struct Membership {
    owned: Vec<Option<SlotEntry>>,
    /// Queued RGs with the width request they were submitted with (consumed
    /// by the ledger admit when a slot frees; None = unbounded).
    waitq: VecDeque<(Arc<ResourceGroup>, Option<WidthRequest>)>,
}

/// Per-drive observability accumulators (the WFIN marker channel —
/// fabled run-m0-parallel-accept.sh parses `MORSEL|WFIN|…` off server
/// stderr). Plain thread-owned data written at morsel/task cadence and
/// read by the drive's owner after completion: no synchronization, no
/// loom-visible operations. Timestamps are the scheduler clock's ns.
#[derive(Default, Clone, Copy)]
pub struct DriveLocal {
    /// Tasks this local executed (claim loops entered with a live join).
    pub tasks: u64,
    pub morsels: u64,
    pub granules: u64,
    /// Sum of executed-morsel durations (excludes claim/park/settle time).
    pub busy_ns: u64,
    /// Clock at the first claimed morsel's execution start; 0 = none ran.
    pub first_claim_ns: u64,
    /// Clock at the end of the last executed morsel (the WFIN t_us).
    pub last_end_ns: u64,
    // ---- CPROBE accumulators (agg192-contention): plain thread-owned
    // counters, always accumulated (a handful of u64 adds on owned memory);
    // EMITTED only under PGRUST_RUNTIME_CPROBE=1 (see Runtime::drive_pinned).
    /// Steps that returned [`Step::Retry`] (invalidated-slot windows).
    pub steps_retry: u64,
    /// Wall ns spent in the retry re-step loop (armed probe only; 0 when
    /// the probe is off — the clock is not read).
    pub retry_spin_ns: u64,
    /// Claim-cursor CAS failures (contended `TaskSetRt::cursor`).
    pub cas_retries: u64,
    /// Pinned-drive slow-path membership lookups (stale slot-word cache —
    /// one global mutex acquisition each).
    pub pinned_lookups: u64,
    /// Wall ns waiting in execution-permit acquire (armed probe only).
    pub permit_wait_ns: u64,
    /// Parks taken by this driver.
    pub parks: u64,
}

/// Thread-local scheduling bookkeeping (one per worker, owned by the worker
/// loop — deliberately NOT thread_local! so loom can drive it).
pub struct WorkerLocal {
    worker: usize,
    /// WFIN drive accumulators (fresh per `Runtime::external_local`; pool
    /// workers accumulate for their thread's lifetime — only the external
    /// pinned drives read them today).
    pub drive: DriveLocal,
    /// Slot-word cache: (seq, task set) per slot; revalidated by one atomic
    /// read of the slot word.
    cache: Vec<Option<(u64, Arc<TaskSetRt>)>>,
    /// Per-slot WFIN accumulation (marker contract; see [`markers_enabled`]).
    /// Untouched (empty vec) when markers are off.
    wfin: Vec<Option<WfinStat>>,
    /// Per-slot batched stats accumulation (see [`StatAcc`]). Always on.
    stat: Vec<Option<StatAcc>>,
    /// Pinned-drive fast path: the last (slot, seq) this local drove for its
    /// pinned RG. Revalidated by one slot-word read per step; the membership
    /// lock is touched only when it goes stale (publish/finalize events).
    pinned_slot: Option<(usize, u64)>,
    /// M2 inc-2 bound-engagement skip cache: per slot, the exact slot WORD
    /// this worker will not serve again (its serve returned Refused/Closed
    /// for that publication). The pick passes over a slot whose current
    /// word still matches — a republish/invalidation (word change)
    /// self-clears the entry. Purely a scheduling preference: a stale skip
    /// only delays this worker's next serve attempt, never execution safety.
    bound_skip: Vec<Option<u64>>,
    /// STILL INERT after M5-4 (decision recorded): the ratified §5.3 shape
    /// is per-SLOT pass/stride ("each slot gets a stride/pass"), which M5-4
    /// activated on [`crate::taskset::Slot`] directly — at task cadence
    /// (~t_max) one Relaxed fetch_add per task is uncontended, so the
    /// SIGMOD'21 §2.3 thread-local pass replication (these fields + the
    /// worker mailboxes) stays dormant until a measured sync cost demands it.
    #[allow(dead_code)]
    local_pass: u64,
    #[allow(dead_code)]
    global_pass: u64,
    /// Session-affinity token (M5-4): the leader session this worker is
    /// currently sticky-bound to (ceremony-v2 retention), 0 = none. Feeds
    /// the equal-pass pick tiebreak; set by the driving thread via
    /// [`WorkerLocal::set_session_token`].
    session_token: u64,
    /// GL-STMTTASK-2 (wake elision): true while this pool worker carries a
    /// live spin_enter mark on the park lot — woken (or fresh) and
    /// searching for work. Cleared at the claim points inside
    /// [`Scheduler::worker_step`] (a long task body must never count as a
    /// spinner) and consumed by [`ParkLot::park_worker`]. Only the step-v2
    /// pool loop under `wake_spinner_enabled` ever sets it.
    pub(crate) spinning: bool,
}

impl WorkerLocal {
    /// Record the leader-session token this worker's thread is sticky-bound
    /// to (0 = none). Purely a pick-tiebreak preference — never a
    /// correctness input (a mismatched pick still executes correctly; the
    /// session bind machinery revalidates its own keys).
    pub fn set_session_token(&mut self, token: u64) {
        self.session_token = token;
    }

    /// GL-STMTTASK-2: live search-phase mark (see the field doc).
    pub fn spinning(&self) -> bool {
        self.spinning
    }

    /// Pin-board lane / worker index (CPROBE line identity).
    pub(crate) fn worker_id(&self) -> usize {
        self.worker
    }
}

/// `PGRUST_MORSEL_MARKERS=1` arms the acceptance-instrument marker channel:
/// one server-stderr line per (worker, task set) participation —
/// `MORSEL|WFIN|qid=|pipe=|worker=|t_us=|tasks=|task_avg_us=` — parsed by the
/// M0 instruments' worker-finish-spread verdict (fabled
/// m0-acceptance-instruments; ≤1-task-duration acceptance). Default OFF:
/// zero cost beyond one branch per task.
fn markers_enabled() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_MORSEL_MARKERS").as_deref(), Ok("1") | Ok("on"))
    })
}

/// agg192-contention step-loop fix kill switch: `PGRUST_RUNTIME_STEP_V2=0`
/// restores the per-step permit acquire/release + unthrottled Retry spin
/// (the pre-fix loops, byte-identical). Default ON. Read once.
pub(crate) fn step_v2() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_STEP_V2").map_or(true, |v| v.trim() != "0"))
}

/// GL-STMTTASK-2 change-4 arming switch: `PGRUST_POOL_WAKE_SPINNER`, armed
/// iff exactly `1`/`on` (t35 exact-spelling law; DEFAULT OFF — the
/// measured-increment posture). ON ⇒ the new-work submission wake becomes
/// spinner-elided + LIFO-directed ([`pgsync::ParkLot::wake_work`]) and the
/// step-v2 pool loop parks on the directed stack with search-phase
/// accounting. OFF ⇒ `wake_all` + plain parks, byte-identical to the
/// pre-lane world. Requires the step-v2 loop (the legacy loop never marks
/// spinners; with STEP_V2=0 this switch is inert by construction).
pub(crate) fn wake_spinner_enabled() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_POOL_WAKE_SPINNER").ok().as_deref().map(str::trim),
            Some("1") | Some("on")
        ) && step_v2()
    })
}

/// `PGRUST_RUNTIME_CPROBE=1` arms the contention-probe marker channel: one
/// `MORSEL|CPROBE|…` line per pinned driver per drive (parsed off server
/// stderr like WFIN). Default OFF: counters still accumulate (plain
/// thread-owned u64 adds), but no clock reads and no emission.
pub(crate) fn cprobe_enabled() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_CPROBE").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Consecutive Retry steps a driver spins (yield) before parking on the
/// pre-captured epoch. Retry windows are invalidated-slot waits (seal /
/// last-out / straggler tails); publish and completion both wake_all, so
/// the park is lost-wakeup-free. Small: each spin is a full step's global
/// ceremony — the 48xl finding-#1 storm.
pub(crate) const RETRY_PARK_AFTER: u32 = 16;

/// Claim-coalescing target for whole-boundary sources (dop1-tax fix 1):
/// a claim spans up to `PGRUST_RUNTIME_COALESCE_EPOCHS / active_workers`
/// epochs (default 8 → ×8 at DOP1, ×4 at 2, ×2 at 3-4, off at ≥5). `1` (or
/// `0`) disables coalescing — the inc-2 one-epoch-per-claim behavior, the
/// A/B arm. Read once.
fn coalesce_epochs() -> u64 {
    static N: crate::sync::OnceLock<u64> = crate::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_COALESCE_EPOCHS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(8)
    })
}

/// End-game terminal sub-RG split (tails192 #6, 48xl finding): at high
/// width the whole-boundary rule floors terminal claims at ONE whole RG,
/// making the photo-finish inert — the last few heavy RGs run on a
/// shrinking worker set while the rest of the crowd idles (48xl expr-key/10M-group arms:
/// 180-250ms finish skew = 30-45% of the drive at {96,191}). When the
/// remaining tail has fewer whole RGs than live workers, fall back to
/// sizer-driven granule claims INSIDE the RG (never across granules) —
/// dict-rebuild duplication is bounded to the last < W claims, the regime
/// the drive-scaling law never measured (its +78% was steady-state
/// splitting). Engages only at width > 32 (16-core behavior unchanged by
/// construction). Kill switch: PGRUST_RUNTIME_ENDGAME_SPLIT=0.
fn endgame_split_enabled() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_ENDGAME_SPLIT").map_or(true, |v| v.trim() != "0")
    })
}

/// Batched stats accumulator (agg192-contention hygiene, coordinator-
/// approved 2026-07-15): the per-task global/per-RG observability ticks
/// (tasks, morsels, granules, sizing decisions) accumulate thread-locally
/// per (worker, published task set) and flush on the same boundaries as
/// WFIN (seq change, observed exhaustion, idle transition, drive exit,
/// stop) — at 191 workers the per-task Relaxed RMWs on the packed
/// RuntimeStats/RgStats lines were ~10 shared-line hits per task per
/// worker. EXCEPTION kept synchronous: the FIRST task claim of each
/// (worker, task set) ticks `rg.stats.tasks_claimed` immediately — the
/// helper-death liveness backstop's `claimed == 0` fallback-vs-error gate
/// (runtime_agg.rs and siblings) must see claimed work even if a helper
/// later dies holding an unflushed accumulator. Counts stay EXACT for
/// every reader that observes completion (all flush points precede the
/// participant's exit); safety never reads these (M0 contract).
struct StatAcc {
    seq: u64,
    ts: Arc<TaskSetRt>,
    /// Claims BEYOND the first (the first is flushed synchronously, see
    /// the struct doc).
    tasks_claimed: u64,
    tasks_completed: u64,
    morsels: u64,
    granules: u64,
    /// Sizing-decision ticks: [ramp, default, shutdown].
    sizing: [u64; 3],
}

/// One worker's accumulated participation in one published task set.
struct WfinStat {
    seq: u64,
    qid: u64,
    pipe: usize,
    tasks: u64,
    task_ns: u64,
    /// Monotonic end of the worker's LAST task in the set (the finish time
    /// the spread instrument reads).
    last_end_ns: u64,
}

impl WfinStat {
    fn emit(&self, worker: usize) {
        let avg_us = if self.tasks > 0 { self.task_ns / self.tasks / 1000 } else { 0 };
        eprintln!(
            "MORSEL|WFIN|qid={}|pipe={}|worker={}|t_us={}|tasks={}|task_avg_us={}",
            self.qid,
            self.pipe,
            worker,
            self.last_end_ns / 1000,
            self.tasks,
            avg_us
        );
    }
}

impl WorkerLocal {
    /// Fold one completed task into the slot's WFIN accumulator; a stale
    /// accumulator (earlier publication in the same slot) flushes first.
    fn wfin_observe(&mut self, ts: &TaskSetRt, task_ns: u64, end_ns: u64) {
        if self.wfin.is_empty() {
            return;
        }
        let slot = &mut self.wfin[ts.slot];
        match slot {
            Some(s) if s.seq == ts.seq => {
                s.tasks += 1;
                s.task_ns += task_ns;
                s.last_end_ns = end_ns;
            }
            _ => {
                if let Some(old) = slot.take() {
                    old.emit(self.worker);
                }
                *slot = Some(WfinStat {
                    seq: ts.seq,
                    qid: ts.rg.query_id,
                    pipe: ts.index,
                    tasks: 1,
                    task_ns,
                    last_end_ns: end_ns,
                });
            }
        }
    }

    /// Flush the slot's accumulator (the worker observed the set exhausted —
    /// its participation is over; no worker can claim past an exhausted
    /// cursor).
    fn wfin_flush_slot(&mut self, slot: usize) {
        if self.wfin.is_empty() {
            return;
        }
        if let Some(s) = self.wfin[slot].take() {
            s.emit(self.worker);
        }
    }

    /// Flush every accumulator (idle transition / pinned-drive exit): any
    /// remaining participation record is final — the worker is leaving the
    /// execution loop.
    pub(crate) fn wfin_flush_all(&mut self) {
        if self.wfin.is_empty() {
            return;
        }
        for slot in 0..self.wfin.len() {
            self.wfin_flush_slot(slot);
        }
    }
}

pub(crate) struct Scheduler {
    slots: Vec<Slot>,
    /// Active-slot bitmask (2×u64 = 128 slots). M0 reads it directly per
    /// pick (read-mostly, uncontended at 16 workers); M5 syncs it into the
    /// thread-local views via the worker mailboxes.
    active: [AtomicU64; 2],
    /// M4 maintenance preference mask (docs/design/m4-bgjobs.md §3.5):
    /// subset of `active` holding Maintenance-class slots; the pick scans it
    /// first. ADVISORY ONLY — a stale bit resolves to Retry through the slot
    /// word, so no ordering discipline against `active` is needed. Cost on
    /// the foreground path: two zero-mask loads per pick.
    maint: [AtomicU64; 2],
    membership: Mutex<Membership>,
    pins: PinBoard,
    /// INERT until M5: per-worker change/return masks.
    #[allow(dead_code)]
    mailboxes: Vec<WorkerMailbox>,
    pub(crate) park: ParkLot,
    /// External pin-board lane lease bitmask (word w bit b = lane w*64+b
    /// busy). Lanes are leased through Runtime::acquire_external_lane;
    /// MAX_EXTERNAL_LANES = 256 ⇒ 4 words, scanned low-word-first.
    pub(crate) external_lanes: [AtomicU64; EXTERNAL_LANE_WORDS],
    /// Execution-permit semaphore: exactly `permits` (= cores) permits; any
    /// task-executing thread holds one (acquired by the pool loop around
    /// worker_step). The hard runnable cap of the §2.5 permit model. The
    /// pool runs `cores + K` threads (§2.8): the K standbys block here until
    /// a declared blocking section releases a permit through
    /// [`crate::sync::IoGuard`].
    pub(crate) permits: Semaphore,
    stop: AtomicBool,
    /// M5-4 stride activation switch (env default; tests toggle per
    /// instance). OFF ⇒ the M0 FIFO lowest-index pick, byte-identical.
    stride: AtomicBool,
    /// M5+1 pipeline-DAG dispatch switch (env default, DEFAULT OFF this
    /// increment; tests toggle per instance). OFF ⇒ the sequential
    /// one-slot-per-RG task-set walk, byte-identical.
    dag: AtomicBool,
    /// M5-5 decaying-priorities switch (env default ON; tests toggle per
    /// instance). OFF ⇒ every RG stays at p0 — the M5-4 equal-shares
    /// scheduler exactly.
    decay: AtomicBool,
    /// M5-5 decay rate λ ∈ (0,1) per consumed quantum (immutable after
    /// construction; ratified constant, env-overridable for calibration).
    decay_lambda: f64,
    /// M5-5 decay quantum in consumed-CPU ns (atomic: virtual-clock tests
    /// tighten it per instance to reach decay boundaries deterministically).
    decay_quantum_ns: AtomicU64,
    /// M5-5 starvation floor p_min ≥ 1 (atomic: the adversarial-skew tests
    /// probe alternative floors per instance; production is the ratified
    /// default).
    p_min: AtomicU32,
    /// Track-4 Q0: utility-class stride weight (atomic: per-instance test
    /// probe on the p_min precedent; production is the env default).
    p_util: AtomicU32,
    /// Track-4 kill switch (env default ON; tests toggle per instance).
    /// OFF ⇒ Utility submissions fold to Foreground — pre-QoS behavior
    /// byte-identically.
    util_qos: AtomicBool,
    /// Global pass watermark (monotone max of charged passes): a NEWLY
    /// admitted RG's slot pass starts here — standard stride join, no
    /// credit for queue wait, no monopoly for late arrivals.
    global_pass: AtomicU64,
    /// WS-B admission ledger (single-executor Phase 0.1): the width
    /// authority behind `ledger_on`. Inert (policy words never read on any
    /// hot path) while the switch is off.
    pub(crate) ledger: AdmissionLedger,
    /// PGRUST_RUNTIME_LEDGER_V2 switch (env default OFF; tests toggle per
    /// instance via [`crate::Runtime::set_ledger`]). OFF ⇒ today's
    /// scheduler, byte-identical.
    ledger_on: AtomicBool,
    clock: Arc<dyn Clock>,
    params: SizingParams,
    pub(crate) stats: RuntimeStats,
    /// Total pool threads (cores + standbys) — the pin board is sized by
    /// THREADS, not permits: a thread blocked in an I/O section keeps its
    /// pin and its finalization-marker obligations.
    nthreads: usize,
    next_seq: AtomicU64,
    next_rg_id: AtomicU64,
    /// POOL-QOS interactive-demand ledger (GL-POOLDB-1 mitigation): global
    /// unmet-width count over live FRESH (undecayed) bound RGs. Nonzero ⇔
    /// demoted pool serves should yield threads/permits at their next
    /// morsel boundary. One Relaxed load on the demoted step path; zero on
    /// every other path.
    qos_demand: AtomicU64,
    /// Per-slot remaining unmet width backing `qos_demand` (word-paired:
    /// publish charges, serve starts consume, slot release flushes — the
    /// global can never leak past its slot's lifetime).
    slot_bound_need: Vec<AtomicU32>,
    /// POOL-QOS: external-lane ordinals PARKED by yielded participants,
    /// keyed to their RG — released when the RG completes (swept at slot
    /// release). Parking prevents a rejoining serve from leasing the
    /// departed participant's lane and inheriting its mid-accept per-lane
    /// slot state (the sink Local↔executor pairing is per-lane). Bounded
    /// by the board's yield budget (≤ 2×tickets per engagement).
    parked_lanes: Mutex<Vec<(std::sync::Weak<ResourceGroup>, usize)>>,
    trace: bool,
}

impl Scheduler {
    pub(crate) fn new(
        nthreads: usize,
        permits: usize,
        nslots: usize,
        params: SizingParams,
        clock: Arc<dyn Clock>,
        trace: bool,
    ) -> Scheduler {
        assert!(nthreads > 0);
        assert!(permits > 0 && permits <= nthreads);
        assert!(nslots > 0 && nslots <= DEFAULT_SLOTS);
        Scheduler {
            slots: (0..nslots).map(|_| Slot::new()).collect(),
            active: [AtomicU64::new(0), AtomicU64::new(0)],
            maint: [AtomicU64::new(0), AtomicU64::new(0)],
            membership: Mutex::new(Membership {
                owned: (0..nslots).map(|_| None).collect(),
                waitq: VecDeque::new(),
            }),
            pins: PinBoard::new(nthreads + MAX_EXTERNAL_LANES),
            mailboxes: (0..nthreads).map(|_| WorkerMailbox::new()).collect(),
            park: ParkLot::new(),
            external_lanes: std::array::from_fn(|_| AtomicU64::new(0)),
            permits: Semaphore::new(permits),
            stop: AtomicBool::new(false),
            stride: AtomicBool::new(stride_default()),
            dag: AtomicBool::new(dag_default()),
            decay: AtomicBool::new(decay_default()),
            decay_lambda: decay_lambda_default(),
            decay_quantum_ns: AtomicU64::new(decay_quantum_default()),
            p_min: AtomicU32::new(p_min_default()),
            p_util: AtomicU32::new(p_util_default()),
            util_qos: AtomicBool::new(util_qos_default()),
            global_pass: AtomicU64::new(0),
            ledger: AdmissionLedger::new(nslots, LedgerBudgets::from_env(permits as u32)),
            ledger_on: AtomicBool::new(ledger_default()),
            qos_demand: AtomicU64::new(0),
            slot_bound_need: (0..nslots).map(|_| AtomicU32::new(0)).collect(),
            parked_lanes: Mutex::new(Vec::new()),
            clock,
            params,
            stats: RuntimeStats::default(),
            nthreads,
            next_seq: AtomicU64::new(0),
            next_rg_id: AtomicU64::new(0),
            trace,
        }
    }

    pub(crate) fn nthreads(&self) -> usize {
        self.nthreads
    }

    pub(crate) fn worker_local(&self, worker: usize) -> WorkerLocal {
        assert!(worker < self.nthreads);
        WorkerLocal {
            worker,
            drive: DriveLocal::default(),
            cache: (0..self.slots.len()).map(|_| None).collect(),
            wfin: if markers_enabled() {
                (0..self.slots.len()).map(|_| None).collect()
            } else {
                Vec::new()
            },
            stat: (0..self.slots.len()).map(|_| None).collect(),
            pinned_slot: None,
            bound_skip: (0..self.slots.len()).map(|_| None).collect(),
            local_pass: 0,
            global_pass: 0,
            session_token: 0,
            spinning: false,
        }
    }

    /// Bookkeeping for an EXTERNAL participant thread (M1 pinned driver):
    /// pin-board lane `nthreads + ordinal`.
    pub(crate) fn external_local(&self, ordinal: usize) -> WorkerLocal {
        assert!(ordinal < MAX_EXTERNAL_LANES, "external participant lanes exhausted");
        WorkerLocal {
            worker: self.nthreads + ordinal,
            drive: DriveLocal::default(),
            cache: (0..self.slots.len()).map(|_| None).collect(),
            wfin: if markers_enabled() {
                (0..self.slots.len()).map(|_| None).collect()
            } else {
                Vec::new()
            },
            stat: (0..self.slots.len()).map(|_| None).collect(),
            pinned_slot: None,
            bound_skip: (0..self.slots.len()).map(|_| None).collect(),
            local_pass: 0,
            global_pass: 0,
            session_token: 0,
            spinning: false,
        }
    }

    /// M5-4: per-instance stride toggle (tests / A-B; production reads the
    /// PGRUST_RUNTIME_STRIDE default once at construction).
    pub(crate) fn set_stride(&self, on: bool) {
        self.stride.store(on, Ordering::SeqCst);
    }

    pub(crate) fn stride_enabled(&self) -> bool {
        self.stride.load(Ordering::Relaxed)
    }

    /// M5+1: per-instance pipeline-DAG dispatch toggle (tests / A-B;
    /// production reads the PGRUST_RUNTIME_PIPELINE_DAG default once at
    /// construction — default OFF this increment).
    pub(crate) fn set_dag(&self, on: bool) {
        self.dag.store(on, Ordering::SeqCst);
    }

    pub(crate) fn dag_enabled(&self) -> bool {
        self.dag.load(Ordering::Relaxed)
    }

    /// M5-5: per-instance decay toggle (tests / A-B; production reads the
    /// PGRUST_RUNTIME_DECAY default once at construction — default ON).
    pub(crate) fn set_decay(&self, on: bool) {
        self.decay.store(on, Ordering::SeqCst);
    }

    pub(crate) fn decay_enabled(&self) -> bool {
        self.decay.load(Ordering::Relaxed)
    }

    /// M5-5 test hook: tighten the decay quantum so deterministic
    /// virtual-clock tests cross decay boundaries. Production keeps the
    /// ratified constant.
    pub(crate) fn set_decay_quantum_ns(&self, ns: u64) {
        self.decay_quantum_ns.store(ns.max(1), Ordering::SeqCst);
    }

    /// M5-5 test hook: probe alternative starvation floors (adversarial
    /// skew tests). Production keeps the ratified default.
    pub(crate) fn set_p_min(&self, p: u32) {
        self.p_min.store(p.clamp(1, crate::rg::INITIAL_PRIORITY), Ordering::SeqCst);
    }

    pub(crate) fn p_min_value(&self) -> u32 {
        self.p_min.load(Ordering::Relaxed)
    }

    /// Track-4 Q0 test hook: probe alternative utility stride weights
    /// (the set_p_min precedent). Production keeps the env default.
    pub(crate) fn set_p_util(&self, p: u32) {
        self.p_util.store(p.clamp(1, crate::rg::INITIAL_PRIORITY), Ordering::SeqCst);
    }

    pub(crate) fn p_util_value(&self) -> u32 {
        self.p_util.load(Ordering::Relaxed)
    }

    /// Track-4 kill-switch toggle (tests / A-B; production reads the
    /// PGRUST_RUNTIME_UTIL_QOS default once at construction — default ON).
    /// Toggle BEFORE submitting: the class fold happens at submit.
    pub(crate) fn set_util_qos(&self, on: bool) {
        self.util_qos.store(on, Ordering::SeqCst);
    }

    pub(crate) fn util_qos_enabled(&self) -> bool {
        self.util_qos.load(Ordering::Relaxed)
    }

    /// WS-B: per-instance ledger toggle (tests / A-B; production reads the
    /// PGRUST_RUNTIME_LEDGER_V2 default once at construction — default OFF
    /// this increment). Toggle BEFORE submitting: join/leave accounting
    /// assumes the switch is stable across a task.
    pub(crate) fn set_ledger(&self, on: bool) {
        self.ledger_on.store(on, Ordering::SeqCst);
    }

    pub(crate) fn ledger_enabled(&self) -> bool {
        self.ledger_on.load(Ordering::Relaxed)
    }

    /// Unified gang width lease (ledger.rs "Unified gang entries"): admit
    /// a non-pool parallel gang as a frozen non-shedding POOL-face entry.
    /// None ⇔ ledger OFF or [`crate::ledger::MAX_GANG_ENTRIES`] — FAIL-OPEN,
    /// the caller keeps today's uncapped path. The grant may be 0 (the
    /// caller must have a serial path). Takes ledger.inner ALONE — never
    /// called under the membership lock (lock-order note in the ledger
    /// module doc; same law as the external face).
    pub(crate) fn lease_gang_width(&self, requested: u32) -> Option<(usize, u32)> {
        if !self.ledger_on.load(Ordering::Relaxed) {
            return None;
        }
        self.ledger.admit_gang(requested)
    }

    /// Settle a gang entry to its ACTIVE width; widened pool targets wake
    /// parked workers (the retire wake discipline). NOT gated on
    /// ledger_on: a live entry must stay settleable across a test toggle.
    pub(crate) fn settle_gang_width(&self, id: usize, active: u32) {
        if self.ledger.settle_gang(id, active) > 0 {
            self.park.wake_all();
        }
    }

    /// Retire a gang entry (lease drop), waking parked workers when pool
    /// targets widen. NOT gated on ledger_on (as settle).
    pub(crate) fn retire_gang_width(&self, id: usize) {
        if self.ledger.retire_gang(id) > 0 {
            self.park.wake_all();
        }
    }

    /// WS-O inc-2 debug accessor: worker `worker`'s pin-board entry is
    /// settled (see PinBoard::is_settled — asserts/diagnostics only).
    pub(crate) fn pin_settled(&self, worker: usize) -> bool {
        self.pins.is_settled(worker)
    }

    /// Scheduler clock read (WFIN leader marks share the workers' domain).
    pub(crate) fn clock_now_ns(&self) -> u64 {
        self.clock.now_ns()
    }

    pub(crate) fn snapshot(&self) -> RuntimeStatsSnapshot {
        self.stats.snapshot()
    }

    pub(crate) fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.park.wake_all();
    }

    /// Call sites must guard with `if self.trace { ... }` BEFORE building the
    /// message — the format! argument otherwise allocates on every
    /// submit/publish/finalize even with tracing off (m2-integration
    /// std-collections audit, AGENTS.md rule 7: mallocs are a tracked
    /// metric on engaged paths).
    fn trace(&self, msg: &str) {
        if self.trace {
            eprintln!("[pgrust-runtime] {msg}");
        }
    }

    // ---- membership: submit / publish / admit -----------------------------

    pub(crate) fn submit(
        self: &Arc<Self>,
        spec: QuerySpec,
        pinned: bool,
        class: RgClass,
        session_token: u64,
        width: Option<WidthRequest>,
        bound: Option<BoundDescriptor>,
        on_rg: Option<&mut dyn FnMut(&crate::RgHandle)>,
    ) -> Arc<ResourceGroup> {
        assert!(
            !(pinned && class == RgClass::Maintenance),
            "maintenance RGs are pool-executed, never pinned"
        );
        assert!(
            bound.is_none() || pinned,
            "bound-engagement descriptors ride PINNED submissions only"
        );
        // Track-4 kill switch (PGRUST_RUNTIME_UTIL_QOS=0): fold Utility to
        // Foreground BEFORE the RG is built — class tag, stride weight, and
        // ledger tier all revert in this one place, restoring the pre-QoS
        // submit byte-identically.
        let class = if class == RgClass::Utility && !self.util_qos.load(Ordering::Relaxed) {
            RgClass::Foreground
        } else {
            class
        };
        let rg_id = self.next_rg_id.fetch_add(1, Ordering::SeqCst) + 1;
        let rg = ResourceGroup::new(
            rg_id,
            spec,
            pinned,
            class,
            session_token,
            Arc::downgrade(self),
            bound,
        );
        if class == RgClass::Utility {
            // Q0: the class weight IS the priority word — zero pick-path
            // change. Stored before any publish derives the slot stride
            // from it (publish_taskset_locked). At the default weight
            // (= the p_min floor) the M5-5 decay site skips utility RGs
            // outright via its `priority > p_min` guard.
            rg.priority.store(self.p_util.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        // M2 inc-3 rung 3: hand the caller its RgHandle BEFORE any
        // publication can make the RG pool-visible. A bound submission's
        // serve dispatches into arm code that resolves the RG through a
        // caller-side cell (payload.rg) — under the old order (publish,
        // return, caller sets the cell) a pool pick could reach the driver
        // inside the rg-set-after-publish window and pay a spurious
        // claim + "rg gone" refusal + detach (churn the rung-1 needle fix
        // had to tolerate; at 1-ticket boards one such refusal >= tickets
        // flipped the whole engagement to the fallback channel). Running
        // the callback here makes set-before-publish structural: there is
        // no state in which the RG is pick-visible with the cell unset.
        if let Some(f) = on_rg {
            f(&crate::RgHandle { rg: Arc::clone(&rg) });
        }
        rg.submit_ns.store(self.clock.now_ns().max(1), Ordering::Relaxed);
        RuntimeStats::tick(&self.stats.rgs_submitted);
        if self.trace {
            self.trace(&format!("rg {} submitted (query {})", rg.rg_id, rg.query_id));
        }
        self.emit_dag(&rg);
        if rg.tasksets.is_empty() {
            rg.done_ns.store(self.clock.now_ns().max(1), Ordering::Relaxed);
            rg.completion.complete(RgOutcome::Completed);
            RuntimeStats::tick(&self.stats.rgs_completed);
            self.emit_rgdone(&rg, false);
            return rg;
        }
        let mut m = lock(&self.membership);
        match class {
            // Utility shares Foreground's FIFO admission exactly (Track-4
            // Q0): no queue overtake in either direction — the class
            // differentiates SERVICE (stride weight, ledger tier), never
            // admission order. The 128-slot array makes queue pressure
            // rare; a queued utility RG waits like any query.
            RgClass::Foreground | RgClass::Utility => {
                // FIFO admission: never overtake queued RGs.
                if m.waitq.is_empty() {
                    if let Some(slot) = m.owned.iter().position(Option::is_none) {
                        self.start_rg_locked(&mut m, Arc::clone(&rg), slot, width);
                        return rg;
                    }
                }
                m.waitq.push_back((Arc::clone(&rg), width));
            }
            RgClass::Maintenance => {
                // Starvation floor (§3.5): a due job cycle takes any free
                // slot regardless of queued foreground RGs, and if every
                // slot is busy it goes to the FRONT of the queue (the
                // handful of jobs never meaningfully reorders the FIFO).
                if let Some(slot) = m.owned.iter().position(Option::is_none) {
                    self.start_rg_locked(&mut m, Arc::clone(&rg), slot, width);
                    return rg;
                }
                m.waitq.push_front((Arc::clone(&rg), width));
            }
        }
        rg
    }

    /// Mark the RG's first ready task set(s) started and publish. Caller
    /// holds the membership lock (lock order: membership, then progress).
    ///
    /// DAG dispatch (M5+1): admission publishes EVERY dependency-satisfied
    /// task set the free slots allow — a UNION ALL's branches / a
    /// multi-join's independent build sides start together. `slot` (the
    /// caller's free pick) takes the deepest; extras fan out into remaining
    /// free slots; capacity shortfalls defer (the deferred pipeline
    /// re-publishes when one of the query's own pipelines finishes).
    fn start_rg_locked(
        &self,
        m: &mut Membership,
        rg: Arc<ResourceGroup>,
        slot: usize,
        width: Option<WidthRequest>,
    ) {
        // WS-B ledger admission (knob-gated; lock order membership →
        // ledger.inner, this caller holds membership). Entries are
        // slot-keyed: under DAG dispatch only the RG's FIRST slot is
        // managed this increment — fan-out siblings fail open (unmanaged)
        // through every ledger entry point. The arrival's wake half rides
        // publish_taskset_locked's existing wake_all, which the advertises
        // flag suppresses for sub-JOIN_THRESHOLD admissions.
        if self.ledger_on.load(Ordering::Relaxed) {
            let req = width
                .unwrap_or_else(|| WidthRequest::unbounded(self.ledger.budgets().cores));
            // Track-4 Q1: the RG class selects the budget tier — Utility
            // enters the capped tier, everything else is Standard (today's
            // algebra exactly; Maintenance entries are few/single-morsel
            // and ride Standard).
            let tier = match rg.class {
                RgClass::Utility => LedgerClass::Utility,
                RgClass::Foreground | RgClass::Maintenance => LedgerClass::Standard,
            };
            let _nudge = self.ledger.admit(slot, req, tier);
        }
        // Stride join (M5-4, refined at M5-5): a newly-admitted RG starts at
        // the stride VIRTUAL TIME — the minimum pass among currently-active
        // slots — falling back to the M5-4 global-pass watermark when
        // nothing is active (or when the M5-5 kill switch is off, which
        // restores M5-4 exactly). M5-4's max-watermark join made
        // submit→first-service grow O(K·t_max) under a K-query background:
        // every active slot had to lap the watermark before the arrival's
        // first pick (MULTI panel measured 25µs → 3.9ms → 11ms at
        // K=1/2/6×4-workers). Joining at the min active pass restores the
        // §3.4 law (short-query latency under batch load approaches
        // isolated) with NO queue-wait credit and NO monopoly — the arrival
        // merely ties the most-behind incumbent and advances at its own
        // stride from there — and NO change to the starvation floor: a
        // floor RG's pass sits at most one (decayed, 16x) jump above the
        // min, so the 1/(1+K·p0/p_min) share bound is unchanged. Stored
        // BEFORE the publish makes the slot pickable; persists across the
        // RG's task-set republishes (publish never resets it).
        let watermark = self.global_pass.load(Ordering::Relaxed);
        let join = if self.decay.load(Ordering::Relaxed) && self.stride.load(Ordering::Relaxed)
        {
            let mut min_active: Option<u64> = None;
            for (i, mask) in self.active.iter().enumerate() {
                let mut w = mask.load(Ordering::Relaxed);
                while w != 0 {
                    let b = w.trailing_zeros() as usize;
                    w &= w - 1;
                    let s = i * 64 + b;
                    if s >= self.slots.len() {
                        break;
                    }
                    let p = self.slots[s].pass.load(Ordering::Relaxed);
                    min_active = Some(min_active.map_or(p, |m| m.min(p)));
                }
            }
            min_active.map_or(watermark, |m| m.min(watermark))
        } else {
            watermark
        };
        self.slots[slot].pass.store(join, Ordering::Relaxed);
        if !self.dag.load(Ordering::Relaxed) {
            let first = {
                let mut p = lock(&rg.progress);
                rg.next_ready(&mut p).expect("fresh RG must have a ready task set (index 0)")
            };
            self.publish_taskset_locked(m, rg, first, slot);
            return;
        }
        // DAG join: the QUERY's shared stride account starts at the same
        // join pass; every slot publish mirrors it into the slot's pass.
        rg.pass_account.store(join, Ordering::Relaxed);
        let free = m.owned.iter().filter(|e| e.is_none()).count();
        debug_assert!(free >= 1, "caller must hand start_rg_locked a free slot");
        let (ready, deferred) = {
            let mut p = lock(&rg.progress);
            rg.ready_all(&mut p, free)
        };
        assert!(!ready.is_empty(), "fresh RG must have a ready task set (index 0)");
        RuntimeStats::add(&self.stats.dag_ready_deferred, deferred as u64);
        self.publish_taskset_locked(m, Arc::clone(&rg), ready[0], slot);
        for &r in &ready[1..] {
            let s = m
                .owned
                .iter()
                .position(Option::is_none)
                .expect("ready_all is capped by the free-slot count");
            RuntimeStats::tick(&self.stats.dag_fanout_publishes);
            self.publish_taskset_locked(m, Arc::clone(&rg), r, s);
        }
    }

    fn publish_taskset_locked(
        &self,
        m: &mut Membership,
        rg: Arc<ResourceGroup>,
        index: usize,
        slot: usize,
    ) {
        // Submission gating (M5+1 structural deadlock premise 1): a task set
        // is only ever PUBLISHED with every dependency finalized — no
        // running task can wait on an unfinished pipeline because nothing
        // dependency-unsatisfied is visible to any worker.
        #[cfg(debug_assertions)]
        {
            let p = lock(&rg.progress);
            debug_assert!(
                rg.tasksets[index].deps.iter().all(|&d| p.done[d]),
                "published task set {index} with unsatisfied dependencies"
            );
        }
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let c0 = rg.tasksets[index].source.startup_c0();
        // Generation binding (H1): hand the work its generation BEFORE the
        // slot word below admits any worker — generation-keyed partial state
        // (the M2 sink plumbing) is armed before the first claim can land.
        rg.tasksets[index].work.bind_generation(rg.generation());
        let whole_claims = rg.tasksets[index].source.whole_boundary_claims();
        let coalesce = whole_claims && rg.tasksets[index].source.coalesce_claims();
        let ts = Arc::new(TaskSetRt {
            rg,
            index,
            slot,
            seq,
            cursor: crate::taskset::CachePadded(AtomicU64::new(0)),
            sizer: crate::taskset::CachePadded(crate::sizing::SizerShared::new()),
            active_workers: crate::taskset::CachePadded(AtomicU64::new(0)),
            fin_counter: crate::sync::atomic::AtomicI64::new(0),
            finalized: AtomicBool::new(false),
            c0,
            whole_claims,
            coalesce,
        });
        if self.trace {
            self.trace(&format!(
                "publish rg {} taskset {} in slot {slot} seq {seq}",
                ts.rg.rg_id, index
            ));
        }
        let pinned = ts.rg.pinned;
        let bound = ts.rg.bound.is_some();
        let class = ts.rg.class;
        // Stride inputs (M5-4), refreshed at every publish: the slot's
        // stride derives from the RG's CURRENT priority (constant p_0 at
        // equal shares; M5-5's decay updates flow through here), and the
        // session token feeds the equal-pass affinity tiebreak.
        self.slots[slot]
            .stride
            .store(stride_for(ts.rg.priority.load(Ordering::Relaxed)), Ordering::Relaxed);
        self.slots[slot].session.store(ts.rg.session_token, Ordering::Relaxed);
        // M5+1 advisory words: same-query identification + dependency depth
        // for the within-query pick refinement (read only under DAG mode).
        self.slots[slot].rg.store(ts.rg.rg_id, Ordering::Relaxed);
        self.slots[slot].depth.store(ts.rg.depth[index], Ordering::Relaxed);
        if self.dag.load(Ordering::Relaxed) {
            // The QUERY's shared stride account is the slot's pass (§3.6:
            // one account summed over the query's pipelines). Mirrored at
            // publish and at every advance; single-pipeline RGs see the
            // exact per-slot sequence sequential mode would produce.
            self.slots[slot].pass.store(ts.rg.pass_account.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        // POOL-QOS demand charge (under the membership lock, like every
        // slot-word transition): flush any stale need left on this slot,
        // then charge the engagement's unmet width iff the RG is bound AND
        // still INTERACTIVE-class at this publish (a fresh engagement's
        // first publish always is; a decayed RG's later task-set publishes
        // are not — demand is a fresh-start latency instrument, not a
        // width entitlement).
        if pool_qos_enabled() {
            self.qos_flush_slot(slot);
            if let Some(bd) = ts.rg.bound.as_ref() {
                if ts.rg.priority.load(Ordering::Relaxed) >= qos_interactive_p() && bd.width > 0
                {
                    self.slot_bound_need[slot].store(bd.width, Ordering::SeqCst);
                    self.qos_demand.fetch_add(bd.width as u64, Ordering::SeqCst);
                    RuntimeStats::add(&self.stats.qos_demand_published, bd.width as u64);
                }
            }
        }
        m.owned[slot] = Some(SlotEntry { seq, ts });
        self.slots[slot].word.store((seq << 1) | 1, Ordering::SeqCst);
        // WS-B JOIN_THRESHOLD (knob-gated; OFF ⇒ advert is true and the
        // path below is byte-identical): a sub-threshold admission never
        // sets the active bit and never wakes the pool — the submitter
        // executes alone (caller-as-worker; inert unless a submitter opts
        // in with an est_work_ns under the threshold).
        let advert =
            !self.ledger_on.load(Ordering::Relaxed) || self.ledger.advertises(slot);
        // Pinned RGs are invisible to the pool's pick: only external
        // participants (drive_pinned) may execute them — pool workers have
        // no session binding for the query (M1). M2 inc-2: a pinned RG
        // CARRYING a bound-engagement descriptor is the exception — the
        // descriptor IS the bindability proof, so publication sets the
        // active bit and pool workers claim engagements through its serve
        // (never morsels directly: worker_step's gate dispatches on
        // rg.bound before run_task can see the task set).
        if (!pinned || bound) && advert {
            self.set_active(slot, class);
        }
        RuntimeStats::tick(&self.stats.tasksets_published);
        // Wake parked workers: new work exists (external pinned drivers
        // park on the same epoch eventcount).
        //
        // GL-STMTTASK-2 (wake elision, PGRUST_POOL_WAKE_SPINNER=1|on,
        // DEFAULT OFF): the publish becomes epoch-bump + spinner check —
        // notify only when no searcher covers the work, and then exactly
        // ONE idle worker (LIFO; see ParkLot::wake_work). RG-specific
        // legacy parkers keep their own publish/completion/abort wake_all
        // sites; this is the NEW-WORK inbox wake only.
        if advert {
            if wake_spinner_enabled() {
                self.park.wake_work();
            } else {
                self.park.wake_all();
            }
        }
    }

    /// POOL-QOS: return `slot`'s remaining unmet width to zero, settling
    /// the global demand word. Called at publish (stale flush) and slot
    /// release — the pairing that keeps `qos_demand` leak-free.
    fn qos_flush_slot(&self, slot: usize) {
        let stale = self.slot_bound_need[slot].swap(0, Ordering::SeqCst);
        if stale > 0 {
            self.qos_demand.fetch_sub(stale as u64, Ordering::SeqCst);
        }
    }

    /// POOL-QOS: live interactive demand (one Relaxed load — the demoted
    /// step-boundary gate).
    pub(crate) fn qos_demand_live(&self) -> bool {
        self.qos_demand.load(Ordering::Relaxed) > 0
    }

    /// POOL-QOS: park a yielded participant's external-lane ordinal until
    /// its RG completes (see the field doc). The caller forgot the RAII
    /// lease; this owns the eventual bit clear.
    pub(crate) fn park_lane_for_rg(&self, rg: &Arc<ResourceGroup>, ordinal: usize) {
        lock(&self.parked_lanes).push((Arc::downgrade(rg), ordinal));
    }

    /// POOL-QOS: release parked lanes whose RG is gone or completed. Runs
    /// at slot release (completion cadence); the vec is yield-budget-small.
    fn sweep_parked_lanes(&self) {
        let mut parked = lock(&self.parked_lanes);
        parked.retain(|(rg, ordinal)| {
            let live = rg.upgrade().is_some_and(|rg| rg.completion.try_wait().is_none());
            if !live {
                self.external_lanes[ordinal / 64]
                    .fetch_and(!(1u64 << (ordinal % 64)), Ordering::SeqCst);
            }
            live
        });
    }

    fn set_active(&self, slot: usize, class: RgClass) {
        if class == RgClass::Maintenance {
            self.maint[slot / 64].fetch_or(1u64 << (slot % 64), Ordering::SeqCst);
        }
        self.active[slot / 64].fetch_or(1u64 << (slot % 64), Ordering::SeqCst);
    }

    fn clear_active(&self, slot: usize) {
        self.maint[slot / 64].fetch_and(!(1u64 << (slot % 64)), Ordering::SeqCst);
        self.active[slot / 64].fetch_and(!(1u64 << (slot % 64)), Ordering::SeqCst);
    }

    fn pick_slot(&self, local: &WorkerLocal) -> Option<usize> {
        // WS-B ledger filter switch: one cached-bool branch when OFF (the
        // byte-identity contract). ON composes wants_workers into the
        // active-slot scans; the maintenance preference and the FIFO kill-
        // switch path stay unfiltered (maintenance cycles are single-morsel
        // and few; PGRUST_RUNTIME_STRIDE=0 is a diagnostics configuration —
        // both recorded in notes/se-ws-b-ledger.md).
        let ledger_on = self.ledger_on.load(Ordering::Relaxed);
        // M4 preference: any Maintenance-class slot first (§3.5 starvation
        // floor; mask is usually zero — two loads on the foreground path).
        // Evaluated BEFORE the stride pick (m5-planner §3.2 reconciliation):
        // maintenance cycles are few and single-morsel, and their passes
        // charge normally below, so the preference cannot starve foreground.
        // A stale hit revalidates through the slot word into Retry.
        for (i, word) in self.maint.iter().enumerate() {
            let mask = word.load(Ordering::SeqCst);
            if mask != 0 {
                let slot = i * 64 + mask.trailing_zeros() as usize;
                if slot < self.slots.len() {
                    return Some(slot);
                }
            }
        }
        let m0 = self.active[0].load(Ordering::SeqCst);
        let m1 = self.active[1].load(Ordering::SeqCst);
        if m0 | m1 == 0 {
            return None;
        }
        // ONE active RG: the pick is forced — no pass reads, exactly the M0
        // lowest-index pick (the single-query bit-identity anchor: stride
        // and FIFO provably agree on this path). Ledger ON: a full slot
        // (granted == target) parks the surplus worker instead of handing
        // it a claim it would be refused (try_join) — with an unbounded
        // width request target = full width and the filter is a no-op, the
        // same identity anchor.
        if m0.count_ones() + m1.count_ones() == 1 {
            let (i, mask) = if m0 != 0 { (0usize, m0) } else { (1, m1) };
            let slot = i * 64 + mask.trailing_zeros() as usize;
            if slot >= self.slots.len() {
                return None;
            }
            if self.bound_skipped(local, slot) {
                return None;
            }
            if ledger_on && !self.ledger.wants_workers(slot) {
                return None;
            }
            return Some(slot);
        }
        if !self.stride.load(Ordering::Relaxed) {
            // Kill switch (PGRUST_RUNTIME_STRIDE=0): the M0 FIFO pick
            // (lowest active index this worker has not bound-skipped).
            for (i, mask) in [m0, m1].into_iter().enumerate() {
                let mut w = mask;
                while w != 0 {
                    let b = w.trailing_zeros() as usize;
                    w &= w - 1;
                    let slot = i * 64 + b;
                    if slot >= self.slots.len() {
                        break;
                    }
                    if !self.bound_skipped(local, slot) {
                        return Some(slot);
                    }
                }
            }
            return None;
        }
        // M5-4 stride pick (inter-query §5.3): lowest pass among active
        // slots; equal-pass ties prefer the slot whose RG's leader session
        // this worker is sticky-bound to (§5.2 affinity tiebreak — equal
        // pass ONLY, no bounded pass penalty, per the design's §10 default),
        // then lowest index (scan order). All reads Relaxed and advisory: a
        // stale winner revalidates through the slot word into Retry.
        let mut best: Option<(u64, usize)> = None;
        for (i, mask) in [m0, m1].into_iter().enumerate() {
            let mut w = mask;
            while w != 0 {
                let b = w.trailing_zeros() as usize;
                w &= w - 1;
                let slot = i * 64 + b;
                if slot >= self.slots.len() {
                    break;
                }
                // WS-B pick filter: skip saturated / non-advertising
                // entries — freed capacity flows to under-target slots in
                // pass order (the ledger consumes stride via this filter;
                // it never duplicates pass accounting). Advisory like every
                // pick input: a stale hit resolves through try_join into
                // Retry.
                if self.bound_skipped(local, slot) {
                    continue;
                }
                if ledger_on && !self.ledger.wants_workers(slot) {
                    continue;
                }
                let pass = self.slots[slot].pass.load(Ordering::Relaxed);
                match best {
                    None => best = Some((pass, slot)),
                    Some((bp, bs)) => {
                        if pass < bp {
                            best = Some((pass, slot));
                        } else if pass == bp
                            && local.session_token != 0
                            && self.slots[slot].session.load(Ordering::Relaxed)
                                == local.session_token
                            && self.slots[bs].session.load(Ordering::Relaxed)
                                != local.session_token
                        {
                            RuntimeStats::tick(&self.stats.affinity_tiebreaks);
                            best = Some((pass, slot));
                        } else if pass == bp && self.dag.load(Ordering::Relaxed) {
                            // M5+1 within-query refinement (§3.6): among a
                            // query's runnable pipelines at equal pass (its
                            // slots share one mirrored account, so they tie),
                            // prefer the DEEPER one — the critical path
                            // releases the most downstream work. Same-query
                            // only (advisory rg words match, nonzero); ties
                            // keep scan = submission order. Never crosses
                            // the affinity tiebreak: same-query slots carry
                            // the same session token.
                            let brg = self.slots[bs].rg.load(Ordering::Relaxed);
                            if brg != 0
                                && brg == self.slots[slot].rg.load(Ordering::Relaxed)
                                && self.slots[slot].depth.load(Ordering::Relaxed)
                                    > self.slots[bs].depth.load(Ordering::Relaxed)
                            {
                                RuntimeStats::tick(&self.stats.dag_depth_picks);
                                best = Some((pass, slot));
                            }
                        }
                    }
                }
            }
        }
        best.map(|(_, slot)| slot)
    }

    // ---- the worker step ---------------------------------------------------

    /// One scheduling decision + at most one task execution. The pool loop
    /// (and the loom models) drive this in a loop; on `Idle` the caller
    /// parks on an epoch captured BEFORE the call.
    /// GL-STMTTASK-2: clear this worker's search-phase (spinner) mark at a
    /// CLAIM point — work was found; a task/serve body must never count as
    /// a spinner or submissions would elide wakes while every other worker
    /// is parked. The park itself consumes the mark on the park path
    /// (ParkLot::park_worker); Retry keeps it (still searching).
    fn note_spin_claimed(&self, local: &mut WorkerLocal) {
        if local.spinning {
            local.spinning = false;
            self.park.spin_found_work();
        }
    }

    pub(crate) fn worker_step(&self, local: &mut WorkerLocal) -> Step {
        if self.stop.load(Ordering::SeqCst) {
            // Stop = a flush boundary (StatAcc contract): the loop exits.
            self.stat_flush_all(local);
            self.note_spin_claimed(local);
            return Step::Stop;
        }
        let Some(slot) = self.pick_slot(local) else {
            // Idle transition: nothing is runnable — any WFIN/stats
            // accumulation still held is this worker's final word on those
            // sets.
            self.stat_flush_all(local);
            local.wfin_flush_all();
            return Step::Idle;
        };

        // Protocol step 1: publish-target-before-claim.
        self.pins.publish(local.worker, slot);

        match self.resolve(local, slot) {
            None => {
                // Protocol step 4: settle own pin; pay any marker debt.
                self.settle(local.worker);
                Step::Retry
            }
            // M2 inc-2 bindability gate: a bound-descriptor RG is never
            // task-claimed by a pool worker — one ENGAGEMENT is served
            // through the descriptor instead (bind → external-lane drive →
            // unbind). Checked on the post-publish resolve, so a slot that
            // rolled to a bound RG between pick and revalidation can never
            // leak into run_task (an unbound morsel execution would error).
            // The pin is settled BEFORE the serve (inside serve_bound; the
            // serve's drive completes this very RG, and finalization waits
            // on every marked pin — holding our pool-lane pin across the
            // serve would deadlock the drive against our own settle).
            Some(ts) if ts.rg.bound.is_some() => {
                self.note_spin_claimed(local);
                self.serve_bound(local, &ts)
            }
            Some(ts) => {
                self.note_spin_claimed(local);
                // M2 inc-3 rung 3: ordinary (non-bound) work is about to run
                // on this thread — evict any parked session retention first
                // (a pool worker's sticky-parked session view must never be
                // live under unbound task bodies). One thread-local read +
                // branch when no residue is hinted (the pool-sticky posture
                // is the only setter). The stale affinity token dies with
                // the retention (advisory tiebreak input only).
                if crate::session_residue() {
                    local.session_token = 0;
                    crate::evict_session_residue_for_unbound_work();
                }
                let step = self.run_task_admitted(local, &ts);
                // Protocol step 4: settle own pin; pay any marker debt.
                self.settle(local.worker);
                step
            }
        }
    }

    /// M2 inc-2: serve one bound engagement on this pool worker (see
    /// [`BoundDescriptor`]). The serve nests a WHOLE pinned drive with its
    /// own permit rhythm, so the step's permit is given up around the call
    /// — worker_step is entered with the permit held (both pool-loop
    /// generations) and must return holding it. An unwind out of the serve
    /// is exit-committed (FATAL: the thread is dying and the pool glue owns
    /// drain + respawn); the permit then deliberately stays released — the
    /// pool loop's unwind path releases nothing, so accounting balances.
    ///
    /// The caller's published pin is settled HERE, before the serve (see
    /// worker_step); the settle-exactly-once contract (PinBoard's
    /// settle-without-publish assert) is why this branch owns it.
    fn serve_bound(&self, local: &mut WorkerLocal, ts: &Arc<TaskSetRt>) -> Step {
        self.settle(local.worker);
        let Some(bd) = ts.rg.bound.clone() else {
            return Step::Retry; // unreachable (caller gated); fail closed
        };
        // POOL-QOS: this serve is about to meet one unit of the slot's
        // unmet width — consume it BEFORE the serve so demoted holders stop
        // yielding toward width that is already being met. Returned on any
        // non-Served verdict (the pre-claim refusal window is µs).
        let took_need = pool_qos_enabled()
            && self.slot_bound_need[ts.slot]
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
        if took_need {
            self.qos_demand.fetch_sub(1, Ordering::SeqCst);
        }
        // Keep the io-layer permit flag ACCURATE across the serve: the
        // pool loop noted the permit held, but we are about to give it up —
        // a stale `true` would let a connect-phase uring wait inside the
        // serve donate a permit this thread no longer holds (count
        // inflation). With the flag false, the serve's I/O blocks plainly —
        // exactly a gang/launched external drive's posture. (The spill
        // facade is unaffected and stays armed: its sections run only
        // inside task bodies, where the nested drive holds a real permit.)
        #[cfg(not(loom))]
        crate::io::note_permit(false);
        self.permits.release();
        let served = (bd.serve)(&bd.payload);
        self.permits.acquire();
        #[cfg(not(loom))]
        crate::io::note_permit(true);
        match served {
            BoundServe::Served => {
                RuntimeStats::tick(&self.stats.bound_serves);
                // M2 inc-3 rung 3 affinity hint: if the serve parked a
                // session retention on this thread (the adapter noted the
                // residue before returning), record the served RG's leader-
                // session token so the equal-pass pick tiebreak prefers this
                // session's next engagement landing HERE — the sticky-resume
                // path. Cleared when the retention is not (re)parked.
                local.session_token = if crate::session_residue() {
                    ts.rg.session_token
                } else {
                    0
                };
                Step::Ran
            }
            BoundServe::Refused | BoundServe::Closed => {
                // POOL-QOS: the width unit was not met — return it (the
                // slot may have been flushed/republished meanwhile; only
                // re-add if the slot's need word still belongs to this
                // publication's lifetime, which the release-flush pairing
                // guarantees at worst as a transient over-count settled by
                // the release flush).
                if took_need {
                    self.slot_bound_need[ts.slot].fetch_add(1, Ordering::SeqCst);
                    self.qos_demand.fetch_add(1, Ordering::SeqCst);
                }
                // This worker will not serve THIS publication again
                // (identity/db refusal, closed board, participant cap):
                // remember the slot WORD so the pick skips the slot until
                // it changes — Idle parks stay reachable and the refusing
                // worker cannot spin on the board.
                local.bound_skip[ts.slot] = Some((ts.seq << 1) | 1);
                RuntimeStats::tick(&self.stats.bound_skips);
                Step::Retry
            }
        }
    }

    /// M2 inc-2: true ⇔ this worker bound-skips `slot` at its CURRENT word
    /// (see [`WorkerLocal::bound_skip`]). One branch when the cache entry is
    /// empty — the every-pick cost on unbound workloads.
    #[inline]
    fn bound_skipped(&self, local: &WorkerLocal, slot: usize) -> bool {
        match local.bound_skip.get(slot).copied().flatten() {
            Some(word) => self.slots[slot].word.load(Ordering::SeqCst) == word,
            None => false,
        }
    }

    /// [`Scheduler::run_task`] wrapped in the WS-B ledger's join/leave
    /// accounting (knob-gated; OFF = one cached-bool branch, no other
    /// change). A refused grant maps to [`Step::Retry`]: the worker
    /// re-picks — the filter now skips the saturated slot — and the drive
    /// loops' bounded-Retry discipline parks it on the epoch captured
    /// BEFORE the step, so the leave/renudge/admit wakes are
    /// lost-wakeup-free. The refused worker is still pinned; the caller's
    /// settle pays any marker debt exactly as on the invalidated-slot
    /// Retry.
    fn run_task_admitted(&self, local: &mut WorkerLocal, ts: &Arc<TaskSetRt>) -> Step {
        let ledger_on = self.ledger_on.load(Ordering::Relaxed);
        if ledger_on && !self.ledger.try_join(ts.slot) {
            return Step::Retry;
        }
        // Publish the grant for the declared-blocking-section entry points
        // (§2.8 composition): a task body that donates its execution permit
        // must donate its width grant with it (ledger_donate_current).
        let _grant = if ledger_on { Some(GrantCtx::set(self, ts.slot)) } else { None };
        let end = self.run_task(local, ts);
        if ledger_on {
            // WORKER-FREED RE-PICK: this worker re-picks on its own; the
            // hint covers PARKED peers when the slot turned joinable again.
            //
            // STARVED ends never wake (CALLER-C2 LEDGER HANG fix): a
            // Starved end means this worker's own claim just proved
            // nothing is claimable — a joinable-again wake re-offers
            // peers a slot they can only starve on, and the wake bumps
            // the park epoch, so every park (the pool eventcount AND the
            // caller-C2 bounded idle park's epoch pre-check) returns
            // immediately. With granted == target (any sole-worker slot:
            // cores=1, or the pinned caller-C2 drive) that is a
            // SELF-WAKE busy-loop: starve → leave-hint → wake_all →
            // epoch moved → re-step → starve → …, the idle park never
            // entered, 100% CPU until a publish arrives (never, if the
            // producer IS the parked duty — the observed suite hang
            // under global PGRUST_RUNTIME_LEDGER_V2=1). Claimable work
            // is never stranded by the gate: a publish that lands
            // mid-task bumps the epoch itself (notify_source_progress →
            // wake_all), this worker's own pre-captured park epoch has
            // therefore moved, and it re-picks the slot; parked peers
            // ride that publish wake or the bounded re-nudge. The leave
            // ACCOUNTING stays unconditional — only the wake is gated.
            if self.ledger.leave(ts.slot) > 0 && !matches!(end, TaskEnd::Starved) {
                self.park.wake_all();
            }
        }
        match end {
            TaskEnd::Exhausted => {
                // Protocol step 2: exhausted → invalidate (coordinator
                // election by slot-word CAS).
                self.coordinate(ts);
                Step::Ran
            }
            TaskEnd::Budget => Step::Ran,
            // Starved stream: park (epoch captured before this step, so
            // a publish that landed mid-task wakes the park at once).
            TaskEnd::Starved => Step::Idle,
        }
    }

    /// One scheduling step of an EXTERNAL participant restricted to ONE
    /// pinned RG (M1: a bound parallel helper executes only the query whose
    /// session state it carries). Same protocol as `worker_step` — publish
    /// before slot-word read, run, coordinate on exhaustion, settle — with
    /// the pick replaced by a membership lookup of the RG's occupied slot.
    /// Deliberately does NOT observe `stop`: external participants are
    /// session-driven; their exit condition is RG completion (the caller
    /// re-tests `RgHandle::try_outcome` around every step).
    pub(crate) fn worker_step_pinned(
        &self,
        local: &mut WorkerLocal,
        rg: &Arc<ResourceGroup>,
    ) -> Step {
        // Fast path: the cached (slot, seq) revalidated by one slot-word
        // read — the membership lock is a publish/finalize-event cost, not a
        // per-step cost (the sched-probe decision-cost budget).
        let slot = match local.pinned_slot {
            Some((slot, seq))
                if self.slots[slot].word.load(Ordering::SeqCst) == (seq << 1) | 1 =>
            {
                Some(slot)
            }
            _ => {
                // CPROBE: stale-cache membership lookup (one global mutex
                // acquisition; per-iteration during invalidated-slot spins).
                local.drive.pinned_lookups += 1;
                let found = {
                    let m = lock(&self.membership);
                    if self.dag.load(Ordering::Relaxed) {
                        // M5+1: a pinned RG may occupy several slots. Among
                        // its live pipelines prefer the DEEPEST (§3.6
                        // critical-path priority), then the least-crowded
                        // (spreads the gang across independent pipelines),
                        // then lowest index. Advisory — a stale choice
                        // revalidates through the slot word into Retry.
                        let mut best: Option<(u64, u64, usize, u64)> = None;
                        for (i, e) in m.owned.iter().enumerate() {
                            let Some(e) = e.as_ref().filter(|e| Arc::ptr_eq(&e.ts.rg, rg))
                            else {
                                continue;
                            };
                            let depth = e.ts.rg.depth[e.ts.index];
                            let crowd = e.ts.active_workers.load(Ordering::SeqCst);
                            let better = match &best {
                                None => true,
                                Some((bd, bc, _, _)) => {
                                    depth > *bd || (depth == *bd && crowd < *bc)
                                }
                            };
                            if better {
                                best = Some((depth, crowd, i, e.seq));
                            }
                        }
                        best.map(|(_, _, i, seq)| (i, seq))
                    } else {
                        m.owned.iter().enumerate().find_map(|(i, e)| {
                            e.as_ref()
                                .filter(|e| Arc::ptr_eq(&e.ts.rg, rg))
                                .map(|e| (i, e.seq))
                        })
                    }
                };
                local.pinned_slot = found;
                found.map(|(slot, _)| slot)
            }
        };
        let Some(slot) = slot else {
            // Queued behind other RGs, or completed: the caller re-tests
            // completion and parks on an epoch captured before this call.
            // Idle = a flush boundary (StatAcc contract).
            self.stat_flush_all(local);
            return Step::Idle;
        };

        // Protocol step 1: publish-target-before-claim.
        self.pins.publish(local.worker, slot);

        let step = match self.resolve(local, slot) {
            None => Step::Retry,
            Some(ts) if !Arc::ptr_eq(&ts.rg, rg) => {
                // The slot rolled to a different RG between lookup and
                // revalidation; not ours to run.
                Step::Retry
            }
            // Ledger ON: external pinned drivers consult the same
            // join/leave/should_continue words as pool workers — this is
            // where cross-query narrowing between concurrent pinned gangs
            // becomes real (integration contract 1c ruling 4).
            Some(ts) => self.run_task_admitted(local, &ts),
        };

        // Protocol step 4: settle own pin; pay any marker debt.
        self.settle(local.worker);
        step
    }

    // ---- batched stats (see [`StatAcc`]) -----------------------------------

    fn stat_flush(&self, acc: StatAcc) {
        // Zero-guarded: fetch_add(0) is still a shared-line RMW.
        if acc.tasks_claimed > 0 {
            RuntimeStats::add(&self.stats.tasks_claimed, acc.tasks_claimed);
            RuntimeStats::add(&acc.ts.rg.stats.tasks_claimed, acc.tasks_claimed);
        }
        if acc.tasks_completed > 0 {
            RuntimeStats::add(&self.stats.tasks_completed, acc.tasks_completed);
            RuntimeStats::add(&acc.ts.rg.stats.tasks_completed, acc.tasks_completed);
        }
        if acc.morsels > 0 {
            RuntimeStats::add(&self.stats.morsels_claimed, acc.morsels);
            RuntimeStats::add(&acc.ts.rg.stats.morsels_claimed, acc.morsels);
        }
        if acc.granules > 0 {
            RuntimeStats::add(&self.stats.granules_executed, acc.granules);
            RuntimeStats::add(&acc.ts.rg.stats.granules_executed, acc.granules);
        }
        if acc.sizing[0] > 0 {
            RuntimeStats::add(&self.stats.sizing_ramp, acc.sizing[0]);
        }
        if acc.sizing[1] > 0 {
            RuntimeStats::add(&self.stats.sizing_default, acc.sizing[1]);
        }
        if acc.sizing[2] > 0 {
            RuntimeStats::add(&self.stats.sizing_shutdown, acc.sizing[2]);
        }
    }

    fn stat_flush_slot(&self, local: &mut WorkerLocal, slot: usize) {
        if let Some(acc) = local.stat[slot].take() {
            self.stat_flush(acc);
        }
    }

    pub(crate) fn stat_flush_all(&self, local: &mut WorkerLocal) {
        for slot in 0..local.stat.len() {
            self.stat_flush_slot(local, slot);
        }
    }

    /// Record one task claim: the FIRST claim of a (worker, task set)
    /// participation ticks synchronously (the helper-death `claimed == 0`
    /// gate — [`StatAcc`] doc); the rest accumulate.
    fn stat_task_claimed(&self, local: &mut WorkerLocal, ts: &Arc<TaskSetRt>) {
        let slot = ts.slot;
        match &mut local.stat[slot] {
            Some(a) if a.seq == ts.seq => a.tasks_claimed += 1,
            other => {
                if let Some(old) = other.take() {
                    self.stat_flush(old);
                }
                RuntimeStats::tick(&self.stats.tasks_claimed);
                RuntimeStats::tick(&ts.rg.stats.tasks_claimed);
                *other = Some(StatAcc {
                    seq: ts.seq,
                    ts: Arc::clone(ts),
                    tasks_claimed: 0,
                    tasks_completed: 0,
                    morsels: 0,
                    granules: 0,
                    sizing: [0; 3],
                });
            }
        }
    }

    /// Fold a completed task's morsel/granule/sizing counts + the completion
    /// into the slot accumulator (installed by [`Self::stat_task_claimed`]).
    fn stat_task_done(
        &self,
        local: &mut WorkerLocal,
        ts: &Arc<TaskSetRt>,
        morsels: u64,
        granules: u64,
        sizing: [u64; 3],
    ) {
        match &mut local.stat[ts.slot] {
            Some(a) if a.seq == ts.seq => {
                a.tasks_completed += 1;
                a.morsels += morsels;
                a.granules += granules;
                for i in 0..3 {
                    a.sizing[i] += sizing[i];
                }
            }
            // No accumulator (impossible after stat_task_claimed, kept
            // fail-open): flush synchronously.
            _ => {
                self.stat_flush(StatAcc {
                    seq: ts.seq,
                    ts: Arc::clone(ts),
                    tasks_claimed: 0,
                    tasks_completed: 1,
                    morsels,
                    granules,
                    sizing,
                });
            }
        }
    }

    /// Revalidate the slot word (single atomic read on the cached path) and
    /// return the task set it names.
    fn resolve(&self, local: &mut WorkerLocal, slot: usize) -> Option<Arc<TaskSetRt>> {
        let word = self.slots[slot].word.load(Ordering::SeqCst);
        if word & 1 == 0 {
            return None;
        }
        let seq = word >> 1;
        if let Some((cseq, ts)) = &local.cache[slot] {
            if *cseq == seq {
                return Some(Arc::clone(ts));
            }
        }
        let m = lock(&self.membership);
        let entry = m.owned[slot].as_ref()?;
        if entry.seq != seq {
            return None;
        }
        local.cache[slot] = Some((seq, Arc::clone(&entry.ts)));
        Some(Arc::clone(&entry.ts))
    }

    /// Execute one task: claim boundary-clamped morsel ranges from the shared
    /// cursor until the duration budget is spent, the set is exhausted, or
    /// the generation dies. [`TaskEnd::Exhausted`] ⇔ the task set is
    /// exhausted (or its generation is dead) and finalization should be
    /// driven; [`TaskEnd::Starved`] ⇔ an open stream source has nothing
    /// claimable (park, producer wakes).
    fn run_task(&self, local: &mut WorkerLocal, ts: &Arc<TaskSetRt>) -> TaskEnd {
        // Batched (StatAcc): first claim of the participation synchronous,
        // the rest thread-local until a flush boundary.
        self.stat_task_claimed(local, ts);
        // Submit→first-service instrument (§3.5): CAS-once at the RG's first
        // task admission (one Relaxed load per task thereafter).
        if ts.rg.first_service_ns.load(Ordering::Relaxed) == 0 {
            let _ = ts.rg.first_service_ns.compare_exchange(
                0,
                self.clock.now_ns().max(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        let task_t0 = if local.wfin.is_empty() { 0 } else { self.clock.now_ns() };
        // Live width AFTER this worker joins: the claim-duration DOP scaling
        // input (tails192 #4) — identity at ≤32 by construction, so 16-core
        // pods and the mt16 vectors size exactly as before.
        let task_width = ts.active_workers.fetch_add(1, Ordering::SeqCst) + 1;
        // Per-task observability counts, folded into the slot's StatAcc
        // after the task (declared here so both match arms share the fold).
        let mut t_morsels = 0u64;
        let mut t_granules = 0u64;
        let mut t_sizing = [0u64; 3];

        // Generation gate (H1): a task of an aborted (closed) generation is
        // unconsumable — the merged lifecycle's fail-closed armed join
        // refuses, so no participant, no morsel. The exhausted path then
        // drives ordinary invalidate/finalize cleanup.
        let end = match ts.rg.handle.join() {
            Err(_refused) => {
                RuntimeStats::tick(&self.stats.generation_refusals);
                TaskEnd::Exhausted
            }
            Ok(participant) => {
                local.drive.tasks += 1;
                let mut sizer = TaskSizer::new(self.params.scaled_for_width(task_width), ts.c0);
                let mut end = TaskEnd::Budget;
                // Per-task observability accumulators (dop1-tax fix 5):
                // morsel/granule/cpu counters are EXACT but flushed to the
                // shared relaxed atomics once per TASK, not per morsel —
                // the counts are observability (snapshots, trace, stride
                // accounting inputs), never safety; nothing reads them at
                // sub-task granularity. Budget/flush metering (the sink
                // lanes' safety accounting) lives operator-side and is
                // untouched.
                let mut t_cpu_ns = 0u64;
                loop {
                    // Morsel-boundary cancel point (Leis-style): an abort is
                    // observed within one morsel.
                    if ts.rg.is_aborted() {
                        end = TaskEnd::Exhausted;
                        break;
                    }
                    // WS-O C1 claim-boundary duty hook (None on every pool
                    // worker: one TL read). False = end this task at the
                    // boundary (`end` is already TaskEnd::Budget) and fall
                    // back to the caller's step loop, where the full
                    // error-carrying duty runs.
                    if let Some(duty) = CALLER_DUTY.with(std::cell::Cell::get) {
                        // SAFETY: installed only for the extent of the
                        // caller's drive frame on this thread (CallerDutyCtx
                        // RAII; see its SAFETY contract).
                        if !unsafe { (*duty)() } {
                            break;
                        }
                    }
                    // WS-B ledger claim-boundary verdict (knob-gated; OFF =
                    // one cached-bool branch). Yield — an arrival narrowed
                    // this entry below its live grants — rides the EXISTING
                    // TaskEnd::Budget path: the finalization protocol (slot
                    // word / pin board / fin counter) never sees the ledger.
                    if self.ledger_on.load(Ordering::Relaxed) {
                        match self.ledger.should_continue(ts.slot) {
                            ClaimVerdict::Yield => {
                                // `end` is already TaskEnd::Budget.
                                break;
                            }
                            ClaimVerdict::Continue => {
                                // BOUNDED RE-NUDGE: an under-target entry
                                // may request one wake per boundary, capped
                                // by the per-recompute budget.
                                if self.ledger.renudge(ts.slot) {
                                    self.park.wake_all();
                                }
                            }
                        }
                    }
                    let range = match self.claim_morsel(
                        ts,
                        &mut sizer,
                        &mut local.drive,
                        &mut t_sizing,
                    ) {
                        Claim::Range(range) => range,
                        Claim::Starved => {
                            end = TaskEnd::Starved;
                            break;
                        }
                        Claim::Exhausted => {
                            end = TaskEnd::Exhausted;
                            break;
                        }
                    };
                    let granules = range.end - range.start;
                    let t0 = self.clock.now_ns();
                    // Execute under the participant's operation count. A
                    // refusal means the close (abort) landed between the
                    // boundary check and the claim: drain WITHOUT running
                    // the claimed range — aborted generations need not
                    // execute every granule, only never twice.
                    let worker = local.worker;
                    let work = ts.work();
                    if participant
                        .run(|| {
                            work.run_morsel(worker, range);
                            Ok(())
                        })
                        .is_err()
                    {
                        end = TaskEnd::Exhausted;
                        break;
                    }
                    let t1 = self.clock.now_ns();
                    let dt = t1.saturating_sub(t0);
                    // WFIN accumulators (thread-owned plain data).
                    local.drive.morsels += 1;
                    local.drive.granules += granules;
                    local.drive.busy_ns += dt;
                    if local.drive.first_claim_ns == 0 {
                        local.drive.first_claim_ns = t0;
                    }
                    local.drive.last_end_ns = t1;
                    sizer.observe(&ts.sizer, granules, dt);
                    t_morsels += 1;
                    t_granules += granules;
                    t_cpu_ns += dt;
                    if sizer.task_done() {
                        break;
                    }
                }
                if t_morsels > 0 {
                    // Stride accounting (LIVE as of M5-4): the RG's CPU
                    // consumption (fairness-instrument readback) and the
                    // slot's pass advance. The `.max(1)` floors zero-dt
                    // virtual-clock charges so multi-RG progress still
                    // rotates (task-count round-robin) in deterministic
                    // tests and loom models.
                    let cpu_total = ts
                        .rg
                        .cpu_consumed_ns
                        .fetch_add(t_cpu_ns, Ordering::Relaxed)
                        .saturating_add(t_cpu_ns);
                    if self.stride.load(Ordering::Relaxed) {
                        let stride = self.slots[ts.slot].stride.load(Ordering::Relaxed);
                        let adv = (t_cpu_ns.saturating_mul(stride) >> PASS_SHIFT).max(1);
                        let new_pass = if self.dag.load(Ordering::Relaxed) {
                            // M5+1 (§3.6): the QUERY is the fair-share
                            // principal — advance the RG's shared account
                            // (the SUM of its pipelines' quanta) and mirror
                            // it into this slot's pass word. Sibling slots
                            // lag by at most their own last task (advisory;
                            // they re-sync on their next advance). With one
                            // pipeline this is value-identical to the slot
                            // fetch_add below.
                            let np =
                                ts.rg.pass_account.fetch_add(adv, Ordering::Relaxed) + adv;
                            self.slots[ts.slot].pass.store(np, Ordering::Relaxed);
                            np
                        } else {
                            self.slots[ts.slot].pass.fetch_add(adv, Ordering::Relaxed) + adv
                        };
                        // Watermark: monotone max (new admissions join here).
                        let mut cur = self.global_pass.load(Ordering::Relaxed);
                        while new_pass > cur {
                            match self.global_pass.compare_exchange_weak(
                                cur,
                                new_pass,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break,
                                Err(c) => cur = c,
                            }
                        }
                        // M5-5 decaying priorities (inter-query §5.4): when
                        // the RG's consumed CPU crosses a decay-quantum
                        // boundary, recompute p(q) = max(p_min, p0·λ^q) and
                        // refresh THIS slot's stride in place so the new
                        // share takes effect at the next advance (sibling
                        // DAG slots refresh on republish — advisory, same
                        // argument as the pass words). The completed task
                        // was charged at the stride it RAN under (the load
                        // above precedes this refresh). The decay_quanta
                        // CAS makes each boundary apply exactly once across
                        // racing workers; priority is monotone
                        // non-increasing (min with current) so late racers
                        // never raise it. Skipped entirely once the RG sits
                        // at the floor.
                        if self.decay.load(Ordering::Relaxed) {
                            let qn = self.decay_quantum_ns.load(Ordering::Relaxed).max(1);
                            let q = cpu_total / qn;
                            let prev_q = ts.rg.decay_quanta.load(Ordering::Relaxed);
                            let p_min = self.p_min.load(Ordering::Relaxed);
                            if q > prev_q
                                && ts.rg.priority.load(Ordering::Relaxed) > p_min
                                && ts
                                    .rg
                                    .decay_quanta
                                    .compare_exchange(
                                        prev_q,
                                        q,
                                        Ordering::Relaxed,
                                        Ordering::Relaxed,
                                    )
                                    .is_ok()
                            {
                                let p0 = crate::rg::INITIAL_PRIORITY as f64;
                                let decayed =
                                    (p0 * self.decay_lambda.powi(q.min(64) as i32)) as u32;
                                let cur_p = ts.rg.priority.load(Ordering::Relaxed);
                                let p = decayed.max(p_min).min(cur_p);
                                if p < cur_p {
                                    ts.rg.priority.store(p, Ordering::Relaxed);
                                    self.slots[ts.slot]
                                        .stride
                                        .store(stride_for(p), Ordering::Relaxed);
                                    RuntimeStats::tick(&self.stats.priority_decays);
                                }
                            }
                        }
                    }
                }
                // Armed-outcome discipline: a worker's task ends
                // successfully even when it drained an abort — failure is
                // recorded on the lifecycle by the aborting side, never by
                // drained workers. (An unfinished Drop would cancel the
                // generation; complete() is the required exit.)
                let _ = participant.complete();
                end
            }
        };

        ts.active_workers.fetch_sub(1, Ordering::SeqCst);
        // Batched (StatAcc): completion + morsel/granule/sizing fold; an
        // observed exhaustion ends this worker's participation (no claim
        // can succeed past an exhausted cursor), so flush the slot then.
        self.stat_task_done(local, ts, t_morsels, t_granules, t_sizing);
        if matches!(end, TaskEnd::Exhausted) {
            self.stat_flush_slot(local, ts.slot);
        }
        // WFIN marker channel (off = one branch): fold this task into the
        // slot's accumulator, same flush boundary.
        if !local.wfin.is_empty() {
            let now = self.clock.now_ns();
            local.wfin_observe(ts, now.saturating_sub(task_t0), now);
            if matches!(end, TaskEnd::Exhausted) {
                local.wfin_flush_slot(ts.slot);
            }
        }
        end
    }

    fn claim_morsel(
        &self,
        ts: &TaskSetRt,
        sizer: &mut TaskSizer,
        drive: &mut DriveLocal,
        sizing_acc: &mut [u64; 3],
    ) -> Claim {
        // Stream sources: claimable up to the producer's watermark; only a
        // CLOSED stream's watermark is exhaustion (closed read before the
        // watermark inside stream_state — see the MorselSource contract).
        let (total, closed) = match ts.source().stream_state() {
            Some(state) => state,
            None => (ts.source().total_granules(), true),
        };
        loop {
            let cur = ts.cursor.load(Ordering::SeqCst);
            if cur >= total {
                return if closed { Claim::Exhausted } else { Claim::Starved };
            }
            let workers = ts.active_workers.load(Ordering::SeqCst).max(1);
            let (want, decision) = sizer.next_size(&ts.sizer, total - cur, workers);
            // Never split a granule (whole-granule ranges by construction);
            // never cross a row-group / dictionary-epoch boundary.
            let bound = ts.source().next_boundary_after(cur).min(total);
            debug_assert!(bound > cur, "MorselSource boundary contract violated");
            // Whole-boundary claims (drive-scaling inc-2): epoch-heavy
            // sources never stop a claim short of the boundary — a split
            // epoch is executed by 2+ workers, each rebuilding the epoch's
            // dictionary/memo state (the measured dict-memo-shape DOP15 +78% busy
            // inflation). The sizer still observes for phase/stats.
            // End-game terminal sub-RG split (tails192 #6): the whole-RG
            // floor below makes the Shutdown sizing inert; when the tail
            // holds fewer whole RGs than live workers, claim sizer-sized
            // granule ranges inside the RG instead (photo-finish restored,
            // dict duplication bounded to the last < W claims). The Ramp
            // gate keeps first claims whole (no cold-start splitting);
            // width > 32 keeps 16-core byte behavior unchanged.
            let terminal_split = ts.whole_claims
                && workers > crate::sizing::DOPSCALE_W0
                && decision != SizingDecision::Ramp
                && endgame_split_enabled()
                && (total - cur) < workers.saturating_mul(bound - cur);
            let end = if ts.whole_claims && !terminal_split {
                // Claim coalescing (dop1-tax fix 1): at LOW live width the
                // per-claim drive re-entry (scan reposition + drain prologue
                // + partial export, ~30-45µs each on dict-memo-heavy shapes) is
                // the dominant DOP-1 tax — span SEVERAL epochs per claim and
                // let the work body iterate per-epoch segments (one dict
                // snapshot per segment; the epoch rules hold inside the
                // claim). Width signal: `ts.active_workers`, already loaded
                // above for the photo-finish W — the taskset's own live
                // participant count, zero extra cost, and (unlike the pool's
                // running count) it counts EXTERNAL pinned-drive lanes,
                // which is what actually executes M1/M2 engagements. The
                // factor decays to 1 as workers join, so a mid-query DOP
                // widening naturally reverts to single-epoch claims; gating
                // on the Default phase keeps the FIRST claims (Startup ramp,
                // before the gang's joins are all visible) single-epoch —
                // no stale-width giant claim can front-run a widening.
                // Photo-finish (Shutdown) sizing is untouched.
                let mut end = bound;
                if ts.coalesce && decision == SizingDecision::Default {
                    let factor = coalesce_epochs() / workers;
                    if factor > 1 {
                        // Fair-share clamp: never claim past this worker's
                        // 1/W share of the remainder — late tail claims
                        // shrink toward one epoch as remaining work runs
                        // out, preserving the ≤1-task finish-spread posture.
                        let fair_end = cur + ((total - cur) / workers).max(1);
                        for _ in 1..factor {
                            if end >= total || end >= fair_end {
                                break;
                            }
                            end = ts.source().next_boundary_after(end).min(total);
                        }
                    }
                    // Claim-duration DOP scaling at HIGH width (tails192
                    // #4): whole-claims sources discard the sizer's size —
                    // one epoch per claim — so at W>32 the shared-cursor
                    // touch rate scales with W at flat per-epoch cost
                    // (191 × ~2ms ≈ 95K touches/s, the 48xl in-drive
                    // inflation family). `want` already carries the
                    // width-scaled t_max target (TaskSizer is constructed
                    // with scaled params): span additional WHOLE epochs
                    // (dict-epoch rules hold inside a claim, same as the
                    // low-width coalesce above) until the claim reaches the
                    // duration target, under the same fair-share clamp so
                    // late claims still shrink toward one epoch. Identity
                    // at W ≤ 32 and under PGRUST_RUNTIME_TMAX_DOPSCALE=0;
                    // Startup/Shutdown phases untouched (photo finish keeps
                    // its posture).
                    if workers > crate::sizing::DOPSCALE_W0
                        && crate::sizing::dopscale_enabled()
                    {
                        let fair_end = cur + ((total - cur) / workers).max(1);
                        while end < total && end < fair_end && end - cur < want {
                            end = ts.source().next_boundary_after(end).min(total);
                        }
                    }
                }
                end
            } else {
                cur.saturating_add(want).min(bound).max(cur + 1)
            };
            if ts
                .cursor
                .compare_exchange(cur, end, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Batched (StatAcc): folded into the slot accumulator at
                // task end, flushed at the WFIN-class boundaries.
                sizing_acc[match decision {
                    SizingDecision::Ramp => 0,
                    SizingDecision::Default => 1,
                    SizingDecision::Shutdown => 2,
                }] += 1;
                return Claim::Range(cur..end);
            }
            // CPROBE: contended-cursor evidence (thread-owned counter).
            drive.cas_retries += 1;
        }
    }

    /// Protocol steps 2+3: invalidate the slot (unique coordinator via CAS),
    /// then mark still-pinned workers and fund the finalization counter.
    fn coordinate(&self, ts: &Arc<TaskSetRt>) {
        let valid = (ts.seq << 1) | 1;
        if self.slots[ts.slot]
            .word
            .compare_exchange(valid, ts.seq << 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // someone else coordinates (or already did)
        }
        self.clear_active(ts.slot);
        RuntimeStats::tick(&self.stats.tasksets_invalidated);
        if self.trace {
            self.trace(&format!(
                "invalidate rg {} taskset {} slot {} seq {}",
                ts.rg.rg_id, ts.index, ts.slot, ts.seq
            ));
        }
        let mut marked = 0i64;
        for w in 0..self.nthreads + MAX_EXTERNAL_LANES {
            if self.pins.mark(w, ts.slot) {
                marked += 1;
            }
        }
        RuntimeStats::add(&self.stats.finalize_marks, marked as u64);
        let after = ts.fin_counter.fetch_add(marked, Ordering::SeqCst) + marked;
        // The coordinator is itself still pinned (its own mark is in
        // `marked` and its decrement hasn't happened), so `after >= 1` here;
        // the zero check is kept because the protocol's rule is "whoever
        // moves the counter to zero runs finalization", not "the add can't".
        if after == 0 {
            self.last_out(ts);
        }
    }

    /// Protocol step 4: clear own pin; if a coordinator counted us, pay the
    /// decrement — and if that decrement is the zero crossing, we are
    /// provably the last worker out (step 5).
    fn settle(&self, worker: usize) {
        let Some(slot) = self.pins.settle(worker) else {
            return;
        };
        // Safe by protocol: the marked task set is still owned at `slot`
        // because last-out (the only replacer) is blocked on our decrement —
        // see module doc.
        let ts = {
            let m = lock(&self.membership);
            let entry = m.owned[slot]
                .as_ref()
                .expect("marked worker's task set must still be owned");
            Arc::clone(&entry.ts)
        };
        let after = ts.fin_counter.fetch_sub(1, Ordering::SeqCst) - 1;
        if after < 0 {
            // Transiently negative: we drained before the coordinator's add
            // landed. Legal and expected under the protocol.
            RuntimeStats::tick(&self.stats.finalize_negative_observed);
        }
        if after == 0 {
            self.last_out(&ts);
        }
    }

    /// Protocol step 5: at-most-once finalization by the provably-last
    /// worker out, then activate the RG's next task set in the same slot —
    /// or complete the RG and admit the next queued one.
    ///
    /// DAG dispatch (M5+1): the finalize is the DEPENDENCY-EDGE
    /// SATISFACTION event — it publishes every newly-ready pipeline (the
    /// deepest reuses this slot; the rest fan out into free slots, deferring
    /// on capacity). A finishing pipeline with live siblings releases its
    /// slot WITHOUT completing the RG; the RG completes only at live == 0
    /// with nothing ready (all done, or aborted).
    fn last_out(&self, ts: &Arc<TaskSetRt>) {
        let was = ts.finalized.swap(true, Ordering::SeqCst);
        assert!(!was, "finalization must run at most once");
        let rg = Arc::clone(&ts.rg);
        let aborted = rg.is_aborted();
        if !aborted {
            ts.work().finalize();
        }
        RuntimeStats::tick(&self.stats.finalize_events);
        if self.trace {
            self.trace(&format!(
                "finalize rg {} taskset {} (aborted={aborted})",
                rg.rg_id, ts.index
            ));
        }

        if self.dag.load(Ordering::Relaxed) {
            self.last_out_dag(ts, rg, aborted);
            return;
        }

        // ---- sequential walk (DAG dispatch off): today's behavior --------
        // Progress under the RG lock only (never while holding membership).
        let next = {
            let mut p = lock(&rg.progress);
            p.done[ts.index] = true;
            p.live -= 1;
            if aborted {
                p.aborted = true;
                None
            } else {
                rg.next_ready(&mut p)
            }
        };

        match next {
            Some(i) => {
                let mut m = lock(&self.membership);
                debug_assert!(
                    matches!(&m.owned[ts.slot], Some(e) if e.seq == ts.seq),
                    "slot ownership changed before last-out"
                );
                self.publish_taskset_locked(&mut m, rg, i, ts.slot);
            }
            None => {
                #[cfg(debug_assertions)]
                if !aborted {
                    let p = lock(&rg.progress);
                    debug_assert!(
                        p.done.iter().all(|&d| d),
                        "RG completed with unfinished task sets (dep DAG hole)"
                    );
                }
                self.release_slot_and_admit(ts.slot, ts.seq);
                self.complete_rg(&rg, aborted);
            }
        }
    }

    /// The M5+1 edge-satisfaction/submission handshake (module doc; loom
    /// model `dag_fanout_publish_exactly_once`). Membership is taken FIRST
    /// (lock order: membership, then progress) so the ready computation,
    /// its capacity cap, and the publishes are one atomic membership event —
    /// two concurrently-finishing dependencies of one consumer serialize
    /// here, and exactly one of them observes the consumer ready.
    fn last_out_dag(&self, ts: &Arc<TaskSetRt>, rg: Arc<ResourceGroup>, aborted: bool) {
        enum After {
            Publish(Vec<usize>),
            Release { complete: bool },
        }
        let mut m = lock(&self.membership);
        debug_assert!(
            matches!(&m.owned[ts.slot], Some(e) if e.seq == ts.seq),
            "slot ownership changed before last-out"
        );
        let after = {
            let mut p = lock(&rg.progress);
            p.done[ts.index] = true;
            p.live -= 1;
            if aborted {
                // No new submissions on a dead generation; live published
                // siblings drain through their own generation-refusal
                // last-outs — the LAST one completes the RG.
                p.aborted = true;
                After::Release { complete: p.live == 0 }
            } else {
                // This slot is still ours (freed below or reused), so the
                // capacity for newly-ready pipelines is free slots + 1.
                let free = m.owned.iter().filter(|e| e.is_none()).count();
                let (ready, deferred) = rg.ready_all(&mut p, free + 1);
                RuntimeStats::add(&self.stats.dag_ready_deferred, deferred as u64);
                if ready.is_empty() {
                    if p.live == 0 {
                        debug_assert!(
                            p.done.iter().all(|&d| d),
                            "RG completed with unfinished task sets (dep DAG hole)"
                        );
                        After::Release { complete: true }
                    } else {
                        After::Release { complete: false }
                    }
                } else {
                    After::Publish(ready)
                }
            }
        };
        match after {
            After::Publish(ready) => {
                // Deepest into this (retained) slot — the within-query
                // critical-path preference — then fan out.
                self.publish_taskset_locked(&mut m, Arc::clone(&rg), ready[0], ts.slot);
                for &r in &ready[1..] {
                    let s = m
                        .owned
                        .iter()
                        .position(Option::is_none)
                        .expect("ready_all is capped by the free-slot count");
                    RuntimeStats::tick(&self.stats.dag_fanout_publishes);
                    self.publish_taskset_locked(&mut m, Arc::clone(&rg), r, s);
                }
            }
            After::Release { complete } => {
                let complete_aborted = self.release_slot_locked(&mut m, ts.slot, ts.seq);
                drop(m);
                for q in complete_aborted {
                    self.complete_queued_aborted(&q);
                }
                if complete {
                    self.complete_rg(&rg, aborted);
                }
            }
        }
    }

    /// RG completion tail shared by both dispatch modes: retire the
    /// generation, publish the outcome, wake waiters.
    fn complete_rg(&self, rg: &Arc<ResourceGroup>, aborted: bool) {
        // The RG leaves the scheduler: drain (a no-op — every participant
        // is provably gone, see retire_lifecycle) and retire its generation
        // before the leader wakes.
        rg.retire_lifecycle();
        rg.done_ns.store(self.clock.now_ns().max(1), Ordering::Relaxed);
        rg.completion.complete(if aborted { RgOutcome::Aborted } else { RgOutcome::Completed });
        RuntimeStats::tick(&self.stats.rgs_completed);
        if aborted {
            RuntimeStats::tick(&self.stats.rgs_aborted);
        }
        self.emit_rgdone(rg, aborted);
        // Parked pinned drivers observe completion by re-testing
        // try_outcome after a wake; the completion word itself only
        // unparks registered leader waiters.
        //
        // GL-STMTTASK-2 (wake elision, same knob as the submission wake):
        // completion is NOT new work for pool workers — only legacy
        // (external-driver) parkers need the notify; a parked pool worker
        // stays parked (slot release re-admission publishes through
        // publish_taskset_locked, which carries its own wake).
        if wake_spinner_enabled() {
            self.park.wake_legacy();
        } else {
            self.park.wake_all();
        }
        if self.trace {
            self.trace(&format!("rg {} complete (aborted={aborted})", rg.rg_id));
        }
    }

    fn release_slot_and_admit(&self, slot: usize, seq: u64) {
        let complete_aborted = {
            let mut m = lock(&self.membership);
            self.release_slot_locked(&mut m, slot, seq)
        };
        for rg in complete_aborted {
            self.complete_queued_aborted(&rg);
        }
        // POOL-QOS: completion cadence — free parked lanes whose RG ended.
        if pool_qos_enabled() {
            self.sweep_parked_lanes();
        }
    }

    /// Free `slot` and admit the next queued RG into it. Returns RGs popped
    /// while already aborted — they complete without ever running (the
    /// caller finishes them OUTSIDE the membership lock).
    fn release_slot_locked(
        &self,
        m: &mut Membership,
        slot: usize,
        seq: u64,
    ) -> Vec<Arc<ResourceGroup>> {
        let mut complete_aborted: Vec<Arc<ResourceGroup>> = Vec::new();
        debug_assert!(
            matches!(&m.owned[slot], Some(e) if e.seq == seq),
            "releasing a slot we do not own"
        );
        m.owned[slot] = None;
        // POOL-QOS: the slot's engagement is over — flush its unmet width
        // from the demand word (an interactive RG that completed under-
        // width must stop drawing yields).
        if pool_qos_enabled() {
            self.qos_flush_slot(slot);
        }
        // WS-B ledger retirement (knob-gated): both completion paths — the
        // sequential last-out and the DAG release — funnel here, BEFORE the
        // waitq pop can admit the next RG into the slot. The wake hint
        // covers workers parked under the old, narrower targets (their
        // entries just widened). Queued-abort completions never reach a
        // slot and have nothing to retire (retire on a never-admitted slot
        // is a no-op).
        if self.ledger_on.load(Ordering::Relaxed) && self.ledger.retire(slot) > 0 {
            self.park.wake_all();
        }
        while let Some((rg, width)) = m.waitq.pop_front() {
            if rg.is_aborted() {
                complete_aborted.push(rg);
                continue;
            }
            self.start_rg_locked(m, rg, slot, width);
            break;
        }
        complete_aborted
    }

    /// Complete an aborted RG that never reached a slot (popped aborted at
    /// admission, or reaped from the wait queue at abort time). The caller
    /// must have REMOVED the RG from the wait queue under the membership
    /// lock — removal is the exactly-once election for this completion.
    fn complete_queued_aborted(&self, rg: &Arc<ResourceGroup>) {
        {
            let mut p = lock(&rg.progress);
            p.aborted = true;
        }
        rg.retire_lifecycle();
        rg.done_ns.store(self.clock.now_ns().max(1), Ordering::Relaxed);
        rg.completion.complete(RgOutcome::Aborted);
        RuntimeStats::tick(&self.stats.rgs_completed);
        RuntimeStats::tick(&self.stats.rgs_aborted);
        self.emit_rgdone(rg, true);
        self.park.wake_all();
    }

    /// M5-4 slot-reclamation fix (m5-planner §3.3 / §7 row M5-4): reap an
    /// aborted RG from the WAIT QUEUE at abort time, completing it promptly
    /// — a queued abort must not wait for an unrelated slot to free (the
    /// m1-scan-pipelines boundary note). Exactly-once with the admission
    /// pop: both paths remove under the membership lock, and only the
    /// remover completes. Not found ⇒ the RG was already admitted (its
    /// abort drains through the ordinary protocol) or already popped.
    pub(crate) fn reap_queued_abort(&self, rg: &Arc<ResourceGroup>) {
        let removed = {
            let mut m = lock(&self.membership);
            match m.waitq.iter().position(|(q, _)| Arc::ptr_eq(q, rg)) {
                Some(i) => {
                    m.waitq.remove(i);
                    true
                }
                None => false,
            }
        };
        if !removed {
            return;
        }
        RuntimeStats::tick(&self.stats.queued_aborts_reaped);
        if self.trace {
            self.trace(&format!("rg {} reaped from wait queue (aborted while queued)", rg.rg_id));
        }
        self.complete_queued_aborted(rg);
    }

    /// `MORSEL|DAG|…` per-query pipeline-DAG trace (§3.6 instruments; same
    /// PGRUST_MORSEL_MARKERS switch as WFIN/LFIN/RGDONE): one line at
    /// submit with the decomposition — pipeline count, dependency edges
    /// (`d->i`), per-pipeline depth, and the dispatch mode. Dispatch ORDER
    /// is read off the publish/WFIN/RGDONE stream correlated by qid/pipe.
    /// Off = one branch per submission.
    fn emit_dag(&self, rg: &ResourceGroup) {
        if !markers_enabled() {
            return;
        }
        let mut edges = String::new();
        for (i, ts) in rg.tasksets.iter().enumerate() {
            for &d in &ts.deps {
                if !edges.is_empty() {
                    edges.push(',');
                }
                edges.push_str(&format!("{d}->{i}"));
            }
        }
        let depths =
            rg.depth.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
        eprintln!(
            "MORSEL|DAG|qid={}|rg={}|mode={}|pipes={}|edges={}|depths={}",
            rg.query_id,
            rg.rg_id,
            if self.dag.load(Ordering::Relaxed) { "dag" } else { "seq" },
            rg.tasksets.len(),
            edges,
            depths,
        );
    }

    /// `MORSEL|RGDONE|…` completion trace (§3.5 submit→service channels;
    /// same PGRUST_MORSEL_MARKERS switch and clock domain as WFIN/LFIN):
    /// per-RG submit→first-service→done timestamps and the CPU readback the
    /// MULTI-arm fairness verdicts consume. Off = one branch per completion.
    fn emit_rgdone(&self, rg: &ResourceGroup, aborted: bool) {
        if !markers_enabled() {
            return;
        }
        eprintln!(
            "MORSEL|RGDONE|qid={}|rg={}|class={:?}|outcome={}|submit_us={}|first_us={}|done_us={}|cpu_us={}|prio={}",
            rg.query_id,
            rg.rg_id,
            rg.class,
            if aborted { "aborted" } else { "completed" },
            rg.submit_ns.load(Ordering::Relaxed) / 1000,
            rg.first_service_ns.load(Ordering::Relaxed) / 1000,
            rg.done_ns.load(Ordering::Relaxed) / 1000,
            rg.cpu_consumed_ns.load(Ordering::Relaxed) / 1000,
            rg.priority.load(Ordering::Relaxed),
        );
        // GL-POOLDB-1 G3 gap: class-behavior emission channels for the
        // letter rig (markers_enabled-gated like RGDONE itself).
        // MORSEL|QOS| — the pool-qos snapshot at this RG's completion.
        if pool_qos_enabled() {
            let s = self.stats.snapshot();
            eprintln!(
                "MORSEL|QOS|qid={}|rg={}|demand_live={}|demand_published={}|yields={}|prio_acquires={}|permit_defers={}|mem_holds={}",
                rg.query_id,
                rg.rg_id,
                self.qos_demand.load(Ordering::Relaxed),
                s.qos_demand_published,
                s.qos_yields,
                s.qos_priority_acquires,
                s.qos_permit_defers,
                s.qos_mem_holds,
            );
        }
        // MORSEL|LEDGER| — the WS-B admission-ledger snapshot (the letter
        // spec's named future-proof grep; ledger default OFF ⇒ absent).
        if self.ledger_on.load(Ordering::Relaxed) {
            let ls = self.ledger.snapshot();
            eprintln!("MORSEL|LEDGER|qid={}|rg={}|{:?}", rg.query_id, rg.rg_id, ls);
        }
    }
}
