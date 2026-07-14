//! ResourceGroup: one per query / background job (Umbra RG, lit-review §1.1).
//! Holds the query's ordered TaskSets in pipeline-dependency-DAG order, its
//! generation-keyed lifecycle, per-RG stats, and the completion state the
//! submitting leader parks on.
//!
//! Leaders SUBMIT AND PARK only — there is deliberately NO leader-execution
//! path (decided 2026-07-13 design review, redesign doc §2.5: under the
//! cores-permit cap a participating leader only displaces a worker; the
//! deferred participation mechanism is documented there and must not be
//! built here).

use std::sync::Arc;

use crate::lifecycle::{Generation, ParticipantOwner, QueryTaskLifecycle, TaskHandle};
use crate::morsel::{MorselRange, MorselSource};
use crate::stats::RgStats;
use crate::sync::atomic::{AtomicU32, AtomicU64};
use crate::sync::{lock, Mutex};

use types_error::{PgError, ERROR};

/// Umbra's initial priority p_0 = 10^4 (SIGMOD'21 §3.2). As of M5-4 this
/// seeds the slot stride (equal shares: every RG holds p_0 — the decaying
/// update `p_{i+1} = max(p_min, λ·p_i)` is M5-5's, deliberately not here).
pub const INITIAL_PRIORITY: u32 = 10_000;

/// The work body of one pipeline's task set. M0 exercises this with
/// synthetic implementations; M1 plugs lane pipelines in
/// (fork → accept_local → combine → finalize maps onto run_morsel/finalize
/// at this altitude — the sink contract lives above this trait).
pub trait TaskSetWork: Send + Sync {
    /// Generation binding (H1 structural fix, M2-prep): called by the
    /// scheduler when this task set is PUBLISHED — strictly before any
    /// worker can claim a morsel of it (the slot word that admits workers
    /// is stored after this call) and again on every re-publish (rescan
    /// regeneration, M1+). Work that keys partial state by generation (the
    /// sink plumbing in [`crate::sink`]) records it here; stateless work
    /// keeps the default no-op.
    fn bind_generation(&self, _generation: Generation) {}

    /// Execute one claimed morsel (whole-granule range). Called in parallel
    /// from many workers.
    fn run_morsel(&self, worker: usize, range: MorselRange);

    /// At-most-once pipeline finalization, run by the provably-last worker
    /// out of the task set (the last-worker-out protocol in sched.rs).
    /// Single-threaded by protocol.
    fn finalize(&self);
}

pub struct TaskSetSpec {
    pub source: Arc<dyn MorselSource>,
    pub work: Arc<dyn TaskSetWork>,
    /// Indices of task sets in the same RG that must have FINALIZED before
    /// this one may start. DAG order: every dep index < this set's index.
    pub deps: Vec<usize>,
}

pub struct QuerySpec {
    pub query_id: u64,
    pub tasksets: Vec<TaskSetSpec>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RgOutcome {
    Completed,
    Aborted,
}

/// RG scheduling class (M4, docs/design/m4-bgjobs.md §3.5). Maintenance RGs
/// (background-job cycles) are preferred by the pool's pick over foreground
/// FIFO order, and overtake the wait queue on submission — the minimal
/// starvation floor, deliberately NOT a scheduler: maintenance slots are few
/// (one per due job cycle) and their task sets are single-morsel, so the
/// foreground tax is bounded by one worker × one cycle body. The slot word
/// stays the sole execution authority; the class only orders the pick.
/// Stride/priority activation stays M5.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RgClass {
    Foreground,
    Maintenance,
}

pub(crate) struct RgProgress {
    pub(crate) started: Vec<bool>,
    pub(crate) done: Vec<bool>,
    pub(crate) aborted: bool,
}

/// RG completion state the submitting leader parks on (§2.5: submit-and-
/// park is the leader's ONLY interaction).
///
/// RE-HOMED onto the real Waiter (M0 lane C, replacing this struct's
/// interim Mutex+Condvar): a waiting leader registers its thread's
/// WakerHandle under the same lock that guards the outcome — complete()
/// either sees the handle (and unparks it) or the waiter sees the outcome,
/// so a wake cannot be lost — then parks on its own slot. Multiple
/// CompletionWaiter clones may wait; every registered handle is unparked.
/// Spurious/stale wakes and the Waiter's recheck cadence backstop re-test
/// the predicate by looping. Under cfg(loom) the wake side is inert (the
/// waiter crate's global slot surface is production-only); the loom models
/// deliberately poll try_wait, as production shutdown does.
pub(crate) struct Completion {
    state: Mutex<CompletionInner>,
}

struct CompletionInner {
    outcome: Option<RgOutcome>,
    /// Registered leader waker handles (waiter::WakerHandle as u64).
    /// Unused under cfg(loom) — nothing parks there.
    wakers: Vec<u64>,
}

impl Completion {
    fn new() -> Self {
        Completion {
            state: Mutex::new(CompletionInner { outcome: None, wakers: Vec::new() }),
        }
    }

    pub(crate) fn complete(&self, outcome: RgOutcome) {
        let mut g = lock(&self.state);
        debug_assert!(g.outcome.is_none(), "resource group completed twice");
        g.outcome = Some(outcome);
        let wakers = std::mem::take(&mut g.wakers);
        drop(g);
        #[cfg(not(loom))]
        for w in wakers {
            let _ = waiter::unpark_word(w);
        }
        #[cfg(loom)]
        debug_assert!(wakers.is_empty(), "loom models must poll try_wait, not park");
    }

    #[cfg(not(loom))]
    fn wait(&self) -> RgOutcome {
        loop {
            {
                let mut g = lock(&self.state);
                if let Some(outcome) = g.outcome {
                    return outcome;
                }
                let h = waiter::current_handle().as_u64();
                if !g.wakers.contains(&h) {
                    g.wakers.push(h);
                }
            }
            // Notified (real wake), Recheck (cadence backstop), or a
            // spurious wake aimed at a previous registration: loop and
            // re-test the outcome either way.
            let _ = waiter::park();
        }
    }

    /// cfg(loom): nothing exercises a parked leader in the models (they
    /// poll try_wait); keep wait() compiling for API parity.
    #[cfg(loom)]
    fn wait(&self) -> RgOutcome {
        loop {
            if let Some(outcome) = self.try_wait() {
                return outcome;
            }
            loom::thread::yield_now();
        }
    }

    fn try_wait(&self) -> Option<RgOutcome> {
        lock(&self.state).outcome
    }

    /// Register a waker word to be unparked at completion WITHOUT parking
    /// this thread (M4 bgjobs dispatcher: single thread observing many
    /// RGs). Returns true ⇔ the RG is already complete — no wake will
    /// follow; the caller consumes `try_wait()` instead. Same lock-ordered
    /// no-lost-wake argument as `wait()`: complete() either sees the word
    /// (and unparks it) or this registration sees the outcome.
    #[cfg(not(loom))]
    fn register_waker_word(&self, word: u64) -> bool {
        let mut g = lock(&self.state);
        if g.outcome.is_some() {
            return true;
        }
        if !g.wakers.contains(&word) {
            g.wakers.push(word);
        }
        false
    }
}

/// The runtime scheduler's [`ParticipantOwner`] — the "dispatcher-owned
/// participant shutdown protocol" the donor lifecycle's fail-closed
/// admission demands. The scheduler affirmatively owns stop and liveness
/// for its participants: pool workers are the ONLY joiners, they observe a
/// close (abort) at every morsel boundary (run_task's is_aborted check and
/// the per-morsel operation guard) and drain within one morsel, so
/// `permits_join` is unconditionally true. `request_stop` needs no side
/// channel: the lifecycle word itself (OPEN cleared) is the stop signal the
/// workers poll. `generation_stopped` stays false — drain always waits for
/// real participant exit; workers cannot wedge between morsel boundaries.
struct SchedulerOwner;

impl ParticipantOwner for SchedulerOwner {
    fn permits_join(&self, _generation: Generation) -> bool {
        true
    }

    fn request_stop(&self, _generation: Generation) {}

    fn generation_stopped(&self, _generation: Generation) -> bool {
        false
    }
}

fn abort_error() -> Box<PgError> {
    PgError::new(ERROR, "runtime resource group aborted").into()
}

pub struct ResourceGroup {
    pub(crate) rg_id: u64,
    pub(crate) query_id: u64,
    /// M1 pinned submission (`Runtime::submit_pinned`): the RG's task sets
    /// are executed ONLY by external participant threads driving
    /// `Runtime::drive_pinned` — publication never sets the global active
    /// bit, so pool workers (which cannot bind the query's session state
    /// yet; §2.3 db-pinning is M2+) never claim from it. All other protocol
    /// machinery (cursor, sizing, pin board, last-worker-out finalization,
    /// abort drain) is identical.
    pub(crate) pinned: bool,
    /// M4 scheduling class (see [`RgClass`]). Maintenance implies !pinned
    /// (asserted at submit): job cycles are executed by pool workers.
    pub(crate) class: RgClass,
    /// Query-owned generation machinery (H1 structural fix): every task the
    /// runtime carves for this RG carries (query_id, generation) and enters
    /// shared state only through the generation's fail-closed armed join
    /// (lane A's merged lifecycle: TaskHandle::join → TaskParticipant).
    pub(crate) task: Arc<QueryTaskLifecycle>,
    /// The RG's single published generation (M0 RGs never rescan, so the
    /// generation never rotates; reinitialize exists on the lifecycle for
    /// the M1+ rescan path).
    pub(crate) handle: TaskHandle,
    pub(crate) tasksets: Vec<TaskSetSpec>,
    pub(crate) progress: Mutex<RgProgress>,
    pub(crate) completion: Completion,
    pub(crate) stats: RgStats,
    /// Priority feeding the slot stride (M5-4: constant p_0 = equal shares;
    /// M5-5 activates the decay update `p_{i+1} = max(p_min, λ·p_i)` here).
    pub(crate) priority: AtomicU32,
    /// CPU nanoseconds consumed by this RG's tasks — the quantity stride
    /// passes advance by (LIVE as of M5-4), and the per-RG CPU-share
    /// readback of the fairness instruments (§3.5).
    pub(crate) cpu_consumed_ns: AtomicU64,
    /// Session-affinity token of the submitting leader (0 = none): the
    /// equal-pass pick tiebreak prefers workers sticky-bound to this session
    /// (M5-4; set via Runtime::submit_with_affinity — QuerySpec deliberately
    /// unchanged for backward compatibility, integration-train note).
    pub(crate) session_token: u64,
    /// Scheduler back-reference for the queued-abort reap (M5-4 slot-
    /// reclamation fix): an aborted RG still in the wait queue completes
    /// promptly instead of waiting for a slot to free. Weak — the RG never
    /// keeps the scheduler alive.
    pub(crate) sched: std::sync::Weak<crate::sched::Scheduler>,
    /// Submit→service instrument channel (§3.5), scheduler-clock ns:
    /// submit time, first task admission, completion. 0 = not yet.
    pub(crate) submit_ns: AtomicU64,
    pub(crate) first_service_ns: AtomicU64,
    pub(crate) done_ns: AtomicU64,
}

impl ResourceGroup {
    pub(crate) fn new(
        rg_id: u64,
        spec: QuerySpec,
        pinned: bool,
        class: RgClass,
        session_token: u64,
        sched: std::sync::Weak<crate::sched::Scheduler>,
    ) -> Arc<ResourceGroup> {
        let n = spec.tasksets.len();
        for (i, ts) in spec.tasksets.iter().enumerate() {
            for &d in &ts.deps {
                assert!(d < i, "task-set deps must be DAG-ordered (dep {d} >= index {i})");
            }
        }
        let task = QueryTaskLifecycle::with_owner(Arc::new(SchedulerOwner));
        let handle = task.publish().expect("fresh lifecycle publishes once");
        Arc::new(ResourceGroup {
            rg_id,
            query_id: spec.query_id,
            pinned,
            class,
            task,
            handle,
            tasksets: spec.tasksets,
            progress: Mutex::new(RgProgress {
                started: vec![false; n],
                done: vec![false; n],
                aborted: false,
            }),
            completion: Completion::new(),
            stats: RgStats::default(),
            priority: AtomicU32::new(INITIAL_PRIORITY),
            cpu_consumed_ns: AtomicU64::new(0),
            session_token,
            sched,
            submit_ns: AtomicU64::new(0),
            first_service_ns: AtomicU64::new(0),
            done_ns: AtomicU64::new(0),
        })
    }

    /// First not-yet-started task set whose deps have all finalized, marked
    /// started. One task set of an RG is active at a time in M0 (single
    /// slot; bushy parallelism deliberately avoided — 2014 §pipelines).
    pub(crate) fn next_ready(&self, progress: &mut RgProgress) -> Option<usize> {
        for i in 0..self.tasksets.len() {
            if !progress.started[i] && self.tasksets[i].deps.iter().all(|&d| progress.done[d]) {
                progress.started[i] = true;
                return Some(i);
            }
        }
        None
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    pub fn generation(&self) -> Generation {
        self.handle.generation()
    }

    pub fn stats(&self) -> crate::stats::RgStatsSnapshot {
        self.stats.snapshot()
    }

    /// True ⇔ the RG's generation no longer accepts work: the lifecycle word
    /// is closed (abort) or retired. Valid while the RG is live — after
    /// normal completion the lifecycle is retired too, but nothing consults
    /// this then (last_out reads it BEFORE completing; the wait queue only
    /// holds not-yet-started RGs).
    pub(crate) fn is_aborted(&self) -> bool {
        use crate::lifecycle::LifecycleState;
        !matches!(
            self.handle.lifecycle().state(),
            LifecycleState::Armed | LifecycleState::Running
        )
    }

    /// Drain and retire the RG's generation — called exactly once when the
    /// RG leaves the scheduler (last-worker-out completion, or queued-abort
    /// completion). All participants are provably gone by then: last-out is
    /// gated on every pinned worker's settle and participants exist only
    /// inside run_task, so the drain never waits. The lifecycle's recorded
    /// first error (the abort) is deliberately dropped here: the RG's
    /// outcome channel is [`Completion`], not the lifecycle error.
    pub(crate) fn retire_lifecycle(&self) {
        let lifecycle = Arc::clone(self.handle.lifecycle());
        let _ = self
            .task
            .close_generation_and_wait_with(&lifecycle, || false, || Ok(()));
    }
}

/// The submitting leader's completion handle: park on `wait()` and get woken
/// at finalize/abort. Submit-and-park is the ONLY leader interaction (§2.5).
#[derive(Clone)]
pub struct CompletionWaiter {
    pub(crate) rg: Arc<ResourceGroup>,
}

impl CompletionWaiter {
    pub fn wait(&self) -> RgOutcome {
        self.rg.completion.wait()
    }

    pub fn try_wait(&self) -> Option<RgOutcome> {
        self.rg.completion.try_wait()
    }

    /// Register a packed waiter::WakerHandle word to be unparked when the
    /// RG completes, without parking. Returns true ⇔ already complete (no
    /// wake follows — consume `try_wait()`).
    #[cfg(not(loom))]
    pub fn register_waker_word(&self, word: u64) -> bool {
        self.rg.completion.register_waker_word(word)
    }
}

/// Control handle for a submitted RG (abort, stats).
#[derive(Clone)]
pub struct RgHandle {
    pub(crate) rg: Arc<ResourceGroup>,
}

/// Weak RG handle: breaks the `work → RG → tasksets → work` Arc cycle when
/// a TaskSetWork implementation needs an abort path back into its own RG
/// (M1 scan pipelines: the work records its first error and aborts). An
/// upgrade fails only after the submitting leader dropped its handles —
/// at which point nothing executes the work anymore.
#[derive(Clone)]
pub struct WeakRgHandle {
    pub(crate) rg: std::sync::Weak<ResourceGroup>,
}

impl WeakRgHandle {
    pub fn upgrade(&self) -> Option<RgHandle> {
        self.rg.upgrade().map(|rg| RgHandle { rg })
    }
}

impl RgHandle {
    pub fn downgrade(&self) -> WeakRgHandle {
        WeakRgHandle { rg: Arc::downgrade(&self.rg) }
    }

    /// Non-blocking completion probe (the external pinned driver's loop
    /// condition; the submitting leader keeps the CompletionWaiter).
    pub fn try_outcome(&self) -> Option<RgOutcome> {
        self.rg.completion.try_wait()
    }

    /// Abort the RG's generation: cancel closes the lifecycle word, so no
    /// new task may consume it (the fail-closed join refuses — unconsumable
    /// by construction); in-flight morsels drain at their next boundary and
    /// the cleanup rides the ordinary last-worker-out protocol, completing
    /// the RG as Aborted. Idempotent (first recorded error wins; the close
    /// is a no-op on an already-closed word).
    ///
    /// M5-4 slot-reclamation fix: an aborted RG still QUEUED (never admitted
    /// to a slot) must not wait for a slot to free before completing — the
    /// reap removes it from the wait queue and completes it promptly.
    /// Exactly-once with the admission pop: both remove under the membership
    /// lock, and whoever removes the RG is the one that completes it. The
    /// cancel-then-reap order closes the race with admission: an RG popped
    /// after the cancel is observed aborted at the pop (completed there); an
    /// RG popped before it starts normally and aborts through the ordinary
    /// generation-refusal drain.
    pub fn abort(&self) {
        self.rg.task.cancel(abort_error());
        if let Some(sched) = self.rg.sched.upgrade() {
            sched.reap_queued_abort(&self.rg);
        }
    }

    /// Abort observation for work bodies that subdivide a COALESCED claim
    /// (dop1-tax fix 1): checking between epoch segments keeps cancel
    /// latency at epoch grain even when one claim spans several epochs.
    pub fn is_aborted(&self) -> bool {
        self.rg.is_aborted()
    }

    pub fn stats(&self) -> crate::stats::RgStatsSnapshot {
        self.rg.stats.snapshot()
    }

    /// The submitting query's id (WFIN marker correlation key).
    pub fn query_id(&self) -> u64 {
        self.rg.query_id
    }

    /// CPU nanoseconds consumed by this RG's tasks so far — the per-RG CPU
    /// share readback of the multi-query fairness instruments (§3.5; the
    /// proportional-share error of the K-sweep gate is computed from this).
    pub fn cpu_consumed_ns(&self) -> u64 {
        use crate::sync::atomic::Ordering;
        self.rg.cpu_consumed_ns.load(Ordering::Relaxed)
    }

    /// Submit→service instrument channel (§3.5), scheduler-clock ns:
    /// `(submit_ns, first_service_ns, done_ns)`; 0 = not (yet) recorded.
    /// submit→first_service is the time-to-service the MULTI-arm latency
    /// distributions read; submit→done is the completion latency.
    pub fn service_times(&self) -> (u64, u64, u64) {
        use crate::sync::atomic::Ordering;
        (
            self.rg.submit_ns.load(Ordering::Relaxed),
            self.rg.first_service_ns.load(Ordering::Relaxed),
            self.rg.done_ns.load(Ordering::Relaxed),
        )
    }
}
