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

/// M2 inc-2 (PGPROC-leasing pool workers — scratchpad/night/
/// m2-pool-binding-scope.md §3 inc-2): the BOUND-ENGAGEMENT descriptor a
/// pinned RG may carry at submission ([`crate::Runtime::submit_pinned_bound`]).
/// Its presence is the scheduler's BINDABILITY gate: publication sets the
/// pool-visible active bit (a plain pinned RG stays invisible to the pool's
/// pick), and a pool worker whose pick lands on the RG dispatches ONE
/// engagement through `serve` instead of claiming morsels directly — the
/// serve claims a participation ticket from the (opaque) board, brings up /
/// verifies the thread's leased PGPROC identity, binds the query's task
/// binding, drives the RG as an EXTERNAL participant (its own leased
/// pin-board lane) to an outcome, unbinds, and detaches. The runtime never
/// interprets the payload — the parallel/arm layers own the binder and the
/// board (the standing-gang engagement machinery, re-homed per-RG).
#[derive(Clone)]
pub struct BoundDescriptor {
    /// Serve one engagement on the calling pool thread (see above). Runs
    /// WITHOUT the caller's execution permit — the scheduler releases it
    /// around the call because the nested drive has its own permit rhythm
    /// (a held permit here would deadlock at capacity: every worker holding
    /// one while its drive waits for another). Must not unwind except to
    /// KILL the thread (exit-committed FATALs; the pool spawn glue owns the
    /// identity drain + respawn).
    pub serve: fn(&Arc<dyn std::any::Any + Send + Sync>) -> BoundServe,
    /// Opaque engagement board (the parallel crate's per-RG entry).
    pub payload: Arc<dyn std::any::Any + Send + Sync>,
    /// POOL-QOS: the engagement's requested width (the board's ticket
    /// count / dop). Feeds the interactive-demand ledger: a FRESH
    /// (undecayed) bound RG published with width W wants W serves; unmet
    /// width is what demoted serves yield their threads toward. Advisory —
    /// never a correctness input.
    pub width: u32,
}

/// One `BoundDescriptor::serve` call's scheduling verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundServe {
    /// A ticket was claimed and served to completion (bind → drive →
    /// unbind → detach all happened inside the call).
    Served,
    /// THIS worker can never serve THIS engagement (identity bring-up
    /// failure, database-pin mismatch, binder refusal surface): skip the
    /// slot for the rest of the publication.
    Refused,
    /// Board closed or participant cap reached: nothing left to serve —
    /// skip the slot for the rest of the publication.
    Closed,
}

/// RG scheduling class (M4, docs/design/m4-bgjobs.md §3.5; Track-4 QoS,
/// scratchpad/night/pool-qos-design.md §1/§3). Maintenance RGs
/// (background-job cycles) are preferred by the pool's pick over foreground
/// FIFO order, and overtake the wait queue on submission — the minimal
/// starvation floor, deliberately NOT a scheduler: maintenance slots are few
/// (one per due job cycle) and their task sets are single-morsel, so the
/// foreground tax is bounded by one worker × one cycle body. The slot word
/// stays the sole execution authority; the class only orders the pick.
///
/// Utility (Track-4 Q0/Q1) is the BULK-BACKGROUND class — vacuum drivers,
/// index build, COPY compute after their M4 migrations. It is the opposite
/// of Maintenance and must never be conflated with it: no pick preference,
/// no queue overtake. The class is compiled entirely into two existing
/// mechanisms at submit time — (i) the RG's priority word is seeded at the
/// utility weight p_util (default = the ratified p_min floor, p0/16) so the
/// M5-4/M5-5 stride pick gives it the floor's proven ≥1/(1+K·p0/p_util)
/// share (no starvation, no new pick code), and (ii) the WS-B ledger admits
/// it into the capped utility budget tier (soft cap: full fair share on an
/// idle box, min(B_util, cores − Σ foreground targets) otherwise) so
/// foreground occupancy is structurally protected, with reclaim bounded by
/// one claim via the existing should_continue Yield.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RgClass {
    Foreground,
    Maintenance,
    Utility,
}

pub(crate) struct RgProgress {
    pub(crate) started: Vec<bool>,
    pub(crate) done: Vec<bool>,
    pub(crate) aborted: bool,
    /// Published-but-not-finalized task sets (live pipelines). Sequential
    /// walk: 0 or 1 by construction. DAG dispatch (M5+1): the RG completes
    /// only when `live == 0` AND nothing is ready — a finishing pipeline
    /// with live siblings releases its slot without completing the RG.
    pub(crate) live: usize,
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

    pub(crate) fn try_wait(&self) -> Option<RgOutcome> {
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
    /// M2 inc-2 bound-engagement descriptor (implies `pinned`, asserted at
    /// submit): present ⇔ pool workers may claim engagements on this RG
    /// through the descriptor's serve (see [`BoundDescriptor`]). None on
    /// every other submission — byte-identical scheduling.
    pub(crate) bound: Option<BoundDescriptor>,
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
    /// M5-5 LIVE: decays with consumed CPU, `p(q) = max(p_min, p0·λ^q)`,
    /// applied at the task-completion charge site in sched.rs; monotone
    /// non-increasing over the RG's life).
    pub(crate) priority: AtomicU32,
    /// M5-5 decay bookkeeping: last consumed-CPU quantum count the decay
    /// was applied at (CAS-guarded — each boundary applies exactly once
    /// across racing workers).
    pub(crate) decay_quanta: AtomicU64,
    /// CPU nanoseconds consumed by this RG's tasks — the quantity stride
    /// passes advance by (LIVE as of M5-4), and the per-RG CPU-share
    /// readback of the fairness instruments (§3.5).
    pub(crate) cpu_consumed_ns: AtomicU64,
    /// Session-affinity token of the submitting leader (0 = none): the
    /// equal-pass pick tiebreak prefers workers sticky-bound to this session
    /// (M5-4; set via Runtime::submit_with_affinity — QuerySpec deliberately
    /// unchanged for backward compatibility, integration-train note).
    pub(crate) session_token: u64,
    /// Query-level stride account (M5+1 pipeline-DAG dispatch, m5-planner
    /// §3.6): the QUERY is the fair-share principal — under DAG dispatch all
    /// of the RG's live pipeline slots advance THIS account (pass advances
    /// on the SUM of their consumed quanta) and each slot's pass word
    /// mirrors it, so a query with 4 live pipelines gets the same aggregate
    /// share as a single-pipeline peer. Unused (stays 0) in sequential-walk
    /// mode. With ONE pipeline the account's value sequence is identical to
    /// the slot's own fetch_add — single-pipeline scheduling is unchanged
    /// by construction.
    pub(crate) pass_account: AtomicU64,
    /// Dependency depth per task set (M5+1, §3.6): depth[i] = length of the
    /// longest dependency chain of task sets ABOVE i (its downstream
    /// release). Within-query pick priority is deepest-first (ties by
    /// submission = index order). Computed once at construction from the
    /// deps DAG; immutable.
    pub(crate) depth: Vec<u64>,
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
        bound: Option<BoundDescriptor>,
    ) -> Arc<ResourceGroup> {
        let n = spec.tasksets.len();
        for (i, ts) in spec.tasksets.iter().enumerate() {
            for &d in &ts.deps {
                assert!(d < i, "task-set deps must be DAG-ordered (dep {d} >= index {i})");
            }
        }
        // Dependency depth (§3.6): longest chain of dependents above each
        // task set. Descending index order finalizes depth[j] before j is
        // visited as anyone's dep (dependents always have larger indices).
        let mut depth = vec![0u64; n];
        for j in (0..n).rev() {
            for &d in &spec.tasksets[j].deps {
                depth[d] = depth[d].max(depth[j] + 1);
            }
        }
        let task = QueryTaskLifecycle::with_owner(Arc::new(SchedulerOwner));
        let handle = task.publish().expect("fresh lifecycle publishes once");
        Arc::new(ResourceGroup {
            rg_id,
            query_id: spec.query_id,
            pinned,
            class,
            bound,
            task,
            handle,
            tasksets: spec.tasksets,
            progress: Mutex::new(RgProgress {
                started: vec![false; n],
                done: vec![false; n],
                aborted: false,
                live: 0,
            }),
            completion: Completion::new(),
            stats: RgStats::default(),
            priority: AtomicU32::new(INITIAL_PRIORITY),
            decay_quanta: AtomicU64::new(0),
            cpu_consumed_ns: AtomicU64::new(0),
            session_token,
            pass_account: AtomicU64::new(0),
            depth,
            sched,
            submit_ns: AtomicU64::new(0),
            first_service_ns: AtomicU64::new(0),
            done_ns: AtomicU64::new(0),
        })
    }

    /// First not-yet-started task set whose deps have all finalized, marked
    /// started. One task set of an RG is active at a time on the sequential
    /// walk (single slot; the M0 shape — DAG dispatch is the M5+1 opt-in,
    /// see [`ResourceGroup::ready_all`]).
    pub(crate) fn next_ready(&self, progress: &mut RgProgress) -> Option<usize> {
        for i in 0..self.tasksets.len() {
            if !progress.started[i] && self.tasksets[i].deps.iter().all(|&d| progress.done[d]) {
                progress.started[i] = true;
                progress.live += 1;
                return Some(i);
            }
        }
        None
    }

    /// EVERY not-yet-started task set whose deps have all finalized, capped
    /// at `max` (the caller's slot capacity), deepest-first (§3.6
    /// within-query priority; stable sort keeps submission = index order on
    /// ties). Marks the taken ones started; the rest stay unstarted-ready
    /// and are re-found by the next call (a later finalize with freed
    /// capacity). Returns `(taken, deferred)` where `deferred` counts ready
    /// task sets left behind for lack of capacity. M5+1 DAG dispatch only.
    pub(crate) fn ready_all(
        &self,
        progress: &mut RgProgress,
        max: usize,
    ) -> (Vec<usize>, usize) {
        let mut ready: Vec<usize> = (0..self.tasksets.len())
            .filter(|&i| {
                !progress.started[i]
                    && self.tasksets[i].deps.iter().all(|&d| progress.done[d])
            })
            .collect();
        ready.sort_by_key(|&i| std::cmp::Reverse(self.depth[i]));
        let deferred = ready.len().saturating_sub(max);
        ready.truncate(max);
        for &i in &ready {
            progress.started[i] = true;
            progress.live += 1;
        }
        (ready, deferred)
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

    /// Current (possibly decayed) priority — M5-5 instrument readback:
    /// p0 = fresh; p_min = fully-decayed floor. Tests assert the decay
    /// trajectory and the floor clamp through this.
    pub fn priority(&self) -> u32 {
        use crate::sync::atomic::Ordering;
        self.rg.priority.load(Ordering::Relaxed)
    }
}
