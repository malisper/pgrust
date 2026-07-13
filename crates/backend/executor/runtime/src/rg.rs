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
use crate::sync::{lock, Condvar, Mutex};

use types_error::{PgError, ERROR};

/// Umbra's initial priority p_0 = 10^4 (SIGMOD'21 §3.2). INERT in M0.
pub const INITIAL_PRIORITY: u32 = 10_000;

/// The work body of one pipeline's task set. M0 exercises this with
/// synthetic implementations; M1 plugs lane pipelines in
/// (fork → accept_local → combine → finalize maps onto run_morsel/finalize
/// at this altitude — the sink contract lives above this trait).
pub trait TaskSetWork: Send + Sync {
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

pub(crate) struct RgProgress {
    pub(crate) started: Vec<bool>,
    pub(crate) done: Vec<bool>,
    pub(crate) aborted: bool,
}

pub(crate) struct Completion {
    state: Mutex<Option<RgOutcome>>,
    cv: Condvar,
}

impl Completion {
    fn new() -> Self {
        Completion { state: Mutex::new(None), cv: Condvar::new() }
    }

    pub(crate) fn complete(&self, outcome: RgOutcome) {
        let mut g = lock(&self.state);
        debug_assert!(g.is_none(), "resource group completed twice");
        *g = Some(outcome);
        drop(g);
        self.cv.notify_all();
    }

    fn wait(&self) -> RgOutcome {
        let mut g = lock(&self.state);
        loop {
            if let Some(outcome) = *g {
                return outcome;
            }
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn try_wait(&self) -> Option<RgOutcome> {
        *lock(&self.state)
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
    /// INERT until M5 (stride activation): decaying priority seed. Present
    /// so per-task bookkeeping never needs restructuring (lit-review §5.4).
    pub(crate) priority: AtomicU32,
    /// INERT until M5: CPU nanoseconds consumed by this RG's tasks (the
    /// quantity stride passes advance by).
    pub(crate) cpu_consumed_ns: AtomicU64,
}

impl ResourceGroup {
    pub(crate) fn new(rg_id: u64, spec: QuerySpec) -> Arc<ResourceGroup> {
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
}

/// Control handle for a submitted RG (abort, stats).
#[derive(Clone)]
pub struct RgHandle {
    pub(crate) rg: Arc<ResourceGroup>,
}

impl RgHandle {
    /// Abort the RG's generation: cancel closes the lifecycle word, so no
    /// new task may consume it (the fail-closed join refuses — unconsumable
    /// by construction); in-flight morsels drain at their next boundary and
    /// the cleanup rides the ordinary last-worker-out protocol, completing
    /// the RG as Aborted. Idempotent (first recorded error wins; the close
    /// is a no-op on an already-closed word).
    pub fn abort(&self) {
        self.rg.task.cancel(abort_error());
    }

    pub fn stats(&self) -> crate::stats::RgStatsSnapshot {
        self.rg.stats.snapshot()
    }
}
