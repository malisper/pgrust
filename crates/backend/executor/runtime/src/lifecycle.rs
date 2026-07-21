//! Task lifecycle state machine for the morsel runtime.
//!
//! PROVENANCE: extracted as a DESIGN DONOR from the accepted scan-task
//! lifecycle, `morsel/real-scan-v93-20260710` @ f4f905dd8
//! (`crates/backend/executor/execmain/src/scan_task.rs`, "fix(execmain):
//! linearize scan-task shutdown", qualified 2026-07-10; journal
//! docs/optimizations/2026-07-10-morsel-real-scan-prerequisite.md). The
//! donor's execparallel/Gather wiring (ParallelShared target, pgrcolumnar scan
//! descriptor claims, plan-shape eligibility walk) is deliberately REMOVED —
//! the new runtime's pool/scheduler own that wiring. What is kept, with the
//! donor's semantics intact:
//!
//! - The combined AtomicU64 CAS lifecycle: publication, open/retired flags,
//!   participant count, and operation count live in ONE atomic word so join
//!   and close share a single ARM-safe linearization order (the donor's
//!   join-vs-close P1 fix; the Loom model below reproduces the donor's).
//! - Query-owned generations: every published execution gets an immutable
//!   generation identity; rescan/reinitialize publishes a NEW generation and
//!   stale handles can never join or migrate into it. Aborted/cancelled
//!   generations are unconsumable by construction (the H1 CaseDict-TLS structural
//!   fix: partial state keyed by a generation dies with it).
//! - Armed participant outcomes: a joined participant must explicitly
//!   `complete()` or `fail()`; an unfinished Drop (error unwind, panic,
//!   forgotten handle) records an error and cancels the generation, so owner
//!   teardown can never report success over an abandoned claim.
//! - Fail-closed admission: joining requires a [`ParticipantOwner`] that
//!   affirmatively permits it and owns stop/liveness for drain;
//!   [`ForeignParticipationDisabled`] is the production default until a
//!   dispatcher supplies that ownership.
//! - Interruptible drain: close-and-wait services interrupts on every timed
//!   wake (repeatedly — later cancel/ProcDie must not be swallowed by an
//!   earlier benign service), retains the FIRST error, and wakes waiters only
//!   on a closed transition to idle (no per-operation Condvar broadcast).
//!
//! Everything here is inert until the runtime's pool/scheduler (lane B) and
//! an execution owner arm it; no production caller exists at M0.

// MERGE NOTE (m0-integration): synchronization comes from the crate's
// loom/std shim so lane B's scheduler models (tests/loom.rs) explore the
// lifecycle's own edges too — under `--cfg loom` every CAS, mutex, and
// condvar below is a loom-checked type. `Arc` stays std per the shim's
// rules (sync.rs). Semantics are the donor's, unchanged.
use std::sync::Arc;
use std::time::Duration;

use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Condvar, Mutex};

use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};

/// Identity of one published task execution. Query-owned: the owning query
/// allocates generations monotonically and retires each one before the next
/// is published; shared state keyed by a Generation can never be consumed by
/// a different execution (H1's structural fix).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

impl Generation {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Observable lifecycle states decoded from the packed CAS word. The word is
/// the truth; this enum is a read-side projection for callers and tests.
///
/// Donor mapping: Idle = unpublished; Armed = published+open with no
/// participants; Running = published+open with participants; Draining =
/// closed but not yet retired (participants/operations may still be
/// finishing); Closed = retired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleState {
    Idle,
    Armed,
    Running,
    Draining,
    Closed,
}

/// Stop/liveness ownership for participants. Fail-closed admission: `join`
/// consults `permits_join` BEFORE the CAS, and drain treats
/// `generation_stopped` as the owner's promise that no permitted participant
/// is still running (the donor's dispatcher contract).
pub trait ParticipantOwner: Send + Sync {
    fn permits_join(&self, generation: Generation) -> bool;
    fn request_stop(&self, generation: Generation);
    fn generation_stopped(&self, generation: Generation) -> bool;
}

/// Production default until a dispatcher supplies stop/liveness ownership:
/// no join is permitted, every generation is already stopped. Foreign
/// participation stays impossible, loudly (the donor's
/// `ForeignParticipationDisabled` owner).
pub struct ForeignParticipationDisabled;

impl ParticipantOwner for ForeignParticipationDisabled {
    fn permits_join(&self, _generation: Generation) -> bool {
        false
    }

    fn request_stop(&self, _generation: Generation) {}

    fn generation_stopped(&self, _generation: Generation) -> bool {
        true
    }
}

// One AtomicU64 carries the whole lifecycle so join and close have one
// linearization domain (donor bit layout, verbatim):
//   bit 63 OPEN       — accepting joins/operations
//   bit 62 RETIRED    — drain finished; terminal
//   bit 61 PUBLISHED  — publish happened (0 = Idle)
//   bits 31..60       — participant (join) count
//   bits 0..30        — in-flight operation count
const OPEN: u64 = 1 << 63;
const RETIRED: u64 = 1 << 62;
const PUBLISHED: u64 = 1 << 61;
const ACTIVE_SHIFT: u32 = 31;
const ACTIVE_ONE: u64 = 1 << ACTIVE_SHIFT;
const ACTIVE_MASK: u64 = ((1 << 30) - 1) << ACTIVE_SHIFT;
const OPERATION_MASK: u64 = (1 << ACTIVE_SHIFT) - 1;

fn active_count(state: u64) -> usize {
    ((state & ACTIVE_MASK) >> ACTIVE_SHIFT) as usize
}

fn operation_count(state: u64) -> usize {
    (state & OPERATION_MASK) as usize
}

fn join_transition(state: u64) -> Option<u64> {
    if state & (OPEN | RETIRED | PUBLISHED) != (OPEN | PUBLISHED)
        || state & ACTIVE_MASK == ACTIVE_MASK
    {
        None
    } else {
        Some(state + ACTIVE_ONE)
    }
}

fn operation_transition(state: u64) -> Option<u64> {
    if state & (OPEN | RETIRED | PUBLISHED) != (OPEN | PUBLISHED)
        || state & ACTIVE_MASK == 0
        || state & OPERATION_MASK == OPERATION_MASK
    {
        None
    } else {
        Some(state + 1)
    }
}

/// One generation's lifecycle: the CAS word plus first-error retention and
/// the drain rendezvous (donor `Generation` struct, renamed; the claims
/// counter moved out with the scan wiring).
pub struct TaskLifecycle {
    generation: Generation,
    state: AtomicU64,
    first_error: Mutex<Option<Box<PgError>>>,
    wait_lock: Mutex<()>,
    cv: Condvar,
}

impl TaskLifecycle {
    pub fn new(generation: Generation) -> Arc<Self> {
        Arc::new(Self {
            generation,
            state: AtomicU64::new(0),
            first_error: Mutex::new(None),
            wait_lock: Mutex::new(()),
            cv: Condvar::new(),
        })
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn state(&self) -> LifecycleState {
        let state = self.state.load(Ordering::Acquire);
        if state & PUBLISHED == 0 {
            LifecycleState::Idle
        } else if state & RETIRED != 0 {
            LifecycleState::Closed
        } else if state & OPEN == 0 {
            LifecycleState::Draining
        } else if active_count(state) == 0 {
            LifecycleState::Armed
        } else {
            LifecycleState::Running
        }
    }

    /// First error wins; later reports are dropped (donor `record_error`).
    pub fn record_error(&self, error: Box<PgError>) {
        let mut first = self.first_error.lock().unwrap_or_else(|e| e.into_inner());
        if first.is_none() {
            *first = Some(error);
        }
    }

    /// Idle -> Armed. False if the generation was ever published before
    /// (single-publication rule).
    pub fn arm(&self) -> bool {
        self.state
            .compare_exchange(0, PUBLISHED | OPEN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Clears OPEN (idempotent); marks PUBLISHED so a never-armed lifecycle
    /// closes straight from Idle. Joins and new operations refuse afterward.
    fn close_word(&self) -> u64 {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & PUBLISHED != 0 && state & OPEN == 0 {
                return state;
            }
            let next = (state | PUBLISHED) & !OPEN;
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return next,
                Err(observed) => state = observed,
            }
        }
    }

    fn retire(&self) {
        let previous = self.state.fetch_or(RETIRED, Ordering::AcqRel);
        debug_assert_eq!(previous & OPEN, 0);
    }

    pub fn retired(&self) -> bool {
        self.state.load(Ordering::Acquire) & RETIRED != 0
    }

    fn idle(&self) -> bool {
        self.state.load(Ordering::Acquire) & (ACTIVE_MASK | OPERATION_MASK) == 0
    }

    fn try_join(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let Some(next) = join_transition(state) else {
                return false;
            };
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(observed) => state = observed,
            }
        }
    }

    fn try_enter_operation(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let Some(next) = operation_transition(state) else {
                return false;
            };
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(observed) => state = observed,
            }
        }
    }

    fn leave_operation(&self) {
        let previous = self.state.fetch_sub(1, Ordering::Release);
        debug_assert!(operation_count(previous) > 0);
        debug_assert!(active_count(previous) > 0);
    }

    /// Participant exit. Wakes the drain waiter ONLY on a closed transition
    /// to idle (donor perf-review fix: no broadcast per operation); the
    /// wait_lock bridge makes the count change and the wait atomic on the
    /// closed path.
    fn leave_participant(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            debug_assert!(active_count(state) > 0);
            if state & OPEN == 0 {
                let _wait = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
                let previous = self.state.fetch_sub(ACTIVE_ONE, Ordering::AcqRel);
                debug_assert!(active_count(previous) > 0);
                let next = previous - ACTIVE_ONE;
                if next & (ACTIVE_MASK | OPERATION_MASK) == 0 {
                    self.cv.notify_all();
                }
                return;
            }
            match self.state.compare_exchange_weak(
                state,
                state - ACTIVE_ONE,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => state = observed,
            }
        }
    }
}

/// A query-owned lifecycle slot: rotates generations across publish /
/// reinitialize (rescan) so stale handles can never migrate (donor
/// `QueryScanTask`, minus target/scan wiring).
pub struct QueryTaskLifecycle {
    current: Mutex<Arc<TaskLifecycle>>,
    next_generation: AtomicU64,
    owner: Arc<dyn ParticipantOwner>,
}

/// A cloneable handle onto one published generation (donor `ScanTaskHandle`).
#[derive(Clone)]
pub struct TaskHandle {
    task: Arc<QueryTaskLifecycle>,
    lifecycle: Arc<TaskLifecycle>,
}

/// An armed join. Must end in `complete()` or `fail()`; Drop without either
/// records an error and cancels the generation (donor `ScanParticipant`).
pub struct TaskParticipant {
    handle: TaskHandle,
    armed: bool,
}

struct OperationGuard {
    lifecycle: Arc<TaskLifecycle>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.lifecycle.leave_operation();
    }
}

impl QueryTaskLifecycle {
    /// Production constructor: fail-closed until a dispatcher owner exists.
    pub fn new_fail_closed() -> Arc<Self> {
        Self::with_owner(Arc::new(ForeignParticipationDisabled))
    }

    pub fn with_owner(owner: Arc<dyn ParticipantOwner>) -> Arc<Self> {
        Arc::new(Self {
            current: Mutex::new(TaskLifecycle::new(Generation(1))),
            next_generation: AtomicU64::new(2),
            owner,
        })
    }

    pub fn current(&self) -> Arc<TaskLifecycle> {
        Arc::clone(&self.current.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Publish the current generation (Idle -> Armed), once.
    pub fn publish(self: &Arc<Self>) -> PgResult<TaskHandle> {
        let lifecycle = self.current();
        if !lifecycle.arm() {
            return Err(unsupported("runtime task lifecycle was published twice"));
        }
        Ok(TaskHandle {
            task: Arc::clone(self),
            lifecycle,
        })
    }

    /// Rescan: retire the current generation (draining it if still live) and
    /// publish a NEW one. Old handles keep their retired generation and can
    /// never join the new one.
    ///
    /// Donor residual (recorded loudly there too): callers must retire the
    /// old generation BEFORE resetting any shared claim state the next
    /// generation reuses — safe while participants are impossible; a real
    /// dispatcher must keep that order.
    pub fn reinitialize(self: &Arc<Self>) -> PgResult<TaskHandle> {
        let old = self.current();
        if !old.retired() {
            self.close_generation_and_wait(&old)?;
        }
        let id = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let lifecycle = TaskLifecycle::new(Generation(id));
        *self.current.lock().unwrap_or_else(|e| e.into_inner()) = Arc::clone(&lifecycle);
        assert!(lifecycle.arm());
        Ok(TaskHandle {
            task: Arc::clone(self),
            lifecycle,
        })
    }

    /// Stop accepting joins/operations; participants finish or fail.
    pub fn close(&self) {
        let lifecycle = self.current();
        lifecycle.close_word();
        self.owner.request_stop(lifecycle.generation());
    }

    /// Close with an error recorded (first error wins).
    pub fn cancel(&self, error: Box<PgError>) {
        let lifecycle = self.current();
        self.cancel_generation(&lifecycle, error);
    }

    fn cancel_generation(&self, lifecycle: &TaskLifecycle, error: Box<PgError>) {
        lifecycle.record_error(error);
        lifecycle.close_word();
        self.owner.request_stop(lifecycle.generation());
    }

    /// Close and drain the current generation; returns the first recorded
    /// error, if any. Interrupts are serviced on every timed wake.
    pub fn close_and_wait(&self) -> PgResult<()> {
        let lifecycle = self.current();
        self.close_generation_and_wait(&lifecycle)
    }

    fn close_generation_and_wait(&self, lifecycle: &Arc<TaskLifecycle>) -> PgResult<()> {
        self.close_generation_and_wait_with(
            lifecycle,
            ::init_small::globals::InterruptPending,
            ::postgres_seams::check_for_interrupts::call,
        )
    }

    /// Drain core (donor `close_generation_and_wait_with`, verbatim
    /// semantics): interruptible, re-armed interrupts serviced every wake, a
    /// service error cancels the generation but drain continues until idle
    /// or the owner reports stopped; first error is retained; the generation
    /// retires exactly once at the end.
    pub fn close_generation_and_wait_with(
        &self,
        lifecycle: &Arc<TaskLifecycle>,
        mut interrupt_pending: impl FnMut() -> bool,
        mut service_interrupts: impl FnMut() -> PgResult<()>,
    ) -> PgResult<()> {
        lifecycle.close_word();
        self.owner.request_stop(lifecycle.generation());
        loop {
            let guard = lifecycle
                .wait_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if lifecycle.idle() {
                drop(guard);
                break;
            }
            if self.owner.generation_stopped(lifecycle.generation()) {
                drop(guard);
                lifecycle.record_error(unfinished_error(
                    "runtime task participant owner stopped with unfinished participants",
                ));
                break;
            }
            let (guard, _) = lifecycle
                .cv
                .wait_timeout(guard, Duration::from_millis(10))
                .unwrap_or_else(|e| e.into_inner());
            drop(guard);
            if interrupt_pending() {
                if let Err(error) = service_interrupts() {
                    lifecycle.record_error(error);
                    self.owner.request_stop(lifecycle.generation());
                }
            }
        }
        lifecycle.retire();
        match lifecycle
            .first_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl TaskHandle {
    pub fn generation(&self) -> Generation {
        self.lifecycle.generation()
    }

    pub fn lifecycle(&self) -> &Arc<TaskLifecycle> {
        &self.lifecycle
    }

    /// Fail-closed join: the owner must permit it AND the CAS must find the
    /// generation open (Armed/Running) with join headroom.
    pub fn join(&self) -> PgResult<TaskParticipant> {
        if !self.task.owner.permits_join(self.lifecycle.generation()) {
            return Err(unsupported(
                "runtime task lifecycle requires a dispatcher-owned participant shutdown protocol",
            ));
        }
        if !self.lifecycle.try_join() {
            return Err(unsupported("runtime task lifecycle is not joinable"));
        }
        Ok(TaskParticipant {
            handle: self.clone(),
            armed: true,
        })
    }
}

impl TaskParticipant {
    fn enter_operation(&self) -> PgResult<OperationGuard> {
        let lifecycle = &self.handle.lifecycle;
        if !lifecycle.try_enter_operation() {
            return Err(unsupported(
                "runtime task lifecycle closed while participant was active",
            ));
        }
        Ok(OperationGuard {
            lifecycle: Arc::clone(lifecycle),
        })
    }

    pub fn generation(&self) -> Generation {
        self.handle.lifecycle.generation()
    }

    /// Run one unit of work under the operation count. The claim source
    /// itself (scan cursor, morsel queue, ...) is the caller's; the donor's
    /// pgrcolumnar row-group cursor claim lived exactly here.
    pub fn run<T>(&self, f: impl FnOnce() -> PgResult<T>) -> PgResult<T> {
        let _operation = self.enter_operation()?;
        f()
    }

    /// Successful outcome. Refuses if the generation was already retired
    /// (stale completion must not report success).
    pub fn complete(mut self) -> PgResult<()> {
        if self.handle.lifecycle.retired() {
            return Err(unsupported(
                "stale runtime task participant completed after retirement",
            ));
        }
        self.armed = false;
        Ok(())
    }

    /// Failed outcome: records the error and cancels the generation.
    pub fn fail(mut self, error: Box<PgError>) {
        self.handle
            .task
            .cancel_generation(&self.handle.lifecycle, error);
        self.armed = false;
    }
}

impl Drop for TaskParticipant {
    fn drop(&mut self) {
        if self.armed {
            let message = if std::thread::panicking() {
                "runtime task participant panicked before completion"
            } else {
                "runtime task participant dropped before completion"
            };
            self.handle
                .task
                .cancel_generation(&self.handle.lifecycle, unfinished_error(message));
        }
        self.handle.lifecycle.leave_participant();
    }
}

fn unsupported(message: &'static str) -> Box<PgError> {
    PgError::new(ERROR, message)
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into()
}

fn unfinished_error(message: &'static str) -> Box<PgError> {
    PgError::new(ERROR, message).into()
}

// Unit tests run under plain `cargo test` (shim = std there). Under
// `--cfg loom` they are compiled out: they use real threads and timed waits
// outside loom::model, which loom types forbid. The donor loom model below
// (`loom_join_and_close_share_one_linearization_domain`) builds its own loom
// atomics and runs as a PLAIN test via the unconditional loom dev-dependency
// — exactly how lane A's gates ran it.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64 as StdAtomicU64, AtomicUsize};

    struct TestOwner {
        stopped: AtomicBool,
        stop_requests: AtomicUsize,
        stop_on_request: bool,
    }

    impl ParticipantOwner for TestOwner {
        fn permits_join(&self, _generation: Generation) -> bool {
            true
        }

        fn request_stop(&self, _generation: Generation) {
            self.stop_requests.fetch_add(1, Ordering::Relaxed);
            if self.stop_on_request {
                self.stopped.store(true, Ordering::Release);
            }
        }

        fn generation_stopped(&self, _generation: Generation) -> bool {
            self.stopped.load(Ordering::Acquire)
        }
    }

    fn make_task_with_owner(
        stop_on_request: bool,
    ) -> (Arc<QueryTaskLifecycle>, TaskHandle, Arc<TestOwner>) {
        let owner = Arc::new(TestOwner {
            stopped: AtomicBool::new(false),
            stop_requests: AtomicUsize::new(0),
            stop_on_request,
        });
        let task = QueryTaskLifecycle::with_owner(owner.clone());
        let handle = task.publish().unwrap();
        (task, handle, owner)
    }

    fn make_task(stop_on_request: bool) -> (Arc<QueryTaskLifecycle>, TaskHandle) {
        let (task, handle, _) = make_task_with_owner(stop_on_request);
        (task, handle)
    }

    #[test]
    fn state_projection_follows_the_word() {
        let lifecycle = TaskLifecycle::new(Generation(7));
        assert_eq!(lifecycle.state(), LifecycleState::Idle);
        assert!(lifecycle.arm());
        assert_eq!(lifecycle.state(), LifecycleState::Armed);
        assert!(!lifecycle.arm(), "single publication");
        assert!(lifecycle.try_join());
        assert_eq!(lifecycle.state(), LifecycleState::Running);
        lifecycle.close_word();
        assert_eq!(lifecycle.state(), LifecycleState::Draining);
        lifecycle.leave_participant();
        lifecycle.retire();
        assert_eq!(lifecycle.state(), LifecycleState::Closed);
        assert!(!lifecycle.try_join(), "closed lifecycle refuses joins");
    }

    #[test]
    fn real_participants_claim_and_complete() {
        let (task, handle) = make_task(false);
        let cursor = Arc::new(StdAtomicU64::new(0));
        let mut claimed = std::thread::scope(|scope| {
            let mut threads = Vec::new();
            for _ in 0..8 {
                let handle = handle.clone();
                let cursor = Arc::clone(&cursor);
                threads.push(scope.spawn(move || {
                    let participant = handle.join().unwrap();
                    let rows = (0..32)
                        .map(|_| {
                            participant
                                .run(|| Ok(cursor.fetch_add(1, Ordering::SeqCst)))
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    participant.complete().unwrap();
                    rows
                }));
            }
            threads
                .into_iter()
                .flat_map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        task.close_and_wait().unwrap();
        claimed.sort_unstable();
        assert_eq!(claimed, (0..256).collect::<Vec<_>>());
    }

    #[test]
    fn unfinished_drop_and_explicit_failure_surface() {
        let (task, handle) = make_task(false);
        drop(handle.join().unwrap());
        let error = task.close_and_wait().unwrap_err();
        assert_eq!(
            error.message(),
            "runtime task participant dropped before completion"
        );

        let (task, handle) = make_task(false);
        handle
            .join()
            .unwrap()
            .fail(PgError::new(ERROR, "explicit failure").into());
        let error = task.close_and_wait().unwrap_err();
        assert_eq!(error.message(), "explicit failure");
    }

    #[test]
    fn panic_boundary_records_failure() {
        let (task, handle) = make_task(false);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _participant = handle.join().unwrap();
            panic!("participant panic");
        }));
        let error = task.close_and_wait().unwrap_err();
        assert_eq!(
            error.message(),
            "runtime task participant panicked before completion"
        );
    }

    #[test]
    fn stopped_owner_retires_stalled_participant_without_releasing_operations() {
        let (task, handle) = make_task(true);
        let participant = handle.join().unwrap();
        let error = task.close_and_wait().unwrap_err();
        assert!(error.message().contains("owner stopped with unfinished"));
        assert!(participant.run(|| Ok(())).is_err());
        drop(participant);
    }

    #[test]
    fn stale_handle_cannot_join_new_generation() {
        let (task, stale) = make_task(false);
        task.close_and_wait().unwrap();
        assert!(task.publish().is_err());
        let current = task.reinitialize().unwrap();
        assert_ne!(stale.generation(), current.generation());
        assert!(!Arc::ptr_eq(stale.lifecycle(), current.lifecycle()));
        assert!(stale.lifecycle().retired());
        assert!(stale.join().is_err());
        let participant = current.join().unwrap();
        participant.complete().unwrap();
        task.close_and_wait().unwrap();
    }

    #[test]
    fn repeated_interrupt_service_records_later_cancellation() {
        let (task, handle, owner) = make_task_with_owner(false);
        let lifecycle = Arc::clone(handle.lifecycle());
        let mut participant = Some(handle.join().unwrap());
        let mut calls = 0;
        let error = task
            .close_generation_and_wait_with(
                &lifecycle,
                || true,
                || {
                    calls += 1;
                    match calls {
                        1 => Ok(()),
                        2 => Err(PgError::new(ERROR, "re-armed cancellation").into()),
                        _ => {
                            participant.take().unwrap().complete().unwrap();
                            Err(PgError::new(ERROR, "later termination").into())
                        }
                    }
                },
            )
            .unwrap_err();
        assert_eq!(calls, 3);
        assert_eq!(owner.stop_requests.load(Ordering::Relaxed), 3);
        assert_eq!(error.message(), "re-armed cancellation");
        assert!(lifecycle.idle());
        assert!(lifecycle.retired());
    }

    #[test]
    fn repeated_operations_restore_count_without_wake_state() {
        let (task, handle) = make_task(false);
        let participant = handle.join().unwrap();
        for _ in 0..1024 {
            participant.run(|| Ok(())).unwrap();
            assert_eq!(
                handle.lifecycle().state.load(Ordering::Acquire),
                PUBLISHED | OPEN | ACTIVE_ONE
            );
        }
        participant.complete().unwrap();
        task.close_and_wait().unwrap();
    }

    // Donor Loom model (f4f905dd8): join and close race on the ONE packed
    // word; every accepted join must be visible to close's observed word.
    #[test]
    fn loom_join_and_close_share_one_linearization_domain() {
        loom::model(|| {
            use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
            use loom::sync::Arc;

            let state = Arc::new(AtomicU64::new(PUBLISHED | OPEN));
            let joined = Arc::new(AtomicBool::new(false));
            let close_observed = Arc::new(AtomicU64::new(u64::MAX));

            let join_state = Arc::clone(&state);
            let join_result = Arc::clone(&joined);
            let join = loom::thread::spawn(move || {
                let mut observed = join_state.load(Ordering::Acquire);
                loop {
                    let Some(next) = join_transition(observed) else {
                        return;
                    };
                    match join_state.compare_exchange_weak(
                        observed,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            join_result.store(true, Ordering::Release);
                            return;
                        }
                        Err(actual) => observed = actual,
                    }
                }
            });

            let close_state = Arc::clone(&state);
            let close_result = Arc::clone(&close_observed);
            let close = loom::thread::spawn(move || {
                let mut observed = close_state.load(Ordering::Acquire);
                loop {
                    let next = observed & !OPEN;
                    match close_state.compare_exchange_weak(
                        observed,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            close_result.store(active_count(next) as u64, Ordering::Release);
                            return;
                        }
                        Err(actual) => observed = actual,
                    }
                }
            });

            join.join().unwrap();
            close.join().unwrap();
            let final_state = state.load(Ordering::Acquire);
            let did_join = joined.load(Ordering::Acquire);
            assert_eq!(final_state & OPEN, 0);
            assert_eq!(active_count(final_state), usize::from(did_join));
            assert_eq!(
                close_observed.load(Ordering::Acquire) as usize,
                active_count(final_state)
            );
        });
    }

    #[test]
    fn close_and_operation_race_has_one_linearization_order() {
        for _ in 0..128 {
            let (task, handle) = make_task(false);
            let participant = handle.join().unwrap();
            let cursor = Arc::new(StdAtomicU64::new(0));
            let worker_cursor = Arc::clone(&cursor);
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let worker_barrier = Arc::clone(&barrier);
            let worker = std::thread::spawn(move || {
                worker_barrier.wait();
                let result =
                    participant.run(|| Ok(worker_cursor.fetch_add(1, Ordering::SeqCst)));
                participant.complete().unwrap();
                result
            });
            barrier.wait();
            task.close_and_wait().unwrap();
            let result = worker.join().unwrap();
            assert_eq!(cursor.load(Ordering::SeqCst), u64::from(result.is_ok()));
            assert!(handle.join().is_err());
        }
    }

    #[test]
    fn production_owner_refuses_foreign_participants_loudly() {
        let task = QueryTaskLifecycle::new_fail_closed();
        let handle = task.publish().unwrap();
        let error = handle.join().err().expect("production join must refuse");
        assert!(error
            .message()
            .contains("dispatcher-owned participant shutdown protocol"));
        task.close_and_wait().unwrap();
    }
}
