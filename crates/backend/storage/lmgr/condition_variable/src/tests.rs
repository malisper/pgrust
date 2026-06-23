//! Unit tests for the condition-variable port.
//!
//! The seams stand in for the `PGPROC` array (each backend's `cvWaitLink`
//! node and process latch), the backend identity, this backend's `MyLatch`,
//! and interrupt servicing, so the in-crate algorithm — the
//! prepare/sleep/cancel state machine and the sentinel-based broadcast loop —
//! runs against real proclist semantics.

use super::*;

use std::collections::BTreeMap;
use std::sync::{Mutex, Once, OnceLock};

/// Simulated `PGPROC` array state: `cvWaitLink` per procno plus a count of
/// `SetLatch(&proc->procLatch)` calls, and this backend's procno.
#[derive(Default)]
struct Sim {
    nodes: BTreeMap<ProcNumber, proclist_node>,
    latch_sets: BTreeMap<ProcNumber, u32>,
    my_procno: ProcNumber,
}

fn sim() -> &'static Mutex<Sim> {
    static SIM: OnceLock<Mutex<Sim>> = OnceLock::new();
    SIM.get_or_init(|| Mutex::new(Sim::default()))
}

/// Install all fake seam implementations exactly once per process.
fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        proc_seams::proc_cv_wait_link::set(|procno| {
            sim().lock().unwrap().nodes.get(&procno).copied().unwrap_or_default()
        });
        proc_seams::set_proc_cv_wait_link::set(|procno, node| {
            sim().lock().unwrap().nodes.insert(procno, node);
        });
        proc_seams::set_proc_latch::set(|procno| {
            let mut s = sim().lock().unwrap();
            *s.latch_sets.entry(procno).or_insert(0) += 1;
        });
        my_proc_number::set(|| sim().lock().unwrap().my_procno);
        // The resolution seam: reconstruct the live CV from the recorded
        // identity. In these single-thread tests the prepared CV is alive on
        // the stack at the recorded address, exactly the C invariant.
        cv_seams::with_target_cv::set(with_target_cv);
        // The latch never gets set in these single-backend tests, so a wait
        // simply burns (a slice of) its timeout, like a WL_TIMEOUT return.
        latch_seams::wait_latch_my_latch::set(|_events, timeout, _wei| {
            if timeout >= 0 {
                std::thread::sleep(std::time::Duration::from_millis(timeout.clamp(0, 10) as u64));
            }
            Ok(WL_TIMEOUT)
        });
        latch_seams::reset_latch_my_latch::set(|| {});
        check_for_interrupts::set(|| Ok(()));
    });
}

/// Per-test fixture: serialize tests (shared `Sim`), install seams, reset
/// the simulated state and this thread's `cv_sleep_target`.
struct Fixture {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl Fixture {
    fn new(my_procno: ProcNumber) -> Self {
        static TEST_LOCK: Mutex<()> = Mutex::new(());
        let guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        install_seams();
        {
            let mut s = sim().lock().unwrap();
            *s = Sim::default();
            s.my_procno = my_procno;
        }
        set_sleep_target(None);
        Fixture { _guard: guard }
    }
}

/// Run `body` over the CV's wakeup head/tail with exclusive access (tests are
/// single-threaded under `TEST_LOCK`, so the spinlock is not contended). Used
/// by helpers that fabricate or inspect list state directly.
fn with_wakeup<R>(cv: &ConditionVariable, body: impl FnOnce(&mut proclist_head) -> R) -> R {
    // SAFETY: tests serialize on `TEST_LOCK`, so there is no concurrent access
    // to this CV's wakeup list.
    body(unsafe { &mut *cv.wakeup.ptr() })
}

/// Enqueue a fabricated waiter (not the running backend) onto a CV, exactly
/// as that backend's own `ConditionVariablePrepareToSleep` would.
fn enqueue_waiter(cv: &ConditionVariable, procno: ProcNumber) {
    with_wakeup(cv, |wakeup| proclist_push_tail(wakeup, procno));
}

fn latch_count(procno: ProcNumber) -> u32 {
    sim().lock().unwrap().latch_sets.get(&procno).copied().unwrap_or(0)
}

#[test]
fn init_clears_lock_and_empties_queue() {
    let _f = Fixture::new(7);
    let cv = ConditionVariable::new();
    cv.mutex.tas();
    with_wakeup(&cv, |w| {
        w.head = 3;
        w.tail = 3;
    });

    ConditionVariableInit(&cv);
    assert!(cv.mutex.is_free());
    assert_eq!(cv.wakeup.get().head, INVALID_PROC_NUMBER);
    assert_eq!(cv.wakeup.get().tail, INVALID_PROC_NUMBER);
}

#[test]
fn prepare_enqueues_self_and_sets_target() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv);
    assert!(sleep_target() == Some(cv_identity(&cv)));
    // Backend 5 is now the sole waiter, and the spinlock was released.
    assert_eq!(cv.wakeup.get().head, 5);
    assert_eq!(cv.wakeup.get().tail, 5);
    assert!(cv.mutex.is_free());
}

#[test]
fn prepare_on_second_cv_cancels_first() {
    let _f = Fixture::new(5);
    let cv1 = ConditionVariable::new();
    let cv2 = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv1);
    ConditionVariablePrepareToSleep(&cv2);
    assert!(sleep_target() == Some(cv_identity(&cv2)));
    // Backend 5 was removed from cv1's list by the cancel.
    assert_eq!(cv1.wakeup.get().head, INVALID_PROC_NUMBER);
    assert_eq!(cv2.wakeup.get().head, 5);
}

#[test]
fn timed_sleep_first_call_only_prepares() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    // No prior prepare: first TimedSleep prepares and returns false (not a
    // timeout), exactly like the C "tested twice" path.
    let timed_out = ConditionVariableTimedSleep(&cv, 50, 0).unwrap();
    assert!(!timed_out);
    assert!(sleep_target() == Some(cv_identity(&cv)));
    assert_eq!(cv.wakeup.get().head, 5);
}

#[test]
fn timed_sleep_returns_false_when_signaled() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv);
    // Simulate a signal: pop backend 5 out of the list (it is no longer
    // "contained"), so the sleep loop sees `done` and re-queues us.
    assert_eq!(with_wakeup(&cv, |w| proclist_pop_head_node(w)), 5);

    let timed_out = ConditionVariableTimedSleep(&cv, 50, 0).unwrap();
    assert!(!timed_out, "signaled wake must report not-timed-out");
    // We were put back into the wait list while the caller re-checks.
    assert_eq!(cv.wakeup.get().head, 5);
}

#[test]
fn timed_sleep_reports_timeout_when_deadline_passes() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv);
    // Backend 5 stays in the list (no signal), so each loop iteration is a
    // spurious wakeup; the latch fake burns the timeout in real time and the
    // recompute eventually crosses the 5ms deadline.
    let timed_out = ConditionVariableTimedSleep(&cv, 5, 0).unwrap();
    assert!(timed_out);
}

#[test]
fn signal_wakes_oldest_waiter() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    enqueue_waiter(&cv, 11);
    enqueue_waiter(&cv, 12);

    ConditionVariableSignal(&cv);
    // Oldest (11) is popped and latched; 12 remains.
    assert_eq!(latch_count(11), 1);
    assert_eq!(latch_count(12), 0);
    assert_eq!(cv.wakeup.get().head, 12);
    assert_eq!(cv.wakeup.get().tail, 12);
}

#[test]
fn signal_on_empty_queue_is_noop() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();
    ConditionVariableSignal(&cv);
    assert!(proclist_is_empty(&cv.wakeup.get()));
}

#[test]
fn broadcast_wakes_all_present_waiters() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    enqueue_waiter(&cv, 11);
    enqueue_waiter(&cv, 12);
    enqueue_waiter(&cv, 13);

    ConditionVariableBroadcast(&cv);

    // All three waiters latched exactly once; the running backend (5, the
    // sentinel) is never latched.
    assert_eq!(latch_count(11), 1);
    assert_eq!(latch_count(12), 1);
    assert_eq!(latch_count(13), 1);
    assert_eq!(latch_count(5), 0);
    assert!(proclist_is_empty(&cv.wakeup.get()));
}

#[test]
fn broadcast_single_waiter_no_sentinel() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    enqueue_waiter(&cv, 11);
    ConditionVariableBroadcast(&cv);
    assert_eq!(latch_count(11), 1);
    assert!(proclist_is_empty(&cv.wakeup.get()));
}

#[test]
fn broadcast_empty_queue_is_noop() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();
    ConditionVariableBroadcast(&cv);
    assert!(proclist_is_empty(&cv.wakeup.get()));
}

#[test]
fn broadcast_cancels_own_prepared_sleep_first() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv);
    enqueue_waiter(&cv, 11);

    ConditionVariableBroadcast(&cv);
    // Our own prepared sleep was cancelled (we are not latched), the other
    // waiter was wakened, and the target is cleared.
    assert!(sleep_target().is_none());
    assert_eq!(latch_count(5), 0);
    assert_eq!(latch_count(11), 1);
    assert!(proclist_is_empty(&cv.wakeup.get()));
}

#[test]
fn cancel_with_no_pending_sleep_returns_false() {
    let _f = Fixture::new(5);
    assert!(!ConditionVariableCancelSleep());
}

#[test]
fn cancel_after_prepare_dequeues_and_reports_unsignaled() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv);

    let signaled = ConditionVariableCancelSleep();
    assert!(!signaled, "still in the list => not signaled");
    assert!(sleep_target().is_none());
    assert_eq!(cv.wakeup.get().head, INVALID_PROC_NUMBER);
}

#[test]
fn cancel_reports_signaled_when_already_removed() {
    let _f = Fixture::new(5);
    let cv = ConditionVariable::new();

    ConditionVariablePrepareToSleep(&cv);
    // Simulate a signal having removed backend 5 from the list before cancel.
    assert_eq!(with_wakeup(&cv, |w| proclist_pop_head_node(w)), 5);

    let signaled = ConditionVariableCancelSleep();
    assert!(signaled, "removed-by-signal => CancelSleep reports signaled");
    assert!(sleep_target().is_none());
}
