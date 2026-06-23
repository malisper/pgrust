//! Condition variables (`storage/lmgr/condition_variable.c`).
//!
//! Condition variables let one process wait until a specific condition occurs
//! without knowing the identity of the process it is waiting for. Unlike
//! LWLock waits, CV waits are interruptible, and because a CV stores no
//! pointers (only a spinlock and a `proclist_head` whose links are `PGPROC`
//! indices) it is safe to place inside a dynamic shared memory segment.
//!
//! The shared `ConditionVariable` data shape lives in `types-condvar`. Each
//! `PGPROC`'s `cvWaitLink` node and process latch are reached through the
//! proc owner's seams (`backend-storage-lmgr-proc-seams`); this backend's
//! `MyLatch` wait/reset goes through the latch owner's seams.

#![allow(non_snake_case)]

use core::cell::Cell;

use latch_seams as latch_seams;
use condition_variable_seams as cv_seams;
use lmgr_proc_seams as proc_seams;
use s_lock::{s_init_lock, s_lock_macro, Spinlock};
use ::postgres_seams::check_for_interrupts;
use ::init_small_seams::my_proc_number;
use ::instr_time::instr_time_set_current;
use ::condvar::ConditionVariable;
use ::types_core::instrument::instr_time;
use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use ::types_error::PgResult;
use ::types_storage::storage::{proclist_head, proclist_node};
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

/// Identity of a `ConditionVariable`, standing in for the C pointer value held
/// in `cv_sleep_target`. Two references compare equal iff they refer to the
/// same object, exactly like comparing the C pointers. The value is only ever
/// compared (here) or resolved by the `with_target_cv` seam; it is never
/// dereferenced in this crate and never arithmetic'd.
type CvIdentity = usize;

fn cv_identity(cv: &ConditionVariable) -> CvIdentity {
    // Address-based identity, mirroring C's `cv_sleep_target == cv` pointer
    // comparison. `from_ref` yields the address without writing a raw-pointer
    // type in the surface.
    core::ptr::from_ref(cv) as CvIdentity
}

thread_local! {
    /// `static ConditionVariable *cv_sleep_target = NULL;`
    ///
    /// `None` means "not prepared to sleep on any condition variable". This is
    /// per-backend (process-local in C) state, hence a thread-local. We record
    /// only the prepared CV's *identity* (its address), never a borrow or raw
    /// pointer we deref: all the C identity comparisons (`cv_sleep_target ==
    /// cv`, `cv_sleep_target != NULL`) become comparisons of this recorded
    /// `CvIdentity`. `ConditionVariableCancelSleep` reaches the live CV behind
    /// the recorded identity through the `with_target_cv` resolution seam.
    static CV_SLEEP_TARGET: Cell<Option<CvIdentity>> = const { Cell::new(None) };
}

fn sleep_target() -> Option<CvIdentity> {
    CV_SLEEP_TARGET.with(Cell::get)
}

fn set_sleep_target(target: Option<CvIdentity>) {
    CV_SLEEP_TARGET.with(|slot| slot.set(target));
}

/// `SpinLockAcquire(&cv->mutex)` returning a release-on-`Drop` guard
/// (`SpinLockRelease` is the drop). `func` stands in for the C call site's
/// `__func__` in the stuck-spinlock report.
struct SpinLockGuard<'a>(&'a Spinlock);

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        self.0.unlock();
    }
}

fn spin_lock_acquire<'a>(lock: &'a Spinlock, func: &'static str) -> SpinLockGuard<'a> {
    s_lock_macro(lock, Some(file!()), line!() as i32, Some(func));
    SpinLockGuard(lock)
}

/// Acquire `cv->mutex`, run `body` over the `&mut proclist_head` the lock now
/// exclusively protects, and release the lock on return (the guard's `Drop`).
///
/// This is the single place a `&mut proclist_head` is produced from a shared
/// `&ConditionVariable`, and it is sound for exactly the reason C is: the
/// `wakeup` list lives behind a `CvWakeupList` (`UnsafeCell`) and is mutated
/// only while `cv->mutex` is held, so holding the spinlock for the body's
/// duration gives unique access. No `&mut`-over-`&` reference cast is involved
/// — the `&mut` is derived through the cell's `ptr()`, the established
/// `LWLockWaitList` idiom.
fn with_wakeup_locked<R>(
    cv: &ConditionVariable,
    func: &'static str,
    body: impl FnOnce(&mut proclist_head) -> R,
) -> R {
    let _guard = spin_lock_acquire(&cv.mutex, func);
    // SAFETY: `_guard` holds `cv->mutex` for the whole of `body`, which is the
    // exclusion the wakeup-list protocol requires; the pointer comes from the
    // CV's own `UnsafeCell` and is valid and uniquely accessible here.
    let wakeup = unsafe { &mut *cv.wakeup.ptr() };
    body(wakeup)
}

// ---------------------------------------------------------------------------
// proclist helpers (storage/proclist.h) specialized to `cvWaitLink`, 1:1 with
// the `proclist_*_offset` inline helpers. Each PGPROC's `cvWaitLink` node is
// read/written through the proc seams, as the C
// `proclist_node_get(procno, offsetof(PGPROC, cvWaitLink))` macro does.
// ---------------------------------------------------------------------------

fn proclist_init(list: &mut proclist_head) {
    list.head = INVALID_PROC_NUMBER;
    list.tail = INVALID_PROC_NUMBER;
}

fn proclist_is_empty(list: &proclist_head) -> bool {
    list.head == INVALID_PROC_NUMBER
}

fn proclist_push_tail(list: &mut proclist_head, procno: ProcNumber) {
    let mut node = proc_seams::proc_cv_wait_link::call(procno);
    debug_assert!(node.next == 0 && node.prev == 0);

    if list.tail == INVALID_PROC_NUMBER {
        debug_assert!(list.head == INVALID_PROC_NUMBER);
        node.prev = INVALID_PROC_NUMBER;
        list.head = procno;
    } else {
        node.prev = list.tail;
        debug_assert!(node.prev != INVALID_PROC_NUMBER);
        let mut tail_node = proc_seams::proc_cv_wait_link::call(node.prev);
        tail_node.next = procno;
        proc_seams::set_proc_cv_wait_link::call(node.prev, tail_node);
    }

    node.next = INVALID_PROC_NUMBER;
    list.tail = procno;
    proc_seams::set_proc_cv_wait_link::call(procno, node);
}

fn proclist_delete(list: &mut proclist_head, procno: ProcNumber) {
    let node = proc_seams::proc_cv_wait_link::call(procno);

    if node.prev == INVALID_PROC_NUMBER {
        list.head = node.next;
    } else {
        let mut prev_node = proc_seams::proc_cv_wait_link::call(node.prev);
        prev_node.next = node.next;
        proc_seams::set_proc_cv_wait_link::call(node.prev, prev_node);
    }

    if node.next == INVALID_PROC_NUMBER {
        list.tail = node.prev;
    } else {
        let mut next_node = proc_seams::proc_cv_wait_link::call(node.next);
        next_node.prev = node.prev;
        proc_seams::set_proc_cv_wait_link::call(node.next, next_node);
    }

    // mark as if not in a list, for debugging
    proc_seams::set_proc_cv_wait_link::call(procno, proclist_node { next: 0, prev: 0 });
}

/// `proclist_contains` — a node not in any list has `next == prev == 0`; if
/// either link is set it must in fact be in this list (verified in O(1) for
/// the head/tail positions, exactly like the C asserts).
fn proclist_contains(list: &proclist_head, procno: ProcNumber) -> bool {
    let node = proc_seams::proc_cv_wait_link::call(procno);

    if node.prev == 0 && node.next == 0 {
        return false;
    }

    debug_assert!(list.head != procno || node.prev == INVALID_PROC_NUMBER);
    debug_assert!(list.tail != procno || node.next == INVALID_PROC_NUMBER);
    true
}

/// `proclist_pop_head_node` — remove and return the head of the list, which
/// must not be empty. (C returns the containing `PGPROC *`; here the
/// pgprocno names it.)
fn proclist_pop_head_node(list: &mut proclist_head) -> ProcNumber {
    debug_assert!(!proclist_is_empty(list));
    let procno = list.head;
    proclist_delete(list, procno);
    procno
}

/// `ConditionVariableInit` — initialize a condition variable.
///
/// Per the C contract this runs before the CV is published to any other
/// backend (shmem initialization).
pub fn ConditionVariableInit(cv: &ConditionVariable) {
    s_init_lock(&cv.mutex);
    // `proclist_init` runs before the CV is published to any other backend, so
    // there is no contention; we still reach the head through the cell's
    // pointer rather than any reference cast. SAFETY: single-threaded
    // initialization, unique access by the C contract.
    proclist_init(unsafe { &mut *cv.wakeup.ptr() });
}

/// `ConditionVariablePrepareToSleep` — prepare to wait on a given condition
/// variable.
///
/// This can optionally be called before entering a test/sleep loop: doing so
/// is more efficient if we'll need to sleep at least once, while omitting it
/// is more efficient when the first test of the exit condition is likely to
/// succeed. Caution: "before entering the loop" means you *must* test the
/// exit condition between calling this and calling `ConditionVariableSleep`.
pub fn ConditionVariablePrepareToSleep(cv: &ConditionVariable) {
    let pgprocno: ProcNumber = my_proc_number::call();

    // If some other sleep is already prepared, cancel it; this is necessary
    // because we have just one static variable tracking the prepared sleep,
    // and also only one cvWaitLink in our PGPROC. It's okay to do this
    // because whenever control does return to the other test-and-sleep loop,
    // its ConditionVariableSleep call will just re-establish that sleep as
    // the prepared one.
    if sleep_target().is_some() {
        ConditionVariableCancelSleep();
    }

    // Record the condition variable on which we will sleep.
    set_sleep_target(Some(cv_identity(cv)));

    // Add myself to the wait queue.
    with_wakeup_locked(cv, "ConditionVariablePrepareToSleep", |wakeup| {
        proclist_push_tail(wakeup, pgprocno);
    });
}

/// `ConditionVariableSleep` — wait for the given condition variable to be
/// signaled.
///
/// This should be called in a predicate loop that tests for a specific exit
/// condition and otherwise sleeps, like so:
///
/// ```c
/// ConditionVariablePrepareToSleep(cv);  // optional
/// while (condition for which we are waiting is not true)
///     ConditionVariableSleep(cv, wait_event_info);
/// ConditionVariableCancelSleep();
/// ```
///
/// `wait_event_info` should be a value from one of the WaitEventXXX enums
/// defined in pgstat.h.
pub fn ConditionVariableSleep(cv: &ConditionVariable, wait_event_info: u32) -> PgResult<()> {
    ConditionVariableTimedSleep(cv, -1 /* no timeout */, wait_event_info)?;
    Ok(())
}

/// `ConditionVariableTimedSleep` — wait for a condition variable to be
/// signaled or a timeout (in milliseconds) to be reached.
///
/// Returns true when the timeout expires, otherwise false.
pub fn ConditionVariableTimedSleep(
    cv: &ConditionVariable,
    timeout: i64,
    wait_event_info: u32,
) -> PgResult<bool> {
    let mut cur_timeout: i64 = -1;
    let mut start_time = instr_time::default();
    let mut cur_time = instr_time::default();
    let wait_events: u32;

    // If the caller didn't prepare to sleep explicitly, then do so now and
    // return immediately. The caller's predicate loop should immediately
    // call again if its exit condition is not yet met. This will result in
    // the exit condition being tested twice before we first sleep. The extra
    // test can be prevented by calling ConditionVariablePrepareToSleep(cv)
    // first.
    //
    // If we are currently prepared to sleep on some other CV, we just cancel
    // that and prepare this one; see ConditionVariablePrepareToSleep.
    if sleep_target() != Some(cv_identity(cv)) {
        ConditionVariablePrepareToSleep(cv);
        return Ok(false);
    }

    // Record the current time so that we can calculate the remaining timeout
    // if we are woken up spuriously.
    if timeout >= 0 {
        instr_time_set_current(&mut start_time);
        debug_assert!((0..=i32::MAX as i64).contains(&timeout));
        cur_timeout = timeout;
        wait_events = WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH;
    } else {
        wait_events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH;
    }

    loop {
        let mut done = false;

        // Wait for latch to be set. (If we're awakened for some other
        // reason, the code below will cope anyway.)
        latch_seams::wait_latch_my_latch::call(wait_events, cur_timeout, wait_event_info)?;

        // Reset latch before examining the state of the wait list.
        latch_seams::reset_latch_my_latch::call();

        // If this process has been taken out of the wait list, then we know
        // that it has been signaled by ConditionVariableSignal (or
        // ConditionVariableBroadcast), so we should return to the caller. But
        // that doesn't guarantee that the exit condition is met, only that we
        // ought to check it. So we must put the process back into the wait
        // list, to ensure we don't miss any additional wakeup occurring while
        // the caller checks its exit condition. We can take ourselves out of
        // the wait list only when the caller calls
        // ConditionVariableCancelSleep.
        //
        // If we're still in the wait list, then the latch must have been set
        // by something other than ConditionVariableSignal; though we don't
        // guarantee not to return spuriously, we'll avoid this obvious case.
        {
            let my_procno = my_proc_number::call();
            with_wakeup_locked(cv, "ConditionVariableTimedSleep", |wakeup| {
                if !proclist_contains(wakeup, my_procno) {
                    done = true;
                    proclist_push_tail(wakeup, my_procno);
                }
            });
        }

        // Check for interrupts, and return spuriously if that caused the
        // current sleep target to change (meaning that interrupt handler code
        // waited for a different condition variable). In C
        // CHECK_FOR_INTERRUPTS() can longjmp out on a pending cancel/ERROR;
        // here that abort propagates as an `Err`.
        check_for_interrupts::call()?;
        if sleep_target() != Some(cv_identity(cv)) {
            done = true;
        }

        // We were signaled, so return.
        if done {
            return Ok(false);
        }

        // If we're not done, update cur_timeout for next iteration.
        if timeout >= 0 {
            instr_time_set_current(&mut cur_time);
            cur_time.subtract(start_time);
            cur_timeout = timeout - cur_time.get_millisec() as i64;

            // Have we crossed the timeout threshold?
            if cur_timeout <= 0 {
                return Ok(true);
            }
        }
    }
}

/// `ConditionVariableCancelSleep` — cancel any pending sleep operation.
///
/// We just need to remove ourselves from the wait queue of any condition
/// variable for which we have previously prepared a sleep.
///
/// Does nothing if nothing is pending; this allows this function to be
/// called during transaction abort to clean up any unfinished CV sleep.
///
/// Returns true if we've been signaled.
pub fn ConditionVariableCancelSleep() -> bool {
    let Some(target) = sleep_target() else {
        return false;
    };

    let my_procno = my_proc_number::call();

    // Resolve the recorded CV identity back to the live `&ConditionVariable`
    // through the resolution seam (the one address-to-reference reconstruction,
    // confined out of this function body), then run the spinlock-guarded
    // contains/delete-or-mark-signaled branch in-crate — exactly as
    // Signal/Broadcast/Sleep run their branch over their own `cv` argument.
    let signaled = cv_seams::with_target_cv::call(
        target,
        &mut |cv: &ConditionVariable| {
            with_wakeup_locked(cv, "ConditionVariableCancelSleep", |wakeup| {
                if proclist_contains(wakeup, my_procno) {
                    proclist_delete(wakeup, my_procno);
                    false
                } else {
                    true
                }
            })
        },
    );

    set_sleep_target(None);

    signaled
}

/// `with_target_cv` resolution: reconstruct the live `&ConditionVariable` from
/// the identity recorded in `cv_sleep_target` and hand it to `body`. This is
/// the single, audited address-to-reference reconstruction — the C analogue is
/// the `cv_sleep_target` pointer dereference inside `ConditionVariableCancelSleep`.
fn with_target_cv(target: CvIdentity, body: &mut dyn FnMut(&ConditionVariable) -> bool) -> bool {
    debug_assert!(target != 0, "with_target_cv: null cv_sleep_target");
    // SAFETY: `target` is the address recorded by ConditionVariablePrepareToSleep
    // from a live `&ConditionVariable`. The C protocol guarantees the CV
    // (shmem/DSM-resident, stable address) outlives the prepared sleep: a
    // backend must cancel its sleep before the CV's segment can go away —
    // exactly the lifetime over which C's `cv_sleep_target` pointer is valid.
    // The reconstructed `&` is shared (the crate-wide CV handle convention) and
    // any mutation of `wakeup` happens only under the CV mutex inside `body`.
    let cv = unsafe { &*(target as *const ConditionVariable) };
    body(cv)
}

/// `ConditionVariableSignal` — wake up the oldest process sleeping on the CV,
/// if there is any.
///
/// Note: it's difficult to tell whether this has any real effect: we know
/// whether we took an entry off the list, but the entry might only be a
/// sentinel. Hence, think twice before proposing that this should return a
/// flag telling whether it woke somebody.
pub fn ConditionVariableSignal(cv: &ConditionVariable) {
    let mut proc: Option<ProcNumber> = None;

    // Remove the first process from the wakeup queue (if any).
    with_wakeup_locked(cv, "ConditionVariableSignal", |wakeup| {
        if !proclist_is_empty(wakeup) {
            proc = Some(proclist_pop_head_node(wakeup));
        }
    });

    // If we found someone sleeping, set their latch to wake them up.
    if let Some(procno) = proc {
        proc_seams::set_proc_latch::call(procno);
    }
}

/// `ConditionVariableBroadcast` — wake up all processes sleeping on the given
/// CV.
///
/// This guarantees to wake all processes that were sleeping on the CV at
/// time of call, but processes that add themselves to the list mid-call will
/// typically not get awakened.
pub fn ConditionVariableBroadcast(cv: &ConditionVariable) {
    let pgprocno: ProcNumber = my_proc_number::call();
    let mut proc: Option<ProcNumber> = None;
    let mut have_sentinel = false;

    // In some use-cases, it is common for awakened processes to immediately
    // re-queue themselves. If we just naively try to reduce the wakeup list
    // to empty, we'll get into a potentially-indefinite loop against such a
    // process. The semantics we really want are just to be sure that we have
    // wakened all processes that were in the list at entry. We can use our
    // own cvWaitLink as a sentinel to detect when we've finished.
    //
    // A seeming flaw in this approach is that someone else might signal the
    // CV and in doing so remove our sentinel entry. But that's fine: since
    // CV waiters are always added and removed in order, that must mean that
    // every previous waiter has been wakened, so we're done. We'll get an
    // extra "set" on our latch from the someone else's signal, which is
    // slightly inefficient but harmless.
    //
    // We can't insert our cvWaitLink as a sentinel if it's already in use in
    // some other proclist. While that's not expected to be true for typical
    // uses of this function, we can deal with it by simply canceling any
    // prepared CV sleep. The next call to ConditionVariableSleep will take
    // care of re-establishing the lost state.
    if sleep_target().is_some() {
        ConditionVariableCancelSleep();
    }

    // Inspect the state of the queue. If it's empty, we have nothing to do.
    // If there's exactly one entry, we need only remove and signal that
    // entry. Otherwise, remove the first entry and insert our sentinel.
    with_wakeup_locked(cv, "ConditionVariableBroadcast", |wakeup| {
        // While we're here, let's assert we're not in the list.
        debug_assert!(!proclist_contains(wakeup, pgprocno));

        if !proclist_is_empty(wakeup) {
            proc = Some(proclist_pop_head_node(wakeup));
            if !proclist_is_empty(wakeup) {
                proclist_push_tail(wakeup, pgprocno);
                have_sentinel = true;
            }
        }
    });

    // Awaken first waiter, if there was one.
    if let Some(procno) = proc {
        proc_seams::set_proc_latch::call(procno);
    }

    while have_sentinel {
        // Each time through the loop, remove the first wakeup list entry,
        // and signal it unless it's our sentinel. Repeat as long as the
        // sentinel remains in the list.
        //
        // Notice that if someone else removes our sentinel, we will waken
        // one additional process before exiting. That's intentional, because
        // if someone else signals the CV, they may be intending to waken
        // some third process that added itself to the list after we added
        // the sentinel. Better to give a spurious wakeup (which should be
        // harmless beyond wasting some cycles) than to lose a wakeup.
        proc = None;
        with_wakeup_locked(cv, "ConditionVariableBroadcast", |wakeup| {
            if !proclist_is_empty(wakeup) {
                proc = Some(proclist_pop_head_node(wakeup));
            }
            have_sentinel = proclist_contains(wakeup, pgprocno);
        });

        // `proc != NULL && proc != MyProc` — don't set our own latch when we
        // popped our own sentinel.
        if let Some(procno) = proc {
            if procno != pgprocno {
                proc_seams::set_proc_latch::call(procno);
            }
        }
    }
}

/// Install this crate's implementations of every seam in
/// `backend-storage-lmgr-condition-variable-seams`.
pub fn init_seams() {
    cv_seams::condition_variable_timed_sleep::set(ConditionVariableTimedSleep);
    cv_seams::condition_variable_cancel_sleep::set(ConditionVariableCancelSleep);
    cv_seams::condition_variable_broadcast::set(ConditionVariableBroadcast);
    // The seam declares `cv: &mut ConditionVariable` (its shmem/DSM callers
    // hold a mutable borrow); the ported body only needs `&` (SpinLockInit +
    // proclist_init operate through interior mutability). A thin adapter
    // reborrows shared without touching the body. opacity-inherited.
    cv_seams::condition_variable_init::set(|cv| ConditionVariableInit(cv));
    cv_seams::condition_variable_sleep::set(ConditionVariableSleep);
    cv_seams::condition_variable_signal::set(ConditionVariableSignal);
    cv_seams::condition_variable_prepare_to_sleep::set(ConditionVariablePrepareToSleep);
    // The address-to-reference resolution for CancelSleep. The recorded
    // identity is this crate's own `CvIdentity`, so this crate owns and
    // installs the resolution; the lone `unsafe` deref is confined to
    // `with_target_cv` (mirrors the src-idiomatic `with_target_cv` seam).
    cv_seams::with_target_cv::set(with_target_cv);
}

#[cfg(test)]
mod tests;
