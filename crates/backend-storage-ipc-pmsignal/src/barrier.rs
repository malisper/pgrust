//! Port of `src/backend/storage/ipc/barrier.c`: barriers for synchronizing
//! cooperating processes.
//!
//! A [`Barrier`] is a phased synchronization point living in shared memory. It
//! supports either a static set of participants known up front, or a dynamic
//! set that processes can join (`BarrierAttach`) or leave
//! (`BarrierDetach`/`BarrierArriveAndDetach`) at any time. A phase counter
//! tracks progress through a multi-phase parallel algorithm (parallel hash
//! join's build/batch/grow barriers). Static barriers behave like POSIX
//! `pthread_barrier_t`; dynamic barriers like Java's `Phaser`.
//!
//! The control flow is ported 1:1 from `barrier.c` over the shared-memory
//! [`Barrier`] struct. The `slock_t` over `barrier->mutex` is the real
//! [`Spinlock`] driven directly by `backend-storage-lmgr-s-lock`; the embedded
//! condition variable is operated through
//! `backend-storage-lmgr-condition-variable-seams`.
//!
//! C `Assert`s are modeled as `debug_assert!`. The CV functions are `void` in
//! C; their seamed ports return a `PgResult` only so an interrupt during a
//! sleep can be modeled (C takes that exit via a `longjmp` from *inside*
//! `ConditionVariableSleep`, never returning to `barrier.c`). The barrier's
//! public surface matches C's `bool`/`int` returns exactly — no error channel —
//! so those `Result`s are explicitly discarded here, leaving the
//! interrupt/abort control transfer to the installed CV implementation, as in C.

use backend_storage_lmgr_condition_variable_seams as cv;
use backend_storage_lmgr_s_lock::{s_init_lock, s_lock_macro, s_unlock};
use types_condvar::{Barrier, ConditionVariable};
use types_core::uint32;
use types_storage::storage::proclist_head;

/// `SpinLockAcquire(&barrier->mutex)`: `s_lock` if the test-and-set fails. C's
/// `SpinLockAcquire` macro is `TAS_SPIN` then `s_lock` on contention; the
/// s-lock crate's `s_lock_macro` is that exact wrapper.
#[inline]
fn spin_lock_acquire(lock: &types_storage::Spinlock) {
    s_lock_macro(lock, Some("barrier.c"), 0, Some("BarrierMutex"));
}

/// Initialize this barrier.
///
/// To use a static party size, provide the number of participants to wait for
/// at each phase, indicating that that number of backends is implicitly
/// attached. To use a dynamic party size, specify zero here and then use
/// [`BarrierAttach`] and [`BarrierDetach`]/[`BarrierArriveAndDetach`] to
/// register and deregister participants explicitly.
///
/// ```c
/// void BarrierInit(Barrier *barrier, int participants)
/// ```
pub fn BarrierInit(barrier: &mut Barrier, participants: i32) {
    s_init_lock(&barrier.mutex);
    barrier.participants = participants;
    barrier.arrived = 0;
    barrier.phase = 0;
    barrier.elected = 0;
    barrier.static_party = participants > 0;
    // ConditionVariableInit(&barrier->condition_variable):
    //   SpinLockInit(&cv->mutex); proclist_init(&cv->wakeup).
    barrier.condition_variable = ConditionVariable {
        mutex: types_storage::Spinlock::default(),
        wakeup: proclist_head::default(),
    };
}

/// Arrive at this barrier, wait for all other attached participants to arrive
/// too and then return. Increments the current phase. The caller must be
/// attached.
///
/// Returns `true` in one arbitrarily chosen participant and `false` in all
/// others. The return code can be used to elect one participant to execute a
/// phase of work that must be done serially while other participants wait.
///
/// ```c
/// bool BarrierArriveAndWait(Barrier *barrier, uint32 wait_event_info)
/// ```
pub fn BarrierArriveAndWait(barrier: &mut Barrier, wait_event_info: uint32) -> bool {
    let mut release = false;
    let mut elected;
    let start_phase;
    let next_phase;

    spin_lock_acquire(&barrier.mutex);
    start_phase = barrier.phase;
    next_phase = start_phase + 1;
    barrier.arrived += 1;
    if barrier.arrived == barrier.participants {
        release = true;
        barrier.arrived = 0;
        barrier.phase = next_phase;
        barrier.elected = next_phase;
    }
    s_unlock(&barrier.mutex);

    // If we were the last expected participant to arrive, we can release our
    // peers and return true to indicate that this backend has been elected to
    // perform any serial work.
    if release {
        cv::condition_variable_broadcast::call(&barrier.condition_variable);
        return true;
    }

    // Otherwise we have to wait for the last participant to arrive and advance
    // the phase.
    elected = false;
    cv::condition_variable_prepare_to_sleep::call(&barrier.condition_variable);
    loop {
        // We know that phase must either be start_phase, indicating that we
        // need to keep waiting, or next_phase, indicating that the last
        // participant has either arrived or detached so that the next phase
        // has begun. The phase cannot advance any further than that without
        // this backend's participation, because this backend is attached.
        spin_lock_acquire(&barrier.mutex);
        debug_assert!(barrier.phase == start_phase || barrier.phase == next_phase);
        release = barrier.phase == next_phase;
        if release && barrier.elected != next_phase {
            // Usually the backend that arrives last and releases the others is
            // elected to return true, so it can begin processing serial work
            // while it has a CPU timeslice. However, if the barrier advanced
            // because someone detached, then one of the awoken backends must be
            // elected.
            barrier.elected = barrier.phase;
            elected = true;
        }
        s_unlock(&barrier.mutex);
        if release {
            break;
        }
        let _ = cv::condition_variable_sleep::call(&barrier.condition_variable, wait_event_info);
    }
    let _ = cv::condition_variable_cancel_sleep::call();

    elected
}

/// Arrive at this barrier, but detach rather than waiting. Returns `true` if
/// the caller was the last to detach.
///
/// ```c
/// bool BarrierArriveAndDetach(Barrier *barrier)
/// ```
pub fn BarrierArriveAndDetach(barrier: &mut Barrier) -> bool {
    BarrierDetachImpl(barrier, true)
}

/// Arrive at a barrier, and detach all but the last to arrive. Returns `true`
/// if the caller was the last to arrive, and is therefore still attached.
///
/// ```c
/// bool BarrierArriveAndDetachExceptLast(Barrier *barrier)
/// ```
pub fn BarrierArriveAndDetachExceptLast(barrier: &mut Barrier) -> bool {
    spin_lock_acquire(&barrier.mutex);
    if barrier.participants > 1 {
        barrier.participants -= 1;
        s_unlock(&barrier.mutex);
        return false;
    }
    debug_assert!(barrier.participants == 1);
    barrier.phase += 1;
    s_unlock(&barrier.mutex);

    true
}

/// Attach to a barrier. All waiting participants will now wait for this
/// participant to call [`BarrierArriveAndWait`], [`BarrierDetach`] or
/// [`BarrierArriveAndDetach`]. Returns the current phase.
///
/// ```c
/// int BarrierAttach(Barrier *barrier)
/// ```
pub fn BarrierAttach(barrier: &mut Barrier) -> i32 {
    debug_assert!(!barrier.static_party);

    spin_lock_acquire(&barrier.mutex);
    barrier.participants += 1;
    let phase = barrier.phase;
    s_unlock(&barrier.mutex);

    phase
}

/// Detach from a barrier. This may release other waiters from
/// [`BarrierArriveAndWait`] and advance the phase if they were only waiting for
/// this backend. Returns `true` if this participant was the last to detach.
///
/// ```c
/// bool BarrierDetach(Barrier *barrier)
/// ```
pub fn BarrierDetach(barrier: &mut Barrier) -> bool {
    BarrierDetachImpl(barrier, false)
}

/// Return the current phase of a barrier. The caller must be attached.
///
/// It is OK to read `barrier->phase` without locking, because it can't change
/// without us (we are attached to it), and we executed a memory barrier when we
/// either attached or participated in changing it last time.
///
/// ```c
/// int BarrierPhase(Barrier *barrier)
/// ```
pub fn BarrierPhase(barrier: &Barrier) -> i32 {
    barrier.phase
}

/// Return an instantaneous snapshot of the number of participants currently
/// attached to this barrier. For debugging purposes only.
///
/// ```c
/// int BarrierParticipants(Barrier *barrier)
/// ```
pub fn BarrierParticipants(barrier: &mut Barrier) -> i32 {
    spin_lock_acquire(&barrier.mutex);
    let participants = barrier.participants;
    s_unlock(&barrier.mutex);

    participants
}

/// Detach from a barrier. If `arrive` is true then also increment the phase if
/// there are no other participants. If there are other participants waiting,
/// then the phase will be advanced and they'll be released if they were only
/// waiting for the caller. Returns `true` if this participant was the last to
/// detach.
///
/// ```c
/// static inline bool BarrierDetachImpl(Barrier *barrier, bool arrive)
/// ```
#[inline]
fn BarrierDetachImpl(barrier: &mut Barrier, arrive: bool) -> bool {
    let release;
    let last;

    debug_assert!(!barrier.static_party);

    spin_lock_acquire(&barrier.mutex);
    debug_assert!(barrier.participants > 0);
    barrier.participants -= 1;

    // If any other participants are waiting and we were the last participant
    // waited for, release them. If no other participants are waiting, but this
    // is a BarrierArriveAndDetach() call, then advance the phase too.
    if (arrive || barrier.participants > 0) && barrier.arrived == barrier.participants {
        release = true;
        barrier.arrived = 0;
        barrier.phase += 1;
    } else {
        release = false;
    }

    last = barrier.participants == 0;
    s_unlock(&barrier.mutex);

    if release {
        cv::condition_variable_broadcast::call(&barrier.condition_variable);
    }

    last
}
