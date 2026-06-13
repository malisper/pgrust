//! Seam declarations for the `backend-storage-ipc-barrier` unit
//! (`storage/ipc/barrier.c`): the dynamic-party phased barrier protocol used to
//! coordinate parallel-query backends.
//!
//! The [`Barrier`](types_condvar::Barrier) struct itself is the shared-memory
//! data shape from `types-condvar`; these functions are its attach/arrive/wait
//! protocol. The caller resolves the barrier out of its DSA/DSM-resident parent
//! (e.g. `ParallelHashJoinState::build_barrier`) and passes a borrow. The owning
//! unit installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.
//!
//! These never `ereport(ERROR)` (they spin/sleep on a condition variable and
//! manipulate counters under a spinlock), so they are infallible.

#![allow(non_snake_case)]

use types_condvar::Barrier;
use types_core::uint32;

seam_core::seam!(
    /// `BarrierInit(barrier, participants)` (barrier.c): initialize a barrier
    /// in place for a known (`participants > 0`) or dynamic (`0`) party.
    pub fn BarrierInit(barrier: &mut Barrier, participants: i32)
);

seam_core::seam!(
    /// `BarrierArriveAndWait(barrier, wait_event_info)` (barrier.c): arrive at
    /// the barrier and wait for all other participants; returns `true` to the
    /// participant elected to do the next phase's serial work.
    pub fn BarrierArriveAndWait(barrier: &mut Barrier, wait_event_info: uint32) -> bool
);

seam_core::seam!(
    /// `BarrierArriveAndDetach(barrier)` (barrier.c): arrive and immediately
    /// detach without waiting; returns `true` if this was the last to arrive.
    pub fn BarrierArriveAndDetach(barrier: &mut Barrier) -> bool
);

seam_core::seam!(
    /// `BarrierArriveAndDetachExceptLast(barrier)` (barrier.c): like
    /// `BarrierArriveAndDetach` but the final participant stays attached;
    /// returns `false` to that last participant.
    pub fn BarrierArriveAndDetachExceptLast(barrier: &mut Barrier) -> bool
);

seam_core::seam!(
    /// `BarrierAttach(barrier)` (barrier.c): join the barrier's current phase;
    /// returns that phase number.
    pub fn BarrierAttach(barrier: &mut Barrier) -> i32
);

seam_core::seam!(
    /// `BarrierDetach(barrier)` (barrier.c): leave the barrier; returns `true`
    /// if this caused the phase to advance.
    pub fn BarrierDetach(barrier: &mut Barrier) -> bool
);

seam_core::seam!(
    /// `BarrierPhase(barrier)` (barrier.c): the current phase number. A plain
    /// field read in C.
    pub fn BarrierPhase(barrier: &Barrier) -> i32
);

seam_core::seam!(
    /// `BarrierParticipants(barrier)` (barrier.c): the number of participants
    /// currently attached.
    pub fn BarrierParticipants(barrier: &mut Barrier) -> i32
);
