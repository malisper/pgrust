//! The `ConditionVariable` data shape (`storage/condition_variable.h`).

use types_storage::storage::{proclist_head, Spinlock};

/// `ConditionVariable` (`storage/condition_variable.h`).
///
/// C: `{ slock_t mutex; proclist_head wakeup; }` — a spinlock plus the list
/// of waiting proc numbers (linked through each `PGPROC.cvWaitLink`), with
/// the actual sleeping done on each waiter's process latch. Shmem-resident
/// and concurrently accessed, so it is neither `Copy` nor `Clone`. The
/// `wakeup` head/tail are mutated only while `mutex` is held; the protocol
/// functions (`storage/lmgr/condition_variable.c`) belong to the
/// `backend-storage-lmgr-condition-variable` crate and are reached across
/// cycles through `backend-storage-lmgr-condition-variable-seams`.
///
/// `ConditionVariableInit` in C just initializes the struct in place
/// (`SpinLockInit` + `proclist_init`); here construction is initialization
/// ([`ConditionVariable::new`]), and the owner crate's `ConditionVariableInit`
/// re-initializes in place.
#[derive(Debug, Default)]
pub struct ConditionVariable {
    /// spinlock protecting the wakeup list
    pub mutex: Spinlock,
    /// list of wake-able processes
    pub wakeup: proclist_head,
}

impl ConditionVariable {
    /// A free spinlock and an empty wakeup list.
    pub fn new() -> Self {
        Self::default()
    }
}

/// `CV_MINIMAL_SIZE` (`storage/condition_variable.h`) — pad a condition
/// variable to a power-of-two size so an array of them does not cross a
/// cache line boundary: `sizeof(ConditionVariable) <= 16 ? 16 : 32`.
pub const CV_MINIMAL_SIZE: usize = if core::mem::size_of::<ConditionVariable>() <= 16 {
    16
} else {
    32
};

/// `ConditionVariableMinimallyPadded` (`storage/condition_variable.h`) — in C
/// a union of a `ConditionVariable` with a `char pad[CV_MINIMAL_SIZE]`; the
/// alignment attribute reproduces both the size and placement guarantee.
#[repr(align(16))]
#[derive(Debug, Default)]
pub struct ConditionVariableMinimallyPadded {
    pub cv: ConditionVariable,
}

const _: () = assert!(core::mem::size_of::<ConditionVariableMinimallyPadded>() == CV_MINIMAL_SIZE);
