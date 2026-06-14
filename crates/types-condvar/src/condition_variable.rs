//! The `ConditionVariable` data shape (`storage/condition_variable.h`).

use core::cell::UnsafeCell;

use types_storage::storage::{proclist_head, Spinlock};

/// The `wakeup` field of a [`ConditionVariable`]: a `proclist_head` that, per
/// `condition_variable.c`'s protocol, is mutated only while the CV's `mutex`
/// spinlock is held. Backends share `&ConditionVariable` handles to the same
/// shmem/DSM-resident CV, so the head/tail live in an `UnsafeCell`; the runtime
/// exclusion that makes `ptr()` access sound is the held `mutex`, exactly as in
/// C (where `cv->wakeup` is reached through a `ConditionVariable *` regardless
/// of constness). This mirrors `LWLockWaitList` in `types-storage`, the
/// established idiom for a `proclist_head` mutated through a shared handle under
/// a spinlock.
#[derive(Debug, Default)]
pub struct CvWakeupList {
    cell: UnsafeCell<proclist_head>,
}

// SAFETY: cross-thread access is serialized by the owning CV's `mutex`
// spinlock (condition_variable.c's wakeup-list protocol), exactly as for
// `LWLockWaitList` under `LW_FLAG_LOCKED`.
unsafe impl Sync for CvWakeupList {}

impl CvWakeupList {
    /// An empty wakeup list (`head == tail == INVALID_PROC_NUMBER`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Raw pointer to the list head. Dereferencing requires holding the owning
    /// CV's `mutex` spinlock (or otherwise having exclusive access, e.g.
    /// single-threaded initialization).
    pub fn ptr(&self) -> *mut proclist_head {
        self.cell.get()
    }

    /// Exclusive-access view (used where the caller legitimately holds
    /// `&mut ConditionVariable`, e.g. construction/initialization).
    pub fn get_mut(&mut self) -> &mut proclist_head {
        self.cell.get_mut()
    }

    /// A snapshot copy of the head/tail. Reading requires the same exclusion as
    /// `ptr()`; provided so callers that only need the indices (e.g. tests,
    /// `proclist_is_empty` under the lock) need not write `unsafe`.
    pub fn get(&self) -> proclist_head {
        // SAFETY: callers hold the CV mutex (or have exclusive init access);
        // `proclist_head` is `Copy`, so this is a plain read of two indices.
        unsafe { *self.cell.get() }
    }
}

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
    /// list of wake-able processes. The head/tail live behind a [`CvWakeupList`]
    /// (`UnsafeCell`) because all backends reach a shmem-resident CV through a
    /// shared `&ConditionVariable`, yet C mutates `cv->wakeup` while holding
    /// `cv->mutex`; the spinlock is the exclusion that makes that sound.
    pub wakeup: CvWakeupList,
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
