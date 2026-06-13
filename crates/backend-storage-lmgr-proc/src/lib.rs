//! Per-process shared-memory slot management (`storage/lmgr/proc.c`).
//!
//! SCAFFOLD STAGE. This crate owns the `PGPROC` array and the cluster-wide
//! `ProcGlobal` (`PROC_HDR`) header, the proc freelists, the lock wait queue,
//! and the deadlock-check / lock-error-cleanup bracket around a `ProcSleep`.
//! proc.c is large, so it is split into family modules that mirror the C file
//! one-to-one:
//!
//! - [`proc_shmem`]     — shmem sizing + `InitProcGlobal` (the one-time setup
//!   of the `PGPROC` array, the dense `ProcGlobal` mirror arrays, and the four
//!   freelists).
//! - [`proc_lifecycle`] — a backend/aux process claiming and releasing its
//!   slot (`InitProcess`/`InitProcessPhase2`/`InitAuxiliaryProcess`/`ProcKill`/
//!   `AuxiliaryProcKill`/`RemoveProcFromArray`/`AuxiliaryPidGetProc`).
//! - [`proc_waitqueue`] — joining a heavyweight lock's wait queue, sleeping on
//!   it, being woken, the deadlock-timeout check, and cleanup
//!   (`JoinWaitQueue`/`ProcSleep`/`ProcWakeup`/`ProcLockWakeup`/`CheckDeadLock`/
//!   `LockErrorCleanup`/`ProcReleaseLocks`/`GetLockHoldersAndWaiters`).
//! - [`proc_misc`]      — signal/wait helpers and lock-group membership
//!   (`ProcWaitForSignal`/`ProcSendSignal`/`BecomeLockGroupLeader`/
//!   `BecomeLockGroupMember`/`IsWaitingForLock`/`HaveNFreeProcs`/...).
//!
//! RECLAIMED into this crate (real algorithm + real `PGPROC`/`PROC_HDR`, not
//! seams): the freelist `dlist` push/pop over the real `PGPROC` array, the
//! wait-queue priority insertion, and `lock_group_held_locks`.
//!
//! OUTWARD seams (neighbors this unit calls into): procarray, latch, lwlock,
//! pmsignal, syncrep, condition-variable, pgstat wait-event, the deadlock
//! checker, and lock.c (`LockCheckConflicts`/`GrantLock`/`RemoveFromWaitQueue`/
//! `LockReleaseAll`/...). Each is reached through that owner's per-owner seam
//! crate (panicking until it lands); none is restructured around.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub mod globals;
pub mod proc_lifecycle;
pub mod proc_misc;
pub mod proc_shmem;
pub mod proc_waitqueue;

mod inward_seams;

/// Install this crate's implementations of every seam declared in
/// `backend-storage-lmgr-proc-seams` (the `PGPROC`-field accessors and the
/// proc.c helpers other units call into).
pub fn init_seams() {
    inward_seams::install();
}
