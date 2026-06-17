//! Per-process shared-memory slot management (`storage/lmgr/proc.c`).
//!
//! This crate owns the `PGPROC` array and the cluster-wide `ProcGlobal`
//! (`PROC_HDR`) header, the proc freelists, the lock wait queue, and the
//! deadlock-check / lock-error-cleanup bracket around a `ProcSleep`. proc.c is
//! large, so it is split into family modules that mirror the C file one-to-one:
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
mod seam;

/// Install this crate's implementations of every seam declared in
/// `backend-storage-lmgr-proc-seams` (the `PGPROC`-field accessors and the
/// proc.c helpers other units call into).
pub fn init_seams() {
    inward_seams::install();

    // GUC variable backing storage owned by proc.c (the `conf->variable` that
    // guc_tables.c points its table entries at). These are plain int/bool GUC
    // globals defined in proc.c (`DeadlockTimeout = 1000`, the five timeouts =
    // 0, `log_lock_waits = false`); none come from the ControlFile. Install the
    // owner get/set so the GUC engine's `.read()`/`.set()` reach this storage.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::DeadlockTimeout.install(GucVarAccessors {
            get: globals::DeadlockTimeout,
            set: globals::set_DeadlockTimeout,
        });
        vars::StatementTimeout.install(GucVarAccessors {
            get: globals::StatementTimeout,
            set: globals::set_StatementTimeout,
        });
        vars::LockTimeout.install(GucVarAccessors {
            get: globals::LockTimeout,
            set: globals::set_LockTimeout,
        });
        vars::IdleInTransactionSessionTimeout.install(GucVarAccessors {
            get: globals::IdleInTransactionSessionTimeout,
            set: globals::set_IdleInTransactionSessionTimeout,
        });
        vars::TransactionTimeout.install(GucVarAccessors {
            get: globals::TransactionTimeout,
            set: globals::set_TransactionTimeout,
        });
        vars::IdleSessionTimeout.install(GucVarAccessors {
            get: globals::IdleSessionTimeout,
            set: globals::set_IdleSessionTimeout,
        });
        vars::log_lock_waits.install(GucVarAccessors {
            get: globals::log_lock_waits,
            set: globals::set_log_lock_waits,
        });
    }
}
