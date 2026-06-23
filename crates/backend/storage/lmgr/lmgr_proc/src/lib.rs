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
        use ::guc_tables::{vars, GucVarAccessors};
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

    // `BecomeLockGroupLeader()` / `BecomeLockGroupMember()` (proc.c) — the
    // lock-group attach that `access/transam/parallel.c` reaches outward for.
    // proc.c owns the bodies; the parallel-rt slots are declared in
    // `backend-access-transam-parallel-rt-seams`. Both are faithful thin `::set`s.
    //
    // The member slot is keyed by the leader's `ProcNumber` (the carrier the
    // worker reads out of `FixedParallelState::parallel_leader_proc_number`),
    // resolved to the leader `PGPROC` slot by `BecomeLockGroupMemberByNumber`.
    // This replaces the retired `PgProcHandle` (`PGPROC *`) contract: a
    // process-local pointer is meaningless across the leader→DSM→worker
    // hand-off, and `parallel_leader_pgproc` was never populated.
    parallel_rt_seams::become_lock_group_leader::set(
        proc_misc::BecomeLockGroupLeader,
    );
    parallel_rt_seams::become_lock_group_member::set(
        proc_misc::BecomeLockGroupMemberByNumber,
    );

    // `ProcSleep`'s lock-wait progress / autovac-cancel diagnostics
    // (proc.c:1523/1531/1590). These `ereport(...)` + `kill(SIGINT)` bodies are
    // proc.c's own logic; they were homed on `backend-tcop-postgres-seams`
    // (the `postgres::` alias proc_waitqueue calls through) only because the
    // wait-queue and its message text live here while the seam crate names the
    // file `postgres.c` historically owned. Install them from the real owner
    // with faithful bodies (the message text is already formatted at the call
    // sites; these only emit the report / send the signal).
    postgres_seams::report_autovac_cancel::set(seam::report_autovac_cancel);
    postgres_seams::signal_autovacuum_worker::set(seam::signal_autovacuum_worker);
    postgres_seams::report_lock_wait_log::set(seam::report_lock_wait_log);

    // Startup-process buffer-pin-wait bufid (proc.c
    // Get/SetStartupBufferPinWaitBufId, backed by ProcGlobal). The getter is the
    // bufmgr-side outward seam read by HoldingBufferPinThatDelaysRecovery; the
    // setter is published by the LockBufferForCleanup InHotStandby park leg
    // (installed by the standby/recovery owner).
    bufmgr_seams::startup_buffer_pin_wait_buf_id::set(
        proc_lifecycle::GetStartupBufferPinWaitBufId,
    );
    lmgr_proc_seams::set_startup_buffer_pin_wait_buf_id::set(
        proc_lifecycle::SetStartupBufferPinWaitBufId,
    );

    // `pid_get_proc(pid)` of mcxtfuncs.c's `pg_log_backend_memory_contexts`:
    //
    //   proc = BackendPidGetProc(pid);
    //   if (proc == NULL) proc = AuxiliaryPidGetProc(pid);
    //   if (proc == NULL) return NULL;
    //   procNumber = GetNumberFromPGProc(proc);
    //
    // `GetNumberFromPGProc(proc)` is `proc->procNumber` — the slot's own
    // `ProcNumber`, which is exactly the value `BackendPidGetProc` /
    // `AuxiliaryPidGetProc` resolve a pid to here. The backend scan crosses into
    // procarray (`backend_pid_get_proc_role`, installed by procarray); the
    // auxiliary scan is `proc_lifecycle::AuxiliaryPidGetProc`, owned here.
    ::mcxtfuncs_seams::pid_get_proc::set(|pid| {
        use ::mcxtfuncs_seams::McxtSignalTarget;
        let proc_number = procarray_seams::backend_pid_get_proc_role::call(pid)
            .map(|(_role_id, procno)| procno)
            .or_else(|| proc_lifecycle::AuxiliaryPidGetProc(pid));
        Ok(proc_number.map(|proc_number| McxtSignalTarget { proc_number }))
    });
}
