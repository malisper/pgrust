//! Port of three `src/backend/storage/ipc/` files:
//!
//! - `pmsignal.c` — signaling between the postmaster and its child processes.
//! - `barrier.c` — the dynamic-party phased barrier (`Barrier`) used to
//!   coordinate parallel-query backends.
//! - `signalfuncs.c` — the SQL-callable backend-signaling functions
//!   (`pg_cancel_backend`, `pg_terminate_backend`, `pg_reload_conf`,
//!   `pg_rotate_logfile`).
//!
//! (`ipc.c`, the fourth file of this catalog unit, is already ported in
//! `backend-storage-ipc-dsm-core`, so it is not re-ported here.)
//!
//! # Model notes (audit against these)
//!
//! - The C `PMSignalData` lives in shared memory via
//!   `ShmemInitStruct("PMSignalState", size, &found)`, valid in both the
//!   postmaster and its children. Here a backend is a thread and shared memory
//!   is explicitly shared, synchronized state, so the control block is a
//!   process-global [`OnceLock`] of [`PMSignalState`]: first
//!   [`PMSignalShmemInit`] constructs it (the C `!found` arm — every field
//!   zero-initialized), later calls find it. The per-reason flags
//!   (`PMSignalFlags[]`), `sigquit_reason`, and the per-child `PMChildFlags[]`
//!   are `volatile sig_atomic_t` in C (lock-free, atomic loads/stores); they
//!   are atomics here (`AtomicI32`/`AtomicU32`), preserving the C lockless
//!   discipline. The byte-size `ShmemInitStruct` handshake has no analogue, so
//!   [`PMSignalShmemInit`] does not consume [`PMSignalShmemSize`]; the size
//!   function itself stays ported (its `mul_size`/`add_size` overflow ereports
//!   are the shmem allocator's failure surface).
//! - `num_child_flags` is, in the postmaster, a local copy of
//!   `MaxLivePostmasterChildren()`. We do not keep a separate mutable copy: it
//!   is always `MaxLivePostmasterChildren()`, read through the
//!   `backend-postmaster-pmchild-seams` seam where C consults either the local
//!   copy or `PMSignalState->num_child_flags`. The shared `num_child_flags`
//!   field is still published on first creation (it backs the C
//!   `RegisterPostmasterChildActive`/`MarkPostmasterChildWalSender` asserts).
//! - `MyPMChildSlot` (globals.c) and `PostmasterPid` (globals.c) are read
//!   through `backend-utils-init-small-seams`. `IsUnderPostmaster` likewise.
//! - `postmaster_possibly_dead` is a `volatile sig_atomic_t` set by a signal
//!   handler in the *owning* process; the death-pipe probe, the signal-handler
//!   install, and the `prctl`/`procctl` parent-death request are all the OS
//!   signal layer — they stay behind this unit's own seam crate
//!   (`postmaster_is_alive` / `postmaster_death_signal_init`), installed here
//!   only as thin pass-throughs to the not-yet-ported platform layer.
//! - The `Barrier` data shape lives in `types-condvar`; its `slock_t mutex` is
//!   a real [`Spinlock`] driven by `backend-storage-lmgr-s-lock`, and its
//!   embedded `ConditionVariable` is operated through
//!   `backend-storage-lmgr-condition-variable-seams`. The CV protocol calls are
//!   `void` in C; their seams return `PgResult` only to model an interrupt
//!   during a sleep (C takes that exit via `longjmp` from *inside*
//!   `ConditionVariableSleep`, never returning to `barrier.c`), so the barrier's
//!   public surface — which has no error channel in C — discards those results.
//! - `kill(2)` is the OS boundary and is called via `libc` directly, like the
//!   other signal-layer ports.
//! - C `Assert`s become `debug_assert!` (project convention). `elog(FATAL)` /
//!   `elog(ERROR)` / `ereport(ERROR|WARNING)` become `PgResult` `Err` or a
//!   `WARNING` report, per AGENTS.md.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use backend_utils_error::ereport;
use types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};

mod barrier;
mod pmsignal;
mod signalfuncs;

pub use barrier::{
    BarrierArriveAndDetach, BarrierArriveAndDetachExceptLast, BarrierArriveAndWait, BarrierAttach,
    BarrierDetach, BarrierInit, BarrierParticipants, BarrierPhase,
};
pub use pmsignal::{
    CheckPostmasterSignal, GetQuitSignalReason, IsPostmasterChildWalSender,
    MarkPostmasterChildInactive, MarkPostmasterChildSlotAssigned, MarkPostmasterChildSlotUnassigned,
    MarkPostmasterChildWalSender, PMSignalShmemInit, PMSignalShmemSize, PostmasterDeathSignalInit,
    PostmasterIsAliveInternal, RegisterPostmasterChildActive, SendPostmasterSignal,
    SetQuitSignalReason, PMSignalReason, QuitSignalReason, NUM_PMSIGNALS, PM_CHILD_ACTIVE,
    PM_CHILD_ASSIGNED, PM_CHILD_UNUSED, PM_CHILD_WALSENDER,
};
pub use signalfuncs::{
    pg_cancel_backend, pg_reload_conf, pg_rotate_logfile, pg_signal_backend, pg_terminate_backend,
    SIGNAL_BACKEND_ERROR, SIGNAL_BACKEND_NOAUTOVAC, SIGNAL_BACKEND_NOPERMISSION,
    SIGNAL_BACKEND_NOSUPERUSER, SIGNAL_BACKEND_SUCCESS,
};

// ---------------------------------------------------------------------------
// add_size / mul_size — checked shmem-size arithmetic (storage/shmem.c).
//
// `PMSignalShmemSize` is the only consumer; ported in-crate because the helpers
// are tiny and shmem.c's owner is reached only for these two (inlined) checks.
// ---------------------------------------------------------------------------

/// `add_size(s1, s2)` — checked addition; raises the exact "requested shared
/// memory size overflows size_t" error C does on wraparound.
#[inline]
fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    s1.checked_add(s2).ok_or_else(size_overflow)
}

/// `mul_size(s1, s2)` — checked multiplication; raises the same error on
/// overflow, returning 0 when either operand is 0 (matching the C guard).
#[inline]
fn mul_size(s1: usize, s2: usize) -> PgResult<usize> {
    if s1 == 0 || s2 == 0 {
        return Ok(0);
    }
    s1.checked_mul(s2).ok_or_else(size_overflow)
}

fn size_overflow() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg("requested shared memory size overflows size_t")
        .into_error()
}

/// Install this crate's inward seams: the whole `barrier.c` protocol
/// (`backend-storage-ipc-barrier-seams`) and the `pmsignal.c` entry points
/// other units reach across a dependency cycle
/// (`backend-storage-ipc-pmsignal-seams`).
pub fn init_seams() {
    // barrier.c
    backend_storage_ipc_barrier_seams::BarrierInit::set(barrier::BarrierInit);
    backend_storage_ipc_barrier_seams::BarrierArriveAndWait::set(barrier::BarrierArriveAndWait);
    backend_storage_ipc_barrier_seams::BarrierArriveAndDetach::set(barrier::BarrierArriveAndDetach);
    backend_storage_ipc_barrier_seams::BarrierArriveAndDetachExceptLast::set(
        barrier::BarrierArriveAndDetachExceptLast,
    );
    backend_storage_ipc_barrier_seams::BarrierAttach::set(barrier::BarrierAttach);
    backend_storage_ipc_barrier_seams::BarrierDetach::set(barrier::BarrierDetach);
    backend_storage_ipc_barrier_seams::BarrierPhase::set(barrier::BarrierPhase);
    backend_storage_ipc_barrier_seams::BarrierParticipants::set(barrier::BarrierParticipants);

    // pmsignal.c
    backend_storage_ipc_pmsignal_seams::postmaster_is_alive::set(pmsignal::PostmasterIsAlive);
    backend_storage_ipc_pmsignal_seams::postmaster_death_signal_init::set(
        pmsignal::PostmasterDeathSignalInit_seam,
    );
    backend_storage_ipc_pmsignal_seams::send_postmaster_signal_bgworker_change::set(
        pmsignal::send_postmaster_signal_bgworker_change,
    );
    backend_storage_ipc_pmsignal_seams::pm_signal_shmem_size::set(pmsignal::PMSignalShmemSize);
    backend_storage_ipc_pmsignal_seams::pm_signal_shmem_init::set(pmsignal::PMSignalShmemInit);
}

#[cfg(test)]
mod tests;
