//! The `FixedParallelState` DSM driver (`access/transam/parallel.c`).
//!
//! `FixedParallelState` is the fixed-size record the leader writes into the
//! parallel-query DSM segment and each worker reads back. Its `mutex`
//! (`slock_t`) is a *genuine cross-process spinlock* living in the shared DSM
//! mapping — the worker's `ParallelWorkerReportLastRecEnd` does
//! `SpinLockAcquire(&fps->mutex); ...; SpinLockRelease(&fps->mutex)` over the
//! same physical bytes the leader initialized with `SpinLockInit`. We mirror
//! that exactly with an in-segment [`Spinlock`] and the contended-backoff
//! `s_lock` acquire loop.
//!
//! The seam contract threads only the raw chunk address (`base: usize`) that
//! `shm_toc_allocate` / `shm_toc_lookup` returned, mirroring the C
//! `fps = (FixedParallelState *) shm_toc_allocate(...)`. The driver forms a
//! `*FixedParallelStateShared` at that address and reads/writes fields in
//! place. The value-typed [`FixedParallelState`] carrier (which has *no* mutex
//! field — the lock is the driver's concern) is the marshaled view the seam
//! passes/returns; the shared layout below is the on-the-wire DSM record.

use types_core::{pid_t, Oid, ProcNumber, TimestampTz, XLogRecPtr};
use types_parallel::FixedParallelState;
use types_storage::storage::Spinlock;

use backend_storage_lmgr_s_lock::{s_lock_macro, s_unlock};

/// The DSM-resident `FixedParallelState` record (`parallel.c:82`), field-for-
/// field with the C struct including the `mutex` `slock_t` between
/// `serializable_xact_handle` and `last_xlog_end`. `#[repr(C)]` so the byte
/// layout the leader writes is the one each worker reads.
///
/// Pointer-shaped C fields (`PGPROC *parallel_leader_pgproc`,
/// `SerializableXactHandle serializable_xact_handle`) are carried as their raw
/// machine words (`usize`), exactly as the value-typed [`FixedParallelState`]
/// carries them. `parallel_leader_pgproc` is never populated (stays 0); the
/// leader's identity travels in `parallel_leader_proc_number`.
#[repr(C)]
struct FixedParallelStateShared {
    database_id: Oid,
    authenticated_user_id: Oid,
    session_user_id: Oid,
    outer_user_id: Oid,
    current_user_id: Oid,
    temp_namespace_id: Oid,
    temp_toast_namespace_id: Oid,
    sec_context: i32,
    session_user_is_superuser: bool,
    role_is_superuser: bool,
    parallel_leader_pgproc: usize,
    parallel_leader_pid: pid_t,
    parallel_leader_proc_number: ProcNumber,
    xact_ts: TimestampTz,
    stmt_ts: TimestampTz,
    serializable_xact_handle: usize,
    /// `slock_t mutex` — protects `last_xlog_end`; a genuine cross-process
    /// spinlock in the shared DSM mapping.
    mutex: Spinlock,
    last_xlog_end: XLogRecPtr,
}

/// `InitializeParallelDSM`'s fixed-state write (`parallel.c:343-359`): copy the
/// leader-collected fields in, `SpinLockInit(&fps->mutex)`, and zero
/// `last_xlog_end`.
///
/// SAFETY: `base` is the address `shm_toc_allocate` reserved for a
/// `sizeof(FixedParallelState)` chunk; it is writable, suitably aligned, and
/// the leader is the sole writer pre-launch. We fully initialize every field
/// (no padding read).
pub fn fps_init(base: usize, state: FixedParallelState) {
    let p = base as *mut FixedParallelStateShared;
    unsafe {
        core::ptr::write(
            p,
            FixedParallelStateShared {
                database_id: state.database_id,
                authenticated_user_id: state.authenticated_user_id,
                session_user_id: state.session_user_id,
                outer_user_id: state.outer_user_id,
                current_user_id: state.current_user_id,
                temp_namespace_id: state.temp_namespace_id,
                temp_toast_namespace_id: state.temp_toast_namespace_id,
                sec_context: state.sec_context,
                session_user_is_superuser: state.session_user_is_superuser,
                role_is_superuser: state.role_is_superuser,
                parallel_leader_pgproc: state.parallel_leader_pgproc,
                parallel_leader_pid: state.parallel_leader_pid,
                parallel_leader_proc_number: state.parallel_leader_proc_number,
                xact_ts: state.xact_ts,
                stmt_ts: state.stmt_ts,
                serializable_xact_handle: state.serializable_xact_handle,
                // SpinLockInit(&fps->mutex)
                mutex: Spinlock::new(),
                // fps->last_xlog_end = 0
                last_xlog_end: 0,
            },
        );
    }
}

/// Read the fixed state back into the value-typed carrier (`ParallelWorkerMain`
/// `fps = shm_toc_lookup(...)` then reads each `fps->field`). The mutex is the
/// driver's; the carrier omits it.
///
/// SAFETY: `base` addresses a fully-initialized `FixedParallelStateShared`
/// (the leader ran [`fps_init`] before publishing the chunk). The scalar fields
/// are read by copy; `mutex`/`last_xlog_end` are not protected for this bulk
/// read because the worker reads them before any worker has started bumping
/// `last_xlog_end` (mirroring C, which reads these fields without the lock in
/// the restore path).
pub fn fps_read(base: usize) -> FixedParallelState {
    let p = base as *const FixedParallelStateShared;
    unsafe {
        FixedParallelState {
            database_id: (*p).database_id,
            authenticated_user_id: (*p).authenticated_user_id,
            session_user_id: (*p).session_user_id,
            outer_user_id: (*p).outer_user_id,
            current_user_id: (*p).current_user_id,
            temp_namespace_id: (*p).temp_namespace_id,
            temp_toast_namespace_id: (*p).temp_toast_namespace_id,
            sec_context: (*p).sec_context,
            session_user_is_superuser: (*p).session_user_is_superuser,
            role_is_superuser: (*p).role_is_superuser,
            parallel_leader_pgproc: (*p).parallel_leader_pgproc,
            parallel_leader_pid: (*p).parallel_leader_pid,
            parallel_leader_proc_number: (*p).parallel_leader_proc_number,
            xact_ts: (*p).xact_ts,
            stmt_ts: (*p).stmt_ts,
            serializable_xact_handle: (*p).serializable_xact_handle,
            last_xlog_end: read_last_xlog_end_locked(p),
        }
    }
}

/// `ReinitializeParallelDSM` (`parallel.c:532`): `fps->last_xlog_end = 0`.
///
/// SAFETY: `base` addresses an initialized shared record. The write of
/// `last_xlog_end` is taken under the mutex to be consistent with the
/// concurrent `report_last_rec_end` writers (the C reinit runs after all
/// workers have exited, so there is in fact no contender, but taking the lock
/// is harmless and keeps the access discipline uniform).
pub fn fps_reset_last_xlog_end(base: usize) {
    let p = base as *mut FixedParallelStateShared;
    let lock = unsafe { &(*p).mutex };
    s_lock_macro(lock, Some(file!()), line!() as i32, Some("fps_reset_last_xlog_end"));
    unsafe {
        (*p).last_xlog_end = 0;
    }
    s_unlock(lock);
}

/// `WaitForParallelWorkersToFinish` (`parallel.c:902-904`): read
/// `fps->last_xlog_end` (the leader folds it into `XactLastRecEnd`).
///
/// SAFETY: `base` addresses an initialized shared record. Read under the mutex
/// to observe a torn-free value against concurrent worker writers.
pub fn fps_get_last_xlog_end(base: usize) -> XLogRecPtr {
    let p = base as *const FixedParallelStateShared;
    read_last_xlog_end_locked(p)
}

/// `ParallelWorkerReportLastRecEnd` (`parallel.c:1598-1601`):
/// `SpinLockAcquire(&fps->mutex); if (fps->last_xlog_end < last_xlog_end)
/// fps->last_xlog_end = last_xlog_end; SpinLockRelease(&fps->mutex);`.
///
/// This is the one genuinely cross-process critical section: many workers race
/// to publish the maximum `XactLastRecEnd` into the single shared word.
///
/// SAFETY: `base` is `MyFixedParallelState`, the address of the initialized
/// shared record in the attached DSM mapping. The mutex and `last_xlog_end`
/// are interior-mutable shared bytes; the spinlock serializes the read-modify-
/// write against every other worker.
pub fn fps_report_last_rec_end(base: usize, last_xlog_end: XLogRecPtr) {
    let p = base as *mut FixedParallelStateShared;
    let lock = unsafe { &(*p).mutex };
    s_lock_macro(lock, Some(file!()), line!() as i32, Some("fps_report_last_rec_end"));
    unsafe {
        if (*p).last_xlog_end < last_xlog_end {
            (*p).last_xlog_end = last_xlog_end;
        }
    }
    s_unlock(lock);
}

/// Shared helper: read `last_xlog_end` under the mutex.
///
/// SAFETY: caller guarantees `p` addresses an initialized shared record.
fn read_last_xlog_end_locked(p: *const FixedParallelStateShared) -> XLogRecPtr {
    let lock = unsafe { &(*p).mutex };
    s_lock_macro(lock, Some(file!()), line!() as i32, Some("fps_read_last_xlog_end"));
    let v = unsafe { (*p).last_xlog_end };
    s_unlock(lock);
    v
}
