//! The `XLogRecoveryCtl` shared-memory region of
//! `access/transam/xlogrecovery.c` (PostgreSQL 18.3): the
//! `XLogRecoveryCtlData` shmem struct, `XLogRecoveryShmemSize` /
//! `XLogRecoveryShmemInit`, and the shmem state accessors the startup process
//! and other backends read/write under `info_lck`.
//!
//! This is the F0 *recovery-shmem keystone*. It mirrors the proven `XLogCtl`
//! pattern from `xlog.c` (task #111): the C file-scope global
//! `XLogRecoveryCtl` (a `XLogRecoveryCtlData *` into shared memory) is
//! reproduced as a backend-thread-local cell holding the *real*
//! shared-memory pointer reserved through `ShmemInitStruct`. The struct is
//! laid out `#[repr(C)]` field-for-field with the C struct, so the embedded
//! `recoveryWakeupLatch` (a shared `Latch`), `recoveryNotPausedCV` (a
//! `ConditionVariable`) and `info_lck` spinlock are the genuine shared words
//! the redo loop, the walreceiver, and the SIGHUP handler synchronize on.
//!
//! No driver (`StartupXLOG` / `PerformWalRecovery`) is ported yet; this unit
//! only stands up the shared region and its accessors so later families can
//! fill the recovery machinery on top of it.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate std;

use core::cell::Cell;
use core::mem::size_of;

use types_condvar::condition_variable::ConditionVariable;
use types_core::{Size, TimeLineID, TimestampTz, TransactionId, XLogRecPtr};
use types_storage::latch::{Latch, LatchHandle};
use types_storage::storage::Spinlock;
use types_wal::wal::RecoveryPauseState;

use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_lmgr_condition_variable_seams as condvar;

use types_error::PgResult;

// ===========================================================================
// XLogRecoveryCtlData — shared-memory state for WAL recovery
// (xlogrecovery.c:311). Named `XLogRecoveryShared` here, but laid out
// `#[repr(C)]` field-for-field with the C struct.
// ===========================================================================

/// `XLogRecoveryCtlData` (xlogrecovery.c:311) — the total shared-memory state
/// for WAL recovery. The single instance is created once by the startup
/// process via [`XLogRecoveryShmemInit`] and reached by every backend through
/// the [`XLOG_RECOVERY_CTL`] pointer.
#[repr(C)]
pub struct XLogRecoveryShared {
    /// `SharedHotStandbyActive` — whether hot-standby queries are allowed.
    /// Protected by `info_lck`.
    pub SharedHotStandbyActive: bool,

    /// `SharedPromoteIsTriggered` — whether a standby promotion has been
    /// triggered. Protected by `info_lck`.
    pub SharedPromoteIsTriggered: bool,

    /// `recoveryWakeupLatch` — used to wake up the startup process to continue
    /// WAL replay when it is waiting for WAL to arrive or for promotion. A
    /// shared latch (distinct from the startup process's `procLatch`, which is
    /// used for recovery-conflict waits).
    pub recoveryWakeupLatch: Latch,

    /// `lastReplayedReadRecPtr` — start position of the last record
    /// successfully replayed.
    pub lastReplayedReadRecPtr: XLogRecPtr,
    /// `lastReplayedEndRecPtr` — end+1 position of the last record replayed.
    pub lastReplayedEndRecPtr: XLogRecPtr,
    /// `lastReplayedTLI` — timeline of the last record replayed.
    pub lastReplayedTLI: TimeLineID,

    /// `replayEndRecPtr` — while replaying a record (i.e. inside a redo
    /// function), the end+1 of the record being replayed; otherwise equal to
    /// `lastReplayedEndRecPtr`.
    pub replayEndRecPtr: XLogRecPtr,
    /// `replayEndTLI` — timeline of `replayEndRecPtr`.
    pub replayEndTLI: TimeLineID,

    /// `recoveryLastXTime` — timestamp of the last COMMIT/ABORT record replayed
    /// (or being replayed).
    pub recoveryLastXTime: TimestampTz,

    /// `currentChunkStartTime` — timestamp of when we started replaying the
    /// current chunk of WAL data; only relevant for replication or archive
    /// recovery.
    pub currentChunkStartTime: TimestampTz,

    /// `recoveryPauseState` — the recovery pause request/effect state.
    pub recoveryPauseState: RecoveryPauseState,

    /// `recoveryNotPausedCV` — condition variable signalled when recovery is
    /// no longer paused.
    pub recoveryNotPausedCV: ConditionVariable,

    /// `info_lck` — spinlock protecting the shared variables above.
    pub info_lck: Spinlock,
}

// ===========================================================================
// C file-scope global (xlogrecovery.c:371), plus a couple of backend-local
// caches mirrored from the same TU. The pointer holds the genuine
// shared-memory address reserved by ShmemInitStruct, so reads/writes go
// through the real shared words. It is a per-backend process local in C (each
// backend attaches to the same segment), modeled here as a thread-local cell.
// ===========================================================================

std::thread_local! {
    /// `static XLogRecoveryCtlData *XLogRecoveryCtl = NULL` (xlogrecovery.c:371).
    static XLOG_RECOVERY_CTL: Cell<*mut XLogRecoveryShared> =
        const { Cell::new(core::ptr::null_mut()) };
}

/// The live `XLogRecoveryCtl` pointer, or null before `XLogRecoveryShmemInit`.
#[inline]
fn ctl_ptr() -> *mut XLogRecoveryShared {
    XLOG_RECOVERY_CTL.with(Cell::get)
}

/// `&XLogRecoveryCtl->...` — a borrow of the shared struct. Panics if the
/// shmem region has not been initialized yet (the C code would dereference a
/// NULL pointer); callers run only after `XLogRecoveryShmemInit`.
#[inline]
pub(crate) fn ctl() -> &'static XLogRecoveryShared {
    let p = ctl_ptr();
    debug_assert!(!p.is_null(), "XLogRecoveryCtl accessed before XLogRecoveryShmemInit");
    // SAFETY: `p` points at the live `ShmemInitStruct` region, which outlives
    // the process; the field words are individually synchronized by `info_lck`
    // / the latch / the CV at their points of use.
    unsafe { &*p }
}

/// `&mut XLogRecoveryCtl->...` — a mutable borrow of the shared struct, used
/// by the startup process (the sole writer of most fields, the others guarded
/// by `info_lck` at the call site).
#[inline]
#[allow(clippy::mut_from_ref)]
pub(crate) fn ctl_mut() -> &'static mut XLogRecoveryShared {
    let p = ctl_ptr();
    debug_assert!(!p.is_null(), "XLogRecoveryCtl accessed before XLogRecoveryShmemInit");
    // SAFETY: see `ctl`. The startup process is the sole writer; cross-backend
    // readers take `info_lck`.
    unsafe { &mut *p }
}

// ===========================================================================
// SpinLockAcquire / SpinLockRelease / SpinLockInit (storage/s_lock.h macros),
// over the embedded `info_lck`.
// ===========================================================================

#[inline]
pub(crate) fn spin_lock_acquire(lock: &Spinlock) {
    backend_storage_lmgr_s_lock::s_lock_macro(lock, Some(file!()), line!() as i32, None);
}

#[inline]
pub(crate) fn spin_lock_release(lock: &Spinlock) {
    backend_storage_lmgr_s_lock::s_unlock(lock);
}

#[inline]
fn spin_lock_init(lock: &Spinlock) {
    backend_storage_lmgr_s_lock::s_init_lock(lock);
}

// ===========================================================================
// XLogRecoveryShmemSize / XLogRecoveryShmemInit (xlogrecovery.c:453 / 464).
// ===========================================================================

/// `Size XLogRecoveryShmemSize(void)` (xlogrecovery.c:453) — the shared-memory
/// bytes this subsystem needs.
pub fn XLogRecoveryShmemSize() -> PgResult<Size> {
    // XLogRecoveryCtl
    let size: Size = size_of::<XLogRecoveryShared>();

    Ok(size)
}

/// `void XLogRecoveryShmemInit(void)` (xlogrecovery.c:464) — allocate-or-attach
/// the `XLogRecoveryCtl` shared-memory structure.
pub fn XLogRecoveryShmemInit() -> PgResult<()> {
    // XLogRecoveryCtl = ShmemInitStruct("XLOG Recovery Ctl",
    //                                   XLogRecoveryShmemSize(), &found);
    let (raw, found) = shmem::shmem_init_struct::call("XLOG Recovery Ctl", XLogRecoveryShmemSize()?)?;
    let ctl_ptr = raw as *mut XLogRecoveryShared;
    XLOG_RECOVERY_CTL.with(|c| c.set(ctl_ptr));

    if found {
        return Ok(());
    }

    // SAFETY: fresh region of `XLogRecoveryShmemSize()` bytes; single-process
    // init (this runs in the postmaster before any backend forks).
    unsafe {
        // memset(XLogRecoveryCtl, 0, sizeof(XLogRecoveryCtlData)).
        //
        // `RecoveryPauseState` is a fieldless enum whose all-zero bit pattern
        // is `NotPaused` (RECOVERY_NOT_PAUSED == 0); zeroing the rest matches
        // the C memset, but we write the typed default to keep the enum
        // discriminant well-formed rather than relying on a raw zero write.
        core::ptr::write_bytes(raw, 0, size_of::<XLogRecoveryShared>());
        core::ptr::write(
            &mut (*ctl_ptr).recoveryPauseState,
            RecoveryPauseState::NotPaused,
        );

        let ctl = &mut *ctl_ptr;

        // SpinLockInit(&XLogRecoveryCtl->info_lck).
        spin_lock_init(&ctl.info_lck);

        // InitSharedLatch(&XLogRecoveryCtl->recoveryWakeupLatch): mark the
        // embedded latch as a cleared, shared latch owned by no-one. (The
        // latch unit's `InitSharedLatch` is keyed by `LatchHandle`; the
        // embedded-in-shmem latch is initialized inline here exactly as
        // proc_shmem does for `PGPROC.procLatch`, which is the same body.)
        ctl.recoveryWakeupLatch
            .is_set
            .store(0, core::sync::atomic::Ordering::SeqCst);
        ctl.recoveryWakeupLatch
            .maybe_sleeping
            .store(0, core::sync::atomic::Ordering::SeqCst);
        ctl.recoveryWakeupLatch
            .is_shared
            .store(true, core::sync::atomic::Ordering::SeqCst);
        ctl.recoveryWakeupLatch
            .owner_pid
            .store(0, core::sync::atomic::Ordering::SeqCst);

        // ConditionVariableInit(&XLogRecoveryCtl->recoveryNotPausedCV).
        condvar::condition_variable_init::call(&mut ctl.recoveryNotPausedCV);
    }

    Ok(())
}

/// `&XLogRecoveryCtl->recoveryWakeupLatch` as a [`LatchHandle`] — the recovery
/// wakeup latch identified for `OwnLatch`/`SetLatch`/`WaitLatch` by the latch
/// unit's handle convention (the single recovery-shmem latch). Returned for
/// later families (`OwnLatch` in `InitWalRecovery`, `WakeupRecovery`'s
/// `SetLatch`) that thread it through the latch seams.
#[inline]
pub fn recovery_wakeup_latch_handle() -> LatchHandle {
    // The recovery-shmem latch is a single, distinct shared latch; the latch
    // unit reserves handle slot 0 for "no latch", so this subsystem's latch is
    // a dedicated reserved id agreed with the latch unit.
    LatchHandle::new(RECOVERY_WAKEUP_LATCH_ID)
}

/// Reserved `LatchHandle` id for the recovery wakeup latch (a single,
/// process-global shared latch). It lives in this subsystem's own shmem
/// (`XLogRecoveryCtl->recoveryWakeupLatch`), not the latch unit's registry, so
/// this is still an unregistered local id — a latent handle for the not-yet-
/// wired recovery `OwnLatch`/`SetLatch` families. It stays in the *local*
/// handle space (the `PROC_TAG` bit clear), distinct from the per-PGPROC
/// `procLatch` space (`LatchHandle::proc`).
const RECOVERY_WAKEUP_LATCH_ID: usize = types_storage::latch::PROC_TAG - 1;

// ===========================================================================
// Shmem state accessors (the genuine shmem reads/writes, under info_lck where
// the C code takes the spinlock). These are the recovery-state replay
// accessors of xlogrecovery.c, re-homed onto the real shared struct.
// ===========================================================================

/// `XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI)` (xlogrecovery.c:4581)
/// — the latest redo apply position. Returns `(recptr, replayTLI)`. Read under
/// `info_lck`.
pub fn get_xlog_replay_rec_ptr() -> (XLogRecPtr, TimeLineID) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let recptr = ctl.lastReplayedEndRecPtr;
    let tli = ctl.lastReplayedTLI;
    spin_lock_release(&ctl.info_lck);
    (recptr, tli)
}

/// `XLogRecPtr GetCurrentReplayRecPtr(TimeLineID *replayEndTLI)`
/// (xlogrecovery.c:4604) — position of the last applied record, or the record
/// being applied. Returns `(recptr, replayEndTLI)`. Read under `info_lck`.
pub fn get_current_replay_rec_ptr() -> (XLogRecPtr, TimeLineID) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let recptr = ctl.replayEndRecPtr;
    let tli = ctl.replayEndTLI;
    spin_lock_release(&ctl.info_lck);
    (recptr, tli)
}

/// `TimestampTz GetLatestXTime(void)` (xlogrecovery.c:4638) — timestamp of the
/// latest processed COMMIT/ABORT record during recovery. Read under `info_lck`.
pub fn get_latest_xtime() -> TimestampTz {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let xtime = ctl.recoveryLastXTime;
    spin_lock_release(&ctl.info_lck);
    xtime
}

/// `static void SetLatestXTime(TimestampTz xtime)` (xlogrecovery.c:4627) —
/// set the latest replayed COMMIT/ABORT timestamp. Written under `info_lck`.
/// Its only C caller is the recovery-record-apply path (`RecordKnownAssigned`/
/// `ApplyWalRecord`'s timestamp tracking), part of the not-yet-ported replay
/// driver, so it is currently unwired.
#[allow(dead_code)]
pub(crate) fn set_latest_xtime(xtime: TimestampTz) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    ctl_mut().recoveryLastXTime = xtime;
    spin_lock_release(&ctl.info_lck);
}

/// `TimestampTz GetCurrentChunkReplayStartTime(void)` (xlogrecovery.c:4668) —
/// the timestamp of the WAL chunk currently being replayed. Read under
/// `info_lck`.
pub fn get_current_chunk_replay_start_time() -> TimestampTz {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let xtime = ctl.currentChunkStartTime;
    spin_lock_release(&ctl.info_lck);
    xtime
}

/// `static void SetCurrentChunkStartTime(TimestampTz xtime)`
/// (xlogrecovery.c:4656) — set the chunk-replay start timestamp. Written under
/// `info_lck`. Its only C caller is `WaitForWALToBecomeAvailable` (part of the
/// not-yet-ported page-read driver), so it is currently unwired.
#[allow(dead_code)]
pub(crate) fn set_current_chunk_start_time(xtime: TimestampTz) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    ctl_mut().currentChunkStartTime = xtime;
    spin_lock_release(&ctl.info_lck);
}

/// `RecoveryPauseState GetRecoveryPauseState(void)` (xlogrecovery.c:3127) —
/// the current recovery pause state. Read under `info_lck`.
pub fn get_recovery_pause_state() -> RecoveryPauseState {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let state = ctl.recoveryPauseState;
    spin_lock_release(&ctl.info_lck);
    state
}

/// `void SetRecoveryPause(bool recoveryPause)` (xlogrecovery.c:3094) —
/// request or clear a recovery pause. Written under `info_lck`.
pub fn set_recovery_pause(recovery_pause: bool) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    if !recovery_pause {
        ctl_mut().recoveryPauseState = RecoveryPauseState::NotPaused;
    } else if ctl.recoveryPauseState == RecoveryPauseState::NotPaused {
        ctl_mut().recoveryPauseState = RecoveryPauseState::PauseRequested;
    }
    spin_lock_release(&ctl.info_lck);
}

/// `bool HotStandbyActive(void)` (xlogrecovery.c) — reads
/// `SharedHotStandbyActive` under `info_lck`.
pub fn hot_standby_active() -> bool {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let active = ctl.SharedHotStandbyActive;
    spin_lock_release(&ctl.info_lck);
    active
}

/// Suppress an unused-import warning for `TransactionId` should later families
/// add `recoveryStopXid` handling; it is part of the recovery-stop carrier
/// pulled in alongside the shmem keystone.
#[allow(dead_code)]
type _RecoveryStopXid = TransactionId;
