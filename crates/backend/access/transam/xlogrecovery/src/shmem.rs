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

use condvar::condition_variable::ConditionVariable;
use types_core::{Size, TimeLineID, TimestampTz, TransactionId, XLogRecPtr};
use types_storage::latch::{Latch, LatchHandle};
use types_storage::storage::Spinlock;
use wal::wal::RecoveryPauseState;
use wal::xlogrecovery_carriers::XLogSource;

use latch as latch;
use startup_seams as startup;
use walreceiverfuncs_seams as walrcv;

use ipc_shmem_seams as shmem;
use condition_variable_seams as condvar;

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
    s_lock::s_lock_macro(lock, Some(file!()), line!() as i32, None);
}

#[inline]
pub(crate) fn spin_lock_release(lock: &Spinlock) {
    s_lock::s_unlock(lock);
}

#[inline]
fn spin_lock_init(lock: &Spinlock) {
    s_lock::s_init_lock(lock);
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

/// `void SetRecoveryPause(bool recoveryPause)` (xlogrecovery.c:3111) —
/// request or clear a recovery pause. Written under `info_lck`. When clearing
/// the pause, broadcasts `recoveryNotPausedCV` so a paused redo loop wakes.
pub fn set_recovery_pause(recovery_pause: bool) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    if !recovery_pause {
        ctl_mut().recoveryPauseState = RecoveryPauseState::NotPaused;
    } else if ctl.recoveryPauseState == RecoveryPauseState::NotPaused {
        ctl_mut().recoveryPauseState = RecoveryPauseState::PauseRequested;
    }
    spin_lock_release(&ctl.info_lck);

    if !recovery_pause {
        condvar::condition_variable_broadcast::call(&ctl.recoveryNotPausedCV);
    }
}

/// `static void ConfirmRecoveryPaused(void)` (xlogrecovery.c:3131) — once the
/// redo loop notices a pause request, transition `PauseRequested` -> `Paused`.
/// Written under `info_lck`.
pub fn confirm_recovery_paused() {
    let ctl = ctl();
    /* If recovery pause is requested then set it paused */
    spin_lock_acquire(&ctl.info_lck);
    if ctl.recoveryPauseState == RecoveryPauseState::PauseRequested {
        ctl_mut().recoveryPauseState = RecoveryPauseState::Paused;
    }
    spin_lock_release(&ctl.info_lck);
}

/// `PerformWalRecovery` initial-progress block (xlogrecovery.c:1682-1700): seed
/// the shared replay-progress words "as if we had just replayed the record
/// before the REDO location", under `info_lck`.
pub(crate) fn init_replay_progress(
    last_read: XLogRecPtr,
    last_end: XLogRecPtr,
    last_tli: TimeLineID,
) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let m = ctl_mut();
    m.lastReplayedReadRecPtr = last_read;
    m.lastReplayedEndRecPtr = last_end;
    m.lastReplayedTLI = last_tli;
    m.replayEndRecPtr = last_end;
    m.replayEndTLI = last_tli;
    m.recoveryLastXTime = 0;
    m.currentChunkStartTime = 0;
    m.recoveryPauseState = RecoveryPauseState::NotPaused;
    spin_lock_release(&ctl.info_lck);
}

/// `ApplyWalRecord` (xlogrecovery.c:1991-1994): update the shared
/// `replayEndRecPtr`/`replayEndTLI` before replaying a record, under `info_lck`,
/// so `XLogFlush` updates `minRecoveryPoint` correctly.
pub(crate) fn set_replay_end(end_rec_ptr: XLogRecPtr, tli: TimeLineID) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let m = ctl_mut();
    m.replayEndRecPtr = end_rec_ptr;
    m.replayEndTLI = tli;
    spin_lock_release(&ctl.info_lck);
}

/// `ApplyWalRecord` (xlogrecovery.c:2028-2032): update the shared
/// `lastReplayed*` words after a record has been successfully replayed, under
/// `info_lck`.
pub(crate) fn set_last_replayed(
    read_rec_ptr: XLogRecPtr,
    end_rec_ptr: XLogRecPtr,
    tli: TimeLineID,
) {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let m = ctl_mut();
    m.lastReplayedReadRecPtr = read_rec_ptr;
    m.lastReplayedEndRecPtr = end_rec_ptr;
    m.lastReplayedTLI = tli;
    spin_lock_release(&ctl.info_lck);
}

/// `(XLogRecoveryCtl->lastReplayedEndRecPtr, lastReplayedTLI)` read assuming the
/// caller is the startup process (`CheckRecoveryConsistency`, xlogrecovery.c:
/// 2211-2215: "assume that we are called in the startup process, and hence
/// don't need a lock"). Mirrors the C lock-free read.
pub(crate) fn last_replayed_end_rec_ptr_unlocked() -> (XLogRecPtr, TimeLineID) {
    let ctl = ctl();
    (ctl.lastReplayedEndRecPtr, ctl.lastReplayedTLI)
}

/// `(XLogRecoveryCtl->lastReplayedReadRecPtr, lastReplayedTLI)` — the start
/// position of the last replayed record and its timeline, read by the startup
/// process in `FinishWalRecovery` (xlogrecovery.c:1538-1539). The startup
/// process is the sole writer of these fields during recovery, so this is a
/// lock-free read, matching the C access (it is read directly without taking
/// `info_lck` there).
pub(crate) fn last_replayed_read_rec_ptr_tli_unlocked() -> (XLogRecPtr, TimeLineID) {
    let ctl = ctl();
    (ctl.lastReplayedReadRecPtr, ctl.lastReplayedTLI)
}

/// `((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState`
/// (xlogrecovery.c:1808): the redo loop's intentionally-unlocked pause-state
/// peek (the comment there explains why no `info_lck` is taken).
pub(crate) fn recovery_pause_state_unlocked() -> RecoveryPauseState {
    ctl().recoveryPauseState
}

/// `CheckRecoveryConsistency` (xlogrecovery.c:2283-2285): publish
/// `SharedHotStandbyActive = true` under `info_lck`.
pub(crate) fn set_shared_hot_standby_active() {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    ctl_mut().SharedHotStandbyActive = true;
    spin_lock_release(&ctl.info_lck);
}

/// `XLogRecPtr GetXLogReplayRecPtr(NULL)` — the NULL-`replayTLI` form, dropping
/// the timeline. A thin wrapper over [`get_xlog_replay_rec_ptr`] for the
/// callers that only want the LSN.
pub fn get_xlog_replay_recptr_only() -> XLogRecPtr {
    get_xlog_replay_rec_ptr().0
}

// ===========================================================================
// Backend-local file-static globals (xlogrecovery.c:178-261). These are C
// process-local `static`s that ANY backend connected to shared memory keeps —
// the cross-backend accessors (`HotStandbyActive`, `PromoteIsTriggered`, …)
// cache the shared flag into one of these on first read. Modeled as
// thread-locals (one per backend thread), matching the per-process C statics.
// They are the canonical home; the corresponding `XLogRecoveryState` mirror
// fields are the startup process's own copy threaded through replay.
// ===========================================================================

std::thread_local! {
    /// `static bool LocalHotStandbyActive = false;` (xlogrecovery.c:178).
    static LOCAL_HOT_STANDBY_ACTIVE: Cell<bool> = const { Cell::new(false) };
    /// `static bool LocalPromoteIsTriggered = false;` (xlogrecovery.c:184).
    static LOCAL_PROMOTE_IS_TRIGGERED: Cell<bool> = const { Cell::new(false) };
    /// `static bool doRequestWalReceiverReply;` (xlogrecovery.c:187).
    static DO_REQUEST_WAL_RECEIVER_REPLY: Cell<bool> = const { Cell::new(false) };
    /// `static XLogSource currentSource = XLOG_FROM_ANY;` (xlogrecovery.c:248).
    static CURRENT_SOURCE: Cell<XLogSource> = const { Cell::new(XLogSource::Any) };
    /// `static bool pendingWalRcvRestart = false;` (xlogrecovery.c:250).
    static PENDING_WAL_RCV_RESTART: Cell<bool> = const { Cell::new(false) };
    /// `static TimestampTz XLogReceiptTime = 0;` (xlogrecovery.c:260).
    static XLOG_RECEIPT_TIME: Cell<TimestampTz> = const { Cell::new(0) };
    /// `static XLogSource XLogReceiptSource = XLOG_FROM_ANY;` (xlogrecovery.c:261).
    static XLOG_RECEIPT_SOURCE: Cell<XLogSource> = const { Cell::new(XLogSource::Any) };
}

/// `bool HotStandbyActive(void)` (xlogrecovery.c:4543). We check shared state
/// each time only until Hot Standby is active; once seen true the local cache
/// short-circuits (Hot Standby can never be de-activated).
pub fn hot_standby_active() -> bool {
    if LOCAL_HOT_STANDBY_ACTIVE.with(Cell::get) {
        return true;
    }
    // spinlock is essential on machines with weak memory ordering!
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let v = ctl.SharedHotStandbyActive;
    spin_lock_release(&ctl.info_lck);
    LOCAL_HOT_STANDBY_ACTIVE.with(|c| c.set(v));
    v
}

/// `bool PromoteIsTriggered(void)` (xlogrecovery.c:4435). Works in any process
/// connected to shared memory. Caches the shared flag locally; once a promotion
/// is triggered it can't be triggered again, so we stop re-reading shmem.
pub fn promote_is_triggered() -> bool {
    if LOCAL_PROMOTE_IS_TRIGGERED.with(Cell::get) {
        return true;
    }
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    let v = ctl.SharedPromoteIsTriggered;
    spin_lock_release(&ctl.info_lck);
    LOCAL_PROMOTE_IS_TRIGGERED.with(|c| c.set(v));
    v
}

/// `static void SetPromoteIsTriggered(void)` (xlogrecovery.c:4453). Sets the
/// shared flag, ends any recovery pause, and records the local cache.
pub(crate) fn set_promote_is_triggered() {
    let ctl = ctl();
    spin_lock_acquire(&ctl.info_lck);
    ctl_mut().SharedPromoteIsTriggered = true;
    spin_lock_release(&ctl.info_lck);

    // Mark the recovery pause state as 'not paused' because the paused state
    // ends and promotion continues if a promotion is triggered while recovery
    // is paused. Otherwise pg_get_wal_replay_pause_state() can mistakenly
    // return 'paused' while a promotion is ongoing.
    set_recovery_pause(false);

    LOCAL_PROMOTE_IS_TRIGGERED.with(|c| c.set(true));
}

/// `void WakeupRecovery(void)` (xlogrecovery.c:4519) — wake up the startup
/// process to replay newly arrived WAL, or to notice that failover has been
/// requested. Sets the shared recovery-wakeup latch.
pub fn wakeup_recovery() {
    latch::SetLatchPtr(&ctl().recoveryWakeupLatch);
}

/// `void XLogRequestWalReceiverReply(void)` (xlogrecovery.c:4528) — schedule a
/// walreceiver wakeup in the main recovery loop (the redo loop consumes the
/// flag on its next iteration).
pub fn xlog_request_wal_receiver_reply() {
    DO_REQUEST_WAL_RECEIVER_REPLY.with(|c| c.set(true));
}

/// `void RemovePromoteSignalFiles(void)` (xlogrecovery.c:4495) — remove the
/// files signaling a standby promotion request. C `unlink(PROMOTE_SIGNAL_FILE)`,
/// ignoring errors (the file may not exist).
pub fn remove_promote_signal_files() {
    let _ = std::fs::remove_file(PROMOTE_SIGNAL_FILE);
}

/// `bool CheckPromoteSignal(void)` (xlogrecovery.c:4504) — true iff the
/// promote-signal file exists. C `stat(PROMOTE_SIGNAL_FILE, &stat_buf) == 0`.
pub fn check_promote_signal() -> bool {
    std::path::Path::new(PROMOTE_SIGNAL_FILE).exists()
}

/// `static bool CheckForStandbyTrigger(void)` (xlogrecovery.c:4474) — check
/// whether a promote request has arrived. Crate-internal: called by the replay
/// loop and `RecoveryRequiresIntParameter` while paused. Consumes the
/// promote-signal file via the startup process's signal helpers.
pub(crate) fn check_for_standby_trigger() -> bool {
    if LOCAL_PROMOTE_IS_TRIGGERED.with(Cell::get) {
        return true;
    }

    if startup::is_promote_signaled::call() && check_promote_signal() {
        // ereport(LOG, errmsg("received promote request"))
        let _ = utils_error::elog(types_error::LOG, "received promote request");
        remove_promote_signal_files();
        startup::reset_promote_signaled::call();
        set_promote_is_triggered();
        return true;
    }

    false
}

// --- Page-read driver accessors for the file-static read-state thread-locals
// (`currentSource`, `pendingWalRcvRestart`, `XLogReceiptTime`,
// `XLogReceiptSource`). The driver runs as the reader's `page_read` callback
// (`fn`, no `&mut st`), so it reaches these through the crate's canonical
// thread-local home here. ---

/// `currentSource` — the WAL source the recovery state machine is currently
/// reading from.
#[inline]
pub(crate) fn current_source() -> XLogSource {
    CURRENT_SOURCE.with(Cell::get)
}
/// Set `currentSource`.
#[inline]
pub(crate) fn set_current_source(s: XLogSource) {
    CURRENT_SOURCE.with(|c| c.set(s));
}
/// `pendingWalRcvRestart`.
#[inline]
pub(crate) fn pending_wal_rcv_restart() -> bool {
    PENDING_WAL_RCV_RESTART.with(Cell::get)
}
/// Set `pendingWalRcvRestart`.
#[inline]
pub(crate) fn set_pending_wal_rcv_restart(v: bool) {
    PENDING_WAL_RCV_RESTART.with(|c| c.set(v));
}
/// Set `XLogReceiptTime`.
#[inline]
pub(crate) fn set_xlog_receipt_time(t: TimestampTz) {
    XLOG_RECEIPT_TIME.with(|c| c.set(t));
}
/// Set `XLogReceiptSource`.
#[inline]
pub(crate) fn set_xlog_receipt_source(s: XLogSource) {
    XLOG_RECEIPT_SOURCE.with(|c| c.set(s));
}

/// `PROMOTE_SIGNAL_FILE` (access/xlog.h) — relative to `$PGDATA`.
const PROMOTE_SIGNAL_FILE: &str = "promote";

/// `void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream)`
/// (xlogrecovery.c:4683) — time of receipt of the current chunk of XLOG data,
/// and whether it arrived via streaming replication. Returns
/// `(rtime, from_stream)`. Must run in the startup process (this state is not
/// exported to shared memory); the C `Assert(InRecovery)` is the startup-process
/// invariant.
pub fn get_xlog_receipt_time() -> (TimestampTz, bool) {
    let rtime = XLOG_RECEIPT_TIME.with(Cell::get);
    let from_stream = XLOG_RECEIPT_SOURCE.with(Cell::get) == XLogSource::Stream;
    (rtime, from_stream)
}

/// `void StartupRequestWalReceiverRestart(void)` (xlogrecovery.c:4416) — if we
/// are streaming and the walreceiver is up, flag that it must be restarted
/// because a critical option changed. `void` in C: the `WalRcvRunning` read is
/// infallible at this point (its `PgResult` models a shmem state-write that
/// never `ereport`s here), so a `false` reading short-circuits as "not running".
pub fn startup_request_wal_receiver_restart() {
    if CURRENT_SOURCE.with(Cell::get) == XLogSource::Stream
        && walrcv::wal_rcv_running::call().unwrap_or(false)
    {
        // ereport(LOG, errmsg("WAL receiver process shutdown requested"))
        let _ = utils_error::elog(
            types_error::LOG,
            "WAL receiver process shutdown requested",
        );
        PENDING_WAL_RCV_RESTART.with(|c| c.set(true));
    }
}

/// Suppress an unused-import warning for `TransactionId` should later families
/// add `recoveryStopXid` handling; it is part of the recovery-stop carrier
/// pulled in alongside the shmem keystone.
#[allow(dead_code)]
type _RecoveryStopXid = TransactionId;
