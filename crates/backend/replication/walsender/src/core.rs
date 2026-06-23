//! Shared core declarations for the walsender port.
//!
//! Port of the file-scope declarations at the top of
//! `src/backend/replication/walsender.c` plus the closely-related
//! header types (`replication/walsender.h`, `replication/walsender_private.h`).
//!
//! Two pieces of state live here:
//!
//!  * The C file-static process state (`static XLogRecPtr sentPtr` family, the
//!    promoted function-local statics, the GUCs, and the exported flag globals)
//!    is collected into one owned [`WalSndProc`] held in a single process-local
//!    cell.  PG walsenders are single-threaded per process, so this mirrors the
//!    C semantics without `static mut`.
//!
//!  * The shared-memory `WalSndCtlData` array (`WalSndCtl`) — the per-walsender
//!    `WalSnd` slots, their spinlocks, the three condition variables, and the
//!    sync-rep queue header — is owned here as a real `#[repr(C)]` shmem struct
//!    allocated through `ShmemInitStruct`, exactly as `slotsync.c`'s
//!    `SlotSyncCtx` is owned by its crate.  `MyWalSnd` is this backend's index
//!    into `WalSndCtl->walsnds`.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

extern crate alloc;

use core::cell::{Cell, RefCell};
use core::ffi::c_int;

use ::condvar::ConditionVariable;
use ::types_core::primitive::{pid_t, sig_atomic_t};
use ::types_storage::storage::Spinlock;

pub use ::types_core::primitive::{Size, TimeLineID, TimestampTz, TransactionId, XLogRecPtr};
pub use ::types_core::primitive::uint32;
pub use ::types_core::Oid;
pub use types_datetime::{Interval, TimeOffset};

pub use ::replication::replnodes::{
    AlterReplicationSlotCmd, BaseBackupCmd, CreateReplicationSlotCmd, DropReplicationSlotCmd,
    IdentifySystemCmd, ReadReplicationSlotCmd, ReplCommand, ReplicationKind, StartReplicationCmd,
    TimeLineHistoryCmd, UploadManifestCmd, VariableShowStmt,
};
pub use ::replication::walsender::{SyncState, WalSenderRow, WalSnd, WalSndState};

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`): `#define InvalidXLogRecPtr 0`.
pub const InvalidXLogRecPtr: XLogRecPtr = 0;
/// `InvalidOid` (postgres_ext.h): `((Oid) 0)`.
pub const InvalidOid: ::types_core::Oid = 0;
/// `InvalidTransactionId` (`access/transam.h`).
pub const InvalidTransactionId: ::types_core::primitive::TransactionId = 0;

// ---------------------------------------------------------------------------
// Constants (walsender.c)
// ---------------------------------------------------------------------------

/// Minimum interval used by walsender for stats flushes, in ms.
pub const WALSENDER_STATS_FLUSH_INTERVAL: c_int = 1000;
/// `XLOG_BLCKSZ` (`access/xlog_internal.h`); WAL block size (build default 8 kB).
pub const XLOG_BLCKSZ: usize = 8192;
/// Maximum data payload in a WAL data message.  Must be >= `XLOG_BLCKSZ`.
pub const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;
/// The size of our buffer of time samples.
pub const LAG_TRACKER_BUFFER_SIZE: usize = 8192;

// Sync-rep wait modes (`replication/syncrep.h`); used to size LagTracker arrays.
pub const SYNC_REP_NO_WAIT: c_int = -1;
pub const SYNC_REP_WAIT_WRITE: c_int = 0;
pub const SYNC_REP_WAIT_FLUSH: c_int = 1;
pub const SYNC_REP_WAIT_APPLY: c_int = 2;
pub const NUM_SYNC_REP_WAIT_MODE: usize = 3;
pub const SYNC_REP_WAIT_COMPLETE: c_int = 2;

/// `READ_REPLICATION_SLOT_COLS` (local to ReadReplicationSlot).
pub const READ_REPLICATION_SLOT_COLS: c_int = 3;
/// `PG_STAT_GET_WAL_SENDERS_COLS` (local to pg_stat_get_wal_senders).
pub const PG_STAT_GET_WAL_SENDERS_COLS: c_int = 12;
/// `WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS` (local to WalSndUpdateProgress).
pub const WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS: c_int = 1000;

pub const TEXTOID: ::types_core::Oid = 25;
pub const INT8OID: ::types_core::Oid = 20;
/// `ROLE_PG_READ_ALL_STATS` (catalog/pg_authid.dat) — predefined role OID 3375.
pub const ROLE_PG_READ_ALL_STATS: ::types_core::Oid = 3375;

// Flags for WalSndCtlData->sync_standbys_status (walsender_private.h).
pub const SYNC_STANDBY_INIT: u8 = 1 << 0;
pub const SYNC_STANDBY_DEFINED: u8 = 1 << 1;

// ---------------------------------------------------------------------------
// File-static typedefs (walsender.c) — owned values.
// ---------------------------------------------------------------------------

/// A sample associating a WAL location with the time it was written.
#[derive(Clone, Copy, Debug)]
pub struct WalTimeSample {
    pub lsn: XLogRecPtr,
    pub time: TimestampTz,
}

impl WalTimeSample {
    pub const fn zeroed() -> Self {
        WalTimeSample { lsn: InvalidXLogRecPtr, time: 0 }
    }
}

impl Default for WalTimeSample {
    fn default() -> Self {
        Self::zeroed()
    }
}

/// `struct LagTracker` (walsender.c) — the cyclic buffer of WAL time samples.
pub struct LagTracker {
    pub last_lsn: XLogRecPtr,
    pub buffer: [WalTimeSample; LAG_TRACKER_BUFFER_SIZE],
    pub write_head: c_int,
    pub read_heads: [c_int; NUM_SYNC_REP_WAIT_MODE],
    pub last_read: [WalTimeSample; NUM_SYNC_REP_WAIT_MODE],
    /// Overflow entries for read heads that collide with the write head.
    pub overflowed: [WalTimeSample; NUM_SYNC_REP_WAIT_MODE],
}

impl LagTracker {
    /// Zero-initialized tracker (`MemoryContextAllocZero(sizeof(LagTracker))`).
    pub fn zeroed() -> Self {
        LagTracker {
            last_lsn: InvalidXLogRecPtr,
            buffer: [WalTimeSample::zeroed(); LAG_TRACKER_BUFFER_SIZE],
            write_head: 0,
            read_heads: [0; NUM_SYNC_REP_WAIT_MODE],
            last_read: [WalTimeSample::zeroed(); NUM_SYNC_REP_WAIT_MODE],
            overflowed: [WalTimeSample::zeroed(); NUM_SYNC_REP_WAIT_MODE],
        }
    }
}

/// `typedef void (*WalSndSendDataCallback) (void);`
pub type WalSndSendDataCallback = fn();

// ---------------------------------------------------------------------------
// Process-local walsender state.
// ---------------------------------------------------------------------------

/// All of walsender.c's process-local mutable state, owned in one place.
pub struct WalSndProc {
    /// am I a walsender process?
    pub am_walsender: bool,
    /// am I cascading WAL to another standby?
    pub am_cascading_walsender: bool,
    /// connected to a database?
    pub am_db_walsender: bool,
    /// state for WalSndWakeupRequest.
    pub wake_wal_senders: bool,

    /// `int max_wal_senders = 10;`
    pub max_wal_senders: c_int,
    /// `int wal_sender_timeout = 60 * 1000;` (ms)
    pub wal_sender_timeout: c_int,
    /// `bool log_replication_commands = false;`
    pub log_replication_commands: bool,

    pub sendTimeLine: TimeLineID,
    pub sendTimeLineNextTLI: TimeLineID,
    pub sendTimeLineIsHistoric: bool,
    pub sendTimeLineValidUpto: XLogRecPtr,
    /// next WAL location to send.
    pub sentPtr: XLogRecPtr,
    /// timestamp of last ProcessRepliesIfAny().
    pub last_processing: TimestampTz,
    /// timestamp of last ProcessRepliesIfAny() that saw a standby reply.
    pub last_reply_timestamp: TimestampTz,
    pub waiting_for_ping_response: bool,
    pub streamingDoneSending: bool,
    pub streamingDoneReceiving: bool,
    /// are we there yet?
    pub WalSndCaughtUp: bool,
    pub got_SIGUSR2: sig_atomic_t,
    pub got_STOPPING: sig_atomic_t,
    /// set while streaming.
    pub replication_active: sig_atomic_t,

    // Function-local statics promoted to keep cross-call caching semantics.
    pub WalSndWaitForWal_RecentFlushPtr: XLogRecPtr,
    pub WalSndUpdateProgress_sendTime: TimestampTz,
    pub ProcessStandbyReplyMessage_fullyAppliedLastTime: bool,
    pub XLogSendLogical_flushPtr: XLogRecPtr,

    /// `static LagTracker *lag_tracker;` — allocated at walsender init.
    pub lag_tracker: Option<LagTracker>,

    /// `static WalSnd *MyWalSnd = NULL;` — this backend's index into
    /// `WalSndCtl->walsnds`, or -1 when not yet set.
    pub my_walsnd: c_int,
}

impl WalSndProc {
    const fn seed() -> Self {
        WalSndProc {
            am_walsender: false,
            am_cascading_walsender: false,
            am_db_walsender: false,
            wake_wal_senders: false,
            max_wal_senders: 10,
            wal_sender_timeout: 60 * 1000,
            log_replication_commands: false,
            sendTimeLine: 0,
            sendTimeLineNextTLI: 0,
            sendTimeLineIsHistoric: false,
            sendTimeLineValidUpto: InvalidXLogRecPtr,
            sentPtr: InvalidXLogRecPtr,
            last_processing: 0,
            last_reply_timestamp: 0,
            waiting_for_ping_response: false,
            streamingDoneSending: false,
            streamingDoneReceiving: false,
            WalSndCaughtUp: false,
            got_SIGUSR2: 0,
            got_STOPPING: 0,
            replication_active: 0,
            WalSndWaitForWal_RecentFlushPtr: InvalidXLogRecPtr,
            WalSndUpdateProgress_sendTime: 0,
            ProcessStandbyReplyMessage_fullyAppliedLastTime: false,
            XLogSendLogical_flushPtr: InvalidXLogRecPtr,
            lag_tracker: None,
            my_walsnd: -1,
        }
    }
}

impl Default for WalSndProc {
    fn default() -> Self {
        WalSndProc::seed()
    }
}

// Single-threaded process-local home for the walsender state.
struct ProcCell(RefCell<WalSndProc>);
// SAFETY: walsenders are single-threaded per process.
unsafe impl Sync for ProcCell {}

static PROC: ProcCell = ProcCell(RefCell::new(WalSndProc::seed()));

/// Run `f` with mutable access to the process-local walsender state.
#[inline]
pub fn with_proc<R>(f: impl FnOnce(&mut WalSndProc) -> R) -> R {
    f(&mut PROC.0.borrow_mut())
}

/// Read the process state.
#[inline]
pub fn proc_get<R>(f: impl FnOnce(&WalSndProc) -> R) -> R {
    f(&PROC.0.borrow())
}

// The file-static `output_message` StringInfo (walsender.c): the reusable
// libpq output buffer the keepalive / WAL-data paths frame into.
struct OutputMessageCell(RefCell<alloc::vec::Vec<u8>>);
// SAFETY: walsenders are single-threaded per process (see ProcCell).
unsafe impl Sync for OutputMessageCell {}

static OUTPUT_MESSAGE: OutputMessageCell = OutputMessageCell(RefCell::new(alloc::vec::Vec::new()));

/// Run `f` with mutable access to the process-local `output_message` buffer
/// (`StringInfoData output_message`, walsender.c).
#[inline]
pub fn with_output_message<R>(f: impl FnOnce(&mut alloc::vec::Vec<u8>) -> R) -> R {
    f(&mut OUTPUT_MESSAGE.0.borrow_mut())
}

// ---------------------------------------------------------------------------
// The shared-memory WalSndCtlData array (`WalSndCtl`).
//
// Owned here as a real `#[repr(C)]` shmem struct, allocated through
// `ShmemInitStruct` in WalSndShmemInit, exactly as slotsync.c's SlotSyncCtx is
// owned by its crate.  The per-slot `mutex` is a real spinlock; the three CVs
// are real condition variables reached through the condition-variable seams.
// ---------------------------------------------------------------------------

/// `typedef struct WalSnd` (`replication/walsender_private.h`).  The live
/// shmem slot.  `#[repr(C)]` so the flexible-array layout matches the
/// `ShmemInitStruct` reservation.
#[repr(C)]
pub struct WalSndSlot {
    pub pid: pid_t,
    pub state: WalSndState,
    pub sentPtr: XLogRecPtr,
    pub needreload: bool,
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,
    pub writeLag: ::types_datetime::TimeOffset,
    pub flushLag: ::types_datetime::TimeOffset,
    pub applyLag: ::types_datetime::TimeOffset,
    pub sync_standby_priority: c_int,
    pub mutex: Spinlock,
    pub replyTime: TimestampTz,
    pub kind: ReplicationKind,
}

/// `typedef struct WalSndCtlData` (`replication/walsender_private.h`).  One per
/// cluster.  The `walsnds[]` flexible-array member is reached via pointer
/// arithmetic off the end of the fixed header, mirroring the C layout produced
/// by `ShmemInitStruct(WalSndShmemSize())`.
#[repr(C)]
pub struct WalSndCtlData {
    /// `dlist_head SyncRepQueue[NUM_SYNC_REP_WAIT_MODE]` — owned by syncrep.c;
    /// stored here (the struct's owner) and operated on by syncrep via the
    /// `with_sync_rep_queue` accessor. Modeled as a `proclist_head` (pgprocno
    /// head/tail) — the shmem-safe intrusive representation, exactly like the
    /// LWLock/CV wait lists; the per-PGPROC links live in `syncRepLinks`.
    pub SyncRepQueue: [::types_storage::storage::proclist_head; NUM_SYNC_REP_WAIT_MODE],
    /// `XLogRecPtr lsn[NUM_SYNC_REP_WAIT_MODE]`.
    pub lsn: [XLogRecPtr; NUM_SYNC_REP_WAIT_MODE],
    /// `bits8 sync_standbys_status`.
    pub sync_standbys_status: u8,
    pub wal_flush_cv: ConditionVariable,
    pub wal_replay_cv: ConditionVariable,
    pub wal_confirm_rcv_cv: ConditionVariable,
    // `WalSnd walsnds[FLEXIBLE_ARRAY_MEMBER]` follows in shmem; reached via
    // `walsnds_ptr()`.
}

/// `static WalSndCtlData *WalSndCtl = NULL;` — this backend's mapped pointer.
struct WalSndCtlCell(Cell<*mut WalSndCtlData>);
// SAFETY: single-threaded per process; the shared data is protected by the
// per-slot spinlocks and the CV mutexes.
unsafe impl Sync for WalSndCtlCell {}

static WAL_SND_CTL: WalSndCtlCell = WalSndCtlCell(Cell::new(core::ptr::null_mut()));

/// Store the mapped `WalSndCtl` pointer (set in `WalSndShmemInit`).
#[inline]
pub fn set_wal_snd_ctl(p: *mut WalSndCtlData) {
    WAL_SND_CTL.0.set(p);
}

/// `WalSndCtl` (panics if accessed before `WalSndShmemInit`, like C
/// dereferencing a NULL `WalSndCtl`).
#[inline]
pub fn wal_snd_ctl<'a>() -> &'a WalSndCtlData {
    let p = WAL_SND_CTL.0.get();
    assert!(!p.is_null(), "WalSndCtl accessed before WalSndShmemInit");
    unsafe { &*p }
}

#[inline]
pub fn wal_snd_ctl_mut<'a>() -> &'a mut WalSndCtlData {
    let p = WAL_SND_CTL.0.get();
    assert!(!p.is_null(), "WalSndCtl accessed before WalSndShmemInit");
    unsafe { &mut *p }
}

#[inline]
pub fn wal_snd_ctl_is_set() -> bool {
    !WAL_SND_CTL.0.get().is_null()
}

/// `&WalSndCtl->walsnds[i]` — pointer into the flexible-array member, which
/// starts immediately after the fixed `WalSndCtlData` header.
#[inline]
pub fn walsnds_slot<'a>(i: c_int) -> &'a WalSndSlot {
    let base = WAL_SND_CTL.0.get();
    assert!(!base.is_null(), "WalSndCtl accessed before WalSndShmemInit");
    unsafe {
        let arr = (base as *mut u8).add(core::mem::size_of::<WalSndCtlData>()) as *const WalSndSlot;
        &*arr.add(i as usize)
    }
}

#[inline]
pub fn walsnds_slot_mut<'a>(i: c_int) -> &'a mut WalSndSlot {
    let base = WAL_SND_CTL.0.get();
    assert!(!base.is_null(), "WalSndCtl accessed before WalSndShmemInit");
    unsafe {
        let arr = (base as *mut u8).add(core::mem::size_of::<WalSndCtlData>()) as *mut WalSndSlot;
        &mut *arr.add(i as usize)
    }
}

/// `SpinLockAcquire(&walsnd->mutex)`.
#[inline]
pub fn slot_spin_acquire(slot: &WalSndSlot) {
    if s_lock::tas(&slot.mutex) != 0 {
        s_lock::s_lock(&slot.mutex, None, 0, None);
    }
}

/// `SpinLockRelease(&walsnd->mutex)`.
#[inline]
pub fn slot_spin_release(slot: &WalSndSlot) {
    s_lock::s_unlock(&slot.mutex);
}

/// Run `f` with mutable access to `WalSndCtl->SyncRepQueue[mode]` (a
/// `proclist_head`). syncrep.c owns these queue heads but they are stored in
/// this crate's shmem struct, so the queue operations reach them through here
/// while holding `SyncRepLock`.
#[inline]
pub fn with_sync_rep_queue<R>(
    mode: usize,
    f: impl FnOnce(&mut ::types_storage::storage::proclist_head) -> R,
) -> R {
    f(&mut wal_snd_ctl_mut().SyncRepQueue[mode])
}

/// `WalSndCtl->lsn[mode]`.
#[inline]
pub fn ctl_lsn(mode: usize) -> XLogRecPtr {
    wal_snd_ctl().lsn[mode]
}

/// `WalSndCtl->lsn[mode] = lsn`.
#[inline]
pub fn set_ctl_lsn(mode: usize, lsn: XLogRecPtr) {
    wal_snd_ctl_mut().lsn[mode] = lsn;
}

/// `WalSndCtl->sync_standbys_status = status`.
#[inline]
pub fn set_ctl_sync_standbys_status(status: u8) {
    wal_snd_ctl_mut().sync_standbys_status = status;
}

/// `am_cascading_walsender` — whether this walsender is cascading WAL to
/// another standby (synchronous cascade replication is not allowed, so a
/// cascading sender always gets priority zero).
pub fn am_cascading_walsender() -> bool {
    proc_get(|p| p.am_cascading_walsender)
}

/// `MyWalSnd->state` (under the slot mutex). `MyWalSnd` must be set.
pub fn WalSndGetState() -> WalSndState {
    let slot = walsnds_slot(crate::shmem_array::my_walsnd_index());
    slot_spin_acquire(slot);
    let s = slot.state;
    slot_spin_release(slot);
    s
}

/// `MyWalSnd->flush` (under the slot mutex). `MyWalSnd` must be set.
pub fn WalSndGetFlush() -> XLogRecPtr {
    let slot = walsnds_slot(crate::shmem_array::my_walsnd_index());
    slot_spin_acquire(slot);
    let f = slot.flush;
    slot_spin_release(slot);
    f
}

/// The fields `SyncRepGetCandidateStandbys` snapshots from `WalSndCtl->walsnds[i]`
/// under the slot mutex (`SpinLockAcquire(&walsnd->mutex)` ... `SpinLockRelease`),
/// plus whether the slot is `MyWalSnd`.
#[derive(Clone, Copy, Debug)]
pub struct WalSndCandidate {
    pub pid: pid_t,
    pub state: WalSndState,
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,
    pub sync_standby_priority: c_int,
    pub is_me: bool,
}

/// Snapshot `WalSndCtl->walsnds[i]` under its spinlock for sync-standby
/// candidate selection. `is_me` is `(&walsnds[i] == MyWalSnd)`.
pub fn walsnd_candidate_snapshot(i: c_int) -> WalSndCandidate {
    let slot = walsnds_slot(i);
    slot_spin_acquire(slot);
    let snap = WalSndCandidate {
        pid: slot.pid,
        state: slot.state,
        write: slot.write,
        flush: slot.flush,
        apply: slot.apply,
        sync_standby_priority: slot.sync_standby_priority,
        is_me: crate::shmem_array::my_walsnd_is_set()
            && i == crate::shmem_array::my_walsnd_index(),
    };
    slot_spin_release(slot);
    snap
}

/// `MyWalSnd->sync_standby_priority` (under the slot mutex).
pub fn my_sync_standby_priority() -> c_int {
    let slot = walsnds_slot(crate::shmem_array::my_walsnd_index());
    slot_spin_acquire(slot);
    let p = slot.sync_standby_priority;
    slot_spin_release(slot);
    p
}

/// `SpinLockAcquire(&MyWalSnd->mutex); MyWalSnd->sync_standby_priority = prio;
/// SpinLockRelease;` — `SyncRepInitConfig`'s priority publish.
pub fn set_my_sync_standby_priority(prio: c_int) {
    let idx = crate::shmem_array::my_walsnd_index();
    let slot = walsnds_slot_mut(idx);
    slot_spin_acquire(slot);
    slot.sync_standby_priority = prio;
    slot_spin_release(slot);
}

/// A `Size`-typed shmem accumulation total (kept as `Size`).
pub type SizeT = Size;
