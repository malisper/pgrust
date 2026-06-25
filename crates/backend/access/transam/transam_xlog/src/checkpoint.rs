//! The checkpoint creation path of `access/transam/xlog.c` (PostgreSQL 18.3):
//! [`CreateCheckPoint`] (xlog.c:6951), [`CheckPointGuts`] (xlog.c:7574), and
//! [`CreateRestartPoint`] (xlog.c:7655).
//!
//! The control flow is ported 1:1 from the C. The fields C threads through
//! file-scope globals (`RedoRecPtr`, the in-memory `*ControlFile`,
//! `LocalXLogInsertAllowed`, `CheckpointStats`) are owned by [`CheckpointState`]
//! and threaded as `&mut CheckpointState` — the running checkpointer is the sole
//! mutator of this state, so it is owned, not reached through a `static mut`
//! shmem pointer.
//!
//! The genuinely-external cross-subsystem reads/calls (the WAL-insert-lock
//! driver, the buffer-pool/SLRU/replication checkpoint callbacks, the
//! TransamVariables/multixact/commit-ts/procarray snapshots, the WAL-segment
//! recycling, slot invalidation, sync-request drain, checkpoint logging) cross
//! into subsystems that are not ported on this frontier. Each is a deferred
//! external in [`ext`]: it panics loudly (the `xlog-checkpoint-deps` debt) until
//! the owning subsystem lands. The checkpoint LOGIC itself — every branch, the
//! REDO-pointer computation, the control-file content updates, the retention
//! horizon recompute — is present and faithful here. See DESIGN_DEBT.md.

#![allow(non_snake_case)]

use alloc::vec::Vec;

use ::utils_error::{PgError, PgResult};

use ::control::{CheckPoint, ControlFileData, DBState};
use ::types_core::{FullTransactionId, InvalidTransactionId, TimeLineID, TimestampTz, XLogRecPtr};
use ::wal::xlog_consts::{
    CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_FORCE, CHECKPOINT_IS_SHUTDOWN, DELAY_CHKPT_COMPLETE,
    DELAY_CHKPT_START, SIZE_OF_XLOG_LONG_PHD, SIZE_OF_XLOG_SHORT_PHD,
};

use crate::{
    InvalidXLogRecPtr, WalConfig, XLByteToSeg, XLogRecPtrIsInvalid, XLogSegmentOffset, XLOG_BLCKSZ,
};

/// `RS_INVAL_WAL_REMOVED` (replication/slot.h) — `1 << 0`.
const RS_INVAL_WAL_REMOVED: i32 = 1 << 0;
/// `RS_INVAL_IDLE_TIMEOUT` (replication/slot.h) — `1 << 3`.
const RS_INVAL_IDLE_TIMEOUT: i32 = 1 << 3;

/// `CheckpointStatsData` (xlog.c) — accumulated per-checkpoint timing/counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckpointStats {
    pub ckpt_start_t: TimestampTz,
    pub ckpt_write_t: TimestampTz,
    pub ckpt_sync_t: TimestampTz,
    pub ckpt_sync_end_t: TimestampTz,
    pub ckpt_end_t: TimestampTz,
    /// `ckpt_segs_added` — # of new xlog segments created (incremented by
    /// `PreallocXlogFiles`).
    pub ckpt_segs_added: i32,
    /// `ckpt_segs_removed` — # of old xlog segments deleted (incremented by
    /// `RemoveXlogFile`).
    pub ckpt_segs_removed: i32,
    /// `ckpt_segs_recycled` — # of old xlog segments recycled into future
    /// segments (incremented by `RemoveXlogFile`). The remaining C
    /// `CheckpointStatsData` counters (`ckpt_bufs_written`, …) are not yet
    /// modeled.
    pub ckpt_segs_recycled: i32,
}

/// The checkpoint process's owned file-scope state (xlog.c file-scope globals
/// `RedoRecPtr`, the in-memory `*ControlFile`, `LocalXLogInsertAllowed`).
pub struct CheckpointState {
    pub RedoRecPtr: XLogRecPtr,
    pub ControlFile: ControlFileData,
    pub LocalXLogInsertAllowed: i32,
    pub config: WalConfig,
    pub stats: CheckpointStats,
}

impl CheckpointState {
    /// Construct the checkpoint state from the current control file + WAL config.
    pub fn new(control_file: ControlFileData, config: WalConfig) -> Self {
        CheckpointState {
            RedoRecPtr: control_file.checkPointCopy.redo,
            ControlFile: control_file,
            LocalXLogInsertAllowed: -1,
            config,
            stats: CheckpointStats::default(),
        }
    }
}

// ===========================================================================
// CheckPoint <-> on-disk byte image (C-ABI, 88 bytes, LP64).
// ===========================================================================

/// `sizeof(CheckPoint)` on LP64 (catalog/pg_control.h).
pub const SIZE_OF_CHECK_POINT: usize = 88;

/// Serialize a [`CheckPoint`] into its 88-byte C-ABI image (field order +
/// alignment padding identical to the C struct).
pub fn checkpoint_to_bytes(cp: &CheckPoint) -> Vec<u8> {
    let mut b = Vec::with_capacity(SIZE_OF_CHECK_POINT);
    // redo: XLogRecPtr (u64)               @0
    b.extend_from_slice(&cp.redo.to_ne_bytes());
    // ThisTimeLineID: TimeLineID (u32)     @8
    b.extend_from_slice(&cp.ThisTimeLineID.to_ne_bytes());
    // PrevTimeLineID: TimeLineID (u32)     @12
    b.extend_from_slice(&cp.PrevTimeLineID.to_ne_bytes());
    // fullPageWrites: bool (1) + 3 pad     @16
    b.push(cp.fullPageWrites as u8);
    b.extend_from_slice(&[0u8; 3]);
    // wal_level: int (i32)                 @20
    b.extend_from_slice(&cp.wal_level.to_ne_bytes());
    // nextXid: FullTransactionId (u64)     @24
    b.extend_from_slice(&cp.nextXid.value.to_ne_bytes());
    // nextOid: Oid (u32)                   @32
    b.extend_from_slice(&cp.nextOid.to_ne_bytes());
    // nextMulti: MultiXactId (u32)         @36
    b.extend_from_slice(&cp.nextMulti.to_ne_bytes());
    // nextMultiOffset: MultiXactOffset(u32)@40
    b.extend_from_slice(&cp.nextMultiOffset.to_ne_bytes());
    // oldestXid: TransactionId (u32)       @44
    b.extend_from_slice(&cp.oldestXid.to_ne_bytes());
    // oldestXidDB: Oid (u32)               @48
    b.extend_from_slice(&cp.oldestXidDB.to_ne_bytes());
    // oldestMulti: MultiXactId (u32)       @52
    b.extend_from_slice(&cp.oldestMulti.to_ne_bytes());
    // oldestMultiDB: Oid (u32)             @56
    b.extend_from_slice(&cp.oldestMultiDB.to_ne_bytes());
    // time: pg_time_t (i64)                @60 -> 8-align: pad to @64
    b.extend_from_slice(&[0u8; 4]);
    b.extend_from_slice(&cp.time.to_ne_bytes());
    // oldestCommitTsXid: TransactionId(u32)@72
    b.extend_from_slice(&cp.oldestCommitTsXid.to_ne_bytes());
    // newestCommitTsXid: TransactionId(u32)@76
    b.extend_from_slice(&cp.newestCommitTsXid.to_ne_bytes());
    // oldestActiveXid: TransactionId (u32) @80 + 4 tail pad -> 88
    b.extend_from_slice(&cp.oldestActiveXid.to_ne_bytes());
    b.extend_from_slice(&[0u8; 4]);
    debug_assert_eq!(b.len(), SIZE_OF_CHECK_POINT);
    b
}

/// `INSERT_FREESPACE(endptr)` = `XLOG_BLCKSZ - (endptr % XLOG_BLCKSZ)`, 0 at a
/// page boundary.
#[inline]
fn insert_freespace(endptr: XLogRecPtr) -> u32 {
    let rem = (endptr as usize) % XLOG_BLCKSZ;
    if rem == 0 {
        0
    } else {
        (XLOG_BLCKSZ - rem) as u32
    }
}

// ===========================================================================
// CreateCheckPoint — xlog.c:6951.
// ===========================================================================

/// Perform a checkpoint — either during shutdown, or on-the-fly. `flags` is a
/// bitwise OR of the `CHECKPOINT_*` constants. Returns `true` if a new
/// checkpoint was performed, `false` if skipped (system idle). Faithful to
/// `CreateCheckPoint` (xlog.c:6951).
pub fn CreateCheckPoint(st: &mut CheckpointState, flags: i32) -> PgResult<bool> {
    let wal_segment_size = st.config.wal_segment_size;
    let wal_level = st.config.wal_level as i32;

    let shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) != 0;

    if ext::RecoveryInProgress() && (flags & CHECKPOINT_END_OF_RECOVERY) == 0 {
        return Err(PgError::error("can't create a checkpoint during recovery"));
    }

    st.stats = CheckpointStats::default();
    st.stats.ckpt_start_t = ext::GetCurrentTimestamp();

    ext::SyncPreCheckpoint();

    if shutdown {
        st.ControlFile.state = DBState::Shutdowning;
        update_control_file(st);
    }

    let mut checkPoint = CheckPoint::default();
    checkPoint.time = ext::GetWallClockTime();

    if !shutdown && ext::XLogStandbyInfoActive() {
        checkPoint.oldestActiveXid = ext::GetOldestActiveTransactionId();
    } else {
        checkPoint.oldestActiveXid = InvalidTransactionId;
    }

    let last_important_lsn = ext::GetLastImportantRecPtr();

    if (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE)) == 0
        && last_important_lsn == st.ControlFile.checkPoint
    {
        return Ok(false);
    }

    let mut old_xlog_allowed: i32 = 0;
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        old_xlog_allowed = local_set_xlog_insert_allowed(st);
    }

    checkPoint.ThisTimeLineID = ext::GetWALInsertionTimeLine();
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        checkPoint.PrevTimeLineID = ext::GetWALPrevTimeLine();
    } else {
        checkPoint.PrevTimeLineID = checkPoint.ThisTimeLineID;
    }

    ext::WALInsertLockAcquireExclusive();

    checkPoint.fullPageWrites = ext::GetInsertFullPageWrites();
    checkPoint.wal_level = wal_level;

    if shutdown {
        let mut cur_insert = ext::GetXLogInsertRecPtr();
        let freespace = insert_freespace(cur_insert);
        if freespace == 0 {
            if XLogSegmentOffset(cur_insert, wal_segment_size) == 0 {
                cur_insert += SIZE_OF_XLOG_LONG_PHD as u64;
            } else {
                cur_insert += SIZE_OF_XLOG_SHORT_PHD as u64;
            }
        }
        checkPoint.redo = cur_insert;

        st.RedoRecPtr = checkPoint.redo;
        set_shared_redo_rec_ptr(checkPoint.redo);
    }

    ext::WALInsertLockRelease();

    if !shutdown {
        st.RedoRecPtr = ext::InsertCheckpointRedoRecord(wal_level);
        checkPoint.redo = st.RedoRecPtr;
    }

    set_shared_redo_rec_ptr(checkPoint.redo);

    if st.config.log_checkpoints {
        ext::LogCheckpointStart(flags, false);
    }

    ext::UpdateCheckpointDisplay(flags, false, false);

    let (next_xid, oldest_xid, oldest_xid_db) = ext::GetCheckpointXidSnapshot();
    checkPoint.nextXid = next_xid;
    checkPoint.oldestXid = oldest_xid;
    checkPoint.oldestXidDB = oldest_xid_db;

    let (oldest_cts, newest_cts) = ext::GetCheckpointCommitTsSnapshot();
    checkPoint.oldestCommitTsXid = oldest_cts;
    checkPoint.newestCommitTsXid = newest_cts;

    checkPoint.nextOid = ext::GetCheckpointNextOid(!shutdown);

    let (next_multi, next_multi_off, oldest_multi, oldest_multi_db) =
        ext::MultiXactGetCheckptMulti(shutdown);
    checkPoint.nextMulti = next_multi;
    checkPoint.nextMultiOffset = next_multi_off;
    checkPoint.oldestMulti = oldest_multi;
    checkPoint.oldestMultiDB = oldest_multi_db;

    wait_for_delaying_backends(DELAY_CHKPT_START);

    CheckPointGuts(st, checkPoint.redo, flags);

    wait_for_delaying_backends(DELAY_CHKPT_COMPLETE);

    if !shutdown && ext::XLogStandbyInfoActive() {
        ext::LogStandbySnapshot();
    }

    let cp_bytes = checkpoint_to_bytes(&checkPoint);
    let (proc_last_rec_ptr, recptr) = ext::InsertCheckpointRecord(cp_bytes, shutdown);

    crate::XLogFlush(recptr);

    if shutdown {
        if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
            st.LocalXLogInsertAllowed = old_xlog_allowed;
        } else {
            st.LocalXLogInsertAllowed = 0;
        }
    }

    if shutdown && checkPoint.redo != proc_last_rec_ptr {
        return Err(PgError::error(
            "concurrent write-ahead log activity while database system is shutting down",
        ));
    }

    let prior_redo_ptr = st.ControlFile.checkPointCopy.redo;

    if shutdown {
        st.ControlFile.state = DBState::Shutdowned;
    }
    st.ControlFile.checkPoint = proc_last_rec_ptr;
    st.ControlFile.checkPointCopy = checkPoint;
    st.ControlFile.minRecoveryPoint = InvalidXLogRecPtr;
    st.ControlFile.minRecoveryPointTLI = 0;
    st.ControlFile.unloggedLSN = ext::GetFakeLSNForUnloggedRel();
    update_control_file(st);

    set_ckpt_full_xid(checkPoint.nextXid);

    ext::WakeupWalSummarizer();

    ext::SyncPostCheckpoint();

    if prior_redo_ptr != InvalidXLogRecPtr {
        ext::UpdateCheckPointDistanceEstimate(st.RedoRecPtr.wrapping_sub(prior_redo_ptr));
    }

    let mut log_seg_no = XLByteToSeg(st.RedoRecPtr, wal_segment_size);
    log_seg_no = ext::KeepLogSeg(recptr, log_seg_no);
    if ext::InvalidateObsoleteReplicationSlots(RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT, log_seg_no)
    {
        log_seg_no = XLByteToSeg(st.RedoRecPtr, wal_segment_size);
        log_seg_no = ext::KeepLogSeg(recptr, log_seg_no);
    }
    log_seg_no = log_seg_no.wrapping_sub(1);
    ext::RemoveOldXlogFiles(log_seg_no, st.RedoRecPtr, recptr, checkPoint.ThisTimeLineID);

    if !shutdown {
        crate::write::PreallocXlogFiles(recptr, checkPoint.ThisTimeLineID, &mut st.stats)?;
    }

    if !ext::RecoveryInProgress() {
        ext::TruncateSUBTRANS(ext::GetOldestTransactionIdConsideredRunning());
    }

    ext::LogCheckpointEnd(false);

    ext::UpdateCheckpointDisplay(flags, false, true);

    Ok(true)
}

// ===========================================================================
// CheckPointGuts — xlog.c:7574.
// ===========================================================================

/// Flush all data in shared memory to disk, and fsync. The common code shared
/// between regular checkpoints and recovery restartpoints. Faithful to
/// `CheckPointGuts` (xlog.c:7574).
pub fn CheckPointGuts(st: &mut CheckpointState, check_point_redo: XLogRecPtr, flags: i32) {
    st.stats.ckpt_write_t = ext::GetCurrentTimestamp();
    ext::CheckPointGutsCallbacks(check_point_redo, flags);

    ext::CheckPointBuffers(flags);

    st.stats.ckpt_sync_t = ext::GetCurrentTimestamp();
    ext::ProcessSyncRequests();
    st.stats.ckpt_sync_end_t = ext::GetCurrentTimestamp();
}

// ===========================================================================
// CreateRestartPoint — xlog.c:7655.
// ===========================================================================

/// Establish a restartpoint if possible (during WAL recovery). Returns `true`
/// if a new restartpoint was established. Faithful to `CreateRestartPoint`
/// (xlog.c:7655).
pub fn CreateRestartPoint(st: &mut CheckpointState, flags: i32) -> PgResult<bool> {
    let wal_segment_size = st.config.wal_segment_size;

    let (last_check_point_rec_ptr, last_check_point_end_ptr, last_check_point) =
        ext::GetLastCheckPointForRestart();

    if !ext::RecoveryInProgress() {
        return Ok(false);
    }

    if XLogRecPtrIsInvalid(last_check_point_rec_ptr)
        || last_check_point.redo <= st.ControlFile.checkPointCopy.redo
    {
        ext::UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);
        if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
            st.ControlFile.state = DBState::ShutdownedInRecovery;
            update_control_file(st);
        }
        return Ok(false);
    }

    ext::WALInsertLockAcquireExclusive();
    st.RedoRecPtr = last_check_point.redo;
    set_shared_redo_rec_ptr(last_check_point.redo);
    ext::WALInsertLockRelease();
    set_shared_redo_rec_ptr(last_check_point.redo);

    st.stats = CheckpointStats::default();
    st.stats.ckpt_start_t = ext::GetCurrentTimestamp();

    if st.config.log_checkpoints {
        ext::LogCheckpointStart(flags, true);
    }
    ext::UpdateCheckpointDisplay(flags, true, false);

    CheckPointGuts(st, last_check_point.redo, flags);

    let prior_redo_ptr = st.ControlFile.checkPointCopy.redo;

    if st.ControlFile.checkPointCopy.redo < last_check_point.redo {
        st.ControlFile.checkPoint = last_check_point_rec_ptr;
        st.ControlFile.checkPointCopy = last_check_point;

        if st.ControlFile.state == DBState::InArchiveRecovery {
            if st.ControlFile.minRecoveryPoint < last_check_point_end_ptr {
                st.ControlFile.minRecoveryPoint = last_check_point_end_ptr;
                st.ControlFile.minRecoveryPointTLI = last_check_point.ThisTimeLineID;
            }
            if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                st.ControlFile.state = DBState::ShutdownedInRecovery;
            }
        }
        update_control_file(st);
    }

    if prior_redo_ptr != InvalidXLogRecPtr {
        ext::UpdateCheckPointDistanceEstimate(st.RedoRecPtr.wrapping_sub(prior_redo_ptr));
    }

    let mut log_seg_no = XLByteToSeg(st.RedoRecPtr, wal_segment_size);

    let receive_ptr = ext::GetWalRcvFlushRecPtr();
    let (replay_ptr, mut replay_tli) = ext::GetXLogReplayRecPtr();
    let endptr = if receive_ptr < replay_ptr {
        replay_ptr
    } else {
        receive_ptr
    };
    log_seg_no = ext::KeepLogSeg(endptr, log_seg_no);

    if ext::InvalidateObsoleteReplicationSlots(RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT, log_seg_no)
    {
        log_seg_no = XLByteToSeg(st.RedoRecPtr, wal_segment_size);
        log_seg_no = ext::KeepLogSeg(endptr, log_seg_no);
    }
    log_seg_no = log_seg_no.wrapping_sub(1);

    if !ext::RecoveryInProgress() {
        replay_tli = ext::GetWALInsertionTimeLine();
    }
    ext::RemoveOldXlogFiles(log_seg_no, st.RedoRecPtr, endptr, replay_tli);

    crate::write::PreallocXlogFiles(endptr, replay_tli, &mut st.stats)?;

    if st.config.EnableHotStandby {
        ext::TruncateSUBTRANS(ext::GetOldestTransactionIdConsideredRunning());
    }

    ext::LogCheckpointEnd(true);
    ext::UpdateCheckpointDisplay(flags, true, true);

    ext::MaybeRunArchiveCleanupCommand();

    Ok(true)
}

// ===========================================================================
// Local helpers mirroring the inline file-scope macros / setters.
// ===========================================================================

/// `PreallocXlogFiles(EndOfLog, newTLI)` (xlog.c:6133) — the WAL-startup driver
/// (`StartupXLOG`) preallocates additional log files past the end of WAL. In C
/// this updates the file-static `CheckpointStats.ckpt_segs_added`; at this point
/// in startup the global stats are scratch (reset by the next
/// `LogCheckpointStart`), so we pass a throwaway counter here.
pub(crate) fn prealloc_xlog_files(endptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    let mut stats = CheckpointStats::default();
    crate::write::PreallocXlogFiles(endptr, tli, &mut stats)
}

/// `LocalSetXLogInsertAllowed()` (xlog.c) — set `LocalXLogInsertAllowed = 1`,
/// returning the old value.
fn local_set_xlog_insert_allowed(st: &mut CheckpointState) -> i32 {
    let old = st.LocalXLogInsertAllowed;
    st.LocalXLogInsertAllowed = 1;
    old
}

/// `UpdateControlFile()` (xlog.c) — persist the in-memory `ControlFileData`. The
/// checkpoint body has set the contents (grounded above); the disk write is the
/// deferred control-file codec/I/O leg.
fn update_control_file(st: &CheckpointState) {
    ext::PersistControlFile(st.ControlFile);
}

/// `RedoRecPtr = XLogCtl->RedoRecPtr = redo` shmem publish. The shmem copy lives
/// in the deferred XLogCtl driver; the authoritative backend-local value is
/// `st.RedoRecPtr`. Publishing to shmem is the driver's job.
fn set_shared_redo_rec_ptr(redo: XLogRecPtr) {
    ext::SetSharedRedoRecPtr(redo);
}

/// `XLogCtl->ckptFullXid = nextXid` under info_lck — a deferred XLogCtl shmem
/// publish.
fn set_ckpt_full_xid(next_xid: FullTransactionId) {
    ext::SetCkptFullXid(next_xid);
}

/// The `GetVirtualXIDsDelayingChkpt` / `HaveVirtualXIDsDelayingChkpt` wait loop
/// (xlog.c:7221-7256). Drains the fsync queue while waiting.
fn wait_for_delaying_backends(delay_type: i32) {
    let vxids = ext::GetVirtualXIDsDelayingChkpt(delay_type);
    if !vxids.is_empty() {
        loop {
            ext::AbsorbSyncRequests();
            if !ext::HaveVirtualXIDsDelayingChkpt(vxids.clone(), delay_type) {
                break;
            }
        }
    }
}

// ===========================================================================
// Deferred cross-subsystem externals (xlog-checkpoint-deps debt).
//
// Each of these is owned by a subsystem not ported on this frontier (the
// WAL-insert-lock driver, bufmgr, the SLRU/replication checkpoint callbacks,
// varsup/multixact/commit-ts snapshots, sync.c, slot.c, subtrans, walsummarizer,
// walreceiver, xlogrecovery, the recovery-command runner, the control-file disk
// codec, and the XLogInsert engine for the two checkpoint records). They panic
// loudly until the owner lands — never a silent stub. See DESIGN_DEBT.md.
// ===========================================================================
mod ext {
    use super::*;
    use ::types_core::{MultiXactId, MultiXactOffset, Oid, TimeLineID, TransactionId};

    macro_rules! deferred {
        ($( $(#[$attr:meta])* pub fn $name:ident ( $($arg:ident : $argty:ty),* $(,)? ) $(-> $ret:ty)? ; )+) => {
            $(
                $(#[$attr])*
                pub fn $name ( $($arg : $argty),* ) $(-> $ret)? {
                    $( let _ = &$arg; )*
                    panic!(concat!(
                        "checkpoint dependency not ported (xlog-checkpoint-deps debt): ",
                        stringify!($name)
                    ))
                }
            )+
        };
    }

    deferred! {
        pub fn RecoveryInProgress() -> bool;
        pub fn GetCurrentTimestamp() -> TimestampTz;
        pub fn GetWallClockTime() -> ::types_core::pg_time_t;
        pub fn SyncPreCheckpoint();
        pub fn SyncPostCheckpoint();
        pub fn XLogStandbyInfoActive() -> bool;
        pub fn GetOldestActiveTransactionId() -> TransactionId;
        pub fn GetLastImportantRecPtr() -> XLogRecPtr;
        pub fn GetWALInsertionTimeLine() -> TimeLineID;
        pub fn GetWALPrevTimeLine() -> TimeLineID;
        pub fn WALInsertLockAcquireExclusive();
        pub fn WALInsertLockRelease();
        pub fn GetInsertFullPageWrites() -> bool;
        pub fn GetXLogInsertRecPtr() -> XLogRecPtr;
        pub fn InsertCheckpointRedoRecord(wal_level: i32) -> XLogRecPtr;
        pub fn LogCheckpointStart(flags: i32, restartpoint: bool);
        pub fn LogCheckpointEnd(restartpoint: bool);
        pub fn UpdateCheckpointDisplay(flags: i32, restartpoint: bool, reset: bool);
        pub fn GetCheckpointXidSnapshot() -> (FullTransactionId, TransactionId, Oid);
        pub fn GetCheckpointCommitTsSnapshot() -> (TransactionId, TransactionId);
        pub fn GetCheckpointNextOid(include_oidcount: bool) -> Oid;
        pub fn MultiXactGetCheckptMulti(shutdown: bool) -> (MultiXactId, MultiXactOffset, MultiXactId, Oid);
        pub fn LogStandbySnapshot() -> XLogRecPtr;
        pub fn InsertCheckpointRecord(checkpoint_bytes: Vec<u8>, shutdown: bool) -> (XLogRecPtr, XLogRecPtr);
        pub fn GetFakeLSNForUnloggedRel() -> XLogRecPtr;
        pub fn WakeupWalSummarizer();
        pub fn UpdateCheckPointDistanceEstimate(nbytes: u64);
        pub fn KeepLogSeg(recptr: XLogRecPtr, log_seg_no: ::types_core::XLogSegNo) -> ::types_core::XLogSegNo;
        pub fn InvalidateObsoleteReplicationSlots(cause: i32, oldest_seg_no: ::types_core::XLogSegNo) -> bool;
        pub fn RemoveOldXlogFiles(segno: ::types_core::XLogSegNo, lastredoptr: XLogRecPtr, endptr: XLogRecPtr, insert_tli: TimeLineID);
        pub fn TruncateSUBTRANS(oldest_xact: TransactionId);
        pub fn GetOldestTransactionIdConsideredRunning() -> TransactionId;
        pub fn CheckPointGutsCallbacks(checkpoint_redo: XLogRecPtr, flags: i32);
        pub fn CheckPointBuffers(flags: i32);
        pub fn ProcessSyncRequests();
        pub fn AbsorbSyncRequests();
        pub fn GetVirtualXIDsDelayingChkpt(delay_type: i32) -> Vec<u8>;
        pub fn HaveVirtualXIDsDelayingChkpt(encoded_vxids: Vec<u8>, delay_type: i32) -> bool;
        pub fn PersistControlFile(control_file: ControlFileData);
        pub fn SetSharedRedoRecPtr(redo: XLogRecPtr);
        pub fn SetCkptFullXid(next_xid: FullTransactionId);
        // CreateRestartPoint-only recovery externals.
        pub fn GetLastCheckPointForRestart() -> (XLogRecPtr, XLogRecPtr, CheckPoint);
        pub fn UpdateMinRecoveryPoint(lsn: XLogRecPtr, force: bool);
        pub fn GetWalRcvFlushRecPtr() -> XLogRecPtr;
        pub fn GetXLogReplayRecPtr() -> (XLogRecPtr, TimeLineID);
        pub fn MaybeRunArchiveCleanupCommand();
    }
}
