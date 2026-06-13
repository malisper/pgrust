//! Seam declarations for the `backend-access-transam-xlog` unit (`xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use types_wal::{ArchiveMode, WalLevel};

seam_core::seam!(
    /// `xlog_redo(record)` (xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogSetAsyncXactLSN(asyncXactLSN)` — mark the LSN as to-be-synced and
    /// nudge the WAL writer.
    pub fn xlog_set_async_xact_lsn(async_xact_lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XLogArchiveLibrary` (xlog.c GUC string): the configured archive
    /// library name, "" when unset. Returns an owned copy of the current
    /// value (the C bare read is a `char *` global; pgarch copies it via
    /// `pstrdup` before reload). Infallible.
    pub fn xlog_archive_library() -> alloc::string::String
);

seam_core::seam!(
    /// `XLogArchiveCommand` (xlog.c GUC string): the configured archive shell
    /// command, "" when unset. Returns an owned copy of the current value.
    /// Infallible.
    pub fn xlog_archive_command() -> alloc::string::String
);

seam_core::seam!(
    /// `int wal_level` (xlog.c GUC) — the effective `wal_level` value.
    pub fn wal_level() -> WalLevel
);

seam_core::seam!(
    /// `wal_segment_size` (xlog.c global, bytes-per-WAL-segment). A plain
    /// global read — infallible.
    pub fn wal_segment_size() -> i32
);

seam_core::seam!(
    /// `InRecovery` (xlog.c global) — true in the startup process during
    /// crash/archive recovery.
    pub fn in_recovery() -> bool
);

seam_core::seam!(
    /// `StartupXLOG()` (xlog.c) — perform crash/archive recovery and bring
    /// the system to a consistent, writable state. Many of its paths
    /// `ereport(ERROR)` (besides the FATAL/PANIC ones), so the error
    /// propagates to the caller.
    pub fn startup_xlog() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `bool RecoveryInProgress(void)` (xlog.c) — true if WAL recovery is
    /// still in progress (we are a standby / in crash recovery). Reads
    /// backend-local + shared state; cannot `ereport`.
    pub fn recovery_in_progress() -> bool
);

seam_core::seam!(
    /// `XLogLogicalInfoActive()` (`access/xlog.h`): `wal_level >= logical`.
    /// The `wal_level` global is owned by xlog.c.
    pub fn xlog_logical_info_active() -> bool
);

seam_core::seam!(
    /// `XLogStandbyInfoActive()` (`access/xlog.h`): `wal_level >= replica`.
    pub fn xlog_standby_info_active() -> bool
);

seam_core::seam!(
    /// `XLogEnsureRecordSpace(max_block_id, ndatas)` (xloginsert.c, owned with
    /// the xlog insert path): ensure the WAL insertion buffers can register
    /// `ndatas` rdata chunks. Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_ensure_record_space(ndatas: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `EndPrepare`'s WAL insert: `XLogBeginInsert` + per-chunk
    /// `XLogRegisterData` + `XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN)` +
    /// `XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE)`. `body` is the assembled
    /// prepare-record buffer (flat). Returns the prepare-record *end* LSN. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_insert_prepare(body: &[u8]) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `ProcLastRecPtr` (xlog.c global): the *start* LSN of the record this
    /// backend most recently inserted. Pure read of backend-local state.
    pub fn proc_last_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogFlush(lsn)` — ensure WAL is flushed up to `lsn`; I/O errors
    /// `ereport(ERROR)` (PANIC inside critical sections).
    pub fn xlog_flush(lsn: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    /// Read `XactLastRecEnd` (xlog.c per-backend global): end of the last WAL
    /// record this transaction inserted; 0 if none.
    pub fn xact_last_rec_end() -> XLogRecPtr
);

seam_core::seam!(
    /// Write `XactLastRecEnd` (the xact engine resets it to 0 at transaction
    /// end).
    pub fn set_xact_last_rec_end(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// Write `XactLastCommitEnd` (xlog.c per-backend global): end of the last
    /// commit record.
    pub fn set_xact_last_commit_end(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XlogReadTwoPhaseData(lsn, &buf, &len)` (xlog.c): re-read the prepare
    /// record body from WAL (used when COMMIT/ABORT PREPARED happens before the
    /// next checkpoint, and by `CheckPointTwoPhase`). Returns the rmgr data
    /// bytes. Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_read_twophase_data(lsn: XLogRecPtr) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// `BootStrapXLOG(data_checksum_version)` (xlog.c): create the initial WAL
    /// segment and control file at bootstrap. `ereport(PANIC)` on an I/O
    /// failure (modeled as `Err`).
    pub fn boot_strap_xlog(data_checksum_version: u32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckpointStats.ckpt_slru_written++` (xlog.c's `CheckpointStats`
    /// global, bumped directly by slru.c during checkpoint write-all).
    /// Narrow write-side capability on the owner's global, same shape as
    /// `set_my_backend_type` (see DESIGN_DEBT.md).
    pub fn count_ckpt_slru_written()
);

seam_core::seam!(
    /// `GetActiveWalLevelOnStandby()` (xlog.c): the effective `wal_level` on a
    /// standby, read from the control file's last checkpoint. Shared-state
    /// read; infallible.
    pub fn GetActiveWalLevelOnStandby() -> types_logical::WalLevel
);

seam_core::seam!(
    /// `log_recovery_conflict_waits` (the GUC, owned by xlog.c) — whether the
    /// startup process should log long recovery-conflict waits.
    pub fn log_recovery_conflict_waits() -> bool
);

seam_core::seam!(
    /// `GetFlushRecPtr(*insertTLI)` (xlog.c) — the LSN up to which WAL is
    /// flushed, with the corresponding insert timeline. Returns `(lsn, tli)`.
    pub fn get_flush_rec_ptr() -> (XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    /// `GetWALInsertionTimeLineIfSet()` (xlog.c) — the insert TLI once it has
    /// been initialized in shared memory, else `0` (the C `InvalidTimeLineID`
    /// / 0 sentinel before recovery finishes).
    pub fn get_wal_insertion_timeline_if_set() -> TimeLineID
);

seam_core::seam!(
    /// `XLogRecPtr GetRedoRecPtr(void)` (xlog.c) — the current redo pointer.
    pub fn get_redo_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogRecPtr GetXLogInsertRecPtr(void)` (xlog.c) — current insert position.
    pub fn get_xlog_insert_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `XLogRecPtr GetXLogReplayRecPtr(TimeLineID *)` (xlogrecovery.c) — last
    /// replayed position (called with NULL by slot.c, so no TLI out).
    pub fn get_xlog_replay_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn)` (xlog.c) —
    /// publish the oldest LSN required by replication slots.
    pub fn xlog_set_replication_slot_minimum_lsn(lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `XLogSegNo XLogGetLastRemovedSegno(void)` (xlog.c).
    pub fn xlog_get_last_removed_segno() -> XLogSegNo
);

seam_core::seam!(
    /// `XLogRecPtr LogStandbySnapshot(void)` (standby.c) — log an
    /// `xl_running_xacts` record and return the end LSN. Can `ereport(ERROR)`.
    pub fn log_standby_snapshot() -> types_error::PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `bool StandbyMode` (xlogrecovery.c) — running in standby mode.
    pub fn standby_mode() -> bool
);

seam_core::seam!(
    /// `bool EnableHotStandby` (xlog.c) — the `hot_standby` GUC value.
    pub fn enable_hot_standby() -> bool
);

seam_core::seam!(
    /// `GetSystemIdentifier()` (xlog.c) — the cluster's 64-bit system id.
    pub fn get_system_identifier() -> u64
);

seam_core::seam!(
    /// `XLogArchiveMode` (xlog.c GUC) — the `archive_mode` setting.
    pub fn xlog_archive_mode() -> ArchiveMode
);

seam_core::seam!(
    /// `XLogFileInit(segno, tli)` (xlog.c) — create/open the given WAL segment
    /// file, returning the fd. `ereport(ERROR)` on failure.
    pub fn xlog_file_init(segno: XLogSegNo, tli: TimeLineID) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `issue_xlog_fsync(fd, segno, tli)` (xlog.c) — fsync the WAL segment;
    /// `ereport` on failure.
    pub fn issue_xlog_fsync(
        fd: i32,
        segno: XLogSegNo,
        tli: TimeLineID
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogGetOldestSegno(tli)` (xlog.c) — the oldest WAL segment number that
    /// still exists on disk for `tli`, or `0` if none.
    pub fn xlog_get_oldest_segno(tli: TimeLineID) -> XLogSegNo
);

// ---------------------------------------------------------------------------
// Local WAL read, consumed by xlogutils.c's read_local_xlog_page page-read
// callback. (The flush position uses the `get_flush_rec_ptr` seam above.)
// ---------------------------------------------------------------------------

/// The `WALReadError` fields needed by `WALReadRaiseError`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalReadErrorInfo {
    /// `wre_errno`.
    pub wre_errno: i32,
    /// `wre_off` — the offset within the segment at which the read failed.
    pub wre_off: i32,
    /// `wre_req` — the number of bytes requested.
    pub wre_req: i32,
    /// `wre_read` — the number of bytes actually read (<0 error, 0 short).
    pub wre_read: i32,
    /// `wre_seg.ws_segno`.
    pub wre_seg_segno: XLogSegNo,
    /// `wre_seg.ws_tli`.
    pub wre_seg_tli: TimeLineID,
}

/// Outcome of `WALRead`, mirroring the C `bool`-return plus `WALReadError`
/// out-parameter contract consumed by `WALReadRaiseError`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalReadOutcome {
    /// `WALRead` returned true; the read bytes (the C writes them through the
    /// borrowed `cur_page` pointer; the owned model returns them).
    Ok(Vec<u8>),
    /// `WALRead` returned false; the populated `WALReadError` to be raised.
    Error(WalReadErrorInfo),
}

seam_core::seam!(
    /// `WALRead(state, cur_page, targetPagePtr, count, tli, &errinfo)` (xlog.c)
    /// — read `count` bytes of WAL at `target_page_ptr` on timeline `tli`. On
    /// success returns `WalReadOutcome::Ok(bytes)` with the `count` valid
    /// bytes; on failure returns `WalReadOutcome::Error(errinfo)`.
    pub fn wal_read(
        target_page_ptr: XLogRecPtr,
        count: i32,
        tli: TimeLineID,
    ) -> WalReadOutcome
);

seam_core::seam!(
    /// `XLogGetReplicationSlotMinimumLSN()` (xlog.c): the oldest LSN required
    /// by any replication slot, or `InvalidXLogRecPtr` if none. Read under the
    /// `info_lck` spinlock by the owner.
    pub fn xlog_get_replication_slot_minimum_lsn() -> XLogRecPtr
);
