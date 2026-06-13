//! Seam declarations for the `backend-access-transam-xlog` unit (`xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `xlog_redo(record)` (xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn xlog_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

use types_core::{XLogRecPtr, XLogSegNo};

seam_core::seam!(
    /// `bool RecoveryInProgress(void)` (xlog.c) — true if WAL recovery is
    /// still in progress (we are a standby / in crash recovery).
    pub fn recovery_in_progress() -> bool
);

seam_core::seam!(
    /// `int wal_level` (xlog.c) — the effective `wal_level` GUC value
    /// (`WalLevel` enum codes; `WAL_LEVEL_MINIMAL`=0, `_REPLICA`=1, `_LOGICAL`=2).
    pub fn wal_level() -> i32
);

seam_core::seam!(
    /// `int wal_segment_size` (xlog.c) — WAL segment size in bytes.
    pub fn wal_segment_size() -> i32
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
    /// `void XLogFlush(XLogRecPtr record)` (xlog.c). Can `ereport(ERROR)`.
    pub fn xlog_flush(record: XLogRecPtr) -> types_error::PgResult<()>
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
