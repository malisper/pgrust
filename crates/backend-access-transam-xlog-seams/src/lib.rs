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

seam_core::seam!(
    /// `GetRedoRecPtr()` (xlog.c) — the redo pointer, the oldest LSN that might
    /// still be needed for crash recovery. Pure shmem read; infallible.
    pub fn get_redo_rec_ptr() -> types_core::primitive::XLogRecPtr
);

seam_core::seam!(
    /// `XLogGetReplicationSlotMinimumLSN()` (xlog.c) — the minimum restart_lsn
    /// across all replication slots, or `InvalidXLogRecPtr` if none. Pure shmem
    /// read; infallible.
    pub fn xlog_get_replication_slot_minimum_lsn() -> types_core::primitive::XLogRecPtr
);

seam_core::seam!(
    /// `XLogGetLastRemovedSegno()` (xlog.c) — the highest WAL segment number
    /// removed so far. Pure shmem read; infallible.
    pub fn xlog_get_last_removed_segno() -> types_core::primitive::XLogSegNo
);

seam_core::seam!(
    /// `wal_segment_size` (xlog.c) — the size of each WAL segment in bytes.
    pub fn wal_segment_size() -> i32
);

seam_core::seam!(
    /// `wal_level` GUC (xlog.c) — the level of information written to WAL
    /// (`types_wal::WAL_LEVEL_*`). Read of the GUC subsystem value owned by
    /// xlog.c.
    pub fn wal_level() -> i32
);
