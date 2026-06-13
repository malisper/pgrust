//! Seam declarations for the `backend-access-transam-xlog` unit (`xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use types_core::XLogRecPtr;
use types_error::PgResult;
use types_wal::WalLevel;

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
    /// `wal_level` (xlog.c GUC).
    pub fn wal_level() -> WalLevel
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
    /// `RecoveryInProgress()` (xlog.c): true while hot-standby recovery is
    /// running. Reads backend-local + shared state; cannot `ereport`.
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
    /// `RecoveryInProgress()` (xlog.c): true while the server is in archive
    /// recovery / standby mode. Shared-state read; infallible.
    pub fn RecoveryInProgress() -> bool
);

seam_core::seam!(
    /// `GetActiveWalLevelOnStandby()` (xlog.c): the effective `wal_level` on a
    /// standby, read from the control file's last checkpoint. Shared-state
    /// read; infallible.
    pub fn GetActiveWalLevelOnStandby() -> types_logical::WalLevel
);
