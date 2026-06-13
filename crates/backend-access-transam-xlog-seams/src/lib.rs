//! Seam declarations for the `backend-access-transam-xlog` unit (`xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::XLogRecPtr;
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
