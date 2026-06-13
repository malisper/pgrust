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
