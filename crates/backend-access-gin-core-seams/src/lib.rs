//! Seam declarations for the `backend-access-gin-core` unit (`ginxlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `gin_redo(record)` (ginxlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn gin_redo(record: &mut types_wal::rmgr::XLogReaderState) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `gin_xlog_startup()` (ginxlog.c) — create this AM's recovery temporary memory
    /// context at the start of WAL replay (`rm_startup` slot); OOM
    /// `ereport(ERROR)` carried on `Err`.
    pub fn gin_xlog_startup() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `gin_xlog_cleanup()` (ginxlog.c) — delete this AM's recovery temporary memory
    /// context at the end of WAL replay (`rm_cleanup` slot).
    pub fn gin_xlog_cleanup()
);

seam_core::seam!(
    /// `gin_mask(pagedata, blkno)` (ginxlog.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn gin_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);
