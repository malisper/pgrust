//! Seam declarations for the `backend-access-spg-xlog` unit (`spgxlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `spg_redo(record)` (spgxlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn spg_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `spg_xlog_startup()` (spgxlog.c) — create this AM's recovery temporary memory
    /// context under `parent` at the start of WAL replay (`rm_startup` slot); OOM
    /// `ereport(ERROR)` carried on `Err`.
    pub fn spg_xlog_startup(parent: mcx::Mcx<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `spg_xlog_cleanup()` (spgxlog.c) — delete this AM's recovery temporary memory
    /// context at the end of WAL replay (`rm_cleanup` slot).
    pub fn spg_xlog_cleanup()
);

seam_core::seam!(
    /// `spg_mask(pagedata, blkno)` (spgxlog.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn spg_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);
