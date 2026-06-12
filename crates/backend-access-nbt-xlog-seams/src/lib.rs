//! Seam declarations for the `backend-access-nbt-xlog` unit (`nbtxlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `btree_redo(record)` (nbtxlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn btree_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `btree_xlog_startup()` (nbtxlog.c) — create this AM's recovery temporary memory
    /// context under `parent` at the start of WAL replay (`rm_startup` slot); OOM
    /// `ereport(ERROR)` carried on `Err`.
    pub fn btree_xlog_startup(parent: mcx::Mcx<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `btree_xlog_cleanup()` (nbtxlog.c) — delete this AM's recovery temporary memory
    /// context at the end of WAL replay (`rm_cleanup` slot).
    pub fn btree_xlog_cleanup()
);

seam_core::seam!(
    /// `btree_mask(pagedata, blkno)` (nbtxlog.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn btree_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);
