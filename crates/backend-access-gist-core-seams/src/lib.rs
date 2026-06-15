//! Seam declarations for the `backend-access-gist-core` unit (`gistxlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `gist_redo(record)` (gistxlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn gist_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `gist_xlog_startup()` (gistxlog.c) — create this AM's recovery temporary memory
    /// context under `parent` at the start of WAL replay (`rm_startup` slot); OOM
    /// `ereport(ERROR)` carried on `Err`.
    pub fn gist_xlog_startup(parent: mcx::Mcx<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `gist_xlog_cleanup()` (gistxlog.c) — delete this AM's recovery temporary memory
    /// context at the end of WAL replay (`rm_cleanup` slot).
    pub fn gist_xlog_cleanup()
);

seam_core::seam!(
    /// `gist_mask(pagedata, blkno)` (gistxlog.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn gist_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `gistXLogPageReuse(rel, heaprel, blkno, deleteXid)` (gistxlog.c) — emit
    /// the `XLOG_GIST_PAGE_REUSE` conflict record when recycling a deleted page
    /// in `gistNewBuffer`. The record doesn't modify the page; it only provides
    /// a Hot-Standby conflict point. Owned by the gistxlog layer (unported); the
    /// `gistNewBuffer` insertion helper reaches it through this seam.
    pub fn gist_xlog_page_reuse<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        heaprel: &types_rel::Relation<'mcx>,
        blkno: types_core::BlockNumber,
        delete_xid: types_core::xact::FullTransactionId,
    ) -> types_error::PgResult<()>
);
