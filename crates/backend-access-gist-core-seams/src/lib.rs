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

seam_core::seam!(
    /// `gistXLogSplit(page_is_leaf, dist, orig_rlink, orignsn, leftchildbuf,
    /// markfollowright)` (gistxlog.c) — WAL-log a GiST page split. `dist` is the
    /// `SplitPageLayout` chain produced by `gistSplit` (one entry per produced
    /// half, each carrying its `buffer`/`page` block + on-disk tuple bytes).
    /// Returns the record's `XLogRecPtr`. Owned by the GiST xlog (F7) lane;
    /// panics until that lands.
    pub fn gist_xlog_split<'mcx>(
        page_is_leaf: bool,
        dist: &[types_gist::SplitPageLayout<'mcx>],
        orig_rlink: types_core::BlockNumber,
        orignsn: types_gist::GistNSN,
        left_child_buf: types_storage::Buffer,
        mark_follow_right: bool,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
);

seam_core::seam!(
    /// `gistXLogUpdate(buffer, todelete, ntodelete, itup, ituplen,
    /// leftchildbuf)` (gistxlog.c) — WAL-log a GiST page update (delete some
    /// offsets, add some tuples). `todelete` are the offsets to remove; `itup`
    /// are the on-disk byte images to add. Returns the record's `XLogRecPtr`.
    /// Owned by the GiST xlog (F7) lane; panics until that lands.
    pub fn gist_xlog_update(
        buffer: types_storage::Buffer,
        todelete: &[types_core::primitive::OffsetNumber],
        itup: &[&[u8]],
        left_child_buf: types_storage::Buffer,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
);

seam_core::seam!(
    /// `gistXLogDelete(buffer, todelete, ntodelete, snapshotConflictHorizon,
    /// heaprel)` (gistxlog.c) — WAL-log the deletion of LP_DEAD index tuples on
    /// a leaf page. Returns the record's `XLogRecPtr`. Owned by the GiST xlog
    /// (F7) lane; panics until that lands.
    pub fn gist_xlog_delete<'mcx>(
        buffer: types_storage::Buffer,
        todelete: &[types_core::primitive::OffsetNumber],
        snapshot_conflict_horizon: types_core::primitive::TransactionId,
        heaprel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
);

seam_core::seam!(
    /// `gistXLogPageDelete(buffer, xid, parentBuffer, downlinkOffset)`
    /// (gistxlog.c:552) — WAL-log the deletion of a GiST leaf page: mark the
    /// child deleted with `xid` and remove the parent's downlink at
    /// `downlinkOffset`. Returns the record's `XLogRecPtr`. Owned by the GiST
    /// xlog (F7) lane; panics until that lands.
    pub fn gist_xlog_page_delete(
        buffer: types_storage::Buffer,
        xid: types_core::xact::FullTransactionId,
        parent_buffer: types_storage::Buffer,
        downlink_offset: types_core::primitive::OffsetNumber,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
);

seam_core::seam!(
    /// `gistGetFakeLSN(rel)` (gist.c) — produce a fake LSN for an unlogged or
    /// temp GiST index (so NSN interlocks still order correctly without real
    /// WAL). Owned by the GiST xlog (F7) lane; panics until that lands.
    pub fn gist_get_fake_lsn<'mcx>(
        rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
);
