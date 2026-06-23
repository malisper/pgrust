//! Seam declarations for the `backend-access-heap-heapam-xlog` unit (`heapam_xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `heap_redo(record)` (heapam_xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn heap_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap2_redo(record)` (heapam_xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn heap2_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap_mask(pagedata, blkno)` (heapam_xlog.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn heap_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);
