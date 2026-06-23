//! Seam declarations for the `backend-access-brin-xlog` unit (`brin_xlog.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `brin_redo(record)` (brin_xlog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn brin_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `brin_mask(pagedata, blkno)` (brin_xlog.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn brin_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);
