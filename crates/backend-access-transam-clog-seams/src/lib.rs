//! Seam declarations for the `backend-access-transam-clog` unit
//! (`access/transam/clog.c`), including the rmgr-table callbacks it owns
//! (slots of `RmgrTable`, populated from `access/rmgrlist.h` by
//! `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::xact::XidStatus;
use types_core::{TransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    /// `clog_redo(record)` (clog.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn clog_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `TransactionIdGetStatus(xid, &lsn)` (clog.c): fetch the commit status
    /// of `xid` from pg_xact, plus the commit-record LSN that conservatively
    /// bounds the flush (the C out-parameter). The SLRU page read can
    /// `ereport(ERROR)` on I/O failure, carried on `Err`.
    pub fn transaction_id_get_status(xid: TransactionId) -> PgResult<(XidStatus, XLogRecPtr)>
);

seam_core::seam!(
    /// `TransactionIdSetTreeStatus(xid, nsubxids, subxids, status, lsn)`
    /// (clog.c): mark a toplevel xid and its committed subxids with `status`
    /// in pg_xact (subxids on other pages are marked subcommitted first; that
    /// page-split handling lives in clog). The SLRU page access can
    /// `ereport(ERROR)` on I/O failure, carried on `Err`.
    pub fn transaction_id_set_tree_status(
        xid: TransactionId,
        subxids: &[TransactionId],
        status: XidStatus,
        lsn: XLogRecPtr,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `clogsyncfiletag(const FileTag *ftag, char *path)` (the `syncsw[SYNC_HANDLER_*]`
    /// sync callback this SLRU owns) — fsync the SLRU segment the tag names,
    /// returning the `0`/`<0` code, resolved path, and saved `errno`.
    pub fn clogsyncfiletag(ftag: types_storage::sync::FileTag) -> types_error::PgResult<types_storage::sync::FileTagOpResult>
);
