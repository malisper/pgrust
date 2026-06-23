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
    pub fn clog_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
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

seam_core::seam!(
    /// `CLOGShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn clog_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `CLOGShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn clog_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExtendCLOG(newestXact)` (clog.c): zero the clog page that would hold
    /// `newestXact` if it is the first XID of a new page, while the caller
    /// holds `XidGenLock`. The SLRU page write can `ereport(ERROR)`, carried
    /// on `Err`. Owner unported; scaffolded slot.
    pub fn extend_clog(newest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `StartupCLOG()` (clog.c) — set clog's idea of the latest page number from
    /// `TransamVariables->nextXid` at startup. Called once from `StartupXLOG`
    /// (xlog.c:5675) on the WAL-startup path; reads `nextXid` through the varsup
    /// seam and writes the SLRU `latest_page_number` under the bank lock. Plain
    /// shared-memory store; kept fallible to match the shared-state channel.
    pub fn startup_clog() -> PgResult<()>
);

seam_core::seam!(
    /// `TrimCLOG()` (clog.c) — zero the tail of the current clog page at the end
    /// of recovery. Called once from `StartupXLOG` (xlog.c:6160). The SLRU page
    /// write can `ereport(ERROR)`, carried on `Err`.
    pub fn trim_clog() -> PgResult<()>
);

seam_core::seam!(
    /// `CheckPointCLOG()` (clog.c) — flush all dirty pg_xact SLRU pages to disk
    /// (the CLOG arm of `CheckPointGuts` → `CheckPointGutsCallbacks`). The SLRU
    /// page write can `ereport(ERROR)`, carried on `Err`.
    pub fn check_point_clog() -> PgResult<()>
);

seam_core::seam!(
    /// `TruncateCLOG(oldestXact, oldestxid_datoid)` (clog.c) — truncate pg_xact
    /// up to (but not including) the page holding `oldestXact`. Called from
    /// vacuum's `vac_truncate_clog`. The SLRU truncation can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn truncate_clog(
        oldest_xact: TransactionId,
        oldestxid_datoid: types_core::Oid,
    ) -> PgResult<()>
);
