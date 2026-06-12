//! Seam declarations for the `backend-access-transam-clog` unit
//! (`access/transam/clog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::xact::XidStatus;
use types_core::{TransactionId, XLogRecPtr};
use types_error::PgResult;

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
