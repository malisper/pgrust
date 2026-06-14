//! Seam declarations for the `backend-access-transam-transam` unit
//! (`access/transam/transam.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{TransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    /// `TransactionIdDidCommit(xid)` — clog lookup; can `ereport(ERROR)` on
    /// clog I/O failure.
    ///
    /// `transaction_xmin` is C's `TransactionXmin` global (snapmgr.c), threaded
    /// explicitly here: the body reads it when chasing a sub-committed xid's
    /// parent through pg_subtrans. Consumers pass their snapshot xmin.
    pub fn transaction_id_did_commit(
        xid: TransactionId,
        transaction_xmin: TransactionId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `TransactionIdDidAbort(xid)` — clog lookup; can `ereport(ERROR)` on
    /// clog I/O failure.
    ///
    /// `transaction_xmin` is C's `TransactionXmin` global (snapmgr.c), threaded
    /// explicitly here: the body reads it when chasing a sub-committed xid's
    /// parent through pg_subtrans. Consumers pass their snapshot xmin.
    pub fn transaction_id_did_abort(
        xid: TransactionId,
        transaction_xmin: TransactionId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `TransactionIdPrecedes(id1, id2)` — modulo-2^31 circular comparison.
    pub fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool
);

seam_core::seam!(
    /// `TransactionIdCommitTree(xid, nxids, xids)` — mark a commit tree
    /// committed in pg_xact (synchronous form).
    pub fn transaction_id_commit_tree(xid: TransactionId, children: &[TransactionId]) -> PgResult<()>
);

seam_core::seam!(
    /// `TransactionIdAsyncCommitTree(xid, nxids, xids, lsn)` — async form,
    /// recording the LSN the XLOG must be flushed to first.
    pub fn transaction_id_async_commit_tree(
        xid: TransactionId,
        children: &[TransactionId],
        lsn: XLogRecPtr,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `TransactionIdAbortTree(xid, nxids, xids)` — mark a tree aborted in
    /// pg_xact.
    pub fn transaction_id_abort_tree(xid: TransactionId, children: &[TransactionId]) -> PgResult<()>
);

seam_core::seam!(
    /// `TransactionIdLatest(mainxid, nxids, xids)` — newest XID among the
    /// tree, by TransactionIdFollows order. Pure.
    pub fn transaction_id_latest(main_xid: TransactionId, children: &[TransactionId]) -> TransactionId
);
