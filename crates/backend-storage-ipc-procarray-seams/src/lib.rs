//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `ProcArrayEndTransaction(MyProc, latestXid)` — advertise no transaction
    /// in progress (the proc argument is always `MyProc` from xact.c).
    pub fn proc_array_end_transaction(latest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcArrayClearTransaction(MyProc)` — PREPARE's variant.
    pub fn proc_array_clear_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `XidCacheRemoveRunningXids(xid, nxids, xids, latestXid)` — drop aborted
    /// subxids from PGPROC's subxid cache.
    pub fn xid_cache_remove_running_xids(
        xid: TransactionId,
        children: &[TransactionId],
        latest_xid: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcArrayApplyXidAssignment(topxid, nsubxids, subxids)` — redo-side
    /// subxid bookkeeping for hot standby.
    pub fn proc_array_apply_xid_assignment(
        xtop: TransactionId,
        subxids: &[TransactionId],
    ) -> PgResult<()>
);
