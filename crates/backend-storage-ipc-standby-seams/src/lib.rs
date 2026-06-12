//! Seam declarations for the `backend-storage-ipc-standby` unit
//! (`storage/ipc/standby.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

seam_core::seam!(
    /// `RecordKnownAssignedTransactionIds(xid)` — hot-standby bookkeeping for
    /// as-yet-unobserved XIDs in a completion record.
    pub fn record_known_assigned_transaction_ids(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `ExpireTreeKnownAssignedTransactionIds(xid, nsubxids, subxids,
    /// max_xid)`.
    pub fn expire_tree_known_assigned_transaction_ids(
        xid: TransactionId,
        subxids: &[TransactionId],
        max_xid: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `StandbyReleaseLockTree(xid, nsubxids, subxids)` — release the tree's
    /// AccessExclusiveLocks during replay.
    pub fn standby_release_lock_tree(xid: TransactionId, subxids: &[TransactionId]) -> PgResult<()>
);

seam_core::seam!(
    /// `LogStandbyInvalidations(nmsgs, msgs, relcacheInitFileInval)` — emit a
    /// bespoke invalidations WAL record for an xid-less committing transaction
    /// (the slice carries `nmsgs`).
    pub fn log_standby_invalidations(
        msgs: &[SharedInvalidationMessage],
        relcache_init_file_inval: bool,
    ) -> PgResult<()>
);
