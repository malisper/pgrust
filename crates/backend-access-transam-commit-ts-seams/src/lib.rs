//! Seam declarations for the `backend-access-transam-commit-ts` unit
//! (`access/transam/commit_ts.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{RepOriginId, TimestampTz, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    /// `TransactionTreeSetCommitTsData(xid, nsubxids, subxids, timestamp,
    /// nodeid)` — record commit timestamp + origin for a transaction tree.
    pub fn transaction_tree_set_commit_ts_data(
        xid: TransactionId,
        subxids: &[TransactionId],
        timestamp: TimestampTz,
        node_id: RepOriginId,
    ) -> PgResult<()>
);
