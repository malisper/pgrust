//! Seam declarations for the `backend-access-transam-commit-ts` unit (`commit_ts.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::{RepOriginId, TimestampTz, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    /// `commit_ts_redo(record)` (commit_ts.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn commit_ts_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

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
