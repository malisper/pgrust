//! Seam declarations for the `backend-access-transam-commit-ts` unit (`commit_ts.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `commit_ts_redo(record)` (commit_ts.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn commit_ts_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `TransactionTreeSetCommitTsData(xid, nsubxids, subxids, timestamp,
    /// nodeid)` (commit_ts.c): record the commit timestamp + replication-origin
    /// id for a toplevel xid and its committed subxids. The SLRU page write can
    /// `ereport(ERROR)` on I/O failure, carried on `Err`.
    pub fn transaction_tree_set_commit_ts_data(
        xid: types_core::TransactionId,
        subxids: &[types_core::TransactionId],
        timestamp: types_core::TimestampTz,
        nodeid: types_core::RepOriginId,
    ) -> types_error::PgResult<()>
);
