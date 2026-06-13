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

seam_core::seam!(
    /// `TransactionIdGetCommitTsData(xid, &ts, &nodeid)` (commit_ts.c): the
    /// commit timestamp and replication origin of `xid` as
    /// `(found, ts, nodeid)`; a too-old or not-yet-committed xid is
    /// `(false, 0, InvalidRepOriginId)`. An invalid or permanent xid, or
    /// commit-ts tracking being inactive, is `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn transaction_id_get_commit_ts_data(
        xid: types_core::TransactionId,
    ) -> types_error::PgResult<(
        bool,
        types_core::TimestampTz,
        types_core::primitive::RepOriginId,
    )>
);

seam_core::seam!(
    /// `committssyncfiletag(const FileTag *ftag, char *path)` (commit_ts.c, the
    /// `syncsw[SYNC_HANDLER_COMMIT_TS]` sync callback) — fsync the SLRU segment
    /// the tag names, returning the `0`/`<0` code, resolved path, and saved
    /// `errno`.
    pub fn committssyncfiletag(ftag: types_storage::sync::FileTag) -> types_error::PgResult<types_storage::sync::FileTagOpResult>
);
