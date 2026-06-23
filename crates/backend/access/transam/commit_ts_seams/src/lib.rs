//! Seam declarations for the `backend-access-transam-commit-ts` unit (`commit_ts.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use ::types_core::{RepOriginId, TimestampTz, TransactionId};
use ::types_error::PgResult;

seam_core::seam!(
    /// `commit_ts_redo(record)` (commit_ts.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn commit_ts_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> ::types_error::PgResult<()>
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
        xid: ::types_core::TransactionId,
    ) -> ::types_error::PgResult<(
        bool,
        ::types_core::TimestampTz,
        ::types_core::primitive::RepOriginId,
    )>
);

seam_core::seam!(
    /// `committssyncfiletag(const FileTag *ftag, char *path)` (commit_ts.c, the
    /// `syncsw[SYNC_HANDLER_COMMIT_TS]` sync callback) — fsync the SLRU segment
    /// the tag names, returning the `0`/`<0` code, resolved path, and saved
    /// `errno`.
    pub fn committssyncfiletag(ftag: types_storage::sync::FileTag) -> ::types_error::PgResult<types_storage::sync::FileTagOpResult>
);

seam_core::seam!(
    /// `CommitTsShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn commit_ts_shmem_size() -> ::types_error::PgResult<::types_core::Size>
);

seam_core::seam!(
    /// `CommitTsShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn commit_ts_shmem_init() -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExtendCommitTs(newestXact)` (commit_ts.c): zero the commit-ts page
    /// that would hold `newestXact` if it is the first XID of a new page,
    /// while the caller holds `XidGenLock`. The SLRU page write can
    /// `ereport(ERROR)`, carried on `Err`. Owner unported; scaffolded slot.
    pub fn extend_commit_ts(newest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `StartupCommitTs()` (commit_ts.c) — activate the commit-timestamp module
    /// at startup. Called once from `StartupXLOG` (xlog.c:5690). The owner wraps
    /// its private `CommitTsState`. SLRU activation can `ereport(ERROR)`.
    pub fn startup_commit_ts() -> PgResult<()>
);

seam_core::seam!(
    /// `CompleteCommitTsInitialization()` (commit_ts.c) — finish commit-ts setup
    /// at end of recovery, activating or deactivating per the GUC. Called once
    /// from `StartupXLOG` (xlog.c:6211). Fallible (SLRU writes).
    pub fn complete_commit_ts_initialization() -> PgResult<()>
);

seam_core::seam!(
    /// `SetCommitTsLimit(oldestXact, newestXact)` (commit_ts.c) — seed the oldest
    /// and newest XID endpoints for which a commit timestamp can be consulted.
    /// Called from `StartupXLOG` (xlog.c:5641) and `BootStrapXLOG`. Plain
    /// shared-memory store under `CommitTsLock`; fallible to match the channel.
    pub fn set_commit_ts_limit(oldest_xact: TransactionId, newest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `TruncateCommitTs(oldestXact)` (commit_ts.c) — truncate the commitTS SLRU
    /// up to the page holding `oldestXact`. Called from vacuum's
    /// `vac_truncate_clog`. The SLRU truncation can `ereport(ERROR)`.
    pub fn truncate_commit_ts(oldest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `AdvanceOldestCommitTsXid(oldestXact)` (commit_ts.c) — bump the oldest
    /// XID for which commit timestamps are retained. Called from vacuum's
    /// `vac_truncate_clog` before truncation.
    pub fn advance_oldest_commit_ts_xid(oldest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `CommitTsParameterChange(newvalue, oldvalue)` (commit_ts.c) — activate or
    /// deactivate the commit-timestamp module when an `XLOG_PARAMETER_CHANGE`
    /// WAL record is replayed during recovery (xlog.c:8634). Runs only in the
    /// recovery process; the owner wraps its private `CommitTsState`. SLRU
    /// activation/deactivation can `ereport(ERROR)`.
    pub fn commit_ts_parameter_change(newvalue: bool, oldvalue: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `CheckPointCommitTs()` (commit_ts.c) — flush all dirty commitTS SLRU pages
    /// to disk at a checkpoint (`CheckPointGuts`, xlog.c:7586). The SLRU writes
    /// can `ereport(ERROR)`.
    pub fn check_point_commit_ts() -> PgResult<()>
);
