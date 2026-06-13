//! Seam declarations for the `backend-replication-logical-origin` unit (`origin.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::{RepOriginId, TimestampTz, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    /// `replorigin_redo(record)` (origin.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn replorigin_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Read `replorigin_session_origin`.
    pub fn replorigin_session_origin() -> RepOriginId
);

seam_core::seam!(
    /// Read `replorigin_session_origin_lsn`.
    pub fn replorigin_session_origin_lsn() -> XLogRecPtr
);

seam_core::seam!(
    /// Read `replorigin_session_origin_timestamp`.
    pub fn replorigin_session_origin_timestamp() -> TimestampTz
);

seam_core::seam!(
    /// Write `replorigin_session_origin_timestamp` (RecordTransactionCommit
    /// fills it in when no origin timestamp was provided).
    pub fn set_replorigin_session_origin_timestamp(timestamp: TimestampTz)
);

seam_core::seam!(
    /// `replorigin_session_advance(remote_commit, local_commit)` — move this
    /// session's origin LSNs forward.
    pub fn replorigin_session_advance(
        remote_commit: XLogRecPtr,
        local_commit: XLogRecPtr,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `replorigin_session_origin_timestamp = ts` (origin.c global): write-back
    /// of the derived commit timestamp. Pure write of backend-local state.
    pub fn set_replorigin_session_timestamp(ts: types_core::TimestampTz)
);

seam_core::seam!(
    /// `replorigin_advance(node, remote_commit, local_commit, go_backward,
    /// wal_log)` — redo-side origin progress update.
    pub fn replorigin_advance(
        node: RepOriginId,
        remote_commit: XLogRecPtr,
        local_commit: XLogRecPtr,
        go_backward: bool,
        wal_log: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `replorigin_by_oid(roident, missing_ok, &roname)` (origin.c): the
    /// replication origin's name, copied into `mcx` (C: allocated in the
    /// current context). A missing origin is `Ok(None)` when `missing_ok`
    /// (C: returns false with `*roname = NULL`) and `ereport(ERROR)` (`Err`)
    /// otherwise. `Err` includes OOM from the copy.
    pub fn replorigin_by_oid<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        roident: types_core::primitive::RepOriginId,
        missing_ok: bool,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);
