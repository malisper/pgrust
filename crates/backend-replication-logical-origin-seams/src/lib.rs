//! Seam declarations for the `backend-replication-logical-origin` unit (`origin.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `replorigin_redo(record)` (origin.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn replorigin_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `replorigin_session_advance(remote_commit, local_commit)` (origin.c):
    /// advance the session's replication origin progress. Holds the origin
    /// lock; can `ereport(ERROR)`, carried on `Err`.
    pub fn replorigin_session_advance(
        remote_commit: types_core::XLogRecPtr,
        local_commit: types_core::XLogRecPtr,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `replorigin_session_origin_timestamp = ts` (origin.c global): write-back
    /// of the derived commit timestamp. Pure write of backend-local state.
    pub fn set_replorigin_session_timestamp(ts: types_core::TimestampTz)
);

seam_core::seam!(
    /// `replorigin_advance(node, remote_commit, local_commit, go_backward,
    /// wal_log)` (origin.c): advance a specific origin's progress during redo
    /// (`PrepareRedoAdd`). The two-arg seam fixes `go_backward = false`,
    /// `wal_log = false` as `PrepareRedoAdd` uses. Can `ereport(ERROR)`.
    pub fn replorigin_advance(
        node: types_core::RepOriginId,
        remote_commit: types_core::XLogRecPtr,
        local_commit: types_core::XLogRecPtr,
    ) -> types_error::PgResult<()>
);
