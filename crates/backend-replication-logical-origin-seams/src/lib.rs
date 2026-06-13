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
    /// `replorigin_session_origin_lsn = lsn` (origin.c session-state global):
    /// record the remote LSN the current session's replication origin has
    /// reached, so streaming can restart from the right place after a crash.
    /// Plain backend-local write; infallible.
    pub fn set_replorigin_session_origin_lsn(lsn: types_core::XLogRecPtr)
);

seam_core::seam!(
    /// `replorigin_session_origin_timestamp = ts` (origin.c session-state
    /// global): record the commit timestamp for the current session's
    /// replication origin. Plain backend-local write; infallible.
    pub fn set_replorigin_session_origin_timestamp(ts: types_core::TimestampTz)
);
