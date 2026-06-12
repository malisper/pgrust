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
    pub fn replorigin_redo(record: &mut types_wal::rmgr::XLogReaderState) -> types_error::PgResult<()>
);
