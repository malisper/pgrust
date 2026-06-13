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
