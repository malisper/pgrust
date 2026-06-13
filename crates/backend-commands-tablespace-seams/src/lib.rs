//! Seam declarations for the `backend-commands-tablespace` unit (`tablespace.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `tblspc_redo(record)` (tablespace.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn tblspc_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_tablespace_name(spc_oid)` (tablespace.c): the name of the
    /// tablespace, scanned out of pg_tablespace (which has no syscache), or
    /// `Ok(None)` if no such tablespace exists. The name is palloc'd in the
    /// caller's current context (here: `mcx`). `Err` carries the scan's error
    /// surface plus OOM from the copy.
    pub fn get_tablespace_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        spc_oid: types_core::Oid,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);
