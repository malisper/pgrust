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
    pub fn tblspc_redo(record: &mut wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
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

seam_core::seam!(
    /// `get_tablespace_oid(tablespacename, missing_ok)` (tablespace.c): the
    /// tablespace's OID. With `missing_ok = false` a missing tablespace raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_tablespace_oid(
        tablespacename: &str,
        missing_ok: bool,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `PrepareTempTablespaces(void)` (tablespace.c) — set up the per-session
    /// list of temp tablespaces so `OpenTemporaryFile` can spread temp files
    /// across them. Idempotent; `BufFileCreateTemp` calls it defensively.
    /// Catalog access can `ereport(ERROR)`, carried on `Err`.
    pub fn prepare_temp_tablespaces() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `TablespaceCreateDbspace(spcOid, dbOid, isRedo)` (tablespace.c) — create
    /// the per-database subdirectory under a tablespace the first time it is
    /// used in this database. `md.c`'s `mdcreate` calls it before opening a new
    /// relation file. `Err` carries its `mkdir`/`stat` `ereport(ERROR)`s.
    pub fn tablespace_create_dbspace(
        spc_oid: types_core::Oid,
        db_oid: types_core::Oid,
        is_redo: bool,
    ) -> types_error::PgResult<()>
);
