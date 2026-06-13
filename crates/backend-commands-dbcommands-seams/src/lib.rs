//! Seam declarations for the `backend-commands-dbcommands` unit (`dbcommands.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `dbase_redo(record)` (dbcommands.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn dbase_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_database_oid(dbname, missing_ok)` (dbcommands.c) — look up a
    /// database's OID by name. Returns `InvalidOid` when `missing_ok` and the
    /// database does not exist; `ereport(ERROR)`s otherwise (carried on `Err`).
    pub fn get_database_oid(dbname: &str, missing_ok: bool) -> types_error::PgResult<types_core::primitive::Oid>
);
