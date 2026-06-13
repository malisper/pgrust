//! Seam declarations for the `backend-commands-dbcommands` unit (`dbcommands.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `dbase_redo(record)` (dbcommands.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn dbase_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_database_name(dbid)` (dbcommands.c): the database's name copied
    /// out of the syscache into `mcx` (C: `pstrdup` in the current context),
    /// or `None` if there is no such database. `Err` includes OOM from the
    /// copy.
    pub fn get_database_name<'mcx>(mcx: Mcx<'mcx>, dbid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

// --- backend-utils-init-postinit consumer (dbcommands.c) ---

seam_core::seam!(
    /// `database_is_invalid_form(datform)` (dbcommands.c): is the pg_database
    /// row in the "invalid" state (a failed/interrupted CREATE DATABASE)? The
    /// C reads `datform->datconnlimit == DATCONNLIMIT_INVALID_DB`, so only
    /// `datconnlimit` crosses.
    pub fn database_is_invalid_form(datconnlimit: i32) -> bool
);
