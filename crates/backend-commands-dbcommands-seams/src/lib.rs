//! Seam declarations for the `backend-commands-dbcommands` unit
//! (`commands/dbcommands.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_database_name(dbid)` (dbcommands.c): the database's name copied
    /// out of the syscache (C: `pstrdup` in the current context), or `None`
    /// if there is no such database. `Err` includes OOM from the copy.
    pub fn get_database_name(dbid: Oid) -> PgResult<Option<String>>
);
