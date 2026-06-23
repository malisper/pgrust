//! Seam declarations for the FDW handler dispatch (`foreign/fdwapi.h`).
//!
//! `GetFdwRoutine` (`foreign/foreign.c`) resolves a foreign-data wrapper's
//! callback table by calling its handler function:
//! `datum = OidFunctionCall0(fdwhandler); routine = DatumGetPointer(datum)`,
//! then validates `routine != NULL && IsA(routine, FdwRoutine)`. The handler
//! and the `FdwRoutine` struct it returns are extension-owned (the function
//! pointers cannot cross the owned-tree boundary), so the fmgr dispatch plus
//! the `IsA` node check collapse into this one seam, owned by the FDW-provider
//! layer. It returns the trimmed callback-presence table
//! ([`::nodes::FdwRoutine`]); `None` is the C "did not return an
//! FdwRoutine" failure (`routine == NULL || !IsA(...)`), which the caller turns
//! into its `elog(ERROR, ...)`.
//!
//! The owning unit (the FDW-provider/fmgr layer) installs this from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use ::nodes::FdwRoutine;

seam_core::seam!(
    /// `OidFunctionCall0(fdwhandler)` + `IsA(routine, FdwRoutine)` validation
    /// (`GetFdwRoutine`, foreign.c): invoke the FDW handler and project its
    /// returned `FdwRoutine` struct to the callback-presence table. `Ok(None)`
    /// is the C `routine == NULL || !IsA(routine, FdwRoutine)` case. The
    /// handler may `ereport(ERROR)`, carried on `Err`.
    pub fn fdw_routine_from_handler(fdwhandler: Oid) -> PgResult<Option<FdwRoutine>>
);
