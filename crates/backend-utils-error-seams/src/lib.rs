//! Seam declarations for the `backend-utils-error` unit (`utils/error/elog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::{PgError, PgResult};

seam_core::seam!(
    /// `ereport(level, (errcode(...), errmsg(...), ...))` — emit one report
    /// (errstart/errfinish). For levels below `ERROR` the report is emitted and
    /// `Ok(())` returned (C `errfinish` returns); at `ERROR` and above the
    /// report comes back as `Err` (the C longjmp).
    pub fn ereport(err: PgError) -> PgResult<()>
);
