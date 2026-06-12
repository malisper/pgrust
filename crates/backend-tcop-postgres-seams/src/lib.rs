//! Seam declarations for the `backend-tcop-postgres` unit
//! (`tcop/postgres.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` (miscadmin.h): if an interrupt is pending,
    /// service it via `ProcessInterrupts()` (tcop/postgres.c). A query-cancel
    /// or backend-termination interrupt surfaces as the `Err` (the C
    /// `ereport(ERROR/FATAL, ...)` longjmp).
    pub fn check_for_interrupts() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `debug_query_string = NULL` (tcop/postgres.c): reset the
    /// currently-executing query string before exit-time cleanup clobbers it
    /// (`proc_exit_prepare`).
    pub fn reset_debug_query_string()
);
