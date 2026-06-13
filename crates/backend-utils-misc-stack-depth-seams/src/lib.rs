//! Seam declarations for the `backend-utils-misc-stack-depth` unit
//! (`utils/misc/stack_depth.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `check_stack_depth()` (miscadmin.h / utils/misc/stack_depth.c) —
    /// `ereport(ERROR, ERRCODE_STATEMENT_TOO_COMPLEX)` if the execution stack
    /// has grown too deep; the `Err` is that ereport.
    pub fn check_stack_depth() -> types_error::PgResult<()>
);
