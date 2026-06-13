//! Seam declarations for the `backend-utils-misc-more` unit
//! (`utils/misc/ps_status.c`, `pg_controldata.c`, `rls.c`, `superuser.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `init_ps_display(fixed_part)` (`ps_status.c`) — set this process's ps
    /// title; `None` mirrors the C `NULL` (derive the fixed part from
    /// `MyBackendType`). Infallible in C (assert-only).
    pub fn init_ps_display(fixed_part: Option<&str>)
);

// --- backend-utils-init-postinit consumer (ps_status.c) ---

seam_core::seam!(
    /// `set_ps_display(activity)` (ps_status.c): set the process-title activity
    /// string.
    pub fn set_ps_display(activity: &str)
);
