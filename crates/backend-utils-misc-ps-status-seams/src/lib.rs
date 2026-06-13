//! Seam declarations for `utils/misc/ps_status.c` (catalog unit
//! `backend-utils-misc-more`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `set_ps_display_suffix(suffix)` — append a fixed suffix (e.g.
    /// "waiting") to the ps activity display. The implementation copies the
    /// string.
    pub fn set_ps_display_suffix(suffix: &str)
);

seam_core::seam!(
    /// `set_ps_display_remove_suffix()`.
    pub fn set_ps_display_remove_suffix()
);

seam_core::seam!(
    /// `update_process_title` (ps_status.c GUC).
    pub fn update_process_title() -> bool
);

seam_core::seam!(
    /// `init_ps_display(fixed_part)` (ps_status.c) — set the fixed (initial)
    /// part of the process-title display. `fixed_part` is the worker's
    /// `bgw_name` C string (NUL-terminated bytes).
    pub fn init_ps_display(fixed_part: &[u8])
);
