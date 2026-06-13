//! Seam declarations for GUC variables defined in `utils/misc/guc_tables.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `cluster_name` (guc_tables.c) — the `cluster_name` GUC string (boot
    /// value `""`); the seam returns an owned copy.
    pub fn cluster_name() -> String
);
