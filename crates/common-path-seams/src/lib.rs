//! Seam declarations for the path-name helpers of `src/common/path.c` that
//! `src/backend/utils/fmgr/dfmgr.c` reaches.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `canonicalize_path(path)` (`common/path.c`) — clean up the path
    /// (collapse `.`/`..`, drop duplicate/trailing separators). The canonical
    /// form is returned because canonicalization can change the length.
    pub fn canonicalize_path(path: String) -> String
);

seam_core::seam!(
    /// `is_absolute_path(path)` (`common/path.c`, non-Windows: begins with
    /// `/`).
    pub fn is_absolute_path(path: &str) -> bool
);
