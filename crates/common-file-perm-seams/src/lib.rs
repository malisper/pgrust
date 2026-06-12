//! Seam declarations for the `src/common/file_perm.c` unit (catalog rows
//! `common-batch*`), which owns the data-directory permission globals.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// Read `pg_mode_mask` (`common/file_perm.c`) — the umask for data-
    /// directory file creation (`PG_MODE_MASK_OWNER` = 0077 until
    /// `SetDataDirectoryCreatePerm` chooses group access). A `mode_t` value.
    pub fn pg_mode_mask() -> u32
);
