//! Seam declarations for the `port/path.c` path utilities.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `is_absolute_path(filename)` (`port/path.c`): whether `filename` is an
    /// absolute path (a leading `/` on POSIX, or a drive/UNC prefix on
    /// Windows). Pure string predicate, infallible.
    pub fn is_absolute_path(filename: &str) -> bool
);
