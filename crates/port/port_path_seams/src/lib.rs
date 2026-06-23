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

seam_core::seam!(
    /// Read a timezone-abbreviation file named `filename` (no path) from the
    /// installation's `<sharedir>/timezonesets/` directory, as `ParseTzFile`
    /// (`utils/misc/tzparser.c`) does via `get_share_path(my_exec_path, ...)`
    /// (`port/path.c`) followed by `AllocateFile`/`AllocateDir`
    /// (`storage/fd.c`). Returns the file's lines, or a classified open/read
    /// failure so the caller can pick the matching diagnostic. The owner
    /// (`get_share_path` build-config relativization + `fd.c` directory I/O) is
    /// not yet ported; the call panics until it lands.
    pub fn read_tz_file(filename: &str) -> misc_more2::TzFileResult
);
