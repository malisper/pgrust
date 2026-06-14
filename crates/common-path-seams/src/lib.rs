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

seam_core::seam!(
    /// `get_share_path(my_exec_path, ret_path)` (`common/path.c`) — derive the
    /// installation's `share` directory from the executable path. Infallible in
    /// C (writes into a fixed `MAXPGPATH` buffer); the canonical share path is
    /// returned here because its length is not known to the caller.
    pub fn get_share_path(my_exec_path: &str) -> String
);

seam_core::seam!(
    /// `get_progname(argv0)` (`common/path.c`) — strip the directory and any
    /// `.exe` suffix from `argv[0]`, returning the bare program name (a fresh
    /// owned copy, mirroring the C `strdup`). On a NULL/empty `argv0` the C
    /// returns the literal `"(null)"`.
    pub fn get_progname(argv0: &str) -> String
);
