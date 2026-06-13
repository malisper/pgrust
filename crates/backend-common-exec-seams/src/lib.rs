//! Seam declaration for the standalone-process executable/library path
//! resolution `InitStandaloneProcess` performs via `find_my_exec` /
//! `get_pkglib_path` (`src/common/exec.c`). Calls panic until the owner lands.

seam_core::seam!(
    /// Resolve `my_exec_path` (via `find_my_exec(argv0, ...)`) and
    /// `pkglib_path` (via `get_pkglib_path(my_exec_path, ...)`) when no
    /// postmaster has set them (`miscinit.c:203`). `elog(FATAL)` if the
    /// executable path cannot be located.
    pub fn resolve_standalone_paths(argv0: &str) -> types_error::PgResult<()>
);
