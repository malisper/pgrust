//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::SubTransactionId;

seam_core::seam!(
    /// `MakePGDirectory(directoryName)` (`storage/file/fd.c`) —
    /// `mkdir(directoryName, pg_dir_create_mode)`. Returns the `mkdir`
    /// result (`0` on success, `-1` with errno set on failure); infallible
    /// at the ereport level.
    pub fn make_pg_directory(directory_name: &str) -> i32
);

seam_core::seam!(
    /// `AtEOXact_Files(isCommit)` — close transaction-lifetime files; WARNs
    /// about leaks at commit.
    pub fn at_eoxact_files(is_commit: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_Files(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_files(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);

// --- backend-utils-init-postinit consumers (fd.c) ---

seam_core::seam!(
    /// `InitFileAccess()` (fd.c): initialize the virtual file descriptor cache.
    /// `Err` carries its `ereport` surface.
    pub fn init_file_access() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitTemporaryFileAccess()` (fd.c): set up temporary-file accounting
    /// (after pgstat). `Err` carries its `ereport` surface.
    pub fn init_temporary_file_access() -> types_error::PgResult<()>
);

/// Result of `access(path, F_OK)` (postinit.c database-directory check).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessResult {
    /// `access() == 0` — the path exists.
    Ok,
    /// `errno == ENOENT` — the path does not exist.
    NoEnt,
    /// Any other `errno` (carried as the raw value).
    Other(i32),
}

seam_core::seam!(
    /// `access(path, F_OK)` (unistd, used by InitPostgres): probe whether the
    /// database directory exists. Returns the classified outcome (the C `== -1`
    /// + `errno` branch). `Err` is reserved for the seam's own failure surface
    /// (none expected; OS errno is returned in [`AccessResult::Other`]).
    pub fn access_f_ok(path: &str) -> types_error::PgResult<AccessResult>
);
