//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `AllocateFile(path, PG_BINARY_W)` + `fwrite` + `FreeFile` (fd.c) — write
    /// all of `bytes` to a freshly created file. The caller (snapmgr) chooses
    /// the path and owns the `.tmp`+`rename` ordering; this only performs the
    /// fd.c-tracked open/write/close. Open/write failures surface as
    /// `ereport(ERROR, errcode_for_file_access)` on `Err`.
    pub fn allocate_file_write(path: &str, bytes: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `rename(from, to)` — the rename half of fd.c's atomic-write dance.
    /// Returns the raw `rename(2)` result (`0` on success, `-1` with errno set);
    /// the caller raises the ereport.
    pub fn rename_file(from: &str, to: &str) -> i32
);

seam_core::seam!(
    /// `AllocateFile(path, PG_BINARY_R)` + `fstat` + `fread` + `FreeFile`
    /// (fd.c) — read the whole file into a byte buffer. Returns `Ok(None)` when
    /// the file does not exist (`errno == ENOENT`, which snapmgr maps to its
    /// own "snapshot does not exist" error); other open/read failures surface
    /// as `ereport(ERROR)` on `Err`.
    pub fn allocate_file_read(path: &str) -> PgResult<Option<Vec<u8>>>
);

seam_core::seam!(
    /// `unlink(path)` — remove a file. Returns the raw `unlink(2)` result
    /// (`0` on success, `-1` with errno set); the caller decides the log level.
    pub fn unlink_file(path: &str) -> i32
);

seam_core::seam!(
    /// `AllocateDir(dir)` + `ReadDirExtended(.., LOG)` + `FreeDir` (fd.c) —
    /// list a directory's entries (excluding `.`/`..`). Read problems are
    /// logged at LOG by fd.c and skipped, so this returns the names it could
    /// read. Cannot `ereport` at ERROR.
    pub fn read_dir_names(dir: &str) -> Vec<String>
);

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
