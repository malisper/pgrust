//! Seam declarations for the `backend-storage-file-fileset` unit
//! (`storage/file/fileset.c`): the segment-file create/open/delete protocol
//! over a `FileSet`, used by fileset-based `BufFile`s. The owning unit installs
//! these from its `init_seams()` when it lands; until then a call panics
//! loudly.

use ::types_error::PgResult;
use ::execparallel::FileSetHandle;
use ::types_storage::file::File;

seam_core::seam!(
    /// `File FileSetCreate(FileSet *fileset, const char *name)` (fileset.c) —
    /// create a new segment file in `fileset` with the given (segment) name.
    /// `ereport(ERROR)`s on failure, carried on `Err`; the returned VFD is `> 0`.
    pub fn file_set_create(fileset: FileSetHandle, name: &str) -> PgResult<File>
);

seam_core::seam!(
    /// `File FileSetOpen(FileSet *fileset, const char *name, int mode)`
    /// (fileset.c) — open an existing segment file. Returns the VFD (`> 0`) on
    /// success, or `Ok(File(<= 0))` when the segment does not exist (buffile's
    /// probe loop terminates on `<= 0`). A genuine open error `ereport`s, on `Err`.
    pub fn file_set_open(fileset: FileSetHandle, name: &str, mode: i32) -> PgResult<File>
);

seam_core::seam!(
    /// `bool FileSetDelete(FileSet *fileset, const char *name, bool error_on_failure)`
    /// (fileset.c) — delete a segment file. Returns `true` if a file was
    /// deleted, `false` if it did not exist. With buffile always passing
    /// `error_on_failure = true`, a real unlink failure `ereport`s, on `Err`.
    pub fn file_set_delete(
        fileset: FileSetHandle,
        name: &str,
        error_on_failure: bool,
    ) -> PgResult<bool>
);
