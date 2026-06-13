//! `fd-temp-files` — temporary-file creation and the temp-tablespace state.
//!
//! `OpenTemporaryFile[InTablespace]`, `TempTablespacePath`, the
//! `PathName{Create,Delete}Temporary{Dir,File}` family,
//! `RegisterTemporaryFile`, the temp-tablespace list
//! (`SetTempTablespaces`/`TempTablespacesAreSet`/`GetTempTablespaces`/
//! `GetNextTempTableSpace`), and the PRNG-based temp-name generation.

use std::path::Path;

use types_core::Oid;
use types_error::PgResult;
use types_storage::File;

/// `OpenTemporaryFile(bool interXact)` (fd.c) — open an anonymous temp file in
/// a temp tablespace, registered for end-of-transaction (or end-of-query)
/// cleanup.
pub fn OpenTemporaryFile(_inter_xact: bool) -> PgResult<File> {
    todo!("fd.c OpenTemporaryFile: choose tablespace, OpenTemporaryFileInTablespace, register")
}

/// `OpenTemporaryFileInTablespace(Oid tblspcOid, bool rejectError)` (fd.c) —
/// open a temp file in the given tablespace.
pub(crate) fn OpenTemporaryFileInTablespace(
    _tblspc_oid: Oid,
    _reject_error: bool,
) -> PgResult<File> {
    todo!("fd.c OpenTemporaryFileInTablespace: build temp dir/path, PathNameOpenFile O_TEMPORARY")
}

/// `TempTablespacePath(char *path, Oid tablespace)` (fd.c) — render the
/// per-tablespace temp directory path.
pub fn TempTablespacePath(_tablespace: Oid) -> String {
    todo!("fd.c TempTablespacePath: base/pgsql_tmp or pg_tblspc/<oid>/<ver>/pgsql_tmp")
}

/// `PathNameCreateTemporaryDir(const char *basedir, const char *directory)`
/// (fd.c).
pub fn PathNameCreateTemporaryDir(
    _basedir: impl AsRef<Path>,
    _directory: impl AsRef<Path>,
) -> PgResult<()> {
    todo!("fd.c PathNameCreateTemporaryDir: MakePGDirectory with parent retry")
}

/// `PathNameDeleteTemporaryDir(const char *dirname)` (fd.c).
pub fn PathNameDeleteTemporaryDir(_dirname: impl AsRef<Path>) -> PgResult<()> {
    todo!("fd.c PathNameDeleteTemporaryDir: walk + unlink + rmdir")
}

/// `PathNameCreateTemporaryFile(const char *path, bool error_on_failure)`
/// (fd.c).
pub fn PathNameCreateTemporaryFile(
    _path: impl AsRef<Path>,
    _error_on_failure: bool,
) -> PgResult<File> {
    todo!("fd.c PathNameCreateTemporaryFile: PathNameOpenFile create + register")
}

/// `PathNameOpenTemporaryFile(const char *path, int mode)` (fd.c).
pub fn PathNameOpenTemporaryFile(_path: impl AsRef<Path>, _mode: i32) -> PgResult<File> {
    todo!("fd.c PathNameOpenTemporaryFile: PathNameOpenFile of an existing shared temp file")
}

/// `PathNameDeleteTemporaryFile(const char *path, bool error_on_failure)`
/// (fd.c) — returns whether the file existed.
pub fn PathNameDeleteTemporaryFile(
    _path: impl AsRef<Path>,
    _error_on_failure: bool,
) -> PgResult<bool> {
    todo!("fd.c PathNameDeleteTemporaryFile: stat + unlink + temp-file accounting")
}

/// `RegisterTemporaryFile(File file)` (fd.c) — mark an open VFD as a temp file
/// to be cleaned up at end of transaction.
pub fn RegisterTemporaryFile(_file: File) {
    todo!("fd.c RegisterTemporaryFile: set FD_CLOSE_AT_EOXACT, have_xact_temporary_files")
}

/// `ReportTemporaryFileUsage(const char *path, off_t size)` (fd.c) — report a
/// just-removed temp file's size to pgstat and, when `log_temp_files >= 0` and
/// the file is large enough, emit a LOG line. Part of the temp-file accounting
/// this family owns; called by `FileClose` and the temp-file delete paths.
pub(crate) fn ReportTemporaryFileUsage(_path: &str, _size: i64) {
    todo!("fd.c ReportTemporaryFileUsage: pgstat_report_tempfile + log_temp_files LOG")
}

// ---------------------------------------------------------------------------
// Temp-tablespace list (fd.c:2900-3000 region).
// ---------------------------------------------------------------------------

/// `SetTempTablespaces(Oid *tableSpaces, int numSpaces)` (fd.c).
pub fn SetTempTablespaces(_table_spaces: &[Oid]) {
    todo!("fd.c SetTempTablespaces: copy into tempTableSpaces, randomize nextTempTableSpace")
}

/// `TempTablespacesAreSet(void)` (fd.c).
pub fn TempTablespacesAreSet() -> bool {
    todo!("fd.c TempTablespacesAreSet: numTempTableSpaces >= 0")
}

/// `GetTempTablespaces(Oid *tableSpaces, int numSpaces)` (fd.c) — copy out the
/// current list; returns the number copied.
pub fn GetTempTablespaces(_table_spaces: &mut [Oid]) -> i32 {
    todo!("fd.c GetTempTablespaces: copy tempTableSpaces out")
}

/// `GetNextTempTableSpace(void)` (fd.c) — round-robin pick.
pub fn GetNextTempTableSpace() -> Oid {
    todo!("fd.c GetNextTempTableSpace: round-robin over tempTableSpaces")
}
