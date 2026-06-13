//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::SubTransactionId;
use types_error::PgResult;

/// One directory entry as returned by [`list_dir`] — mirrors the parts of
/// `struct dirent` + `struct stat` that the `pg_ls_*` callers read.
pub struct DirEntryInfo<'mcx> {
    /// `de->d_name` — the entry's file name.
    pub name: mcx::PgString<'mcx>,
    /// `attrib.st_size` — file size in bytes.
    pub size: i64,
    /// `attrib.st_mtime` converted via `time_t_to_timestamptz` — last
    /// modification time.
    pub modification: types_core::TimestampTz,
    /// `S_ISDIR(attrib.st_mode)` — is the entry a directory?
    pub isdir: bool,
    /// `S_ISREG(attrib.st_mode)` — is the entry a regular file?
    pub isreg: bool,
}

/// The result of [`stat_file`] — the parts of `struct stat` that
/// `pg_stat_file` exposes, with `time_t` fields already converted via
/// `time_t_to_timestamptz`.
#[derive(Clone, Copy, Debug)]
pub struct StatInfo {
    /// `fst.st_size`.
    pub size: i64,
    /// `time_t_to_timestamptz(fst.st_atime)`.
    pub access: types_core::TimestampTz,
    /// `time_t_to_timestamptz(fst.st_mtime)`.
    pub modification: types_core::TimestampTz,
    /// `time_t_to_timestamptz(fst.st_ctime)` (Unix status-change time).
    pub change: types_core::TimestampTz,
    /// `S_ISDIR(fst.st_mode)`.
    pub isdir: bool,
}

seam_core::seam!(
    /// `MakePGDirectory(directoryName)` (`storage/file/fd.c`) —
    /// `mkdir(directoryName, pg_dir_create_mode)`. Returns the `mkdir`
    /// result (`0` on success, `-1` with errno set on failure); infallible
    /// at the ereport level.
    pub fn make_pg_directory(directory_name: &str) -> i32
);

seam_core::seam!(
    /// The `read_binary_file()` core (genfile.c) once the filename is
    /// validated: `AllocateFile(PG_BINARY_R)`, `fseeko` to `seek_offset`
    /// (`SEEK_SET` if `>= 0`, else `SEEK_END`), then read either exactly
    /// `bytes_to_read` bytes (when `>= 0`) or the rest of the file (when
    /// negative, capped at `MaxAllocSize - VARHDRSZ`), and `FreeFile`. The
    /// `AllocateFile`/stdio machinery is fd-owned; the seam returns the raw
    /// bytes in `mcx`. `Ok(None)` mirrors C's `missing_ok && errno == ENOENT`
    /// (file absent); `Err` carries open/seek/read/`file length too large`
    /// `ereport(ERROR)`s.
    pub fn read_server_file<'mcx>(
        mcx: Mcx<'mcx>,
        filename: &str,
        seek_offset: i64,
        bytes_to_read: i64,
        missing_ok: bool,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `stat(filename, &fst)` behind the fd owner (genfile.c `pg_stat_file`).
    /// `Ok(None)` mirrors C's `missing_ok && errno == ENOENT`; other failures
    /// raise `could not stat file` as `Err`.
    pub fn stat_file(filename: &str, missing_ok: bool) -> PgResult<Option<StatInfo>>
);

seam_core::seam!(
    /// `AllocateDir(dirname)` + the full `ReadDir` walk + `FreeDir`
    /// (genfile.c). Returns one [`DirEntryInfo`] per entry (including `.`/`..`
    /// and the per-file `stat`; the caller applies the dot-dir / hidden-file /
    /// regular-file filters exactly as the specific `pg_ls_*` variant does).
    /// `Ok(None)` mirrors `missing_ok && errno == ENOENT` (the directory is
    /// absent); `Err` carries the `ReadDir`/`stat` `ereport(ERROR)`s.
    pub fn list_dir<'mcx>(
        mcx: Mcx<'mcx>,
        dirname: &str,
        missing_ok: bool,
    ) -> PgResult<Option<PgVec<'mcx, DirEntryInfo<'mcx>>>>
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
