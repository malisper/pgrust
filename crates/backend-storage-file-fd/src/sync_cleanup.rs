//! `fd-sync-and-cleanup` — fsync helpers, durable rename/unlink, the
//! data-directory sync walk, temp-file removal, and the transaction-end /
//! proc-exit cleanup.
//!
//! The `pg_fsync` family, `fsync_fname`/`durable_rename`/`durable_unlink`,
//! `walkdir`/`SyncDataDirectory`/`fsync_fname_ext`, `RemovePgTempFiles`/
//! `RemovePgTempFilesInDir`, and `AtEOSubXact_Files`/`AtEOXact_Files`/
//! `BeforeShmemExit_Files`/`CleanupTempFiles`. Owns the `at_eoxact_files`
//! and `at_eosubxact_files` seam adapters (installed by `init_seams`).

use std::fs::File as StdFile;
use std::path::Path;

use types_core::SubTransactionId;
use types_error::{ErrorLevel, PgResult};

// ---------------------------------------------------------------------------
// pg_fsync family (fd.c:445-560). Early-out when enableFsync is off.
// ---------------------------------------------------------------------------

/// `pg_fsync(int fd)` (fd.c).
pub fn pg_fsync(_file: &StdFile) -> PgResult<()> {
    todo!("fd.c pg_fsync: pg_fsync_writethrough or pg_fsync_no_writethrough per sync_method")
}

/// `pg_fsync_no_writethrough(int fd)` (fd.c).
pub fn pg_fsync_no_writethrough(_file: &StdFile) -> PgResult<()> {
    todo!("fd.c pg_fsync_no_writethrough: fsync if enableFsync")
}

/// `pg_fsync_writethrough(int fd)` (fd.c).
pub fn pg_fsync_writethrough(_file: &StdFile) -> PgResult<()> {
    todo!("fd.c pg_fsync_writethrough: F_FULLFSYNC / fsync writethrough")
}

/// `pg_fdatasync(int fd)` (fd.c).
pub fn pg_fdatasync(_file: &StdFile) -> PgResult<()> {
    todo!("fd.c pg_fdatasync: fdatasync if enableFsync")
}

/// `pg_file_exists(const char *name)` (fd.c).
pub fn pg_file_exists(_name: impl AsRef<Path>) -> PgResult<bool> {
    todo!("fd.c pg_file_exists: stat, distinguish ENOENT from error")
}

/// `pg_flush_data(int fd, off_t offset, off_t nbytes)` (fd.c).
pub fn pg_flush_data(_file: &StdFile, _offset: i64, _nbytes: i64) -> PgResult<()> {
    todo!("fd.c pg_flush_data: sync_file_range / posix_fadvise(DONTNEED) / msync")
}

/// `pg_truncate(const char *path, off_t length)` (fd.c).
pub fn pg_truncate(_path: impl AsRef<Path>, _length: i64) -> PgResult<()> {
    todo!("fd.c pg_truncate: truncate(2) with data_sync_retry handling")
}

/// `fsync_fname(const char *fname, bool isdir)` (fd.c).
pub fn fsync_fname(_path: impl AsRef<Path>, _isdir: bool) -> PgResult<()> {
    todo!("fd.c fsync_fname: fsync_fname_ext(fname, isdir, false, data_sync_elevel(ERROR))")
}

/// `fsync_fname_ext(const char *fname, bool isdir, bool ignore_perm, int elevel)`
/// (fd.c).
pub fn fsync_fname_ext(
    _fname: impl AsRef<Path>,
    _isdir: bool,
    _ignore_perm: bool,
    _elevel: ErrorLevel,
) -> PgResult<()> {
    todo!("fd.c fsync_fname_ext: open + pg_fsync + close, elevel on failure")
}

/// `durable_rename(const char *oldfile, const char *newfile, int elevel)`
/// (fd.c).
pub fn durable_rename(
    _oldfile: impl AsRef<Path>,
    _newfile: impl AsRef<Path>,
    _elevel: ErrorLevel,
) -> PgResult<()> {
    todo!("fd.c durable_rename: fsync old, rename, fsync new + parent dir")
}

/// `durable_unlink(const char *fname, int elevel)` (fd.c).
pub fn durable_unlink(_fname: impl AsRef<Path>, _elevel: ErrorLevel) -> PgResult<()> {
    todo!("fd.c durable_unlink: unlink + fsync parent dir")
}

/// `data_sync_elevel(int elevel)` (fd.c) — bump to PANIC unless data_sync_retry.
pub fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    crate::vfd_core::data_sync_elevel(elevel)
}

// ---------------------------------------------------------------------------
// Data-directory sync walk (fd.c:3242-3560 region).
// ---------------------------------------------------------------------------

/// `walkdir(const char *path, void (*action)(...), bool process_symlinks, int elevel)`
/// (fd.c) — recurse a directory tree applying `action`.
pub(crate) fn walkdir(
    _path: impl AsRef<Path>,
    _process_symlinks: bool,
    _elevel: ErrorLevel,
    _action: &mut dyn FnMut(&Path, bool, ErrorLevel) -> PgResult<()>,
) -> PgResult<()> {
    todo!("fd.c walkdir: recurse, call action per entry then on the dir itself")
}

/// `unlink_if_exists_fname(const char *fname, bool isdir, int elevel)` (fd.c) —
/// a `walkdir` action: remove the entry (`rmdir` for a directory, `unlink`
/// otherwise), tolerating `ENOENT`, logging other failures at `elevel`.
pub(crate) fn unlink_if_exists_fname(
    _fname: &Path,
    _isdir: bool,
    _elevel: ErrorLevel,
) -> PgResult<()> {
    todo!("fd.c unlink_if_exists_fname: rmdir/unlink tolerating ENOENT, log at elevel")
}

/// `SyncDataDirectory(void)` (fd.c) — fsync (or syncfs) the whole data dir.
pub fn SyncDataDirectory() -> PgResult<()> {
    todo!("fd.c SyncDataDirectory: pre_sync_fname walk + datadir_fsync_fname walk, or syncfs")
}

// ---------------------------------------------------------------------------
// Temp-file removal (fd.c:3320-3560 region).
// ---------------------------------------------------------------------------

/// `RemovePgTempFiles(void)` (fd.c) — remove leftover temp files at startup.
pub fn RemovePgTempFiles() -> PgResult<()> {
    todo!("fd.c RemovePgTempFiles: walk base + tablespaces, RemovePgTempFilesInDir")
}

/// `RemovePgTempFilesInDir(const char *tmpdirname, bool missing_ok, bool unlink_all)`
/// (fd.c).
pub fn RemovePgTempFilesInDir(
    _tmpdirname: impl AsRef<Path>,
    _missing_ok: bool,
    _unlink_all: bool,
) -> PgResult<()> {
    todo!("fd.c RemovePgTempFilesInDir: unlink matching pgsql_tmp entries")
}

/// `looks_like_temp_rel_name(const char *name)` (fd.c).
pub fn looks_like_temp_rel_name(_name: &str) -> bool {
    todo!("fd.c looks_like_temp_rel_name: tNNN_NNN[_fork] pattern")
}

/// `RemovePgTempRelationFiles(const char *tsdirname)` (fd.c).
pub(crate) fn RemovePgTempRelationFiles(_tsdirname: impl AsRef<Path>) -> PgResult<()> {
    todo!("fd.c RemovePgTempRelationFiles: walk dbspaces, RemovePgTempRelationFilesInDbspace")
}

// ---------------------------------------------------------------------------
// Transaction-end / proc-exit cleanup (fd.c:3722-3937 region).
// ---------------------------------------------------------------------------

/// `CleanupTempFiles(bool isCommit, bool isProcExit)` (fd.c).
pub(crate) fn CleanupTempFiles(_is_commit: bool, _is_proc_exit: bool) {
    todo!("fd.c CleanupTempFiles: close FD_CLOSE_AT_EOXACT vfds, WARN on commit leaks")
}

/// `AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)`
/// (fd.c) — reassign allocated descriptors created in the subxact to the parent.
pub fn AtEOSubXact_Files(
    _is_commit: bool,
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
) {
    todo!("fd.c AtEOSubXact_Files: reassign allocatedDescs create_subid to parent")
}

/// `AtEOXact_Files(bool isCommit)` (fd.c) — clean up files at end of xact.
pub fn AtEOXact_Files(_is_commit: bool) {
    todo!("fd.c AtEOXact_Files: CleanupTempFiles(isCommit, false), reset temp tablespaces")
}

/// `BeforeShmemExit_Files(int code, Datum arg)` (fd.c) — proc-exit cleanup.
pub fn BeforeShmemExit_Files() {
    todo!("fd.c BeforeShmemExit_Files: CleanupTempFiles(false, true), closeAllVfds")
}
