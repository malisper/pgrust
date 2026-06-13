//! `fd-allocated-desc` — the `allocatedDescs` table and the stdio/dir/pipe/
//! transient-fd handle families.
//!
//! `AllocateFile`/`FreeFile`, `OpenTransientFile[Perm]`/`CloseTransientFile`,
//! `OpenPipeStream`/`ClosePipeStream`, `AllocateDir`/`ReadDir`/`ReadDirExtended`/
//! `FreeDir`, and `closeAllVfds`. Owns the `with_allocated_dir`,
//! `open_transient_file` and `close_transient_file` seam adapters
//! (installed by `init_seams`).

use std::os::fd::RawFd;
use std::path::Path;

use types_error::{ErrorLevel, PgResult};
use types_storage::{Dir, DirEnt};

/// `reserveAllocatedDesc(void)` (fd.c) — ensure room in `allocatedDescs`,
/// growing it (and `maxAllocatedDescs`) as needed. Returns whether room exists.
pub(crate) fn reserveAllocatedDesc() -> bool {
    todo!("fd.c reserveAllocatedDesc: grow allocatedDescs up to max_safe_fds/2")
}

/// `FreeDesc(AllocateDesc *desc)` (fd.c) — close one allocated descriptor and
/// compact the table.
pub(crate) fn FreeDesc(_index: i32) -> PgResult<i32> {
    todo!("fd.c FreeDesc: close the underlying handle, remove from allocatedDescs")
}

/// `AllocateFile(const char *name, const char *mode)` (fd.c) — `fopen` a
/// tracked stdio stream; returns its index in the allocated-descriptor table.
pub fn AllocateFile(_name: impl AsRef<Path>, _mode: &str) -> PgResult<i32> {
    todo!("fd.c AllocateFile: reserveAllocatedDesc + fopen + record AllocateDescFile")
}

/// `FreeFile(FILE *file)` (fd.c) — `fclose` a stream opened with `AllocateFile`.
pub fn FreeFile(_index_to_free: i32) -> PgResult<()> {
    todo!("fd.c FreeFile: locate AllocateDescFile, FreeDesc")
}

/// `OpenTransientFile(const char *fileName, int fileFlags)` (fd.c).
pub fn OpenTransientFile(file_name: impl AsRef<Path>, file_flags: i32) -> PgResult<i32> {
    OpenTransientFilePerm(file_name, file_flags, crate::vfd_core::pg_file_create_mode())
}

/// `OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode)`
/// (fd.c) — open a tracked raw kernel fd for transaction-end cleanup.
pub fn OpenTransientFilePerm(
    _file_name: impl AsRef<Path>,
    _file_flags: i32,
    _file_mode: u32,
) -> PgResult<i32> {
    todo!("fd.c OpenTransientFilePerm: reserveAllocatedDesc + BasicOpenFilePerm + record RawFD")
}

/// `CloseTransientFile(int fd)` (fd.c) — close an `OpenTransientFile` handle.
pub fn CloseTransientFile(_index_to_close: i32) -> PgResult<()> {
    todo!("fd.c CloseTransientFile: locate AllocateDescRawFD, FreeDesc")
}

/// `OpenPipeStream(const char *command, const char *mode)` (fd.c) — `popen` a
/// tracked pipe stream.
pub fn OpenPipeStream(_command: &str, _mode: &str) -> PgResult<i32> {
    todo!("fd.c OpenPipeStream: reserveAllocatedDesc + popen + record AllocateDescPipe")
}

/// `ClosePipeStream(FILE *file)` (fd.c) — `pclose` a pipe; returns wait status.
pub fn ClosePipeStream(_index: i32) -> PgResult<i32> {
    todo!("fd.c ClosePipeStream: locate AllocateDescPipe, FreeDesc, return pclose status")
}

/// `AllocateDir(const char *dirname)` (fd.c) — `opendir` a tracked directory.
/// `Ok(None)` mirrors C returning NULL (caller checks errno).
pub fn AllocateDir(_dirname: impl AsRef<Path>) -> PgResult<Option<Dir>> {
    todo!("fd.c AllocateDir: reserveAllocatedDesc + opendir + record AllocateDescDir")
}

/// `ReadDir(DIR *dir, const char *dirname)` (fd.c).
pub fn ReadDir(dir: Option<Dir>, dirname: impl AsRef<Path>) -> PgResult<Option<DirEnt>> {
    ReadDirExtended(dir, dirname, types_error::ERROR)
}

/// `ReadDirExtended(DIR *dir, const char *dirname, int elevel)` (fd.c).
pub fn ReadDirExtended(
    _dir: Option<Dir>,
    _dirname: impl AsRef<Path>,
    _elevel: ErrorLevel,
) -> PgResult<Option<DirEnt>> {
    todo!("fd.c ReadDirExtended: readdir, skip . and .., ereport(elevel) on error")
}

/// `FreeDir(DIR *dir)` (fd.c).
pub fn FreeDir(_dir: Option<Dir>) -> PgResult<()> {
    todo!("fd.c FreeDir: locate AllocateDescDir, FreeDesc")
}

/// `closeAllVfds(void)` (fd.c) — close every open VFD (used before EXEC_BACKEND
/// fork).
pub fn closeAllVfds() {
    todo!("fd.c closeAllVfds: LruDelete every open VFD")
}

/// Raw kernel fd behind a transient-file index (helper for callers that need
/// the fd, e.g. `fstat`).
pub fn TransientFileRawFd(_index: i32) -> Result<RawFd, i32> {
    todo!("fd.c: return AllocateDescRawFD.fd")
}

// ---------------------------------------------------------------------------
// Seam adapters installed by `init_seams`.
// ---------------------------------------------------------------------------

/// `AllocateDir`/`ReadDir`/`FreeDir` as one owned walk — the seam shape for
/// `with_allocated_dir`. Opens the directory, invokes `f` with each entry's
/// `d_name`, and closes it on every path. `f` returns `Ok(true)` to stop the
/// scan early; the walk returns the last callback value (`false` once the
/// directory is exhausted).
pub fn with_allocated_dir(
    _dirname: &str,
    _f: &mut dyn FnMut(&str) -> PgResult<bool>,
) -> PgResult<bool> {
    todo!("fd.c AllocateDir + ReadDir loop + FreeDir; close on every path including f's Err")
}

/// Seam adapter for `open_transient_file`.
pub fn seam_open_transient_file(file_name: &str, file_flags: i32) -> PgResult<i32> {
    OpenTransientFile(file_name, file_flags)
}

/// Seam adapter for `close_transient_file` — returns the `close()` result.
pub fn seam_close_transient_file(_fd: i32) -> i32 {
    todo!("fd.c CloseTransientFile: return close() result for the seam shape")
}
