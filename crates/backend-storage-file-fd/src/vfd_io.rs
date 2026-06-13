//! `fd-vfd-io` — the per-VFD I/O surface.
//!
//! `PathNameOpenFile[Perm]`, `FileClose`, the AIO read/write family
//! (`FileReadV`/`FileWriteV`/`FileStartReadV`, with `PgAioHandle` reached
//! through the `storage-aio` seam when that lands), and the
//! `FilePrefetch`/`Writeback`/`Sync`/`Zero`/`Fallocate`/`Size`/`Truncate`
//! operations plus `FilePathName`/`FileGetRawDesc`.

use std::io::{IoSlice, IoSliceMut};
use std::os::fd::RawFd;
use std::path::Path;

use types_error::PgResult;
use types_storage::File;

/// `PathNameOpenFile(const char *fileName, int fileFlags)` (fd.c).
pub fn PathNameOpenFile(file_name: impl AsRef<Path>, file_flags: i32) -> PgResult<File> {
    PathNameOpenFilePerm(
        file_name,
        file_flags,
        crate::vfd_core::pg_file_create_mode(),
    )
}

/// `PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)`
/// (fd.c) — allocate a VFD and open the named file into it.
pub fn PathNameOpenFilePerm(
    _file_name: impl AsRef<Path>,
    _file_flags: i32,
    _file_mode: u32,
) -> PgResult<File> {
    todo!("fd.c PathNameOpenFilePerm: AllocateVfd + BasicOpenFilePerm + Insert")
}

/// `FileClose(File file)` (fd.c) — close the VFD, deleting the file if
/// `FD_DELETE_AT_CLOSE`, and free the slot.
pub fn FileClose(_file: File) -> PgResult<()> {
    todo!("fd.c FileClose: LruDelete if open, unlink if delete-at-close, FreeVfd")
}

/// `FilePrefetch(File file, off_t offset, off_t amount, uint32 wait_event_info)`
/// (fd.c).
pub fn FilePrefetch(
    _file: File,
    _offset: i64,
    _amount: i64,
    _wait_event_info: u32,
) -> PgResult<i32> {
    todo!("fd.c FilePrefetch: posix_fadvise(WILLNEED)")
}

/// `FileWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info)`
/// (fd.c).
pub fn FileWriteback(
    _file: File,
    _offset: i64,
    _nbytes: i64,
    _wait_event_info: u32,
) -> PgResult<()> {
    todo!("fd.c FileWriteback: pg_flush_data")
}

/// `FileReadV(File file, const struct iovec *iov, int iovcnt, off_t offset, ...)`
/// (fd.c).
pub fn FileReadV(
    _file: File,
    _iov: &mut [IoSliceMut<'_>],
    _offset: i64,
    _wait_event_info: u32,
) -> PgResult<usize> {
    todo!("fd.c FileReadV: FileAccess + preadv")
}

/// `FileStartReadV(PgAioHandle *ioh, File file, int iovcnt, off_t offset, ...)`
/// (fd.c) — kick off an async read via the AIO engine
/// (`storage-aio` seam once it lands).
pub fn FileStartReadV(
    _file: File,
    _iovcnt: i32,
    _offset: i64,
    _wait_event_info: u32,
) -> PgResult<i32> {
    todo!("fd.c FileStartReadV: FileAccess + pgaio_io_start_readv via storage-aio seam")
}

/// `FileWriteV(File file, const struct iovec *iov, int iovcnt, off_t offset, ...)`
/// (fd.c) — enforces `temp_file_limit` for temp files.
pub fn FileWriteV(
    _file: File,
    _iov: &[IoSlice<'_>],
    _offset: i64,
    _wait_event_info: u32,
) -> PgResult<usize> {
    todo!("fd.c FileWriteV: FileAccess + temp_file_limit check + pwritev")
}

/// `FileSync(File file, uint32 wait_event_info)` (fd.c).
pub fn FileSync(_file: File, _wait_event_info: u32) -> PgResult<()> {
    todo!("fd.c FileSync: FileAccess + pg_fsync")
}

/// `FileZero(File file, off_t offset, off_t amount, uint32 wait_event_info)`
/// (fd.c).
pub fn FileZero(
    _file: File,
    _offset: i64,
    _amount: i64,
    _wait_event_info: u32,
) -> PgResult<i32> {
    todo!("fd.c FileZero: pg_pwrite_zeros")
}

/// `FileFallocate(File file, off_t offset, off_t amount, uint32 wait_event_info)`
/// (fd.c).
pub fn FileFallocate(
    _file: File,
    _offset: i64,
    _amount: i64,
    _wait_event_info: u32,
) -> PgResult<i32> {
    todo!("fd.c FileFallocate: posix_fallocate")
}

/// `FileSize(File file)` (fd.c).
pub fn FileSize(_file: File) -> PgResult<i64> {
    todo!("fd.c FileSize: FileAccess + lseek(SEEK_END)")
}

/// `FileTruncate(File file, off_t offset, uint32 wait_event_info)` (fd.c).
pub fn FileTruncate(_file: File, _offset: i64, _wait_event_info: u32) -> PgResult<i32> {
    todo!("fd.c FileTruncate: FileAccess + ftruncate + temp-file accounting")
}

/// `FilePathName(File file)` (fd.c) — the file name behind a VFD.
pub fn FilePathName(_file: File) -> String {
    todo!("fd.c FilePathName: return VfdCache[file].fileName")
}

/// `FileGetRawDesc(File file)` (fd.c) — the underlying kernel fd.
pub fn FileGetRawDesc(_file: File) -> PgResult<RawFd> {
    todo!("fd.c FileGetRawDesc: FileAccess + return VfdCache[file].fd")
}

/// `FileGetRawFlags(File file)` (fd.c).
pub fn FileGetRawFlags(_file: File) -> i32 {
    todo!("fd.c FileGetRawFlags: return VfdCache[file].fileFlags")
}

/// `FileGetRawMode(File file)` (fd.c).
pub fn FileGetRawMode(_file: File) -> u32 {
    todo!("fd.c FileGetRawMode: return VfdCache[file].fileMode")
}
