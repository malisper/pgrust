//! `fd-vfd-io` — the per-VFD I/O surface.
//!
//! `PathNameOpenFile[Perm]`, `FileClose`, the AIO read/write family
//! (`FileReadV`/`FileWriteV`/`FileStartReadV`, with `PgAioHandle` reached
//! through the `storage-aio` seam when that lands), and the
//! `FilePrefetch`/`Writeback`/`Sync`/`Zero`/`Fallocate`/`Size`/`Truncate`
//! operations plus `FilePathName`/`FileGetRawDesc`.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
#[cfg(not(target_family = "wasm"))]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(not(target_family = "wasm"))]
use std::os::unix::io::FromRawFd;
#[cfg(target_family = "wasm")]
use wasm_libc_shim::osfd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;

use backend_utils_error::ereport;
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::File;

use backend_storage_aio_aio_seams as aio_seams;
use backend_utils_activity_waitevent_seams as waitevent;

use crate::vfd_core::{self, FD_DELETE_AT_CLOSE, FD_TEMP_FILE_LIMIT};

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("fd.c", 0, funcname)
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}
#[cfg(not(target_os = "macos"))]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}
fn get_errno() -> i32 {
    unsafe { *errno_location() }
}
fn set_errno(value: i32) {
    unsafe {
        *errno_location() = value;
    }
}

/// `PG_O_DIRECT` (`storage/fd.h`) — the open-flag bit indicating direct I/O.
/// `O_DIRECT` on Linux; the synthetic `F_NOCACHE` sentinel on macOS; 0 where
/// unsupported.
#[cfg(target_os = "linux")]
const PG_O_DIRECT: i32 = libc::O_DIRECT;
#[cfg(target_os = "macos")]
const PG_O_DIRECT: i32 = 0x8000_0000u32 as i32;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const PG_O_DIRECT: i32 = 0;

/// `BLCKSZ` — the buffer/page size; the unit of the `pg_pwrite_zeros` iovec
/// loop.
const BLCKSZ: usize = 8192;
/// `PG_IOV_MAX` — the most iovecs a single vectored write batches.
const PG_IOV_MAX: usize = 32;

// ---------------------------------------------------------------------------
// VFD field access. A `File` is an index into the per-backend VFD cache; the
// kernel handle is the owned `StdFile` inside `Vfd`. After `FileAccess`
// succeeds the handle is open, so reading its raw fd is sound.
// ---------------------------------------------------------------------------

/// The kernel fd backing an *open* VFD (the C `VfdCache[file].fd`).
fn vfd_raw_fd(file: i32) -> RawFd {
    vfd_core::with_fd(|fd| {
        fd.vfd_cache[file as usize]
            .handle
            .as_ref()
            .expect("vfd_raw_fd on a closed VFD")
            .as_raw_fd()
    })
}

/// `pg_preadv(fd, iov, iovcnt, offset)` (port) — positioned vectored read.
fn pg_preadv(fd: RawFd, iov: &mut [std::io::IoSliceMut<'_>], offset: i64) -> isize {
    unsafe {
        libc::preadv(
            fd,
            iov.as_mut_ptr() as *const libc::iovec,
            iov.len() as libc::c_int,
            offset as libc::off_t,
        )
    }
}

/// `pg_ftruncate(int fd, off_t length)` (fd.c:703) — `ftruncate(2)` with an
/// `EINTR` retry loop. Returns the raw `ftruncate` result (0 / -1 with errno).
fn pg_ftruncate(fd: RawFd, length: i64) -> i32 {
    loop {
        let ret = unsafe { libc::ftruncate(fd, length as libc::off_t) };
        if ret == -1 && get_errno() == libc::EINTR {
            continue;
        }
        return ret;
    }
}

/// `pg_pwritev(fd, iov, iovcnt, offset)` (port) — positioned vectored write.
fn pg_pwritev(fd: RawFd, iov: &[std::io::IoSlice<'_>], offset: i64) -> isize {
    unsafe {
        libc::pwritev(
            fd,
            iov.as_ptr() as *const libc::iovec,
            iov.len() as libc::c_int,
            offset as libc::off_t,
        )
    }
}

// ---------------------------------------------------------------------------
// PathNameOpenFile[Perm] (fd.c:1578-1650).
// ---------------------------------------------------------------------------

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
    file_name: impl AsRef<Path>,
    mut file_flags: i32,
    file_mode: u32,
) -> PgResult<File> {
    let file_name = file_name.as_ref();
    // We need an owned copy of the file name; fail cleanly if no room. (Rust's
    // owned `String` replaces fd.c's strdup; OOM aborts rather than returning.)
    let fnamecopy = file_name
        .to_str()
        .expect("PathNameOpenFilePerm: non-UTF-8 path")
        .to_owned();

    let file = vfd_core::with_fd(vfd_core::AllocateVfd)?;

    // Close excess kernel FDs.
    vfd_core::with_fd(vfd_core::ReleaseLruFiles)?;

    // Descriptors managed by VFDs are implicitly marked O_CLOEXEC.
    file_flags |= libc::O_CLOEXEC;

    let handle = match vfd_core::BasicOpenFilePerm(file_name, file_flags, file_mode) {
        Ok(h) => h,
        Err(_) => {
            // BasicOpenFilePerm failed (errno set). Mirror C: free the VFD and
            // return -1 with errno preserved.
            let save_errno = get_errno();
            vfd_core::with_fd(|fd| vfd_core::FreeVfd(fd, file));
            set_errno(save_errno);
            return Ok(File(-1));
        }
    };

    vfd_core::with_fd(|fd| {
        let vfd_p = &mut fd.vfd_cache[file as usize];
        vfd_p.handle = Some(handle);
        vfd_p.is_open = true;
        vfd_p.file_name = Some(fnamecopy);
        // Saved flags are adjusted to be OK for re-opening file.
        vfd_p.file_flags = file_flags & !(libc::O_CREAT | libc::O_TRUNC | libc::O_EXCL);
        vfd_p.file_mode = file_mode;
        vfd_p.file_size = 0;
        vfd_p.fdstate = 0x0;
        vfd_p.has_resowner = false;
    });
    vfd_core::with_fd(|fd| fd.nfile += 1);

    vfd_core::with_fd(|fd| vfd_core::Insert(fd, file));

    Ok(File(file))
}

// ---------------------------------------------------------------------------
// FileClose (fd.c:1982-2078).
// ---------------------------------------------------------------------------

/// `FileClose(File file)` (fd.c) — close the VFD, deleting the file if
/// `FD_DELETE_AT_CLOSE`, and free the slot.
pub fn FileClose(file: File) -> PgResult<()> {
    let file = file.0;
    // If the kernel handle is open, hand off any in-flight AIO, close it, and
    // remove from the LRU ring.
    let is_open = vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].is_open);
    if is_open {
        let raw_fd = vfd_raw_fd(file);
        aio_seams::pgaio_closing_fd::call(raw_fd);

        // Close the file by dropping the owned handle. We may need to panic on
        // failure to close non-temporary files (see LruDelete); dropping a
        // StdFile cannot surface a close error, so the elevel branch in C is
        // moot here.
        let (handle, fdstate) = vfd_core::with_fd(|fd| {
            let vfd_p = &mut fd.vfd_cache[file as usize];
            vfd_p.is_open = false;
            (vfd_p.handle.take(), vfd_p.fdstate)
        });
        let _ = fdstate;
        drop(handle);
        vfd_core::with_fd(|fd| fd.nfile -= 1);

        // Remove the file from the lru ring.
        vfd_core::with_fd(|fd| vfd_core::Delete(fd, file));
    }

    let fdstate = vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].fdstate);

    if fdstate & FD_TEMP_FILE_LIMIT != 0 {
        // Subtract its size from current usage (do first in case of error).
        // The size update is folded into this single `with_fd` borrow: calling
        // the `fd_sub_temp_size` helper here would re-enter `with_fd` while the
        // outer borrow is live and panic ("RefCell already borrowed").
        vfd_core::with_fd(|fd| {
            let sz = fd.vfd_cache[file as usize].file_size;
            fd.temporary_files_size = fd.temporary_files_size.wrapping_sub(sz as u64);
            fd.vfd_cache[file as usize].file_size = 0;
        });
    }

    // Delete the file if it was temporary, and make a log entry if wanted.
    if fdstate & FD_DELETE_AT_CLOSE != 0 {
        // Reset the flag first to ensure that a repeat (abort-path) call can't
        // loop; the worst-case consequence is failing to emit log message(s),
        // not failing to attempt the unlink.
        vfd_core::with_fd(|fd| {
            fd.vfd_cache[file as usize].fdstate &= !FD_DELETE_AT_CLOSE;
        });

        let file_name = vfd_core::with_fd(|fd| {
            fd.vfd_cache[file as usize]
                .file_name
                .clone()
                .expect("FileClose: FD_DELETE_AT_CLOSE on unnamed VFD")
        });

        // First try the stat().
        let mut statbuf: libc::stat = unsafe { std::mem::zeroed() };
        let cpath = path_cstring(&file_name);
        let stat_errno = if unsafe { libc::stat(cpath.as_ptr(), &mut statbuf) } != 0 {
            get_errno()
        } else {
            0
        };

        // In any case do the unlink.
        if unsafe { libc::unlink(cpath.as_ptr()) } != 0 {
            let en = get_errno();
            ereport(types_error::LOG)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not delete file \"{file_name}\": %m"))
                .finish(loc("FileClose"))?;
        }

        // And last report the stat results.
        if stat_errno == 0 {
            crate::temp_files::ReportTemporaryFileUsage(&file_name, statbuf.st_size as u64);
        } else {
            set_errno(stat_errno);
            ereport(types_error::LOG)
                .with_saved_errno(stat_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{file_name}\": %m"))
                .finish(loc("FileClose"))?;
        }
    }

    // Unregister it from the resource owner.
    if vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].has_resowner) {
        vfd_core::ResourceOwnerForgetFile(file);
    }

    // Return the Vfd slot to the free list.
    vfd_core::with_fd(|fd| vfd_core::FreeVfd(fd, file));

    Ok(())
}

/// Build a NUL-terminated C path from a byte path for the raw libc calls.
fn path_cstring(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path.as_bytes()).expect("path contains interior NUL")
}


// ---------------------------------------------------------------------------
// FilePrefetch / FileWriteback (fd.c:2082-2162).
// ---------------------------------------------------------------------------

/// `FilePrefetch(File file, off_t offset, off_t amount, uint32 wait_event_info)`
/// (fd.c).
pub fn FilePrefetch(
    file: File,
    offset: i64,
    amount: i64,
    wait_event_info: u32,
) -> PgResult<i32> {
    let file = file.0;
    // macOS uses fcntl(F_RDADVISE); Linux/others with posix_fadvise use
    // POSIX_FADV_WILLNEED. Either way we must first make sure the VFD is open.
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code);
    }
    let raw_fd = vfd_raw_fd(file);

    #[cfg(target_os = "macos")]
    {
        #[repr(C)]
        struct Radvisory {
            ra_offset: libc::off_t,
            ra_count: libc::c_int,
        }
        let ra = Radvisory {
            ra_offset: offset as libc::off_t,
            ra_count: amount as libc::c_int,
        };
        waitevent::pgstat_report_wait_start::call(wait_event_info);
        let rc = unsafe { libc::fcntl(raw_fd, libc::F_RDADVISE, &ra) };
        waitevent::pgstat_report_wait_end::call();
        if rc != -1 {
            Ok(0)
        } else {
            Ok(get_errno())
        }
    }
    #[cfg(target_os = "linux")]
    {
        loop {
            waitevent::pgstat_report_wait_start::call(wait_event_info);
            let return_code = unsafe {
                libc::posix_fadvise(raw_fd, offset, amount, libc::POSIX_FADV_WILLNEED)
            };
            waitevent::pgstat_report_wait_end::call();

            if return_code == libc::EINTR {
                continue;
            }
            return Ok(return_code);
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (raw_fd, offset, amount, wait_event_info);
        Ok(0)
    }
}

/// `FileWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info)`
/// (fd.c).
pub fn FileWriteback(
    file: File,
    offset: i64,
    nbytes: i64,
    wait_event_info: u32,
) -> PgResult<()> {
    let file = file.0;
    if nbytes <= 0 {
        return Ok(());
    }

    if vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].file_flags) & PG_O_DIRECT != 0 {
        return Ok(());
    }

    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(());
    }
    let handle = std::mem::ManuallyDrop::new(unsafe { take_borrowed(file) });

    waitevent::pgstat_report_wait_start::call(wait_event_info);
    crate::sync_cleanup::pg_flush_data(&handle, offset, nbytes)?;
    waitevent::pgstat_report_wait_end::call();

    Ok(())
}

/// Materialize a borrowed `StdFile` view over a VFD's kernel fd without owning
/// it (the cache keeps ownership). The caller wraps it in `ManuallyDrop` so the
/// fd is never closed here.
unsafe fn take_borrowed(file: i32) -> std::fs::File {
    std::fs::File::from_raw_fd(vfd_raw_fd(file))
}

// ---------------------------------------------------------------------------
// FileReadV / FileStartReadV / FileWriteV (fd.c:2164-2349).
// ---------------------------------------------------------------------------

/// `FileReadV(File file, const struct iovec *iov, int iovcnt, off_t offset, ...)`
/// (fd.c).
pub fn FileReadV(
    file: File,
    iov: &mut [std::io::IoSliceMut<'_>],
    offset: i64,
    wait_event_info: u32,
) -> PgResult<isize> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code as isize);
    }
    let raw_fd = vfd_raw_fd(file);

    loop {
        waitevent::pgstat_report_wait_start::call(wait_event_info);
        let return_code = pg_preadv(raw_fd, iov, offset);
        waitevent::pgstat_report_wait_end::call();

        if return_code < 0 {
            // OK to retry if interrupted.
            if get_errno() == libc::EINTR {
                continue;
            }
        }

        return Ok(return_code);
    }
}

/// `FileStartReadV(PgAioHandle *ioh, File file, int iovcnt, off_t offset, ...)`
/// (fd.c) — stage an async read via the AIO engine. The handle's iovec is set
/// up on the AIO side, so only `fd`/`iovcnt`/`offset` cross the
/// `storage-aio` seam (panics until the AIO unit lands).
pub fn FileStartReadV(
    file: File,
    iovcnt: i32,
    offset: i64,
    _wait_event_info: u32,
) -> PgResult<i32> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code);
    }
    let raw_fd = vfd_raw_fd(file);

    aio_seams::pgaio_io_start_readv::call(raw_fd, iovcnt, offset as u64);

    Ok(0)
}

/// `FileWriteV(File file, const struct iovec *iov, int iovcnt, off_t offset, ...)`
/// (fd.c) — enforces `temp_file_limit` for temp files.
pub fn FileWriteV(
    file: File,
    iov: &[std::io::IoSlice<'_>],
    offset: i64,
    wait_event_info: u32,
) -> PgResult<isize> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code as isize);
    }
    let raw_fd = vfd_raw_fd(file);

    // If enforcing temp_file_limit and it's a temp file, check whether the
    // write would overrun temp_file_limit, and throw error if so.
    let temp_file_limit = vfd_core::temp_file_limit();
    let (fdstate, file_size, temporary_files_size) = vfd_core::with_fd(|fd| {
        let vfd_p = &fd.vfd_cache[file as usize];
        (vfd_p.fdstate, vfd_p.file_size, fd.temporary_files_size)
    });
    if temp_file_limit >= 0 && (fdstate & FD_TEMP_FILE_LIMIT != 0) {
        let mut past_write = offset;
        for s in iov {
            past_write += s.len() as i64;
        }

        if past_write > file_size {
            let mut new_total = temporary_files_size;
            new_total += (past_write - file_size) as u64;
            if new_total > (temp_file_limit as u64) * 1024u64 {
                ereport(ERROR)
                    .errcode(types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                    .errmsg(format!(
                        "temporary file size exceeds \"temp_file_limit\" ({temp_file_limit}kB)"
                    ))
                    .finish(loc("FileWriteV"))?;
            }
        }
    }

    loop {
        waitevent::pgstat_report_wait_start::call(wait_event_info);
        let return_code = pg_pwritev(raw_fd, iov, offset);
        waitevent::pgstat_report_wait_end::call();

        if return_code >= 0 {
            // Some callers expect short writes to set errno; traditionally a
            // short write implies disk space shortage. Set ENOSPC for all
            // successful writes so a caller that finds the write short can
            // ereport "%m".
            set_errno(libc::ENOSPC);

            // Maintain fileSize and temporary_files_size if it's a temp file.
            if fdstate & FD_TEMP_FILE_LIMIT != 0 {
                let past_write = offset + return_code as i64;
                vfd_core::with_fd(|fd| {
                    let cur = fd.vfd_cache[file as usize].file_size;
                    if past_write > cur {
                        fd.temporary_files_size += (past_write - cur) as u64;
                        fd.vfd_cache[file as usize].file_size = past_write;
                    }
                });
            }
            return Ok(return_code);
        } else {
            // OK to retry if interrupted.
            if get_errno() == libc::EINTR {
                continue;
            }
            return Ok(return_code);
        }
    }
}

// ---------------------------------------------------------------------------
// FileSync / FileZero / FileFallocate (fd.c:2351-2461).
// ---------------------------------------------------------------------------

/// `FileSync(File file, uint32 wait_event_info)` (fd.c).
pub fn FileSync(file: File, wait_event_info: u32) -> PgResult<()> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(());
    }
    let handle = std::mem::ManuallyDrop::new(unsafe { take_borrowed(file) });

    waitevent::pgstat_report_wait_start::call(wait_event_info);
    let result = crate::sync_cleanup::pg_fsync(&handle);
    waitevent::pgstat_report_wait_end::call();

    result
}

/// `FileZero(File file, off_t offset, off_t amount, uint32 wait_event_info)`
/// (fd.c) — zero a region of the file. Returns 0 on success, -1 otherwise with
/// errno set.
pub fn FileZero(
    file: File,
    offset: i64,
    amount: i64,
    wait_event_info: u32,
) -> PgResult<i32> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code);
    }
    let raw_fd = vfd_raw_fd(file);

    waitevent::pgstat_report_wait_start::call(wait_event_info);
    let written = pg_pwrite_zeros(raw_fd, amount as usize, offset);
    waitevent::pgstat_report_wait_end::call();

    if written < 0 {
        Ok(-1)
    } else if written != amount as isize {
        // If errno is unset, assume the problem is no disk space.
        if get_errno() == 0 {
            set_errno(libc::ENOSPC);
        }
        Ok(-1)
    } else {
        Ok(0)
    }
}

/// `pg_pwrite_zeros(int fd, size_t size, off_t offset)` (common/file_utils.c) —
/// write `size` bytes of zeros at `offset` using vectored I/O, returning the
/// total written or a negative value with errno set.
pub(crate) fn pg_pwrite_zeros(fd: RawFd, size: usize, mut offset: i64) -> isize {
    // A single shared zero block, written repeatedly via the iovec batch.
    let zbuffer = [0u8; BLCKSZ];
    let mut remaining_size = size;
    let mut total_written: isize = 0;

    while remaining_size > 0 {
        let mut iov: Vec<std::io::IoSlice<'_>> = Vec::with_capacity(PG_IOV_MAX);
        while iov.len() < PG_IOV_MAX && remaining_size > 0 {
            let this_iov_size = remaining_size.min(BLCKSZ);
            iov.push(std::io::IoSlice::new(&zbuffer[..this_iov_size]));
            remaining_size -= this_iov_size;
        }

        let written = pg_pwritev_with_retry(fd, &iov, offset);

        if written < 0 {
            return written;
        }

        offset += written as i64;
        total_written += written;
    }

    total_written
}

/// `pg_pwritev_with_retry(int fd, const struct iovec *iov, int iovcnt, off_t offset)`
/// (common/file_utils.c) — `pg_pwritev` that retries on partial write. On error
/// it returns -1 (it is unspecified how much was written).
fn pg_pwritev_with_retry(fd: RawFd, iov: &[std::io::IoSlice<'_>], mut offset: i64) -> isize {
    // We'd better have space to make a copy, in case we need to retry.
    if iov.len() > PG_IOV_MAX {
        set_errno(libc::EINVAL);
        return -1;
    }

    // First loop uses the caller's array; later loops use a local mutable copy.
    let mut iov_copy: Vec<std::io::IoSlice<'_>> = iov.to_vec();
    let mut cur: &mut [std::io::IoSlice<'_>] = &mut iov_copy;
    let mut sum: isize = 0;

    loop {
        // Write as much as we can.
        let part = pg_pwritev(fd, cur, offset);
        if part < 0 {
            return -1;
        }

        // Count our progress.
        sum += part;
        offset += part as i64;

        // See what is left: drop fully written leading slices and trim the
        // partially written one (`compute_remaining_iovec`).
        cur = compute_remaining_iovec(cur, part as usize);
        if cur.is_empty() {
            return sum;
        }
    }
}

/// `compute_remaining_iovec(...)` (common/file_utils.c) — advance an iovec list
/// past `written` already-written bytes, returning the remaining slices.
fn compute_remaining_iovec<'a, 'b>(
    iov: &'a mut [std::io::IoSlice<'b>],
    mut written: usize,
) -> &'a mut [std::io::IoSlice<'b>] {
    let total = iov.len();
    let mut start = 0usize;
    while start < total {
        let len = iov[start].len();
        if written >= len {
            written -= len;
            start += 1;
        } else {
            break;
        }
    }
    if start >= total {
        return &mut iov[total..];
    }
    let tail = &mut iov[start..];
    if written > 0 {
        // Trim the partially written leading slice. SAFETY: the slice only ever
        // shrinks forward into the same zero buffer, which outlives this call.
        let rest = tail[0].len() - written;
        let ptr = unsafe { tail[0].as_ptr().add(written) };
        tail[0] = std::io::IoSlice::new(unsafe { std::slice::from_raw_parts(ptr, rest) });
    }
    tail
}

/// `FileFallocate(File file, off_t offset, off_t amount, uint32 wait_event_info)`
/// (fd.c).
pub fn FileFallocate(
    file: File,
    offset: i64,
    amount: i64,
    wait_event_info: u32,
) -> PgResult<i32> {
    let file = file.0;
    #[cfg(target_os = "linux")]
    {
        let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
        if return_code < 0 {
            return Ok(-1);
        }
        let raw_fd = vfd_raw_fd(file);

        loop {
            waitevent::pgstat_report_wait_start::call(wait_event_info);
            let return_code =
                unsafe { libc::posix_fallocate(raw_fd, offset as libc::off_t, amount as libc::off_t) };
            waitevent::pgstat_report_wait_end::call();

            if return_code == 0 {
                return Ok(0);
            } else if return_code == libc::EINTR {
                continue;
            }

            // For compatibility with %m printing etc.
            set_errno(return_code);

            // Return on a "real" failure; if fallocate is unsupported fall
            // through to the FileZero()-backed implementation.
            if return_code != libc::EINVAL && return_code != libc::EOPNOTSUPP {
                return Ok(-1);
            }
            break;
        }
    }

    // No posix_fallocate (e.g. macOS) or it reported unsupported: zero-fill.
    FileZero(File(file), offset, amount, wait_event_info)
}

// ---------------------------------------------------------------------------
// FileSize / FileTruncate (fd.c:2463-2507).
// ---------------------------------------------------------------------------

/// `FileSize(File file)` (fd.c).
pub fn FileSize(file: File) -> PgResult<i64> {
    let file = file.0;
    if !vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].is_open) {
        if vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))? < 0 {
            return Ok(-1);
        }
    }

    let raw_fd = vfd_raw_fd(file);
    Ok(unsafe { libc::lseek(raw_fd, 0, libc::SEEK_END) } as i64)
}

/// `FileTruncate(File file, off_t offset, uint32 wait_event_info)` (fd.c).
pub fn FileTruncate(file: File, offset: i64, wait_event_info: u32) -> PgResult<i32> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code);
    }
    let raw_fd = vfd_raw_fd(file);

    waitevent::pgstat_report_wait_start::call(wait_event_info);
    let return_code = pg_ftruncate(raw_fd, offset);
    waitevent::pgstat_report_wait_end::call();

    if return_code == 0 {
        vfd_core::with_fd(|fd| {
            let cur = fd.vfd_cache[file as usize].file_size;
            if cur > offset {
                // Adjust our state for truncation of a temp file.
                debug_assert!(fd.vfd_cache[file as usize].fdstate & FD_TEMP_FILE_LIMIT != 0);
                fd.temporary_files_size -= (cur - offset) as u64;
                fd.vfd_cache[file as usize].file_size = offset;
            }
        });
    }

    Ok(return_code)
}

// ---------------------------------------------------------------------------
// FilePathName / FileGetRawDesc / FileGetRawFlags / FileGetRawMode
// (fd.c:2515-2562).
// ---------------------------------------------------------------------------

/// `FilePathName(File file)` (fd.c) — the file name behind a VFD.
pub fn FilePathName(file: File) -> String {
    let file = file.0;
    vfd_core::with_fd(|fd| {
        fd.vfd_cache[file as usize]
            .file_name
            .clone()
            .expect("FilePathName on unused VFD")
    })
}

/// `FileGetRawDesc(File file)` (fd.c) — the underlying kernel fd.
pub fn FileGetRawDesc(file: File) -> PgResult<RawFd> {
    let file = file.0;
    let return_code = vfd_core::with_fd(|fd| vfd_core::FileAccess(fd, file))?;
    if return_code < 0 {
        return Ok(return_code);
    }
    Ok(vfd_raw_fd(file))
}

/// `FileGetRawFlags(File file)` (fd.c).
pub fn FileGetRawFlags(file: File) -> i32 {
    let file = file.0;
    vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].file_flags)
}

/// `FileGetRawMode(File file)` (fd.c).
pub fn FileGetRawMode(file: File) -> u32 {
    let file = file.0;
    vfd_core::with_fd(|fd| fd.vfd_cache[file as usize].file_mode)
}

// ---------------------------------------------------------------------------
// Seam adapters installed by `init_seams` — the VFD temp-file API consumed by
// `buffile.c`. `FileReadV`/`FileWriteV` are the vectored primitives; buffile
// reads/writes a single buffer, so these wrap the single-iovec case (which is
// exactly the C `FileRead`/`FileWrite` single-buffer convenience wrappers).
// ---------------------------------------------------------------------------

/// Seam adapter for `file_read` — single-buffer `FileRead(file, buf, offset, ...)`.
pub fn seam_file_read(
    file: File,
    buf: &mut [u8],
    offset: i64,
    wait_event_info: u32,
) -> PgResult<isize> {
    let mut iov = [std::io::IoSliceMut::new(buf)];
    FileReadV(file, &mut iov, offset, wait_event_info)
}

/// Seam adapter for `file_write` — single-buffer `FileWrite(file, buf, offset, ...)`.
pub fn seam_file_write(
    file: File,
    buf: &[u8],
    offset: i64,
    wait_event_info: u32,
) -> PgResult<isize> {
    let iov = [std::io::IoSlice::new(buf)];
    FileWriteV(file, &iov, offset, wait_event_info)
}

/// Seam adapter for `file_close` — `FileClose` is infallible at the ereport
/// level (its only failures are logged at LOG inside fd.c), so the seam's
/// `()` return discards the (LOG-level) `Err`.
pub fn seam_file_close(file: File) {
    let _ = FileClose(file);
}
