use std::io::{IoSlice, IoSliceMut};
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd};

use ::elog::ereport;
use ::types_core::BLCKSZ;
use ::types_error::{PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERROR, LOG};
use ::types_resowner::ResourceOwner;
use ::types_storage::File;

use crate::vfd::{
    self, cpath, get_errno, loc, set_errno, with_fd, FdState, RawOf, FD_DELETE_AT_CLOSE,
    FD_TEMP_FILE_LIMIT, PG_O_DIRECT,
};

// port/pg_iovec.h: Min(IOV_MAX, 128); IOV_MAX is 1024 on Linux and macOS.
pub const PG_IOV_MAX: usize = 128;

fn vfd_raw(fd: &FdState, file: i32) -> RawFd {
    fd.vfd_cache[file as usize].fd.as_ref().expect("I/O on closed VFD").as_raw()
}

fn pg_preadv(fd: RawFd, iov: &mut [IoSliceMut<'_>], offset: i64) -> isize {
    // SAFETY: IoSliceMut is ABI-compatible with iovec; fd is live.
    unsafe {
        libc::preadv(
            fd,
            iov.as_mut_ptr().cast::<libc::iovec>(),
            iov.len() as libc::c_int,
            offset as libc::off_t,
        )
    }
}

fn pg_pwritev(fd: RawFd, iov: &[IoSlice<'_>], offset: i64) -> isize {
    // SAFETY: IoSlice is ABI-compatible with iovec; fd is live.
    unsafe {
        libc::pwritev(
            fd,
            iov.as_ptr().cast::<libc::iovec>(),
            iov.len() as libc::c_int,
            offset as libc::off_t,
        )
    }
}

pub fn PathNameOpenFile(file_name: &str, file_flags: i32) -> PgResult<File> {
    PathNameOpenFilePerm(file_name, file_flags, vfd::pg_file_create_mode())
}

// C contract: File(-1) with errno set on open failure, no ereport.
pub fn PathNameOpenFilePerm(file_name: &str, file_flags: i32, file_mode: u32) -> PgResult<File> {
    let fnamecopy = file_name.to_owned();
    let file_flags = file_flags | libc::O_CLOEXEC;

    with_fd(|fd| {
        let file = vfd::AllocateVfd(fd);
        vfd::ReleaseLruFiles(fd)?;

        let raw = vfd::BasicOpenFilePermInternal(fd, file_name, file_flags, file_mode)?;
        if raw < 0 {
            let save_errno = get_errno();
            vfd::FreeVfd(fd, file);
            set_errno(save_errno);
            return Ok(File(-1));
        }
        fd.nfile += 1;

        let vfd_p = &mut fd.vfd_cache[file as usize];
        // SAFETY: `raw` is a freshly opened descriptor now owned by the VFD.
        vfd_p.fd = Some(unsafe { OwnedFd::from_raw_fd(raw) });
        vfd_p.file_name = Some(fnamecopy);
        vfd_p.file_flags = file_flags & !(libc::O_CREAT | libc::O_TRUNC | libc::O_EXCL);
        vfd_p.file_mode = file_mode;
        vfd_p.file_size = 0;
        vfd_p.fdstate = 0x0;
        vfd_p.resowner = ResourceOwner::NULL;

        vfd::Insert(fd, file);
        Ok(File(file))
    })
}

pub fn FileClose(file: File) -> PgResult<()> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let close_failure = with_fd(|fd| {
        if !vfd::FileIsNotOpen(fd, file) {
            let handle = fd.vfd_cache[file as usize].fd.take().unwrap();
            crate::pgaio_closing_fd_if_engine_present(handle.as_raw());

            let raw = handle.into_raw_fd();
            // SAFETY: live descriptor released from its guard; closed once here.
            let failed = unsafe { libc::close(raw) } != 0;
            let en = get_errno();

            fd.nfile -= 1;
            vfd::Delete(fd, file);
            if failed {
                return Some((en, fd.vfd_cache[file as usize].fdstate));
            }
        }
        None
    });
    if let Some((en, fdstate)) = close_failure {
        let elevel = if fdstate & FD_TEMP_FILE_LIMIT != 0 {
            LOG
        } else {
            crate::vfd::data_sync_elevel(LOG)
        };
        let name = with_fd(|fd| fd.vfd_cache[file as usize].file_name.clone().unwrap_or_default());
        ereport(elevel)
            .with_saved_errno(en)
            .errmsg_internal(format!("could not close file \"{name}\": %m"))
            .finish(loc("FileClose"))?;
    }

    let (fdstate, file_name) = with_fd(|fd| {
        let vfd_p = &mut fd.vfd_cache[file as usize];
        let fdstate = vfd_p.fdstate;
        if fdstate & FD_TEMP_FILE_LIMIT != 0 {
            // Subtract its size from current usage (do first in case of error).
            let sz = vfd_p.file_size as u64;
            vfd_p.file_size = 0;
            fd.temporary_files_size = fd.temporary_files_size.wrapping_sub(sz);
        }
        if fdstate & FD_DELETE_AT_CLOSE != 0 {
            // Reset first so an abort-path re-entry can't loop; worst case is a
            // missing log line, never a skipped unlink.
            fd.vfd_cache[file as usize].fdstate &= !FD_DELETE_AT_CLOSE;
        }
        (fdstate, fd.vfd_cache[file as usize].file_name.clone())
    });

    if fdstate & FD_DELETE_AT_CLOSE != 0 {
        let name = file_name.expect("FD_DELETE_AT_CLOSE on unnamed VFD");
        let path = cpath(&name);

        // SAFETY: NUL-terminated path; statbuf is a valid out-param.
        let mut statbuf: libc::stat = unsafe { std::mem::zeroed() };
        let stat_errno = if unsafe { libc::stat(path.as_ptr(), &mut statbuf) } != 0 {
            get_errno()
        } else {
            0
        };

        // SAFETY: NUL-terminated path.
        if unsafe { libc::unlink(path.as_ptr()) } != 0 {
            ereport(LOG)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not delete file \"{name}\": %m"))
                .finish(loc("FileClose"))?;
        }

        if stat_errno == 0 {
            crate::temp::ReportTemporaryFileUsage(&name, statbuf.st_size as u64)?;
        } else {
            ereport(LOG)
                .with_saved_errno(stat_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{name}\": %m"))
                .finish(loc("FileClose"))?;
        }
    }

    let owner = with_fd(|fd| fd.vfd_cache[file as usize].resowner);
    if !owner.is_null() {
        vfd::resowner::resource_owner_forget_file(owner, File(file));
    }

    with_fd(|fd| vfd::FreeVfd(fd, file));
    Ok(())
}

pub fn FilePrefetch(file: File, offset: i64, amount: i64, wait_event_info: u32) -> PgResult<i32> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(rc);
    }

    #[cfg(target_os = "linux")]
    {
        loop {
            waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
            // SAFETY: `raw` is the open VFD's descriptor.
            let rc = unsafe {
                libc::posix_fadvise(raw, offset, amount, libc::POSIX_FADV_WILLNEED)
            };
            waitevent_seams::pgstat_report_wait_end::call();
            if rc == libc::EINTR {
                continue;
            }
            return Ok(rc);
        }
    }
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
        waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
        // SAFETY: `raw` is the open VFD's descriptor; F_RDADVISE reads `ra`.
        let rc = unsafe { libc::fcntl(raw, libc::F_RDADVISE, &ra) };
        waitevent_seams::pgstat_report_wait_end::call();
        if rc != -1 {
            Ok(0)
        } else {
            Ok(get_errno())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (raw, offset, amount, wait_event_info);
        Ok(0)
    }
}

pub fn FileStartBufferRead(file: File, offset: i64, buffer: i32) -> PgResult<bool> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));
    if !aio_seams::uring_buf_read::is_installed() {
        return Ok(false);
    }
    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(false);
    }
    Ok(aio_seams::uring_buf_read::call(raw, offset, buffer))
}

pub fn FileWriteback(file: File, offset: i64, nbytes: i64, wait_event_info: u32) -> PgResult<()> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    if nbytes <= 0 {
        return Ok(());
    }
    if with_fd(|fd| fd.vfd_cache[file as usize].file_flags) & PG_O_DIRECT != 0 {
        return Ok(());
    }

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(());
    }

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    let result = crate::sync::pg_flush_data(raw, offset, nbytes);
    waitevent_seams::pgstat_report_wait_end::call();
    result
}

pub fn FileReadV(
    file: File,
    iov: &mut [IoSliceMut<'_>],
    offset: i64,
    wait_event_info: u32,
) -> PgResult<isize> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(rc as isize);
    }

    loop {
        waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
        let return_code = pg_preadv(raw, iov, offset);
        waitevent_seams::pgstat_report_wait_end::call();

        if return_code < 0 && get_errno() == libc::EINTR {
            continue;
        }
        return Ok(return_code);
    }
}

pub fn FileRead(file: File, buf: &mut [u8], offset: i64, wait_event_info: u32) -> PgResult<isize> {
    let mut iov = [IoSliceMut::new(buf)];
    FileReadV(file, &mut iov, offset, wait_event_info)
}

pub fn FileStartReadV(file: File, iovcnt: i32, offset: i64, _wait_event_info: u32) -> PgResult<i32> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(rc);
    }

    aio_seams::pgaio_io_start_readv::call(raw, iovcnt, offset);
    Ok(0)
}

pub fn FileWriteV(
    file: File,
    iov: &[IoSlice<'_>],
    offset: i64,
    wait_event_info: u32,
) -> PgResult<isize> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw, fdstate, file_size, temp_total) =
        with_fd(|fd| -> PgResult<(i32, RawFd, u16, i64, u64)> {
            let rc = vfd::FileAccess(fd, file)?;
            if rc < 0 {
                return Ok((rc, -1, 0, 0, 0));
            }
            let vfd_p = &fd.vfd_cache[file as usize];
            Ok((rc, vfd_raw(fd, file), vfd_p.fdstate, vfd_p.file_size, fd.temporary_files_size))
        })?;
    if rc < 0 {
        return Ok(rc as isize);
    }

    let temp_file_limit = guc_tables::vars::temp_file_limit.read();
    if temp_file_limit >= 0 && (fdstate & FD_TEMP_FILE_LIMIT != 0) {
        let mut past_write = offset;
        for s in iov {
            past_write += s.len() as i64;
        }
        if past_write > file_size {
            let new_total = temp_total + (past_write - file_size) as u64;
            if new_total > (temp_file_limit as u64) * 1024 {
                ereport(ERROR)
                    .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                    .errmsg(format!(
                        "temporary file size exceeds \"temp_file_limit\" ({temp_file_limit}kB)"
                    ))
                    .finish(loc("FileWriteV"))?;
            }
        }
    }

    loop {
        waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
        let return_code = pg_pwritev(raw, iov, offset);
        waitevent_seams::pgstat_report_wait_end::call();

        if return_code >= 0 {
            // Short writes traditionally imply disk-space shortage; set ENOSPC
            // for every successful write so short-write callers can report %m.
            set_errno(libc::ENOSPC);

            if fdstate & FD_TEMP_FILE_LIMIT != 0 {
                let past_write = offset + return_code as i64;
                with_fd(|fd| {
                    let vfd_p = &mut fd.vfd_cache[file as usize];
                    if past_write > vfd_p.file_size {
                        fd.temporary_files_size += (past_write - vfd_p.file_size) as u64;
                        vfd_p.file_size = past_write;
                    }
                });
            }
            return Ok(return_code);
        }
        if get_errno() == libc::EINTR {
            continue;
        }
        return Ok(return_code);
    }
}

pub fn FileWrite(file: File, buf: &[u8], offset: i64, wait_event_info: u32) -> PgResult<isize> {
    let iov = [IoSlice::new(buf)];
    FileWriteV(file, &iov, offset, wait_event_info)
}

pub fn FileSync(file: File, wait_event_info: u32) -> PgResult<i32> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(rc);
    }

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    let return_code = crate::sync::pg_fsync(raw);
    waitevent_seams::pgstat_report_wait_end::call();
    Ok(return_code)
}

pub fn FileZero(file: File, offset: i64, amount: i64, wait_event_info: u32) -> PgResult<i32> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(rc);
    }

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    let written = pg_pwrite_zeros(raw, amount as usize, offset);
    waitevent_seams::pgstat_report_wait_end::call();

    if written < 0 {
        Ok(-1)
    } else if written != amount as isize {
        if get_errno() == 0 {
            set_errno(libc::ENOSPC);
        }
        Ok(-1)
    } else {
        Ok(0)
    }
}

// common/file_utils.c pg_pwrite_zeros: fd.c's FileZero is its only backend
// caller so far, so it lives here until the common unit lands.
pub fn pg_pwrite_zeros(fd: RawFd, size: usize, mut offset: i64) -> isize {
    let zbuffer = [0u8; BLCKSZ];
    let mut remaining = size;
    let mut total_written: isize = 0;
    let mut iov: Vec<IoSlice<'_>> = Vec::with_capacity(PG_IOV_MAX);

    while remaining > 0 {
        iov.clear();
        while iov.len() < PG_IOV_MAX && remaining > 0 {
            let this_len = remaining.min(BLCKSZ);
            iov.push(IoSlice::new(&zbuffer[..this_len]));
            remaining -= this_len;
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

fn pg_pwritev_with_retry(fd: RawFd, iov: &[IoSlice<'_>], mut offset: i64) -> isize {
    if iov.len() > PG_IOV_MAX {
        set_errno(libc::EINVAL);
        return -1;
    }

    let mut iov_copy: Vec<IoSlice<'_>> = iov.to_vec();
    let mut cur: &mut [IoSlice<'_>] = &mut iov_copy;
    let mut sum: isize = 0;

    loop {
        let part = pg_pwritev(fd, cur, offset);
        if part < 0 {
            return -1;
        }

        sum += part;
        offset += part as i64;

        cur = compute_remaining_iovec(cur, part as usize);
        if cur.is_empty() {
            return sum;
        }
    }
}

fn compute_remaining_iovec<'a, 'b>(
    iov: &'a mut [IoSlice<'b>],
    mut written: usize,
) -> &'a mut [IoSlice<'b>] {
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
        let rest = tail[0].len() - written;
        // SAFETY: shrinking forward within the same backing buffer, which
        // outlives this call.
        let ptr = unsafe { tail[0].as_ptr().add(written) };
        tail[0] = IoSlice::new(unsafe { std::slice::from_raw_parts(ptr, rest) });
    }
    tail
}

pub fn FileFallocate(file: File, offset: i64, amount: i64, wait_event_info: u32) -> PgResult<i32> {
    #[cfg(target_os = "linux")]
    {
        let filev = file.0;
        debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, filev)));

        let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
            let rc = vfd::FileAccess(fd, filev)?;
            Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, filev) }))
        })?;
        if rc < 0 {
            return Ok(-1);
        }

        loop {
            waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
            // SAFETY: `raw` is the open VFD's descriptor.
            let rc = unsafe {
                libc::posix_fallocate(raw, offset as libc::off_t, amount as libc::off_t)
            };
            waitevent_seams::pgstat_report_wait_end::call();

            if rc == 0 {
                return Ok(0);
            }
            if rc == libc::EINTR {
                continue;
            }
            // For compatibility with %m printing etc.
            set_errno(rc);
            if rc != libc::EINVAL && rc != libc::EOPNOTSUPP {
                return Ok(-1);
            }
            break;
        }
    }

    // No posix_fallocate, or it reported unsupported: zero-fill instead.
    FileZero(file, offset, amount, wait_event_info)
}

pub fn FileSize(file: File) -> PgResult<i64> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let raw = with_fd(|fd| -> PgResult<RawFd> {
        if vfd::FileIsNotOpen(fd, file) && vfd::FileAccess(fd, file)? < 0 {
            return Ok(-1);
        }
        Ok(vfd_raw(fd, file))
    })?;
    if raw < 0 {
        return Ok(-1);
    }

    // SAFETY: `raw` is the open VFD's descriptor.
    Ok(unsafe { libc::lseek(raw, 0, libc::SEEK_END) } as i64)
}

pub fn FileTruncate(file: File, offset: i64, wait_event_info: u32) -> PgResult<i32> {
    let file = file.0;
    debug_assert!(with_fd(|fd| vfd::FileIsValid(fd, file)));

    let (rc, raw) = with_fd(|fd| -> PgResult<(i32, RawFd)> {
        let rc = vfd::FileAccess(fd, file)?;
        Ok((rc, if rc < 0 { -1 } else { vfd_raw(fd, file) }))
    })?;
    if rc < 0 {
        return Ok(rc);
    }

    waitevent_seams::pgstat_report_wait_start::call(wait_event_info);
    let return_code = crate::sync::pg_ftruncate(raw, offset);
    waitevent_seams::pgstat_report_wait_end::call();

    if return_code == 0 {
        with_fd(|fd| {
            let vfd_p = &mut fd.vfd_cache[file as usize];
            if vfd_p.file_size > offset {
                debug_assert!(vfd_p.fdstate & FD_TEMP_FILE_LIMIT != 0);
                fd.temporary_files_size -= (vfd_p.file_size - offset) as u64;
                vfd_p.file_size = offset;
            }
        });
    }

    Ok(return_code)
}

pub fn FilePathName(file: File) -> String {
    with_fd(|fd| {
        debug_assert!(vfd::FileIsValid(fd, file.0));
        fd.vfd_cache[file.0 as usize].file_name.clone().expect("FilePathName on unused VFD")
    })
}

pub fn FileGetRawDesc(file: File) -> PgResult<i32> {
    let file = file.0;
    with_fd(|fd| {
        let rc = vfd::FileAccess(fd, file)?;
        if rc < 0 {
            return Ok(rc);
        }
        debug_assert!(vfd::FileIsValid(fd, file));
        Ok(vfd_raw(fd, file))
    })
}

pub fn FileGetRawFlags(file: File) -> i32 {
    with_fd(|fd| {
        debug_assert!(vfd::FileIsValid(fd, file.0));
        fd.vfd_cache[file.0 as usize].file_flags
    })
}

pub fn FileGetRawMode(file: File) -> u32 {
    with_fd(|fd| {
        debug_assert!(vfd::FileIsValid(fd, file.0));
        fd.vfd_cache[file.0 as usize].file_mode
    })
}

// fd.c's NOT_USED FileInvalidate, kept for the sinval hook it documents.
pub fn FileInvalidate(file: File) -> PgResult<()> {
    with_fd(|fd| {
        debug_assert!(vfd::FileIsValid(fd, file.0));
        if !vfd::FileIsNotOpen(fd, file.0) {
            vfd::LruDelete(fd, file.0)?;
        }
        Ok(())
    })
}
