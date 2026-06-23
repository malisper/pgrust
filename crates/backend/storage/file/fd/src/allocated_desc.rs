//! `fd-allocated-desc` — the `allocatedDescs` table and the stdio/dir/pipe/
//! transient-fd handle families.
//!
//! `AllocateFile`/`FreeFile`, `OpenTransientFile[Perm]`/`CloseTransientFile`,
//! `OpenPipeStream`/`ClosePipeStream`, `AllocateDir`/`ReadDir`/`ReadDirExtended`/
//! `FreeDir`, and `closeAllVfds`. Owns the `with_allocated_dir`,
//! `open_transient_file` and `close_transient_file` seam adapters
//! (installed by `init_seams`).

use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;

use ::types_error::{
    ErrorLocation, ErrorLevel, PgError, PgResult, ERROR, LOG, ERRCODE_INSUFFICIENT_RESOURCES,
    ERRCODE_OUT_OF_MEMORY,
};
use ::types_storage::{Dir, DirEnt, FD_MINFREE};

use crate::vfd_core::{
    self, with_fd, AllocateDesc, AllocatedHandle, DirHandle, PipeHandle,
};

const SRCFILE: &str = "../src/backend/storage/file/fd.c";

/// `reserveAllocatedDesc(void)` (fd.c:2569) — ensure room in `allocatedDescs`,
/// growing it (and `maxAllocatedDescs`) as needed. Returns whether room exists.
///
/// The idiomatic table is a `Vec<AllocateDesc>`: `numAllocatedDescs` is its
/// `len()`, `maxAllocatedDescs` its `capacity()`. C grows the array with
/// `malloc`/`realloc`; here `Vec::reserve` is the equivalent, and "out of
/// memory" cannot be observed at the API level, so the fatal/non-fatal OOM
/// branches collapse — the cap-policy decisions (`FD_MINFREE / 3` initial,
/// `max_safe_fds / 3` ceiling) are reproduced exactly.
pub(crate) fn reserveAllocatedDesc() -> PgResult<bool> {
    with_fd(|fd| {
        let num = fd.allocated_descs.len() as i32;
        let max = fd.allocated_descs.capacity() as i32;

        // Quick out if array already has a free slot.
        if num < max {
            return Ok(true);
        }

        // If the array hasn't yet been created in the current process,
        // initialize it with FD_MINFREE / 3 elements. We don't look at
        // max_safe_fds immediately because set_max_safe_fds() may not have run
        // yet.
        if fd.allocated_descs.capacity() == 0 {
            let new_max = (FD_MINFREE / 3) as usize;
            fd.allocated_descs.reserve_exact(new_max);
            return Ok(true);
        }

        // Consider enlarging the array beyond the initial allocation. By the
        // time this happens, max_safe_fds should be known accurately.
        //
        // Cap allocated descriptors at max_safe_fds / 3 so they can't hog all
        // the available FDs.
        let new_max = vfd_core::max_safe_fds() / 3;
        if new_max > max {
            fd.allocated_descs
                .reserve_exact((new_max - num) as usize);
            return Ok(true);
        }

        // Can't enlarge allocatedDescs[] any more.
        Ok(false)
    })
}

/// `GetCurrentSubTransactionId()` — routed through xact's owner seam (unported).
fn get_current_sub_transaction_id() -> types_core::SubTransactionId {
    transam_xact_seams::get_current_sub_transaction_id::call()
}

/// `pgaio_closing_fd(fd)` — routed through aio-core's owner seam (unported).
fn pgaio_closing_fd(fd: i32) {
    aio_core_seams::pgaio_closing_fd::call(fd);
}

/// `FreeDesc(AllocateDesc *desc)` (fd.c:2803) — close one allocated descriptor
/// and compact the table.
///
/// `desc` is identified by its index. The "close the underlying object" switch
/// maps onto dropping the owned handle (`fclose`/`pclose`/`closedir`/`close`),
/// after notifying AIO for the raw-fd case. C compacts by moving the last
/// element over the freed slot (`*desc = allocatedDescs[--numAllocatedDescs]`);
/// `Vec::swap_remove` is exactly that.
pub(crate) fn FreeDesc(index: i32) -> PgResult<i32> {
    with_fd(|fd| {
        // Remove the entry first (swap_remove == C's compaction), then close
        // the underlying object as it drops.
        let desc = fd.allocated_descs.swap_remove(index as usize);
        let result = match desc.desc {
            AllocatedHandle::File(file) => {
                // fclose
                drop(file);
                0
            }
            AllocatedHandle::Pipe(pipe) => {
                // pclose: wait for the child and return its status.
                pclose(pipe)
            }
            AllocatedHandle::Dir(dir) => {
                // closedir
                drop(dir);
                0
            }
            AllocatedHandle::RawFd(file) => {
                // pgaio_closing_fd(desc->desc.fd); close(desc->desc.fd)
                pgaio_closing_fd(file.as_raw_fd());
                // Dropping the owned StdFile closes the kernel fd; close()'s
                // return value is the file's drop, which we treat as 0.
                drop(file);
                0
            }
        };
        Ok(result)
    })
}

/// `AllocateFile(const char *name, const char *mode)` (fd.c:2644) — `fopen` a
/// tracked stdio stream; returns its index in the allocated-descriptor table.
///
/// The idiomatic API returns the table index in place of the C `FILE *`. The
/// TryAgain/EMFILE-ENFILE retry loop, the `reserveAllocatedDesc` gate and the
/// `ReleaseLruFiles` excess-fd close are reproduced exactly.
pub fn AllocateFile(name: impl AsRef<Path>, mode: &str) -> PgResult<i32> {
    let name = name.as_ref();

    // Can we allocate another non-virtual FD?
    if !reserveAllocatedDesc()? {
        let max = with_fd(|fd| fd.allocated_descs.capacity() as i32);
        return Err(ereport_error(
            ERRCODE_INSUFFICIENT_RESOURCES,
            format!(
                "exceeded maxAllocatedDescs ({}) while trying to open file \"{}\"",
                max,
                name.display()
            ),
            2655,
            "AllocateFile",
        ));
    }

    // Close excess kernel FDs.
    vfd_core::with_fd(vfd_core::ReleaseLruFiles)?;

    loop {
        match open_stdio(name, mode) {
            Ok(file) => {
                let create_subid = get_current_sub_transaction_id();
                return with_fd(|fd| {
                    fd.allocated_descs.push(AllocateDesc {
                        create_subid,
                        desc: AllocatedHandle::File(file),
                    });
                    Ok((fd.allocated_descs.len() - 1) as i32)
                });
            }
            Err(errno) => {
                if errno == libc::EMFILE || errno == libc::ENFILE {
                    let save_errno = errno;
                    ereport_log_out_of_fds()?;
                    if vfd_core::with_fd(vfd_core::ReleaseLruFile) {
                        continue;
                    }
                    let _ = save_errno;
                }
                // C returns NULL with errno set; the idiomatic API surfaces the
                // failure as an Err carrying the saved errno so the caller can
                // mirror the C "AllocateDir failed, errno examined later" path.
                return Err(file_access_error(
                    errno,
                    format!("could not open file \"{}\": %m", name.display()),
                    2649,
                    "AllocateFile",
                    ERROR,
                ));
            }
        }
    }
}

/// `FreeFile(FILE *file)` (fd.c:2843) — `fclose` a stream opened with
/// `AllocateFile`.
pub fn FreeFile(index_to_free: i32) -> PgResult<()> {
    // Remove file from list of allocated files, if it's present. C scans from
    // the top down and matches the FILE*; here the caller passes the table
    // index, so we validate the slot's kind and free it.
    let found = with_fd(|fd| {
        let i = index_to_free as usize;
        i < fd.allocated_descs.len() && matches!(fd.allocated_descs[i].desc, AllocatedHandle::File(_))
    });
    if found {
        FreeDesc(index_to_free)?;
        return Ok(());
    }

    // Only get here if someone passes us a file not in allocatedDescs.
    ereport_warning(
        "file passed to FreeFile was not obtained from AllocateFile",
        2861,
        "FreeFile",
    )?;
    Ok(())
}

/// `OpenTransientFile(const char *fileName, int fileFlags)` (fd.c:2694).
pub fn OpenTransientFile(file_name: impl AsRef<Path>, file_flags: i32) -> PgResult<i32> {
    OpenTransientFilePerm(file_name, file_flags, vfd_core::pg_file_create_mode())
}

/// `OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode)`
/// (fd.c:2703) — open a tracked raw kernel fd for transaction-end cleanup.
///
/// Returns the table index on success, mirroring the C `int fd` return via the
/// idiomatic handle. On open failure C returns `-1`; here `BasicOpenFilePerm`
/// surfaces the failure as `Err`, which we propagate.
pub fn OpenTransientFilePerm(
    file_name: impl AsRef<Path>,
    file_flags: i32,
    file_mode: u32,
) -> PgResult<i32> {
    let file_name = file_name.as_ref();

    // Can we allocate another non-virtual FD?
    if !reserveAllocatedDesc()? {
        let max = with_fd(|fd| fd.allocated_descs.capacity() as i32);
        return Err(ereport_error(
            ERRCODE_INSUFFICIENT_RESOURCES,
            format!(
                "exceeded maxAllocatedDescs ({}) while trying to open file \"{}\"",
                max,
                file_name.display()
            ),
            2714,
            "OpenTransientFilePerm",
        ));
    }

    // Close excess kernel FDs.
    vfd_core::with_fd(vfd_core::ReleaseLruFiles)?;

    // C's BasicOpenFilePerm returns -1 with errno set; OpenTransientFilePerm
    // then returns that -1 to the caller (durable_rename etc.), which inspects
    // errno to tolerate cases like ENOENT. Mirror that by surfacing the saved
    // errno on the returned error rather than ereporting here.
    let file = match vfd_core::BasicOpenFilePermOrErrno(file_name, file_flags, file_mode)? {
        Ok(file) => file,
        Err(saved) => {
            return Err(file_access_error(
                saved,
                "could not open file: %m".to_string(),
                2720,
                "OpenTransientFilePerm",
                ERROR,
            ));
        }
    };
    let raw_fd = file.as_raw_fd();
    let create_subid = get_current_sub_transaction_id();
    with_fd(|fd| {
        fd.allocated_descs.push(AllocateDesc {
            create_subid,
            desc: AllocatedHandle::RawFd(file),
        });
    });
    // C returns the kernel fd; the idiomatic transient API keys later
    // CloseTransientFile by that fd value, so return it.
    Ok(raw_fd)
}

/// `OpenPipeStream(const char *command, const char *mode)` (fd.c:2747) —
/// `popen` a tracked pipe stream.
pub fn OpenPipeStream(command: &str, mode: &str) -> PgResult<i32> {
    // Can we allocate another non-virtual FD?
    if !reserveAllocatedDesc()? {
        let max = with_fd(|fd| fd.allocated_descs.capacity() as i32);
        return Err(ereport_error(
            ERRCODE_INSUFFICIENT_RESOURCES,
            format!(
                "exceeded maxAllocatedDescs ({}) while trying to execute command \"{}\"",
                max, command
            ),
            2759,
            "OpenPipeStream",
        ));
    }

    // Close excess kernel FDs.
    vfd_core::with_fd(vfd_core::ReleaseLruFiles)?;

    // C flushes stdio, sets SIGPIPE to SIG_DFL across the popen, then restores
    // SIG_IGN. std::process::Command spawns a child whose signal disposition is
    // the default (SIG_DFL) regardless of the parent's mask, so the popen'd
    // program already runs with default SIGPIPE handling — no signal dance is
    // needed at this layer.
    loop {
        match popen(command, mode) {
            Ok(pipe) => {
                let create_subid = get_current_sub_transaction_id();
                return with_fd(|fd| {
                    fd.allocated_descs.push(AllocateDesc {
                        create_subid,
                        desc: AllocatedHandle::Pipe(pipe),
                    });
                    Ok((fd.allocated_descs.len() - 1) as i32)
                });
            }
            Err(errno) => {
                if errno == libc::EMFILE || errno == libc::ENFILE {
                    ereport_log_out_of_fds()?;
                    if vfd_core::with_fd(vfd_core::ReleaseLruFile) {
                        continue;
                    }
                }
                return Err(file_access_error(
                    errno,
                    format!("could not execute command \"{}\": %m", command),
                    2747,
                    "OpenPipeStream",
                    ERROR,
                ));
            }
        }
    }
}

/// `ClosePipeStream(FILE *file)` (fd.c:3055) — `pclose` a pipe; returns wait
/// status.
pub fn ClosePipeStream(index: i32) -> PgResult<i32> {
    // Remove file from list of allocated files, if it's present.
    let found = with_fd(|fd| {
        let i = index as usize;
        i < fd.allocated_descs.len() && matches!(fd.allocated_descs[i].desc, AllocatedHandle::Pipe(_))
    });
    if found {
        return FreeDesc(index);
    }

    // Only get here if someone passes us a file not in allocatedDescs.
    ereport_warning(
        "file passed to ClosePipeStream was not obtained from OpenPipeStream",
        3071,
        "ClosePipeStream",
    )?;
    Ok(-1)
}

/// `OpenPipeStream(const char *command, const char *mode)` (fd.c:2747),
/// faithful to C's contract that a `popen` failure returns `NULL` with `errno`
/// set rather than `ereport`ing. The `reserveAllocatedDesc` exhaustion still
/// `ereport(ERROR)`s; the `EMFILE`/`ENFILE` retry reports at `LOG`. Used by
/// `run_ssl_passphrase_command`, which reports a `NULL` at its own loglevel.
pub fn OpenPipeStreamOrNull(command: &str, mode: &str) -> PgResult<Option<i32>> {
    // Can we allocate another non-virtual FD?
    if !reserveAllocatedDesc()? {
        let max = with_fd(|fd| fd.allocated_descs.capacity() as i32);
        return Err(ereport_error(
            ERRCODE_INSUFFICIENT_RESOURCES,
            format!(
                "exceeded maxAllocatedDescs ({}) while trying to execute command \"{}\"",
                max, command
            ),
            2759,
            "OpenPipeStream",
        ));
    }

    // Close excess kernel FDs.
    vfd_core::with_fd(vfd_core::ReleaseLruFiles)?;

    loop {
        match popen(command, mode) {
            Ok(pipe) => {
                let create_subid = get_current_sub_transaction_id();
                return with_fd(|fd| {
                    fd.allocated_descs.push(AllocateDesc {
                        create_subid,
                        desc: AllocatedHandle::Pipe(pipe),
                    });
                    Ok(Some((fd.allocated_descs.len() - 1) as i32))
                });
            }
            Err(errno) => {
                if errno == libc::EMFILE || errno == libc::ENFILE {
                    ereport_log_out_of_fds()?;
                    if vfd_core::with_fd(vfd_core::ReleaseLruFile) {
                        continue;
                    }
                }
                // C: `return NULL;` with `errno` left set.
                vfd_core::set_errno_pub(errno);
                return Ok(None);
            }
        }
    }
}

/// `fgets(buf, size, fh)` + `ferror(fh)` against the pipe stream at table
/// `index` (the read side of `OpenPipeStream(command, "r")`). Reads at most
/// `size - 1` bytes up to and including the first newline. Returns the bytes
/// read, EOF, or the read error's errno.
pub(crate) fn pipe_read_line(index: i32, size: i32) -> PipeReadLineOutcome {
    use std::io::Read;
    if size <= 0 {
        return PipeReadLineOutcome::Eof;
    }
    let max = (size - 1) as usize; // fgets reads at most size-1 bytes + NUL.
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return PipeReadLineOutcome::Error(libc::EBADF);
        }
        let stdout = match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::Pipe(pipe) => match pipe.stdout.as_mut() {
                Some(s) => s,
                None => return PipeReadLineOutcome::Error(libc::EBADF),
            },
            _ => return PipeReadLineOutcome::Error(libc::EBADF),
        };
        // fgets: read byte-by-byte, stopping at a newline (kept) or `size-1`.
        let mut out: Vec<u8> = Vec::new();
        let mut byte = [0u8; 1];
        while out.len() < max {
            match stdout.read(&mut byte) {
                Ok(0) => break, // EOF
                Ok(_) => {
                    out.push(byte[0]);
                    if byte[0] == b'\n' {
                        break;
                    }
                }
                Err(e) => return PipeReadLineOutcome::Error(e.raw_os_error().unwrap_or(libc::EIO)),
            }
        }
        if out.is_empty() {
            // fgets returned NULL; ferror is false here (a hard error returned
            // above), so this is EOF.
            PipeReadLineOutcome::Eof
        } else {
            PipeReadLineOutcome::Line(out)
        }
    })
}

/// In-crate mirror of the [`crate::seams`] pipe-read outcome (kept private to
/// avoid an `allocated_desc` -> seam-crate type dependency).
pub(crate) enum PipeReadLineOutcome {
    Line(Vec<u8>),
    Eof,
    Error(i32),
}

/// `bytesread = fread(databuf, 1, maxread, copy_file)` against a pipe stream at
/// table `index` (the read side of `OpenPipeStream(command, "r")`, COPY FROM
/// PROGRAM). Reads up to `maxread` bytes from the child's stdout; mirrors
/// stdio's `fread`, which returns a short count only at EOF. Returns the bytes
/// read (an empty `Vec` signals EOF) or the read error's errno.
pub(crate) fn pipe_read_chunk(index: i32, maxread: usize) -> Result<Vec<u8>, i32> {
    use std::io::Read;
    if maxread == 0 {
        return Ok(Vec::new());
    }
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return Err(libc::EBADF);
        }
        let stdout = match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::Pipe(pipe) => match pipe.stdout.as_mut() {
                Some(s) => s,
                None => return Err(libc::EBADF),
            },
            _ => return Err(libc::EBADF),
        };
        // fread fills up to `maxread` bytes; a pipe `read(2)` can return fewer
        // than requested before EOF, which fread papers over only at EOF.
        // Loop until the buffer is full or the stream signals EOF, so a single
        // call delivers a full chunk (matching fread's count semantics).
        let mut out = vec![0u8; maxread];
        let mut filled = 0usize;
        while filled < maxread {
            match stdout.read(&mut out[filled..]) {
                Ok(0) => break, // EOF
                Ok(k) => filled += k,
                Err(e) => return Err(e.raw_os_error().unwrap_or(libc::EIO)),
            }
        }
        out.truncate(filled);
        Ok(out)
    })
}

/// `AllocateDir(const char *dirname)` (fd.c:2907) — `opendir` a tracked
/// directory. `Ok(None)` mirrors C returning NULL (caller checks errno via the
/// following `ReadDir`).
pub fn AllocateDir(dirname: impl AsRef<Path>) -> PgResult<Option<Dir>> {
    let dirname = dirname.as_ref();

    // Can we allocate another non-virtual FD?
    if !reserveAllocatedDesc()? {
        let max = with_fd(|fd| fd.allocated_descs.capacity() as i32);
        return Err(ereport_error(
            ERRCODE_INSUFFICIENT_RESOURCES,
            format!(
                "exceeded maxAllocatedDescs ({}) while trying to open directory \"{}\"",
                max,
                dirname.display()
            ),
            2918,
            "AllocateDir",
        ));
    }

    // Close excess kernel FDs.
    vfd_core::with_fd(vfd_core::ReleaseLruFiles)?;

    loop {
        match std::fs::read_dir(dirname) {
            Ok(iter) => {
                let create_subid = get_current_sub_transaction_id();
                return with_fd(|fd| {
                    fd.allocated_descs.push(AllocateDesc {
                        create_subid,
                        desc: AllocatedHandle::Dir(DirHandle { iter: Some(iter) }),
                    });
                    Ok(Some((fd.allocated_descs.len() - 1) as i32))
                });
            }
            Err(e) => {
                let errno = e.raw_os_error().unwrap_or(0);
                if errno == libc::EMFILE || errno == libc::ENFILE {
                    ereport_log_out_of_fds()?;
                    if vfd_core::with_fd(vfd_core::ReleaseLruFile) {
                        continue;
                    }
                }
                // C returns NULL with errno set; failure detection is left to
                // the following ReadDir/ReadDirExtended call, which reports it.
                return Ok(None);
            }
        }
    }
}

/// `ReadDir(DIR *dir, const char *dirname)` (fd.c:2973).
pub fn ReadDir(dir: Option<Dir>, dirname: impl AsRef<Path>) -> PgResult<Option<DirEnt>> {
    ReadDirExtended(dir, dirname, ERROR)
}

/// `ReadDirExtended(DIR *dir, const char *dirname, int elevel)` (fd.c:2988).
///
/// A `None` `dir` mirrors C's NULL `DIR *` (a failed `AllocateDir`): report a
/// generic open failure at `elevel` and return `None`. Otherwise pull the next
/// entry from the directory iterator; a read error reports at `elevel`. With
/// `elevel < ERROR` the report returns and we hand back `None`, so the caller's
/// loop falls out as though the directory were exhausted.
pub fn ReadDirExtended(
    dir: Option<Dir>,
    dirname: impl AsRef<Path>,
    elevel: ErrorLevel,
) -> PgResult<Option<DirEnt>> {
    let dirname = dirname.as_ref();

    // Give a generic message for AllocateDir failure, if caller didn't.
    let index = match dir {
        None => {
            let errno = current_errno();
            ereport_at(file_access_error(
                errno,
                format!("could not open directory \"{}\": %m", dirname.display()),
                2998,
                "ReadDirExtended",
                elevel,
            ))?;
            return Ok(None);
        }
        Some(index) => index,
    };

    // errno = 0; if ((dent = readdir(dir)) != NULL) return dent;
    let next = with_fd(|fd| {
        let i = index as usize;
        match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::Dir(dir) => match dir.iter.as_mut() {
                Some(it) => it.next(),
                None => None,
            },
            _ => None,
        }
    });

    match next {
        Some(Ok(entry)) => {
            let d_name = entry.file_name().to_string_lossy().into_owned();
            Ok(Some(DirEnt { d_name }))
        }
        Some(Err(e)) => {
            let errno = e.raw_os_error().unwrap_or(0);
            ereport_at(file_access_error(
                errno,
                format!("could not read directory \"{}\": %m", dirname.display()),
                3009,
                "ReadDirExtended",
                elevel,
            ))?;
            Ok(None)
        }
        // readdir returned NULL with errno == 0 => end of directory.
        None => Ok(None),
    }
}

/// `FreeDir(DIR *dir)` (fd.c:3025).
pub fn FreeDir(dir: Option<Dir>) -> PgResult<()> {
    // Nothing to do if AllocateDir failed.
    let index = match dir {
        None => return Ok(()),
        Some(index) => index,
    };

    // Remove dir from list of allocated dirs, if it's present.
    let found = with_fd(|fd| {
        let i = index as usize;
        i < fd.allocated_descs.len() && matches!(fd.allocated_descs[i].desc, AllocatedHandle::Dir(_))
    });
    if found {
        FreeDesc(index)?;
        return Ok(());
    }

    // Only get here if someone passes us a dir not in allocatedDescs.
    ereport_warning(
        "dir passed to FreeDir was not obtained from AllocateDir",
        3047,
        "FreeDir",
    )?;
    Ok(())
}

/// `CloseTransientFile(int fd)` (fd.c:2871) — close an `OpenTransientFile`
/// handle.
pub fn CloseTransientFile(fd_to_close: i32) -> PgResult<()> {
    // Remove fd from list of allocated files, if it's present. C scans from the
    // top down matching the kernel fd value; do the same here.
    let index = with_fd(|fd| {
        for i in (0..fd.allocated_descs.len()).rev() {
            if let AllocatedHandle::RawFd(file) = &fd.allocated_descs[i].desc {
                if file.as_raw_fd() == fd_to_close {
                    return Some(i as i32);
                }
            }
        }
        None
    });
    if let Some(index) = index {
        FreeDesc(index)?;
        return Ok(());
    }

    // Only get here if someone passes us a file not in allocatedDescs.
    ereport_warning(
        "fd passed to CloseTransientFile was not obtained from OpenTransientFile",
        2887,
        "CloseTransientFile",
    )?;

    pgaio_closing_fd(fd_to_close);
    // close(fd)
    unsafe { libc::close(fd_to_close) };
    Ok(())
}

/// `closeAllVfds(void)` (fd.c:3084) — close every open VFD (used before
/// EXEC_BACKEND fork).
pub fn closeAllVfds() -> PgResult<()> {
    // if (SizeVfdCache > 0) { Assert(FileIsNotOpen(0)); for (i = 1; ...) }
    let size = with_fd(|fd| fd.size_vfd_cache());
    if size > 0 {
        for i in 1..size as i32 {
            let is_open = with_fd(|fd| fd.vfd_cache[i as usize].is_open);
            if is_open {
                with_fd(|fd| vfd_core::LruDelete(fd, i));
            }
        }
    }
    Ok(())
}

/// Raw kernel fd behind a transient-file index (helper for callers that need
/// the fd, e.g. `fstat`).
pub fn TransientFileRawFd(fd_value: i32) -> Result<RawFd, i32> {
    // The transient-file API keys on the kernel fd value (see
    // OpenTransientFilePerm). Confirm it is still a live raw-fd descriptor.
    with_fd(|fd| {
        for desc in &fd.allocated_descs {
            if let AllocatedHandle::RawFd(file) = &desc.desc {
                if file.as_raw_fd() == fd_value {
                    return Ok(file.as_raw_fd());
                }
            }
        }
        Err(-1)
    })
}

// ---------------------------------------------------------------------------
// Local OS helpers (the direct libc calls fd.c makes here).
// ---------------------------------------------------------------------------

/// `fopen(name, mode)` — open a buffered stdio stream. Returns the owned file
/// or the failing errno (C returns NULL with errno set).
fn open_stdio(name: &Path, mode: &str) -> Result<std::fs::File, i32> {
    use std::fs::OpenOptions;

    // Map the fopen mode string onto OpenOptions, covering the modes the
    // backend actually passes (r, w, a, r+, w+, a+, with an optional 'b').
    let m = mode.trim_end_matches('b');
    let mut opts = OpenOptions::new();
    match m {
        "r" => {
            opts.read(true);
        }
        "w" => {
            opts.write(true).create(true).truncate(true);
        }
        "a" => {
            opts.append(true).create(true);
        }
        "r+" => {
            opts.read(true).write(true);
        }
        "w+" => {
            opts.read(true).write(true).create(true).truncate(true);
        }
        "a+" => {
            opts.read(true).append(true).create(true);
        }
        _ => {
            opts.read(true);
        }
    }
    opts.open(name).map_err(|e| e.raw_os_error().unwrap_or(0))
}

/// `popen(command, mode)` — spawn `/bin/sh -c command` with its stdin or stdout
/// connected to a pipe (per `mode`). Returns the failing errno on spawn error.
fn popen(command: &str, mode: &str) -> Result<PipeHandle, i32> {
    use std::process::{Command, Stdio};

    let reading = mode.starts_with('r');
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(command);
    if reading {
        cmd.stdout(Stdio::piped());
    } else {
        cmd.stdin(Stdio::piped());
    }
    match cmd.spawn() {
        Ok(mut child) => {
            let stdout = child.stdout.take();
            let stdin = child.stdin.take();
            Ok(PipeHandle {
                child,
                stdout,
                stdin,
            })
        }
        Err(e) => Err(e.raw_os_error().unwrap_or(0)),
    }
}

/// `pclose(file)` — close the pipe end and wait for the child, returning its
/// raw wait status (the value C's `pclose` returns).
fn pclose(mut pipe: PipeHandle) -> i32 {
    // Closing our end of the pipe lets the child see EOF / SIGPIPE, matching
    // pclose closing the stdio stream before waitpid.
    drop(pipe.stdout.take());
    drop(pipe.stdin.take());
    match pipe.child.wait() {
        Ok(status) => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                // pclose returns the raw wait(2) status word.
                if let Some(code) = status.code() {
                    (code & 0xff) << 8
                } else {
                    status.signal().unwrap_or(0) & 0x7f
                }
            }
            #[cfg(not(unix))]
            {
                status.code().unwrap_or(-1)
            }
        }
        Err(_) => -1,
    }
}

// ---------------------------------------------------------------------------
// ereport helpers (routed through the elog owner seam).
// ---------------------------------------------------------------------------

fn current_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn strerror(errno: i32) -> String {
    // SAFETY: strerror returns a pointer to a NUL-terminated static string; we
    // copy it out immediately.
    unsafe {
        let ptr = libc::strerror(errno);
        if ptr.is_null() {
            format!("unrecognized error {errno}")
        } else {
            std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// Build the file-access `PgError` for the given errno + (pre-`%m`) message,
/// at `elevel`, mirroring `ereport(elevel, (errcode_for_file_access(),
/// errmsg(...)))`.
fn file_access_error(
    errno: i32,
    message: String,
    line: i32,
    func: &str,
    elevel: ErrorLevel,
) -> PgError {
    let sqlstate =
        error_seams::sqlstate_for_file_access::call(errno);
    let msg = message.replace("%m", &strerror(errno));
    PgError::new(elevel, msg)
        .with_sqlstate(sqlstate)
        .with_saved_errno(errno)
        .with_error_location(ErrorLocation::new(SRCFILE, line, func))
}

/// `ereport(ERROR, (errcode(code), errmsg(message)))` — build the ERROR-level
/// `PgError` so the caller returns it as `Err`.
fn ereport_error(
    code: ::types_error::SqlState,
    message: String,
    line: i32,
    func: &str,
) -> PgError {
    PgError::new(ERROR, message)
        .with_sqlstate(code)
        .with_error_location(ErrorLocation::new(SRCFILE, line, func))
}

/// `ereport(WARNING, errmsg(message))` — emit a WARNING-level report (returns
/// `Ok(())`, since WARNING is below ERROR).
fn ereport_warning(message: &str, line: i32, func: &str) -> PgResult<()> {
    error_seams::ereport::call(
        PgError::new(::types_error::WARNING, message.to_owned())
            .with_error_location(ErrorLocation::new(SRCFILE, line, func)),
    )
}

/// The shared `ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
/// errmsg("out of file descriptors: %m; release and retry")))` emitted on the
/// EMFILE/ENFILE retry path. C saves and restores errno around it; we expand
/// `%m` from the current errno.
fn ereport_log_out_of_fds() -> PgResult<()> {
    let errno = current_errno();
    let msg = format!("out of file descriptors: {}; release and retry", strerror(errno));
    error_seams::ereport::call(
        PgError::new(LOG, msg)
            .with_sqlstate(ERRCODE_INSUFFICIENT_RESOURCES)
            .with_saved_errno(errno)
            .with_error_location(ErrorLocation::new(SRCFILE, 0, "")),
    )
}

/// Emit a pre-built report through the elog seam: at ERROR+ it returns `Err`,
/// below ERROR it returns `Ok(())`.
fn ereport_at(err: PgError) -> PgResult<()> {
    error_seams::ereport::call(err)
}

// keep ERRCODE_OUT_OF_MEMORY referenced — it documents reserveAllocatedDesc's
// fatal OOM branch, which collapses under Vec (see that function's doc).
#[allow(dead_code)]
const _OOM: ::types_error::SqlState = ERRCODE_OUT_OF_MEMORY;

// ---------------------------------------------------------------------------
// Seam adapters installed by `init_seams`.
// ---------------------------------------------------------------------------

/// `AllocateDir`/`ReadDir`/`FreeDir` as one owned walk — the seam shape for
/// `with_allocated_dir`. Opens the directory, invokes `f` with each entry's
/// `d_name`, and closes it on every path. `f` returns `Ok(true)` to stop the
/// scan early; the walk returns the last callback value (`false` once the
/// directory is exhausted).
pub fn with_allocated_dir(
    dirname: &str,
    f: &mut dyn FnMut(&str) -> PgResult<bool>,
) -> PgResult<bool> {
    // AllocateDir may ereport(ERROR) on exceeding maxAllocatedDescs; propagate.
    let dir = AllocateDir(dirname)?;

    let mut last = false;
    loop {
        // A failed AllocateDir (dir == None) surfaces here as ReadDir's
        // could-not-open ERROR, exactly as in the C shortcut pattern.
        let ent = match ReadDir(dir, dirname) {
            Ok(Some(ent)) => ent,
            Ok(None) => break, // directory exhausted (or read error reported below ERROR)
            Err(e) => {
                // Close the directory on the error path, then propagate.
                let _ = FreeDir(dir);
                return Err(e);
            }
        };

        match f(&ent.d_name) {
            Ok(stop) => {
                last = stop;
                if stop {
                    break;
                }
            }
            Err(e) => {
                let _ = FreeDir(dir);
                return Err(e);
            }
        }
    }

    FreeDir(dir)?;
    Ok(last)
}

/// Seam adapter for `open_transient_file`.
pub fn seam_open_transient_file(file_name: &str, file_flags: i32) -> PgResult<i32> {
    OpenTransientFile(file_name, file_flags)
}

/// Seam adapter for `close_transient_file` — returns the `close()` result.
pub fn seam_close_transient_file(fd: i32) -> i32 {
    // The seam's contract is the C `CloseTransientFile`'s `int` return (the
    // close() result). CloseTransientFile here returns PgResult<()>; the only
    // Err it can produce is the not-obtained-from-OpenTransientFile WARNING,
    // which in C still proceeds to close(fd) and returns its result. So we
    // discard the (warning) Err and report success.
    match CloseTransientFile(fd) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------------------
// Stream/file helpers for the AllocateFile + OpenPipeStream seam adapters.
//
// `AllocateFile`/`OpenPipeStream` return a *table index*; the stdio `FILE *`
// lives inside `allocatedDescs[index]`. These helpers reach the underlying
// owned handle by index to perform the `fwrite`/`fread` the C callers issue
// directly on the `FILE *`.
// ---------------------------------------------------------------------------

/// `fwrite(buf, 1, len, file)` against the buffered stream at table `index`.
/// Returns `None` on a full successful write, or `Some(errno)` on a short /
/// failed write (`fwrite(...) != 1 || ferror(...)` in C). Pipe streams write to
/// the child's stdin.
pub(crate) fn stream_write(index: i32, buf: &[u8]) -> Option<i32> {
    use std::io::Write;
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return Some(libc::EBADF);
        }
        match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::File(file) => match file.write_all(buf) {
                Ok(()) => None,
                Err(e) => Some(e.raw_os_error().unwrap_or(libc::EIO)),
            },
            AllocatedHandle::Pipe(pipe) => match pipe.stdin.as_mut() {
                Some(stdin) => match stdin.write_all(buf) {
                    Ok(()) => None,
                    Err(e) => Some(e.raw_os_error().unwrap_or(libc::EPIPE)),
                },
                None => Some(libc::EPIPE),
            },
            _ => Some(libc::EBADF),
        }
    })
}

/// `fstat(fileno(file), &st)` against the stream at table `index` (copyto's
/// directory check). Returns the file metadata or the failing errno.
pub(crate) fn AllocatedFileMetadata(index: i32) -> Result<std::fs::Metadata, i32> {
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return Err(libc::EBADF);
        }
        match &fd.allocated_descs[i].desc {
            AllocatedHandle::File(file) => {
                file.metadata().map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))
            }
            _ => Err(libc::EBADF),
        }
    })
}

/// `fseeko(file, seek_offset, whence)` on the stream at table `index`:
/// `SEEK_SET` when `seek_offset >= 0`, else `SEEK_END`. Returns the failing
/// errno on a seek error (the `read_binary_file` fseeko half).
pub(crate) fn stream_seek(index: i32, seek_offset: i64) -> Result<(), i32> {
    use std::io::{Seek, SeekFrom};
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return Err(libc::EBADF);
        }
        match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::File(file) => {
                let pos = if seek_offset >= 0 {
                    SeekFrom::Start(seek_offset as u64)
                } else {
                    SeekFrom::End(seek_offset)
                };
                file.seek(pos)
                    .map(|_| ())
                    .map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))
            }
            _ => Err(libc::EBADF),
        }
    })
}

/// `fread` exactly up to `n` bytes from the stream at table `index` (the
/// explicit `bytes_to_read >= 0` branch of `read_binary_file`). A short read at
/// EOF returns fewer bytes — mirroring C's `nbytes = fread(...)`. Returns the
/// failing errno on a read error.
pub(crate) fn stream_read_n(index: i32, n: usize) -> Result<Vec<u8>, i32> {
    use std::io::Read;
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return Err(libc::EBADF);
        }
        match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::File(file) => {
                let mut out = vec![0u8; n];
                let mut filled = 0usize;
                while filled < n {
                    match file.read(&mut out[filled..]) {
                        Ok(0) => break,
                        Ok(k) => filled += k,
                        Err(e) => return Err(e.raw_os_error().unwrap_or(libc::EIO)),
                    }
                }
                out.truncate(filled);
                Ok(out)
            }
            _ => Err(libc::EBADF),
        }
    })
}

/// Read the entire stream at table `index` into a byte buffer (the
/// `fstat`+`fread` pattern snapmgr's `ImportSnapshot` uses). Returns the failing
/// errno on a read error.
pub(crate) fn stream_read_all(index: i32) -> Result<Vec<u8>, i32> {
    use std::io::Read;
    with_fd(|fd| {
        let i = index as usize;
        if i >= fd.allocated_descs.len() {
            return Err(libc::EBADF);
        }
        match &mut fd.allocated_descs[i].desc {
            AllocatedHandle::File(file) => {
                let mut out = Vec::new();
                file.read_to_end(&mut out)
                    .map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))?;
                Ok(out)
            }
            _ => Err(libc::EBADF),
        }
    })
}
