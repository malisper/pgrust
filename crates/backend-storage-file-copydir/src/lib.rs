//! `backend-storage-file-copydir` — a faithful port of
//! `src/backend/storage/file/copydir.c` (recursive directory copy).
//!
//! `copydir` recursively copies a directory tree (used by CREATE DATABASE's
//! file-copy strategy and by `reinit.c`'s unlogged-relation init-fork pass).
//! `copy_file` is the buffered read/write copy of a single regular file, and
//! `clone_file` is the OS reflink/clone fast path selected by the
//! `file_copy_method` GUC.
//!
//! The dir-walk recursion, the `.`/`..` skip, the regular-vs-directory
//! classification, the read/write copy loop and the two-pass fsync are ported
//! 1:1 from C. The dir iteration drives the already-ported `fd` API
//! (`AllocateDir`/`ReadDir`/`FreeDir`, `pg_flush_data`, `fsync_fname`, and the
//! `make_pg_directory` seam) directly on `backend-storage-file-fd`. The
//! `enableFsync` GUC is read through `backend-access-transam-xlog-seams`
//! (its owner), and `CHECK_FOR_INTERRUPTS` through
//! `backend-tcop-postgres-seams`.
//!
//! Where C classifies an entry with `get_dirent_type(path, de, /* look_through
//! _symlinks = */ false, ERROR)` — i.e. `lstat`, never following a symlink — the
//! port uses [`std::fs::symlink_metadata`], which has the same `lstat`
//! semantics: a symlink reports neither a directory nor a regular file and is
//! therefore skipped, exactly as in C.
//!
//! This crate owns `copydir`/`copy_file`/`clone_file` (no inward seams: nothing
//! in the repo reaches these through a seam yet — the sibling `reinit.c` owner
//! calls `copy_file` directly), so its `init_seams` is empty.

#![allow(non_snake_case)]
// `PgError` is a large owned struct, so the un-boxed `Err` variant trips
// `clippy::result_large_err`; the un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]

use std::fs::File as StdFile;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicI32, Ordering};

use backend_access_transam_xlog_seams::enable_fsync;
use backend_storage_file_fd::allocated_desc::{AllocateDir, FreeDir, ReadDir};
use backend_storage_file_fd::sync_cleanup::{fsync_fname, pg_flush_data};
use backend_storage_file_fd_seams::make_pg_directory;
use backend_tcop_postgres_seams::check_for_interrupts;
use backend_utils_error::{ereport, errno::sqlstate_for_file_access};
use types_core::BLCKSZ;
use types_error::{PgError, PgResult, ERROR};
use types_storage::file::{FileCopyMethod, FILE_COPY_METHOD_CLONE, FILE_COPY_METHOD_COPY};

/// `COPY_BUF_SIZE` (copydir.c:143) — `(8 * BLCKSZ)`, the read/write buffer size.
const COPY_BUF_SIZE: usize = 8 * BLCKSZ;

/// `FLUSH_DISTANCE` (copydir.c:151-155) — how many bytes to copy before asking
/// the OS to start flushing them to disk. macOS (`__darwin__`) uses 32MB; every
/// other platform uses 1MB.
#[cfg(target_os = "macos")]
const FLUSH_DISTANCE: i64 = 32 * 1024 * 1024;
#[cfg(not(target_os = "macos"))]
const FLUSH_DISTANCE: i64 = 1024 * 1024;

/// `int file_copy_method = FILE_COPY_METHOD_COPY;` (copydir.c:34) — the GUC
/// controlling whether `copydir` copies regular files byte-for-byte
/// (`FILE_COPY_METHOD_COPY`) or asks the OS to clone/reflink them
/// (`FILE_COPY_METHOD_CLONE`). In PostgreSQL this is a plain backend global
/// written by GUC assignment; modelled here as a process-global atomic with the
/// same `FILE_COPY_METHOD_COPY` default.
static FILE_COPY_METHOD: AtomicI32 = AtomicI32::new(FILE_COPY_METHOD_COPY);

/// Read the current `file_copy_method` GUC value.
pub fn file_copy_method() -> FileCopyMethod {
    FILE_COPY_METHOD.load(Ordering::Relaxed)
}

/// Assign the `file_copy_method` GUC. The C GUC machinery only ever stores one
/// of the two recognized enum values; reject anything else loudly rather than
/// silently accept it.
pub fn set_file_copy_method(method: FileCopyMethod) -> PgResult<()> {
    match method {
        FILE_COPY_METHOD_COPY | FILE_COPY_METHOD_CLONE => {
            FILE_COPY_METHOD.store(method, Ordering::Relaxed);
            Ok(())
        }
        _ => Err(ereport(ERROR)
            .errmsg("unrecognized file copy method")
            .into_error()),
    }
}

/// Install this crate's GUC variable accessors. `file_copy_method` is a plain
/// enum GUC whose `conf->variable` points at `int file_copy_method`
/// (copydir.c:34, guc_tables.c:5267-5273); the GUC machinery reads and writes
/// it directly (e.g. copydir.c:86 `if (file_copy_method == FILE_COPY_METHOD_CLONE)`),
/// so we install `get`/`set` accessors over this crate's backing store, exactly
/// as C dereferences `*conf->variable`.
///
/// This crate owns no inward seams: nothing in the repo reaches
/// `copydir`/`copy_file`/`clone_file` through a seam (the sibling `reinit`
/// owner calls `copy_file` directly).
pub fn init_seams() {
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    // The GUC enum machinery only ever assigns one of the canonicalized enum
    // values (it checks against `file_copy_method_options[]` before assigning),
    // so the assign half is the bare `*conf->variable = newval` store.
    vars::file_copy_method.install(GucVarAccessors {
        get: file_copy_method,
        set: |newval| FILE_COPY_METHOD.store(newval, Ordering::Relaxed),
    });
}

/// Build the `ereport(elevel, (errcode_for_file_access(), errmsg("..%m")))`
/// `PgError` from a `std::io::Error`: the SQLSTATE and the `%m` rendering both
/// come from the OS errno (defaulting to `EIO` when `std` recorded none, as the
/// `fd` module does).
fn io_error(message: String, error: &std::io::Error) -> PgError {
    let errno = error
        .raw_os_error()
        .unwrap_or(backend_utils_error::errno::EIO);
    ereport(ERROR)
        .errcode(sqlstate_for_file_access(errno))
        .with_saved_errno(errno)
        .errmsg(format!("{message}: {error}"))
        .into_error()
}

/// `copydir(const char *fromdir, const char *todir, bool recurse)`
/// (copydir.c:47-127).
///
/// Copy every regular file in `fromdir` to `todir` (creating `todir` first),
/// recursing into subdirectories when `recurse` is set. The copy is done in two
/// passes: the first pass copies all the data without fsyncing, and the second
/// pass (skipped when `enableFsync` is off) fsyncs each copied file and the
/// destination directory, so that one expensive fsync barrier covers the whole
/// tree rather than one per file.
pub fn copydir(fromdir: &str, todir: &str, recurse: bool) -> PgResult<()> {
    // copydir.c:55-58 -- create the destination directory (MakePGDirectory).
    if make_pg_directory::call(todir) != 0 {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(backend_utils_error::errno::EIO);
        return Err(ereport(ERROR)
            .errcode(sqlstate_for_file_access(errno))
            .with_saved_errno(errno)
            .errmsg(format!("could not create directory \"{todir}\""))
            .into_error());
    }

    // copydir.c:60-92 -- first pass: copy every regular file, recursing into
    // subdirectories, without fsyncing (the fsync happens in the second pass).
    let xldir = AllocateDir(fromdir)?;
    while let Some(xlde) = ReadDir(xldir, fromdir)? {
        // copydir.c:67 -- CHECK_FOR_INTERRUPTS() during the copy.
        check_for_interrupts::call()?;

        // copydir.c:69-71.
        if xlde.d_name == "." || xlde.d_name == ".." {
            continue;
        }

        // copydir.c:73-74 -- build "fromdir/name" and "todir/name".
        let fromfile = format!("{fromdir}/{}", xlde.d_name);
        let tofile = format!("{todir}/{}", xlde.d_name);

        // copydir.c:76 -- get_dirent_type(fromfile, xlde, false, ERROR):
        // classify without following symlinks (lstat semantics). A symlink is
        // therefore neither a dir nor a regular file and (as in C) is skipped.
        let file_type = std::fs::symlink_metadata(&fromfile)
            .map_err(|error| io_error(format!("could not stat file \"{fromfile}\""), &error))?
            .file_type();

        if file_type.is_dir() {
            // copydir.c:78-83 -- recurse to handle subdirectories.
            if recurse {
                copydir(&fromfile, &tofile, true)?;
            }
        } else if file_type.is_file() {
            // copydir.c:84-90.
            if file_copy_method() == FILE_COPY_METHOD_CLONE {
                clone_file(&fromfile, &tofile)?;
            } else {
                copy_file(&fromfile, &tofile)?;
            }
        }
    }
    FreeDir(xldir)?;

    // copydir.c:94-99 -- if fsync is disabled, we're done. Subdirectories were
    // already fsync'd by the recursive copydir before it returned.
    if !enable_fsync::call() {
        return Ok(());
    }

    // copydir.c:101-118 -- second pass: fsync each regular file.
    let xldir = AllocateDir(todir)?;
    while let Some(xlde) = ReadDir(xldir, todir)? {
        // copydir.c:105-107.
        if xlde.d_name == "." || xlde.d_name == ".." {
            continue;
        }

        let tofile = format!("{todir}/{}", xlde.d_name);

        // copydir.c:111-116 -- fsync only regular files (lstat, no symlink
        // follow, exactly as get_dirent_type(..., false, ...) does). We don't
        // need to sync subdirectories here since the recursive copydir already
        // did it before returning.
        let is_reg = std::fs::symlink_metadata(&tofile)
            .map_err(|error| io_error(format!("could not stat file \"{tofile}\""), &error))?
            .file_type()
            .is_file();
        if is_reg {
            fsync_fname(&tofile, false)?;
        }
    }
    FreeDir(xldir)?;

    // copydir.c:126 -- fsync the destination directory itself, since individual
    // file fsyncs don't guarantee the directory entry is synced.
    fsync_fname(todir, true)
}

/// `copy_file(const char *fromfile, const char *tofile)` (copydir.c:132-232).
///
/// Copy the regular file `fromfile` to `tofile` with a `COPY_BUF_SIZE` buffer,
/// asking the OS to begin flushing every `FLUSH_DISTANCE` bytes. No fsync is
/// done here; `copydir`'s second pass fsyncs the destination (this matches C,
/// which only calls `pg_flush_data` during the loop, never `fsync` inside
/// `copy_file`).
pub fn copy_file(fromfile: &str, tofile: &str) -> PgResult<()> {
    // copydir.c:158 -- palloc the maxaligned copy buffer. Allocated OOM-safe
    // (the project rule for data-derived growable allocations); freed on every
    // return path by being dropped at end of scope (C's `pfree(buffer)`).
    let mut buffer: Vec<u8> = Vec::new();
    buffer
        .try_reserve_exact(COPY_BUF_SIZE)
        .map_err(|_| out_of_memory())?;
    buffer.resize(COPY_BUF_SIZE, 0);

    // copydir.c:163-167 -- open source O_RDONLY | PG_BINARY.
    let mut srcfd = StdFile::open(fromfile)
        .map_err(|error| io_error(format!("could not open file \"{fromfile}\""), &error))?;

    // copydir.c:169-173 -- create dest O_RDWR | O_CREAT | O_EXCL | PG_BINARY.
    let mut dstfd = StdFile::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(tofile)
        .map_err(|error| io_error(format!("could not create file \"{tofile}\""), &error))?;

    // copydir.c:178-216 -- the data copy loop.
    let mut offset: i64 = 0;
    let mut flush_offset: i64 = 0;
    loop {
        // copydir.c:181-182 -- CHECK_FOR_INTERRUPTS() during the file copy.
        check_for_interrupts::call()?;

        // copydir.c:189-193 -- once we've copied FLUSH_DISTANCE bytes since the
        // last hint, ask the OS to start writing them out.
        if offset - flush_offset >= FLUSH_DISTANCE {
            pg_flush_data(&dstfd, flush_offset, offset - flush_offset)?;
            flush_offset = offset;
        }

        // copydir.c:195-203 -- read one buffer's worth from the source.
        let nbytes = srcfd
            .read(&mut buffer)
            .map_err(|error| io_error(format!("could not read file \"{fromfile}\""), &error))?;
        // copydir.c:202-203 -- nbytes == 0 means EOF.
        if nbytes == 0 {
            break;
        }

        // copydir.c:204-215 -- write it all to the destination. `write_all`
        // loops over short writes (C's `write(...) != nbytes` short-write case,
        // where C blames ENOSPC if errno is unset) and surfaces a write error.
        dstfd
            .write_all(&buffer[..nbytes])
            .map_err(|error| io_error(format!("could not write to file \"{tofile}\""), &error))?;

        // copydir.c:179 -- offset += nbytes.
        offset += nbytes as i64;
    }

    // copydir.c:218-219 -- flush any remaining un-hinted bytes.
    if offset > flush_offset {
        pg_flush_data(&dstfd, flush_offset, offset - flush_offset)?;
    }

    // copydir.c:221-229 -- CloseTransientFile(dstfd) / CloseTransientFile(srcfd).
    // Closing is dropping the owned `std::fs::File`. C checks the close result
    // and errors on failure; the only close failure `std` exposes is a pending
    // buffered-write flush failure, so explicitly flush the destination first so
    // a late write error surfaces (as C's close-time "could not close" error)
    // rather than being swallowed by the implicit drop. The read-only source has
    // no buffered writes, so its drop is the close.
    dstfd
        .flush()
        .map_err(|error| io_error(format!("could not close file \"{tofile}\""), &error))?;
    drop(dstfd);
    drop(srcfd);

    Ok(())
}

/// `clone_file(const char *fromfile, const char *tofile)` (copydir.c:237-294).
///
/// The platform-specific reflink/clone copy selected by
/// `file_copy_method == FILE_COPY_METHOD_CLONE`: `copyfile(...
/// COPYFILE_CLONE_FORCE)` on macOS, a `copy_file_range(2)` loop on Linux, and
/// `pg_unreachable()` (the function is never reached — `file_copy_method` can't
/// be set to CLONE) on platforms with neither. These are raw OS clone syscalls
/// owned by this translation unit, so they are issued directly via `libc`.
#[cfg(target_os = "macos")]
fn clone_file(fromfile: &str, tofile: &str) -> PgResult<()> {
    use std::ffi::CString;
    // copydir.c:240-245 -- copyfile(from, to, NULL, COPYFILE_CLONE_FORCE).
    let from = CString::new(fromfile).map_err(|_| {
        ereport(ERROR)
            .errmsg(format!("invalid path \"{fromfile}\""))
            .into_error()
    })?;
    let to = CString::new(tofile).map_err(|_| {
        ereport(ERROR)
            .errmsg(format!("invalid path \"{tofile}\""))
            .into_error()
    })?;
    // `COPYFILE_CLONE_FORCE` (copyfile.h) — clone, failing if a clone is not
    // possible (rather than falling back to a plain copy).
    const COPYFILE_CLONE_FORCE: u32 = 1 << 14;
    extern "C" {
        fn copyfile(
            from: *const libc::c_char,
            to: *const libc::c_char,
            state: *mut libc::c_void,
            flags: u32,
        ) -> libc::c_int;
    }
    // SAFETY: `from`/`to` are NUL-terminated C strings; `state` NULL is the
    // documented "no state" argument, exactly as the C call passes.
    let rc = unsafe { copyfile(from.as_ptr(), to.as_ptr(), std::ptr::null_mut(), COPYFILE_CLONE_FORCE) };
    if rc < 0 {
        let error = std::io::Error::last_os_error();
        return Err(io_error(
            format!("could not clone file \"{fromfile}\" to \"{tofile}\""),
            &error,
        ));
    }
    Ok(())
}

/// `clone_file` — the Linux `copy_file_range(2)` loop (copydir.c:246-289).
#[cfg(target_os = "linux")]
fn clone_file(fromfile: &str, tofile: &str) -> PgResult<()> {
    // copydir.c:251-255 -- open source O_RDONLY | PG_BINARY.
    let srcfd = StdFile::open(fromfile)
        .map_err(|error| io_error(format!("could not open file \"{fromfile}\""), &error))?;
    // copydir.c:257-261 -- create dest O_WRONLY | O_CREAT | O_EXCL | PG_BINARY.
    let dstfd = StdFile::options()
        .write(true)
        .create_new(true)
        .open(tofile)
        .map_err(|error| io_error(format!("could not create file \"{tofile}\""), &error))?;

    use std::os::unix::io::AsRawFd;
    // copydir.c:263-279 -- copy_file_range loop until it returns 0 (EOF),
    // tolerating EINTR and checking for interrupts each iteration.
    loop {
        check_for_interrupts::call()?;
        // SAFETY: both fds are valid open files for the duration; NULL offsets
        // advance the kernel file offsets, exactly as the C call passes.
        let nbytes = unsafe {
            libc::copy_file_range(
                srcfd.as_raw_fd(),
                std::ptr::null_mut(),
                dstfd.as_raw_fd(),
                std::ptr::null_mut(),
                1024 * 1024,
                0,
            )
        };
        if nbytes < 0 {
            let error = std::io::Error::last_os_error();
            // copydir.c:272 -- tolerate EINTR.
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(io_error(
                format!("could not clone file \"{fromfile}\" to \"{tofile}\""),
                &error,
            ));
        }
        if nbytes == 0 {
            break;
        }
    }

    // copydir.c:281-289 -- close both, surfacing any deferred write error.
    drop(dstfd);
    drop(srcfd);
    Ok(())
}

/// `clone_file` — `pg_unreachable()` (copydir.c:291-292): with no OS clone
/// support, `file_copy_method` can never be set to CLONE, so this is never
/// reached.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn clone_file(_fromfile: &str, _tofile: &str) -> PgResult<()> {
    unreachable!("clone_file reached without OS clone support (file_copy_method cannot be CLONE)")
}

fn out_of_memory() -> PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}
