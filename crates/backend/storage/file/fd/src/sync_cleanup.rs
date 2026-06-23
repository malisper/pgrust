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
use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::FromRawFd;
use std::path::Path;

use ::types_core::SubTransactionId;
use types_error::{ErrorLevel, PgError, PgResult, DEBUG1, ERROR, LOG, WARNING};
use types_storage::{
    PG_TBLSPC_DIR, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX, TABLESPACE_VERSION_DIRECTORY,
};

use crate::allocated_desc::{
    AllocateDir, CloseTransientFile, FreeDir, OpenTransientFile, ReadDirExtended, TransientFileRawFd,
};
use crate::vfd_io::{FileClose, FilePathName};

// ---------------------------------------------------------------------------
// open(2) flag bits used by this module (fcntl.h). Stable across the platforms
// PostgreSQL builds on; defined here to keep the crate libc-free.
// ---------------------------------------------------------------------------

const O_RDONLY: i32 = 0;
const O_RDWR: i32 = 2;
/// `PG_BINARY` (`c.h`) — 0 on non-Windows (no text/binary distinction).
const PG_BINARY: i32 = 0;

// ---------------------------------------------------------------------------
// errno values (POSIX ABI numbers; identical across Linux/BSD/macOS for the
// ones used here). Mirrors the C `errno == E*` comparisons.
// ---------------------------------------------------------------------------

mod errno {
    pub const ENOENT: i32 = 2;
    pub const EIO: i32 = 5;
    pub const EBADF: i32 = 9;
    pub const EACCES: i32 = 13;
    pub const ENOTDIR: i32 = 20;
    pub const EISDIR: i32 = 21;
    pub const EINVAL: i32 = 22;
    pub const ENOSYS: i32 = 38;
}

// ---------------------------------------------------------------------------
// Seam accessors for foreign-owned globals/services.
// ---------------------------------------------------------------------------

/// `enableFsync` (xlog.c GUC). Routed through the xlog owner's seam.
fn enable_fsync() -> bool {
    transam_xlog_seams::enable_fsync::call()
}

/// `wal_sync_method` (xlog.c GUC). Routed through the xlog owner's seam.
fn wal_sync_method() -> wal::WalSyncMethod {
    transam_xlog_seams::wal_sync_method::call()
}

/// `CHECK_FOR_INTERRUPTS()` (miscadmin.h). Routed through tcop's seam.
fn check_for_interrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

/// `begin_startup_progress_phase()` (startup.c). Routed through the owner seam.
fn begin_startup_progress_phase() {
    startup_seams::begin_startup_progress_phase::call();
}

/// `ereport(level, ...)` for a non-throwing report (level < ERROR). Routed
/// through the elog owner's seam. The `Result` (the C longjmp at >= ERROR) is
/// ignored at LOG/WARNING, matching `elog(LOG, ...)`/`elog(WARNING, ...)`.
fn elog(level: ErrorLevel, message: String) {
    let _ = error_seams::ereport::call(PgError::new(level, message));
}

// ---------------------------------------------------------------------------
// Error helpers (mirror C `ereport(elevel, (errcode_for_file_access(),
// errmsg("...: %m"))); return -1`).
// ---------------------------------------------------------------------------

/// `errno` of an `io::Error`, defaulting to `EIO` when none is recorded.
fn raw_errno(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(errno::EIO)
}

/// Build a file-access `PgError` at `level` from a message and an `io::Error`,
/// carrying the saved errno.
fn io_error_level(level: ErrorLevel, message: String, error: &io::Error) -> PgError {
    PgError::new(level, format!("{message}: {error}")).with_saved_errno(raw_errno(error))
}

/// Report `error` at `level`: at `>= ERROR` return `Err` (C longjmp); below
/// `ERROR` emit a log line and return `Ok(())` (C `ereport` then `return -1`).
fn report_io(level: ErrorLevel, message: String, error: &io::Error) -> PgResult<()> {
    if level >= ERROR {
        Err(io_error_level(level, message, error))
    } else {
        elog(level, format!("{message}: {error}"));
        Ok(())
    }
}

/// As [`report_io`] but for an errno already in hand (no `io::Error`).
fn report_errno(level: ErrorLevel, message: String, errno: i32) -> PgResult<()> {
    let err = PgError::new(level, message).with_saved_errno(errno);
    if level >= ERROR {
        Err(err)
    } else {
        let _ = error_seams::ereport::call(err);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// pg_fsync family (fd.c:385-560). Early-out when enableFsync is off.
// ---------------------------------------------------------------------------

/// `pg_fsync(int fd)` (fd.c:388) — fsync with or without writethrough per
/// `wal_sync_method`.
pub fn pg_fsync(file: &StdFile) -> PgResult<()> {
    // fd.c:426-432 -- consult wal_sync_method only for the writethrough case;
    // otherwise plain fsync.
    if wal_sync_method() == wal::WalSyncMethod::FsyncWritethrough {
        pg_fsync_writethrough(file)
    } else {
        pg_fsync_no_writethrough(file)
    }
}

/// `pg_fsync_no_writethrough(int fd)` (fd.c:441) — `fsync`, but nothing when
/// `enableFsync` is off.
pub fn pg_fsync_no_writethrough(file: &StdFile) -> PgResult<()> {
    if !enable_fsync() {
        return Ok(());
    }
    // C retries on EINTR; std's `sync_all` (fsync) handles EINTR internally.
    file.sync_all()
        .map_err(|error| io_error_level(ERROR, "could not fsync file".into(), &error))
}

/// `pg_fsync_writethrough(int fd)` (fd.c:461) — `F_FULLFSYNC` where available.
pub fn pg_fsync_writethrough(file: &StdFile) -> PgResult<()> {
    if !enable_fsync() {
        return Ok(());
    }
    // fd.c:465-470 -- macOS/iOS have F_FULLFSYNC; the strongest portable safe
    // primitive is sync_all (a normal fsync). Elsewhere C sets errno=ENOSYS
    // and returns -1 (fd.c:468), surfaced here as an error carrying ENOSYS.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        file.sync_all()
            .map_err(|error| io_error_level(ERROR, "could not fsync file".into(), &error))
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        let _ = file;
        Err(PgError::new(ERROR, "could not fsync file (writethrough)".to_string())
            .with_saved_errno(errno::ENOSYS))
    }
}

/// `pg_fdatasync(int fd)` (fd.c:480) — `fdatasync`, but nothing when
/// `enableFsync` is off.
pub fn pg_fdatasync(file: &StdFile) -> PgResult<()> {
    if !enable_fsync() {
        return Ok(());
    }
    // C retries on EINTR; std's `sync_data` (fdatasync) handles EINTR internally.
    file.sync_data()
        .map_err(|error| io_error_level(ERROR, "could not fdatasync file".into(), &error))
}

/// `pg_file_exists(const char *name)` (fd.c:503) — true if the path exists and
/// is not a directory.
pub fn pg_file_exists(name: impl AsRef<Path>) -> PgResult<bool> {
    let name = name.as_ref();
    match std::fs::metadata(name) {
        // fd.c:509-510 -- exists; true iff not a directory.
        Ok(meta) => Ok(!meta.is_dir()),
        Err(error) => {
            let e = raw_errno(&error);
            // fd.c:511 -- ENOENT / ENOTDIR / EACCES => "does not exist"; any
            // other errno is an ereport(ERROR).
            if e == errno::ENOENT || e == errno::ENOTDIR || e == errno::EACCES {
                Ok(false)
            } else {
                Err(io_error_level(
                    ERROR,
                    format!("could not access file \"{}\"", name.display()),
                    &error,
                ))
            }
        }
    }
}

/// `pg_flush_data(int fd, off_t offset, off_t nbytes)` (fd.c:524) — advise the
/// OS to flush dirty data. A performance hint only. `std` exposes no portable
/// writeback-hint primitive (`sync_file_range`/`msync`/`posix_fadvise` are all
/// `libc`-only), so this is the no-op fall-through fd.c itself uses on
/// platforms where none of those is available, still honoring the `enableFsync`
/// early-out (fd.c:533-534).
pub fn pg_flush_data(_file: &StdFile, _offset: i64, _nbytes: i64) -> PgResult<()> {
    if !enable_fsync() {
        return Ok(());
    }
    Ok(())
}

/// `pg_truncate(const char *path, off_t length)` (fd.c:720) — truncate a file
/// to `length` by name, retrying on EINTR (handled by std internally).
pub fn pg_truncate(path: impl AsRef<Path>, length: i64) -> PgResult<()> {
    let path = path.as_ref();
    // C uses truncate(2) on non-WIN32 (fd.c:740). std has no truncate-by-name,
    // so open for writing and set_len; both the open and set_len errors surface
    // with the path-bearing message C uses at the call sites.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| {
            io_error_level(
                ERROR,
                format!("could not truncate file \"{}\"", path.display()),
                &error,
            )
        })?;
    file.set_len(length as u64).map_err(|error| {
        io_error_level(
            ERROR,
            format!("could not truncate file \"{}\"", path.display()),
            &error,
        )
    })
}

/// `fsync_fname(const char *fname, bool isdir)` (fd.c:756).
pub fn fsync_fname(path: impl AsRef<Path>, isdir: bool) -> PgResult<()> {
    fsync_fname_ext(path, isdir, false, data_sync_elevel(ERROR))
}

// ---------------------------------------------------------------------------
// durable_rename / durable_unlink (fd.c:782-891).
// ---------------------------------------------------------------------------

/// `durable_rename(const char *oldfile, const char *newfile, int elevel)`
/// (fd.c:782) — rename issuing the fsyncs required for crash durability.
pub fn durable_rename(
    oldfile: impl AsRef<Path>,
    newfile: impl AsRef<Path>,
    elevel: ErrorLevel,
) -> PgResult<()> {
    let oldfile = oldfile.as_ref();
    let newfile = newfile.as_ref();

    // fd.c:793 -- fsync the source first (return -1 on failure).
    fsync_fname_ext(oldfile, false, false, elevel)?;

    // fd.c:796-831 -- fsync any pre-existing target before the rename. ENOENT
    // (no pre-existing target) is skipped.
    match OpenTransientFile(newfile, PG_BINARY | O_RDWR) {
        Ok(fd) => {
            // fd.c:809-822 -- pg_fsync(fd); on error close + ereport + return.
            if let Err(error) = fsync_transient_fd(fd) {
                let save = error.saved_errno().unwrap_or(errno::EIO);
                let _ = CloseTransientFile(fd);
                return report_errno(
                    elevel,
                    format!("could not fsync file \"{}\"", newfile.display()),
                    save,
                );
            }
            // fd.c:824-830 -- CloseTransientFile; ereport + return on failure.
            if let Err(error) = CloseTransientFile(fd) {
                let save = error.saved_errno().unwrap_or(errno::EIO);
                return report_errno(
                    elevel,
                    format!("could not close file \"{}\"", newfile.display()),
                    save,
                );
            }
        }
        // fd.c:797-806 -- open failure; ENOENT means no target, anything else
        // is an ereport + return.
        Err(error) if error.saved_errno() == Some(errno::ENOENT) => {}
        Err(error) => {
            let save = error.saved_errno().unwrap_or(errno::EIO);
            return report_errno(
                elevel,
                format!("could not open file \"{}\"", newfile.display()),
                save,
            );
        }
    }

    // fd.c:834-841 -- the real rename.
    if let Err(error) = std::fs::rename(oldfile, newfile) {
        return report_io(
            elevel,
            format!(
                "could not rename file \"{}\" to \"{}\"",
                oldfile.display(),
                newfile.display()
            ),
            &error,
        );
    }

    // fd.c:847-851 -- fsync the renamed file and its containing directory.
    fsync_fname_ext(newfile, false, false, elevel)?;
    fsync_parent_path(newfile, elevel)?;
    Ok(())
}

/// `durable_unlink(const char *fname, int elevel)` (fd.c:872) — remove a file
/// durably (fsync the parent directory afterwards).
pub fn durable_unlink(fname: impl AsRef<Path>, elevel: ErrorLevel) -> PgResult<()> {
    let fname = fname.as_ref();
    // fd.c:874-881 -- unlink; ereport + return on failure.
    if let Err(error) = std::fs::remove_file(fname) {
        return report_io(
            elevel,
            format!("could not remove file \"{}\"", fname.display()),
            &error,
        );
    }
    // fd.c:887 -- fsync the parent directory.
    fsync_parent_path(fname, elevel)?;
    Ok(())
}

/// `data_sync_elevel(int elevel)` (fd.c:4001) — bump to PANIC unless
/// `data_sync_retry`.
pub fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    crate::vfd_core::data_sync_elevel(elevel)
}

// ---------------------------------------------------------------------------
// Transient-fd fsync glue.
//
// fd.c's `pg_fsync(int fd)` takes a raw kernel fd; this family obtains transient
// fds (an index into the allocated-descriptor table) and must fsync them. We
// borrow the kernel fd as a non-owning `StdFile` (the descriptor stays owned by
// the allocated-descriptor table; `CloseTransientFile` closes it), call the
// `&StdFile` `pg_fsync`, and never drop the borrow.
// ---------------------------------------------------------------------------

fn fsync_transient_fd(index: i32) -> PgResult<()> {
    let raw = TransientFileRawFd(index).map_err(|e| {
        PgError::new(ERROR, "could not access transient file descriptor".to_string())
            .with_saved_errno(e)
    })?;
    // SAFETY: `raw` is a live kernel fd owned by the allocated-descriptor table.
    // ManuallyDrop ensures we never close it here; the table closes it via
    // CloseTransientFile, mirroring C's `pg_fsync(fd)` (no ownership transfer).
    let file = ManuallyDrop::new(unsafe { StdFile::from_raw_fd(raw) });
    pg_fsync(&file)
}

// ---------------------------------------------------------------------------
// fsync_fname_ext / fsync_parent_path (fd.c:3862-3998 region).
// ---------------------------------------------------------------------------

/// `fsync_fname_ext(const char *fname, bool isdir, bool ignore_perm, int elevel)`
/// (fd.c:3862).
pub fn fsync_fname_ext(
    fname: impl AsRef<Path>,
    isdir: bool,
    ignore_perm: bool,
    elevel: ErrorLevel,
) -> PgResult<()> {
    let fname = fname.as_ref();

    // fd.c:3872-3877 -- directories are opened O_RDONLY, files O_RDWR.
    let mut flags = PG_BINARY;
    if !isdir {
        flags |= O_RDWR;
    } else {
        flags |= O_RDONLY;
    }

    let fd = match OpenTransientFile(fname, flags) {
        Ok(fd) => fd,
        Err(error) => {
            let e = error.saved_errno().unwrap_or(errno::EIO);
            // fd.c:3884-3899 -- some kernels disallow opening a directory R/W,
            // or refuse a read-only dir open; tolerate those, and (if
            // ignore_perm) an EACCES on a file. Otherwise ereport + return.
            #[allow(clippy::if_same_then_else)]
            if isdir && (e == errno::EISDIR || e == errno::EACCES) {
                return Ok(());
            } else if ignore_perm && e == errno::EACCES {
                return Ok(());
            }
            return report_errno(
                elevel,
                format!("could not open file \"{}\"", fname.display()),
                e,
            );
        }
    };

    // fd.c:3901-3923 -- pg_fsync; some kernels reject fsync on a directory fd
    // (EBADF / EINVAL), which is tolerated for directories. Otherwise close +
    // ereport + return.
    if let Err(error) = fsync_transient_fd(fd) {
        let e = error.saved_errno().unwrap_or(errno::EIO);
        if !(isdir && (e == errno::EBADF || e == errno::EINVAL)) {
            let _ = CloseTransientFile(fd);
            return report_errno(
                elevel,
                format!("could not fsync file \"{}\"", fname.display()),
                e,
            );
        }
    }

    // fd.c:3925-3931 -- CloseTransientFile; ereport + return on failure.
    if let Err(error) = CloseTransientFile(fd) {
        let e = error.saved_errno().unwrap_or(errno::EIO);
        return report_errno(
            elevel,
            format!("could not close file \"{}\"", fname.display()),
            e,
        );
    }
    Ok(())
}

/// `fsync_parent_path(const char *fname, int elevel)` (fd.c:3960 region) —
/// fsync the directory containing `fname`.
fn fsync_parent_path(fname: &Path, elevel: ErrorLevel) -> PgResult<()> {
    // C `get_parent_directory` strips the last path component, leaving "" when
    // there is none, which C then treats as ".".
    let parent = fname.parent().filter(|p| !p.as_os_str().is_empty());
    let parentpath = parent.unwrap_or_else(|| Path::new("."));
    fsync_fname_ext(parentpath, true, false, elevel)
}

// ---------------------------------------------------------------------------
// Data-directory sync walk (fd.c:3609-3859 region).
// ---------------------------------------------------------------------------

/// The per-entry action a [`walkdir`] applies.
#[derive(Clone, Copy)]
pub(crate) enum WalkAction {
    /// `pre_sync_fname` (fd.c:3786).
    PreSync,
    /// `datadir_fsync_fname` (fd.c:3824).
    DatadirFsync,
    /// `unlink_if_exists_fname` (fd.c:3837).
    UnlinkIfExists,
}

impl WalkAction {
    fn apply(self, fname: &Path, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
        match self {
            WalkAction::PreSync => pre_sync_fname(fname, isdir, elevel),
            WalkAction::DatadirFsync => datadir_fsync_fname(fname, isdir, elevel),
            WalkAction::UnlinkIfExists => unlink_if_exists_fname(fname, isdir, elevel),
        }
    }
}

/// `pre_sync_fname(const char *fname, bool isdir, int elevel)` (fd.c:3786,
/// `#ifdef PG_FLUSH_DATA_WORKS`) — hint the kernel that `fname` is about to be
/// fsync'd. Directories are skipped; the flush itself is a best-effort hint
/// (`pg_flush_data` is a no-op in the safe port), but the open/close churn and
/// its error logging at `elevel` are reproduced.
fn pre_sync_fname(fname: &Path, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
    // fd.c:3793-3794 -- don't try to flush directories.
    if isdir {
        return Ok(());
    }

    // fd.c:3796 -- ereport_startup_progress hint (folded into the phase report).
    let fname_str = fname.to_str().expect("pre_sync_fname: non-UTF-8 path");

    // fd.c:3799 -- OpenTransientFile(fname, O_RDONLY | PG_BINARY).
    let fd = match OpenTransientFile(fname_str, O_RDONLY | PG_BINARY) {
        Ok(fd) => fd,
        Err(error) => {
            // fd.c:3801-3808 -- EACCES is silently ignored; otherwise log.
            let e = error.saved_errno().unwrap_or(errno::EIO);
            if e == errno::EACCES {
                return Ok(());
            }
            return report_errno(
                elevel,
                format!("could not open file \"{fname_str}\""),
                e,
            );
        }
    };

    // fd.c:3814 -- pg_flush_data(fd, 0, 0); errors ignored (hint only / no-op).
    if let Ok(raw) = TransientFileRawFd(fd) {
        // SAFETY: `raw` is owned by the transient-file slot; ManuallyDrop keeps
        // the slot owning it (CloseTransientFile closes it below).
        let borrowed = ManuallyDrop::new(unsafe { StdFile::from_raw_fd(raw) });
        let _ = pg_flush_data(&borrowed, 0, 0);
    }

    // fd.c:3816-3821 -- CloseTransientFile; ereport at elevel on failure.
    if let Err(error) = CloseTransientFile(fd) {
        let e = error.saved_errno().unwrap_or(errno::EIO);
        return report_errno(
            elevel,
            format!("could not close file \"{fname_str}\""),
            e,
        );
    }
    Ok(())
}

/// `walkdir(const char *path, void (*action)(...), bool process_symlinks,
/// int elevel)` (fd.c:3723) — recurse a directory tree applying `action` to
/// each entry, then to the directory itself.
pub(crate) fn walkdir(
    path: &Path,
    action: WalkAction,
    process_symlinks: bool,
    elevel: ErrorLevel,
) -> PgResult<()> {
    // fd.c:3733-3736 -- AllocateDir; on open failure ReadDir reports below.
    let dir = AllocateDir(path)?;

    // fd.c:3738-3768 -- iterate entries.
    while let Some(de) = ReadDirExtended(dir, path, elevel)? {
        // fd.c:3744 -- CHECK_FOR_INTERRUPTS().
        check_for_interrupts()?;

        if de.d_name == "." || de.d_name == ".." {
            continue;
        }
        let subpath = path.join(&de.d_name);

        // fd.c:3750-3757 -- stat (follow symlinks) or lstat (don't).
        let meta = if process_symlinks {
            std::fs::metadata(&subpath)
        } else {
            std::fs::symlink_metadata(&subpath)
        };
        match meta {
            // fd.c:3762-3765 -- regular file: apply action.
            Ok(m) if m.is_file() => action.apply(&subpath, false, elevel)?,
            // fd.c:3766-3767 -- directory: recurse (no symlink following inside).
            Ok(m) if m.is_dir() => walkdir(&subpath, action, false, elevel)?,
            // fd.c logs stat failures via the LOG variant of ReadDir's helpers;
            // anything that is neither a file nor a directory is skipped.
            _ => {}
        }
    }

    // fd.c:3773-3781 -- after the entries, fsync the directory itself, but only
    // if it could be opened (C does this unconditionally after FreeDir, which
    // tolerates a NULL dir; the open failure has already been logged).
    let dir_present = dir.is_some();
    FreeDir(dir)?;
    if dir_present {
        action.apply(path, true, elevel)?;
    }
    Ok(())
}

/// `datadir_fsync_fname(const char *fname, bool isdir, int elevel)` (fd.c:3824).
fn datadir_fsync_fname(fname: &Path, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
    // fd.c:3829-3835 -- pg_flush_data hint omitted (no-op in the safe port),
    // then fsync_fname_ext with ignore_perm = true.
    fsync_fname_ext(fname, isdir, true, elevel)
}

/// `unlink_if_exists_fname(const char *fname, bool isdir, int elevel)`
/// (fd.c:3837).
fn unlink_if_exists_fname(fname: &Path, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
    if isdir {
        // fd.c:3841-3846 -- rmdir; ignore ENOENT, ereport otherwise.
        if let Err(error) = std::fs::remove_dir(fname) {
            if raw_errno(&error) != errno::ENOENT {
                return report_io(
                    elevel,
                    format!("could not remove directory \"{}\"", fname.display()),
                    &error,
                );
            }
        }
    } else {
        // fd.c:3850-3857 -- PathNameDeleteTemporaryFile handles its own errors.
        let fname_str = fname
            .to_str()
            .expect("unlink_if_exists_fname: non-UTF-8 path");
        let _ = crate::temp_files::PathNameDeleteTemporaryFile(fname_str, false);
    }
    Ok(())
}

/// `do_syncfs(const char *path)` (fd.c:3563, `#if defined(HAVE_SYNCFS)`) —
/// `OpenTransientFile(path, O_RDONLY)` + `syncfs(fd)` + `CloseTransientFile`,
/// all failures logged at LOG (non-fatal startup sync). `syncfs(2)` is a Linux
/// syscall; on platforms without it the `recovery_init_sync_method = syncfs`
/// GUC value is unavailable so this path is never reached, mirroring the C's
/// `HAVE_SYNCFS` guard.
#[cfg(target_os = "linux")]
fn do_syncfs(path: &str) {
    // fd.c:3568 -- ereport_startup_progress hint (folded into the phase report).
    // fd.c:3570-3577 -- OpenTransientFile(path, O_RDONLY); LOG + return on error.
    let fd = match OpenTransientFile(path, O_RDONLY) {
        Ok(fd) => fd,
        Err(error) => {
            let save = error.saved_errno().unwrap_or(errno::EIO);
            let _ = report_errno(LOG, format!("could not open file \"{path}\""), save);
            return;
        }
    };
    // fd.c:3578-3582 -- syncfs(fd); LOG on error (but still CloseTransientFile).
    let raw = TransientFileRawFd(fd);
    match raw {
        Ok(raw) => {
            // SAFETY: `raw` is a live kernel fd owned by the allocated-descriptor
            // table; CloseTransientFile closes it. syncfs(2) only reads the fd.
            if unsafe { libc::syncfs(raw) } < 0 {
                let e = io::Error::last_os_error();
                let _ = report_io(
                    LOG,
                    format!("could not synchronize file system for file \"{path}\""),
                    &e,
                );
            }
        }
        Err(save) => {
            let _ = report_errno(
                LOG,
                format!("could not synchronize file system for file \"{path}\""),
                save,
            );
        }
    }
    // fd.c:3583 -- CloseTransientFile(fd).
    let _ = CloseTransientFile(fd);
}

/// Non-Linux fall-through: `syncfs(2)` does not exist, so (matching the C's
/// `HAVE_SYNCFS` guard) the `recovery_init_sync_method = syncfs` GUC value is
/// unavailable and this is never reached. Provided only so `SyncDataDirectory`
/// type-checks across platforms.
#[cfg(not(target_os = "linux"))]
fn do_syncfs(path: &str) {
    let _ = path;
    unreachable!(
        "recovery_init_sync_method = syncfs is unavailable on platforms without syncfs(2)"
    );
}

/// `SyncDataDirectory(void)` (fd.c:3609) — fsync (or syncfs) the whole data
/// directory at startup.
pub fn SyncDataDirectory() -> PgResult<()> {
    // fd.c:3617-3620 -- nothing to do if fsync is disabled.
    if !enable_fsync() {
        return Ok(());
    }

    // fd.c:3625-3639 -- check whether pg_wal is a symlink (a separately-mounted
    // WAL directory); on stat failure log and treat as not-a-symlink.
    let xlog_is_symlink = match std::fs::symlink_metadata("pg_wal") {
        Ok(meta) => meta.file_type().is_symlink(),
        Err(error) => {
            elog(LOG, format!("could not stat file \"pg_wal\": {error}"));
            false
        }
    };

    // fd.c:3645-3680 -- recovery_init_sync_method == syncfs path.
    if crate::vfd_core::recovery_init_sync_method() == ::types_storage::DATA_DIR_SYNC_METHOD_SYNCFS {
        begin_startup_progress_phase();
        do_syncfs(".");
        let dir = AllocateDir(PG_TBLSPC_DIR)?;
        while let Some(de) = ReadDirExtended(dir, PG_TBLSPC_DIR, LOG)? {
            if de.d_name == "." || de.d_name == ".." {
                continue;
            }
            let path = format!("{PG_TBLSPC_DIR}/{}", de.d_name);
            do_syncfs(&path);
        }
        FreeDir(dir)?;
        if xlog_is_symlink {
            do_syncfs("pg_wal");
        }
        return Ok(());
    }

    // fd.c:3674-3687 (#ifdef PG_FLUSH_DATA_WORKS) -- the pre-fsync pass hints
    // the kernel that we're about to fsync; errors are logged only at DEBUG1.
    // pg_flush_data is a no-op in the safe port, but the walk/open/close churn
    // and its error logging are reproduced for fidelity.
    begin_startup_progress_phase();

    walkdir(Path::new("."), WalkAction::PreSync, false, DEBUG1)?;
    if xlog_is_symlink {
        walkdir(Path::new("pg_wal"), WalkAction::PreSync, false, DEBUG1)?;
    }
    walkdir(Path::new(PG_TBLSPC_DIR), WalkAction::PreSync, true, DEBUG1)?;

    // fd.c:3689-3712 -- the fsync pass, in the same order; this carries the
    // durability guarantee.
    begin_startup_progress_phase();

    walkdir(Path::new("."), WalkAction::DatadirFsync, false, LOG)?;
    if xlog_is_symlink {
        walkdir(Path::new("pg_wal"), WalkAction::DatadirFsync, false, LOG)?;
    }
    walkdir(Path::new(PG_TBLSPC_DIR), WalkAction::DatadirFsync, true, LOG)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Temp-file removal (fd.c:3337-3556 region).
// ---------------------------------------------------------------------------

/// `RemovePgTempFiles(void)` (fd.c:3338) — remove leftover temp files at
/// startup. Failures are reported at LOG and the walk keeps going.
pub fn RemovePgTempFiles() -> PgResult<()> {
    // fd.c:3346-3351 -- first the temp files in the default tablespace.
    let temp_path = format!("base/{PG_TEMP_FILES_DIR}");
    RemovePgTempFilesInDir(&temp_path, true, false)?;
    RemovePgTempRelationFiles("base")?;

    // fd.c:3354-3389 -- then walk all tablespaces.
    let spc_dir = AllocateDir(PG_TBLSPC_DIR)?;
    while let Some(spc_de) = ReadDirExtended(spc_dir, PG_TBLSPC_DIR, LOG)? {
        if spc_de.d_name == "." || spc_de.d_name == ".." {
            continue;
        }
        let temp_path = format!(
            "{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}/{PG_TEMP_FILES_DIR}",
            spc_de.d_name
        );
        RemovePgTempFilesInDir(&temp_path, true, false)?;

        let rel_path = format!(
            "{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}",
            spc_de.d_name
        );
        RemovePgTempRelationFiles(&rel_path)?;
    }
    FreeDir(spc_dir)?;
    Ok(())
}

/// `RemovePgTempFilesInDir(const char *tmpdirname, bool missing_ok, bool unlink_all)`
/// (fd.c:3398).
pub fn RemovePgTempFilesInDir(
    tmpdirname: impl AsRef<Path>,
    missing_ok: bool,
    unlink_all: bool,
) -> PgResult<()> {
    let tmpdirname = tmpdirname.as_ref();
    // fd.c:3407-3414 -- AllocateDir; a NULL dir with missing_ok returns quietly.
    let temp_dir = AllocateDir(tmpdirname)?;
    if temp_dir.is_none() && missing_ok {
        return Ok(());
    }

    // fd.c:3416-3451 -- iterate entries.
    while let Some(temp_de) = ReadDirExtended(temp_dir, tmpdirname, LOG)? {
        if temp_de.d_name == "." || temp_de.d_name == ".." {
            continue;
        }
        let rm_path = tmpdirname.join(&temp_de.d_name);

        // fd.c:3424 -- remove anything when unlink_all, else only PG_TEMP_FILE_PREFIX.
        if unlink_all || temp_de.d_name.starts_with(PG_TEMP_FILE_PREFIX) {
            // fd.c:3426-3445 -- lstat to distinguish dir vs file; recurse into
            // directories then rmdir, else unlink.
            match std::fs::symlink_metadata(&rm_path) {
                Err(_) => continue,
                Ok(meta) if meta.is_dir() => {
                    RemovePgTempFilesInDir(&rm_path, false, true)?;
                    if let Err(err) = std::fs::remove_dir(&rm_path) {
                        elog(
                            LOG,
                            format!("could not remove directory \"{}\": {err}", rm_path.display()),
                        );
                    }
                }
                Ok(_) => {
                    if let Err(err) = std::fs::remove_file(&rm_path) {
                        elog(
                            LOG,
                            format!("could not remove file \"{}\": {err}", rm_path.display()),
                        );
                    }
                }
            }
        } else {
            // fd.c:3447-3450 -- unexpected non-temp file in the temp directory.
            elog(
                LOG,
                format!(
                    "unexpected file found in temporary-files directory: \"{}\"",
                    rm_path.display()
                ),
            );
        }
    }
    FreeDir(temp_dir)?;
    Ok(())
}

/// `RemovePgTempRelationFiles(const char *tsdirname)` (fd.c:3458) — walk the
/// per-database directories of a tablespace removing temp relation files.
pub(crate) fn RemovePgTempRelationFiles(tsdirname: impl AsRef<Path>) -> PgResult<()> {
    let tsdirname = tsdirname.as_ref();
    let ts_dir = AllocateDir(tsdirname)?;
    while let Some(de) = ReadDirExtended(ts_dir, tsdirname, LOG)? {
        // fd.c:3470-3477 -- strspn(d_name, "0123456789") != strlen(d_name)
        // skips non-database directories ("." / ".." and any non-numeric name).
        if de.d_name.is_empty() || !de.d_name.bytes().all(|b: u8| b.is_ascii_digit()) {
            continue;
        }
        let dbspace_path = tsdirname.join(&de.d_name);
        RemovePgTempRelationFilesInDbspace(&dbspace_path)?;
    }
    FreeDir(ts_dir)?;
    Ok(())
}

/// `RemovePgTempRelationFilesInDbspace(const char *dbspacedirname)` (fd.c:3486).
fn RemovePgTempRelationFilesInDbspace(dbspacedirname: &Path) -> PgResult<()> {
    let dbspace_dir = AllocateDir(dbspacedirname)?;
    while let Some(de) = ReadDirExtended(dbspace_dir, dbspacedirname, LOG)? {
        // fd.c:3496-3497 -- only files matching a temp relation name pattern.
        if !looks_like_temp_rel_name(&de.d_name) {
            continue;
        }
        let rm_path = dbspacedirname.join(&de.d_name);
        if let Err(err) = std::fs::remove_file(&rm_path) {
            elog(
                LOG,
                format!("could not remove file \"{}\": {err}", rm_path.display()),
            );
        }
    }
    FreeDir(dbspace_dir)?;
    Ok(())
}

/// `looks_like_temp_rel_name(const char *name)` (fd.c:3514) — match
/// `t<digits>_<digits>[_<forkname>][.<segment>]`.
pub fn looks_like_temp_rel_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    // fd.c:3520 -- must start with 't'.
    if bytes.first() != Some(&b't') {
        return false;
    }

    // fd.c:3523-3527 -- a non-empty run of digits, then '_'.
    let mut pos = 1;
    let start = pos;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    if pos == start || bytes.get(pos) != Some(&b'_') {
        return false;
    }

    // fd.c:3529-3533 -- another non-empty run of digits.
    pos += 1;
    let start = pos;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    if pos == start {
        return false;
    }

    // fd.c:3535-3545 -- optional "_<forkname>".
    if bytes.get(pos) == Some(&b'_') {
        let Some(consumed) = forkname_chars(&name[pos + 1..]) else {
            return false;
        };
        pos += consumed + 1;
    }

    // fd.c:3547-3556 -- optional ".<segment-number>" (non-empty digit run).
    if bytes.get(pos) == Some(&b'.') {
        let seg_start = pos + 1;
        let mut segpos = seg_start;
        while bytes.get(segpos).is_some_and(u8::is_ascii_digit) {
            segpos += 1;
        }
        if segpos == seg_start {
            return false;
        }
        pos = segpos;
    }

    // fd.c:3558 -- the whole string must have been consumed.
    pos == bytes.len()
}

/// `forkNames` lookup (`common/relpath.c`) — match a fork-name prefix, returning
/// the number of characters it consumes.
fn forkname_chars(name: &str) -> Option<usize> {
    // forkNames[] = { "main", "fsm", "vm", "init" }; "main" never appears in a
    // temp relation file name (it is the unsuffixed fork), so only the suffixed
    // forks are matched here, mirroring the C `forkname_chars` callers.
    ["fsm", "vm", "init"]
        .into_iter()
        .find(|fork| name.starts_with(fork))
        .map(str::len)
}

// ---------------------------------------------------------------------------
// Transaction-end / proc-exit cleanup (fd.c:3196-3314 region).
// ---------------------------------------------------------------------------

/// `CleanupTempFiles(bool isCommit, bool isProcExit)` (fd.c:3266).
pub(crate) fn CleanupTempFiles(is_commit: bool, is_proc_exit: bool) {
    // fd.c:3274-3304 -- at proc-exit (or whenever transaction temp files exist)
    // close FD_DELETE_AT_CLOSE / FD_CLOSE_AT_EOXACT VFDs. Collect the indices
    // first to avoid holding the FdState borrow across FileClose (which itself
    // mutates FdState).
    struct ToClose {
        file: ::types_storage::File,
        warn: bool,
    }

    let to_close: Vec<ToClose> = crate::vfd_core::with_fd(|fd| {
        let mut result = Vec::new();
        if is_proc_exit || fd.have_xact_temporary_files {
            // FileIsNotOpen(0): the ring header (slot 0) must never be open.
            debug_assert!(!fd.vfd_cache[0].is_open, "VFD ring header corrupted");
            for i in 1..fd.size_vfd_cache() {
                let v = &fd.vfd_cache[i];
                let fdstate = v.fdstate;
                if ((fdstate & crate::vfd_core::FD_DELETE_AT_CLOSE != 0)
                    || (fdstate & crate::vfd_core::FD_CLOSE_AT_EOXACT != 0))
                    && v.file_name.is_some()
                {
                    if is_proc_exit {
                        result.push(ToClose { file: ::types_storage::File(i as i32), warn: false });
                    } else if fdstate & crate::vfd_core::FD_CLOSE_AT_EOXACT != 0 {
                        result.push(ToClose { file: ::types_storage::File(i as i32), warn: true });
                    }
                }
            }
            fd.have_xact_temporary_files = false;
        }
        result
    });

    for ToClose { file, warn } in to_close {
        if warn {
            let name = FilePathName(file);
            elog(
                WARNING,
                format!("temporary file {name} not closed at end-of-transaction"),
            );
        }
        let _ = FileClose(file);
    }

    // fd.c:3307-3309 -- complain about leftover allocated descriptors at commit.
    let num_allocated = crate::vfd_core::with_fd(|fd| fd.allocated_descs.len());
    if is_commit && num_allocated > 0 {
        elog(
            WARNING,
            format!(
                "{num_allocated} temporary files and directories not closed at end-of-transaction"
            ),
        );
    }

    // fd.c:3311-3313 -- free all remaining allocated descriptors (FreeDesc
    // compacts by swapping the last into slot 0, so always free index 0).
    loop {
        let remaining = crate::vfd_core::with_fd(|fd| fd.allocated_descs.len());
        if remaining == 0 {
            break;
        }
        let _ = crate::allocated_desc::FreeDesc(0);
    }
}

/// `AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)`
/// (fd.c:3196) — reassign (commit) or close (abort) descriptors created in the
/// subxact.
pub fn AtEOSubXact_Files(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    // fd.c:3201-3213 -- FreeDesc compacts the table (swaps the last entry into
    // the freed slot), so on abort we re-check the same index.
    let mut i = 0;
    loop {
        let action = crate::vfd_core::with_fd(|fd| {
            if i >= fd.allocated_descs.len() {
                return 0; // done
            }
            if fd.allocated_descs[i].create_subid == my_subid {
                if is_commit {
                    fd.allocated_descs[i].create_subid = parent_subid;
                    1 // advance
                } else {
                    2 // free (caller frees outside the borrow, then re-checks i)
                }
            } else {
                1 // advance
            }
        });
        match action {
            0 => break,
            1 => i += 1,
            _ => {
                let _ = crate::allocated_desc::FreeDesc(i as i32);
                // re-check same index after the swap-remove compaction.
            }
        }
    }
}

/// `AtEOXact_Files(bool isCommit)` (fd.c:3229).
pub fn AtEOXact_Files(is_commit: bool) {
    CleanupTempFiles(is_commit, false);
    // fd.c:3232-3233 -- forget the transaction-local temp tablespace list.
    crate::vfd_core::with_fd(|fd| {
        fd.temp_table_spaces = None;
        fd.next_temp_table_space = 0;
    });
}

/// `BeforeShmemExit_Files(int code, Datum arg)` (fd.c:3243) — proc-exit cleanup
/// of *all* temp files, including interXact ones.
pub fn BeforeShmemExit_Files() {
    CleanupTempFiles(false, true);
    // fd.c:3248-3250 -- prevent further temp files (assert-only in C).
    crate::vfd_core::with_fd(|fd| fd.temporary_files_allowed = false);
}
