use std::os::fd::RawFd;
use std::path::Path;

use ::elog::ereport;
use ::types_core::SubTransactionId;
use ::types_error::{ErrorLevel, PgResult, DEBUG1, ERROR, LOG, WARNING};
use ::types_storage::{
    File, DATA_DIR_SYNC_METHOD_SYNCFS, PG_TBLSPC_DIR, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
    TABLESPACE_VERSION_DIRECTORY,
};

use crate::desc::{AllocateDir, CloseTransientFile, FreeDesc, FreeDir, OpenTransientFile, ReadDirExtended};
use crate::vfd::{self, get_errno, loc, set_errno, with_fd, FD_CLOSE_AT_EOXACT, FD_DELETE_AT_CLOSE};

// xlog.h enum WalSyncMethod.
const WAL_SYNC_METHOD_FSYNC_WRITETHROUGH: i32 = 3;

fn enable_fsync() -> bool {
    init_small::globals::enableFsync()
}

pub fn pg_fsync(fd: RawFd) -> i32 {
    #[cfg(debug_assertions)]
    {
        // fsync portability assert: files must be opened writable, directories
        // read-only (fd.c:391-424). fstat failure is ignored.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: st is a valid out-param; fd may be any descriptor.
        if unsafe { libc::fstat(fd, &mut st) } == 0 {
            // SAFETY: F_GETFL reads the descriptor flags.
            let desc_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) } & libc::O_ACCMODE;
            if st.st_mode & libc::S_IFMT == libc::S_IFDIR {
                debug_assert!(desc_flags == libc::O_RDONLY);
            } else {
                debug_assert!(desc_flags != libc::O_RDONLY);
            }
        }
        set_errno(0);
    }

    // HAVE_FSYNC_WRITETHROUGH platforms only (macOS here).
    #[cfg(target_os = "macos")]
    if guc_tables::vars::wal_sync_method.read() == WAL_SYNC_METHOD_FSYNC_WRITETHROUGH {
        return pg_fsync_writethrough(fd);
    }
    #[cfg(not(target_os = "macos"))]
    let _ = WAL_SYNC_METHOD_FSYNC_WRITETHROUGH;

    pg_fsync_no_writethrough(fd)
}

pub fn pg_fsync_no_writethrough(fd: RawFd) -> i32 {
    if !enable_fsync() {
        return 0;
    }
    loop {
        // SAFETY: fsync(2) on a caller-owned descriptor.
        let rc = unsafe { libc::fsync(fd) };
        if rc == -1 && get_errno() == libc::EINTR {
            continue;
        }
        return rc;
    }
}

pub fn pg_fsync_writethrough(fd: RawFd) -> i32 {
    if !enable_fsync() {
        return 0;
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        // SAFETY: F_FULLFSYNC on a caller-owned descriptor.
        if unsafe { libc::fcntl(fd, libc::F_FULLFSYNC, 0) } == -1 {
            -1
        } else {
            0
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        let _ = fd;
        set_errno(libc::ENOSYS);
        -1
    }
}

// The libc crate doesn't declare fdatasync for apple targets; macOS has it.
#[cfg(target_os = "macos")]
extern "C" {
    fn fdatasync(fd: libc::c_int) -> libc::c_int;
}
#[cfg(not(target_os = "macos"))]
use libc::fdatasync;

pub fn pg_fdatasync(fd: RawFd) -> i32 {
    if !enable_fsync() {
        return 0;
    }
    loop {
        // SAFETY: fdatasync(2) on a caller-owned descriptor.
        let rc = unsafe { fdatasync(fd) };
        if rc == -1 && get_errno() == libc::EINTR {
            continue;
        }
        return rc;
    }
}

pub fn pg_file_exists(name: &str) -> PgResult<bool> {
    let path = vfd::cpath(name);
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: NUL-terminated path; st is a valid out-param.
    if unsafe { libc::stat(path.as_ptr(), &mut st) } == 0 {
        return Ok(st.st_mode & libc::S_IFMT != libc::S_IFDIR);
    }
    let en = get_errno();
    if !(en == libc::ENOENT || en == libc::ENOTDIR || en == libc::EACCES) {
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not access file \"{name}\": %m"))
            .finish(loc("pg_file_exists"))?;
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
pub fn pg_flush_data(fd: RawFd, offset: i64, nbytes: i64) -> PgResult<()> {
    if !enable_fsync() {
        return Ok(());
    }

    thread_local! {
        static NOT_IMPLEMENTED_BY_KERNEL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if NOT_IMPLEMENTED_BY_KERNEL.get() {
        return Ok(());
    }

    loop {
        // SAFETY: sync_file_range(2) on a caller-owned descriptor.
        let rc = unsafe {
            libc::sync_file_range(fd, offset, nbytes, libc::SYNC_FILE_RANGE_WRITE)
        };
        if rc != 0 {
            if rc == libc::EINTR {
                continue;
            }
            let en = get_errno();
            // One warning then silence when the kernel lacks sync_file_range
            // (e.g. Windows WSL).
            let elevel = if en == libc::ENOSYS {
                NOT_IMPLEMENTED_BY_KERNEL.set(true);
                WARNING
            } else {
                data_sync_elevel(WARNING)
            };
            ereport(elevel)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg("could not flush dirty data: %m")
                .finish(loc("pg_flush_data"))?;
        }
        return Ok(());
    }
}

#[cfg(not(target_os = "linux"))]
pub fn pg_flush_data(fd: RawFd, offset: i64, mut nbytes: i64) -> PgResult<()> {
    use ::types_error::FATAL;

    if !enable_fsync() {
        return Ok(());
    }

    // msync(MS_ASYNC) writeback: mmap the range, sync it, unmap (fd.c:589-670).
    thread_local! {
        static PAGESIZE: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
    }

    if offset == 0 && nbytes == 0 {
        // SAFETY: lseek(2) on a caller-owned descriptor.
        nbytes = unsafe { libc::lseek(fd, 0, libc::SEEK_END) } as i64;
        if nbytes < 0 {
            ereport(WARNING)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg("could not determine dirty data size: %m")
                .finish(loc("pg_flush_data"))?;
            return Ok(());
        }
    }

    let mut pagesize = PAGESIZE.get();
    if pagesize == 0 {
        // SAFETY: sysconf query.
        pagesize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as i64;
        PAGESIZE.set(pagesize);
    }
    if pagesize > 0 {
        nbytes = (nbytes / pagesize) * pagesize;
    }
    if nbytes <= 0 {
        return Ok(());
    }

    let p = if nbytes <= isize::MAX as i64 {
        // SAFETY: read-only shared mapping of a live descriptor's range.
        unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                nbytes as libc::size_t,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                offset as libc::off_t,
            )
        }
    } else {
        libc::MAP_FAILED
    };

    if p != libc::MAP_FAILED {
        // SAFETY: p maps nbytes bytes, established above.
        if unsafe { libc::msync(p, nbytes as libc::size_t, libc::MS_ASYNC) } != 0 {
            ereport(data_sync_elevel(WARNING))
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg("could not flush dirty data: %m")
                .finish(loc("pg_flush_data"))?;
        }
        // SAFETY: unmapping the region mapped above.
        if unsafe { libc::munmap(p, nbytes as libc::size_t) } != 0 {
            // FATAL because the mapping would otherwise remain.
            ereport(FATAL)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg("could not munmap() while flushing data: %m")
                .finish(loc("pg_flush_data"))?;
        }
    }
    Ok(())
}

pub(crate) fn pg_ftruncate(fd: RawFd, length: i64) -> i32 {
    loop {
        // SAFETY: ftruncate(2) on a caller-owned descriptor.
        let ret = unsafe { libc::ftruncate(fd, length as libc::off_t) };
        if ret == -1 && get_errno() == libc::EINTR {
            continue;
        }
        return ret;
    }
}

pub fn pg_truncate(path: &str, length: i64) -> i32 {
    let cpath = vfd::cpath(path);
    loop {
        // SAFETY: truncate(2) on a NUL-terminated path.
        let ret = unsafe { libc::truncate(cpath.as_ptr(), length as libc::off_t) };
        if ret == -1 && get_errno() == libc::EINTR {
            continue;
        }
        return ret;
    }
}

pub fn fsync_fname(fname: &str, isdir: bool) -> PgResult<()> {
    fsync_fname_ext(fname, isdir, false, data_sync_elevel(ERROR)).map(|_| ())
}

pub fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    vfd::data_sync_elevel(elevel)
}

// durable_rename (fd.c:782): 0 on success, -1 otherwise; errno not valid on
// return. At elevel >= ERROR failures propagate as Err instead.
pub fn durable_rename(oldfile: &str, newfile: &str, elevel: ErrorLevel) -> PgResult<i32> {
    if fsync_fname_ext(oldfile, false, false, elevel)? != 0 {
        return Ok(-1);
    }

    let fd = OpenTransientFile(newfile, libc::O_RDWR)?;
    if fd < 0 {
        if get_errno() != libc::ENOENT {
            ereport(elevel)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{newfile}\": %m"))
                .finish(loc("durable_rename"))?;
            return Ok(-1);
        }
    } else {
        if pg_fsync(fd) != 0 {
            let save_errno = get_errno();
            CloseTransientFile(fd);
            ereport(elevel)
                .with_saved_errno(save_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{newfile}\": %m"))
                .finish(loc("durable_rename"))?;
            return Ok(-1);
        }

        if CloseTransientFile(fd) != 0 {
            ereport(elevel)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{newfile}\": %m"))
                .finish(loc("durable_rename"))?;
            return Ok(-1);
        }
    }

    let cold = vfd::cpath(oldfile);
    let cnew = vfd::cpath(newfile);
    // SAFETY: both paths are NUL-terminated.
    if unsafe { libc::rename(cold.as_ptr(), cnew.as_ptr()) } < 0 {
        ereport(elevel)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!(
                "could not rename file \"{oldfile}\" to \"{newfile}\": %m"
            ))
            .finish(loc("durable_rename"))?;
        return Ok(-1);
    }

    if fsync_fname_ext(newfile, false, false, elevel)? != 0 {
        return Ok(-1);
    }
    if fsync_parent_path(newfile, elevel)? != 0 {
        return Ok(-1);
    }
    Ok(0)
}

pub fn durable_unlink(fname: &str, elevel: ErrorLevel) -> PgResult<i32> {
    let cpath = vfd::cpath(fname);
    // SAFETY: NUL-terminated path.
    if unsafe { libc::unlink(cpath.as_ptr()) } < 0 {
        ereport(elevel)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not remove file \"{fname}\": %m"))
            .finish(loc("durable_unlink"))?;
        return Ok(-1);
    }

    if fsync_parent_path(fname, elevel)? != 0 {
        return Ok(-1);
    }
    Ok(0)
}

// fsync_fname_ext (fd.c:3862): 0 on success, -1 otherwise (Err at >= ERROR).
pub fn fsync_fname_ext(
    fname: &str,
    isdir: bool,
    ignore_perm: bool,
    elevel: ErrorLevel,
) -> PgResult<i32> {
    let flags = if isdir { libc::O_RDONLY } else { libc::O_RDWR };

    let fd = OpenTransientFile(fname, flags)?;

    if fd < 0 {
        let en = get_errno();
        if isdir && (en == libc::EISDIR || en == libc::EACCES) {
            return Ok(0);
        }
        if ignore_perm && en == libc::EACCES {
            return Ok(0);
        }
        ereport(elevel)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{fname}\": %m"))
            .finish(loc("fsync_fname_ext"))?;
        return Ok(-1);
    }

    let returncode = pg_fsync(fd);

    if returncode != 0 && !(isdir && (get_errno() == libc::EBADF || get_errno() == libc::EINVAL)) {
        let save_errno = get_errno();
        CloseTransientFile(fd);
        ereport(elevel)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{fname}\": %m"))
            .finish(loc("fsync_fname_ext"))?;
        return Ok(-1);
    }

    if CloseTransientFile(fd) != 0 {
        ereport(elevel)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{fname}\": %m"))
            .finish(loc("fsync_fname_ext"))?;
        return Ok(-1);
    }

    Ok(0)
}

fn fsync_parent_path(fname: &str, elevel: ErrorLevel) -> PgResult<i32> {
    // get_parent_directory yields "" for a bare file name; treat as ".".
    let parent = Path::new(fname).parent().filter(|p| !p.as_os_str().is_empty());
    let parentpath = parent.and_then(Path::to_str).unwrap_or(".");
    fsync_fname_ext(parentpath, true, false, elevel)
}

#[derive(Clone, Copy)]
pub(crate) enum WalkAction {
    PreSync,
    DatadirFsync,
    UnlinkIfExists,
}

impl WalkAction {
    fn apply(self, fname: &str, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
        match self {
            WalkAction::PreSync => pre_sync_fname(fname, isdir, elevel),
            WalkAction::DatadirFsync => datadir_fsync_fname(fname, isdir, elevel),
            WalkAction::UnlinkIfExists => unlink_if_exists_fname(fname, isdir, elevel),
        }
    }
}

pub(crate) fn walkdir(
    path: &str,
    action: WalkAction,
    process_symlinks: bool,
    elevel: ErrorLevel,
) -> PgResult<()> {
    let dir = AllocateDir(path)?;

    while let Some(de) = ReadDirExtended(dir, path, elevel)? {
        postgres_seams::check_for_interrupts::call()?;

        if de.d_name == "." || de.d_name == ".." {
            continue;
        }
        let subpath = format!("{path}/{}", de.d_name);

        let meta = if process_symlinks {
            std::fs::metadata(&subpath)
        } else {
            std::fs::symlink_metadata(&subpath)
        };
        match meta {
            Ok(m) if m.is_file() => action.apply(&subpath, false, elevel)?,
            Ok(m) if m.is_dir() => walkdir(&subpath, action, false, elevel)?,
            // lstat failure was reported by ReadDir's helpers at elevel in C;
            // non-file non-dir entries are skipped.
            _ => {}
        }
    }

    let dir_present = dir.is_some();
    FreeDir(dir)?;
    if dir_present {
        action.apply(path, true, elevel)?;
    }
    Ok(())
}

fn pre_sync_fname(fname: &str, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
    if isdir {
        return Ok(());
    }

    let fd = OpenTransientFile(fname, libc::O_RDONLY)?;
    if fd < 0 {
        let en = get_errno();
        if en == libc::EACCES {
            return Ok(());
        }
        ereport(elevel)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{fname}\": %m"))
            .finish(loc("pre_sync_fname"))?;
        return Ok(());
    }

    let _ = pg_flush_data(fd, 0, 0);

    if CloseTransientFile(fd) != 0 {
        ereport(elevel)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{fname}\": %m"))
            .finish(loc("pre_sync_fname"))?;
    }
    Ok(())
}

fn datadir_fsync_fname(fname: &str, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
    // The pg_flush_data hint ran in the PreSync pass; ignore_perm here.
    fsync_fname_ext(fname, isdir, true, elevel).map(|_| ())
}

fn unlink_if_exists_fname(fname: &str, isdir: bool, elevel: ErrorLevel) -> PgResult<()> {
    if isdir {
        if let Err(e) = std::fs::remove_dir(fname) {
            if e.raw_os_error() != Some(libc::ENOENT) {
                ereport(elevel)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove directory \"{fname}\": %m"))
                    .finish(loc("unlink_if_exists_fname"))?;
            }
        }
    } else {
        crate::temp::PathNameDeleteTemporaryFile(fname, false)?;
    }
    Ok(())
}

// do_syncfs (fd.c:3563, HAVE_SYNCFS): failures logged at LOG, never fatal.
#[cfg(target_os = "linux")]
fn do_syncfs(path: &str) -> PgResult<()> {
    let fd = OpenTransientFile(path, libc::O_RDONLY)?;
    if fd < 0 {
        ereport(LOG)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(loc("do_syncfs"))?;
        return Ok(());
    }
    // SAFETY: syncfs(2) only reads the descriptor.
    if unsafe { libc::syncfs(fd) } < 0 {
        ereport(LOG)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not synchronize file system for file \"{path}\": %m"))
            .finish(loc("do_syncfs"))?;
    }
    CloseTransientFile(fd);
    Ok(())
}

// Without syncfs(2) the `recovery_init_sync_method = syncfs` GUC value is
// unavailable (C's HAVE_SYNCFS guard), so this is unreachable.
#[cfg(not(target_os = "linux"))]
fn do_syncfs(path: &str) -> PgResult<()> {
    let _ = path;
    unreachable!("recovery_init_sync_method = syncfs without syncfs(2)")
}

pub fn SyncDataDirectory() -> PgResult<()> {
    if !enable_fsync() {
        return Ok(());
    }

    let xlog_is_symlink = match std::fs::symlink_metadata("pg_wal") {
        Ok(meta) => meta.file_type().is_symlink(),
        Err(e) => {
            ereport(LOG)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg("could not stat file \"pg_wal\": %m")
                .finish(loc("SyncDataDirectory"))?;
            false
        }
    };

    if vfd::recovery_init_sync_method() == DATA_DIR_SYNC_METHOD_SYNCFS {
        startup_seams::begin_startup_progress_phase::call();
        do_syncfs(".")?;
        let dir = AllocateDir(PG_TBLSPC_DIR)?;
        while let Some(de) = ReadDirExtended(dir, PG_TBLSPC_DIR, LOG)? {
            if de.d_name == "." || de.d_name == ".." {
                continue;
            }
            do_syncfs(&format!("{PG_TBLSPC_DIR}/{}", de.d_name))?;
        }
        FreeDir(dir)?;
        if xlog_is_symlink {
            do_syncfs("pg_wal")?;
        }
        return Ok(());
    }

    startup_seams::begin_startup_progress_phase::call();

    walkdir(".", WalkAction::PreSync, false, DEBUG1)?;
    if xlog_is_symlink {
        walkdir("pg_wal", WalkAction::PreSync, false, DEBUG1)?;
    }
    walkdir(PG_TBLSPC_DIR, WalkAction::PreSync, true, DEBUG1)?;

    startup_seams::begin_startup_progress_phase::call();

    walkdir(".", WalkAction::DatadirFsync, false, LOG)?;
    if xlog_is_symlink {
        walkdir("pg_wal", WalkAction::DatadirFsync, false, LOG)?;
    }
    walkdir(PG_TBLSPC_DIR, WalkAction::DatadirFsync, true, LOG)?;
    Ok(())
}

pub fn RemovePgTempFiles() -> PgResult<()> {
    RemovePgTempFilesInDir(&format!("base/{PG_TEMP_FILES_DIR}"), true, false)?;
    RemovePgTempRelationFiles("base")?;

    let spc_dir = AllocateDir(PG_TBLSPC_DIR)?;
    while let Some(spc_de) = ReadDirExtended(spc_dir, PG_TBLSPC_DIR, LOG)? {
        if spc_de.d_name == "." || spc_de.d_name == ".." {
            continue;
        }
        RemovePgTempFilesInDir(
            &format!(
                "{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}/{PG_TEMP_FILES_DIR}",
                spc_de.d_name
            ),
            true,
            false,
        )?;
        RemovePgTempRelationFiles(&format!(
            "{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}",
            spc_de.d_name
        ))?;
    }
    FreeDir(spc_dir)?;
    Ok(())
}

pub fn RemovePgTempFilesInDir(tmpdirname: &str, missing_ok: bool, unlink_all: bool) -> PgResult<()> {
    let temp_dir = AllocateDir(tmpdirname)?;
    if temp_dir.is_none() && missing_ok && get_errno() == libc::ENOENT {
        return Ok(());
    }

    while let Some(temp_de) = ReadDirExtended(temp_dir, tmpdirname, LOG)? {
        if temp_de.d_name == "." || temp_de.d_name == ".." {
            continue;
        }
        let rm_path = format!("{tmpdirname}/{}", temp_de.d_name);

        if unlink_all || temp_de.d_name.starts_with(PG_TEMP_FILE_PREFIX) {
            match std::fs::symlink_metadata(&rm_path) {
                Err(_) => continue,
                Ok(meta) if meta.is_dir() => {
                    RemovePgTempFilesInDir(&rm_path, false, true)?;
                    if let Err(e) = std::fs::remove_dir(&rm_path) {
                        ereport(LOG)
                            .with_saved_errno(e.raw_os_error().unwrap_or(0))
                            .errcode_for_file_access()
                            .errmsg(format!("could not remove directory \"{rm_path}\": %m"))
                            .finish(loc("RemovePgTempFilesInDir"))?;
                    }
                }
                Ok(_) => {
                    if let Err(e) = std::fs::remove_file(&rm_path) {
                        ereport(LOG)
                            .with_saved_errno(e.raw_os_error().unwrap_or(0))
                            .errcode_for_file_access()
                            .errmsg(format!("could not remove file \"{rm_path}\": %m"))
                            .finish(loc("RemovePgTempFilesInDir"))?;
                    }
                }
            }
        } else {
            ereport(LOG)
                .errmsg(format!(
                    "unexpected file found in temporary-files directory: \"{rm_path}\""
                ))
                .finish(loc("RemovePgTempFilesInDir"))?;
        }
    }
    FreeDir(temp_dir)?;
    Ok(())
}

pub(crate) fn RemovePgTempRelationFiles(tsdirname: &str) -> PgResult<()> {
    let ts_dir = AllocateDir(tsdirname)?;
    while let Some(de) = ReadDirExtended(ts_dir, tsdirname, LOG)? {
        if de.d_name.is_empty() || !de.d_name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        RemovePgTempRelationFilesInDbspace(&format!("{tsdirname}/{}", de.d_name))?;
    }
    FreeDir(ts_dir)?;
    Ok(())
}

fn RemovePgTempRelationFilesInDbspace(dbspacedirname: &str) -> PgResult<()> {
    let dbspace_dir = AllocateDir(dbspacedirname)?;
    while let Some(de) = ReadDirExtended(dbspace_dir, dbspacedirname, LOG)? {
        if !looks_like_temp_rel_name(&de.d_name) {
            continue;
        }
        let rm_path = format!("{dbspacedirname}/{}", de.d_name);
        if let Err(e) = std::fs::remove_file(&rm_path) {
            ereport(LOG)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{rm_path}\": %m"))
                .finish(loc("RemovePgTempRelationFilesInDbspace"))?;
        }
    }
    FreeDir(dbspace_dir)?;
    Ok(())
}

// t<digits>_<digits>[_<forkname>][.<segment>] (fd.c:3514).
pub fn looks_like_temp_rel_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.first() != Some(&b't') {
        return false;
    }

    let mut pos = 1;
    let start = pos;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    if pos == start || bytes.get(pos) != Some(&b'_') {
        return false;
    }

    pos += 1;
    let start = pos;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    if pos == start {
        return false;
    }

    if bytes.get(pos) == Some(&b'_') {
        let Some(consumed) = forkname_chars(&name[pos + 1..]) else {
            return false;
        };
        pos += consumed + 1;
    }

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

    pos == bytes.len()
}

// relpath.c forkname_chars: matches the suffixed forks only ("main" is the
// unsuffixed fork and never appears here).
fn forkname_chars(name: &str) -> Option<usize> {
    ["fsm", "vm", "init"]
        .into_iter()
        .find(|fork| name.starts_with(fork))
        .map(str::len)
}

pub fn AtEOSubXact_Files(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    // FreeDesc swap-compacts, so on abort the same index is re-checked.
    let mut i = 0;
    loop {
        enum Step {
            Done,
            Advance,
            Free,
        }
        let step = with_fd(|fd| {
            if i >= fd.allocated_descs.len() {
                return Step::Done;
            }
            if fd.allocated_descs[i].create_subid == my_subid {
                if is_commit {
                    fd.allocated_descs[i].create_subid = parent_subid;
                    Step::Advance
                } else {
                    Step::Free
                }
            } else {
                Step::Advance
            }
        });
        match step {
            Step::Done => break,
            Step::Advance => i += 1,
            Step::Free => {
                FreeDesc(i as i32);
            }
        }
    }
}

pub fn AtEOXact_Files(is_commit: bool) -> PgResult<()> {
    CleanupTempFiles(is_commit, false)?;
    with_fd(|fd| {
        fd.temp_table_spaces = None;
        fd.next_temp_table_space = 0;
    });
    Ok(())
}

pub fn BeforeShmemExit_Files() {
    let _ = CleanupTempFiles(false, true);
    // No temp files may be created after this point (assert-only in C).
    with_fd(|fd| fd.temporary_files_allowed = false);
}

pub(crate) fn CleanupTempFiles(is_commit: bool, is_proc_exit: bool) -> PgResult<()> {
    struct ToClose {
        file: File,
        warn: bool,
    }

    // Collect first: FileClose mutates FdState, so the borrow can't span it.
    let to_close: Vec<ToClose> = with_fd(|fd| {
        let mut result = Vec::new();
        if is_proc_exit || fd.have_xact_temporary_files {
            debug_assert!(vfd::FileIsNotOpen(fd, 0), "VFD ring header corrupted");
            for i in 1..fd.size_vfd_cache() {
                let v = &fd.vfd_cache[i];
                if (v.fdstate & (FD_DELETE_AT_CLOSE | FD_CLOSE_AT_EOXACT)) != 0
                    && v.file_name.is_some()
                {
                    if is_proc_exit {
                        result.push(ToClose {
                            file: File(i as i32),
                            warn: false,
                        });
                    } else if v.fdstate & FD_CLOSE_AT_EOXACT != 0 {
                        result.push(ToClose {
                            file: File(i as i32),
                            warn: true,
                        });
                    }
                }
            }
            fd.have_xact_temporary_files = false;
        }
        result
    });

    for ToClose { file, warn } in to_close {
        if warn {
            let name = crate::io::FilePathName(file);
            ereport(WARNING)
                .errmsg_internal(format!(
                    "temporary file {name} not closed at end-of-transaction"
                ))
                .finish(loc("CleanupTempFiles"))?;
        }
        crate::io::FileClose(file)?;
    }

    let num_allocated = with_fd(|fd| fd.allocated_descs.len());
    if is_commit && num_allocated > 0 {
        ereport(WARNING)
            .errmsg_internal(format!(
                "{num_allocated} temporary files and directories not closed at end-of-transaction"
            ))
            .finish(loc("CleanupTempFiles"))?;
    }

    while with_fd(|fd| !fd.allocated_descs.is_empty()) {
        FreeDesc(0);
    }
    Ok(())
}
