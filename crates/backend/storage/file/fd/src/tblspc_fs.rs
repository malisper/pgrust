//! Inward-seam adapters for the raw POSIX filesystem primitives that
//! `commands/tablespace.c` issues directly on tablespace directories and
//! symlinks (`stat`/`lstat` with `S_ISDIR`/`S_ISLNK`, `MakePGDirectory`,
//! `pg_mkdir_p`, `chmod`, `symlink`, `rmdir`, `unlink`).
//!
//! These are pure libc / `src/port` primitives owned by the fd unit; the
//! `backend-storage-file-tblspc-fs-seams` decl crate has no separate owner, so
//! the fd owner installs them (a sanctioned cross-crate install). Errors are
//! reported as the C-style `errno` integer so the caller can reproduce
//! tablespace.c's exact `errno == ENOENT`/`EEXIST` branching.

use ::tblspc_fs_seams::{StatKind, StatResult};
use ::types_error::PgResult;

use crate::vfd_core;

fn errno_now() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn path_cstring(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path).unwrap_or_else(|_| std::ffi::CString::new("").unwrap())
}

/// Classify a `stat`/`lstat` `st_mode`: `S_ISDIR` -> `Dir`, `S_ISLNK` ->
/// `Symlink`, else `Other`.
fn classify(mode: libc::mode_t) -> StatKind {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => StatKind::Dir,
        libc::S_IFLNK => StatKind::Symlink,
        _ => StatKind::Other,
    }
}

/// `stat(path, &st)` — follows symlinks. `Found(kind)` on success, `Failed(errno)`
/// on `< 0`. Seam adapter for `stat`.
pub fn seam_stat(path: &str) -> PgResult<StatResult> {
    let cpath = path_cstring(path);
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: cpath is NUL-terminated; st is a valid out-param.
    if unsafe { libc::stat(cpath.as_ptr(), &mut st) } < 0 {
        Ok(StatResult::Failed(errno_now()))
    } else {
        Ok(StatResult::Found(classify(st.st_mode)))
    }
}

/// `lstat(path, &st)` — does NOT follow symlinks. Seam adapter for `lstat`.
pub fn seam_lstat(path: &str) -> PgResult<StatResult> {
    let cpath = path_cstring(path);
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: cpath is NUL-terminated; st is a valid out-param.
    if unsafe { libc::lstat(cpath.as_ptr(), &mut st) } < 0 {
        Ok(StatResult::Failed(errno_now()))
    } else {
        Ok(StatResult::Found(classify(st.st_mode)))
    }
}

/// `MakePGDirectory(path)` (fd.c) — `mkdir` with `pg_dir_create_mode`. `Ok(())`
/// on success, `Err(errno)` on `< 0`. Seam adapter for the tblspc-fs
/// `make_pg_directory`.
pub fn seam_make_pg_directory(path: &str) -> PgResult<Result<(), i32>> {
    if vfd_core::seam_make_pg_directory(path) < 0 {
        Ok(Err(errno_now()))
    } else {
        Ok(Ok(()))
    }
}

/// `pg_mkdir_p(path, pg_dir_create_mode)` (src/port/pgmkdirp.c). Seam adapter for
/// the tblspc-fs `pg_mkdir_p`.
pub fn seam_pg_mkdir_p(path: &str) -> PgResult<Result<(), i32>> {
    Ok(vfd_core::seam_pg_mkdir_p(path))
}

/// `chmod(path, pg_dir_create_mode)`. Seam adapter for `chmod_dir`.
pub fn seam_chmod_dir(path: &str) -> PgResult<Result<(), i32>> {
    let cpath = path_cstring(path);
    let mode = vfd_core::pg_dir_create_mode() as libc::mode_t;
    // SAFETY: cpath is NUL-terminated.
    if unsafe { libc::chmod(cpath.as_ptr(), mode) } != 0 {
        Ok(Err(errno_now()))
    } else {
        Ok(Ok(()))
    }
}

/// `symlink(oldpath, newpath)` — create `newpath` pointing at `oldpath`. Seam
/// adapter for `symlink`.
pub fn seam_symlink(oldpath: &str, newpath: &str) -> PgResult<Result<(), i32>> {
    let cold = path_cstring(oldpath);
    let cnew = path_cstring(newpath);
    // SAFETY: both paths are NUL-terminated.
    if unsafe { libc::symlink(cold.as_ptr(), cnew.as_ptr()) } < 0 {
        Ok(Err(errno_now()))
    } else {
        Ok(Ok(()))
    }
}

/// `rmdir(path)`. Seam adapter for the tblspc-fs `rmdir`.
pub fn seam_rmdir(path: &str) -> PgResult<Result<(), i32>> {
    Ok(vfd_core::seam_rmdir(path))
}

/// `unlink(path)`. Seam adapter for the tblspc-fs `unlink`.
pub fn seam_unlink(path: &str) -> PgResult<Result<(), i32>> {
    let cpath = path_cstring(path);
    // SAFETY: cpath is NUL-terminated.
    if unsafe { libc::unlink(cpath.as_ptr()) } < 0 {
        Ok(Err(errno_now()))
    } else {
        Ok(Ok(()))
    }
}
