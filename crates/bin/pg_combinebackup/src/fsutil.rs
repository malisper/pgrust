//! Frontend filesystem helpers pg_combinebackup relies on from src/common
//! and src/port: file_perm.c (SetDataDirectoryCreatePerm and the create-mode
//! globals), pg_check_dir, pg_mkdir_p, get_dirent_type, canonicalize_path,
//! and the recursive fsync used by sync_pgdata's fsync method.

use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::flog::{errno_message, pg_fatal};

/* C: file_perm.h defaults (owner-only). */
const PG_FILE_MODE_OWNER: u32 = 0o600;
const PG_DIR_MODE_OWNER: u32 = 0o700;
const PG_FILE_MODE_GROUP: u32 = 0o640;
const PG_DIR_MODE_GROUP: u32 = 0o750;

static FILE_CREATE_MODE: AtomicU32 = AtomicU32::new(PG_FILE_MODE_OWNER);
static DIR_CREATE_MODE: AtomicU32 = AtomicU32::new(PG_DIR_MODE_OWNER);

/// C: SetDataDirectoryCreatePerm (common/file_perm.c) — group access iff the
/// data directory mode has full group read+execute.
pub fn set_data_directory_create_perm(data_dir_mode: u32) {
    if (data_dir_mode & PG_DIR_MODE_GROUP) == PG_DIR_MODE_GROUP {
        FILE_CREATE_MODE.store(PG_FILE_MODE_GROUP, Ordering::Relaxed);
        DIR_CREATE_MODE.store(PG_DIR_MODE_GROUP, Ordering::Relaxed);
    } else {
        FILE_CREATE_MODE.store(PG_FILE_MODE_OWNER, Ordering::Relaxed);
        DIR_CREATE_MODE.store(PG_DIR_MODE_OWNER, Ordering::Relaxed);
    }
}

pub fn pg_file_create_mode() -> u32 {
    FILE_CREATE_MODE.load(Ordering::Relaxed)
}

pub fn pg_dir_create_mode() -> u32 {
    DIR_CREATE_MODE.load(Ordering::Relaxed)
}

/// C: pg_check_dir (src/port/pgcheckdir.c), collapsed to the distinctions
/// pg_combinebackup acts on: 0 = does not exist, 1 = exists and empty,
/// 4 = exists and not empty (C's 2/3/4 are all fatal for us), -1 = error.
pub fn pg_check_dir(dir: &Path) -> i32 {
    match std::fs::read_dir(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(_) => -1,
        Ok(mut entries) => {
            if entries.next().is_some() {
                4
            } else {
                1
            }
        }
    }
}

/// C: mkdir with pg_dir_create_mode.
pub fn mkdir_mode(path: &Path) -> std::io::Result<()> {
    let c = cstring(path);
    // SAFETY: NUL-terminated path.
    if unsafe { libc::mkdir(c.as_ptr(), pg_dir_create_mode() as libc::mode_t) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// C: pg_mkdir_p (mkdir -p with pg_dir_create_mode). Returns Ok if the leaf
/// now exists as a directory.
pub fn pg_mkdir_p(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.is_dir() {
            pg_mkdir_p(parent)?;
        }
    }
    match mkdir_mode(path) {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::EEXIST) && path.is_dir() => Ok(()),
        Err(e) => Err(e),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PgFileType {
    Reg,
    Dir,
    Lnk,
    Unknown,
}

/// C: get_dirent_type(..., look_through_symlinks=false, PG_LOG_ERROR). On a
/// stat failure the C caller logs and exits(1); we do the same here.
pub fn get_dirent_type(path: &Path) -> PgFileType {
    match std::fs::symlink_metadata(path) {
        Ok(md) => {
            let ft = md.file_type();
            if ft.is_file() {
                PgFileType::Reg
            } else if ft.is_dir() {
                PgFileType::Dir
            } else if ft.is_symlink() {
                PgFileType::Lnk
            } else {
                PgFileType::Unknown
            }
        }
        Err(e) => {
            crate::flog::log_error(&format!(
                "could not stat file \"{}\": {}",
                path.display(),
                errno_message(&e)
            ));
            crate::flog::exit_program(1)
        }
    }
}

/// C: canonicalize_path (src/port/path.c), for the Unix absolute paths this
/// tool compares (-T arguments and tablespace symlink targets): collapse
/// duplicate separators, strip trailing separators, drop "." components and
/// resolve ".." lexically.
pub fn canonicalize_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for comp in path.split('/') {
        match comp {
            "" | "." => {}
            ".." => {
                if let Some(last) = out.last() {
                    if *last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !absolute {
                    out.push("..");
                }
            }
            c => out.push(c),
        }
    }
    let joined = out.join("/");
    if absolute {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

/// C: slurp_file (pg_combinebackup.c) — whole file into memory with a length
/// cap; error message identity pinned.
pub fn slurp_file(path: &Path, maxlen: u64) -> Vec<u8> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            path.display(),
            errno_message(&e)
        ),
    };
    let st_size = match f.metadata() {
        Ok(md) => md.len(),
        Err(e) => pg_fatal!(
            "could not stat file \"{}\": {}",
            path.display(),
            errno_message(&e)
        ),
    };
    if st_size > maxlen {
        pg_fatal!("file \"{}\" is too large", path.display());
    }
    let mut buf = Vec::with_capacity(st_size as usize);
    use std::io::Read;
    match f.by_ref().take(st_size).read_to_end(&mut buf) {
        Ok(n) if n as u64 == st_size => {}
        Ok(n) => pg_fatal!(
            "could not read file \"{}\": read {} of {}",
            path.display(),
            n,
            st_size
        ),
        Err(e) => pg_fatal!(
            "could not read file \"{}\": {}",
            path.display(),
            errno_message(&e)
        ),
    }
    buf
}

/// C: sync_pgdata(..., DATA_DIR_SYNC_METHOD_FSYNC) — recursively fsync every
/// file and directory under `dir`. I/O errors are logged (as in
/// fsync_fname's non-fatal frontend variant) but do not abort.
pub fn fsync_dir_recurse(dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            match get_dirent_type(&p) {
                PgFileType::Dir => fsync_dir_recurse(&p),
                PgFileType::Reg => fsync_fname(&p, false),
                _ => {}
            }
        }
    }
    fsync_fname(dir, true);
}

/// C: fsync_fname (frontend flavor: logs errors, does not exit).
fn fsync_fname(path: &Path, isdir: bool) {
    let c = cstring(path);
    let flags = if isdir { libc::O_RDONLY } else { libc::O_RDWR };
    // SAFETY: NUL-terminated path.
    let fd = unsafe { libc::open(c.as_ptr(), flags) };
    if fd < 0 {
        // Matches the frontend fsync_fname: ignore unopenable entries
        // (EACCES etc., and directories that cannot be opened read-write).
        return;
    }
    // SAFETY: fd is open and owned here.
    unsafe {
        if libc::fsync(fd) != 0 {
            let e = std::io::Error::last_os_error();
            crate::flog::log_error(&format!(
                "could not fsync file \"{}\": {}",
                path.display(),
                errno_message(&e)
            ));
        }
        libc::close(fd);
    }
}

pub fn cstring(path: &Path) -> std::ffi::CString {
    std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path contains NUL")
}
