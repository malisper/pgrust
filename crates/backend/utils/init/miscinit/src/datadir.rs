#![allow(non_snake_case)]

use elog::ereport;
use init_small::globals as g;
use types_error::{ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL};

use crate::process::{loc, ValidatePgVersion};

// make_absolute_path (port/path.c): prepend cwd and canonicalize. A
// getcwd() failure is the backend's elog(ERROR, "could not get current
// working directory: %m") (path.c:868) — at postmaster start, with no
// error-catching frame, that surfaces as the FATAL boot line, never a panic.
pub fn make_absolute_path(path: &str) -> PgResult<String> {
    let abs = if path.starts_with('/') {
        path.to_string()
    } else {
        let cwd = match std::env::current_dir() {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(e) => {
                return ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errmsg("could not get current working directory: %m")
                    .finish(ErrorLocation::new("src/port/path.c", 868, "make_absolute_path"))
                    .map(|()| String::new());
            }
        };
        format!("{cwd}/{path}")
    };
    // C's make_absolute_path finishes with canonicalize_path(): a purely
    // lexical normalizer (collapses //, /./, resolves /../ textually, strips a
    // trailing slash) — no realpath/symlink resolution and no existence check.
    Ok(pg_path::canonicalize_path(&abs))
}

pub fn SetDataDir(dir: &str) -> PgResult<()> {
    g::SetDataDir(&make_absolute_path(dir)?);
    Ok(())
}

const PG_MODE_MASK_OWNER: u32 = 0o077;
const PG_MODE_MASK_GROUP: u32 = 0o027;
const PG_DIR_MODE_OWNER: u32 = 0o700;
const PG_DIR_MODE_GROUP: u32 = 0o750;
const PG_FILE_MODE_OWNER: u32 = 0o600;
const PG_FILE_MODE_GROUP: u32 = 0o640;

// SetDataDirectoryCreatePerm (common/file_perm.c); the create-mode globals
// live in fd until that unit lands. Returns the umask value.
fn set_data_directory_create_perm(data_dir_mode: u32) -> u32 {
    if data_dir_mode & PG_DIR_MODE_GROUP == PG_DIR_MODE_GROUP {
        fd::vfd::set_pg_dir_create_mode(PG_DIR_MODE_GROUP);
        fd::vfd::set_pg_file_create_mode(PG_FILE_MODE_GROUP);
        PG_MODE_MASK_GROUP
    } else {
        fd::vfd::set_pg_dir_create_mode(PG_DIR_MODE_OWNER);
        fd::vfd::set_pg_file_create_mode(PG_FILE_MODE_OWNER);
        PG_MODE_MASK_OWNER
    }
}

pub fn checkDataDir() -> PgResult<()> {
    let data_dir = g::DataDir().expect("checkDataDir: DataDir is set");
    let cpath = std::ffi::CString::new(data_dir).expect("DataDir has no NUL");
    // SAFETY: stat fills the zeroed out-param only on rc==0, which gates reads.
    let (rc, st) = unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        (libc::stat(cpath.as_ptr(), &mut st), st)
    };
    if rc != 0 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno == libc::ENOENT {
            ereport(FATAL)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("data directory \"{data_dir}\" does not exist"))
                .finish(loc(356, "checkDataDir"))?;
        }
        ereport(FATAL)
            .with_saved_errno(errno)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not read permissions of directory \"{data_dir}\": %m"
            ))
            .finish(loc(361, "checkDataDir"))?;
    }

    if st.st_mode & libc::S_IFMT != libc::S_IFDIR {
        ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "specified data directory \"{data_dir}\" is not a directory"
            ))
            .finish(loc(369, "checkDataDir"))?;
    }

    // Essential to the two-postmasters interlock (CreateLockFile); do not weaken.
    // SAFETY: geteuid has no failure modes.
    // wasm32: WASI has no uids (C's non-POSIX arm shape — ownership check
    // skipped, like C skips it under WIN32); fd_filestat carries no owner.
    #[cfg(not(target_family = "wasm"))]
    if st.st_uid != unsafe { libc::geteuid() } {
        ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("data directory \"{data_dir}\" has wrong ownership"))
            .errhint("The server must be started by the user that owns the data directory.")
            .finish(loc(385, "checkDataDir"))?;
    }

    if st.st_mode as u32 & PG_MODE_MASK_GROUP != 0 {
        ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("data directory \"{data_dir}\" has invalid permissions"))
            .errdetail("Permissions should be u=rwx (0700) or u=rwx,g=rx (0750).")
            .finish(loc(405, "checkDataDir"))?;
    }

    let mask = set_data_directory_create_perm(st.st_mode as u32);
    // SAFETY: umask is async-signal-safe and process-global by design here.
    // wasm32: no umask on WASI (files carry no mode bits); the create-mode
    // globals above still record the owner/group decision for lock files.
    #[cfg(not(target_family = "wasm"))]
    unsafe {
        libc::umask(mask as libc::mode_t);
    }
    #[cfg(target_family = "wasm")]
    let _ = mask;
    g::set_data_directory_mode(fd::vfd::pg_dir_create_mode() as i32);

    ValidatePgVersion(data_dir)
}
