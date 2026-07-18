use ::elog::ereport;
use ::types_core::{InvalidOid, Oid, OidIsValid, MAXPGPATH};
use ::types_error::{PgResult, ERROR, LOG};
use ::types_storage::{
    File, PG_TBLSPC_DIR, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX, TABLESPACE_VERSION_DIRECTORY,
};

use crate::vfd::{
    self, get_errno, loc, resowner, with_fd, FdState, FD_CLOSE_AT_EOXACT, FD_DELETE_AT_CLOSE,
    FD_TEMP_FILE_LIMIT,
};

// pg_tablespace_d.h, verified against catalog/pg_tablespace.dat (18.3).
pub const DEFAULTTABLESPACE_OID: Oid = 1663;
pub const GLOBALTABLESPACE_OID: Oid = 1664;

const TEMP_OPEN_FLAGS: i32 = libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC;

pub fn OpenTemporaryFile(inter_xact: bool) -> PgResult<File> {
    let mut file = File(0);

    debug_assert!(with_fd(|fd| fd.temporary_files_allowed), "temp file access not up");

    // Make sure the current resource owner has space for this File before we
    // open it, if we'll be registering it below.
    if !inter_xact {
        resowner::resource_owner_enlarge(resowner::current_resource_owner());
    }

    // Prefer the next temp tablespace, but force interXact files into the
    // database default so they don't block tablespace drops.
    if with_fd(num_temp_tablespaces) > 0 && !inter_xact {
        let tblspc = GetNextTempTableSpace();
        if OidIsValid(tblspc) {
            file = OpenTemporaryFileInTablespace(tblspc, false)?;
        }
    }

    if file.0 <= 0 {
        let my_tblspc = init_small::globals::MyDatabaseTableSpace();
        let tblspc = if OidIsValid(my_tblspc) {
            my_tblspc
        } else {
            DEFAULTTABLESPACE_OID
        };
        file = OpenTemporaryFileInTablespace(tblspc, true)?;
    }

    with_fd(|fd| {
        fd.vfd_cache[file.0 as usize].fdstate |= FD_DELETE_AT_CLOSE | FD_TEMP_FILE_LIMIT;
    });

    if !inter_xact {
        with_fd(|fd| RegisterTemporaryFile(fd, file));
    }

    Ok(file)
}

pub fn TempTablespacePath(tablespace: Oid) -> String {
    let path = if tablespace == InvalidOid
        || tablespace == DEFAULTTABLESPACE_OID
        || tablespace == GLOBALTABLESPACE_OID
    {
        format!("base/{PG_TEMP_FILES_DIR}")
    } else {
        format!("{PG_TBLSPC_DIR}/{tablespace}/{TABLESPACE_VERSION_DIRECTORY}/{PG_TEMP_FILES_DIR}")
    };
    truncate_to_maxpgpath(path)
}

// snprintf(path, MAXPGPATH, ...) truncates at MAXPGPATH - 1 bytes.
fn truncate_to_maxpgpath(mut path: String) -> String {
    let limit = MAXPGPATH - 1;
    if path.len() > limit {
        let mut end = limit;
        while end > 0 && !path.is_char_boundary(end) {
            end -= 1;
        }
        path.truncate(end);
    }
    path
}

pub(crate) fn OpenTemporaryFileInTablespace(tblspc_oid: Oid, reject_error: bool) -> PgResult<File> {
    let tempdirpath = TempTablespacePath(tblspc_oid);

    let counter = with_fd(|fd| {
        let c = fd.temp_file_counter;
        fd.temp_file_counter += 1;
        c
    });
    let tempfilepath = truncate_to_maxpgpath(format!(
        "{tempdirpath}/{PG_TEMP_FILE_PREFIX}{}.{counter}",
        init_small::globals::MyProcPid()
    ));

    // No O_EXCL: an orphaned temp file may be reused.
    let mut file = crate::io::PathNameOpenFile(&tempfilepath, TEMP_OPEN_FLAGS)?;
    if file.0 <= 0 {
        // The tablespace's tempfile directory may not exist yet. Ignore
        // MakePGDirectory failure (a racing backend may have created it); the
        // second open attempt reports any real problem.
        let _ = vfd::MakePGDirectory(&tempdirpath);

        file = crate::io::PathNameOpenFile(&tempfilepath, TEMP_OPEN_FLAGS)?;
        if file.0 <= 0 && reject_error {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errmsg_internal(format!(
                    "could not create temporary file \"{tempfilepath}\": %m"
                ))
                .finish(loc("OpenTemporaryFileInTablespace"))?;
        }
    }

    Ok(file)
}

pub fn PathNameCreateTemporaryFile(path: &str, error_on_failure: bool) -> PgResult<File> {
    debug_assert!(with_fd(|fd| fd.temporary_files_allowed), "temp file access not up");

    resowner::resource_owner_enlarge(resowner::current_resource_owner());

    let file = crate::io::PathNameOpenFile(path, TEMP_OPEN_FLAGS)?;
    if file.0 <= 0 {
        if error_on_failure {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not create temporary file \"{path}\": %m"))
                .finish(loc("PathNameCreateTemporaryFile"))?;
        }
        return Ok(file);
    }

    with_fd(|fd| {
        fd.vfd_cache[file.0 as usize].fdstate |= FD_TEMP_FILE_LIMIT;
        RegisterTemporaryFile(fd, file);
    });

    Ok(file)
}

pub fn PathNameOpenTemporaryFile(path: &str, mode: i32) -> PgResult<File> {
    debug_assert!(with_fd(|fd| fd.temporary_files_allowed), "temp file access not up");

    resowner::resource_owner_enlarge(resowner::current_resource_owner());

    let file = crate::io::PathNameOpenFile(path, mode)?;

    // If no such file, then we don't raise an error.
    if file.0 <= 0 && get_errno() != libc::ENOENT {
        ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open temporary file \"{path}\": %m"))
            .finish(loc("PathNameOpenTemporaryFile"))?;
    }

    if file.0 > 0 {
        with_fd(|fd| RegisterTemporaryFile(fd, file));
    }

    Ok(file)
}

pub fn PathNameCreateTemporaryDir(basedir: &str, directory: &str) -> PgResult<()> {
    if vfd::MakePGDirectory(directory) < 0 {
        if get_errno() == libc::EEXIST {
            return Ok(());
        }

        // Try creating basedir first; tolerate EEXIST to close the race against
        // another process following the same algorithm.
        if vfd::MakePGDirectory(basedir) < 0 && get_errno() != libc::EEXIST {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("cannot create temporary directory \"{basedir}\": %m"))
                .finish(loc("PathNameCreateTemporaryDir"))?;
        }

        if vfd::MakePGDirectory(directory) < 0 && get_errno() != libc::EEXIST {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "cannot create temporary subdirectory \"{directory}\": %m"
                ))
                .finish(loc("PathNameCreateTemporaryDir"))?;
        }
    }
    Ok(())
}

pub fn PathNameDeleteTemporaryDir(dirname: &str) -> PgResult<()> {
    // Silently ignore a missing directory.
    let mut info = vfs::FileInfo::zeroed();
    if vfs::lstat(&vfd::cpath(dirname), &mut info) != 0 && get_errno() == libc::ENOENT {
        return Ok(());
    }

    // Used on cleanup paths: failures are logged, not propagated.
    crate::sync::walkdir(dirname, crate::sync::WalkAction::UnlinkIfExists, false, LOG)
}

pub fn PathNameDeleteTemporaryFile(path: &str, error_on_failure: bool) -> PgResult<bool> {
    let cp = vfd::cpath(path);
    let mut md = vfs::FileInfo::zeroed();
    let (stat_errno, filesize) = if vfs::stat(&cp, &mut md) == 0 {
        (0, md.size as u64)
    } else {
        (get_errno(), 0)
    };

    // Unlike FileClose's deletion, tolerate non-existence: BufFileDeleteFileSet
    // probes segments until it runs out.
    if stat_errno == libc::ENOENT {
        return Ok(false);
    }

    if vfs::unlink(&cp) != 0 {
        let unlink_errno = get_errno();
        if unlink_errno != libc::ENOENT {
            ereport(if error_on_failure { ERROR } else { LOG })
                .with_saved_errno(unlink_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not unlink temporary file \"{path}\": %m"))
                .finish(loc("PathNameDeleteTemporaryFile"))?;
        }
        return Ok(false);
    }

    if stat_errno == 0 {
        ReportTemporaryFileUsage(path, filesize)?;
    } else {
        ereport(LOG)
            .with_saved_errno(stat_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\": %m"))
            .finish(loc("PathNameDeleteTemporaryFile"))?;
    }

    Ok(true)
}

pub(crate) fn ReportTemporaryFileUsage(path: &str, size: u64) -> PgResult<()> {
    pgstat_seams::pgstat_report_tempfile::call(size);

    let log_temp_files = guc_tables::vars::log_temp_files.read();
    if log_temp_files >= 0 && (size / 1024) >= log_temp_files as u64 {
        ereport(LOG)
            .errmsg(format!("temporary file: path \"{path}\", size {size}"))
            .finish(loc("ReportTemporaryFileUsage"))?;
    }
    Ok(())
}

pub(crate) fn RegisterTemporaryFile(fd: &mut FdState, file: File) {
    let owner = resowner::current_resource_owner();
    resowner::resource_owner_remember_file(owner, file);
    fd.vfd_cache[file.0 as usize].resowner = owner;

    fd.vfd_cache[file.0 as usize].fdstate |= FD_CLOSE_AT_EOXACT;
    fd.have_xact_temporary_files = true;
}

pub fn SetTempTablespaces(table_spaces: &[Oid]) {
    let num_spaces = table_spaces.len();
    // C picks a pg_prng random start to spread sort files across tablespaces.
    let start = if num_spaces > 1 {
        pg_prng::global_prng(|p| p.u64_range(0, num_spaces as u64 - 1)) as i32
    } else {
        0
    };
    with_fd(|fd| {
        fd.temp_table_spaces = Some(table_spaces.to_vec());
        fd.next_temp_table_space = start;
    });
}

pub fn TempTablespacesAreSet() -> bool {
    with_fd(|fd| fd.temp_table_spaces.is_some())
}

pub fn GetTempTablespaces(table_spaces: &mut [Oid]) -> i32 {
    debug_assert!(TempTablespacesAreSet());
    with_fd(|fd| {
        let src = fd.temp_table_spaces.as_deref().unwrap_or(&[]);
        debug_assert!(src.len() <= table_spaces.len());
        let n = src.len().min(table_spaces.len());
        table_spaces[..n].copy_from_slice(&src[..n]);
        n as i32
    })
}

pub fn GetNextTempTableSpace() -> Oid {
    with_fd(|fd| {
        let num = fd.temp_table_spaces.as_deref().map_or(0, <[Oid]>::len);
        if num > 0 {
            fd.next_temp_table_space += 1;
            if fd.next_temp_table_space as usize >= num {
                fd.next_temp_table_space = 0;
            }
            fd.temp_table_spaces.as_ref().unwrap()[fd.next_temp_table_space as usize]
        } else {
            InvalidOid
        }
    })
}

pub(crate) fn num_temp_tablespaces(fd: &mut FdState) -> i32 {
    match &fd.temp_table_spaces {
        Some(v) => v.len() as i32,
        None => -1,
    }
}
