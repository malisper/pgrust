//! `fd-temp-files` — temporary-file creation and the temp-tablespace state.
//!
//! `OpenTemporaryFile[InTablespace]`, `TempTablespacePath`, the
//! `PathName{Create,Delete}Temporary{Dir,File}` family,
//! `RegisterTemporaryFile`, the temp-tablespace list
//! (`SetTempTablespaces`/`TempTablespacesAreSet`/`GetTempTablespaces`/
//! `GetNextTempTableSpace`), and the PRNG-based temp-name generation.

use std::path::Path;

use ::utils_error::ereport;
use ::types_catalog::catalog::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID};
use ::types_core::primitive::MAXPGPATH;
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{ErrorLocation, PgResult, ERROR, LOG};
use types_storage::{
    File, PG_TBLSPC_DIR, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX, TABLESPACE_VERSION_DIRECTORY,
};

use crate::vfd_core::{with_fd, FD_CLOSE_AT_EOXACT, FD_DELETE_AT_CLOSE, FD_TEMP_FILE_LIMIT};

/// Error location helper, mirroring `__FILE__`/`__func__`.
fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/storage/file/fd.c", 0, funcname)
}

/// Map the scaffold's `PgResult<File>` open result onto C's `File` sentinel:
/// fd.c's `PathNameOpenFile` returns `-1` (i.e. `file <= 0`) on a failed open
/// without raising an error, so the temp-file callers branch on `file <= 0`.
/// The VFD-cache layer surfaces that silent failure as `Err`, which we collapse
/// back to `0` here so the C control flow is preserved exactly.
fn open_or_zero(result: PgResult<File>) -> File {
    result.unwrap_or(File(0))
}

/// `OpenTemporaryFile(bool interXact)` (fd.c) — open an anonymous temp file in
/// a temp tablespace, registered for end-of-transaction (or end-of-query)
/// cleanup.
pub fn OpenTemporaryFile(inter_xact: bool) -> PgResult<File> {
    let mut file: File = File(0);

    debug_assert!(temporary_files_allowed(), "check temp file access is up");

    // Make sure the current resource owner has space for this File before we
    // open it, if we'll be registering it below. (RAII model: the VFD cache is
    // a growable Vec, so ResourceOwnerEnlarge is a no-op preallocation.)
    if !inter_xact {
        // ResourceOwnerEnlarge(CurrentResourceOwner) — no-op under RAII.
    }

    // If some temp tablespace(s) have been given to us, try to use the next
    // one.  If a given tablespace can't be found, we silently fall back to the
    // database's default tablespace.
    //
    // BUT: if the temp file is slated to outlive the current transaction, force
    // it into the database's default tablespace, so that it will not pose a
    // threat to possible tablespace drop attempts.
    if num_temp_tablespaces() > 0 && !inter_xact {
        let tblspc_oid = GetNextTempTableSpace();

        if OidIsValid(tblspc_oid) {
            file = OpenTemporaryFileInTablespace(tblspc_oid, false)?;
        }
    }

    // If not, or if tablespace is bad, create in database's default tablespace.
    // MyDatabaseTableSpace should normally be set before we get here, but just
    // in case it isn't, fall back to pg_default tablespace.
    if file.0 <= 0 {
        let my_tblspc = my_database_table_space();
        let tblspc = if my_tblspc != InvalidOid {
            my_tblspc
        } else {
            DEFAULTTABLESPACE_OID
        };
        file = OpenTemporaryFileInTablespace(tblspc, true)?;
    }

    // Mark it for deletion at close and temporary file size limit.
    with_fd(|fd| {
        fd.vfd_cache[file.0 as usize].fdstate |= FD_DELETE_AT_CLOSE | FD_TEMP_FILE_LIMIT;
    });

    // Register it with the current resource owner.
    if !inter_xact {
        RegisterTemporaryFile(file);
    }

    Ok(file)
}

/// `OpenTemporaryFileInTablespace(Oid tblspcOid, bool rejectError)` (fd.c) —
/// open a temp file in the given tablespace.
pub(crate) fn OpenTemporaryFileInTablespace(
    tblspc_oid: Oid,
    reject_error: bool,
) -> PgResult<File> {
    let tempdirpath = TempTablespacePath(tblspc_oid);

    // Generate a tempfile name that should be unique within the current
    // database instance.
    let counter = with_fd(|fd| {
        let c = fd.temp_file_counter;
        fd.temp_file_counter += 1;
        c
    });
    let tempfilepath = format!(
        "{tempdirpath}/{PG_TEMP_FILE_PREFIX}{}.{counter}",
        my_proc_pid()
    );

    // Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
    // temp file that can be reused.
    let mut file = open_or_zero(crate::vfd_io::PathNameOpenFile(
        &tempfilepath,
        o_rdwr() | o_creat() | o_trunc() | pg_binary(),
    ));
    if file.0 <= 0 {
        // We might need to create the tablespace's tempfile directory, if no one
        // has yet done so.
        //
        // Don't check for an error from MakePGDirectory; it could fail if
        // someone else just did the same thing.  If it doesn't work then we'll
        // bomb out on the second create attempt, instead.
        let _ = crate::vfd_core::seam_make_pg_directory(&tempdirpath);

        file = open_or_zero(crate::vfd_io::PathNameOpenFile(
            &tempfilepath,
            o_rdwr() | o_creat() | o_trunc() | pg_binary(),
        ));
        if file.0 <= 0 && reject_error {
            // C: elog(ERROR, "could not create temporary file \"%s\": %m", ...).
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errmsg_internal(format!(
                    "could not create temporary file \"{tempfilepath}\": %m"
                ))
                .finish(loc("OpenTemporaryFileInTablespace"))?;
        }
    }

    Ok(file)
}

/// `TempTablespacePath(char *path, Oid tablespace)` (fd.c) — render the
/// per-tablespace temp directory path.
pub fn TempTablespacePath(tablespace: Oid) -> String {
    // Identify the tempfile directory for this tablespace.
    //
    // If someone tries to specify pg_global, use pg_default instead.
    let path = if tablespace == InvalidOid
        || tablespace == DEFAULTTABLESPACE_OID
        || tablespace == GLOBALTABLESPACE_OID
    {
        format!("base/{PG_TEMP_FILES_DIR}")
    } else {
        // All other tablespaces are accessed via symlinks.
        format!("{PG_TBLSPC_DIR}/{tablespace}/{TABLESPACE_VERSION_DIRECTORY}/{PG_TEMP_FILES_DIR}")
    };
    // snprintf into a MAXPGPATH buffer truncates; reproduce that bound.
    truncate_to_maxpgpath(path)
}

/// Mirror `snprintf(path, MAXPGPATH, ...)`'s truncation at `MAXPGPATH - 1`
/// bytes (C leaves room for the NUL terminator).
fn truncate_to_maxpgpath(mut path: String) -> String {
    let limit = MAXPGPATH - 1;
    if path.len() > limit {
        // Truncate on a char boundary at or below the byte limit.
        let mut end = limit;
        while end > 0 && !path.is_char_boundary(end) {
            end -= 1;
        }
        path.truncate(end);
    }
    path
}

/// `PathNameCreateTemporaryFile(const char *path, bool error_on_failure)`
/// (fd.c).
pub fn PathNameCreateTemporaryFile(path: &str, error_on_failure: bool) -> PgResult<File> {
    debug_assert!(temporary_files_allowed(), "check temp file access is up");

    // ResourceOwnerEnlarge(CurrentResourceOwner) — no-op under RAII.

    // Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
    // temp file that can be reused.
    let file = open_or_zero(crate::vfd_io::PathNameOpenFile(
        path,
        o_rdwr() | o_creat() | o_trunc() | pg_binary(),
    ));
    if file.0 <= 0 {
        if error_on_failure {
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not create temporary file \"{path}\": %m"))
                .finish(loc("PathNameCreateTemporaryFile"))?;
        } else {
            return Ok(file);
        }
    }

    // Mark it for temp_file_limit accounting.
    with_fd(|fd| {
        fd.vfd_cache[file.0 as usize].fdstate |= FD_TEMP_FILE_LIMIT;
    });

    // Register it for automatic close.
    RegisterTemporaryFile(file);

    Ok(file)
}

/// `PathNameOpenTemporaryFile(const char *path, int mode)` (fd.c).
pub fn PathNameOpenTemporaryFile(path: &str, mode: i32) -> PgResult<File> {
    debug_assert!(temporary_files_allowed(), "check temp file access is up");

    // ResourceOwnerEnlarge(CurrentResourceOwner) — no-op under RAII.

    let open = crate::vfd_io::PathNameOpenFile(path, mode | pg_binary());
    let file = open_or_zero_ref(&open);

    // If no such file, then we don't raise an error.
    if file.0 <= 0 && last_errno() != enoent() {
        ereport(ERROR)
            .with_saved_errno(last_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open temporary file \"{path}\": %m"))
            .finish(loc("PathNameOpenTemporaryFile"))?;
    }

    if file.0 > 0 {
        // Register it for automatic close.
        RegisterTemporaryFile(file);
    }

    Ok(file)
}

/// Like [`open_or_zero`] but borrows the `PgResult` so the caller can still
/// inspect `errno` on the failure path (mirroring C's `errno` after a failed
/// `PathNameOpenFile`).
fn open_or_zero_ref(result: &PgResult<File>) -> File {
    match result {
        Ok(f) => *f,
        Err(_) => File(0),
    }
}

/// `PathNameCreateTemporaryDir(const char *basedir, const char *directory)`
/// (fd.c).
pub fn PathNameCreateTemporaryDir(basedir: &str, directory: &str) -> PgResult<()> {
    if crate::vfd_core::seam_make_pg_directory(directory) < 0 {
        if last_errno() == eexist() {
            return Ok(());
        }

        // Failed.  Try to create basedir first in case it's missing. Tolerate
        // EEXIST to close a race against another process following the same
        // algorithm.
        if crate::vfd_core::seam_make_pg_directory(basedir) < 0 && last_errno() != eexist() {
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "cannot create temporary directory \"{basedir}\": %m"
                ))
                .finish(loc("PathNameCreateTemporaryDir"))?;
        }

        // Try again.
        if crate::vfd_core::seam_make_pg_directory(directory) < 0 && last_errno() != eexist() {
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "cannot create temporary subdirectory \"{directory}\": %m"
                ))
                .finish(loc("PathNameCreateTemporaryDir"))?;
        }
    }
    Ok(())
}

/// `PathNameDeleteTemporaryDir(const char *dirname)` (fd.c).
pub fn PathNameDeleteTemporaryDir(dirname: &str) -> PgResult<()> {
    // Silently ignore missing directory.
    match std::fs::symlink_metadata(dirname) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        _ => {}
    }

    // Currently, walkdir doesn't offer a way for our passed in function to
    // maintain state.  Perhaps it should, so that we could tell the caller
    // whether this operation succeeded or failed.  Since this operation is used
    // in a cleanup path, we wouldn't actually behave differently: we'll just log
    // failures.
    crate::sync_cleanup::walkdir(
        Path::new(dirname),
        crate::sync_cleanup::WalkAction::UnlinkIfExists,
        false,
        LOG,
    )
}

/// `PathNameDeleteTemporaryFile(const char *path, bool error_on_failure)`
/// (fd.c) — returns whether the file existed.
pub fn PathNameDeleteTemporaryFile(path: &str, error_on_failure: bool) -> PgResult<bool> {
    // Get the final size for pgstat reporting.
    let (stat_errno, filesize) = match std::fs::metadata(path) {
        Ok(md) => (0, md.len()),
        Err(e) => (e.raw_os_error().unwrap_or(0), 0),
    };

    // Unlike FileClose's automatic file deletion code, we tolerate non-existence
    // to support BufFileDeleteFileSet which doesn't know how many segments it
    // has to delete until it runs out.
    if stat_errno == enoent() {
        return Ok(false);
    }

    if let Err(e) = std::fs::remove_file(path) {
        let unlink_errno = e.raw_os_error().unwrap_or(0);
        if unlink_errno != enoent() {
            ereport(if error_on_failure { ERROR } else { LOG })
                .with_saved_errno(unlink_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not unlink temporary file \"{path}\": %m"))
                .finish(loc("PathNameDeleteTemporaryFile"))?;
        }
        return Ok(false);
    }

    if stat_errno == 0 {
        ReportTemporaryFileUsage(path, filesize);
    } else {
        ereport(LOG)
            .with_saved_errno(stat_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\": %m"))
            .finish(loc("PathNameDeleteTemporaryFile"))?;
    }

    Ok(true)
}

/// `ReportTemporaryFileUsage(const char *path, off_t size)` (fd.c) — report a
/// deleted temp file's size to the stats subsystem and the log.
pub(crate) fn ReportTemporaryFileUsage(path: &str, size: u64) {
    stat_seams::pgstat_report_tempfile::call(size);

    let log_temp_files = crate::vfd_core::log_temp_files();
    if log_temp_files >= 0 && (size / 1024) >= log_temp_files as u64 {
        let _ = ereport(LOG)
            .errmsg(format!("temporary file: path \"{path}\", size {size}"))
            .finish(loc("ReportTemporaryFileUsage"));
    }
}

/// `RegisterTemporaryFile(File file)` (fd.c) — mark an open VFD as a temp file
/// to be cleaned up at end of transaction.
pub fn RegisterTemporaryFile(file: File) {
    // ResourceOwnerRememberFile(CurrentResourceOwner, file) + recording the
    // owner is the RAII ownership glue: the VFD records that it is owned by the
    // current resource owner.
    with_fd(|fd| {
        let vfd = &mut fd.vfd_cache[file.0 as usize];
        vfd.has_resowner = true;

        // Backup mechanism for closing at end of xact.
        vfd.fdstate |= FD_CLOSE_AT_EOXACT;
    });
    with_fd(|fd| fd.have_xact_temporary_files = true);
}

// ---------------------------------------------------------------------------
// Temp-tablespace list (fd.c:3100-3186 region).
// ---------------------------------------------------------------------------

/// `SetTempTablespaces(Oid *tableSpaces, int numSpaces)` (fd.c).
pub fn SetTempTablespaces(table_spaces: &[Oid]) {
    // Assert(numSpaces >= 0) — a slice length is always non-negative.
    let num_spaces = table_spaces.len();
    with_fd(|fd| {
        fd.temp_table_spaces = Some(table_spaces.to_vec());

        // Select a random starting point in the list.  This is to minimize
        // conflicts between backends that are most likely sharing the same list
        // of temp tablespaces.  Note that if we create multiple temp files in
        // the same transaction, we'll advance circularly through the list ---
        // this ensures that large temporary sort files are nicely spread across
        // all available tablespaces.
        fd.next_temp_table_space = if num_spaces > 1 {
            prng::global_prng(|state| state.u64_range(0, (num_spaces - 1) as u64)) as i32
        } else {
            0
        };
    });
}

/// `TempTablespacesAreSet(void)` (fd.c).
pub fn TempTablespacesAreSet() -> bool {
    // numTempTableSpaces >= 0; the `None` state mirrors C's `-1`.
    with_fd(|fd| fd.temp_table_spaces.is_some())
}

/// `GetTempTablespaces(Oid *tableSpaces, int numSpaces)` (fd.c) — copy out the
/// current list; returns the number copied.
pub fn GetTempTablespaces(table_spaces: &mut [Oid]) -> i32 {
    debug_assert!(TempTablespacesAreSet());
    with_fd(|fd| {
        let src = fd.temp_table_spaces.as_deref().unwrap_or(&[]);
        let mut i = 0;
        while i < src.len() && i < table_spaces.len() {
            table_spaces[i] = src[i];
            i += 1;
        }
        i as i32
    })
}

/// `GetNextTempTableSpace(void)` (fd.c) — round-robin pick.
pub fn GetNextTempTableSpace() -> Oid {
    with_fd(|fd| {
        let num = fd.temp_table_spaces.as_deref().map_or(0, <[Oid]>::len);
        if num > 0 {
            // Advance nextTempTableSpace counter with wraparound.
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

// ---------------------------------------------------------------------------
// Helpers for unported neighbors (routed through the owning unit's seam) and
// the OS open(2) flag constants.
// ---------------------------------------------------------------------------

/// `numTempTableSpaces` (fd.c) — `-1` while unset, else the list length.
fn num_temp_tablespaces() -> i32 {
    with_fd(|fd| match &fd.temp_table_spaces {
        Some(v) => v.len() as i32,
        None => -1,
    })
}

/// `MyProcPid` (globals.c) — routed through the owning unit's seam.
fn my_proc_pid() -> i32 {
    init_small_seams::my_proc_pid::call()
}

/// `MyDatabaseTableSpace` (globals.c) — routed through the owning unit's seam.
fn my_database_table_space() -> Oid {
    init_small_seams::my_database_table_space::call()
}

/// Current `errno`, routed through fd's own seam (the VFD/OS layer sets it).
fn last_errno() -> i32 {
    fd_seams::last_errno::call()
}

/// `temporary_files_allowed` (fd.c static) — set by `InitTemporaryFileAccess`.
fn temporary_files_allowed() -> bool {
    with_fd(|fd| fd.temporary_files_allowed)
}

const fn enoent() -> i32 {
    libc::ENOENT
}
const fn eexist() -> i32 {
    libc::EEXIST
}

fn o_rdwr() -> i32 {
    libc::O_RDWR
}
fn o_creat() -> i32 {
    libc::O_CREAT
}
fn o_trunc() -> i32 {
    libc::O_TRUNC
}
/// `PG_BINARY` (`port.h`) — `0` on non-Windows.
fn pg_binary() -> i32 {
    0
}
