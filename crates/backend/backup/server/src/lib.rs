//! Port of PostgreSQL's `basebackup_server` (`src/backend/backup/
//! basebackup_server.c`): the `'server'` base-backup [`Bbsink`], which stores
//! each archive (and the backup manifest) in a directory on the *server's*
//! filesystem rather than streaming it to the frontend (the `TARGET 'server'`
//! option of `BASE_BACKUP`).
//!
//! All of the file's own logic is ported here over the owned [`BbsinkOps`]
//! trait: the `bbsink_server_new` create-and-validate constructor (the
//! privilege probe sequencing, the relative-path rejection, and the directory
//! state dispatch on the `pg_check_dir` status code, each raising the file's
//! own error with the identical message text and SQLSTATE), the per-archive /
//! manifest filename construction, the buffered-write accounting, the
//! fsync-then-close on archive end, and the close-then-`durable_rename` on
//! manifest end.
//!
//! The C `bbsink_server` embeds a `bbsink base` and is cast to/from `bbsink *`;
//! this port carries the server-specific fields (`pathname`, `file`,
//! `filepos`) in [`BbsinkServer`], the [`BbsinkOps`] implementation installed
//! into the surrounding [`Bbsink`] (which owns the forwarding chain and the
//! shared working buffer). A real server-side file is a [`File`] VFD handle;
//! the crate never holds a raw descriptor.
//!
//! Cross-crate calls: the file I/O (`PathNameOpenFile`, `FileWrite`,
//! `FileSync`, `FileClose`, `FilePathName`, `durable_rename`,
//! `MakePGDirectory`) goes to `backend-storage-file-fd` directly (no cycle);
//! the transaction / privilege / current-user / directory-probe externals are
//! seamed through their owners' seam crates (`start_transaction_command`,
//! `commit_transaction_command`, `has_privs_of_role`, `get_user_id`,
//! `pg_check_dir`, `is_absolute_path`), panicking until those owners land.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use ::sink::{
    bbsink_forward_archive_contents, bbsink_forward_begin_archive,
    bbsink_forward_begin_backup, bbsink_forward_begin_manifest, bbsink_forward_cleanup,
    bbsink_forward_end_archive, bbsink_forward_end_backup, bbsink_forward_end_manifest,
    bbsink_forward_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
};
use ::fd::sync_cleanup::durable_rename;
use ::fd::vfd_io::{
    FileClose, FilePathName, FileSync, FileWriteV, PathNameOpenFile,
};
use ::fd_seams::make_pg_directory;
use ::utils_error::{ereport, errno};
use ::mcx::Mcx;
use ::types_catalog::catalog::ROLE_PG_WRITE_SERVER_FILES;
use ::types_core::primitive::{Size, TimeLineID, XLogRecPtr};
use ::types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DISK_FULL, ERRCODE_DUPLICATE_FILE,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_NAME, ERROR,
};
use ::types_storage::File;

use transam_xact_seams as xact;
use acl_seams as acl;
use miscinit_seams as miscinit;
use pgcheckdir_seams as pgcheckdir;
use port_path_seams as path;

/// The C source path used in `ereport` error locations (`__FILE__`).
const SRCFILE: &str = "src/backend/backup/basebackup_server.c";

/// `WAIT_EVENT_BASEBACKUP_WRITE` (`utils/activity/wait_event_names.txt`,
/// `WaitEventIO`): `PG_WAIT_IO | <index>`. `BASEBACKUP_WRITE` is the 6th IO
/// entry (0-based index 5).
const WAIT_EVENT_BASEBACKUP_WRITE: u32 = PG_WAIT_IO | 5;
/// `WAIT_EVENT_BASEBACKUP_SYNC` — the 5th IO entry (0-based index 4).
const WAIT_EVENT_BASEBACKUP_SYNC: u32 = PG_WAIT_IO | 4;
/// `PG_WAIT_IO` class base (`utils/wait_event.h`).
const PG_WAIT_IO: u32 = 0x0A00_0000;

/// `O_CREAT | O_EXCL | O_WRONLY | PG_BINARY`. `PG_BINARY` is `0` on non-Windows.
const OPEN_FLAGS: i32 = libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY;

/// The "no file open" sentinel, matching the C `bbsink_server.file == 0`
/// (`File` is `0`/`<= 0` when nothing is open).
const NO_FILE: File = File(0);

/// The 'server' base-backup sink (C `struct bbsink_server`).
///
/// The base-sink fields (`bbs_buffer`, `bbs_buffer_length`, `bbs_next`,
/// `bbs_state`) live in the surrounding [`Bbsink`]; this struct holds only the
/// server-specific state C keeps in the derived struct (field order preserved:
/// `pathname`, `file`, `filepos`).
pub struct BbsinkServer {
    /// Directory in which the backup is to be stored (C `char *pathname`).
    pathname: String,
    /// Currently open file, or [`NO_FILE`] (`0`) if nothing is open (C
    /// `File file`).
    file: File,
    /// Current position within the open file (C `off_t filepos`).
    filepos: i64,
}

/// Create a new 'server' [`Bbsink`] (C `bbsink_server_new`).
///
/// `next` is the successor sink; `pathname` is the absolute directory in which
/// the archives and manifest are stored. Returns the constructed server sink,
/// or the file's own error if the current user lacks `pg_write_server_files`,
/// if the path is relative, or if the target directory is non-empty or
/// inaccessible.
pub fn bbsink_server_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    pathname: String,
) -> PgResult<Box<Bbsink<'mcx>>> {
    // bbsink_server *sink = palloc0(sizeof(bbsink_server));
    // *((const bbsink_ops **) &sink->base.bbs_ops) = &bbsink_server_ops;
    // sink->pathname = pathname;
    // sink->base.bbs_next = next;
    let ops = BbsinkServer {
        pathname: pathname.clone(),
        file: NO_FILE,
        filepos: 0,
    };
    let sink = Box::new(Bbsink::new(mcx, Box::new(ops), Some(next)));

    // Replication permission is not sufficient in this case.
    xact::start_transaction_command::call()?;
    if !acl::has_privs_of_role::call(miscinit::get_user_id::call(), ROLE_PG_WRITE_SERVER_FILES)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to create backup stored on server")
            .errdetail(format!(
                "Only roles with privileges of the \"{}\" role may create a backup stored on the server.",
                "pg_write_server_files"
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(SRCFILE, 75, "bbsink_server_new")));
    }
    xact::commit_transaction_command::call()?;

    // It's not a good idea to store your backups in the same directory that
    // you're backing up.  If we allowed a relative path here, that could easily
    // happen accidentally, so we don't.  The user could still accomplish the
    // same thing by including the absolute path to $PGDATA in the pathname, but
    // that's likely an intentional bad decision rather than an accident.
    let pathname = pathname.as_str();
    if !path::is_absolute_path::call(pathname) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg("relative path not allowed for backup stored on server")
            .into_error()
            .with_error_location(ErrorLocation::new(SRCFILE, 89, "bbsink_server_new")));
    }

    match pgcheckdir::pg_check_dir::call(pathname) {
        0 => {
            // Does not exist, so create it using the same permissions we'd use
            // for a new subdirectory of the data directory itself.
            if make_pg_directory::call(pathname) < 0 {
                return Err(file_access_error(
                    errno::current_errno(),
                    format!("could not create directory \"{pathname}\": %m"),
                    102,
                    "bbsink_server_new",
                ));
            }
        }
        1 => {
            // Exists, empty.
        }
        2 | 3 | 4 => {
            // Exists, not empty.
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_FILE)
                .errmsg(format!("directory \"{pathname}\" exists but is not empty"))
                .into_error()
                .with_error_location(ErrorLocation::new(SRCFILE, 116, "bbsink_server_new")));
        }
        _ => {
            // Access problem (pg_check_dir returned -1); errno is set live.
            return Err(file_access_error(
                errno::current_errno(),
                format!("could not access directory \"{pathname}\": %m"),
                124,
                "bbsink_server_new",
            ));
        }
    }

    Ok(sink)
}

impl BbsinkServer {
    /// Open the correct output file for this archive (C
    /// `bbsink_server_begin_archive`).
    fn begin_archive_impl<'mcx>(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        debug_assert_eq!(self.file, NO_FILE);
        debug_assert_eq!(self.filepos, 0);

        // filename = psprintf("%s/%s", mysink->pathname, archive_name);
        let filename = format!("{}/{}", self.pathname, archive_name);

        // mysink->file = PathNameOpenFile(filename,
        //                                 O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
        // if (mysink->file <= 0) ereport(ERROR, ...);
        self.file = open_output_file(&filename, 149, "bbsink_server_begin_archive")?;

        bbsink_forward_begin_archive(sink, state, archive_name)
    }

    /// Write the data to the output file (C `bbsink_server_archive_contents`).
    fn archive_contents_impl<'mcx>(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        // nbytes = FileWrite(mysink->file, mysink->base.bbs_buffer, len,
        //                    mysink->filepos, WAIT_EVENT_BASEBACKUP_WRITE);
        let nbytes = write_at(
            self.file,
            self.filepos,
            sink.buffer_slice(len),
            len,
            175,
            182,
            "bbsink_server_archive_contents",
        )?;

        self.filepos += nbytes as i64;

        bbsink_forward_archive_contents(sink, state, len)
    }

    /// fsync and close the current output file (C `bbsink_server_end_archive`).
    fn end_archive_impl<'mcx>(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
    ) -> PgResult<()> {
        // We intentionally don't use data_sync_elevel here, because the server
        // shouldn't PANIC just because we can't guarantee that the backup has
        // been written down to disk.  Running recovery won't fix anything in
        // this case anyway.
        if let Err(e) = FileSync(self.file, WAIT_EVENT_BASEBACKUP_SYNC) {
            return Err(file_access_error(
                e.saved_errno().unwrap_or_else(errno::current_errno),
                format!("could not fsync file \"{}\": %m", FilePathName(self.file)),
                208,
                "bbsink_server_end_archive",
            ));
        }

        // We're done with this file now.
        let _ = FileClose(self.file);
        self.file = NO_FILE;
        self.filepos = 0;

        bbsink_forward_end_archive(sink, state)
    }

    /// Open the output file to which we will write the manifest (C
    /// `bbsink_server_begin_manifest`).
    ///
    /// Just like pg_basebackup, we write the manifest first under a temporary
    /// name and then rename it into place after fsync.  That way, if the
    /// manifest is there and under the correct name, the user can be sure that
    /// the backup completed.
    fn begin_manifest_impl<'mcx>(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
    ) -> PgResult<()> {
        debug_assert_eq!(self.file, NO_FILE);

        // tmp_filename = psprintf("%s/backup_manifest.tmp", mysink->pathname);
        let tmp_filename = format!("{}/backup_manifest.tmp", self.pathname);

        // mysink->file = PathNameOpenFile(tmp_filename,
        //                                 O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
        // if (mysink->file <= 0) ereport(ERROR, ...);
        self.file = open_output_file(&tmp_filename, 242, "bbsink_server_begin_manifest")?;

        bbsink_forward_begin_manifest(sink, state)
    }

    /// Each chunk of manifest data is written to the open file (C
    /// `bbsink_server_manifest_contents`).
    fn manifest_contents_impl<'mcx>(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        // nbytes = FileWrite(mysink->file, mysink->base.bbs_buffer, len,
        //                    mysink->filepos, WAIT_EVENT_BASEBACKUP_WRITE);
        let nbytes = write_at(
            self.file,
            self.filepos,
            sink.buffer_slice(len),
            len,
            268,
            275,
            "bbsink_server_manifest_contents",
        )?;

        self.filepos += nbytes as i64;

        bbsink_forward_manifest_contents(sink, state, len)
    }

    /// fsync the backup manifest, close the file, and then rename it into place
    /// (C `bbsink_server_end_manifest`).
    fn end_manifest_impl<'mcx>(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
    ) -> PgResult<()> {
        // We're done with this file now.
        let _ = FileClose(self.file);
        self.file = NO_FILE;

        // Rename it into place.  This also fsyncs the temporary file, so we
        // don't need to do that here.  We don't use data_sync_elevel here for
        // the same reasons as in bbsink_server_end_archive.
        let tmp_filename = format!("{}/backup_manifest.tmp", self.pathname);
        let filename = format!("{}/backup_manifest", self.pathname);
        // durable_rename(tmp_filename, filename, ERROR);
        durable_rename(&tmp_filename, &filename, ERROR)?;

        bbsink_forward_end_manifest(sink, state)
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkServer {
    /// C `bbsink_server_ops.begin_backup = bbsink_forward_begin_backup`.
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_backup(sink, state)
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        self.begin_archive_impl(sink, state, archive_name)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.archive_contents_impl(sink, state, len)
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        self.end_archive_impl(sink, state)
    }

    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        self.begin_manifest_impl(sink, state)
    }

    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.manifest_contents_impl(sink, state, len)
    }

    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        self.end_manifest_impl(sink, state)
    }

    /// C `bbsink_server_ops.end_backup = bbsink_forward_end_backup`.
    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        bbsink_forward_end_backup(sink, state, endptr, endtli)
    }

    /// C `bbsink_server_ops.cleanup = bbsink_forward_cleanup`.
    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_cleanup(sink, state)
    }
}

/* ------------------------------------------------------------------------- */
/* helpers                                                                   */
/* ------------------------------------------------------------------------- */

/// `mysink->file = PathNameOpenFile(name, O_CREAT|O_EXCL|O_WRONLY|PG_BINARY); if
/// (mysink->file <= 0) ereport(ERROR, errcode_for_file_access(), "could not
/// create file %m")`.
fn open_output_file(name: &str, lineno: i32, funcname: &'static str) -> PgResult<File> {
    match PathNameOpenFile(name, OPEN_FLAGS) {
        Ok(file) if file.0 > 0 => Ok(file),
        Ok(_) => Err(file_access_error(
            errno::current_errno(),
            format!("could not create file \"{name}\": %m"),
            lineno,
            funcname,
        )),
        Err(e) => Err(file_access_error(
            e.saved_errno().unwrap_or_else(errno::current_errno),
            format!("could not create file \"{name}\": %m"),
            lineno,
            funcname,
        )),
    }
}

/// `nbytes = FileWrite(...); if (nbytes != len) { if (nbytes < 0) ereport(...
/// could not write file %m ...); ereport(... DISK_FULL short write ...); }`.
/// On success returns the written count so the caller can advance `filepos`.
fn write_at(
    file: File,
    offset: i64,
    data: &[u8],
    len: Size,
    write_err_lineno: i32,
    short_write_lineno: i32,
    funcname: &'static str,
) -> PgResult<usize> {
    let iov = [std::io::IoSlice::new(data)];
    // C: nbytes = FileWrite(...). FileWrite returns the byte count, or a
    // negative value on a failed write. The repo's FileWriteV surfaces a hard
    // failure as Err (carrying the saved errno) and a FileAccess refusal as a
    // negative Ok; map both to C's `nbytes < 0` branch.
    let nbytes: isize = match FileWriteV(file, &iov, offset, WAIT_EVENT_BASEBACKUP_WRITE) {
        Ok(n) => n,
        Err(e) => {
            return Err(file_access_error_hint(
                e.saved_errno().unwrap_or_else(errno::current_errno),
                format!("could not write file \"{}\": %m", FilePathName(file)),
                write_err_lineno,
                funcname,
            ));
        }
    };

    if nbytes as usize != len {
        if nbytes < 0 {
            // could not write file: complain with the saved errno.
            return Err(file_access_error_hint(
                errno::current_errno(),
                format!("could not write file \"{}\": %m", FilePathName(file)),
                write_err_lineno,
                funcname,
            ));
        }
        // Short write: complain appropriately (ERRCODE_DISK_FULL).
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DISK_FULL)
            .errmsg(format!(
                "could not write file \"{}\": wrote only {} of {} bytes at offset {}",
                FilePathName(file),
                nbytes as i32,
                len as i32,
                offset as u32
            ))
            .errhint("Check free disk space.")
            .into_error()
            .with_error_location(ErrorLocation::new(SRCFILE, short_write_lineno, funcname)));
    }

    Ok(nbytes as usize)
}

/// Build the file's own `ereport(errcode_for_file_access(), errmsg("...: %m"))`
/// for a failing file/dir operation, with `errno` driving both the SQLSTATE and
/// the `%m` expansion.
fn file_access_error(errno: i32, message: String, lineno: i32, funcname: &'static str) -> PgError {
    ereport(ERROR)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(message)
        .into_error()
        .with_error_location(ErrorLocation::new(SRCFILE, lineno, funcname))
}

/// As [`file_access_error`], but also attaches the "Check free disk space."
/// hint that the write-failure call sites carry.
fn file_access_error_hint(
    errno: i32,
    message: String,
    lineno: i32,
    funcname: &'static str,
) -> PgError {
    ereport(ERROR)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(message)
        .errhint("Check free disk space.")
        .into_error()
        .with_error_location(ErrorLocation::new(SRCFILE, lineno, funcname))
}

/// This crate owns no inward seam crate (no other crate calls into
/// `basebackup_server.c` across a dependency cycle), so its installer is empty.
/// Wired into `seams-init::init_all()` for uniformity and to satisfy the
/// recurrence guard.
pub fn init_seams() {}
