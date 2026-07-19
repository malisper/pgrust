//! Port of `basebackup_server.c` (PostgreSQL 18.3): store basebackup archives
//! on the server. A forwarding bbsink that additionally writes every archive
//! (and the manifest, via a durable-rename dance) into a server directory.

use ::elog::ereport;
use ::mcx::Mcx;
use ::sink::{
    bbsink_forward_archive_contents, bbsink_forward_begin_archive, bbsink_forward_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_forward_manifest_contents,
    Bbsink, BbsinkOps, BbsinkState,
};
use ::types_core::primitive::Size;
use ::types_core::{Oid, TimeLineID, XLogRecPtr};
use ::types_error::{
    PgResult, ERRCODE_DISK_FULL, ERRCODE_DUPLICATE_FILE, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_NAME, ERROR,
};
use ::types_storage::File;

use crate::loc;

// pg_authid.dat: pg_write_server_files.
const ROLE_PG_WRITE_SERVER_FILES: Oid = 4570;

// wait_event_names.txt WaitEventIO section: AIO_IO_COMPLETION(0),
// AIO_IO_URING_SUBMIT(1), AIO_IO_URING_EXECUTION(2), BASEBACKUP_READ(3),
// BASEBACKUP_SYNC(4), BASEBACKUP_WRITE(5).
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_BASEBACKUP_SYNC: u32 = PG_WAIT_IO + 4;
const WAIT_EVENT_BASEBACKUP_WRITE: u32 = PG_WAIT_IO + 5;

struct ServerOps {
    /// Directory in which the backup is to be stored.
    pathname: String,
    /// Currently open file (None if nothing open).
    file: Option<File>,
    /// Current file position.
    filepos: i64,
}

/// bbsink_server_new: permission check, pathname validation, directory
/// creation, then the forwarding sink.
pub fn bbsink_server_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    pathname: &str,
) -> PgResult<Box<Bbsink<'mcx>>> {
    // Replication permission is not sufficient in this case.
    xact_seams::start_transaction_command::call()?;
    if !acl_seams::has_privs_of_role::call(
        miscinit_seams::get_user_id::call(),
        ROLE_PG_WRITE_SERVER_FILES,
    )? {
        ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to create backup stored on server")
            .errdetail(
                "Only roles with privileges of the \"pg_write_server_files\" role may create a backup stored on the server.",
            )
            .finish(loc("bbsink_server_new"))?;
    }
    xact_seams::commit_transaction_command::call()?;

    // It's not a good idea to store your backups in the directory you're
    // backing up, so relative paths are not allowed.
    if !pathname.starts_with('/') {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg("relative path not allowed for backup stored on server")
            .finish(loc("bbsink_server_new"))?;
    }

    match pg_check_dir(pathname) {
        0 => {
            // Does not exist: create it with the same permissions we'd use
            // for a new subdirectory of the data directory itself.
            if fd::MakePGDirectory(pathname) < 0 {
                let e = std::io::Error::last_os_error();
                ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not create directory \"{pathname}\": {e}"))
                    .finish(loc("bbsink_server_new"))?;
            }
        }
        1 => {} // Exists, empty.
        2..=4 => {
            // Exists, not empty.
            ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_FILE)
                .errmsg(format!("directory \"{pathname}\" exists but is not empty"))
                .finish(loc("bbsink_server_new"))?;
        }
        _ => {
            // Access problem.
            let e = std::io::Error::last_os_error();
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not access directory \"{pathname}\": {e}"))
                .finish(loc("bbsink_server_new"))?;
        }
    }

    Ok(Box::new(Bbsink::new(
        mcx,
        Box::new(ServerOps {
            pathname: pathname.to_string(),
            file: None,
            filepos: 0,
        }),
        Some(next),
    )))
}

/// pg_check_dir (src/common/pgcheckdir.c): 0 = does not exist, 1 = empty,
/// 2 = only dot files, 3 = contains a mount point, 4 = not empty, -1 = error.
fn pg_check_dir(dir: &str) -> i32 {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::NotFound {
                0
            } else {
                -1
            };
        }
    };
    let mut result = 1;
    let mut dot_found = false;
    let mut mount_found = false;
    for entry in rd {
        let name = match entry {
            Ok(de) => de.file_name(),
            Err(_) => return -1,
        };
        let name = name.to_string_lossy().into_owned();
        if name == "." || name == ".." {
            continue;
        }
        if name.starts_with('.') {
            dot_found = true;
        } else if name == "lost+found" {
            mount_found = true;
        } else {
            return 4;
        }
    }
    if mount_found {
        result = 3;
    } else if dot_found {
        result = 2;
    }
    result
}

impl ServerOps {
    fn write_chunk(&mut self, sink: &Bbsink<'_>, len: Size, func: &'static str) -> PgResult<()> {
        let file = self.file.expect("server sink file must be open");
        let nbytes = fd::FileWrite(
            file,
            sink.buffer_slice(len),
            self.filepos,
            WAIT_EVENT_BASEBACKUP_WRITE,
        )?;
        if nbytes < 0 || nbytes as usize != len {
            let path = fd::FilePathName(file);
            if nbytes < 0 {
                let e = std::io::Error::last_os_error();
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not write file \"{path}\": {e}"))
                    .errhint("Check free disk space.")
                    .finish(loc(func));
            }
            // Short write: complain appropriately.
            return ereport(ERROR)
                .errcode(ERRCODE_DISK_FULL)
                .errmsg(format!(
                    "could not write file \"{path}\": wrote only {nbytes} of {len} bytes at offset {}",
                    self.filepos
                ))
                .errhint("Check free disk space.")
                .finish(loc(func));
        }
        self.filepos += nbytes as i64;
        Ok(())
    }

    fn open_exclusive(&mut self, filename: &str, func: &'static str) -> PgResult<()> {
        debug_assert!(self.file.is_none());
        let file = fd::PathNameOpenFile(
            filename,
            libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY,
        )?;
        if file.0 <= 0 {
            let e = std::io::Error::last_os_error();
            return ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not create file \"{filename}\": {e}"))
                .finish(loc(func));
        }
        self.file = Some(file);
        Ok(())
    }
}

impl<'mcx> BbsinkOps<'mcx> for ServerOps {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_backup(sink, state)
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        debug_assert!(self.filepos == 0);
        let filename = format!("{}/{archive_name}", self.pathname);
        self.open_exclusive(&filename, "bbsink_server_begin_archive")?;
        bbsink_forward_begin_archive(sink, state, archive_name)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.write_chunk(sink, len, "bbsink_server_archive_contents")?;
        bbsink_forward_archive_contents(sink, state, len)
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Intentionally not data_sync_elevel: the server shouldn't PANIC just
        // because the backup couldn't be made durable.
        let file = self.file.expect("server sink file must be open");
        if fd::FileSync(file, WAIT_EVENT_BASEBACKUP_SYNC)? < 0 {
            let path = fd::FilePathName(file);
            let e = std::io::Error::last_os_error();
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{path}\": {e}"))
                .finish(loc("bbsink_server_end_archive"))?;
        }
        fd::FileClose(file)?;
        self.file = None;
        self.filepos = 0;
        bbsink_forward_end_archive(sink, state)
    }

    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Written under a temporary name, renamed into place after fsync, so
        // a manifest under the correct name implies a complete backup.
        let tmp_filename = format!("{}/backup_manifest.tmp", self.pathname);
        self.open_exclusive(&tmp_filename, "bbsink_server_begin_manifest")?;
        bbsink_forward_begin_manifest(sink, state)
    }

    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.write_chunk(sink, len, "bbsink_server_manifest_contents")?;
        bbsink_forward_manifest_contents(sink, state, len)
    }

    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        let file = self.file.take().expect("server sink file must be open");
        fd::FileClose(file)?;
        self.filepos = 0;

        // Rename into place; durable_rename fsyncs the temporary file. Not
        // data_sync_elevel for the same reasons as end_archive.
        let tmp_filename = format!("{}/backup_manifest.tmp", self.pathname);
        let filename = format!("{}/backup_manifest", self.pathname);
        fd::durable_rename(&tmp_filename, &filename, ERROR)?;

        bbsink_forward_end_manifest(sink, state)
    }

    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        bbsink_forward_end_backup(sink, state, endptr, endtli)
    }

    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_cleanup(sink, state)
    }
}
