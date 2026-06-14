//! Inward-seam adapters that this unit owns but that don't belong to a single
//! family module — they marshal a consumer-side fd.c I/O idiom (an
//! `AllocateFile`+`fwrite`+`FreeFile` store, an `OpenTransientFile`+`read`
//! load, a directory walk, etc.) onto the already-ported fd.c logic.
//!
//! Each adapter is a thin wrapper: it builds paths / flags and delegates to the
//! family functions; no I/O algorithm lives here that isn't a direct mirror of
//! the C the calling site issues against fd.c.
//!
//! All of these are installed by [`crate::init_seams`].

use std::os::fd::FromRawFd;
use std::path::Path;

use types_error::{ErrorLevel, PgError, PgResult, ERROR, FATAL, LOG};

use backend_storage_file_fd_seams::{
    CreateEmptyFileOutcome, PgFileStream, RelmapReadOutcome, RelmapWriteOutcome,
};

use crate::{allocated_desc, sync_cleanup, vfd_core, vfd_io};

const SRCFILE: &str = "../src/backend/storage/file/fd.c";

/// `RELMAPPER_FILENAME` (relmapper.c).
const RELMAPPER_FILENAME: &str = "pg_filenode.map";
/// `RELMAPPER_TEMP_FILENAME` (relmapper.c).
const RELMAPPER_TEMP_FILENAME: &str = "pg_filenode.map.tmp";
/// `sizeof(RelMapFile)` = magic(4) + num_mappings(4) + mappings(MAX_MAPPINGS*8)
/// + crc(4), with MAX_MAPPINGS = 64. Matches relmapper's `SIZEOF_RELMAPFILE`.
const SIZEOF_RELMAPFILE: usize = 4 + 4 + (64 * 8) + 4;

/// `PG_BINARY` (`c.h`) — 0 on non-Windows.
const PG_BINARY: i32 = 0;

fn errno_now() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn strerror(errno: i32) -> String {
    // SAFETY: strerror returns a pointer to a NUL-terminated static string we
    // copy out immediately.
    unsafe {
        let ptr = libc::strerror(errno);
        if ptr.is_null() {
            format!("unrecognized error {errno}")
        } else {
            std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// Build an `errcode_for_file_access()` `PgError` at `elevel` from a pre-`%m`
/// message and an errno (mirrors `ereport(elevel, (errcode_for_file_access(),
/// errmsg(...)))`).
fn file_access_error(elevel: ErrorLevel, errno: i32, message: String) -> PgError {
    let sqlstate = backend_utils_error_seams::sqlstate_for_file_access::call(errno);
    let msg = message.replace("%m", &strerror(errno));
    PgError::new(elevel, msg)
        .with_sqlstate(sqlstate)
        .with_saved_errno(errno)
        .with_error_location(types_error::ErrorLocation::new(SRCFILE, 0, ""))
}

// ===========================================================================
// snapmgr.c — `AllocateFile`+`fwrite`/`fread`+`FreeFile` stores/loads.
// ===========================================================================

/// `allocate_file_write` — `AllocateFile(path, PG_BINARY_W)` + `fwrite` +
/// `FreeFile`, the OS-coupled half of snapmgr's `ExportSnapshot`. The caller
/// owns the `.tmp`+rename ordering.
pub fn allocate_file_write(path: &str, bytes: &[u8]) -> PgResult<()> {
    // if (!(f = AllocateFile(pathtmp, PG_BINARY_W))) ereport(ERROR, ...)
    let index = match allocated_desc::AllocateFile(Path::new(path), "wb") {
        Ok(index) => index,
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(0);
            return Err(file_access_error(
                ERROR,
                errno,
                format!("could not create file \"{path}\": %m"),
            ));
        }
    };

    // if (fwrite(buf.data, buf.len, 1, f) != 1) ereport(ERROR, ...)
    if let Some(errno) = allocated_desc::stream_write(index, bytes) {
        let _ = allocated_desc::FreeFile(index);
        return Err(file_access_error(
            ERROR,
            errno,
            format!("could not write to file \"{path}\": %m"),
        ));
    }

    // if (FreeFile(f)) ereport(ERROR, ...)
    allocated_desc::FreeFile(index)
}

/// `create_empty_file` — `AllocateFile(path, "w")` immediately followed by
/// `FreeFile` (xlogarchive.c's `.ready`/`.done` status-file idiom). Never
/// throws: the open-failure and the deferred FreeFile-failure are returned as
/// [`CreateEmptyFileOutcome`] variants carrying `errno`, so the caller can emit
/// its own non-throwing `ereport(LOG)` and continue.
pub fn create_empty_file(path: &str) -> CreateEmptyFileOutcome {
    // fd = AllocateFile(archiveStatusPath, "w"); if (fd == NULL) { LOG; return }
    let index = match allocated_desc::AllocateFile(Path::new(path), "w") {
        Ok(index) => index,
        Err(e) => {
            return CreateEmptyFileOutcome::CreateFailed(e.saved_errno().unwrap_or(0));
        }
    };

    // if (FreeFile(fd)) { LOG; return }
    match allocated_desc::FreeFile(index) {
        Ok(()) => CreateEmptyFileOutcome::Ok,
        Err(e) => CreateEmptyFileOutcome::WriteFailed(e.saved_errno().unwrap_or(0)),
    }
}

/// `allocate_file_read` — `AllocateFile(path, PG_BINARY_R)` + `fstat` + `fread`
/// + `FreeFile`, the OS-coupled half of snapmgr's `ImportSnapshot`. Returns
/// `Ok(None)` when the file is absent (`errno == ENOENT`), which snapmgr maps to
/// its own "snapshot does not exist" error.
pub fn allocate_file_read(path: &str) -> PgResult<Option<Vec<u8>>> {
    // f = AllocateFile(path, PG_BINARY_R); if (!f) { if (errno == ENOENT)
    // return None-signal; else ereport(ERROR) }
    let index = match allocated_desc::AllocateFile(Path::new(path), "rb") {
        Ok(index) => index,
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(0);
            if errno == libc::ENOENT {
                return Ok(None);
            }
            return Err(file_access_error(
                ERROR,
                errno,
                format!("could not open file \"{path}\" for reading: %m"),
            ));
        }
    };

    // fstat + fread the whole file (read_to_end gives the same image).
    let bytes = match allocated_desc::stream_read_all(index) {
        Ok(bytes) => bytes,
        Err(errno) => {
            let _ = allocated_desc::FreeFile(index);
            return Err(file_access_error(
                ERROR,
                errno,
                format!("could not read file \"{path}\": %m"),
            ));
        }
    };

    allocated_desc::FreeFile(index)?;
    Ok(Some(bytes))
}

/// `read_dir_names_logged` — `AllocateDir(dir)` + `ReadDirExtended(.., LOG)` +
/// `FreeDir`. Read problems are logged at LOG and skipped (snapmgr's
/// `DeleteAllExportedSnapshotFiles` runs in the startup process, where ERROR
/// would block startup), so this returns whatever names it could read and never
/// raises ERROR.
pub fn read_dir_names_logged(dir: &str) -> Vec<String> {
    let mut names = Vec::new();
    // AllocateDir failure surfaces from the first ReadDirExtended(LOG), which
    // logs and returns NULL; the loop then never executes.
    let dirh = match allocated_desc::AllocateDir(Path::new(dir)) {
        Ok(d) => d,
        Err(_) => return names,
    };
    loop {
        match allocated_desc::ReadDirExtended(dirh, Path::new(dir), LOG) {
            Ok(Some(ent)) => {
                if ent.d_name == "." || ent.d_name == ".." {
                    continue;
                }
                names.push(ent.d_name);
            }
            // End of directory, or a read error already logged at LOG.
            Ok(None) | Err(_) => break,
        }
    }
    let _ = allocated_desc::FreeDir(dirh);
    names
}

// ===========================================================================
// relmapper.c — `OpenTransientFile`+`read`/`write`+`CloseTransientFile` and the
// durable rename. The relmapper algorithm (magic/CRC validation, WAL, sinval)
// stays in-crate; only these raw load/store steps are fd-owned.
// ===========================================================================

fn relmap_path(dbpath: &str, file: &str) -> String {
    format!("{dbpath}/{file}")
}

/// `relmap_read_file` — load unit behind `read_relmap_file`: open
/// `dbpath/pg_filenode.map` read-only, `read()` `sizeof(RelMapFile)` bytes,
/// close. Returns the raw outcome; the relmapper validates the image.
pub fn relmap_read_file(dbpath: &str) -> PgResult<RelmapReadOutcome> {
    let mapfilename = relmap_path(dbpath, RELMAPPER_FILENAME);

    // fd = OpenTransientFile(mapfilename, O_RDONLY | PG_BINARY);
    let fd = match allocated_desc::OpenTransientFile(
        Path::new(&mapfilename),
        libc::O_RDONLY | PG_BINARY,
    ) {
        Ok(fd) => fd,
        Err(e) => {
            return Ok(RelmapReadOutcome::OpenFailed {
                errno: e.saved_errno().unwrap_or(0),
            });
        }
    };

    // r = read(fd, map, sizeof(RelMapFile));
    let mut buf = vec![0u8; SIZEOF_RELMAPFILE];
    let r = transient_read_raw(fd, &mut buf);
    if r < 0 {
        let errno = errno_now();
        let _ = allocated_desc::CloseTransientFile(fd);
        return Ok(RelmapReadOutcome::ReadFailed { errno });
    }
    if (r as usize) != SIZEOF_RELMAPFILE {
        let _ = allocated_desc::CloseTransientFile(fd);
        return Ok(RelmapReadOutcome::ShortRead { got: r as i64 });
    }

    // if (CloseTransientFile(fd) != 0) ...
    match allocated_desc::CloseTransientFile(fd) {
        Ok(()) => {}
        Err(e) => {
            return Ok(RelmapReadOutcome::CloseFailed {
                errno: e.saved_errno().unwrap_or(0),
            });
        }
    }

    buf.truncate(SIZEOF_RELMAPFILE);
    Ok(RelmapReadOutcome::Ok { bytes: buf })
}

/// `relmap_write_temp` — first store step behind `write_relmap_file`: open
/// `dbpath/pg_filenode.map.tmp` (O_WRONLY|O_CREAT|O_TRUNC|PG_BINARY), `write()`
/// `bytes`, close.
pub fn relmap_write_temp(dbpath: &str, bytes: &[u8]) -> PgResult<RelmapWriteOutcome> {
    let maptempfilename = relmap_path(dbpath, RELMAPPER_TEMP_FILENAME);

    // fd = OpenTransientFile(maptempfilename, O_WRONLY|O_CREAT|O_TRUNC|PG_BINARY);
    let fd = match allocated_desc::OpenTransientFile(
        Path::new(&maptempfilename),
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | PG_BINARY,
    ) {
        Ok(fd) => fd,
        Err(e) => {
            return Ok(RelmapWriteOutcome::OpenFailed {
                errno: e.saved_errno().unwrap_or(0),
            });
        }
    };

    // if (write(fd, newmap, sizeof(RelMapFile)) != sizeof(RelMapFile)) { if
    // (errno == 0) errno = ENOSPC; ereport(ERROR, ...) }
    let w = transient_write_raw(fd, bytes);
    if w != bytes.len() as isize {
        let mut errno = errno_now();
        if errno == 0 {
            errno = libc::ENOSPC;
        }
        let _ = allocated_desc::CloseTransientFile(fd);
        return Ok(RelmapWriteOutcome::WriteFailed { errno });
    }

    // if (CloseTransientFile(fd) != 0) ereport(ERROR, ...)
    match allocated_desc::CloseTransientFile(fd) {
        Ok(()) => Ok(RelmapWriteOutcome::Ok),
        Err(e) => Ok(RelmapWriteOutcome::CloseFailed {
            errno: e.saved_errno().unwrap_or(0),
        }),
    }
}

/// `relmap_durable_rename` — final store step: `durable_rename(tmp, real,
/// ERROR)`. relmapper always passes ERROR (which becomes PANIC in a critical
/// section); a failure surfaces as `Err`.
pub fn relmap_durable_rename(dbpath: &str) -> PgResult<()> {
    let mapfilename = relmap_path(dbpath, RELMAPPER_FILENAME);
    let maptempfilename = relmap_path(dbpath, RELMAPPER_TEMP_FILENAME);
    sync_cleanup::durable_rename(&maptempfilename, &mapfilename, ERROR)
}

// ===========================================================================
// timeline.c — `AllocateFile(path, "r")` reads with FATAL on a non-ENOENT open
// failure.
// ===========================================================================

/// `read_file_or_absent` — `AllocateFile(path, "r")` + read loop, allocated in
/// `mcx`. Returns `None` when absent (`errno == ENOENT`); `FATAL` on any other
/// open failure and `ERROR` on a read failure (exactly the timeline.c contract).
pub fn read_file_or_absent<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    path: &str,
) -> PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    // fd = AllocateFile(path, "r"); if (fd == NULL) { if (errno != ENOENT)
    // ereport(FATAL, ...); else return None }
    let index = match allocated_desc::AllocateFile(Path::new(path), "r") {
        Ok(index) => index,
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(0);
            if errno != libc::ENOENT {
                return Err(file_access_error(
                    FATAL,
                    errno,
                    format!("could not open file \"{path}\": %m"),
                ));
            }
            return Ok(None);
        }
    };

    let bytes = match allocated_desc::stream_read_all(index) {
        Ok(bytes) => bytes,
        Err(errno) => {
            let _ = allocated_desc::FreeFile(index);
            return Err(file_access_error(
                ERROR,
                errno,
                format!("could not read file \"{path}\": %m"),
            ));
        }
    };
    allocated_desc::FreeFile(index)?;

    let mut out = mcx::vec_with_capacity_in(mcx, bytes.len())?;
    out.extend_from_slice(&bytes);
    Ok(Some(out))
}

/// `file_exists` — `AllocateFile(path, "r")` then `FreeFile`. `true` if it could
/// be opened, `false` on `errno == ENOENT`, `FATAL` on any other open failure.
pub fn file_exists(path: &str) -> PgResult<bool> {
    match allocated_desc::AllocateFile(Path::new(path), "r") {
        Ok(index) => {
            allocated_desc::FreeFile(index)?;
            Ok(true)
        }
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(0);
            if errno == libc::ENOENT {
                Ok(false)
            } else {
                Err(file_access_error(
                    FATAL,
                    errno,
                    format!("could not open file \"{path}\": %m"),
                ))
            }
        }
    }
}

// ===========================================================================
// copyto.c — the COPY-TO stream family. `PgFileStream(u64)` carries the
// allocated-descriptor table index; `STDOUT_STREAM` is the sentinel for the
// COPY TO STDOUT-to-server-log path.
// ===========================================================================

/// Sentinel `PgFileStream` standing for the stdio `stdout` global.
const STDOUT_STREAM: u64 = u64::MAX;

/// `open_copy_to_file` — copyto.c:952-985: `umask(S_IWGRP|S_IWOTH)`,
/// `AllocateFile(filename, PG_BINARY_W)`, restore umask, the open-failure
/// `ereport` (with the psql `\copy` hint), then the `fstat`/`S_ISDIR` directory
/// check.
pub fn open_copy_to_file(filename: &str) -> PgResult<PgFileStream> {
    // oumask = umask(S_IWGRP | S_IWOTH);
    // SAFETY: umask(2) just swaps the process file-mode-creation mask.
    let oumask = unsafe { libc::umask((libc::S_IWGRP | libc::S_IWOTH) as libc::mode_t) };

    let open_result = allocated_desc::AllocateFile(Path::new(filename), "wb");

    // PG_FINALLY: umask(oumask);
    // SAFETY: restore the saved mask.
    unsafe {
        libc::umask(oumask);
    }

    let index = match open_result {
        Ok(index) => index,
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(0);
            let mut err = file_access_error(
                ERROR,
                errno,
                format!("could not open file \"{filename}\" for writing: %m"),
            );
            // errno == ENOENT || errno == EACCES -> the psql \copy hint.
            if errno == libc::ENOENT || errno == libc::EACCES {
                err = err.with_hint(
                    "COPY TO instructs the PostgreSQL server process to write a file. \
                     You may want a client-side facility such as psql's \\copy."
                        .to_string(),
                );
            }
            return Err(err);
        }
    };

    // if (fstat(fileno(cstate->copy_file), &st)) ereport(ERROR, ...);
    // if (S_ISDIR(st.st_mode)) ereport(ERROR, "is a directory");
    match allocated_desc::AllocatedFileMetadata(index) {
        Ok(meta) => {
            if meta.is_dir() {
                // ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                //                 errmsg("\"%s\" is a directory", filename)))
                let _ = allocated_desc::FreeFile(index);
                return Err(PgError::new(ERROR, format!("\"{filename}\" is a directory"))
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .with_error_location(types_error::ErrorLocation::new(SRCFILE, 0, "")));
            }
        }
        Err(errno) => {
            let _ = allocated_desc::FreeFile(index);
            return Err(file_access_error(
                ERROR,
                errno,
                format!("could not stat file \"{filename}\": %m"),
            ));
        }
    }

    Ok(PgFileStream(index as u64))
}

/// `open_pipe_stream_write` — `OpenPipeStream(command, PG_BINARY_W)`: popen the
/// command for writing. A failure is the C "could not execute command: %m"
/// `ereport`, carried on `Err`.
pub fn open_pipe_stream_write(command: &str) -> PgResult<PgFileStream> {
    let index = allocated_desc::OpenPipeStream(command, "w")?;
    Ok(PgFileStream(index as u64))
}

/// `copy_write_file` — `fwrite(buf, len, 1, copy_file)` + `ferror(copy_file)`.
/// `Ok(None)` on success, `Ok(Some(errno))` on a short write / stream error (the
/// value `%m` should expand to). Writes to the server log for STDOUT.
pub fn copy_write_file(stream: PgFileStream, buf: &[u8]) -> PgResult<Option<i32>> {
    if stream.0 == STDOUT_STREAM {
        // COPY TO STDOUT-to-server-log: write to the process stdout.
        use std::io::Write;
        let mut out = std::io::stdout();
        return match out.write_all(buf).and_then(|()| out.flush()) {
            Ok(()) => Ok(None),
            Err(e) => Ok(Some(e.raw_os_error().unwrap_or(libc::EIO))),
        };
    }
    Ok(allocated_desc::stream_write(stream.0 as i32, buf))
}

/// `free_file` — `FreeFile(copy_file)` (`fclose`). A nonzero close is the C
/// "could not close file: %m" `ereport`, carried on `Err`.
pub fn free_file(stream: PgFileStream, filename: &str) -> PgResult<()> {
    if stream.0 == STDOUT_STREAM {
        // stdout is not a FreeFile target.
        return Ok(());
    }
    match allocated_desc::FreeFile(stream.0 as i32) {
        Ok(()) => Ok(()),
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(0);
            Err(file_access_error(
                ERROR,
                errno,
                format!("could not close file \"{filename}\": %m"),
            ))
        }
    }
}

/// `close_pipe_to_program` — `ClosePipeStream(copy_file)` (`pclose`). `-1` is
/// "could not close pipe to external command: %m"; a nonzero child exit is
/// `ERRCODE_EXTERNAL_ROUTINE_EXCEPTION` "program \"%s\" failed".
pub fn close_pipe_to_program(stream: PgFileStream, filename: &str) -> PgResult<()> {
    let pclose_rc = allocated_desc::ClosePipeStream(stream.0 as i32)?;
    if pclose_rc == -1 {
        let errno = errno_now();
        return Err(file_access_error(
            ERROR,
            errno,
            "could not close pipe to external command: %m".to_string(),
        ));
    }
    if pclose_rc != 0 {
        let detail = wait_result_to_str(pclose_rc);
        return Err(PgError::new(
            ERROR,
            format!("program \"{filename}\" failed"),
        )
        .with_sqlstate(types_error::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION)
        .with_detail(detail)
        .with_error_location(types_error::ErrorLocation::new(SRCFILE, 0, "")));
    }
    Ok(())
}

/// `stdout_stream` — the C stdio `stdout` global as a registered stream token.
pub fn stdout_stream() -> PgFileStream {
    PgFileStream(STDOUT_STREAM)
}

/// `wait_result_to_str(int exitstatus)` (`common/wait_error.c`) — render a child
/// `pclose`/`system` wait status. `-1` is handled by the caller (the `pclose_rc
/// == -1` branch produces the `%m` message there), so this covers the
/// `WIFEXITED`/`WIFSIGNALED`/unrecognized cases, including the 126/127 shell
/// special exit codes and the signal name.
fn wait_result_to_str(exitstatus: i32) -> String {
    if libc::WIFEXITED(exitstatus) {
        match libc::WEXITSTATUS(exitstatus) {
            126 => "command not executable".to_string(),
            127 => "command not found".to_string(),
            code => format!("child process exited with exit code {code}"),
        }
    } else if libc::WIFSIGNALED(exitstatus) {
        let sig = libc::WTERMSIG(exitstatus);
        format!(
            "child process was terminated by signal {sig}: {}",
            pg_strsignal(sig)
        )
    } else {
        format!("child process exited with unrecognized status {exitstatus}")
    }
}

/// `pg_strsignal(int signum)` (`port/strsignal.c`) — the human-readable signal
/// name, or "unrecognized signal" when unknown.
fn pg_strsignal(signum: i32) -> String {
    // SAFETY: strsignal returns a pointer to a (possibly static) NUL-terminated
    // string we copy out immediately.
    unsafe {
        let ptr = libc::strsignal(signum);
        if ptr.is_null() {
            "unrecognized signal".to_string()
        } else {
            std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

// ===========================================================================
// Raw-fd primitives for the transient-file API (slot.c) and the raw-fd
// `pg_fsync`. These take a kernel fd value (the `OpenTransientFile` return).
// ===========================================================================

fn transient_read_raw(fd: i32, buf: &mut [u8]) -> isize {
    let raw = match allocated_desc::TransientFileRawFd(fd) {
        Ok(raw) => raw,
        Err(_) => return -1,
    };
    // SAFETY: raw is a live kernel fd owned by the descriptor table; read(2)
    // into the caller's buffer.
    unsafe { libc::read(raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) }
}

fn transient_write_raw(fd: i32, buf: &[u8]) -> isize {
    let raw = match allocated_desc::TransientFileRawFd(fd) {
        Ok(raw) => raw,
        Err(_) => return -1,
    };
    // SAFETY: raw is a live kernel fd owned by the descriptor table; write(2)
    // from the caller's buffer.
    unsafe { libc::write(raw, buf.as_ptr() as *const libc::c_void, buf.len()) }
}

/// `transient_read` — `read(fd, buf, len)` against a transient fd. Bytes read
/// (`>= 0`) or `-errno`.
pub fn transient_read(fd: i32, buf: &mut [u8]) -> isize {
    let r = transient_read_raw(fd, buf);
    if r < 0 {
        -(errno_now() as isize)
    } else {
        r
    }
}

/// `pg_pread(fd, buf, offset)` — positioned read against a bare kernel fd (the
/// `BasicOpenFile` return value). Bytes read (`>= 0`) or `-errno`. Mirrors the
/// C `pg_pread` used by `xlogreader.c`'s `WALRead`, which `pread(2)`s the WAL
/// segment fd held in `state->seg.ws_file`.
pub fn pg_pread(fd: i32, buf: &mut [u8], offset: i64) -> isize {
    // SAFETY: `fd` is a live bare kernel fd owned by the caller (a WAL segment
    // opened via BasicOpenFile); pread(2) into the caller's buffer at `offset`.
    let r = unsafe {
        libc::pread(
            fd,
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
            offset as libc::off_t,
        )
    };
    if r < 0 {
        -(errno_now() as isize)
    } else {
        r
    }
}

/// `pg_pwrite(fd, buf, offset)` — positioned write against a bare kernel fd (a
/// WAL segment opened via `BasicOpenFile`/`XLogFileInit`). Bytes written
/// (`>= 0`) or `-errno`. Mirrors the C `pg_pwrite` used by `xlog.c`'s
/// `XLogWrite` to dump WAL-buffer pages into the segment file.
pub fn pg_pwrite(fd: i32, buf: &[u8], offset: i64) -> isize {
    // SAFETY: `fd` is a live bare kernel fd owned by the caller; pwrite(2) from
    // the caller's buffer at `offset`.
    let r = unsafe {
        libc::pwrite(
            fd,
            buf.as_ptr() as *const libc::c_void,
            buf.len(),
            offset as libc::off_t,
        )
    };
    if r < 0 {
        -(errno_now() as isize)
    } else {
        r
    }
}

/// `pg_pwrite_zeros(fd, size, offset)` (common/file_utils.c) — zero-fill `size`
/// bytes of a bare kernel fd at `offset`, allocating the file space. Total
/// bytes written (`>= 0`) or `-errno`. Used by `XLogFileInit` to pre-fill a
/// fresh WAL segment when `wal_init_zero` is on.
pub fn pg_pwrite_zeros(fd: i32, size: usize, offset: i64) -> isize {
    let r = vfd_io::pg_pwrite_zeros(fd, size, offset);
    if r < 0 {
        -(errno_now() as isize)
    } else {
        r
    }
}

/// `BasicOpenFile(path, flags)` — open with arbitrary `open(2)` flags against a
/// bare kernel fd (the form `xlog.c`'s `XLogFileInit`/`XLogFileOpen` use).
/// `Ok(fd)` on success; `Err(errno)` carries the errno the caller inspects.
pub fn basic_open_file_flags(path: &str, flags: i32) -> Result<i32, i32> {
    match vfd_core::BasicOpenFilePermFd(Path::new(path), flags, vfd_core::pg_file_create_mode()) {
        Ok(-1) => Err(errno_now()),
        Ok(fd) => Ok(fd),
        Err(e) => Err(e.saved_errno().unwrap_or(errno_now())),
    }
}

/// `transient_write` — `write(fd, buf, len)` against a transient fd. Bytes
/// written (`>= 0`) or `-errno`.
pub fn transient_write(fd: i32, buf: &[u8]) -> isize {
    let w = transient_write_raw(fd, buf);
    if w < 0 {
        -(errno_now() as isize)
    } else {
        w
    }
}

/// `pg_fsync(int fd)` (fd.c) on a raw kernel fd. Returns `0` on success or
/// `-errno` on failure (the seam contract for both file-seams and fd-seams).
/// Honors `enableFsync`/`wal_sync_method`, borrowing the fd non-owningly so the
/// caller keeps ownership (mirrors C's `pg_fsync(fd)` taking the bare int).
pub fn seam_pg_fsync(fd: i32) -> i32 {
    use std::mem::ManuallyDrop;
    // The fd may be a transient-table index keyed by the kernel fd value (slot
    // path), or a bare kernel fd. TransientFileRawFd resolves the table form;
    // if it isn't tracked, fall back to treating the value as the kernel fd.
    let raw = allocated_desc::TransientFileRawFd(fd).unwrap_or(fd);
    // SAFETY: `raw` is a live kernel fd; ManuallyDrop ensures we never close it
    // (the owner — descriptor table or caller — closes it), matching C.
    let file = ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(raw) });
    match sync_cleanup::pg_fsync(&file) {
        Ok(()) => 0,
        Err(e) => -(e.saved_errno().unwrap_or(libc::EIO)),
    }
}

/// `fsync_fname(fname, isdir)` (fd.c) — fsync a file or directory, ereporting at
/// `data_sync_elevel(ERROR)` (so `Err` may carry ERROR or PANIC).
pub fn seam_fsync_fname(fname: &str, isdir: bool) -> PgResult<()> {
    sync_cleanup::fsync_fname(Path::new(fname), isdir)
}

/// `durable_rename(oldfile, newfile, elevel)` (fd.c).
pub fn seam_durable_rename(
    oldfile: &str,
    newfile: &str,
    elevel: ErrorLevel,
) -> PgResult<()> {
    sync_cleanup::durable_rename(Path::new(oldfile), Path::new(newfile), elevel)
}

/// `data_sync_elevel(elevel)` (fd.c).
pub fn seam_data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    vfd_core::data_sync_elevel(elevel)
}

/// `pg_file_exists(name)` (fd.c) — exists and not a directory.
pub fn seam_pg_file_exists(name: &str) -> PgResult<bool> {
    sync_cleanup::pg_file_exists(Path::new(name))
}

/// `basic_open_file(path)` — `BasicOpenFile(path, O_RDONLY | PG_BINARY)`. The
/// caller (xlogutils' `wal_segment_open`) keeps the returned kernel fd, so we
/// return the raw fd value and do not wrap it in an owned (dropping) handle.
/// `Ok(fd)` on success; `Err(errno)` carries the errno the caller inspects.
pub fn basic_open_file(path: &str) -> Result<i32, i32> {
    match vfd_core::BasicOpenFilePermFd(
        Path::new(path),
        O_RDONLY | PG_BINARY,
        vfd_core::pg_file_create_mode(),
    ) {
        // BasicOpenFilePermFd returns -1 (errno set) on failure, the kernel fd
        // otherwise; it only returns Err on a hard ereport, which O_RDONLY can't
        // hit. Treat any Err as the recorded errno.
        Ok(-1) => Err(errno_now()),
        Ok(fd) => Ok(fd),
        Err(e) => Err(e.saved_errno().unwrap_or(errno_now())),
    }
}

const O_RDONLY: i32 = libc::O_RDONLY;

/// `int OpenTransientFile(const char *path, int flags)` (fd.c) for the fd-seams
/// `i32` contract: the transient fd (`>= 0`) on success, or `-errno` on failure
/// (C returns -1 with errno set). The `maxAllocatedDescs` exhaustion is itself
/// an `ereport(ERROR)` in C; here it can't be expressed as `-errno`, so we
/// surface it as `-EMFILE` (the closest "too many descriptors" code).
pub fn open_transient_file_i32(path: &str, flags: i32) -> i32 {
    match allocated_desc::OpenTransientFile(Path::new(path), flags) {
        Ok(fd) => fd,
        Err(e) => {
            let errno = e.saved_errno().unwrap_or(libc::EMFILE);
            -errno
        }
    }
}

/// `int CloseTransientFile(int fd)` (fd.c) for the fd-seams `i32` contract: 0 on
/// success, `-errno` on failure.
pub fn close_transient_file_i32(fd: i32) -> i32 {
    match allocated_desc::CloseTransientFile(fd) {
        Ok(()) => 0,
        Err(e) => -e.saved_errno().unwrap_or(libc::EIO),
    }
}

// ===========================================================================
// Directory / tree helpers owned by the fd-seams crate (the C lives in
// common/file_utils.c `get_dirent_type` and common/rmtree.c `rmtree`; their
// decls are owned here, so the real logic lands in-crate).
// ===========================================================================

// PGFileType codes (common/file_utils.h).
const PGFILETYPE_ERROR: i32 = 0;
const PGFILETYPE_UNKNOWN: i32 = 1;
const PGFILETYPE_REG: i32 = 2;
const PGFILETYPE_DIR: i32 = 3;
const PGFILETYPE_LNK: i32 = 4;

/// `get_dirent_type(path, de, look_through_symlinks=false, elevel=LOG)`
/// (common/file_utils.c) — classify a directory entry by `lstat`. The seam takes
/// only the path (no `dirent`), so it always falls to the stat-based path.
pub fn get_dirent_type(path: &str) -> i32 {
    let cpath = match std::ffi::CString::new(path) {
        Ok(c) => c,
        Err(_) => return PGFILETYPE_ERROR,
    };
    // look_through_symlinks == false -> lstat.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: cpath is NUL-terminated; st is a valid out-param.
    let sret = unsafe { libc::lstat(cpath.as_ptr(), &mut st) };
    if sret < 0 {
        // C logs at elevel (LOG) and returns PGFILETYPE_ERROR.
        let _ = backend_utils_error_seams::ereport::call(file_access_error(
            LOG,
            errno_now(),
            format!("could not stat file \"{path}\": %m"),
        ));
        return PGFILETYPE_ERROR;
    }
    let mode = st.st_mode & libc::S_IFMT;
    if mode == libc::S_IFREG {
        PGFILETYPE_REG
    } else if mode == libc::S_IFDIR {
        PGFILETYPE_DIR
    } else if mode == libc::S_IFLNK {
        PGFILETYPE_LNK
    } else {
        PGFILETYPE_UNKNOWN
    }
}

/// `rmtree(path, rmtopdir)` (common/rmtree.c) — recursively remove a directory
/// tree. Returns `true` on full success; any failure is logged at WARNING (here
/// via the elog seam) and yields `false`, matching C.
pub fn rmtree(path: &str, rmtopdir: bool) -> bool {
    let mut result = true;

    // dir = OPENDIR(path); if (dir == NULL) { warning; return false; }
    let entries = match std::fs::read_dir(path) {
        Ok(rd) => rd,
        Err(e) => {
            warn_file(format!("could not open directory \"{path}\""), &e);
            return false;
        }
    };

    let mut subdirs: Vec<String> = Vec::new();

    for ent in entries {
        let ent = match ent {
            Ok(ent) => ent,
            Err(e) => {
                warn_file(format!("could not read directory \"{path}\""), &e);
                result = false;
                continue;
            }
        };
        let name = ent.file_name();
        let name = name.to_string_lossy();
        // readdir already excludes nothing; C skips "."/".." which read_dir omits.
        let pathbuf = format!("{path}/{name}");
        match get_dirent_type(&pathbuf) {
            PGFILETYPE_ERROR => { /* already logged, press on */ }
            PGFILETYPE_DIR => subdirs.push(pathbuf),
            _ => {
                // if (unlink(pathbuf) != 0 && errno != ENOENT) warning;
                if let Err(e) = std::fs::remove_file(&pathbuf) {
                    if e.raw_os_error() != Some(libc::ENOENT) {
                        warn_file(format!("could not remove file \"{pathbuf}\""), &e);
                        result = false;
                    }
                }
            }
        }
    }

    // Recurse into subdirectories (deferred until the parent dir is closed).
    for sub in subdirs {
        if !rmtree(&sub, true) {
            result = false;
        }
    }

    if rmtopdir {
        // if (rmdir(path) != 0) warning;
        if let Err(e) = std::fs::remove_dir(path) {
            warn_file(format!("could not remove directory \"{path}\""), &e);
            result = false;
        }
    }

    result
}

fn warn_file(message: String, error: &std::io::Error) {
    let errno = error.raw_os_error().unwrap_or(libc::EIO);
    let _ = backend_utils_error_seams::ereport::call(file_access_error(
        types_error::WARNING,
        errno,
        format!("{message}: %m"),
    ));
}

/// `path_is_dir(path)` — `stat(path) == 0 && S_ISDIR(st.st_mode)`.
pub fn path_is_dir(path: &str) -> bool {
    match std::fs::metadata(path) {
        Ok(meta) => meta.is_dir(),
        Err(_) => false,
    }
}

/// `read_dir_names(dirname)` — `AllocateDir(dirname)` + `ReadDir` loop collapsed
/// to the entry names (excluding `.`/`..`). May `ereport(ERROR)` if the
/// directory cannot be opened.
pub fn read_dir_names(dirname: &str) -> PgResult<Vec<String>> {
    let mut names = Vec::new();
    allocated_desc::with_allocated_dir(dirname, &mut |name: &str| {
        if name != "." && name != ".." {
            names.push(name.to_string());
        }
        Ok(false)
    })?;
    Ok(names)
}
