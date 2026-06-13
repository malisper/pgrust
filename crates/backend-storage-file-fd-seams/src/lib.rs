//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `AllocateFile(path, PG_BINARY_W)` + `fwrite` + `FreeFile` (fd.c) — write
    /// all of `bytes` to a freshly created file. The caller (snapmgr) chooses
    /// the path and owns the `.tmp`+`rename` ordering; this only performs the
    /// fd.c-tracked open/write/close. Open/write failures surface as
    /// `ereport(ERROR, errcode_for_file_access)` on `Err`.
    pub fn allocate_file_write(path: &str, bytes: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `AllocateFile(path, PG_BINARY_R)` + `fstat` + `fread` + `FreeFile`
    /// (fd.c) — read the whole file into a byte buffer. Returns `Ok(None)` when
    /// the file does not exist (`errno == ENOENT`, which snapmgr maps to its
    /// own "snapshot does not exist" error); other open/read failures surface
    /// as `ereport(ERROR)` on `Err`.
    pub fn allocate_file_read(path: &str) -> PgResult<Option<Vec<u8>>>
);

/// An open `FILE *` registered with the virtual-file-descriptor machinery
/// (`AllocateFile`/`OpenPipeStream`). C's `FILE *` is a genuinely opaque
/// stdio handle, so the owned model carries it as this token; fd.c owns the
/// stream behind it and the read/write/close primitives dispatch on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgFileStream(pub u64);

/// Outcome of the "`OpenTransientFile(dbpath/\"pg_filenode.map\", O_RDONLY |
/// PG_BINARY)` + `read()` of `sizeof(RelMapFile)` bytes + `CloseTransientFile`"
/// load unit behind `relmapper.c`'s `read_relmap_file`. The file descriptor (a
/// held resource) lives entirely inside the fd owner; the caller never holds
/// it. The raw `errno`/byte-count are carried back so the relmapper algorithm
/// can reproduce the exact error reports in-crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelmapReadOutcome {
    /// `OpenTransientFile` returned `< 0`; `errno` was left set.
    OpenFailed { errno: i32 },
    /// `read()` returned `< 0`; `errno` was left set.
    ReadFailed { errno: i32 },
    /// `read()` returned a short (non-negative) count `got`.
    ShortRead { got: i64 },
    /// `CloseTransientFile` returned non-zero; `errno` was left set.
    CloseFailed { errno: i32 },
    /// The full `sizeof(RelMapFile)` image was read successfully.
    Ok { bytes: Vec<u8> },
}

/// Outcome of the "`OpenTransientFile(dbpath/\"pg_filenode.map.tmp\", O_WRONLY |
/// O_CREAT | O_TRUNC | PG_BINARY)` + `write()` + `CloseTransientFile`" first
/// store step behind `relmapper.c`'s `write_relmap_file`, kept separate from the
/// rename so the in-crate algorithm preserves C's "write temp, [WAL],
/// durable_rename" ordering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelmapWriteOutcome {
    /// Opening the temp file failed; `errno` was left set.
    OpenFailed { errno: i32 },
    /// Writing the image failed (short or `< 0` write); `errno` is the value to
    /// expand `%m` with (relmapper substitutes `ENOSPC` when write left it 0).
    WriteFailed { errno: i32 },
    /// Closing the temp file failed; `errno` was left set.
    CloseFailed { errno: i32 },
    /// Open/write/close all succeeded.
    Ok,
}

seam_core::seam!(
    /// Load unit behind `read_relmap_file`: open `dbpath/pg_filenode.map`
    /// read-only, `read()` `sizeof(RelMapFile)` bytes, close. Returns the raw
    /// outcome; the relmapper algorithm validates magic/num_mappings/CRC.
    pub fn relmap_read_file(dbpath: &str) -> PgResult<RelmapReadOutcome>
);

seam_core::seam!(
    /// First store step behind `write_relmap_file`: open
    /// `dbpath/pg_filenode.map.tmp` (O_WRONLY|O_CREAT|O_TRUNC|PG_BINARY),
    /// `write()` `bytes`, close. Returns the raw outcome.
    pub fn relmap_write_temp(dbpath: &str, bytes: &[u8]) -> PgResult<RelmapWriteOutcome>
);

seam_core::seam!(
    /// Final store step behind `write_relmap_file`:
    /// `durable_rename(dbpath/pg_filenode.map.tmp, dbpath/pg_filenode.map,
    /// ERROR)`. relmapper always passes ERROR, which becomes PANIC inside a
    /// critical section; a failure surfaces as `Err`.
    pub fn relmap_durable_rename(dbpath: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_file_exists(name)` (`storage/file/fd.c`) — true if the path exists
    /// and is not a directory. May `ereport(ERROR)` for an access error other
    /// than `ENOENT`/`ENOTDIR`/`EACCES`, surfaced as `Err`.
    pub fn pg_file_exists(name: &str) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `MakePGDirectory(directoryName)` (`storage/file/fd.c`) —
    /// `mkdir(directoryName, pg_dir_create_mode)`. Returns the `mkdir`
    /// result (`0` on success, `-1` with errno set on failure); infallible
    /// at the ereport level. Use [`last_errno`] for the failure errno.
    pub fn make_pg_directory(directory_name: &str) -> i32
);

seam_core::seam!(
    /// The COPY-TO file open path (copyto.c:952-985), which is OS/fd-coupled:
    /// `umask(S_IWGRP | S_IWOTH)`, `AllocateFile(filename, PG_BINARY_W)` inside
    /// PG_TRY/PG_FINALLY restoring the umask, the open-failure `ereport`
    /// (`errcode_for_file_access`, "could not open file ... for writing: %m",
    /// plus the ENOENT/EACCES psql `\copy` hint), then `fstat` and the
    /// `S_ISDIR` "is a directory" check. All of this `ereport`s on failure
    /// (carried on `Err`); on success it returns the open stream token. The
    /// caller has already verified the path is absolute.
    pub fn open_copy_to_file(filename: &str) -> PgResult<PgFileStream>
);

seam_core::seam!(
    /// `OpenPipeStream(command, PG_BINARY_W)` (fd.c) for COPY TO PROGRAM
    /// (copyto.c:929-934): `popen` the command for writing, registering the
    /// pipe with the vfd machinery. A NULL return is the C "could not execute
    /// command: %m" `ereport`, carried on `Err`; success returns the stream.
    pub fn open_pipe_stream_write(command: &str) -> PgResult<PgFileStream>
);

seam_core::seam!(
    /// The bare `fwrite(buf, len, 1, copy_file)` + `ferror(copy_file)` write
    /// primitive for COPY TO to a file or program pipe (copyto.c:452-454). This
    /// is the genuinely fd-owned part: it performs the stdio write and reports
    /// only whether it failed. On failure it returns the OS `errno` so the
    /// caller (copyto, which owns the EPIPE/`is_program`/message-selection
    /// control flow) can reproduce the exact `ereport`. `Ok` carries the C
    /// `fwrite(...) != 1 || ferror(...)` condition: `None` on success, `Some(errno)`
    /// on a short write or stream error (the value `%m` should expand to).
    pub fn copy_write_file(stream: PgFileStream, buf: &[u8]) -> PgResult<Option<i32>>
);

seam_core::seam!(
    /// `FreeFile(copy_file)` (fd.c) — `fclose` the stream and deregister it.
    /// A nonzero close result is the C "could not close file: %m" `ereport`
    /// (copyto.c:595-599), carried on `Err`; `filename` supplies the message.
    pub fn free_file(stream: PgFileStream, filename: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `ClosePipeStream(copy_file)` (fd.c) — `pclose` the program pipe. The
    /// pclose return code drives copyto.c:568-580: `-1` is "could not close
    /// pipe to external command: %m", a nonzero exit is `ERRCODE_EXTERNAL_
    /// ROUTINE_EXCEPTION` "program \"%s\" failed" with the `wait_result_to_str`
    /// detail. Both are carried on `Err`; success is `Ok(())`.
    pub fn close_pipe_to_program(stream: PgFileStream, filename: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `stdout` (the C stdio global) as a registered stream token, for the
    /// COPY TO STDOUT-to-server-log path (copyto.c:919, `cstate->copy_file =
    /// stdout`). Infallible.
    pub fn stdout_stream() -> PgFileStream
);

seam_core::seam!(
    /// `AtEOXact_Files(isCommit)` — close transaction-lifetime files; WARNs
    /// about leaks at commit.
    pub fn at_eoxact_files(is_commit: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_Files(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_files(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);

seam_core::seam!(
    /// `set_max_safe_fds()` (fd.c): probe how many files can be opened and set
    /// `max_safe_fds`. `ereport(FATAL)` when too few are available.
    pub fn set_max_safe_fds() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Current `errno` (used to reproduce `errcode_for_file_access()` /`%m`
    /// at a caller's chosen `elevel` after a failed primitive below).
    pub fn last_errno() -> i32
);

seam_core::seam!(
    /// `int OpenTransientFile(const char *path, int flags)` (`fd.c`). On
    /// success returns the transient fd index (>= 0); on failure returns the
    /// negative `-errno` (C returns -1 with `errno` set — encoded here so the
    /// caller can reconstruct its `ereport`).
    pub fn open_transient_file(path: &str, flags: i32) -> i32
);

seam_core::seam!(
    /// `int CloseTransientFile(int fd)` (`fd.c`). 0 on success, `-errno` on
    /// failure.
    pub fn close_transient_file(fd: i32) -> i32
);

seam_core::seam!(
    /// `write(fd, buf, len)` against a transient fd. Returns bytes written
    /// (>=0) or `-errno`.
    pub fn transient_write(fd: i32, buf: &[u8]) -> isize
);

seam_core::seam!(
    /// `read(fd, buf, len)` against a transient fd. Returns bytes read (>=0)
    /// or `-errno`. Reads into `buf`.
    pub fn transient_read(fd: i32, buf: &mut [u8]) -> isize
);

seam_core::seam!(
    /// `int pg_fsync(int fd)` (`fd.c`). 0 on success, `-errno` on failure.
    pub fn pg_fsync(fd: i32) -> i32
);

seam_core::seam!(
    /// `void fsync_fname(const char *fname, bool isdir)` (`fd.c`). Errors are
    /// handled internally at `data_sync_elevel(ERROR)`, carried on `Err`.
    pub fn fsync_fname(fname: &str, isdir: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `unlink(path)`. 0 on success, `-errno` on failure.
    pub fn unlink_file(path: &str) -> i32
);

seam_core::seam!(
    /// `rename(from, to)`. 0 on success, `-errno` on failure.
    pub fn rename_file(from: &str, to: &str) -> i32
);

seam_core::seam!(
    /// `bool rmtree(const char *path, bool rmtopdir)` (`common/rmtree.c`) —
    /// remove a directory tree. Returns true on full success (false logs a
    /// warning internally, like C).
    pub fn rmtree(path: &str, rmtopdir: bool) -> bool
);

seam_core::seam!(
    /// `stat(path)` test for "exists and is a directory" — slot.c only needs
    /// `stat(tmppath, &st) == 0 && S_ISDIR(st.st_mode)`.
    pub fn path_is_dir(path: &str) -> bool
);

seam_core::seam!(
    /// `AllocateDir(dirname)` + `ReadDir()` loop collapsed: return the entry
    /// names in `dirname` (excluding `.`/`..`). Can `ereport(ERROR)` if the
    /// directory cannot be opened (C `AllocateDir`/`ReadDir`), carried on `Err`.
    pub fn read_dir_names(dirname: &str) -> types_error::PgResult<Vec<String>>
);

seam_core::seam!(
    /// `AllocateDir(dir)` + `ReadDirExtended(.., LOG)` + `FreeDir` (fd.c) —
    /// list a directory's entries (excluding `.`/`..`) at LOG severity. Read
    /// problems (including a failed `AllocateDir`) are logged at LOG by fd.c and
    /// skipped, so this returns the names it could read; cannot `ereport` at
    /// ERROR. (snapmgr's `DeleteAllExportedSnapshotFiles` uses the LOG variant.)
    pub fn read_dir_names_logged(dir: &str) -> Vec<String>
);

seam_core::seam!(
    /// `get_dirent_type(path, de, look_through_symlinks, elevel)` (`fd.c`) —
    /// classify a directory entry. Returns the `PGFileType` code
    /// (`PGFILETYPE_ERROR`=0, `_UNKNOWN`=1, `_REG`=2, `_DIR`=3, `_LNK`=4).
    pub fn get_dirent_type(path: &str) -> i32
);

// --- backend-utils-init-postinit consumers (fd.c) ---

seam_core::seam!(
    /// `InitFileAccess()` (fd.c): initialize the virtual file descriptor cache.
    /// `Err` carries its `ereport` surface.
    pub fn init_file_access() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitTemporaryFileAccess()` (fd.c): set up temporary-file accounting
    /// (after pgstat). `Err` carries its `ereport` surface.
    pub fn init_temporary_file_access() -> types_error::PgResult<()>
);

/// Result of `access(path, F_OK)` (postinit.c database-directory check).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessResult {
    /// `access() == 0` — the path exists.
    Ok,
    /// `errno == ENOENT` — the path does not exist.
    NoEnt,
    /// Any other `errno` (carried as the raw value).
    Other(i32),
}

seam_core::seam!(
    /// `access(path, F_OK)` (unistd, used by InitPostgres): probe whether the
    /// database directory exists. Returns the classified outcome (the C `== -1`
    /// + `errno` branch). `Err` is reserved for the seam's own failure surface
    /// (none expected; OS errno is returned in [`AccessResult::Other`]).
    pub fn access_f_ok(path: &str) -> types_error::PgResult<AccessResult>
);

seam_core::seam!(
    /// `BasicOpenFile(path, O_RDONLY | PG_BINARY)` (fd.c) — open a file
    /// outside the virtual-fd pool. `Ok(fd)` on success; `Err(errno)` carries
    /// the `errno` the C caller inspects (e.g. `ENOENT`) to choose its
    /// `ereport` message.
    pub fn basic_open_file(path: &str) -> Result<i32, i32>
);
