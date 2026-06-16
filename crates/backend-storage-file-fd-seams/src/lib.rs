//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use types_core::SubTransactionId;
use types_error::PgResult;
use types_storage::file::File;

/// One directory entry as returned by [`list_dir`] — mirrors the parts of
/// `struct dirent` + `struct stat` that the `pg_ls_*` callers read.
pub struct DirEntryInfo<'mcx> {
    /// `de->d_name` — the entry's file name.
    pub name: mcx::PgString<'mcx>,
    /// `attrib.st_size` — file size in bytes.
    pub size: i64,
    /// `attrib.st_mtime` converted via `time_t_to_timestamptz` — last
    /// modification time.
    pub modification: types_core::TimestampTz,
    /// `S_ISDIR(attrib.st_mode)` — is the entry a directory?
    pub isdir: bool,
    /// `S_ISREG(attrib.st_mode)` — is the entry a regular file?
    pub isreg: bool,
}

/// The result of [`stat_file`] — the parts of `struct stat` that
/// `pg_stat_file` exposes, with `time_t` fields already converted via
/// `time_t_to_timestamptz`.
#[derive(Clone, Copy, Debug)]
pub struct StatInfo {
    /// `fst.st_size`.
    pub size: i64,
    /// `time_t_to_timestamptz(fst.st_atime)`.
    pub access: types_core::TimestampTz,
    /// `time_t_to_timestamptz(fst.st_mtime)`.
    pub modification: types_core::TimestampTz,
    /// `time_t_to_timestamptz(fst.st_ctime)` (Unix status-change time).
    pub change: types_core::TimestampTz,
    /// `S_ISDIR(fst.st_mode)`.
    pub isdir: bool,
}

seam_core::seam!(
    /// `AllocateFile(path, PG_BINARY_W)` + `fwrite` + `FreeFile` (fd.c) — write
    /// all of `bytes` to a freshly created file. The caller (snapmgr) chooses
    /// the path and owns the `.tmp`+`rename` ordering; this only performs the
    /// fd.c-tracked open/write/close. Open/write failures surface as
    /// `ereport(ERROR, errcode_for_file_access)` on `Err`.
    pub fn allocate_file_write(path: &str, bytes: &[u8]) -> PgResult<()>
);

/// The outcome of [`create_empty_file`] — which of the two C steps failed (if
/// any), carrying `errno` so the non-throwing caller can build its own
/// `ereport(LOG)` message. Mirrors xlogarchive.c's `AllocateFile`-then-`FreeFile`
/// pair, where a `NULL` open and a non-zero `FreeFile` are reported separately.
#[derive(Clone, Copy, Debug)]
pub enum CreateEmptyFileOutcome {
    /// Both the open and the close (flush) succeeded.
    Ok,
    /// `AllocateFile` returned `NULL`; carries the saved `errno`.
    CreateFailed(i32),
    /// `FreeFile` returned non-zero (deferred write error); carries `errno`.
    WriteFailed(i32),
}

seam_core::seam!(
    /// `AllocateFile(path, "w")` immediately followed by `FreeFile` (fd.c) —
    /// create an otherwise-empty file (the archive `.ready`/`.done` status-file
    /// idiom in xlogarchive.c). Unlike [`allocate_file_write`] this never
    /// throws: failures are returned as [`CreateEmptyFileOutcome`] so the caller
    /// can emit its own non-throwing `ereport(LOG)` and continue.
    pub fn create_empty_file(path: &str) -> CreateEmptyFileOutcome
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
    /// The `read_binary_file()` core (genfile.c) once the filename is
    /// validated: `AllocateFile(PG_BINARY_R)`, `fseeko` to `seek_offset`
    /// (`SEEK_SET` if `>= 0`, else `SEEK_END`), then read either exactly
    /// `bytes_to_read` bytes (when `>= 0`) or the rest of the file (when
    /// negative, capped at `MaxAllocSize - VARHDRSZ`), and `FreeFile`. The
    /// `AllocateFile`/stdio machinery is fd-owned; the seam returns the raw
    /// bytes in `mcx`. `Ok(None)` mirrors C's `missing_ok && errno == ENOENT`
    /// (file absent); `Err` carries open/seek/read/`file length too large`
    /// `ereport(ERROR)`s.
    pub fn read_server_file<'mcx>(
        mcx: Mcx<'mcx>,
        filename: &str,
        seek_offset: i64,
        bytes_to_read: i64,
        missing_ok: bool,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `stat(filename, &fst)` behind the fd owner (genfile.c `pg_stat_file`).
    /// `Ok(None)` mirrors C's `missing_ok && errno == ENOENT`; other failures
    /// raise `could not stat file` as `Err`.
    pub fn stat_file(filename: &str, missing_ok: bool) -> PgResult<Option<StatInfo>>
);

seam_core::seam!(
    /// `lstat(filename, &st)` behind the fd owner (dbcommands.c
    /// `remove_dbtablespaces` / `check_db_file_conflict`): like [`stat_file`] but
    /// does not follow a symlink (the `pg_tblspc/<oid>` entries are symlinks, so
    /// the database commands lstat them to test for the per-database
    /// subdirectory's existence and `S_ISDIR`). `Ok(None)` mirrors C's
    /// `missing_ok && errno == ENOENT`.
    pub fn lstat_file(filename: &str, missing_ok: bool) -> PgResult<Option<StatInfo>>
);

seam_core::seam!(
    /// `AllocateDir(dirname)` + the full `ReadDir` walk + `FreeDir`
    /// (genfile.c). Returns one [`DirEntryInfo`] per entry (including `.`/`..`
    /// and the per-file `stat`; the caller applies the dot-dir / hidden-file /
    /// regular-file filters exactly as the specific `pg_ls_*` variant does).
    /// `Ok(None)` mirrors `missing_ok && errno == ENOENT` (the directory is
    /// absent); `Err` carries the `ReadDir`/`stat` `ereport(ERROR)`s.
    pub fn list_dir<'mcx>(
        mcx: Mcx<'mcx>,
        dirname: &str,
        missing_ok: bool,
    ) -> PgResult<Option<PgVec<'mcx, DirEntryInfo<'mcx>>>>
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
    /// `OpenPipeStream(command, "r")` (fd.c) — `popen` the command for reading,
    /// registering the pipe with the vfd machinery. Unlike
    /// [`open_pipe_stream_write`], this faithfully reproduces the C contract that
    /// a `popen` failure returns `NULL` (here `Ok(None)`) with `errno` left set
    /// (retrievable via [`last_errno`]) rather than `ereport`ing — the
    /// `run_ssl_passphrase_command` caller reports the failure at its own
    /// `is_server_start`-dependent loglevel. `Err` carries only the
    /// `reserveAllocatedDesc` exhaustion `ereport(ERROR)`.
    pub fn open_pipe_stream_read(command: &str) -> PgResult<Option<PgFileStream>>
);

/// Outcome of a `fgets(buf, size, fh)` + `ferror(fh)` read against a pipe
/// stream opened for reading ([`open_pipe_stream_read`]). Mirrors the three C
/// observable cases in `run_ssl_passphrase_command`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipeReadLine {
    /// `fgets` returned the buffer: one line (up to `size - 1` bytes, including
    /// any trailing newline) was read. Carries the bytes actually read.
    Line(Vec<u8>),
    /// `fgets` returned `NULL` and `ferror(fh)` was false: end of file with no
    /// data (the caller leaves `buf` as the empty string).
    Eof,
    /// `fgets` returned `NULL` and `ferror(fh)` was true: a read error. Carries
    /// the OS `errno` for the caller's `%m` expansion.
    Error(i32),
}

seam_core::seam!(
    /// `fgets(buf, size, fh)` + `ferror(fh)` against a pipe stream opened with
    /// [`open_pipe_stream_read`] (fd-owned stdio). Reads at most `size - 1`
    /// bytes up to and including the first newline. Returns the
    /// [`PipeReadLine`] outcome; the caller owns the strip-CRLF / message logic.
    pub fn pipe_read_line(stream: PgFileStream, size: i32) -> PgResult<PipeReadLine>
);

seam_core::seam!(
    /// `ClosePipeStream(fh)` (fd.c) — `pclose` the pipe and deregister it,
    /// returning the raw `pclose` wait status (`-1` on a `pclose` error with
    /// `errno` set, `0` on a clean child exit, non-zero for the child's wait
    /// status). Unlike [`close_pipe_to_program`], this returns the raw code so
    /// the `run_ssl_passphrase_command` caller can build its own
    /// loglevel-dependent `ereport`s. `Err` is reserved for the
    /// not-from-OpenPipeStream `ereport(WARNING)`'s longjmp surface (none in
    /// practice).
    pub fn close_pipe_stream(stream: PgFileStream) -> PgResult<i32>
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
    /// Read the full contents of the file at `path` (`AllocateFile(path, "r")`
    /// + read loop, allocated in `mcx`). Returns `None` when the file is absent
    /// (`errno == ENOENT`); raises `FATAL` on any other open failure and
    /// `ERROR` on a read failure, exactly as the timeline.c callers expect.
    pub fn read_file_or_absent<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        path: &str,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// Probe whether the file at `path` exists (`AllocateFile(path, "r")` then
    /// `FreeFile`). `true` if it could be opened, `false` if `errno == ENOENT`,
    /// `FATAL` on any other open failure.
    pub fn file_exists(path: &str) -> types_error::PgResult<bool>
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
    /// `pg_pread(fd, buf, count, offset)` — positioned read against a bare
    /// kernel fd (the `BasicOpenFile` return value, e.g. the WAL segment fd a
    /// reader holds in `state->seg.ws_file`). Returns bytes read (>=0) or
    /// `-errno`; reads `buf.len()` bytes into `buf` at `offset`. Consumed by
    /// `xlogreader.c`'s `WALRead`.
    pub fn pg_pread(fd: i32, buf: &mut [u8], offset: i64) -> isize
);

seam_core::seam!(
    /// `pg_pwrite(fd, buf, count, offset)` — positioned write against a bare
    /// kernel fd (the `BasicOpenFile`/`XLogFileInit` return value). Returns
    /// bytes written (>=0) or `-errno`; writes `buf.len()` bytes at `offset`.
    /// Consumed by `xlog.c`'s `XLogWrite` (the WAL-buffer-to-segment dump).
    pub fn pg_pwrite(fd: i32, buf: &[u8], offset: i64) -> isize
);

seam_core::seam!(
    /// `pg_pwrite_zeros(fd, size, offset)` (`fd.c`) — zero-fill `size` bytes of
    /// a bare kernel fd starting at `offset`, allocating the file space (used to
    /// initialize a fresh WAL segment when `wal_init_zero` is on). Returns the
    /// total bytes written (>=0) or `-errno`.
    pub fn pg_pwrite_zeros(fd: i32, size: usize, offset: i64) -> isize
);

seam_core::seam!(
    /// `BasicOpenFile(path, flags)` (`fd.c`) — open a file with arbitrary
    /// `open(2)` flags against a bare kernel fd (the form `xlog.c`'s
    /// `XLogFileInit`/`XLogFileOpen` use, passing `O_RDWR|O_CREAT|O_EXCL` /
    /// the `get_sync_bit` flags). `Ok(fd)` on success; `Err(errno)` carries the
    /// errno the caller inspects (e.g. `ENOENT` -> create path).
    pub fn basic_open_file_flags(path: &str, flags: i32) -> Result<i32, i32>
);

seam_core::seam!(
    /// `int pg_fsync(int fd)` (`fd.c`). 0 on success, `-errno` on failure.
    pub fn pg_fsync(fd: i32) -> i32
);

seam_core::seam!(
    /// `pg_pwrite(fd, buf, offset)` against a *transient* fd (the
    /// `OpenTransientFile` return value, a fd.c descriptor-table index). The
    /// installed impl resolves the transient index to the kernel fd first.
    /// Returns bytes written (>=0) or `-errno`. Consumed by
    /// `rewriteheap.c`'s `heap_xlog_logical_rewrite` replay.
    pub fn pg_pwrite_transient(fd: i32, buf: &[u8], offset: i64) -> isize
);

seam_core::seam!(
    /// `pg_ftruncate(fd, length)` against a *transient* fd (the
    /// `OpenTransientFile` return value). The installed impl resolves the
    /// transient index to the kernel fd first. Returns 0 on success or
    /// `-errno`. Consumed by `rewriteheap.c`'s `heap_xlog_logical_rewrite`
    /// replay (truncate-tail-to-offset).
    pub fn pg_ftruncate_transient(fd: i32, length: i64) -> i32
);

seam_core::seam!(
    /// `close(fd)` — close a bare kernel fd (the `BasicOpenFile` return value,
    /// e.g. the recovery WAL segment fd). Returns 0 on success or `-errno`. The
    /// recovery page-read driver (`XLogPageRead`) calls `close()` on its
    /// `readFile` directly, not the VFD layer.
    pub fn close_fd(fd: i32) -> i32
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

// --- backend-storage-file-buffile consumers: the VFD temp-file API (fd.c) ---
//
// `File` is fd.c's virtual file descriptor (`typedef int File`); these are the
// primitives `buffile.c` builds its buffered I/O on. Each `ereport`s on a hard
// failure (carried on `Err`); the success values mirror the C return contract.

seam_core::seam!(
    /// `File OpenTemporaryFile(bool interXact)` (fd.c) — open an anonymous
    /// temporary file in a temp tablespace, registered with the current
    /// resource owner. `interXact` keeps it open across transaction end.
    /// Returns the VFD (`> 0`); open failures `ereport(ERROR)`, carried on `Err`.
    pub fn open_temporary_file(inter_xact: bool) -> types_error::PgResult<File>
);

seam_core::seam!(
    /// `void FileClose(File file)` (fd.c) — close the VFD and, for a temp file,
    /// unlink its backing file. Infallible at the ereport level (errors are
    /// logged at LOG inside fd.c).
    pub fn file_close(file: File)
);

seam_core::seam!(
    /// `ssize_t FileRead(File file, void *buffer, size_t amount, off_t offset,
    /// uint32 wait_event_info)` (fd.c, the single-buffer read buffile uses).
    /// Reads up to `buf.len()` bytes at `offset` into `buf`. Returns the byte
    /// count read (`>= 0`) on success or a negative value on an OS read error
    /// (C returns `-1` with `errno` set); buffile reports `Err` on `< 0`.
    pub fn file_read(file: File, buf: &mut [u8], offset: i64, wait_event_info: u32)
        -> types_error::PgResult<isize>
);

seam_core::seam!(
    /// `ssize_t FileWrite(File file, const void *buffer, size_t amount,
    /// off_t offset, uint32 wait_event_info)` (fd.c, single-buffer write).
    /// Writes `buf` at `offset`. Returns bytes written (`> 0`) or `<= 0` on a
    /// write error (out of space etc.); buffile reports `Err` on `<= 0`. A hard
    /// fd.c failure (e.g. enlarging the temp-file accounting beyond the limit)
    /// is itself an `ereport(ERROR)`, carried on `Err`.
    pub fn file_write(file: File, buf: &[u8], offset: i64, wait_event_info: u32)
        -> types_error::PgResult<isize>
);

seam_core::seam!(
    /// `off_t FileSize(File file)` (fd.c) — the current size of the underlying
    /// OS file. Returns the size (`>= 0`) or a negative value on a stat error
    /// (C returns `-1` with `errno` set).
    pub fn file_size(file: File) -> types_error::PgResult<i64>
);

seam_core::seam!(
    /// `int FileTruncate(File file, off_t offset, uint32 wait_event_info)`
    /// (fd.c) — truncate the underlying OS file to `offset`. Returns `0` on
    /// success or a negative value on failure (C returns `-1` with `errno`).
    pub fn file_truncate(file: File, offset: i64, wait_event_info: u32)
        -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `char *FilePathName(File file)` (fd.c) — the path of the underlying OS
    /// file, used only to build `%m` error messages. Infallible.
    pub fn file_path_name(file: File) -> String
);

seam_core::seam!(
    /// `File PathNameOpenFile(const char *fileName, int fileFlags)` (fd.c) —
    /// allocate a VFD and open the named file into it with the default file
    /// mode. Returns the opened VFD on success; on an open failure it returns a
    /// negative [`File`] (the C `< 0`) with `errno` set (retrievable via
    /// [`last_errno`]) rather than `ereport`ing, exactly like C — the caller
    /// inspects `errno` to decide whether to tolerate the failure or raise its
    /// own `ereport`. `Err` is reserved for the VFD-allocation `ereport(ERROR)`.
    pub fn path_name_open_file(file_name: &str, file_flags: i32)
        -> types_error::PgResult<File>
);

seam_core::seam!(
    /// `lstat(path, &statbuf)` returning the raw `st_mtime` (a `time_t`)
    /// (walsummary.c `RemoveWalSummaryIfOlderThan`). `Ok(None)` mirrors C's
    /// `errno == ENOENT` (the file does not exist); any other `lstat` failure
    /// is the C `ereport(ERROR, errcode_for_file_access, "could not stat
    /// file")`, carried on `Err`. The modification time is returned unconverted
    /// (seconds since the epoch) so the caller can compare it against a
    /// `time_t` cutoff exactly as the C does.
    pub fn lstat_mtime(path: &str) -> types_error::PgResult<Option<i64>>
);

seam_core::seam!(
    /// `pg_mkdir_p(path, pg_dir_create_mode)` (`port/pgmkdirp.c`, reached via
    /// fd.c's directory helpers) — create the directory `path` and any missing
    /// parent directories. Returns `Ok(())` on success; on failure returns the
    /// failing `errno` (the caller, `recovery_create_dbdir`, builds the
    /// `ereport(PANIC)`).
    pub fn pg_mkdir_p(path: &str) -> Result<(), i32>
);

seam_core::seam!(
    /// `rmdir(path)` (libc, reached via movedb in dbcommands.c): remove the
    /// (empty) directory `path`. Returns `Ok(())` on success; on failure returns
    /// the failing `errno` (the caller builds the `elog(ERROR)`).
    pub fn rmdir(path: &str) -> Result<(), i32>
);

seam_core::seam!(
    /// `CreateDirAndVersionFile(dbpath, dbid, tsid, isRedo)` (dbcommands.c) on
    /// the WAL-replay path (`isRedo = true`): `MakePGDirectory(dbpath)`
    /// (tolerating `EEXIST`), then `OpenTransientFile(PG_VERSION, O_WRONLY |
    /// O_CREAT | O_EXCL)` (re-opening `O_TRUNC` when it already exists in
    /// replay), `write` of `PG_MAJORVERSION`, `pg_fsync` + `fsync_fname(dbpath)`
    /// and `CloseTransientFile`. The whole fd-tracked dir+version-file write is
    /// fd/storage-owned; the non-redo `XLogInsert` half stays in the deferred
    /// dbcommands createdb family. `Err` carries the
    /// `errcode_for_file_access` `ereport(ERROR)`s.
    pub fn create_db_dir_and_version_file(
        dbpath: &str,
        dbid: types_core::Oid,
        tsid: types_core::Oid,
        is_redo: bool,
    ) -> PgResult<()>
);
