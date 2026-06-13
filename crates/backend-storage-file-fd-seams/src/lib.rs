//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::SubTransactionId;
use types_error::PgResult;

/// An open `FILE *` registered with the virtual-file-descriptor machinery
/// (`AllocateFile`/`OpenPipeStream`). C's `FILE *` is a genuinely opaque
/// stdio handle, so the owned model carries it as this token; fd.c owns the
/// stream behind it and the read/write/close primitives dispatch on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgFileStream(pub u64);

seam_core::seam!(
    /// `MakePGDirectory(directoryName)` (`storage/file/fd.c`) —
    /// `mkdir(directoryName, pg_dir_create_mode)`. Returns the `mkdir`
    /// result (`0` on success, `-1` with errno set on failure); infallible
    /// at the ereport level.
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
    /// `fwrite(buf, len, 1, copy_file)` + `ferror` for COPY TO to a file or
    /// program pipe (copyto.c:451-483). On a short write/ferror it raises the
    /// "could not write to COPY file/program: %m" `ereport` — for a program,
    /// after the EPIPE `ClosePipeToProgram` path that surfaces the subprocess
    /// exit code. `is_program`/`filename` select that message; carried on
    /// `Err`. Success is `Ok(())`.
    pub fn copy_write_file(
        stream: PgFileStream,
        buf: &[u8],
        is_program: bool,
        filename: Option<&str>,
    ) -> PgResult<()>
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
