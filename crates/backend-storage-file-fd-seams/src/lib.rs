//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::SubTransactionId;

seam_core::seam!(
    /// `MakePGDirectory(directoryName)` (`storage/file/fd.c`) —
    /// `mkdir(directoryName, pg_dir_create_mode)`. Returns the `mkdir`
    /// result (`0` on success, `-1` with errno set on failure); infallible
    /// at the ereport level.
    pub fn make_pg_directory(directory_name: &str) -> i32
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
    /// Atomically emplace a finished file: write `content` to a temp file under
    /// `XLOGDIR`, `pg_fsync` it, then `durable_rename` it to `final_path`
    /// (`OpenTransientFile`/`write`/`pg_fsync`/`CloseTransientFile`/
    /// `durable_rename`). `replace_existing == false` asserts the destination
    /// did not already exist (`writeTimeLineHistory`); `true` replaces any
    /// existing file (`writeTimeLineHistoryFile`). Raises `ERROR` on I/O
    /// failure.
    pub fn durable_write_file(
        final_path: &str,
        content: &[u8],
        replace_existing: bool,
    ) -> types_error::PgResult<()>
);
