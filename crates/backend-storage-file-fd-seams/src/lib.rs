//! Seam declarations for the `backend-storage-file-fd` unit
//! (`storage/file/fd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::SubTransactionId;
use types_error::PgResult;

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
