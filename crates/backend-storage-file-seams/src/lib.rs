//! Seam declarations for the `backend-storage-file` unit
//! (`storage/file/fd.c` and friends).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::{ErrorLevel, PgResult};

seam_core::seam!(
    /// The `AllocateDir(dirname)` / `ReadDir(dir, dirname)` / `FreeDir(dir)`
    /// triple (`storage/file/fd.c`) as one owned walk: the owner opens the
    /// directory through the fd bookkeeping layer, invokes `f` with each
    /// entry's `d_name`, and closes the directory on every path (including
    /// when `f` errors) — the `DIR *` never crosses the seam, so there is no
    /// bare-token release to leak. `Err` carries `AllocateDir`'s
    /// `ereport(ERROR, "exceeded maxAllocatedDescs ...")`, `ReadDir`'s
    /// could-not-open / could-not-read `ereport(ERROR)` (as in C, an open
    /// failure surfaces from the first `ReadDir` call, naming `dirname`), or
    /// an `Err` from `f` itself. `f` returns `Ok(true)` to stop the scan
    /// early (the C callers' `break` on a true callback result); the seam
    /// returns the last callback value (`false` when the directory was
    /// exhausted), mirroring `SlruScanDirectory`'s contract.
    pub fn with_allocated_dir(
        dirname: &str,
        f: &mut dyn FnMut(&str) -> PgResult<bool>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `OpenTransientFile(const char *fileName, int fileFlags)` — open a file
    /// tracked for transaction-end cleanup. Returns the kernel fd, or `-1`
    /// on open failure (with `errno` set). `Err` carries the
    /// `ereport(ERROR, "exceeded maxAllocatedDescs ...")`.
    pub fn open_transient_file(file_name: &str, file_flags: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `CloseTransientFile(int fd)` — returns the `close()` result.
    pub fn close_transient_file(fd: i32) -> i32
);

seam_core::seam!(
    /// `ReserveExternalFD()` — count one externally-consumed FD against
    /// `max_safe_fds`, releasing LRU virtual FDs if needed.
    pub fn reserve_external_fd()
);

seam_core::seam!(
    /// `ReleaseExternalFD()` — release a reservation made with
    /// `reserve_external_fd`.
    pub fn release_external_fd()
);

seam_core::seam!(
    /// `pg_fsync(int fd)` (`storage/file/fd.c`) — fsync honoring the
    /// `wal_sync_method` writethrough setting. Returns the fsync result
    /// (`0` on success, `-1` with `errno` set); infallible at the ereport
    /// level (its only report is a DATA_CORRUPTION warning on a
    /// non-syncable fd in assert builds).
    pub fn pg_fsync(fd: i32) -> i32
);

seam_core::seam!(
    /// `fsync_fname(fname, isdir)` (`storage/file/fd.c`) — fsync a file or
    /// directory, ereporting on failure at `data_sync_elevel(ERROR)` (so
    /// `Err` may carry ERROR or PANIC).
    pub fn fsync_fname(fname: &str, isdir: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `data_sync_elevel(int elevel)` (`storage/file/fd.c`) — the severity to
    /// report data-sync failures at: the given level if `data_sync_retry`,
    /// otherwise PANIC.
    pub fn data_sync_elevel(elevel: types_error::ErrorLevel) -> types_error::ErrorLevel
);
