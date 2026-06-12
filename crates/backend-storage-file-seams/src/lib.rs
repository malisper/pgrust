//! Seam declarations for the `backend-storage-file` unit
//! (`storage/file/fd.c` and friends).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! `DIR *` handles are carried as opaque `u64` tokens (`0` is the C `NULL`);
//! the owner mints and resolves them.

use types_error::PgResult;

seam_core::seam!(
    /// `AllocateDir(const char *dirname)` — open a directory through the fd
    /// bookkeeping layer. Returns the opaque `DIR *` token, `0` on open
    /// failure (with `errno` left set, as in C). `Err` carries the
    /// `ereport(ERROR, "exceeded maxAllocatedDescs ...")`.
    pub fn allocate_dir(dirname: &str) -> PgResult<u64>
);

seam_core::seam!(
    /// `ReadDir(DIR *dir, const char *dirname)` — next entry's `d_name`, or
    /// `None` at end of directory. `Err` carries the `ereport(ERROR)` for a
    /// read failure or a `0` (NULL) dir token.
    pub fn read_dir(dir: u64, dirname: &str) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `FreeDir(DIR *dir)` — close a directory opened with `allocate_dir`;
    /// returns the `closedir()` result (`0` tokens are a no-op returning 0).
    pub fn free_dir(dir: u64) -> i32
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
