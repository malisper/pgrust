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
    /// at the ereport level. Use [`last_errno`] for the failure errno.
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
    /// `get_dirent_type(path, de, look_through_symlinks, elevel)` (`fd.c`) —
    /// classify a directory entry. Returns the `PGFileType` code
    /// (`PGFILETYPE_ERROR`=0, `_UNKNOWN`=1, `_REG`=2, `_DIR`=3, `_LNK`=4).
    pub fn get_dirent_type(path: &str) -> i32
);
