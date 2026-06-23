//! Seam declarations for the raw POSIX filesystem syscalls that
//! `commands/tablespace.c` issues directly on tablespace directories and
//! symlinks: `stat`/`lstat` (with `S_ISDIR`/`S_ISLNK` classification),
//! `MakePGDirectory`/`pg_mkdir_p` (`mkdir` with `pg_dir_create_mode`),
//! `chmod`, `symlink`, `rmdir`, and `unlink`.
//!
//! The owner (`storage/file/fd.c` + `src/port`) is not yet ported with these
//! raw primitives; calls panic until they land. Errors are reported as the
//! C-style `errno` integer so the caller can reproduce tablespace.c's exact
//! `errno == ENOENT`/`EEXIST` branching and `errcode_for_file_access()`
//! mapping.

/// `errno == ENOENT` (file_perm / errno.h).
pub const ENOENT: i32 = 2;
/// `errno == EEXIST`.
pub const EEXIST: i32 = 17;

/// What kind of filesystem object a `stat`/`lstat` found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatKind {
    /// `S_ISDIR(st.st_mode)`.
    Dir,
    /// `S_ISLNK(st.st_mode)` — meaningful only for `lstat`.
    Symlink,
    /// Anything else (regular file, socket, …).
    Other,
}

/// The result of a `stat`/`lstat`: success with the object kind, or a failure
/// carrying the `errno`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatResult {
    Found(StatKind),
    Failed(i32),
}

seam_core::seam!(
    /// `stat(path, &st)` — follows symlinks. `Found(kind)` on success
    /// (`kind` from `S_ISDIR`), `Failed(errno)` on `< 0`.
    pub fn stat(path: &str) -> types_error::PgResult<StatResult>
);

seam_core::seam!(
    /// `lstat(path, &st)` — does NOT follow symlinks; distinguishes
    /// `Symlink` from `Dir`.
    pub fn lstat(path: &str) -> types_error::PgResult<StatResult>
);

seam_core::seam!(
    /// `MakePGDirectory(directoryName)` (fd.c) — `mkdir` with
    /// `pg_dir_create_mode`. `Ok(())` on success, `Err(errno)` on `< 0`.
    pub fn make_pg_directory(path: &str) -> types_error::PgResult<Result<(), i32>>
);

seam_core::seam!(
    /// `pg_mkdir_p(path, pg_dir_create_mode)` (src/port/pgmkdirp.c) — create
    /// every missing parent. `Ok(())` on success, `Err(errno)` on `< 0`.
    pub fn pg_mkdir_p(path: &str) -> types_error::PgResult<Result<(), i32>>
);

seam_core::seam!(
    /// `chmod(path, pg_dir_create_mode)`. `Ok(())` on success, `Err(errno)`
    /// otherwise.
    pub fn chmod_dir(path: &str) -> types_error::PgResult<Result<(), i32>>
);

seam_core::seam!(
    /// `symlink(oldpath, newpath)` — create `newpath` pointing at `oldpath`.
    /// `Ok(())` on success, `Err(errno)` on `< 0`.
    pub fn symlink(oldpath: &str, newpath: &str) -> types_error::PgResult<Result<(), i32>>
);

seam_core::seam!(
    /// `rmdir(path)`. `Ok(())` on success, `Err(errno)` on `< 0`.
    pub fn rmdir(path: &str) -> types_error::PgResult<Result<(), i32>>
);

seam_core::seam!(
    /// `unlink(path)`. `Ok(())` on success, `Err(errno)` on `< 0`.
    pub fn unlink(path: &str) -> types_error::PgResult<Result<(), i32>>
);
