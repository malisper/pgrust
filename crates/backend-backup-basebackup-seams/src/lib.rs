//! Outward seam declarations for the base-backup driver
//! (`src/backend/backup/basebackup.c`, PostgreSQL 18.3).
//!
//! These are the cross-subsystem calls `basebackup.c` makes whose owners are
//! not yet ported in this repo. Each is declared here (the consumer-owned seam
//! convention) and is installed from the owning subsystem's `init_seams()`
//! once that subsystem lands; until then a call panics loudly
//! (mirror-PG-and-panic).
//!
//! What is *not* declared here (already-landed owners, called directly by the
//! driver): the `bbsink_*` chain (`backend-backup-sink`), the leaf sinks
//! (`backend-backup-copy` / `-server` / `-target` / compression / `-throttle` /
//! `-sink-support` progress), the manifest (`backend-backup-manifest`), the
//! incremental engine (`backend-backup-incremental`), WAL-filename and
//! `CheckXLogRemoved` helpers (`backend-access-transam-xlog`),
//! `build_backup_content` (`backend-access-transam-xlogbackup`), page checksum
//! (`backend-storage-page` / `-page-checksum`), `parse_filename_for_nontemp_relation`
//! (`backend-storage-file-reinit`), `looks_like_temp_rel_name`
//! (`backend-storage-file-fd`), and the file/dir primitives
//! (`backend-storage-file-fd-seams`: `open_transient_file`, `pg_pread`,
//! `read_dir_names`, …). The session/recovery/checksum-enabled flags and
//! `get_backup_status` come from `backend-access-transam-xlog-seams`, the
//! progress + `WalSndSetState` from their owners' seams.

#![allow(non_snake_case)]

use types_error::PgResult;
use backend_backup_sink::TablespaceInfo;

/// `enum tarError` (pgtar.h:19-25) — the result of `tarCreateHeader`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TarError {
    /// `TAR_OK` — header written successfully.
    Ok,
    /// `TAR_NAME_TOO_LONG` — file name too long for the tar format.
    NameTooLong,
    /// `TAR_SYMLINK_TOO_LONG` — symlink target too long for the tar format.
    SymlinkTooLong,
}

/// `TAR_BLOCK_SIZE` (pgtar.h:17).
pub const TAR_BLOCK_SIZE: usize = 512;

/// The 512-byte tar header produced by [`tar_create_header`].
#[derive(Clone, Copy)]
pub struct TarHeader {
    /// `enum tarError` return code.
    pub rc: TarError,
    /// The 512 header bytes written into `h` (valid only when `rc == Ok`).
    pub bytes: [u8; TAR_BLOCK_SIZE],
}

seam_core::seam!(
    /// `tarCreateHeader(h, filename, linktarget, size, mode, uid, gid, mtime)`
    /// (`src/common/file_utils.c`/`pgtar.c`) — render a 512-byte tar member
    /// header. Returns the result code together with the rendered header bytes.
    /// `linktarget == None` matches the C `NULL`. `pgtar.c` is unported, so this
    /// panics until that unit lands.
    pub fn tar_create_header(
        filename: &str,
        linktarget: Option<&str>,
        size: i64,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime: i64,
    ) -> TarHeader
);

/// The parts of `struct stat` that `basebackup.c` consumes (via `lstat`).
#[derive(Clone, Copy, Debug)]
pub struct LstatInfo {
    /// `statbuf.st_size`.
    pub size: i64,
    /// `statbuf.st_mode` (the full mode word, including the file-type bits).
    pub mode: u32,
    /// `statbuf.st_uid`.
    pub uid: u32,
    /// `statbuf.st_gid`.
    pub gid: u32,
    /// `statbuf.st_mtime`.
    pub mtime: i64,
}

seam_core::seam!(
    /// `lstat(path, &statbuf)` — stat a path *without* following symlinks.
    /// Returns `Ok(Some(info))` on success, `Ok(None)` when `errno == ENOENT`
    /// (the caller decides whether a vanished file is an error), and `Err` for
    /// any other failure (the C `errcode_for_file_access()` `ereport(ERROR)`).
    /// The raw `lstat`/errno surface is not yet owned by a ported unit.
    pub fn lstat_file(path: &str) -> PgResult<Option<LstatInfo>>
);

seam_core::seam!(
    /// `readlink(path, buf, sizeof(buf))` — read a symbolic link's target.
    /// `Ok(Some(target))` on success; `Err` carries the C
    /// `errcode_for_file_access()` read error or the `ERRCODE_PROGRAM_LIMIT_EXCEEDED`
    /// "target is too long" error (`rllen >= MAXPGPATH`).
    pub fn read_link(path: &str) -> PgResult<String>
);

seam_core::seam!(
    /// `geteuid()` — effective user id, used to build the injected `stat` for
    /// `sendFileWithContent`. (On Windows the C code uses `0`.)
    pub fn geteuid() -> u32
);

seam_core::seam!(
    /// `getegid()` — effective group id (see [`geteuid`]).
    pub fn getegid() -> u32
);

seam_core::seam!(
    /// `time(NULL)` — current wall-clock time in seconds, for the injected file
    /// `st_mtime` in `sendFileWithContent`.
    pub fn time_now() -> i64
);

seam_core::seam!(
    /// `pg_file_create_mode` (`common/file_perm.c` global) — default file
    /// creation mode for the injected file in `sendFileWithContent`.
    pub fn pg_file_create_mode() -> u32
);

seam_core::seam!(
    /// `pg_dir_create_mode` (`common/file_perm.c` global) — default directory
    /// creation mode, used by `convert_link_to_directory` to fabricate the
    /// `S_IFDIR | pg_dir_create_mode` mode for a symlinked directory.
    pub fn pg_dir_create_mode() -> u32
);

/// The metadata `do_pg_backup_start` fills in for the base-backup driver: the
/// freshly-populated [`BackupState`](types_wal::BackupState), the list of
/// tablespaces to back up (the C `tablespaces` out-list, dropped by the
/// `pg_backup_start()` SQL path but required by `basebackup.c`), and the
/// rendered `tablespace_map` file bytes.
pub struct BackupStartResult {
    /// The `BackupState` filled by `do_pg_backup_start`.
    pub state: types_wal::BackupState,
    /// `List *tablespaces` — one entry per auxiliary tablespace (PGDATA's own
    /// entry is appended by the caller).
    pub tablespaces: Vec<TablespaceInfo>,
    /// The rendered `tablespace_map` file contents (the C `StringInfo`).
    pub tablespace_map: Vec<u8>,
}

seam_core::seam!(
    /// `do_pg_backup_start(backupidstr, fast, &tablespaces, state, tblspcmapfile)`
    /// (xlog.c:8866) — the base-backup variant: unlike the SQL `pg_backup_start()`
    /// path (which passes `tablespaces = NULL`), `basebackup.c` needs the
    /// tablespace out-list. Writes a checkpoint, fills `state`, enumerates the
    /// tablespaces, and renders the `tablespace_map` file. Can `ereport(ERROR)`.
    ///
    /// The repo's `backend-access-transam-xlog-seams::do_pg_backup_start` drops
    /// the tablespace list (it only serves `pg_backup_start()`); this richer
    /// variant is declared for the base-backup consumer and installed by the
    /// same xlog owner when it lands.
    pub fn do_pg_backup_start_for_basebackup(
        backupidstr: &str,
        fast: bool,
    ) -> PgResult<BackupStartResult>
);

seam_core::seam!(
    /// `do_pg_abort_backup(code, arg)` (xlog.c) — the `PG_ENSURE_ERROR_CLEANUP`
    /// handler that decrements the backup counter if a base backup fails between
    /// `do_pg_backup_start()` and `do_pg_backup_stop()`. `arg` mirrors the C
    /// `BoolGetDatum(false)` "during an error" flag (`emit_warning`).
    pub fn do_pg_abort_backup(emit_warning: bool)
);
