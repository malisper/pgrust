//! (`access/transam/xlogarchive.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `RestoreArchivedFile(path, xlogfname, "RECOVERYHISTORY", 0, false)`
    /// (xlogarchive.c) — attempt to restore the file named `xlogfname` from the
    /// archive. On success returns `Some(path)` (the path of the restored file,
    /// allocated in `mcx`); on failure (not in the archive) returns `None`. Can
    /// `ereport` at `ERROR`/`FATAL` on a `restore_command` failure.
    pub fn restore_archived_history_file<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        xlogfname: &str,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `RestoreArchivedFile(path, xlogfname, recovername, expectedSize,
    /// cleanupEnabled)` (xlogarchive.c) — attempt to restore a WAL segment (or
    /// any file) named `xlogfname` from the archive into a temp recovery name.
    /// On success returns `Some(path)` (the restored file's path, in `mcx`); on
    /// failure (not in the archive) returns `None`. Can `ereport` at
    /// `ERROR`/`FATAL` on a `restore_command` failure.
    pub fn restore_archived_file<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        xlogfname: &str,
        recovername: &str,
        expected_size: i64,
        cleanup_enabled: bool,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `KeepFileRestoredFromArchive(path, xlogfname)` (xlogarchive.c) — move a
    /// file just restored from the archive into `pg_wal` under its final name so
    /// it is kept for future reference. Can `ereport` on rename failure.
    pub fn keep_file_restored_from_archive(path: &str, xlogfname: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogArchiveForceDone(fname)` — create a `.done` file to prevent the
    /// segment from being archived later; `ereport(ERROR)` on I/O failure.
    pub fn xlog_archive_force_done(fname: String) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogArchiveNotify(xlog)` (xlogarchive.c) — create the
    /// `archive_status/<xlog>.ready` marker so the archiver picks up the freshly
    /// written file. Can `ereport` on file-create failure.
    pub fn xlog_archive_notify(fname: String) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `XLogArchiveIsReady(xlog)` (xlogarchive.c) — true if the segment has a
    /// pending `archive_status/<xlog>.ready` notification. Pure `stat`.
    pub fn xlog_archive_is_ready(xlog: &str) -> bool
);

seam_core::seam!(
    /// `XLogArchiveCleanup(xlog)` (xlogarchive.c) — remove the segment's
    /// `archive_status/<xlog>.{done,ready}` markers (failures ignored, as in C).
    pub fn xlog_archive_cleanup(xlog: &str)
);
