use types_core::{TimeLineID, XLogSegNo};
use types_error::PgResult;

seam_core::seam!(
    // XLogArchiveNotify (xlogarchive.c).
    pub fn xlog_archive_notify(xlog: &str) -> PgResult<()>
);

seam_core::seam!(
    // XLogArchiveNotifySeg (xlogarchive.c).
    pub fn xlog_archive_notify_seg(segno: XLogSegNo, tli: TimeLineID) -> PgResult<()>
);

seam_core::seam!(
    // XLogArchivingActive() (xlog.h macro; installed by xlogarchive for
    // consumers below transam_xlog).
    pub fn xlog_archiving_active() -> bool
);

seam_core::seam!(
    // RestoreArchivedFile (xlogarchive.c): Some(restored temp path under
    // pg_wal) on success; None = caller falls back to pg_wal/<xlogfname>.
    pub fn restore_archived_file(
        xlogfname: &str,
        recovername: &str,
        expected_size: i64,
        cleanup_enabled: bool,
    ) -> PgResult<Option<String>>
);

seam_core::seam!(
    // XLogArchiveCheckDone (xlogarchive.c).
    pub fn xlog_archive_check_done(xlog: &str) -> PgResult<bool>
);

seam_core::seam!(
    // KeepFileRestoredFromArchive (xlogarchive.c).
    pub fn keep_file_restored_from_archive(path: &str, xlogfname: &str) -> PgResult<()>
);

seam_core::seam!(
    // XLogArchiveCleanup (xlogarchive.c).
    pub fn xlog_archive_cleanup(xlog: &str)
);

seam_core::seam!(
    // XLogArchiveIsReady (xlogarchive.c).
    pub fn xlog_archive_is_ready(xlog: &str) -> bool
);

seam_core::seam!(
    // XLogArchiveIsReadyOrDone (xlogarchive.c).
    pub fn xlog_archive_is_ready_or_done(xlog: &str) -> bool
);

seam_core::seam!(
    // ExecuteRecoveryCommand (xlogarchive.c).
    pub fn execute_recovery_command(
        command: &str,
        command_name: &str,
        fail_on_signal: bool,
        wait_event_info: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    // XLogArchiveIsBusy(xlog) (xlogarchive.c).
    pub fn xlog_archive_is_busy(xlog: &str) -> bool
);
