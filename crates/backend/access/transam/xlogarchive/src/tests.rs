use std::cell::Cell;
use std::path::Path;
use std::sync::{Mutex, Once, OnceLock};

use crate::*;

thread_local! {
    static TEST_ARCHIVE_MODE: Cell<i32> = const { Cell::new(0) };
    // DST P1: XLogArchiveForceDone now rides fd's durable_rename, whose
    // pg_fsync consults the wal_sync_method GUC slot (fd sync.rs); install
    // the same test accessor fd's own setup uses (fd/src/tests.rs).
    static TEST_WAL_SYNC_METHOD: Cell<i32> = const { Cell::new(0) };
}

fn install_vars() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        pgarch::PgArchShmemInit();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
    });
    guc_tables::vars::XLogArchiveMode.install_if_absent(guc_tables::GucVarAccessors {
        get: || TEST_ARCHIVE_MODE.get(),
        set: |v| TEST_ARCHIVE_MODE.set(v),
    });
    guc_tables::vars::wal_sync_method.install_if_absent(guc_tables::GucVarAccessors {
        get: || TEST_WAL_SYNC_METHOD.get(),
        set: |v| TEST_WAL_SYNC_METHOD.set(v),
    });
}

fn with_wal_cwd(f: impl FnOnce()) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|e| e.into_inner());
    install_vars();
    let dir = std::env::temp_dir().join(format!(
        "xlogarchive-test-{}-{:?}",
        std::process::id(),
        std::time::Instant::now()
    ));
    std::fs::create_dir_all(dir.join("pg_wal/archive_status")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    f();
    let _ = std::env::set_current_dir(std::env::temp_dir());
    let _ = std::fs::remove_dir_all(&dir);
}

const SEG: &str = "000000010000000000000001";

fn ready_path(xlog: &str) -> String {
    format!("pg_wal/archive_status/{xlog}.ready")
}

fn done_path(xlog: &str) -> String {
    format!("pg_wal/archive_status/{xlog}.done")
}

#[test]
fn notify_creates_ready_and_history_forces_dir_scan() {
    with_wal_cwd(|| {
        XLogArchiveNotify(SEG).unwrap();
        assert!(Path::new(&ready_path(SEG)).exists());
        assert!(!Path::new(&done_path(SEG)).exists());

        // A history-file notify forces the archiver's next directory scan.
        XLogArchiveNotify("00000002.history").unwrap();
        assert!(Path::new(&ready_path("00000002.history")).exists());
    });
}

#[test]
fn notify_seg_uses_segment_naming() {
    with_wal_cwd(|| {
        // wal_segment_size boot value is 16MB.
        XLogArchiveNotifySeg(5, 1).unwrap();
        assert!(Path::new(&ready_path("000000010000000000000005")).exists());
    });
}

#[test]
fn force_done_state_machine() {
    with_wal_cwd(|| {
        // .ready -> .done rename.
        std::fs::write(ready_path(SEG), b"").unwrap();
        XLogArchiveForceDone(SEG).unwrap();
        assert!(!Path::new(&ready_path(SEG)).exists());
        assert!(Path::new(&done_path(SEG)).exists());

        // Already done: idempotent, and a reappearing .ready is left alone.
        std::fs::write(ready_path(SEG), b"").unwrap();
        XLogArchiveForceDone(SEG).unwrap();
        assert!(Path::new(&ready_path(SEG)).exists());

        // Neither file: .done created from nothing.
        let other = "000000010000000000000002";
        XLogArchiveForceDone(other).unwrap();
        assert!(Path::new(&done_path(other)).exists());
    });
}

#[test]
fn check_done_state_machine() {
    with_wal_cwd(|| {
        // archive_mode=off: always deletable, no .ready side effect.
        TEST_ARCHIVE_MODE.set(0);
        assert!(XLogArchiveCheckDone(SEG).unwrap());
        assert!(!Path::new(&ready_path(SEG)).exists());

        // archive_mode=always (avoids the recovery-state probe): no status
        // file -> creates .ready and reports not-deletable; .done wins.
        TEST_ARCHIVE_MODE.set(2);
        assert!(!XLogArchiveCheckDone(SEG).unwrap());
        assert!(Path::new(&ready_path(SEG)).exists());
        assert!(!XLogArchiveCheckDone(SEG).unwrap());

        std::fs::rename(ready_path(SEG), done_path(SEG)).unwrap();
        assert!(XLogArchiveCheckDone(SEG).unwrap());
    });
}

#[test]
fn busy_ready_done_probes() {
    with_wal_cwd(|| {
        // No status files and no WAL file: not busy (checkpoint removed it).
        assert!(!XLogArchiveIsBusy(SEG));
        assert!(!XLogArchiveIsReady(SEG));
        assert!(!XLogArchiveIsReadyOrDone(SEG));

        // WAL file present without status files: busy.
        std::fs::write(format!("pg_wal/{SEG}"), b"").unwrap();
        assert!(XLogArchiveIsBusy(SEG));

        std::fs::write(ready_path(SEG), b"").unwrap();
        assert!(XLogArchiveIsBusy(SEG));
        assert!(XLogArchiveIsReady(SEG));
        assert!(XLogArchiveIsReadyOrDone(SEG));

        std::fs::rename(ready_path(SEG), done_path(SEG)).unwrap();
        assert!(!XLogArchiveIsBusy(SEG));
        assert!(!XLogArchiveIsReady(SEG));
        assert!(XLogArchiveIsReadyOrDone(SEG));

        XLogArchiveCleanup(SEG);
        assert!(!Path::new(&done_path(SEG)).exists());
        assert!(!Path::new(&ready_path(SEG)).exists());
    });
}

#[test]
fn build_restore_command_substitution() {
    let cmd = BuildRestoreCommand(
        "cp /arch/%f %p # last %r",
        "pg_wal/RECOVERYXLOG",
        "000000010000000000000003",
        "000000010000000000000001",
    )
    .unwrap();
    assert_eq!(
        cmd,
        "cp /arch/000000010000000000000003 pg_wal/RECOVERYXLOG # last 000000010000000000000001"
    );
}

#[test]
fn restore_archived_file_not_in_archive_recovery() {
    // ArchiveRecoveryRequested=false short-circuits to the pg_wal fallback.
    xlogrecovery_seams::archive_recovery_requested::set(|| false);
    assert_eq!(RestoreArchivedFile(SEG, "RECOVERYXLOG", 0, false).unwrap(), None);
}
