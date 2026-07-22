use std::sync::Once;

use types_core::{ForkNumber, BLCKSZ, INVALID_PROC_NUMBER};
use types_storage::sync::{FileTag, SyncRequestHandler, SyncRequestType};
use types_storage::{RelFileLocator, RelFileLocatorBackend};

use super::*;

fn fork_suffix(forknum: ForkNumber) -> &'static str {
    match forknum {
        ForkNumber::MAIN_FORKNUM => "",
        ForkNumber::FSM_FORKNUM => "_fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "_vm",
        ForkNumber::INIT_FORKNUM => "_init",
        ForkNumber::InvalidForkNumber => panic!("invalid fork"),
    }
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        smgr::init_seams();
        crate::init_seams();

        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        relpath_seams::relpathbackend::set(|rlocator, _backend, forknum| {
            format!(
                "base/{}/{}{}",
                rlocator.dbOid,
                rlocator.relNumber,
                fork_suffix(forknum)
            )
        });
        relpath_seams::relpathperm::set(|rlocator, forknum| {
            format!(
                "base/{}/{}{}",
                rlocator.dbOid,
                rlocator.relNumber,
                fork_suffix(forknum)
            )
        });
        tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));
        // fdatasync (PLATFORM_DEFAULT_WAL_SYNC_METHOD on macOS): fd's
        // writethrough branch stays cold.
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || 2,
            set: |_| {},
        });

        let dir = std::env::temp_dir().join(format!("pgrust_sync_{}", std::process::id()));
        std::fs::create_dir_all(dir.join("base/5")).unwrap();
        std::env::set_current_dir(&dir).unwrap();
    });
    // Per test thread: VFD cache, pendingOps and GUC mirrors are all TLS.
    init_small::globals::set_enableFsync(true);
    fd::InitFileAccess();
    InitSync().unwrap();
}

fn key(rel: u32) -> RelFileLocatorBackend {
    RelFileLocatorBackend {
        locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: rel },
        backend: INVALID_PROC_NUMBER,
    }
}

fn create_rel_with_block(rel: u32) -> RelFileLocatorBackend {
    let k = key(rel);
    smgr::smgropen(k.locator, k.backend).unwrap();
    smgr::smgrcreate(k, ForkNumber::MAIN_FORKNUM, false).unwrap();
    let block = [0x5Au8; BLCKSZ];
    smgr::smgrextend(k, ForkNumber::MAIN_FORKNUM, 0, &block, false).unwrap();
    k
}

fn md_tag(rel: u32) -> FileTag {
    FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        ForkNumber::MAIN_FORKNUM,
        key(rel).locator,
        0,
    )
}

#[test]
fn extend_registers_and_process_drains() {
    setup();
    create_rel_with_block(20001);
    let (fsyncs, _) = pending_counts();
    assert!(fsyncs >= 1, "smgrextend registered a dirty segment");
    ProcessSyncRequests().unwrap();
    assert_eq!(pending_counts().0, 0);
}

#[test]
fn duplicate_requests_merge() {
    setup();
    let tag = md_tag(20002);
    RegisterSyncRequest(tag, SyncRequestType::SYNC_REQUEST, false).unwrap();
    RegisterSyncRequest(tag, SyncRequestType::SYNC_REQUEST, false).unwrap();
    assert_eq!(pending_counts().0, 1);
    RegisterSyncRequest(tag, SyncRequestType::SYNC_FORGET_REQUEST, true).unwrap();
    // Canceled entry: removed without touching the (nonexistent) file.
    ProcessSyncRequests().unwrap();
    assert_eq!(pending_counts().0, 0);
}

#[test]
fn missing_file_fails_after_retry() {
    setup();
    // data_sync_retry=on keeps fsync failure at ERROR; default promotes to
    // PANIC (process abort, covered by fsync_failure_aborts_process).
    fd::vfd::set_data_sync_retry(true);
    let tag = md_tag(20003);
    RegisterSyncRequest(tag, SyncRequestType::SYNC_REQUEST, false).unwrap();
    let err = ProcessSyncRequests().unwrap_err();
    assert!(
        format!("{err:?}").contains("could not fsync file"),
        "got: {err:?}"
    );
}

#[test]
fn filter_cancels_matching_database() {
    setup();
    RegisterSyncRequest(md_tag(20004), SyncRequestType::SYNC_REQUEST, false).unwrap();
    RegisterSyncRequest(md_tag(20005), SyncRequestType::SYNC_REQUEST, false).unwrap();
    // ForgetDatabaseSyncRequests' shape: dbOid-keyed FILTER cancels both.
    smgr::ForgetDatabaseSyncRequests(5).unwrap();
    ProcessSyncRequests().unwrap();
    assert_eq!(pending_counts().0, 0);
}

#[test]
fn filter_leaves_other_database() {
    setup();
    fd::vfd::set_data_sync_retry(true);
    RegisterSyncRequest(md_tag(20006), SyncRequestType::SYNC_REQUEST, false).unwrap();
    smgr::ForgetDatabaseSyncRequests(999).unwrap();
    assert!(ProcessSyncRequests().is_err(), "uncanceled missing file still fsyncs");
}

#[test]
fn unlink_waits_for_next_checkpoint_cycle() {
    setup();
    let k = create_rel_with_block(20007);
    ProcessSyncRequests().unwrap();
    let path = format!("base/{}/{}", k.locator.dbOid, k.locator.relNumber);
    assert!(std::path::Path::new(&path).exists());

    RegisterSyncRequest(md_tag(20007), SyncRequestType::SYNC_UNLINK_REQUEST, true).unwrap();
    // Same cycle: the entry is new, nothing is unlinked yet.
    SyncPostCheckpoint().unwrap();
    assert!(std::path::Path::new(&path).exists());
    assert_eq!(pending_counts().1, 1);

    SyncPreCheckpoint().unwrap();
    SyncPostCheckpoint().unwrap();
    assert!(!std::path::Path::new(&path).exists());
    assert_eq!(pending_counts().1, 0);
}

#[test]
#[ignore = "abort child of fsync_failure_aborts_process"]
fn fsync_failure_abort_child() {
    setup();
    RegisterSyncRequest(md_tag(20009), SyncRequestType::SYNC_REQUEST, false).unwrap();
    // data_sync_retry=off: the fsync-failure ereport must not return control
    // (fsyncgate); elog PANIC unwinds PanicExitThread under the thread model
    // (notes/crash-restart-design.md) and the postmaster maps it to
    // WTERMSIG(SIGABRT).
    let r = std::panic::catch_unwind(|| ProcessSyncRequests());
    match r {
        Err(payload) if payload.is::<types_error::PanicExitThread>() => std::process::exit(42),
        _ => std::process::exit(0),
    }
}

#[test]
fn fsync_failure_aborts_process() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["tests::fsync_failure_abort_child", "--exact", "--ignored", "--test-threads=1"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(42), "child must unwind PanicExitThread: {out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("PANIC:  could not fsync file"), "missing PANIC report: {stderr}");
}
