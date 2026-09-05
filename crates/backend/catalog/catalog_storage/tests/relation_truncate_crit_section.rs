//! audit-18.6 b108 (a186-candidate-fp-catalog-storage-cf628fe607fdb4631002-1):
//! storage.c:386-428 RelationTruncate sets DELAY_CHKPT_START|COMPLETE on
//! MyProc and runs the WAL insert, XLogFlush and smgrtruncate inside
//! START_CRIT_SECTION/END_CRIT_SECTION. Any ERROR raised there is a PANIC
//! (elog.c errstart's critical-section promotion): the backend dies and
//! crash recovery resets the flags. The port propagated the Err with `?`
//! out of the still-open critical section; a caller that recovers instead
//! of dying (autovacuum's PG_CATCH, a plpgsql EXCEPTION block around
//! TRUNCATE of a same-transaction table) keeps CritSectionCount > 0 and the
//! delayChkptFlags set for the rest of its life, and every later checkpoint
//! waits on them.
//!
//! Own test binary: the md harness moves the process cwd and PGPROC/critical
//! section state is process-global.

use std::sync::atomic::Ordering::Relaxed;

use types_core::primitive::{ForkNumber, INVALID_PROC_NUMBER};
use types_core::BLCKSZ;
use types_storage::{RelFileLocator, RelFileLocatorBackend, DELAY_CHKPT_COMPLETE, DELAY_CHKPT_START};

fn fork_suffix(forknum: ForkNumber) -> &'static str {
    match forknum {
        ForkNumber::MAIN_FORKNUM => "",
        ForkNumber::FSM_FORKNUM => "_fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "_vm",
        ForkNumber::INIT_FORKNUM => "_init",
        ForkNumber::InvalidForkNumber => panic!("invalid fork"),
    }
}

/// The smgr b095 harness (smgr/tests/common) plus a bound PGPROC for the
/// delayChkptFlags bookkeeping.
fn setup(tag: &str) -> std::path::PathBuf {
    use init_small::globals as g;

    guc_tables::init_seams();
    elog::init_seams();
    // xlog.c owns the wal_level cell (absent in this rig): the default
    // replica level makes the fake permanent relation RelationNeedsWAL.
    static WAL_LEVEL: std::sync::atomic::AtomicI32 =
        std::sync::atomic::AtomicI32::new(transam_xlog::WAL_LEVEL_REPLICA);
    guc_tables::vars::wal_level.install_if_absent(guc_tables::GucVarAccessors {
        get: || WAL_LEVEL.load(Relaxed),
        set: |v| WAL_LEVEL.store(v, Relaxed),
    });
    fd::init_seams();
    smgr::init_seams();

    xact_seams::get_current_sub_transaction_id::set(|| 1);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    pgstat_seams::pgstat_report_tempfile::set(|_| {});
    relpath_seams::relpathbackend::set(|rlocator, _backend, forknum| {
        format!("base/{}/{}{}", rlocator.dbOid, rlocator.relNumber, fork_suffix(forknum))
    });
    sync_seams::register_sync_request::set(|_tag, _ty, _retry| Ok(true));
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));

    // One PGPROC arena, this thread bound to slot 0 (InitProcGlobal sizes
    // from MaxBackends = connections + autovac + workers + wal senders +
    // NUM_SPECIAL_WORKER_PROCS).
    pg_sema_seams::pg_semaphore_create::set(|_| {});
    g::SetMaxConnections(4);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(4 + 3 + 2 + 2 + 2);
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    lmgr_proc::bind_task_proc(0);

    let dir = std::env::temp_dir().join(format!("pgrust_catstor_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(dir.join("base/5")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    fd::InitFileAccess();
    dir
}

#[test]
fn error_inside_truncate_critical_section_is_a_panic() {
    use init_small::globals as g;

    let dir = setup("truncate_crit");
    let locator = RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16384 };
    let key = RelFileLocatorBackend { locator, backend: INVALID_PROC_NUMBER };

    smgr::smgropen(locator, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(key, ForkNumber::MAIN_FORKNUM, false).unwrap();
    let block = [0u8; BLCKSZ];
    smgr::smgrextend(key, ForkNumber::MAIN_FORKNUM, 0, &block, false).unwrap();
    smgr::smgrextend(key, ForkNumber::MAIN_FORKNUM, 1, &block, false).unwrap();

    // The fake entry is a permanent relation (RelationNeedsWAL under the
    // default wal_level): the truncate record insert is the first step
    // inside the critical section, and it fails (out-of-space WAL insert).
    xloginsert_seams::xlog_insert_record::set(|_, _, _, _, _| {
        Err(Box::new(types_error::PgError::error("could not write to WAL: No space left on device")))
    });
    let rel = xlogutils::CreateFakeRelcacheEntry(locator);
    assert_eq!(g::CritSectionCount(), 0);

    // unwind-ok: c-callback (catch boundary stands in for the process exit)
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        catalog_storage::RelationTruncate(&rel, 1)
    }));
    let crit_after = g::CritSectionCount();
    let flags_after = lmgr_proc::GetPGProcByNumber(0).delayChkptFlags.load(Relaxed);
    g::SetCritSectionCount(0);
    lmgr_proc::GetPGProcByNumber(0)
        .delayChkptFlags
        .fetch_and(!(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE), Relaxed);
    let _ = std::fs::remove_dir_all(dir);

    match outcome {
        Err(payload) => assert!(
            payload.is::<types_error::PanicExitThread>(),
            "critical-section error must be C's PANIC (abort), got another panic payload"
        ),
        Ok(Ok(())) => panic!("RelationTruncate succeeded past a failing WAL insert"),
        Ok(Err(e)) => panic!(
            "storage.c:386: ERROR inside the truncate critical section must PANIC, \
             but the port returned a recoverable Err({}) with CritSectionCount={crit_after} \
             and delayChkptFlags={flags_after:#x} still set",
            e.message()
        ),
    }
}
