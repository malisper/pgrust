use super::*;
use init_small::globals as g;
use std::sync::atomic::{AtomicBool, AtomicU32 as StdAtomicU32};
use std::sync::{Mutex, Once, OnceLock};
use types_core::BackendType;

static IN_PARALLEL: AtomicBool = AtomicBool::new(false);
static IN_BOOTSTRAP: AtomicBool = AtomicBool::new(false);
static IN_RECOVERY_SEAM: AtomicBool = AtomicBool::new(false);
static DB_EXISTS: AtomicBool = AtomicBool::new(true);
static DB_NAME_LOOKUPS: StdAtomicU32 = StdAtomicU32::new(0);
static LOGGED_NEXT_OID: StdAtomicU32 = StdAtomicU32::new(0);
static EXTEND_SUBTRANS_CALLS: StdAtomicU32 = StdAtomicU32::new(0);
static EXTEND_COMMIT_TS_CALLS: StdAtomicU32 = StdAtomicU32::new(0);

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("varsup_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_xact")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        g::SetMaxConnections(8);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(17);
        g::SetMyProcPid(4242);
        g::set_transaction_buffers(64);

        shmem_seams::shmem_init_struct::set(|name, size| {
            let mut reg = shmem_registry().lock().unwrap();
            if let Some(&addr) = reg.get(name) {
                return Ok((std::ptr::with_exposed_provenance_mut(addr), true));
            }
            let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
            let p = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!p.is_null());
            reg.insert(name.to_string(), p.expose_provenance());
            Ok((p, false))
        });
        shmem_seams::add_size::set(|a, b| Ok(a + b));
        shmem_seams::mul_size::set(|a, b| Ok(a * b));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });

        file_seams::open_transient_file::set(|name, flags| {
            let c = std::ffi::CString::new(name).unwrap();
            Ok(unsafe { libc::open(c.as_ptr(), flags, 0o600 as libc::c_uint) })
        });
        file_seams::close_transient_file::set(|fd| unsafe { libc::close(fd) });
        file_seams::pg_fsync::set(|fd| unsafe { libc::fsync(fd) });
        file_seams::fsync_fname::set(|_, _| Ok(()));
        file_seams::data_sync_elevel::set(|e| e);
        file_seams::with_allocated_dir::set(|dirname, cb| {
            let mut ret = false;
            for entry in std::fs::read_dir(dirname).unwrap() {
                ret = cb(entry.unwrap().file_name().to_str().unwrap())?;
                if ret {
                    break;
                }
            }
            Ok(ret)
        });
        sync_seams::register_sync_request::set(|_, _, _| Ok(true));

        pgstat_seams::pgstat_get_slru_index::set(|_| 0);
        pgstat_seams::pgstat_count_slru_page_zeroed::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_hit::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_read::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_written::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_exists::set(|_| {});
        pgstat_seams::pgstat_count_slru_flush::set(|_| {});
        pgstat_seams::pgstat_count_slru_truncate::set(|_| {});
        pgstat_seams::pgstat_count_checkpointer_slru_written::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});

        xlogutils_seams::in_recovery::set(|| false);
        transam_xlog_seams::xlog_flush::set(|_| Ok(()));
        transam_xlog_seams::count_ckpt_slru_written::set(|| {});
        transam_xlog_seams::recovery_in_progress::set(|| {
            IN_RECOVERY_SEAM.load(Relaxed)
        });
        transam_xlog_seams::xlog_put_next_oid::set(|next_oid| {
            LOGGED_NEXT_OID.store(next_oid, Relaxed);
            Ok(())
        });
        xloginsert_seams::xlog_insert::set(|_, _, _| Ok(0x1000));

        xact_seams::is_in_parallel_mode::set(|| IN_PARALLEL.load(Relaxed));
        xact_seams::is_transaction_state::set(|| false);
        miscinit_seams::is_bootstrap_processing_mode::set(|| IN_BOOTSTRAP.load(Relaxed));
        subtrans_seams::extend_subtrans::set(|_| {
            EXTEND_SUBTRANS_CALLS.fetch_add(1, Relaxed);
            Ok(())
        });
        commit_ts_seams::extend_commit_ts::set(|_| {
            EXTEND_COMMIT_TS_CALLS.fetch_add(1, Relaxed);
            Ok(())
        });
        dbcommands_seams::get_database_name::set(|_| {
            DB_NAME_LOOKUPS.fetch_add(1, Relaxed);
            Ok(Some("testdb".to_string()))
        });
        syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(DB_EXISTS.load(Relaxed)));

        pg_sema_seams::pg_semaphore_create::set(|_| {});
        pg_sema_seams::pg_semaphore_reset::set(|_| {});
        pg_sema_seams::pg_semaphore_lock::set(|_| {});
        pg_sema_seams::pg_semaphore_unlock::set(|_| {});
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        latch_seams::own_latch::set(|_| {});
        latch_seams::disown_latch::set(|_| {});
        latch_seams::set_latch::set(|_| {});
        latch_seams::set_latch_my_latch::set(|| {});
        latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
        latch_seams::reset_latch_my_latch::set(|| {});
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        autovacuum_seams::wake_autovacuum_launcher::set(|| {});
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        lock_seams::lock_release_all::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});
        procarray_seams::proc_array_add::set(|_| Ok(()));
        procarray_seams::proc_array_remove::set(|_, _| Ok(()));

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });

        init_seams();
        clog::init_seams();
        VarsupShmemInit();
        clog::CLOGShmemInit().unwrap();
        clog::BootStrapCLOG().unwrap();
    });
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_proc() {
    if MyProc().is_none() {
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    }
}

fn reset_proc_xid_state() {
    let proc = my_proc();
    proc.xid.value.store(types_core::InvalidTransactionId, Relaxed);
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;
    ProcGlobal().xids[pgxactoff]
        .value
        .store(types_core::InvalidTransactionId, Relaxed);
    proc.subxidStatus.set(Default::default());
    ProcGlobal().subxidStates[pgxactoff].set(Default::default());
}

#[test]
fn shmem_size_matches_struct() {
    let _l = test_lock();
    setup();

    // The procarray re-export identity is proven by procarray's own tests
    // (cargo test-lib duplication makes it untestable from here).
    assert_eq!(VarsupShmemSize(), core::mem::size_of::<TransamVariablesShared>());
}

#[test]
fn get_new_transaction_id_advances_and_publishes() {
    let _l = test_lock();
    setup();
    with_proc();
    reset_proc_xid_state();

    let tv = TransamVariables();
    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, 100).value, Relaxed);
    let sub0 = EXTEND_SUBTRANS_CALLS.load(Relaxed);
    let cts0 = EXTEND_COMMIT_TS_CALLS.load(Relaxed);

    let full = GetNewTransactionId(false).unwrap();
    assert_eq!(full.xid(), 100);
    assert_eq!(FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid(), 101);

    let proc = my_proc();
    assert_eq!(proc.xid.read(), 100);
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;
    assert_eq!(ProcGlobal().xids[pgxactoff].read(), 100);
    assert_eq!(EXTEND_SUBTRANS_CALLS.load(Relaxed), sub0 + 1);
    assert_eq!(EXTEND_COMMIT_TS_CALLS.load(Relaxed), cts0 + 1);
    assert_eq!(ReadNextTransactionId().unwrap(), 101);

    reset_proc_xid_state();
}

#[test]
fn subxact_ids_fill_cache_then_overflow() {
    let _l = test_lock();
    setup();
    with_proc();
    reset_proc_xid_state();

    let proc = my_proc();
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;
    proc.xid.value.store(200, Relaxed);
    ProcGlobal().xids[pgxactoff].value.store(200, Relaxed);

    let full = GetNewTransactionId(true).unwrap();
    assert_eq!(proc.subxidStatus.get().count, 1);
    assert_eq!(proc.subxids.get().xids[0], full.xid());
    assert_eq!(ProcGlobal().subxidStates[pgxactoff].get().count, 1);

    // fill the cache to the brim, then one more overflows
    let mut status = proc.subxidStatus.get();
    status.count = PGPROC_MAX_CACHED_SUBXIDS as u8;
    proc.subxidStatus.set(status);
    ProcGlobal().subxidStates[pgxactoff].set(status);

    GetNewTransactionId(true).unwrap();
    assert!(proc.subxidStatus.get().overflowed);
    assert!(ProcGlobal().subxidStates[pgxactoff].get().overflowed);

    reset_proc_xid_state();
}

#[test]
fn bootstrap_parallel_and_recovery_modes() {
    let _l = test_lock();
    setup();
    with_proc();
    reset_proc_xid_state();

    IN_BOOTSTRAP.store(true, Relaxed);
    let full = GetNewTransactionId(false).unwrap();
    assert_eq!(full.xid(), BootstrapTransactionId);
    assert_eq!(my_proc().xid.read(), BootstrapTransactionId);
    IN_BOOTSTRAP.store(false, Relaxed);

    IN_PARALLEL.store(true, Relaxed);
    assert!(GetNewTransactionId(false).is_err());
    IN_PARALLEL.store(false, Relaxed);

    IN_RECOVERY_SEAM.store(true, Relaxed);
    assert!(GetNewTransactionId(false).is_err());
    assert!(GetNewObjectId().is_err());
    IN_RECOVERY_SEAM.store(false, Relaxed);

    reset_proc_xid_state();
}

#[test]
fn advance_next_full_transaction_id_infers_epoch() {
    let _l = test_lock();
    setup();

    let tv = TransamVariables();
    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, 1000).value, Relaxed);

    // below nextXid: no-op
    AdvanceNextFullTransactionIdPastXid(500).unwrap();
    assert_eq!(FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid(), 1000);

    AdvanceNextFullTransactionIdPastXid(2000).unwrap();
    let next = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
    assert_eq!((next.epoch(), next.xid()), (0, 2001));

    // xid wraps past 2^32: epoch bumps
    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, 0xFFFF_FFF0).value, Relaxed);
    AdvanceNextFullTransactionIdPastXid(5).unwrap();
    let next = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
    assert_eq!((next.epoch(), next.xid()), (1, 6));
}

#[test]
fn advance_oldest_clog_xid_moves_forward_only() {
    let _l = test_lock();
    setup();

    let tv = TransamVariables();
    tv.oldestClogXid.store(100, Relaxed);
    AdvanceOldestClogXid(500).unwrap();
    assert_eq!(tv.oldestClogXid.load(Relaxed), 500);
    AdvanceOldestClogXid(200).unwrap();
    assert_eq!(tv.oldestClogXid.load(Relaxed), 500);
}

#[test]
fn set_transaction_id_limit_computes_c_limits() {
    let _l = test_lock();
    setup();

    let tv = TransamVariables();
    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, 1000).value, Relaxed);
    let oldest: TransactionId = 1000;
    SetTransactionIdLimit(oldest, 7).unwrap();

    let wrap = oldest.wrapping_add(MaxTransactionId >> 1);
    assert_eq!(tv.oldestXid.load(Relaxed), oldest);
    assert_eq!(tv.oldestXidDB.load(Relaxed), 7);
    assert_eq!(tv.xidWrapLimit.load(Relaxed), wrap);
    assert_eq!(tv.xidStopLimit.load(Relaxed), wrap - 3_000_000);
    assert_eq!(tv.xidWarnLimit.load(Relaxed), wrap - 40_000_000);
    assert_eq!(
        tv.xidVacLimit.load(Relaxed),
        oldest + g::autovacuum_freeze_max_age() as u32
    );

    DB_EXISTS.store(true, Relaxed);
    assert!(!ForceTransactionIdLimitUpdate().unwrap());
    DB_EXISTS.store(false, Relaxed);
    assert!(ForceTransactionIdLimitUpdate().unwrap());
    DB_EXISTS.store(true, Relaxed);

    tv.xidVacLimit.store(0, Relaxed);
    assert!(ForceTransactionIdLimitUpdate().unwrap());
}

#[test]
fn xid_stop_limit_refuses_assignment() {
    let _l = test_lock();
    setup();
    with_proc();
    reset_proc_xid_state();

    let tv = TransamVariables();
    let oldest: TransactionId = 1000;
    SetTransactionIdLimit(oldest, 7).unwrap();
    let stop = tv.xidStopLimit.load(Relaxed);
    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, stop | 1).value, Relaxed);

    g::SetIsUnderPostmaster(true);
    let err = GetNewTransactionId(false);
    g::SetIsUnderPostmaster(false);
    assert!(err.is_err());
    assert!(DB_NAME_LOOKUPS.load(Relaxed) > 0);

    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, 3).value, Relaxed);
    tv.xidVacLimit.store(0, Relaxed);
    SetTransactionIdLimit(3, 7).unwrap();
    reset_proc_xid_state();
}

#[test]
fn object_id_generator_prefetches_and_advances() {
    let _l = test_lock();
    setup();

    let tv = TransamVariables();
    assert_eq!(tv.nextOid.load(Relaxed), 0);

    // initdb path: force the counter to FirstUnpinnedObjectId first
    StopGeneratingPinnedObjectIds().unwrap();
    assert_eq!(tv.nextOid.load(Relaxed), FirstUnpinnedObjectId);

    let oid = GetNewObjectId().unwrap();
    assert_eq!(oid, FirstUnpinnedObjectId);
    assert_eq!(LOGGED_NEXT_OID.load(Relaxed), FirstUnpinnedObjectId + 8192);
    assert_eq!(tv.oidCount.load(Relaxed), 8191);

    let oid2 = GetNewObjectId().unwrap();
    assert_eq!(oid2, FirstUnpinnedObjectId + 1);
    // no new prefetch record
    assert_eq!(LOGGED_NEXT_OID.load(Relaxed), FirstUnpinnedObjectId + 8192);

    // moving the counter backwards is refused
    assert!(SetNextObjectId(FirstGenbkiObjectId).is_err());
}
