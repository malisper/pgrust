use super::*;
use init_small::globals as g;
use std::sync::{Mutex, Once, OnceLock};

static XLOG_INSERTS: Mutex<Vec<(u8, u8, Vec<u8>)>> = Mutex::new(Vec::new());
static IN_RECOVERY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn test_shmem_init_struct(name: &str, size: usize) -> PgResult<(*mut u8, bool)> {
    let mut reg = shmem_registry().lock().unwrap();
    if let Some(&addr) = reg.get(name) {
        return Ok((std::ptr::with_exposed_provenance_mut(addr), true));
    }
    let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
    let p = unsafe { std::alloc::alloc_zeroed(layout) };
    assert!(!p.is_null());
    reg.insert(name.to_string(), p.expose_provenance());
    Ok((p, false))
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("commit_ts_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_commit_ts")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        g::SetMaxConnections(8);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(17);
        g::SetMyProcPid(4242);
        g::set_commit_timestamp_buffers(16);

        shmem_seams::shmem_init_struct::set(test_shmem_init_struct);
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
        transam_xlog_seams::recovery_in_progress::set(|| IN_RECOVERY.load(Relaxed));
        transam_xlog_seams::xlog_flush::set(|_| Ok(()));
        transam_xlog_seams::count_ckpt_slru_written::set(|| {});
        xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
            let mut data = Vec::new();
            for f in fragments {
                data.extend_from_slice(f);
            }
            XLOG_INSERTS.lock().unwrap().push((rmid, info, data));
            Ok(0x1000)
        });
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);

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
        varsup::VarsupShmemInit();
        varsup::init_seams();

        init_seams();
        CommitTsShmemInit().unwrap();
    });
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn deactivate() {
    DeactivateCommitTs().unwrap();
}

fn activate() {
    StartupCommitTs().unwrap();
}

#[test]
fn constants_match_c_headers() {
    assert_eq!(SizeOfCommitTimestampEntry, 10);
    assert_eq!(COMMIT_TS_XACTS_PER_PAGE, 819);
    assert_eq!(RM_COMMIT_TS_ID, 18);
    assert_eq!(COMMIT_TS_ZEROPAGE, 0x00);
    assert_eq!(COMMIT_TS_TRUNCATE, 0x10);
    assert_eq!(TransactionIdToCTsPage(819), 1);
    assert_eq!(TransactionIdToCTsEntry(820), 1);
}

#[test]
fn entry_codec_roundtrip() {
    let mut page = vec![0u8; BLCKSZ];
    write_entry(&mut page, 0, 0x1122_3344_5566_7788, 0xABCD);
    write_entry(&mut page, 818, -1, 7);
    assert_eq!(read_entry(&page, 0), (0x1122_3344_5566_7788, 0xABCD));
    assert_eq!(read_entry(&page, 818), (-1, 7));
    // Packed layout: entry 1 starts at byte 10.
    assert_eq!(page[10..18], [0u8; 8]);
}

#[test]
fn disabled_module_arms_and_error_texts() {
    let _l = test_lock();
    setup();
    deactivate();

    // Order-independence: earlier tests in this binary may have recorded
    // inserts (the activate-path test clears at ITS start, not its end).
    XLOG_INSERTS.lock().unwrap().clear();

    // No-op writers while inactive.
    TransactionTreeSetCommitTsData(100, &[101, 102], 42, 0).unwrap();
    ExtendCommitTs(FirstNormalTransactionId).unwrap();
    assert!(XLOG_INSERTS.lock().unwrap().is_empty());

    let err = TransactionIdGetCommitTsData(100).unwrap_err();
    assert_eq!(err.message(), "could not get commit timestamp data");
    assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        err.hint(),
        Some("Make sure the configuration parameter \"track_commit_timestamp\" is set.")
    );

    let err = GetLatestCommitTsData().unwrap_err();
    assert_eq!(err.message(), "could not get commit timestamp data");

    // Recovery flavor of the hint.
    IN_RECOVERY.store(true, Relaxed);
    let err = TransactionIdGetCommitTsData(100).unwrap_err();
    assert_eq!(
        err.hint(),
        Some(
            "Make sure the configuration parameter \"track_commit_timestamp\" is set on the primary server."
        )
    );
    IN_RECOVERY.store(false, Relaxed);

    // Invalid / non-normal xids error or no-op regardless of activation.
    let err = TransactionIdGetCommitTsData(InvalidTransactionId).unwrap_err();
    assert_eq!(
        err.message(),
        "cannot retrieve commit timestamp for transaction 0"
    );
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(TransactionIdGetCommitTsData(1).unwrap().is_none());
    assert!(TransactionIdGetCommitTsData(2).unwrap().is_none());
}

#[test]
fn activation_state_machine_and_set_get() {
    let _l = test_lock();
    setup();
    deactivate();

    let tv = TransamVariables();
    assert_eq!(tv.oldestCommitTsXid.load(Relaxed), InvalidTransactionId);

    activate();
    assert!(shared().commitTsActive.load(Relaxed));
    let next = varsup::ReadNextTransactionId().unwrap();
    assert_eq!(tv.oldestCommitTsXid.load(Relaxed), next);
    assert_eq!(tv.newestCommitTsXid.load(Relaxed), next);
    // ActivateCommitTs wrote the current segment file.
    assert!(std::fs::read_dir("pg_commit_ts").unwrap().next().is_some());
    // Re-activation is a no-op.
    activate();

    let xid = next;
    TransactionTreeSetCommitTsData(xid, &[xid + 1, xid + 2], 777_000, 3).unwrap();
    assert_eq!(tv.newestCommitTsXid.load(Relaxed), xid + 2);

    // Cached last-commit hit.
    assert_eq!(TransactionIdGetCommitTsData(xid).unwrap(), Some((777_000, 3)));
    // SLRU read path for the subxid.
    assert_eq!(TransactionIdGetCommitTsData(xid + 1).unwrap(), Some((777_000, 3)));
    assert_eq!(TransactionIdGetCommitTsData(xid + 2).unwrap(), Some((777_000, 3)));

    let (last_xid, last_ts, last_node) = GetLatestCommitTsData().unwrap();
    assert_eq!((last_xid, last_ts, last_node), (xid, 777_000, 3));

    // Out-of-range xids return None without touching the SLRU.
    SetCommitTsLimit(xid, xid + 2).unwrap();
    assert!(TransactionIdGetCommitTsData(xid + 100).unwrap().is_none());

    AdvanceOldestCommitTsXid(xid + 1).unwrap();
    assert_eq!(tv.oldestCommitTsXid.load(Relaxed), xid + 1);
    // AdvanceOldestCommitTsXid never moves backwards.
    AdvanceOldestCommitTsXid(xid).unwrap();
    assert_eq!(tv.oldestCommitTsXid.load(Relaxed), xid + 1);

    // Deactivation clears state and removes all segment files.
    deactivate();
    assert!(!shared().commitTsActive.load(Relaxed));
    assert_eq!(tv.oldestCommitTsXid.load(Relaxed), InvalidTransactionId);
    assert_eq!(tv.newestCommitTsXid.load(Relaxed), InvalidTransactionId);
    assert!(std::fs::read_dir("pg_commit_ts").unwrap().next().is_none());
}

#[test]
fn extend_and_wal_records() {
    let _l = test_lock();
    setup();
    deactivate();
    activate();

    XLOG_INSERTS.lock().unwrap().clear();

    // Mid-page xid: no work.
    ExtendCommitTs(5).unwrap();
    assert!(XLOG_INSERTS.lock().unwrap().is_empty());

    // First xid of page 2 zeroes the page and logs it.
    let first_of_page2 = 2 * COMMIT_TS_XACTS_PER_PAGE;
    ExtendCommitTs(first_of_page2).unwrap();
    {
        let inserts = XLOG_INSERTS.lock().unwrap();
        let (rmid, info, data) = inserts.last().expect("ZEROPAGE record");
        assert_eq!((*rmid, *info), (RM_COMMIT_TS_ID, COMMIT_TS_ZEROPAGE));
        assert_eq!(data.as_slice(), &2i64.to_ne_bytes());
    }

    deactivate();
}
