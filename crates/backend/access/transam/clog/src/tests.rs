use super::*;
use init_small::globals as g;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, Once, OnceLock};
use types_core::BackendType;

static XLOG_INSERTS: Mutex<Vec<(u8, u8, Vec<u8>)>> = Mutex::new(Vec::new());
static NEXT_XID: AtomicU64 = AtomicU64::new(3);

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
        let tmp = std::env::temp_dir().join(format!("clog_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_xact")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        g::SetMaxConnections(8);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(17);
        g::SetMyProcPid(4242);
        g::set_transaction_buffers(64);

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

        varsup_seams::read_next_transaction_id::set(|| {
            Ok(NEXT_XID.load(std::sync::atomic::Ordering::Relaxed) as TransactionId)
        });
        varsup_seams::advance_oldest_clog_xid::set(|_| Ok(()));

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
        CLOGShmemInit().unwrap();
        BootStrapCLOG().unwrap();
    });
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn extend_through(xid: TransactionId) {
    for page in 0..=TransactionIdToPage(xid) {
        ExtendCLOG((page as u32).wrapping_mul(CLOG_XACTS_PER_PAGE).max(3)).unwrap();
    }
}

fn status_of(xid: TransactionId) -> XidStatus {
    TransactionIdGetStatus(xid).unwrap().0
}

#[test]
fn constants_match_c_headers() {
    assert_eq!(CLOG_XACTS_PER_PAGE, 32768);
    assert_eq!(CLOG_LSNS_PER_PAGE, 1024);
    assert_eq!(RM_CLOG_ID, 3);
    assert_eq!(WAIT_EVENT_XACT_GROUP_UPDATE, 0x0800_0038);
    assert_eq!(clog_max_allowed_buffers(), 65536);
    assert_eq!(TransactionIdToPage(32768), 1);
    assert_eq!(TransactionIdToByte(5), 1);
    assert_eq!(TransactionIdToBIndex(5), 1);
}

#[test]
fn commit_and_abort_roundtrip() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 100;
    assert_eq!(status_of(xid), TRANSACTION_STATUS_IN_PROGRESS);

    TransactionIdSetTreeStatus(xid, &[], TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr).unwrap();
    assert_eq!(status_of(xid), TRANSACTION_STATUS_COMMITTED);

    let aborted: TransactionId = 101;
    TransactionIdSetTreeStatus(aborted, &[], TRANSACTION_STATUS_ABORTED, InvalidXLogRecPtr)
        .unwrap();
    assert_eq!(status_of(aborted), TRANSACTION_STATUS_ABORTED);
    assert_eq!(status_of(102), TRANSACTION_STATUS_IN_PROGRESS);
}

#[test]
fn tree_commit_same_page() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 200;
    let subxids = [201, 202, 203];
    TransactionIdSetTreeStatus(xid, &subxids, TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr)
        .unwrap();

    assert_eq!(status_of(xid), TRANSACTION_STATUS_COMMITTED);
    for sub in subxids {
        assert_eq!(status_of(sub), TRANSACTION_STATUS_COMMITTED);
    }
}

#[test]
fn tree_commit_across_pages() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 300;
    let subxids = [
        301,
        CLOG_XACTS_PER_PAGE + 5,
        CLOG_XACTS_PER_PAGE + 6,
        2 * CLOG_XACTS_PER_PAGE + 7,
    ];
    extend_through(subxids[3]);

    TransactionIdSetTreeStatus(xid, &subxids, TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr)
        .unwrap();

    assert_eq!(status_of(xid), TRANSACTION_STATUS_COMMITTED);
    for sub in subxids {
        assert_eq!(status_of(sub), TRANSACTION_STATUS_COMMITTED);
    }

    let axid: TransactionId = 400;
    let asub = [CLOG_XACTS_PER_PAGE + 100];
    TransactionIdSetTreeStatus(axid, &asub, TRANSACTION_STATUS_ABORTED, InvalidXLogRecPtr)
        .unwrap();
    assert_eq!(status_of(axid), TRANSACTION_STATUS_ABORTED);
    assert_eq!(status_of(asub[0]), TRANSACTION_STATUS_ABORTED);
}

#[test]
fn async_commit_records_group_lsn() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 500;
    let lsn: XLogRecPtr = 0xABCD_0000;
    TransactionIdSetTreeStatus(xid, &[], TRANSACTION_STATUS_COMMITTED, lsn).unwrap();

    let (status, got) = TransactionIdGetStatus(xid).unwrap();
    assert_eq!(status, TRANSACTION_STATUS_COMMITTED);
    assert!(got >= lsn);
}

#[test]
fn extend_emits_zeropage_wal_record() {
    let _l = test_lock();
    setup();

    let page5_first: TransactionId = 5 * CLOG_XACTS_PER_PAGE;
    XLOG_INSERTS.lock().unwrap().clear();
    ExtendCLOG(page5_first).unwrap();
    ExtendCLOG(page5_first + 1).unwrap(); // not a page boundary: no record

    let inserts = XLOG_INSERTS.lock().unwrap();
    assert_eq!(inserts.len(), 1);
    let (rmid, info, data) = &inserts[0];
    assert_eq!((*rmid, *info), (RM_CLOG_ID, CLOG_ZEROPAGE));
    assert_eq!(i64::from_ne_bytes(data[..8].try_into().unwrap()), 5);
}

#[test]
fn group_update_leader_walks_queue_and_wakes() {
    let _l = test_lock();
    setup();

    if lmgr_proc::MyProc().is_none() {
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    }
    let my_procno = lmgr_proc::MyProc().unwrap();
    let proc = GetPGProcByNumber(my_procno);

    let my_xid: TransactionId = 600;
    let member_xid: TransactionId = 601;
    let pageno = TransactionIdToPage(my_xid);

    proc.xid.value.store(my_xid, Relaxed);

    // A never-initialized allProcs slot stands in for a queued follower.
    let member_procno = (ProcGlobal().allProcs.len() - 1) as types_core::ProcNumber;
    assert_ne!(member_procno, my_procno);
    let member = GetPGProcByNumber(member_procno);
    member.clogGroupMember.store(true, Relaxed);
    member.clogGroupMemberXid.store(member_xid, Relaxed);
    member
        .clogGroupMemberXidStatus
        .store(TRANSACTION_STATUS_COMMITTED, Relaxed);
    member.clogGroupMemberPage.store(pageno, Relaxed);
    member.clogGroupMemberLsn.store(InvalidXLogRecPtr, Relaxed);

    let lock = SimpleLruGetBankLock(XactCtl(), pageno);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let b2 = barrier.clone();
    let helper = std::thread::spawn(move || {
        let held = LwGuard::acquire(lock, LW_EXCLUSIVE).unwrap();
        b2.wait();
        // Push the fake follower once the main thread is queued as leader.
        loop {
            let head = ProcGlobal().clogGroupFirst.value.load(Acquire);
            if head == my_procno as u32 {
                break;
            }
            std::thread::yield_now();
        }
        member.clogGroupNext.value.store(my_procno as u32, Relaxed);
        ProcGlobal()
            .clogGroupFirst
            .value
            .store(member_procno as u32, Release);
        held.release().unwrap();
    });

    barrier.wait();
    TransactionIdSetTreeStatus(my_xid, &[], TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr)
        .unwrap();
    helper.join().unwrap();

    assert_eq!(status_of(my_xid), TRANSACTION_STATUS_COMMITTED);
    assert_eq!(status_of(member_xid), TRANSACTION_STATUS_COMMITTED);
    assert!(!proc.clogGroupMember.load(Relaxed));
    assert!(!member.clogGroupMember.load(Relaxed));
    assert_eq!(
        ProcGlobal().clogGroupFirst.value.load(Relaxed),
        INVALID_PROC_NUMBER as u32
    );

    proc.xid.value.store(InvalidTransactionId, Relaxed);
}

#[test]
fn startup_and_trim_zero_page_tail() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 7 * CLOG_XACTS_PER_PAGE + 21;
    extend_through(xid);
    TransactionIdSetTreeStatus(xid, &[], TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr).unwrap();
    TransactionIdSetTreeStatus(xid + 1, &[], TRANSACTION_STATUS_ABORTED, InvalidXLogRecPtr)
        .unwrap();

    NEXT_XID.store((xid + 1) as u64, std::sync::atomic::Ordering::Relaxed);
    StartupCLOG().unwrap();
    TrimCLOG().unwrap();

    // xid precedes nextXid: survives; xid+1 is at/after nextXid: zeroed.
    assert_eq!(status_of(xid), TRANSACTION_STATUS_COMMITTED);
    assert_eq!(status_of(xid + 1), TRANSACTION_STATUS_IN_PROGRESS);
}

#[test]
fn checkpoint_writes_dirty_pages() {
    let _l = test_lock();
    setup();

    TransactionIdSetTreeStatus(50, &[], TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr).unwrap();
    CheckPointCLOG().unwrap();
    assert!(std::fs::metadata("pg_xact/0000").unwrap().len() >= BLCKSZ as u64);
}
