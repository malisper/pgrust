use super::*;
use init_small::globals as g;
use std::sync::{Mutex, Once, OnceLock};

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("transam_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_xact")).unwrap();
        std::fs::create_dir_all(tmp.join("pg_subtrans")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        g::set_transaction_buffers(64);
        g::set_subtransaction_buffers(64);

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
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        transam_xlog_seams::xlog_flush::set(|_| Ok(()));
        transam_xlog_seams::count_ckpt_slru_written::set(|| {});
        xlogutils_seams::in_recovery::set(|| false);
        xloginsert_seams::xlog_insert::set(|_, _, _| Ok(0x1000));
        varsup_seams::read_next_transaction_id::set(|| Ok(3));
        varsup_seams::advance_oldest_clog_xid::set(|_| Ok(()));

        clog::init_seams();
        subtrans::init_seams();
        init_seams();
        clog::CLOGShmemInit().unwrap();
        clog::BootStrapCLOG().unwrap();
        subtrans::SUBTRANSShmemInit().unwrap();
        subtrans::BootStrapSUBTRANS().unwrap();
    });
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn permanent_xids_are_committed() {
    let _l = test_lock();
    setup();

    assert!(TransactionIdDidCommit(BootstrapTransactionId).unwrap());
    assert!(TransactionIdDidCommit(FrozenTransactionId).unwrap());
    assert!(!TransactionIdDidAbort(BootstrapTransactionId).unwrap());
    assert!(!TransactionIdDidAbort(FrozenTransactionId).unwrap());
}

#[test]
fn commit_and_abort_trees_resolve() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 100;
    let subxids = [101, 102];
    TransactionIdCommitTree(xid, &subxids).unwrap();

    assert!(TransactionIdDidCommit(xid).unwrap());
    for sub in subxids {
        assert!(TransactionIdDidCommit(sub).unwrap());
        assert!(!TransactionIdDidAbort(sub).unwrap());
    }

    let axid: TransactionId = 200;
    TransactionIdAbortTree(axid, &[201]).unwrap();
    assert!(TransactionIdDidAbort(axid).unwrap());
    assert!(TransactionIdDidAbort(201).unwrap());
    assert!(!TransactionIdDidCommit(axid).unwrap());

    assert!(!TransactionIdDidCommit(300).unwrap());
    assert!(!TransactionIdDidAbort(300).unwrap());
}

#[test]
fn single_item_cache_holds_final_states_only() {
    let _l = test_lock();
    setup();

    let committed: TransactionId = 400;
    TransactionIdCommitTree(committed, &[]).unwrap();
    assert!(TransactionIdDidCommit(committed).unwrap());
    assert_eq!(cachedFetchXid.get(), committed);
    assert_eq!(cachedFetchXidStatus.get(), TRANSACTION_STATUS_COMMITTED);

    assert!(!TransactionIdDidCommit(401).unwrap());
    assert_eq!(cachedFetchXid.get(), committed);

    let aborted: TransactionId = 402;
    TransactionIdAbortTree(aborted, &[]).unwrap();
    assert!(TransactionIdDidAbort(aborted).unwrap());
    assert_eq!(cachedFetchXid.get(), aborted);
    assert_eq!(cachedFetchXidStatus.get(), TRANSACTION_STATUS_ABORTED);
}

#[test]
fn async_commit_lsn_flows_to_get_commit_lsn() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 500;
    let lsn: XLogRecPtr = 0x5000_0000;
    TransactionIdAsyncCommitTree(xid, &[], lsn).unwrap();

    assert!(TransactionIdDidCommit(xid).unwrap());
    assert!(TransactionIdGetCommitLSN(xid).unwrap() >= lsn);
    cachedFetchXid.set(InvalidTransactionId);
    assert!(TransactionIdGetCommitLSN(xid).unwrap() >= lsn);
    assert_eq!(
        TransactionIdGetCommitLSN(FrozenTransactionId).unwrap(),
        InvalidXLogRecPtr
    );
}

#[test]
fn latest_xid_uses_wraparound_compare() {
    assert_eq!(TransactionIdLatest(10, &[]), 10);
    assert_eq!(TransactionIdLatest(10, &[12, 15, 11]), 15);
    assert_eq!(TransactionIdLatest(0xFFFF_FFF0, &[3]), 3);
}

#[test]
fn comparison_predicates_match_c() {
    assert!(TransactionIdPrecedes(3, 4));
    assert!(TransactionIdFollows(4, 3));
    assert!(TransactionIdFollowsOrEquals(4, 4));
    assert!(TransactionIdPrecedesOrEquals(4, 4));
    assert!(TransactionIdPrecedes(0xFFFF_FFFF, 3));
    assert!(TransactionIdPrecedes(BootstrapTransactionId, 0xFFFF_FFFF));
}
