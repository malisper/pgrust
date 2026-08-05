use super::*;
use init_small::globals as g;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, Once, OnceLock};

static NEXT_XID: AtomicU64 = AtomicU64::new(3);

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("subtrans_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_subtrans")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

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
        varsup_seams::read_next_transaction_id::set(|| Ok(NEXT_XID.load(Relaxed) as TransactionId));

        init_seams();
        SUBTRANSShmemInit().unwrap();
        BootStrapSUBTRANS().unwrap();
    });
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn extend_through(xid: TransactionId) {
    for page in 0..=TransactionIdToPage(xid) {
        ExtendSUBTRANS((page as u32).wrapping_mul(SUBTRANS_XACTS_PER_PAGE).max(3)).unwrap();
    }
}

#[test]
fn constants_match_c() {
    assert_eq!(SUBTRANS_XACTS_PER_PAGE, 2048);
    assert_eq!(TransactionIdToPage(2048), 1);
    assert_eq!(TransactionIdToEntry(2049), 1);
}

#[test]
fn set_and_get_parent() {
    let _l = test_lock();
    setup();

    extend_through(100);
    assert_eq!(SubTransGetParent(100).unwrap(), InvalidTransactionId);

    SubTransSetParent(100, 90).unwrap();
    assert_eq!(SubTransGetParent(100).unwrap(), 90);

    // idempotent re-set of the same parent
    SubTransSetParent(100, 90).unwrap();
    assert_eq!(SubTransGetParent(100).unwrap(), 90);
}

#[test]
fn topmost_walks_chain_across_pages() {
    let _l = test_lock();
    setup();

    let top: TransactionId = 500;
    let mid: TransactionId = SUBTRANS_XACTS_PER_PAGE + 7;
    let leaf: TransactionId = 2 * SUBTRANS_XACTS_PER_PAGE + 3;
    extend_through(leaf);

    SubTransSetParent(mid, top).unwrap();
    SubTransSetParent(leaf, mid).unwrap();

    assert_eq!(SubTransGetTopmostTransaction(leaf).unwrap(), top);
    assert_eq!(SubTransGetTopmostTransaction(mid).unwrap(), top);
    assert_eq!(SubTransGetTopmostTransaction(top).unwrap(), top);
}

#[test]
fn startup_zeroes_active_pages() {
    let _l = test_lock();
    setup();

    let xid: TransactionId = 5 * SUBTRANS_XACTS_PER_PAGE + 11;
    extend_through(xid);
    SubTransSetParent(xid, 4 * SUBTRANS_XACTS_PER_PAGE).unwrap();
    assert_ne!(SubTransGetParent(xid).unwrap(), InvalidTransactionId);

    NEXT_XID.store((xid + 100) as u64, Relaxed);
    StartupSUBTRANS(4 * SUBTRANS_XACTS_PER_PAGE).unwrap();

    assert_eq!(SubTransGetParent(xid).unwrap(), InvalidTransactionId);
}

#[test]
fn truncate_removes_old_segments() {
    let _l = test_lock();
    setup();

    let seg1_xid: TransactionId = 33 * SUBTRANS_XACTS_PER_PAGE + 10;
    extend_through(seg1_xid);
    CheckPointSUBTRANS().unwrap();
    assert!(std::fs::metadata("pg_subtrans/0000").is_ok());

    TruncateSUBTRANS(seg1_xid).unwrap();
    assert!(std::fs::metadata("pg_subtrans/0000").is_err());
    assert!(std::fs::metadata("pg_subtrans/0001").is_ok());
}
