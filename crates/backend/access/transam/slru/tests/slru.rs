use slru::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once, OnceLock};
use types_storage::sync::SyncRequestHandler;

static PAGE_HITS: AtomicU64 = AtomicU64::new(0);
static PAGE_READS: AtomicU64 = AtomicU64::new(0);

fn shmem_registry() -> &'static Mutex<HashMap<String, usize>> {
    static R: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

fn test_shmem_init_struct(name: &str, size: usize) -> types_error::PgResult<(*mut u8, bool)> {
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

fn cpath(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).unwrap()
}

fn test_open_transient_file(name: &str, flags: i32) -> types_error::PgResult<i32> {
    Ok(unsafe { libc::open(cpath(name).as_ptr(), flags, 0o600 as libc::c_uint) })
}

fn test_with_allocated_dir(
    dirname: &str,
    cb: &mut dyn FnMut(&str) -> types_error::PgResult<bool>,
) -> types_error::PgResult<bool> {
    let mut ret = false;
    for entry in std::fs::read_dir(dirname).unwrap() {
        let entry = entry.unwrap();
        ret = cb(entry.file_name().to_str().unwrap())?;
        if ret {
            break;
        }
    }
    Ok(ret)
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("slru_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        shmem_seams::shmem_init_struct::set(test_shmem_init_struct);

        file_seams::open_transient_file::set(test_open_transient_file);
        file_seams::close_transient_file::set(|fd| unsafe { libc::close(fd) });
        file_seams::pg_fsync::set(|fd| unsafe { libc::fsync(fd) });
        file_seams::fsync_fname::set(|_, _| Ok(()));
        file_seams::data_sync_elevel::set(|e| e);
        file_seams::with_allocated_dir::set(test_with_allocated_dir);

        sync_seams::register_sync_request::set(|_, _, _| Ok(true));

        pgstat_seams::pgstat_get_slru_index::set(|_| 0);
        pgstat_seams::pgstat_count_slru_page_zeroed::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_hit::set(|_| {
            PAGE_HITS.fetch_add(1, Ordering::Relaxed);
        });
        pgstat_seams::pgstat_count_slru_page_read::set(|_| {
            PAGE_READS.fetch_add(1, Ordering::Relaxed);
        });
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
    });
}

fn page_lt(a: i64, b: i64) -> bool {
    a < b
}

fn init(name: &str, dir: &str, nlsns: i32) -> SlruCtlData {
    install_seams();
    std::fs::create_dir_all(dir).unwrap();
    let mut ctl = SimpleLruInit(
        name,
        64,
        nlsns,
        dir,
        1,
        2,
        SyncRequestHandler::SYNC_HANDLER_NONE,
        false,
    )
    .unwrap();
    ctl.PagePrecedes = Some(page_lt);
    ctl
}

#[test]
fn file_names_and_buffers_check() {
    install_seams();
    let ctl = init("slru_names", "slru_names_dir", 0);
    assert_eq!(SlruFileName(&ctl, 0).as_str(), "slru_names_dir/0000");
    assert_eq!(SlruFileName(&ctl, 0x1234).as_str(), "slru_names_dir/1234");
    assert_eq!(SlruFileName(&ctl, 0x12345).as_str(), "slru_names_dir/12345");
    assert_eq!(SlruFileName(&ctl, 0xFF_FFFF).as_str(), "slru_names_dir/FFFFFF");

    let long = SimpleLruInit(
        "slru_names_long",
        64,
        0,
        "pg_xact",
        1,
        2,
        SyncRequestHandler::SYNC_HANDLER_NONE,
        true,
    )
    .unwrap();
    assert_eq!(SlruFileName(&long, 0).as_str(), "pg_xact/000000000000000");
    assert_eq!(
        SlruFileName(&long, 0x123456789ABCDEF).as_str(),
        "pg_xact/123456789ABCDEF"
    );

    assert_eq!(check_slru_buffers("xact_buffers", 32), (true, None));
    let (ok, detail) = check_slru_buffers("xact_buffers", 17);
    assert!(!ok);
    assert_eq!(
        detail.as_deref(),
        Some("\"xact_buffers\" must be a multiple of 16.")
    );
}

#[test]
fn shmem_size_shape() {
    let base = SimpleLruShmemSize(64, 0);
    assert!(base > 64 * 8192);
    assert_eq!(base % 32, 0);
    assert_eq!(SimpleLruShmemSize(64, 2) - base, 64 * 2 * 8);
}

#[test]
#[cfg_attr(miri, ignore)] // libc file I/O; macOS Miri shims incomplete
fn zero_write_read_roundtrip() {
    let ctl = init("slru_rw", "slru_rw_dir", 0);

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(&ctl, 3), lwlock::LW_EXCLUSIVE).unwrap();
    let slotno = SimpleLruZeroPage(&ctl, 3, &mut bank).unwrap();
    assert_eq!(ctl.page_status(slotno, &bank), SLRU_PAGE_VALID);
    ctl.page_buffer_mut(slotno, &mut bank)[..4].copy_from_slice(b"QALG");
    SimpleLruWritePage(&ctl, slotno, &mut bank).unwrap();
    bank.release().unwrap();

    let on_disk = std::fs::read("slru_rw_dir/0000").unwrap();
    assert_eq!(on_disk.len(), 4 * 8192);
    assert_eq!(&on_disk[3 * 8192..3 * 8192 + 4], b"QALG");

    assert!(SimpleLruDoesPhysicalPageExist(&ctl, 3).unwrap());
    assert!(!SimpleLruDoesPhysicalPageExist(&ctl, 40).unwrap());

    // A second SLRU over the same directory must physically read the page.
    let ctl2 = init("slru_rw_2", "slru_rw_dir", 0);
    let reads_before = PAGE_READS.load(Ordering::Relaxed);
    let (slot2, bank2) = SimpleLruReadPage_ReadOnly(&ctl2, 3, 0).unwrap();
    assert_eq!(&ctl2.page_buffer(slot2, &bank2)[..4], b"QALG");
    bank2.release().unwrap();
    assert_eq!(PAGE_READS.load(Ordering::Relaxed), reads_before + 1);

    let hits_before = PAGE_HITS.load(Ordering::Relaxed);
    let (slot3, bank3) = SimpleLruReadPage_ReadOnly(&ctl2, 3, 0).unwrap();
    assert_eq!(slot2, slot3);
    bank3.release().unwrap();
    assert_eq!(PAGE_HITS.load(Ordering::Relaxed), hits_before + 1);
}

#[test]
fn attach_shares_state() {
    let ctl_a = init("slru_shared", "slru_shared_dir", 0);
    let ctl_b = init("slru_shared", "slru_shared_dir", 0);

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(&ctl_a, 7), lwlock::LW_EXCLUSIVE).unwrap();
    let slot = SimpleLruZeroPage(&ctl_a, 7, &mut bank).unwrap();
    ctl_a.page_buffer_mut(slot, &mut bank)[0] = 0x5A;
    bank.release().unwrap();

    // The sibling handle sees the same buffer without any physical read.
    let reads_before = PAGE_READS.load(Ordering::Relaxed);
    let (slot_b, bank_b) = SimpleLruReadPage_ReadOnly(&ctl_b, 7, 0).unwrap();
    assert_eq!(slot_b, slot);
    assert_eq!(ctl_b.page_buffer(slot_b, &bank_b)[0], 0x5A);
    bank_b.release().unwrap();
    assert_eq!(PAGE_READS.load(Ordering::Relaxed), reads_before);
}

#[test]
#[cfg_attr(miri, ignore)] // libc file I/O; macOS Miri shims incomplete
fn truncate_deletes_old_segments() {
    let ctl = init("slru_trunc", "slru_trunc_dir", 0);

    for pageno in [0i64, 64] {
        let mut bank =
            LwGuard::acquire(SimpleLruGetBankLock(&ctl, pageno), lwlock::LW_EXCLUSIVE).unwrap();
        let slot = SimpleLruZeroPage(&ctl, pageno, &mut bank).unwrap();
        SimpleLruWritePage(&ctl, slot, &mut bank).unwrap();
        bank.release().unwrap();
    }
    assert!(std::fs::metadata("slru_trunc_dir/0000").is_ok());
    assert!(std::fs::metadata("slru_trunc_dir/0002").is_ok());

    let found = SlruScanDirectory(&ctl, |ctl, filename, segpage| {
        SlruScanDirCbReportPresence(ctl, filename, segpage, 32)
    })
    .unwrap();
    assert!(found);

    SimpleLruTruncate(&ctl, 32).unwrap();
    assert!(std::fs::metadata("slru_trunc_dir/0000").is_err());
    assert!(std::fs::metadata("slru_trunc_dir/0002").is_ok());
}

#[test]
#[cfg_attr(miri, ignore)] // libc file I/O; macOS Miri shims incomplete
fn delete_segment_drops_file_and_buffers() {
    let ctl = init("slru_delseg", "slru_delseg_dir", 0);

    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(&ctl, 2), lwlock::LW_EXCLUSIVE).unwrap();
    let slot = SimpleLruZeroPage(&ctl, 2, &mut bank).unwrap();
    SimpleLruWritePage(&ctl, slot, &mut bank).unwrap();
    bank.release().unwrap();
    assert!(std::fs::metadata("slru_delseg_dir/0000").is_ok());

    SlruDeleteSegment(&ctl, 0).unwrap();
    assert!(std::fs::metadata("slru_delseg_dir/0000").is_err());

    let bank = LwGuard::acquire(SimpleLruGetBankLock(&ctl, 2), lwlock::LW_EXCLUSIVE).unwrap();
    assert_eq!(ctl.page_status(slot, &bank), SLRU_PAGE_EMPTY);
    bank.release().unwrap();
}

#[test]
#[cfg_attr(miri, ignore)] // elog's %m expansion calls libc::strerror, unshimmed in Miri
fn missing_page_read_reports_and_releases_locks() {
    let ctl = init("slru_missing", "slru_missing_dir", 0);

    let banklock = SimpleLruGetBankLock(&ctl, 5);
    let mut bank = LwGuard::acquire(banklock, lwlock::LW_EXCLUSIVE).unwrap();
    let err = SimpleLruReadPage(&ctl, 5, true, 1234, &mut bank).unwrap_err();
    assert_eq!(err.message(), "could not access status of transaction 1234");
    assert!(err.detail().unwrap().starts_with("Could not open file \"slru_missing_dir/0000\":"));
    drop(bank);

    // The unwound path left no lock held: both locks are re-acquirable.
    let bank = LwGuard::conditional_acquire(banklock, lwlock::LW_EXCLUSIVE)
        .unwrap()
        .expect("bank lock leaked");
    bank.release().unwrap();
}

#[test]
fn page_precedes_self_checks() {
    let mut ctl = init("slru_precedes", "slru_precedes_dir", 0);
    // CLOGPagePrecedes (clog.c): 32768 xacts per page, wraparound-aware.
    fn clog_precedes(page1: i64, page2: i64) -> bool {
        const PER_PAGE: u32 = 32768;
        let xid1 = (page1 as u32).wrapping_mul(PER_PAGE).wrapping_add(3 + 1);
        let xid2 = (page2 as u32).wrapping_mul(PER_PAGE).wrapping_add(3 + 1);
        types_core::TransactionIdPrecedes(xid1, xid2)
            && types_core::TransactionIdPrecedes(xid1, xid2.wrapping_add(PER_PAGE - 1))
    }
    ctl.PagePrecedes = Some(clog_precedes);
    SlruPagePrecedesUnitTests(&ctl, 32768);
}
