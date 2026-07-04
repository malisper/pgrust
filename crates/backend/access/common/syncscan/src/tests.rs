use super::*;
use std::sync::{Mutex, Once};

#[test]
fn lock_offset_is_sync_scan() {
    assert_eq!(lwlock::GetLWTrancheName(SYNC_SCAN_LOCK_OFFSET as u16), "SyncScan");
}

#[test]
fn report_interval_matches_c() {
    assert_eq!(SYNC_SCAN_REPORT_INTERVAL, 16);
}

fn loc(n: u32) -> RelFileLocator {
    RelFileLocator::new(1663, 5, n)
}

#[test]
fn ss_search_lru_semantics() {
    let mut sl = boot_image();

    assert_eq!(ss_search(&mut sl, loc(1), 0, false), 0);
    assert_eq!(sl.head, (SYNC_SCAN_NELEM - 1) as u8);
    assert_eq!(ss_search(&mut sl, loc(1), 640, true), 640);
    assert_eq!(ss_search(&mut sl, loc(1), 999, false), 640);

    for n in 2..=SYNC_SCAN_NELEM as u32 + 1 {
        ss_search(&mut sl, loc(n), 16 * n, true);
    }
    // loc(1) fell off the LRU: rediscovery recreates it at the probe location.
    assert_eq!(ss_search(&mut sl, loc(1), 0, false), 0);

    let mut seen = 0;
    let mut idx = sl.head;
    let mut prev = NONE;
    while idx != NONE {
        assert_eq!(sl.items[idx as usize].prev, prev);
        prev = idx;
        idx = sl.items[idx as usize].next;
        seen += 1;
    }
    assert_eq!(seen, SYNC_SCAN_NELEM);
    assert_eq!(sl.tail, prev);
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        g::SetMyProcNumber(0);
        shmem::init_seams();
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        lwlock::CreateLWLocks(false).unwrap();
        SyncScanShmemInit();
    });
}

#[test]
fn get_report_roundtrip_and_crash_reset() {
    let _g = serial();
    setup();

    assert_eq!(get_location(loc(41), 4096).unwrap(), 0);
    report_location(loc(41), 17).unwrap();
    assert_eq!(get_location(loc(41), 4096).unwrap(), 0);
    report_location(loc(41), 2048).unwrap();
    assert_eq!(get_location(loc(41), 4096).unwrap(), 2048);
    assert_eq!(get_location(loc(41), 1024).unwrap(), 0);

    SyncScanShmemResetAfterCrash();
    report_location(loc(42), 512).unwrap();
    assert_eq!(get_location(loc(41), 4096).unwrap(), 0);
    assert_eq!(get_location(loc(42), 4096).unwrap(), 512);
}
