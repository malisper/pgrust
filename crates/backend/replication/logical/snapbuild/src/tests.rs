use super::*;
use reorderbuffer::ReorderBuffer;
use std::sync::{Mutex, Once};

thread_local! {
    static XMIN_CALLS: std::cell::RefCell<Vec<(XLogRecPtr, TransactionId)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static RESTART_CALLS: std::cell::RefCell<Vec<(XLogRecPtr, XLogRecPtr)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        reorderbuffer::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| types_core::TopSubTransactionId);
        logical_hooks::logical_increase_xmin_for_slot::set(|lsn, xmin| {
            XMIN_CALLS.with(|c| c.borrow_mut().push((lsn, xmin)));
            Ok(())
        });
        logical_hooks::logical_increase_restart_decoding_for_slot::set(|lsn, restart| {
            RESTART_CALLS.with(|c| c.borrow_mut().push((lsn, restart)));
            Ok(())
        });
        let dir = std::env::temp_dir().join(format!("snapbuild_test_{}", std::process::id()));
        std::fs::create_dir_all(dir.join(PG_LOGICAL_SNAPSHOTS_DIR)).unwrap();
        std::env::set_current_dir(&dir).unwrap();
    });
}

fn boot() {
    setup();
    XMIN_CALLS.with(|c| c.borrow_mut().clear());
    RESTART_CALLS.with(|c| c.borrow_mut().clear());
}

fn running<'a>(
    oldest: TransactionId,
    next: TransactionId,
    xids: &'a [TransactionId],
) -> XlRunningXacts<'a> {
    XlRunningXacts {
        xcnt: xids.len() as u32,
        subxcnt: 0,
        subxid_overflow: false,
        next_xid: next,
        oldest_running_xid: oldest,
        latest_completed_xid: next.wrapping_sub(1),
        xids,
    }
}

fn rb() -> ReorderBuffer {
    ReorderBuffer::allocate("snapbuild_test_slot").expect("allocate")
}

#[test]
fn no_running_xacts_jumps_straight_to_consistent() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    assert_eq!(b.current_state(), Start);

    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();

    assert_eq!(b.current_state(), Consistent);
    assert_eq!(b.xmin, 8);
    assert_eq!(b.xmax, 8);
    assert_eq!(b.next_phase_at, InvalidTransactionId);
    assert_eq!(b.start_decoding_at, 0x101);
    // find_snapshot returned false: no cleanup pass, no slot xmin advance yet.
    assert!(XMIN_CALLS.with(|c| c.borrow().is_empty()));
}

#[test]
fn start_building_full_consistent_transitions() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, true, 0);

    b.process_running_xacts(&mut rb, 0x100, &running(5, 10, &[]))
        .unwrap();
    assert_eq!(b.current_state(), Building);
    assert_eq!(b.next_phase_at, 10);
    assert_eq!(b.xmax, 10);
    // cleanup pass ran: xmin tracks the record's oldestRunningXid.
    assert_eq!(b.xmin, 5);
    assert_eq!(XMIN_CALLS.with(|c| c.borrow().clone()), vec![(0x100, 5)]);

    // oldestRunningXid still below next_phase_at: no transition.
    b.process_running_xacts(&mut rb, 0x200, &running(7, 12, &[]))
        .unwrap();
    assert_eq!(b.current_state(), Building);
    assert_eq!(b.next_phase_at, 10);

    b.process_running_xacts(&mut rb, 0x300, &running(10, 15, &[]))
        .unwrap();
    assert_eq!(b.current_state(), FullSnapshot);
    assert_eq!(b.next_phase_at, 15);

    b.process_running_xacts(&mut rb, 0x400, &running(12, 18, &[]))
        .unwrap();
    assert_eq!(b.current_state(), FullSnapshot);

    b.process_running_xacts(&mut rb, 0x500, &running(15, 20, &[]))
        .unwrap();
    assert_eq!(b.current_state(), Consistent);
    assert_eq!(b.next_phase_at, InvalidTransactionId);
    // slot xmin advanced on every record's cleanup pass
    assert_eq!(
        XMIN_CALLS.with(|c| c.borrow().clone()),
        vec![(0x100, 5), (0x200, 7), (0x300, 10), (0x400, 12), (0x500, 15)]
    );
    // nothing serialized yet: restart-decoding advance has no target
    assert!(RESTART_CALLS.with(|c| c.borrow().is_empty()));
}

#[test]
fn xmin_horizon_too_low_defers_all_transitions() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(100, 0, false, true, 0);

    b.process_running_xacts(&mut rb, 0x100, &running(50, 60, &[]))
        .unwrap();
    assert_eq!(b.current_state(), Start);
    assert_eq!(b.xmin, 50);
    assert_eq!(XMIN_CALLS.with(|c| c.borrow().clone()), vec![(0x100, 50)]);

    b.process_running_xacts(&mut rb, 0x200, &running(100, 110, &[]))
        .unwrap();
    assert_eq!(b.current_state(), Building);
}

#[test]
fn wait_snapshot_skips_xids_beyond_cutoff() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, true, 0);
    // xid 11 follows cutoff 10: skipped without touching the lock table.
    b.process_running_xacts(&mut rb, 0x100, &running(5, 10, &[11]))
        .unwrap();
    assert_eq!(b.current_state(), Building);
}

#[test]
fn commit_before_building_only_advances_start_decoding_at() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0x50, false, false, 0);

    b.commit_txn(&mut rb, 0x80, 20, &[], 0).unwrap();
    assert_eq!(b.start_decoding_at, 0x81);
    assert!(b.committed_xip.is_empty());
    assert!(b.committed_includes_all_transactions);

    // commit below start_decoding_at leaves it alone
    b.commit_txn(&mut rb, 0x60, 21, &[], 0).unwrap();
    assert_eq!(b.start_decoding_at, 0x81);
}

#[test]
fn commit_without_catalog_changes_stops_including_all_transactions() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();

    b.commit_txn(&mut rb, 0x200, 9, &[], 0).unwrap();
    assert!(b.committed_xip.is_empty());
    assert!(!b.committed_includes_all_transactions);
    assert_eq!(b.xmax, 8);
}

#[test]
fn commit_with_catalog_changes_is_tracked_and_advances_xmax() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();

    rb.xid_set_catalog_changes(9, 0x150);
    b.commit_txn(&mut rb, 0x200, 9, &[], 0).unwrap();

    assert_eq!(&*b.committed_xip, &[9]);
    assert_eq!(b.xmax, 10);
    assert!(b.snapshot.is_some());
    let snap = b.snapshot.clone().unwrap();
    assert_eq!(snap.snapshot_type, SNAPSHOT_HISTORIC_MVCC);
    assert_eq!(&snap.xip[..snap.xcnt as usize], &[9]);
    assert_eq!(snap.xmin, 8);
    assert_eq!(snap.xmax, 10);
}

#[test]
fn subxact_catalog_changes_force_toplevel_timetravel() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();

    // restored catchange path: subxid known catalog-modifying only on disk
    b.catchange_xip.push(11);
    b.commit_txn(&mut rb, 0x200, 9, &[11], XACT_XINFO_HAS_INVALS)
        .unwrap();

    assert_eq!(&*b.committed_xip, &[11, 9]);
    assert_eq!(b.xmax, 12);
    let snap = b.snapshot.clone().unwrap();
    assert_eq!(&snap.xip[..snap.xcnt as usize], &[9, 11]);
}

#[test]
fn catchange_without_invals_flag_is_ignored() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();

    b.catchange_xip.push(9);
    b.commit_txn(&mut rb, 0x200, 9, &[], 0).unwrap();
    assert!(b.committed_xip.is_empty());
}

#[test]
fn purge_drops_xids_below_xmin() {
    let _g = test_lock();
    boot();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.committed_xip.extend_from_slice(&[5, 10, 15]);
    b.catchange_xip.extend_from_slice(&[5, 10, 15]);

    b.xmin = InvalidTransactionId;
    b.purge_older_txn().unwrap();
    assert_eq!(&*b.committed_xip, &[5, 10, 15]);

    b.xmin = 10;
    b.purge_older_txn().unwrap();
    assert_eq!(&*b.committed_xip, &[10, 15]);
    assert_eq!(&*b.catchange_xip, &[10, 15]);

    b.xmin = 100;
    b.purge_older_txn().unwrap();
    assert!(b.committed_xip.is_empty());
    assert!(b.catchange_xip.is_empty());
}

#[test]
fn purge_runs_from_process_running_xacts() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();
    rb.xid_set_catalog_changes(9, 0x150);
    b.commit_txn(&mut rb, 0x200, 9, &[], 0).unwrap();
    assert_eq!(&*b.committed_xip, &[9]);

    b.process_running_xacts(&mut rb, 0x300, &running(20, 25, &[]))
        .unwrap();
    assert!(b.committed_xip.is_empty());
    assert_eq!(b.xmin, 20);
}

#[test]
fn process_change_gates_on_state_and_next_phase() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);

    assert!(!b.process_change(&mut rb, 7, 0x10));

    b.state = Building;
    b.next_phase_at = 10;
    assert!(!b.process_change(&mut rb, 7, 0x10));

    b.state = FullSnapshot;
    b.xmin = 10;
    b.xmax = 10;
    // pre-FULL_SNAPSHOT xid: not decodable
    assert!(!b.process_change(&mut rb, 7, 0x10));
    assert!(b.process_change(&mut rb, 12, 0x20));
    assert!(rb.xid_has_base_snapshot(12));
    let first = b.snapshot.clone().unwrap();

    assert!(b.process_change(&mut rb, 13, 0x30));
    assert!(std::rc::Rc::ptr_eq(&first, &b.snapshot.clone().unwrap()));
}

#[test]
fn get_or_build_snapshot_reuses_prebuilt() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();
    let s1 = b.get_or_build_snapshot();
    let s2 = b.get_or_build_snapshot();
    assert!(std::rc::Rc::ptr_eq(&s1, &s2));
}

#[test]
fn distribute_adds_snapshot_to_in_progress_txns_with_base_snapshot() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.process_running_xacts(&mut rb, 0x100, &running(8, 8, &[]))
        .unwrap();

    // txn 20 has a base snapshot; txn 30 has none.
    assert!(b.process_change(&mut rb, 20, 0x150));
    rb.process_xid(30, 0x160);

    rb.xid_set_catalog_changes(40, 0x170);
    b.commit_txn(&mut rb, 0x200, 40, &[], 0).unwrap();

    let txn20 = rb.toplevel_txns().find(|&id| rb.txn(id).xid == 20).unwrap();
    // one distributed-snapshot change queued on txn 20
    assert_eq!(rb.txn(txn20).nentries, 1);
    let txn30 = rb.toplevel_txns().find(|&id| rb.txn(id).xid == 30).unwrap();
    assert_eq!(rb.txn(txn30).nentries, 0);
}

#[test]
fn committed_growth_follows_c_schedule() {
    let _g = test_lock();
    boot();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    assert_eq!(b.committed_xcnt_space, 128);
    for xid in 0..128u32 {
        b.add_committed_txn(xid + 3).unwrap();
    }
    assert_eq!(b.committed_xcnt_space, 128);
    b.add_committed_txn(1000).unwrap();
    assert_eq!(b.committed_xcnt_space, 257);
    assert_eq!(b.committed_xip.len(), 129);
}

#[test]
fn ondisk_known_answer() {
    let _g = test_lock();
    boot();
    let mut b = allocate_snapshot_builder(3, 0x0000000100000028, false, false, 0);
    b.state = Consistent;
    b.xmin = 100;
    b.xmax = 200;
    b.last_serialized_snapshot = 0x100000000;
    b.next_phase_at = InvalidTransactionId;
    b.committed_xip.extend_from_slice(&[10, 11]);

    let image = ondisk::build_image(&b, &[12]);
    assert_eq!(image.len(), 156);
    assert_eq!(u32::from_ne_bytes(image[0..4].try_into().unwrap()), SNAPBUILD_MAGIC);
    assert_eq!(u32::from_ne_bytes(image[8..12].try_into().unwrap()), SNAPBUILD_VERSION);
    assert_eq!(u32::from_ne_bytes(image[12..16].try_into().unwrap()), 156);
    // CRC-32C known answer computed independently over bytes [8..156].
    assert_eq!(
        u32::from_ne_bytes(image[4..8].try_into().unwrap()),
        0x40289915
    );
    assert_eq!(ondisk::image_checksum(&image), 0x40289915);
}

#[test]
fn serialize_then_restore_round_trips_through_disk() {
    let _g = test_lock();
    boot();
    let mut rb1 = rb();
    let mut b1 = allocate_snapshot_builder(0, 0, false, false, 0);
    b1.process_running_xacts(&mut rb1, 0x100, &running(8, 8, &[]))
        .unwrap();
    rb1.xid_set_catalog_changes(9, 0x150);
    b1.commit_txn(&mut rb1, 0x200, 9, &[], 0).unwrap();

    let lsn = 0x300;
    let _ = std::fs::remove_file(ondisk::snapshot_path(lsn));
    b1.serialization_point(&mut rb1, lsn).unwrap();
    assert!(snap_build_snapshot_exists(lsn).unwrap());
    assert_eq!(b1.last_serialized_snapshot, lsn);
    assert_eq!(rb1.current_restart_decoding_lsn(), lsn);

    // serializing again at the same LSN reuses the existing file
    let mut rb1b = rb();
    let mut b1b = allocate_snapshot_builder(0, 0, false, false, 0);
    b1b.state = Consistent;
    b1b.xmin = 8;
    b1b.xmax = 10;
    b1b.serialization_point(&mut rb1b, lsn).unwrap();
    assert_eq!(b1b.last_serialized_snapshot, lsn);

    let mut rb2 = rb();
    let mut b2 = allocate_snapshot_builder(0, 0, false, false, 0);
    b2.serialization_point(&mut rb2, lsn).unwrap();
    assert_eq!(b2.current_state(), Consistent);
    assert_eq!(b2.xmin, 8);
    assert_eq!(b2.xmax, 10);
    assert_eq!(&*b2.committed_xip, &[9]);
    assert_eq!(b2.committed_xcnt_space, 1);
    assert_eq!(rb2.current_restart_decoding_lsn(), lsn);
    let snap = b2.snapshot.clone().unwrap();
    assert_eq!(&snap.xip[..snap.xcnt as usize], &[9]);

    // an on-disk state below the xmin horizon is not interesting
    let mut rb3 = rb();
    let mut b3 = allocate_snapshot_builder(50, 0, false, false, 0);
    b3.serialization_point(&mut rb3, lsn).unwrap();
    assert_eq!(b3.current_state(), Start);

    let _ = std::fs::remove_file(ondisk::snapshot_path(lsn));
}

#[test]
fn restore_missing_file_is_not_interesting() {
    let _g = test_lock();
    boot();
    let mut rb = rb();
    let mut b = allocate_snapshot_builder(0, 0, false, false, 0);
    b.serialization_point(&mut rb, 0xDEAD0000).unwrap();
    assert_eq!(b.current_state(), Start);
}

#[test]
fn in_slot_creation_skips_restore() {
    let _g = test_lock();
    boot();
    let mut rb1 = rb();
    let mut b1 = allocate_snapshot_builder(0, 0, false, false, 0);
    b1.process_running_xacts(&mut rb1, 0x100, &running(8, 8, &[]))
        .unwrap();
    let lsn = 0x400;
    let _ = std::fs::remove_file(ondisk::snapshot_path(lsn));
    b1.serialization_point(&mut rb1, lsn).unwrap();

    let mut rb2 = rb();
    let mut b2 = allocate_snapshot_builder(0, 0, false, true, 0);
    b2.process_running_xacts(&mut rb2, lsn, &running(5, 10, &[]))
        .unwrap();
    // running xacts present and restore skipped: BUILDING, not CONSISTENT
    assert_eq!(b2.current_state(), Building);

    let mut rb3 = rb();
    let mut b3 = allocate_snapshot_builder(0, 0, false, false, 0);
    b3.process_running_xacts(&mut rb3, lsn, &running(5, 10, &[]))
        .unwrap();
    assert_eq!(b3.current_state(), Consistent);

    let _ = std::fs::remove_file(ondisk::snapshot_path(lsn));
}

#[test]
fn xact_needs_skip_uses_start_decoding_at() {
    let _g = test_lock();
    boot();
    let b = allocate_snapshot_builder(0, 0x100, false, false, 0);
    assert!(b.xact_needs_skip(0xFF));
    assert!(!b.xact_needs_skip(0x100));
}

#[test]
fn parse_snap_name_matches_sscanf() {
    assert_eq!(ondisk::parse_snap_name("A-B.snap"), Some((0xA, 0xB)));
    assert_eq!(
        ondisk::parse_snap_name("1-2.snap.123.tmp"),
        Some((1, 2))
    );
    // sscanf counts both conversions before the literal mismatch
    assert_eq!(ondisk::parse_snap_name("A-B-C.snap"), Some((0xA, 0xB)));
    assert_eq!(ondisk::parse_snap_name("12X-3.snap"), None);
    assert_eq!(ondisk::parse_snap_name("state"), None);
    assert_eq!(ondisk::parse_snap_name("-3.snap"), None);
    assert_eq!(ondisk::parse_snap_name("3-.snap"), None);
}

#[test]
fn state_discriminants_match_c() {
    assert_eq!(Start as i32, -1);
    assert_eq!(Building as i32, 0);
    assert_eq!(FullSnapshot as i32, 1);
    assert_eq!(Consistent as i32, 2);
    assert!(Start < Building && Building < FullSnapshot && FullSnapshot < Consistent);
}
