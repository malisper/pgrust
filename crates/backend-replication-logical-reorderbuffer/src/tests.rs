//! Seam-free unit tests for the foundational reorderbuffer family.

use super::*;
use types_snapshot::snapshot::{GlobalVisStateHandle, SnapshotType};
use types_storage::sinval::SharedInvalSnapshotMsg;

fn mk_inval() -> SharedInvalidationMessage {
    SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg { dbId: 0, relId: 0 })
}

fn mk_snapshot(xmin: TransactionId) -> SnapshotData {
    SnapshotData {
        snapshot_type: SnapshotType::SNAPSHOT_HISTORIC_MVCC,
        vistest: GlobalVisStateHandle::new(0),
        xmin,
        xmax: xmin + 100,
        xcnt: 0,
        xip: Vec::new(),
        subxcnt: 0,
        subxip: Vec::new(),
        suboverflowed: false,
        takenDuringRecovery: false,
        copied: false,
        curcid: 0,
        speculativeToken: 0,
        active_count: 0,
        regd_count: 0,
        snapXactCompletionCount: 0,
    }
}

#[test]
fn process_xid_creates_toplevel_txn() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(100, 0x10);
    assert_eq!(rb.toplevel_txns(), vec![100]);
    // InvalidTransactionId is a no-op.
    rb.process_xid(InvalidTransactionId, 0x20);
    assert_eq!(rb.toplevel_txns(), vec![100]);
}

#[test]
fn one_entry_cache_hit_and_miss() {
    let mut rb = ReorderBuffer::allocate();
    // miss + no create => None and caches "does not exist"
    assert!(!rb.xid_has_catalog_changes(50));
    // create via process_xid
    rb.process_xid(50, 0x10);
    assert!(!rb.xid_has_catalog_changes(50));
    rb.xid_set_catalog_changes(50, 0x10);
    assert!(rb.xid_has_catalog_changes(50));
}

#[test]
fn base_snapshot_and_oldest_xmin() {
    let mut rb = ReorderBuffer::allocate();
    assert_eq!(rb.get_oldest_xmin(), InvalidTransactionId);
    rb.set_base_snapshot(200, 0x100, mk_snapshot(42));
    rb.set_base_snapshot(300, 0x200, mk_snapshot(99));
    assert!(rb.xid_has_base_snapshot(200));
    assert!(rb.xid_has_base_snapshot(300));
    assert!(!rb.xid_has_base_snapshot(400));
    // oldest (lowest base-snapshot lsn) is xid 200, xmin 42
    assert_eq!(rb.get_oldest_xmin(), 42);
}

#[test]
fn catalog_changes_xacts_sorted_and_counted() {
    let mut rb = ReorderBuffer::allocate();
    rb.xid_set_catalog_changes(300, 0x10);
    rb.xid_set_catalog_changes(100, 0x20);
    rb.xid_set_catalog_changes(200, 0x30);
    assert_eq!(rb.catchange_count(), 3);
    assert_eq!(rb.get_catalog_changes_xacts(), vec![100, 200, 300]);
    // idempotent: setting again does not duplicate
    rb.xid_set_catalog_changes(100, 0x40);
    assert_eq!(rb.catchange_count(), 3);
}

#[test]
fn oldest_txn_is_first_by_lsn() {
    let mut rb = ReorderBuffer::allocate();
    assert_eq!(rb.get_oldest_txn(), None);
    rb.process_xid(700, 0x10);
    rb.process_xid(800, 0x20);
    assert_eq!(rb.get_oldest_txn(), Some(700));
}

#[test]
fn add_new_command_id_and_snapshot_queue_changes() {
    let mut rb = ReorderBuffer::allocate();
    rb.add_new_command_id(500, 0x10, 7);
    rb.add_snapshot(500, 0x20, mk_snapshot(5));
    // both queued onto the txn's change list
    let n = rb.with_txn_for_test(500, |t| t.changes.len());
    assert_eq!(n, 2);
}

#[test]
fn add_new_tuple_cids_appends_to_tuplecids() {
    let mut rb = ReorderBuffer::allocate();
    rb.add_new_tuple_cids(
        600,
        0x10,
        RelFileLocator::default(),
        ItemPointerData::default(),
        1,
        2,
        3,
    );
    let (nt, ntc) = rb.with_txn_for_test(600, |t| (t.tuplecids.len(), t.ntuplecids));
    assert_eq!(nt, 1);
    assert_eq!(ntc, 1);
}

#[test]
fn invalidations_round_trip() {
    let mut rb = ReorderBuffer::allocate();
    // no txn yet => empty
    assert!(rb.get_invalidations(900).is_empty());
    rb.process_xid(900, 0x10);
    // distributed invalidations accumulate on the (top) txn
    let msgs = vec![mk_inval()];
    rb.add_distributed_invalidations(900, 0x20, msgs.clone());
    let n = rb.with_txn_for_test(900, |t| t.invalidations_distributed.len());
    assert_eq!(n, 1);
}

impl ReorderBuffer {
    /// Test-only accessor mirroring `with_txn`.
    fn with_txn_for_test<R>(
        &mut self,
        xid: TransactionId,
        f: impl FnOnce(&mut ReorderBufferTXN) -> R,
    ) -> R {
        self.with_txn(xid, f)
    }
}
