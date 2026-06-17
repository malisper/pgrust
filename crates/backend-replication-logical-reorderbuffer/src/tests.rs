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
        reg_id: 0,
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

#[test]
fn build_tuple_cid_hash_maps_locator_tid_to_cmin_cmax() {
    let mut rb = ReorderBuffer::allocate();
    // No catalog changes => no hash built.
    rb.add_new_tuple_cids(
        600,
        0x10,
        RelFileLocator::default(),
        ItemPointerData::new(5, 7),
        1,
        2,
        0,
    );
    rb.build_tuple_cid_hash(600);
    assert!(rb.with_txn_for_test(600, |t| t.tuplecid_hash.is_none()));

    // With catalog changes the hash is built from the recorded tuplecids.
    rb.xid_set_catalog_changes(600, 0x20);
    rb.build_tuple_cid_hash(600);
    let key = ReorderBufferTupleCidKey {
        rlocator: RelFileLocator::default(),
        tid: ItemPointerData::new(5, 7),
    };
    let ent = rb
        .with_txn_for_test(600, |t| t.tuplecid_hash.as_ref().unwrap().get(&key).copied())
        .unwrap();
    assert_eq!(ent.cmin, 1);
    assert_eq!(ent.cmax, 2);
}

#[test]
fn copy_snap_builds_subxip_with_toplevel_and_subtxns() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(1000, 0x10);
    // record two subtransactions on the txn.
    rb.with_txn_for_test(1000, |t| {
        t.subtxns = vec![1005, 1002];
        t.nsubtxns = 2;
    });
    let orig = {
        let mut s = mk_snapshot(10);
        s.xip = vec![3, 4];
        s.xcnt = 2;
        s
    };
    let snap = rb.copy_snap(&orig, 1000, 42);
    assert!(snap.copied);
    assert_eq!(snap.active_count, 1);
    assert_eq!(snap.regd_count, 0);
    assert_eq!(snap.curcid, 42);
    assert_eq!(snap.xip, vec![3, 4]);
    // subxip = toplevel xid + subtxns, sorted.
    assert_eq!(snap.subxip, vec![1000, 1002, 1005]);
    assert_eq!(snap.subxcnt, 3);
}

// ---------------------------------------------------------------------------
// change-replay family tests
// ---------------------------------------------------------------------------

fn mk_change(lsn: XLogRecPtr) -> ReorderBufferChange {
    let mut c = ReorderBufferChange::alloc();
    c.lsn = lsn;
    c.action = ReorderBufferChangeType::Message;
    c.data = ReorderBufferChangeData::Msg {
        prefix: Vec::new(),
        message: Vec::new(),
    };
    c
}

#[test]
fn assign_child_links_subxact_and_marks_top() {
    let mut rb = ReorderBuffer::allocate();
    // toplevel 100 seen, then 105 seen as a top-level txn first.
    rb.process_xid(100, 0x10);
    rb.process_xid(105, 0x20);
    assert_eq!(rb.toplevel_txns(), vec![100, 105]);

    rb.assign_child(100, 105, 0x30);
    // 105 removed from toplevel list, marked subxact of 100.
    assert_eq!(rb.toplevel_txns(), vec![100]);
    assert!(rb.with_txn_for_test(105, |t| t.is_known_subxact()));
    assert_eq!(rb.with_txn_for_test(105, |t| t.toplevel_xid), 100);
    assert_eq!(rb.with_txn_for_test(100, |t| t.nsubtxns), 1);
    assert_eq!(rb.with_txn_for_test(100, |t| t.subtxns.clone()), vec![105]);

    // idempotent: assigning again is a no-op.
    rb.assign_child(100, 105, 0x40);
    assert_eq!(rb.with_txn_for_test(100, |t| t.nsubtxns), 1);
}

#[test]
fn transfer_snap_to_parent_moves_base_snapshot_up() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(200, 0x10);
    rb.process_xid(205, 0x20);
    // subxact 205 has a base snapshot, toplevel 200 does not.
    rb.set_base_snapshot(205, 0x25, mk_snapshot(7));
    assert!(rb.with_txn_for_test(205, |t| t.base_snapshot.is_some()));

    rb.assign_child(200, 205, 0x30);
    // snapshot transferred to the toplevel; subxact cleared.
    assert!(rb.with_txn_for_test(200, |t| t.base_snapshot.is_some()));
    assert!(rb.with_txn_for_test(205, |t| t.base_snapshot.is_none()));
    assert_eq!(rb.get_oldest_xmin(), 7);
}

#[test]
fn commit_child_associates_then_unknown_is_noop() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(300, 0x10);
    rb.process_xid(305, 0x20);
    rb.commit_child(300, 305, 0x40, 0x41);
    assert!(rb.with_txn_for_test(305, |t| t.is_known_subxact()));
    assert_eq!(rb.with_txn_for_test(305, |t| t.final_lsn), 0x40);
    assert_eq!(rb.with_txn_for_test(305, |t| t.end_lsn), 0x41);
    // a subxid we never saw -> nothing happens.
    rb.commit_child(300, 999, 0x50, 0x51);
    assert_eq!(rb.with_txn_for_test(300, |t| t.nsubtxns), 1);
}

#[test]
fn iter_merges_changes_across_subtxns_in_lsn_order() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(400, 0x1);
    rb.process_xid(405, 0x2);
    rb.assign_child(400, 405, 0x3);

    // toplevel changes at lsn 0x11, 0x14; subxact changes at 0x12, 0x13, 0x15.
    // first_lsn (0x1/0x2) <= all change lsns, satisfying AssertChangeLsnOrder.
    rb.with_txn_for_test(400, |t| {
        t.changes = vec![mk_change(0x11), mk_change(0x14)];
        t.nentries = 2;
        t.nentries_mem = 2;
    });
    rb.with_txn_for_test(405, |t| {
        t.changes = vec![mk_change(0x12), mk_change(0x13), mk_change(0x15)];
        t.nentries = 3;
        t.nentries_mem = 3;
    });

    let lsns = rb.iter_lsns_for_test(400);
    assert_eq!(lsns, vec![0x11, 0x12, 0x13, 0x14, 0x15]);
}

#[test]
fn cleanup_txn_removes_txn_and_subtxns() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(500, 0x10);
    rb.process_xid(505, 0x20);
    rb.assign_child(500, 505, 0x30);
    rb.with_txn_for_test(500, |t| {
        t.changes = vec![mk_change(1)];
        t.nentries = 1;
        t.nentries_mem = 1;
    });

    rb.cleanup_txn(500);
    // both the toplevel and its subxact are gone from by_txn + lists.
    assert!(rb.by_txn_get(500).is_none());
    assert!(rb.by_txn_get(505).is_none());
    assert_eq!(rb.toplevel_txns(), Vec::<TransactionId>::new());
}

#[test]
fn queue_decoded_change_appends_tp_change_and_marks_streamable() {
    use backend_replication_logical_reorderbuffer_seams::{DecodedChangeKind, DecodedTuple};
    let mut rb = ReorderBuffer::allocate();
    let newt = DecodedTuple {
        t_len: 4,
        t_self: ItemPointerData::new(1, 2),
        t_table_oid: 42,
        data: vec![0xde, 0xad, 0xbe, 0xef],
    };
    rb.queue_decoded_change(
        700,
        0x100,
        DecodedChangeKind::Insert,
        RelFileLocator::default(),
        None,
        Some(newt),
        false,
    );
    // change is queued on the txn, counters bumped, streamable flag set.
    assert_eq!(rb.with_txn_for_test(700, |t| t.changes.len()), 1);
    assert_eq!(rb.with_txn_for_test(700, |t| t.nentries), 1);
    assert!(rb.with_txn_for_test(700, |t| t.txn_flags & RBTXN_HAS_STREAMABLE_CHANGE != 0));
    let ok = rb.with_txn_for_test(700, |t| match &t.changes[0].data {
        ReorderBufferChangeData::Tp { newtuple, oldtuple, .. } => {
            oldtuple.is_none()
                && newtuple.as_ref().map(|b| (b.t_len, b.data.clone()))
                    == Some((4, vec![0xde, 0xad, 0xbe, 0xef]))
        }
        _ => false,
    });
    assert!(ok);
    assert_eq!(rb.with_txn_for_test(700, |t| t.changes[0].action), ReorderBufferChangeType::Insert);
}

#[test]
fn queue_truncate_appends_truncate_change() {
    let mut rb = ReorderBuffer::allocate();
    rb.queue_truncate(710, 0x100, true, false, vec![9, 10]);
    assert_eq!(rb.with_txn_for_test(710, |t| t.changes.len()), 1);
    let ok = rb.with_txn_for_test(710, |t| match &t.changes[0].data {
        ReorderBufferChangeData::Truncate { cascade, restart_seqs, relids } => {
            *cascade && !*restart_seqs && relids == &[9, 10]
        }
        _ => false,
    });
    assert!(ok);
}

#[test]
fn queue_message_transactional_appends_message() {
    let mut rb = ReorderBuffer::allocate();
    rb.queue_message(720, 0x100, true, b"pre".to_vec(), b"body".to_vec());
    let ok = rb.with_txn_for_test(720, |t| match &t.changes[0].data {
        ReorderBufferChangeData::Msg { prefix, message } => prefix == b"pre" && message == b"body",
        _ => false,
    });
    assert!(ok);
}

#[test]
fn add_invalidations_accumulates_under_toptxn_and_queues() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(730, 0x10);
    rb.process_xid(735, 0x20);
    rb.assign_child(730, 735, 0x30);
    // invalidations recorded against a subxact collect under the toplevel txn.
    rb.add_invalidations(735, 0x40, vec![mk_inval(), mk_inval()]);
    assert_eq!(rb.with_txn_for_test(730, |t| t.invalidations.len()), 2);
    // and an Invalidation change is queued.
    let queued = rb.with_txn_for_test(735, |t| {
        t.changes.iter().any(|c| matches!(c.data, ReorderBufferChangeData::Inval(_)))
    });
    assert!(queued);
}

#[test]
fn remember_prepare_info_then_skip_prepare_sets_flags() {
    let mut rb = ReorderBuffer::allocate();
    // unknown txn -> false, no creation.
    assert!(!rb.remember_prepare_info(740, 0x10, 0x11, 0, 0, 0));

    rb.process_xid(740, 0x10);
    assert!(rb.remember_prepare_info(740, 0x100, 0x101, 123, 5, 0x99));
    assert_eq!(rb.with_txn_for_test(740, |t| t.final_lsn), 0x100);
    assert_eq!(rb.with_txn_for_test(740, |t| t.end_lsn), 0x101);
    assert_eq!(rb.with_txn_for_test(740, |t| t.origin_id), 5);
    assert!(rb.with_txn_for_test(740, |t| t.is_prepared()));

    rb.skip_prepare(740);
    assert!(rb.with_txn_for_test(740, |t| t.txn_flags & RBTXN_SKIPPED_PREPARE != 0));
}

#[test]
fn abort_unknown_txn_is_noop() {
    let mut rb = ReorderBuffer::allocate();
    // No txn -> nothing to remove, no panic.
    rb.abort(999, 0x10, 0);
    rb.forget(999, 0x10);
    rb.invalidate(999, 0x10);
}

#[test]
fn abort_removes_non_streamed_txn_without_invalidations() {
    let mut rb = ReorderBuffer::allocate();
    rb.process_xid(750, 0x10);
    rb.with_txn_for_test(750, |t| {
        t.changes = vec![mk_change(0x20)];
        t.nentries = 1;
        t.nentries_mem = 1;
    });
    rb.abort(750, 0x30, 555);
    // cleanup removed the txn.
    assert!(rb.by_txn_get(750).is_none());
    assert_eq!(rb.toplevel_txns(), Vec::<TransactionId>::new());
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

    /// Test-only driver of the k-way merge: collect the LSNs the iterator
    /// yields for `txn_xid` (and its subtxns) in order.
    fn iter_lsns_for_test(&mut self, txn_xid: TransactionId) -> Vec<XLogRecPtr> {
        self.iter_lsns_collect(txn_xid)
    }
}
