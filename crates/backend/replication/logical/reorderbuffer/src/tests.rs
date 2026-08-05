use std::cell::RefCell;
use std::rc::Rc;

use datum::Datum;
use mcx::{PgString, PgVec};
use snapmgr::Snapshot;
use types_core::{InvalidCommandId, Oid, TransactionId, XLogRecPtr};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::{SharedInvalCatcacheMsg, SharedInvalidationMessage};
use types_tuple::{
    FormData_pg_attribute, NameData, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn rb() -> ReorderBuffer {
    crate::startup::install_gucs();
    ReorderBuffer::allocate("test_slot").expect("allocate")
}

fn snap(xmin: TransactionId) -> Snapshot {
    let mut s = SnapshotData::sentinel(rb_mcx(), SnapshotType::SNAPSHOT_MVCC);
    s.xmin = xmin;
    s.xmax = xmin;
    Rc::new(s)
}

fn msg_change(text: &str) -> ReorderBufferChange {
    let mcx = rb_mcx();
    let mut message = PgVec::new_in(mcx);
    mcx::vec_append_bytes(&mut message, text.as_bytes()).unwrap();
    ReorderBufferChange::new(
        Message,
        ReorderBufferChangeData::Msg {
            prefix: PgString::from_str_in("test", mcx).unwrap(),
            message,
        },
    )
}

fn inval_msg(hash: u32) -> SharedInvalidationMessage {
    SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg { id: 1, dbId: 5, hashValue: hash })
}

#[test]
fn change_type_codes_match_reorderbuffer_h() {
    assert_eq!(Insert as i32, 0);
    assert_eq!(Update as i32, 1);
    assert_eq!(Delete as i32, 2);
    assert_eq!(Message as i32, 3);
    assert_eq!(Invalidation as i32, 4);
    assert_eq!(InternalSnapshot as i32, 5);
    assert_eq!(InternalCommandId as i32, 6);
    assert_eq!(InternalTupleCid as i32, 7);
    assert_eq!(InternalSpecInsert as i32, 8);
    assert_eq!(InternalSpecConfirm as i32, 9);
    assert_eq!(InternalSpecAbort as i32, 10);
    assert_eq!(Truncate as i32, 11);
}

#[test]
fn txn_flags_match_reorderbuffer_h() {
    assert_eq!(RBTXN_HAS_CATALOG_CHANGES, 0x0001);
    assert_eq!(RBTXN_IS_SUBXACT, 0x0002);
    assert_eq!(RBTXN_IS_SERIALIZED, 0x0004);
    assert_eq!(RBTXN_IS_SERIALIZED_CLEAR, 0x0008);
    assert_eq!(RBTXN_IS_STREAMED, 0x0010);
    assert_eq!(RBTXN_HAS_PARTIAL_CHANGE, 0x0020);
    assert_eq!(RBTXN_IS_PREPARED, 0x0040);
    assert_eq!(RBTXN_SKIPPED_PREPARE, 0x0080);
    assert_eq!(RBTXN_HAS_STREAMABLE_CHANGE, 0x0100);
    assert_eq!(RBTXN_SENT_PREPARE, 0x0200);
    assert_eq!(RBTXN_IS_COMMITTED, 0x0400);
    assert_eq!(RBTXN_IS_ABORTED, 0x0800);
    assert_eq!(RBTXN_DISTR_INVAL_OVERFLOWED, 0x1000);
}

#[test]
fn txn_by_xid_creates_and_caches() {
    let mut rb = rb();
    rb.process_xid(10, 100);
    rb.process_xid(5, 200);

    let (a, is_new) = rb.txn_by_xid(10, false, 0, false);
    assert!(!is_new);
    let a = a.unwrap();
    assert_eq!(rb.txn(a).xid, 10);
    assert_eq!(rb.txn(a).first_lsn, 100);

    let (a2, _) = rb.txn_by_xid(10, false, 0, false);
    assert_eq!(a2, Some(a));

    assert!(rb.txn_by_xid(99, false, 0, false).0.is_none());
    // Cached negative lookup stays negative until a create.
    assert!(rb.txn_by_xid(99, false, 0, false).0.is_none());
    let (c, is_new) = rb.txn_by_xid(99, true, 300, true);
    assert!(is_new);
    assert!(c.is_some());

    let oldest = rb.get_oldest_txn().unwrap();
    assert_eq!(rb.txn(oldest).xid, 10);
}

#[test]
fn assign_child_moves_subtxn_off_toplevel_list() {
    let mut rb = rb();
    rb.process_xid(2, 50);
    rb.process_xid(1, 60);

    rb.assign_child(1, 2, 60);
    let (sub, _) = rb.txn_by_xid(2, false, 0, false);
    let sub = sub.unwrap();
    assert!(rb.txn(sub).is_known_subxact());
    assert_eq!(rb.txn(sub).toplevel_xid, 1);

    let (top, _) = rb.txn_by_xid(1, false, 0, false);
    let top = top.unwrap();
    assert_eq!(rb.txn(top).nsubtxns, 1);
    assert_eq!(rb.get_oldest_txn(), Some(top));

    // Idempotent for an already-known subxact.
    rb.assign_child(1, 2, 70);
    assert_eq!(rb.txn(top).nsubtxns, 1);
}

#[test]
fn base_snapshot_transfers_to_parent_when_earlier() {
    let mut rb = rb();
    rb.process_xid(2, 10);
    rb.process_xid(1, 20);
    rb.set_base_snapshot(2, 10, snap(700));
    rb.set_base_snapshot(1, 20, snap(800));

    rb.assign_child(1, 2, 25);

    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    let sub = rb.txn_by_xid(2, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(top).base_snapshot_lsn, 10);
    assert_eq!(rb.txn(top).base_snapshot.as_ref().unwrap().xmin, 700);
    assert!(rb.txn(sub).base_snapshot.is_none());
    assert_eq!(rb.get_oldest_xmin(), 700);
}

#[test]
fn base_snapshot_kept_when_parent_earlier() {
    let mut rb = rb();
    rb.process_xid(1, 10);
    rb.process_xid(2, 15);
    rb.set_base_snapshot(1, 10, snap(600));
    rb.set_base_snapshot(2, 15, snap(650));

    rb.assign_child(1, 2, 25);

    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    let sub = rb.txn_by_xid(2, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(top).base_snapshot_lsn, 10);
    assert_eq!(rb.txn(top).base_snapshot.as_ref().unwrap().xmin, 600);
    assert!(rb.txn(sub).base_snapshot.is_none());
}

#[test]
fn xid_has_base_snapshot_follows_toplevel() {
    let mut rb = rb();
    rb.process_xid(1, 10);
    rb.process_xid(2, 15);
    rb.assign_child(1, 2, 15);
    assert!(!rb.xid_has_base_snapshot(2));
    rb.set_base_snapshot(2, 16, snap(500));
    // A known subxact's base snapshot lands on the toplevel txn.
    assert!(rb.xid_has_base_snapshot(1));
    assert!(rb.xid_has_base_snapshot(2));
    let sub = rb.txn_by_xid(2, false, 0, false).0.unwrap();
    assert!(rb.txn(sub).base_snapshot.is_none());
}

#[test]
fn queue_change_updates_memory_accounting() {
    let mut rb = rb();
    rb.queue_change(7, 100, msg_change("hello"), false).unwrap();

    let txn = rb.txn_by_xid(7, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(txn).nentries, 1);
    assert_eq!(rb.txn(txn).nentries_mem, 1);
    assert!(rb.size > 0);
    assert_eq!(rb.txn(txn).size, rb.size);
    assert_eq!(rb.txn(txn).total_size, rb.size);

    let cid = rb.txn(txn).changes.head;
    let expected = std::mem::size_of::<ReorderBufferChange>()
        + "test".len() + 1
        + "hello".len()
        + 2 * std::mem::size_of::<usize>();
    assert_eq!(rb.change_size(cid), expected);

    rb.cleanup_txn(txn).unwrap();
    assert_eq!(rb.size, 0);
    assert!(rb.txn_by_xid(7, false, 0, false).0.is_none());
    assert!(rb.get_oldest_txn().is_none());
}

#[test]
fn subtxn_changes_roll_up_into_top_total_size() {
    let mut rb = rb();
    rb.queue_change(1, 10, msg_change("a"), false).unwrap();
    let s_a = rb.size;
    rb.queue_change(2, 11, msg_change("b"), false).unwrap();
    let s_b = rb.size - s_a;
    rb.assign_child(1, 2, 11);
    rb.queue_change(2, 12, msg_change("c"), false).unwrap();
    let s_c = rb.size - s_a - s_b;

    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    let sub = rb.txn_by_xid(2, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(top).size, s_a);
    assert_eq!(rb.txn(sub).size, s_b + s_c);
    // As in C, total_size accrued before the child assignment stays where it
    // was counted; only post-assignment changes roll up to the new top.
    assert_eq!(rb.txn(top).total_size, s_a + s_c);
    assert_eq!(rb.txn(sub).total_size, s_b);
    assert_eq!(rb.txn(top).size + rb.txn(sub).size, rb.size);

    rb.cleanup_txn(top).unwrap();
    assert_eq!(rb.size, 0);
    assert!(rb.txn_by_xid(2, false, 0, false).0.is_none());
}

#[test]
fn iterator_merges_subtxn_streams_in_lsn_order() {
    let mut rb = rb();
    for lsn in [1u64, 4, 7] {
        rb.queue_change(1, lsn, msg_change("t"), false).unwrap();
    }
    for lsn in [2u64, 5, 8] {
        rb.queue_change(2, lsn, msg_change("s"), false).unwrap();
    }
    for lsn in [3u64, 5, 9] {
        rb.queue_change(3, lsn, msg_change("u"), false).unwrap();
    }
    rb.assign_child(1, 2, 20);
    rb.assign_child(1, 3, 21);

    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    let lsns = rb.iter_collect_lsns(top);
    assert_eq!(lsns, vec![1, 2, 3, 4, 5, 5, 7, 8, 9]);
}

#[test]
fn iterator_handles_empty_and_single_stream() {
    let mut rb = rb();
    rb.process_xid(1, 5);
    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    assert!(rb.iter_collect_lsns(top).is_empty());

    for lsn in [6u64, 7, 8] {
        rb.queue_change(1, lsn, msg_change("x"), false).unwrap();
    }
    assert_eq!(rb.iter_collect_lsns(top), vec![6, 7, 8]);
}

#[test]
fn truncate_txn_discards_changes_keeps_txn() {
    let mut rb = rb();
    rb.queue_change(1, 10, msg_change("a"), false).unwrap();
    rb.queue_change(1, 11, msg_change("b"), false).unwrap();
    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(top).nentries, 2);

    rb.truncate_txn(top, false).unwrap();
    assert_eq!(rb.txn(top).nentries, 0);
    assert_eq!(rb.txn(top).nentries_mem, 0);
    assert_eq!(rb.txn(top).size, 0);
    assert_eq!(rb.size, 0);
    assert!(rb.txn_by_xid(1, false, 0, false).0.is_some());
    rb.cleanup_txn(top).unwrap();
}

#[test]
fn invalidations_accumulate_on_toplevel() {
    let mut rb = rb();
    rb.process_xid(1, 10);
    rb.process_xid(2, 11);
    rb.assign_child(1, 2, 11);

    rb.add_invalidations(2, 12, &[inval_msg(1), inval_msg(2)]).unwrap();
    rb.add_invalidations(1, 13, &[inval_msg(3)]).unwrap();

    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    let sub = rb.txn_by_xid(2, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(top).invalidations.len(), 3);
    assert!(rb.txn(sub).invalidations.is_empty());
    // The change itself is queued under the originating xid.
    assert_eq!(rb.txn(sub).nentries, 1);
    assert_eq!(rb.txn(top).nentries, 1);
    assert_eq!(rb.get_invalidations(1).len(), 3);
    assert_eq!(rb.get_invalidations(2).len(), 0);
}

#[test]
fn distributed_invalidations_overflow_sets_flag_and_clears() {
    let mut rb = rb();
    rb.process_xid(1, 10);

    let half = MAX_DISTR_INVAL_MSG_PER_TXN / 2 + 1;
    let msgs: Vec<SharedInvalidationMessage> = vec![inval_msg(7); half];
    rb.add_distributed_invalidations(1, 11, &msgs).unwrap();
    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    assert!(!rb.txn(top).distr_inval_overflowed());
    assert_eq!(rb.txn(top).invalidations_distributed.len(), half);

    rb.add_distributed_invalidations(1, 12, &msgs).unwrap();
    assert!(rb.txn(top).distr_inval_overflowed());
    assert!(rb.txn(top).invalidations_distributed.is_empty());

    // Further messages are dropped from the distributed store, still queued.
    rb.add_distributed_invalidations(1, 13, &[inval_msg(9)]).unwrap();
    assert!(rb.txn(top).invalidations_distributed.is_empty());
    assert_eq!(rb.txn(top).nentries, 3);
}

#[test]
fn catalog_changes_tracked_and_sorted() {
    let mut rb = rb();
    rb.process_xid(9, 10);
    rb.process_xid(3, 11);
    rb.xid_set_catalog_changes(9, 10);
    rb.xid_set_catalog_changes(3, 11);
    rb.xid_set_catalog_changes(3, 12);
    assert!(rb.xid_has_catalog_changes(9));
    assert!(!rb.xid_has_catalog_changes(4));
    assert_eq!(rb.get_catalog_changes_xacts(), vec![3, 9]);

    // A subxact marks its toplevel too.
    rb.process_xid(11, 13);
    rb.assign_child(11, 12, 14);
    rb.xid_set_catalog_changes(12, 15);
    assert!(rb.xid_has_catalog_changes(11));
    assert_eq!(rb.get_catalog_changes_xacts(), vec![3, 9, 11, 12]);
}

#[test]
fn copy_snap_collects_subxids_sorted() {
    let mut rb = rb();
    rb.process_xid(50, 10);
    rb.process_xid(9, 11);
    rb.process_xid(70, 12);
    rb.assign_child(50, 9, 11);
    rb.assign_child(50, 70, 12);

    let top = rb.txn_by_xid(50, false, 0, false).0.unwrap();
    let base = snap(400);
    let copy = rb.copy_snap(&base, top, 4);
    assert!(copy.copied);
    assert_eq!(copy.curcid.get(), 4);
    assert_eq!(copy.subxcnt, 3);
    assert_eq!(&copy.subxip[..3], &[9, 50, 70]);
    assert_eq!(copy.active_count.get(), 1);
    assert_eq!(copy.regd_count.get(), 0);
    assert_eq!(copy.xmin, 400);
}

#[test]
fn build_tuplecid_hash_and_resolve() {
    let mut rb = rb();
    rb.process_xid(1, 10);
    rb.xid_set_catalog_changes(1, 10);

    let locator = types_storage::RelFileLocator::new(1663, 5, 16384);
    let tid = types_tuple::ItemPointerData::new(3, 7);
    rb.add_new_tuple_cids(1, 11, locator, tid, 2, InvalidCommandId, 0);
    rb.add_new_tuple_cids(1, 12, locator, tid, 2, 5, 1);

    let top = rb.txn_by_xid(1, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(top).ntuplecids, 2);
    rb.build_tuplecid_hash(top);

    let hash = rb.txn(top).tuplecid_hash.clone().unwrap();
    {
        let h = hash.borrow();
        let ent = h
            .get(&ReorderBufferTupleCidKey { rlocator: locator, tid })
            .unwrap();
        assert_eq!(ent.cmin, 2);
        assert_eq!(ent.cmax, 5);
    }

    let any: Rc<dyn std::any::Any> = hash;
    let image = [0u64; 4];
    let htup = unsafe {
        types_tuple::HeapTupleData::from_raw_parts(image.as_ptr() as *const u8, 24, tid, 999)
    };
    let s = snap(100);
    let got = ResolveCminCmaxDuringDecoding(Some(&any), &s, &htup, locator).unwrap();
    assert_eq!(got, Some((2, 5)));

    let other = types_tuple::ItemPointerData::new(9, 9);
    let htup2 = unsafe {
        types_tuple::HeapTupleData::from_raw_parts(image.as_ptr() as *const u8, 24, other, 999)
    };
    let got = ResolveCminCmaxDuringDecoding(Some(&any), &s, &htup2, locator).unwrap();
    assert_eq!(got, None);
    assert_eq!(ResolveCminCmaxDuringDecoding(None, &s, &htup, locator).unwrap(), None);
}

thread_local! {
    static DELIVERED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn recording_message_cb(
    _rb: &mut ReorderBuffer,
    txn: Option<TxnId>,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
) -> types_error::PgResult<()> {
    DELIVERED.with(|d| {
        d.borrow_mut().push(format!(
            "{}:{}:{}:{}:{}",
            txn.map(|t| t as i64).unwrap_or(-1),
            lsn,
            transactional,
            prefix,
            String::from_utf8_lossy(message)
        ))
    });
    Ok(())
}

#[test]
fn non_transactional_message_delivered_with_historic_snapshot() {
    let mut rb = rb();
    rb.callbacks.message = recording_message_cb;
    DELIVERED.with(|d| d.borrow_mut().clear());

    assert!(!snapmgr::HistoricSnapshotActive());
    rb.queue_message(0, Some(snap(300)), 42, false, "pfx", b"payload")
        .unwrap();
    assert!(!snapmgr::HistoricSnapshotActive());

    DELIVERED.with(|d| {
        assert_eq!(d.borrow().as_slice(), ["-1:42:false:pfx:payload".to_string()]);
    });
}

#[test]
fn transactional_message_is_queued() {
    let mut rb = rb();
    rb.queue_message(4, None, 43, true, "pfx", b"body").unwrap();
    let txn = rb.txn_by_xid(4, false, 0, false).0.unwrap();
    assert_eq!(rb.txn(txn).nentries, 1);
    let cid = rb.txn(txn).changes.head;
    assert_eq!(rb.change(cid).action, Message);
    match &rb.change(cid).data {
        ReorderBufferChangeData::Msg { prefix, message } => {
            assert_eq!(prefix.as_str(), "pfx");
            assert_eq!(&message[..], b"body");
        }
        _ => panic!("expected Msg data"),
    }
}

fn attr(name: &str, num: i16, typid: Oid, len: i16, byval: bool) -> FormData_pg_attribute {
    let mut attname = NameData::default();
    attname.namestrcpy(name);
    FormData_pg_attribute {
        attname,
        atttypid: typid,
        attlen: len,
        attnum: num,
        attbyval: byval,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    }
}

fn toast_descs() -> (TupleDescData<'static>, TupleDescData<'static>) {
    let mcx = rb_mcx();
    let main_desc = tupdesc::CreateTupleDesc(
        mcx,
        &[attr("payload", 1, 25, -1, false)],
    )
    .unwrap();
    let toast_desc = tupdesc::CreateTupleDesc(
        mcx,
        &[
            attr("chunk_id", 1, 26, 4, true),
            attr("chunk_seq", 2, 23, 4, true),
            attr("chunk_data", 3, 17, -1, false),
        ],
    )
    .unwrap();
    (main_desc, toast_desc)
}

fn inline_varlena(data: &[u8]) -> Vec<u8> {
    let len = (data.len() + 4) as u32;
    let mut v = (len << 2).to_ne_bytes().to_vec();
    v.extend_from_slice(data);
    v
}

fn ondisk_toast_pointer(valueid: u32, rawsize: i32, extsize: u32) -> [u8; 18] {
    let mut p = [0u8; 18];
    p[0] = 0x01;
    p[1] = 18;
    p[2..6].copy_from_slice(&rawsize.to_ne_bytes());
    p[6..10].copy_from_slice(&extsize.to_ne_bytes());
    p[10..14].copy_from_slice(&valueid.to_ne_bytes());
    p[14..18].copy_from_slice(&16u32.to_ne_bytes());
    p
}

fn tuple_change(
    rb: &ReorderBuffer,
    desc: &TupleDescData<'static>,
    values: &[Datum],
    isnull: &[bool],
) -> ReorderBufferChange {
    let tup = heaptuple::heap_form_tuple(rb.mcx, desc, values, isnull).unwrap();
    ReorderBufferChange::new(
        Insert,
        ReorderBufferChangeData::Tp {
            rlocator: types_storage::RelFileLocator::new(1663, 5, 55555),
            clear_toast_afterwards: true,
            oldtuple: None,
            newtuple: Some(tup),
        },
    )
}

fn unlink_tail(rb: &mut ReorderBuffer, txn: TxnId) -> ChangeId {
    let id = rb.txn(txn).changes.tail;
    assert_ne!(id, INVALID_ID);
    let mut list = rb.txn(txn).changes;
    dl_delete(&mut rb.changes, &mut list, id, |c| &mut c.node);
    rb.txn_mut(txn).changes = list;
    id
}

#[test]
fn toast_chunks_reassemble_into_inline_varlena() {
    let mut rb = rb();
    let (main_desc, toast_desc) = toast_descs();
    let xid: TransactionId = 21;
    let valueid: u32 = 9001;

    let chunk1 = inline_varlena(b"hello ");
    let chunk2 = inline_varlena(b"toasted world");
    let raw_len = 6 + 13;

    for (seq, chunk) in [(0i32, &chunk1), (1i32, &chunk2)] {
        let values = [
            Datum::from_usize(valueid as usize),
            Datum::from_usize(seq as u32 as usize),
            Datum::from_usize(chunk.as_ptr() as usize),
        ];
        let change = tuple_change(&rb, &toast_desc, &values, &[false, false, false]);
        rb.queue_change(xid, 100 + seq as u64, change, true).unwrap();
        let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
        let cid = unlink_tail(&mut rb, txn);
        rb.toast_append_chunk_with_desc(txn, &toast_desc, cid).unwrap();
    }

    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    {
        let hash = rb.txn(txn).toast_hash.as_ref().unwrap();
        let ent = hash.get(&valueid).unwrap();
        assert_eq!(ent.num_chunks, 2);
        assert_eq!(ent.size, raw_len);
        assert_eq!(ent.last_chunk_seq, 1);
    }

    let pointer = ondisk_toast_pointer(valueid, raw_len as i32 + 4, raw_len as u32);
    let values = [Datum::from_usize(pointer.as_ptr() as usize)];
    let change = tuple_change(&rb, &main_desc, &values, &[false]);
    rb.queue_change(xid, 110, change, false).unwrap();
    let cid = rb.txn(txn).changes.tail;

    let size_before = rb.size;
    rb.toast_replace_with_descs(txn, &main_desc, &toast_desc, cid).unwrap();
    assert_ne!(rb.size, size_before);

    match &rb.change(cid).data {
        ReorderBufferChangeData::Tp { newtuple: Some(t), .. } => {
            let mut values = [Datum::from_usize(0)];
            let mut isnull = [true];
            types_tuple::heap_deform_tuple(t.as_tuple(), &main_desc, &mut values, &mut isnull);
            assert!(!isnull[0]);
            let img = unsafe { crate::toast::varlena_image(values[0].as_usize() as *const u8) };
            assert_eq!(img.len(), raw_len + 4);
            assert_eq!(&img[4..], b"hello toasted world");
        }
        _ => panic!("expected Tp data"),
    }

    rb.toast_reset(txn);
    assert!(rb.txn(txn).toast_hash.is_none());
    rb.cleanup_txn(txn).unwrap();
    assert_eq!(rb.size, 0);
}

#[test]
fn toast_chunk_sequence_gap_errors() {
    let mut rb = rb();
    let (_, toast_desc) = toast_descs();
    let chunk = inline_varlena(b"abc");
    let values = [
        Datum::from_usize(77usize),
        Datum::from_usize(1usize),
        Datum::from_usize(chunk.as_ptr() as usize),
    ];
    let change = tuple_change(&rb, &toast_desc, &values, &[false, false, false]);
    rb.queue_change(31, 10, change, true).unwrap();
    let txn = rb.txn_by_xid(31, false, 0, false).0.unwrap();
    let cid = unlink_tail(&mut rb, txn);
    let err = rb.toast_append_chunk_with_desc(txn, &toast_desc, cid).unwrap_err();
    assert!(err.message().contains("instead of seq 0"), "{err:?}");
}

#[test]
fn commit_of_unknown_or_snapshotless_txn_is_cheap() {
    let mut rb = rb();
    // Unknown xid: no-op.
    rb.commit(999, 100, 101, 0, 0, 0).unwrap();

    // Known but without a base snapshot: cleaned up without replay.
    rb.queue_change(5, 10, msg_change("x"), false).unwrap();
    let txn = rb.txn_by_xid(5, false, 0, false).0.unwrap();
    // No invalidations, no base snapshot -> ReorderBufferCleanupTXN path.
    rb.commit(5, 100, 101, 0, 0, 0).unwrap();
    let _ = txn;
    assert!(rb.txn_by_xid(5, false, 0, false).0.is_none());
    assert_eq!(rb.size, 0);
}

#[test]
fn startup_reorder_buffer_removes_spill_files() {
    let dir = std::env::temp_dir().join(format!("pgrust_rb_startup_{}", std::process::id()));
    let slot = dir.join("pg_replslot/myslot");
    std::fs::create_dir_all(&slot).unwrap();
    std::fs::write(slot.join("xid-5-lsn-0-1.spill"), b"x").unwrap();
    std::fs::write(slot.join("state"), b"s").unwrap();
    let bad = dir.join("pg_replslot/Not-A-Slot");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("xid-9-lsn-0-1.spill"), b"x").unwrap();

    let dir_str: &'static str = Box::leak(dir.to_string_lossy().into_owned().into_boxed_str());
    init_small::globals::SetDataDir(dir_str);

    StartupReorderBuffer().unwrap();

    assert!(!slot.join("xid-5-lsn-0-1.spill").exists());
    assert!(slot.join("state").exists());
    assert!(bad.join("xid-9-lsn-0-1.spill").exists());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn change_size_formula_matches_shapes() {
    let mut rb = rb();
    rb.add_snapshot(3, 10, snap(100)).unwrap();
    let txn = rb.txn_by_xid(3, false, 0, false).0.unwrap();
    let cid = rb.txn(txn).changes.head;
    assert_eq!(
        rb.change_size(cid),
        std::mem::size_of::<ReorderBufferChange>() + std::mem::size_of::<SnapshotData>()
    );

    rb.add_new_command_id(3, 11, 2).unwrap();
    let cid2 = rb.txn(txn).changes.tail;
    assert_eq!(rb.change_size(cid2), std::mem::size_of::<ReorderBufferChange>());

    // Tuplecid changes never count toward the memory limit.
    let before = rb.size;
    rb.add_new_tuple_cids(
        3,
        12,
        types_storage::RelFileLocator::new(1, 2, 3),
        types_tuple::ItemPointerData::new(0, 1),
        1,
        InvalidCommandId,
        0,
    );
    assert_eq!(rb.size, before);
}

#[test]
fn abort_and_forget_discard_transactions() {
    let mut rb = rb();
    rb.queue_change(8, 10, msg_change("z"), false).unwrap();
    rb.abort(8, 20, 12345).unwrap();
    assert!(rb.txn_by_xid(8, false, 0, false).0.is_none());
    assert_eq!(rb.size, 0);

    rb.queue_change(9, 30, msg_change("z"), false).unwrap();
    rb.forget(9, 40).unwrap();
    assert!(rb.txn_by_xid(9, false, 0, false).0.is_none());

    rb.queue_change(11, 50, msg_change("z"), false).unwrap();
    rb.queue_change(12, 60, msg_change("z"), false).unwrap();
    rb.abort_old(12).unwrap();
    assert!(rb.txn_by_xid(11, false, 0, false).0.is_none());
    assert!(rb.txn_by_xid(12, false, 0, false).0.is_some());
}

#[test]
fn queued_change_to_aborted_txn_is_dropped() {
    let mut rb = rb();
    rb.process_xid(13, 10);
    let txn = rb.txn_by_xid(13, false, 0, false).0.unwrap();
    rb.txn_mut(txn).txn_flags |= RBTXN_IS_ABORTED;
    rb.queue_change(13, 11, msg_change("dropped"), false).unwrap();
    assert_eq!(rb.txn(txn).nentries, 0);
    assert_eq!(rb.size, 0);
}

// --- two-phase (prepared transaction) machinery ---

thread_local! {
    static TWOPC_EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn twopc_events() -> Vec<String> {
    TWOPC_EVENTS.with(|e| e.borrow().clone())
}

fn rec_prepare_cb(rb: &mut ReorderBuffer, txn: TxnId, lsn: XLogRecPtr) -> types_error::PgResult<()> {
    let gid = rb.txn(txn).gid.clone().unwrap_or_default();
    TWOPC_EVENTS.with(|e| e.borrow_mut().push(format!("prepare:{gid}:{lsn}")));
    Ok(())
}

fn rec_commit_prepared_cb(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    lsn: XLogRecPtr,
) -> types_error::PgResult<()> {
    let gid = rb.txn(txn).gid.clone().unwrap_or_default();
    TWOPC_EVENTS.with(|e| e.borrow_mut().push(format!("commit_prepared:{gid}:{lsn}")));
    Ok(())
}

fn rec_rollback_prepared_cb(
    rb: &mut ReorderBuffer,
    txn: TxnId,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: types_core::TimestampTz,
) -> types_error::PgResult<()> {
    let gid = rb.txn(txn).gid.clone().unwrap_or_default();
    TWOPC_EVENTS.with(|e| {
        e.borrow_mut()
            .push(format!("rollback_prepared:{gid}:{prepare_end_lsn}:{prepare_time}"))
    });
    Ok(())
}

#[test]
fn remember_prepare_info_marks_prepared() {
    let mut rb = rb();
    assert!(!rb.remember_prepare_info(21, 100, 110, 777, 3, 55)); // unknown xid

    rb.process_xid(21, 90);
    assert!(rb.remember_prepare_info(21, 100, 110, 777, 3, 55));
    let txn = rb.txn_by_xid(21, false, 0, false).0.unwrap();
    let t = rb.txn(txn);
    assert_eq!(t.final_lsn, 100);
    assert_eq!(t.end_lsn, 110);
    assert_eq!(t.xact_time, 777);
    assert_eq!(t.origin_id, 3);
    assert_eq!(t.origin_lsn, 55);
    assert!(t.is_prepared());
    assert!(!t.sent_prepare());
    assert_eq!(t.txn_flags & RBTXN_PREPARE_STATUS_MASK, RBTXN_IS_PREPARED);
}

#[test]
fn skip_prepare_marks_skipped() {
    let mut rb = rb();
    rb.process_xid(22, 90);
    assert!(rb.remember_prepare_info(22, 100, 110, 777, 0, 0));
    rb.skip_prepare(22);
    let txn = rb.txn_by_xid(22, false, 0, false).0.unwrap();
    assert_eq!(
        rb.txn(txn).txn_flags & RBTXN_PREPARE_STATUS_MASK,
        RBTXN_IS_PREPARED | RBTXN_SKIPPED_PREPARE
    );
}

#[test]
fn prepare_sends_prepare_callback_for_changeless_txn() {
    let mut rb = rb();
    rb.callbacks.prepare = rec_prepare_cb;
    TWOPC_EVENTS.with(|e| e.borrow_mut().clear());

    rb.process_xid(23, 90);
    assert!(rb.remember_prepare_info(23, 100, 110, 777, 0, 0));
    rb.prepare(23, "gid_23").unwrap();

    // A txn with no base snapshot skips ProcessTXN; ReorderBufferPrepare's
    // trailing arm must still send the prepare with the prepare-record LSN.
    assert_eq!(twopc_events(), ["prepare:gid_23:100".to_string()]);

    // The prepared txn stays alive until COMMIT/ROLLBACK PREPARED.
    let txn = rb.txn_by_xid(23, false, 0, false).0.unwrap();
    assert!(rb.txn(txn).sent_prepare());
}

#[test]
fn finish_prepared_replays_skipped_txn_then_sends_commit_prepared() {
    let mut rb = rb();
    rb.callbacks.prepare = rec_prepare_cb;
    rb.callbacks.commit_prepared = rec_commit_prepared_cb;
    TWOPC_EVENTS.with(|e| e.borrow_mut().clear());

    // Decoded-before-two_phase_at shape: prepare at LSN 100 was skipped,
    // two_phase_at is 200, COMMIT PREPARED arrives at LSN 500.
    rb.process_xid(24, 90);
    assert!(rb.remember_prepare_info(24, 100, 110, 777, 0, 0));
    rb.skip_prepare(24);
    rb.finish_prepared(24, 500, 510, 200, 888, 0, 0, "gid_24", true)
        .unwrap();

    // final_lsn (100) < two_phase_at (200) and is_commit: the replay arm runs
    // first. With no base snapshot there is nothing to decode, so replay
    // returns without sending prepare (C's ReorderBufferReplay early-return;
    // the trailing send-prepare arm lives only in ReorderBufferPrepare) and
    // only commit_prepared goes out, with the commit record's LSN. A txn
    // with changes replays begin_prepare/changes/prepare here first.
    assert_eq!(twopc_events(), ["commit_prepared:gid_24:500".to_string()]);

    // The txn is fully cleaned up afterwards.
    assert!(rb.txn_by_xid(24, false, 0, false).0.is_none());
}

#[test]
fn finish_prepared_already_sent_goes_straight_to_commit_prepared() {
    let mut rb = rb();
    rb.callbacks.prepare = rec_prepare_cb;
    rb.callbacks.commit_prepared = rec_commit_prepared_cb;
    TWOPC_EVENTS.with(|e| e.borrow_mut().clear());

    // Decoded-at-prepare-time shape: prepare at LSN 300 >= two_phase_at 200.
    rb.process_xid(25, 290);
    assert!(rb.remember_prepare_info(25, 300, 310, 777, 0, 0));
    rb.prepare(25, "gid_25").unwrap();
    rb.finish_prepared(25, 500, 510, 200, 888, 0, 0, "gid_25", true)
        .unwrap();

    assert_eq!(
        twopc_events(),
        [
            "prepare:gid_25:300".to_string(),
            "commit_prepared:gid_25:500".to_string()
        ]
    );
    assert!(rb.txn_by_xid(25, false, 0, false).0.is_none());
}

#[test]
fn finish_prepared_rollback_uses_prepare_record_info() {
    let mut rb = rb();
    rb.callbacks.prepare = rec_prepare_cb;
    rb.callbacks.rollback_prepared = rec_rollback_prepared_cb;
    TWOPC_EVENTS.with(|e| e.borrow_mut().clear());

    rb.process_xid(26, 290);
    assert!(rb.remember_prepare_info(26, 300, 310, 777, 0, 0));
    rb.prepare(26, "gid_26").unwrap();
    rb.finish_prepared(26, 600, 610, 200, 999, 0, 0, "gid_26", false)
        .unwrap();

    // rollback_prepared carries the PREPARE record's end LSN and time.
    assert_eq!(
        twopc_events(),
        [
            "prepare:gid_26:300".to_string(),
            "rollback_prepared:gid_26:310:777".to_string()
        ]
    );
    assert!(rb.txn_by_xid(26, false, 0, false).0.is_none());
}

#[test]
fn finish_prepared_unknown_xid_is_noop() {
    let mut rb = rb();
    rb.finish_prepared(27, 500, 510, 200, 888, 0, 0, "gid_27", true)
        .unwrap();
}

// --- CheckXidAlive (concurrent-abort detection during prepared decode) ---

thread_local! {
    static DID_COMMIT_ANSWER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Seams are set-once per process; every test that needs the stub funnels
// through this. The stub answers from a thread-local so parallel tests
// cannot see each other's value.
fn install_did_commit_stub() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        transam_seams::transaction_id_did_commit::set(|_| {
            Ok(DID_COMMIT_ANSWER.with(|c| c.get()))
        });
    });
}

#[test]
fn setup_check_xid_live_matches_c_arms() {
    install_did_commit_stub();
    xact::SetCheckXidAlive(types_core::InvalidTransactionId);

    // Uncommitted xid: published for the catalog-scan guards.
    DID_COMMIT_ANSWER.with(|c| c.set(false));
    crate::replay::setup_check_xid_live(501).unwrap();
    assert_eq!(xact::CheckXidAlive(), 501);

    // Same xid again: the TransactionIdEquals early-return arm — no re-probe
    // (the commit status is not consulted; flipping it must not matter).
    DID_COMMIT_ANSWER.with(|c| c.set(true));
    crate::replay::setup_check_xid_live(501).unwrap();
    assert_eq!(xact::CheckXidAlive(), 501);

    // A different, already-committed xid resets to invalid.
    crate::replay::setup_check_xid_live(502).unwrap();
    assert_eq!(xact::CheckXidAlive(), types_core::InvalidTransactionId);

    xact::SetCheckXidAlive(types_core::InvalidTransactionId);
}

#[test]
fn check_xid_guard_clears_state_on_panic_unwind() {
    xact::SetCheckXidAlive(777);
    xact::SetBsysscan(true);

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = crate::replay::CheckXidLiveGuard;
        panic!("simulated replay panic");
    }));
    assert!(unwound.is_err());

    // The thread survives a caught panic in the threaded server; a leaked
    // CheckXidAlive would poison every later catalog scan on this thread.
    assert_eq!(xact::CheckXidAlive(), types_core::InvalidTransactionId);
    assert!(!xact::bsysscan());
}

#[test]
fn check_xid_guard_is_idempotent_when_clean() {
    xact::SetCheckXidAlive(types_core::InvalidTransactionId);
    xact::SetBsysscan(false);
    {
        let _guard = crate::replay::CheckXidLiveGuard;
    }
    assert_eq!(xact::CheckXidAlive(), types_core::InvalidTransactionId);
    assert!(!xact::bsysscan());
}

// ---- spill-to-disk ---------------------------------------------------------

const TEST_SEG_SIZE: u64 = 16 * 1024 * 1024;

// Per-test spill environment: seams once per process, a private DataDir per
// test thread (DataDir is thread-local), and a slot dir for the files.
fn spill_rb(slot: &str) -> ReorderBuffer {
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
        transam_xlog_seams::wal_segment_size::set(|| TEST_SEG_SIZE as i32);
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        // Eviction candidates read as in-progress: no truncation, plain spill.
        procarray_seams::transaction_id_is_in_progress::set(|_| Ok(true));
    });
    let base = std::env::temp_dir().join(format!("pgrust-rb-spill-{}-{slot}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(base.join("pg_replslot").join(slot)).unwrap();
    init_small::globals::SetDataDir(base.to_str().unwrap());
    crate::startup::install_gucs();
    ReorderBuffer::allocate(slot).expect("allocate")
}

fn slot_dir(slot: &str) -> std::path::PathBuf {
    crate::startup::replslot_dir().unwrap().join(slot)
}

fn spill_files(slot: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(slot_dir(slot))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("xid"))
        .collect();
    names.sort();
    names
}

fn snap_change(xmin: TransactionId, xips: &[TransactionId]) -> ReorderBufferChange {
    let mut s = SnapshotData::sentinel(rb_mcx(), SnapshotType::SNAPSHOT_HISTORIC_MVCC);
    s.xmin = xmin;
    s.xmax = xmin + 10;
    let mut xip = PgVec::new_in(rb_mcx());
    xip.extend_from_slice(xips);
    s.xcnt = xip.len() as u32;
    s.xip = xip;
    s.suboverflowed = true;
    s.curcid.set(7);
    s.copied = true;
    ReorderBufferChange::new(InternalSnapshot, ReorderBufferChangeData::Snapshot(Rc::new(s)))
}

fn drain_lsns(rb: &mut ReorderBuffer, txn: TxnId) -> Vec<XLogRecPtr> {
    let mut state = None;
    rb.iter_txn_init(txn, &mut state).unwrap();
    let mut state = state.unwrap();
    let mut lsns = Vec::new();
    while let Some(cid) = rb.iter_txn_next(&mut state).unwrap() {
        lsns.push(rb.change(cid).lsn);
    }
    rb.iter_txn_finish(state);
    lsns
}

#[test]
fn spill_roundtrip_preserves_every_change_type() {
    let slot = "spill_roundtrip";
    let mut rb = spill_rb(slot);
    let xid: TransactionId = 60;
    let (main_desc, _) = toast_descs();

    // One change of every payload-carrying kind, ascending LSNs.
    rb.queue_change(xid, 100, msg_change("alpha"), false).unwrap();
    let payload = inline_varlena(b"spilled tuple payload");
    let values = [Datum::from_usize(payload.as_ptr() as usize)];
    rb.queue_change(xid, 110, tuple_change(&rb, &main_desc, &values, &[false]), false)
        .unwrap();
    rb.queue_change(
        xid,
        120,
        ReorderBufferChange::new(
            Invalidation,
            ReorderBufferChangeData::Inval {
                invalidations: {
                    let mut v = PgVec::new_in(rb_mcx());
                    v.extend_from_slice(&[inval_msg(11), inval_msg(22)]);
                    v
                },
            },
        ),
        false,
    )
    .unwrap();
    rb.queue_change(xid, 130, snap_change(500, &[501, 502, 503]), false).unwrap();
    rb.queue_change(
        xid,
        140,
        ReorderBufferChange::new(InternalCommandId, ReorderBufferChangeData::CommandId(42)),
        false,
    )
    .unwrap();
    rb.queue_change(
        xid,
        150,
        ReorderBufferChange::new(
            Truncate,
            ReorderBufferChangeData::Truncate {
                cascade: true,
                restart_seqs: false,
                relids: {
                    let mut v: PgVec<'static, Oid> = PgVec::new_in(rb_mcx());
                    v.extend_from_slice(&[16384, 16400]);
                    v
                },
            },
        ),
        false,
    )
    .unwrap();

    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    let size_before = rb.txn(txn).size;
    assert!(size_before > 0);

    rb.serialize_txn(txn).unwrap();

    assert!(rb.txn(txn).is_serialized());
    assert_eq!(rb.txn(txn).nentries_mem, 0);
    assert_eq!(rb.txn(txn).nentries, 6);
    assert_eq!(rb.txn(txn).size, 0);
    assert_eq!(rb.size, 0);
    assert_eq!(rb.spillTxns, 1);
    assert_eq!(rb.spillCount, 1);
    assert_eq!(rb.spillBytes, size_before as i64);
    // All LSNs below one segment: exactly one file, named for segment 0.
    assert_eq!(spill_files(slot), vec![format!("xid-{xid}-lsn-0-0.spill")]);

    // Restore through the iterator and deep-verify each payload.
    let mut state = None;
    rb.iter_txn_init(txn, &mut state).unwrap();
    let mut state = state.unwrap();
    let mut seen = Vec::new();
    while let Some(cid) = rb.iter_txn_next(&mut state).unwrap() {
        let change = rb.change(cid);
        seen.push((change.lsn, change.action));
        match (change.lsn, &change.data) {
            (100, ReorderBufferChangeData::Msg { prefix, message }) => {
                assert_eq!(prefix.as_str(), "test");
                assert_eq!(&message[..], b"alpha");
            }
            (110, ReorderBufferChangeData::Tp { rlocator, clear_toast_afterwards, oldtuple, newtuple }) => {
                assert_eq!(*rlocator, types_storage::RelFileLocator::new(1663, 5, 55555));
                assert!(*clear_toast_afterwards);
                assert!(oldtuple.is_none());
                let t = newtuple.as_ref().unwrap();
                let mut values = [Datum::from_usize(0)];
                let mut isnull = [true];
                types_tuple::heap_deform_tuple(t.as_tuple(), &main_desc, &mut values, &mut isnull);
                assert!(!isnull[0]);
                let img = unsafe { crate::toast::varlena_image(values[0].as_usize() as *const u8) };
                assert_eq!(&img[4..], b"spilled tuple payload");
            }
            (120, ReorderBufferChangeData::Inval { invalidations }) => {
                assert_eq!(&invalidations[..], &[inval_msg(11), inval_msg(22)]);
            }
            (130, ReorderBufferChangeData::Snapshot(s)) => {
                assert_eq!(s.snapshot_type, SnapshotType::SNAPSHOT_HISTORIC_MVCC);
                assert_eq!(s.xmin, 500);
                assert_eq!(s.xmax, 510);
                assert_eq!(s.xcnt, 3);
                assert_eq!(&s.xip[..3], &[501, 502, 503]);
                assert_eq!(s.subxcnt, 0);
                assert!(s.suboverflowed);
                assert_eq!(s.curcid.get(), 7);
                assert!(s.copied);
            }
            (140, ReorderBufferChangeData::CommandId(c)) => assert_eq!(*c, 42),
            (150, ReorderBufferChangeData::Truncate { cascade, restart_seqs, relids }) => {
                assert!(*cascade);
                assert!(!*restart_seqs);
                assert_eq!(&relids[..], &[16384, 16400]);
            }
            (lsn, _) => panic!("unexpected restored change at lsn {lsn}"),
        }
    }
    rb.iter_txn_finish(state);
    assert_eq!(
        seen.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
        vec![100, 110, 120, 130, 140, 150]
    );

    // Cleanup removes the on-disk files.
    rb.cleanup_txn(txn).unwrap();
    assert!(spill_files(slot).is_empty());
    assert_eq!(rb.size, 0);
}

#[test]
fn spill_eviction_fires_from_queue_change_and_reports_stats() {
    let slot = "spill_evict";
    let mut rb = spill_rb(slot);
    // 8 kB limit; each message is ~1 kB.
    (guc_tables::vars::logical_decoding_work_mem.get().set)(8);

    thread_local! {
        static STATS_FLUSHES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }
    fn bump(_rb: &mut ReorderBuffer) {
        STATS_FLUSHES.with(|c| c.set(c.get() + 1));
    }
    rb.update_stats = Some(bump);

    let body = "x".repeat(1024);
    for i in 0..64u64 {
        rb.queue_change(70, 1000 + i, msg_change(&body), false).unwrap();
    }

    let limit = 8 * 1024;
    assert!(rb.size < limit, "eviction must keep rb under the limit");
    assert_eq!(rb.spillTxns, 1);
    assert!(rb.spillCount >= 2, "several eviction passes expected");
    assert!(rb.spillBytes > 0);
    assert!(STATS_FLUSHES.with(|c| c.get()) >= 2);
    assert!(!spill_files(slot).is_empty());

    let txn = rb.txn_by_xid(70, false, 0, false).0.unwrap();
    assert!(rb.txn(txn).is_serialized());
    assert_eq!(rb.txn(txn).nentries, 64);
    // In-memory tail = the changes queued after the last eviction.
    assert_eq!(
        rb.txn(txn).nentries - rb.txn(txn).nentries_mem,
        64 - rb.txn(txn).nentries_mem
    );

    // A partially-serialized txn iterates spilled-then-live in LSN order.
    let lsns = drain_lsns(&mut rb, txn);
    assert_eq!(lsns, (0..64u64).map(|i| 1000 + i).collect::<Vec<_>>());

    rb.cleanup_txn(txn).unwrap();
    assert!(spill_files(slot).is_empty());
}

#[test]
fn spill_restore_runs_in_bounded_batches() {
    let slot = "spill_batches";
    let mut rb = spill_rb(slot);
    let xid: TransactionId = 80;
    let n: u64 = 5000; // crosses the 4096-change restore batch bound

    for i in 0..n {
        rb.queue_change(xid, 10_000 + i, msg_change(&format!("m{i}")), false).unwrap();
    }
    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    rb.serialize_txn(txn).unwrap();
    assert_eq!(rb.txn(txn).nentries, n);
    assert_eq!(rb.txn(txn).nentries_mem, 0);

    let mut state = None;
    rb.iter_txn_init(txn, &mut state).unwrap();
    let mut state = state.unwrap();
    let mut count = 0u64;
    let mut prev = 0;
    while let Some(cid) = rb.iter_txn_next(&mut state).unwrap() {
        let change = rb.change(cid);
        assert!(change.lsn > prev);
        prev = change.lsn;
        match &change.data {
            ReorderBufferChangeData::Msg { message, .. } => {
                assert_eq!(&message[..], format!("m{count}").as_bytes());
            }
            _ => panic!("expected Msg data"),
        }
        count += 1;
        // Never more than one restore batch in memory.
        assert!(rb.txn(txn).nentries_mem <= 4096);
    }
    rb.iter_txn_finish(state);
    assert_eq!(count, n);

    rb.cleanup_txn(txn).unwrap();
    assert!(spill_files(slot).is_empty());
    assert_eq!(rb.size, 0);
}

#[test]
fn spill_splits_files_per_wal_segment() {
    let slot = "spill_segs";
    let mut rb = spill_rb(slot);
    let xid: TransactionId = 90;

    rb.queue_change(xid, 100, msg_change("seg0"), false).unwrap();
    rb.queue_change(xid, TEST_SEG_SIZE + 200, msg_change("seg1"), false).unwrap();
    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    rb.serialize_txn(txn).unwrap();

    assert_eq!(
        spill_files(slot),
        vec![
            format!("xid-{xid}-lsn-0-0.spill"),
            format!("xid-{xid}-lsn-0-{:X}.spill", TEST_SEG_SIZE),
        ]
    );

    let lsns = drain_lsns(&mut rb, txn);
    assert_eq!(lsns, vec![100, TEST_SEG_SIZE + 200]);

    rb.cleanup_txn(txn).unwrap();
    assert!(spill_files(slot).is_empty());
}

#[test]
fn spill_iterates_subtxns_merged_by_lsn() {
    let slot = "spill_subtxn";
    let mut rb = spill_rb(slot);
    let (top_xid, sub_xid): (TransactionId, TransactionId) = (100, 101);

    rb.queue_change(top_xid, 10, msg_change("t10"), false).unwrap();
    rb.queue_change(sub_xid, 20, msg_change("s20"), false).unwrap();
    rb.queue_change(top_xid, 30, msg_change("t30"), false).unwrap();
    rb.queue_change(sub_xid, 40, msg_change("s40"), false).unwrap();
    rb.assign_child(top_xid, sub_xid, 20);

    let top = rb.txn_by_xid(top_xid, false, 0, false).0.unwrap();
    let sub = rb.txn_by_xid(sub_xid, false, 0, false).0.unwrap();

    // Serializing the toplevel recurses into the subtransaction; each spills
    // into its own xid file and counts separately in the spill stats.
    rb.serialize_txn(top).unwrap();
    assert!(rb.txn(top).is_serialized());
    assert!(rb.txn(sub).is_serialized());
    assert_eq!(rb.spillTxns, 2);
    assert_eq!(rb.spillCount, 2);
    assert_eq!(spill_files(slot).len(), 2);

    let lsns = drain_lsns(&mut rb, top);
    assert_eq!(lsns, vec![10, 20, 30, 40]);

    rb.cleanup_txn(top).unwrap();
    assert!(spill_files(slot).is_empty());
}

#[test]
fn spill_stats_count_a_txn_once_across_repeat_spills() {
    let slot = "spill_stats_once";
    let mut rb = spill_rb(slot);
    let xid: TransactionId = 110;

    rb.queue_change(xid, 100, msg_change("first"), false).unwrap();
    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    rb.serialize_txn(txn).unwrap();
    rb.queue_change(xid, 200, msg_change("second"), false).unwrap();
    rb.serialize_txn(txn).unwrap();

    assert_eq!(rb.spillTxns, 1, "same txn spilled twice counts once");
    assert_eq!(rb.spillCount, 2);

    let lsns = drain_lsns(&mut rb, txn);
    assert_eq!(lsns, vec![100, 200]);
    rb.cleanup_txn(txn).unwrap();
}

#[test]
fn spill_immediate_debug_mode_serializes_every_change() {
    let slot = "spill_immediate";
    let mut rb = spill_rb(slot);
    (guc_tables::vars::debug_logical_replication_streaming.get().set)(
        guc_tables::consts::DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE,
    );
    let xid: TransactionId = 120;

    rb.queue_change(xid, 100, msg_change("a"), false).unwrap();
    assert_eq!(rb.size, 0, "immediate mode evicts on every queue");
    rb.queue_change(xid, 110, msg_change("b"), false).unwrap();
    assert_eq!(rb.size, 0);

    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    assert!(rb.txn(txn).is_serialized());
    assert_eq!(rb.txn(txn).nentries, 2);
    assert_eq!(rb.txn(txn).nentries_mem, 0);

    (guc_tables::vars::debug_logical_replication_streaming.get().set)(
        guc_tables::consts::DEBUG_LOGICAL_REP_STREAMING_BUFFERED,
    );

    let lsns = drain_lsns(&mut rb, txn);
    assert_eq!(lsns, vec![100, 110]);
    rb.cleanup_txn(txn).unwrap();
    assert!(spill_files(slot).is_empty());
}

#[test]
fn spill_toast_chunks_reassemble_after_restore() {
    let slot = "spill_toast";
    let mut rb = spill_rb(slot);
    let (main_desc, toast_desc) = toast_descs();
    let xid: TransactionId = 130;
    let valueid: u32 = 9100;

    let chunk1 = inline_varlena(b"spill ");
    let chunk2 = inline_varlena(b"survives toast");
    let raw_len = 6 + 14;
    for (seq, chunk) in [(0u64, &chunk1), (1u64, &chunk2)] {
        let values = [
            Datum::from_usize(valueid as usize),
            Datum::from_usize(seq as usize),
            Datum::from_usize(chunk.as_ptr() as usize),
        ];
        let change = tuple_change(&rb, &toast_desc, &values, &[false, false, false]);
        rb.queue_change(xid, 100 + seq, change, true).unwrap();
    }
    let pointer = ondisk_toast_pointer(valueid, raw_len as i32 + 4, raw_len as u32);
    let values = [Datum::from_usize(pointer.as_ptr() as usize)];
    rb.queue_change(xid, 110, tuple_change(&rb, &main_desc, &values, &[false]), false)
        .unwrap();

    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    rb.serialize_txn(txn).unwrap();
    assert_eq!(rb.txn(txn).nentries_mem, 0);

    // Replay-shaped consumption of the restored stream: chunk inserts are
    // extracted into the toast hash, the main tuple is then rewritten.
    let mut state = None;
    rb.iter_txn_init(txn, &mut state).unwrap();
    let mut state = state.unwrap();
    let mut main_cid = None;
    while let Some(cid) = rb.iter_txn_next(&mut state).unwrap() {
        if rb.change(cid).lsn < 110 {
            rb.iter_extract_change(&mut state, cid);
            rb.toast_append_chunk_with_desc(txn, &toast_desc, cid).unwrap();
        } else {
            main_cid = Some(cid);
            rb.toast_replace_with_descs(txn, &main_desc, &toast_desc, cid).unwrap();
        }
    }
    let main_cid = main_cid.unwrap();
    match &rb.change(main_cid).data {
        ReorderBufferChangeData::Tp { newtuple: Some(t), .. } => {
            let mut values = [Datum::from_usize(0)];
            let mut isnull = [true];
            types_tuple::heap_deform_tuple(t.as_tuple(), &main_desc, &mut values, &mut isnull);
            assert!(!isnull[0]);
            let img = unsafe { crate::toast::varlena_image(values[0].as_usize() as *const u8) };
            assert_eq!(&img[4..], b"spill survives toast");
        }
        _ => panic!("expected Tp data"),
    }
    rb.iter_txn_finish(state);
    rb.toast_reset(txn);
    rb.cleanup_txn(txn).unwrap();
    assert!(spill_files(slot).is_empty());
}

#[test]
fn spill_old_and_new_tuples_roundtrip_identity_fields() {
    let slot = "spill_tuples";
    let mut rb = spill_rb(slot);
    let (main_desc, _) = toast_descs();
    let xid: TransactionId = 140;

    let oldv = inline_varlena(b"old row");
    let newv = inline_varlena(b"new row");
    let old_tup = {
        let values = [Datum::from_usize(oldv.as_ptr() as usize)];
        let mut t = heaptuple::heap_form_tuple(rb.mcx, &main_desc, &values, &[false]).unwrap();
        t.t_self = types_tuple::ItemPointerData {
            ip_blkid: types_tuple::BlockIdData { bi_hi: 1, bi_lo: 2 },
            ip_posid: 3,
        };
        t.t_tableOid = 4242;
        t
    };
    let new_tup = {
        let values = [Datum::from_usize(newv.as_ptr() as usize)];
        let mut t = heaptuple::heap_form_tuple(rb.mcx, &main_desc, &values, &[false]).unwrap();
        t.t_self = types_tuple::ItemPointerData {
            ip_blkid: types_tuple::BlockIdData { bi_hi: 5, bi_lo: 6 },
            ip_posid: 7,
        };
        t.t_tableOid = 4242;
        t
    };
    let old_image: Vec<u8> = old_tup.image().to_vec();
    let new_image: Vec<u8> = new_tup.image().to_vec();

    let change = ReorderBufferChange::new(
        Update,
        ReorderBufferChangeData::Tp {
            rlocator: types_storage::RelFileLocator::new(1663, 5, 77777),
            clear_toast_afterwards: false,
            oldtuple: Some(old_tup),
            newtuple: Some(new_tup),
        },
    );
    rb.queue_change(xid, 100, change, false).unwrap();
    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();
    rb.serialize_txn(txn).unwrap();

    let mut state = None;
    rb.iter_txn_init(txn, &mut state).unwrap();
    let mut state = state.unwrap();
    let cid = rb.iter_txn_next(&mut state).unwrap().unwrap();
    match &rb.change(cid).data {
        ReorderBufferChangeData::Tp { clear_toast_afterwards, oldtuple, newtuple, .. } => {
            assert!(!*clear_toast_afterwards);
            let o = oldtuple.as_ref().unwrap();
            assert_eq!(o.image(), &old_image[..], "old tuple image byte-exact");
            assert_eq!(o.t_self.ip_blkid.bi_hi, 1);
            assert_eq!(o.t_self.ip_blkid.bi_lo, 2);
            assert_eq!(o.t_self.ip_posid, 3);
            assert_eq!(o.t_tableOid, 4242);
            let nt = newtuple.as_ref().unwrap();
            assert_eq!(nt.image(), &new_image[..], "new tuple image byte-exact");
            assert_eq!(nt.t_self.ip_posid, 7);
        }
        _ => panic!("expected Tp data"),
    }
    assert!(rb.iter_txn_next(&mut state).unwrap().is_none());
    rb.iter_txn_finish(state);
    rb.cleanup_txn(txn).unwrap();
}

#[test]
fn spill_tuplecid_and_speculative_forms_roundtrip() {
    let slot = "spill_misc_forms";
    let mut rb = spill_rb(slot);
    let xid: TransactionId = 150;

    // Spec-confirm keeps its locator and clear-toast flag through disk (C
    // memcpy's the base struct; the port encodes the Tp payload for it too).
    let change = ReorderBufferChange::new(
        InternalSpecConfirm,
        ReorderBufferChangeData::Tp {
            rlocator: types_storage::RelFileLocator::new(1663, 5, 88888),
            clear_toast_afterwards: true,
            oldtuple: None,
            newtuple: None,
        },
    );
    rb.queue_change(xid, 100, change, false).unwrap();
    let txn = rb.txn_by_xid(xid, false, 0, false).0.unwrap();

    // TupleCid changes live on the separate tuplecids list and are never
    // spilled with the change stream; queue one to prove spill leaves it be.
    rb.add_new_tuple_cids(
        xid,
        105,
        types_storage::RelFileLocator::new(1663, 5, 88888),
        types_tuple::ItemPointerData::default(),
        1,
        2,
        3,
    );

    rb.serialize_txn(txn).unwrap();
    assert_eq!(rb.txn(txn).ntuplecids, 1);
    assert!(!rb.txn(txn).tuplecids.is_empty());

    let mut state = None;
    rb.iter_txn_init(txn, &mut state).unwrap();
    let mut state = state.unwrap();
    let cid = rb.iter_txn_next(&mut state).unwrap().unwrap();
    assert_eq!(rb.change(cid).action, InternalSpecConfirm);
    match &rb.change(cid).data {
        ReorderBufferChangeData::Tp { rlocator, clear_toast_afterwards, oldtuple, newtuple } => {
            assert_eq!(rlocator.relNumber, 88888);
            assert!(*clear_toast_afterwards);
            assert!(oldtuple.is_none() && newtuple.is_none());
        }
        _ => panic!("expected Tp data"),
    }
    assert!(rb.iter_txn_next(&mut state).unwrap().is_none());
    rb.iter_txn_finish(state);
    rb.cleanup_txn(txn).unwrap();
}

// ---- streaming eviction selection -------------------------------------------

#[test]
fn largest_streamable_top_txn_selection_rules() {
    let mut rb = rb();

    // txn 200: streamable, base snapshot, two changes.
    rb.queue_change(200, 100, msg_change(&"x".repeat(500)), false).unwrap();
    rb.queue_change(200, 110, msg_change("small"), false).unwrap();
    rb.set_base_snapshot(200, 90, snap(200));
    // txn 201: bigger but no base snapshot -> not a candidate.
    rb.queue_change(201, 120, msg_change(&"y".repeat(5000)), false).unwrap();
    // txn 202: base snapshot but only non-streamable changes.
    rb.add_new_command_id(202, 130, 7).unwrap();
    rb.set_base_snapshot(202, 125, snap(202));

    let t200 = rb.txn_by_xid(200, false, 0, false).0.unwrap();
    let picked = rb.largest_streamable_top_txn();
    assert_eq!(picked, Some(t200), "only the snapshot-bearing streamable txn qualifies");

    // A partial change disqualifies the toplevel.
    rb.txn_mut(t200).txn_flags |= RBTXN_HAS_PARTIAL_CHANGE;
    assert_eq!(rb.largest_streamable_top_txn(), None);
    rb.txn_mut(t200).txn_flags &= !RBTXN_HAS_PARTIAL_CHANGE;

    // An aborted txn is skipped.
    rb.txn_mut(t200).txn_flags |= RBTXN_IS_ABORTED;
    assert_eq!(rb.largest_streamable_top_txn(), None);
}

#[test]
fn can_start_streaming_needs_callbacks_and_ready_flag() {
    let mut rb = rb();
    assert!(!rb.can_stream());
    rb.streaming_ready = true;
    assert!(!rb.can_start_streaming(), "no stream callbacks installed");

    fn noop_lsn_cb(
        _: &mut ReorderBuffer,
        _: TxnId,
        _: XLogRecPtr,
    ) -> types_error::PgResult<()> {
        Ok(())
    }
    rb.callbacks.stream_start = Some(noop_lsn_cb);
    assert!(rb.can_stream());
    assert!(rb.can_start_streaming());
    rb.streaming_ready = false;
    assert!(!rb.can_start_streaming(), "decode loop has not reached a consistent point");
}

#[test]
fn save_txn_snapshot_copies_uncopied_snapshots() {
    let mut rb = rb();
    rb.queue_change(210, 100, msg_change("x"), false).unwrap();
    let txn = rb.txn_by_xid(210, false, 0, false).0.unwrap();

    // Uncopied (shared snapbuild-style) snapshot: a private copy is stored.
    let shared = snap(210);
    assert!(!shared.copied);
    rb.save_txn_snapshot(txn, &shared, 3);
    {
        let stored = rb.txn(txn).snapshot_now.as_ref().unwrap();
        assert!(stored.copied, "shared snapshots are copied before storing");
        assert_eq!(stored.curcid.get(), 3);
        assert_eq!(rb.txn(txn).command_id, 3);
    }

    // Already-copied snapshot: stored as-is (same Rc identity).
    let copied = rb.copy_snap(&snap(211), txn, 5);
    rb.save_txn_snapshot(txn, &copied, 5);
    let stored = rb.txn(txn).snapshot_now.as_ref().unwrap();
    assert!(Rc::ptr_eq(stored, &copied), "copied snapshots are not re-copied");
}
