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

    rb.cleanup_txn(txn);
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

    rb.cleanup_txn(top);
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

    rb.truncate_txn(top, false);
    assert_eq!(rb.txn(top).nentries, 0);
    assert_eq!(rb.txn(top).nentries_mem, 0);
    assert_eq!(rb.txn(top).size, 0);
    assert_eq!(rb.size, 0);
    assert!(rb.txn_by_xid(1, false, 0, false).0.is_some());
    rb.cleanup_txn(top);
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
    rb.cleanup_txn(txn);
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
