//! Unit tests for the grounded page-format / build core of `nbtsort.c`:
//! the on-page tuple/opaque byte codecs, the leaf page layout produced by
//! `_bt_blnewpage`, the slide-left rightmost rearrange, the minus-infinity
//! first-data-item truncation on internal pages, and the posting-list builder.
//!
//! These exercise the in-crate byte codecs against the safe
//! `backend-storage-page` API. `_bt_blnewpage` is driven through the
//! `smgr_bulk_get_buf` seam (installed here to hand back a zeroed `BLCKSZ`
//! page), exactly as the build does. No buffer cache / WAL / relcache.

use super::*;

use ::mcx::{Mcx, MemoryContext, PgVec};
use std::sync::Once;

use ::types_core::primitive::{Oid, INVALID_PROC_NUMBER};
use ::rel::{Relation, RelationData};
use ::types_storage::RelFileLocator;

const IDX_OID: Oid = 16384;

/// Install the `smgr_bulk_get_buf` seam (test-only) to hand back a fresh zeroed
/// `BLCKSZ` page in the call's context, mirroring the bulk writer's behaviour.
fn install_test_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        bulk::smgr_bulk_get_buf::set(|mcx, _bulkstate| {
            let mut v: PgVec<'_, u8> = vec_with_capacity_in(mcx, BLCKSZ)?;
            v.resize(BLCKSZ, 0);
            Ok(v)
        });
    });
}

/// Build a minimal in-memory index relation (no cache cell, no close authority).
fn make_index<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    use ::rel::FormData_pg_class;
    use ::types_tuple::heaptuple::TupleDescData;
    use ::mcx::PgString;

    let td = TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    };
    let rd_rel = FormData_pg_class {
        relname: PgString::from_str_in("idx", mcx).unwrap(),
        relnamespace: 0,
        relowner: 0,
        relrowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        reltoastrelid: 0,
        reltablespace: 0,
        relfilenode: 0,
        relisshared: false,
        relhasindex: false,
        relhassubclass: false,
        relpersistence: b'p',
        relkind: b'i',
        reltype: 0,
        relam: 0,
        relispopulated: true,
        relreplident: b'n',
        relispartition: false,
        relfrozenxid: 0,
        relminmxid: 0,
    };
    let data = RelationData {
        rd_id: IDX_OID,
        rd_locator: RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: INVALID_PROC_NUMBER,
        rd_rel,
        rd_att: ::mcx::alloc_in(mcx, td).unwrap(),
        rd_options: None,
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_trigdesc: None,
        pgstat_enabled: false,
    };
    Relation::open(data, None)
}

/// A throwaway `BTWriteState` whose bulk state is a no-op token; the test bulk
/// seam ignores it.
fn make_wstate<'mcx>(mcx: Mcx<'mcx>) -> BTWriteState<'mcx> {
    let index = make_index(mcx);
    let heap = make_index(mcx);
    BTWriteState {
        heap,
        index,
        keysz: 1,
        natts: 1,
        inskey: None,
        bulkstate: Some(BulkWriteState::new(mcx, ()).unwrap()),
        btws_pages_alloced: 0,
    }
}

/// Build a plain non-pivot leaf index tuple carrying a single 4-byte key and a
/// heap TID, mirroring the on-disk `IndexTuple` layout (length MAXALIGN'd, as
/// `index_form_tuple` produces, so it is a valid `_bt_form_posting` base).
fn make_itup(key: i32, blk: BlockNumber, off: OffsetNumber) -> std::vec::Vec<u8> {
    let sz = maxalign(SIZE_OF_INDEX_TUPLE_DATA + 4);
    let mut buf = std::vec![0u8; sz];
    {
        let mut hdr = index_tuple_header(&buf);
        hdr.t_info = sz as u16;
        hdr.t_tid = ItemPointerData::new(blk, off);
        write_tuple_header(&mut buf, &hdr);
    }
    buf[SIZE_OF_INDEX_TUPLE_DATA..SIZE_OF_INDEX_TUPLE_DATA + 4].copy_from_slice(&key.to_ne_bytes());
    buf
}

#[test]
fn item_pointer_byte_roundtrip() {
    for (blk, off) in [(0u32, 1u16), (0x0001_2345, 7), (0x00FF_FFFF, 0x7FFF)] {
        let ptr = ItemPointerData::new(blk, off);
        let mut b = [0u8; 6];
        write_item_pointer(&mut b, &ptr);
        let back = read_item_pointer(&b);
        assert_eq!(back, ptr);
        assert_eq!(ItemPointerGetBlockNumberNoCheck(&back), blk);
        assert_eq!(ItemPointerGetOffsetNumberNoCheck(&back), off);
    }
}

#[test]
fn form_posting_roundtrip_two_htids() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = make_itup(7, 1, 1);
    let htids = [ItemPointerData::new(1, 1), ItemPointerData::new(1, 5)];
    let posting = _bt_form_posting(mcx, &base, &htids, 2).unwrap();
    let hdr = index_tuple_header(&posting);
    assert!(BTreeTupleIsPosting(&hdr));
    assert_eq!(BTreeTupleGetNPosting(&hdr), 2);
    assert_eq!(posting_list_n(&posting, 0), htids[0]);
    assert_eq!(posting_list_n(&posting, 1), htids[1]);
}

#[test]
fn form_posting_single_htid_is_plain_tuple() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = make_itup(7, 1, 1);
    let htids = [ItemPointerData::new(3, 9)];
    let tup = _bt_form_posting(mcx, &base, &htids, 1).unwrap();
    let hdr = index_tuple_header(&tup);
    assert!(!BTreeTupleIsPosting(&hdr));
    assert!(!BTreeTupleIsPivot(&hdr));
    assert_eq!(hdr.t_tid, htids[0]);
}

#[test]
fn pageinit_then_opaque_roundtrip() {
    let mut buf = std::vec![0u8; BLCKSZ];
    _bt_pageinit(&mut buf, BLCKSZ).unwrap();
    {
        let mut page = PageMut::new(&mut buf).unwrap();
        let opaque = BTPageOpaqueData {
            btpo_prev: 11,
            btpo_next: 22,
            btpo_level: 3,
            btpo_flags: BTP_LEAF,
            btpo_cycleid: 0,
        };
        write_opaque(&mut page, &opaque);
    }
    let page = PageRef::new(&buf).unwrap();
    let got = BTPageGetOpaque(&page).unwrap();
    assert_eq!(got.btpo_prev, 11);
    assert_eq!(got.btpo_next, 22);
    assert_eq!(got.btpo_level, 3);
    assert_eq!(got.btpo_flags, BTP_LEAF);
}

#[test]
fn blnewpage_sets_leaf_opaque_and_reserves_hikey() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut wstate = make_wstate(mcx);

    let buf = _bt_blnewpage(mcx, &mut wstate, 0).unwrap();
    let page = PageRef::new(&buf).unwrap();
    let opaque = BTPageGetOpaque(&page).unwrap();
    assert_eq!(opaque.btpo_prev, P_NONE);
    assert_eq!(opaque.btpo_next, P_NONE);
    assert_eq!(opaque.btpo_level, 0);
    assert_eq!(opaque.btpo_flags, BTP_LEAF);
    // P_HIKEY line pointer made to appear allocated -> one slot reserved.
    assert_eq!(PageGetMaxOffsetNumber(&page), P_HIKEY);

    let inner = _bt_blnewpage(mcx, &mut wstate, 1).unwrap();
    let ipage = PageRef::new(&inner).unwrap();
    let iopaque = BTPageGetOpaque(&ipage).unwrap();
    assert_eq!(iopaque.btpo_level, 1);
    assert_eq!(iopaque.btpo_flags, 0); // internal page: no BTP_LEAF
}

#[test]
fn sortaddtup_then_slideleft_rightmost_layout() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut wstate = make_wstate(mcx);
    let mut buf = _bt_blnewpage(mcx, &mut wstate, 0).unwrap();

    let a = make_itup(10, 1, 1);
    let b = make_itup(20, 1, 2);
    {
        let mut page = PageMut::new(&mut buf).unwrap();
        _bt_sortaddtup(&mut page, a.len(), &a, P_FIRSTKEY, false).unwrap();
        _bt_sortaddtup(&mut page, b.len(), &b, OffsetNumberNext(P_FIRSTKEY), false).unwrap();
    }
    {
        let page = PageRef::new(&buf).unwrap();
        assert_eq!(PageGetMaxOffsetNumber(&page), OffsetNumberNext(P_FIRSTKEY));
    }

    _bt_slideleft(&mut buf).unwrap();
    let page = PageRef::new(&buf).unwrap();
    // After sliding, the first data item is at P_HIKEY and there are 2 items.
    assert_eq!(PageGetMaxOffsetNumber(&page), P_FIRSTKEY);
    let id1 = PageGetItemId(&page, P_HIKEY).unwrap();
    let it1 = PageGetItem(&page, &id1).unwrap();
    assert_eq!(&it1[..a.len()], &a[..]);
    let id2 = PageGetItemId(&page, P_FIRSTKEY).unwrap();
    let it2 = PageGetItem(&page, &id2).unwrap();
    assert_eq!(&it2[..b.len()], &b[..]);
}

#[test]
fn sortaddtup_newfirstdataitem_truncates_to_minus_infinity() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut wstate = make_wstate(mcx);
    let mut buf = _bt_blnewpage(mcx, &mut wstate, 1).unwrap();

    let itup = make_itup(42, 7, 3);
    {
        let mut page = PageMut::new(&mut buf).unwrap();
        _bt_sortaddtup(&mut page, itup.len(), &itup, P_FIRSTKEY, true).unwrap();
    }
    let page = PageRef::new(&buf).unwrap();
    let id = PageGetItemId(&page, P_FIRSTKEY).unwrap();
    let stored = PageGetItem(&page, &id).unwrap();
    // The stored item is exactly an IndexTupleData header (minus-infinity pivot).
    assert_eq!(stored.len(), SIZE_OF_INDEX_TUPLE_DATA);
    let hdr = index_tuple_header(stored);
    assert!(BTreeTupleIsPivot(&hdr));
}

#[test]
fn dedup_save_htid_respects_maxpostingsize() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut dstate = new_load_dedup_state(mcx).unwrap();
    // Tiny cap: only the base fits, the second tuple cannot be merged.
    dstate.maxpostingsize = 8;
    let base = make_itup(5, 1, 1);
    _bt_dedup_start_pending(mcx, &mut dstate, &base).unwrap();
    assert_eq!(dstate.nitems, 1);
    let next = make_itup(5, 1, 2);
    assert!(!_bt_dedup_save_htid(&mut dstate, &next));
    assert_eq!(dstate.nitems, 1);
}
