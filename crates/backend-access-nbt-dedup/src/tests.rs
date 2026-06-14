//! Unit tests for the posting-list / dedup-state byte half of `nbtdedup.c`.
//!
//! These exercise the in-crate page-format logic (`_bt_form_posting`,
//! `_bt_update_posting`, `_bt_swap_posting`, `_bt_posting_valid`) end-to-end on
//! owned tuple bytes — no buffer cache, WAL, or relcache, so they need no seam
//! providers installed.

use super::*;
use backend_storage_page::{ItemPointerSetBlockNumber, ItemPointerSetOffsetNumber};
use mcx::MemoryContext;

/// Build a minimal plain non-pivot index tuple of the given MAXALIGN'd key size
/// whose single heap TID is `tid`.
fn plain_tuple(keysize: usize, tid: ItemPointerData) -> std::vec::Vec<u8> {
    let keysize = maxalign(keysize);
    let mut bytes = std::vec![0u8; keysize];
    let hdr = IndexTupleData {
        t_tid: tid,
        t_info: keysize as u16, // INDEX_SIZE_MASK portion, no flags
    };
    write_index_tuple_header(&mut bytes, &hdr);
    bytes
}

fn tid(blk: u32, off: u16) -> ItemPointerData {
    let mut t = ItemPointerData::default();
    ItemPointerSetBlockNumber(&mut t, blk);
    ItemPointerSetOffsetNumber(&mut t, off);
    t
}

#[test]
fn form_posting_single_htid_is_plain() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    let htids = [tid(7, 3)];
    let out = _bt_form_posting(mcx, &base, &htids, 1).unwrap();
    let hdr = index_tuple_header(&out);
    assert!(!BTreeTupleIsPosting(&hdr));
    assert_eq!(IndexTupleSize(&hdr), 16);
    assert_eq!(hdr.t_tid, tid(7, 3));
}

#[test]
fn form_posting_multi_htid_roundtrips() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    let htids = [tid(1, 1), tid(1, 2), tid(2, 5)];
    let out = _bt_form_posting(mcx, &base, &htids, 3).unwrap();
    let hdr = index_tuple_header(&out);
    assert!(BTreeTupleIsPosting(&hdr));
    assert_eq!(BTreeTupleGetNPosting(&hdr) as usize, 3);
    assert_eq!(BTreeTupleGetPostingOffset(&hdr) as usize, 16);
    for (i, expect) in htids.iter().enumerate() {
        assert_eq!(&posting_list_n(&out, i), expect);
    }
    assert!(_bt_posting_valid(&out));
    // First/last heap TID accessors match the posting list extremes.
    assert_eq!(heap_tid(&out).unwrap(), htids[0]);
    assert_eq!(max_heap_tid(&out), htids[2]);
}

#[test]
fn posting_valid_rejects_out_of_order() {
    // Build a posting tuple with descending TIDs directly (bypassing
    // _bt_form_posting, which asserts validity just like C does).
    let keysize = 16usize;
    let nhtids = 2usize;
    let newsize = maxalign(keysize + nhtids * SIZEOF_IPD);
    let mut bytes = std::vec![0u8; newsize];
    {
        let mut hdr = index_tuple_header(&bytes);
        hdr.t_info = newsize as u16;
        BTreeTupleSetPosting(&mut hdr, nhtids as u16, keysize as i32);
        write_index_tuple_header(&mut bytes, &hdr);
    }
    // Descending TIDs are invalid.
    write_posting(&mut bytes, keysize, &[tid(2, 1), tid(1, 1)]);
    assert!(!_bt_posting_valid(&bytes));
}

#[test]
fn update_posting_drops_tids() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    let htids = [tid(1, 1), tid(1, 2), tid(1, 3), tid(2, 1)];
    let orig = _bt_form_posting(mcx, &base, &htids, 4).unwrap();
    let updated = _bt_update_posting(mcx, &orig, &[1, 2]).unwrap(); // drop two middle TIDs
    let hdr = index_tuple_header(&updated);
    assert!(BTreeTupleIsPosting(&hdr));
    assert_eq!(BTreeTupleGetNPosting(&hdr) as usize, 2);
    assert_eq!(posting_list_n(&updated, 0), tid(1, 1));
    assert_eq!(posting_list_n(&updated, 1), tid(2, 1));
}

#[test]
fn update_posting_to_single() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    let htids = [tid(1, 1), tid(1, 2)];
    let orig = _bt_form_posting(mcx, &base, &htids, 2).unwrap();
    let updated = _bt_update_posting(mcx, &orig, &[0]).unwrap();
    let hdr = index_tuple_header(&updated);
    assert!(!BTreeTupleIsPosting(&hdr));
    assert_eq!(hdr.t_tid, tid(1, 2));
}

#[test]
fn swap_posting_swaps_max_tid() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    // oposting heap TIDs in ascending order.
    let htids = [tid(1, 1), tid(1, 5), tid(1, 9)];
    let oposting = _bt_form_posting(mcx, &base, &htids, 3).unwrap();
    // newitem's TID goes between (1,1) and (1,5) at postingoff=1: use (1,3).
    // After the swap, nposting = [(1,1),(1,3),(1,5)] and newitem gets old max (1,9).
    let mut newitem = plain_tuple(16, tid(1, 3));
    let nposting = _bt_swap_posting(mcx, &mut newitem, &oposting, 1).unwrap();
    assert_eq!(index_tuple_header(&newitem).t_tid, tid(1, 9));
    assert!(_bt_posting_valid(&nposting));
    assert_eq!(posting_list_n(&nposting, 0), tid(1, 1));
    assert_eq!(posting_list_n(&nposting, 1), tid(1, 3));
    assert_eq!(posting_list_n(&nposting, 2), tid(1, 5));
    assert_eq!(BTreeTupleGetNPosting(&index_tuple_header(&nposting)) as usize, 3);
}

#[test]
fn swap_posting_out_of_range_errors() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    let htids = [tid(1, 1), tid(1, 5), tid(1, 9)];
    let oposting = _bt_form_posting(mcx, &base, &htids, 3).unwrap();
    let mut newitem = plain_tuple(16, tid(1, 3));
    // postingoff must satisfy 0 < off < nhtids(=3); off=3 is out of range.
    assert!(_bt_swap_posting(mcx, &mut newitem, &oposting, 3).is_err());
    // off=0 is also rejected.
    assert!(_bt_swap_posting(mcx, &mut newitem, &oposting, 0).is_err());
}

#[test]
fn dedup_state_init_has_full_interval_array() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let state = new_dedup_state(mcx, BTMaxItemSize / 2).unwrap();
    assert!(state.deduplicate);
    assert_eq!(state.nintervals, 0);
    assert_eq!(state.nitems, 0);
    assert_eq!(state.baseoff, 0); // InvalidOffsetNumber
    // intervals is preallocated to MaxIndexTuplesPerPage (matches the C palloc).
    assert_eq!(state.intervals.len(), MaxIndexTuplesPerPage);
}

#[test]
fn dedup_start_then_save_merges_tids() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let mut state = new_dedup_state(mcx, BTMaxItemSize / 2).unwrap();
    let base = plain_tuple(16, tid(1, 1));
    let other = plain_tuple(16, tid(1, 2));

    _bt_dedup_start_pending(&mut state, &base, P_HIKEY).unwrap();
    assert_eq!(state.nitems, 1);
    assert_eq!(state.nhtids(), 1);
    assert_eq!(state.baseoff, P_HIKEY);
    assert_eq!(state.basetupsize, 16);

    let merged = _bt_dedup_save_htid(&mut state, &other).unwrap();
    assert!(merged);
    assert_eq!(state.nitems, 2);
    assert_eq!(state.nhtids(), 2);
    assert_eq!(state.htids[0], tid(1, 1));
    assert_eq!(state.htids[1], tid(1, 2));
}

#[test]
fn dedup_save_respects_maxpostingsize() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    // A tiny maxpostingsize forces _bt_dedup_save_htid to decline the merge.
    let mut state = new_dedup_state(mcx, 8).unwrap();
    let base = plain_tuple(16, tid(1, 1));
    let other = plain_tuple(16, tid(1, 2));

    _bt_dedup_start_pending(&mut state, &base, P_HIKEY).unwrap();
    // basetupsize(16) + 2*6 TIDs, MAXALIGN'd, far exceeds maxpostingsize=8.
    let merged = _bt_dedup_save_htid(&mut state, &other).unwrap();
    assert!(!merged);
    assert_eq!(state.nitems, 1);
    assert_eq!(state.nhtids(), 1);
}

#[test]
fn dedup_start_from_posting_copies_all_tids() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let base = plain_tuple(16, tid(1, 1));
    let htids = [tid(1, 1), tid(1, 2), tid(1, 3)];
    let posting = _bt_form_posting(mcx, &base, &htids, 3).unwrap();

    let mut state = new_dedup_state(mcx, BTMaxItemSize / 2).unwrap();
    _bt_dedup_start_pending(&mut state, &posting, P_HIKEY).unwrap();
    assert_eq!(state.nhtids(), 3);
    for (i, expect) in htids.iter().enumerate() {
        assert_eq!(&state.htids[i], expect);
    }
    assert_eq!(state.basetupsize, 16); // posting offset, excludes the list
}
