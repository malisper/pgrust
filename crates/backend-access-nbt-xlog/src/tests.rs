//! Unit tests for the pure (non-seam) helpers of the nbtree WAL redo port:
//! the on-disk WAL-record decoders and the B-tree page-format helpers.

use super::*;

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
    assert_eq!(maxalign(SizeOfPageHeaderData), 24);
}

#[test]
fn offset_helpers() {
    assert_eq!(OffsetNumberNext(1), 2);
    assert_eq!(OffsetNumberPrev(2), 1);
    assert_eq!(P_HIKEY, 1);
    assert_eq!(P_FIRSTKEY, 2);
}

#[test]
fn decode_insert_reads_offnum() {
    let b = 0x1234u16.to_ne_bytes();
    assert_eq!(decode_insert(&b).offnum, 0x1234);
}

#[test]
fn decode_split_reads_all_fields() {
    let mut b = Vec::new();
    b.extend_from_slice(&7u32.to_ne_bytes()); // level
    b.extend_from_slice(&11u16.to_ne_bytes()); // firstrightoff
    b.extend_from_slice(&13u16.to_ne_bytes()); // newitemoff
    b.extend_from_slice(&5u16.to_ne_bytes()); // postingoff
    let s = decode_split(&b);
    assert_eq!(s.level, 7);
    assert_eq!(s.firstrightoff, 11);
    assert_eq!(s.newitemoff, 13);
    assert_eq!(s.postingoff, 5);
}

#[test]
fn decode_vacuum_and_delete() {
    let mut v = Vec::new();
    v.extend_from_slice(&3u16.to_ne_bytes()); // ndeleted
    v.extend_from_slice(&2u16.to_ne_bytes()); // nupdated
    let vac = decode_vacuum(&v);
    assert_eq!(vac.ndeleted, 3);
    assert_eq!(vac.nupdated, 2);

    let mut d = Vec::new();
    d.extend_from_slice(&0xDEADBEEFu32.to_ne_bytes()); // horizon
    d.extend_from_slice(&4u16.to_ne_bytes()); // ndeleted
    d.extend_from_slice(&1u16.to_ne_bytes()); // nupdated
    d.push(1); // isCatalogRel
    let del = decode_delete(&d);
    assert_eq!(del.snapshotConflictHorizon, 0xDEADBEEF);
    assert_eq!(del.ndeleted, 4);
    assert_eq!(del.nupdated, 1);
    assert!(del.isCatalogRel);
}

#[test]
fn decode_unlink_page_field_layout() {
    let mut b = vec![0u8; 40];
    b[0..4].copy_from_slice(&100u32.to_ne_bytes()); // leftsib
    b[4..8].copy_from_slice(&200u32.to_ne_bytes()); // rightsib
    b[8..12].copy_from_slice(&3u32.to_ne_bytes()); // level
    // [12..16] pad for 8-byte alignment of safexid
    b[16..24].copy_from_slice(&0x1122334455u64.to_ne_bytes()); // safexid
    b[24..28].copy_from_slice(&7u32.to_ne_bytes()); // leafleftsib
    b[28..32].copy_from_slice(&8u32.to_ne_bytes()); // leafrightsib
    b[32..36].copy_from_slice(&9u32.to_ne_bytes()); // leaftopparent
    let u = decode_unlink_page(&b);
    assert_eq!(u.leftsib, 100);
    assert_eq!(u.rightsib, 200);
    assert_eq!(u.level, 3);
    assert_eq!(u.safexid.value, 0x1122334455);
    assert_eq!(u.leafleftsib, 7);
    assert_eq!(u.leafrightsib, 8);
    assert_eq!(u.leaftopparent, 9);
}

#[test]
fn decode_reuse_page_field_layout() {
    let mut b = vec![0u8; 25];
    b[0..4].copy_from_slice(&1u32.to_ne_bytes()); // spcOid
    b[4..8].copy_from_slice(&2u32.to_ne_bytes()); // dbOid
    b[8..12].copy_from_slice(&3u32.to_ne_bytes()); // relNumber
    b[12..16].copy_from_slice(&42u32.to_ne_bytes()); // block
    b[16..24].copy_from_slice(&0x99u64.to_ne_bytes()); // horizon
    b[24] = 1; // isCatalogRel
    let r = decode_reuse_page(&b);
    assert_eq!(r.locator.spcOid, 1);
    assert_eq!(r.locator.dbOid, 2);
    assert_eq!(r.locator.relNumber, 3);
    assert_eq!(r.block, 42);
    assert_eq!(r.snapshotConflictHorizon.value, 0x99);
    assert!(r.isCatalogRel);
}

#[test]
fn decode_dedup_intervals_round_trip() {
    // Two intervals: {baseoff:5, nitems:3}, {baseoff:9, nitems:2}
    let mut b = Vec::new();
    b.extend_from_slice(&5u16.to_ne_bytes());
    b.extend_from_slice(&3u16.to_ne_bytes());
    b.extend_from_slice(&9u16.to_ne_bytes());
    b.extend_from_slice(&2u16.to_ne_bytes());
    let v = decode_dedup_intervals(&b, 2);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].baseoff, 5);
    assert_eq!(v[0].nitems, 3);
    assert_eq!(v[1].baseoff, 9);
    assert_eq!(v[1].nitems, 2);
}

#[test]
fn index_tuple_header_round_trips() {
    let mut hdr = IndexTupleData::default();
    hdr.t_tid.ip_blkid.bi_hi = 0x0102;
    hdr.t_tid.ip_blkid.bi_lo = 0x0304;
    hdr.t_tid.ip_posid = 0x0506;
    hdr.t_info = 0x0708;
    let mut bytes = vec![0u8; 8];
    write_index_tuple_header(&mut bytes, &hdr);
    let back = index_tuple_header(&bytes);
    assert_eq!(back.t_tid.ip_blkid.bi_hi, 0x0102);
    assert_eq!(back.t_tid.ip_blkid.bi_lo, 0x0304);
    assert_eq!(back.t_tid.ip_posid, 0x0506);
    assert_eq!(back.t_info, 0x0708);
}

#[test]
fn make_trunctuple_sets_top_parent_and_alt_tid() {
    let bytes = make_trunctuple(1234);
    let hdr = index_tuple_header(&bytes);
    // top parent stored in the block-number field
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&hdr.t_tid), 1234);
    // INDEX_ALT_TID_MASK set by BTreeTupleSetNAtts
    assert_ne!(hdr.t_info & INDEX_ALT_TID_MASK, 0);
    // 0 key attributes / not a heap-tid pivot => offset == 0
    assert_eq!(hdr.t_tid.ip_posid, 0);
}

#[test]
fn btree_tuple_downlink_set_get() {
    let mut hdr = IndexTupleData::default();
    BTreeTupleSetDownLink(&mut hdr, 0xABCD);
    assert_eq!(BTreeTupleGetDownLink(&hdr), 0xABCD);
}
