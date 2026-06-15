//! Seam-free unit tests for the pure page-byte helpers of `ginvacuum.c`.

use super::*;

/// `GinPageIsRecyclable` returns true for an all-zero (PageIsNew) page.
#[test]
fn recyclable_new_page() {
    let page = vec![0u8; types_core::primitive::BLCKSZ as usize];
    assert_eq!(GinPageIsRecyclable(&page).unwrap(), true);
}

/// `gin_page_set_delete_xid` / `gin_page_get_delete_xid` round-trip on the
/// `pd_prune_xid` field.
#[test]
fn delete_xid_roundtrip() {
    let mut page = vec![0u8; types_core::primitive::BLCKSZ as usize];
    assert_eq!(gin_page_get_delete_xid(&page), 0);
    gin_page_set_delete_xid(&mut page, 0x0102_0304);
    assert_eq!(gin_page_get_delete_xid(&page), 0x0102_0304);
}

/// `transaction_id_is_valid` matches `TransactionIdIsValid`.
#[test]
fn xid_validity() {
    assert!(!transaction_id_is_valid(0));
    assert!(transaction_id_is_valid(1));
}

/// `index_tuple_header` decodes the 8-byte IndexTupleData header.
#[test]
fn itup_header_decode() {
    // t_tid.ip_blkid = {bi_hi=0x0201, bi_lo=0x0403}, ip_posid=0x0605, t_info=0x0807.
    let bytes = [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let itup = index_tuple_header(&bytes);
    assert_eq!(itup.t_tid.ip_blkid.bi_hi, 0x0201);
    assert_eq!(itup.t_tid.ip_blkid.bi_lo, 0x0403);
    assert_eq!(itup.t_tid.ip_posid, 0x0605);
    assert_eq!(itup.t_info, 0x0807);
}
