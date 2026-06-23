//! Unit tests for the pure ItemPointer / page-flag helpers of ginget.c.

use super::*;

fn ip(blk: BlockNumber, off: OffsetNumber) -> ItemPointerData {
    let mut p = ItemPointerData::default();
    ip_set(&mut p, blk, off);
    p
}

#[test]
fn item_pointer_round_trip() {
    let p = ip(0x1234_5678, 0x9abc);
    assert_eq!(ip_block(&p), 0x1234_5678);
    assert_eq!(ip_offset(&p), 0x9abc);
}

#[test]
fn item_pointer_min_max() {
    let mut p = ItemPointerData::default();
    ip_set_min(&mut p);
    assert!(ip_is_min(&p));
    assert_eq!(ip_block(&p), 0);
    assert_eq!(ip_offset(&p), 0);

    ip_set_max(&mut p);
    assert_eq!(ip_block(&p), InvalidBlockNumber);
    assert_eq!(ip_offset(&p), 0xffff);
    assert!(ip_is_lossy_page(&p));
}

#[test]
fn item_pointer_lossy_page() {
    let mut p = ItemPointerData::default();
    ip_set_lossy_page(&mut p, 42);
    assert!(ip_is_lossy_page(&p));
    assert_eq!(ip_block(&p), 42);
    assert_eq!(ip_offset(&p), 0xffff);
}

#[test]
fn item_pointer_valid_invalid() {
    let p = ip(5, 3);
    assert!(ip_is_valid(&p));
    let mut q = ItemPointerData::default();
    ip_set_invalid(&mut q);
    assert!(!ip_is_valid(&q));
}

#[test]
fn item_pointer_equals_and_offsets() {
    let a = ip(7, 2);
    let b = ip(7, 2);
    let c = ip(7, 3);
    assert!(ip_equals(&a, &b));
    assert!(!ip_equals(&a, &c));
    assert_eq!(offset_next(2), 3);
    assert_eq!(offset_prev(3), 2);
}

#[test]
fn block_number_validity() {
    assert!(block_is_valid(0));
    assert!(!block_is_valid(InvalidBlockNumber));
}

#[test]
fn bool_to_tri_maps() {
    assert_eq!(bool_to_tri(true), gin::GIN_TRUE);
    assert_eq!(bool_to_tri(false), gin::GIN_FALSE);
}
