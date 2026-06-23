//! Unit tests for the GIN entry-page byte codec.

use super::*;

/// `maxalign` / `shortalign` mirror the c.h macros.
#[test]
fn alignment_helpers() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
    assert_eq!(shortalign(0), 0);
    assert_eq!(shortalign(1), 2);
    assert_eq!(shortalign(2), 2);
    assert_eq!(shortalign(3), 4);
}

/// The on-disk `IndexTupleData` header round-trips through the byte readers and
/// the `t_info` writer preserves the size field.
#[test]
fn header_roundtrip() {
    let mut tup = alloc::vec![0u8; 16];
    // t_info = INDEX_NULL_MASK | 12 (size).
    write_t_info(&mut tup, INDEX_NULL_MASK | 12);
    assert!(index_tuple_has_nulls(&tup));
    assert_eq!(index_tuple_size(&tup), 12);

    // GinSetNPosting writes the t_tid offset (ip_posid).
    gin_set_n_posting(&mut tup, 5);
    assert_eq!(gin_get_n_posting(&tup), 5);

    // GinSetDownlink writes t_tid block number, ip_posid = InvalidOffsetNumber.
    gin_set_downlink(&mut tup, 42);
    assert_eq!(gin_get_downlink(&tup), 42);

    // GinSetPostingOffset sets the compressed bit and the offset.
    gin_set_posting_offset(&mut tup, 24);
    assert!(gin_itup_is_compressed(&tup));
    assert_eq!(gin_get_posting_data_offset(&tup), 24);
}

/// `GinFormInteriorTuple` of a non-leaf tuple copies it as-is and overwrites the
/// downlink.
#[test]
fn form_interior_tuple_copies_and_sets_downlink() {
    // A non-leaf entry tuple image: 8-byte header (size 8, no nulls), no data.
    let mut src = alloc::vec![0u8; 8];
    write_t_info(&mut src, 8);
    gin_set_downlink(&mut src, 7);

    // page flags = 0 (non-leaf, non-data) — build a tiny page-special image.
    // GinFormInteriorTuple only reads GinPageIsLeaf(page); a zero special-area
    // page reports non-leaf.
    let mut page = alloc::vec![0u8; BLCKSZ];
    // pd_special points just past a zeroed special area; leave flags = 0.
    page[16..18].copy_from_slice(&((BLCKSZ - 8) as u16).to_ne_bytes());

    let nitup = GinFormInteriorTuple(&src, &page, 99);
    assert_eq!(gin_get_downlink(&nitup), 99);
    assert_eq!(index_tuple_size(&nitup), 8);
}
