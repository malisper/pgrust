//! Unit tests for the pure-byte helpers of the gininsert F1 spine.
//!
//! The descent-driven functions (`ginEntryInsert` / `addItemPointersToLeafTuple`
//! / `buildFreshLeafTuple` / `gininsert`) need a live buffer manager + GIN index
//! relation, which the standalone test harness does not provide; they are
//! exercised end-to-end by the GIN AM integration once a build provider lands.
//! Here we lock down the on-disk byte arithmetic (`GinSetPostingTree`,
//! `SizeOfGinPostingList`, the `header_of` decode), which is what the descent
//! relies on to round-trip leaf tuples.

use super::*;

use ::gindatapage::{GinGetDownlink, GinGetNPosting, GIN_TREE_POSTING};

/// `GinSetPostingTree` marks a tuple as a posting tree (`GinGetNPosting ==
/// GIN_TREE_POSTING`) and stores the root block as the t_tid block number
/// (`GinGetDownlink`).
#[test]
fn gin_set_posting_tree_roundtrips() {
    // An 8-byte IndexTuple header is all GinSetPostingTree touches.
    let mut itup = alloc::vec![0u8; 16];
    gin_set_posting_tree(&mut itup, 0x0001_2345);

    let hdr = header_of(&itup);
    assert!(GinIsPostingTree(&hdr));
    assert_eq!(GinGetNPosting(&hdr), GIN_TREE_POSTING);
    assert_eq!(GinGetDownlink(&hdr), 0x0001_2345);
}

/// `SizeOfGinPostingList` = 8-byte header + SHORTALIGN(nbytes). The on-disk
/// `nbytes` field lives at byte offset 6 (after the 6-byte `first` ItemPointer).
#[test]
fn size_of_gin_posting_list_short_aligns() {
    let mut seg = alloc::vec![0u8; 8];
    // nbytes = 5 (odd) -> SHORTALIGN(5) = 6 -> total 8 + 6 = 14.
    seg[6..8].copy_from_slice(&5u16.to_ne_bytes());
    assert_eq!(size_of_gin_posting_list(&seg), 14);

    // nbytes = 4 (even) -> total 8 + 4 = 12.
    seg[6..8].copy_from_slice(&4u16.to_ne_bytes());
    assert_eq!(size_of_gin_posting_list(&seg), 12);
}

/// `header_of` decodes the t_tid block/offset and t_info from the leading
/// 8 bytes in native byte order.
#[test]
fn header_of_decodes_t_tid() {
    let mut itup = alloc::vec![0u8; 16];
    // block 0x00AB_CDEF -> bi_hi=0x00AB, bi_lo=0xCDEF; offset 7; t_info 0x1234.
    itup[0..2].copy_from_slice(&0x00ABu16.to_ne_bytes());
    itup[2..4].copy_from_slice(&0xCDEFu16.to_ne_bytes());
    itup[4..6].copy_from_slice(&7u16.to_ne_bytes());
    itup[6..8].copy_from_slice(&0x1234u16.to_ne_bytes());

    let hdr = header_of(&itup);
    assert_eq!(GinGetDownlink(&hdr), 0x00AB_CDEF);
    assert_eq!(hdr.t_tid.ip_posid, 7);
    assert_eq!(hdr.t_info, 0x1234);
}
