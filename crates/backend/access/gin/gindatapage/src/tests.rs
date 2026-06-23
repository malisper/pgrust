//! Tests for the GIN page-byte substrate.

use super::*;
use ::types_core::primitive::BLCKSZ;
use ::gin::{GIN_DATA, GIN_LEAF, GIN_META};
use ::types_tuple::heaptuple::{BlockIdData, IndexTupleData, ItemPointerData};

fn fresh_data_leaf_page() -> Vec<u8> {
    let mut page = vec![0u8; BLCKSZ];
    GinInitPage(&mut page, (GIN_DATA | GIN_LEAF) as u32, BLCKSZ).unwrap();
    page
}

#[test]
fn alignment_helpers() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
    assert_eq!(shortalign(0), 0);
    assert_eq!(shortalign(1), 2);
    assert_eq!(shortalign(3), 4);
}

#[test]
fn layout_constants() {
    assert_eq!(SIZE_OF_GIN_PAGE_OPAQUE, 8);
    assert_eq!(SIZE_OF_POSTING_ITEM, 10);
    assert_eq!(SIZE_OF_ITEM_POINTER, 6);
    assert_eq!(SIZE_OF_GIN_POSTING_LIST_HEADER, 8);
    // page contents start after the MAXALIGN'd 24-byte header.
    assert_eq!(page_contents_offset(), 24);
    // data offset = contents + MAXALIGN(6) = 24 + 8 = 32.
    assert_eq!(gin_data_page_data_offset(), 32);
    // GinDataPageMaxDataSize = 8192 - 24 - 8 - 8 = 8152.
    assert_eq!(GinDataPageMaxDataSize(), 8152);
}

#[test]
fn init_page_sets_opaque() {
    let mut page = vec![0u8; BLCKSZ];
    GinInitPage(&mut page, GIN_META as u32, BLCKSZ).unwrap();
    let opaque = gin_page_get_opaque(&page);
    assert_eq!(opaque.flags, GIN_META);
    assert_eq!(opaque.maxoff, 0);
    assert_eq!(opaque.rightlink, InvalidBlockNumber);
    assert!(GinPageRightMost(&page));
    assert!(!GinPageIsLeaf(&page));
    assert!(!GinPageIsData(&page));
}

#[test]
fn flag_predicates_and_setters() {
    let mut page = fresh_data_leaf_page();
    assert!(GinPageIsLeaf(&page));
    assert!(GinPageIsData(&page));
    assert!(!GinPageIsCompressed(&page));
    assert!(!GinPageIsDeleted(&page));

    GinPageSetCompressed(&mut page);
    assert!(GinPageIsCompressed(&page));

    GinPageSetDeleted(&mut page);
    assert!(GinPageIsDeleted(&page));
    GinPageSetNonDeleted(&mut page);
    assert!(!GinPageIsDeleted(&page));

    GinPageSetNonLeaf(&mut page);
    assert!(!GinPageIsLeaf(&page));
    GinPageSetLeaf(&mut page);
    assert!(GinPageIsLeaf(&page));
}

#[test]
fn rightlink_and_maxoff_roundtrip() {
    let mut page = fresh_data_leaf_page();
    gin_page_set_rightlink(&mut page, 12345);
    assert_eq!(gin_page_get_rightlink(&page), 12345);
    assert!(!GinPageRightMost(&page));

    gin_page_set_maxoff(&mut page, 42);
    assert_eq!(gin_page_get_maxoff(&page), 42);
}

#[test]
fn right_bound_roundtrip() {
    let mut page = fresh_data_leaf_page();
    let bound = ItemPointerData::new(7, 3);
    gin_data_page_set_right_bound(&mut page, &bound);
    let got = gin_data_page_get_right_bound(&page);
    assert_eq!(got.ip_blkid.block_number(), 7);
    assert_eq!(got.ip_posid, 3);
}

#[test]
fn data_size_roundtrip() {
    let mut page = fresh_data_leaf_page();
    GinDataPageSetDataSize(&mut page, 100);
    assert_eq!(GinDataLeafPageGetPostingListSize(&page), 100);
}

#[test]
fn non_leaf_free_space() {
    let mut page = vec![0u8; BLCKSZ];
    GinInitPage(&mut page, GIN_DATA as u32, BLCKSZ).unwrap();
    gin_page_set_maxoff(&mut page, 3);
    // free = MaxDataSize - 3 * sizeof(PostingItem) = 8152 - 30 = 8122.
    assert_eq!(GinNonLeafDataPageGetFreeSpace(&page), 8152 - 30);
}

#[test]
fn posting_item_roundtrip_on_page() {
    let mut page = vec![0u8; BLCKSZ];
    GinInitPage(&mut page, GIN_DATA as u32, BLCKSZ).unwrap();
    let mut item = PostingItem {
        child_blkno: BlockIdData::new(0),
        key: ItemPointerData::new(99, 5),
    };
    PostingItemSetBlockNumber(&mut item, 4242);
    assert_eq!(PostingItemGetBlockNumber(&item), 4242);

    GinDataPageSetPostingItem(&mut page, 1, &item);
    GinDataPageSetPostingItem(&mut page, 2, &item);
    let got = GinDataPageGetPostingItem(&page, 2);
    assert_eq!(PostingItemGetBlockNumber(&got), 4242);
    assert_eq!(got.key.ip_blkid.block_number(), 99);
    assert_eq!(got.key.ip_posid, 5);

    // The two items occupy adjacent 10-byte slots starting at the data offset.
    assert_eq!(gin_data_page_posting_item_offset(1), gin_data_page_data_offset());
    assert_eq!(
        gin_data_page_posting_item_offset(2),
        gin_data_page_data_offset() + SIZE_OF_POSTING_ITEM
    );
}

#[test]
fn posting_item_byte_serialization() {
    let item = PostingItem {
        child_blkno: BlockIdData::new(0x0001_0002),
        key: ItemPointerData::new(0x0003_0004, 0x0506),
    };
    let mut buf = [0u8; SIZE_OF_POSTING_ITEM];
    write_posting_item(&mut buf, &item);
    let back = read_posting_item(&buf);
    assert_eq!(back.child_blkno.block_number(), item.child_blkno.block_number());
    assert_eq!(back.key, item.key);
}

#[test]
fn posting_list_header_decode() {
    // Image: ItemPointerData first (6 bytes) + uint16 nbytes (2 bytes) + payload.
    let mut buf = vec![0u8; 8 + 7];
    // first = block 10, offset 2.
    write_item_pointer(&mut buf[0..6], &ItemPointerData::new(10, 2));
    buf[6..8].copy_from_slice(&7u16.to_ne_bytes());
    assert_eq!(read_posting_list_nbytes(&buf), 7);
    let first = read_posting_list_first(&buf);
    assert_eq!(first.ip_blkid.block_number(), 10);
    assert_eq!(first.ip_posid, 2);
    // SizeOfGinPostingList = 8 + SHORTALIGN(7) = 8 + 8 = 16.
    assert_eq!(size_of_gin_posting_list(&buf), 16);
}

#[test]
fn itup_posting_tree_and_n_posting() {
    let mut itup = IndexTupleData::default();
    // A normal leaf tuple with 5 heap pointers packed.
    itup.t_tid.ip_posid = 5;
    assert_eq!(GinGetNPosting(&itup), 5);
    assert!(!GinIsPostingTree(&itup));

    // Mark as a posting-tree pointer.
    itup.t_tid.ip_posid = GIN_TREE_POSTING;
    assert!(GinIsPostingTree(&itup));
}

#[test]
fn itup_posting_offset_and_compressed() {
    let mut itup = IndexTupleData::default();
    // Compressed posting list at byte offset 64:
    // block-number of t_tid = 64 | GIN_ITUP_COMPRESSED.
    itup.t_tid
        .ip_blkid
        .set_block_number(64 | GIN_ITUP_COMPRESSED);
    assert!(GinItupIsCompressed(&itup));
    assert_eq!(GinGetPostingOffset(&itup), 64);
    assert_eq!(GinGetPosting(&itup), 64);

    // Uncompressed (pre-9.4) posting list at offset 32.
    let mut itup2 = IndexTupleData::default();
    itup2.t_tid.ip_blkid.set_block_number(32);
    assert!(!GinItupIsCompressed(&itup2));
    assert_eq!(GinGetPostingOffset(&itup2), 32);
}

#[test]
fn itup_downlink() {
    let mut itup = IndexTupleData::default();
    itup.t_tid.ip_blkid.set_block_number(777);
    assert_eq!(GinGetDownlink(&itup), 777);
}

#[test]
fn category_offset_and_roundtrip() {
    // No nulls: IndexInfoFindDataOffset = MAXALIGN(8) = 8.
    let itup = IndexTupleData::default();
    // single-column: category byte at offset 8.
    assert_eq!(GinCategoryOffset(&itup, true), 8);
    // multi-column: + sizeof(int16) = 10.
    assert_eq!(GinCategoryOffset(&itup, false), 10);

    let mut bytes = vec![0u8; 16];
    GinSetNullCategory(&itup, &mut bytes, true, ::gin::GIN_CAT_NULL_KEY);
    assert_eq!(
        GinGetNullCategory(&itup, &bytes, true),
        ::gin::GIN_CAT_NULL_KEY
    );

    GinSetNullCategory(&itup, &mut bytes, false, ::gin::GIN_CAT_EMPTY_QUERY);
    assert_eq!(
        GinGetNullCategory(&itup, &bytes, false),
        ::gin::GIN_CAT_EMPTY_QUERY
    );
}
