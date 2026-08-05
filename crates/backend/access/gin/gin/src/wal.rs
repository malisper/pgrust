//! ginxlog.h record images: byte layouts match the C structs exactly
//! (ginxlogCreatePostingTree 4, ginxlogInsert 2, ginxlogSplit 26 (locator 12
//! + rrlink/leftChild/rightChild 12 + flags 2), ginxlogDeletePage 12,
//! ginxlogUpdateMeta 80, ginxlogInsertListPage 8, ginxlogDeleteListPages 64,
//! ginxlogInsertEntry header 4, ginxlogRecompressDataLeaf 2,
//! ginxlogInsertDataInternal 12).

use ::gin_vocab::{GinMetaPageData, PostingItem};
use ::types_core::{BlockNumber, OffsetNumber, TransactionId};
use ::types_rel::Relation;

fn locator_bytes(rel: &Relation<'_>) -> [u8; 12] {
    let loc = rel.rd_locator.get();
    let mut b = [0u8; 12];
    b[0..4].copy_from_slice(&loc.spcOid.to_ne_bytes());
    b[4..8].copy_from_slice(&loc.dbOid.to_ne_bytes());
    b[8..12].copy_from_slice(&loc.relNumber.to_ne_bytes());
    b
}

pub(crate) fn ginxlog_create_posting_tree(size: u32) -> [u8; 4] {
    size.to_ne_bytes()
}

pub(crate) fn ginxlog_insert(flags: u16) -> [u8; 2] {
    flags.to_ne_bytes()
}

pub(crate) fn ginxlog_insert_entry_header(offset: OffsetNumber, is_delete: bool) -> [u8; 4] {
    let mut b = [0u8; 4];
    b[0..2].copy_from_slice(&offset.to_ne_bytes());
    b[2] = is_delete as u8;
    b
}

pub(crate) fn ginxlog_recompress_header(nactions: u16) -> [u8; 2] {
    nactions.to_ne_bytes()
}

pub(crate) fn ginxlog_insert_data_internal(offset: OffsetNumber, newitem: &PostingItem) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0..2].copy_from_slice(&offset.to_ne_bytes());
    // SAFETY: PostingItem is a 10-byte POD.
    unsafe {
        core::ptr::copy_nonoverlapping(
            (newitem as *const PostingItem).cast::<u8>(),
            b.as_mut_ptr().add(2),
            10,
        );
    }
    b
}

pub(crate) fn ginxlog_split(
    rel: &Relation<'_>,
    rrlink: BlockNumber,
    left_child_blkno: BlockNumber,
    right_child_blkno: BlockNumber,
    flags: u16,
) -> [u8; 28] {
    // C sizeof(ginxlogSplit) = 28 (12 locator + 3*4 + 2 flags + 2 pad).
    let mut b = [0u8; 28];
    b[0..12].copy_from_slice(&locator_bytes(rel));
    b[12..16].copy_from_slice(&rrlink.to_ne_bytes());
    b[16..20].copy_from_slice(&left_child_blkno.to_ne_bytes());
    b[20..24].copy_from_slice(&right_child_blkno.to_ne_bytes());
    b[24..26].copy_from_slice(&flags.to_ne_bytes());
    b
}

pub(crate) fn ginxlog_delete_page(
    parent_offset: OffsetNumber,
    right_link: BlockNumber,
    delete_xid: TransactionId,
) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0..2].copy_from_slice(&parent_offset.to_ne_bytes());
    b[4..8].copy_from_slice(&right_link.to_ne_bytes());
    b[8..12].copy_from_slice(&delete_xid.to_ne_bytes());
    b
}

pub(crate) fn metadata_bytes(meta: &GinMetaPageData) -> [u8; 56] {
    // SAFETY: GinMetaPageData is a 56-byte repr(C) POD.
    unsafe { core::mem::transmute::<GinMetaPageData, [u8; 56]>(*meta) }
}

pub(crate) fn ginxlog_update_meta(
    rel: &Relation<'_>,
    metadata: &GinMetaPageData,
    prev_tail: BlockNumber,
    new_rightlink: BlockNumber,
    ntuples: i32,
) -> [u8; 88] {
    // C layout: locator 12 + pad 4 + metadata 56 (8-aligned at 16) + prevTail
    // + newRightlink + ntuples + trailing pad 4 = 88.
    let mut b = [0u8; 88];
    b[0..12].copy_from_slice(&locator_bytes(rel));
    b[16..72].copy_from_slice(&metadata_bytes(metadata));
    b[72..76].copy_from_slice(&prev_tail.to_ne_bytes());
    b[76..80].copy_from_slice(&new_rightlink.to_ne_bytes());
    b[80..84].copy_from_slice(&ntuples.to_ne_bytes());
    b
}

pub(crate) fn ginxlog_insert_listpage(rightlink: BlockNumber, ntuples: i32) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[0..4].copy_from_slice(&rightlink.to_ne_bytes());
    b[4..8].copy_from_slice(&ntuples.to_ne_bytes());
    b
}

pub(crate) fn ginxlog_delete_listpages(metadata: &GinMetaPageData, ndeleted: i32) -> [u8; 64] {
    let mut b = [0u8; 64];
    b[0..56].copy_from_slice(&metadata_bytes(metadata));
    b[56..60].copy_from_slice(&ndeleted.to_ne_bytes());
    b
}
