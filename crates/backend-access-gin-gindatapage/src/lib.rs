//! GIN page-byte substrate (`access/ginblock.h` page + index-tuple macros).
//!
//! This crate owns the byte-level accessors mapping the C macros from
//! `ginblock.h` (`GinPageGetOpaque`, `GinPageIsLeaf`, `GinDataPageGetData`,
//! `GinDataPageGetRightBound`, `GinDataLeafPageGetPostingList…`,
//! `GinDataPageGetPostingItem`, …) onto operations over a page byte image
//! (`&[u8]` / `&mut [u8]`), plus the GIN *index-tuple* byte macros
//! (`GinGetPostingOffset` / `GinItupIsCompressed` / `GinGetPosting` /
//! `GinGetNPosting` / `GinGetDownlink` / `GinIsPostingTree` /
//! `GinCategoryOffset` / `GinGetNullCategory` / `GinSetNullCategory`) which
//! decode the `t_tid` / `t_info` fields of an [`IndexTupleData`].
//!
//! Everything here is pure byte computation using the same alignment math as
//! the C macros so the produced bytes are bit-identical to PostgreSQL. The page
//! special-area layout / sizes and the [`GinPageOpaqueData`] / [`PostingItem`]
//! carriers come from `types_gin`; page header initialization is delegated to
//! [`backend_storage_page::PageInit`]. This is the foundational substrate the
//! data (posting-tree) page codec (`gindatapage.c`) is built on top of.

use backend_utils_error::PgResult;
use types_core::primitive::{BlockNumber, OffsetNumber, BLCKSZ};
use types_core::InvalidBlockNumber;
use types_gin::{
    GinNullCategory, GinPageOpaqueData, GIN_COMPRESSED, GIN_DATA, GIN_DELETED, GIN_LEAF,
    SIZEOF_GIN_PAGE_OPAQUE_DATA, SIZEOF_POSTING_ITEM,
};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_tuple::heaptuple::{
    BlockIdData, IndexTupleData, ItemPointerData, INDEX_NULL_MASK,
};

#[cfg(test)]
mod tests;

/// GIN posting-tree (data tree) page handling — the `gindatapage.c` data-tree
/// [`types_gin::GinBtreeData`] vtable callbacks + posting-tree entry points
/// built on top of this byte substrate.
pub mod datatree;

// Re-export the posting-tree entry points the rest of GIN (ginget / gininsert /
// ginvacuum) calls, exactly as `gindatapage.c` exports them.
pub use datatree::{
    createPostingTree, ginInsertItemPointers, ginPrepareDataScan, ginScanBeginPostingTree,
    ginVacuumPostingTreeLeaf, GinDataLeafPageGetItems, GinDataLeafPageGetItemsToTbm,
};

// Re-export the PostingItem carrier so consumers of the data-page substrate get
// it (and its sizing constant) from the substrate owner. The struct itself is
// owned by `types_gin` (re-exported from `types_xlog_records::ginxlog`).
pub use types_gin::PostingItem;

// ---------------------------------------------------------------------------
// Alignment helpers (c.h) and fixed layout offsets.
// ---------------------------------------------------------------------------

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8).
#[inline]
pub const fn maxalign(len: usize) -> usize {
    (len + (8 - 1)) & !(8 - 1)
}

/// `SHORTALIGN(LEN)` — round up to a multiple of `ALIGNOF_SHORT` (2).
#[inline]
pub const fn shortalign(len: usize) -> usize {
    (len + (2 - 1)) & !(2 - 1)
}

// Fixed byte offset of `pd_lower` within a page (PageHeaderData native layout):
// pd_lsn(8) + pd_checksum(2) + pd_flags(2) = 12.  Matches the C `PageHeaderData`
// layout and the `OFF_PD_LOWER` in `backend_storage_page`.
const OFF_PD_LOWER: usize = 12;

// Fixed byte offset of `pd_special` within a page header (after
// pd_lower(2) + pd_upper(2)): 12 + 2 + 2 = 16.  This is what
// `PageGetSpecialPointer` reads to locate the special area.
const OFF_PD_SPECIAL: usize = 16;

/// `sizeof(GinPageOpaqueData)` (ginblock.h) == 8.
pub const SIZE_OF_GIN_PAGE_OPAQUE: usize = SIZEOF_GIN_PAGE_OPAQUE_DATA;

/// `sizeof(PostingItem)` (ginblock.h) == 10 (`BlockIdData`(4) +
/// `ItemPointerData`(6)).
pub const SIZE_OF_POSTING_ITEM: usize = SIZEOF_POSTING_ITEM;

/// `sizeof(ItemPointerData)` == 6.
pub const SIZE_OF_ITEM_POINTER: usize = 6;

/// `offsetof(GinPostingList, bytes)` == 8 (`ItemPointerData`(6) + `uint16`(2)).
pub const SIZE_OF_GIN_POSTING_LIST_HEADER: usize = 8;

/// `MAXALIGN(SizeOfPageHeaderData)` — the byte offset of `PageGetContents`.
#[inline]
pub fn page_contents_offset() -> usize {
    maxalign(SizeOfPageHeaderData)
}

// ---------------------------------------------------------------------------
// GinPostingList header byte accessors (a compressed posting list image).
// ---------------------------------------------------------------------------

/// `SizeOfGinPostingList(plist)` (ginblock.h) for a posting list whose on-disk
/// image starts at `buf`. Reads `nbytes` and adds the short-aligned payload.
#[inline]
pub fn size_of_gin_posting_list(buf: &[u8]) -> usize {
    SIZE_OF_GIN_POSTING_LIST_HEADER + shortalign(read_posting_list_nbytes(buf) as usize)
}

/// Read a posting list's `nbytes` field from its on-disk image at offset 0
/// (`{ItemPointerData first; uint16 nbytes; …}`, `nbytes` at byte 6).
#[inline]
pub fn read_posting_list_nbytes(buf: &[u8]) -> u16 {
    u16::from_ne_bytes([buf[6], buf[7]])
}

/// Read a posting list's `first` item pointer from its on-disk image (bytes
/// 0..6).
#[inline]
pub fn read_posting_list_first(buf: &[u8]) -> ItemPointerData {
    read_item_pointer(&buf[0..6])
}

// ---------------------------------------------------------------------------
// GinPageGetOpaque accessors (page special area).
// ---------------------------------------------------------------------------

/// Byte offset of the GIN opaque struct within a page (`pd_special`), read from
/// the page header at offset 16. `GinPageGetOpaque(page) ==
/// PageGetSpecialPointer(page)`.
#[inline]
fn opaque_offset(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[OFF_PD_SPECIAL], page[OFF_PD_SPECIAL + 1]]) as usize
}

/// `GinPageGetOpaque(page)->rightlink`.
#[inline]
pub fn gin_page_get_rightlink(page: &[u8]) -> BlockNumber {
    let o = opaque_offset(page);
    u32::from_ne_bytes(page[o..o + 4].try_into().unwrap())
}

/// `GinPageGetOpaque(page)->rightlink = blkno`.
#[inline]
pub fn gin_page_set_rightlink(page: &mut [u8], blkno: BlockNumber) {
    let o = opaque_offset(page);
    page[o..o + 4].copy_from_slice(&blkno.to_ne_bytes());
}

/// `GinPageGetOpaque(page)->maxoff`.
#[inline]
pub fn gin_page_get_maxoff(page: &[u8]) -> OffsetNumber {
    let o = opaque_offset(page);
    u16::from_ne_bytes(page[o + 4..o + 6].try_into().unwrap())
}

/// `GinPageGetOpaque(page)->maxoff = n`.
#[inline]
pub fn gin_page_set_maxoff(page: &mut [u8], n: OffsetNumber) {
    let o = opaque_offset(page);
    page[o + 4..o + 6].copy_from_slice(&n.to_ne_bytes());
}

/// `GinPageGetOpaque(page)->flags`.
#[inline]
pub fn gin_page_get_flags(page: &[u8]) -> u16 {
    let o = opaque_offset(page);
    u16::from_ne_bytes(page[o + 6..o + 8].try_into().unwrap())
}

/// `GinPageGetOpaque(page)->flags = f`.
#[inline]
pub fn gin_page_set_flags(page: &mut [u8], f: u16) {
    let o = opaque_offset(page);
    page[o + 6..o + 8].copy_from_slice(&f.to_ne_bytes());
}

/// Read the entire [`GinPageOpaqueData`] out of a page's special area.
#[inline]
pub fn gin_page_get_opaque(page: &[u8]) -> GinPageOpaqueData {
    GinPageOpaqueData {
        rightlink: gin_page_get_rightlink(page),
        maxoff: gin_page_get_maxoff(page),
        flags: gin_page_get_flags(page),
    }
}

// Flag predicates / setters (ginblock.h:113..130).

/// `GinPageIsLeaf(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageIsLeaf(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_LEAF != 0
}

/// `GinPageSetLeaf(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageSetLeaf(page: &mut [u8]) {
    let f = gin_page_get_flags(page);
    gin_page_set_flags(page, f | GIN_LEAF);
}

/// `GinPageSetNonLeaf(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageSetNonLeaf(page: &mut [u8]) {
    let f = gin_page_get_flags(page);
    gin_page_set_flags(page, f & !GIN_LEAF);
}

/// `GinPageIsData(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageIsData(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_DATA != 0
}

/// `GinPageSetData(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageSetData(page: &mut [u8]) {
    let f = gin_page_get_flags(page);
    gin_page_set_flags(page, f | GIN_DATA);
}

/// `GinPageIsCompressed(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageIsCompressed(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_COMPRESSED != 0
}

/// `GinPageSetCompressed(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageSetCompressed(page: &mut [u8]) {
    let f = gin_page_get_flags(page);
    gin_page_set_flags(page, f | GIN_COMPRESSED);
}

/// `GinPageIsDeleted(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageIsDeleted(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_DELETED != 0
}

/// `GinPageSetDeleted(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageSetDeleted(page: &mut [u8]) {
    let f = gin_page_get_flags(page);
    gin_page_set_flags(page, f | GIN_DELETED);
}

/// `GinPageSetNonDeleted(page)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageSetNonDeleted(page: &mut [u8]) {
    let f = gin_page_get_flags(page);
    gin_page_set_flags(page, f & !GIN_DELETED);
}

/// `GinPageRightMost(page)` (ginblock.h:130) —
/// `GinPageGetOpaque(page)->rightlink == InvalidBlockNumber`.
#[inline]
#[allow(non_snake_case)]
pub fn GinPageRightMost(page: &[u8]) -> bool {
    gin_page_get_rightlink(page) == InvalidBlockNumber
}

// ---------------------------------------------------------------------------
// Data (posting tree) page layout (ginblock.h:279..323).
// ---------------------------------------------------------------------------

/// `GinDataPageMaxDataSize` (ginblock.h:320) —
/// `BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ItemPointerData))
/// - MAXALIGN(sizeof(GinPageOpaqueData))`.
#[inline]
#[allow(non_snake_case)]
pub fn GinDataPageMaxDataSize() -> usize {
    BLCKSZ
        - maxalign(SizeOfPageHeaderData)
        - maxalign(SIZE_OF_ITEM_POINTER)
        - maxalign(SIZE_OF_GIN_PAGE_OPAQUE)
}

/// Byte offset of `GinDataPageGetData(page)` ==
/// `PageGetContents + MAXALIGN(sizeof(ItemPointerData))`. This is also the start
/// of `GinDataLeafPageGetPostingList(page)`.
#[inline]
pub fn gin_data_page_data_offset() -> usize {
    page_contents_offset() + maxalign(SIZE_OF_ITEM_POINTER)
}

/// Byte offset of `GinDataLeafPageGetPostingList(page)` (ginblock.h:279) — same
/// as [`gin_data_page_data_offset`].
#[inline]
pub fn gin_data_leaf_page_posting_list_offset() -> usize {
    gin_data_page_data_offset()
}

/// `GinDataPageGetRightBound(page)` (ginblock.h:289) — read the right-bound
/// ItemPointer stored at `PageGetContents(page)`.
#[inline]
pub fn gin_data_page_get_right_bound(page: &[u8]) -> ItemPointerData {
    let o = page_contents_offset();
    read_item_pointer(&page[o..])
}

/// Write `GinDataPageGetRightBound(page) = bound`.
#[inline]
pub fn gin_data_page_set_right_bound(page: &mut [u8], bound: &ItemPointerData) {
    let o = page_contents_offset();
    write_item_pointer(&mut page[o..], bound);
}

/// Write the page header's `pd_lower` field (offset 12) directly. The GIN
/// data-page macros set `pd_lower` to express the data size
/// (`((PageHeader) page)->pd_lower = …`).
#[inline]
fn set_pd_lower(page: &mut [u8], value: u16) {
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

/// Read the page header's `pd_lower` field (offset 12).
#[inline]
fn get_pd_lower(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OFF_PD_LOWER], page[OFF_PD_LOWER + 1]])
}

/// `GinDataPageSetDataSize(page, size)` (ginblock.h:310): set `pd_lower` to
/// `size + MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(sizeof(ItemPointerData))`.
#[inline]
#[allow(non_snake_case)]
pub fn GinDataPageSetDataSize(page: &mut [u8], size: usize) {
    debug_assert!(size <= GinDataPageMaxDataSize());
    let pd_lower = size + maxalign(SizeOfPageHeaderData) + maxalign(SIZE_OF_ITEM_POINTER);
    set_pd_lower(page, pd_lower as u16);
}

/// `GinDataLeafPageGetPostingListSize(page)` (ginblock.h:281) —
/// `pd_lower - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ItemPointerData))`.
#[inline]
#[allow(non_snake_case)]
pub fn GinDataLeafPageGetPostingListSize(page: &[u8]) -> usize {
    let pd_lower = get_pd_lower(page) as usize;
    pd_lower - maxalign(SizeOfPageHeaderData) - maxalign(SIZE_OF_ITEM_POINTER)
}

/// `GinNonLeafDataPageGetFreeSpace(page)` (ginblock.h:316) —
/// `GinDataPageMaxDataSize - maxoff * sizeof(PostingItem)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinNonLeafDataPageGetFreeSpace(page: &[u8]) -> usize {
    GinDataPageMaxDataSize() - gin_page_get_maxoff(page) as usize * SIZE_OF_POSTING_ITEM
}

/// Byte offset of `GinDataPageGetPostingItem(page, i)` (1-based;
/// ginblock.h:299).
#[inline]
pub fn gin_data_page_posting_item_offset(i: OffsetNumber) -> usize {
    gin_data_page_data_offset() + ((i as usize) - 1) * SIZE_OF_POSTING_ITEM
}

/// Read `GinDataPageGetPostingItem(page, i)`.
#[inline]
#[allow(non_snake_case)]
pub fn GinDataPageGetPostingItem(page: &[u8], i: OffsetNumber) -> PostingItem {
    let o = gin_data_page_posting_item_offset(i);
    read_posting_item(&page[o..])
}

/// Write `GinDataPageGetPostingItem(page, i) = item`.
#[inline]
#[allow(non_snake_case)]
pub fn GinDataPageSetPostingItem(page: &mut [u8], i: OffsetNumber, item: &PostingItem) {
    let o = gin_data_page_posting_item_offset(i);
    write_posting_item(&mut page[o..], item);
}

// ---------------------------------------------------------------------------
// PostingItem (de)serialization + accessors (ginblock.h:181..193).
// ---------------------------------------------------------------------------

/// `PostingItemGetBlockNumber(pointer)` (ginblock.h:190).
#[inline]
#[allow(non_snake_case)]
pub fn PostingItemGetBlockNumber(item: &PostingItem) -> BlockNumber {
    item.child_blkno.block_number()
}

/// `PostingItemSetBlockNumber(pointer, blkno)` (ginblock.h:193).
#[inline]
#[allow(non_snake_case)]
pub fn PostingItemSetBlockNumber(item: &mut PostingItem, blkno: BlockNumber) {
    item.child_blkno.set_block_number(blkno);
}

/// Read a `PostingItem` (10 bytes) from `buf`.
#[inline]
pub fn read_posting_item(buf: &[u8]) -> PostingItem {
    let bi_hi = u16::from_ne_bytes([buf[0], buf[1]]);
    let bi_lo = u16::from_ne_bytes([buf[2], buf[3]]);
    let key = read_item_pointer(&buf[4..]);
    PostingItem {
        child_blkno: BlockIdData { bi_hi, bi_lo },
        key,
    }
}

/// Write a `PostingItem` (10 bytes) into `buf`.
#[inline]
pub fn write_posting_item(buf: &mut [u8], item: &PostingItem) {
    buf[0..2].copy_from_slice(&item.child_blkno.bi_hi.to_ne_bytes());
    buf[2..4].copy_from_slice(&item.child_blkno.bi_lo.to_ne_bytes());
    write_item_pointer(&mut buf[4..], &item.key);
}

/// Read an `ItemPointerData` (6 bytes) from `buf` —
/// `{BlockIdData(4) ip_blkid; uint16 ip_posid}`.
#[inline]
pub fn read_item_pointer(buf: &[u8]) -> ItemPointerData {
    let mut iptr = ItemPointerData::new(0, u16::from_ne_bytes([buf[4], buf[5]]));
    iptr.ip_blkid.bi_hi = u16::from_ne_bytes([buf[0], buf[1]]);
    iptr.ip_blkid.bi_lo = u16::from_ne_bytes([buf[2], buf[3]]);
    iptr
}

/// Write an `ItemPointerData` (6 bytes) into `buf`.
#[inline]
pub fn write_item_pointer(buf: &mut [u8], iptr: &ItemPointerData) {
    buf[0..2].copy_from_slice(&iptr.ip_blkid.bi_hi.to_ne_bytes());
    buf[2..4].copy_from_slice(&iptr.ip_blkid.bi_lo.to_ne_bytes());
    buf[4..6].copy_from_slice(&iptr.ip_posid.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// GIN index-tuple byte macros (ginblock.h:218..258) — t_tid / t_info math.
//
// These decode the `GinItemPointerGet*NoCheck` of an entry/non-leaf tuple's
// `t_tid` (and `t_info` for the null-category offset). PostgreSQL deliberately
// uses the *NoCheck* item-pointer accessors here because `ip_posid` is not
// always a "valid" offset (it stores N-posting / posting-tree markers).
// ---------------------------------------------------------------------------

/// `GIN_TREE_POSTING` (ginblock.h:231) — N-posting sentinel marking a tuple
/// that points to a posting tree.
pub const GIN_TREE_POSTING: OffsetNumber = 0xffff;

/// `GIN_ITUP_COMPRESSED` (ginblock.h:236) — high bit of the posting offset
/// marking a compressed posting list.
pub const GIN_ITUP_COMPRESSED: u32 = 1u32 << 31;

/// `GinItemPointerGetBlockNumber(&itup->t_tid)` — the *NoCheck* block-number
/// accessor (block id of the tuple's `t_tid`).
#[inline]
fn gin_item_pointer_get_block_number(tid: &ItemPointerData) -> BlockNumber {
    tid.ip_blkid.block_number()
}

/// `GinItemPointerGetOffsetNumber(&itup->t_tid)` — the *NoCheck* offset-number
/// accessor (`ip_posid` of the tuple's `t_tid`).
#[inline]
fn gin_item_pointer_get_offset_number(tid: &ItemPointerData) -> OffsetNumber {
    tid.ip_posid
}

/// `GinGetNPosting(itup)` (ginblock.h:229) — number of heap pointers packed in
/// a leaf entry tuple (stored as the offset number of `t_tid`).
#[inline]
#[allow(non_snake_case)]
pub fn GinGetNPosting(itup: &IndexTupleData) -> OffsetNumber {
    gin_item_pointer_get_offset_number(&itup.t_tid)
}

/// `GinIsPostingTree(itup)` (ginblock.h:232) — the leaf tuple points to a
/// posting tree (`GinGetNPosting == GIN_TREE_POSTING`).
#[inline]
#[allow(non_snake_case)]
pub fn GinIsPostingTree(itup: &IndexTupleData) -> bool {
    GinGetNPosting(itup) == GIN_TREE_POSTING
}

/// `GinGetPostingOffset(itup)` (ginblock.h:237) — byte offset of the posting
/// list within the leaf tuple (block-number of `t_tid`, masking off the
/// compressed bit).
#[inline]
#[allow(non_snake_case)]
pub fn GinGetPostingOffset(itup: &IndexTupleData) -> u32 {
    gin_item_pointer_get_block_number(&itup.t_tid) & !GIN_ITUP_COMPRESSED
}

/// `GinGetPosting(itup)` (ginblock.h:239) — byte offset (within the index
/// tuple) at which the posting list begins (`(char*) itup +
/// GinGetPostingOffset(itup)`). Returned as the offset, since here the tuple is
/// a value rather than a raw `char*`.
#[inline]
#[allow(non_snake_case)]
pub fn GinGetPosting(itup: &IndexTupleData) -> usize {
    GinGetPostingOffset(itup) as usize
}

/// `GinItupIsCompressed(itup)` (ginblock.h:240) — the leaf tuple's posting list
/// is compressed (`GIN_ITUP_COMPRESSED` bit set in the block-number of
/// `t_tid`).
#[inline]
#[allow(non_snake_case)]
pub fn GinItupIsCompressed(itup: &IndexTupleData) -> bool {
    (gin_item_pointer_get_block_number(&itup.t_tid) & GIN_ITUP_COMPRESSED) != 0
}

/// `GinGetDownlink(itup)` (ginblock.h:258) — child block of a non-leaf entry
/// tuple (block-number of `t_tid`).
#[inline]
#[allow(non_snake_case)]
pub fn GinGetDownlink(itup: &IndexTupleData) -> BlockNumber {
    gin_item_pointer_get_block_number(&itup.t_tid)
}

/// `IndexInfoFindDataOffset(t_info)` (`access/itup.h`) — the data-area offset
/// within an index tuple. Replicated here (pure byte computation) since the GIN
/// null-category offset macro needs it and this substrate must not depend on
/// the index-tuple owner crate.
///
/// `MAXALIGN(sizeof(IndexTupleData))` with no nulls;
/// `MAXALIGN(sizeof(IndexTupleData) + sizeof(IndexAttributeBitMapData))` with.
#[inline]
fn index_info_find_data_offset(t_info: u16) -> usize {
    // sizeof(IndexTupleData) == 8; sizeof(IndexAttributeBitMapData) ==
    // (INDEX_MAX_KEYS + 7) / 8.
    const SIZEOF_INDEX_TUPLE_DATA: usize = 8;
    const SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA: usize =
        (types_core::INDEX_MAX_KEYS as usize + 7) / 8;
    if (t_info & INDEX_NULL_MASK) == 0 {
        maxalign(SIZEOF_INDEX_TUPLE_DATA)
    } else {
        maxalign(SIZEOF_INDEX_TUPLE_DATA + SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA)
    }
}

/// `GinCategoryOffset(itup, ginstate)` (ginblock.h:218) — byte offset (within
/// the index tuple) of the `GinNullCategory` byte:
/// `IndexInfoFindDataOffset(itup->t_info) + (oneCol ? 0 : sizeof(int16))`.
///
/// `one_col` is `GinState.oneCol`; for a multi-column index the category byte
/// follows the leading `int16` attribute number.
#[inline]
#[allow(non_snake_case)]
pub fn GinCategoryOffset(itup: &IndexTupleData, one_col: bool) -> usize {
    index_info_find_data_offset(itup.t_info) + if one_col { 0 } else { core::mem::size_of::<i16>() }
}

/// `GinGetNullCategory(itup, ginstate)` (ginblock.h:221) — read the
/// `GinNullCategory` byte from the index tuple's on-disk image (`itup_bytes`)
/// at [`GinCategoryOffset`].
#[inline]
#[allow(non_snake_case)]
pub fn GinGetNullCategory(
    itup: &IndexTupleData,
    itup_bytes: &[u8],
    one_col: bool,
) -> GinNullCategory {
    let off = GinCategoryOffset(itup, one_col);
    itup_bytes[off] as GinNullCategory
}

/// `GinSetNullCategory(itup, ginstate, c)` (ginblock.h:223) — write the
/// `GinNullCategory` byte into the index tuple's on-disk image (`itup_bytes`)
/// at [`GinCategoryOffset`].
#[inline]
#[allow(non_snake_case)]
pub fn GinSetNullCategory(
    itup: &IndexTupleData,
    itup_bytes: &mut [u8],
    one_col: bool,
    c: GinNullCategory,
) {
    let off = GinCategoryOffset(itup, one_col);
    itup_bytes[off] = c as u8;
}

// ---------------------------------------------------------------------------
// GinInitPage (ginutil.c:342) — pure page-byte initialization.
// ---------------------------------------------------------------------------

/// Write the [`GinPageOpaqueData`] into the page's special area.
#[inline]
fn write_opaque(page: &mut [u8], opaque: &GinPageOpaqueData) {
    let o = opaque_offset(page);
    page[o..o + 4].copy_from_slice(&opaque.rightlink.to_ne_bytes());
    page[o + 4..o + 6].copy_from_slice(&opaque.maxoff.to_ne_bytes());
    page[o + 6..o + 8].copy_from_slice(&opaque.flags.to_ne_bytes());
}

/// `GinInitPage(page, f, pageSize)` (ginutil.c:342): initialize a GIN page's
/// header (`PageInit`, special area = `sizeof(GinPageOpaqueData)`), then set the
/// opaque `flags = f`, `maxoff = 0`, and `rightlink = InvalidBlockNumber`.
#[allow(non_snake_case)]
pub fn GinInitPage(page: &mut [u8], f: u32, page_size: usize) -> PgResult<()> {
    backend_storage_page::PageInit(page, page_size, SIZE_OF_GIN_PAGE_OPAQUE)?;

    let opaque = GinPageOpaqueData {
        rightlink: InvalidBlockNumber,
        maxoff: 0,
        flags: f as u16,
    };
    write_opaque(page, &opaque);
    Ok(())
}
