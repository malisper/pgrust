//! GIN posting-tree (data tree) page handling — the `gindatapage.c` data-tree
//! [`GinBtreeData`] vtable callbacks plus the disassembled-leaf recompression
//! machinery and the exported posting-tree entry points.
//!
//! This is the **L3** data-tree leg of GIN: it installs the nine data-page
//! method-table callbacks (`dataLocateItem` / `dataGetLeftMostPage` /
//! `dataIsMoveRight` / `dataFindChildPtr` / `dataBeginPlaceToPage` /
//! `dataExecPlaceToPage` / `dataPrepareDownlink` / `ginDataFillRoot`) into a
//! [`GinBtreeData`] via [`ginPrepareDataScan`], so that the `ginbtree.c` spine
//! (the landed [`backend_access_gin_ginbtree`] crate) can drive insertion and
//! search through the posting tree. It mirrors C exactly:
//!
//! * the byte-level posting-item / page accessors come from the [`crate`] byte
//!   substrate (`ginblock.h` macros);
//! * the compressed posting-list codec (`ginCompressPostingList` /
//!   `ginPostingListDecode` / `ginMergeItemPointers`) comes from the audited
//!   `gin-core-probe` (`ginpostinglist.rs`);
//! * page bytes are reached through the bufmgr seam (`BufferGetPage(buffer)`),
//!   exactly like the entry-tree callbacks;
//! * WAL records are registered through the `xloginsert` seams (the spine has
//!   already begun the record for the in-`ginbtree` legs);
//! * the dlist of `leafSegmentInfo` becomes an owned `Vec`, with `lastleft`
//!   modelled as a `usize` index into it (the C `dlist_node *`).
//!
//! `ginVacuumPostingTreeLeaf` is a sanctioned panic leg: it needs the
//! `ginvacuum.c` `GinVacuumState` / `ginVacuumItemPointers` that are not yet
//! ported (see `ginvacuum prefetch` sanctioned panic in the GIN tower).

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::Mcx;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_error::{ereport, PgResult};
use types_error::error::ERROR;

use backend_access_gin_core_probe::ginpostinglist::{
    ginCompareItemPointers, ginCompressPostingList, ginMergeItemPointers, ginPostingListDecode,
    ginPostingListDecodeAllSegments,
};
use types_core::primitive::{BlockNumber, OffsetNumber, BLCKSZ};
use types_core::InvalidBlockNumber;
use types_gin::{
    BeginPlaceToPageResult, GinBtreeData, GinBtreeDataLeafInsertData, GinBtreeStack,
    GinInsertPayload, GinPlaceToPageRC, GinStatsData, PostingItem, PtpWorkspace,
    GIN_SEGMENT_ADDITEMS, GIN_SEGMENT_DELETE, GIN_SEGMENT_INSERT, GIN_SEGMENT_REPLACE,
    GIN_SEGMENT_UNMODIFIED,
};
use types_rel::Relation;
use types_storage::storage::Buffer;
use types_tuple::heaptuple::{
    ItemPointerData, FIRST_OFFSET_NUMBER as FirstOffsetNumber,
    INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
};

use crate::{
    gin_data_leaf_page_posting_list_offset, gin_data_page_get_right_bound,
    gin_data_page_posting_item_offset, gin_data_page_set_right_bound, gin_page_get_maxoff,
    gin_page_set_maxoff, gin_page_set_rightlink, read_posting_list_first, shortalign,
    size_of_gin_posting_list, write_posting_item,
    GinDataPageMaxDataSize, GinDataPageGetPostingItem, GinDataPageSetDataSize,
    GinDataPageSetPostingItem, GinDataLeafPageGetPostingListSize, GinInitPage,
    GinNonLeafDataPageGetFreeSpace, GinPageIsCompressed, GinPageIsData, GinPageIsDeleted,
    GinPageIsLeaf, GinPageRightMost, GinPageSetCompressed, PostingItemGetBlockNumber,
    PostingItemSetBlockNumber, SIZE_OF_POSTING_ITEM,
};

// ===========================================================================
// gindatapage.c constants (lines 34..43).
// ===========================================================================

/// `GinPostingListSegmentMaxSize` (gindatapage.c:34).
const GIN_POSTING_LIST_SEGMENT_MAX_SIZE: usize = 384;
/// `GinPostingListSegmentTargetSize` (gindatapage.c:35).
const GIN_POSTING_LIST_SEGMENT_TARGET_SIZE: usize = 256;
/// `GinPostingListSegmentMinSize` (gindatapage.c:36).
const GIN_POSTING_LIST_SEGMENT_MIN_SIZE: usize = 128;

/// `MinTuplesPerSegment` (gindatapage.c:43) — `(GinPostingListSegmentMaxSize -
/// 2) / 6`.
const MIN_TUPLES_PER_SEGMENT: usize = (GIN_POSTING_LIST_SEGMENT_MAX_SIZE - 2) / 6;

/// `RM_GIN_ID` (rmgrlist.h) — GIN resource-manager id.
const RM_GIN_ID: types_core::RmgrId = 13;
/// `XLOG_GIN_CREATE_PTREE` (ginxlog.h).
const XLOG_GIN_CREATE_PTREE: u8 = 0x30;
/// `REGBUF_WILL_INIT` (xloginsert.h).
const REGBUF_WILL_INIT: u8 = 0x02;
/// `REGBUF_STANDARD` (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x04;

/// `sizeof(ItemPointerData)` on disk.
const SIZE_OF_ITEM_POINTER: usize = 6;

// ===========================================================================
// disassembledLeaf / leafSegmentInfo (gindatapage.c:48..103).
//
// The C dlist of leafSegmentInfo becomes an owned Vec; `lastleft` (a
// dlist_node *) becomes a usize index into that Vec.
// ===========================================================================

/// `leafSegmentInfo` (gindatapage.c:70).
struct LeafSegmentInfo {
    /// `char action` — one of the `GIN_SEGMENT_*` codes.
    action: u8,
    /// `ItemPointerData *modifieditems` (`GIN_SEGMENT_ADDITEMS`).
    modifieditems: Vec<ItemPointerData>,
    /// `uint16 nmodifieditems`.
    nmodifieditems: u16,
    /// `GinPostingList *seg` — the compressed on-disk image of this segment, if
    /// present (`None` == C `NULL`).
    seg: Option<Vec<u8>>,
    /// `ItemPointer items` + `int nitems` — the decoded items, if present.
    /// `None` == C `NULL`.
    items: Option<Vec<ItemPointerData>>,
}

impl LeafSegmentInfo {
    /// `seginfo->nitems` (only valid when `items` is `Some`).
    fn nitems(&self) -> usize {
        self.items.as_ref().map_or(0, |v| v.len())
    }
}

/// `disassembledLeaf` (gindatapage.c:48).
struct DisassembledLeaf {
    /// `dlist_head segments` — the segment list.
    segments: Vec<LeafSegmentInfo>,
    /// `dlist_node *lastleft` — index of the last segment on the left page.
    lastleft: usize,
    /// `int lsize` — total size on left page.
    lsize: i32,
    /// `int rsize` — total size on right page.
    rsize: i32,
    /// `bool oldformat` — page is in pre-9.4 format on disk.
    oldformat: bool,
    /// `void *walinfo` / `int walinfolen` — WAL data from
    /// `computeLeafRecompressWALData`.
    walinfo: Vec<u8>,
}

// ===========================================================================
// bufmgr page helpers (mirroring the entry-tree callbacks).
// ===========================================================================

/// `BufferGetPage(buf)` copied out as an owned image (read-only callbacks).
fn page_bytes(buffer: Buffer) -> PgResult<Vec<u8>> {
    let mut out = Vec::new();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = page.to_vec();
        Ok(())
    })?;
    Ok(out)
}

/// `GinDataLeafPageGetFreeSpace(page)` == `PageGetExactFreeSpace(page)`
/// (ginblock.h:287).
fn gin_data_leaf_page_free_space(page: &[u8]) -> PgResult<usize> {
    let pr = backend_storage_page::PageRef::new(page)?;
    Ok(backend_storage_page::PageGetExactFreeSpace(&pr) as usize)
}

/// `GinDataLeafPageGetPostingList(page)` — a slice of the posting-list area.
fn posting_list_slice(page: &[u8]) -> &[u8] {
    let off = gin_data_leaf_page_posting_list_offset();
    let len = GinDataLeafPageGetPostingListSize(page);
    &page[off..off + len]
}

/// `GinNextPostingListSegment(seg)` — advance over one segment image.
fn size_of_segment(buf: &[u8]) -> usize {
    size_of_gin_posting_list(buf)
}

// ===========================================================================
// GinDataLeafPageGetItems (gindatapage.c:134)
// ===========================================================================

/// `GinDataLeafPageGetItems(page, &nitems, advancePast)`: read all TIDs from a
/// leaf data page to a single uncompressed ascending array.
pub fn GinDataLeafPageGetItems(page: &[u8], advance_past: ItemPointerData) -> Vec<ItemPointerData> {
    if GinPageIsCompressed(page) {
        let pl = posting_list_slice(page);
        // `seg` starts at offset 0 within `pl`; `len` is the full posting-list
        // size. Skip to the segment containing advancePast+1.
        let mut seg_off = 0usize;
        let mut len = pl.len();

        if types_tuple::heaptuple::item_pointer_is_valid(&advance_past) {
            let mut next_off = seg_off + size_of_segment(&pl[seg_off..]);
            while next_off < pl.len()
                && ginCompareItemPointers(&read_posting_list_first(&pl[next_off..]), &advance_past)
                    <= 0
            {
                seg_off = next_off;
                next_off = seg_off + size_of_segment(&pl[seg_off..]);
            }
            len = pl.len() - seg_off;
        }

        if len > 0 {
            ginPostingListDecodeAllSegments(&pl[seg_off..], len as i32, None)
        } else {
            Vec::new()
        }
    } else {
        // Pre-9.4 uncompressed page: items are the GinDataPageGetData array.
        data_leaf_page_get_uncompressed(page)
    }
}

/// `dataLeafPageGetUncompressed(page, &nitems)` (gindatapage.c:211): the
/// uncompressed item array of a pre-9.4 leaf page (`maxoff` items at
/// `GinDataPageGetData`).
fn data_leaf_page_get_uncompressed(page: &[u8]) -> Vec<ItemPointerData> {
    debug_assert!(!GinPageIsCompressed(page));
    let nitems = gin_page_get_maxoff(page) as usize;
    // GinDataPageGetData(page) == GinDataLeafPageGetPostingList(page) offset.
    let base = gin_data_leaf_page_posting_list_offset();
    let mut out = Vec::with_capacity(nitems);
    for i in 0..nitems {
        let o = base + i * SIZE_OF_ITEM_POINTER;
        out.push(crate::read_item_pointer(&page[o..]));
    }
    out
}

// ===========================================================================
// data-tree search callbacks (gindatapage.c:234..374)
// ===========================================================================

/// `dataIsMoveRight(btree, page)` (gindatapage.c:234).
fn dataIsMoveRight<'mcx>(btree: &mut GinBtreeData<'mcx>, buffer: Buffer) -> PgResult<bool> {
    let page = page_bytes(buffer)?;
    let iptr = gin_data_page_get_right_bound(&page);

    if GinPageRightMost(&page) {
        return Ok(false);
    }
    if GinPageIsDeleted(&page) {
        return Ok(true);
    }
    Ok(ginCompareItemPointers(&btree.itemptr, &iptr) > 0)
}

/// `dataLocateItem(btree, stack)` (gindatapage.c:252): binary search for the
/// correct `PostingItem` in a non-leaf data page.
fn dataLocateItem<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    stack: &mut GinBtreeStack,
) -> PgResult<BlockNumber> {
    let page = page_bytes(stack.buffer)?;
    debug_assert!(!GinPageIsLeaf(&page));
    debug_assert!(GinPageIsData(&page));

    if btree.fullScan {
        stack.off = FirstOffsetNumber;
        stack.predictNumber *= gin_page_get_maxoff(&page) as u32;
        return dataGetLeftMostPage(&page);
    }

    let mut low = FirstOffsetNumber;
    let maxoff = gin_page_get_maxoff(&page);
    let mut high = maxoff;
    debug_assert!(high >= low);
    high += 1;

    while high > low {
        let mid = low + ((high - low) / 2);
        let pitem = GinDataPageGetPostingItem(&page, mid);

        let result = if mid == maxoff {
            // Right infinity, page already correctly chosen by dataIsMoveRight.
            -1
        } else {
            ginCompareItemPointers(&btree.itemptr, &pitem.key)
        };

        if result == 0 {
            stack.off = mid;
            return Ok(PostingItemGetBlockNumber(&pitem));
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    debug_assert!(high >= FirstOffsetNumber && high <= maxoff);
    stack.off = high;
    let pitem = GinDataPageGetPostingItem(&page, high);
    Ok(PostingItemGetBlockNumber(&pitem))
}

/// `dataFindChildPtr(btree, page, blkno, storedOff)` (gindatapage.c:319).
fn dataFindChildPtr<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
    blkno: BlockNumber,
    stored_off: OffsetNumber,
) -> PgResult<OffsetNumber> {
    let page = page_bytes(buffer)?;
    let mut maxoff = gin_page_get_maxoff(&page);
    debug_assert!(!GinPageIsLeaf(&page));
    debug_assert!(GinPageIsData(&page));

    // if page isn't changed, we return storedOff.
    if stored_off >= FirstOffsetNumber && stored_off <= maxoff {
        let pitem = GinDataPageGetPostingItem(&page, stored_off);
        if PostingItemGetBlockNumber(&pitem) == blkno {
            return Ok(stored_off);
        }
        // we hope the needed pointer goes right; true if there was no deletion.
        let mut i = stored_off + 1;
        while i <= maxoff {
            let pitem = GinDataPageGetPostingItem(&page, i);
            if PostingItemGetBlockNumber(&pitem) == blkno {
                return Ok(i);
            }
            i += 1;
        }
        maxoff = stored_off - 1;
    }

    // last chance.
    let mut i = FirstOffsetNumber;
    while i <= maxoff {
        let pitem = GinDataPageGetPostingItem(&page, i);
        if PostingItemGetBlockNumber(&pitem) == blkno {
            return Ok(i);
        }
        i += 1;
    }

    Ok(InvalidOffsetNumber)
}

/// `dataGetLeftMostPage(btree, page)` (gindatapage.c:364): block number of the
/// leftmost child of a non-leaf data page.
fn dataGetLeftMostPage(page: &[u8]) -> PgResult<BlockNumber> {
    debug_assert!(!GinPageIsLeaf(page));
    debug_assert!(GinPageIsData(page));
    debug_assert!(gin_page_get_maxoff(page) >= FirstOffsetNumber);

    let pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber);
    Ok(PostingItemGetBlockNumber(&pitem))
}

/// `getLeftMostChild` vtable adapter (takes the buffer, reads its page).
fn dataGetLeftMostChild<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
) -> PgResult<BlockNumber> {
    let page = page_bytes(buffer)?;
    dataGetLeftMostPage(&page)
}

// ===========================================================================
// GinDataPageAddPostingItem / GinPageDeletePostingItem (gindatapage.c:380 / 417)
// ===========================================================================

/// `GinDataPageAddPostingItem(page, data, offset)` (gindatapage.c:380): add a
/// `PostingItem` to a non-leaf data page at `offset` (or append when
/// `offset == InvalidOffsetNumber`).
pub fn GinDataPageAddPostingItem(page: &mut [u8], data: &PostingItem, offset: OffsetNumber) {
    let mut maxoff = gin_page_get_maxoff(page);
    debug_assert!(PostingItemGetBlockNumber(data) != InvalidBlockNumber);
    debug_assert!(!GinPageIsLeaf(page));

    if offset == InvalidOffsetNumber {
        // ptr = GinDataPageGetPostingItem(page, maxoff + 1).
        let ptr = gin_data_page_posting_item_offset(maxoff + 1);
        write_posting_item(&mut page[ptr..], data);
    } else {
        let ptr = gin_data_page_posting_item_offset(offset);
        if offset != maxoff + 1 {
            // memmove(ptr + sizeof, ptr, (maxoff - offset + 1) * sizeof).
            let nmove = (maxoff - offset + 1) as usize * SIZE_OF_POSTING_ITEM;
            page.copy_within(ptr..ptr + nmove, ptr + SIZE_OF_POSTING_ITEM);
        }
        write_posting_item(&mut page[ptr..], data);
    }

    maxoff += 1;
    gin_page_set_maxoff(page, maxoff);

    // Set pd_lower to the end of the posting items.
    GinDataPageSetDataSize(page, maxoff as usize * SIZE_OF_POSTING_ITEM);
}

/// `GinPageDeletePostingItem(page, offset)` (gindatapage.c:417): delete a
/// `PostingItem` from a non-leaf data page.
pub fn GinPageDeletePostingItem(page: &mut [u8], offset: OffsetNumber) {
    let mut maxoff = gin_page_get_maxoff(page);
    debug_assert!(!GinPageIsLeaf(page));
    debug_assert!(offset >= FirstOffsetNumber && offset <= maxoff);

    if offset != maxoff {
        let dst = gin_data_page_posting_item_offset(offset);
        let src = gin_data_page_posting_item_offset(offset + 1);
        let nmove = (maxoff - offset) as usize * SIZE_OF_POSTING_ITEM;
        page.copy_within(src..src + nmove, dst);
    }

    maxoff -= 1;
    gin_page_set_maxoff(page, maxoff);
    GinDataPageSetDataSize(page, maxoff as usize * SIZE_OF_POSTING_ITEM);
}

// ===========================================================================
// data-leaf insert consumed-count side channel.
//
// In C, `dataBeginPlaceToPageLeaf` advances `items->curitem` through the shared
// `void *insertdata` pointer, and `ginInsertItemPointers` reads it back after
// `ginInsertValue` returns. The owned vtable threads `insertdata` immutably
// (`&GinInsertPayload`), so the data-leaf begin callback publishes the new
// `curitem` here for `ginInsertItemPointers` to pick up. There is at most one
// GIN data-leaf insertion in flight per backend (single-threaded inside the
// critical/insert path, exactly like C's non-reentrant XLog infra), so a single
// cell is faithful.
// ===========================================================================

use core::cell::Cell;

thread_local! {
    /// The `items->curitem` value the last data-leaf `beginPlaceToPage`
    /// produced. `ginInsertItemPointers` resets it to the pre-call value before
    /// each `ginInsertValue` and reads it afterwards.
    static DATALEAF_CURITEM: Cell<u32> = const { Cell::new(0) };
}

// ===========================================================================
// dataBeginPlaceToPageLeaf (gindatapage.c:448)
// ===========================================================================

/// `dataBeginPlaceToPageLeaf(...)` (gindatapage.c:448): prepare to insert into a
/// leaf data page, computing a split image when it doesn't fit. `is_build`
/// mirrors `btree->isBuild`; the consumed `items->curitem` is published through
/// [`DATALEAF_CURITEM`].
#[allow(clippy::too_many_arguments)]
fn dataBeginPlaceToPageLeaf(
    is_build: bool,
    buf: Buffer,
    items: &mut GinBtreeDataLeafInsertData,
) -> PgResult<BeginPlaceToPageResult> {
    // This mirrors gindatapage.c:448 exactly, with `append`/`needsplit`/
    // rebalance in one place so `is_build` is available.
    let page = page_bytes(buf)?;
    let cur = items.curitem as usize;
    let mut maxitems = (items.nitem - items.curitem) as usize;
    let rbound = gin_data_page_get_right_bound(&page);

    if !GinPageRightMost(&page) {
        let mut i = 0usize;
        while i < maxitems {
            if ginCompareItemPointers(&items.items[cur + i], &rbound) > 0 {
                debug_assert!(i > 0);
                break;
            }
            i += 1;
        }
        maxitems = i;
    }

    let mut leaf = disassembleLeaf(&page);

    let max_old_item: ItemPointerData;
    let append: bool;
    if !leaf.segments.is_empty() {
        let last = leaf.segments.len() - 1;
        if leaf.segments[last].items.is_none() {
            let decoded =
                ginPostingListDecode(leaf.segments[last].seg.as_ref().expect("seg"), None);
            leaf.segments[last].items = Some(decoded);
        }
        let last_items = leaf.segments[last].items.as_ref().unwrap();
        max_old_item = last_items[last_items.len() - 1];
        append = ginCompareItemPointers(&items.items[cur], &max_old_item) >= 0;
    } else {
        max_old_item = ItemPointerData::new(0, 0);
        append = true;
    }

    let freespace = if GinPageIsCompressed(&page) {
        gin_data_leaf_page_free_space(&page)?
    } else {
        0
    };

    if append {
        maxitems = maxitems.min(freespace + GinDataPageMaxDataSize());
    } else {
        let mut nnewsegments = freespace / GIN_POSTING_LIST_SEGMENT_MAX_SIZE;
        nnewsegments += GinDataPageMaxDataSize() / GIN_POSTING_LIST_SEGMENT_MAX_SIZE;
        maxitems = maxitems.min(nnewsegments * MIN_TUPLES_PER_SEGMENT);
    }

    let new_items_slice = items.items[cur..cur + maxitems].to_vec();
    if !addItemsToLeaf(&mut leaf, &new_items_slice) {
        items.curitem += maxitems as u32;
        DATALEAF_CURITEM.with(|c| c.set(items.curitem));
        return Ok(BeginPlaceToPageResult {
            rc: GinPlaceToPageRC::GPTP_NO_WORK,
            ptp_workspace: PtpWorkspace::default(),
            newlpage: None,
            newrpage: None,
        });
    }

    let mut remaining = ItemPointerData::new(0, InvalidOffsetNumber);
    let needsplit = leafRepackItems(&mut leaf, &mut remaining)?;

    if types_tuple::heaptuple::item_pointer_is_valid(&remaining) {
        if !append || ginCompareItemPointers(&max_old_item, &remaining) >= 0 {
            return Err(ereport(ERROR)
                .errmsg("could not split GIN page; all old items didn't fit")
                .into_error());
        }
        let mut i = 0usize;
        while i < maxitems {
            if ginCompareItemPointers(&items.items[cur + i], &remaining) >= 0 {
                break;
            }
            i += 1;
        }
        if i == 0 {
            return Err(ereport(ERROR)
                .errmsg("could not split GIN page; no new items fit")
                .into_error());
        }
        maxitems = i;
    }

    let result;
    if !needsplit {
        // Prepare WAL data describing the changes. In C this is gated on
        // `RelationNeedsWAL(btree->index) && !btree->isBuild`, which the begin
        // callback cannot recompute (it has only the index Oid). We always
        // build the (cheap) recompress WAL buffer here; `dataExecPlaceToPageLeaf`
        // registers it only when the spine's `ws.want_wal` (the relation-level
        // gate) is set. Behaviour-preserving: an unregistered buffer is dropped.
        computeLeafRecompressWALData(&mut leaf);
        result = BeginPlaceToPageResult {
            rc: GinPlaceToPageRC::GPTP_INSERT,
            ptp_workspace: PtpWorkspace {
                inner: Some(Box::new(leaf)),
                want_wal: false,
            },
            newlpage: None,
            newrpage: None,
        };
    } else {
        // Rebalance the split when not building.
        if !is_build {
            while dlist_has_prev(leaf.lastleft) {
                let lastleftinfo = &leaf.segments[leaf.lastleft];
                if lastleftinfo.action != GIN_SEGMENT_DELETE {
                    let segsize =
                        size_of_gin_posting_list(lastleftinfo.seg.as_ref().expect("seg")) as i32;
                    if (leaf.lsize - segsize) - (leaf.rsize + segsize) < 0 {
                        break;
                    }
                    if append && (leaf.lsize - segsize) < (BLCKSZ as i32 * 3) / 4 {
                        break;
                    }
                    leaf.lsize -= segsize;
                    leaf.rsize += segsize;
                }
                leaf.lastleft -= 1;
            }
        }
        debug_assert!(leaf.lsize as usize <= GinDataPageMaxDataSize());
        debug_assert!(leaf.rsize as usize <= GinDataPageMaxDataSize());

        let last = leaf.lastleft;
        if leaf.segments[last].items.is_none() {
            let decoded =
                ginPostingListDecode(leaf.segments[last].seg.as_ref().expect("seg"), None);
            leaf.segments[last].items = Some(decoded);
        }
        let lb_items = leaf.segments[last].items.as_ref().unwrap();
        let lbound = lb_items[lb_items.len() - 1];

        let mut newlpage = alloc::vec![0u8; BLCKSZ];
        let mut newrpage = alloc::vec![0u8; BLCKSZ];
        dataPlaceToPageLeafSplit(&leaf, lbound, rbound, &mut newlpage, &mut newrpage)?;

        result = BeginPlaceToPageResult {
            rc: GinPlaceToPageRC::GPTP_SPLIT,
            ptp_workspace: PtpWorkspace {
                inner: Some(Box::new(leaf)),
                want_wal: false,
            },
            newlpage: Some(newlpage),
            newrpage: Some(newrpage),
        };
    }

    items.curitem += maxitems as u32;
    DATALEAF_CURITEM.with(|c| c.set(items.curitem));
    Ok(result)
}

/// `dlist_has_prev(&segments, lastleft)` — there is a segment before `lastleft`.
#[inline]
fn dlist_has_prev(lastleft: usize) -> bool {
    lastleft > 0
}

// ===========================================================================
// dataExecPlaceToPageLeaf (gindatapage.c:716)
// ===========================================================================

/// `dataExecPlaceToPageLeaf(...)` (gindatapage.c:716): apply the recompressed
/// leaf to the page in the critical section, register WAL.
fn dataExecPlaceToPageLeaf(buf: Buffer, ws: &mut PtpWorkspace) -> PgResult<()> {
    let leaf = ws
        .inner
        .as_mut()
        .and_then(|b| b.downcast_mut::<DisassembledLeaf>())
        .expect("dataExecPlaceToPageLeaf workspace is a DisassembledLeaf");

    // Apply changes to page.
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        dataPlaceToPageLeafRecompress(page, leaf)
    })?;

    bufmgr::mark_buffer_dirty::call(buf);

    if ws.want_wal {
        xlog_register_buffer(0, buf, REGBUF_STANDARD)?;
        xlog_register_buf_data(0, &leaf.walinfo)?;
    }

    Ok(())
}

// ===========================================================================
// computeLeafRecompressWALData (gindatapage.c:872)
// ===========================================================================

/// `computeLeafRecompressWALData(leaf)` (gindatapage.c:872): construct a
/// `ginxlogRecompressDataLeaf` record into `leaf->walinfo`.
fn computeLeafRecompressWALData(leaf: &mut DisassembledLeaf) {
    // Count the modified segments.
    let nmodified = leaf
        .segments
        .iter()
        .filter(|s| s.action != GIN_SEGMENT_UNMODIFIED)
        .count();

    // walbufbegin = palloc(sizeof(ginxlogRecompressDataLeaf) + BLCKSZ +
    //               nmodified * 2). sizeof(ginxlogRecompressDataLeaf) == 2
    // (a single uint16 nactions).
    let mut walbuf: Vec<u8> = Vec::with_capacity(2 + BLCKSZ + nmodified * 2);

    // recompress_xlog->nactions = nmodified.
    walbuf.extend_from_slice(&(nmodified as u16).to_ne_bytes());

    let mut segno: u8 = 0;
    for seginfo in &leaf.segments {
        let mut action = seginfo.action;

        if action == GIN_SEGMENT_UNMODIFIED {
            segno += 1;
            continue;
        }

        let mut segsize = 0usize;
        if action != GIN_SEGMENT_DELETE {
            segsize = size_of_gin_posting_list(seginfo.seg.as_ref().expect("seg"));
        }

        // If the uncompressed added items take more space than the compressed
        // segment, store the compressed segment instead.
        if action == GIN_SEGMENT_ADDITEMS
            && seginfo.nmodifieditems as usize * SIZE_OF_ITEM_POINTER > segsize
        {
            action = GIN_SEGMENT_REPLACE;
        }

        walbuf.push(segno);
        walbuf.push(action);

        match action {
            GIN_SEGMENT_DELETE => {}
            GIN_SEGMENT_ADDITEMS => {
                walbuf.extend_from_slice(&seginfo.nmodifieditems.to_ne_bytes());
                for it in &seginfo.modifieditems {
                    push_item_pointer(&mut walbuf, it);
                }
            }
            GIN_SEGMENT_INSERT | GIN_SEGMENT_REPLACE => {
                let seg = seginfo.seg.as_ref().expect("seg");
                walbuf.extend_from_slice(&seg[..segsize]);
                // datalen = SHORTALIGN(segsize): pad to short alignment.
                let pad = shortalign(segsize) - segsize;
                for _ in 0..pad {
                    walbuf.push(0);
                }
            }
            _ => unreachable!("unexpected GIN leaf action {action}"),
        }

        if action != GIN_SEGMENT_INSERT {
            segno += 1;
        }
    }

    leaf.walinfo = walbuf;
}

/// Append a 6-byte on-disk `ItemPointerData`.
fn push_item_pointer(out: &mut Vec<u8>, it: &ItemPointerData) {
    out.extend_from_slice(&it.ip_blkid.bi_hi.to_ne_bytes());
    out.extend_from_slice(&it.ip_blkid.bi_lo.to_ne_bytes());
    out.extend_from_slice(&it.ip_posid.to_ne_bytes());
}

// ===========================================================================
// dataPlaceToPageLeafRecompress (gindatapage.c:978)
// ===========================================================================

/// `dataPlaceToPageLeafRecompress(buf, leaf)` (gindatapage.c:978): assemble a
/// disassembled leaf back into the target page bytes.
fn dataPlaceToPageLeafRecompress(page: &mut [u8], leaf: &DisassembledLeaf) -> PgResult<()> {
    let mut modified = false;

    // Convert a pre-9.4 page; force-copy all segments.
    if !GinPageIsCompressed(page) {
        debug_assert!(leaf.oldformat);
        GinPageSetCompressed(page);
        gin_page_set_maxoff(page, InvalidOffsetNumber);
        modified = true;
    }

    let base = gin_data_leaf_page_posting_list_offset();
    let mut ptr = base;
    let mut newsize = 0usize;

    for seginfo in &leaf.segments {
        if seginfo.action != GIN_SEGMENT_UNMODIFIED {
            modified = true;
        }
        if seginfo.action != GIN_SEGMENT_DELETE {
            let seg = seginfo.seg.as_ref().expect("seg");
            let segsize = size_of_gin_posting_list(seg);
            if modified {
                page[ptr..ptr + segsize].copy_from_slice(&seg[..segsize]);
            }
            ptr += segsize;
            newsize += segsize;
        }
    }

    debug_assert!(newsize <= GinDataPageMaxDataSize());
    GinDataPageSetDataSize(page, newsize);
    Ok(())
}

// ===========================================================================
// dataPlaceToPageLeafSplit (gindatapage.c:1034)
// ===========================================================================

/// `dataPlaceToPageLeafSplit(leaf, lbound, rbound, lpage, rpage)`
/// (gindatapage.c:1034): write the segments to two temp page images.
fn dataPlaceToPageLeafSplit(
    leaf: &DisassembledLeaf,
    lbound: ItemPointerData,
    rbound: ItemPointerData,
    lpage: &mut [u8],
    rpage: &mut [u8],
) -> PgResult<()> {
    use types_gin::{GIN_COMPRESSED, GIN_DATA, GIN_LEAF};

    GinInitPage(lpage, (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as u32, BLCKSZ)?;
    GinInitPage(rpage, (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as u32, BLCKSZ)?;

    // firstright = dlist_next_node(&segments, lastleft): index lastleft + 1.
    let firstright = leaf.lastleft + 1;

    // Copy segments that go to the left page (indices 0..firstright).
    let base = gin_data_leaf_page_posting_list_offset();
    let mut ptr = base;
    let mut lsize = 0usize;
    for seginfo in &leaf.segments[..firstright] {
        if seginfo.action != GIN_SEGMENT_DELETE {
            let seg = seginfo.seg.as_ref().expect("seg");
            let segsize = size_of_gin_posting_list(seg);
            lpage[ptr..ptr + segsize].copy_from_slice(&seg[..segsize]);
            ptr += segsize;
            lsize += segsize;
        }
    }
    debug_assert!(lsize as i32 == leaf.lsize);
    GinDataPageSetDataSize(lpage, lsize);
    gin_data_page_set_right_bound(lpage, &lbound);

    // Copy segments that go to the right page (indices firstright..end).
    let mut ptr = base;
    let mut rsize = 0usize;
    for seginfo in &leaf.segments[firstright..] {
        if seginfo.action != GIN_SEGMENT_DELETE {
            let seg = seginfo.seg.as_ref().expect("seg");
            let segsize = size_of_gin_posting_list(seg);
            rpage[ptr..ptr + segsize].copy_from_slice(&seg[..segsize]);
            ptr += segsize;
            rsize += segsize;
        }
    }
    debug_assert!(rsize as i32 == leaf.rsize);
    GinDataPageSetDataSize(rpage, rsize);
    gin_data_page_set_right_bound(rpage, &rbound);

    Ok(())
}

// ===========================================================================
// dataBeginPlaceToPageInternal / dataExecPlaceToPageInternal
// (gindatapage.c:1119 / 1145)
// ===========================================================================

/// `dataBeginPlaceToPageInternal(...)` (gindatapage.c:1119).
fn dataBeginPlaceToPageInternal(
    is_build: bool,
    buf: Buffer,
    stack: &GinBtreeStack,
    pitem: &PostingItem,
    updateblkno: BlockNumber,
) -> PgResult<BeginPlaceToPageResult> {
    let page = page_bytes(buf)?;

    if GinNonLeafDataPageGetFreeSpace(&page) < SIZE_OF_POSTING_ITEM {
        let (lpage, rpage) =
            dataSplitPageInternal(is_build, &page, stack, pitem, updateblkno)?;
        return Ok(BeginPlaceToPageResult {
            rc: GinPlaceToPageRC::GPTP_SPLIT,
            ptp_workspace: PtpWorkspace::default(),
            newlpage: Some(lpage),
            newrpage: Some(rpage),
        });
    }

    Ok(BeginPlaceToPageResult {
        rc: GinPlaceToPageRC::GPTP_INSERT,
        ptp_workspace: PtpWorkspace::default(),
        newlpage: None,
        newrpage: None,
    })
}

/// `dataExecPlaceToPageInternal(...)` (gindatapage.c:1145).
fn dataExecPlaceToPageInternal(
    want_wal: bool,
    buf: Buffer,
    stack: &GinBtreeStack,
    pitem: &PostingItem,
    updateblkno: BlockNumber,
) -> PgResult<()> {
    let off = stack.off;

    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        // Update existing downlink to point to next page.
        let mut existing = GinDataPageGetPostingItem(page, off);
        PostingItemSetBlockNumber(&mut existing, updateblkno);
        GinDataPageSetPostingItem(page, off, &existing);

        // Add new item.
        GinDataPageAddPostingItem(page, pitem, off);
        Ok(())
    })?;

    bufmgr::mark_buffer_dirty::call(buf);

    if want_wal {
        // ginxlogInsertDataInternal { OffsetNumber offset; PostingItem newitem; }
        // offset(2) + pad(2) so PostingItem(10) is 4-aligned? PostingItem has
        // BlockIdData(4)+ItemPointerData(6); its alignment is 2. The struct is
        // { OffsetNumber offset; PostingItem newitem; } — offset(2),
        // newitem at 2 (PostingItem is short-aligned), total 12.
        let mut data = Vec::with_capacity(2 + SIZE_OF_POSTING_ITEM);
        data.extend_from_slice(&off.to_ne_bytes());
        let mut pi = [0u8; SIZE_OF_POSTING_ITEM];
        write_posting_item(&mut pi, pitem);
        data.extend_from_slice(&pi);

        xlog_register_buffer(0, buf, REGBUF_STANDARD)?;
        xlog_register_buf_data(0, &data)?;
    }

    Ok(())
}

// ===========================================================================
// dataBeginPlaceToPage / dataExecPlaceToPage vtable callbacks
// (gindatapage.c:1201 / 1231)
// ===========================================================================

/// `dataBeginPlaceToPage(...)` (gindatapage.c:1201) — the vtable
/// `beginPlaceToPage` callback for the data tree.
fn dataBeginPlaceToPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    _mcx: Mcx<'mcx>,
    buf: Buffer,
    stack: &mut GinBtreeStack,
    insertdata: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
) -> PgResult<BeginPlaceToPageResult> {
    let page = page_bytes(buf)?;
    debug_assert!(GinPageIsData(&page));

    if GinPageIsLeaf(&page) {
        let mut items = match insertdata {
            GinInsertPayload::DataLeaf(d) => d.clone(),
            _ => unreachable!("data leaf insert payload"),
        };
        // The consumed `items.curitem` is published through DATALEAF_CURITEM for
        // `ginInsertItemPointers` (the spine threads `insertdata` immutably).
        dataBeginPlaceToPageLeaf(btree.isBuild, buf, &mut items)
    } else {
        let pitem = match insertdata {
            GinInsertPayload::DataInternal(p) => *p,
            _ => unreachable!("data internal insert payload"),
        };
        dataBeginPlaceToPageInternal(btree.isBuild, buf, stack, &pitem, updateblkno)
    }
}

/// `dataExecPlaceToPage(...)` (gindatapage.c:1231) — the vtable
/// `execPlaceToPage` callback for the data tree.
fn dataExecPlaceToPage<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    _mcx: Mcx<'mcx>,
    buf: Buffer,
    stack: &mut GinBtreeStack,
    insertdata: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
    ws: &mut PtpWorkspace,
) -> PgResult<()> {
    let page = page_bytes(buf)?;

    if GinPageIsLeaf(&page) {
        dataExecPlaceToPageLeaf(buf, ws)
    } else {
        let pitem = match insertdata {
            GinInsertPayload::DataInternal(p) => *p,
            _ => unreachable!("data internal insert payload"),
        };
        dataExecPlaceToPageInternal(ws.want_wal, buf, stack, &pitem, updateblkno)
    }
}

// ===========================================================================
// dataSplitPageInternal (gindatapage.c:1252)
// ===========================================================================

/// `dataSplitPageInternal(...)` (gindatapage.c:1252): split an internal data
/// page and insert the new item, returning two temp page images.
fn dataSplitPageInternal(
    is_build: bool,
    oldpage: &[u8],
    stack: &GinBtreeStack,
    insert_item: &PostingItem,
    updateblkno: BlockNumber,
) -> PgResult<(Vec<u8>, Vec<u8>)> {
    let off = stack.off as usize;
    let mut nitems = gin_page_get_maxoff(oldpage) as usize;
    let page_size = oldpage.len();
    let oldbound = gin_data_page_get_right_bound(oldpage);
    let oldflags = crate::gin_page_get_flags(oldpage) as u32;

    let mut lpage = alloc::vec![0u8; page_size];
    let mut rpage = alloc::vec![0u8; page_size];
    GinInitPage(&mut lpage, oldflags, page_size)?;
    GinInitPage(&mut rpage, oldflags, page_size)?;

    // Construct a new list of PostingItems = old items + new item.
    // allitems[(BLCKSZ / sizeof(PostingItem)) + 1].
    let mut allitems: Vec<PostingItem> = Vec::with_capacity(BLCKSZ / SIZE_OF_POSTING_ITEM + 1);

    // memcpy first (off - 1) items.
    for i in FirstOffsetNumber..off as OffsetNumber {
        allitems.push(GinDataPageGetPostingItem(oldpage, i));
    }
    // allitems[off - 1] = *insertdata.
    allitems.push(*insert_item);
    // memcpy &allitems[off] = items off..nitems (the remaining old items).
    for i in off as OffsetNumber..=nitems as OffsetNumber {
        allitems.push(GinDataPageGetPostingItem(oldpage, i));
    }
    nitems += 1;

    // Update existing downlink at allitems[off] to point to next page.
    PostingItemSetBlockNumber(&mut allitems[off], updateblkno);

    let separator = if is_build && GinPageRightMost(oldpage) {
        GinNonLeafDataPageGetFreeSpace(&rpage) / SIZE_OF_POSTING_ITEM
    } else {
        nitems / 2
    };
    let nleftitems = separator;
    let nrightitems = nitems - separator;

    for (i, item) in allitems[..nleftitems].iter().enumerate() {
        GinDataPageSetPostingItem(&mut lpage, (i + 1) as OffsetNumber, item);
    }
    gin_page_set_maxoff(&mut lpage, nleftitems as OffsetNumber);
    for (i, item) in allitems[separator..separator + nrightitems].iter().enumerate() {
        GinDataPageSetPostingItem(&mut rpage, (i + 1) as OffsetNumber, item);
    }
    gin_page_set_maxoff(&mut rpage, nrightitems as OffsetNumber);

    GinDataPageSetDataSize(&mut lpage, nleftitems * SIZE_OF_POSTING_ITEM);
    GinDataPageSetDataSize(&mut rpage, nrightitems * SIZE_OF_POSTING_ITEM);

    // right bound for left page = key of last left item.
    let lbound = GinDataPageGetPostingItem(&lpage, nleftitems as OffsetNumber).key;
    gin_data_page_set_right_bound(&mut lpage, &lbound);
    // right bound for right page = old bound.
    gin_data_page_set_right_bound(&mut rpage, &oldbound);

    Ok((lpage, rpage))
}

// ===========================================================================
// dataPrepareDownlink (gindatapage.c:1333)
// ===========================================================================

/// `dataPrepareDownlink(btree, lbuf)` (gindatapage.c:1333): build the downlink
/// `PostingItem` insertion payload for `lbuf`.
fn dataPrepareDownlink<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    _mcx: Mcx<'mcx>,
    lbuf: Buffer,
) -> PgResult<GinInsertPayload<'mcx>> {
    let lblkno = bufmgr::buffer_get_block_number::call(lbuf);
    let lpage = page_bytes(lbuf)?;

    let mut pitem = PostingItem {
        child_blkno: types_tuple::heaptuple::BlockIdData::new(0),
        key: gin_data_page_get_right_bound(&lpage),
    };
    PostingItemSetBlockNumber(&mut pitem, lblkno);

    Ok(GinInsertPayload::DataInternal(pitem))
}

// ===========================================================================
// ginDataFillRoot (gindatapage.c:1349)
// ===========================================================================

/// `ginDataFillRoot(btree, root, lblkno, lpage, rblkno, rpage)`
/// (gindatapage.c:1349): fill a new data root from the child right bounds.
fn ginDataFillRoot<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    root: &mut [u8],
    lblkno: BlockNumber,
    lpage: &[u8],
    rblkno: BlockNumber,
    rpage: &[u8],
) -> PgResult<()> {
    let mut li = PostingItem {
        child_blkno: types_tuple::heaptuple::BlockIdData::new(0),
        key: gin_data_page_get_right_bound(lpage),
    };
    PostingItemSetBlockNumber(&mut li, lblkno);
    GinDataPageAddPostingItem(root, &li, InvalidOffsetNumber);

    let mut ri = PostingItem {
        child_blkno: types_tuple::heaptuple::BlockIdData::new(0),
        key: gin_data_page_get_right_bound(rpage),
    };
    PostingItemSetBlockNumber(&mut ri, rblkno);
    GinDataPageAddPostingItem(root, &ri, InvalidOffsetNumber);

    Ok(())
}

// ===========================================================================
// disassembleLeaf (gindatapage.c:1369)
// ===========================================================================

/// `disassembleLeaf(page)` (gindatapage.c:1369).
fn disassembleLeaf(page: &[u8]) -> DisassembledLeaf {
    let mut leaf = DisassembledLeaf {
        segments: Vec::new(),
        lastleft: 0,
        lsize: 0,
        rsize: 0,
        oldformat: false,
        walinfo: Vec::new(),
    };

    if GinPageIsCompressed(page) {
        // One leafSegmentInfo per segment.
        let pl = posting_list_slice(page);
        let mut off = 0usize;
        while off < pl.len() {
            let segsize = size_of_segment(&pl[off..]);
            leaf.segments.push(LeafSegmentInfo {
                action: GIN_SEGMENT_UNMODIFIED,
                modifieditems: Vec::new(),
                nmodifieditems: 0,
                seg: Some(pl[off..off + segsize].to_vec()),
                items: None,
            });
            off += segsize;
        }
        leaf.oldformat = false;
    } else {
        // Pre-9.4 uncompressed page: one REPLACE segment with all items.
        let uncompressed = data_leaf_page_get_uncompressed(page);
        if !uncompressed.is_empty() {
            leaf.segments.push(LeafSegmentInfo {
                action: GIN_SEGMENT_REPLACE,
                modifieditems: Vec::new(),
                nmodifieditems: 0,
                seg: None,
                items: Some(uncompressed),
            });
        }
        leaf.oldformat = true;
    }

    leaf
}

// ===========================================================================
// addItemsToLeaf (gindatapage.c:1444)
// ===========================================================================

/// `addItemsToLeaf(leaf, newItems, nNewItems)` (gindatapage.c:1444): distribute
/// the new items into the segments, merging where they overlap. Returns true if
/// any new items were added.
fn addItemsToLeaf(leaf: &mut DisassembledLeaf, new_items: &[ItemPointerData]) -> bool {
    let n_new_items = new_items.len();
    let mut nextnew = 0usize; // index into new_items
    let mut newleft = n_new_items;
    let mut modified = false;

    // Completely empty page: one new segment with all the new items.
    if leaf.segments.is_empty() {
        leaf.segments.push(LeafSegmentInfo {
            action: GIN_SEGMENT_INSERT,
            modifieditems: Vec::new(),
            nmodifieditems: 0,
            seg: None,
            items: Some(new_items.to_vec()),
        });
        return true;
    }

    let nsegs = leaf.segments.len();
    let mut idx = 0usize;
    while idx < leaf.segments.len() {
        let is_last = idx + 1 >= leaf.segments.len();

        // How many of the new items fall into this segment?
        let nthis = if is_last {
            newleft
        } else {
            // next_first = next->items[0] or next->seg->first.
            let next_first = {
                let next = &leaf.segments[idx + 1];
                if let Some(items) = &next.items {
                    items[0]
                } else {
                    read_posting_list_first(next.seg.as_ref().expect("next seg"))
                }
            };
            let mut nthis = 0usize;
            while nthis < newleft
                && ginCompareItemPointers(&new_items[nextnew + nthis], &next_first) < 0
            {
                nthis += 1;
            }
            nthis
        };

        if nthis == 0 {
            idx += 1;
            continue;
        }

        // Decode the existing items if necessary.
        if leaf.segments[idx].items.is_none() {
            let decoded =
                ginPostingListDecode(leaf.segments[idx].seg.as_ref().expect("seg"), None);
            leaf.segments[idx].items = Some(decoded);
        }

        // Fast path: appending to the end of the page; split off a new segment
        // if the last segment would grow past the target size.
        let fast_path = {
            let cur = &leaf.segments[idx];
            let cur_items = cur.items.as_ref().unwrap();
            is_last
                && ginCompareItemPointers(&cur_items[cur_items.len() - 1], &new_items[nextnew]) < 0
                && cur.seg.is_some()
                && size_of_gin_posting_list(cur.seg.as_ref().unwrap())
                    >= GIN_POSTING_LIST_SEGMENT_TARGET_SIZE
        };
        if fast_path {
            leaf.segments.push(LeafSegmentInfo {
                action: GIN_SEGMENT_INSERT,
                modifieditems: Vec::new(),
                nmodifieditems: 0,
                seg: None,
                items: Some(new_items[nextnew..nextnew + nthis].to_vec()),
            });
            modified = true;
            break;
        }

        // Merge the new items with the existing items.
        let (tmpitems, ntmpitems) = {
            let cur_items = leaf.segments[idx].items.as_ref().unwrap();
            let mut nmerged = 0i32;
            let merged = ginMergeItemPointers(
                cur_items,
                cur_items.len() as u32,
                &new_items[nextnew..nextnew + nthis],
                nthis as u32,
                &mut nmerged,
            );
            (merged, nmerged as usize)
        };

        let cur_nitems = leaf.segments[idx].nitems();
        if ntmpitems != cur_nitems {
            // Track additions for a compact ADDITEMS WAL record, if no dups.
            if ntmpitems == nthis + cur_nitems
                && leaf.segments[idx].action == GIN_SEGMENT_UNMODIFIED
            {
                leaf.segments[idx].action = GIN_SEGMENT_ADDITEMS;
                leaf.segments[idx].modifieditems =
                    new_items[nextnew..nextnew + nthis].to_vec();
                leaf.segments[idx].nmodifieditems = nthis as u16;
            } else {
                leaf.segments[idx].action = GIN_SEGMENT_REPLACE;
            }

            leaf.segments[idx].items = Some(tmpitems);
            leaf.segments[idx].seg = None;
            modified = true;
        }

        nextnew += nthis;
        newleft -= nthis;
        if newleft == 0 {
            break;
        }

        idx += 1;
    }

    let _ = nsegs;
    modified
}

// ===========================================================================
// leafRepackItems (gindatapage.c:1571)
// ===========================================================================

/// `leafRepackItems(leaf, remaining)` (gindatapage.c:1571): recompress all
/// modified segments, splitting between pages if they don't fit. Returns true if
/// the page must be split; `*remaining` is set to the first item that didn't fit
/// (or invalid).
fn leafRepackItems(leaf: &mut DisassembledLeaf, remaining: &mut ItemPointerData) -> PgResult<bool> {
    let mut pgused = 0usize;
    let mut needsplit = false;

    // ItemPointerSetInvalid(remaining).
    *remaining = ItemPointerData::new(InvalidBlockNumber, InvalidOffsetNumber);

    // Iterate by index, allowing insertion of adjacent items.
    let mut cur = 0usize;
    while cur < leaf.segments.len() {
        // Compress the posting list, if necessary.
        if leaf.segments[cur].action != GIN_SEGMENT_DELETE {
            if leaf.segments[cur].seg.is_none() {
                let nitems = leaf.segments[cur].nitems();
                let mut npacked: i32;
                if nitems > GIN_POSTING_LIST_SEGMENT_MAX_SIZE {
                    npacked = 0; // no chance it would fit.
                } else {
                    let items = leaf.segments[cur].items.clone().expect("items");
                    let mut np = 0i32;
                    let compressed = ginCompressPostingList(
                        &items,
                        items.len() as i32,
                        GIN_POSTING_LIST_SEGMENT_MAX_SIZE as i32,
                        Some(&mut np),
                    );
                    npacked = np;
                    leaf.segments[cur].seg = Some(compressed.bytes);
                }

                if npacked as usize != leaf.segments[cur].nitems() {
                    // Too large. Compress to the target size, and create a new
                    // segment for the remaining items (inserted after cur).
                    let items = leaf.segments[cur].items.clone().expect("items");
                    let mut np = 0i32;
                    let compressed = ginCompressPostingList(
                        &items,
                        items.len() as i32,
                        GIN_POSTING_LIST_SEGMENT_TARGET_SIZE as i32,
                        Some(&mut np),
                    );
                    npacked = np;
                    leaf.segments[cur].seg = Some(compressed.bytes);
                    if leaf.segments[cur].action != GIN_SEGMENT_INSERT {
                        leaf.segments[cur].action = GIN_SEGMENT_REPLACE;
                    }

                    let rest = items[npacked as usize..].to_vec();
                    let newseg = LeafSegmentInfo {
                        action: GIN_SEGMENT_INSERT,
                        modifieditems: Vec::new(),
                        nmodifieditems: 0,
                        seg: None,
                        items: Some(rest),
                    };
                    leaf.segments.insert(cur + 1, newseg);
                }
            }

            // If the segment is very small, merge it with the next segment.
            let has_next = cur + 1 < leaf.segments.len();
            if has_next
                && size_of_gin_posting_list(leaf.segments[cur].seg.as_ref().expect("seg"))
                    < GIN_POSTING_LIST_SEGMENT_MIN_SIZE
            {
                if leaf.segments[cur].items.is_none() {
                    let decoded =
                        ginPostingListDecode(leaf.segments[cur].seg.as_ref().expect("seg"), None);
                    leaf.segments[cur].items = Some(decoded);
                }
                if leaf.segments[cur + 1].items.is_none() {
                    let decoded = ginPostingListDecode(
                        leaf.segments[cur + 1].seg.as_ref().expect("next seg"),
                        None,
                    );
                    leaf.segments[cur + 1].items = Some(decoded);
                }

                let cur_items = leaf.segments[cur].items.clone().unwrap();
                let next_items = leaf.segments[cur + 1].items.clone().unwrap();
                let mut nmerged = 0i32;
                let merged = ginMergeItemPointers(
                    &cur_items,
                    cur_items.len() as u32,
                    &next_items,
                    next_items.len() as u32,
                    &mut nmerged,
                );
                debug_assert!(nmerged as usize == cur_items.len() + next_items.len());
                leaf.segments[cur + 1].items = Some(merged);
                leaf.segments[cur + 1].seg = None;
                leaf.segments[cur + 1].action = GIN_SEGMENT_REPLACE;
                leaf.segments[cur + 1].modifieditems = Vec::new();
                leaf.segments[cur + 1].nmodifieditems = 0;

                if leaf.segments[cur].action == GIN_SEGMENT_INSERT {
                    leaf.segments.remove(cur);
                    // `continue` without advancing: the next segment shifted to
                    // `cur`. In C, cur_node = next_node here; the merged segment
                    // is reprocessed.
                    continue;
                } else {
                    leaf.segments[cur].action = GIN_SEGMENT_DELETE;
                    leaf.segments[cur].seg = None;
                }
            }

            leaf.segments[cur].items = None;
        }

        if leaf.segments[cur].action == GIN_SEGMENT_DELETE {
            cur += 1;
            continue;
        }

        // Did we exceed the size that fits on one page?
        let segsize = size_of_gin_posting_list(leaf.segments[cur].seg.as_ref().expect("seg"));
        if pgused + segsize > GinDataPageMaxDataSize() {
            if !needsplit {
                // switch to right page.
                debug_assert!(pgused > 0);
                leaf.lastleft = cur - 1;
                needsplit = true;
                leaf.lsize = pgused as i32;
                pgused = 0;
            } else {
                // Filled both pages; the last constructed segment did not fit.
                *remaining = read_posting_list_first(leaf.segments[cur].seg.as_ref().expect("seg"));
                // Remove all segments that did not fit (from cur to end).
                leaf.segments.truncate(cur);
                break;
            }
        }

        pgused += segsize;
        cur += 1;
    }

    if !needsplit {
        leaf.lsize = pgused as i32;
        leaf.rsize = 0;
    } else {
        leaf.rsize = pgused as i32;
    }

    debug_assert!(leaf.lsize as usize <= GinDataPageMaxDataSize());
    debug_assert!(leaf.rsize as usize <= GinDataPageMaxDataSize());

    // Make a copy of every segment after the first modified one. (Our segments
    // are already owned `Vec<u8>` copies, never aliasing the page, so this
    // copy-on-write step is a no-op for correctness — kept as a comment marker
    // to mirror gindatapage.c:1736.)

    Ok(needsplit)
}

// ===========================================================================
// ginVacuumPostingTreeLeaf (gindatapage.c:738) — sanctioned panic leg.
// ===========================================================================

/// `ginVacuumPostingTreeLeaf(indexrel, buffer, gvs)` (gindatapage.c:738).
///
/// Sanctioned panic leg: this needs the `ginvacuum.c` `GinVacuumState` /
/// `ginVacuumItemPointers` machinery (the GIN vacuum prefetch / read_stream
/// path), which is not yet ported. It is unreachable until `ginvacuum.c` lands.
pub fn ginVacuumPostingTreeLeaf<'mcx>(_indexrel: &Relation<'mcx>, _buffer: Buffer) -> PgResult<()> {
    panic!(
        "ginVacuumPostingTreeLeaf: ginvacuum.c (GinVacuumState / ginVacuumItemPointers) \
         is not yet ported (sanctioned GIN vacuum panic leg)"
    )
}

// ===========================================================================
// createPostingTree (gindatapage.c:1775)
// ===========================================================================

/// `createPostingTree(index, items, nitems, buildStats, entrybuffer)`
/// (gindatapage.c:1775): create a new posting tree containing `items` (sorted,
/// no duplicates). Returns the root block number.
pub fn createPostingTree<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    items: &[ItemPointerData],
    mut build_stats: Option<&mut GinStatsData>,
    entrybuffer: Buffer,
) -> PgResult<BlockNumber> {
    use types_gin::{GIN_COMPRESSED, GIN_DATA, GIN_LEAF};

    let nitems = items.len();
    let is_build = build_stats.is_some();

    // Construct the new root page in memory first.
    let mut tmppage = alloc::vec![0u8; BLCKSZ];
    GinInitPage(&mut tmppage, (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as u32, BLCKSZ)?;
    gin_page_set_rightlink(&mut tmppage, InvalidBlockNumber);

    // Write as many items to the root page as fit, in max-size segments.
    let mut nrootitems = 0usize;
    let mut rootsize = 0usize;
    let base = gin_data_leaf_page_posting_list_offset();
    let mut ptr = base;
    while nrootitems < nitems {
        let mut npacked = 0i32;
        let segment = ginCompressPostingList(
            &items[nrootitems..],
            (nitems - nrootitems) as i32,
            GIN_POSTING_LIST_SEGMENT_MAX_SIZE as i32,
            Some(&mut npacked),
        );
        let segsize = segment.size();
        if rootsize + segsize > GinDataPageMaxDataSize() {
            break;
        }
        tmppage[ptr..ptr + segsize].copy_from_slice(&segment.bytes[..segsize]);
        ptr += segsize;
        rootsize += segsize;
        nrootitems += npacked as usize;
    }
    GinDataPageSetDataSize(&mut tmppage, rootsize);

    // Get a new physical page and copy the in-memory page to it.
    let buffer = gin_new_buffer(index)?;
    let blkno = bufmgr::buffer_get_block_number::call(buffer);

    // Copy predicate locks from the entry tree leaf to the posting tree.
    let entry_blkno = bufmgr::buffer_get_block_number::call(entrybuffer);
    predicate_lock_page_split(index.rd_id, entry_blkno, blkno)?;

    // PageRestoreTempPage(tmppage, page): copy our temp image into the buffer.
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        page.copy_from_slice(&tmppage);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(buffer);

    if relation_needs_wal(index) && !is_build {
        // ginxlogCreatePostingTree { uint32 size; } then the posting-list bytes.
        xlog_begin_insert()?;
        let data = (rootsize as u32).to_ne_bytes();
        xlog_register_data(&data)?;
        // The posting-list bytes from the now-written page.
        xlog_register_data(&tmppage[base..base + rootsize])?;
        xlog_register_buffer(0, buffer, REGBUF_WILL_INIT)?;

        let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_CREATE_PTREE)?;
        bufmgr::page_set_lsn::call(buffer, recptr)?;
    }

    bufmgr::unlock_release_buffer::call(buffer);

    // During index build, count the newly-added data page.
    if let Some(stats) = build_stats.as_deref_mut() {
        stats.nDataPages += 1;
    }

    // Add any remaining TIDs to the newly-created posting tree.
    if nitems > nrootitems {
        ginInsertItemPointers(mcx, index, blkno, &items[nrootitems..], build_stats)?;
    }

    Ok(blkno)
}

// ===========================================================================
// ginPrepareDataScan (gindatapage.c:1882)
// ===========================================================================

/// `ginPrepareDataScan(btree, index, rootBlkno)` (gindatapage.c:1882): set up a
/// [`GinBtreeData`] for data (posting tree) access, installing the data-tree
/// method table into the vtable.
pub fn ginPrepareDataScan<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    index: &Relation<'mcx>,
    root_blkno: BlockNumber,
) {
    // memset(btree, 0, sizeof(GinBtreeData)).
    *btree = GinBtreeData::default();

    btree.index = index.rd_id;
    btree.rootBlkno = root_blkno;

    btree.findChildPage = Some(dataLocateItem);
    btree.getLeftMostChild = Some(dataGetLeftMostChild);
    btree.isMoveRight = Some(dataIsMoveRight);
    btree.findItem = None;
    btree.findChildPtr = Some(dataFindChildPtr);
    btree.beginPlaceToPage = Some(dataBeginPlaceToPage);
    btree.execPlaceToPage = Some(dataExecPlaceToPage);
    btree.fillRoot = Some(ginDataFillRoot);
    btree.prepareDownlink = Some(dataPrepareDownlink);

    btree.isData = true;
    btree.fullScan = false;
    btree.isBuild = false;
}

// ===========================================================================
// ginInsertItemPointers (gindatapage.c:1908)
// ===========================================================================

/// `ginInsertItemPointers(index, rootBlkno, items, nitem, buildStats)`
/// (gindatapage.c:1908): insert an array of item pointers into the posting tree,
/// possibly executing several tree scans.
pub fn ginInsertItemPointers<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    root_blkno: BlockNumber,
    items: &[ItemPointerData],
    mut build_stats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    let mut btree = GinBtreeData::default();
    ginPrepareDataScan(&mut btree, index, root_blkno);
    btree.isBuild = build_stats.is_some();

    let mut insertdata = GinBtreeDataLeafInsertData {
        items: items.to_vec(),
        nitem: items.len() as u32,
        curitem: 0,
    };

    while insertdata.curitem < insertdata.nitem {
        // search for the leaf page where the first item should go.
        btree.itemptr = insertdata.items[insertdata.curitem as usize];
        let stack = backend_access_gin_ginbtree::ginFindLeafPage(
            &mut btree, mcx, false, true, index,
        )?;

        // The data-leaf `beginPlaceToPage` (reached through the spine's vtable
        // dispatch) advances `items->curitem` by however many it placed and
        // publishes the new value through DATALEAF_CURITEM. Seed the cell with
        // the current value first, in case the placement is GPTP_NO_WORK on a
        // path that doesn't reach the leaf begin (defensive; the spine always
        // calls beginPlaceToPage on the leaf here).
        DATALEAF_CURITEM.with(|c| c.set(insertdata.curitem));
        let payload = GinInsertPayload::DataLeaf(insertdata.clone());
        backend_access_gin_ginbtree::ginInsertValue(
            &mut btree,
            mcx,
            stack,
            &payload,
            build_stats.as_deref_mut(),
            index,
        )?;
        // Recover the consumed `curitem` produced by the data-leaf begin call.
        insertdata.curitem = DATALEAF_CURITEM.with(|c| c.get());
    }

    Ok(())
}

// ===========================================================================
// ginScanBeginPostingTree (gindatapage.c:1936)
// ===========================================================================

/// `ginScanBeginPostingTree(btree, index, rootBlkno)` (gindatapage.c:1936):
/// start a new full scan on a posting tree.
pub fn ginScanBeginPostingTree<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    root_blkno: BlockNumber,
) -> PgResult<Box<GinBtreeStack>> {
    ginPrepareDataScan(btree, index, root_blkno);
    btree.fullScan = true;
    backend_access_gin_ginbtree::ginFindLeafPage(btree, mcx, true, false, index)
}

// ===========================================================================
// seam pass-throughs (mirroring the ginbtree helpers).
// ===========================================================================

fn gin_new_buffer<'mcx>(index: &Relation<'mcx>) -> PgResult<Buffer> {
    backend_access_gin_ginutil_seams::gin_new_buffer::call(index)
}

fn relation_needs_wal(index: &Relation<'_>) -> bool {
    backend_utils_cache_relcache_seams::relation_needs_wal::call(index)
}

fn predicate_lock_page_split(
    index_oid: types_core::Oid,
    old_blkno: BlockNumber,
    new_blkno: BlockNumber,
) -> PgResult<()> {
    backend_storage_lmgr_predicate_seams::predicate_lock_page_split::call(
        index_oid, old_blkno, new_blkno,
    )
}

fn xlog_begin_insert() -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_begin_insert::call()
}

fn xlog_register_data(data: &[u8]) -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_register_data::call(data)
}

fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_register_buffer::call(block_id, buffer, flags)
}

fn xlog_register_buf_data(block_id: u8, data: &[u8]) -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_register_buf_data::call(block_id, data)
}

fn xlog_insert_record(rmid: types_core::RmgrId, info: u8) -> PgResult<types_core::XLogRecPtr> {
    backend_access_transam_xloginsert_seams::xlog_insert_record::call(rmid, info)
}
