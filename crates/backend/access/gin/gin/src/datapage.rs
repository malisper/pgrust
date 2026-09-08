//! gindatapage.c: posting-tree pages. Pre-9.4 uncompressed leaves (a
//! pg_upgrade lineage) are read as C does and converted to the compressed
//! format on their first modification.

use ::bufmgr_seams as bm;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use init_small::globals::{EndCriticalSection, StartCriticalSection};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageRef, PageTemp};
use ::types_tuple::itemptr::{FirstOffsetNumber, InvalidOffsetNumber, ItemPointerData};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_WILL_INIT};

use crate::btree::{Frame, GinBt, GinPlace, GinStack};
use crate::postinglist::{
    ginCompressPostingList, ginMergeItemPointers, ginPostingListDecodeAllSegments, seg_first,
    seg_size, validate_posting_list_segments,
};
use crate::util::{gin_init_page_bytes, GinNewBuffer};
use crate::{
    opaque_of, page_bytes, page_mut, page_opaque, page_ref, relation_needs_wal,
    vec_append, write_opaque_to, GinPageIsCompressed, GinPageIsDeleted, GinPageIsLeaf,
    GinPageRightMost, RM_GIN,
};

pub(crate) const GinPostingListSegmentMaxSize: usize = 384;
pub(crate) const GinPostingListSegmentTargetSize: usize = 256;
pub(crate) const GinPostingListSegmentMinSize: usize = 128;
const MinTuplesPerSegment: usize = (GinPostingListSegmentMaxSize - 2) / 6;


#[inline]
pub fn data_page_right_bound(bytes: &[u8]) -> ItemPointerData {
    // SAFETY: right bound at PageGetContents (offset 24) of a BLCKSZ image.
    unsafe { bytes.as_ptr().add(24).cast::<ItemPointerData>().read_unaligned() }
}

#[inline]
pub(crate) fn set_data_page_right_bound(bytes: &mut [u8], bound: &ItemPointerData) {
    // SAFETY: as data_page_right_bound.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(24)
            .cast::<ItemPointerData>()
            .write_unaligned(*bound)
    }
}

#[inline]
pub(crate) fn data_leaf_posting_list_size(bytes: &[u8]) -> usize {
    let pd_lower = u16::from_ne_bytes([bytes[12], bytes[13]]) as usize;
    // pd_lower comes from disk; a crafted value below GinDataPageDataOffset would
    // wrap this usize subtraction. saturating_sub keeps size==0 (empty) callers
    // safe; the read paths use data_leaf_posting_list_checked for a typed error.
    pd_lower.saturating_sub(GinDataPageDataOffset)
}

#[cold]
#[inline(never)]
fn corrupt_pd_lower(pd_lower: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "corrupted GIN posting-tree leaf: pd_lower {pd_lower} out of range \
             [{GinDataPageDataOffset}, {}]",
            GinDataPageDataOffset + GinDataPageMaxDataSize
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

/// Bounds-checked GinDataLeafPageGetItems posting-list accessor. Validates the
/// on-disk pd_lower and the segment chain before any raw segment walk; returns
/// a typed data-corruption error rather than reading past the page image.
#[inline]
pub(crate) fn data_leaf_posting_list_checked(bytes: &[u8]) -> PgResult<&[u8]> {
    let pd_lower = u16::from_ne_bytes([bytes[12], bytes[13]]) as usize;
    if pd_lower < GinDataPageDataOffset || pd_lower > GinDataPageDataOffset + GinDataPageMaxDataSize {
        return Err(corrupt_pd_lower(pd_lower));
    }
    let all = &bytes[GinDataPageDataOffset..pd_lower];
    validate_posting_list_segments(all)?;
    Ok(all)
}

/// GinDataPageSetDataSize.
#[inline]
pub(crate) fn set_data_page_data_size(bytes: &mut [u8], size: usize) {
    debug_assert!(size <= GinDataPageMaxDataSize);
    let lower = (size + GinDataPageDataOffset) as u16;
    bytes[12..14].copy_from_slice(&lower.to_ne_bytes());
}

#[inline]
pub fn posting_item_at(bytes: &[u8], off: OffsetNumber) -> PostingItem {
    let p = GinDataPageDataOffset + (off as usize - 1) * 10;
    // SAFETY: PostingItem is 10-byte POD within the image (caller bounds).
    unsafe { bytes.as_ptr().add(p).cast::<PostingItem>().read_unaligned() }
}

#[inline]
fn write_posting_item(bytes: &mut [u8], off: OffsetNumber, item: &PostingItem) {
    let p = GinDataPageDataOffset + (off as usize - 1) * 10;
    // SAFETY: as posting_item_at; exclusive access.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(p)
            .cast::<PostingItem>()
            .write_unaligned(*item)
    }
}

/// Maximum posting items a non-leaf (internal) posting-tree data page can hold.
/// maxoff is read straight from the on-disk page opaque and drives raw-pointer
/// PostingItem access (posting_item_at/write_posting_item); a legitimate page
/// never declares more than this many items (GinDataPageMaxDataSize is the byte
/// budget for the item array, each PostingItem is 10 bytes).
pub(crate) const GinMaxNonLeafDataItems: usize =
    GinDataPageMaxDataSize / core::mem::size_of::<PostingItem>();

#[cold]
#[inline(never)]
fn corrupt_maxoff(maxoff: OffsetNumber) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "corrupted GIN posting-tree internal page: maxoff {maxoff} exceeds \
             maximum {GinMaxNonLeafDataItems} posting items per page"
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

/// Validated GinPageGetOpaque(page)->maxoff for a non-leaf data page. maxoff is
/// attacker-controlled on a crafted/corrupt page and is used as a raw-pointer
/// bound for PostingItem reads and writes; reject any value that would push
/// posting_item_at/write_posting_item past the BLCKSZ page image with a typed
/// data-corruption error instead of accessing out of bounds.
#[inline]
pub(crate) fn nonleaf_maxoff_checked(bytes: &[u8]) -> PgResult<OffsetNumber> {
    let maxoff = opaque_of(bytes).maxoff;
    if maxoff as usize > GinMaxNonLeafDataItems {
        return Err(corrupt_maxoff(maxoff));
    }
    Ok(maxoff)
}

/// GinNonLeafDataPageGetFreeSpace. maxoff is disk-derived; use saturating
/// arithmetic so an oversized (corrupt) maxoff yields zero free space, forcing
/// the split/error path instead of wrapping in release and passing the "fits"
/// guard on a corrupt page.
#[inline]
fn nonleaf_free_space(bytes: &[u8]) -> usize {
    GinDataPageMaxDataSize.saturating_sub(opaque_of(bytes).maxoff as usize * 10)
}

/// GinDataPageAddPostingItem over a raw image.
pub(crate) fn gin_data_page_add_posting_item(
    bytes: &mut [u8],
    data: &PostingItem,
    offset: OffsetNumber,
) {
    let mut opaque = opaque_of(bytes);
    let maxoff = opaque.maxoff;
    debug_assert!(PostingItemGetBlockNumber(data) != InvalidBlockNumber);
    debug_assert!(!GinPageIsLeaf(&opaque));

    if offset == InvalidOffsetNumber || offset == maxoff + 1 {
        write_posting_item(bytes, maxoff + 1, data);
    } else {
        let start = GinDataPageDataOffset + (offset as usize - 1) * 10;
        let n = (maxoff - offset + 1) as usize * 10;
        bytes.copy_within(start..start + n, start + 10);
        write_posting_item(bytes, offset, data);
    }
    opaque.maxoff = maxoff + 1;
    write_opaque_to(bytes, &opaque);
    set_data_page_data_size(bytes, opaque.maxoff as usize * 10);
}

/// GinPageDeletePostingItem over a raw image.
pub(crate) fn gin_page_delete_posting_item(bytes: &mut [u8], offset: OffsetNumber) {
    let mut opaque = opaque_of(bytes);
    let maxoff = opaque.maxoff;
    debug_assert!(!GinPageIsLeaf(&opaque));
    debug_assert!(offset >= FirstOffsetNumber && offset <= maxoff);

    if offset != maxoff {
        let dst = GinDataPageDataOffset + (offset as usize - 1) * 10;
        let src = dst + 10;
        let n = (maxoff - offset) as usize * 10;
        bytes.copy_within(src..src + n, dst);
    }
    opaque.maxoff = maxoff - 1;
    write_opaque_to(bytes, &opaque);
    set_data_page_data_size(bytes, opaque.maxoff as usize * 10);
}

/// dataLeafPageGetUncompressed (gindatapage.c:211-224): on a pre-9.4 format
/// leaf the whole page content is the raw ItemPointerData array and the item
/// count is the opaque's maxoff. Appends the array to `out`. maxoff comes from
/// disk: an array that would run past the data area is corruption (C reads it
/// unchecked).
#[cold]
#[inline(never)]
fn data_leaf_page_get_uncompressed(
    bytes: &[u8],
    out: &mut PgVec<'_, ItemPointerData>,
) -> PgResult<()> {
    debug_assert!(!GinPageIsCompressed(&opaque_of(bytes)));
    let nitems = opaque_of(bytes).maxoff as usize;
    if nitems * 6 > GinDataPageMaxDataSize {
        return Err(Box::new(
            PgError::error(format!(
                "corrupted GIN posting-tree leaf: {nitems} uncompressed items exceed the page data area"
            ))
            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
        ));
    }
    let mcx = *out.allocator();
    out.try_reserve(nitems).map_err(|_| mcx.oom(nitems * 6))?;
    let mut at = GinDataPageDataOffset;
    for _ in 0..nitems {
        let hi = u16::from_ne_bytes([bytes[at], bytes[at + 1]]);
        let lo = u16::from_ne_bytes([bytes[at + 2], bytes[at + 3]]);
        let posid = u16::from_ne_bytes([bytes[at + 4], bytes[at + 5]]);
        out.push(ItemPointerData::new(((hi as u32) << 16) | lo as u32, posid));
        at += 6;
    }
    Ok(())
}

/// GinDataLeafPageGetItems: append page TIDs (segments past advancePast).
pub fn gin_data_leaf_page_get_items(
    bytes: &[u8],
    advance_past: &ItemPointerData,
    out: &mut PgVec<'_, ItemPointerData>,
) -> PgResult<()> {
    if !GinPageIsCompressed(&opaque_of(bytes)) {
        // gindatapage.c:167-173: the whole uncompressed array (advancePast
        // only skips compressed segments).
        return data_leaf_page_get_uncompressed(bytes, out);
    }
    let all = data_leaf_posting_list_checked(bytes)?;
    let mut off = 0usize;
    if gin_item_pointer_offset(advance_past) != 0 || gin_item_pointer_block(advance_past) != 0 {
        // Skip to the segment containing advancePast+1.
        let mut next = if all.is_empty() { 0 } else { seg_size(all) };
        while next < all.len() {
            let seg = &all[next..];
            if ginCompareItemPointers(&seg_first(seg), advance_past) <= 0 {
                off = next;
                next += seg_size(seg);
            } else {
                break;
            }
        }
    }
    ginPostingListDecodeAllSegments(&all[off..], out)
}

/// GinDataLeafPageGetItemsToTbm.
pub(crate) fn gin_data_leaf_page_get_items_to_tbm(
    mcx: Mcx<'_>,
    bytes: &[u8],
    tbm: &mut ::tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    if !GinPageIsCompressed(&opaque_of(bytes)) {
        // gindatapage.c:195-199.
        let mut uncompressed = mcx::vec_new_in(mcx);
        data_leaf_page_get_uncompressed(bytes, &mut uncompressed)?;
        if !uncompressed.is_empty() {
            tbm.add_tuples(uncompressed.as_slice(), false)?;
        }
        return Ok(uncompressed.len() as i64);
    }
    crate::postinglist::ginPostingListDecodeAllSegmentsToTbm(
        mcx,
        data_leaf_posting_list_checked(bytes)?,
        tbm,
    )
}


#[derive(Clone, Copy)]
enum SegBytes {
    /// Unmodified on-page segment (pin+lock held across begin→exec).
    Page(*const u8, usize),
    /// Scratch-owned image.
    Owned(*const u8, usize),
}

impl SegBytes {
    #[inline]
    fn as_slice<'x>(&self) -> &'x [u8] {
        // SAFETY: Page variant rides the buffer pin/lock; Owned rides the
        // scratch context — both live across the leaf operation.
        unsafe {
            match *self {
                SegBytes::Page(p, n) | SegBytes::Owned(p, n) => core::slice::from_raw_parts(p, n),
            }
        }
    }
    #[inline]
    fn len(&self) -> usize {
        match *self {
            SegBytes::Page(_, n) | SegBytes::Owned(_, n) => n,
        }
    }
}

enum SegItems {
    /// Slice of the incoming new-items array: (start, len).
    New(usize, usize),
    /// Scratch-owned array (leaked into scratch: bulk-freed).
    Owned(*const ItemPointerData, usize),
}

struct SegInfo {
    action: u8,
    seg: Option<SegBytes>,
    items: Option<SegItems>,
    moditems: Option<(usize, usize)>,
}

pub(crate) struct DisassembledLeaf {
    segs: Vec<SegInfo>,
    lastleft: usize,
    lsize: usize,
    rsize: usize,
    walinfo: Vec<u8>,
    /// Page is in pre-9.4 format on disk.
    oldformat: bool,
}

fn owned_seg<'s>(mcx: Mcx<'s>, bytes: PgVec<'s, u8>) -> SegBytes {
    let n = bytes.len();
    let p = bytes.as_ptr();
    core::mem::forget(bytes);
    let _ = mcx;
    SegBytes::Owned(p, n)
}

fn owned_items<'s>(mcx: Mcx<'s>, items: PgVec<'s, ItemPointerData>) -> SegItems {
    let n = items.len();
    let p = items.as_ptr();
    core::mem::forget(items);
    let _ = mcx;
    SegItems::Owned(p, n)
}

fn items_slice<'x>(si: &SegItems, new_items: &'x [ItemPointerData]) -> &'x [ItemPointerData] {
    match *si {
        SegItems::New(s, n) => &new_items[s..s + n],
        // SAFETY: scratch-owned array, live for the leaf operation.
        SegItems::Owned(p, n) => unsafe { core::slice::from_raw_parts(p, n) },
    }
}

/// disassembleLeaf.
fn disassemble_leaf<'s>(mcx: Mcx<'s>, bytes: &[u8]) -> PgResult<DisassembledLeaf> {
    if !GinPageIsCompressed(&opaque_of(bytes)) {
        // gindatapage.c:1413-1428: a pre-9.4 uncompressed page is one REPLACE
        // segment carrying the item array; an empty one has no segments.
        let mut uncompressed = mcx::vec_new_in(mcx);
        data_leaf_page_get_uncompressed(bytes, &mut uncompressed)?;
        let mut segs = Vec::new();
        if !uncompressed.is_empty() {
            segs.push(SegInfo {
                action: GIN_SEGMENT_REPLACE,
                seg: None,
                items: Some(owned_items(mcx, uncompressed)),
                moditems: None,
            });
        }
        return Ok(DisassembledLeaf {
            segs,
            lastleft: 0,
            lsize: 0,
            rsize: 0,
            walinfo: Vec::new(),
            oldformat: true,
        });
    }
    // Bounds-check pd_lower and the whole segment chain before recording any
    // (ptr, seg_size) extent: a crafted segment size would otherwise drive an
    // out-of-page from_raw_parts read in SegBytes::as_slice.
    let all = data_leaf_posting_list_checked(bytes)?;
    let mut segs = Vec::new();
    let mut off = 0usize;
    while off < all.len() {
        let seg = &all[off..];
        let n = seg_size(seg);
        segs.push(SegInfo {
            action: GIN_SEGMENT_UNMODIFIED,
            seg: Some(SegBytes::Page(seg.as_ptr(), n)),
            items: None,
            moditems: None,
        });
        off += n;
    }
    Ok(DisassembledLeaf {
        segs,
        lastleft: 0,
        lsize: 0,
        rsize: 0,
        walinfo: Vec::new(),
        oldformat: false,
    })
}

fn decode_seg<'s>(mcx: Mcx<'s>, seg: &SegBytes) -> PgResult<SegItems> {
    let mut v = mcx::vec_new_in(mcx);
    ginPostingListDecodeAllSegments(seg.as_slice(), &mut v)?;
    Ok(owned_items(mcx, v))
}

/// addItemsToLeaf. Returns false when every new item was a duplicate.
fn add_items_to_leaf<'s>(
    mcx: Mcx<'s>,
    leaf: &mut DisassembledLeaf,
    new_items: &[ItemPointerData],
    new_start: usize,
    n_new: usize,
) -> PgResult<bool> {
    if leaf.segs.is_empty() {
        leaf.segs.push(SegInfo {
            action: GIN_SEGMENT_INSERT,
            seg: None,
            items: Some(SegItems::New(new_start, n_new)),
            moditems: None,
        });
        return Ok(true);
    }

    let mut modified = false;
    let mut nextnew = new_start;
    let mut newleft = n_new;

    let mut i = 0usize;
    while i < leaf.segs.len() {
        let has_next = i + 1 < leaf.segs.len();
        let nthis = if !has_next {
            newleft
        } else {
            let next_first = {
                let next = &leaf.segs[i + 1];
                if let Some(items) = &next.items {
                    items_slice(items, new_items)[0]
                } else {
                    seg_first(next.seg.as_ref().expect("segment bytes").as_slice())
                }
            };
            let mut n = 0usize;
            while n < newleft
                && ginCompareItemPointers(&new_items[nextnew + n], &next_first) < 0
            {
                n += 1;
            }
            n
        };
        if nthis == 0 {
            i += 1;
            continue;
        }

        if leaf.segs[i].items.is_none() {
            let seg = *leaf.segs[i].seg.as_ref().expect("segment bytes");
            leaf.segs[i].items = Some(decode_seg(mcx, &seg)?);
        }

        // Append fast path: start a fresh segment rather than growing the
        // last one past the target size.
        let cur_items_last = {
            let items = items_slice(leaf.segs[i].items.as_ref().unwrap(), new_items);
            items[items.len() - 1]
        };
        if !has_next
            && ginCompareItemPointers(&cur_items_last, &new_items[nextnew]) < 0
            && leaf.segs[i]
                .seg
                .as_ref()
                .is_some_and(|s| s.len() >= GinPostingListSegmentTargetSize)
        {
            leaf.segs.push(SegInfo {
                action: GIN_SEGMENT_INSERT,
                seg: None,
                items: Some(SegItems::New(nextnew, nthis)),
                moditems: None,
            });
            modified = true;
            break;
        }

        let (merged, merged_is_pure_add) = {
            let cur = &leaf.segs[i];
            let old = items_slice(cur.items.as_ref().unwrap(), new_items);
            let merged =
                ginMergeItemPointers(mcx, old, &new_items[nextnew..nextnew + nthis])?;
            let pure = merged.len() == old.len() + nthis;
            (merged, pure)
        };
        let old_len = items_slice(leaf.segs[i].items.as_ref().unwrap(), new_items).len();
        if merged.len() != old_len {
            let cur = &mut leaf.segs[i];
            if merged_is_pure_add && cur.action == GIN_SEGMENT_UNMODIFIED {
                cur.action = GIN_SEGMENT_ADDITEMS;
                cur.moditems = Some((nextnew, nthis));
            } else {
                cur.action = GIN_SEGMENT_REPLACE;
            }
            cur.items = Some(owned_items(mcx, merged));
            cur.seg = None;
            modified = true;
        }

        nextnew += nthis;
        newleft -= nthis;
        if newleft == 0 {
            break;
        }
        i += 1;
    }

    Ok(modified)
}

/// leafRepackItems. Returns (needsplit, remaining-first-item-if-overflow).
fn leaf_repack_items<'s>(
    mcx: Mcx<'s>,
    leaf: &mut DisassembledLeaf,
    new_items: &[ItemPointerData],
) -> PgResult<(bool, ItemPointerData)> {
    let mut pgused = 0usize;
    let mut needsplit = false;
    let mut remaining = ItemPointerData::invalid();

    let mut i = 0usize;
    while i < leaf.segs.len() {
        if leaf.segs[i].action != GIN_SEGMENT_DELETE {
            if leaf.segs[i].seg.is_none() {
                let items_len =
                    items_slice(leaf.segs[i].items.as_ref().unwrap(), new_items).len();
                let mut npacked = 0usize;
                // C: nitems > GinPostingListSegmentMaxSize has no chance to
                // fit (each item is at least one byte).
                if items_len <= GinPostingListSegmentMaxSize {
                    let items = items_slice(leaf.segs[i].items.as_ref().unwrap(), new_items);
                    let (img, n) =
                        ginCompressPostingList(mcx, items, GinPostingListSegmentMaxSize)?;
                    npacked = n;
                    if n == items_len {
                        leaf.segs[i].seg = Some(owned_seg(mcx, img));
                    }
                }
                if npacked != items_len {
                    // Re-pack to the target size, spill the rest into a new
                    // INSERT segment processed on the next iteration.
                    let items = items_slice(leaf.segs[i].items.as_ref().unwrap(), new_items);
                    let (img, n) =
                        ginCompressPostingList(mcx, items, GinPostingListSegmentTargetSize)?;
                    let rest = match leaf.segs[i].items.as_ref().unwrap() {
                        SegItems::New(s, len) => SegItems::New(s + n, len - n),
                        SegItems::Owned(p, len) => {
                            // SAFETY: scratch-owned array; subslice stays live.
                            SegItems::Owned(unsafe { p.add(n) }, len - n)
                        }
                    };
                    leaf.segs[i].seg = Some(owned_seg(mcx, img));
                    if leaf.segs[i].action != GIN_SEGMENT_INSERT {
                        leaf.segs[i].action = GIN_SEGMENT_REPLACE;
                    }
                    leaf.segs.insert(
                        i + 1,
                        SegInfo {
                            action: GIN_SEGMENT_INSERT,
                            seg: None,
                            items: Some(rest),
                            moditems: None,
                        },
                    );
                }
            }

            // Merge a very small segment into the next one.
            if leaf.segs[i].seg.as_ref().unwrap().len() < GinPostingListSegmentMinSize
                && i + 1 < leaf.segs.len()
            {
                if leaf.segs[i].items.is_none() {
                    let seg = *leaf.segs[i].seg.as_ref().unwrap();
                    leaf.segs[i].items = Some(decode_seg(mcx, &seg)?);
                }
                if leaf.segs[i + 1].items.is_none() {
                    let seg = *leaf.segs[i + 1].seg.as_ref().expect("next segment bytes");
                    leaf.segs[i + 1].items = Some(decode_seg(mcx, &seg)?);
                }
                let merged = {
                    let a = items_slice(leaf.segs[i].items.as_ref().unwrap(), new_items);
                    let b = items_slice(leaf.segs[i + 1].items.as_ref().unwrap(), new_items);
                    let m = ginMergeItemPointers(mcx, a, b)?;
                    debug_assert!(m.len() == a.len() + b.len());
                    m
                };
                {
                    let next = &mut leaf.segs[i + 1];
                    next.items = Some(owned_items(mcx, merged));
                    next.seg = None;
                    next.action = GIN_SEGMENT_REPLACE;
                    next.moditems = None;
                }
                if leaf.segs[i].action == GIN_SEGMENT_INSERT {
                    leaf.segs.remove(i);
                    continue;
                } else {
                    leaf.segs[i].action = GIN_SEGMENT_DELETE;
                    leaf.segs[i].seg = None;
                }
            } else {
                leaf.segs[i].items = None;
            }
        }

        if leaf.segs[i].action == GIN_SEGMENT_DELETE {
            i += 1;
            continue;
        }

        let segsize = leaf.segs[i].seg.as_ref().unwrap().len();
        if pgused + segsize > GinDataPageMaxDataSize {
            if !needsplit {
                debug_assert!(pgused > 0);
                // lastleft = previous non-... previous node.
                leaf.lastleft = i - 1;
                needsplit = true;
                leaf.lsize = pgused;
                pgused = 0;
            } else {
                remaining = seg_first(leaf.segs[i].seg.as_ref().unwrap().as_slice());
                leaf.segs.truncate(i);
                break;
            }
        }
        pgused += segsize;
        i += 1;
    }

    if !needsplit {
        leaf.lsize = pgused;
        leaf.rsize = 0;
    } else {
        leaf.rsize = pgused;
    }
    debug_assert!(leaf.lsize <= GinDataPageMaxDataSize);
    debug_assert!(leaf.rsize <= GinDataPageMaxDataSize);

    // Copy every unmodified on-page segment after the first modified one:
    // writing earlier bytes to the page may overwrite them.
    let mut modified = false;
    for seg in leaf.segs.iter_mut() {
        if !modified && seg.action != GIN_SEGMENT_UNMODIFIED {
            modified = true;
        } else if modified && seg.action == GIN_SEGMENT_UNMODIFIED {
            let bytes = seg.seg.as_ref().unwrap().as_slice();
            let mut own: PgVec<'s, u8> = mcx::vec_with_capacity_in(mcx, bytes.len())?;
            vec_append(&mut own, bytes)?;
            seg.seg = Some(owned_seg(mcx, own));
        }
    }

    Ok((needsplit, remaining))
}

/// computeLeafRecompressWALData.
fn compute_leaf_recompress_wal_data(leaf: &mut DisassembledLeaf, new_items: &[ItemPointerData]) {
    let nmodified = leaf
        .segs
        .iter()
        .filter(|s| s.action != GIN_SEGMENT_UNMODIFIED)
        .count();

    let mut buf: Vec<u8> = Vec::with_capacity(2 + BLCKSZ + nmodified * 2);
    buf.extend_from_slice(&crate::wal::ginxlog_recompress_header(nmodified as u16));

    let mut segno = 0u8;
    for seg in leaf.segs.iter() {
        let mut action = seg.action;
        if action == GIN_SEGMENT_UNMODIFIED {
            segno += 1;
            continue;
        }
        let segsize = if action != GIN_SEGMENT_DELETE {
            seg.seg.as_ref().unwrap().len()
        } else {
            0
        };
        if action == GIN_SEGMENT_ADDITEMS {
            let (_, n) = seg.moditems.expect("ADDITEMS modified items");
            if n * 6 > segsize {
                action = GIN_SEGMENT_REPLACE;
            }
        }
        buf.push(segno);
        buf.push(action);
        match action {
            GIN_SEGMENT_DELETE => {}
            GIN_SEGMENT_ADDITEMS => {
                let (s, n) = seg.moditems.expect("ADDITEMS modified items");
                buf.extend_from_slice(&(n as u16).to_ne_bytes());
                for it in &new_items[s..s + n] {
                    // SAFETY: ItemPointerData is a 6-byte POD.
                    buf.extend_from_slice(unsafe {
                        core::slice::from_raw_parts((it as *const ItemPointerData).cast::<u8>(), 6)
                    });
                }
            }
            GIN_SEGMENT_INSERT | GIN_SEGMENT_REPLACE => {
                // Segment images are already SHORTALIGN'd.
                buf.extend_from_slice(seg.seg.as_ref().unwrap().as_slice());
            }
            other => panic!("unexpected GIN leaf action {other}"),
        }
        if action != GIN_SEGMENT_INSERT {
            segno += 1;
        }
    }
    leaf.walinfo = buf;
}

/// dataPlaceToPageLeafRecompress over the buffer page.
fn data_place_to_page_leaf_recompress(buf: Buffer, leaf: &DisassembledLeaf) -> PgResult<()> {
    // SAFETY: pin + exclusive lock held.
    let mut page = unsafe { page_mut(buf) };
    // SAFETY: borrow confined to this function.
    let bytes = unsafe { crate::page_bytes_mut(&mut page) };
    let mut modified = false;
    if !GinPageIsCompressed(&opaque_of(bytes)) {
        // gindatapage.c:992-998: a pre-9.4 page converts its header here and
        // every segment is copied to the page whether modified or not.
        debug_assert!(leaf.oldformat);
        let mut o = opaque_of(bytes);
        o.flags |= GIN_COMPRESSED;
        o.maxoff = InvalidOffsetNumber;
        write_opaque_to(bytes, &o);
        modified = true;
    }
    let mut ptr = GinDataPageDataOffset;
    let mut newsize = 0usize;
    for seg in leaf.segs.iter() {
        if seg.action != GIN_SEGMENT_UNMODIFIED {
            modified = true;
        }
        if seg.action != GIN_SEGMENT_DELETE {
            let src = seg.seg.as_ref().unwrap();
            let segsize = src.len();
            if modified {
                // Page-backed bytes would alias the region being rewritten;
                // leaf_repack_items owns every segment written after the
                // first modification.
                debug_assert!(matches!(src, SegBytes::Owned(..)));
                let s = src.as_slice();
                bytes[ptr..ptr + segsize].copy_from_slice(s);
            }
            ptr += segsize;
            newsize += segsize;
        }
    }
    debug_assert!(newsize <= GinDataPageMaxDataSize);
    set_data_page_data_size(bytes, newsize);
    Ok(())
}

/// dataPlaceToPageLeafSplit into two temp images.
fn data_place_to_page_leaf_split(
    leaf: &DisassembledLeaf,
    lbound: ItemPointerData,
    rbound: ItemPointerData,
    lpage: &mut PageTemp,
    rpage: &mut PageTemp,
) {
    gin_init_page_bytes(lpage.as_mut_bytes(), GIN_DATA | GIN_LEAF | GIN_COMPRESSED);
    gin_init_page_bytes(rpage.as_mut_bytes(), GIN_DATA | GIN_LEAF | GIN_COMPRESSED);

    let firstright = leaf.lastleft + 1;

    let mut ptr = GinDataPageDataOffset;
    let mut lsize = 0usize;
    for seg in &leaf.segs[..firstright] {
        if seg.action != GIN_SEGMENT_DELETE {
            let s = seg.seg.as_ref().unwrap().as_slice();
            lpage.as_mut_bytes()[ptr..ptr + s.len()].copy_from_slice(s);
            ptr += s.len();
            lsize += s.len();
        }
    }
    debug_assert!(lsize == leaf.lsize);
    set_data_page_data_size(lpage.as_mut_bytes(), lsize);
    set_data_page_right_bound(lpage.as_mut_bytes(), &lbound);

    let mut ptr = GinDataPageDataOffset;
    let mut rsize = 0usize;
    for seg in &leaf.segs[firstright..] {
        if seg.action != GIN_SEGMENT_DELETE {
            let s = seg.seg.as_ref().unwrap().as_slice();
            rpage.as_mut_bytes()[ptr..ptr + s.len()].copy_from_slice(s);
            ptr += s.len();
            rsize += s.len();
        }
    }
    debug_assert!(rsize == leaf.rsize);
    set_data_page_data_size(rpage.as_mut_bytes(), rsize);
    set_data_page_right_bound(rpage.as_mut_bytes(), &rbound);
}


pub(crate) enum DataPayload {
    Leaf {
        /// Raw view of the caller's sorted TID array; lives across the
        /// insertion loop (SAFETY: caller keeps it alive).
        items: (*const ItemPointerData, usize),
        curitem: usize,
    },
    None,
}

pub(crate) struct DataBtree<'a, 'r, 's> {
    pub rel: &'a Relation<'r>,
    pub root: BlockNumber,
    pub is_build: bool,
    pub full_scan: bool,
    pub itemptr: ItemPointerData,
    pub scratch: Mcx<'s>,
    pub payload: DataPayload,
    /// Downlink for the in-flight parent insert (C passes it as insertdata).
    downlink: Option<PostingItem>,
    ws: Option<DisassembledLeaf>,
}

impl<'a, 'r, 's> DataBtree<'a, 'r, 's> {
    /// ginPrepareDataScan.
    pub fn new(rel: &'a Relation<'r>, root: BlockNumber, scratch: Mcx<'s>) -> Self {
        DataBtree {
            rel,
            root,
            is_build: false,
            full_scan: false,
            itemptr: ItemPointerData::invalid(),
            scratch,
            payload: DataPayload::None,
            downlink: None,
            ws: None,
        }
    }

    fn new_items(&self) -> &[ItemPointerData] {
        match &self.payload {
            // SAFETY: caller-owned TID array outliving the insert loop.
            DataPayload::Leaf { items, .. } => unsafe {
                core::slice::from_raw_parts(items.0, items.1)
            },
            _ => &[],
        }
    }

    /// dataBeginPlaceToPageLeaf.
    fn begin_leaf(&mut self, buf: Buffer, is_rightmost_hint: bool) -> PgResult<GinPlace> {
        let _ = is_rightmost_hint;
        let (curitem, all_len) = match &self.payload {
            DataPayload::Leaf { items, curitem } => (*curitem, items.1),
            _ => panic!("data leaf insert without leaf payload"),
        };
        let new_items_all = self.new_items();
        let new_items = &new_items_all[curitem..];
        let mut maxitems = all_len - curitem;

        // SAFETY: pin + exclusive lock held across begin→exec.
        let bytes = page_bytes(&unsafe { page_ref(buf) });
        let opaque = opaque_of(bytes);
        let rbound = data_page_right_bound(bytes);

        if !GinPageRightMost(&opaque) {
            let mut i = 0usize;
            while i < maxitems {
                if ginCompareItemPointers(&new_items[i], &rbound) > 0 {
                    debug_assert!(i > 0);
                    break;
                }
                i += 1;
            }
            maxitems = i;
        }

        let mut leaf = disassemble_leaf(self.scratch, bytes)?;

        // Appending to the end of the page?
        let (append, max_old_item) = if !leaf.segs.is_empty() {
            let last = leaf.segs.len() - 1;
            if leaf.segs[last].items.is_none() {
                let seg = *leaf.segs[last].seg.as_ref().unwrap();
                leaf.segs[last].items = Some(decode_seg(self.scratch, &seg)?);
            }
            let max_old = {
                let items = items_slice(leaf.segs[last].items.as_ref().unwrap(), new_items_all);
                items[items.len() - 1]
            };
            (
                ginCompareItemPointers(&new_items[0], &max_old) >= 0,
                max_old,
            )
        } else {
            (true, ItemPointerData::new(0, 0))
        };

        let freespace = if GinPageIsCompressed(&opaque) {
            // GinDataLeafPageGetFreeSpace = PageGetExactFreeSpace.
            let pd_lower = u16::from_ne_bytes([bytes[12], bytes[13]]) as usize;
            let pd_upper = u16::from_ne_bytes([bytes[14], bytes[15]]) as usize;
            pd_upper.saturating_sub(pd_lower)
        } else {
            0
        };
        if append {
            maxitems = maxitems.min(freespace + GinDataPageMaxDataSize);
        } else {
            let nnewsegments = freespace / GinPostingListSegmentMaxSize
                + GinDataPageMaxDataSize / GinPostingListSegmentMaxSize;
            maxitems = maxitems.min(nnewsegments * MinTuplesPerSegment);
        }

        if !add_items_to_leaf(self.scratch, &mut leaf, new_items_all, curitem, maxitems)? {
            if let DataPayload::Leaf { curitem: c, .. } = &mut self.payload {
                *c += maxitems;
            }
            return Ok(GinPlace::NoWork);
        }

        let (needsplit, remaining) = leaf_repack_items(self.scratch, &mut leaf, new_items_all)?;

        // ItemPointerIsValid: some items did not fit after the split.
        if remaining.ip_posid != 0 {
            if !append || ginCompareItemPointers(&max_old_item, &remaining) >= 0 {
                // gindatapage.c:580 elog(ERROR): XX000, catchable.
                return Err(Box::new(PgError::error(
                    "could not split GIN page; all old items didn't fit",
                )));
            }
            let mut i = 0usize;
            while i < maxitems {
                if ginCompareItemPointers(&new_items[i], &remaining) >= 0 {
                    break;
                }
                i += 1;
            }
            if i == 0 {
                // gindatapage.c:589 elog(ERROR): XX000, catchable.
                return Err(Box::new(PgError::error(
                    "could not split GIN page; no new items fit",
                )));
            }
            maxitems = i;
        }

        let place;
        if !needsplit {
            if relation_needs_wal(self.rel) && !self.is_build {
                compute_leaf_recompress_wal_data(&mut leaf, new_items_all);
            }
            self.ws = Some(leaf);
            place = GinPlace::Insert;
        } else {
            // Balance the split 50/50 unless building; when appending aim for
            // a 75% full left page.
            if !self.is_build {
                while leaf.lastleft > 0 {
                    let li = leaf.lastleft;
                    if leaf.segs[li].action != GIN_SEGMENT_DELETE {
                        let segsize = leaf.segs[li].seg.as_ref().unwrap().len();
                        if (leaf.lsize - segsize) as i64 - (leaf.rsize + segsize) as i64 - 0 < 0 {
                            break;
                        }
                        if append && (leaf.lsize - segsize) < (BLCKSZ * 3) / 4 {
                            break;
                        }
                        leaf.lsize -= segsize;
                        leaf.rsize += segsize;
                    }
                    leaf.lastleft -= 1;
                }
            }
            debug_assert!(leaf.lsize <= GinDataPageMaxDataSize);
            debug_assert!(leaf.rsize <= GinDataPageMaxDataSize);

            let li = leaf.lastleft;
            if leaf.segs[li].items.is_none() {
                let seg = *leaf.segs[li].seg.as_ref().unwrap();
                leaf.segs[li].items = Some(decode_seg(self.scratch, &seg)?);
            }
            let lbound = {
                let items = items_slice(leaf.segs[li].items.as_ref().unwrap(), new_items_all);
                items[items.len() - 1]
            };

            let mut newlpage = PageTemp::new(BLCKSZ)?;
            let mut newrpage = PageTemp::new(BLCKSZ)?;
            data_place_to_page_leaf_split(&leaf, lbound, rbound, &mut newlpage, &mut newrpage);
            place = GinPlace::Split(newlpage, newrpage);
        }

        if let DataPayload::Leaf { curitem: c, .. } = &mut self.payload {
            *c += maxitems;
        }
        Ok(place)
    }
}

impl<'r> GinBt<'r> for DataBtree<'_, 'r, '_> {
    const IS_DATA: bool = true;

    fn root_blkno(&self) -> BlockNumber {
        self.root
    }
    fn is_build(&self) -> bool {
        self.is_build
    }
    fn full_scan(&self) -> bool {
        self.full_scan
    }

    /// dataLocateItem.
    fn find_child_page(&self, page: &PageRef<'_>, frame: &mut Frame) -> PgResult<BlockNumber> {
        let bytes = page_bytes(page);
        let opaque = opaque_of(bytes);
        debug_assert!(!GinPageIsLeaf(&opaque) && crate::GinPageIsData(&opaque));
        // maxoff bounds every posting_item_at below; reject a crafted value
        // before any raw access rather than reading past the page image.
        let maxoff = nonleaf_maxoff_checked(bytes)?;

        if self.full_scan {
            frame.off = FirstOffsetNumber;
            frame.predictNumber *= maxoff as u32;
            return self.get_leftmost_child(page);
        }

        let mut low = FirstOffsetNumber;
        let mut high = maxoff;
        debug_assert!(high >= low);
        high += 1;

        while high > low {
            let mid = low + (high - low) / 2;
            let pitem = posting_item_at(bytes, mid);
            let result = if mid == maxoff {
                -1
            } else {
                ginCompareItemPointers(&self.itemptr, &pitem.key)
            };
            if result == 0 {
                frame.off = mid;
                return Ok(PostingItemGetBlockNumber(&pitem));
            } else if result > 0 {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        debug_assert!(high >= FirstOffsetNumber && high <= maxoff);
        frame.off = high;
        Ok(PostingItemGetBlockNumber(&posting_item_at(bytes, high)))
    }

    /// dataGetLeftMostPage.
    fn get_leftmost_child(&self, page: &PageRef<'_>) -> PgResult<BlockNumber> {
        let bytes = page_bytes(page);
        let maxoff = nonleaf_maxoff_checked(bytes)?;
        debug_assert!(maxoff >= FirstOffsetNumber);
        Ok(PostingItemGetBlockNumber(&posting_item_at(bytes, FirstOffsetNumber)))
    }

    /// dataIsMoveRight.
    fn is_move_right(&self, page: &PageRef<'_>) -> PgResult<bool> {
        let bytes = page_bytes(page);
        let opaque = opaque_of(bytes);
        if GinPageRightMost(&opaque) {
            return Ok(false);
        }
        if GinPageIsDeleted(&opaque) {
            return Ok(true);
        }
        Ok(ginCompareItemPointers(&self.itemptr, &data_page_right_bound(bytes)) > 0)
    }

    /// dataFindChildPtr.
    fn find_child_ptr(
        &self,
        page: &PageRef<'_>,
        blkno: BlockNumber,
        stored_off: OffsetNumber,
    ) -> PgResult<OffsetNumber> {
        let bytes = page_bytes(page);
        let mut maxoff = nonleaf_maxoff_checked(bytes)?;
        if stored_off >= FirstOffsetNumber && stored_off <= maxoff {
            if PostingItemGetBlockNumber(&posting_item_at(bytes, stored_off)) == blkno {
                return Ok(stored_off);
            }
            for i in stored_off + 1..=maxoff {
                if PostingItemGetBlockNumber(&posting_item_at(bytes, i)) == blkno {
                    return Ok(i);
                }
            }
            maxoff = stored_off - 1;
        }
        for i in FirstOffsetNumber..=maxoff {
            if PostingItemGetBlockNumber(&posting_item_at(bytes, i)) == blkno {
                return Ok(i);
            }
        }
        Ok(InvalidOffsetNumber)
    }

    /// dataBeginPlaceToPage.
    fn begin_place_to_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
        is_rightmost_insert_hint: bool,
    ) -> PgResult<GinPlace> {
        let _ = off;
        // SAFETY: pin + exclusive lock held.
        let is_leaf = { GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buf) })) };
        if is_leaf {
            self.begin_leaf(buf, is_rightmost_insert_hint)
        } else {
            // dataBeginPlaceToPageInternal.
            // SAFETY: pin + exclusive lock held.
            let fits = {
                let bytes = page_bytes(&unsafe { page_ref(buf) });
                // Reject a corrupt maxoff before the free-space decision: a
                // split (the !fits branch) would otherwise walk posting items
                // up to the crafted maxoff in split_internal and read/write
                // out of bounds.
                nonleaf_maxoff_checked(bytes)?;
                nonleaf_free_space(bytes) >= 10
            };
            if fits {
                Ok(GinPlace::Insert)
            } else {
                let (l, r) = self.split_internal(buf, off, update_blkno)?;
                Ok(GinPlace::Split(l, r))
            }
        }
    }

    /// dataExecPlaceToPage.
    fn exec_place_to_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
    ) -> PgResult<Vec<Vec<u8>>> {
        // SAFETY: pin + exclusive lock held.
        let is_leaf = { GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buf) })) };
        if is_leaf {
            let leaf = self.ws.take().expect("leaf workspace");
            data_place_to_page_leaf_recompress(buf, &leaf)?;
            bm::mark_buffer_dirty::call(buf)?;
            if !leaf.walinfo.is_empty() {
                Ok(vec![leaf.walinfo])
            } else {
                Ok(Vec::new())
            }
        } else {
            let pitem = self.downlink.expect("data internal insert without posting item");
            // SAFETY: pin + exclusive lock held.
            let mut page = unsafe { page_mut(buf) };
            // SAFETY: borrow confined to this block.
            let bytes = unsafe { crate::page_bytes_mut(&mut page) };
            let mut existing = posting_item_at(bytes, off);
            PostingItemSetBlockNumber(&mut existing, update_blkno);
            write_posting_item(bytes, off, &existing);
            gin_data_page_add_posting_item(bytes, &pitem, off);
            bm::mark_buffer_dirty::call(buf)?;
            Ok(vec![crate::wal::ginxlog_insert_data_internal(off, &pitem).to_vec()])
        }
    }

    /// dataPrepareDownlink.
    fn prepare_downlink(&mut self, lbuf: Buffer) -> PgResult<()> {
        // SAFETY: pin + exclusive lock held on lbuf.
        let bytes = page_bytes(&unsafe { page_ref(lbuf) });
        let mut pitem = PostingItem::default();
        PostingItemSetBlockNumber(&mut pitem, bm::buffer_get_block_number::call(lbuf));
        pitem.key = data_page_right_bound(bytes);
        self.downlink = Some(pitem);
        Ok(())
    }

    /// ginDataFillRoot.
    fn fill_root(
        &self,
        root: &mut [u8],
        lblkno: BlockNumber,
        lpage: &[u8],
        rblkno: BlockNumber,
        rpage: &[u8],
    ) -> PgResult<()> {
        gin_data_fill_root(root, lblkno, lpage, rblkno, rpage);
        Ok(())
    }
}

/// ginDataFillRoot over raw images (shared with redo).
pub(crate) fn gin_data_fill_root(
    root: &mut [u8],
    lblkno: BlockNumber,
    lpage: &[u8],
    rblkno: BlockNumber,
    rpage: &[u8],
) {
    let mut li = PostingItem::default();
    li.key = data_page_right_bound(lpage);
    PostingItemSetBlockNumber(&mut li, lblkno);
    gin_data_page_add_posting_item(root, &li, InvalidOffsetNumber);

    let mut ri = PostingItem::default();
    ri.key = data_page_right_bound(rpage);
    PostingItemSetBlockNumber(&mut ri, rblkno);
    gin_data_page_add_posting_item(root, &ri, InvalidOffsetNumber);
}

impl DataBtree<'_, '_, '_> {
    /// dataSplitPageInternal.
    fn split_internal(
        &mut self,
        origbuf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
    ) -> PgResult<(PageTemp, PageTemp)> {
        let pitem = self.downlink.expect("data internal split without posting item");
        // SAFETY: pin + exclusive lock held.
        let oldbytes = page_bytes(&unsafe { page_ref(origbuf) });
        let old_opaque = opaque_of(oldbytes);
        let nitems = old_opaque.maxoff as usize;
        let oldbound = data_page_right_bound(oldbytes);

        let mut lpage = PageTemp::new(BLCKSZ)?;
        let mut rpage = PageTemp::new(BLCKSZ)?;
        gin_init_page_bytes(lpage.as_mut_bytes(), old_opaque.flags);
        gin_init_page_bytes(rpage.as_mut_bytes(), old_opaque.flags);

        let mut allitems: Vec<PostingItem> = Vec::with_capacity(nitems + 1);
        for i in 1..off {
            allitems.push(posting_item_at(oldbytes, i));
        }
        allitems.push(pitem);
        for i in off..=(nitems as OffsetNumber) {
            allitems.push(posting_item_at(oldbytes, i));
        }
        let nitems = nitems + 1;
        // Update the existing (shifted) downlink to the new right page.
        PostingItemSetBlockNumber(&mut allitems[off as usize], update_blkno);

        let separator = if self.is_build && GinPageRightMost(&old_opaque) {
            GinDataPageMaxDataSize / 10
        } else {
            nitems / 2
        };
        let nleft = separator;
        let nright = nitems - separator;

        for (i, item) in allitems[..nleft].iter().enumerate() {
            write_posting_item(lpage.as_mut_bytes(), (i + 1) as OffsetNumber, item);
        }
        {
            let mut o = opaque_of(lpage.as_bytes());
            o.maxoff = nleft as OffsetNumber;
            write_opaque_to(lpage.as_mut_bytes(), &o);
            set_data_page_data_size(lpage.as_mut_bytes(), nleft * 10);
        }
        for (i, item) in allitems[separator..].iter().enumerate() {
            write_posting_item(rpage.as_mut_bytes(), (i + 1) as OffsetNumber, item);
        }
        {
            let mut o = opaque_of(rpage.as_bytes());
            o.maxoff = nright as OffsetNumber;
            write_opaque_to(rpage.as_mut_bytes(), &o);
            set_data_page_data_size(rpage.as_mut_bytes(), nright * 10);
        }

        let lbound = posting_item_at(lpage.as_bytes(), nleft as OffsetNumber).key;
        set_data_page_right_bound(lpage.as_mut_bytes(), &lbound);
        set_data_page_right_bound(rpage.as_mut_bytes(), &oldbound);

        Ok((lpage, rpage))
    }
}

/// createPostingTree: returns the new root block number.
pub(crate) fn createPostingTree<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    items: &[ItemPointerData],
    mut buildStats: Option<&mut GinStatsData>,
    entrybuffer: Buffer,
) -> PgResult<BlockNumber> {
    let is_build = buildStats.is_some();
    let mut tmppage = PageTemp::new(BLCKSZ)?;
    gin_init_page_bytes(tmppage.as_mut_bytes(), GIN_DATA | GIN_LEAF | GIN_COMPRESSED);

    let mut nrootitems = 0usize;
    let mut rootsize = 0usize;
    while nrootitems < items.len() {
        let (segment, npacked) =
            ginCompressPostingList(mcx, &items[nrootitems..], GinPostingListSegmentMaxSize)?;
        let segsize = segment.len();
        if rootsize + segsize > GinDataPageMaxDataSize {
            break;
        }
        let dst = GinDataPageDataOffset + rootsize;
        tmppage.as_mut_bytes()[dst..dst + segsize].copy_from_slice(&segment);
        rootsize += segsize;
        nrootitems += npacked;
    }
    set_data_page_data_size(tmppage.as_mut_bytes(), rootsize);

    let buffer = GinNewBuffer(rel)?;
    let blkno = bm::buffer_get_block_number::call(buffer);

    predicate_seams::predicate_lock_page_split::call(
        rel,
        bm::buffer_get_block_number::call(entrybuffer),
        blkno,
    )?;

    // gindatapage.c:1834 START_CRIT_SECTION(): PageRestoreTempPage through
    // UnlockReleaseBuffer.
    StartCriticalSection();
    // PageRestoreTempPage.
    bm::overwrite_buffer_page::call(buffer, tmppage.as_bytes());
    bm::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(rel) && !is_build {
        let data = crate::wal::ginxlog_create_posting_tree(rootsize as u32);
        // SAFETY: pin + exclusive lock held.
        let posting = {
            let page = unsafe { page_ref(buffer) };
            let bytes = page_bytes(&page);
            &bytes[GinDataPageDataOffset..GinDataPageDataOffset + rootsize]
        };
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_CREATE_PTREE,
            0,
            &[&data, posting],
            &[XLogRegBuf {
                block_id: 0,
                buffer,
                flags: REGBUF_WILL_INIT,
                bufdata: &[],
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(buffer) }.set_lsn(recptr);
    }

    bm::lock_buffer::call(buffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(buffer)?;
    // gindatapage.c:1859 END_CRIT_SECTION().
    EndCriticalSection();

    if let Some(stats) = buildStats.as_deref_mut() {
        stats.nDataPages += 1;
    }

    if items.len() > nrootitems {
        ginInsertItemPointers(
            mcx,
            rel,
            blkno,
            &items[nrootitems..],
            buildStats,
        )?;
    }

    Ok(blkno)
}

/// ginInsertItemPointers.
pub(crate) fn ginInsertItemPointers<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    root_blkno: BlockNumber,
    items: &[ItemPointerData],
    mut buildStats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    let mut btree = DataBtree::new(rel, root_blkno, mcx);
    btree.is_build = buildStats.is_some();
    btree.payload = DataPayload::Leaf {
        items: (items.as_ptr(), items.len()),
        curitem: 0,
    };

    loop {
        let curitem = match &btree.payload {
            DataPayload::Leaf { curitem, .. } => *curitem,
            _ => unreachable!(),
        };
        if curitem >= items.len() {
            break;
        }
        btree.itemptr = items[curitem];
        let mut stack = crate::btree::ginFindLeafPage(mcx, rel, &mut btree, false, true)?;
        crate::btree::ginInsertValue(mcx, rel, &mut btree, &mut stack, buildStats.as_deref_mut())?;
    }
    Ok(())
}

/// ginScanBeginPostingTree: descend to the leftmost leaf, share-locked.
pub(crate) fn ginScanBeginPostingTree<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    root_blkno: BlockNumber,
) -> PgResult<GinStack<'s>> {
    let mut btree = DataBtree::new(rel, root_blkno, mcx);
    btree.full_scan = true;
    crate::btree::ginFindLeafPage(mcx, rel, &mut btree, true, false)
}

/// ginVacuumPostingTreeLeaf: drop dead TIDs from every segment of a
/// posting-tree leaf, recompress in place, WAL-log the actions.
pub(crate) fn ginVacuumPostingTreeLeaf<'s>(
    scratch: Mcx<'s>,
    gvs: &mut crate::vacuum::GinVacuumState<'_, '_, '_, '_>,
    buffer: Buffer,
) -> PgResult<()> {
    let rel = gvs.rel;
    // SAFETY: pin + exclusive lock held by the caller.
    let bytes = page_bytes(&unsafe { page_ref(buffer) });
    let mut leaf = disassemble_leaf(scratch, bytes)?;

    let mut removed_something = false;
    for seg in leaf.segs.iter_mut() {
        let old_seg_size = seg.seg.as_ref().map_or(GinDataPageMaxDataSize, |s| s.len());
        if seg.items.is_none() {
            let sb = *seg.seg.as_ref().expect("segment bytes");
            seg.items = Some(decode_seg(scratch, &sb)?);
        }
        let items = items_slice(seg.items.as_ref().unwrap(), &[]);
        let Some(cleaned) = crate::vacuum::ginVacuumItemPointers(scratch, gvs, items)? else {
            continue;
        };
        if !cleaned.is_empty() {
            let ncleaned = cleaned.len();
            let (packed, npacked) = ginCompressPostingList(scratch, &cleaned, old_seg_size)?;
            if npacked != ncleaned {
                // gindatapage.c:782 elog(ERROR): XX000, catchable.
                return Err(Box::new(PgError::error(
                    "could not fit vacuumed posting list",
                )));
            }
            seg.seg = Some(owned_seg(scratch, packed));
            seg.items = Some(owned_items(scratch, cleaned));
            seg.action = GIN_SEGMENT_REPLACE;
        } else {
            seg.seg = None;
            seg.items = None;
            seg.action = GIN_SEGMENT_DELETE;
        }
        removed_something = true;
    }

    if !removed_something {
        return Ok(());
    }

    // dataPlaceToPageLeafRecompress requires owned copies of every surviving
    // segment at or after the first modification (in-place shift aliasing).
    let mut modified = false;
    for seg in leaf.segs.iter_mut() {
        if seg.action != GIN_SEGMENT_UNMODIFIED {
            modified = true;
        }
        if modified && seg.action != GIN_SEGMENT_DELETE {
            if let Some(SegBytes::Page(..)) = seg.seg {
                let src = seg.seg.as_ref().unwrap().as_slice();
                let mut copy: PgVec<'_, u8> = mcx::vec_with_capacity_in(scratch, src.len())?;
                vec_append(&mut copy, src)?;
                seg.seg = Some(owned_seg(scratch, copy));
            }
        }
    }

    let need_wal = relation_needs_wal(rel);
    if need_wal {
        compute_leaf_recompress_wal_data(&mut leaf, &[]);
    }

    // gindatapage.c:845 START_CRIT_SECTION(): apply the changes to the page
    // and log them.
    StartCriticalSection();
    data_place_to_page_leaf_recompress(buffer, &leaf)?;
    bm::mark_buffer_dirty::call(buffer)?;

    if need_wal {
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_VACUUM_DATA_LEAF_PAGE,
            0,
            &[],
            &[XLogRegBuf {
                block_id: 0,
                buffer,
                flags: ::xloginsert_seams::REGBUF_STANDARD,
                bufdata: &[&leaf.walinfo],
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(buffer) }.set_lsn(recptr);
    }
    // gindatapage.c:862 END_CRIT_SECTION().
    EndCriticalSection();
    Ok(())
}

/// GinDataLeafPageIsEmpty (ginblock.h:285): posting-list size on a
/// compressed leaf, maxoff < FirstOffsetNumber on a pre-9.4 one.
pub(crate) fn gin_data_leaf_page_is_empty(bytes: &[u8]) -> bool {
    let opaque = opaque_of(bytes);
    if GinPageIsCompressed(&opaque) {
        data_leaf_posting_list_size(bytes) == 0
    } else {
        opaque.maxoff < FirstOffsetNumber
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::gin_vocab::GIN_DATA;

    fn make_internal_page(maxoff: OffsetNumber) -> Vec<u8> {
        let mut bytes = vec![0u8; BLCKSZ];
        gin_init_page_bytes(&mut bytes, GIN_DATA);
        let mut opaque = opaque_of(&bytes);
        opaque.maxoff = maxoff;
        write_opaque_to(&mut bytes, &opaque);
        bytes
    }

    #[test]
    fn nonleaf_maxoff_checked_accepts_legit_and_rejects_crafted() {
        // The largest legitimate maxoff keeps every posting_item_at within the
        // BLCKSZ image.
        let ok = make_internal_page(GinMaxNonLeafDataItems as OffsetNumber);
        assert_eq!(
            nonleaf_maxoff_checked(&ok).unwrap(),
            GinMaxNonLeafDataItems as OffsetNumber
        );

        // One past the maximum, and a maximally-crafted maxoff, must yield a
        // typed data-corruption error rather than an out-of-bounds access.
        for crafted in [GinMaxNonLeafDataItems as OffsetNumber + 1, 816, u16::MAX] {
            let page = make_internal_page(crafted);
            let err = nonleaf_maxoff_checked(&page).unwrap_err();
            assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
    }

    #[test]
    fn nonleaf_free_space_saturates_on_crafted_maxoff() {
        // A crafted maxoff far beyond the legitimate maximum must not wrap the
        // free-space computation (which would make the insert "fits" guard pass
        // on a corrupt page); it saturates to zero, forcing the split/error path.
        let page = make_internal_page(u16::MAX);
        assert_eq!(nonleaf_free_space(&page), 0);

        // A legitimately empty page still reports full free space.
        let empty = make_internal_page(0);
        assert_eq!(nonleaf_free_space(&empty), GinDataPageMaxDataSize);
    }
}
