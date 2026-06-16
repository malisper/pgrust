//! GiST insertion spine (`access/gist/gist.c`): the tree-descent insert path
//! `gistdoinsert`, the page-split-or-insert core `gistplacetopage`, the downlink
//! machinery (`gistinserttuple(s)` / `gistfinishsplit` / `gistformdownlink` /
//! `gistFindPath` / `gistFindCorrectParent` / `gistfixsplit`), the recursive
//! `gistSplit`, and the leaf prune `gistprunepage`.
//!
//! Model notes (owned tree vs. C):
//!   * Index tuples are on-disk byte images (`&[u8]` / `PgVec<u8>`), exactly what
//!     `index_form_tuple` produces and `PageGetItem` returns.
//!   * Page bytes are reached through the bufmgr seam: a snapshot read via
//!     `buffer_get_page`, in-place edits via `with_buffer_page` (wrapped in
//!     [`page_modify`] / [`page_read`], like `backend-access-brin-pageops`).
//!   * The `GISTInsertStack` descent is a `Vec<GISTInsertStack>` whose frames are
//!     never removed (C `palloc`s them and never frees within the insert); the
//!     `parent` link is a `Vec` index and "current" is a separate index.
//!   * Critical sections (`START_CRIT_SECTION` / `END_CRIT_SECTION`) are not
//!     modelled in this repo, matching the sibling AMs.
//!   * The GiST-specific WAL writers (`gistXLogSplit` / `gistXLogUpdate` /
//!     `gistXLogDelete` / `gistGetFakeLSN`) live in the GiST xlog (F7) lane;
//!     they are reached through `backend-access-gist-core-seams` and panic until
//!     that lane lands.

use alloc::vec::Vec;
use backend_access_gist_core_seams::{
    gist_get_fake_lsn, gist_xlog_delete, gist_xlog_split, gist_xlog_update,
};
use backend_storage_buffer_bufmgr_seams::{
    buffer_get_block_number, buffer_get_lsn_atomic, buffer_get_page, lock_buffer,
    mark_buffer_dirty, read_buffer, release_buffer, unlock_release_buffer, with_buffer_page,
};
use backend_storage_lmgr_predicate_seams::{
    check_for_serializable_conflict_in_page, predicate_lock_page_split,
};
use backend_storage_page::{
    PageAddItemExtended, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetTempPageCopySpecial, PageIndexMultiDelete, PageIndexTupleDelete,
    PageIndexTupleOverwrite, PageMut, PageRef, PageRestoreTempPage,
};
use backend_utils_error::{ereport, PgResult};
use mcx::{Mcx, PgVec};
use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, Size, TransactionId, XLogRecPtr,
};
use types_core::xact::InvalidTransactionId;
use types_error::error::{ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};
use types_gist::{
    gistxlogPage, GISTInsertStack, GISTInsertState, GISTPageSplitInfo, GISTSTATE, GistBuildLSN,
    GistSplitVector, SplitPageLayout, F_LEAF, GIST_MAX_SPLIT_PAGES, GIST_ROOT_BLKNO,
};
use types_rel::Relation;
use types_storage::buf::BufferIsValid;
use types_storage::storage::Buffer;
use types_tableam::amapi::{IndexInfo, IndexUniqueCheck};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, FIRST_OFFSET_NUMBER, INVALID_OFFSET_NUMBER};

use crate::gist_page::{
    gist_page_get_nsn, gist_page_rightlink, gist_page_set_nsn, gistcheckpage, gistfillbuffer,
    set_gist_page_flags, set_gist_page_rightlink, GISTInitBuffer, GistClearFollowRight,
    GistClearPageHasGarbage, GistFollowRight, GistMarkFollowRight, GistPageHasGarbage,
    GistPageIsDeleted, GistPageIsLeaf,
};
use crate::gistutil::{
    gist_tuple_is_invalid, gist_tuple_set_valid, gistFormTuple, gistNewBuffer, gistchoose,
    gistextractpage, gistfillitupvec, gistfitpage, gistgetadjusted, gistjoinvector, gistnospace,
    index_tuple_size, initGISTstate, itup_block_number, itup_set_block_number,
};

/// `BUFFER_LOCK_UNLOCK` / `GIST_UNLOCK` (bufmgr.h / gist_private.h).
const GIST_UNLOCK: i32 = 0;
/// `BUFFER_LOCK_SHARE` / `GIST_SHARE`.
const GIST_SHARE: i32 = 1;
/// `BUFFER_LOCK_EXCLUSIVE` / `GIST_EXCLUSIVE`.
const GIST_EXCLUSIVE: i32 = 2;

/// `InvalidBuffer` (buf.h) — buffer 0.
const INVALID_BUFFER: Buffer = 0;

// ===========================================================================
// Page read/modify helpers over the bufmgr seam (mirrors brin_internal.rs).
// ===========================================================================

/// Run a fallible in-place edit on a (caller-locked) buffer page.
fn page_modify<R>(buf: Buffer, f: impl FnOnce(&mut [u8]) -> PgResult<R>) -> PgResult<R> {
    let mut slot: Option<PgResult<R>> = None;
    let mut once = Some(f);
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let g = once.take().expect("page_modify closure run once");
        let r = g(page);
        let ok = r.is_ok();
        slot = Some(r);
        if ok {
            Ok(())
        } else {
            Err(edit_failed())
        }
    })
    .ok();
    slot.expect("page_modify produced a value")
}

fn edit_failed() -> types_error::PgError {
    ereport(ERROR)
        .errmsg_internal("gist page edit failed")
        .into_error()
}

/// `PageSetLSN(page, lsn)` over the page byte image.
fn page_set_lsn_bytes(page: &mut [u8], lsn: XLogRecPtr) {
    // pd_lsn is the leading PageXLogRecPtr { xlogid, xrecoff } of PageHeaderData.
    let xlogid = (lsn >> 32) as u32;
    let xrecoff = (lsn & 0xFFFF_FFFF) as u32;
    page[0..4].copy_from_slice(&xlogid.to_ne_bytes());
    page[4..8].copy_from_slice(&xrecoff.to_ne_bytes());
}

/// `PageGetLSN(page)` over the page byte image.
fn page_get_lsn_bytes(page: &[u8]) -> XLogRecPtr {
    let xlogid = u32::from_ne_bytes([page[0], page[1], page[2], page[3]]);
    let xrecoff = u32::from_ne_bytes([page[4], page[5], page[6], page[7]]);
    ((xlogid as u64) << 32) | (xrecoff as u64)
}

// ===========================================================================
// gistplacetopage (gist.c:229)
// ===========================================================================

/// Inputs for [`gistplacetopage`], grouping the C parameter list.
pub struct PlaceToPage<'mcx, 'a> {
    pub rel: &'a Relation<'mcx>,
    pub freespace: Size,
    pub giststate: &'a GISTSTATE<'mcx>,
    pub buffer: Buffer,
    /// the on-disk byte images to insert (`IndexTuple *itup`, `ntup`).
    pub itup: &'a [&'a [u8]],
    pub oldoffnum: OffsetNumber,
    pub left_child_buf: Buffer,
    pub mark_follow_right: bool,
    pub heap_rel: &'a Relation<'mcx>,
    pub is_build: bool,
}

/// Output of [`gistplacetopage`].
pub struct PlaceToPageResult<'mcx> {
    /// whether the page was split.
    pub is_split: bool,
    /// `*splitinfo` (empty for the non-split / root-split case).
    pub split_info: Vec<GISTPageSplitInfo<'mcx>>,
    /// `*newblkno`, the block the first inserted/updated tuple landed on.
    pub new_blkno: BlockNumber,
}

/// `gistplacetopage(...)` (gist.c:229): place `itup` into `buffer`, splitting the
/// page when it doesn't fit. See the C comment for the locking / split contract.
pub fn gistplacetopage<'mcx>(args: PlaceToPage<'mcx, '_>) -> PgResult<PlaceToPageResult<'mcx>> {
    let PlaceToPage {
        rel,
        freespace,
        giststate,
        buffer,
        itup,
        oldoffnum,
        left_child_buf,
        mark_follow_right,
        heap_rel,
        is_build,
    } = args;
    let mcx = giststate.tempCxt;

    let blkno = buffer_get_block_number::call(buffer);
    let page_snap = buffer_get_page::call(mcx, buffer)?;
    let is_leaf = GistPageIsLeaf(&page_snap)?;

    // Refuse to modify an incompletely-split page.
    if GistFollowRight(&page_snap)? {
        return Err(ereport(ERROR)
            .errmsg_internal("concurrent GiST page split was incomplete")
            .into_error());
    }
    debug_assert!(!GistPageIsDeleted(&page_snap)?);

    let mut split_info: Vec<GISTPageSplitInfo<'mcx>> = Vec::new();
    let mut new_blkno = blkno;
    let recptr;

    // is_split = gistnospace(page, itup, ntup, oldoffnum, freespace);
    let mut is_split = gistnospace(&page_snap, itup, oldoffnum, freespace as usize)?;

    // If a leaf page is full, try first to delete dead tuples, then re-check.
    if is_split && is_leaf && GistPageHasGarbage(&page_snap)? {
        gistprunepage(mcx, rel, buffer, heap_rel)?;
        let page_snap2 = buffer_get_page::call(mcx, buffer)?;
        is_split = gistnospace(&page_snap2, itup, oldoffnum, freespace as usize)?;
    }

    if is_split {
        // ---- no space for insertion: split ----
        let is_rootsplit = blkno == GIST_ROOT_BLKNO;

        // Form the itup vector to split. If replacing an old tuple, drop it.
        let page_for_extract = buffer_get_page::call(mcx, buffer)?;
        let mut itvec: Vec<PgVec<'mcx, u8>> = gistextractpage(mcx, &page_for_extract)?;
        if oldoffnum != INVALID_OFFSET_NUMBER {
            let pos = (oldoffnum - FIRST_OFFSET_NUMBER) as usize;
            itvec.remove(pos);
        }
        // gistjoinvector(itvec, &tlen, itup, ntup)
        let mut addvec: Vec<PgVec<'mcx, u8>> = Vec::with_capacity(itup.len());
        for it in itup {
            addvec.push(mcx::slice_in(mcx, it)?);
        }
        gistjoinvector(&mut itvec, &addvec)?;

        let itvec_refs: Vec<&[u8]> = itvec.iter().map(|v| v.as_slice()).collect();
        let mut dist = gistSplit(mcx, rel, blkno, &itvec_refs, giststate)?;

        // Check that the split didn't produce too many pages.
        let mut npage = dist.len() as i32;
        if is_rootsplit {
            npage += 1;
        }
        if npage > GIST_MAX_SPLIT_PAGES {
            return Err(ereport(ERROR)
                .errmsg_internal(alloc::format!(
                    "GiST page split into too many halves ({npage}, maximum {GIST_MAX_SPLIT_PAGES})"
                ))
                .into_error());
        }

        // Working page images, one per dist entry (parallel to `dist`).
        let page_size = page_snap.len();
        let mut pages: Vec<alloc::vec::Vec<u8>> = Vec::with_capacity(dist.len());

        let mut oldrlink = InvalidBlockNumber;
        let mut oldnsn: types_gist::GistNSN = 0;

        // Set up pages. The original page becomes the new leftmost page (unless
        // root-split, where the original becomes the new root and ALL dist halves
        // get fresh buffers). For a non-root split the original page's old
        // rightlink/NSN are saved and the original page becomes dist[0].
        if !is_rootsplit {
            // save old rightlink and NSN from the original page
            let orig = buffer_get_page::call(mcx, buffer)?;
            oldrlink = gist_page_rightlink(&orig)?;
            oldnsn = gist_page_get_nsn(&orig)?;

            dist[0].buffer = buffer;
            dist[0].block.blkno = buffer_get_block_number::call(buffer);
            // dist->page = PageGetTempPageCopySpecial(BufferGetPage(buffer));
            let temp = {
                let pref = PageRef::new(&orig)?;
                PageGetTempPageCopySpecial(&pref)?
            };
            let mut tbytes = temp.as_bytes().to_vec();
            // clean all flags except F_LEAF
            set_gist_page_flags(&mut tbytes, if is_leaf { F_LEAF } else { 0 })?;
            pages.push(tbytes);
            dist[0].page = dist[0].block.blkno;
        }

        // Allocate new buffers for the new halves (all halves for a root split,
        // all-but-the-leftmost otherwise). `pages` grows in dist order.
        let start = if is_rootsplit { 0 } else { 1 };
        for i in start..dist.len() {
            let newbuf = gistNewBuffer(mcx, rel, heap_rel)?;
            GISTInitBuffer(newbuf, if is_leaf { F_LEAF } else { 0 })?;
            dist[i].buffer = newbuf;
            dist[i].block.blkno = buffer_get_block_number::call(newbuf);
            dist[i].page = dist[i].block.blkno;
            predicate_lock_page_split::call(
                rel.rd_id,
                buffer_get_block_number::call(buffer),
                buffer_get_block_number::call(newbuf),
            )?;
            let np = buffer_get_page::call(mcx, newbuf)?;
            pages.push(np.to_vec());
        }
        debug_assert_eq!(pages.len(), dist.len());

        // Now that block numbers are known, set up downlink tuples.
        for d in dist.iter_mut() {
            if let Some(it) = d.itup.as_mut() {
                itup_set_block_number(it, d.block.blkno);
                gist_tuple_set_valid(it);
            }
        }

        // The first inserted tuple's t_tid identifies which page it landed on.
        let first_tid: [u8; 6] = {
            let mut t = [0u8; 6];
            t.copy_from_slice(&itup[0][0..6]);
            t
        };

        // Root split: build the new root page with the downlinks directly.
        let mut root_layout: Option<SplitPageLayout<'mcx>> = None;
        let mut root_page_bytes: Option<alloc::vec::Vec<u8>> = None;
        if is_rootsplit {
            // rootpg.page = PageGetTempPageCopySpecial(BufferGetPage(rootpg.buffer));
            let orig = buffer_get_page::call(mcx, buffer)?;
            let temp = {
                let pref = PageRef::new(&orig)?;
                PageGetTempPageCopySpecial(&pref)?
            };
            let mut tbytes = temp.as_bytes().to_vec();
            set_gist_page_flags(&mut tbytes, 0)?;

            let ndownlinks = dist.len();
            let downlinks: Vec<&[u8]> = dist
                .iter()
                .map(|d| d.itup.as_ref().expect("downlink present").as_slice())
                .collect();
            let list = gistfillitupvec(mcx, &downlinks)?;
            let lenlist = list.len() as i32;

            root_layout = Some(SplitPageLayout {
                block: gistxlogPage {
                    blkno: GIST_ROOT_BLKNO,
                    num: ndownlinks as i32,
                },
                list,
                lenlist,
                itup: None,
                page: GIST_ROOT_BLKNO,
                buffer,
            });
            root_page_bytes = Some(tbytes);
        } else {
            // Prepare split-info to be returned to the caller.
            for d in dist.iter() {
                split_info.push(GISTPageSplitInfo {
                    buf: d.buffer,
                    downlink: d
                        .itup
                        .as_ref()
                        .expect("downlink present")
                        .clone(),
                });
            }
        }

        // Assemble the full processing order: [root_layout?] ++ dist.
        // We keep `dist` and the optional root separately and iterate the root
        // first (C prepends rootpg to dist).
        //
        // Fill all pages with their tuples and set rightlinks / follow-right.
        let dist_len = dist.len();
        // Helper to fill one page byte image from a concatenated tuple list.
        let fill_page = |pbytes: &mut [u8], list: &[u8], num: i32| -> PgResult<()> {
            let mut data_off = 0usize;
            let mut pmut = PageMut::new(pbytes)?;
            for i in 0..num as usize {
                let thistup = &list[data_off..];
                let sz = index_tuple_size(thistup);
                let off = (i as OffsetNumber) + FIRST_OFFSET_NUMBER;
                let l = PageAddItemExtended(&mut pmut, &thistup[..sz], off, 0)?;
                if l == INVALID_OFFSET_NUMBER {
                    return Err(edit_failed());
                }
                data_off += sz;
            }
            Ok(())
        };

        // Fill the synthetic root first (if any).
        if let (Some(rl), Some(rb)) = (root_layout.as_ref(), root_page_bytes.as_mut()) {
            fill_page(rb, &rl.list, rl.block.num)?;
            // newblkno: does any root downlink match the first tuple? (root holds
            // downlinks, not the inserted leaf tuple, so it won't match — matches
            // C which compares thistup->t_tid to (*itup)->t_tid.)
            // rightlink: root page keeps oldrlink (it's GIST_ROOT_BLKNO).
            set_gist_page_rightlink(rb, oldrlink)?;
            // root is the right-most among {root, dist...}? No: root->next = dist,
            // so root has a next and blkno == GIST_ROOT_BLKNO => rightlink=oldrlink.
            GistClearFollowRight(rb)?;
            gist_page_set_nsn(rb, oldnsn)?;
        }

        // Fill the dist pages.
        for i in 0..dist_len {
            let num = dist[i].block.num;
            let list = dist[i].list.clone();
            fill_page(&mut pages[i], &list, num)?;

            // newblkno: if this page holds the first inserted tuple, record it.
            {
                let pref = PageRef::new(&pages[i])?;
                let maxoff = PageGetMaxOffsetNumber(&pref);
                let mut o = FIRST_OFFSET_NUMBER;
                while o <= maxoff {
                    let id = PageGetItemId(&pref, o)?;
                    let it = PageGetItem(&pref, &id)?;
                    if it.len() >= 6 && it[0..6] == first_tid {
                        new_blkno = dist[i].block.blkno;
                        break;
                    }
                    o += 1;
                }
            }

            // Set up rightlinks.
            let next_blkno = if i + 1 < dist_len {
                Some(dist[i + 1].block.blkno)
            } else {
                None
            };
            let has_next_overall = next_blkno.is_some();
            if let Some(nb) = next_blkno {
                if dist[i].block.blkno != GIST_ROOT_BLKNO {
                    set_gist_page_rightlink(&mut pages[i], nb)?;
                } else {
                    set_gist_page_rightlink(&mut pages[i], oldrlink)?;
                }
            } else {
                set_gist_page_rightlink(&mut pages[i], oldrlink)?;
            }

            // Mark all but the right-most page follow-right (non-root, when
            // markfollowright). Otherwise clear it.
            if has_next_overall && !is_rootsplit && mark_follow_right {
                GistMarkFollowRight(&mut pages[i])?;
            } else {
                GistClearFollowRight(&mut pages[i])?;
            }

            // Copy the NSN of the original page to all pages.
            gist_page_set_nsn(&mut pages[i], oldnsn)?;
        }

        // Prepare WAL space; NB must match gistXLogSplit's calculation.
        if !is_build && relation_needs_wal(rel) {
            backend_access_transam_xlog_seams::xlog_ensure_record_space::call(npage, 1 + npage * 2)?;
        }

        // Mark buffers dirty before XLogInsert.
        if let (Some(rl), _) = (root_layout.as_ref(), ()) {
            mark_buffer_dirty::call(rl.buffer);
        }
        for d in dist.iter() {
            mark_buffer_dirty::call(d.buffer);
        }
        if BufferIsValid(left_child_buf) {
            mark_buffer_dirty::call(left_child_buf);
        }

        // The leftmost page (dist[0] for non-root; the root buffer for root) was
        // a temp copy meant to replace the old page — copy it back.
        if is_rootsplit {
            // Root buffer gets the synthetic root page image.
            let rb = root_page_bytes.take().expect("root page bytes");
            page_modify(buffer, |page: &mut [u8]| {
                let temp = page_temp_from_bytes(&rb)?;
                let mut pmut = PageMut::new(page)?;
                PageRestoreTempPage(temp, &mut pmut)
            })?;
        } else {
            let lb = pages[0].clone();
            page_modify(buffer, |page: &mut [u8]| {
                let temp = page_temp_from_bytes(&lb)?;
                let mut pmut = PageMut::new(page)?;
                PageRestoreTempPage(temp, &mut pmut)
            })?;
        }

        // Write the remaining (non-leftmost) dist pages back to their buffers.
        let write_start = if is_rootsplit { 0 } else { 1 };
        for i in write_start..dist_len {
            let pb = pages[i].clone();
            page_modify(dist[i].buffer, |page: &mut [u8]| {
                page.copy_from_slice(&pb);
                Ok(())
            })?;
        }

        // Determine the record pointer.
        if is_build {
            recptr = GistBuildLSN;
        } else if relation_needs_wal(rel) {
            // gistXLogSplit over the full dist chain (root prepended for root
            // split). The WAL writer lives in the GiST xlog (F7) lane.
            let full: Vec<SplitPageLayout<'mcx>> = build_xlog_chain(&mut dist, root_layout.take());
            recptr = gist_xlog_split::call(
                is_leaf,
                &full,
                oldrlink,
                oldnsn,
                left_child_buf,
                mark_follow_right,
            )?;
            // restore dist from full (root prepended), so subsequent loops use it
            restore_dist_from_chain(&mut dist, full, is_rootsplit);
        } else {
            recptr = gist_get_fake_lsn::call(rel)?;
        }
        let _ = page_size;

        // Set LSN on all pages (root + dist).
        if is_rootsplit {
            page_modify(buffer, |page: &mut [u8]| {
                page_set_lsn_bytes(page, recptr);
                Ok(())
            })?;
            for i in 0..dist_len {
                page_modify(dist[i].buffer, |page: &mut [u8]| {
                    page_set_lsn_bytes(page, recptr);
                    Ok(())
                })?;
            }
        } else {
            for d in dist.iter() {
                page_modify(d.buffer, |page: &mut [u8]| {
                    page_set_lsn_bytes(page, recptr);
                    Ok(())
                })?;
            }
        }

        // For a root split, the downlinks are already in the new root, so release
        // all new child buffers and keep only the root locked.
        if is_rootsplit {
            for d in dist.iter() {
                unlock_release_buffer::call(d.buffer);
            }
        }
    } else {
        // ---- enough space (also reached when ntup == 0) ----
        // Delete old tuple if any, then insert new tuple(s).
        page_modify(buffer, |page: &mut [u8]| -> PgResult<()> {
            if oldoffnum != INVALID_OFFSET_NUMBER {
                if itup.len() == 1 {
                    // one-for-one replacement
                    let mut pmut = PageMut::new(page)?;
                    PageIndexTupleOverwrite(&mut pmut, oldoffnum, itup[0])?;
                } else {
                    {
                        let mut pmut = PageMut::new(page)?;
                        PageIndexTupleDelete(&mut pmut, oldoffnum)?;
                    }
                    let owned: Vec<alloc::vec::Vec<u8>> =
                        itup.iter().map(|t| t.to_vec()).collect();
                    gistfillbuffer(page, &owned, INVALID_OFFSET_NUMBER)?;
                }
            } else {
                let owned: Vec<alloc::vec::Vec<u8>> = itup.iter().map(|t| t.to_vec()).collect();
                gistfillbuffer(page, &owned, INVALID_OFFSET_NUMBER)?;
            }
            Ok(())
        })?;

        mark_buffer_dirty::call(buffer);
        if BufferIsValid(left_child_buf) {
            mark_buffer_dirty::call(left_child_buf);
        }

        if is_build {
            recptr = GistBuildLSN;
        } else if relation_needs_wal(rel) {
            let deloffs: Vec<OffsetNumber> = if oldoffnum != INVALID_OFFSET_NUMBER {
                alloc::vec![oldoffnum]
            } else {
                Vec::new()
            };
            recptr = gist_xlog_update::call(buffer, &deloffs, itup, left_child_buf)?;
        } else {
            recptr = gist_get_fake_lsn::call(rel)?;
        }
        page_modify(buffer, |page: &mut [u8]| {
            page_set_lsn_bytes(page, recptr);
            Ok(())
        })?;

        new_blkno = blkno;
    }

    // If we inserted the downlink for a child page, set NSN + clear follow-right
    // on the left child, AFTER writing the WAL record.
    if BufferIsValid(left_child_buf) {
        page_modify(left_child_buf, |leftpg: &mut [u8]| -> PgResult<()> {
            gist_page_set_nsn(leftpg, recptr)?;
            GistClearFollowRight(leftpg)?;
            page_set_lsn_bytes(leftpg, recptr);
            Ok(())
        })?;
    }

    Ok(PlaceToPageResult {
        is_split,
        split_info,
        new_blkno,
    })
}

/// Build the full WAL chain for `gistXLogSplit`: root (if present) prepended to
/// `dist`. The split layouts are moved out of `dist` (it is rebuilt afterwards).
fn build_xlog_chain<'mcx>(
    dist: &mut Vec<SplitPageLayout<'mcx>>,
    root: Option<SplitPageLayout<'mcx>>,
) -> Vec<SplitPageLayout<'mcx>> {
    let mut out: Vec<SplitPageLayout<'mcx>> = Vec::with_capacity(dist.len() + 1);
    if let Some(r) = root {
        out.push(r);
    }
    out.append(dist);
    out
}

/// Rebuild `dist` from the chain produced by [`build_xlog_chain`] (drop the
/// prepended synthetic root for a root split).
fn restore_dist_from_chain<'mcx>(
    dist: &mut Vec<SplitPageLayout<'mcx>>,
    mut full: Vec<SplitPageLayout<'mcx>>,
    was_rootsplit: bool,
) {
    if was_rootsplit && !full.is_empty() {
        full.remove(0);
    }
    *dist = full;
}

/// Build a [`types_storage::bufpage::PageTemp`] from an existing byte image.
fn page_temp_from_bytes(bytes: &[u8]) -> PgResult<types_storage::bufpage::PageTemp> {
    let mut temp = types_storage::bufpage::PageTemp::new(bytes.len() as Size)?;
    temp.as_mut_bytes().copy_from_slice(bytes);
    Ok(temp)
}

/// `RelationNeedsWAL(rel)` via the relcache seam.
fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    backend_utils_cache_relcache_seams::relation_needs_wal::call(rel)
}

// ===========================================================================
// gistSplit (gist.c:1449)
// ===========================================================================

/// `gistSplit(r, page, itup, len, giststate)` (gist.c:1449): split a page,
/// recursively, until the keys fit on every page. Returns the produced page
/// halves in left-to-right order.
pub fn gistSplit<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    page_blkno: BlockNumber,
    itup: &[&[u8]],
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<Vec<SplitPageLayout<'mcx>>> {
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    let len = itup.len() as i32;
    debug_assert!(len > 0);

    // A single tuple that doesn't fit can't be helped by splitting.
    if len == 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(alloc::format!(
                "index row size {} exceeds maximum {} for index \"{}\"",
                index_tuple_size(itup[0]),
                crate::gist_page::GiSTPageSize,
                r.name()
            ))
            .into_error());
    }

    let ncols = giststate
        .nonLeafTupdesc
        .as_ref()
        .expect("gistSplit: nonLeafTupdesc")
        .as_ref()
        .natts as usize;

    let mut v = GistSplitVector {
        splitVector: Default::default(),
        spl_lattr: alloc::vec![None; ncols],
        spl_lisnull: alloc::vec![true; ncols],
        spl_rattr: alloc::vec![None; ncols],
        spl_risnull: alloc::vec![true; ncols],
        spl_dontcare: Vec::new(),
    };

    crate::gistsplit::gistSplitByKey(mcx, r, page_blkno, itup, len, giststate, &mut v, 0)?;

    // Form left and right vectors.
    let mut lvectup: Vec<&[u8]> = Vec::with_capacity(v.splitVector.spl_left.len());
    for &off in v.splitVector.spl_left.iter() {
        lvectup.push(itup[(off - 1) as usize]);
    }
    let mut rvectup: Vec<&[u8]> = Vec::with_capacity(v.splitVector.spl_right.len());
    for &off in v.splitVector.spl_right.iter() {
        rvectup.push(itup[(off - 1) as usize]);
    }

    // finalize splitting (may need another split)
    let mut res: Vec<SplitPageLayout<'mcx>>;
    if !gistfitpage(&rvectup) {
        res = gistSplit(mcx, r, page_blkno, &rvectup, giststate)?;
    } else {
        let list = gistfillitupvec(mcx, &rvectup)?;
        let lenlist = list.len() as i32;
        let attr_owned: Vec<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> = v
            .spl_rattr
            .iter()
            .map(|d| d.clone().unwrap_or(types_tuple::backend_access_common_heaptuple::Datum::ByVal(0)))
            .collect();
        let itupd = gistFormTuple(mcx, giststate, r, &attr_owned, &v.spl_risnull, false)?;
        res = alloc::vec![SplitPageLayout {
            block: gistxlogPage {
                blkno: InvalidBlockNumber,
                num: v.splitVector.spl_right.len() as i32,
            },
            list,
            lenlist,
            itup: Some(itupd),
            page: InvalidBlockNumber,
            buffer: INVALID_BUFFER,
        }];
    }

    if !gistfitpage(&lvectup) {
        let mut subres = gistSplit(mcx, r, page_blkno, &lvectup, giststate)?;
        // install res on subres's tail
        subres.append(&mut res);
        res = subres;
    } else {
        let list = gistfillitupvec(mcx, &lvectup)?;
        let lenlist = list.len() as i32;
        let attr_owned: Vec<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> = v
            .spl_lattr
            .iter()
            .map(|d| d.clone().unwrap_or(types_tuple::backend_access_common_heaptuple::Datum::ByVal(0)))
            .collect();
        let itupd = gistFormTuple(mcx, giststate, r, &attr_owned, &v.spl_lisnull, false)?;
        let left = SplitPageLayout {
            block: gistxlogPage {
                blkno: InvalidBlockNumber,
                num: v.splitVector.spl_left.len() as i32,
            },
            list,
            lenlist,
            itup: Some(itupd),
            page: InvalidBlockNumber,
            buffer: INVALID_BUFFER,
        };
        let mut newres = alloc::vec![left];
        newres.append(&mut res);
        res = newres;
    }

    Ok(res)
}

// ===========================================================================
// gistprunepage (gist.c:1674)
// ===========================================================================

/// `gistprunepage(rel, page, buffer, heapRel)` (gist.c:1674): remove LP_DEAD
/// items from a leaf page (buffer exclusively locked).
pub fn gistprunepage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buffer: Buffer,
    heap_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let page = buffer_get_page::call(mcx, buffer)?;
    debug_assert!(GistPageIsLeaf(&page)?);

    // Collect LP_DEAD offsets.
    let mut deletable: Vec<OffsetNumber> = Vec::new();
    {
        let pref = PageRef::new(&page)?;
        let maxoff = PageGetMaxOffsetNumber(&pref);
        let mut offnum = FIRST_OFFSET_NUMBER;
        while offnum <= maxoff {
            let id = PageGetItemId(&pref, offnum)?;
            if backend_storage_page::ItemIdIsDead(&id) {
                deletable.push(offnum);
            }
            offnum += 1;
        }
    }

    if !deletable.is_empty() {
        let mut snapshot_conflict_horizon: TransactionId = InvalidTransactionId;

        if backend_access_transam_xlog_seams::xlog_standby_info_active::call()
            && relation_needs_wal(rel)
        {
            snapshot_conflict_horizon =
                backend_access_heap_heapam_seams::index_compute_xid_horizon_for_tuples::call(
                    rel, heap_rel, buffer, &deletable,
                )?;
        }

        page_modify(buffer, |page: &mut [u8]| -> PgResult<()> {
            let mut pmut = PageMut::new(page)?;
            PageIndexMultiDelete(&mut pmut, &deletable)?;
            drop(pmut);
            GistClearPageHasGarbage(page)?;
            Ok(())
        })?;

        mark_buffer_dirty::call(buffer);

        let recptr = if relation_needs_wal(rel) {
            gist_xlog_delete::call(buffer, &deletable, snapshot_conflict_horizon, heap_rel)?
        } else {
            gist_get_fake_lsn::call(rel)?
        };
        page_modify(buffer, |page: &mut [u8]| {
            page_set_lsn_bytes(page, recptr);
            Ok(())
        })?;
    }

    Ok(())
}

// ===========================================================================
// gistdoinsert + the downlink machinery (gist.c).
//
// The C `GISTInsertStack` is a pointer-linked stack of palloc'd frames. We model
// it as `state.stack: Vec<GISTInsertStack>` (frames never removed within an
// insert) with `parent: Option<usize>` linking by index; the "current" frame is
// a separate `cur: usize`.
// ===========================================================================

/// `gistinsert(r, values, isnull, ht_ctid, heapRel, checkUnique, indexUnchanged,
/// indexInfo)` (gist.c:164): the public `aminsert` entry — the thin wrapper for
/// GiST tuple insertion. It locks no relation itself; it just forms an index
/// tuple over the heap row's `values`/`isnull`, stamps the heap CTID into the
/// tuple's `t_tid`, and hands off to [`gistdoinsert`] to descend the tree and
/// place it. GiST never reports a unique conflict, so it always returns `false`.
///
/// In C the per-statement `GISTSTATE` is cached in `indexInfo->ii_AmCache` (built
/// once, with its own `tempCxt` temporary memory context reset after each insert)
/// so it is rebuilt only on the first call of a statement. Our `IndexInfo`
/// carrier (`payload: Option<Box<dyn Any + 'static>>`) cannot hold the
/// `'mcx`-bound `GISTSTATE`, so we rebuild it on every call from `initGISTstate`
/// (behaviour-preserving — the cache is a pure performance hint that changes no
/// on-disk state and produces identical results, exactly like the documented
/// `brininsert` `ii_AmCache` rebuild-per-call). The C `tempCxt` reset is the
/// owned-model `mcx` lifetime: the per-call `GISTSTATE`, the formed tuple, and
/// everything `gistdoinsert` allocates live in `mcx` and are reclaimed when the
/// caller's context ends.
#[allow(clippy::too_many_arguments)]
pub fn gistinsert<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    heap_rel: &Relation<'mcx>,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    index_info: &mut IndexInfo,
) -> PgResult<bool> {
    // See the function doc: the C ii_AmCache GISTSTATE cache cannot be expressed
    // over the 'static IndexInfo payload, so rebuild per call (behaviour-
    // preserving). createTempGistContext's temporary context is the mcx lifetime.
    let _ = index_info;
    let giststate = initGISTstate(mcx, r)?;

    // itup = gistFormTuple(giststate, r, values, isnull, true);
    let mut itup = gistFormTuple(mcx, &giststate, r, values, isnull, true)?;

    // itup->t_tid = *ht_ctid;  -- the leading ItemPointerData of the on-disk
    // image is `BlockIdData ip_blkid` (`bi_hi` then `bi_lo`, each u16) followed by
    // `OffsetNumber ip_posid` (u16): 6 bytes at offset 0. This overwrites the
    // 0xffff sentinel offset that gistFormTuple stamped.
    itup[0..2].copy_from_slice(&ht_ctid.ip_blkid.bi_hi.to_ne_bytes());
    itup[2..4].copy_from_slice(&ht_ctid.ip_blkid.bi_lo.to_ne_bytes());
    itup[4..6].copy_from_slice(&ht_ctid.ip_posid.to_ne_bytes());

    // gistdoinsert(r, itup, 0, giststate, heapRel, false);
    gistdoinsert(mcx, r, &itup, 0, &giststate, heap_rel, false)?;

    // GiST never reports a unique-constraint conflict.
    Ok(false)
}

/// `gistdoinsert(r, itup, freespace, giststate, heapRel, is_build)` (gist.c:638):
/// the workhorse for inserting one tuple into a GiST index. Walks down the path
/// of smallest penalty, updating parent keys as it goes.
#[allow(unused_assignments)]
pub fn gistdoinsert<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    itup: &[u8],
    freespace: Size,
    giststate: &GISTSTATE<'mcx>,
    heap_rel: &Relation<'mcx>,
    is_build: bool,
) -> PgResult<()> {
    let mut state = GISTInsertState {
        r: r.rd_id,
        heapRel: heap_rel.rd_id,
        freespace,
        is_build,
        stack: Vec::new(),
    };

    // Start from the root.
    state.stack.push(GISTInsertStack {
        blkno: GIST_ROOT_BLKNO,
        buffer: INVALID_BUFFER,
        page: GIST_ROOT_BLKNO,
        lsn: 0,
        retry_from_parent: false,
        downlinkoffnum: INVALID_OFFSET_NUMBER,
        parent: None,
    });
    let mut cur: usize = 0;
    let mut xlocked = false;

    loop {
        // If we split an internal page while descending, retry at the parent.
        while state.stack[cur].retry_from_parent {
            if xlocked {
                lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;
            }
            xlocked = false;
            release_buffer::call(state.stack[cur].buffer);
            cur = state.stack[cur].parent.expect("retry_from_parent at root");
        }

        if xlog_rec_ptr_is_invalid(state.stack[cur].lsn) {
            state.stack[cur].buffer = read_buffer::call(r, state.stack[cur].blkno)?;
        }

        // Be optimistic: grab a shared lock first.
        if !xlocked {
            lock_buffer::call(state.stack[cur].buffer, GIST_SHARE)?;
            gistcheckpage(r.name(), state.stack[cur].buffer)?;
        }

        let page = buffer_get_page::call(mcx, state.stack[cur].buffer)?;
        state.stack[cur].lsn = if xlocked {
            page_get_lsn_bytes(&page)
        } else {
            buffer_get_lsn_atomic::call(state.stack[cur].buffer)?
        };

        // Fix an incomplete split (crashed inserter never inserted the downlink).
        if GistFollowRight(&page)? {
            if !xlocked {
                lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;
                lock_buffer::call(state.stack[cur].buffer, GIST_EXCLUSIVE)?;
                xlocked = true;
                let page2 = buffer_get_page::call(mcx, state.stack[cur].buffer)?;
                if !GistFollowRight(&page2)? {
                    continue;
                }
            }
            gistfixsplit(mcx, &mut state, cur, giststate, r, heap_rel)?;

            unlock_release_buffer::call(state.stack[cur].buffer);
            xlocked = false;
            cur = state.stack[cur].parent.expect("fixsplit at root");
            continue;
        }

        // Concurrent split / page deletion detection.
        let parent_lsn = state.stack[cur]
            .parent
            .map(|p| state.stack[p].lsn)
            .unwrap_or(0);
        if (state.stack[cur].blkno != GIST_ROOT_BLKNO
            && parent_lsn < gist_page_get_nsn(&page)?)
            || GistPageIsDeleted(&page)?
        {
            unlock_release_buffer::call(state.stack[cur].buffer);
            xlocked = false;
            cur = state.stack[cur].parent.expect("concurrent-split at root");
            continue;
        }

        if !GistPageIsLeaf(&page)? {
            // Internal page: walk down to the child with minimum penalty.
            let downlinkoffnum = gistchoose(mcx, r, &page, itup, giststate)?;
            let (childblkno, idxtuple) = {
                let pref = PageRef::new(&page)?;
                let id = PageGetItemId(&pref, downlinkoffnum)?;
                let it = PageGetItem(&pref, &id)?;
                (itup_block_number(it), it.to_vec())
            };

            if gist_tuple_is_invalid(&idxtuple) {
                return Err(ereport(ERROR)
                    .errmsg(alloc::format!(
                        "index \"{}\" contains an inner tuple marked as invalid",
                        r.name()
                    ))
                    .errdetail(
                        "This is caused by an incomplete page split at crash recovery before \
                         upgrading to PostgreSQL 9.1.",
                    )
                    .errhint("Please REINDEX it.")
                    .into_error());
            }

            // Check the child key is consistent with what we're inserting.
            let newtup = gistgetadjusted(mcx, r, &idxtuple, itup, giststate)?;
            if let Some(newtup) = newtup {
                if !xlocked {
                    lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;
                    lock_buffer::call(state.stack[cur].buffer, GIST_EXCLUSIVE)?;
                    xlocked = true;
                    let page2 = buffer_get_page::call(mcx, state.stack[cur].buffer)?;
                    if page_get_lsn_bytes(&page2) != state.stack[cur].lsn {
                        // page changed while unlocked, retry
                        continue;
                    }
                }

                // Update the tuple; may split, migrating the updated tuple.
                if gistinserttuple(mcx, &mut state, cur, giststate, r, heap_rel, &newtup, downlinkoffnum)? {
                    if state.stack[cur].blkno != GIST_ROOT_BLKNO {
                        unlock_release_buffer::call(state.stack[cur].buffer);
                        xlocked = false;
                        cur = state.stack[cur].parent.expect("rootsplit retry at root");
                    }
                    continue;
                }
            }
            lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;
            xlocked = false;

            // Descend to the chosen child.
            let item = GISTInsertStack {
                blkno: childblkno,
                buffer: INVALID_BUFFER,
                page: childblkno,
                lsn: 0,
                retry_from_parent: false,
                downlinkoffnum,
                parent: Some(cur),
            };
            state.stack.push(item);
            cur = state.stack.len() - 1;
        } else {
            // Leaf page: insert the new key (splitting if needed).
            if !xlocked {
                lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;
                lock_buffer::call(state.stack[cur].buffer, GIST_EXCLUSIVE)?;
                xlocked = true;
                let page2 = buffer_get_page::call(mcx, state.stack[cur].buffer)?;
                state.stack[cur].lsn = page_get_lsn_bytes(&page2);

                if state.stack[cur].blkno == GIST_ROOT_BLKNO {
                    // The only page that can become inner instead of leaf is the
                    // root; recheck it.
                    if !GistPageIsLeaf(&page2)? {
                        lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;
                        xlocked = false;
                        continue;
                    }
                } else {
                    let parent_lsn = state.stack[cur]
                        .parent
                        .map(|p| state.stack[p].lsn)
                        .unwrap_or(0);
                    if GistFollowRight(&page2)?
                        || parent_lsn < gist_page_get_nsn(&page2)?
                        || GistPageIsDeleted(&page2)?
                    {
                        unlock_release_buffer::call(state.stack[cur].buffer);
                        xlocked = false;
                        cur = state.stack[cur].parent.expect("leaf concurrent split at root");
                        continue;
                    }
                }
            }

            gistinserttuple(mcx, &mut state, cur, giststate, r, heap_rel, itup, INVALID_OFFSET_NUMBER)?;
            lock_buffer::call(state.stack[cur].buffer, GIST_UNLOCK)?;

            // Release any pins still held before exiting.
            let mut p = Some(cur);
            while let Some(idx) = p {
                release_buffer::call(state.stack[idx].buffer);
                p = state.stack[idx].parent;
            }
            break;
        }
    }

    Ok(())
}

/// `gistFindPath(r, child, &downlinkoffnum)` (gist.c:913): traverse the tree to
/// find the path from the root to `child`. Returns the parent chain (root-first)
/// as a `Vec<GISTInsertStack>` plus the downlink offset in the direct parent.
fn gistFindPath<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    child: BlockNumber,
) -> PgResult<(Vec<GISTInsertStack>, OffsetNumber)> {
    // Each fifo entry carries its own frame plus a parent index into `nodes`.
    struct Node {
        blkno: BlockNumber,
        downlinkoffnum: OffsetNumber,
        lsn: types_gist::GistNSN,
        parent: Option<usize>,
    }
    let mut nodes: Vec<Node> = Vec::new();
    nodes.push(Node {
        blkno: GIST_ROOT_BLKNO,
        downlinkoffnum: INVALID_OFFSET_NUMBER,
        lsn: 0,
        parent: None,
    });
    // fifo holds indices into `nodes`.
    let mut fifo: alloc::collections::VecDeque<usize> = alloc::collections::VecDeque::new();
    fifo.push_back(0);

    while let Some(top) = fifo.pop_front() {
        let buffer = read_buffer::call(r, nodes[top].blkno)?;
        lock_buffer::call(buffer, GIST_SHARE)?;
        gistcheckpage(r.name(), buffer)?;
        let page = buffer_get_page::call(mcx, buffer)?;

        if GistPageIsLeaf(&page)? {
            // Top-down scan: the rest of the queue must be leaves too.
            unlock_release_buffer::call(buffer);
            break;
        }
        debug_assert!(!GistPageIsDeleted(&page)?);

        nodes[top].lsn = buffer_get_lsn_atomic::call(buffer)?;

        if GistFollowRight(&page)? {
            unlock_release_buffer::call(buffer);
            return Err(ereport(ERROR)
                .errmsg_internal("concurrent GiST page split was incomplete")
                .into_error());
        }

        // Page split while we looked elsewhere: queue the right sibling first.
        let parent_lsn = nodes[top].parent.map(|p| nodes[p].lsn).unwrap_or(0);
        let rightlink = gist_page_rightlink(&page)?;
        if nodes[top].parent.is_some()
            && parent_lsn < gist_page_get_nsn(&page)?
            && rightlink != InvalidBlockNumber
        {
            let pidx = nodes[top].parent;
            nodes.push(Node {
                blkno: rightlink,
                downlinkoffnum: INVALID_OFFSET_NUMBER,
                lsn: 0,
                parent: pidx,
            });
            fifo.push_front(nodes.len() - 1);
        }

        let (maxoff, items): (OffsetNumber, Vec<(OffsetNumber, BlockNumber)>) = {
            let pref = PageRef::new(&page)?;
            let maxoff = PageGetMaxOffsetNumber(&pref);
            let mut items = Vec::new();
            let mut i = FIRST_OFFSET_NUMBER;
            while i <= maxoff {
                let id = PageGetItemId(&pref, i)?;
                let it = PageGetItem(&pref, &id)?;
                items.push((i, itup_block_number(it)));
                i += 1;
            }
            (maxoff, items)
        };
        let _ = maxoff;

        let mut found: Option<OffsetNumber> = None;
        for (i, blkno) in items {
            if blkno == child {
                found = Some(i);
                break;
            } else {
                nodes.push(Node {
                    blkno,
                    downlinkoffnum: i,
                    lsn: 0,
                    parent: Some(top),
                });
                fifo.push_back(nodes.len() - 1);
            }
        }

        if let Some(i) = found {
            unlock_release_buffer::call(buffer);
            // Reconstruct the root-first parent chain from `top`.
            let mut chain_idx = Vec::new();
            let mut p = Some(top);
            while let Some(idx) = p {
                chain_idx.push(idx);
                p = nodes[idx].parent;
            }
            chain_idx.reverse();
            let mut chain: Vec<GISTInsertStack> = Vec::with_capacity(chain_idx.len());
            for (pos, &idx) in chain_idx.iter().enumerate() {
                chain.push(GISTInsertStack {
                    blkno: nodes[idx].blkno,
                    buffer: INVALID_BUFFER,
                    page: nodes[idx].blkno,
                    lsn: nodes[idx].lsn,
                    retry_from_parent: false,
                    downlinkoffnum: nodes[idx].downlinkoffnum,
                    parent: if pos == 0 { None } else { Some(pos - 1) },
                });
            }
            return Ok((chain, i));
        }

        unlock_release_buffer::call(buffer);
    }

    Err(ereport(ERROR)
        .errmsg_internal(alloc::format!(
            "failed to re-find parent of a page in index \"{}\", block {child}",
            r.name()
        ))
        .into_error())
}

/// `gistformdownlink(rel, buf, giststate, stack, is_build)` (gist.c:1139): form a
/// downlink pointer (the union of all keys on `buf`) for the page in `buf`.
fn gistformdownlink<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: Buffer,
    giststate: &GISTSTATE<'mcx>,
    state: &mut GISTInsertState,
    stack_idx: usize,
    is_build: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let page = buffer_get_page::call(mcx, buf)?;
    let mut downlink: Option<PgVec<'mcx, u8>> = None;

    {
        let pref = PageRef::new(&page)?;
        let maxoff = PageGetMaxOffsetNumber(&pref);
        let mut offset = FIRST_OFFSET_NUMBER;
        while offset <= maxoff {
            let id = PageGetItemId(&pref, offset)?;
            let ituple = PageGetItem(&pref, &id)?;
            match downlink.as_ref() {
                None => {
                    downlink = Some(mcx::slice_in(mcx, &ituple[..index_tuple_size(ituple)])?);
                }
                Some(dl) => {
                    let dl_slice = dl.as_slice();
                    if let Some(newdl) =
                        gistgetadjusted(mcx, rel, dl_slice, &ituple[..index_tuple_size(ituple)], giststate)?
                    {
                        downlink = Some(newdl);
                    }
                }
            }
            offset += 1;
        }
    }

    // Completely-empty page: use the original page's downlink from the parent.
    let mut downlink = match downlink {
        Some(d) => d,
        None => {
            let parent = state.stack[stack_idx].parent.expect("formdownlink: no parent");
            lock_buffer::call(state.stack[parent].buffer, GIST_EXCLUSIVE)?;
            gistFindCorrectParent(mcx, rel, state, stack_idx, is_build)?;
            let parent = state.stack[stack_idx].parent.expect("formdownlink: no parent");
            let off = state.stack[stack_idx].downlinkoffnum;
            let pp = buffer_get_page::call(mcx, state.stack[parent].buffer)?;
            let dl = {
                let pref = PageRef::new(&pp)?;
                let id = PageGetItemId(&pref, off)?;
                let it = PageGetItem(&pref, &id)?;
                mcx::slice_in(mcx, &it[..index_tuple_size(it)])?
            };
            lock_buffer::call(state.stack[parent].buffer, GIST_UNLOCK)?;
            dl
        }
    };

    itup_set_block_number(&mut downlink, buffer_get_block_number::call(buf));
    gist_tuple_set_valid(&mut downlink);
    Ok(downlink)
}

/// `gistFindCorrectParent(r, child, is_build)` (gist.c:1026): update the stack so
/// that `child`'s parent is correct. The parent must be exclusively locked on
/// entry and exit (though it may be a different page).
fn gistFindCorrectParent<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    state: &mut GISTInsertState,
    child_idx: usize,
    is_build: bool,
) -> PgResult<()> {
    let parent_idx = state.stack[child_idx].parent.expect("findcorrectparent: root");
    gistcheckpage(r.name(), state.stack[parent_idx].buffer)?;
    let child_blkno = state.stack[child_idx].blkno;

    // Is the downlink still where it was?
    {
        let pp = buffer_get_page::call(mcx, state.stack[parent_idx].buffer)?;
        let pref = PageRef::new(&pp)?;
        let maxoff = PageGetMaxOffsetNumber(&pref);
        let dlo = state.stack[child_idx].downlinkoffnum;
        if dlo != INVALID_OFFSET_NUMBER && dlo <= maxoff {
            let id = PageGetItemId(&pref, dlo)?;
            let it = PageGetItem(&pref, &id)?;
            if itup_block_number(it) == child_blkno {
                return Ok(()); // still there
            }
        }
    }
    let _ = is_build;

    // Scan to re-find the downlink, following rightlinks if the page was split.
    loop {
        let pp = buffer_get_page::call(mcx, state.stack[parent_idx].buffer)?;
        let found = {
            let pref = PageRef::new(&pp)?;
            let maxoff = PageGetMaxOffsetNumber(&pref);
            let mut found: Option<OffsetNumber> = None;
            let mut i = FIRST_OFFSET_NUMBER;
            while i <= maxoff {
                let id = PageGetItemId(&pref, i)?;
                let it = PageGetItem(&pref, &id)?;
                if itup_block_number(it) == child_blkno {
                    found = Some(i);
                    break;
                }
                i += 1;
            }
            found
        };
        if let Some(i) = found {
            state.stack[child_idx].downlinkoffnum = i;
            return Ok(());
        }

        // Move to the right sibling.
        let rightlink = gist_page_rightlink(&pp)?;
        state.stack[parent_idx].blkno = rightlink;
        state.stack[parent_idx].downlinkoffnum = INVALID_OFFSET_NUMBER;
        unlock_release_buffer::call(state.stack[parent_idx].buffer);
        if rightlink == InvalidBlockNumber {
            break; // end of chain, the root was split
        }
        state.stack[parent_idx].buffer = read_buffer::call(r, rightlink)?;
        lock_buffer::call(state.stack[parent_idx].buffer, GIST_EXCLUSIVE)?;
        gistcheckpage(r.name(), state.stack[parent_idx].buffer)?;
    }

    // Search the whole tree to find the parent. First release the old grandparent
    // chain (child->parent was already released above).
    {
        let mut p = state.stack[parent_idx].parent;
        while let Some(idx) = p {
            release_buffer::call(state.stack[idx].buffer);
            p = state.stack[idx].parent;
        }
    }

    // Find a fresh path and splice it onto the stack.
    let (mut chain, downlinkoffnum) = gistFindPath(mcx, r, child_blkno)?;
    // Read all buffers (no lock / gistcheckpage here, per C).
    for frame in chain.iter_mut() {
        frame.buffer = read_buffer::call(r, frame.blkno)?;
        frame.page = frame.blkno;
    }

    // Splice chain into state.stack: append frames, fix parent indices, then set
    // child's parent to the new direct parent.
    let base = state.stack.len();
    let chain_len = chain.len();
    for (pos, mut frame) in chain.into_iter().enumerate() {
        frame.parent = if pos == 0 { None } else { Some(base + pos - 1) };
        state.stack.push(frame);
    }
    let new_parent = base + chain_len - 1;
    state.stack[child_idx].parent = Some(new_parent);
    state.stack[child_idx].downlinkoffnum = downlinkoffnum;

    // Recurse with the new direct parent exclusively locked.
    lock_buffer::call(state.stack[new_parent].buffer, GIST_EXCLUSIVE)?;
    gistFindCorrectParent(mcx, r, state, child_idx, is_build)
}

/// `gistfixsplit(state, giststate)` (gist.c:1199): complete the incomplete split
/// of `state->stack->page` by reading the rightlink chain, forming downlinks, and
/// inserting them into the parent.
fn gistfixsplit<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut GISTInsertState,
    stack_idx: usize,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    _heap_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let _ = ereport(types_error::error::LOG).errmsg(alloc::format!(
        "fixing incomplete split in index \"{}\", block {}",
        r.name(),
        state.stack[stack_idx].blkno
    ));

    let mut buf = state.stack[stack_idx].buffer;
    let mut splitinfo: Vec<GISTPageSplitInfo<'mcx>> = Vec::new();

    loop {
        let page = buffer_get_page::call(mcx, buf)?;
        let downlink = gistformdownlink(mcx, r, buf, giststate, state, stack_idx, state.is_build)?;
        splitinfo.push(GISTPageSplitInfo { buf, downlink });

        if GistFollowRight(&page)? {
            let rl = gist_page_rightlink(&page)?;
            buf = read_buffer::call(r, rl)?;
            lock_buffer::call(buf, GIST_EXCLUSIVE)?;
        } else {
            break;
        }
    }

    gistfinishsplit(mcx, state, stack_idx, giststate, r, _heap_rel, splitinfo, false)
}

/// `gistinserttuple(state, stack, giststate, tuple, oldoffnum)` (gist.c:1259):
/// insert or replace a single tuple in `stack->buffer`. Returns whether the page
/// was split.
#[allow(clippy::too_many_arguments)]
fn gistinserttuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut GISTInsertState,
    stack_idx: usize,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    tuple: &[u8],
    oldoffnum: OffsetNumber,
) -> PgResult<bool> {
    gistinserttuples(
        mcx,
        state,
        stack_idx,
        giststate,
        r,
        heap_rel,
        &[tuple],
        oldoffnum,
        INVALID_BUFFER,
        INVALID_BUFFER,
        false,
        false,
    )
}

/// `gistinserttuples(...)` (gist.c:1293): the extended workhorse — insert/replace
/// possibly-multiple tuples, recursing up to update parent downlinks on a split.
/// See the C comment for the locking contract.
#[allow(clippy::too_many_arguments)]
fn gistinserttuples<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut GISTInsertState,
    stack_idx: usize,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    tuples: &[&[u8]],
    oldoffnum: OffsetNumber,
    leftchild: Buffer,
    rightchild: Buffer,
    unlockbuf: bool,
    unlockleftchild: bool,
) -> PgResult<bool> {
    // rw-conflict check just before modifying the page.
    check_for_serializable_conflict_in_page::call(
        r.rd_id,
        buffer_get_block_number::call(state.stack[stack_idx].buffer),
    )?;

    let res = gistplacetopage(PlaceToPage {
        rel: r,
        freespace: state.freespace,
        giststate,
        buffer: state.stack[stack_idx].buffer,
        itup: tuples,
        oldoffnum,
        left_child_buf: leftchild,
        mark_follow_right: true,
        heap_rel,
        is_build: state.is_build,
    })?;
    let is_split = res.is_split;
    let mut splitinfo = res.split_info;

    // Release locks on the child pages before recursing up.
    if BufferIsValid(rightchild) {
        unlock_release_buffer::call(rightchild);
    }
    if BufferIsValid(leftchild) && unlockleftchild {
        lock_buffer::call(leftchild, GIST_UNLOCK)?;
    }

    if !splitinfo.is_empty() {
        gistfinishsplit(
            mcx,
            state,
            stack_idx,
            giststate,
            r,
            heap_rel,
            core::mem::take(&mut splitinfo),
            unlockbuf,
        )?;
    } else if unlockbuf {
        lock_buffer::call(state.stack[stack_idx].buffer, GIST_UNLOCK)?;
    }

    Ok(is_split)
}

/// `gistfinishsplit(state, stack, giststate, splitinfo, unlockbuf)` (gist.c:1353):
/// finish an incomplete split by inserting/updating downlinks in the parent.
#[allow(clippy::too_many_arguments)]
fn gistfinishsplit<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut GISTInsertState,
    stack_idx: usize,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    splitinfo: Vec<GISTPageSplitInfo<'mcx>>,
    unlockbuf: bool,
) -> PgResult<()> {
    debug_assert!(splitinfo.len() >= 2);

    let parent_idx = state.stack[stack_idx].parent.expect("finishsplit: root parent");
    lock_buffer::call(state.stack[parent_idx].buffer, GIST_EXCLUSIVE)?;

    // Insert downlinks for the siblings right-to-left until two remain.
    let mut pos = splitinfo.len() as i32 - 1;
    while pos > 1 {
        let right = &splitinfo[pos as usize];
        let left = &splitinfo[(pos - 1) as usize];
        gistFindCorrectParent(mcx, r, state, stack_idx, state.is_build)?;
        let parent_idx = state.stack[stack_idx].parent.expect("finishsplit: parent");
        let right_dl = right.downlink.as_slice();
        if gistinserttuples(
            mcx,
            state,
            parent_idx,
            giststate,
            r,
            heap_rel,
            &[right_dl],
            INVALID_OFFSET_NUMBER,
            left.buf,
            right.buf,
            false,
            false,
        )? {
            // parent page was split; the existing downlink might have moved.
            state.stack[stack_idx].downlinkoffnum = INVALID_OFFSET_NUMBER;
        }
        pos -= 1;
    }

    let right = &splitinfo[1];
    let left = &splitinfo[0];
    let tuples: [&[u8]; 2] = [left.downlink.as_slice(), right.downlink.as_slice()];
    gistFindCorrectParent(mcx, r, state, stack_idx, state.is_build)?;
    let parent_idx = state.stack[stack_idx].parent.expect("finishsplit: parent2");
    let downlinkoffnum = state.stack[stack_idx].downlinkoffnum;
    let _ = gistinserttuples(
        mcx,
        state,
        parent_idx,
        giststate,
        r,
        heap_rel,
        &tuples,
        downlinkoffnum,
        left.buf,
        right.buf,
        true,      // unlock parent
        unlockbuf, // unlock stack->buffer if caller wants
    )?;

    // The downlink might have moved (update = remove + re-insert).
    state.stack[stack_idx].downlinkoffnum = INVALID_OFFSET_NUMBER;
    debug_assert!(left.buf == state.stack[stack_idx].buffer);

    // Tell the caller to retry from the parent (re-check which page fits).
    state.stack[stack_idx].retry_from_parent = true;
    Ok(())
}

/// `XLogRecPtrIsInvalid(lsn)` (xlogdefs.h).
#[inline]
fn xlog_rec_ptr_is_invalid(lsn: XLogRecPtr) -> bool {
    lsn == 0
}
