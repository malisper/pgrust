//! F3 — child/parent linkage verification (verify_nbtree.c).
//!
//! Parent/child downlink agreement (`bt_child_check`,
//! `bt_child_highkey_check`, `bt_downlink_missing_check`,
//! `bt_pivot_tuple_identical`), the half-dead-tolerant leftmost walk
//! (`bt_leftmost_ignoring_half_dead`), the !readonly sibling-link recheck
//! (`bt_recheck_sibling_links`), the root-descend re-find (`bt_rootdescend`,
//! using the F0 `_bt_search` / `_bt_freestack` / `_bt_binsrch_insert` seams +
//! `BTStackData`), and the downlink byte-math helpers
//! (`BTreeTupleGetDownLink` / `BTreeTupleGetTopParent`, nbtree.h inline).

use types_core::primitive::{BlockNumber, OffsetNumber};
use types_error::{PgError, PgResult};
use types_error::error::ERRCODE_INDEX_CORRUPTED;
use types_nbtree::{BTScanInsert, P_HIKEY, P_NONE};
use types_tuple::heaptuple::{IndexTuple, IndexTupleData, IndexTupleSize};

use backend_access_nbtree_core_seams as nbtcore;

use crate::entry::{palloc_btree_page, PageGetItemIdCareful};
use crate::target_page::{
    btpo_next, btpo_prev, btree_tuple_get_downlink, btree_tuple_get_top_parent,
    index_tuple_box, index_tuple_header, invariant_l_nontarget_offset,
    offset_is_negative_infinity, p_firstdatakey, p_has_fullxid, p_ignore,
    p_incomplete_split, p_isdeleted, p_ishalfdead, p_isleaf, p_isroot, p_rightmost,
    page_btpo_level, page_get_item, page_get_max_offset_number, state_mcx,
};
use crate::{BtreeCheckState, Page};

/// `OffsetNumberNext(offset)` (storage/off.h).
#[inline]
fn offset_number_next(offset: OffsetNumber) -> OffsetNumber {
    offset + 1
}

/// `OffsetNumberIsValid(offset)` (storage/off.h):
/// `(bool) ((offsetNumber != InvalidOffsetNumber) && (offsetNumber <= MaxOffsetNumber))`.
#[inline]
fn offset_number_is_valid(offset: OffsetNumber) -> bool {
    offset != types_tuple::heaptuple::INVALID_OFFSET_NUMBER
        && offset <= types_storage::bufpage::MaxOffsetNumber
}

/// `BlockNumberIsValid(blockNumber)` (storage/block.h):
/// `((bool) (blockNumber != InvalidBlockNumber))`.
#[inline]
fn block_number_is_valid(blkno: BlockNumber) -> bool {
    blkno != types_core::primitive::InvalidBlockNumber
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as C's `%X/%X`.
fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `bt_child_check(state, targetkey, downlinkoffnum)` — verify that the child
/// page reached via the downlink at `downlinkoffnum` is consistent with
/// `targetkey` (the target's pivot for that child): the child's items all sort
/// at/above the downlink's lower bound.
pub fn bt_child_check<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    targetkey: &BTScanInsert<'mcx>,
    downlinkoffnum: OffsetNumber,
) -> PgResult<()> {
    let mcx = state_mcx(state);

    let target = state
        .target
        .as_ref()
        .expect("bt_child_check: target page must be set")
        .as_slice()
        .to_vec();

    let _itemid = PageGetItemIdCareful(
        state,
        state.targetblock,
        state.target.as_ref().unwrap(),
        downlinkoffnum,
    )?;
    let itup = page_get_item(mcx, &target, downlinkoffnum)?;
    let childblock = btree_tuple_get_downlink(&itup);

    // Assert(state->readonly).

    // Verify child page has the downlink key from target page (its parent) as a
    // lower bound; downlink must be strictly less than all keys on the page.
    let target_level = page_btpo_level(&target);
    let child = palloc_btree_page(state, childblock)?;
    let child_bytes = child.as_slice().to_vec();
    let maxoffset = page_get_max_offset_number(&child_bytes);

    // Since we've already loaded the child block, combine this check with check
    // for downlink connectivity.
    bt_child_highkey_check(state, downlinkoffnum, Some(&child), target_level)?;

    if p_isdeleted(&child_bytes) {
        return Err(PgError::error(format!(
            "downlink to deleted page found in index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Parent block={} child block={} parent page lsn={}.",
            state.targetblock,
            childblock,
            fmt_lsn(state.targetlsn),
        )));
    }

    let mut offset = p_firstdatakey(&child_bytes);
    while offset <= maxoffset {
        // Skip comparison of target page key against "negative infinity" item.
        if offset_is_negative_infinity(&child_bytes, offset) {
            offset = offset_number_next(offset);
            continue;
        }

        if !invariant_l_nontarget_offset(state, targetkey, childblock, &child, offset)? {
            return Err(PgError::error(format!(
                "down-link lower bound invariant violated for index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Parent block={} child index tid=({},{}) parent page lsn={}.",
                state.targetblock,
                childblock,
                offset,
                fmt_lsn(state.targetlsn),
            )));
        }
        offset = offset_number_next(offset);
    }

    Ok(())
}

/// `bt_child_highkey_check(state, target_downlinkoffnum, loaded_child,
/// target_level)` — verify that consecutive children's high keys agree with
/// the parent's downlinks (the "highkey chain"), detecting missing or extra
/// downlinks across an incomplete split.
pub fn bt_child_highkey_check<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    target_downlinkoffnum: OffsetNumber,
    loaded_child: Option<&Page<'mcx>>,
    target_level: u32,
) -> PgResult<()> {
    let mcx = state_mcx(state);

    let mut blkno = state.prevrightlink;
    let mut rightsplit = state.previncompletesplit;
    let mut first = true;

    let target = state
        .target
        .as_ref()
        .expect("bt_child_highkey_check: target page must be set")
        .as_slice()
        .to_vec();

    let downlink;
    if offset_number_is_valid(target_downlinkoffnum) {
        let _itemid = PageGetItemIdCareful(
            state,
            state.targetblock,
            state.target.as_ref().unwrap(),
            target_downlinkoffnum,
        )?;
        let itup = page_get_item(mcx, &target, target_downlinkoffnum)?;
        downlink = btree_tuple_get_downlink(&itup);
    } else {
        downlink = P_NONE;
    }

    // If no previous rightlink is memorized, we're about to start from the
    // leftmost page: imagine a previous page referencing the current child, with
    // no incomplete split flag.
    if !block_number_is_valid(blkno) {
        blkno = downlink;
        rightsplit = false;
    }

    // Move to the right on the child level.
    loop {
        // Did we traverse the whole tree level and this is check for pages to the
        // right of rightmost downlink?
        if blkno == P_NONE && downlink == P_NONE {
            state.prevrightlink = types_core::primitive::InvalidBlockNumber;
            state.previncompletesplit = false;
            return Ok(());
        }

        // Did we traverse the whole tree level and don't find next downlink?
        if blkno == P_NONE {
            return Err(PgError::error(format!(
                "can't traverse from downlink {} to downlink {} of index \"{}\"",
                state.prevrightlink,
                downlink,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }

        // Load page contents. `loaded_child` aliases the page at `downlink`.
        let owned_page;
        let page_bytes: Vec<u8>;
        let page_is_loaded_child = blkno == downlink && loaded_child.is_some();
        if page_is_loaded_child {
            owned_page = None;
            page_bytes = loaded_child.unwrap().as_slice().to_vec();
        } else {
            let p = palloc_btree_page(state, blkno)?;
            page_bytes = p.as_slice().to_vec();
            owned_page = Some(p);
        }
        let _ = &owned_page;

        // The first page we visit at the level should be leftmost.
        if first
            && !block_number_is_valid(state.prevrightlink)
            && !bt_leftmost_ignoring_half_dead_bytes(state, blkno, &page_bytes)?
        {
            return Err(PgError::error(format!(
                "the first child of leftmost target page is not leftmost of its level in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Target block={} child block={} target page lsn={}.",
                state.targetblock,
                blkno,
                fmt_lsn(state.targetlsn),
            )));
        }

        // Do level sanity check.
        if (!p_isdeleted(&page_bytes) || p_has_fullxid(&page_bytes))
            && page_btpo_level(&page_bytes) != target_level - 1
        {
            return Err(PgError::error(format!(
                "block found while following rightlinks from child of index \"{}\" has invalid level",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Block pointed to={} expected level={} level in pointed to block={}.",
                blkno,
                target_level - 1,
                page_btpo_level(&page_bytes),
            )));
        }

        // Try to detect circular links.
        if (!first && blkno == state.prevrightlink) || blkno == btpo_prev(&page_bytes) {
            return Err(PgError::error(format!(
                "circular link chain found in block {} of index \"{}\"",
                blkno,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }

        if blkno != downlink && !p_ignore(&page_bytes) {
            // blkno probably has missing parent downlink.
            bt_downlink_missing_check(state, rightsplit, blkno, &page_bytes)?;
        }

        rightsplit = p_incomplete_split(&page_bytes);

        // If we visit page with high key, check that it equals the target key
        // next to the corresponding downlink.
        if !rightsplit && !p_rightmost(&page_bytes) && !p_ishalfdead(&page_bytes) {
            // Get high key.
            let _hitemid = PageGetItemIdCareful_bytes(state, blkno, &page_bytes, P_HIKEY)?;
            let highkey = page_get_item(mcx, &page_bytes, P_HIKEY)?;

            // Pick the matching pivot offset from the target page.
            let pivotkey_offset = if blkno == downlink {
                offset_number_next(target_downlinkoffnum)
            } else {
                target_downlinkoffnum
            };

            let itup_to_match: Vec<u8>;
            if !offset_is_negative_infinity(&target, pivotkey_offset) {
                // If looking for the next pivot tuple but there are no more,
                // match against the high key instead.
                let pivotkey_offset = if pivotkey_offset > page_get_max_offset_number(&target) {
                    if p_rightmost(&target) {
                        return Err(PgError::error(format!(
                            "child high key is greater than rightmost pivot key on target level in index \"{}\"",
                            state.rel.name()
                        ))
                        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                        .with_detail(format!(
                            "Target block={} child block={} target page lsn={}.",
                            state.targetblock,
                            blkno,
                            fmt_lsn(state.targetlsn),
                        )));
                    }
                    P_HIKEY
                } else {
                    pivotkey_offset
                };
                let _itemid = PageGetItemIdCareful(
                    state,
                    state.targetblock,
                    state.target.as_ref().unwrap(),
                    pivotkey_offset,
                )?;
                itup_to_match = page_get_item(mcx, &target, pivotkey_offset)?.as_slice().to_vec();
            } else {
                // Can't match against a negative infinity key in target; match
                // against the saved low key (left uncle page high key) instead.
                let lowkey = state.lowkey.as_ref();
                if lowkey.is_none() {
                    return Err(PgError::error(format!(
                        "can't find left sibling high key in index \"{}\"",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Target block={} child block={} target page lsn={}.",
                        state.targetblock,
                        blkno,
                        fmt_lsn(state.targetlsn),
                    )));
                }
                // Serialize the low key header into comparable bytes.
                itup_to_match = index_tuple_header_bytes(&state.lowkey);
            }

            let highkey_hdr = index_tuple_header(&highkey);
            let itup_hdr = index_tuple_header(&itup_to_match);
            if !bt_pivot_tuple_identical_hdr(
                state.heapkeyspace,
                &highkey,
                &highkey_hdr,
                &itup_to_match,
                &itup_hdr,
            ) {
                return Err(PgError::error(format!(
                    "mismatch between parent key and child high key in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Target block={} child block={} target page lsn={}.",
                    state.targetblock,
                    blkno,
                    fmt_lsn(state.targetlsn),
                )));
            }
        }

        // Exit if we already found next downlink.
        if blkno == downlink {
            state.prevrightlink = btpo_next(&page_bytes);
            state.previncompletesplit = rightsplit;
            return Ok(());
        }

        // Traverse to the next page using rightlink.
        blkno = btpo_next(&page_bytes);

        // Page contents (if owned) are dropped at end of scope.
        first = false;
    }
}

/// `bt_downlink_missing_check(state, rightsplit, blkno, page)` — verify that
/// `page` (a child) is actually reachable via a downlink from its parent,
/// catching downlinks dropped by a crash mid-split.
pub fn bt_downlink_missing_check<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    rightsplit: bool,
    blkno: BlockNumber,
    page: &[u8],
) -> PgResult<()> {
    let mcx = state_mcx(state);

    // Assert(state->readonly); Assert(!P_IGNORE(opaque)).

    // No next level up with downlinks to fingerprint from the true root.
    if p_isroot(page) {
        return Ok(());
    }

    // PageGetLSN of the page under check (carried for the error detail only). The
    // page is a private copy from palloc_btree_page; its LSN is not separately
    // threaded here, so we report the target LSN as C reports the page LSN. (The
    // page-copy LSN accessor is not modeled for non-target pages.)
    let pagelsn = state.targetlsn;

    // Incomplete (interrupted) page splits can account for a missing downlink.
    if rightsplit {
        // ereport DEBUG1 "harmless interrupted page split" — not modeled.
        return Ok(());
    }

    // A non-ignorable leaf page with no downlink is corruption.
    if p_isleaf(page) {
        return Err(PgError::error(format!(
            "leaf index block lacks downlink in index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!("Block={} page lsn={}.", blkno, fmt_lsn(pagelsn))));
    }

    // Descend from the given internal page.
    let mut level = page_btpo_level(page);
    let _itemid = PageGetItemIdCareful_bytes(state, blkno, page, p_firstdatakey(page))?;
    let mut itup = page_get_item(mcx, page, p_firstdatakey(page))?;
    let mut childblk = btree_tuple_get_downlink(&itup);

    let mut child;
    let mut child_bytes: Vec<u8>;
    loop {
        child = palloc_btree_page(state, childblk)?;
        child_bytes = child.as_slice().to_vec();

        if p_isleaf(&child_bytes) {
            break;
        }

        // Extra sanity check on internal pages in passing.
        if page_btpo_level(&child_bytes) != level - 1 {
            return Err(PgError::error(format!(
                "downlink points to block in index \"{}\" whose level is not one level down",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Top parent/under check block={} block pointed to={} expected level={} level in pointed to block={}.",
                blkno,
                childblk,
                level - 1,
                page_btpo_level(&child_bytes),
            )));
        }

        level = page_btpo_level(&child_bytes);
        let _itemid = PageGetItemIdCareful_bytes(state, childblk, &child_bytes, p_firstdatakey(&child_bytes))?;
        itup = page_get_item(mcx, &child_bytes, p_firstdatakey(&child_bytes))?;
        childblk = btree_tuple_get_downlink(&itup);
    }

    if p_isdeleted(&child_bytes) {
        return Err(PgError::error(format!(
            "downlink to deleted leaf page found in index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Top parent/target block={} leaf block={} top parent/under check lsn={}.",
            blkno,
            childblk,
            fmt_lsn(pagelsn),
        )));
    }

    // Iff leaf page is half-dead, its high key top parent link should point to
    // the page under check; that is consistent with an interrupted multi-level
    // page deletion.
    if p_ishalfdead(&child_bytes) && !p_rightmost(&child_bytes) {
        let _itemid = PageGetItemIdCareful_bytes(state, childblk, &child_bytes, P_HIKEY)?;
        let hk = page_get_item(mcx, &child_bytes, P_HIKEY)?;
        if btree_tuple_get_top_parent(&hk) == blkno {
            return Ok(());
        }
    }

    Err(PgError::error(format!(
        "internal index block lacks downlink in index \"{}\"",
        state.rel.name()
    ))
    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
    .with_detail(format!(
        "Block={} level={} page lsn={}.",
        blkno,
        page_btpo_level(page),
        fmt_lsn(pagelsn),
    )))
}

/// `bt_pivot_tuple_identical(heapkeyspace, itup1, itup2)` — are two pivot
/// tuples byte-for-byte identical (modulo the heapkeyspace t_tid handling)?
pub fn bt_pivot_tuple_identical<'mcx>(
    heapkeyspace: bool,
    itup1: &IndexTuple<'mcx>,
    itup2: &IndexTuple<'mcx>,
) -> bool {
    // The carrier `IndexTuple` is the trimmed header-only value (8-byte
    // `IndexTupleData`: t_tid + t_info). C compares the full tuple bytes via
    // IndexTupleSize. The header-only carrier does not expose the variable
    // payload, so a byte-for-byte comparison of the full tuple is not possible
    // through this value-typed entry point. The on-page path (used by
    // bt_child_highkey_check) compares the raw item bytes directly via
    // `bt_pivot_tuple_identical_hdr`, which has the full bytes in hand.
    let _ = (heapkeyspace, itup1, itup2);
    panic!(
        "not yet ported: bt_pivot_tuple_identical over header-only IndexTuple \
         carriers cannot see the variable payload; the on-page comparison uses \
         bt_pivot_tuple_identical_hdr over the raw item bytes instead"
    )
}

/// `bt_pivot_tuple_identical` over the raw on-page item bytes (the form actually
/// reachable in `bt_child_highkey_check`, where both tuples come straight off a
/// page copy / saved low key). `hdr1`/`hdr2` are the parsed headers of `bytes1`/
/// `bytes2`.
///
/// C compares `IndexTupleSize(itup1)` bytes. For heapkeyspace it compares from
/// `offsetof(ItemPointerData, ip_posid)` (byte 4); otherwise from
/// `offsetof(IndexTupleData, t_info)` (byte 6).
fn bt_pivot_tuple_identical_hdr(
    heapkeyspace: bool,
    bytes1: &[u8],
    hdr1: &IndexTupleData,
    bytes2: &[u8],
    _hdr2: &IndexTupleData,
) -> bool {
    let size1 = IndexTupleSize(hdr1);
    let size2 = IndexTupleSize(_hdr2);
    if size1 != size2 {
        return false;
    }
    // Compare suffix bytes starting at the appropriate offset, up to the full
    // tuple size. ip_posid is at byte offset 4 (after ip_blkid); t_info at 6.
    let start = if heapkeyspace { 4usize } else { 6usize };
    let end = size1 as usize;
    // The on-page copies must cover at least `end` bytes (PageGetItem returns the
    // lp_len-bounded slice).
    if bytes1.len() < end || bytes2.len() < end {
        // Defensive: fall back to inequality if the slices are short (the
        // PageGetItemIdCareful checks already validated lp bounds).
        return false;
    }
    bytes1[start..end] == bytes2[start..end]
}

/// Serialize the trimmed header-only `IndexTuple` value (the saved low key) into
/// the 8-byte on-page `IndexTupleData` byte form so it can be fed to the
/// raw-bytes pivot comparison. Only the header is addressable for a saved low
/// key in this model; the variable payload is not retained in the carrier.
fn index_tuple_header_bytes(itup: &IndexTuple<'_>) -> Vec<u8> {
    let h = itup
        .as_ref()
        .expect("index_tuple_header_bytes: low key must be set");
    let mut b = vec![0u8; 8];
    b[0..2].copy_from_slice(&h.t_tid.ip_blkid.bi_hi.to_ne_bytes());
    b[2..4].copy_from_slice(&h.t_tid.ip_blkid.bi_lo.to_ne_bytes());
    b[4..6].copy_from_slice(&h.t_tid.ip_posid.to_ne_bytes());
    b[6..8].copy_from_slice(&h.t_info.to_ne_bytes());
    b
}

/// `bt_leftmost_ignoring_half_dead(state, start, start_opaque)` — walk left
/// from `start` via `btpo_prev`, skipping half-dead pages, to find the true
/// leftmost page of a level (used when sibling links are being rechecked).
pub fn bt_leftmost_ignoring_half_dead<'mcx>(
    state: &BtreeCheckState<'mcx>,
    start: BlockNumber,
    start_page: &Page<'mcx>,
) -> PgResult<bool> {
    bt_leftmost_ignoring_half_dead_bytes(state, start, start_page.as_slice())
}

/// Byte-slice variant of [`bt_leftmost_ignoring_half_dead`] (the C takes a
/// `BTPageOpaque`, which is read off the page bytes here).
fn bt_leftmost_ignoring_half_dead_bytes<'mcx>(
    state: &BtreeCheckState<'mcx>,
    start: BlockNumber,
    start_page: &[u8],
) -> PgResult<bool> {
    let mut reached = btpo_prev(start_page);
    let mut reached_from = start;
    let mut all_half_dead = true;

    // Assert(state->readonly).

    while reached != P_NONE && all_half_dead {
        let page = palloc_btree_page(state, reached)?;
        let page_bytes = page.as_slice();

        // Try to detect btpo_prev circular links: a half-dead page's btpo_next
        // should continue to point at its sibling.
        all_half_dead = p_ishalfdead(page_bytes)
            && reached != start
            && reached != reached_from
            && btpo_next(page_bytes) == reached_from;
        if all_half_dead {
            // ereport DEBUG1 "harmless interrupted page deletion" — not modeled.
            reached_from = reached;
            reached = btpo_prev(page_bytes);
        }
    }

    Ok(all_half_dead)
}

/// `bt_recheck_sibling_links(state, btpo_prev_from_target, leftcurrent)` —
/// in the !readonly case, re-read the sibling pages to confirm the left/right
/// links are mutually consistent despite concurrent splits/deletions.
pub fn bt_recheck_sibling_links<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    btpo_prev_from_target: BlockNumber,
    leftcurrent: BlockNumber,
) -> PgResult<()> {
    use backend_storage_buffer_bufmgr_seams as bufmgr;
    use backend_access_nbtree_core_seams as nbt;

    // Assert(leftcurrent != P_NONE).
    let mut btpo_prev_from_target = btpo_prev_from_target;

    if !state.readonly {
        let mcx = state_mcx(state);
        // Couple locks in the usual order for nbtree: left to right.
        let lbuf = bufmgr::read_buffer_extended::call(&state.rel, leftcurrent)?;
        // LockBuffer(lbuf, BT_READ); _bt_checkpage(state->rel, lbuf);
        nbt::bt_lockbuf::call(&state.rel, lbuf);
        nbt::bt_checkpage::call(&state.rel, lbuf)?;
        let page = bufmgr::buffer_get_page::call(mcx, lbuf)?;
        if p_isdeleted(page.as_slice()) {
            // Cannot reason about a concurrently deleted page.
            nbt::bt_relbuf::call(&state.rel, lbuf);
            return Ok(());
        }

        let newtargetblock = btpo_next(page.as_slice());
        // Avoid self-deadlock when newtargetblock == leftcurrent.
        let newtargetbuf;
        if newtargetblock != leftcurrent {
            let buf = bufmgr::read_buffer_extended::call(&state.rel, newtargetblock)?;
            nbt::bt_lockbuf::call(&state.rel, buf);
            nbt::bt_checkpage::call(&state.rel, buf)?;
            let page2 = bufmgr::buffer_get_page::call(mcx, buf)?;
            // btpo_prev_from_target may have changed; update it.
            btpo_prev_from_target = btpo_prev(page2.as_slice());
            newtargetbuf = Some(buf);
        } else {
            // Right sibling points back to leftcurrent: index is corrupt. Pretend
            // we read a distinct page with an invalid btpo_prev.
            newtargetbuf = None;
            btpo_prev_from_target = types_core::primitive::InvalidBlockNumber;
        }

        if let Some(buf) = newtargetbuf {
            nbt::bt_relbuf::call(&state.rel, buf);
        }
        nbt::bt_relbuf::call(&state.rel, lbuf);

        if btpo_prev_from_target == leftcurrent {
            // Report split in left sibling (harmless concurrent page split) —
            // ereport DEBUG1, not modeled.
            return Ok(());
        }

        // Index is corrupt. Make sure we report the correct target page.
        state.targetblock = newtargetblock;
    }

    Err(PgError::error(format!(
        "left link/right link pair in index \"{}\" not in agreement",
        state.rel.name()
    ))
    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
    .with_detail(format!(
        "Block={} left block={} left link from block={}.",
        state.targetblock, leftcurrent, btpo_prev_from_target,
    )))
}

/// `bt_rootdescend(state, itup)` — re-find `itup` from the root via a fresh
/// `_bt_search` descent, confirming a new index scan would locate this exact
/// non-pivot tuple (the `rootdescend` option). Frees the `BTStack` afterwards.
pub fn bt_rootdescend<'mcx>(
    state: &BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
) -> PgResult<bool> {
    let mcx = state_mcx(state);

    // key = _bt_mkscankey(state->rel, itup);
    // The seam takes the on-page tuple bytes; serialize the header carrier.
    let itup_bytes = index_tuple_header_bytes(itup);
    let key = nbtcore::bt_mkscankey::call(&state.rel, Some(&itup_bytes))?;
    // Assert(key->heapkeyspace && key->scantid != NULL).

    // Search from root. Assert(state->readonly && state->rootdescend).
    let mut exists = false;
    // _bt_search(state->rel, NULL, key, &lbuf, BT_READ): no heaprel passed in C
    // (NULL). The seam requires a &Relation for heaprel; pass the check's heaprel
    // (used only for recovery-conflict bookkeeping that doesn't apply here).
    let (stack, lbuf) = nbtcore::bt_search::call(&state.rel, &state.heaprel, &key, false)?;

    if lbuf != types_storage::storage::InvalidBuffer {
        // insertstate { itup, itemsz = MAXALIGN(IndexTupleSize(itup)), itup_key
        // = key, postingoff = 0, bounds_valid = false, buf = lbuf }.
        let itemsz = {
            let h = itup
                .as_ref()
                .expect("bt_rootdescend: itup must be set");
            maxalign(IndexTupleSize(h))
        };
        let mut insertstate = types_nbtree::BTInsertStateData {
            itup: index_tuple_box(mcx, &itup_bytes)?,
            itemsz,
            itup_key: key.clone(),
            buf: lbuf,
            bounds_valid: false,
            low: 0,
            stricthigh: 0,
            postingoff: 0,
        };

        // Get matching tuple on leaf page.
        let offnum = nbtcore::bt_binsrch_insert::call(&state.rel, &mut insertstate)?;
        // Compare first >= matching item on the leaf page, if any.
        let page = bufmgr_buffer_page(state, lbuf)?;
        // Should match on first heap TID when the tuple has a posting list.
        if offnum <= page_get_max_offset_number(&page)
            && insertstate.postingoff <= 0
            && nbtcore::bt_compare::call(&state.rel, &key, &page, offnum)? == 0
        {
            exists = true;
        }
        nbtcore::bt_relbuf::call(&state.rel, lbuf);
    }

    nbtcore::bt_freestack::call(stack);

    Ok(exists)
}

/// `MAXALIGN(len)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
#[inline]
fn maxalign(len: types_core::Size) -> types_core::Size {
    (len + 7) & !7
}

/// `BufferGetPage(buf)` materialized as owned bytes via the bufmgr seam, used by
/// [`bt_rootdescend`] for the leaf page reached by `_bt_search`.
fn bufmgr_buffer_page<'mcx>(
    state: &BtreeCheckState<'mcx>,
    buf: types_storage::storage::Buffer,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    use backend_storage_buffer_bufmgr_seams as bufmgr;
    let mcx = state_mcx(state);
    bufmgr::buffer_get_page::call(mcx, buf)
}

/// `BTreeTupleGetDownLink(itup)` (nbtree.h inline) — the child block number
/// stored in a pivot tuple's `t_tid` (the downlink). Pure byte-math.
pub fn BTreeTupleGetDownLink(itup: &IndexTuple<'_>) -> BlockNumber {
    let h = itup
        .as_ref()
        .expect("BTreeTupleGetDownLink: itup must be set");
    h.t_tid.ip_blkid.block_number()
}

/// `BTreeTupleGetTopParent(itup)` (nbtree.h inline) — the "top parent" block
/// link stored in a deleted/half-dead page's tuple. Pure byte-math.
pub fn BTreeTupleGetTopParent(itup: &IndexTuple<'_>) -> BlockNumber {
    let h = itup
        .as_ref()
        .expect("BTreeTupleGetTopParent: itup must be set");
    h.t_tid.ip_blkid.block_number()
}

/// `PageGetItemIdCareful` over a borrowed page byte slice (the page already in
/// hand as `Vec<u8>` / `&[u8]`). Delegates to the entry-module careful reader by
/// reconstructing the `Page` view it expects is not possible (it takes `&Page`),
/// so this performs the identical line-pointer validation directly on bytes.
fn PageGetItemIdCareful_bytes<'mcx>(
    state: &BtreeCheckState<'mcx>,
    block: BlockNumber,
    page: &[u8],
    offset: OffsetNumber,
) -> PgResult<(u32, u32)> {
    crate::entry::page_get_item_id_careful_bytes(state, block, page, offset)
}
