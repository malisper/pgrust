//! F2 — target-page invariant engine (verify_nbtree.c).
//!
//! The central per-page checker (`bt_target_page_check`) that walks a page
//! item-by-item verifying tuple shape (`_bt_check_natts`), posting-list
//! validity, line-pointer sanity, and key ordering against the page's own
//! items via the `invariant_*` offset helpers and `_bt_compare`; the cross-page
//! right-boundary machinery (`bt_right_page_check_scankey`,
//! `offset_is_negative_infinity`); and the unique-check / posting-tuple
//! helpers (`bt_entry_unique_check` lives in [`crate::entry`];
//! `bt_posting_plain_tuple` lives here).
//!
//! Builds directly on the F0 scankey model: every ordering check funnels
//! through an insertion `BTScanInsert` and the `_bt_compare` seam.
//!
//! The `invariant_*` ordering predicates and `bt_posting_plain_tuple` remain
//! decomposition placeholders (filled by a sibling family); only the
//! `bt_target_page_check` / `bt_right_page_check_scankey` /
//! `offset_is_negative_infinity` engine is implemented here.

use types_core::primitive::{BlockNumber, OffsetNumber};
use types_error::{PgError, PgResult};
use types_error::error::ERRCODE_INDEX_CORRUPTED;
use types_nbtree::{BTScanInsert, BTMaxItemSize, BTP_LEAF, P_FIRSTKEY, P_HIKEY, P_NONE};
use types_tuple::heaptuple::{
    item_pointer_is_valid, IndexTuple, IndexTupleData, ItemPointerData,
};

use backend_access_nbtree_core_seams as nbtcore;

use crate::entry::{bt_entry_unique_check, bt_mkscankey_pivotsearch, palloc_btree_page,
    bt_normalize_tuple, PageGetItemIdCareful};
use crate::linkage::{bt_child_check, bt_child_highkey_check, bt_rootdescend};
use crate::{BtreeCheckState, BtreeLastVisibleEntry, Page};

// ===========================================================================
// Page-format / tuple inline helpers (access/nbtree.h, access/itup.h,
// storage/off.h, storage/itemptr.h)
// ---------------------------------------------------------------------------
// These mirror the C inline macros the verifier uses directly. The btree page
// "opaque" special area is read through the `backend-access-nbtree-core` seams
// (`page_opaque` -> (btpo_flags, btpo_cycleid, btpo_next), `page_btpo_level`),
// which panic until that owner lands. Pure offset / TID arithmetic is done
// here.
// ===========================================================================

/// `BTMaxItemSizeNoHeapTid` (`access/nbtree.h`):
/// `MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData))`
/// ` - MAXALIGN(sizeof(BTPageOpaqueData))) / 3)`
///   = `MAXALIGN_DOWN((8192 - 40 - 16) / 3)` = `2712`. This is the
/// `BTMaxItemSize` limit *before* BTREE_VERSION 4 requisitioned 8 bytes
/// (`MAXALIGN(sizeof(ItemPointerData))`) for an explicit heap-TID tiebreaker.
const BT_MAX_ITEM_SIZE_NO_HEAP_TID: types_core::Size = 2712;

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
fn offset_number_next(offset: OffsetNumber) -> OffsetNumber {
    offset + 1
}

/// `BTPageGetOpaque(page)->btpo_flags` (nbtree.h), via the owner seam.
#[inline]
fn btpo_flags(page: &[u8]) -> u16 {
    nbtcore::page_opaque::call(page).0
}

/// `BTPageGetOpaque(page)->btpo_next` (nbtree.h), via the owner seam.
#[inline]
fn btpo_next(page: &[u8]) -> BlockNumber {
    nbtcore::page_opaque::call(page).2
}

/// `P_ISLEAF(opaque)` (`access/nbtree.h`).
#[inline]
fn p_isleaf(page: &[u8]) -> bool {
    (btpo_flags(page) & BTP_LEAF) != 0
}

/// `P_RIGHTMOST(opaque)` (`access/nbtree.h`): `opaque->btpo_next == P_NONE`.
#[inline]
fn p_rightmost(page: &[u8]) -> bool {
    btpo_next(page) == P_NONE
}

/// `P_IGNORE(opaque)` (`access/nbtree.h`):
/// `((opaque)->btpo_flags & (BTP_DELETED | BTP_HALF_DEAD)) != 0`.
#[inline]
fn p_ignore(page: &[u8]) -> bool {
    (btpo_flags(page) & (types_nbtree::BTP_DELETED | types_nbtree::BTP_HALF_DEAD)) != 0
}

/// `P_FIRSTDATAKEY(opaque)` (`access/nbtree.h`):
/// `(P_RIGHTMOST(opaque) ? P_HIKEY : P_FIRSTKEY)`.
#[inline]
fn p_firstdatakey(page: &[u8]) -> OffsetNumber {
    if p_rightmost(page) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

/// `PageGetMaxOffsetNumber(page)` (`bufpage.h`), via the owner seam.
#[inline]
fn page_get_max_offset_number(page: &[u8]) -> OffsetNumber {
    nbtcore::page_get_max_offset_number::call(page)
}

/// Parse the on-page `IndexTupleData` header (`t_tid` + `t_info`) out of the
/// raw item byte slice. The verifier reads tuple bytes off a private page copy
/// and treats the leading 8 bytes as the C `IndexTupleData` header.
fn index_tuple_header(item: &[u8]) -> IndexTupleData {
    let bi_hi = u16::from_ne_bytes([item[0], item[1]]);
    let bi_lo = u16::from_ne_bytes([item[2], item[3]]);
    let ip_posid = u16::from_ne_bytes([item[4], item[5]]);
    let t_info = u16::from_ne_bytes([item[6], item[7]]);
    IndexTupleData {
        t_tid: ItemPointerData {
            ip_blkid: types_tuple::heaptuple::BlockIdData {
                bi_hi,
                bi_lo,
            },
            ip_posid,
        },
        t_info,
    }
}

/// `(IndexTuple) PageGetItem(page, itemid)` (bufpage.h): fetch the item bytes
/// at `offset` from the page, returned as owned bytes in `mcx`, via the owner
/// seam. The verifier then reads the `IndexTupleData` header out of these.
///
/// The per-page allocations live in the verification's `'mcx` arena; the `mcx`
/// handle is the one carried by the (already `'mcx`-allocated) page copies in
/// `BtreeCheckState`, threaded by the caller via [`state_mcx`].
fn page_get_item<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    page: &[u8],
    offset: OffsetNumber,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    nbtcore::page_get_item::call(mcx, page, offset)
}

/// Build an `IndexTuple<'mcx>` header box from raw item bytes, for the helper
/// calls (`bt_mkscankey_pivotsearch`, `bt_entry_unique_check`,
/// `bt_normalize_tuple`, `bt_child_check`) whose contract takes a parsed
/// `IndexTuple` rather than the raw bytes.
fn index_tuple_box<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    item: &[u8],
) -> PgResult<IndexTuple<'mcx>> {
    Ok(Some(mcx::alloc_in(mcx, index_tuple_header(item))?))
}

/// The verification's `'mcx` arena handle, recovered from the allocator of an
/// already-`'mcx`-allocated page copy held by the state (`Mcx` is `Copy`). All
/// per-page item / scankey allocations are made in this arena, matching C's
/// `state->targetcontext`.
fn state_mcx<'mcx>(state: &BtreeCheckState<'mcx>) -> mcx::Mcx<'mcx> {
    *state
        .target
        .as_ref()
        .expect("state_mcx: target page must be set")
        .allocator()
}

/// `ItemPointerCompare(arg1, arg2)` (`storage/itemptr.c`): -1/0/1 ordering of
/// two heap TIDs, comparing block number then offset (both treated as
/// unsigned).
fn item_pointer_compare(a: &ItemPointerData, b: &ItemPointerData) -> i32 {
    let ablk = a.ip_blkid.block_number();
    let bblk = b.ip_blkid.block_number();
    if ablk > bblk {
        1
    } else if ablk < bblk {
        -1
    } else if a.ip_posid > b.ip_posid {
        1
    } else if a.ip_posid < b.ip_posid {
        -1
    } else {
        0
    }
}

/// `BTreeTupleGetMaxHeapTID(itup)` (`access/nbtree.h`): the highest heap TID a
/// non-pivot tuple points at — the last posting-list element for a posting
/// tuple, otherwise the plain `t_tid`. Works with non-pivot tuples only.
fn btree_tuple_get_max_heap_tid(item: &[u8]) -> ItemPointerData {
    if nbtcore::tuple_is_posting::call(item) {
        let nposting = nbtcore::tuple_n_posting::call(item);
        nbtcore::tuple_posting_tid::call(item, nposting - 1)
    } else {
        nbtcore::tuple_heap_tid::call(item)
    }
}

/// `BTreeTupleGetPointsToTID(itup)` (verify_nbtree.c): the heap (or downlink)
/// TID an item conceptually points at, used only to build error-detail
/// strings. Non-pivot tuples report their heap TID (first posting element for
/// posting tuples); pivot tuples report the `t_tid` downlink block verbatim.
fn btree_tuple_get_points_to_tid(item: &[u8]) -> ItemPointerData {
    if !nbtcore::tuple_is_pivot::call(item) {
        // BTreeTupleGetHeapTID: first posting element, or plain t_tid.
        if nbtcore::tuple_is_posting::call(item) {
            nbtcore::tuple_posting_tid::call(item, 0)
        } else {
            nbtcore::tuple_heap_tid::call(item)
        }
    } else {
        index_tuple_header(item).t_tid
    }
}

/// Render `(block,offset)` exactly as C's `psprintf("(%u,%u)", ...)` for an
/// `ItemPointer` via the `...NoCheck` accessors.
fn fmt_tid(tid: &ItemPointerData) -> String {
    format!(
        "({},{})",
        tid.ip_blkid.block_number(),
        tid.ip_posid
    )
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as C's `%X/%X`.
fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

// ===========================================================================
// F2 engine
// ===========================================================================

/// `bt_target_page_check(state)` — the central per-page invariant checker.
/// Iterates over every item on `state.target`, verifying attribute count,
/// posting-list structure, line-pointer bounds, key ordering (via the
/// `invariant_*` helpers), and — for heapallindexed / checkunique — feeding
/// the Bloom filter and the uniqueness tracker.
pub fn bt_target_page_check<'mcx>(state: &mut BtreeCheckState<'mcx>) -> PgResult<()> {
    // BtreeLastVisibleEntry lVis = {InvalidBlockNumber, InvalidOffsetNumber, -1, NULL};
    let mut l_vis = BtreeLastVisibleEntry {
        blkno: types_core::primitive::InvalidBlockNumber,
        offset: types_tuple::heaptuple::INVALID_OFFSET_NUMBER,
        postingIndex: -1,
        tid: None,
    };

    // topaque = BTPageGetOpaque(state->target); max = PageGetMaxOffsetNumber(...)
    // Work on a clone of the target bytes; the !readonly recovery path re-reads
    // state.target mid-function, so the page bytes are reloaded where C does.
    let target = state
        .target
        .as_ref()
        .expect("bt_target_page_check: target page must be set")
        .as_slice()
        .to_vec();
    let max = page_get_max_offset_number(&target);
    // Per-page allocations (item bytes, scankey tuples) live in the `'mcx`
    // verification arena, recovered from the page copy itself.
    let mcx = state_mcx(state);

    // elog(DEBUG2, "verifying %u items ...") — DEBUG2 logging is not modeled.

    // Check the number of attributes in high key (rightmost page has no high key).
    if !p_rightmost(&target) {
        // Verify line pointer before checking tuple.
        let _itemid = PageGetItemIdCareful(state, state.targetblock, state.target.as_ref().unwrap(), P_HIKEY)?;
        if !nbtcore::bt_check_natts::call(&state.rel, state.heapkeyspace, &target, P_HIKEY)? {
            // itup = (IndexTuple) PageGetItem(state->target, itemid);
            let item = page_get_item(mcx, &target, P_HIKEY)?;
            return Err(PgError::error(format!(
                "wrong number of high key index tuple attributes in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index block={} natts={} block type={} page lsn={}.",
                state.targetblock,
                btree_tuple_get_natts(&item, &state.rel),
                if p_isleaf(&target) { "heap" } else { "index" },
                fmt_lsn(state.targetlsn),
            )));
        }
    }

    // Loop over page items, starting from first non-highkey item.
    let mut offset = p_firstdatakey(&target);
    while offset <= max {
        // True if we already called bt_entry_unique_check() for this item.
        let mut unique_checked = false;

        // CHECK_FOR_INTERRUPTS(): the verifier's cancellation point. The
        // interrupt machinery (procsignal) is not a dependency of this crate;
        // the top-level driver loop is where cancellation is otherwise
        // serviced.

        let itemid = PageGetItemIdCareful(state, state.targetblock, state.target.as_ref().unwrap(), offset)?;
        let item = page_get_item(mcx, &target, offset)?;
        let itup = index_tuple_header(&item);
        let tupsize = types_tuple::heaptuple::IndexTupleSize(&itup);

        // lp_len must match the IndexTuple reported length exactly.
        if tupsize != itemid.1 as types_core::Size {
            return Err(PgError::error(format!(
                "index tuple size does not equal lp_len in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid=({},{}) tuple size={} lp_len={} page lsn={}.",
                state.targetblock,
                offset,
                tupsize,
                itemid.1,
                fmt_lsn(state.targetlsn),
            ))
            .with_hint("This could be a torn page problem."));
        }

        // Check the number of index tuple attributes.
        if !nbtcore::bt_check_natts::call(&state.rel, state.heapkeyspace, &target, offset)? {
            let tid = btree_tuple_get_points_to_tid(&item);
            let itid = format!("({},{})", state.targetblock, offset);
            let htid = fmt_tid(&tid);
            return Err(PgError::error(format!(
                "wrong number of index tuple attributes in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid={} natts={} points to {} tid={} page lsn={}.",
                itid,
                btree_tuple_get_natts(&item, &state.rel),
                if p_isleaf(&target) { "heap" } else { "index" },
                htid,
                fmt_lsn(state.targetlsn),
            )));
        }

        // Don't try to generate a scankey using the "negative infinity" item on
        // internal pages (always truncated to zero attributes).
        if offset_is_negative_infinity(&target, offset) {
            // We don't call bt_child_check() for "negative infinity" items, but
            // if performing the downlink connectivity check, do it for every
            // item including the "negative infinity" one.
            if !p_isleaf(&target) && state.readonly {
                let target_level = nbtcore::page_btpo_level::call(&target);
                bt_child_highkey_check(state, offset, None, target_level)?;
            }
            offset = offset_number_next(offset);
            continue;
        }

        // Readonly callers may optionally re-find non-pivot tuples via a fresh
        // search from the root.
        if state.rootdescend && p_isleaf(&target) {
            let itup_box = index_tuple_box(mcx, &item)?;
            if !bt_rootdescend(state, &itup_box)? {
                let tid = btree_tuple_get_points_to_tid(&item);
                let itid = format!("({},{})", state.targetblock, offset);
                let htid = fmt_tid(&tid);
                return Err(PgError::error(format!(
                    "could not find tuple using search from root page in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Index tid={} points to heap tid={} page lsn={}.",
                    itid,
                    htid,
                    fmt_lsn(state.targetlsn),
                )));
            }
        }

        // If tuple is a posting list tuple, make sure posting list TIDs are in
        // order.
        if nbtcore::tuple_is_posting::call(&item) {
            let mut last = nbtcore::tuple_heap_tid::call(&item);
            let nposting = nbtcore::tuple_n_posting::call(&item);
            let mut i = 1;
            while i < nposting {
                let current = nbtcore::tuple_posting_tid::call(&item, i);
                if item_pointer_compare(&current, &last) <= 0 {
                    let itid = format!("({},{})", state.targetblock, offset);
                    return Err(PgError::error(format!(
                        "posting list contains misplaced TID in index \"{}\"",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Index tid={} posting list offset={} page lsn={}.",
                        itid,
                        i,
                        fmt_lsn(state.targetlsn),
                    )));
                }
                last = current;
                i += 1;
            }
        }

        // Build insertion scankey for current page offset.
        let mut skey = {
            let itup_box = index_tuple_box(mcx, &item)?;
            bt_mkscankey_pivotsearch(&state.rel, Some(&itup_box))?
        };

        // Make sure tuple size does not exceed the BTREE_VERSION-specific limit.
        //
        // lowersizelimit = skey->heapkeyspace &&
        //   (P_ISLEAF(topaque) || BTreeTupleGetHeapTID(itup) == NULL)
        let skey_heapkeyspace = skey
            .as_ref()
            .map(|k| k.heapkeyspace)
            .unwrap_or(false);
        let lowersizelimit = skey_heapkeyspace
            && (p_isleaf(&target) || btree_get_heap_tid_opt(&item).is_none());
        let limit = if lowersizelimit {
            BTMaxItemSize
        } else {
            BT_MAX_ITEM_SIZE_NO_HEAP_TID
        };
        if tupsize > limit {
            let tid = btree_tuple_get_points_to_tid(&item);
            let itid = format!("({},{})", state.targetblock, offset);
            let htid = fmt_tid(&tid);
            return Err(PgError::error(format!(
                "index row size {} exceeds maximum for index \"{}\"",
                tupsize,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid={} points to {} tid={} page lsn={}.",
                itid,
                if p_isleaf(&target) { "heap" } else { "index" },
                htid,
                fmt_lsn(state.targetlsn),
            )));
        }

        // Fingerprint leaf page tuples (those that point to the heap).
        if state.heapallindexed && p_isleaf(&target) && !item_id_is_dead(&itemid) {
            if nbtcore::tuple_is_posting::call(&item) {
                // Fingerprint all elements as distinct "plain" tuples.
                let nposting = nbtcore::tuple_n_posting::call(&item);
                let mut i = 0;
                while i < nposting {
                    let logtuple = bt_posting_plain_tuple(&index_tuple_box(mcx, &item)?, i)?;
                    let norm = bt_normalize_tuple(state, &logtuple)?;
                    bloom_add_index_tuple(state, &norm)?;
                    i += 1;
                }
            } else {
                let itup_box = index_tuple_box(mcx, &item)?;
                let norm = bt_normalize_tuple(state, &itup_box)?;
                bloom_add_index_tuple(state, &norm)?;
            }
        }

        // * High key check *
        //
        // Save scantid (may be set to itup's posting tuple max TID below).
        let scantid_save = skey.as_ref().and_then(|k| k.scantid);
        if state.heapkeyspace && nbtcore::tuple_is_posting::call(&item) {
            if let Some(k) = skey.as_mut() {
                k.scantid = Some(btree_tuple_get_max_heap_tid(&item));
            }
        }

        if !p_rightmost(&target) {
            let ok = if p_isleaf(&target) {
                invariant_leq_offset(state, &skey, P_HIKEY)?
            } else {
                invariant_l_offset(state, &skey, P_HIKEY)?
            };
            if !ok {
                let tid = btree_tuple_get_points_to_tid(&item);
                let itid = format!("({},{})", state.targetblock, offset);
                let htid = fmt_tid(&tid);
                return Err(PgError::error(format!(
                    "high key invariant violated for index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Index tid={} points to {} tid={} page lsn={}.",
                    itid,
                    if p_isleaf(&target) { "heap" } else { "index" },
                    htid,
                    fmt_lsn(state.targetlsn),
                )));
            }
        }
        // Reset, in case scantid was set to (itup) posting tuple's max TID.
        if let Some(k) = skey.as_mut() {
            k.scantid = scantid_save;
        }

        // * Item order check *
        if offset_number_next(offset) <= max
            && !invariant_l_offset(state, &skey, offset_number_next(offset))?
        {
            let tid = btree_tuple_get_points_to_tid(&item);
            let itid = format!("({},{})", state.targetblock, offset);
            let htid = fmt_tid(&tid);
            let nitid = format!("({},{})", state.targetblock, offset_number_next(offset));

            // Reuse to get pointed-to heap location of the second item.
            let _itemid2 = PageGetItemIdCareful(
                state,
                state.targetblock,
                state.target.as_ref().unwrap(),
                offset_number_next(offset),
            )?;
            let item2 = page_get_item(mcx, &target, offset_number_next(offset))?;
            let tid2 = btree_tuple_get_points_to_tid(&item2);
            let nhtid = fmt_tid(&tid2);

            return Err(PgError::error(format!(
                "item order invariant violated for index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Lower index tid={} (points to {} tid={}) higher index tid={} (points to {} tid={}) page lsn={}.",
                itid,
                if p_isleaf(&target) { "heap" } else { "index" },
                htid,
                nitid,
                if p_isleaf(&target) { "heap" } else { "index" },
                nhtid,
                fmt_lsn(state.targetlsn),
            )));
        }

        // If the index is unique verify entries' uniqueness via the heap
        // tuples' visibility. Immediately check posting tuples and tuples with
        // repeated keys; postpone the check for keys that appear first.
        if state.checkunique
            && state.indexinfo.as_ref().map(index_info_is_unique).unwrap_or(false)
            && p_isleaf(&target)
            && !skey.as_ref().map(|k| k.anynullkeys).unwrap_or(false)
            && (nbtcore::tuple_is_posting::call(&item)
                || l_vis.tid.as_ref().map(item_pointer_is_valid).unwrap_or(false))
        {
            let itup_box = index_tuple_box(mcx, &item)?;
            bt_entry_unique_check(state, &itup_box, state.targetblock, offset, &mut l_vis)?;
            unique_checked = true;
        }

        if state.checkunique
            && state.indexinfo.as_ref().map(index_info_is_unique).unwrap_or(false)
            && p_isleaf(&target)
            && offset_number_next(offset) <= max
        {
            // Save current scankey tid.
            let scantid2 = skey.as_ref().and_then(|k| k.scantid);

            // Invalidate scankey tid so _bt_compare compares only keys (report
            // equality even if heap TIDs differ).
            if let Some(k) = skey.as_mut() {
                k.scantid = None;
            }

            // If the next key tuple is different, invalidate the last visible
            // entry data; a NULL value never violates uniqueness and is treated
            // as different from any other key. If the next key matches, do the
            // postponed bt_entry_unique_check().
            let cmp = nbtcore::bt_compare::call(
                &state.rel,
                &skey,
                &target,
                offset_number_next(offset),
            )?;
            if cmp != 0 || skey.as_ref().map(|k| k.anynullkeys).unwrap_or(false) {
                l_vis.blkno = types_core::primitive::InvalidBlockNumber;
                l_vis.offset = types_tuple::heaptuple::INVALID_OFFSET_NUMBER;
                l_vis.postingIndex = -1;
                l_vis.tid = None;
            } else if !unique_checked {
                let itup_box = index_tuple_box(mcx, &item)?;
                bt_entry_unique_check(state, &itup_box, state.targetblock, offset, &mut l_vis)?;
            }
            // Restore saved scan key state.
            if let Some(k) = skey.as_mut() {
                k.scantid = scantid2;
            }
        }

        // * Last item check *
        if offset == max {
            // first offset on a right index page (log only)
            let mut rightfirstoffset = types_tuple::heaptuple::INVALID_OFFSET_NUMBER;

            // Get item in next/right page.
            let mut rightkey = bt_right_page_check_scankey(state, &mut rightfirstoffset)?;

            if rightkey.is_some() && !invariant_g_offset(state, &rightkey, max)? {
                // As explained in bt_right_page_check_scankey(), there is a
                // known !readonly race; the canary is that the target page was
                // deleted.
                if !state.readonly {
                    // Get a fresh copy of the target page.
                    let fresh = palloc_btree_page(state, state.targetblock)?;
                    // Note: we deliberately do not update target LSN.
                    let ignore = p_ignore(fresh.as_slice());
                    state.target = Some(fresh);
                    if ignore {
                        return Ok(());
                    }
                }

                return Err(PgError::error(format!(
                    "cross page item order invariant violated for index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Last item on page tid=({},{}) page lsn={}.",
                    state.targetblock,
                    offset,
                    fmt_lsn(state.targetlsn),
                )));
            }

            // If the index has a unique constraint, make sure no more than one
            // equal item is visible.
            if state.checkunique
                && state.indexinfo.as_ref().map(index_info_is_unique).unwrap_or(false)
                && rightkey.is_some()
                && p_isleaf(&target)
                && !p_rightmost(&target)
            {
                let rightblock_number = btpo_next(&target);

                // elog(DEBUG2, "check cross page unique condition") — not modeled.

                // Make _bt_compare compare only index keys without heap TIDs.
                if let Some(k) = rightkey.as_mut() {
                    k.scantid = None;
                }

                // The first key on the next page is the same.
                if nbtcore::bt_compare::call(&state.rel, &rightkey, &target, max)? == 0
                    && !rightkey.as_ref().map(|k| k.anynullkeys).unwrap_or(false)
                {
                    // Do the postponed bt_entry_unique_check() call.
                    if !unique_checked {
                        let itup_box = index_tuple_box(mcx, &item)?;
                        bt_entry_unique_check(
                            state,
                            &itup_box,
                            state.targetblock,
                            offset,
                            &mut l_vis,
                        )?;
                    }

                    // elog(DEBUG2, "cross page equal keys") — not modeled.
                    let rightpage = palloc_btree_page(state, rightblock_number)?;
                    if p_ignore(rightpage.as_slice()) {
                        // pfree(rightpage); break;
                        break;
                    }

                    if !p_isleaf(rightpage.as_slice()) {
                        return Err(PgError::error(format!(
                            "right block of leaf block is non-leaf for index \"{}\"",
                            state.rel.name()
                        ))
                        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                        .with_detail(format!(
                            "Block={} page lsn={}.",
                            state.targetblock,
                            fmt_lsn(state.targetlsn),
                        )));
                    }

                    let _ritemid = PageGetItemIdCareful(
                        state,
                        rightblock_number,
                        &rightpage,
                        rightfirstoffset,
                    )?;
                    let ritem = page_get_item(mcx, rightpage.as_slice(), rightfirstoffset)?;
                    let ritup = index_tuple_box(mcx, &ritem)?;

                    bt_entry_unique_check(
                        state,
                        &ritup,
                        rightblock_number,
                        rightfirstoffset,
                        &mut l_vis,
                    )?;
                    // pfree(rightpage);
                }
            }
        }

        // * Downlink check *
        if !p_isleaf(&target) && state.readonly {
            bt_child_check(state, &skey, offset)?;
        }

        offset = offset_number_next(offset);
    }

    // Special case bt_child_highkey_check() call: finish the level processing
    // for pages to the right of the rightmost downlink.
    if !p_isleaf(&target) && p_rightmost(&target) && state.readonly {
        let target_level = nbtcore::page_btpo_level::call(&target);
        bt_child_highkey_check(
            state,
            types_tuple::heaptuple::INVALID_OFFSET_NUMBER,
            None,
            target_level,
        )?;
    }

    Ok(())
}

/// `bt_right_page_check_scankey(state, rightfirstoffset)` — build the
/// insertion scankey for the cross-page boundary check between the target page
/// and its right sibling, following right-links (`_bt_moveright`) as needed.
/// Returns the scankey (`None` mirrors C's `NULL`) and writes the right page's
/// first data offset.
pub fn bt_right_page_check_scankey<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    rightfirstoffset: &mut OffsetNumber,
) -> PgResult<BTScanInsert> {
    // Determine target's next block number.
    let target = state
        .target
        .as_ref()
        .expect("bt_right_page_check_scankey: target page must be set")
        .as_slice()
        .to_vec();

    // If target is already rightmost, no right sibling; nothing to do here.
    if p_rightmost(&target) {
        return Ok(None);
    }

    let mcx = state_mcx(state);
    let mut targetnext = btpo_next(&target);
    let rightpage;
    loop {
        // CHECK_FOR_INTERRUPTS(): cancellation point (see bt_target_page_check).

        let page = palloc_btree_page(state, targetnext)?;

        if !p_ignore(page.as_slice()) || p_rightmost(page.as_slice()) {
            rightpage = page;
            break;
        }

        // Landed on a deleted or half-dead sibling page. Step right until we
        // locate a live sibling page. (ereport DEBUG2 not modeled.)
        targetnext = btpo_next(page.as_slice());
        // pfree(rightpage) — page is dropped at end of scope.
    }

    let opaque_is_leaf = p_isleaf(rightpage.as_slice());
    let nline = page_get_max_offset_number(rightpage.as_slice());

    // Get first data item, if any.
    let rightitem_offset = if opaque_is_leaf && nline >= p_firstdatakey(rightpage.as_slice()) {
        // Return first data item (if any).
        let off = p_firstdatakey(rightpage.as_slice());
        *rightfirstoffset = off;
        off
    } else if !opaque_is_leaf
        && nline >= offset_number_next(p_firstdatakey(rightpage.as_slice()))
    {
        // Return first item after the internal page's "negative infinity" item.
        offset_number_next(p_firstdatakey(rightpage.as_slice()))
    } else {
        // No first item. Probably an empty leaf page, or an internal page with
        // only a negative infinity item. (ereport DEBUG2 not modeled.)
        return Ok(None);
    };

    // Verify the line pointer (PageGetItemIdCareful), then fetch the item.
    let _rightitem = PageGetItemIdCareful(state, targetnext, &rightpage, rightitem_offset)?;
    let firstitup = page_get_item(mcx, rightpage.as_slice(), rightitem_offset)?;

    // Return first real item scankey.
    let firstitup_box = index_tuple_box(mcx, &firstitup)?;
    bt_mkscankey_pivotsearch(&state.rel, Some(&firstitup_box))
}

/// `offset_is_negative_infinity(opaque, offset)` — is `offset` the
/// negative-infinity item (the first data key on an internal page, which has
/// zero attributes)? For internal pages only, the first item after the high
/// key (if any) is the negative-infinity item; leaf pages never have one.
pub fn offset_is_negative_infinity(page: &[u8], offset: OffsetNumber) -> bool {
    // !P_ISLEAF(opaque) && offset == P_FIRSTDATAKEY(opaque)
    let (flags, _cycleid, next) = nbtcore::page_opaque::call(page);
    let is_leaf = (flags & BTP_LEAF) != 0;
    let firstdatakey = if next == P_NONE { P_HIKEY } else { P_FIRSTKEY };
    !is_leaf && offset == firstdatakey
}

/// `bt_posting_plain_tuple(itup, n)` — materialize the `n`th heap TID of a
/// posting-list tuple as a plain (non-posting) index tuple, via
/// `_bt_form_posting`. Used to feed posting-list entries to the per-TID checks.
pub fn bt_posting_plain_tuple<'mcx>(itup: &IndexTuple<'mcx>, n: i32) -> PgResult<IndexTuple<'mcx>> {
    let _ = (itup, n);
    panic!("decomp: bt_posting_plain_tuple not yet filled")
}

/// `invariant_l_offset(state, key, upperbound)` — does `key` sort strictly
/// less than the target item at `upperbound`? Verifies the line pointer first.
pub fn invariant_l_offset<'mcx>(
    state: &BtreeCheckState<'mcx>,
    key: &BTScanInsert,
    upperbound: OffsetNumber,
) -> PgResult<bool> {
    let _ = (state, key, upperbound);
    panic!("decomp: invariant_l_offset not yet filled")
}

/// `invariant_leq_offset(state, key, upperbound)` — does `key` sort less-than-
/// or-equal to the target item at `upperbound`?
pub fn invariant_leq_offset<'mcx>(
    state: &BtreeCheckState<'mcx>,
    key: &BTScanInsert,
    upperbound: OffsetNumber,
) -> PgResult<bool> {
    let _ = (state, key, upperbound);
    panic!("decomp: invariant_leq_offset not yet filled")
}

/// `invariant_g_offset(state, key, lowerbound)` — does `key` sort strictly
/// greater than the target item at `lowerbound` (or `>=` for !heapkeyspace)?
pub fn invariant_g_offset<'mcx>(
    state: &BtreeCheckState<'mcx>,
    key: &BTScanInsert,
    lowerbound: OffsetNumber,
) -> PgResult<bool> {
    let _ = (state, key, lowerbound);
    panic!("decomp: invariant_g_offset not yet filled")
}

/// `invariant_l_nontarget_offset(state, key, nontargetblock, nontarget,
/// upperbound)` — like `invariant_l_offset`, but the upper-bound item lives on
/// a caller-supplied non-target page (a child of the target). Verifies the
/// line pointer first.
pub fn invariant_l_nontarget_offset<'mcx>(
    state: &BtreeCheckState<'mcx>,
    key: &BTScanInsert,
    nontargetblock: types_core::primitive::BlockNumber,
    nontarget: &Page<'mcx>,
    upperbound: OffsetNumber,
) -> PgResult<bool> {
    let _ = (state, key, nontargetblock, nontarget, upperbound);
    panic!("decomp: invariant_l_nontarget_offset not yet filled")
}

// ===========================================================================
// Small local helpers that don't have a dedicated F0 seam.
// ===========================================================================

/// `BTreeTupleGetNAtts(itup, rel)` rendered for the error-detail strings. The
/// real macro reads the overloaded `t_tid.ip_posid` for pivot tuples and the
/// index's key-attribute count otherwise; here we only need the value C prints
/// in the corruption report. `_bt_check_natts` (which already failed when this
/// is called) is the authoritative check.
fn btree_tuple_get_natts<'mcx>(item: &[u8], rel: &types_rel::Relation<'mcx>) -> u16 {
    let itup = index_tuple_header(item);
    if nbtcore::tuple_is_pivot::call(item) {
        // BTreeTupleGetNAtts pivot path: low 12 bits of t_tid.ip_posid.
        itup.t_tid.ip_posid & types_nbtree::BT_OFFSET_MASK
    } else {
        // Non-pivot: IndexRelationGetNumberOfAttributes(rel).
        rel.rd_att.natts as u16
    }
}

/// `BTreeTupleGetHeapTID(itup)` as an `Option`, used only to compute
/// `lowersizelimit` (NULL when the tuple is a pivot lacking an explicit heap
/// TID). Non-pivot tuples and pivots carrying `BT_PIVOT_HEAP_TID_ATTR` have a
/// heap TID; other pivots return NULL.
fn btree_get_heap_tid_opt(item: &[u8]) -> Option<ItemPointerData> {
    if nbtcore::tuple_is_pivot::call(item) {
        let itup = index_tuple_header(item);
        if (itup.t_tid.ip_posid & types_nbtree::BT_PIVOT_HEAP_TID_ATTR) != 0 {
            // Pivot with an explicit heap-TID attribute: present.
            Some(btree_tuple_get_max_heap_tid(item))
        } else {
            None
        }
    } else if nbtcore::tuple_is_posting::call(item) {
        Some(nbtcore::tuple_posting_tid::call(item, 0))
    } else {
        Some(nbtcore::tuple_heap_tid::call(item))
    }
}

/// `ItemIdIsDead(itemid)` (`storage/itemid.h`): is the line pointer marked
/// `LP_DEAD` (`lp_flags == 3`)? The `PageGetItemIdCareful` careful-read returns
/// `(lp_off, lp_len)`; the dead flag is not carried, so heapallindexed
/// fingerprints every live-or-dead leaf tuple. (Fingerprinting a dead tuple is
/// harmless — the Bloom filter only over-approximates membership.)
fn item_id_is_dead(_itemid: &(u32, u32)) -> bool {
    false
}

/// `IndexInfo->ii_Unique` for the unique-constraint checks. The trimmed
/// `IndexInfo` carries this flag.
fn index_info_is_unique(ii: &types_nodes::execnodes::IndexInfo) -> bool {
    ii.ii_Unique
}

/// `bloom_add_element(state->filter, (unsigned char *) norm, IndexTupleSize(norm))`
/// (`lib/bloomfilter.h`): fingerprint a normalized index tuple into the
/// heapallindexed Bloom filter, via the owner seam.
///
/// C fingerprints the full `IndexTupleSize(norm)` bytes of the normalized
/// tuple. In this repo `bt_normalize_tuple` (F1) returns the trimmed
/// header-only `IndexTuple` value, so only the on-page `IndexTupleData` header
/// (`t_tid` + `t_info`) is addressable here; the element fed to the filter is
/// the serialized header. (`bt_normalize_tuple` itself is an unfilled sibling
/// stub, so this path is unreachable until both it and the tuple-payload model
/// land; the call shape mirrors C exactly.)
fn bloom_add_index_tuple<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    norm: &IndexTuple<'mcx>,
) -> PgResult<()> {
    let header = norm
        .as_ref()
        .expect("bloom_add_index_tuple: normalized tuple must be set");
    let mut bytes = [0u8; 8];
    bytes[0..2].copy_from_slice(&header.t_tid.ip_blkid.bi_hi.to_ne_bytes());
    bytes[2..4].copy_from_slice(&header.t_tid.ip_blkid.bi_lo.to_ne_bytes());
    bytes[4..6].copy_from_slice(&header.t_tid.ip_posid.to_ne_bytes());
    bytes[6..8].copy_from_slice(&header.t_info.to_ne_bytes());
    let filter = state
        .filter
        .as_mut()
        .expect("bloom_add_index_tuple: filter must be set when heapallindexed");
    backend_lib_bloomfilter_seams::bloom_add_element::call(filter, &bytes);
    Ok(())
}
