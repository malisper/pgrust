//! Port of `src/backend/access/nbtree/nbtsplitloc.c` (PostgreSQL 18.3) —
//! choose-split-point code for the B-tree implementation.
//!
//! The public entry point [`_bt_findsplitloc`] finds an appropriate place to
//! split an nbtree page that is about to overflow during insertion.  The main
//! goal is to equalize the free space that ends up on each of the two split
//! pages (after accounting for the inserted tuple), while giving some weight to
//! suffix truncation (especially for leaf pages full of duplicates).  See the
//! `nbtree/README` and the per-function comments below for the full algorithm.
//!
//! All thirteen functions of `nbtsplitloc.c` are ported 1:1 (C names preserved):
//! `_bt_findsplitloc`, `_bt_recsplitloc`, `_bt_deltasortsplits`, `_bt_splitcmp`,
//! `_bt_afternewitemoff`, `_bt_adjacenthtid`, `_bt_bestsplitloc`,
//! `_bt_defaultinterval`, `_bt_strategy`, `_bt_interval_edges`,
//! `_bt_split_penalty`, `_bt_split_lastleft`, `_bt_split_firstright`.
//!
//! # Chosen signature for the public entry point
//!
//! ```ignore
//! pub fn _bt_findsplitloc<'mcx>(
//!     rel: &Relation<'mcx>,
//!     origpage: &PageRef<'_>,
//!     newitemoff: OffsetNumber,
//!     newitemsz: Size,
//!     newitem: &[u8],
//!     newitemonleft: &mut bool,
//! ) -> PgResult<OffsetNumber>
//! ```
//!
//! This mirrors the C `_bt_findsplitloc(Relation rel, Page origpage,
//! OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem, bool
//! *newitemonleft)`.  `origpage` is an already-constructed [`PageRef`] (the
//! caller in `_bt_split` already holds the page image); `newitem` is the full
//! on-page byte image of the incoming tuple; `newitemsz` is MAXALIGNED but
//! *does not* include the line pointer (as the `nbtinsert` caller passes it).
//!
//! # In-crate vs. external
//!
//! The whole file is pure computation over the in-memory page image
//! (`origpage`) plus the candidate `newitem`.  The page line-pointer reads are
//! done in-crate against the safe [`page`] byte-codec API (the
//! same idiomatic style as the merged `nbtdedup` crate — never a raw struct
//! cast), and the `BTPageOpaqueData` / `IndexTupleData` headers are decoded
//! field-by-field.
//!
//! Only two operations are external, and neither is a seam-and-panic:
//!
//!  * `_bt_keep_natts_fast(rel, lastleft, firstright)` — the suffix-truncation
//!    key comparison.  It is a sibling function in this same crate
//!    ([`crate::utils::bt_keep_natts_fast`]).
//!  * `BTGetFillFactor(rel)` — the index's leaf fillfactor.  It reads the
//!    relation's `rd_options` directly via [`Relation::get_fillfactor`]
//!    (defaulting to `BTREE_DEFAULT_FILLFACTOR`); no seam needed.

use types_core::primitive::{BlockNumber, OffsetNumber, Size};
use types_error::{PgError, PgResult};
use types_nbtree::{
    BTPageOpaqueData, BTREE_SINGLEVAL_FILLFACTOR, BT_IS_POSTING, INDEX_ALT_TID_MASK, P_FIRSTKEY,
    P_HIKEY, P_NONE,
};
use rel::Relation;
use types_tuple::heaptuple::{BlockIdData, IndexTupleData, IndexTupleSize, ItemPointerData};

use page::{
    ItemIdGetLength, ItemPointerGetBlockNumber, ItemPointerGetBlockNumberNoCheck,
    ItemPointerGetOffsetNumber, ItemPointerGetOffsetNumberNoCheck, PageGetExactFreeSpace,
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetPageSize, PageGetSpecialPointer,
    PageRef,
};

// ===========================================================================
// Constants (limits.h, stdint.h, c.h, storage/off.h, access/nbtree.h).
// ===========================================================================

/// `INT_MAX` from `limits.h`.
const INT_MAX: i32 = i32::MAX;
/// `SIZE_MAX` from `stdint.h`.
const SIZE_MAX: Size = Size::MAX;

/// `MAXIMUM_ALIGNOF` (`pg_config.h`).
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(len)` (`c.h`).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `SizeOfPageHeaderData` (`storage/bufpage.h`) — the fixed page header (24).
const SizeOfPageHeaderData: usize = 24;

/// `sizeof(ItemIdData)` (`storage/itemid.h`) — one line pointer (4 bytes).
const SIZE_OF_ITEM_ID_DATA: usize = 4;
/// `sizeof(ItemPointerData)` (`storage/itemptr.h`) — one heap TID (6 bytes).
const SIZE_OF_ITEM_POINTER_DATA: usize = 6;
/// `sizeof(IndexTupleData)` (`access/itup.h`) — the index-tuple header (8 bytes).
const SIZE_OF_INDEX_TUPLE_DATA: usize = 8;
/// `sizeof(BTPageOpaqueData)` (`access/nbtree.h`) — the nbtree special area (16).
const SIZE_OF_BT_PAGE_OPAQUE_DATA: usize = 16;

/// `FirstOffsetNumber` (`storage/off.h`).
const FirstOffsetNumber: OffsetNumber = 1;

/// `BTP_LEAF` (`access/nbtree.h`) — leaf page (not internal).
const BTP_LEAF: u16 = 1 << 0;

/// `BTREE_DEFAULT_FILLFACTOR` (`access/nbtree.h`).
const BTREE_DEFAULT_FILLFACTOR: i32 = 90;
/// `BTREE_NONLEAF_FILLFACTOR` (`access/nbtree.h`).
const BTREE_NONLEAF_FILLFACTOR: i32 = 70;

/// `LEAF_SPLIT_DISTANCE` / `INTERNAL_SPLIT_DISTANCE` (nbtsplitloc.c).
const LEAF_SPLIT_DISTANCE: f64 = 0.050;
const INTERNAL_SPLIT_DISTANCE: f64 = 0.075;

// ===========================================================================
// On-disk byte codec (IndexTuple header / BTPageOpaqueData). Decoded with safe
// field-by-field reads (no raw struct casts), the same idiomatic style as the
// nbtdedup crate.
// ===========================================================================

/// Read an [`ItemPointerData`] (6 `#[repr(C)]` bytes) from the start of `bytes`.
fn read_ipd(bytes: &[u8]) -> ItemPointerData {
    debug_assert!(bytes.len() >= 6);
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_ne_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_ne_bytes([bytes[4], bytes[5]]),
    }
}

/// Decode the leading bytes of a page item as an [`IndexTupleData`] header.
fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    debug_assert!(tuple.len() >= 8);
    let t_tid = read_ipd(&tuple[0..6]);
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    IndexTupleData { t_tid, t_info }
}

/// `BTPageGetOpaque(page)` — decode the `BTPageOpaqueData` stored in the page
/// special area (16 bytes; native-endian fields).
fn BTPageGetOpaque(page: &PageRef<'_>) -> PgResult<BTPageOpaqueData> {
    let special = PageGetSpecialPointer(page)?;
    let rd_u32 = |off: usize| -> u32 {
        u32::from_ne_bytes([
            special[off],
            special[off + 1],
            special[off + 2],
            special[off + 3],
        ])
    };
    let rd_u16 = |off: usize| -> u16 { u16::from_ne_bytes([special[off], special[off + 1]]) };
    Ok(BTPageOpaqueData {
        btpo_prev: rd_u32(0),
        btpo_next: rd_u32(4),
        btpo_level: rd_u32(8),
        btpo_flags: rd_u16(12),
        btpo_cycleid: rd_u16(14),
    })
}

/// `P_RIGHTMOST(opaque)`.
#[inline]
fn P_RIGHTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.btpo_next == P_NONE
}

/// `P_ISLEAF(opaque)`.
#[inline]
fn P_ISLEAF(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_LEAF) != 0
}

/// `P_FIRSTDATAKEY(opaque)` — first data key offset, accounting for high key.
#[inline]
fn P_FIRSTDATAKEY(opaque: &BTPageOpaqueData) -> OffsetNumber {
    if P_RIGHTMOST(opaque) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

/// `BTreeTupleIsPosting(itup)`.
#[inline]
fn BTreeTupleIsPosting(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    // presence of BT_IS_POSTING in offset number indicates posting tuple
    (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_IS_POSTING) != 0
}

/// `BTreeTupleGetPostingOffset(posting)`.
#[inline]
fn BTreeTupleGetPostingOffset(posting: &IndexTupleData) -> u32 {
    debug_assert!(BTreeTupleIsPosting(posting));
    ItemPointerGetBlockNumberNoCheck(&posting.t_tid)
}

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
fn OffsetNumberNext(offset_number: OffsetNumber) -> OffsetNumber {
    offset_number + 1
}

/// `OffsetNumberPrev(offsetNumber)` (`storage/off.h`).
#[inline]
fn OffsetNumberPrev(offset_number: OffsetNumber) -> OffsetNumber {
    offset_number - 1
}

/// `BTGetFillFactor(rel)` — leaf fillfactor in percent (read from `rd_options`,
/// defaulting to `BTREE_DEFAULT_FILLFACTOR`). In-crate, no seam.
#[inline]
fn BTGetFillFactor(rel: &Relation<'_>) -> i32 {
    rel.get_fillfactor(BTREE_DEFAULT_FILLFACTOR)
}

/// `IndexRelationGetNumberOfKeyAttributes(rel)`.
#[inline]
fn rel_nkeyatts(rel: &Relation<'_>) -> i32 {
    rel.indnkeyatts()
}

/// `pg_cmp_s16(a, b)` (`common/int.h`).
#[inline]
fn pg_cmp_s16(a: i16, b: i16) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

// ===========================================================================
// Runtime structs (nbtsplitloc.c) — not on-disk/ABI types, so defined here as
// idiomatic Rust. Field names/meaning mirror the C structs 1:1.
// ===========================================================================

/// `FindSplitStrat` — strategy for searching through materialized split points.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FindSplitStrat {
    /// give some weight to truncation
    SplitDefault,
    /// find minimally distinguishing point
    SplitManyDuplicates,
    /// leave left page almost full
    SplitSingleValue,
}
use FindSplitStrat::*;

/// `SplitPoint` — one candidate split point.
#[derive(Clone, Copy, Debug, Default)]
struct SplitPoint {
    /// current leftfree/rightfree delta
    curdelta: i16,
    /// space left on left page post-split
    leftfree: i16,
    /// space left on right page post-split
    rightfree: i16,
    /// first origpage item on rightpage
    firstrightoff: OffsetNumber,
    /// new item goes on left, or right?
    newitemonleft: bool,
}

/// `FindSplitData` — context for the split-point search.
struct FindSplitData<'a, 'mcx> {
    /// index relation
    rel: &'a Relation<'mcx>,
    /// page undergoing split
    origpage: &'a PageRef<'a>,
    /// new item (cause of page split)
    newitem: &'a [u8],
    /// size of newitem (includes line pointer)
    newitemsz: Size,
    /// T if splitting a leaf page
    is_leaf: bool,
    /// T if splitting rightmost page on level
    is_rightmost: bool,
    /// where the new item is to be inserted
    newitemoff: OffsetNumber,
    /// space available for items on left page
    leftspace: i32,
    /// space available for items on right page
    rightspace: i32,
    /// space taken by old items
    olddataitemstotal: i32,
    /// smallest firstright size
    minfirstrightsz: Size,

    /// maximum number of splits
    maxsplits: usize,
    /// current number of splits (== splits.len())
    nsplits: usize,
    /// all candidate split points for page
    splits: std::vec::Vec<SplitPoint>,
    /// current range of acceptable split points
    interval: i32,
}

// ===========================================================================
// _bt_findsplitloc — the public entry point.
// ===========================================================================

/// `_bt_findsplitloc()` -- find an appropriate place to split a page.
///
/// We are passed the intended insert position of the new tuple (`newitemoff`,
/// the offset of the tuple it must go in front of — `maxoff+1` if it goes at the
/// end), its size (`newitemsz`, MAXALIGNED but *not* including its line
/// pointer), and the new tuple itself (`newitem`, the full on-page byte image).
///
/// Returns the index of the first existing tuple that should go on the
/// righthand page (`firstrightoff`), plus a boolean (`*newitemonleft`)
/// indicating whether the new tuple goes on the left or right page.
pub fn _bt_findsplitloc<'mcx>(
    rel: &Relation<'mcx>,
    origpage: &PageRef<'_>,
    newitemoff: OffsetNumber,
    newitemsz: Size,
    newitem: &[u8],
    newitemonleft: &mut bool,
) -> PgResult<OffsetNumber> {
    let opaque = BTPageGetOpaque(origpage)?;
    let maxoff = PageGetMaxOffsetNumber(origpage);

    /* Total free space available on a btree page, after fixed overhead */
    let leftspace = PageGetPageSize(origpage) as i32
        - SizeOfPageHeaderData as i32
        - maxalign(SIZE_OF_BT_PAGE_OPAQUE_DATA) as i32;
    let mut rightspace = leftspace;

    /* The right page will have the same high key as the old page */
    if !P_RIGHTMOST(&opaque) {
        let itemid = PageGetItemId(origpage, P_HIKEY)?;
        rightspace -=
            (maxalign(ItemIdGetLength(&itemid) as usize) + SIZE_OF_ITEM_ID_DATA) as i32;
    }

    /* Count up total space in data items before actually scanning 'em */
    let olddataitemstotal = rightspace - PageGetExactFreeSpace(origpage) as i32;
    let leaffillfactor = BTGetFillFactor(rel);

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    let newitemsz = newitemsz + SIZE_OF_ITEM_ID_DATA;

    /* newitem cannot be a posting list item */
    debug_assert!(!BTreeTupleIsPosting(&index_tuple_header(newitem)));

    /*
     * nsplits should never exceed maxoff because there will be at most as many
     * candidate split points as there are points _between_ tuples, once you
     * imagine that the new item is already on the original page (the final
     * number of splits may be slightly lower because not all points between
     * tuples will be legal).
     */
    let mut state = FindSplitData {
        rel,
        origpage,
        newitem,
        newitemsz,
        is_leaf: P_ISLEAF(&opaque),
        is_rightmost: P_RIGHTMOST(&opaque),
        leftspace,
        rightspace,
        olddataitemstotal,
        minfirstrightsz: SIZE_MAX,
        newitemoff,
        maxsplits: maxoff as usize,
        nsplits: 0,
        splits: std::vec::Vec::with_capacity(maxoff as usize),
        interval: 0,
    };

    /*
     * Scan through the data items and calculate space usage for a split at each
     * possible position
     */
    let mut olddataitemstoleft: i32 = 0;

    let mut offnum = P_FIRSTDATAKEY(&opaque);
    while offnum <= maxoff {
        let itemid = PageGetItemId(origpage, offnum)?;
        let itemsz = maxalign(ItemIdGetLength(&itemid) as usize) + SIZE_OF_ITEM_ID_DATA;

        /*
         * When item offset number is not newitemoff, neither side of the split
         * can be newitem.  Record a split after the previous data item from
         * original page, but before the current data item from original page.
         * (_bt_recsplitloc() will reject the split when there are no previous
         * items, which we rely on.)
         */
        if offnum < newitemoff {
            _bt_recsplitloc(&mut state, offnum, false, olddataitemstoleft, itemsz)?;
        } else if offnum > newitemoff {
            _bt_recsplitloc(&mut state, offnum, true, olddataitemstoleft, itemsz)?;
        } else {
            /*
             * Record a split after all "offnum < newitemoff" original page data
             * items, but before newitem
             */
            _bt_recsplitloc(&mut state, offnum, false, olddataitemstoleft, itemsz)?;

            /*
             * Record a split after newitem, but before data item from original
             * page at offset newitemoff/current offset
             */
            _bt_recsplitloc(&mut state, offnum, true, olddataitemstoleft, itemsz)?;
        }

        olddataitemstoleft += itemsz as i32;
        offnum = OffsetNumberNext(offnum);
    }

    /*
     * Record a split after all original page data items, but before newitem.
     * (Though only when it's possible that newitem will end up alone on new
     * right page.)
     */
    debug_assert!(olddataitemstoleft == olddataitemstotal);
    if newitemoff > maxoff {
        _bt_recsplitloc(&mut state, newitemoff, false, olddataitemstotal, 0)?;
    }

    /*
     * I believe it is not possible to fail to find a feasible split, but just
     * in case ...
     */
    if state.nsplits == 0 {
        return Err(PgError::error(format!(
            "could not find a feasible split point for index \"{}\"",
            rel.name(),
        )));
    }

    /*
     * Start search for a split point among list of legal split points.  Give
     * primary consideration to equalizing available free space in each half of
     * the split initially (start with default strategy), while applying
     * rightmost and split-after-new-item optimizations where appropriate.
     * Either of the two other fallback strategies may be required for cases with
     * a large number of duplicates around the original/space-optimal split
     * point.
     */
    let mut usemult: bool;
    let mut fillfactormult: f64;
    if !state.is_leaf {
        /* fillfactormult only used on rightmost page */
        usemult = state.is_rightmost;
        fillfactormult = BTREE_NONLEAF_FILLFACTOR as f64 / 100.0;
    } else if state.is_rightmost {
        /* Rightmost leaf page --  fillfactormult always used */
        usemult = true;
        fillfactormult = leaffillfactor as f64 / 100.0;
    } else if {
        usemult = false;
        _bt_afternewitemoff(&mut state, maxoff, leaffillfactor, &mut usemult)?
    } {
        /*
         * New item inserted at rightmost point among a localized grouping on a
         * leaf page -- apply "split after new item" optimization, either by
         * applying leaf fillfactor multiplier, or by choosing the exact split
         * point that leaves newitem as lastleft. (usemult is set for us.)
         */
        if usemult {
            /* fillfactormult should be set based on leaf fillfactor */
            fillfactormult = leaffillfactor as f64 / 100.0;
        } else {
            /* find precise split point after newitemoff */
            let mut done: Option<OffsetNumber> = None;
            for i in 0..state.nsplits {
                let split = &state.splits[i];

                if split.newitemonleft && newitemoff == split.firstrightoff {
                    *newitemonleft = true;
                    done = Some(newitemoff);
                    break;
                }
            }
            if let Some(off) = done {
                return Ok(off);
            }

            /*
             * Cannot legally split after newitemoff; proceed with split without
             * using fillfactor multiplier.  This is defensive, and should never
             * be needed in practice.
             */
            fillfactormult = 0.50;
        }
    } else {
        /* Other leaf page.  50:50 page split. */
        usemult = false;
        /* fillfactormult not used, but be tidy */
        fillfactormult = 0.50;
    }

    /*
     * Save leftmost and rightmost splits for page before original ordinal sort
     * order is lost by delta/fillfactormult sort
     */
    let leftpage = state.splits[0];
    let rightpage = state.splits[state.nsplits - 1];

    /* Give split points a fillfactormult-wise delta, and sort on deltas */
    _bt_deltasortsplits(&mut state, fillfactormult, usemult);

    /* Determine split interval for default strategy */
    state.interval = _bt_defaultinterval(&state);

    /*
     * Determine if default strategy/split interval will produce a sufficiently
     * distinguishing split, or if we should change strategies.  Alternative
     * strategies change the range of split points that are considered
     * acceptable (split interval), and possibly change fillfactormult, in order
     * to deal with pages with a large number of duplicates gracefully.
     *
     * Pass low and high splits for the entire page (actually, they're for an
     * imaginary version of the page that includes newitem).
     */
    let mut strategy: FindSplitStrat = SplitDefault;
    let perfectpenalty = _bt_strategy(&mut state, &leftpage, &rightpage, &mut strategy)?;

    match strategy {
        SplitDefault => {
            /*
             * Default strategy worked out (always works out with internal
             * page).  Original split interval still stands.
             */
        }
        /*
         * Many duplicates strategy is used when a heap TID would otherwise be
         * appended, but the page isn't completely full of logical duplicates.
         * The split interval is widened to include all legal candidate split
         * points.
         */
        SplitManyDuplicates => {
            debug_assert!(state.is_leaf);
            /* Shouldn't try to truncate away extra user attributes */
            debug_assert!(perfectpenalty == rel_nkeyatts(state.rel));
            /* No need to resort splits -- no change in fillfactormult/deltas */
            state.interval = state.nsplits as i32;
        }
        /*
         * Single value strategy is used when it is impossible to avoid appending
         * a heap TID.  It arranges to leave the left page very full.
         */
        SplitSingleValue => {
            debug_assert!(state.is_leaf);
            /* Split near the end of the page */
            usemult = true;
            fillfactormult = BTREE_SINGLEVAL_FILLFACTOR as f64 / 100.0;
            /* Resort split points with new delta */
            _bt_deltasortsplits(&mut state, fillfactormult, usemult);
            /* Appending a heap TID is unavoidable, so interval of 1 is fine */
            state.interval = 1;
        }
    }

    /*
     * Search among acceptable split points (using final split interval) for the
     * entry that has the lowest penalty, and is therefore expected to maximize
     * fan-out.  Sets *newitemonleft for us.
     */
    let firstrightoff = _bt_bestsplitloc(&state, perfectpenalty, newitemonleft, strategy)?;

    Ok(firstrightoff)
}

// ===========================================================================
// _bt_recsplitloc — record one candidate split point (if legal).
// ===========================================================================

/// Subroutine to record a particular point between two tuples (possibly the new
/// item) on page in `state` for later analysis.  Also a convenient point to
/// check if the split is legal (if it isn't, it won't be recorded).
///
/// `firstrightoff` is the offset of the first item on the original page that
/// goes to the right page, and `firstrightofforigpagetuplesz` is its size.
/// `olddataitemstoleft` is the total size of all old items to the left of the
/// split point (should not include newitemsz, which is handled here).
fn _bt_recsplitloc(
    state: &mut FindSplitData<'_, '_>,
    firstrightoff: OffsetNumber,
    newitemonleft: bool,
    olddataitemstoleft: i32,
    firstrightofforigpagetuplesz: Size,
) -> PgResult<()> {
    let firstrightsz: Size;
    let mut postingsz: Size = 0;

    /* Is the new item going to be split point's firstright tuple? */
    let newitemisfirstright = firstrightoff == state.newitemoff && !newitemonleft;

    if newitemisfirstright {
        firstrightsz = state.newitemsz;
    } else {
        firstrightsz = firstrightofforigpagetuplesz;

        /*
         * Calculate suffix truncation space saving when firstright tuple is a
         * posting list tuple, though only when the tuple is over 64 bytes
         * including line pointer overhead (arbitrary).  This avoids accessing
         * the tuple in cases where its posting list must be very small (if tuple
         * has one at all).
         *
         * Note: We don't do this in the case where firstright tuple is newitem,
         * since newitem cannot have a posting list.
         */
        if state.is_leaf && firstrightsz > 64 {
            let itemid = PageGetItemId(state.origpage, firstrightoff)?;
            let newhighkey = PageGetItem(state.origpage, &itemid)?;
            let hdr = index_tuple_header(newhighkey);

            if BTreeTupleIsPosting(&hdr) {
                postingsz = IndexTupleSize(&hdr) - BTreeTupleGetPostingOffset(&hdr) as Size;
            }
        }
    }

    /* Account for all the old tuples */
    let mut leftfree: i16 = (state.leftspace - olddataitemstoleft) as i16;
    let mut rightfree: i16 =
        (state.rightspace - (state.olddataitemstotal - olddataitemstoleft)) as i16;

    /*
     * The first item on the right page becomes the high key of the left page;
     * therefore it counts against left space as well as right space (we cannot
     * assume that suffix truncation will make it any smaller).
     *
     * If we are on the leaf level, assume that suffix truncation cannot avoid
     * adding a heap TID to the left half's new high key when splitting at the
     * leaf level.  We do go to the trouble of subtracting away posting list
     * overhead, though only when it looks like it will make an appreciable
     * difference.
     */
    if state.is_leaf {
        leftfree -= (firstrightsz + maxalign(SIZE_OF_ITEM_POINTER_DATA) - postingsz) as i16;
    } else {
        leftfree -= firstrightsz as i16;
    }

    /* account for the new item */
    if newitemonleft {
        leftfree -= state.newitemsz as i16;
    } else {
        rightfree -= state.newitemsz as i16;
    }

    /*
     * If we are not on the leaf level, we will be able to discard the key data
     * from the first item that winds up on the right page.
     */
    if !state.is_leaf {
        rightfree +=
            firstrightsz as i16 - (maxalign(SIZE_OF_INDEX_TUPLE_DATA) + SIZE_OF_ITEM_ID_DATA) as i16;
    }

    /* Record split if legal */
    if leftfree >= 0 && rightfree >= 0 {
        debug_assert!(state.nsplits < state.maxsplits);

        /* Determine smallest firstright tuple size among legal splits */
        state.minfirstrightsz = state.minfirstrightsz.min(firstrightsz);

        state.splits.push(SplitPoint {
            curdelta: 0,
            leftfree,
            rightfree,
            firstrightoff,
            newitemonleft,
        });
        state.nsplits += 1;
    }
    Ok(())
}

// ===========================================================================
// _bt_deltasortsplits / _bt_splitcmp.
// ===========================================================================

/// Subroutine to assign space deltas to materialized array of candidate split
/// points based on current fillfactor, and to sort array using that fillfactor.
fn _bt_deltasortsplits(state: &mut FindSplitData<'_, '_>, fillfactormult: f64, usemult: bool) {
    for i in 0..state.nsplits {
        let split = &mut state.splits[i];
        let mut delta: i16;

        if usemult {
            delta = (fillfactormult * split.leftfree as f64
                - (1.0 - fillfactormult) * split.rightfree as f64) as i16;
        } else {
            delta = split.leftfree - split.rightfree;
        }

        if delta < 0 {
            delta = -delta;
        }

        /* Save delta */
        split.curdelta = delta;
    }

    state.splits[..state.nsplits].sort_by(_bt_splitcmp);
}

/// `qsort`-style comparator used by `_bt_deltasortsplits()`.
fn _bt_splitcmp(split1: &SplitPoint, split2: &SplitPoint) -> core::cmp::Ordering {
    pg_cmp_s16(split1.curdelta, split2.curdelta).cmp(&0)
}

// ===========================================================================
// _bt_afternewitemoff.
// ===========================================================================

/// Subroutine to determine whether or not a non-rightmost leaf page should be
/// split immediately after the would-be original page offset for the
/// new/incoming tuple (or should have leaf fillfactor applied when new item is
/// to the right on original page).  This is appropriate for a pattern of
/// localized monotonically increasing insertions into a composite index.
///
/// Caller uses the optimization when this returns true; the exact action varies.
/// Caller uses original leaf page fillfactor when `*usemult` was also set true
/// here.  Otherwise, caller locates the legal split point that makes the new
/// tuple the lastleft tuple for the split.
fn _bt_afternewitemoff(
    state: &mut FindSplitData<'_, '_>,
    maxoff: OffsetNumber,
    leaffillfactor: i32,
    usemult: &mut bool,
) -> PgResult<bool> {
    debug_assert!(state.is_leaf && !state.is_rightmost);

    let nkeyatts = rel_nkeyatts(state.rel) as i16;

    /* Single key indexes not considered here */
    if nkeyatts == 1 {
        return Ok(false);
    }

    /* Ascending insertion pattern never inferred when new item is first */
    if state.newitemoff == P_FIRSTKEY {
        return Ok(false);
    }

    /*
     * Only apply optimization on pages with equisized tuples, since ordinal keys
     * are likely to be fixed-width.  Conclude that page has equisized tuples
     * when the new item is the same width as the smallest item observed during
     * pass over page, and other non-pivot tuples must be the same width as well.
     * (Note that the possibly-truncated existing high key isn't counted in
     * olddataitemstotal, and must be subtracted from maxoff.)
     */
    if state.newitemsz != state.minfirstrightsz {
        return Ok(false);
    }
    if state.newitemsz * (maxoff as Size - 1) != state.olddataitemstotal as Size {
        return Ok(false);
    }

    /*
     * Avoid applying optimization when tuples are wider than a tuple consisting
     * of two non-NULL int8/int64 attributes (or four non-NULL int4/int32
     * attributes)
     */
    if state.newitemsz
        > maxalign(SIZE_OF_INDEX_TUPLE_DATA + core::mem::size_of::<i64>() * 2) + SIZE_OF_ITEM_ID_DATA
    {
        return Ok(false);
    }

    /*
     * At least the first attribute's value must be equal to the corresponding
     * value in previous tuple to apply optimization.  New item cannot be a
     * duplicate, either.
     *
     * Handle case where new item is to the right of all items on the existing
     * page.  This is suggestive of monotonically increasing insertions in
     * itself, so the "heap TID adjacency" test is not applied here.
     */
    if state.newitemoff > maxoff {
        let itemid = PageGetItemId(state.origpage, maxoff)?;
        let tup = PageGetItem(state.origpage, &itemid)?;
        let keepnatts = crate::utils::bt_keep_natts_fast(state.rel, tup, state.newitem)?;

        if keepnatts > 1 && keepnatts <= nkeyatts as i32 {
            *usemult = true;
            return Ok(true);
        }

        return Ok(false);
    }

    /*
     * "Low cardinality leading column, high cardinality suffix column" indexes
     * with a random insertion pattern present us with a risk of consistently
     * misapplying the optimization.  Heap TID adjacency strongly suggests that
     * the item just to the left was inserted very recently, which limits
     * overapplication of the optimization.
     */
    let itemid = PageGetItemId(state.origpage, OffsetNumberPrev(state.newitemoff))?;
    let tup = PageGetItem(state.origpage, &itemid)?;
    let tuphdr = index_tuple_header(tup);
    /* Do cheaper test first */
    if BTreeTupleIsPosting(&tuphdr)
        || !_bt_adjacenthtid(&tuphdr.t_tid, &index_tuple_header(state.newitem).t_tid)
    {
        return Ok(false);
    }
    /* Check same conditions as rightmost item case, too */
    let keepnatts = crate::utils::bt_keep_natts_fast(state.rel, tup, state.newitem)?;

    if keepnatts > 1 && keepnatts <= nkeyatts as i32 {
        let interp = state.newitemoff as f64 / (maxoff as f64 + 1.0);
        let leaffillfactormult = leaffillfactor as f64 / 100.0;

        /*
         * Don't allow caller to split after a new item when it will result in a
         * split point to the right of the point that a leaf fillfactor split
         * would use -- have caller apply leaf fillfactor instead
         */
        *usemult = interp > leaffillfactormult;

        return Ok(true);
    }

    Ok(false)
}

// ===========================================================================
// _bt_adjacenthtid.
// ===========================================================================

/// Subroutine for determining if two heap TIDS are "adjacent".
///
/// Adjacent means that the high TID is very likely to have been inserted into
/// heap relation immediately after the low TID, probably during the current
/// transaction.
fn _bt_adjacenthtid(lowhtid: &ItemPointerData, highhtid: &ItemPointerData) -> bool {
    let lowblk: BlockNumber = ItemPointerGetBlockNumber(lowhtid);
    let highblk: BlockNumber = ItemPointerGetBlockNumber(highhtid);

    /* Make optimistic assumption of adjacency when heap blocks match */
    if lowblk == highblk {
        return true;
    }

    /* When heap block one up, second offset should be FirstOffsetNumber */
    if lowblk + 1 == highblk && ItemPointerGetOffsetNumber(highhtid) == FirstOffsetNumber {
        return true;
    }

    false
}

// ===========================================================================
// _bt_bestsplitloc.
// ===========================================================================

/// Subroutine to find the "best" split point among candidate split points.  The
/// best split point is the one with the lowest penalty among split points that
/// fall within the current/final split interval.
///
/// "perfectpenalty" is assumed to be the lowest possible penalty among candidate
/// split points.  Returns the index of the first existing tuple that should go
/// on the right page, plus a boolean (`*newitemonleft`).
fn _bt_bestsplitloc(
    state: &FindSplitData<'_, '_>,
    perfectpenalty: i32,
    newitemonleft: &mut bool,
    strategy: FindSplitStrat,
) -> PgResult<OffsetNumber> {
    let mut bestpenalty: i32;
    let mut lowsplit: usize;
    let highsplit = (state.interval as usize).min(state.nsplits);

    bestpenalty = INT_MAX;
    lowsplit = 0;
    for i in lowsplit..highsplit {
        let penalty = _bt_split_penalty(state, &state.splits[i])?;

        if penalty < bestpenalty {
            bestpenalty = penalty;
            lowsplit = i;
        }

        if penalty <= perfectpenalty {
            break;
        }
    }

    let mut final_idx = lowsplit;

    /*
     * There is a risk that the "many duplicates" strategy will repeatedly do the
     * wrong thing when there are monotonically decreasing insertions to the
     * right of a large group of duplicates.  Repeated splits could leave a
     * succession of right half pages with free space that can never be used.
     * This must be avoided.
     */
    {
        let final_ref = &state.splits[final_idx];
        if strategy == SplitManyDuplicates
            && !state.is_rightmost
            && !final_ref.newitemonleft
            && final_ref.firstrightoff >= state.newitemoff
            && final_ref.firstrightoff < state.newitemoff + 9
        {
            /*
             * Avoid the problem by performing a 50:50 split when the new item is
             * just to the right of the would-be "many duplicates" split point.
             * (Note that the test used for an insert that is "just to the right"
             * of the split point is conservative.)
             */
            final_idx = 0;
        }
    }

    let final_ref = &state.splits[final_idx];
    *newitemonleft = final_ref.newitemonleft;
    Ok(final_ref.firstrightoff)
}

// ===========================================================================
// _bt_defaultinterval.
// ===========================================================================

/// Return a split interval to use for the default strategy.  This is a limit on
/// the number of candidate split points to give further consideration to.  Split
/// interval represents an acceptable range of split points -- those that have
/// leftfree and rightfree values that are acceptably balanced.
fn _bt_defaultinterval(state: &FindSplitData<'_, '_>) -> i32 {
    let tolerance: i16;
    let (lowleftfree, lowrightfree, highleftfree, highrightfree): (i16, i16, i16, i16);

    /*
     * Determine leftfree and rightfree values that are higher and lower than
     * we're willing to tolerate.  Note that the final split interval will be
     * about 10% of nsplits in the common case where all non-pivot tuples (data
     * items) from a leaf page are uniformly sized.  We're a bit more aggressive
     * when splitting internal pages.
     */
    if state.is_leaf {
        tolerance = (state.olddataitemstotal as f64 * LEAF_SPLIT_DISTANCE) as i16;
    } else {
        tolerance = (state.olddataitemstotal as f64 * INTERNAL_SPLIT_DISTANCE) as i16;
    }

    /* First candidate split point is the most evenly balanced */
    let spaceoptimal = &state.splits[0];
    lowleftfree = spaceoptimal.leftfree - tolerance;
    lowrightfree = spaceoptimal.rightfree - tolerance;
    highleftfree = spaceoptimal.leftfree + tolerance;
    highrightfree = spaceoptimal.rightfree + tolerance;

    /*
     * Iterate through split points, starting from the split immediately after
     * 'spaceoptimal'.  Find the first split point that divides free space so
     * unevenly that including it in the split interval would be unacceptable.
     */
    for i in 1..state.nsplits {
        let split = &state.splits[i];

        /* Cannot use curdelta here, since its value is often weighted */
        if split.leftfree < lowleftfree
            || split.rightfree < lowrightfree
            || split.leftfree > highleftfree
            || split.rightfree > highrightfree
        {
            return i as i32;
        }
    }

    state.nsplits as i32
}

// ===========================================================================
// _bt_strategy.
// ===========================================================================

/// Subroutine to decide whether split should use default strategy/initial split
/// interval, or whether it should finish splitting the page using alternative
/// strategies (only possible with leaf pages).
///
/// Caller uses alternative strategy (or sticks with default strategy) based on
/// how `*strategy` is set here.  Return value is "perfect penalty".
fn _bt_strategy(
    state: &mut FindSplitData<'_, '_>,
    leftpage: &SplitPoint,
    rightpage: &SplitPoint,
    strategy: &mut FindSplitStrat,
) -> PgResult<i32> {
    let indnkeyatts = rel_nkeyatts(state.rel);

    /* Assume that alternative strategy won't be used for now */
    *strategy = SplitDefault;

    /*
     * Use smallest observed firstright item size for entire page (actually,
     * entire imaginary version of page that includes newitem) as perfect penalty
     * on internal pages.  This can save cycles in the common case where most or
     * all splits have firstright tuples that are the same size.
     */
    if !state.is_leaf {
        return Ok(state.minfirstrightsz as i32);
    }

    /*
     * Use leftmost and rightmost tuples from leftmost and rightmost splits in
     * current split interval
     */
    let (leftinterval, rightinterval) = _bt_interval_edges(state);
    let leftmost = _bt_split_lastleft(state, &state.splits[leftinterval])?;
    let rightmost = _bt_split_firstright(state, &state.splits[rightinterval])?;

    /*
     * If initial split interval can produce a split point that will at least
     * avoid appending a heap TID in new high key, we're done.  Finish split with
     * default strategy and initial split interval.
     */
    let mut perfectpenalty = crate::utils::bt_keep_natts_fast(state.rel, leftmost, rightmost)?;
    if perfectpenalty <= indnkeyatts {
        return Ok(perfectpenalty);
    }

    /*
     * Work out how caller should finish split when even their "perfect" penalty
     * for initial/default split interval indicates that the interval does not
     * contain even a single split that avoids appending a heap TID.
     *
     * Use the leftmost split's lastleft tuple and the rightmost split's
     * firstright tuple to assess every possible split.
     */
    let leftmost = _bt_split_lastleft(state, leftpage)?;
    let rightmost = _bt_split_firstright(state, rightpage)?;

    /*
     * If page (including new item) has many duplicates but is not entirely full
     * of duplicates, a many duplicates strategy split will be performed.  If page
     * is entirely full of duplicates, a single value strategy split will be
     * performed.
     */
    perfectpenalty = crate::utils::bt_keep_natts_fast(state.rel, leftmost, rightmost)?;
    if perfectpenalty <= indnkeyatts {
        *strategy = SplitManyDuplicates;

        /*
         * Many duplicates strategy should split at either side the group of
         * duplicates that enclose the delta-optimal split point.  Return
         * indnkeyatts rather than the true perfect penalty to make that happen.
         */
        return Ok(indnkeyatts);
    }

    /*
     * Single value strategy is only appropriate with ever-increasing heap TIDs;
     * otherwise, original default strategy split should proceed to avoid
     * pathological performance.  Use page high key to infer if this is the
     * rightmost page among pages that store the same duplicate value.
     */
    if state.is_rightmost {
        *strategy = SplitSingleValue;
    } else {
        let itemid = PageGetItemId(state.origpage, P_HIKEY)?;
        let hikey = PageGetItem(state.origpage, &itemid)?;
        perfectpenalty = crate::utils::bt_keep_natts_fast(state.rel, hikey, state.newitem)?;
        if perfectpenalty <= indnkeyatts {
            *strategy = SplitSingleValue;
        } else {
            /*
             * Have caller finish split using default strategy, since page does
             * not appear to be the rightmost page for duplicates of the value the
             * page is filled with
             */
        }
    }

    Ok(perfectpenalty)
}

// ===========================================================================
// _bt_interval_edges.
// ===========================================================================

/// Subroutine to locate leftmost and rightmost splits for current/default split
/// interval.  Returns indices into `state.splits` (it will be the same split iff
/// there is only one split in interval).
fn _bt_interval_edges(state: &FindSplitData<'_, '_>) -> (usize, usize) {
    let highsplit = (state.interval as usize).min(state.nsplits);
    let deltaoptimal = &state.splits[0];
    let mut leftinterval: Option<usize> = None;
    let mut rightinterval: Option<usize> = None;

    /*
     * Delta is an absolute distance to optimal split point, so both the leftmost
     * and rightmost split point will usually be at the end of the array
     */
    let mut i = highsplit as isize - 1;
    while i >= 0 {
        let distant = &state.splits[i as usize];

        if distant.firstrightoff < deltaoptimal.firstrightoff {
            if leftinterval.is_none() {
                leftinterval = Some(i as usize);
            }
        } else if distant.firstrightoff > deltaoptimal.firstrightoff {
            if rightinterval.is_none() {
                rightinterval = Some(i as usize);
            }
        } else if !distant.newitemonleft && deltaoptimal.newitemonleft {
            /*
             * "incoming tuple will become firstright" (distant) is to the left of
             * "incoming tuple will become lastleft" (delta-optimal)
             */
            debug_assert!(distant.firstrightoff == state.newitemoff);
            if leftinterval.is_none() {
                leftinterval = Some(i as usize);
            }
        } else if distant.newitemonleft && !deltaoptimal.newitemonleft {
            /*
             * "incoming tuple will become lastleft" (distant) is to the right of
             * "incoming tuple will become firstright" (delta-optimal)
             */
            debug_assert!(distant.firstrightoff == state.newitemoff);
            if rightinterval.is_none() {
                rightinterval = Some(i as usize);
            }
        } else {
            /* There was only one or two splits in initial split interval */
            debug_assert!(i as usize == 0);
            if leftinterval.is_none() {
                leftinterval = Some(i as usize);
            }
            if rightinterval.is_none() {
                rightinterval = Some(i as usize);
            }
        }

        if let (Some(l), Some(r)) = (leftinterval, rightinterval) {
            return (l, r);
        }

        i -= 1;
    }

    debug_assert!(false);
    (0, 0)
}

// ===========================================================================
// _bt_split_penalty / _bt_split_lastleft / _bt_split_firstright.
// ===========================================================================

/// Subroutine to find penalty for caller's candidate split point.
///
/// On leaf pages, penalty is the attribute number that distinguishes each side
/// of a split (the last attribute that must be included in the new high key for
/// the left page).  On internal pages, penalty is simply the size of the
/// firstright tuple for the split (including line pointer overhead).
fn _bt_split_penalty(state: &FindSplitData<'_, '_>, split: &SplitPoint) -> PgResult<i32> {
    if !state.is_leaf {
        if !split.newitemonleft && split.firstrightoff == state.newitemoff {
            return Ok(state.newitemsz as i32);
        }

        let itemid = PageGetItemId(state.origpage, split.firstrightoff)?;

        return Ok((maxalign(ItemIdGetLength(&itemid) as usize) + SIZE_OF_ITEM_ID_DATA) as i32);
    }

    let lastleft = _bt_split_lastleft(state, split)?;
    let firstright = _bt_split_firstright(state, split)?;

    crate::utils::bt_keep_natts_fast(state.rel, lastleft, firstright)
}

/// Subroutine to get a lastleft IndexTuple for a split point.
fn _bt_split_lastleft<'a>(
    state: &'a FindSplitData<'a, '_>,
    split: &SplitPoint,
) -> PgResult<&'a [u8]> {
    if split.newitemonleft && split.firstrightoff == state.newitemoff {
        return Ok(state.newitem);
    }

    let itemid = PageGetItemId(state.origpage, OffsetNumberPrev(split.firstrightoff))?;
    PageGetItem(state.origpage, &itemid)
}

/// Subroutine to get a firstright IndexTuple for a split point.
fn _bt_split_firstright<'a>(
    state: &'a FindSplitData<'a, '_>,
    split: &SplitPoint,
) -> PgResult<&'a [u8]> {
    if !split.newitemonleft && split.firstrightoff == state.newitemoff {
        return Ok(state.newitem);
    }

    let itemid = PageGetItemId(state.origpage, split.firstrightoff)?;
    PageGetItem(state.origpage, &itemid)
}

#[cfg(test)]
mod tests {
    //! Pure-arithmetic unit tests translated from the src-idiomatic
    //! `nbtsplitloc` tests.  `_bt_findsplitloc` and the strategy/penalty/interval
    //! helpers all operate over a `FindSplitData` built from a real
    //! `Relation<'mcx>` and call `crate::utils::bt_keep_natts_fast`; constructing
    //! a `Relation` plus tuple descriptor for those paths belongs in a crate-wide
    //! integration test (where `utils.rs` is also present).  The tests here cover
    //! the two genuinely self-contained pieces — `pg_cmp_s16` and
    //! `_bt_adjacenthtid` — exactly as in the src-idiomatic suite.

    use super::*;

    #[test]
    fn pg_cmp_s16_matches_three_way() {
        assert_eq!(pg_cmp_s16(1, 2), -1);
        assert_eq!(pg_cmp_s16(2, 2), 0);
        assert_eq!(pg_cmp_s16(3, 2), 1);
        assert_eq!(pg_cmp_s16(i16::MIN, i16::MAX), -1);
    }

    #[test]
    fn adjacenthtid_same_block_is_adjacent() {
        let low = ItemPointerData::new(5, 3);
        let high = ItemPointerData::new(5, 99);
        assert!(_bt_adjacenthtid(&low, &high));
    }

    #[test]
    fn adjacenthtid_next_block_first_offset_is_adjacent() {
        let low = ItemPointerData::new(5, 3);
        let high = ItemPointerData::new(6, FirstOffsetNumber);
        assert!(_bt_adjacenthtid(&low, &high));
    }

    #[test]
    fn adjacenthtid_next_block_other_offset_not_adjacent() {
        let low = ItemPointerData::new(5, 3);
        let high = ItemPointerData::new(6, 7);
        assert!(!_bt_adjacenthtid(&low, &high));
    }

    #[test]
    fn adjacenthtid_far_block_not_adjacent() {
        let low = ItemPointerData::new(5, 3);
        let high = ItemPointerData::new(9, FirstOffsetNumber);
        assert!(!_bt_adjacenthtid(&low, &high));
    }

    #[test]
    fn maxalign_rounds_up_to_eight() {
        assert_eq!(maxalign(0), 0);
        assert_eq!(maxalign(1), 8);
        assert_eq!(maxalign(8), 8);
        assert_eq!(maxalign(9), 16);
    }
}
