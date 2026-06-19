#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of `src/backend/access/nbtree/nbtdedup.c` (PostgreSQL 18.3) —
//! deduplicate or bottom-up delete items in Postgres btrees.
//!
//! The posting-list byte layout is reproduced exactly as in C: a posting tuple
//! is the base tuple's key data followed (at a `MAXALIGN`'d offset) by an array
//! of [`ItemPointerData`] heap TIDs, with the count and posting-list offset
//! stashed in the overloaded `t_tid`. Newly-formed tuples are returned as owned
//! `PgVec<u8>` in the caller's [`Mcx`] (the safe equivalent of a `palloc`'d
//! `IndexTuple`).
//!
//! Boundaries crossing into not-yet-ported sibling units are reached through
//! seams: `_bt_keep_natts_fast` (nbtutils.c) and `_bt_delitems_delete_check`
//! (nbtpage.c) via `backend-access-nbtree-core-seams`; the pinned-buffer page
//! write-back / WAL machinery via the bufmgr / xloginsert / miscinit / relcache
//! seams. The page-scan and dedup logic that *feeds* those boundaries is ported
//! 1:1 in-crate.

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::{BlockNumber, OffsetNumber, RmgrId, Size, BLCKSZ};
use types_error::{PgError, PgResult};
use types_nbtree::{
    BTDedupInterval, BTPageOpaqueData, BTMaxItemSize, BTREE_SINGLEVAL_FILLFACTOR, BTP_HAS_GARBAGE,
    BT_IS_POSTING, BT_OFFSET_MASK, BT_PIVOT_HEAP_TID_ATTR, INDEX_ALT_TID_MASK, MaxIndexTuplesPerPage,
    MaxTIDsPerBTreePage, P_NONE, SizeOfBtreeDedup, TmIndexDelete, TmIndexDeleteOp, TmIndexStatus,
    XLOG_BTREE_DEDUP,
};
use types_rel::Relation;
use types_storage::storage::Buffer;
use types_tuple::heaptuple::{
    BlockIdData, IndexTupleData, IndexTupleSize, ItemPointerData, INDEX_SIZE_MASK,
};
use types_wal::xloginsert::REGBUF_STANDARD;

use backend_storage_page::{
    ItemIdGetLength, ItemIdIsDead, ItemPointerCompare, ItemPointerCopy, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumberNoCheck, ItemPointerIsValid, ItemPointerSetBlockNumber,
    ItemPointerSetOffsetNumber, PageAddItemExtended, PageGetExactFreeSpace, PageGetItem,
    PageGetItemId, PageGetLSN, PageGetMaxOffsetNumber, PageGetPageSize, PageGetSpecialPointer,
    PageGetTempPageCopySpecial, PageMut, PageRef, PageSetLSN,
};

use backend_access_nbtree_core_seams as nbtcore;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscinit;

// ---------------------------------------------------------------------------
// c.h / off.h / itup.h constants the dedup code uses.
// ---------------------------------------------------------------------------

/// `PG_UINT16_MAX`.
const PG_UINT16_MAX: i32 = 0xFFFF;

/// `MAXIMUM_ALIGNOF`.
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(len)` (`c.h`).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `sizeof(ItemPointerData)`.
const SIZEOF_IPD: usize = ::core::mem::size_of::<ItemPointerData>();
/// `sizeof(ItemIdData)`.
const SIZEOF_ITEM_ID: usize = 4;
/// `SizeOfPageHeaderData` (`storage/bufpage.h`).
const SizeOfPageHeaderData: usize = 24;

/// `RM_BTREE_ID` (`access/rmgrlist.h`) — resource-manager id 11 (XLOG=0, XACT,
/// SMGR, CLOG, DBASE, TBLSPC, MULTIXACT, RELMAP, STANDBY, HEAP2, HEAP, BTREE).
const RM_BTREE_ID: RmgrId = 11;

/// `P_HIKEY` — high key lives in item 1 on non-rightmost pages.
const P_HIKEY: OffsetNumber = 1;
/// `P_FIRSTKEY` — data items start in item 2 on non-rightmost pages.
const P_FIRSTKEY: OffsetNumber = 2;

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberNext(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber + 1
}

/// `P_RIGHTMOST(opaque)`.
#[inline]
fn P_RIGHTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.btpo_next == P_NONE
}

/// `P_HAS_GARBAGE(opaque)`.
#[inline]
fn P_HAS_GARBAGE(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_HAS_GARBAGE) != 0
}

/// `P_FIRSTDATAKEY(opaque)` — first data key offset, accounting for the high key.
#[inline]
fn P_FIRSTDATAKEY(opaque: &BTPageOpaqueData) -> OffsetNumber {
    if P_RIGHTMOST(opaque) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

/// `BTPageGetOpaque(page)` — decode the `BTPageOpaqueData` (16 bytes) stored in
/// the page's special area.
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

// ---------------------------------------------------------------------------
// BTreeTupleData inline helpers (nbtree.h "Notes on B-Tree tuple format").
// ---------------------------------------------------------------------------

/// `BTreeTupleIsPivot(itup)`.
#[inline]
fn BTreeTupleIsPivot(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    // absence of BT_IS_POSTING in offset number indicates pivot tuple
    (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_IS_POSTING) == 0
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

/// `BTreeTupleSetPosting(itup, nhtids, postingoffset)`.
#[inline]
fn BTreeTupleSetPosting(itup: &mut IndexTupleData, nhtids: u16, postingoffset: i32) {
    debug_assert!(nhtids > 1);
    debug_assert!((nhtids & 0xF000) == 0);
    debug_assert!(postingoffset as usize == maxalign(postingoffset as usize));
    debug_assert!(!BTreeTupleIsPivot(itup));

    itup.t_info |= INDEX_ALT_TID_MASK;
    ItemPointerSetOffsetNumber(&mut itup.t_tid, nhtids | BT_IS_POSTING);
    ItemPointerSetBlockNumber(&mut itup.t_tid, postingoffset as BlockNumber);
}

/// `BTreeTupleGetNPosting(posting)`.
#[inline]
fn BTreeTupleGetNPosting(posting: &IndexTupleData) -> u16 {
    debug_assert!(BTreeTupleIsPosting(posting));
    ItemPointerGetOffsetNumberNoCheck(&posting.t_tid) & BT_OFFSET_MASK
}

/// `BTreeTupleGetPostingOffset(posting)`.
#[inline]
fn BTreeTupleGetPostingOffset(posting: &IndexTupleData) -> u32 {
    debug_assert!(BTreeTupleIsPosting(posting));
    // BlockIdGetBlockNumber(&posting->t_tid.ip_blkid)
    ((posting.t_tid.ip_blkid.bi_hi as u32) << 16) | (posting.t_tid.ip_blkid.bi_lo as u32)
}

// ---------------------------------------------------------------------------
// Posting-list / heap-TID payload access on the full tuple byte slice.
// ---------------------------------------------------------------------------

/// Interpret the leading 8 bytes of a page item as an [`IndexTupleData`] header.
fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    debug_assert!(tuple.len() >= 8);
    let t_tid = read_ipd(&tuple[0..6]);
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    IndexTupleData { t_tid, t_info }
}

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

/// `BTreeTupleGetPostingN(posting, n)` — the `n`-th heap TID of a posting list.
fn posting_list_n(tuple: &[u8], n: usize) -> ItemPointerData {
    let hdr = index_tuple_header(tuple);
    let off = BTreeTupleGetPostingOffset(&hdr) as usize;
    read_ipd(&tuple[off + n * SIZEOF_IPD..])
}

/// `BTreeTupleGetHeapTID(itup)` — first/lowest heap TID, or `None` when the
/// pivot tuple's heap-TID attribute was truncated.
fn heap_tid(tuple: &[u8]) -> Option<ItemPointerData> {
    let hdr = index_tuple_header(tuple);
    if BTreeTupleIsPivot(&hdr) {
        if (ItemPointerGetOffsetNumberNoCheck(&hdr.t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0 {
            let sz = IndexTupleSize(&hdr);
            let off = sz - SIZEOF_IPD;
            return Some(read_ipd(&tuple[off..]));
        }
        // Heap TID attribute was truncated
        None
    } else if BTreeTupleIsPosting(&hdr) {
        Some(posting_list_n(tuple, 0))
    } else {
        Some(hdr.t_tid)
    }
}

/// `BTreeTupleGetMaxHeapTID(itup)` — highest heap TID (non-pivot tuples only).
fn max_heap_tid(tuple: &[u8]) -> ItemPointerData {
    let hdr = index_tuple_header(tuple);
    debug_assert!(!BTreeTupleIsPivot(&hdr));
    if BTreeTupleIsPosting(&hdr) {
        let nposting = BTreeTupleGetNPosting(&hdr) as usize;
        posting_list_n(tuple, nposting - 1)
    } else {
        hdr.t_tid
    }
}

// ---------------------------------------------------------------------------
// Owned tuple-byte writers (writing through a palloc'd IndexTuple).
// ---------------------------------------------------------------------------

/// Write an [`IndexTupleData`] header into the start of `bytes`.
fn write_index_tuple_header(bytes: &mut [u8], hdr: &IndexTupleData) {
    write_ipd(bytes, 0, &hdr.t_tid);
    let info = hdr.t_info.to_ne_bytes();
    bytes[6] = info[0];
    bytes[7] = info[1];
}

/// Write a single [`ItemPointerData`] into `bytes` at byte offset `off`.
fn write_ipd(bytes: &mut [u8], off: usize, h: &ItemPointerData) {
    let hi = h.ip_blkid.bi_hi.to_ne_bytes();
    let lo = h.ip_blkid.bi_lo.to_ne_bytes();
    let pos = h.ip_posid.to_ne_bytes();
    bytes[off] = hi[0];
    bytes[off + 1] = hi[1];
    bytes[off + 2] = lo[0];
    bytes[off + 3] = lo[1];
    bytes[off + 4] = pos[0];
    bytes[off + 5] = pos[1];
}

/// Write a posting list (`htids`) into `bytes` starting at MAXALIGN'd `off`.
fn write_posting(bytes: &mut [u8], off: usize, htids: &[ItemPointerData]) {
    for (i, h) in htids.iter().enumerate() {
        write_ipd(bytes, off + i * SIZEOF_IPD, h);
    }
}

/// Allocate a zero-filled tuple buffer of `newsize` bytes in `mcx`
/// (the `palloc0(newsize)` of `_bt_form_posting`/`_bt_update_posting`).
fn alloc_tuple_bytes<'mcx>(mcx: Mcx<'mcx>, newsize: usize) -> PgResult<PgVec<'mcx, u8>> {
    let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, newsize)?;
    v.resize(newsize, 0);
    Ok(v)
}

// ---------------------------------------------------------------------------
// Working structs (access/nbtree.h, access/tableam.h) — runtime, not on-disk.
// ---------------------------------------------------------------------------

/// `BTDedupStateData` (`access/nbtree.h`) — working area for deduplication.
///
/// `base`/`htids` are owned copies of the relevant tuple bytes / heap-TID array
/// (charged to `mcx`), mirroring the C pointers into page/working memory.
pub struct BTDedupState<'mcx> {
    /* Deduplication status info for entire pass over page */
    /// Still deduplicating page?
    pub deduplicate: bool,
    /// Number of max-sized tuples so far
    pub nmaxitems: i32,
    /// Limit on size of final tuple
    pub maxpostingsize: Size,

    /* Metadata about base tuple of current pending posting list */
    /// base tuple bytes (used to form new posting list)
    pub base: PgVec<'mcx, u8>,
    /// page offset of base
    pub baseoff: OffsetNumber,
    /// base size without original posting list
    pub basetupsize: Size,

    /* Other metadata about pending posting list */
    /// Heap TIDs in pending posting list
    pub htids: PgVec<'mcx, ItemPointerData>,
    /// Number of existing tuples/line pointers
    pub nitems: i32,
    /// Includes line pointer overhead
    pub phystupsize: Size,

    /// current intervals in array
    pub nintervals: usize,
    pub intervals: PgVec<'mcx, BTDedupInterval>,
}

impl<'mcx> BTDedupState<'mcx> {
    /// `state->nhtids` — number of heap TIDs in `htids`.
    #[inline]
    fn nhtids(&self) -> i32 {
        self.htids.len() as i32
    }
}

/// Construct an initialized [`BTDedupState`] with the given `maxpostingsize`,
/// mirroring the field-by-field initialization in `_bt_dedup_pass` /
/// `_bt_bottomupdel_pass`. `base`/`htids` start empty (C: `base = NULL`,
/// `htids = palloc(maxpostingsize)`; here the heap-TID workspace grows on
/// demand, capped by maxpostingsize in `_bt_dedup_save_htid`).
pub fn new_dedup_state<'mcx>(mcx: Mcx<'mcx>, maxpostingsize: Size) -> PgResult<BTDedupState<'mcx>> {
    // C: state->intervals is sized MaxIndexTuplesPerPage up front.
    let cap = MaxIndexTuplesPerPage;
    let mut intervals: PgVec<'mcx, BTDedupInterval> = vec_with_capacity_in(mcx, cap)?;
    intervals.resize(cap, BTDedupInterval::default());
    Ok(BTDedupState {
        deduplicate: true,
        nmaxitems: 0,
        maxpostingsize,
        base: vec_with_capacity_in(mcx, 0)?,
        baseoff: 0, // InvalidOffsetNumber
        basetupsize: 0,
        htids: vec_with_capacity_in(mcx, 0)?,
        nitems: 0,
        phystupsize: 0,
        nintervals: 0,
        intervals,
    })
}

// ---------------------------------------------------------------------------
// Pure posting-list / tuple builders.
// ---------------------------------------------------------------------------

/// `_bt_form_posting()` — build a posting list tuple from `base` + `htids`.
///
/// Returns the newly-formed tuple as owned bytes (a `palloc`'d `IndexTuple` in
/// C). When `nhtids == 1`, builds a standard non-pivot tuple without a posting
/// list.
pub fn _bt_form_posting<'mcx>(
    mcx: Mcx<'mcx>,
    base: &[u8],
    htids: &[ItemPointerData],
    nhtids: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let basehdr = index_tuple_header(base);
    let keysize: u32 = if BTreeTupleIsPosting(&basehdr) {
        BTreeTupleGetPostingOffset(&basehdr)
    } else {
        IndexTupleSize(&basehdr) as u32
    };

    debug_assert!(!BTreeTupleIsPivot(&basehdr));
    debug_assert!(nhtids > 0 && nhtids <= PG_UINT16_MAX);
    debug_assert!(keysize as usize == maxalign(keysize as usize));

    /* Determine final size of new tuple */
    let newsize: u32 = if nhtids > 1 {
        maxalign(keysize as usize + nhtids as usize * SIZEOF_IPD) as u32
    } else {
        keysize
    };

    debug_assert!(newsize <= INDEX_SIZE_MASK as u32);
    debug_assert!(newsize as usize == maxalign(newsize as usize));

    /* Allocate memory using palloc0() (matches index_form_tuple()) */
    let mut itup_bytes = alloc_tuple_bytes(mcx, newsize as usize)?;
    itup_bytes[..keysize as usize].copy_from_slice(&base[..keysize as usize]);
    {
        let mut hdr = index_tuple_header(&itup_bytes);
        hdr.t_info &= !INDEX_SIZE_MASK;
        hdr.t_info |= newsize as u16;
        write_index_tuple_header(&mut itup_bytes, &hdr);
    }
    if nhtids > 1 {
        /* Form posting list tuple */
        {
            let mut hdr = index_tuple_header(&itup_bytes);
            BTreeTupleSetPosting(&mut hdr, nhtids as u16, keysize as i32);
            write_index_tuple_header(&mut itup_bytes, &hdr);
        }
        write_posting(&mut itup_bytes, keysize as usize, &htids[..nhtids as usize]);
        debug_assert!(_bt_posting_valid(&itup_bytes));
    } else {
        /* Form standard non-pivot tuple */
        let mut hdr = index_tuple_header(&itup_bytes);
        hdr.t_info &= !INDEX_ALT_TID_MASK;
        ItemPointerCopy(&htids[0], &mut hdr.t_tid);
        write_index_tuple_header(&mut itup_bytes, &hdr);
        debug_assert!(ItemPointerIsValid(Some(&hdr.t_tid)));
    }

    Ok(itup_bytes)
}

/// `_bt_update_posting()` — replace a posting list tuple, dropping the TIDs
/// listed in `deletetids`. `origtuple` is the existing tuple's bytes; returns
/// the updated tuple bytes (`vacposting->itup` on return).
pub fn _bt_update_posting<'mcx>(
    mcx: Mcx<'mcx>,
    origtuple: &[u8],
    deletetids: &[u16],
) -> PgResult<PgVec<'mcx, u8>> {
    let orighdr = index_tuple_header(origtuple);
    let ndeletedtids = deletetids.len() as i32;
    let norig = BTreeTupleGetNPosting(&orighdr) as i32;
    let nhtids = norig - ndeletedtids;

    debug_assert!(_bt_posting_valid(origtuple));
    debug_assert!(nhtids > 0 && nhtids < norig);

    /*
     * Determine final size of new tuple.  This calculation needs to match
     * _bt_form_posting().
     */
    let keysize = BTreeTupleGetPostingOffset(&orighdr);
    let newsize: u32 = if nhtids > 1 {
        maxalign(keysize as usize + nhtids as usize * SIZEOF_IPD) as u32
    } else {
        keysize
    };

    debug_assert!(newsize <= INDEX_SIZE_MASK as u32);
    debug_assert!(newsize as usize == maxalign(newsize as usize));

    /* Allocate memory using palloc0() (matches index_form_tuple()) */
    let mut itup_bytes = alloc_tuple_bytes(mcx, newsize as usize)?;
    itup_bytes[..keysize as usize].copy_from_slice(&origtuple[..keysize as usize]);
    {
        let mut hdr = index_tuple_header(&itup_bytes);
        hdr.t_info &= !INDEX_SIZE_MASK;
        hdr.t_info |= newsize as u16;
        write_index_tuple_header(&mut itup_bytes, &hdr);
    }

    /* Collect surviving heap TIDs from the original posting list (ui/d loop) */
    let mut surviving: PgVec<'mcx, ItemPointerData> = vec_with_capacity_in(mcx, nhtids as usize)?;
    let mut d = 0usize;
    for i in 0..norig {
        if d < deletetids.len() && deletetids[d] as i32 == i {
            d += 1;
            continue;
        }
        surviving.push(posting_list_n(origtuple, i as usize));
    }
    debug_assert!(surviving.len() == nhtids as usize);
    debug_assert!(d == deletetids.len());

    if nhtids > 1 {
        /* Form posting list tuple */
        {
            let mut hdr = index_tuple_header(&itup_bytes);
            BTreeTupleSetPosting(&mut hdr, nhtids as u16, keysize as i32);
            write_index_tuple_header(&mut itup_bytes, &hdr);
        }
        write_posting(&mut itup_bytes, keysize as usize, &surviving);
    } else {
        /* Form standard non-pivot tuple */
        let mut hdr = index_tuple_header(&itup_bytes);
        hdr.t_info &= !INDEX_ALT_TID_MASK;
        ItemPointerCopy(&surviving[0], &mut hdr.t_tid);
        write_index_tuple_header(&mut itup_bytes, &hdr);
    }

    debug_assert!(nhtids == 1 || _bt_posting_valid(&itup_bytes));

    Ok(itup_bytes)
}

/// `_bt_swap_posting()` — prepare for a posting list split by swapping the heap
/// TID in `newitem` with the original posting list's TID at `postingoff`.
///
/// Mutates `newitem` (caller's private copy) and returns the new posting list
/// tuple (owned bytes), guaranteed to be the same size as `oposting`.
pub fn _bt_swap_posting<'mcx>(
    mcx: Mcx<'mcx>,
    newitem: &mut [u8],
    oposting: &[u8],
    postingoff: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let ohdr = index_tuple_header(oposting);
    let nhtids = BTreeTupleGetNPosting(&ohdr) as i32;
    debug_assert!(_bt_posting_valid(oposting));

    /*
     * The postingoff argument originated as a _bt_binsrch_posting() return
     * value.  It will be 0 in the event of corruption that makes a leaf page
     * contain a non-pivot tuple that's somehow identical to newitem.  Perform a
     * basic sanity check to catch this case now.
     */
    if !(postingoff > 0 && postingoff < nhtids) {
        return Err(PgError::error(format!(
            "posting list tuple with {nhtids} items cannot be split at offset {postingoff}"
        )));
    }

    /*
     * Move item pointers in posting list to make a gap for the new item's heap
     * TID.  We shift TIDs one place to the right, losing original rightmost TID.
     */
    let mut nposting: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, oposting.len())?;
    nposting.extend_from_slice(oposting);
    let nposthdr = index_tuple_header(&nposting);
    let postoff = BTreeTupleGetPostingOffset(&nposthdr) as usize;
    let replacepos = postoff + postingoff as usize * SIZEOF_IPD;
    let replaceposright = postoff + (postingoff as usize + 1) * SIZEOF_IPD;
    let nmovebytes = (nhtids - postingoff - 1) as usize * SIZEOF_IPD;
    nposting.copy_within(replacepos..replacepos + nmovebytes, replaceposright);

    /* Fill the gap at postingoff with TID of new item (original new TID) */
    let newhdr = index_tuple_header(newitem);
    debug_assert!(!BTreeTupleIsPivot(&newhdr) && !BTreeTupleIsPosting(&newhdr));
    let newitem_tid = newhdr.t_tid;
    write_ipd(&mut nposting, replacepos, &newitem_tid);

    /* Now copy oposting's rightmost/max TID into new item (final new TID) */
    let omax = max_heap_tid(oposting);
    {
        let mut newh = index_tuple_header(newitem);
        ItemPointerCopy(&omax, &mut newh.t_tid);
        write_index_tuple_header(newitem, &newh);
    }

    debug_assert!(ItemPointerCompare(&max_heap_tid(&nposting), &heap_tid(newitem).unwrap()) < 0);
    debug_assert!(_bt_posting_valid(&nposting));

    Ok(nposting)
}

// ---------------------------------------------------------------------------
// Dedup-state machinery (operates on page items + working state).
// ---------------------------------------------------------------------------

/// `_bt_dedup_start_pending()` — create a new pending posting list based on
/// caller's base tuple (a page item byte slice at offset `baseoff`).
pub fn _bt_dedup_start_pending(
    state: &mut BTDedupState<'_>,
    base: &[u8],
    baseoff: OffsetNumber,
) -> PgResult<()> {
    debug_assert!(state.nhtids() == 0);
    debug_assert!(state.nitems == 0);
    let basehdr = index_tuple_header(base);
    debug_assert!(!BTreeTupleIsPivot(&basehdr));

    /* Copy heap TID(s) from new base tuple into working state's array */
    state.htids.clear();
    if !BTreeTupleIsPosting(&basehdr) {
        state.htids.push(basehdr.t_tid);
        state.basetupsize = IndexTupleSize(&basehdr);
    } else {
        let nposting = BTreeTupleGetNPosting(&basehdr) as usize;
        for i in 0..nposting {
            state.htids.push(posting_list_n(base, i));
        }
        /* basetupsize should not include existing posting list */
        state.basetupsize = BTreeTupleGetPostingOffset(&basehdr) as Size;
    }

    /*
     * Save new base tuple itself -- it'll be needed if we actually create a new
     * posting list from new pending posting list.
     */
    state.nitems = 1;
    state.base.clear();
    state.base.extend_from_slice(base);
    state.baseoff = baseoff;
    state.phystupsize = maxalign(IndexTupleSize(&basehdr)) + SIZEOF_ITEM_ID;
    /* Also save baseoff in pending state for interval */
    state.intervals[state.nintervals].baseoff = state.baseoff;
    Ok(())
}

/// `_bt_dedup_save_htid()` — save `itup`'s heap TID(s) into the pending posting
/// list where possible. Returns whether the merge happened.
pub fn _bt_dedup_save_htid(state: &mut BTDedupState<'_>, itup: &[u8]) -> PgResult<bool> {
    let ihdr = index_tuple_header(itup);
    debug_assert!(!BTreeTupleIsPivot(&ihdr));

    let (nhtids, is_posting): (i32, bool) = if !BTreeTupleIsPosting(&ihdr) {
        (1, false)
    } else {
        (BTreeTupleGetNPosting(&ihdr) as i32, true)
    };

    /*
     * Don't append if appending heap TID(s) from itup would put us over
     * maxpostingsize limit.  This must match _bt_form_posting().
     */
    let mergedtupsz = maxalign(state.basetupsize + (state.nhtids() + nhtids) as usize * SIZEOF_IPD);

    if mergedtupsz > state.maxpostingsize {
        /*
         * Count this as an oversized item for single value strategy, though only
         * when there are 50 TIDs in the final posting list tuple.
         */
        if state.nhtids() > 50 {
            state.nmaxitems += 1;
        }

        return Ok(false);
    }

    /* Save heap TIDs to pending posting list tuple */
    state.nitems += 1;
    if is_posting {
        for i in 0..nhtids as usize {
            state.htids.push(posting_list_n(itup, i));
        }
    } else {
        state.htids.push(ihdr.t_tid);
    }
    state.phystupsize += maxalign(IndexTupleSize(&ihdr)) + SIZEOF_ITEM_ID;

    Ok(true)
}

/// `_bt_dedup_finish_pending()` — finalize the pending posting list and add it
/// to `newpage`. Returns the space saving (including line pointer overhead; zero
/// when no deduplication was possible).
pub fn _bt_dedup_finish_pending(
    mcx: Mcx<'_>,
    newpage: &mut PageMut<'_>,
    state: &mut BTDedupState<'_>,
) -> PgResult<Size> {
    debug_assert!(state.nitems > 0);
    debug_assert!(state.nitems <= state.nhtids());
    debug_assert!(state.intervals[state.nintervals].baseoff == state.baseoff);

    let tupoff = OffsetNumberNext(PageGetMaxOffsetNumber(&newpage.as_ref()));
    let tuplesz: Size;
    let spacesaving: Size;
    if state.nitems == 1 {
        /* Use original, unchanged base tuple */
        let basehdr = index_tuple_header(&state.base);
        tuplesz = IndexTupleSize(&basehdr);
        debug_assert!(tuplesz == maxalign(IndexTupleSize(&basehdr)));
        debug_assert!(tuplesz <= BTMaxItemSize);
        if PageAddItemExtended(newpage, &state.base[..tuplesz], tupoff, 0)? == 0 {
            return Err(PgError::error("deduplication failed to add tuple to page"));
        }

        spacesaving = 0;
    } else {
        /* Form a tuple with a posting list */
        let final_tuple = _bt_form_posting(mcx, &state.base, &state.htids, state.nhtids())?;
        let finalhdr = index_tuple_header(&final_tuple);
        tuplesz = IndexTupleSize(&finalhdr);
        debug_assert!(tuplesz <= state.maxpostingsize);

        /* Save final number of items for posting list */
        state.intervals[state.nintervals].nitems = state.nitems as u16;

        debug_assert!(tuplesz == maxalign(IndexTupleSize(&finalhdr)));
        debug_assert!(tuplesz <= BTMaxItemSize);
        if PageAddItemExtended(newpage, &final_tuple[..tuplesz], tupoff, 0)? == 0 {
            return Err(PgError::error("deduplication failed to add tuple to page"));
        }

        spacesaving = state.phystupsize - (tuplesz + SIZEOF_ITEM_ID);
        /* Increment nintervals, since we wrote a new posting list tuple */
        state.nintervals += 1;
        debug_assert!(spacesaving > 0 && spacesaving < BLCKSZ);
    }

    /* Reset state for next pending posting list */
    state.htids.clear();
    state.nitems = 0;
    state.phystupsize = 0;

    Ok(spacesaving)
}

/// `_bt_bottomupdel_finish_pending()` — finalize an interval during bottom-up
/// deletion, moving its TIDs into `delstate`.
fn _bt_bottomupdel_finish_pending<'mcx>(
    page: &PageRef<'_>,
    state: &mut BTDedupState<'mcx>,
    delstate: &mut TmIndexDeleteOp<'mcx>,
) -> PgResult<()> {
    let dupinterval = state.nitems > 1;

    debug_assert!(state.nitems > 0);
    debug_assert!(state.nitems <= state.nhtids());
    debug_assert!(state.intervals[state.nintervals].baseoff == state.baseoff);

    for i in 0..state.nitems {
        let offnum = state.baseoff + i as OffsetNumber;
        let itemid = PageGetItemId(page, offnum)?;
        let itup = PageGetItem(page, &itemid)?;
        let ihdr = index_tuple_header(itup);

        if !BTreeTupleIsPosting(&ihdr) {
            /* Simple case: A plain non-pivot tuple */
            let id = delstate.deltids.len() as i16;
            delstate.deltids.push(TmIndexDelete {
                tid: ihdr.t_tid,
                id,
            });
            delstate.status.push(TmIndexStatus {
                idxoffnum: offnum,
                knowndeletable: false,  /* for now */
                promising: dupinterval, /* simple rule */
                freespace: ItemIdGetLength(&itemid) as i16 + SIZEOF_ITEM_ID as i16,
            });
        } else {
            /* Complicated case: A posting list tuple. */
            let nitem = BTreeTupleGetNPosting(&ihdr) as usize;
            let mut firstpromising = false;
            let mut lastpromising = false;

            debug_assert!(_bt_posting_valid(itup));

            if dupinterval {
                /*
                 * Complicated rule: either the first or last TID in the posting
                 * list gets marked promising (if any at all)
                 */
                let mintid = heap_tid(itup).ok_or_else(|| {
                    PgError::error("_bt_bottomupdel_finish_pending: itup has no heap TID")
                })?;
                let midtid = posting_list_n(itup, nitem / 2);
                let maxtid = max_heap_tid(itup);
                let minblocklist = ItemPointerGetBlockNumber(&mintid);
                let midblocklist = ItemPointerGetBlockNumber(&midtid);
                let maxblocklist = ItemPointerGetBlockNumber(&maxtid);

                /* Only entry with predominant table block can be promising */
                firstpromising = minblocklist == midblocklist;
                lastpromising = !firstpromising && midblocklist == maxblocklist;
            }

            for p in 0..nitem {
                let htid = posting_list_n(itup, p);

                let id = delstate.deltids.len() as i16;
                delstate.deltids.push(TmIndexDelete { tid: htid, id });
                let promising = (firstpromising && p == 0) || (lastpromising && p == nitem - 1);
                delstate.status.push(TmIndexStatus {
                    idxoffnum: offnum,
                    knowndeletable: false, /* for now */
                    promising,
                    freespace: SIZEOF_IPD as i16, /* at worst */
                });
            }
        }
    }

    if dupinterval {
        state.intervals[state.nintervals].nitems = state.nitems as u16;
        state.nintervals += 1;
    }

    /* Reset state for next interval */
    state.htids.clear();
    state.nitems = 0;
    state.phystupsize = 0;
    Ok(())
}

/// `_bt_do_singleval()` — determine whether the page's data items are all
/// duplicates of the same value (single value strategy should be applied).
fn _bt_do_singleval(
    rel: &Relation<'_>,
    page: &PageRef<'_>,
    minoff: OffsetNumber,
    newitem: &[u8],
) -> PgResult<bool> {
    let nkeyatts = rel.indnkeyatts();

    let itemid = PageGetItemId(page, minoff)?;
    let itup = PageGetItem(page, &itemid)?;

    if nbtcore::bt_keep_natts_fast::call(rel, newitem, itup)? > nkeyatts {
        let itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page))?;
        let itup = PageGetItem(page, &itemid)?;

        if nbtcore::bt_keep_natts_fast::call(rel, newitem, itup)? > nkeyatts {
            return Ok(true);
        }
    }

    Ok(false)
}

/// `_bt_singleval_fillfactor()` — lower `maxpostingsize` for the single value
/// strategy to avoid a sixth maxpostingsize-capped tuple.
fn _bt_singleval_fillfactor(page: &PageRef<'_>, state: &mut BTDedupState<'_>, newitemsz: Size) {
    /* This calculation needs to match nbtsplitloc.c */
    let mut leftfree = PageGetPageSize(page)
        - SizeOfPageHeaderData
        - maxalign(::core::mem::size_of::<BTPageOpaqueData>());
    /* Subtract size of new high key (includes pivot heap TID space) */
    leftfree -= newitemsz + maxalign(SIZEOF_IPD);

    /*
     * Reduce maxpostingsize by an amount equal to target free space on left half
     * of page
     */
    let reduction =
        (leftfree as f64 * ((100 - BTREE_SINGLEVAL_FILLFACTOR) as f64 / 100.0)) as Size;
    if state.maxpostingsize > reduction {
        state.maxpostingsize -= reduction;
    } else {
        state.maxpostingsize = 0;
    }
}

/// Clear `BTP_HAS_GARBAGE` in the page's `BTPageOpaqueData` (in the special
/// area). The special area starts at the `pd_special` offset (a `u16` at byte
/// offset 16 of the page header), and `btpo_flags` is at offset 12 within the
/// 16-byte `BTPageOpaqueData`.
fn clear_has_garbage(bytes: &mut [u8]) {
    let special_off = u16::from_ne_bytes([bytes[16], bytes[17]]) as usize;
    let off = special_off + 12;
    let mut flags = u16::from_ne_bytes([bytes[off], bytes[off + 1]]);
    flags &= !BTP_HAS_GARBAGE;
    let nb = flags.to_ne_bytes();
    bytes[off] = nb[0];
    bytes[off + 1] = nb[1];
}

// ---------------------------------------------------------------------------
// Top-level entry points.
// ---------------------------------------------------------------------------

/// `_bt_dedup_pass()` — perform a deduplication pass over `buf`'s leaf page.
///
/// `newitemsz` is MAXALIGN'd but does not include the line pointer (added here).
pub fn _bt_dedup_pass<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: Buffer,
    newitem: &[u8],
    newitemsz: Size,
    bottomupdedup: bool,
) -> PgResult<()> {
    let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
    let page = PageRef::new(&page_bytes)?;
    let opaque = BTPageGetOpaque(&page)?;
    let mut singlevalstrat = false;
    let nkeyatts = rel.indnkeyatts();

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    let newitemsz = newitemsz + SIZEOF_ITEM_ID;

    /*
     * Initialize deduplication state.
     *
     * maxpostingsize is limited to one sixth of a page (Min(BTMaxItemSize / 2,
     * INDEX_SIZE_MASK)).
     */
    let mut state = new_dedup_state(mcx, (BTMaxItemSize / 2).min(INDEX_SIZE_MASK as Size))?;

    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = PageGetMaxOffsetNumber(&page);

    /* Consider applying "single value" strategy */
    if !bottomupdedup {
        singlevalstrat = _bt_do_singleval(rel, &page, minoff, newitem)?;
    }

    /*
     * Deduplicate items from page, and write them to newpage.
     *
     * Copy the original page's LSN into newpage copy; XLogInsert examines it.
     */
    let mut newtemp = PageGetTempPageCopySpecial(&page)?;
    {
        let mut newpage = PageMut::new(newtemp.as_mut_bytes())?;
        PageSetLSN(&mut newpage, PageGetLSN(&page));
    }

    /* Copy high key, if any */
    if !P_RIGHTMOST(&opaque) {
        let hitemid = PageGetItemId(&page, P_HIKEY)?;
        let hitemsz = ItemIdGetLength(&hitemid) as usize;
        let hitem = PageGetItem(&page, &hitemid)?;
        let hitem_owned: PgVec<'mcx, u8> = {
            let mut v = vec_with_capacity_in(mcx, hitemsz)?;
            v.extend_from_slice(&hitem[..hitemsz]);
            v
        };
        let mut newpage = PageMut::new(newtemp.as_mut_bytes())?;
        if PageAddItemExtended(&mut newpage, &hitem_owned, P_HIKEY, 0)? == 0 {
            return Err(PgError::error("deduplication failed to add highkey"));
        }
    }

    let mut offnum = minoff;
    while offnum <= maxoff {
        let itemid = PageGetItemId(&page, offnum)?;
        let itup = PageGetItem(&page, &itemid)?;
        debug_assert!(!ItemIdIsDead(&itemid));

        if offnum == minoff {
            /* No previous/base tuple -- use as base tuple of pending list */
            _bt_dedup_start_pending(&mut state, itup, offnum)?;
        } else if state.deduplicate
            && nbtcore::bt_keep_natts_fast::call(rel, &state.base, itup)? > nkeyatts
            && _bt_dedup_save_htid(&mut state, itup)?
        {
            /* Tuple is equal to base tuple of pending posting list. */
        } else {
            /* Tuple not equal / merge declined: finalize pending list. */
            {
                let mut newpage = PageMut::new(newtemp.as_mut_bytes())?;
                _bt_dedup_finish_pending(mcx, &mut newpage, &mut state)?;
            }

            if singlevalstrat {
                /*
                 * Single value strategy's extra steps.
                 *
                 * Lower maxpostingsize for the sixth and final large posting list
                 * tuple once 5 maxpostingsize-capped tuples have been
                 * formed/observed; stop merging entirely on the sixth.
                 */
                if state.nmaxitems == 5 {
                    _bt_singleval_fillfactor(&page, &mut state, newitemsz);
                } else if state.nmaxitems == 6 {
                    state.deduplicate = false;
                    singlevalstrat = false; /* won't be back here */
                }
            }

            /* itup starts new pending posting list */
            _bt_dedup_start_pending(&mut state, itup, offnum)?;
        }

        offnum = OffsetNumberNext(offnum);
    }

    /* Handle the last item */
    {
        let mut newpage = PageMut::new(newtemp.as_mut_bytes())?;
        _bt_dedup_finish_pending(mcx, &mut newpage, &mut state)?;
    }

    /*
     * If no items suitable for deduplication were found, newpage must be exactly
     * the same as the original page, so just return from function.
     */
    if state.nintervals == 0 {
        // cannot leak memory here (newtemp / state dropped at scope exit)
        return Ok(());
    }

    /*
     * Clear the BTP_HAS_GARBAGE page flag.  The index must be a heapkeyspace
     * index, and as such we'll never pay attention to BTP_HAS_GARBAGE anyway.
     * But keep things tidy.
     */
    if P_HAS_GARBAGE(&opaque) {
        clear_has_garbage(newtemp.as_mut_bytes());
    }

    // Drop the read snapshot of the page before re-entering bufmgr for the
    // in-place write-back below.
    let needs_wal = relcache::relation_needs_wal::call(rel);
    let newbytes: PgVec<'mcx, u8> = {
        let mut v = vec_with_capacity_in(mcx, newtemp.as_bytes().len())?;
        v.extend_from_slice(newtemp.as_bytes());
        v
    };
    // The read borrow of `page` / `page_bytes` ends at its last use above; the
    // bufmgr write-back below re-enters the buffer fresh.

    miscinit::start_crit_section::call();

    // PageRestoreTempPage(newpage, page): replace the buffer's page image with
    // the deduplicated temp page in place.
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        page.copy_from_slice(&newbytes);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(buf);

    /* XLOG stuff */
    if needs_wal {
        // xl_btree_dedup xlrec_dedup; xlrec_dedup.nintervals = state->nintervals;
        let nintervals = state.nintervals as u16;
        let xlrec_dedup = nintervals.to_ne_bytes();

        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
        xloginsert::xlog_register_data::call(&xlrec_dedup[..SizeOfBtreeDedup])?;

        /*
         * The intervals array is not in the buffer, but pretend that it is.
         * When XLogInsert stores the whole buffer, the array need not be stored
         * too.
         */
        let intervals_bytes = serialize_intervals(&state.intervals[..state.nintervals]);
        xloginsert::xlog_register_buf_data::call(0, &intervals_bytes)?;

        let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_DEDUP)?;

        // PageSetLSN(page, recptr)
        bufmgr::page_set_lsn::call(buf, recptr)?;
    }

    miscinit::end_crit_section::call();

    Ok(())
}

/// Serialize a slice of [`BTDedupInterval`] into its on-disk byte image
/// (`baseoff: uint16`, `nitems: uint16`, native-endian; 4 bytes each) for
/// `XLogRegisterBufData`.
fn serialize_intervals(intervals: &[BTDedupInterval]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::with_capacity(intervals.len() * 4);
    for iv in intervals {
        out.extend_from_slice(&iv.baseoff.to_ne_bytes());
        out.extend_from_slice(&iv.nitems.to_ne_bytes());
    }
    out
}

/// `_bt_bottomupdel_pass()` — perform a bottom-up index deletion pass.
///
/// Returns true on success (caller can assume a page split will be avoided for a
/// reasonable time); false when the caller should deduplicate the page.
pub fn _bt_bottomupdel_pass<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: Buffer,
    heap_rel: &Relation<'mcx>,
    newitemsz: Size,
) -> PgResult<bool> {
    let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
    let page = PageRef::new(&page_bytes)?;
    let opaque = BTPageGetOpaque(&page)?;
    let nkeyatts = rel.indnkeyatts();

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    let newitemsz = newitemsz + SIZEOF_ITEM_ID;

    /* Initialize deduplication state (we're not really deduplicating) */
    let mut state = new_dedup_state(mcx, BLCKSZ)?;

    /*
     * Initialize tableam state that describes bottom-up index deletion.
     *
     * deltids/status are sized MaxTIDsPerBTreePage up front, matching the C
     * palloc.
     */
    let cap = MaxTIDsPerBTreePage;
    let deltids: PgVec<'mcx, TmIndexDelete> = vec_with_capacity_in(mcx, cap)?;
    let status: PgVec<'mcx, TmIndexStatus> = vec_with_capacity_in(mcx, cap)?;
    let mut delstate = TmIndexDeleteOp {
        iblknum: bufmgr::buffer_get_block_number::call(buf),
        bottomup: true,
        bottomupfreespace: (BLCKSZ / 16).max(newitemsz) as i32,
        deltids,
        status,
    };

    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = PageGetMaxOffsetNumber(&page);
    let mut offnum = minoff;
    while offnum <= maxoff {
        let itemid = PageGetItemId(&page, offnum)?;
        let itup = PageGetItem(&page, &itemid)?;
        debug_assert!(!ItemIdIsDead(&itemid));

        if offnum == minoff {
            /* itup starts first pending interval */
            _bt_dedup_start_pending(&mut state, itup, offnum)?;
        } else if nbtcore::bt_keep_natts_fast::call(rel, &state.base, itup)? > nkeyatts
            && _bt_dedup_save_htid(&mut state, itup)?
        {
            /* Tuple is equal; just added its TIDs to pending interval */
        } else {
            /* Finalize interval -- move its TIDs to delete state */
            _bt_bottomupdel_finish_pending(&page, &mut state, &mut delstate)?;
            /* itup starts new pending interval */
            _bt_dedup_start_pending(&mut state, itup, offnum)?;
        }

        offnum = OffsetNumberNext(offnum);
    }
    /* Finalize final interval -- move its TIDs to delete state */
    _bt_bottomupdel_finish_pending(&page, &mut state, &mut delstate)?;

    /*
     * We should at least avoid having our caller do a useless deduplication pass
     * after we return in the event of zero promising tuples.
     */
    let neverdedup = state.nintervals == 0;

    // The borrow of `page` (and thus `page_bytes`) ends at its last use above;
    // the seam call below re-reads the buffer fresh.

    /* Ask tableam which TIDs are deletable, then physically delete them */
    nbtcore::bt_delitems_delete_check::call(mcx, rel, buf, heap_rel, delstate)?;

    /* Report "success" to caller unconditionally to avoid deduplication */
    if neverdedup {
        return Ok(true);
    }

    /* Don't dedup when we won't end up back here any time soon anyway */
    let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
    let page = PageRef::new(&page_bytes)?;
    Ok(PageGetExactFreeSpace(&page) >= (BLCKSZ / 24).max(newitemsz))
}

// ---------------------------------------------------------------------------
// Posting-list validity check (C: _bt_posting_valid, USE_ASSERT_CHECKING).
// ---------------------------------------------------------------------------

/// `_bt_posting_valid()` — verify posting list invariants.
pub fn _bt_posting_valid(posting: &[u8]) -> bool {
    let hdr = index_tuple_header(posting);
    if !BTreeTupleIsPosting(&hdr) || BTreeTupleGetNPosting(&hdr) < 2 {
        return false;
    }

    /* Remember first heap TID for loop */
    let mut last = match heap_tid(posting) {
        Some(h) => h,
        None => return false,
    };
    if !ItemPointerIsValid(Some(&last)) {
        return false;
    }

    /* Iterate, starting from second TID */
    let n = BTreeTupleGetNPosting(&hdr) as usize;
    for i in 1..n {
        let htid = posting_list_n(posting, i);
        if !ItemPointerIsValid(Some(&htid)) {
            return false;
        }
        if ItemPointerCompare(&htid, &last) <= 0 {
            return false;
        }
        ItemPointerCopy(&htid, &mut last);
    }

    true
}

#[cfg(test)]
mod tests;

/// Install this crate's owned seams. `nbtdedup.c` owns no inward seam crate
/// (no sibling unit calls back into it across a cycle), so this is a no-op; the
/// dedup-consumed seams it declares in `backend-access-nbtree-core-seams` are
/// outward (installed by their respective owners). Kept for the uniform
/// `seams-init` wiring contract.
pub fn init_seams() {}
