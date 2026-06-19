#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_range_loop)]
#![allow(dead_code)]

//! Port of `src/backend/access/nbtree/nbtinsert.c` (PostgreSQL 18.3) — item
//! insertion in Lehman and Yao btrees.
//!
//! Every top-level function of `nbtinsert.c` is ported here (C names preserved):
//! `_bt_doinsert`, `_bt_search_insert`, `_bt_check_unique`, `_bt_findinsertloc`,
//! `_bt_stepright`, `_bt_insertonpg`, `_bt_split`, `_bt_insert_parent`,
//! `_bt_finish_split`, `_bt_getstackbuf`, `_bt_newlevel`, `_bt_pgaddtup`,
//! `_bt_delete_or_dedup_one_page`, `_bt_simpledel_pass`, `_bt_deadblocks`,
//! `_bt_blk_cmp`.
//!
//! # Repo model (mirrors `nbtdedup.c` / `page.rs` / `search.rs`)
//!
//! Pages are read via `bufmgr::buffer_get_page` into an owned `PgVec<u8>` decoded
//! through [`PageRef`]; mutations go through `bufmgr::with_buffer_page` (the safe
//! equivalent of writing through a pinned buffer's `Page` pointer). Index tuples
//! are carried as owned `PgVec<u8>` / borrowed `&[u8]` byte images exactly as in
//! `nbtdedup.c`. The posting-list builders (`_bt_form_posting`,
//! `_bt_swap_posting`) and the dedup / bottom-up passes are reused verbatim from
//! [`backend_access_nbt_dedup`]; the page-format reads / scankey comparisons /
//! tree descent are reused from the sibling [`crate::search`], [`crate::utils`],
//! [`crate::page`], [`crate::splitloc`] modules.
//!
//! Critical sections / WAL mirror `nbtdedup.c`'s idioms exactly
//! (`miscinit::{start,end}_crit_section`, `xloginsert::*`, `REGBUF_*`).
//!
//! # Genuinely-unported callees (honest seam-and-panic, never `todo!`)
//!
//!   * `table_index_fetch_tuple_check` (tableam dispatch) — `_bt_check_unique`'s
//!     heap-visibility probe, reached through `backend-access-table-tableam-seams`
//!     (calling it directly would cycle, as the dispatch reaches the heap AM,
//!     which transitively depends on nbtree). All of the uniqueness logic
//!     *around* the probe (the `_bt_compare` equality scan, LP_DEAD garbage
//!     marking, the speculative / partial-check handling, the right-sibling
//!     walk, the conflict ereport) is ported.
//!   * `_bt_allocbuf` / `_bt_conditionallockbuf` (nbtpage.c) — no FSM /
//!     ConditionalLockBuffer / ExtendBufferedRel seam exists yet (the page.rs
//!     copies already panic at these exact boundaries). The split / new-root /
//!     fastpath paths bottom out here.
//!   * `CheckForSerializableConflictIn` / `PredicateLockPage(Split)` /
//!     `SpeculativeInsertionWait` / `XactLockTableWait` (SSI / speculative-insert)
//!     — not plumbed to this layer; behaviour-preserving no-ops / honest panics
//!     as noted at each call site.
//!   * `BuildIndexValueDescription` (genam) — cosmetic unique-violation detail;
//!     omitted with a faithful message (the `Err` itself is load-bearing).

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec::Vec as StdVec;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::{BlockNumber, OffsetNumber, RmgrId, Size, TransactionId};
use types_core::xact::TransactionIdIsValid;
use types_error::{PgError, PgResult, ERRCODE_UNIQUE_VIOLATION};
use types_nbtree::{
    xl_btree_insert, xl_btree_metadata, xl_btree_newroot, xl_btree_split, BTMetaPageData,
    BTPageOpaqueData, BTInsertStateData, BTScanInsert, BTStack, BTStackData,
    IndexUniqueCheck, TmIndexDelete, TmIndexDeleteOp, TmIndexStatus, BTMaxItemSize, BTP_HAS_GARBAGE,
    BTP_INCOMPLETE_SPLIT, BTP_LEAF, BTP_ROOT, BTP_SPLIT_END, BTREE_METAPAGE, BTREE_NOVAC_VERSION,
    BT_IS_POSTING, BT_OFFSET_MASK, INDEX_ALT_TID_MASK, MaxIndexTuplesPerPage, MaxTIDsPerBTreePage,
    P_FIRSTKEY, P_HIKEY, P_NONE, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META,
    XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R,
};
use types_rel::Relation;
use types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE};
use types_storage::storage::{Buffer, InvalidBuffer};
use types_tuple::heaptuple::{
    BlockIdData, IndexTupleData, IndexTupleSize, ItemPointerData,
    INVALID_OFFSET_NUMBER, FIRST_OFFSET_NUMBER,
};
use types_wal::xloginsert::{REGBUF_STANDARD, REGBUF_WILL_INIT};

use backend_storage_page::{
    ItemIdGetLength, ItemIdIsDead, ItemPointerCompare, PageAddItemExtended,
    PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetPageSize,
    PageRef,
};

use backend_access_nbt_dedup as dedup;
use backend_access_table_tableam_seams as tableam_seams;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscinit;

use crate::page::{
    bt_relbuf as page_bt_relbuf, _bt_getbuf, _bt_relandgetbuf, _bt_upgrademetapage,
};

// ===========================================================================
// Constants (c.h / off.h / nbtree.h / nbtxlog.h).
// ===========================================================================

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
/// `sizeof(IndexTupleData)` (header only — 8 bytes).
const SIZEOF_INDEX_TUPLE_DATA: usize = 8;

/// `RM_BTREE_ID` (`access/rmgrlist.h`).
const RM_BTREE_ID: RmgrId = 11;

/// `BT_READ` / `BT_WRITE` (`access/nbtree.h`).
const BT_READ: i32 = BUFFER_LOCK_SHARE;
const BT_WRITE: i32 = BUFFER_LOCK_EXCLUSIVE;

/// `BTREE_FASTPATH_MIN_LEVEL` (nbtinsert.c).
const BTREE_FASTPATH_MIN_LEVEL: u32 = 2;

/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = INVALID_OFFSET_NUMBER;
/// `FirstOffsetNumber`.
const FirstOffsetNumber: OffsetNumber = FIRST_OFFSET_NUMBER;
/// `InvalidBlockNumber` (`storage/block.h`).
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

/// `SizeOfBtreeInsert` (`nbtxlog.h`) = `offsetof(xl_btree_insert, offnum) +
/// sizeof(OffsetNumber)` = `sizeof(OffsetNumber)` (single field).
const SizeOfBtreeInsert: usize = ::core::mem::size_of::<OffsetNumber>();
/// `SizeOfBtreeSplit` (`nbtxlog.h`) = `offsetof(xl_btree_split, postingoff) +
/// sizeof(uint16)`: level(u32) + firstrightoff(u16) + newitemoff(u16) +
/// postingoff(u16) = 10 bytes.
const SizeOfBtreeSplit: usize = 4 + 2 + 2 + 2;
/// `SizeOfBtreeNewroot` (`nbtxlog.h`): rootblk(u32) + level(u32) = 8.
const SizeOfBtreeNewroot: usize = 4 + 4;
/// `sizeof(xl_btree_metadata)` on-disk image.
const SIZEOF_XL_BTREE_METADATA: usize = 4 + 4 + 4 + 4 + 4 + 4 + 1;

/// `PageAddItem(page, item, size, off, overwrite=false, is_heap=false)` flags.
const PAI_OVERWRITE: i32 = 0;

/// `OffsetNumberNext(offsetNumber)` / `OffsetNumberPrev(offsetNumber)`.
#[inline]
const fn OffsetNumberNext(o: OffsetNumber) -> OffsetNumber {
    o + 1
}
#[inline]
const fn OffsetNumberPrev(o: OffsetNumber) -> OffsetNumber {
    o.wrapping_sub(1)
}

/// The index/scan memory context for `BufferGetPage` reads + `'mcx`
/// allocations. The `bt_doinsert` seam carries no explicit `Mcx`; the index
/// `Relation` carries `'mcx` metadata, so its allocator is the genuine
/// insert-lifetime context (mirrors `search.rs::rel_mcx`).
#[inline]
fn rel_mcx<'mcx>(rel: &Relation<'mcx>) -> Mcx<'mcx> {
    *rel.rd_opcintype.allocator()
}

/// `IndexRelationGetNumberOfKeyAttributes(rel)`.
#[inline]
fn rel_nkeyatts(rel: &Relation) -> i32 {
    rel.indnkeyatts()
}
/// `IndexRelationGetNumberOfAttributes(rel)`.
#[inline]
fn rel_natts(rel: &Relation) -> i32 {
    rel.rd_att.natts
}
/// `RelationGetRelationName(rel)`.
#[inline]
fn rel_name<'a>(rel: &'a Relation<'a>) -> &'a str {
    rel.name()
}

// ===========================================================================
// IndexTuple header / opaque byte codec (mirrors nbtdedup.c / page.rs).
// ===========================================================================

fn read_ipd(bytes: &[u8]) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_ne_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_ne_bytes([bytes[4], bytes[5]]),
    }
}

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

fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    IndexTupleData {
        t_tid: read_ipd(&tuple[0..6]),
        t_info: u16::from_ne_bytes([tuple[6], tuple[7]]),
    }
}

fn write_index_tuple_header(bytes: &mut [u8], hdr: &IndexTupleData) {
    write_ipd(bytes, 0, &hdr.t_tid);
    let info = hdr.t_info.to_ne_bytes();
    bytes[6] = info[0];
    bytes[7] = info[1];
}

/// `ItemPointerGetBlockNumber(&t_tid)`.
#[inline]
fn ipd_block_number(t: &ItemPointerData) -> BlockNumber {
    ((t.ip_blkid.bi_hi as u32) << 16) | (t.ip_blkid.bi_lo as u32)
}
/// `ItemPointerSetBlockNumber(&mut t_tid, blkno)`.
#[inline]
fn ipd_set_block_number(t: &mut ItemPointerData, blkno: BlockNumber) {
    t.ip_blkid.bi_hi = (blkno >> 16) as u16;
    t.ip_blkid.bi_lo = (blkno & 0xFFFF) as u16;
}
/// `ItemPointerGetOffsetNumber(&t_tid)`.
#[inline]
fn ipd_offset(t: &ItemPointerData) -> u16 {
    t.ip_posid
}
/// `ItemPointerSetOffsetNumber(&mut t_tid, off)`.
#[inline]
fn ipd_set_offset(t: &mut ItemPointerData, off: u16) {
    t.ip_posid = off;
}

// --- BTPageOpaqueData decode / in-place encode (special area). -------------

fn special_offset(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[16], page[17]]) as usize
}

fn decode_opaque(special: &[u8]) -> BTPageOpaqueData {
    let rd_u32 = |off: usize| -> u32 {
        u32::from_ne_bytes([
            special[off],
            special[off + 1],
            special[off + 2],
            special[off + 3],
        ])
    };
    let rd_u16 = |off: usize| -> u16 { u16::from_ne_bytes([special[off], special[off + 1]]) };
    BTPageOpaqueData {
        btpo_prev: rd_u32(0),
        btpo_next: rd_u32(4),
        btpo_level: rd_u32(8),
        btpo_flags: rd_u16(12),
        btpo_cycleid: rd_u16(14),
    }
}

fn opaque_from_page(page: &[u8]) -> PgResult<BTPageOpaqueData> {
    let off = special_offset(page);
    if off + 16 > page.len() {
        return Err(PgError::error("BTPageGetOpaque: special area out of bounds"));
    }
    Ok(decode_opaque(&page[off..off + 16]))
}

fn encode_opaque(page: &mut [u8], opaque: &BTPageOpaqueData) {
    let off = special_offset(page);
    page[off..off + 4].copy_from_slice(&opaque.btpo_prev.to_ne_bytes());
    page[off + 4..off + 8].copy_from_slice(&opaque.btpo_next.to_ne_bytes());
    page[off + 8..off + 12].copy_from_slice(&opaque.btpo_level.to_ne_bytes());
    page[off + 12..off + 14].copy_from_slice(&opaque.btpo_flags.to_ne_bytes());
    page[off + 14..off + 16].copy_from_slice(&opaque.btpo_cycleid.to_ne_bytes());
}

// --- opaque flag predicates (nbtree.h P_* macros). -------------------------

#[inline]
fn P_RIGHTMOST(o: &BTPageOpaqueData) -> bool {
    o.btpo_next == P_NONE
}
#[inline]
fn P_LEFTMOST(o: &BTPageOpaqueData) -> bool {
    o.btpo_prev == P_NONE
}
#[inline]
fn P_ISLEAF(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_LEAF) != 0
}
#[inline]
fn P_ISROOT(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_ROOT) != 0
}
#[inline]
fn P_IGNORE(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & (types_nbtree::BTP_DELETED | types_nbtree::BTP_HALF_DEAD)) != 0
}
#[inline]
fn P_INCOMPLETE_SPLIT(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_INCOMPLETE_SPLIT) != 0
}
#[inline]
fn P_HAS_GARBAGE(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_HAS_GARBAGE) != 0
}
/// `P_FIRSTDATAKEY(opaque)`.
#[inline]
fn P_FIRSTDATAKEY(o: &BTPageOpaqueData) -> OffsetNumber {
    if P_RIGHTMOST(o) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

// --- BTreeTuple pivot/posting/downlink helpers. ----------------------------

fn BTreeTupleIsPivot(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ipd_offset(&itup.t_tid) & BT_IS_POSTING) == 0
}

fn BTreeTupleIsPosting(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ipd_offset(&itup.t_tid) & BT_IS_POSTING) != 0
}

fn BTreeTupleGetNPosting(posting: &IndexTupleData) -> u16 {
    ipd_offset(&posting.t_tid) & BT_OFFSET_MASK
}

fn BTreeTupleGetPostingOffset(posting: &IndexTupleData) -> u32 {
    ipd_block_number(&posting.t_tid)
}

/// `BTreeTupleGetPostingN(posting, n)` — n-th heap TID of a posting list tuple.
fn posting_list_n(tuple: &[u8], n: usize) -> ItemPointerData {
    let hdr = index_tuple_header(tuple);
    let off = BTreeTupleGetPostingOffset(&hdr) as usize;
    read_ipd(&tuple[off + n * SIZEOF_IPD..])
}

/// `BTreeTupleGetDownLink(pivot)`.
fn BTreeTupleGetDownLink(pivot: &[u8]) -> BlockNumber {
    ipd_block_number(&index_tuple_header(pivot).t_tid)
}

/// `BTreeTupleSetDownLink(pivot, blkno)` — write the downlink block number into a
/// pivot tuple's overloaded `t_tid` block id (offset is left untouched).
fn BTreeTupleSetDownLink(bytes: &mut [u8], blkno: BlockNumber) {
    let mut hdr = index_tuple_header(bytes);
    ipd_set_block_number(&mut hdr.t_tid, blkno);
    write_index_tuple_header(bytes, &hdr);
}

/// `BTreeTupleSetNAtts(itup, natts, heaptid)` — mark `itup` a pivot tuple and
/// record `natts` key attributes (and whether a pivot heap-TID attr follows).
fn BTreeTupleSetNAtts(bytes: &mut [u8], natts: u16, heaptid: bool) {
    debug_assert!((natts & INDEX_ALT_TID_MASK) == 0);
    let mut hdr = index_tuple_header(bytes);
    hdr.t_info |= INDEX_ALT_TID_MASK;
    let mut offset = natts;
    if heaptid {
        offset |= types_nbtree::BT_PIVOT_HEAP_TID_ATTR;
    }
    ipd_set_offset(&mut hdr.t_tid, offset);
    write_index_tuple_header(bytes, &hdr);
}

/// `CopyIndexTuple(itup)` over a byte slice — a straight owned copy (in `mcx`).
fn copy_index_tuple<'mcx>(mcx: Mcx<'mcx>, itup: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let sz = IndexTupleSize(&index_tuple_header(itup));
    let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, sz)?;
    v.extend_from_slice(&itup[..sz]);
    Ok(v)
}

/// Build an [`IndexTuple`](types_tuple::heaptuple::IndexTuple) header box from
/// the leading 8 bytes of `itup`, for storing into `insertstate.itup` (whose
/// field type is the header-carrier box; the actual on-page bytes are carried
/// separately by the insert path).
fn index_tuple_box<'mcx>(
    mcx: Mcx<'mcx>,
    itup: &[u8],
) -> PgResult<types_tuple::heaptuple::IndexTuple<'mcx>> {
    Ok(Some(alloc_in(mcx, index_tuple_header(itup))?))
}

// ===========================================================================
// PageAddItem wrapper (the bare `PageAddItem(page, item, sz, off, false, false)`).
// ===========================================================================

/// `PageAddItem(page, item, size, offsetNumber, overwrite=false, is_heap=false)`
/// over a raw page byte slice. Returns the offset (or `InvalidOffsetNumber` on
/// failure). `item` is the tuple bytes; `size` must be the (MAXALIGN'd) item
/// length the caller wants recorded (we pass `&item[..size]`).
fn PageAddItem(page: &mut [u8], item: &[u8], size: usize, off: OffsetNumber) -> OffsetNumber {
    let mut pm = match backend_storage_page::PageMut::new(page) {
        Ok(p) => p,
        Err(_) => return InvalidOffsetNumber,
    };
    let n = size.min(item.len());
    match PageAddItemExtended(&mut pm, &item[..n], off, PAI_OVERWRITE) {
        Ok(o) => o,
        Err(_) => InvalidOffsetNumber,
    }
}

// --- PageHeader pd_upper / pd_special direct reads (for right-page WAL). ----

/// `((PageHeader) page)->pd_upper` (u16 @ offset 14).
#[inline]
fn pd_upper(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[14], page[15]]) as usize
}
/// `((PageHeader) page)->pd_special` (u16 @ offset 16).
#[inline]
fn pd_special(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[16], page[17]]) as usize
}

// ===========================================================================
// xl_btree_* serialization (on-disk byte images for XLogRegister*).
// ===========================================================================

fn serialize_xl_btree_metadata(md: &xl_btree_metadata) -> StdVec<u8> {
    let mut out = StdVec::with_capacity(SIZEOF_XL_BTREE_METADATA);
    out.extend_from_slice(&md.version.to_ne_bytes());
    out.extend_from_slice(&md.root.to_ne_bytes());
    out.extend_from_slice(&md.level.to_ne_bytes());
    out.extend_from_slice(&md.fastroot.to_ne_bytes());
    out.extend_from_slice(&md.fastlevel.to_ne_bytes());
    out.extend_from_slice(&md.last_cleanup_num_delpages.to_ne_bytes());
    out.push(md.allequalimage as u8);
    out
}

// --- meta-page decode (PageGetContents area). ------------------------------

const SizeOfPageHeaderData: usize = 24;
const SIZEOF_BTMETA: usize = ::core::mem::size_of::<BTMetaPageData>();

fn meta_from_page(page: &[u8]) -> BTMetaPageData {
    let base = maxalign(SizeOfPageHeaderData);
    let b = &page[base..];
    let rd_u32 = |off: usize| u32::from_ne_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);
    let rd_f64 = |off: usize| {
        f64::from_ne_bytes([
            b[off],
            b[off + 1],
            b[off + 2],
            b[off + 3],
            b[off + 4],
            b[off + 5],
            b[off + 6],
            b[off + 7],
        ])
    };
    BTMetaPageData {
        btm_magic: rd_u32(0),
        btm_version: rd_u32(4),
        btm_root: rd_u32(8),
        btm_level: rd_u32(12),
        btm_fastroot: rd_u32(16),
        btm_fastlevel: rd_u32(20),
        btm_last_cleanup_num_delpages: rd_u32(24),
        btm_last_cleanup_num_heap_tuples: rd_f64(32),
        btm_allequalimage: b[40] != 0,
    }
}

fn meta_into_page(page: &mut [u8], m: &BTMetaPageData) {
    let base = maxalign(SizeOfPageHeaderData);
    let b = &mut page[base..];
    b[0..4].copy_from_slice(&m.btm_magic.to_ne_bytes());
    b[4..8].copy_from_slice(&m.btm_version.to_ne_bytes());
    b[8..12].copy_from_slice(&m.btm_root.to_ne_bytes());
    b[12..16].copy_from_slice(&m.btm_level.to_ne_bytes());
    b[16..20].copy_from_slice(&m.btm_fastroot.to_ne_bytes());
    b[20..24].copy_from_slice(&m.btm_fastlevel.to_ne_bytes());
    b[24..28].copy_from_slice(&m.btm_last_cleanup_num_delpages.to_ne_bytes());
    b[32..40].copy_from_slice(&m.btm_last_cleanup_num_heap_tuples.to_ne_bytes());
    b[40] = m.btm_allequalimage as u8;
}

/// `BTGetDeduplicateItems(rel)` — the per-index `deduplicate_items` reloption,
/// defaulting to `true` (the trimmed relcache does not model nbtree reloptions;
/// the default is faithful for the overwhelmingly common case, and dedup is
/// additionally gated on `allequalimage`).
fn bt_get_deduplicate_items(_rel: &Relation) -> bool {
    true
}

// ===========================================================================
// Genuinely-unported callees (honest seam-and-panic).
// ===========================================================================

/// The conflict info `_bt_check_unique` reads back from the `SnapshotDirty`
/// that `table_index_fetch_tuple_check` populates (C reads `SnapshotDirty.xmin`,
/// `.xmax`, `.speculativeToken` after the probe). Mirrors the three fields the
/// caller consumes; the probe helper copies them out of the dirty snapshot the
/// tableam dispatch updated in place.
#[derive(Default)]
struct DirtyConflict {
    xmin: TransactionId,
    xmax: TransactionId,
    speculative_token: u32,
}

/// `table_index_fetch_tuple_check(heapRel, &htid, snapshot, &all_dead)`
/// (access/table/tableam.c) — `_bt_check_unique`'s single heap-visibility probe,
/// reached through the tableam-seams dispatch (the heap AM's
/// `heapam_index_fetch_tuple`). `snapshot_self` selects which static snapshot to
/// probe with: `SnapshotSelf` (effects of the current command visible, the
/// "is the tuple we want to insert still live" probe) vs `SnapshotDirty`
/// (in-progress changes visible, the "is there a conflicting tuple" probe).
///
/// C passes a fresh `SnapshotDirtyData` whose `xmin`/`xmax`/`speculativeToken`
/// the `HeapTupleSatisfiesDirty` visibility routine writes back; we build that
/// dirty snapshot here, hand it to the dispatch by `&mut`, and copy the conflict
/// info into `dirty` afterwards (the `SnapshotSelf` probe writes nothing, so its
/// caller passes `dirty = None`).
fn table_index_fetch_tuple_check<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot_self: bool,
    all_dead: Option<&mut bool>,
    dirty: Option<&mut DirtyConflict>,
) -> PgResult<bool> {
    use types_snapshot::snapshot::{SnapshotData, SnapshotType};

    let snap_type = if snapshot_self {
        SnapshotType::SNAPSHOT_SELF
    } else {
        SnapshotType::SNAPSHOT_DIRTY
    };
    let mut snapshot: types_tableam::tableam::Snapshot =
        Some(SnapshotData::sentinel(snap_type));

    // C may rewrite `tid` to a live HOT child's TID on a true return; thread it
    // by value and ignore the rewrite (the caller passes the index entry's TID
    // both times, exactly as C does — the rewrite is read out of the slot the
    // executor scan owns, which this convenience wrapper discards).
    let mut tmptid = *tid;

    let found = tableam_seams::table_index_fetch_tuple_check::call(
        mcx,
        heap_rel,
        &mut tmptid,
        &mut snapshot,
        all_dead,
    )?;

    if let (Some(out), Some(snap)) = (dirty, snapshot.as_ref()) {
        out.xmin = snap.xmin;
        out.xmax = snap.xmax;
        out.speculative_token = snap.speculativeToken;
    }

    Ok(found)
}

/// `index_deform_tuple(itup, RelationGetDescr(rel), values, isnull)` +
/// `BuildIndexValueDescription(rel, values, isnull)` (genam.c) — the optional
/// "(key) = (values)" detail for the unique-violation ereport. C makes this
/// detail optional (`key_desc ? errdetail(...) : 0`): it is NULL when the user
/// lacks rights to see the key values. Building it here requires deforming the
/// index tuple into per-attribute datums (the slot-based `index_deform_tuple`
/// seam) and the genam `build_index_value_description` seam, neither plumbed
/// into nbtree-core; we therefore omit the detail, exactly matching C's
/// NULL-key_desc path. The error itself (message + SQLSTATE) is fully reported.
#[inline]
fn build_index_value_desc<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _itup_bytes: &[u8],
) -> Option<alloc::string::String> {
    None
}

/// `_bt_allocbuf(rel, heaprel)` (nbtpage.c) — allocate a new write-locked nbtree
/// page. Delegates to the single real implementation in `page.rs`.
fn _bt_allocbuf<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
) -> PgResult<Buffer> {
    crate::page::_bt_allocbuf(mcx, rel, heaprel)
}

/// `_bt_conditionallockbuf(rel, buf)` (nbtpage.c) — conditionally BT_WRITE-lock a
/// pinned buffer. (The !RelationUsesLocalBuffers valgrind client request is
/// debug-only and not modeled.)
fn _bt_conditionallockbuf<'mcx>(_rel: &Relation<'mcx>, buf: Buffer) -> bool {
    match bufmgr::conditional_lock_buffer::call(buf) {
        Ok(got) => got,
        Err(e) => panic!("_bt_conditionallockbuf: ConditionalLockBuffer failed: {e:?}"),
    }
}

/// `_bt_vacuum_cycleid(rel)` (nbtutils.c) — the active VACUUM cycle ID, from the
/// `btvacinfo` shmem array. Delegates to the ported reader in `utils`.
fn _bt_vacuum_cycleid<'mcx>(rel: &Relation<'mcx>) -> PgResult<u16> {
    crate::utils::bt_vacuum_cycleid(rel)
}

/// `ReadBuffer(rel, blkno)` (bufmgr) — read-without-lock, for the fastpath cache
/// probe in `_bt_search_insert`. Only `read_buffer_extended` (the locked read)
/// exists as a seam; the fastpath optimization additionally needs a bare pinned
/// read followed by a *conditional* lock, neither of which is reachable, so the
/// fastpath probe is the boundary (it is only an optimization — the non-fastpath
/// `_bt_search` descent below is fully ported).
fn read_buffer_unlocked<'mcx>(_rel: &Relation<'mcx>, _blkno: BlockNumber) -> Buffer {
    panic!("_bt_search_insert: ReadBuffer (bare pinned read for fastpath) not yet ported")
}

/// `RelationGetTargetBlock(rel)` — backend-local rightmost-leaf cache. The
/// nbtree-core crate does not depend on the hio seam crate that holds this Oid
/// keyed cache; the fastpath optimization that reads it is deferred. Returns
/// `InvalidBlockNumber`, which faithfully disables the fastpath (the slow
/// `_bt_search` path is always correct — see `_bt_search_insert`).
#[inline]
fn relation_get_target_block(_rel: &Relation) -> BlockNumber {
    InvalidBlockNumber
}

/// `RelationSetTargetBlock(rel, blkno)` — see [`relation_get_target_block`].
/// Behaviour-preserving no-op (the cache is only a performance hint).
#[inline]
fn relation_set_target_block(_rel: &Relation, _blkno: BlockNumber) {}

// --- SSI / speculative-insert / predicate-locking boundaries. --------------

/// `CheckForSerializableConflictIn(rel, NULL, blkno)` — SSI is not plumbed to
/// this layer (mirrors `search.rs`'s predicate-lock no-ops). Behaviour-preserving
/// for the non-serializable common case.
#[inline]
fn check_for_serializable_conflict_in<'mcx>(_rel: &Relation<'mcx>, _blkno: BlockNumber) {}

/// `PredicateLockPageSplit(rel, lblkno, rblkno)` — SSI not plumbed here.
#[inline]
fn predicate_lock_page_split<'mcx>(_rel: &Relation<'mcx>, _l: BlockNumber, _r: BlockNumber) {}

/// `CHECK_FOR_INTERRUPTS()` — process-global; no-op at this layer (loops remain
/// bounded by index size).
#[inline]
fn check_for_interrupts() {}

// ===========================================================================
// _bt_doinsert  (THE owned seam: bt_doinsert)
// ===========================================================================

/// `_bt_doinsert(rel, itup, checkUnique, indexUnchanged, heapRel)` — handle
/// insertion of a single index tuple into the tree. Installable as the
/// `bt_doinsert` seam.
pub fn bt_doinsert<'mcx>(
    rel: &Relation<'mcx>,
    itup: &[u8],
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    heap_rel: &Relation<'mcx>,
) -> PgResult<bool> {
    let mcx = rel_mcx(rel);
    _bt_doinsert_inner(mcx, rel, itup, check_unique, index_unchanged, heap_rel)
}

fn _bt_doinsert_inner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    itup: &[u8],
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    heap_rel: &Relation<'mcx>,
) -> PgResult<bool> {
    let mut is_unique = false;
    let mut checkingunique = check_unique != IndexUniqueCheck::No;

    /* we need an insertion scan key to do our search, so build one */
    let mut itup_key: BTScanInsert<'mcx> = crate::utils::bt_mkscankey(rel, Some(itup))?;

    if checkingunique {
        let anynullkeys = itup_key.as_ref().unwrap().anynullkeys;
        if !anynullkeys {
            /* No (heapkeyspace) scantid until uniqueness established */
            itup_key.as_mut().unwrap().scantid = None;
        } else {
            /*
             * Scan key for new tuple contains NULL key values. Bypass
             * checkingunique steps (NULL is unequal to everything).
             */
            checkingunique = false;
            debug_assert!(check_unique != IndexUniqueCheck::Existing);
            is_unique = true;
        }
    }

    /*
     * Fill in the BTInsertState working area. itemsz must be MAXALIGN()'d to
     * match the alignment overhead PageAddItem will add later.
     */
    let itemsz = maxalign(IndexTupleSize(&index_tuple_header(itup)));
    let mut insertstate = BTInsertStateData {
        itup: index_tuple_box(mcx, itup)?,
        itemsz,
        itup_key: itup_key.clone(),
        buf: InvalidBuffer,
        bounds_valid: false,
        low: 0,
        stricthigh: 0,
        postingoff: 0,
    };
    /* Carry the actual on-page tuple bytes alongside the header carrier. */
    let mut itup_bytes: PgVec<'mcx, u8> = copy_index_tuple(mcx, itup)?;

    /* the t_tid of the new tuple (for waits / conflict bookkeeping). */
    let itup_t_tid = index_tuple_header(itup).t_tid;

    'search: loop {
        /*
         * Find and lock the leaf page that the tuple should be added to.
         * insertstate.buf will hold a buffer locked in exclusive mode.
         */
        let mut stack = _bt_search_insert(mcx, rel, heap_rel, &mut insertstate)?;

        /*
         * checkingunique inserts: check for conflicts in the locked page/buffer.
         */
        if checkingunique {
            let mut speculative_token: u32 = 0;
            let xwait = _bt_check_unique(
                mcx,
                rel,
                &mut insertstate,
                &mut itup_bytes,
                itup_t_tid,
                heap_rel,
                check_unique,
                &mut is_unique,
                &mut speculative_token,
            )?;

            if xwait != InvalidTransactionId {
                /* Have to wait for the other guy ... */
                page_bt_relbuf(rel, insertstate.buf);
                insertstate.buf = InvalidBuffer;

                /*
                 * Wait for the conflicting xact (or speculative insertion) to
                 * finish, then retry. SpeculativeInsertionWait / XactLockTableWait
                 * are not plumbed to this layer; surface the dependency honestly.
                 */
                if speculative_token != 0 {
                    panic!("_bt_doinsert: SpeculativeInsertionWait not yet ported");
                } else {
                    panic!("_bt_doinsert: XactLockTableWait not yet ported");
                }
                /* (start over: drop stack, goto search — unreachable past panic) */
            }

            /* Uniqueness is established -- restore heap tid as scantid */
            if itup_key.as_ref().unwrap().heapkeyspace {
                insertstate.itup_key.as_mut().unwrap().scantid = Some(itup_t_tid);
                itup_key.as_mut().unwrap().scantid = Some(itup_t_tid);
            }
        }

        if check_unique != IndexUniqueCheck::Existing {
            /*
             * Predicate-locking conflict check (SSI; no-op at this layer).
             */
            check_for_serializable_conflict_in(
                rel,
                bufmgr::buffer_get_block_number::call(insertstate.buf),
            );

            /* Do the insertion (reusing cached binary search bounds). */
            let newitemoff = _bt_findinsertloc(
                mcx,
                rel,
                &mut insertstate,
                &mut itup_bytes,
                checkingunique,
                index_unchanged,
                &stack,
                heap_rel,
            )?;
            let buf = insertstate.buf;
            let postingoff = insertstate.postingoff;
            let isz = insertstate.itemsz;
            _bt_insertonpg(
                mcx,
                rel,
                heap_rel,
                insertstate.itup_key.clone(),
                buf,
                InvalidBuffer,
                stack.take(),
                &itup_bytes,
                isz,
                newitemoff,
                postingoff,
                false,
            )?;
        } else {
            /* just release the buffer */
            page_bt_relbuf(rel, insertstate.buf);
        }

        /* be tidy: stack drops here (C: _bt_freestack); itup_key drops at fn end. */
        drop(stack);
        break 'search;
    }

    Ok(is_unique)
}

// ===========================================================================
// _bt_search_insert
// ===========================================================================

/// `_bt_search_insert(rel, heaprel, insertstate)` — `_bt_search()` wrapper for
/// inserts, with the rightmost-leaf fastpath optimization.
fn _bt_search_insert<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    insertstate: &mut BTInsertStateData<'mcx>,
) -> PgResult<BTStack> {
    debug_assert!(insertstate.buf == InvalidBuffer);
    debug_assert!(!insertstate.bounds_valid);
    debug_assert!(insertstate.postingoff == 0);

    if relation_get_target_block(rel) != InvalidBlockNumber {
        /*
         * Fastpath: simulate a _bt_getbuf() call with conditional locking on the
         * cached rightmost leaf. The bare ReadBuffer + ConditionalLockBuffer
         * primitives are unported; relation_get_target_block() returns
         * InvalidBlockNumber so this branch is never taken (the slow descent
         * below is always correct). The code is retained for fidelity.
         */
        insertstate.buf = read_buffer_unlocked(rel, relation_get_target_block(rel));
        if _bt_conditionallockbuf(rel, insertstate.buf) {
            // (unreachable in this repo — see read_buffer_unlocked panic)
            relation_set_target_block(rel, InvalidBlockNumber);
        } else {
            bufmgr::release_buffer::call(insertstate.buf);
        }
        relation_set_target_block(rel, InvalidBlockNumber);
    }

    /* Cannot use optimization -- descend tree, return proper descent stack. */
    let key = insertstate.itup_key.clone();
    let (stack, buf) = crate::search::bt_search(rel, heaprel, &key, /*access_write=*/ true)?;
    insertstate.buf = buf;
    Ok(stack)
}

// ===========================================================================
// _bt_check_unique
// ===========================================================================

/// `_bt_check_unique(rel, insertstate, heapRel, checkUnique, is_unique,
/// speculativeToken)` — check for a unique-index constraint violation. Returns
/// `InvalidTransactionId` if no conflict, else an xact ID to wait for. An actual
/// conflict returns `Err` (the unique-violation ereport).
fn _bt_check_unique<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    insertstate: &mut BTInsertStateData<'mcx>,
    itup_bytes: &[u8],
    itup_t_tid: ItemPointerData,
    heap_rel: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    is_unique: &mut bool,
    speculative_token: &mut u32,
) -> PgResult<TransactionId> {
    let mut curitup: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 0)?;
    let mut nbuf = InvalidBuffer;
    let mut found = false;
    let mut inposting = false;
    let mut prevalldead = true;
    let mut curposti: i32 = 0;
    let mut cur_curitemid_dead = false;
    let mut dirty_conflict = DirtyConflict::default();

    /* Assume unique until we find a duplicate */
    *is_unique = true;

    /* "page"/"opaque"/"maxoff" refer to the current page being scanned. */
    let mut page: PgVec<'mcx, u8> = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
    let mut opaque = opaque_from_page(&page)?;
    let mut maxoff = PageGetMaxOffsetNumber(&PageRef::new(&page)?);

    /*
     * Find the first tuple with the same key. Also saves the binary search
     * bounds in insertstate.
     */
    debug_assert!(!insertstate.bounds_valid);
    let mut offset = crate::search::bt_binsrch_insert(rel, insertstate)?;

    /* Scan over all equal tuples, looking for live conflicts. */
    debug_assert!(!insertstate.itup_key.as_ref().unwrap().anynullkeys);
    debug_assert!(insertstate.itup_key.as_ref().unwrap().scantid.is_none());

    loop {
        if offset <= maxoff {
            /*
             * Fastpath: cached search bounds let us avoid _bt_compare() when the
             * offset where the key will go is not at the end of the page.
             */
            if nbuf == InvalidBuffer && offset == insertstate.stricthigh {
                debug_assert!(insertstate.bounds_valid);
                break;
            }

            /* read the current item id (when not already inside a posting list). */
            if !inposting {
                let pref = PageRef::new(&page)?;
                let itemid = PageGetItemId(&pref, offset)?;
                cur_curitemid_dead = ItemIdIsDead(&itemid);
            }

            if inposting || !cur_curitemid_dead {
                let htid: ItemPointerData;
                let mut all_dead = false;

                if !inposting {
                    /* Plain tuple, or first TID in posting list tuple */
                    if crate::search::bt_compare(
                        rel,
                        &insertstate.itup_key,
                        &page,
                        offset,
                    )? != 0
                    {
                        break; /* we're past all the equal tuples */
                    }
                    /* Advance curitup (owned copy of the page item bytes). */
                    let pref = PageRef::new(&page)?;
                    let itemid = PageGetItemId(&pref, offset)?;
                    let item = PageGetItem(&pref, &itemid)?;
                    debug_assert!(!BTreeTupleIsPivot(&index_tuple_header(item)));
                    curitup.clear();
                    curitup.extend_from_slice(item);
                }

                let curhdr = index_tuple_header(&curitup);
                /* okay, we gotta fetch the heap tuple using htid ... */
                if !BTreeTupleIsPosting(&curhdr) {
                    debug_assert!(!inposting);
                    htid = curhdr.t_tid;
                } else if !inposting {
                    /* first TID in new posting list */
                    inposting = true;
                    prevalldead = true;
                    curposti = 0;
                    htid = posting_list_n(&curitup, 0);
                } else {
                    debug_assert!(curposti > 0);
                    htid = posting_list_n(&curitup, curposti as usize);
                }

                /*
                 * If we are doing a recheck, we expect to find the tuple we are
                 * rechecking. It's not a duplicate, but keep scanning.
                 */
                if check_unique == IndexUniqueCheck::Existing
                    && ItemPointerCompare(&htid, &itup_t_tid) == 0
                {
                    found = true;
                }
                /*
                 * Check if there are table tuples for this index entry satisfying
                 * SnapshotDirty (HOT chains share a single index entry).
                 */
                else if {
                    let mut dirty = DirtyConflict::default();
                    let dup = table_index_fetch_tuple_check(
                        mcx,
                        heap_rel,
                        &htid,
                        /*snapshot_self=*/ false,
                        Some(&mut all_dead),
                        Some(&mut dirty),
                    )?;
                    dirty_conflict = dirty;
                    dup
                } {
                    /* It is a duplicate. */
                    if check_unique == IndexUniqueCheck::Partial {
                        /*
                         * Partial check: just report the potential conflict and
                         * leave the full check for later. Don't invalidate
                         * binary search bounds.
                         */
                        if nbuf != InvalidBuffer {
                            page_bt_relbuf(rel, nbuf);
                        }
                        *is_unique = false;
                        return Ok(InvalidTransactionId);
                    }

                    /*
                     * If this tuple is being updated by another transaction then
                     * we have to wait for its commit/abort.
                     */
                    let xwait = if TransactionIdIsValid(dirty_conflict.xmin) {
                        dirty_conflict.xmin
                    } else {
                        dirty_conflict.xmax
                    };

                    if TransactionIdIsValid(xwait) {
                        if nbuf != InvalidBuffer {
                            page_bt_relbuf(rel, nbuf);
                        }
                        /* Tell _bt_doinsert to wait... */
                        *speculative_token = dirty_conflict.speculative_token;
                        /* Caller releases lock on buf immediately */
                        insertstate.bounds_valid = false;
                        return Ok(xwait);
                    }

                    /*
                     * Otherwise we have a definite conflict. But before
                     * complaining, look to see if the tuple we want to insert is
                     * itself now committed dead --- if so, don't complain. This
                     * is necessary to support CREATE INDEX CONCURRENTLY: we must
                     * follow HOT-chains, and a live tuple anywhere in this chain
                     * is a unique key conflict.
                     */
                    if table_index_fetch_tuple_check(
                        mcx,
                        heap_rel,
                        &itup_t_tid,
                        /*snapshot_self=*/ true,
                        None,
                        None,
                    )? {
                        /* Normal case --- it's still live */
                    } else {
                        /*
                         * It's been deleted, so no error, and no need to
                         * continue searching
                         */
                        break;
                    }

                    /*
                     * Check for a conflict-in as we would if we were going to
                     * write to this page, so SSI conflicts masked by this unique
                     * constraint violation still get reported.
                     */
                    let conflict_blkno =
                        bufmgr::buffer_get_block_number::call(insertstate.buf);
                    check_for_serializable_conflict_in(rel, conflict_blkno);

                    /*
                     * Definite conflict. Release the buffer locks we hold before
                     * building the value description, which could make catalog
                     * accesses (worst case touching this same index and
                     * deadlocking).
                     */
                    if nbuf != InvalidBuffer {
                        page_bt_relbuf(rel, nbuf);
                    }
                    page_bt_relbuf(rel, insertstate.buf);
                    insertstate.buf = InvalidBuffer;
                    insertstate.bounds_valid = false;

                    /*
                     * Break the tuple down into datums and report the error. The
                     * "(key) = (values)" detail is built via the genam
                     * BuildIndexValueDescription seam; when unavailable (caller
                     * lacks rights, or seam unported) we omit the detail exactly
                     * as C does for a NULL key_desc.
                     */
                    let key_desc = build_index_value_desc(mcx, rel, itup_bytes);
                    let mut err = PgError::error(format!(
                        "duplicate key value violates unique constraint \"{}\"",
                        rel_name(rel)
                    ))
                    .with_sqlstate(ERRCODE_UNIQUE_VIOLATION);
                    if let Some(kd) = key_desc {
                        err = err.with_detail(format!("Key {} already exists.", kd));
                    }
                    return Err(err);
                } else if all_dead
                    && (!inposting
                        || (prevalldead
                            && curposti == BTreeTupleGetNPosting(&curhdr) as i32 - 1))
                {
                    /*
                     * The conflicting tuple (or all HOT chains) is dead to
                     * everyone: mark the index entry killed.
                     */
                    let dirty_buf = if nbuf != InvalidBuffer { nbuf } else { insertstate.buf };
                    bufmgr::with_buffer_page::call(dirty_buf, &mut |pg: &mut [u8]| {
                        mark_item_dead(pg, offset);
                        let mut o = opaque_from_page(pg)?;
                        o.btpo_flags |= BTP_HAS_GARBAGE;
                        encode_opaque(pg, &o);
                        Ok(())
                    })?;
                    bufmgr::mark_buffer_dirty_hint::call(dirty_buf, true);
                }

                /*
                 * Remember if posting list tuple has even a single HOT chain whose
                 * members are not all dead.
                 */
                if !all_dead && inposting {
                    prevalldead = false;
                }
            }
        }

        // C: `if (inposting && curposti < BTreeTupleGetNPosting(curitup) - 1)`.
        // `curitup` is only valid (non-NULL) once we have read a page item this
        // iteration; when `offset > maxoff` and we are not inside a posting
        // list, `curitup` is still empty, so the C `&&` short-circuit never
        // dereferences it. Mirror that: only read the header in the posting
        // branch.
        if inposting
            && curposti < BTreeTupleGetNPosting(&index_tuple_header(&curitup)) as i32 - 1
        {
            /* Advance to next TID in same posting list */
            curposti += 1;
            continue;
        } else if offset < maxoff {
            /* Advance to next tuple */
            curposti = 0;
            inposting = false;
            offset = OffsetNumberNext(offset);
        } else {
            /* If scankey == hikey we gotta check the next page too */
            if P_RIGHTMOST(&opaque) {
                break;
            }
            let highkeycmp = crate::search::bt_compare(
                rel,
                &insertstate.itup_key,
                &page,
                P_HIKEY,
            )?;
            debug_assert!(highkeycmp <= 0);
            if highkeycmp != 0 {
                break;
            }
            /* Advance to next non-dead page --- there must be one */
            loop {
                let nblkno = opaque.btpo_next;
                nbuf = _bt_relandgetbuf(mcx, rel, nbuf, nblkno, BT_READ)?;
                page = bufmgr::buffer_get_page::call(mcx, nbuf)?;
                opaque = opaque_from_page(&page)?;
                if !P_IGNORE(&opaque) {
                    break;
                }
                if P_RIGHTMOST(&opaque) {
                    return Err(PgError::error(format!(
                        "fell off the end of index \"{}\"",
                        rel_name(rel)
                    )));
                }
            }
            curposti = 0;
            inposting = false;
            maxoff = PageGetMaxOffsetNumber(&PageRef::new(&page)?);
            offset = P_FIRSTDATAKEY(&opaque);
            /* Don't invalidate binary search bounds */
        }
    }

    /*
     * If we are doing a recheck then we should have found the tuple we are
     * checking.
     */
    if check_unique == IndexUniqueCheck::Existing && !found {
        return Err(PgError::error(format!(
            "failed to re-find tuple within index \"{}\"",
            rel_name(rel)
        )));
    }

    if nbuf != InvalidBuffer {
        page_bt_relbuf(rel, nbuf);
    }

    let _ = (itup_bytes, &curitup);
    Ok(InvalidTransactionId)
}

/// `ItemIdMarkDead(PageGetItemId(page, offnum))` operating in place on raw page
/// bytes (sets the LP_DEAD lp_flags bits in the line pointer at `offnum`).
fn mark_item_dead(page: &mut [u8], offnum: OffsetNumber) {
    // Line pointers start at SizeOfPageHeaderData; each ItemIdData is 4 bytes.
    // lp_flags occupies bits 15..14 of the second u16 (lp_off:15, lp_flags:2,
    // lp_len:15). LP_DEAD == 3 (both flag bits set).
    let lp_off = SizeOfPageHeaderData + (offnum as usize - 1) * SIZEOF_ITEM_ID;
    // ItemIdData layout: unsigned lp_off:15, lp_flags:2, lp_len:15 (little-end
    // bitfield over a 32-bit word). lp_flags are bits 15..16.
    let mut word = u32::from_ne_bytes([
        page[lp_off],
        page[lp_off + 1],
        page[lp_off + 2],
        page[lp_off + 3],
    ]);
    // clear lp_flags (bits 15,16) then set LP_DEAD (3).
    word &= !(0x3u32 << 15);
    word |= 0x3u32 << 15;
    let nb = word.to_ne_bytes();
    page[lp_off] = nb[0];
    page[lp_off + 1] = nb[1];
    page[lp_off + 2] = nb[2];
    page[lp_off + 3] = nb[3];
}

// ===========================================================================
// _bt_findinsertloc
// ===========================================================================

/// `_bt_findinsertloc(rel, insertstate, checkingunique, indexUnchanged, stack,
/// heapRel)` — find the insert location for a tuple, moving right if necessary.
fn _bt_findinsertloc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    insertstate: &mut BTInsertStateData<'mcx>,
    itup_bytes: &[u8],
    checkingunique: bool,
    index_unchanged: bool,
    stack: &BTStack,
    heap_rel: &Relation<'mcx>,
) -> PgResult<OffsetNumber> {
    let mut page: PgVec<'mcx, u8> = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
    let mut opaque = opaque_from_page(&page)?;

    let heapkeyspace = insertstate.itup_key.as_ref().unwrap().heapkeyspace;

    /* Check 1/3 of a page restriction */
    if insertstate.itemsz > BTMaxItemSize {
        crate::utils::bt_check_third_page(rel, heap_rel, heapkeyspace, &page, itup_bytes)?;
    }

    debug_assert!(P_ISLEAF(&opaque) && !P_INCOMPLETE_SPLIT(&opaque));

    if heapkeyspace {
        /* Keep track of whether checkingunique duplicate seen */
        let mut uniquedup = index_unchanged;

        if checkingunique {
            if insertstate.low < insertstate.stricthigh {
                /* Encountered a duplicate in _bt_check_unique() */
                debug_assert!(insertstate.bounds_valid);
                uniquedup = true;
            }

            loop {
                /*
                 * Does the new tuple belong on this page? Use the strict upper
                 * bound from _bt_check_unique() when available.
                 */
                if insertstate.bounds_valid
                    && insertstate.low <= insertstate.stricthigh
                    && insertstate.stricthigh <= PageGetMaxOffsetNumber(&PageRef::new(&page)?)
                {
                    break;
                }

                /* Test '<=', not '!=', since scantid is set now */
                if P_RIGHTMOST(&opaque)
                    || crate::search::bt_compare(
                        rel,
                        &insertstate.itup_key,
                        &page,
                        P_HIKEY,
                    )? <= 0
                {
                    break;
                }

                _bt_stepright(mcx, rel, heap_rel, insertstate, stack)?;
                /* Update local state after stepping right */
                page = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
                opaque = opaque_from_page(&page)?;
                /* Assume duplicates (if checkingunique) */
                uniquedup = true;
            }
        }

        /*
         * If the target page cannot fit newitem, try to avoid splitting by
         * performing deletion or deduplication now.
         */
        if (PageGetFreeSpace(&PageRef::new(&page)?) as usize) < insertstate.itemsz {
            _bt_delete_or_dedup_one_page(
                mcx,
                rel,
                heap_rel,
                insertstate,
                itup_bytes,
                false,
                checkingunique,
                uniquedup,
                index_unchanged,
            )?;
            page = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
            opaque = opaque_from_page(&page)?;
        }
    } else {
        /*
         * This is a !heapkeyspace (version 2 or 3) index. Scan right past equal
         * keys to find free space, with a "get tired" random stop (condition
         * (c)) that prevents O(N^2) behaviour with many equal keys.
         */
        while (PageGetFreeSpace(&PageRef::new(&page)?) as usize) < insertstate.itemsz {
            /* Before considering moving right, free LP_DEAD items */
            if P_HAS_GARBAGE(&opaque) {
                _bt_delete_or_dedup_one_page(
                    mcx,
                    rel,
                    heap_rel,
                    insertstate,
                    itup_bytes,
                    true,
                    false,
                    false,
                    false,
                )?;
                page = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
                opaque = opaque_from_page(&page)?;

                if (PageGetFreeSpace(&PageRef::new(&page)?) as usize) >= insertstate.itemsz {
                    break;
                }
            }

            if insertstate.bounds_valid
                && insertstate.low <= insertstate.stricthigh
                && insertstate.stricthigh <= PageGetMaxOffsetNumber(&PageRef::new(&page)?)
            {
                break;
            }

            if P_RIGHTMOST(&opaque)
                || crate::search::bt_compare(
                    rel,
                    &insertstate.itup_key,
                    &page,
                    P_HIKEY,
                )? != 0
                || pg_prng::global_prng(|p| p.next_u32()) <= (u32::MAX / 100)
            {
                break;
            }

            _bt_stepright(mcx, rel, heap_rel, insertstate, stack)?;
            page = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
            opaque = opaque_from_page(&page)?;
        }
    }

    /*
     * We should now be on the correct page. Find the offset within the page.
     */
    let mut newitemoff = crate::search::bt_binsrch_insert(rel, insertstate)?;

    if insertstate.postingoff == -1 {
        /*
         * Overlapping posting list tuple with LP_DEAD set. Perform simple index
         * tuple deletion early, then redo the binary search.
         */
        _bt_delete_or_dedup_one_page(
            mcx,
            rel,
            heap_rel,
            insertstate,
            itup_bytes,
            true,
            false,
            false,
            false,
        )?;

        debug_assert!(!insertstate.bounds_valid);
        insertstate.postingoff = 0;
        newitemoff = crate::search::bt_binsrch_insert(rel, insertstate)?;
        debug_assert!(insertstate.postingoff == 0);
    }

    Ok(newitemoff)
}

// ===========================================================================
// _bt_stepright
// ===========================================================================

/// `_bt_stepright(rel, heaprel, insertstate, stack)` — step right to the next
/// non-dead page during insertion (write-locking the target before releasing the
/// current page).
fn _bt_stepright<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    insertstate: &mut BTInsertStateData<'mcx>,
    stack: &BTStack,
) -> PgResult<()> {
    let page = bufmgr::buffer_get_page::call(mcx, insertstate.buf)?;
    let opaque = opaque_from_page(&page)?;

    let mut rbuf = InvalidBuffer;
    let mut rblkno = opaque.btpo_next;
    loop {
        rbuf = _bt_relandgetbuf(mcx, rel, rbuf, rblkno, BT_WRITE)?;
        let rpage = bufmgr::buffer_get_page::call(mcx, rbuf)?;
        let ropaque = opaque_from_page(&rpage)?;

        /* If this page was incompletely split, finish the split now. */
        if P_INCOMPLETE_SPLIT(&ropaque) {
            _bt_finish_split(mcx, rel, heaprel, rbuf, clone_stack(stack))?;
            rbuf = InvalidBuffer;
            continue;
        }

        if !P_IGNORE(&ropaque) {
            break;
        }
        if P_RIGHTMOST(&ropaque) {
            return Err(PgError::error(format!(
                "fell off the end of index \"{}\"",
                rel_name(rel)
            )));
        }

        rblkno = ropaque.btpo_next;
    }
    /* rbuf locked; unlock buf, update state for caller */
    page_bt_relbuf(rel, insertstate.buf);
    insertstate.buf = rbuf;
    insertstate.bounds_valid = false;
    Ok(())
}

/// Clone a `BTStack` chain (boxed linked list). C passes the same `stack`
/// pointer to recursive helpers; the owned-Box model needs an explicit clone
/// when a borrowed stack must be handed to a by-value callee.
pub(crate) fn clone_stack(stack: &BTStack) -> BTStack {
    match stack {
        None => None,
        Some(node) => Some(Box::new(BTStackData {
            bts_blkno: node.bts_blkno,
            bts_offset: node.bts_offset,
            bts_parent: clone_stack(&node.bts_parent),
        })),
    }
}

// ===========================================================================
// _bt_insertonpg
// ===========================================================================

/// `_bt_insertonpg(...)` — insert a tuple on a particular page, recursively
/// splitting / propagating the downlink as needed. On entry `buf` is pinned +
/// write-locked; on return both pin and lock are dropped.
fn _bt_insertonpg<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    itup_key: BTScanInsert<'mcx>,
    buf: Buffer,
    cbuf: Buffer,
    stack: BTStack,
    itup_in: &[u8],
    itemsz: Size,
    mut newitemoff: OffsetNumber,
    postingoff: i32,
    split_only_page: bool,
) -> PgResult<()> {
    let page = bufmgr::buffer_get_page::call(mcx, buf)?;
    let opaque = opaque_from_page(&page)?;
    let isleaf = P_ISLEAF(&opaque);
    let isroot = P_ISROOT(&opaque);
    let isrightmost = P_RIGHTMOST(&opaque);
    let isonly = P_LEFTMOST(&opaque) && P_RIGHTMOST(&opaque);

    /* child buffer must be given iff inserting on an internal page */
    debug_assert!(isleaf == (cbuf == InvalidBuffer));
    debug_assert!(!BTreeTupleIsPosting(&index_tuple_header(itup_in)));
    debug_assert!(maxalign(IndexTupleSize(&index_tuple_header(itup_in))) == itemsz);
    debug_assert!(!P_INCOMPLETE_SPLIT(&opaque));
    debug_assert!(isleaf || newitemoff > P_FIRSTDATAKEY(&opaque));

    /* itup / origitup / nposting working copies for a posting-list split. */
    let mut itup: PgVec<'mcx, u8> = copy_index_tuple(mcx, itup_in)?;
    let mut origitup: Option<PgVec<'mcx, u8>> = None;
    let mut nposting: Option<PgVec<'mcx, u8>> = None;
    let mut oposting_off: OffsetNumber = InvalidOffsetNumber;

    /* Do we need to split an existing posting list item? */
    if postingoff != 0 {
        let (oposting_bytes, itemid_dead) = {
            let pref = PageRef::new(&page)?;
            let itemid = PageGetItemId(&pref, newitemoff)?;
            let item = PageGetItem(&pref, &itemid)?;
            let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, item.len())?;
            v.extend_from_slice(item);
            (v, ItemIdIsDead(&itemid))
        };
        debug_assert!(isleaf
            && itup_key.as_ref().unwrap().heapkeyspace
            && itup_key.as_ref().unwrap().allequalimage);

        if !BTreeTupleIsPosting(&index_tuple_header(&oposting_bytes)) || itemid_dead {
            return Err(PgError::error(format!(
                "table tid from new index tuple ({},{}) overlaps with invalid duplicate tuple at offset {} of block {} in index \"{}\"",
                ipd_block_number(&index_tuple_header(itup_in).t_tid),
                ipd_offset(&index_tuple_header(itup_in).t_tid),
                newitemoff,
                bufmgr::buffer_get_block_number::call(buf),
                rel_name(rel)
            )));
        }

        /* use a mutable copy of itup as our itup from here on */
        let orig = copy_index_tuple(mcx, itup_in)?;
        /* _bt_swap_posting mutates itup (now containing rightmost TID). */
        let np = dedup::_bt_swap_posting(mcx, &mut itup, &oposting_bytes, postingoff)?;
        oposting_off = newitemoff;
        nposting = Some(np);
        origitup = Some(orig);

        /* Alter offset so that newitem goes after posting list */
        newitemoff = OffsetNumberNext(newitemoff);
    }

    /*
     * Do we need to split the page to fit the item on it?
     */
    if (PageGetFreeSpace(&PageRef::new(&page)?) as usize) < itemsz {
        debug_assert!(!split_only_page);

        /* split the buffer into left and right halves */
        let rbuf = _bt_split(
            mcx,
            rel,
            heaprel,
            itup_key.clone(),
            buf,
            cbuf,
            newitemoff,
            itemsz,
            &itup,
            origitup.as_deref(),
            nposting.as_deref(),
            postingoff as u16,
            oposting_off,
        )?;
        predicate_lock_page_split(
            rel,
            bufmgr::buffer_get_block_number::call(buf),
            bufmgr::buffer_get_block_number::call(rbuf),
        );

        /* Ready to do the parent insertion. */
        _bt_insert_parent(mcx, rel, heaprel, buf, rbuf, stack, isroot, isonly)?;
    } else {
        let mut metabuf = InvalidBuffer;
        let mut metad: Option<BTMetaPageData> = None;

        /*
         * split_only_page: ensure fast root link points at or above this page.
         */
        if split_only_page {
            debug_assert!(!isleaf);
            debug_assert!(cbuf != InvalidBuffer);

            metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_WRITE)?;
            let metapg = bufmgr::buffer_get_page::call(mcx, metabuf)?;
            let m = meta_from_page(&metapg);

            if m.btm_fastlevel >= opaque.btpo_level {
                /* no update wanted */
                page_bt_relbuf(rel, metabuf);
                metabuf = InvalidBuffer;
            } else {
                metad = Some(m);
            }
        }

        /* Do the update. No ereport(ERROR) until changes are logged. */
        miscinit::start_crit_section::call();

        /* Overwrite the posting list with its post-split version. */
        if postingoff != 0 {
            let np = nposting.as_ref().unwrap();
            let off = oposting_off;
            bufmgr::with_buffer_page::call(buf, &mut |pg: &mut [u8]| {
                overwrite_item(pg, off, np);
                Ok(())
            })?;
        }

        let mut add_ok = true;
        bufmgr::with_buffer_page::call(buf, &mut |pg: &mut [u8]| {
            if PageAddItem(pg, &itup, itemsz, newitemoff) == InvalidOffsetNumber {
                add_ok = false;
            }
            Ok(())
        })?;
        if !add_ok {
            return Err(PgError::error(format!(
                "failed to add new item to block {} in index \"{}\"",
                bufmgr::buffer_get_block_number::call(buf),
                rel_name(rel)
            )));
        }

        bufmgr::mark_buffer_dirty::call(buf);

        /* upgrade + update meta-page if needed */
        if metabuf != InvalidBuffer {
            let new_blk = bufmgr::buffer_get_block_number::call(buf);
            let new_level = opaque.btpo_level;
            bufmgr::with_buffer_page::call(metabuf, &mut |pg: &mut [u8]| {
                let mut m = meta_from_page(pg);
                if m.btm_version < BTREE_NOVAC_VERSION {
                    _bt_upgrademetapage(pg);
                    m = meta_from_page(pg);
                }
                m.btm_fastroot = new_blk;
                m.btm_fastlevel = new_level;
                meta_into_page(pg, &m);
                Ok(())
            })?;
            metad = Some(meta_from_page(&bufmgr::buffer_get_page::call(mcx, metabuf)?));
            bufmgr::mark_buffer_dirty::call(metabuf);
        }

        /* Clear INCOMPLETE_SPLIT on child if this finishes a split. */
        if !isleaf {
            bufmgr::with_buffer_page::call(cbuf, &mut |pg: &mut [u8]| {
                let mut cop = opaque_from_page(pg)?;
                debug_assert!(P_INCOMPLETE_SPLIT(&cop));
                cop.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
                encode_opaque(pg, &cop);
                Ok(())
            })?;
            bufmgr::mark_buffer_dirty::call(cbuf);
        }

        /* XLOG stuff */
        let needs_wal = relcache::relation_needs_wal::call(rel);
        if needs_wal {
            let xlrec = xl_btree_insert { offnum: newitemoff };
            let xlinfo: u8;

            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_data::call(&xlrec.offnum.to_ne_bytes()[..SizeOfBtreeInsert])?;

            if isleaf && postingoff == 0 {
                xlinfo = XLOG_BTREE_INSERT_LEAF;
            } else if postingoff != 0 {
                debug_assert!(isleaf);
                xlinfo = XLOG_BTREE_INSERT_POST;
            } else {
                /* Internal page insert, which finishes a split on cbuf */
                let mut x = XLOG_BTREE_INSERT_UPPER;
                xloginsert::xlog_register_buffer::call(1, cbuf, REGBUF_STANDARD)?;

                if metabuf != InvalidBuffer {
                    /* Internal page insert + meta update */
                    x = XLOG_BTREE_INSERT_META;
                    let m = metad.as_ref().unwrap();
                    debug_assert!(m.btm_version >= BTREE_NOVAC_VERSION);
                    let xlmeta = xl_btree_metadata {
                        version: m.btm_version,
                        root: m.btm_root,
                        level: m.btm_level,
                        fastroot: m.btm_fastroot,
                        fastlevel: m.btm_fastlevel,
                        last_cleanup_num_delpages: m.btm_last_cleanup_num_delpages,
                        allequalimage: m.btm_allequalimage,
                    };
                    xloginsert::xlog_register_buffer::call(
                        2,
                        metabuf,
                        REGBUF_WILL_INIT | REGBUF_STANDARD,
                    )?;
                    xloginsert::xlog_register_buf_data::call(
                        2,
                        &serialize_xl_btree_metadata(&xlmeta),
                    )?;
                }
                xlinfo = x;
            }

            xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
            if postingoff == 0 {
                /* Just log itup from caller */
                let sz = IndexTupleSize(&index_tuple_header(&itup));
                xloginsert::xlog_register_buf_data::call(0, &itup[..sz])?;
            } else {
                /* Insert with posting list split: log postingoff + origitup. */
                let upostingoff = postingoff as u16;
                xloginsert::xlog_register_buf_data::call(0, &upostingoff.to_ne_bytes())?;
                let orig = origitup.as_ref().unwrap();
                let sz = IndexTupleSize(&index_tuple_header(orig));
                xloginsert::xlog_register_buf_data::call(0, &orig[..sz])?;
            }

            let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, xlinfo)?;

            if metabuf != InvalidBuffer {
                bufmgr::page_set_lsn::call(metabuf, recptr)?;
            }
            if !isleaf {
                bufmgr::page_set_lsn::call(cbuf, recptr)?;
            }
            bufmgr::page_set_lsn::call(buf, recptr)?;
        }

        miscinit::end_crit_section::call();

        /* Release subsidiary buffers */
        if metabuf != InvalidBuffer {
            page_bt_relbuf(rel, metabuf);
        }
        if !isleaf {
            page_bt_relbuf(rel, cbuf);
        }

        /*
         * Cache the block number if this is the rightmost leaf page (fastpath).
         */
        let mut blockcache = InvalidBlockNumber;
        if isrightmost && isleaf && !isroot {
            blockcache = bufmgr::buffer_get_block_number::call(buf);
        }

        /* Release buffer for insertion target block */
        page_bt_relbuf(rel, buf);

        /*
         * Apply the fastpath block cache (gated on tree height). The
         * RelationSetTargetBlock cache is a no-op at this layer (see helpers).
         */
        if blockcache != InvalidBlockNumber
            && crate::page::bt_getrootheight(rel)? >= BTREE_FASTPATH_MIN_LEVEL as i32
        {
            relation_set_target_block(rel, blockcache);
        }

        /* be tidy: nposting / itup dropped at scope exit. */
        drop(stack);
    }

    Ok(())
}

/// Overwrite the existing item at `off` with `newbytes` in place (the
/// `memcpy(oposting, nposting, ...)` of `_bt_insertonpg` / `_bt_split`; the new
/// posting list is guaranteed the same size as the original).
fn overwrite_item(page: &mut [u8], off: OffsetNumber, newbytes: &[u8]) {
    // Locate the item via its line pointer (lp_off:15, lp_flags:2, lp_len:15).
    let lp_off = SizeOfPageHeaderData + (off as usize - 1) * SIZEOF_ITEM_ID;
    let word = u32::from_ne_bytes([
        page[lp_off],
        page[lp_off + 1],
        page[lp_off + 2],
        page[lp_off + 3],
    ]);
    let item_off = (word & 0x7FFF) as usize;
    let item_len = ((word >> 17) & 0x7FFF) as usize;
    let n = newbytes.len().min(item_len);
    page[item_off..item_off + n].copy_from_slice(&newbytes[..n]);
}

// ===========================================================================
// _bt_split
// ===========================================================================

/// `_bt_split(...)` — split a page into left and right halves, inserting newitem
/// and handling coinciding posting-list splits. Returns the new right sibling
/// (pinned + write-locked); the pin/lock on `buf` are maintained.
fn _bt_split<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    itup_key: BTScanInsert<'mcx>,
    buf: Buffer,
    cbuf: Buffer,
    newitemoff: OffsetNumber,
    newitemsz: Size,
    newitem: &[u8],
    orignewitem: Option<&[u8]>,
    nposting: Option<&[u8]>,
    postingoff: u16,
    _oposting_off_unused: OffsetNumber,
) -> PgResult<Buffer> {
    /* origpage snapshot. */
    let origpage: PgVec<'mcx, u8> = bufmgr::buffer_get_page::call(mcx, buf)?;
    let oopaque = opaque_from_page(&origpage)?;
    let isleaf = P_ISLEAF(&oopaque);
    let isrightmost = P_RIGHTMOST(&oopaque);
    let maxoff = PageGetMaxOffsetNumber(&PageRef::new(&origpage)?);
    let origpagenumber = bufmgr::buffer_get_block_number::call(buf);

    /* Choose a split point. */
    let mut newitemonleft = false;
    let firstrightoff = {
        let pref = PageRef::new(&origpage)?;
        crate::splitloc::_bt_findsplitloc(
            rel,
            &pref,
            newitemoff,
            newitemsz,
            newitem,
            &mut newitemonleft,
        )?
    };

    /* Allocate a temp left page image (PageGetTempPage + _bt_pageinit). */
    let page_size = PageGetPageSize(&PageRef::new(&origpage)?);
    let mut leftpage: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, page_size)?;
    leftpage.resize(page_size, 0u8);
    crate::page::_bt_pageinit(&mut leftpage, page_size);

    /* leftpage special area, derived from origpage. */
    let mut lopaque = decode_opaque(&leftpage[special_offset(&leftpage)..]);
    lopaque.btpo_flags = oopaque.btpo_flags;
    lopaque.btpo_flags &= !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    lopaque.btpo_flags |= BTP_INCOMPLETE_SPLIT;
    lopaque.btpo_prev = oopaque.btpo_prev;
    lopaque.btpo_level = oopaque.btpo_level;
    encode_opaque(&mut leftpage, &lopaque);

    /* Copy origpage's LSN into leftpage (XLogInsert examines it). */
    {
        let lsn = bufmgr::page_get_lsn::call(buf)?;
        set_page_lsn_bytes(&mut leftpage, lsn);
    }

    /*
     * Determine origpagepostingoff to pretend the posting split already happened.
     */
    let mut origpagepostingoff: OffsetNumber = InvalidOffsetNumber;
    if postingoff != 0 {
        debug_assert!(isleaf);
        origpagepostingoff = OffsetNumberPrev(newitemoff);
    }

    /*
     * Build the new left high key (firstright; possibly suffix-truncated on leaf
     * pages, firstright-itself on internal pages).
     */
    let mut firstright_bytes: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 0)?;
    let mut itemsz: usize;
    if !newitemonleft && newitemoff == firstrightoff {
        /* incoming tuple becomes firstright */
        itemsz = newitemsz;
        firstright_bytes.extend_from_slice(newitem);
    } else {
        let pref = PageRef::new(&origpage)?;
        let itemid = PageGetItemId(&pref, firstrightoff)?;
        itemsz = ItemIdGetLength(&itemid) as usize;
        let item = PageGetItem(&pref, &itemid)?;
        firstright_bytes.extend_from_slice(item);
        if firstrightoff == origpagepostingoff {
            firstright_bytes.clear();
            firstright_bytes.extend_from_slice(nposting.unwrap());
            itemsz = nposting.unwrap().len();
        }
    }

    let lefthighkey: PgVec<'mcx, u8>;
    if isleaf {
        /* Attempt suffix truncation for leaf page splits. */
        let mut lastleft_bytes: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 0)?;
        if newitemonleft && newitemoff == firstrightoff {
            lastleft_bytes.extend_from_slice(newitem);
        } else {
            let lastleftoff = OffsetNumberPrev(firstrightoff);
            debug_assert!(lastleftoff >= P_FIRSTDATAKEY(&oopaque));
            let pref = PageRef::new(&origpage)?;
            let itemid = PageGetItemId(&pref, lastleftoff)?;
            let item = PageGetItem(&pref, &itemid)?;
            lastleft_bytes.extend_from_slice(item);
            if lastleftoff == origpagepostingoff {
                lastleft_bytes.clear();
                lastleft_bytes.extend_from_slice(nposting.unwrap());
            }
        }

        lefthighkey = crate::utils::bt_truncate(
            mcx,
            rel,
            &lastleft_bytes,
            &firstright_bytes,
            itup_key.as_ref().unwrap(),
        )?;
        itemsz = IndexTupleSize(&index_tuple_header(&lefthighkey));
    } else {
        /* Internal page: use firstright directly as new high key. */
        lefthighkey = {
            let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, firstright_bytes.len())?;
            v.extend_from_slice(&firstright_bytes);
            v
        };
    }

    /* Add new high key to leftpage. */
    let mut afterleftoff = P_HIKEY;
    debug_assert!(itemsz == maxalign(IndexTupleSize(&index_tuple_header(&lefthighkey))));
    if PageAddItem(&mut leftpage, &lefthighkey, itemsz, afterleftoff) == InvalidOffsetNumber {
        return Err(PgError::error(format!(
            "failed to add high key to the left sibling while splitting block {} of index \"{}\"",
            origpagenumber,
            rel_name(rel)
        )));
    }
    afterleftoff = OffsetNumberNext(afterleftoff);

    /*
     * Acquire a new right page (after left page has a new high key). From here
     * on it's not okay to error without zeroing rightpage first.
     */
    let rbuf = _bt_allocbuf(mcx, rel, heaprel)?;
    let rightpagenumber = bufmgr::buffer_get_block_number::call(rbuf);
    /* rightpage initialized by _bt_allocbuf; build the right image locally. */
    let mut rightpage: PgVec<'mcx, u8> = bufmgr::buffer_get_page::call(mcx, rbuf)?;

    /* Finish off leftpage special-area fields. */
    lopaque.btpo_next = rightpagenumber;
    lopaque.btpo_cycleid = _bt_vacuum_cycleid(rel)?;
    encode_opaque(&mut leftpage, &lopaque);

    /* rightpage special area. */
    let mut ropaque = opaque_from_page(&rightpage)?;
    ropaque.btpo_flags = oopaque.btpo_flags;
    ropaque.btpo_flags &= !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    ropaque.btpo_prev = origpagenumber;
    ropaque.btpo_next = oopaque.btpo_next;
    ropaque.btpo_level = oopaque.btpo_level;
    ropaque.btpo_cycleid = lopaque.btpo_cycleid;
    encode_opaque(&mut rightpage, &ropaque);

    /* Add high key to rightpage if origpage is not rightmost. */
    let mut afterrightoff = P_HIKEY;
    if !isrightmost {
        let pref = PageRef::new(&origpage)?;
        let itemid = PageGetItemId(&pref, P_HIKEY)?;
        let rhk_sz = ItemIdGetLength(&itemid) as usize;
        let rhk = PageGetItem(&pref, &itemid)?;
        let rhk_owned: PgVec<'mcx, u8> = {
            let mut v = vec_with_capacity_in(mcx, rhk_sz)?;
            v.extend_from_slice(rhk);
            v
        };
        if PageAddItem(&mut rightpage, &rhk_owned, rhk_sz, afterrightoff) == InvalidOffsetNumber {
            zero_page(&mut rightpage);
            return Err(PgError::error(format!(
                "failed to add high key to the right sibling while splitting block {} of index \"{}\"",
                origpagenumber, rel_name(rel)
            )));
        }
        afterrightoff = OffsetNumberNext(afterrightoff);
    }

    /* Internal page splits truncate first data item on right page. */
    let mut minusinfoff = InvalidOffsetNumber;
    if !isleaf {
        minusinfoff = afterrightoff;
    }

    /* Transfer all data items to the appropriate page. */
    let mut i = P_FIRSTDATAKEY(&oopaque);
    while i <= maxoff {
        let (mut dataitem, mut dsz) = {
            let pref = PageRef::new(&origpage)?;
            let itemid = PageGetItemId(&pref, i)?;
            let sz = ItemIdGetLength(&itemid) as usize;
            let item = PageGetItem(&pref, &itemid)?;
            let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, sz)?;
            v.extend_from_slice(item);
            (v, sz)
        };

        /* replace original item with nposting due to posting split? */
        if i == origpagepostingoff {
            dataitem.clear();
            dataitem.extend_from_slice(nposting.unwrap());
            dsz = nposting.unwrap().len();
        }
        /* does new item belong before this one? */
        else if i == newitemoff {
            if newitemonleft {
                debug_assert!(newitemoff <= firstrightoff);
                if !_bt_pgaddtup(&mut leftpage, newitemsz, newitem, afterleftoff, false) {
                    zero_page(&mut rightpage);
                    return Err(PgError::error(format!(
                        "failed to add new item to the left sibling while splitting block {} of index \"{}\"",
                        origpagenumber, rel_name(rel)
                    )));
                }
                afterleftoff = OffsetNumberNext(afterleftoff);
            } else {
                debug_assert!(newitemoff >= firstrightoff);
                if !_bt_pgaddtup(
                    &mut rightpage,
                    newitemsz,
                    newitem,
                    afterrightoff,
                    afterrightoff == minusinfoff,
                ) {
                    zero_page(&mut rightpage);
                    return Err(PgError::error(format!(
                        "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                        origpagenumber, rel_name(rel)
                    )));
                }
                afterrightoff = OffsetNumberNext(afterrightoff);
            }
        }

        /* decide which page to put it on */
        if i < firstrightoff {
            if !_bt_pgaddtup(&mut leftpage, dsz, &dataitem, afterleftoff, false) {
                zero_page(&mut rightpage);
                return Err(PgError::error(format!(
                    "failed to add old item to the left sibling while splitting block {} of index \"{}\"",
                    origpagenumber, rel_name(rel)
                )));
            }
            afterleftoff = OffsetNumberNext(afterleftoff);
        } else {
            if !_bt_pgaddtup(
                &mut rightpage,
                dsz,
                &dataitem,
                afterrightoff,
                afterrightoff == minusinfoff,
            ) {
                zero_page(&mut rightpage);
                return Err(PgError::error(format!(
                    "failed to add old item to the right sibling while splitting block {} of index \"{}\"",
                    origpagenumber, rel_name(rel)
                )));
            }
            afterrightoff = OffsetNumberNext(afterrightoff);
        }

        i = OffsetNumberNext(i);
    }

    /* Handle case where newitem goes at the end of rightpage. */
    if i <= newitemoff {
        debug_assert!(!newitemonleft && newitemoff == maxoff + 1);
        if !_bt_pgaddtup(
            &mut rightpage,
            newitemsz,
            newitem,
            afterrightoff,
            afterrightoff == minusinfoff,
        ) {
            zero_page(&mut rightpage);
            return Err(PgError::error(format!(
                "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                origpagenumber, rel_name(rel)
            )));
        }
        afterrightoff = OffsetNumberNext(afterrightoff);
    }
    let _ = afterrightoff;

    /*
     * Grab the original right sibling (if any) and update its prev link.
     */
    let mut sbuf = InvalidBuffer;
    let mut spage_set_split_end = false;
    if !isrightmost {
        sbuf = _bt_getbuf(mcx, rel, oopaque.btpo_next, BT_WRITE)?;
        let spage = bufmgr::buffer_get_page::call(mcx, sbuf)?;
        let sopaque = opaque_from_page(&spage)?;
        if sopaque.btpo_prev != origpagenumber {
            zero_page(&mut rightpage);
            return Err(PgError::error(format!(
                "right sibling's left-link doesn't match: block {} links to {} instead of expected {} in index \"{}\"",
                oopaque.btpo_next, sopaque.btpo_prev, origpagenumber, rel_name(rel)
            )));
        }
        /* SPLIT_END flag if right sibling has a different cycleid. */
        if sopaque.btpo_cycleid != ropaque.btpo_cycleid {
            ropaque.btpo_flags |= BTP_SPLIT_END;
            encode_opaque(&mut rightpage, &ropaque);
        }
        spage_set_split_end = true;
    }
    let _ = spage_set_split_end;

    /*
     * NO EREPORT(ERROR) till right sibling is updated.
     */
    miscinit::start_crit_section::call();

    /* Copy the new left page back on top of the original (PageRestoreTempPage). */
    bufmgr::with_buffer_page::call(buf, &mut |pg: &mut [u8]| {
        pg.copy_from_slice(&leftpage);
        Ok(())
    })?;
    /* Write the assembled right page image into the new buffer. */
    bufmgr::with_buffer_page::call(rbuf, &mut |pg: &mut [u8]| {
        pg.copy_from_slice(&rightpage);
        Ok(())
    })?;

    bufmgr::mark_buffer_dirty::call(buf);
    bufmgr::mark_buffer_dirty::call(rbuf);

    if !isrightmost {
        bufmgr::with_buffer_page::call(sbuf, &mut |pg: &mut [u8]| {
            let mut so = opaque_from_page(pg)?;
            so.btpo_prev = rightpagenumber;
            encode_opaque(pg, &so);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(sbuf);
    }

    /* Clear INCOMPLETE_SPLIT on child if this finishes a split. */
    if !isleaf {
        bufmgr::with_buffer_page::call(cbuf, &mut |pg: &mut [u8]| {
            let mut cop = opaque_from_page(pg)?;
            cop.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
            encode_opaque(pg, &cop);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(cbuf);
    }

    /* XLOG stuff */
    let needs_wal = relcache::relation_needs_wal::call(rel);
    if needs_wal {
        let mut xlrec_postingoff: u16 = 0;
        if postingoff != 0 && origpagepostingoff < firstrightoff {
            xlrec_postingoff = postingoff;
        }
        let xlrec = xl_btree_split {
            level: ropaque.btpo_level,
            firstrightoff,
            newitemoff,
            postingoff: xlrec_postingoff,
        };

        xloginsert::xlog_begin_insert::call()?;
        let mut hb = StdVec::with_capacity(SizeOfBtreeSplit);
        hb.extend_from_slice(&xlrec.level.to_ne_bytes());
        hb.extend_from_slice(&xlrec.firstrightoff.to_ne_bytes());
        hb.extend_from_slice(&xlrec.newitemoff.to_ne_bytes());
        hb.extend_from_slice(&xlrec.postingoff.to_ne_bytes());
        xloginsert::xlog_register_data::call(&hb)?;

        xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
        xloginsert::xlog_register_buffer::call(1, rbuf, REGBUF_WILL_INIT)?;
        if !isrightmost {
            xloginsert::xlog_register_buffer::call(2, sbuf, REGBUF_STANDARD)?;
        }
        if !isleaf {
            xloginsert::xlog_register_buffer::call(3, cbuf, REGBUF_STANDARD)?;
        }

        /* Log the new item if it went on the left page. */
        if newitemonleft && xlrec.postingoff == 0 {
            xloginsert::xlog_register_buf_data::call(0, &newitem[..newitemsz.min(newitem.len())])?;
        } else if xlrec.postingoff != 0 {
            debug_assert!(isleaf);
            let orig = orignewitem.unwrap();
            xloginsert::xlog_register_buf_data::call(0, &orig[..newitemsz.min(orig.len())])?;
        }

        /* Log the left page's new high key. */
        let lefthighkey_log: PgVec<'mcx, u8> = if !isleaf {
            /* lefthighkey isn't a local copy; read current pointer from origpage
             * image (now == leftpage, written back to buf). */
            let pref = PageRef::new(&leftpage)?;
            let itemid = PageGetItemId(&pref, P_HIKEY)?;
            let item = PageGetItem(&pref, &itemid)?;
            let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, item.len())?;
            v.extend_from_slice(item);
            v
        } else {
            let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, lefthighkey.len())?;
            v.extend_from_slice(&lefthighkey);
            v
        };
        let lhk_sz = maxalign(IndexTupleSize(&index_tuple_header(&lefthighkey_log)));
        xloginsert::xlog_register_buf_data::call(0, &lefthighkey_log[..lhk_sz.min(lefthighkey_log.len())])?;

        /* Log the right page's data area (pd_upper..pd_special). */
        let rp_upper = pd_upper(&rightpage);
        let rp_special = pd_special(&rightpage);
        xloginsert::xlog_register_buf_data::call(1, &rightpage[rp_upper..rp_special])?;

        let xlinfo = if newitemonleft {
            XLOG_BTREE_SPLIT_L
        } else {
            XLOG_BTREE_SPLIT_R
        };
        let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, xlinfo)?;

        bufmgr::page_set_lsn::call(buf, recptr)?;
        bufmgr::page_set_lsn::call(rbuf, recptr)?;
        if !isrightmost {
            bufmgr::page_set_lsn::call(sbuf, recptr)?;
        }
        if !isleaf {
            bufmgr::page_set_lsn::call(cbuf, recptr)?;
        }
    }

    miscinit::end_crit_section::call();

    /* release the old right sibling */
    if !isrightmost {
        page_bt_relbuf(rel, sbuf);
    }
    /* release the child */
    if !isleaf {
        page_bt_relbuf(rel, cbuf);
    }

    Ok(rbuf)
}

/// `memset(page, 0, BufferGetPageSize)` over a page byte image (the
/// rightpage-zeroing error cleanup in `_bt_split`).
fn zero_page(page: &mut [u8]) {
    for b in page.iter_mut() {
        *b = 0;
    }
}

/// `PageSetLSN` over a raw page byte image (pd_lsn @ offset 0, 8 bytes).
fn set_page_lsn_bytes(page: &mut [u8], lsn: u64) {
    page[0..8].copy_from_slice(&lsn.to_ne_bytes());
}

// ===========================================================================
// _bt_insert_parent
// ===========================================================================

/// `_bt_insert_parent(rel, heaprel, buf, rbuf, stack, isroot, isonly)` — insert
/// the downlink into the parent, completing the split.
fn _bt_insert_parent<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    buf: Buffer,
    rbuf: Buffer,
    stack: BTStack,
    isroot: bool,
    isonly: bool,
) -> PgResult<()> {
    if isroot {
        debug_assert!(stack.is_none());
        debug_assert!(isonly);
        /* create a new root node one level up and update the metapage */
        let rootbuf = _bt_newlevel(mcx, rel, heaprel, buf, rbuf)?;
        /* release the split buffers */
        page_bt_relbuf(rel, rootbuf);
        page_bt_relbuf(rel, rbuf);
        page_bt_relbuf(rel, buf);
    } else {
        let bknum = bufmgr::buffer_get_block_number::call(buf);
        let rbknum = bufmgr::buffer_get_block_number::call(rbuf);
        let page = bufmgr::buffer_get_page::call(mcx, buf)?;

        let mut stack = stack;
        if stack.is_none() {
            /* concurrent ROOT page split */
            let opaque = opaque_from_page(&page)?;
            debug_assert!(!(P_ISLEAF(&opaque)
                && relation_get_target_block(rel) != InvalidBlockNumber));

            /* Find the leftmost page at the next level up */
            let pbuf = crate::search::_bt_get_endpoint(rel, opaque.btpo_level + 1, false)?;
            /* Set up a phony stack entry pointing there */
            stack = Some(Box::new(BTStackData {
                bts_blkno: bufmgr::buffer_get_block_number::call(pbuf),
                bts_offset: InvalidOffsetNumber,
                bts_parent: None,
            }));
            page_bt_relbuf(rel, pbuf);
        }

        /* get high key from left, a strict lower bound for new right page */
        let mut new_item: PgVec<'mcx, u8> = {
            let pref = PageRef::new(&page)?;
            let itemid = PageGetItemId(&pref, P_HIKEY)?;
            let ritem = PageGetItem(&pref, &itemid)?;
            copy_index_tuple(mcx, ritem)?
        };
        BTreeTupleSetDownLink(&mut new_item, rbknum);

        /*
         * Re-find and write lock the parent of buf (recovering from concurrent
         * downlink movement; updates the stack).
         */
        let bts_offset_before = stack.as_ref().unwrap().bts_offset;
        let bts_parent = clone_stack(&stack.as_ref().unwrap().bts_parent);
        let (pbuf, new_bts_offset) =
            _bt_getstackbuf(mcx, rel, heaprel, &mut stack, bknum)?;

        /* Unlock the right child (left child unlocked in _bt_insertonpg). */
        page_bt_relbuf(rel, rbuf);

        if pbuf == InvalidBuffer {
            return Err(PgError::error(format!(
                "failed to re-find parent key in index \"{}\" for split pages {}/{}",
                rel_name(rel),
                bknum,
                rbknum
            )));
        }

        let new_item_sz = maxalign(IndexTupleSize(&index_tuple_header(&new_item)));
        let _ = bts_offset_before;
        /* Recursively insert into the parent. */
        _bt_insertonpg(
            mcx,
            rel,
            heaprel,
            None, /* itup_key (NULL for non-leaf) */
            pbuf,
            buf,
            bts_parent,
            &new_item,
            new_item_sz,
            new_bts_offset + 1,
            0,
            isonly,
        )?;
    }
    Ok(())
}

// ===========================================================================
// _bt_finish_split
// ===========================================================================

/// `_bt_finish_split(rel, heaprel, lbuf, stack)` — finish an incomplete split.
/// On entry `lbuf` is write-locked; on exit it is unlocked and unpinned.
pub fn _bt_finish_split<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    lbuf: Buffer,
    stack: BTStack,
) -> PgResult<()> {
    let lpage = bufmgr::buffer_get_page::call(mcx, lbuf)?;
    let lpageop = opaque_from_page(&lpage)?;
    debug_assert!(P_INCOMPLETE_SPLIT(&lpageop));

    /* Lock right sibling, the one missing the downlink */
    let rbuf = _bt_getbuf(mcx, rel, lpageop.btpo_next, BT_WRITE)?;
    let rpage = bufmgr::buffer_get_page::call(mcx, rbuf)?;
    let rpageop = opaque_from_page(&rpage)?;

    let wasroot;
    if stack.is_none() {
        /* acquire lock on the metapage */
        let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_WRITE)?;
        let metapg = bufmgr::buffer_get_page::call(mcx, metabuf)?;
        let metad = meta_from_page(&metapg);

        wasroot = metad.btm_root == bufmgr::buffer_get_block_number::call(lbuf);

        page_bt_relbuf(rel, metabuf);
    } else {
        wasroot = false;
    }

    /* Was this the only page on the level before split? */
    let wasonly = P_LEFTMOST(&lpageop) && P_RIGHTMOST(&rpageop);

    _bt_insert_parent(mcx, rel, heaprel, lbuf, rbuf, stack, wasroot, wasonly)
}

// ===========================================================================
// _bt_getstackbuf
// ===========================================================================

/// `_bt_getstackbuf(rel, heaprel, stack, child)` — walk back up one step and find
/// the pivot tuple whose downlink points to `child`. Returns the write-locked
/// parent buffer (or `InvalidBuffer`), and the (possibly updated) offset; updates
/// `stack`'s `bts_blkno`/`bts_offset`.
pub(crate) fn _bt_getstackbuf<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    stack: &mut BTStack,
    child: BlockNumber,
) -> PgResult<(Buffer, OffsetNumber)> {
    let mut blkno = stack.as_ref().unwrap().bts_blkno;
    let mut start = stack.as_ref().unwrap().bts_offset;

    loop {
        let buf = _bt_getbuf(mcx, rel, blkno, BT_WRITE)?;
        let page = bufmgr::buffer_get_page::call(mcx, buf)?;
        let opaque = opaque_from_page(&page)?;

        if P_INCOMPLETE_SPLIT(&opaque) {
            let parent = clone_stack(&stack.as_ref().unwrap().bts_parent);
            _bt_finish_split(mcx, rel, heaprel, buf, parent)?;
            continue;
        }

        if !P_IGNORE(&opaque) {
            let minoff = P_FIRSTDATAKEY(&opaque);
            let maxoff = PageGetMaxOffsetNumber(&PageRef::new(&page)?);

            /* start = InvalidOffsetNumber means "search the whole page". */
            if start < minoff {
                start = minoff;
            }
            if start > maxoff {
                start = OffsetNumberNext(maxoff);
            }

            /* Scan to the right first, then to the left. */
            let mut offnum = start;
            while offnum <= maxoff {
                let pref = PageRef::new(&page)?;
                let itemid = PageGetItemId(&pref, offnum)?;
                let item = PageGetItem(&pref, &itemid)?;
                if BTreeTupleGetDownLink(item) == child {
                    let s = stack.as_mut().unwrap();
                    s.bts_blkno = blkno;
                    s.bts_offset = offnum;
                    return Ok((buf, offnum));
                }
                offnum = OffsetNumberNext(offnum);
            }

            let mut offnum = OffsetNumberPrev(start);
            while offnum >= minoff && offnum != InvalidOffsetNumber {
                let pref = PageRef::new(&page)?;
                let itemid = PageGetItemId(&pref, offnum)?;
                let item = PageGetItem(&pref, &itemid)?;
                if BTreeTupleGetDownLink(item) == child {
                    let s = stack.as_mut().unwrap();
                    s.bts_blkno = blkno;
                    s.bts_offset = offnum;
                    return Ok((buf, offnum));
                }
                offnum = OffsetNumberPrev(offnum);
            }
        }

        /* The item we're looking for moved right at least one page. */
        if P_RIGHTMOST(&opaque) {
            page_bt_relbuf(rel, buf);
            return Ok((InvalidBuffer, stack.as_ref().unwrap().bts_offset));
        }
        blkno = opaque.btpo_next;
        start = InvalidOffsetNumber;
        page_bt_relbuf(rel, buf);
    }
}

// ===========================================================================
// _bt_newlevel
// ===========================================================================

/// `_bt_newlevel(rel, heaprel, lbuf, rbuf)` — create a new level above the root.
fn _bt_newlevel<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    lbuf: Buffer,
    rbuf: Buffer,
) -> PgResult<Buffer> {
    let lbkno = bufmgr::buffer_get_block_number::call(lbuf);
    let rbkno = bufmgr::buffer_get_block_number::call(rbuf);
    let lpage = bufmgr::buffer_get_page::call(mcx, lbuf)?;
    let lopaque = opaque_from_page(&lpage)?;

    /* get a new root page */
    let rootbuf = _bt_allocbuf(mcx, rel, heaprel)?;
    let rootblknum = bufmgr::buffer_get_block_number::call(rootbuf);

    /* acquire lock on the metapage */
    let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_WRITE)?;

    /*
     * Create downlink item for left page (old root): "minus infinity".
     */
    let left_item_sz = SIZEOF_INDEX_TUPLE_DATA;
    let mut left_item: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, left_item_sz)?;
    left_item.resize(left_item_sz, 0u8);
    {
        let mut hdr = index_tuple_header(&left_item);
        hdr.t_info = left_item_sz as u16;
        write_index_tuple_header(&mut left_item, &hdr);
    }
    BTreeTupleSetDownLink(&mut left_item, lbkno);
    BTreeTupleSetNAtts(&mut left_item, 0, false);

    /*
     * Create downlink item for right page (key from the high key on left page).
     */
    let mut right_item: PgVec<'mcx, u8> = {
        let pref = PageRef::new(&lpage)?;
        let itemid = PageGetItemId(&pref, P_HIKEY)?;
        let item = PageGetItem(&pref, &itemid)?;
        copy_index_tuple(mcx, item)?
    };
    let right_item_sz = right_item.len();
    BTreeTupleSetDownLink(&mut right_item, rbkno);

    /* NO EREPORT(ERROR) from here till newroot op is logged */
    miscinit::start_crit_section::call();

    /* upgrade metapage if needed */
    let mut metad = {
        let metapg = bufmgr::buffer_get_page::call(mcx, metabuf)?;
        meta_from_page(&metapg)
    };
    if metad.btm_version < BTREE_NOVAC_VERSION {
        bufmgr::with_buffer_page::call(metabuf, &mut |pg: &mut [u8]| {
            _bt_upgrademetapage(pg);
            Ok(())
        })?;
        metad = meta_from_page(&bufmgr::buffer_get_page::call(mcx, metabuf)?);
    }

    let new_root_level = lopaque.btpo_level + 1;

    /* set btree special data + add the two downlinks on the root page. */
    let mut add_ok = true;
    bufmgr::with_buffer_page::call(rootbuf, &mut |pg: &mut [u8]| {
        let mut rootopaque = opaque_from_page(pg)?;
        rootopaque.btpo_prev = P_NONE;
        rootopaque.btpo_next = P_NONE;
        rootopaque.btpo_flags = BTP_ROOT;
        rootopaque.btpo_level = new_root_level;
        rootopaque.btpo_cycleid = 0;
        encode_opaque(pg, &rootopaque);

        if PageAddItem(pg, &left_item, left_item_sz, P_HIKEY) == InvalidOffsetNumber {
            add_ok = false;
            return Ok(());
        }
        if PageAddItem(pg, &right_item, right_item_sz, P_FIRSTKEY) == InvalidOffsetNumber {
            add_ok = false;
        }
        Ok(())
    })?;
    if !add_ok {
        miscinit::end_crit_section::call();
        return Err(PgError::error(format!(
            "failed to add key to new root page while splitting block {} of index \"{}\"",
            lbkno,
            rel_name(rel)
        )));
    }

    /* update metapage data */
    metad.btm_root = rootblknum;
    metad.btm_level = new_root_level;
    metad.btm_fastroot = rootblknum;
    metad.btm_fastlevel = new_root_level;
    bufmgr::with_buffer_page::call(metabuf, &mut |pg: &mut [u8]| {
        let mut m = meta_from_page(pg);
        m.btm_root = rootblknum;
        m.btm_level = new_root_level;
        m.btm_fastroot = rootblknum;
        m.btm_fastlevel = new_root_level;
        meta_into_page(pg, &m);
        Ok(())
    })?;

    /* Clear the incomplete-split flag in the left child */
    debug_assert!(P_INCOMPLETE_SPLIT(&lopaque));
    bufmgr::with_buffer_page::call(lbuf, &mut |pg: &mut [u8]| {
        let mut lop = opaque_from_page(pg)?;
        lop.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
        encode_opaque(pg, &lop);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(lbuf);
    bufmgr::mark_buffer_dirty::call(rootbuf);
    bufmgr::mark_buffer_dirty::call(metabuf);

    /* XLOG stuff */
    let needs_wal = relcache::relation_needs_wal::call(rel);
    if needs_wal {
        let xlrec = xl_btree_newroot {
            rootblk: rootblknum,
            level: metad.btm_level,
        };

        xloginsert::xlog_begin_insert::call()?;
        let mut xb = StdVec::with_capacity(SizeOfBtreeNewroot);
        xb.extend_from_slice(&xlrec.rootblk.to_ne_bytes());
        xb.extend_from_slice(&xlrec.level.to_ne_bytes());
        xloginsert::xlog_register_data::call(&xb)?;

        xloginsert::xlog_register_buffer::call(0, rootbuf, REGBUF_WILL_INIT)?;
        xloginsert::xlog_register_buffer::call(1, lbuf, REGBUF_STANDARD)?;
        xloginsert::xlog_register_buffer::call(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD)?;

        debug_assert!(metad.btm_version >= BTREE_NOVAC_VERSION);
        let md = xl_btree_metadata {
            version: metad.btm_version,
            root: rootblknum,
            level: metad.btm_level,
            fastroot: rootblknum,
            fastlevel: metad.btm_level,
            last_cleanup_num_delpages: metad.btm_last_cleanup_num_delpages,
            allequalimage: metad.btm_allequalimage,
        };
        xloginsert::xlog_register_buf_data::call(2, &serialize_xl_btree_metadata(&md))?;

        /* Log the root page's data area (pd_upper..pd_special). */
        let rootpg = bufmgr::buffer_get_page::call(mcx, rootbuf)?;
        let rp_upper = pd_upper(&rootpg);
        let rp_special = pd_special(&rootpg);
        xloginsert::xlog_register_buf_data::call(0, &rootpg[rp_upper..rp_special])?;

        let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_NEWROOT)?;

        bufmgr::page_set_lsn::call(lbuf, recptr)?;
        bufmgr::page_set_lsn::call(rootbuf, recptr)?;
        bufmgr::page_set_lsn::call(metabuf, recptr)?;
    }

    miscinit::end_crit_section::call();

    /* done with metapage */
    page_bt_relbuf(rel, metabuf);

    Ok(rootbuf)
}

// ===========================================================================
// _bt_pgaddtup
// ===========================================================================

/// `_bt_pgaddtup(page, itemsize, itup, itup_off, newfirstdataitem)` — add a data
/// item to a page during split, truncating to "minus infinity" when it becomes
/// the first data item on an internal right page.
fn _bt_pgaddtup(
    page: &mut [u8],
    mut itemsize: Size,
    itup: &[u8],
    itup_off: OffsetNumber,
    newfirstdataitem: bool,
) -> bool {
    if newfirstdataitem {
        /* trunctuple = *itup; t_info = sizeof(IndexTupleData); SetNAtts(0). */
        let mut trunctuple = [0u8; SIZEOF_INDEX_TUPLE_DATA];
        trunctuple.copy_from_slice(&itup[..SIZEOF_INDEX_TUPLE_DATA]);
        {
            let mut hdr = index_tuple_header(&trunctuple);
            hdr.t_info = SIZEOF_INDEX_TUPLE_DATA as u16;
            write_index_tuple_header(&mut trunctuple, &hdr);
        }
        BTreeTupleSetNAtts(&mut trunctuple, 0, false);
        itemsize = SIZEOF_INDEX_TUPLE_DATA;
        return PageAddItem(page, &trunctuple, itemsize, itup_off) != InvalidOffsetNumber;
    }

    PageAddItem(page, itup, itemsize, itup_off) != InvalidOffsetNumber
}

// ===========================================================================
// _bt_delete_or_dedup_one_page
// ===========================================================================

/// `_bt_delete_or_dedup_one_page(...)` — try to avoid a leaf page split via
/// simple deletion, bottom-up deletion, or deduplication.
fn _bt_delete_or_dedup_one_page<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    insertstate: &mut BTInsertStateData<'mcx>,
    itup_bytes: &[u8],
    simpleonly: bool,
    checkingunique: bool,
    mut uniquedup: bool,
    index_unchanged: bool,
) -> PgResult<()> {
    let buffer = insertstate.buf;
    let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
    let opaque = opaque_from_page(&page)?;

    debug_assert!(P_ISLEAF(&opaque));
    debug_assert!(simpleonly || insertstate.itup_key.as_ref().unwrap().heapkeyspace);
    debug_assert!(!simpleonly || (!checkingunique && !uniquedup && !index_unchanged));

    /* Scan over all items to see which need to be deleted (LP_DEAD). */
    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = PageGetMaxOffsetNumber(&PageRef::new(&page)?);
    let mut deletable: StdVec<OffsetNumber> = StdVec::with_capacity(MaxIndexTuplesPerPage);
    {
        let pref = PageRef::new(&page)?;
        let mut offnum = minoff;
        while offnum <= maxoff {
            let itemid = PageGetItemId(&pref, offnum)?;
            if ItemIdIsDead(&itemid) {
                deletable.push(offnum);
            }
            offnum = OffsetNumberNext(offnum);
        }
    }

    if !deletable.is_empty() {
        _bt_simpledel_pass(
            mcx, rel, buffer, heap_rel, &deletable, itup_bytes, minoff, maxoff,
        )?;
        insertstate.bounds_valid = false;

        /* Return when a page split has already been avoided */
        let freepage = bufmgr::buffer_get_page::call(mcx, buffer)?;
        if (PageGetFreeSpace(&PageRef::new(&freepage)?) as usize) >= insertstate.itemsz {
            return Ok(());
        }

        /* Might as well assume duplicates (if checkingunique) */
        uniquedup = true;
    }

    /*
     * Done with simple deletion; return early for callers that only want it.
     */
    if simpleonly || (checkingunique && !uniquedup) {
        debug_assert!(!index_unchanged);
        return Ok(());
    }

    /* Assume bounds about to be invalidated. */
    insertstate.bounds_valid = false;

    /*
     * Bottom-up index deletion pass.
     */
    if (index_unchanged || uniquedup)
        && dedup::_bt_bottomupdel_pass(mcx, rel, buffer, heap_rel, insertstate.itemsz)?
    {
        return Ok(());
    }

    /* Deduplication pass (when enabled and index-is-allequalimage). */
    if bt_get_deduplicate_items(rel) && insertstate.itup_key.as_ref().unwrap().allequalimage {
        dedup::_bt_dedup_pass(
            mcx,
            rel,
            buffer,
            itup_bytes,
            insertstate.itemsz,
            index_unchanged || uniquedup,
        )?;
    }

    Ok(())
}

// ===========================================================================
// _bt_simpledel_pass
// ===========================================================================

/// `_bt_simpledel_pass(rel, buffer, heapRel, deletable, ndeletable, newitem,
/// minoff, maxoff)` — simple index tuple deletion pass.
fn _bt_simpledel_pass<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buffer: Buffer,
    heap_rel: &Relation<'mcx>,
    deletable: &[OffsetNumber],
    newitem: &[u8],
    minoff: OffsetNumber,
    maxoff: OffsetNumber,
) -> PgResult<()> {
    let page = bufmgr::buffer_get_page::call(mcx, buffer)?;

    /* Get array of table blocks pointed to by LP_DEAD-set tuples */
    let deadblocks = _bt_deadblocks(&page, deletable, newitem)?;

    /* Initialize tableam state that describes the index deletion operation. */
    let cap = MaxTIDsPerBTreePage;
    let deltids: PgVec<'mcx, TmIndexDelete> = vec_with_capacity_in(mcx, cap)?;
    let status: PgVec<'mcx, TmIndexStatus> = vec_with_capacity_in(mcx, cap)?;
    let mut delstate = TmIndexDeleteOp {
        iblknum: bufmgr::buffer_get_block_number::call(buffer),
        bottomup: false,
        bottomupfreespace: 0,
        deltids,
        status,
    };

    let pref = PageRef::new(&page)?;
    let mut offnum = minoff;
    while offnum <= maxoff {
        let itemid = PageGetItemId(&pref, offnum)?;
        let itup = PageGetItem(&pref, &itemid)?;
        let ituphdr = index_tuple_header(itup);
        let item_dead = ItemIdIsDead(&itemid);

        if !BTreeTupleIsPosting(&ituphdr) {
            let tidblock = ipd_block_number(&ituphdr.t_tid);
            if deadblocks.binary_search(&tidblock).is_ok() {
                let id = delstate.deltids.len() as i16;
                delstate.deltids.push(TmIndexDelete {
                    tid: ituphdr.t_tid,
                    id,
                });
                delstate.status.push(TmIndexStatus {
                    idxoffnum: offnum,
                    knowndeletable: item_dead,
                    promising: false,
                    freespace: 0,
                });
            } else {
                debug_assert!(!item_dead);
            }
        } else {
            let nitem = BTreeTupleGetNPosting(&ituphdr) as usize;
            for p in 0..nitem {
                let tid = posting_list_n(itup, p);
                let tidblock = ipd_block_number(&tid);
                if deadblocks.binary_search(&tidblock).is_ok() {
                    let id = delstate.deltids.len() as i16;
                    delstate.deltids.push(TmIndexDelete { tid, id });
                    delstate.status.push(TmIndexStatus {
                        idxoffnum: offnum,
                        knowndeletable: item_dead,
                        promising: false,
                        freespace: 0,
                    });
                } else {
                    debug_assert!(!item_dead);
                }
            }
        }

        offnum = OffsetNumberNext(offnum);
    }

    debug_assert!(delstate.deltids.len() >= deletable.len());

    /* Physically delete LP_DEAD tuples (plus any delete-safe extra TIDs). */
    backend_access_nbtree_core_seams::bt_delitems_delete_check::call(
        mcx, rel, buffer, heap_rel, delstate,
    )
}

// ===========================================================================
// _bt_deadblocks / _bt_blk_cmp
// ===========================================================================

/// `_bt_deadblocks(page, deletable, ndeletable, newitem, nblocks)` — build the
/// sorted, unique-ified array of table block numbers from LP_DEAD index tuple
/// TIDs (plus the newitem's table block).
fn _bt_deadblocks<'mcx>(
    page: &[u8],
    deletable: &[OffsetNumber],
    newitem: &[u8],
) -> PgResult<StdVec<BlockNumber>> {
    let mut tidblocks: StdVec<BlockNumber> = StdVec::with_capacity(deletable.len() + 1);

    /* First add the table block for the incoming newitem. */
    let newhdr = index_tuple_header(newitem);
    debug_assert!(!BTreeTupleIsPosting(&newhdr) && !BTreeTupleIsPivot(&newhdr));
    tidblocks.push(ipd_block_number(&newhdr.t_tid));

    let pref = PageRef::new(page)?;
    for &off in deletable {
        let itemid = PageGetItemId(&pref, off)?;
        let itup = PageGetItem(&pref, &itemid)?;
        let ituphdr = index_tuple_header(itup);
        debug_assert!(ItemIdIsDead(&itemid));

        if !BTreeTupleIsPosting(&ituphdr) {
            tidblocks.push(ipd_block_number(&ituphdr.t_tid));
        } else {
            let nposting = BTreeTupleGetNPosting(&ituphdr) as usize;
            for j in 0..nposting {
                let tid = posting_list_n(itup, j);
                tidblocks.push(ipd_block_number(&tid));
            }
        }
    }

    /* qsort + qunique (_bt_blk_cmp == u32 compare). */
    tidblocks.sort_unstable_by(_bt_blk_cmp);
    tidblocks.dedup();

    Ok(tidblocks)
}

/// `_bt_blk_cmp(arg1, arg2)` — qsort comparison (pg_cmp_u32).
fn _bt_blk_cmp(b1: &BlockNumber, b2: &BlockNumber) -> core::cmp::Ordering {
    b1.cmp(b2)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the one inward seam this module owns (`bt_doinsert`). The integrator
/// wires this into the crate's `init_seams()` in `lib.rs`.
pub fn init_seams_insert() {
    backend_access_nbtree_core_seams::bt_doinsert::set(bt_doinsert);
}
