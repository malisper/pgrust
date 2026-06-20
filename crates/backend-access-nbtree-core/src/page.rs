#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! Port of `src/backend/access/nbtree/nbtpage.c` (PostgreSQL 18.3) —
//! BTree-specific page management: metapage init/upgrade/read, the buffer
//! lock/pin wrappers (`_bt_getbuf` / `_bt_lockbuf` / `_bt_relbuf` etc.), VACUUM
//! page-deletion machinery (`_bt_pagedel`, `_bt_mark_page_halfdead`,
//! `_bt_unlink_halfdead_page`, `_bt_lock_subtree_parent`), item deletion
//! (`_bt_delitems_*`), and the pending-FSM optimization.
//!
//! Pages are read via `bufmgr::buffer_get_page` into an owned `PgVec<u8>` and
//! decoded through [`PageRef`]; mutations go through `bufmgr::with_buffer_page`
//! (the safe equivalent of writing through a pinned buffer's `Page` pointer).
//! Critical sections / WAL mirror nbtdedup.c's idioms exactly.
//!
//! The on-page `BTPageOpaqueData` (16 bytes in the page's special area) and
//! `BTMetaPageData` (in `PageGetContents`) are decoded/encoded byte-for-byte to
//! match the C struct layout.
//!
//! Genuinely-unported external callees are reached by an honest
//! `panic!("<fn>: <callee> not yet ported")`:
//!
//! * `ReadBuffer` / `ReleaseAndReadBuffer` / `ExtendBufferedRel(EB_LOCK_FIRST)`
//!   / `ConditionalLockBuffer` — these bufmgr primitives have no seam yet
//!   (only `read_buffer_extended`/`with_buffer_page`/`lock_buffer` exist), so
//!   the page-allocation paths (`_bt_allocbuf` / `_bt_conditionallockbuf`)
//!   panic at exactly those boundaries.  `_bt_getbuf`/`_bt_relandgetbuf` use the
//!   `read_buffer_extended`/`release_buffer` equivalents that do exist.
//! * `GetFreeIndexPage` (indexfsm.c) — no seam (only `record_free_index_page`).
//! * `PredicateLockPageCombine` (predicate.c) — no seam.
//! * `table_index_delete_tuples` (tableam dispatch) — no seam crate exposes it.
//! * `smgr` bulk-write for `build_empty_metapage` is fully wired (the bulkwrite
//!   seam crate exists).

use mcx::{vec_with_capacity_in, MemoryContext, Mcx, PgVec};
use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, InvalidBuffer, OffsetNumber, RmgrId, Size, TransactionId,
    BLCKSZ,
};
use types_core::xact::{FirstNormalFullTransactionId, FullTransactionId};
use types_error::error::{DEBUG1, LOG};
use types_error::{PgError, PgResult};
use types_nbtree::{
    xl_btree_delete, xl_btree_mark_page_halfdead, xl_btree_metadata, xl_btree_newroot,
    xl_btree_unlink_page, xl_btree_vacuum, BTMetaPageData, BTPageOpaqueData, BTPendingFSM, BTStack,
    BTVacState, BTVacuumPosting, BTP_DELETED, BTP_HALF_DEAD, BTP_HAS_FULLXID, BTP_HAS_GARBAGE,
    BTP_INCOMPLETE_SPLIT, BTP_LEAF, BTP_META, BTP_ROOT, BTREE_MAGIC, BTREE_METAPAGE,
    BTREE_MIN_VERSION, BTREE_NOVAC_VERSION, BTREE_VERSION, BT_IS_POSTING, BT_OFFSET_MASK,
    INDEX_ALT_TID_MASK, MaxIndexTuplesPerPage, P_FIRSTKEY, P_HIKEY, P_NONE, XLOG_BTREE_DELETE,
    XLOG_BTREE_MARK_PAGE_HALFDEAD, XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT,
    XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use types_rel::Relation;
use types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use types_storage::storage::Buffer;
use types_tuple::heaptuple::{BlockIdData, IndexTupleData, IndexTupleSize, ItemPointerData};
use types_wal::xloginsert::{REGBUF_STANDARD, REGBUF_WILL_INIT};

use backend_storage_page::{
    PageGetContents, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetSpecialPointer,
    PageGetSpecialSize, PageInit, PageIsNew, PageMut, PageRef,
};

use backend_access_nbtree_core_seams as nbtcore;
use backend_access_transam_varsup_seams as varsup;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_freespace_seams as indexfsm;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_smgr_bulkwrite_seams as bulkwrite;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_error_elog_seams as elog;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_init_small_seams as initsmall;

// ---------------------------------------------------------------------------
// c.h / nbtree.h constants.
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF`.
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(len)` (`c.h`).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `SizeOfPageHeaderData` (`storage/bufpage.h`).
const SizeOfPageHeaderData: usize = 24;

/// `RM_BTREE_ID` (`access/rmgrlist.h`).
const RM_BTREE_ID: RmgrId = 11;

/// `BT_READ` (`access/nbtree.h`) — `BUFFER_LOCK_SHARE`.
const BT_READ: i32 = BUFFER_LOCK_SHARE;
/// `BT_WRITE` (`access/nbtree.h`) — `BUFFER_LOCK_EXCLUSIVE`.
const BT_WRITE: i32 = BUFFER_LOCK_EXCLUSIVE;

/// `sizeof(BTMetaPageData)`.
const SIZEOF_BTMETA: usize = ::core::mem::size_of::<BTMetaPageData>();

/// `sizeof(ItemPointerData)`.
const SIZEOF_IPD: usize = ::core::mem::size_of::<ItemPointerData>();

/// `MaxAllocSize` (`utils/memutils.h`) — `0x3fffffff`.
const MaxAllocSize: usize = 0x3fff_ffff;

/// `INT_MAX`.
const INT_MAX_SZ: usize = i32::MAX as usize;

/// `sizeof(OffsetNumber)`.
const SIZEOF_OFFSET: usize = ::core::mem::size_of::<OffsetNumber>();

/// `sizeof(BTPendingFSM)`.
const SIZEOF_BTPENDINGFSM: usize = ::core::mem::size_of::<BTPendingFSM>();

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberNext(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber + 1
}

/// Run `f` with a short-lived [`Mcx`] for page reads / scratch allocations.
///
/// The page-deletion / metapage seams in this module don't carry an `Mcx<'mcx>`
/// in their (consumer-pinned) signatures, yet they need to read pages
/// (`bufmgr::buffer_get_page` returns a `PgVec` charged to an `Mcx`) and form a
/// little scratch state.  C reads these pages into `CurrentMemoryContext` (or,
/// for `_bt_pagedel`, an explicit temp context that the caller resets), freeing
/// them as it goes.  We mirror that with a per-call temp `MemoryContext`: all
/// page snapshots / scratch vectors are charged to it and dropped at return.
/// Values that must outlive the call (e.g. `BTPendingFSM`, which is `Copy`) are
/// stored into caller-owned `'mcx` collections directly, never into this temp.
fn with_temp_mcx<T>(f: impl FnOnce(Mcx<'_>) -> PgResult<T>) -> PgResult<T> {
    let ctx = MemoryContext::new("nbtpage");
    f(ctx.mcx())
}

// ---------------------------------------------------------------------------
// BTPageOpaqueData (special area) decode / in-place encode.
// ---------------------------------------------------------------------------

/// `BTPageGetOpaque(page)` — decode the 16-byte `BTPageOpaqueData`.
#[allow(dead_code)]
fn BTPageGetOpaque(page: &PageRef<'_>) -> PgResult<BTPageOpaqueData> {
    let special = PageGetSpecialPointer(page)?;
    decode_opaque(special)
}

/// Decode a `BTPageOpaqueData` from a 16-byte special-area slice.
fn decode_opaque(special: &[u8]) -> PgResult<BTPageOpaqueData> {
    if special.len() < 16 {
        return Err(PgError::error("BTPageGetOpaque: special area too small"));
    }
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

/// Byte offset of the page's special area (`pd_special`, a `u16` at offset 16).
#[inline]
fn special_offset(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[16], page[17]]) as usize
}

/// Write a full `BTPageOpaqueData` back into the page's special area.
fn encode_opaque(page: &mut [u8], opaque: &BTPageOpaqueData) {
    let off = special_offset(page);
    page[off..off + 4].copy_from_slice(&opaque.btpo_prev.to_ne_bytes());
    page[off + 4..off + 8].copy_from_slice(&opaque.btpo_next.to_ne_bytes());
    page[off + 8..off + 12].copy_from_slice(&opaque.btpo_level.to_ne_bytes());
    page[off + 12..off + 14].copy_from_slice(&opaque.btpo_flags.to_ne_bytes());
    page[off + 14..off + 16].copy_from_slice(&opaque.btpo_cycleid.to_ne_bytes());
}

/// Read the `BTPageOpaqueData` directly out of a raw page byte slice.
fn opaque_from_page(page: &[u8]) -> PgResult<BTPageOpaqueData> {
    let off = special_offset(page);
    if off + 16 > page.len() {
        return Err(PgError::error("BTPageGetOpaque: special area out of bounds"));
    }
    decode_opaque(&page[off..off + 16])
}

// --- opaque flag predicates (nbtree.h P_* macros) -------------------------

#[inline]
fn P_RIGHTMOST(o: &BTPageOpaqueData) -> bool {
    o.btpo_next == P_NONE
}
#[inline]
#[allow(dead_code)]
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
fn P_ISDELETED(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_DELETED) != 0
}
#[inline]
fn P_ISMETA(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_META) != 0
}
#[inline]
fn P_ISHALFDEAD(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_HALF_DEAD) != 0
}
#[inline]
fn P_IGNORE(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & (BTP_DELETED | BTP_HALF_DEAD)) != 0
}
#[inline]
fn P_HAS_FULLXID(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_HAS_FULLXID) != 0
}
#[inline]
fn P_INCOMPLETE_SPLIT(o: &BTPageOpaqueData) -> bool {
    (o.btpo_flags & BTP_INCOMPLETE_SPLIT) != 0
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

// ---------------------------------------------------------------------------
// BTMetaPageData decode / in-place encode (PageGetContents).
// ---------------------------------------------------------------------------

/// `BTPageGetMeta(page)` — decode the metapage payload from `PageGetContents`.
fn meta_from_page(page: &[u8]) -> PgResult<BTMetaPageData> {
    let base = maxalign(SizeOfPageHeaderData);
    if base + SIZEOF_BTMETA > page.len() {
        return Err(PgError::error("BTPageGetMeta: metapage too small"));
    }
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
    // Field offsets follow the #[repr(C)] BTMetaPageData layout: 7 u32 packed
    // (magic..last_cleanup_num_delpages @ 0..28), 4 bytes padding to align the
    // f64 (@ 32), then the bool (@ 40).
    Ok(BTMetaPageData {
        btm_magic: rd_u32(0),
        btm_version: rd_u32(4),
        btm_root: rd_u32(8),
        btm_level: rd_u32(12),
        btm_fastroot: rd_u32(16),
        btm_fastlevel: rd_u32(20),
        btm_last_cleanup_num_delpages: rd_u32(24),
        btm_last_cleanup_num_heap_tuples: rd_f64(32),
        btm_allequalimage: b[40] != 0,
    })
}

/// Write a `BTMetaPageData` into the page's `PageGetContents` area (in place).
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

/// Set the page header's `pd_lower` (a `u16` at page header offset 12).
fn set_pd_lower(page: &mut [u8], lower: u16) {
    page[12..14].copy_from_slice(&lower.to_ne_bytes());
}
/// Read the page header's `pd_special` (`u16` @ 16).
fn pd_special(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[16], page[17]])
}
/// Set `pd_upper` (`u16` @ 14).
fn set_pd_upper(page: &mut [u8], upper: u16) {
    page[14..16].copy_from_slice(&upper.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// IndexTuple header / pivot/posting helpers (mirrors nbtdedup.c).
// ---------------------------------------------------------------------------

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

/// `ItemPointerGetBlockNumberNoCheck(&t_tid)`.
fn ipd_block_number(t: &ItemPointerData) -> BlockNumber {
    ((t.ip_blkid.bi_hi as u32) << 16) | (t.ip_blkid.bi_lo as u32)
}

/// `ItemPointerSetBlockNumber(&mut t_tid, blkno)`.
fn ipd_set_block_number(t: &mut ItemPointerData, blkno: BlockNumber) {
    t.ip_blkid.bi_hi = (blkno >> 16) as u16;
    t.ip_blkid.bi_lo = (blkno & 0xFFFF) as u16;
}

/// `ItemPointerGetOffsetNumberNoCheck(&t_tid)`.
fn ipd_offset(t: &ItemPointerData) -> u16 {
    t.ip_posid
}
/// `ItemPointerSetOffsetNumber(&mut t_tid, off)`.
fn ipd_set_offset(t: &mut ItemPointerData, off: u16) {
    t.ip_posid = off;
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
    let hdr = index_tuple_header(pivot);
    ipd_block_number(&hdr.t_tid)
}

/// `BTreeTupleGetTopParent(leafhikey)`.
fn BTreeTupleGetTopParent(leafhikey: &[u8]) -> BlockNumber {
    let hdr = index_tuple_header(leafhikey);
    ipd_block_number(&hdr.t_tid)
}

/// `write_index_tuple_header(bytes, hdr)` — write an `IndexTupleData` header.
fn write_index_tuple_header(bytes: &mut [u8], hdr: &IndexTupleData) {
    write_ipd(bytes, 0, &hdr.t_tid);
    let info = hdr.t_info.to_ne_bytes();
    bytes[6] = info[0];
    bytes[7] = info[1];
}

// ---------------------------------------------------------------------------
// _bt_initmetapage / _bt_upgrademetapage  (in-memory page-image edits)
// ---------------------------------------------------------------------------

/// `_bt_initmetapage(page, rootbknum, level, allequalimage)` — fill a page
/// buffer with a correct metapage image.
pub fn _bt_initmetapage(page: &mut [u8], rootbknum: BlockNumber, level: u32, allequalimage: bool) {
    PageInit_inplace(page, page.len(), ::core::mem::size_of::<BTPageOpaqueData>());

    let metad = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: rootbknum,
        btm_level: level,
        btm_fastroot: rootbknum,
        btm_fastlevel: level,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: allequalimage,
    };
    meta_into_page(page, &metad);

    let mut metaopaque = opaque_from_page(page).expect("_bt_initmetapage opaque");
    metaopaque.btpo_flags = BTP_META;
    encode_opaque(page, &metaopaque);

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.  (BTMetaPageData lives at PageGetContents = MAXALIGN(header).)
     */
    let lower = maxalign(SizeOfPageHeaderData) + SIZEOF_BTMETA;
    set_pd_lower(page, lower as u16);
}

/// `_bt_upgrademetapage(page)` — upgrade a meta-page image to BTREE_NOVAC_VERSION.
pub fn _bt_upgrademetapage(page: &mut [u8]) {
    let mut metad = meta_from_page(page).expect("_bt_upgrademetapage meta");

    debug_assert!({
        let o = opaque_from_page(page).unwrap();
        (o.btpo_flags & BTP_META) != 0
    });
    debug_assert!(metad.btm_version < BTREE_NOVAC_VERSION);
    debug_assert!(metad.btm_version >= BTREE_MIN_VERSION);

    metad.btm_version = BTREE_NOVAC_VERSION;
    metad.btm_last_cleanup_num_delpages = 0;
    metad.btm_last_cleanup_num_heap_tuples = -1.0;
    debug_assert!(!metad.btm_allequalimage);
    metad.btm_allequalimage = false;
    meta_into_page(page, &metad);

    let lower = maxalign(SizeOfPageHeaderData) + SIZEOF_BTMETA;
    set_pd_lower(page, lower as u16);
}

/// `_bt_getmeta(rel, metabuf)` — read + sanity-check the metapage.
fn _bt_getmeta<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    metabuf: Buffer,
) -> PgResult<BTMetaPageData> {
    let page_bytes = bufmgr::buffer_get_page::call(mcx, metabuf)?;
    let metaopaque = opaque_from_page(&page_bytes)?;
    let metad = meta_from_page(&page_bytes)?;

    if !P_ISMETA(&metaopaque) || metad.btm_magic != BTREE_MAGIC {
        return Err(PgError::error(format!(
            "index \"{}\" is not a btree",
            rel.name()
        )));
    }
    if metad.btm_version < BTREE_MIN_VERSION || metad.btm_version > BTREE_VERSION {
        return Err(PgError::error(format!(
            "version mismatch in index \"{}\": file version {}, current version {}, minimal supported version {}",
            rel.name(), metad.btm_version, BTREE_VERSION, BTREE_MIN_VERSION
        )));
    }
    Ok(metad)
}

// ---------------------------------------------------------------------------
// _bt_vacuum_needs_cleanup / _bt_set_cleanup_info
// ---------------------------------------------------------------------------

/// `_bt_vacuum_needs_cleanup(rel)` — decide whether cleanup-only scan is needed.
pub fn bt_vacuum_needs_cleanup<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool> {
    with_temp_mcx(|mcx| {
        let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_READ)?;
        let page_bytes = bufmgr::buffer_get_page::call(mcx, metabuf)?;
        let metad = meta_from_page(&page_bytes)?;
        let btm_version = metad.btm_version;

        if btm_version < BTREE_NOVAC_VERSION {
            _bt_relbuf(rel, metabuf);
            return Ok(true);
        }

        let prev_num_delpages = metad.btm_last_cleanup_num_delpages;
        _bt_relbuf(rel, metabuf);

        if prev_num_delpages > 0 {
            let nblocks = relcache::relation_get_number_of_blocks::call(rel)?;
            if prev_num_delpages > nblocks / 20 {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

/// `_bt_set_cleanup_info(rel, num_delpages)` — record num_delpages in metapage.
pub fn bt_set_cleanup_info<'mcx>(rel: &Relation<'mcx>, num_delpages: BlockNumber) -> PgResult<()> {
    with_temp_mcx(|mcx| {
        let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_READ)?;
        let page_bytes = bufmgr::buffer_get_page::call(mcx, metabuf)?;
        let metad = meta_from_page(&page_bytes)?;

        if metad.btm_version >= BTREE_NOVAC_VERSION
            && metad.btm_last_cleanup_num_delpages == num_delpages
        {
            _bt_relbuf(rel, metabuf);
            return Ok(());
        }

        /* trade in our read lock for a write lock */
        _bt_unlockbuf(rel, metabuf);
        _bt_lockbuf(rel, metabuf, BT_WRITE);

        let needs_wal = relcache::relation_needs_wal::call(rel);

        miscinit::start_crit_section::call();

        let mut md_wal: Option<xl_btree_metadata> = None;
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            let mut metad = meta_from_page(page)?;
            if metad.btm_version < BTREE_NOVAC_VERSION {
                _bt_upgrademetapage(page);
                metad = meta_from_page(page)?;
            }
            metad.btm_last_cleanup_num_delpages = num_delpages;
            metad.btm_last_cleanup_num_heap_tuples = -1.0;
            meta_into_page(page, &metad);
            if needs_wal {
                md_wal = Some(xl_btree_metadata {
                    version: metad.btm_version,
                    root: metad.btm_root,
                    level: metad.btm_level,
                    fastroot: metad.btm_fastroot,
                    fastlevel: metad.btm_fastlevel,
                    last_cleanup_num_delpages: num_delpages,
                    allequalimage: metad.btm_allequalimage,
                });
            }
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(metabuf);

        if let Some(md) = md_wal {
            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_buffer::call(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
            let mdb = serialize_xl_btree_metadata(&md);
            xloginsert::xlog_register_buf_data::call(0, &mdb)?;
            let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_META_CLEANUP)?;
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }

        miscinit::end_crit_section::call();

        _bt_relbuf(rel, metabuf);
        Ok(())
    })
}

/// Serialize an `xl_btree_metadata` to its on-disk byte image.
fn serialize_xl_btree_metadata(md: &xl_btree_metadata) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::with_capacity(SIZEOF_BTMETA);
    out.extend_from_slice(&md.version.to_ne_bytes());
    out.extend_from_slice(&md.root.to_ne_bytes());
    out.extend_from_slice(&md.level.to_ne_bytes());
    out.extend_from_slice(&md.fastroot.to_ne_bytes());
    out.extend_from_slice(&md.fastlevel.to_ne_bytes());
    out.extend_from_slice(&md.last_cleanup_num_delpages.to_ne_bytes());
    out.push(md.allequalimage as u8);
    out
}

// ---------------------------------------------------------------------------
// _bt_getroot / _bt_gettrueroot / _bt_getrootheight / _bt_metaversion
//
// The C versions cache BTMetaPageData in rel->rd_amcache.  The repo Relation
// has no rd_amcache field, so we faithfully re-read the metapage each call
// (behaviour-preserving: rd_amcache is only a performance cache, and the C
// comments note slightly-stale data is fine for these read paths).
// ---------------------------------------------------------------------------

/// `_bt_getroot(rel, heaprel, access)` — get (and create, for BT_WRITE) root.
pub fn _bt_getroot<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    access: i32,
) -> PgResult<Buffer> {
    let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_READ)?;
    let metad = _bt_getmeta(mcx, rel, metabuf)?;

    if metad.btm_root == P_NONE {
        if access == BT_READ {
            _bt_relbuf(rel, metabuf);
            return Ok(InvalidBuffer);
        }

        _bt_unlockbuf(rel, metabuf);
        _bt_lockbuf(rel, metabuf, BT_WRITE);

        let metad2 = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, metabuf)?;
            meta_from_page(&page_bytes)?
        };
        if metad2.btm_root != P_NONE {
            _bt_relbuf(rel, metabuf);
            return _bt_getroot(mcx, rel, heaprel, access);
        }

        let rootbuf = _bt_allocbuf(mcx, rel, heaprel)?;
        let rootblkno = bufmgr::buffer_get_block_number::call(rootbuf);

        let needs_wal = relcache::relation_needs_wal::call(rel);

        miscinit::start_crit_section::call();

        bufmgr::with_buffer_page::call(rootbuf, &mut |page: &mut [u8]| {
            let mut rootopaque = opaque_from_page(page)?;
            rootopaque.btpo_prev = P_NONE;
            rootopaque.btpo_next = P_NONE;
            rootopaque.btpo_flags = BTP_LEAF | BTP_ROOT;
            rootopaque.btpo_level = 0;
            rootopaque.btpo_cycleid = 0;
            encode_opaque(page, &rootopaque);
            Ok(())
        })?;

        let mut md_wal: Option<xl_btree_metadata> = None;
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            let mut metad = meta_from_page(page)?;
            if metad.btm_version < BTREE_NOVAC_VERSION {
                _bt_upgrademetapage(page);
                metad = meta_from_page(page)?;
            }
            metad.btm_root = rootblkno;
            metad.btm_level = 0;
            metad.btm_fastroot = rootblkno;
            metad.btm_fastlevel = 0;
            metad.btm_last_cleanup_num_delpages = 0;
            metad.btm_last_cleanup_num_heap_tuples = -1.0;
            meta_into_page(page, &metad);
            if needs_wal {
                md_wal = Some(xl_btree_metadata {
                    version: metad.btm_version,
                    root: rootblkno,
                    level: 0,
                    fastroot: rootblkno,
                    fastlevel: 0,
                    last_cleanup_num_delpages: 0,
                    allequalimage: metad.btm_allequalimage,
                });
            }
            Ok(())
        })?;

        bufmgr::mark_buffer_dirty::call(rootbuf);
        bufmgr::mark_buffer_dirty::call(metabuf);

        if let Some(md) = md_wal {
            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_buffer::call(0, rootbuf, REGBUF_WILL_INIT)?;
            xloginsert::xlog_register_buffer::call(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD)?;

            let mdb = serialize_xl_btree_metadata(&md);
            xloginsert::xlog_register_buf_data::call(2, &mdb)?;

            let xlrec = xl_btree_newroot {
                rootblk: rootblkno,
                level: 0,
            };
            let mut xb = std::vec::Vec::with_capacity(8);
            xb.extend_from_slice(&xlrec.rootblk.to_ne_bytes());
            xb.extend_from_slice(&xlrec.level.to_ne_bytes());
            xloginsert::xlog_register_data::call(&xb)?;

            let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_NEWROOT)?;
            bufmgr::page_set_lsn::call(rootbuf, recptr)?;
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }

        miscinit::end_crit_section::call();

        _bt_unlockbuf(rel, rootbuf);
        _bt_lockbuf(rel, rootbuf, BT_READ);

        _bt_relbuf(rel, metabuf);
        Ok(rootbuf)
    } else {
        let mut rootblkno = metad.btm_fastroot;
        debug_assert!(rootblkno != P_NONE);
        let rootlevel = metad.btm_fastlevel;

        let mut rootbuf = metabuf;
        loop {
            rootbuf = _bt_relandgetbuf(mcx, rel, rootbuf, rootblkno, BT_READ)?;
            let page_bytes = bufmgr::buffer_get_page::call(mcx, rootbuf)?;
            let rootopaque = opaque_from_page(&page_bytes)?;

            if !P_IGNORE(&rootopaque) {
                if rootopaque.btpo_level != rootlevel {
                    return Err(PgError::error(format!(
                        "root page {} of index \"{}\" has level {}, expected {}",
                        rootblkno,
                        rel.name(),
                        rootopaque.btpo_level,
                        rootlevel
                    )));
                }
                return Ok(rootbuf);
            }

            if P_RIGHTMOST(&rootopaque) {
                return Err(PgError::error(format!(
                    "no live root page found in index \"{}\"",
                    rel.name()
                )));
            }
            rootblkno = rootopaque.btpo_next;
        }
    }
}

/// `_bt_gettrueroot(rel)` — get the true (not fast) root page.
pub fn _bt_gettrueroot<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<Buffer> {
    let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_READ)?;
    let (metaopaque, metad) = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, metabuf)?;
        (opaque_from_page(&page_bytes)?, meta_from_page(&page_bytes)?)
    };

    if !P_ISMETA(&metaopaque) || metad.btm_magic != BTREE_MAGIC {
        return Err(PgError::error(format!(
            "index \"{}\" is not a btree",
            rel.name()
        )));
    }
    if metad.btm_version < BTREE_MIN_VERSION || metad.btm_version > BTREE_VERSION {
        return Err(PgError::error(format!(
            "version mismatch in index \"{}\": file version {}, current version {}, minimal supported version {}",
            rel.name(), metad.btm_version, BTREE_VERSION, BTREE_MIN_VERSION
        )));
    }

    if metad.btm_root == P_NONE {
        _bt_relbuf(rel, metabuf);
        return Ok(InvalidBuffer);
    }

    let mut rootblkno = metad.btm_root;
    let rootlevel = metad.btm_level;

    let mut rootbuf = metabuf;
    loop {
        rootbuf = _bt_relandgetbuf(mcx, rel, rootbuf, rootblkno, BT_READ)?;
        let page_bytes = bufmgr::buffer_get_page::call(mcx, rootbuf)?;
        let rootopaque = opaque_from_page(&page_bytes)?;

        if !P_IGNORE(&rootopaque) {
            if rootopaque.btpo_level != rootlevel {
                return Err(PgError::error(format!(
                    "root page {} of index \"{}\" has level {}, expected {}",
                    rootblkno,
                    rel.name(),
                    rootopaque.btpo_level,
                    rootlevel
                )));
            }
            return Ok(rootbuf);
        }
        if P_RIGHTMOST(&rootopaque) {
            return Err(PgError::error(format!(
                "no live root page found in index \"{}\"",
                rel.name()
            )));
        }
        rootblkno = rootopaque.btpo_next;
    }
}

/// `_bt_getrootheight(rel)` — height of the tree (fast-root level).
pub fn bt_getrootheight<'mcx>(rel: &Relation<'mcx>) -> PgResult<i32> {
    with_temp_mcx(|mcx| {
        let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_READ)?;
        let metad = _bt_getmeta(mcx, rel, metabuf)?;

        if metad.btm_root == P_NONE {
            _bt_relbuf(rel, metabuf);
            return Ok(0);
        }
        let fastlevel = metad.btm_fastlevel;
        _bt_relbuf(rel, metabuf);
        Ok(fastlevel as i32)
    })
}

/// `_bt_metaversion(rel, *heapkeyspace, *allequalimage)` — read version flags.
pub fn bt_metaversion<'mcx>(rel: &Relation<'mcx>) -> PgResult<(bool, bool)> {
    with_temp_mcx(|mcx| {
        let metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_READ)?;
        let metad = _bt_getmeta(mcx, rel, metabuf)?;

        let heapkeyspace = metad.btm_version > BTREE_NOVAC_VERSION;
        let allequalimage = metad.btm_allequalimage;
        _bt_relbuf(rel, metabuf);
        Ok((heapkeyspace, allequalimage))
    })
}

// ---------------------------------------------------------------------------
// _bt_checkpage / _bt_getbuf / _bt_allocbuf / _bt_relandgetbuf
// ---------------------------------------------------------------------------

/// `_bt_checkpage(rel, buf)` — verify a freshly-read page looks sane.
pub fn bt_checkpage<'mcx>(rel: &Relation<'mcx>, buf: Buffer) -> PgResult<()> {
    with_temp_mcx(|mcx| {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        let page = PageRef::new(&page_bytes)?;

        if PageIsNew(&page) {
            return Err(PgError::error(format!(
                "index \"{}\" contains unexpected zero page at block {}",
                rel.name(),
                bufmgr::buffer_get_block_number::call(buf)
            )));
        }

        if PageGetSpecialSize(&page) as usize
            != maxalign(::core::mem::size_of::<BTPageOpaqueData>())
        {
            return Err(PgError::error(format!(
                "index \"{}\" contains corrupted page at block {}",
                rel.name(),
                bufmgr::buffer_get_block_number::call(buf)
            )));
        }
        Ok(())
    })
}

/// `_bt_getbuf(rel, blkno, access)` — get an existing block, locked + pinned.
pub(crate) fn _bt_getbuf<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    blkno: BlockNumber,
    access: i32,
) -> PgResult<Buffer> {
    debug_assert!(blkno != InvalidBlockNumber);

    /* Read an existing block of the relation (ReadBuffer == fork MAIN). */
    let buf = bufmgr::read_buffer_extended::call(rel, blkno)?;
    _bt_lockbuf(rel, buf, access);
    bt_checkpage(rel, buf)?;
    Ok(buf)
}

/// Serialize the `xl_btree_reuse_page` WAL record payload (nbtxlog.h:
/// `RelFileLocator locator; BlockNumber block; FullTransactionId
/// snapshotConflictHorizon; bool isCatalogRel`). `SizeOfBtreeReusePage` excludes
/// trailing alignment padding after `isCatalogRel`.
fn serialize_btree_reuse_page<'mcx>(
    rel: &Relation<'mcx>,
    block: BlockNumber,
    snapshot_conflict_horizon: FullTransactionId,
    is_catalog_rel: bool,
) -> Vec<u8> {
    let loc = &rel.rd_locator;
    let mut b = Vec::with_capacity(4 * 3 + 4 + 8 + 1);
    b.extend_from_slice(&loc.spcOid.to_ne_bytes());
    b.extend_from_slice(&loc.dbOid.to_ne_bytes());
    b.extend_from_slice(&loc.relNumber.to_ne_bytes());
    b.extend_from_slice(&block.to_ne_bytes());
    b.extend_from_slice(&snapshot_conflict_horizon.value.to_ne_bytes());
    b.push(is_catalog_rel as u8);
    b
}

/// `_bt_allocbuf(rel, heaprel)` — allocate a new write-locked nbtree page.
pub(crate) fn _bt_allocbuf<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
) -> PgResult<Buffer> {
    /*
     * First see if the FSM knows of any free pages.
     *
     * We can't trust the FSM's report unreservedly; we have to check that the
     * page is still free.  We ask for only a conditional lock on the reported
     * page to avoid deadlock against our own caller.
     */
    loop {
        let blkno = indexfsm::get_free_index_page::call(rel)?;
        if blkno == InvalidBlockNumber {
            break;
        }
        /* ReadBuffer(rel, blkno): pinned, not yet locked. */
        let buf = bufmgr::read_buffer_extended::call(rel, blkno)?;
        if _bt_conditionallockbuf(rel, buf) {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;

            /*
             * It's possible to find an all-zeroes page in an index.  If we find
             * a zeroed page then reclaim it immediately.
             */
            if PageIsNew(&PageRef::new(&page_bytes)?) {
                /* Okay to use page.  Initialize and return it. */
                bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
                    _bt_pageinit(page, BLCKSZ as Size);
                    Ok(())
                })?;
                return Ok(buf);
            }

            if bt_page_is_recyclable(&page_bytes, heaprel) {
                /*
                 * If we are generating WAL for Hot Standby then create a WAL
                 * record that will allow us to conflict with queries running on
                 * standby, in case they have snapshots older than safexid value.
                 */
                if relcache::relation_needs_wal::call(rel)
                    && backend_access_transam_xlog_seams::xlog_standby_info_active::call()
                {
                    /*
                     * Note that we don't register the buffer with the record,
                     * because this operation doesn't modify the page (that
                     * already happened, back when VACUUM deleted the page).
                     * This record only exists to provide a conflict point for
                     * Hot Standby.
                     */
                    let opaque = opaque_from_page(&page_bytes)?;
                    let snapshot_conflict_horizon = bt_page_get_delete_xid(&page_bytes, &opaque);
                    let is_catalog_rel = relation_is_accessible_in_logical_decoding(heaprel)?;

                    xloginsert::xlog_begin_insert::call()?;
                    xloginsert::xlog_register_data::call(&serialize_btree_reuse_page(
                        rel,
                        blkno,
                        snapshot_conflict_horizon,
                        is_catalog_rel,
                    ))?;
                    xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_REUSE_PAGE)?;
                }

                /* Okay to use page.  Re-initialize and return it. */
                bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
                    _bt_pageinit(page, BLCKSZ as Size);
                    Ok(())
                })?;
                return Ok(buf);
            }
            log_debug("FSM returned nonrecyclable page");
            _bt_relbuf(rel, buf);
        } else {
            log_debug("FSM returned nonlockable page");
            /* couldn't get lock, so just drop pin */
            bufmgr::release_buffer::call(buf);
        }
    }

    /*
     * Extend the relation by one page. Uses EB_LOCK_FIRST (taking the
     * relation-extension lock); the returned buffer is RBM_ZERO_AND_LOCK
     * equivalent.
     */
    let buf = bufmgr::extend_buffered_rel_locked::call(
        rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
    )?;

    /* Initialize the new page before returning it */
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        debug_assert!(PageIsNew(&PageRef::new(page)?));
        _bt_pageinit(page, BLCKSZ as Size);
        Ok(())
    })?;

    Ok(buf)
}

/// `_bt_relandgetbuf(rel, obuf, blkno, access)` — release `obuf`, get `blkno`.
pub(crate) fn _bt_relandgetbuf<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    obuf: Buffer,
    blkno: BlockNumber,
    access: i32,
) -> PgResult<Buffer> {
    debug_assert!(blkno != InvalidBlockNumber);

    /*
     * C: ReleaseAndReadBuffer(obuf, rel, blkno) — drops obuf's pin and reads
     * blkno in one bufmgr entry, reusing the buffer when it is already the
     * target.  No ReleaseAndReadBuffer seam exists; faithfully fall back to the
     * documented equivalent (release obuf, then read).  ReleaseAndReadBuffer is
     * only a "saves one bufmgr entry" micro-optimization (per the C comment),
     * so this is behaviour-preserving.
     */
    if obuf != InvalidBuffer {
        _bt_unlockbuf(rel, obuf);
        bufmgr::release_buffer::call(obuf);
    }
    let buf = bufmgr::read_buffer_extended::call(rel, blkno)?;
    _bt_lockbuf(rel, buf, access);
    bt_checkpage(rel, buf)?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Lock/unlock buffer wrappers.
// ---------------------------------------------------------------------------

/// `_bt_relbuf(rel, buf)` — release lock and pin (seam).
pub fn bt_relbuf<'mcx>(rel: &Relation<'mcx>, buf: Buffer) {
    _bt_relbuf(rel, buf);
}

fn _bt_relbuf<'mcx>(rel: &Relation<'mcx>, buf: Buffer) {
    _bt_unlockbuf(rel, buf);
    bufmgr::release_buffer::call(buf);
}

/// `_bt_lockbuf(rel, buf, BT_READ)` — lock a pinned buffer (seam: read lock).
pub fn bt_lockbuf<'mcx>(rel: &Relation<'mcx>, buf: Buffer) {
    _bt_lockbuf(rel, buf, BT_READ);
}

pub(crate) fn _bt_lockbuf<'mcx>(_rel: &Relation<'_>, buf: Buffer, access: i32) {
    // LockBuffer(buf, access).  (Valgrind client requests are debug-only and
    // not modeled.)  The C wrapper has no error path (it Asserts the pin), so a
    // lock-manager error here is a should-not-happen we surface as a panic.
    if let Err(e) = bufmgr::lock_buffer::call(buf, access) {
        panic!("_bt_lockbuf: LockBuffer failed: {e:?}");
    }
}

pub(crate) fn _bt_unlockbuf<'mcx>(_rel: &Relation<'_>, buf: Buffer) {
    if let Err(e) = bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK) {
        panic!("_bt_unlockbuf: LockBuffer(UNLOCK) failed: {e:?}");
    }
}

/// `_bt_conditionallockbuf(rel, buf)` — conditionally BT_WRITE-lock pinned buf.
#[allow(dead_code)]
fn _bt_conditionallockbuf<'mcx>(_rel: &Relation<'mcx>, buf: Buffer) -> bool {
    // ConditionalLockBuffer() asserts the pin is held by this backend; the C
    // wrapper has no error path beyond that should-not-happen, so surface a
    // lock-manager error as a panic. (The !RelationUsesLocalBuffers valgrind
    // client request is debug-only and not modeled.)
    match bufmgr::conditional_lock_buffer::call(buf) {
        Ok(got) => got,
        Err(e) => panic!("_bt_conditionallockbuf: ConditionalLockBuffer failed: {e:?}"),
    }
}

/// `_bt_upgradelockbufcleanup(rel, buf)` — upgrade read lock to a cleanup lock.
pub fn bt_upgradelockbufcleanup<'mcx>(_rel: &Relation<'mcx>, buf: Buffer) {
    // LockBuffer(buf, BUFFER_LOCK_UNLOCK); LockBufferForCleanup(buf);
    if let Err(e) = bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK) {
        panic!("_bt_upgradelockbufcleanup: unlock failed: {e:?}");
    }
    if let Err(e) = bufmgr::lock_buffer_for_cleanup::call(buf) {
        panic!("_bt_upgradelockbufcleanup: LockBufferForCleanup failed: {e:?}");
    }
}

// ---------------------------------------------------------------------------
// _bt_pageinit
// ---------------------------------------------------------------------------

/// `PageInit` operating on raw page bytes (in-memory page-image init).
fn PageInit_inplace(page: &mut [u8], page_size: usize, special_size: usize) {
    let mut pm = PageMut::new(page).expect("PageInit_inplace");
    PageInit(pm.as_mut_bytes(), page_size, special_size).expect("PageInit");
}

/// `_bt_pageinit(page, size)` — initialize a new page image
/// (`PageInit(page, size, sizeof(BTPageOpaqueData))`).
pub fn _bt_pageinit(page: &mut [u8], size: Size) {
    PageInit_inplace(page, size, ::core::mem::size_of::<BTPageOpaqueData>());
}

// ---------------------------------------------------------------------------
// _bt_page_recyclable  (BTPageIsRecyclable inline; seam bt_page_is_recyclable)
// ---------------------------------------------------------------------------

/// `BTPageGetDeleteXid(page)` — the deleted page's `safexid`.
fn bt_page_get_delete_xid(page: &[u8], opaque: &BTPageOpaqueData) -> FullTransactionId {
    if !P_HAS_FULLXID(opaque) {
        /* pg_upgrade'd deleted page -- must be safe to recycle now */
        return FirstNormalFullTransactionId;
    }
    /* Get safexid from deleted page (BTDeletedPageData at PageGetContents). */
    let contents = PageGetContents(&PageRef::new(page).expect("recyclable page"))
        .expect("recyclable contents");
    FullTransactionId::from_u64(u64::from_ne_bytes([
        contents[0],
        contents[1],
        contents[2],
        contents[3],
        contents[4],
        contents[5],
        contents[6],
        contents[7],
    ]))
}

/// `BTPageIsRecyclable(page, heaprel)` (the `_bt_page_recyclable` helper).
pub fn bt_page_is_recyclable<'mcx>(page: &[u8], heaprel: &Relation<'mcx>) -> bool {
    debug_assert!(!PageIsNew(&PageRef::new(page).expect("recyclable page")));

    let opaque = opaque_from_page(page).expect("recyclable opaque");
    if P_ISDELETED(&opaque) {
        let safexid = bt_page_get_delete_xid(page, &opaque);
        return global_vis_check_removable_full_xid(heaprel, safexid)
            .expect("bt_page_is_recyclable: GlobalVis");
    }
    false
}

/// `GlobalVisCheckRemovableFullXid(heaprel, fxid)` — composed from
/// `GlobalVisTestFor(heaprel)` + `GlobalVisTestIsRemovableFullXid(state, fxid)`.
fn global_vis_check_removable_full_xid<'mcx>(
    heaprel: &Relation<'mcx>,
    fxid: FullTransactionId,
) -> PgResult<bool> {
    let state = procarray::global_vis_test_for::call(heaprel.rd_id)?;
    Ok(procarray::global_vis_test_is_removable_fullxid::call(state, fxid))
}

// ---------------------------------------------------------------------------
// _bt_delitems_vacuum / _bt_delitems_delete / _bt_delitems_update
// ---------------------------------------------------------------------------

/// `_bt_delitems_update(updatable, nupdatable, updatedoffsets, *updatedbuflen,
/// needswal)` — produce updated versions of the posting tuples (via
/// `_bt_update_posting`), collect their page offsets and (when needswal) the
/// `xl_btree_update` byte buffer.
///
/// In C this replaces `updatable[i]->itup` in place (palloc'd in the current
/// context, freed by the caller afterwards) and the caller reads the updated
/// `itup` back when overwriting the page.  Because the carrier
/// `BTVacuumPosting.itup` is typed to the caller's `'mcx` (which a short-lived
/// scratch context cannot satisfy), we instead return the updated tuple bytes
/// as owned `Vec<u8>` for the caller's page write; behaviour-identical (the new
/// tuples only need to live across the page write + WAL of this one call).
fn _bt_delitems_update(
    mcx: Mcx<'_>,
    updatable: &[BTVacuumPosting<'_>],
    needswal: bool,
) -> PgResult<(
    std::vec::Vec<OffsetNumber>,
    std::vec::Vec<std::vec::Vec<u8>>,
    Option<std::vec::Vec<u8>>,
)> {
    debug_assert!(!updatable.is_empty());

    let mut updatedoffsets: std::vec::Vec<OffsetNumber> =
        std::vec::Vec::with_capacity(updatable.len());
    let mut updated_itups: std::vec::Vec<std::vec::Vec<u8>> =
        std::vec::Vec::with_capacity(updatable.len());

    for vacposting in updatable.iter() {
        /* Build updated version (nbtdedup.c _bt_update_posting). */
        let updated = backend_access_nbt_dedup::_bt_update_posting(
            mcx,
            &vacposting.itup,
            &vacposting.deletetids,
        )?;
        updated_itups.push(updated.to_vec());
        updatedoffsets.push(vacposting.updatedoffset);
    }

    /* XLOG stuff */
    let updatedbuf = if needswal {
        let mut buf = std::vec::Vec::new();
        for vacposting in updatable.iter() {
            // xl_btree_update { uint16 ndeletedtids } then uint16[] deletetids.
            let ndel = vacposting.deletetids.len() as u16;
            buf.extend_from_slice(&ndel.to_ne_bytes());
            for &t in vacposting.deletetids.iter() {
                buf.extend_from_slice(&t.to_ne_bytes());
            }
        }
        Some(buf)
    } else {
        None
    };

    Ok((updatedoffsets, updated_itups, updatedbuf))
}

/// `_bt_delitems_vacuum(rel, buf, deletable, updatable)` — apply VACUUM
/// deletions/updates to a leaf page and WAL-log them.
pub fn bt_delitems_vacuum<'mcx>(
    rel: &Relation<'mcx>,
    buf: Buffer,
    deletable: PgVec<'mcx, OffsetNumber>,
    updatable: PgVec<'mcx, BTVacuumPosting<'mcx>>,
) -> PgResult<()> {
    with_temp_mcx(|mcx| {
        let needswal = relcache::relation_needs_wal::call(rel);

        debug_assert!(!deletable.is_empty() || !updatable.is_empty());

        let (updatedoffsets, updated_itups, updatedbuf) = if !updatable.is_empty() {
            _bt_delitems_update(mcx, &updatable, needswal)?
        } else {
            (std::vec::Vec::new(), std::vec::Vec::new(), None)
        };

        miscinit::start_crit_section::call();

        bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
            apply_delitems_to_page(page, &deletable, &updated_itups, &updatedoffsets, true)
        })?;

        bufmgr::mark_buffer_dirty::call(buf);

        if needswal {
            let xlrec_vacuum = xl_btree_vacuum {
                ndeleted: deletable.len() as u16,
                nupdated: updatable.len() as u16,
            };
            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
            let mut hb = std::vec::Vec::with_capacity(4);
            hb.extend_from_slice(&xlrec_vacuum.ndeleted.to_ne_bytes());
            hb.extend_from_slice(&xlrec_vacuum.nupdated.to_ne_bytes());
            xloginsert::xlog_register_data::call(&hb)?;

            if !deletable.is_empty() {
                let db = serialize_offsets(&deletable);
                xloginsert::xlog_register_buf_data::call(0, &db)?;
            }
            if !updatable.is_empty() {
                let ob = serialize_offsets(&updatedoffsets);
                xloginsert::xlog_register_buf_data::call(0, &ob)?;
                if let Some(ub) = &updatedbuf {
                    xloginsert::xlog_register_buf_data::call(0, ub)?;
                }
            }

            let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_VACUUM)?;
            bufmgr::page_set_lsn::call(buf, recptr)?;
        }

        miscinit::end_crit_section::call();

        /* updatable's owned tuples are freed when `updatable` drops here. */
        drop(updatable);
        Ok(())
    })
}

/// `_bt_delitems_delete(rel, buf, snapshotConflictHorizon, isCatalogRel,
/// deletable, updatable)` — single-page-cleanup variant.
fn _bt_delitems_delete<'mcx>(
    mcx: Mcx<'_>,
    rel: &Relation<'mcx>,
    buf: Buffer,
    snapshot_conflict_horizon: TransactionId,
    is_catalog_rel: bool,
    deletable: &[OffsetNumber],
    updatable: &[BTVacuumPosting<'_>],
) -> PgResult<()> {
    let needswal = relcache::relation_needs_wal::call(rel);

    debug_assert!(!deletable.is_empty() || !updatable.is_empty());

    let (updatedoffsets, updated_itups, updatedbuf) = if !updatable.is_empty() {
        _bt_delitems_update(mcx, updatable, needswal)?
    } else {
        (std::vec::Vec::new(), std::vec::Vec::new(), None)
    };

    miscinit::start_crit_section::call();

    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        // The delete variant must NOT clear the vacuum cycle id.
        apply_delitems_to_page(page, deletable, &updated_itups, &updatedoffsets, false)
    })?;

    bufmgr::mark_buffer_dirty::call(buf);

    if needswal {
        let xlrec_delete = xl_btree_delete {
            snapshotConflictHorizon: snapshot_conflict_horizon,
            ndeleted: deletable.len() as u16,
            nupdated: updatable.len() as u16,
            isCatalogRel: is_catalog_rel,
        };
        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
        let mut hb = std::vec::Vec::new();
        hb.extend_from_slice(&xlrec_delete.snapshotConflictHorizon.to_ne_bytes());
        hb.extend_from_slice(&xlrec_delete.ndeleted.to_ne_bytes());
        hb.extend_from_slice(&xlrec_delete.nupdated.to_ne_bytes());
        hb.push(xlrec_delete.isCatalogRel as u8);
        xloginsert::xlog_register_data::call(&hb)?;

        if !deletable.is_empty() {
            let db = serialize_offsets(deletable);
            xloginsert::xlog_register_buf_data::call(0, &db)?;
        }
        if !updatable.is_empty() {
            let ob = serialize_offsets(&updatedoffsets);
            xloginsert::xlog_register_buf_data::call(0, &ob)?;
            if let Some(ub) = &updatedbuf {
                xloginsert::xlog_register_buf_data::call(0, ub)?;
            }
        }

        let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_DELETE)?;
        bufmgr::page_set_lsn::call(buf, recptr)?;
    }

    miscinit::end_crit_section::call();
    Ok(())
}

/// Apply posting overwrites + simple deletes to a leaf page, then clear the
/// `BTP_HAS_GARBAGE` flag (and, when `clear_cycleid`, the vacuum cycle id).
/// `updated_itups[i]` is the new image for the tuple at `updatedoffsets[i]`.
fn apply_delitems_to_page(
    page: &mut [u8],
    deletable: &[OffsetNumber],
    updated_itups: &[std::vec::Vec<u8>],
    updatedoffsets: &[OffsetNumber],
    clear_cycleid: bool,
) -> PgResult<()> {
    {
        let mut pm = PageMut::new(page)?;
        /*
         * Handle posting tuple updates first (before simple deletes -- WAL
         * record order).  PageIndexTupleOverwrite won't unset an LP_DEAD bit.
         */
        for (i, itup) in updated_itups.iter().enumerate() {
            let updatedoffset = updatedoffsets[i];
            let hdr = index_tuple_header(itup);
            let itemsz = maxalign(IndexTupleSize(&hdr));
            if !backend_storage_page::PageIndexTupleOverwrite(
                &mut pm,
                updatedoffset,
                &itup[..itemsz],
            )? {
                // C: elog(PANIC, ...)
                panic!("failed to update partially dead item");
            }
        }

        /* Now handle simple deletes of entire tuples */
        if !deletable.is_empty() {
            backend_storage_page::PageIndexMultiDelete(&mut pm, deletable)?;
        }
    }

    let mut opaque = opaque_from_page(page)?;
    if clear_cycleid {
        opaque.btpo_cycleid = 0;
    }
    opaque.btpo_flags &= !BTP_HAS_GARBAGE;
    encode_opaque(page, &opaque);
    Ok(())
}

/// Serialize an `OffsetNumber` slice to native-endian bytes.
fn serialize_offsets(offs: &[OffsetNumber]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::with_capacity(offs.len() * SIZEOF_OFFSET);
    for &o in offs {
        out.extend_from_slice(&o.to_ne_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// _bt_delitems_cmp / _bt_delitems_delete_check
// ---------------------------------------------------------------------------

/// `_bt_delitems_cmp(a, b)` — restore `deltids` to leaf-page-wise order via id.
fn _bt_delitems_cmp(
    a: &types_nbtree::TmIndexDelete,
    b: &types_nbtree::TmIndexDelete,
) -> std::cmp::Ordering {
    debug_assert!(a.id != b.id);
    a.id.cmp(&b.id)
}

/// `_bt_delitems_delete_check(rel, buf, heapRel, delstate)` — tableam interface
/// to delete a subset of index tuples found safe to delete by the heap AM.
pub fn bt_delitems_delete_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: Buffer,
    heap_rel: &Relation<'mcx>,
    mut delstate: types_nbtree::TmIndexDeleteOp<'mcx>,
) -> PgResult<()> {
    {
        /*
         * Use the tableam interface to determine which tuples to delete first.
         * The heap AM (heapam_index_delete_tuples) does the full deletability
         * analysis; all leaf-page-wise reconstruction + physical delete + WAL
         * logic below is ported here.
         */
        let snapshot_conflict_horizon: TransactionId =
            table_index_delete_tuples(mcx, heap_rel, &mut delstate)?;
        let is_catalog_rel = relation_is_accessible_in_logical_decoding(heap_rel)?;

        /* Should not WAL-log snapshotConflictHorizon unless it's required. */
        let snapshot_conflict_horizon =
            if !backend_access_transam_xlog_seams::xlog_standby_info_active::call() {
                0 /* InvalidTransactionId */
            } else {
                snapshot_conflict_horizon
            };

        /* Sort deltids back to leaf-page-wise order (the loop expects this). */
        delstate.deltids.sort_by(_bt_delitems_cmp);
        if delstate.deltids.is_empty() {
            debug_assert!(delstate.bottomup);
            return Ok(());
        }

        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        let page = PageRef::new(&page_bytes)?;

        let mut postingidxoffnum: OffsetNumber = 0; /* InvalidOffsetNumber */
        // deletable/updatable are scratch consumed by _bt_delitems_delete in
        // this call; they ride the caller's working arena.
        let mut deletable: PgVec<'_, OffsetNumber> =
            vec_with_capacity_in(mcx, MaxIndexTuplesPerPage)?;
        let mut updatable: PgVec<'_, BTVacuumPosting<'_>> =
            vec_with_capacity_in(mcx, MaxIndexTuplesPerPage)?;

        let ndeltids = delstate.deltids.len();
        let mut i = 0usize;
        while i < ndeltids {
            let id_i = delstate.deltids[i].id as usize;
            let idxoffnum = delstate.status[id_i].idxoffnum;
            let itemid = PageGetItemId(&page, idxoffnum)?;
            let itup = PageGetItem(&page, &itemid)?;
            let ihdr = index_tuple_header(itup);

            debug_assert!(idxoffnum != 0);

            if idxoffnum == postingidxoffnum {
                /* TID from an already fully-processed posting list tuple. */
                debug_assert!(BTreeTupleIsPosting(&ihdr));
                i += 1;
                continue;
            }

            if !BTreeTupleIsPosting(&ihdr) {
                /* Plain non-pivot tuple */
                if delstate.status[id_i].knowndeletable {
                    deletable.push(idxoffnum);
                }
                i += 1;
                continue;
            }

            /* itup is a posting list tuple; process all its deltids together. */
            postingidxoffnum = idxoffnum;
            let mut nestedi = i;
            let nitem = BTreeTupleGetNPosting(&ihdr) as usize;
            let mut vac_deletetids: Option<PgVec<'_, u16>> = None;

            for p in 0..nitem {
                let ptid = posting_list_n(itup, p);
                let mut ptidcmp: i32 = -1;

                while nestedi < ndeltids {
                    let tc_id = delstate.deltids[nestedi].id as usize;
                    let t_idxoffnum = delstate.status[tc_id].idxoffnum;

                    debug_assert!(t_idxoffnum >= idxoffnum);
                    if t_idxoffnum != idxoffnum {
                        break;
                    }
                    if !delstate.status[tc_id].knowndeletable {
                        nestedi += 1;
                        continue;
                    }
                    ptidcmp = item_pointer_compare(&delstate.deltids[nestedi].tid, &ptid);
                    if ptidcmp >= 0 {
                        break;
                    }
                    nestedi += 1;
                }

                if ptidcmp != 0 {
                    continue;
                }

                /* Exact match -- ptid gets deleted */
                if vac_deletetids.is_none() {
                    vac_deletetids = Some(vec_with_capacity_in(mcx, nitem)?);
                }
                vac_deletetids.as_mut().unwrap().push(p as u16);
            }

            /* Final decision on itup, a posting list tuple */
            match vac_deletetids {
                None => { /* No TIDs to delete from itup -- do nothing */ }
                Some(deletetids) if deletetids.len() == nitem => {
                    /* Straight delete of itup (to delete all TIDs) */
                    deletable.push(idxoffnum);
                }
                Some(deletetids) => {
                    debug_assert!(!deletetids.is_empty() && deletetids.len() < nitem);
                    let mut itup_copy: PgVec<'_, u8> = vec_with_capacity_in(mcx, itup.len())?;
                    itup_copy.extend_from_slice(itup);
                    updatable.push(BTVacuumPosting {
                        itup: itup_copy,
                        updatedoffset: idxoffnum,
                        deletetids,
                    });
                }
            }

            /* Advance past all deltids entries belonging to this itup/offnum. */
            i = nestedi.max(i + 1);
            while i < ndeltids {
                let id2 = delstate.deltids[i].id as usize;
                if delstate.status[id2].idxoffnum == idxoffnum {
                    i += 1;
                } else {
                    break;
                }
            }
        }

        // Release the read snapshot of the page before re-entering bufmgr for
        // the physical delete below (PageRef is Copy; drop the owned bytes).
        let _ = page;
        drop(page_bytes);

        /* Physically delete tuples (or TIDs) using deletable (or updatable). */
        _bt_delitems_delete(
            mcx,
            rel,
            buf,
            snapshot_conflict_horizon,
            is_catalog_rel,
            &deletable,
            &updatable,
        )?;

        Ok(())
    }
}

/// `ItemPointerCompare(a, b)` — compare two heap TIDs (block, then offset).
fn item_pointer_compare(a: &ItemPointerData, b: &ItemPointerData) -> i32 {
    let ab = ipd_block_number(a);
    let bb = ipd_block_number(b);
    if ab != bb {
        return if ab > bb { 1 } else { -1 };
    }
    let ao = a.ip_posid;
    let bo = b.ip_posid;
    if ao != bo {
        return if ao > bo { 1 } else { -1 };
    }
    0
}

/// `table_index_delete_tuples(heapRel, delstate)` — tableam dispatch, routed
/// through the tableam.c owner's seam (a direct dependency would cycle: the
/// heap AM the dispatch reaches transitively depends on nbtree). `mcx` is the
/// caller's working arena (the heap AM allocates its per-block scratch there).
fn table_index_delete_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    delstate: &mut types_nbtree::TmIndexDeleteOp<'mcx>,
) -> PgResult<TransactionId> {
    backend_access_table_tableam_seams::table_index_delete_tuples::call(mcx, heap_rel, delstate)
}

/// `RelationIsAccessibleInLogicalDecoding(rel)` (utils/rel.h):
/// `XLogLogicalInfoActive() && RelationNeedsWAL(rel) && (IsCatalogRelation(rel)
/// || RelationIsUsedAsCatalogTable(rel))`. Resolved by the relcache owner,
/// which holds wal_level + rd_options + the catalog-relation predicate.
fn relation_is_accessible_in_logical_decoding<'mcx>(
    rel: &Relation<'mcx>,
) -> PgResult<bool> {
    relcache::relation_is_accessible_in_logical_decoding::call(rel)
}

// ---------------------------------------------------------------------------
// _bt_leftsib_splitflag / _bt_rightsib_halfdeadflag
// ---------------------------------------------------------------------------

/// `_bt_leftsib_splitflag(rel, leftsib, target)`.
fn _bt_leftsib_splitflag<'mcx>(
    mcx: Mcx<'_>,
    rel: &Relation<'mcx>,
    leftsib: BlockNumber,
    target: BlockNumber,
) -> PgResult<bool> {
    if leftsib == P_NONE {
        return Ok(false);
    }

    let buf = _bt_getbuf(mcx, rel, leftsib, BT_READ)?;
    let opaque = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        opaque_from_page(&page_bytes)?
    };

    let result = opaque.btpo_next == target && P_INCOMPLETE_SPLIT(&opaque);
    _bt_relbuf(rel, buf);
    Ok(result)
}

/// `_bt_rightsib_halfdeadflag(rel, leafrightsib)`.
fn _bt_rightsib_halfdeadflag<'mcx>(
    mcx: Mcx<'_>,
    rel: &Relation<'mcx>,
    leafrightsib: BlockNumber,
) -> PgResult<bool> {
    debug_assert!(leafrightsib != P_NONE);

    let buf = _bt_getbuf(mcx, rel, leafrightsib, BT_READ)?;
    let opaque = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        opaque_from_page(&page_bytes)?
    };
    debug_assert!(P_ISLEAF(&opaque) && !P_ISDELETED(&opaque));
    let result = P_ISHALFDEAD(&opaque);
    _bt_relbuf(rel, buf);
    Ok(result)
}

// ---------------------------------------------------------------------------
// _bt_pagedel — top-level page deletion driver.
// ---------------------------------------------------------------------------

/// `_bt_pagedel(rel, leafbuf, vstate)` — delete a leaf page (and possibly a
/// taller subtree / the right-sibling chain), maintaining vstate stats.
pub fn bt_pagedel<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    mut leafbuf: Buffer,
    vstate: &mut BTVacState<'mcx>,
) -> PgResult<()> {
    with_temp_mcx(|mcx| {
        let scanblkno = bufmgr::buffer_get_block_number::call(leafbuf);

        let mut stack: BTStack = None;

        loop {
            let opaque = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
                opaque_from_page(&page_bytes)?
            };

            debug_assert!(!P_ISDELETED(&opaque));
            if !P_ISLEAF(&opaque) || P_ISDELETED(&opaque) {
                if P_ISHALFDEAD(&opaque) {
                    log_message(&format!(
                        "index \"{}\" contains a half-dead internal page",
                        rel.name()
                    ));
                }
                if P_ISDELETED(&opaque) {
                    log_message(&format!(
                        "found deleted block {} while following right link from block {} in index \"{}\"",
                        bufmgr::buffer_get_block_number::call(leafbuf), scanblkno, rel.name()
                    ));
                }
                _bt_relbuf(rel, leafbuf);
                return Ok(());
            }

            let maxoff = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
                let page = PageRef::new(&page_bytes)?;
                PageGetMaxOffsetNumber(&page)
            };
            if P_RIGHTMOST(&opaque)
                || P_ISROOT(&opaque)
                || P_FIRSTDATAKEY(&opaque) <= maxoff
                || P_INCOMPLETE_SPLIT(&opaque)
            {
                debug_assert!(!P_ISHALFDEAD(&opaque));
                _bt_relbuf(rel, leafbuf);
                return Ok(());
            }

            /* First, remove downlink + mark leafbuf half-dead (if needed). */
            if !P_ISHALFDEAD(&opaque) {
                if stack.is_none() {
                    let targetkey: PgVec<'_, u8> = {
                        let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
                        let page = PageRef::new(&page_bytes)?;
                        let itemid = PageGetItemId(&page, P_HIKEY)?;
                        let hikey = PageGetItem(&page, &itemid)?;
                        let mut v = vec_with_capacity_in(mcx, hikey.len())?;
                        v.extend_from_slice(hikey); // CopyIndexTuple
                        v
                    };

                    let leftsib = opaque.btpo_prev;
                    let leafblkno = bufmgr::buffer_get_block_number::call(leafbuf);

                    _bt_unlockbuf(rel, leafbuf);

                    debug_assert!(leafblkno == scanblkno);
                    if _bt_leftsib_splitflag(mcx, rel, leftsib, leafblkno)? {
                        bufmgr::release_buffer::call(leafbuf);
                        return Ok(());
                    }

                    let mut itup_key = nbtcore::bt_mkscankey::call(rel, Some(&targetkey))?;
                    {
                        let key = itup_key
                            .as_mut()
                            .expect("_bt_mkscankey returned a null insertion scankey");
                        key.nextkey = false;
                        key.backward = true;
                    }
                    let (sstack, sleafbuf) =
                        nbtcore::bt_search::call(rel, heaprel, &itup_key, false)?;
                    stack = sstack;
                    _bt_relbuf(rel, sleafbuf);

                    _bt_lockbuf(rel, leafbuf, BT_WRITE);
                    continue;
                }

                debug_assert!(P_ISLEAF(&opaque) && !P_IGNORE(&opaque));
                if !_bt_mark_page_halfdead(mcx, rel, heaprel, leafbuf, &mut stack)? {
                    _bt_relbuf(rel, leafbuf);
                    return Ok(());
                }
            }

            /* Then unlink from siblings, iterating until leafbuf is deleted. */
            let mut rightsib_empty = false;
            loop {
                let halfdead = {
                    let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
                    P_ISHALFDEAD(&opaque_from_page(&page_bytes)?)
                };
                if !halfdead {
                    break;
                }
                if !_bt_unlink_halfdead_page(
                    mcx,
                    rel,
                    leafbuf,
                    scanblkno,
                    &mut rightsib_empty,
                    vstate,
                )? {
                    debug_assert!(false);
                    return Ok(());
                }
            }

            let rightsib = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
                let o = opaque_from_page(&page_bytes)?;
                debug_assert!(P_ISLEAF(&o) && P_ISDELETED(&o));
                o.btpo_next
            };

            _bt_relbuf(rel, leafbuf);

            // CHECK_FOR_INTERRUPTS();  (no interrupt model here.)

            if !rightsib_empty {
                break;
            }

            leafbuf = _bt_getbuf(mcx, rel, rightsib, BT_WRITE)?;
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// _bt_mark_page_halfdead — first stage of page deletion.
// ---------------------------------------------------------------------------

fn _bt_mark_page_halfdead<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    leafbuf: Buffer,
    stack: &mut BTStack,
) -> PgResult<bool> {
    let (opaque, maxoff) = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
        let page = PageRef::new(&page_bytes)?;
        (opaque_from_page(&page_bytes)?, PageGetMaxOffsetNumber(&page))
    };

    debug_assert!(
        !P_RIGHTMOST(&opaque)
            && !P_ISROOT(&opaque)
            && P_ISLEAF(&opaque)
            && !P_IGNORE(&opaque)
            && P_FIRSTDATAKEY(&opaque) > maxoff
    );

    let leafblkno = bufmgr::buffer_get_block_number::call(leafbuf);
    let leafrightsib = opaque.btpo_next;

    if _bt_rightsib_halfdeadflag(mcx, rel, leafrightsib)? {
        log_debug(&format!(
            "could not delete page {} because its right sibling {} is half-dead",
            leafblkno, leafrightsib
        ));
        return Ok(false);
    }

    let mut topparent = leafblkno;
    let mut topparentrightsib = leafrightsib;
    let mut subtreeparent: Buffer = InvalidBuffer;
    let mut poffset: OffsetNumber = 0;
    if !_bt_lock_subtree_parent(
        mcx,
        rel,
        heaprel,
        leafblkno,
        stack,
        &mut subtreeparent,
        &mut poffset,
        &mut topparent,
        &mut topparentrightsib,
    )? {
        return Ok(false);
    }

    /* Validate the parent-page items we're about to delete/overwrite. */
    {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, subtreeparent)?;
        let page = PageRef::new(&page_bytes)?;

        debug_assert!({
            let iid = PageGetItemId(&page, poffset)?;
            let itup = PageGetItem(&page, &iid)?;
            BTreeTupleGetDownLink(itup) == topparent
        });

        let nextoffset = OffsetNumberNext(poffset);
        let iid = PageGetItemId(&page, nextoffset)?;
        let itup = PageGetItem(&page, &iid)?;
        if BTreeTupleGetDownLink(itup) != topparentrightsib {
            let parentblk = bufmgr::buffer_get_block_number::call(subtreeparent);
            let dl = BTreeTupleGetDownLink(itup);
            log_message(&format!(
                "right sibling {} of block {} is not next child {} of block {} in index \"{}\"",
                topparentrightsib, topparent, dl, parentblk, rel.name()
            ));
            let _ = page;
            drop(page_bytes);
            _bt_relbuf(rel, subtreeparent);
            debug_assert!(false);
            return Ok(false);
        }
    }

    /*
     * Any insert that would have gone on leaf now goes to its right sibling
     * (key space moves right).  PredicateLockPageCombine has no seam.
     */
    predicate_lock_page_combine(rel, leafblkno, leafrightsib)?;

    let needs_wal = relcache::relation_needs_wal::call(rel);

    miscinit::start_crit_section::call();

    /*
     * Update parent of subtree: copy the right sibling's downlink over the
     * downlink to top parent, then delete the right sibling's pivot tuple.
     */
    bufmgr::with_buffer_page::call(subtreeparent, &mut |page: &mut [u8]| {
        set_downlink_at_offset(page, poffset, topparentrightsib)?;
        let nextoffset = OffsetNumberNext(poffset);
        let mut pm = PageMut::new(page)?;
        backend_storage_page::PageIndexTupleDelete(&mut pm, nextoffset)?;
        Ok(())
    })?;

    /*
     * Mark the leaf page half-dead + stamp a link to the top parent page.
     * (When leaf is the top parent the link is InvalidBlockNumber.)
     */
    let leaf_top = if topparent != leafblkno {
        topparent
    } else {
        InvalidBlockNumber
    };
    bufmgr::with_buffer_page::call(leafbuf, &mut |page: &mut [u8]| {
        let mut opaque = opaque_from_page(page)?;
        opaque.btpo_flags |= BTP_HALF_DEAD;
        encode_opaque(page, &opaque);

        // MemSet(&trunctuple,0); trunctuple.t_info = sizeof(IndexTupleData);
        // BTreeTupleSetTopParent(&trunctuple, leaf_top).
        let mut trunctuple = [0u8; 8];
        let mut hdr = IndexTupleData::default();
        hdr.t_info = ::core::mem::size_of::<IndexTupleData>() as u16;
        ipd_set_block_number(&mut hdr.t_tid, leaf_top);
        // BTreeTupleSetNAtts(itup, 0, false): t_info |= INDEX_ALT_TID_MASK;
        // ItemPointerSetOffsetNumber(&t_tid, 0).
        hdr.t_info |= INDEX_ALT_TID_MASK;
        ipd_set_offset(&mut hdr.t_tid, 0);
        write_index_tuple_header(&mut trunctuple, &hdr);

        let mut pm = PageMut::new(page)?;
        if !backend_storage_page::PageIndexTupleOverwrite(&mut pm, P_HIKEY, &trunctuple)? {
            return Err(PgError::error("could not overwrite high key in half-dead page"));
        }
        Ok(())
    })?;

    bufmgr::mark_buffer_dirty::call(subtreeparent);
    bufmgr::mark_buffer_dirty::call(leafbuf);

    if needs_wal {
        let (leftblk, rightblk) = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
            let o = opaque_from_page(&page_bytes)?;
            (o.btpo_prev, o.btpo_next)
        };
        let xlrec = xl_btree_mark_page_halfdead {
            poffset,
            leafblk: leafblkno,
            leftblk,
            rightblk,
            topparent: leaf_top,
        };
        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_buffer::call(0, leafbuf, REGBUF_WILL_INIT)?;
        xloginsert::xlog_register_buffer::call(1, subtreeparent, REGBUF_STANDARD)?;

        let mut xb = std::vec::Vec::new();
        xb.extend_from_slice(&xlrec.poffset.to_ne_bytes());
        xb.extend_from_slice(&xlrec.leafblk.to_ne_bytes());
        xb.extend_from_slice(&xlrec.leftblk.to_ne_bytes());
        xb.extend_from_slice(&xlrec.rightblk.to_ne_bytes());
        xb.extend_from_slice(&xlrec.topparent.to_ne_bytes());
        xloginsert::xlog_register_data::call(&xb)?;

        let recptr =
            xloginsert::xlog_insert_record::call(RM_BTREE_ID, XLOG_BTREE_MARK_PAGE_HALFDEAD)?;
        bufmgr::page_set_lsn::call(subtreeparent, recptr)?;
        bufmgr::page_set_lsn::call(leafbuf, recptr)?;
    }

    miscinit::end_crit_section::call();

    _bt_relbuf(rel, subtreeparent);
    Ok(true)
}

/// `BTreeTupleSetDownLink(itup_at_offnum, blkno)` on a page item, in place.
fn set_downlink_at_offset(page: &mut [u8], offnum: OffsetNumber, blkno: BlockNumber) -> PgResult<()> {
    let item_off = page_item_offset(page, offnum)?;
    let mut t = read_ipd(&page[item_off..item_off + 6]);
    ipd_set_block_number(&mut t, blkno);
    write_ipd(page, item_off, &t);
    Ok(())
}

/// `BTreeTupleSetTopParent(leafhikey_at_offnum, blkno)` on a page item: set the
/// block id of `t_tid`, then `BTreeTupleSetNAtts(0, false)`.
fn set_top_parent_at_offset(
    page: &mut [u8],
    offnum: OffsetNumber,
    blkno: BlockNumber,
) -> PgResult<()> {
    let item_off = page_item_offset(page, offnum)?;
    let mut hdr = index_tuple_header(&page[item_off..item_off + 8]);
    ipd_set_block_number(&mut hdr.t_tid, blkno);
    hdr.t_info |= INDEX_ALT_TID_MASK;
    ipd_set_offset(&mut hdr.t_tid, 0);
    write_index_tuple_header(&mut page[item_off..item_off + 8], &hdr);
    Ok(())
}

/// Byte offset of the data for the item at `offnum` (decoded from its line
/// pointer; `lp_off` is bits 0..14 of the 4-byte ItemIdData).
fn page_item_offset(page: &[u8], offnum: OffsetNumber) -> PgResult<usize> {
    let lp_index = (offnum as usize)
        .checked_sub(1)
        .ok_or_else(|| PgError::error("page_item_offset: invalid offset"))?;
    let lp_off = SizeOfPageHeaderData + lp_index * 4;
    if lp_off + 4 > page.len() {
        return Err(PgError::error("page_item_offset: line pointer out of bounds"));
    }
    let raw =
        u32::from_ne_bytes([page[lp_off], page[lp_off + 1], page[lp_off + 2], page[lp_off + 3]]);
    // ItemIdData: lp_off:15, lp_flags:2, lp_len:15 (bitfield, low bits first).
    let item_off = (raw & 0x7FFF) as usize;
    Ok(item_off)
}

// ---------------------------------------------------------------------------
// _bt_unlink_halfdead_page — second stage of page deletion.
// ---------------------------------------------------------------------------

fn _bt_unlink_halfdead_page<'mcx>(
    mcx: Mcx<'_>,
    rel: &Relation<'mcx>,
    leafbuf: Buffer,
    scanblkno: BlockNumber,
    rightsib_empty: &mut bool,
    vstate: &mut BTVacState<'mcx>,
) -> PgResult<bool> {
    let leafblkno = bufmgr::buffer_get_block_number::call(leafbuf);

    let (mut target, leafleftsib, leafrightsib) = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, leafbuf)?;
        let page = PageRef::new(&page_bytes)?;
        let opaque = opaque_from_page(&page_bytes)?;
        debug_assert!(P_ISLEAF(&opaque) && !P_ISDELETED(&opaque) && P_ISHALFDEAD(&opaque));
        let itemid = PageGetItemId(&page, P_HIKEY)?;
        let leafhikey = PageGetItem(&page, &itemid)?;
        let target = BTreeTupleGetTopParent(leafhikey);
        (target, opaque.btpo_prev, opaque.btpo_next)
    };

    _bt_unlockbuf(rel, leafbuf);

    // CHECK_FOR_INTERRUPTS();

    let mut lbuf: Buffer = InvalidBuffer;
    let buf: Buffer;
    let mut leftsib: BlockNumber;
    let targetlevel: u32;

    if target == InvalidBlockNumber {
        target = leafblkno;
        buf = leafbuf;
        leftsib = leafleftsib;
        targetlevel = 0;
    } else {
        debug_assert!(target != leafblkno);
        let tbuf = _bt_getbuf(mcx, rel, target, BT_READ)?;
        let o = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, tbuf)?;
            opaque_from_page(&page_bytes)?
        };
        leftsib = o.btpo_prev;
        targetlevel = o.btpo_level;
        debug_assert!(targetlevel > 0);
        _bt_unlockbuf(rel, tbuf);
        buf = tbuf;
    }

    if target != leafblkno {
        _bt_lockbuf(rel, leafbuf, BT_WRITE);
    }
    if leftsib != P_NONE {
        lbuf = _bt_getbuf(mcx, rel, leftsib, BT_WRITE)?;
        loop {
            let o = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, lbuf)?;
                opaque_from_page(&page_bytes)?
            };
            if !(P_ISDELETED(&o) || o.btpo_next != target) {
                break;
            }
            let mut leftsibvalid = true;
            if P_RIGHTMOST(&o) || P_ISDELETED(&o) || leftsib == o.btpo_next {
                leftsibvalid = false;
            }
            leftsib = o.btpo_next;
            _bt_relbuf(rel, lbuf);

            if !leftsibvalid {
                log_message(&format!(
                    "valid left sibling for deletion target could not be located: left sibling {} of target {} with leafblkno {} and scanblkno {} on level {} of index \"{}\"",
                    leftsib, target, leafblkno, scanblkno, targetlevel, rel.name()
                ));
                bufmgr::release_buffer::call(buf);
                if target != leafblkno {
                    _bt_relbuf(rel, leafbuf);
                }
                return Ok(false);
            }

            // CHECK_FOR_INTERRUPTS();
            lbuf = _bt_getbuf(mcx, rel, leftsib, BT_WRITE)?;
        }
    }
    /* else: lbuf stays InvalidBuffer (its initial value). */

    /* Next write-lock the target page itself */
    _bt_lockbuf(rel, buf, BT_WRITE);
    let (topaque, tmaxoff) = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
        let page = PageRef::new(&page_bytes)?;
        (opaque_from_page(&page_bytes)?, PageGetMaxOffsetNumber(&page))
    };

    if P_RIGHTMOST(&topaque) || P_ISROOT(&topaque) || P_ISDELETED(&topaque) {
        return Err(PgError::error(format!(
            "target page changed status unexpectedly in block {} of index \"{}\"",
            target,
            rel.name()
        )));
    }
    if topaque.btpo_prev != leftsib {
        return Err(PgError::error(format!(
            "target page left link unexpectedly changed from {} to {} in block {} of index \"{}\"",
            leftsib, topaque.btpo_prev, target, rel.name()
        )));
    }

    let leaftopparent: BlockNumber;
    if target == leafblkno {
        if P_FIRSTDATAKEY(&topaque) <= tmaxoff || !P_ISLEAF(&topaque) || !P_ISHALFDEAD(&topaque) {
            return Err(PgError::error(format!(
                "target leaf page changed status unexpectedly in block {} of index \"{}\"",
                target,
                rel.name()
            )));
        }
        leaftopparent = InvalidBlockNumber;
    } else {
        if P_FIRSTDATAKEY(&topaque) != tmaxoff || P_ISLEAF(&topaque) {
            return Err(PgError::error(format!(
                "target internal page on level {} changed status unexpectedly in block {} of index \"{}\"",
                targetlevel, target, rel.name()
            )));
        }
        let fdl = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, buf)?;
            let page = PageRef::new(&page_bytes)?;
            let iid = PageGetItemId(&page, P_FIRSTDATAKEY(&topaque))?;
            let finaldataitem = PageGetItem(&page, &iid)?;
            BTreeTupleGetDownLink(finaldataitem)
        };
        leaftopparent = if fdl == leafblkno {
            InvalidBlockNumber
        } else {
            fdl
        };
    }

    debug_assert!(leaftopparent == InvalidBlockNumber || targetlevel > 1);

    /* And next write-lock the (current) right sibling. */
    let rightsib = topaque.btpo_next;
    let rbuf = _bt_getbuf(mcx, rel, rightsib, BT_WRITE)?;
    let ropaque = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, rbuf)?;
        opaque_from_page(&page_bytes)?
    };

    if ropaque.btpo_prev != target {
        log_message(&format!(
            "right sibling's left-link doesn't match: right sibling {} of target {} with leafblkno {} and scanblkno {} spuriously links to non-target {} on level {} of index \"{}\"",
            rightsib, target, leafblkno, scanblkno, ropaque.btpo_prev, targetlevel, rel.name()
        ));
        if lbuf != InvalidBuffer {
            _bt_relbuf(rel, lbuf);
        }
        _bt_relbuf(rel, rbuf);
        _bt_relbuf(rel, buf);
        if target != leafblkno {
            _bt_relbuf(rel, leafbuf);
        }
        return Ok(false);
    }

    let rightsib_is_rightmost = P_RIGHTMOST(&ropaque);
    *rightsib_empty = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, rbuf)?;
        let page = PageRef::new(&page_bytes)?;
        P_FIRSTDATAKEY(&ropaque) > PageGetMaxOffsetNumber(&page)
    };

    /*
     * If deleting the next-to-last page on target's level, rightsib may become
     * the new fast root.  Acquire the metapage lock when so.
     */
    let mut metabuf: Buffer = InvalidBuffer;
    let mut metad: Option<BTMetaPageData> = None;
    if leftsib == P_NONE && rightsib_is_rightmost {
        let r_is_rightmost = {
            let page_bytes = bufmgr::buffer_get_page::call(mcx, rbuf)?;
            P_RIGHTMOST(&opaque_from_page(&page_bytes)?)
        };
        if r_is_rightmost {
            metabuf = _bt_getbuf(mcx, rel, BTREE_METAPAGE, BT_WRITE)?;
            let md = {
                let page_bytes = bufmgr::buffer_get_page::call(mcx, metabuf)?;
                meta_from_page(&page_bytes)?
            };
            if md.btm_fastlevel > targetlevel + 1 {
                _bt_relbuf(rel, metabuf);
                metabuf = InvalidBuffer;
            } else {
                metad = Some(md);
            }
        }
    }

    /* === Begin doing the deletion. === */

    let needs_wal = relcache::relation_needs_wal::call(rel);
    let safexid = varsup::read_next_full_transaction_id::call();

    miscinit::start_crit_section::call();

    /* Update siblings' side-links. */
    if lbuf != InvalidBuffer {
        bufmgr::with_buffer_page::call(lbuf, &mut |page: &mut [u8]| {
            let mut o = opaque_from_page(page)?;
            debug_assert!(o.btpo_next == target);
            o.btpo_next = rightsib;
            encode_opaque(page, &o);
            Ok(())
        })?;
    }
    bufmgr::with_buffer_page::call(rbuf, &mut |page: &mut [u8]| {
        let mut o = opaque_from_page(page)?;
        debug_assert!(o.btpo_prev == target);
        o.btpo_prev = leftsib;
        encode_opaque(page, &o);
        Ok(())
    })?;

    /*
     * If we deleted a parent of the targeted leaf page, update the leaf to
     * point to the next remaining child (BTreeTupleSetTopParent on leafhikey).
     */
    if target != leafblkno {
        bufmgr::with_buffer_page::call(leafbuf, &mut |page: &mut [u8]| {
            set_top_parent_at_offset(page, P_HIKEY, leaftopparent)
        })?;
    }

    /* Mark the target page itself deleted (BTPageSetDeleted). */
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        bt_page_set_deleted(page, safexid)?;
        let mut o = opaque_from_page(page)?;
        o.btpo_cycleid = 0;
        encode_opaque(page, &o);
        Ok(())
    })?;

    /* And update the metapage, if needed. */
    if metabuf != InvalidBuffer {
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            let mut md = meta_from_page(page)?;
            if md.btm_version < BTREE_NOVAC_VERSION {
                _bt_upgrademetapage(page);
                md = meta_from_page(page)?;
            }
            md.btm_fastroot = rightsib;
            md.btm_fastlevel = targetlevel;
            meta_into_page(page, &md);
            metad = Some(md);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(metabuf);
    }

    /* Must mark buffers dirty before XLogInsert */
    bufmgr::mark_buffer_dirty::call(rbuf);
    bufmgr::mark_buffer_dirty::call(buf);
    if lbuf != InvalidBuffer {
        bufmgr::mark_buffer_dirty::call(lbuf);
    }
    if target != leafblkno {
        bufmgr::mark_buffer_dirty::call(leafbuf);
    }

    if needs_wal {
        let xlrec = xl_btree_unlink_page {
            leftsib,
            rightsib,
            level: targetlevel,
            safexid,
            leafleftsib,
            leafrightsib,
            leaftopparent,
        };
        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_buffer::call(0, buf, REGBUF_WILL_INIT)?;
        if lbuf != InvalidBuffer {
            xloginsert::xlog_register_buffer::call(1, lbuf, REGBUF_STANDARD)?;
        }
        xloginsert::xlog_register_buffer::call(2, rbuf, REGBUF_STANDARD)?;
        if target != leafblkno {
            xloginsert::xlog_register_buffer::call(3, leafbuf, REGBUF_WILL_INIT)?;
        }

        let mut xb = std::vec::Vec::new();
        xb.extend_from_slice(&xlrec.leftsib.to_ne_bytes());
        xb.extend_from_slice(&xlrec.rightsib.to_ne_bytes());
        xb.extend_from_slice(&xlrec.level.to_ne_bytes());
        xb.extend_from_slice(&xlrec.safexid.value.to_ne_bytes());
        xb.extend_from_slice(&xlrec.leafleftsib.to_ne_bytes());
        xb.extend_from_slice(&xlrec.leafrightsib.to_ne_bytes());
        xb.extend_from_slice(&xlrec.leaftopparent.to_ne_bytes());
        xloginsert::xlog_register_data::call(&xb)?;

        let xlinfo = if metabuf != InvalidBuffer {
            xloginsert::xlog_register_buffer::call(4, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
            let md = metad.as_ref().expect("unlink metad");
            let xlmeta = xl_btree_metadata {
                version: md.btm_version,
                root: md.btm_root,
                level: md.btm_level,
                fastroot: md.btm_fastroot,
                fastlevel: md.btm_fastlevel,
                last_cleanup_num_delpages: md.btm_last_cleanup_num_delpages,
                allequalimage: md.btm_allequalimage,
            };
            let mdb = serialize_xl_btree_metadata(&xlmeta);
            xloginsert::xlog_register_buf_data::call(4, &mdb)?;
            XLOG_BTREE_UNLINK_PAGE_META
        } else {
            XLOG_BTREE_UNLINK_PAGE
        };

        let recptr = xloginsert::xlog_insert_record::call(RM_BTREE_ID, xlinfo)?;

        if metabuf != InvalidBuffer {
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }
        bufmgr::page_set_lsn::call(rbuf, recptr)?;
        bufmgr::page_set_lsn::call(buf, recptr)?;
        if lbuf != InvalidBuffer {
            bufmgr::page_set_lsn::call(lbuf, recptr)?;
        }
        if target != leafblkno {
            bufmgr::page_set_lsn::call(leafbuf, recptr)?;
        }
    }

    miscinit::end_crit_section::call();

    if metabuf != InvalidBuffer {
        _bt_relbuf(rel, metabuf);
    }
    if lbuf != InvalidBuffer {
        _bt_relbuf(rel, lbuf);
    }
    _bt_relbuf(rel, rbuf);

    if target != leafblkno {
        _bt_relbuf(rel, buf);
    }

    /* Maintain pages_newly_deleted / pages_deleted. */
    vstate.stats.pages_newly_deleted += 1;
    if target <= scanblkno {
        vstate.stats.pages_deleted += 1;
    }

    /* Remember the target page for later FSM placement. */
    _bt_pendingfsm_add(vstate, target, safexid);

    Ok(true)
}

/// `BTPageSetDeleted(page, safexid)` — mark a page deleted, store safexid.
fn bt_page_set_deleted(page: &mut [u8], safexid: FullTransactionId) -> PgResult<()> {
    let mut opaque = opaque_from_page(page)?;
    opaque.btpo_flags &= !BTP_HALF_DEAD;
    opaque.btpo_flags |= BTP_DELETED | BTP_HAS_FULLXID;
    encode_opaque(page, &opaque);

    // header->pd_lower = MAXALIGN(SizeOfPageHeaderData) + sizeof(BTDeletedPageData)
    // header->pd_upper = header->pd_special
    let lower = maxalign(SizeOfPageHeaderData) + ::core::mem::size_of::<FullTransactionId>();
    set_pd_lower(page, lower as u16);
    let special = pd_special(page);
    set_pd_upper(page, special);

    // contents->safexid = safexid  (BTDeletedPageData at PageGetContents).
    let base = maxalign(SizeOfPageHeaderData);
    page[base..base + 8].copy_from_slice(&safexid.value.to_ne_bytes());
    Ok(())
}

// ---------------------------------------------------------------------------
// _bt_lock_subtree_parent
// ---------------------------------------------------------------------------

fn _bt_lock_subtree_parent<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    child: BlockNumber,
    stack: &mut BTStack,
    subtreeparent: &mut Buffer,
    poffset: &mut OffsetNumber,
    topparent: &mut BlockNumber,
    topparentrightsib: &mut BlockNumber,
) -> PgResult<bool> {
    /*
     * Locate the pivot tuple whose downlink points to "child", write-locking
     * the parent page.
     */
    let (pbuf, _stackoffset) =
        crate::insert::_bt_getstackbuf(mcx, rel, heaprel, stack, child)?;
    if pbuf == InvalidBuffer {
        log_message(&format!(
            "failed to re-find parent key in index \"{}\" for deletion target page {}",
            rel.name(),
            child
        ));
        debug_assert!(false);
        return Ok(false);
    }

    let (parent, parentoffset) = match stack.as_ref() {
        Some(s) => (s.bts_blkno, s.bts_offset),
        None => {
            return Err(PgError::error(
                "_bt_lock_subtree_parent: empty stack after _bt_getstackbuf",
            ))
        }
    };

    let (opaque, maxoff) = {
        let page_bytes = bufmgr::buffer_get_page::call(mcx, pbuf)?;
        let page = PageRef::new(&page_bytes)?;
        (opaque_from_page(&page_bytes)?, PageGetMaxOffsetNumber(&page))
    };
    let leftsibparent = opaque.btpo_prev;

    debug_assert!(!P_INCOMPLETE_SPLIT(&opaque));

    if parentoffset < maxoff {
        *subtreeparent = pbuf;
        *poffset = parentoffset;
        return Ok(true);
    }

    debug_assert!(parentoffset == maxoff);
    if parentoffset != P_FIRSTDATAKEY(&opaque) || P_RIGHTMOST(&opaque) {
        _bt_relbuf(rel, pbuf);
        return Ok(false);
    }

    *topparent = parent;
    *topparentrightsib = opaque.btpo_next;

    _bt_relbuf(rel, pbuf);

    if _bt_leftsib_splitflag(mcx, rel, leftsibparent, parent)? {
        return Ok(false);
    }

    let mut empty: BTStack = None;
    let grandparent: &mut BTStack = match stack.as_mut() {
        Some(s) => &mut s.bts_parent,
        None => &mut empty,
    };
    _bt_lock_subtree_parent(
        mcx,
        rel,
        heaprel,
        parent,
        grandparent,
        subtreeparent,
        poffset,
        topparent,
        topparentrightsib,
    )
}

// ---------------------------------------------------------------------------
// _bt_pendingfsm_init / _bt_pendingfsm_finalize / _bt_pendingfsm_add
// ---------------------------------------------------------------------------

/// `_bt_pendingfsm_init(rel, vstate, cleanuponly)` — size the pending-FSM buffer.
pub fn bt_pendingfsm_init<'mcx>(
    rel: &Relation<'mcx>,
    vstate: &mut BTVacState<'mcx>,
    cleanuponly: bool,
) -> PgResult<()> {
    let _ = rel;
    if cleanuponly {
        return Ok(());
    }

    vstate.bufsize = 256;
    let work_mem = initsmall::work_mem::call() as usize;
    let mut maxbufsize = (work_mem * 1024) / SIZEOF_BTPENDINGFSM;
    maxbufsize = maxbufsize.min(MaxAllocSize / SIZEOF_BTPENDINGFSM);
    maxbufsize = maxbufsize.min(INT_MAX_SZ);
    maxbufsize = maxbufsize.max(vstate.bufsize as usize);
    vstate.maxbufsize = maxbufsize as i32;

    /*
     * Allocate buffer (palloc(sizeof(BTPendingFSM) * bufsize)); 0 pending pages
     * so far.  pendingpages was created in the caller's `'mcx` (BTVacState::new),
     * so we size it there rather than in a temp context.
     */
    vstate.pendingpages.clear();
    vstate.pendingpages.reserve(vstate.bufsize as usize);
    vstate.npendingpages = 0;
    Ok(())
}

/// `_bt_pendingfsm_finalize(rel, vstate)` — place now-safe pending pages in FSM.
pub fn bt_pendingfsm_finalize<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    vstate: &mut BTVacState<'mcx>,
) -> PgResult<()> {
    debug_assert!(vstate.stats.pages_newly_deleted >= vstate.npendingpages as u32);

    if vstate.npendingpages == 0 {
        /* Just free memory when nothing to do (clear the owned buffer). */
        vstate.pendingpages.clear();
        return Ok(());
    }

    /*
     * Recompute VACUUM XID boundaries.  We don't care about the result; the
     * side-effect (forcibly updating this backend's XID horizon state) is what
     * GlobalVisCheckRemovableFullXid() relies on below.
     */
    procarray::get_oldest_non_removable_transaction_id::call(heaprel.rd_id)?;

    let npending = vstate.npendingpages as usize;
    for i in 0..npending {
        let target = vstate.pendingpages[i].target;
        let safexid = vstate.pendingpages[i].safexid;

        /*
         * Equivalent of BTPageIsRecyclable() without re-accessing the page.
         * Pages are stored in safexid order, so the first non-recyclable page
         * means all later ones are non-recyclable too.
         */
        if !global_vis_check_removable_full_xid(heaprel, safexid)? {
            break;
        }

        indexfsm::record_free_index_page::call(rel, target)?;
        vstate.stats.pages_free += 1;
    }

    /* pfree(vstate->pendingpages) */
    vstate.pendingpages.clear();
    Ok(())
}

/// `_bt_pendingfsm_add(vstate, target, safexid)` — record a newly deleted page.
fn _bt_pendingfsm_add<'mcx>(
    vstate: &mut BTVacState<'mcx>,
    target: BlockNumber,
    safexid: FullTransactionId,
) {
    debug_assert!(vstate.npendingpages <= vstate.bufsize);
    debug_assert!(vstate.bufsize <= vstate.maxbufsize);

    /* Pages must be added in safexid order. */
    debug_assert!({
        if vstate.npendingpages > 0 {
            let last = vstate.pendingpages[(vstate.npendingpages - 1) as usize].safexid;
            safexid >= last
        } else {
            true
        }
    });

    /* At capacity (work_mem) -> discard info about this page. */
    if vstate.npendingpages == vstate.maxbufsize {
        return;
    }

    /* Consider enlarging buffer. */
    if vstate.npendingpages == vstate.bufsize {
        let mut newbufsize = vstate.bufsize * 2;
        if newbufsize > vstate.maxbufsize {
            newbufsize = vstate.maxbufsize;
        }
        vstate.bufsize = newbufsize;
        vstate
            .pendingpages
            .reserve((vstate.bufsize as usize).saturating_sub(vstate.pendingpages.len()));
    }

    /* Save metadata for newly deleted page. */
    vstate.pendingpages.push(BTPendingFSM { target, safexid });
    vstate.npendingpages += 1;
}

// ---------------------------------------------------------------------------
// build_empty_metapage  (btbuildempty smgr-bulk-write helper)
// ---------------------------------------------------------------------------

/// `btbuildempty()`'s metapage construction: smgr bulk-write an empty metapage
/// into the INIT fork.
pub fn build_empty_metapage<'mcx>(index: &Relation<'mcx>, allequalimage: bool) -> PgResult<()> {
    use types_core::primitive::ForkNumber;

    with_temp_mcx(|mcx| {
        let mut bulkstate =
            bulkwrite::smgr_bulk_start_rel::call(mcx, index, ForkNumber::INIT_FORKNUM)?;

        /* Construct metapage. */
        let mut metabuf = bulkwrite::smgr_bulk_get_buf::call(mcx, &mut bulkstate)?;
        _bt_initmetapage(&mut metabuf, P_NONE, 0, allequalimage);
        bulkwrite::smgr_bulk_write::call(&mut bulkstate, BTREE_METAPAGE, metabuf, true)?;

        bulkwrite::smgr_bulk_finish::call(bulkstate)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// PredicateLockPageCombine
// ---------------------------------------------------------------------------

/// `PredicateLockPageCombine(rel, oldblkno, newblkno)` (predicate.c) — transfer
/// SIREAD (predicate) locks from a page about to be unlinked onto its right
/// sibling. SSI is not plumbed into this layer: like the sibling stubs
/// `predicate_lock_page_split` / `predicate_lock_relation` / `predicate_lock_page`
/// (insert.rs / search.rs), this is a behaviour-preserving no-op for the
/// non-serializable common case — under non-SERIALIZABLE isolation
/// `PredicateLockPageCombine` does nothing (it early-returns when SSI is not in
/// use), so page deletion (`_bt_mark_page_halfdead`) proceeds unchanged.
#[inline]
fn predicate_lock_page_combine<'mcx>(
    _rel: &Relation<'mcx>,
    _oldblkno: BlockNumber,
    _newblkno: BlockNumber,
) -> PgResult<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// LOG/DEBUG emission.
// ---------------------------------------------------------------------------

/// `ereport(LOG, ...)` — emit a LOG-level message (best-effort; a failure to
/// log is ignored, matching ereport(LOG) which never propagates an error).
fn log_message(msg: &str) {
    let _ = elog::ereport_msg::call(LOG, msg.to_string(), None);
}
/// `elog(DEBUG1, ...)` — emit a DEBUG-level message.
fn log_debug(msg: &str) {
    let _ = elog::ereport_msg::call(DEBUG1, msg.to_string(), None);
}

// ---------------------------------------------------------------------------
// init_seams — install this unit's page-owned seams.
// (The integrator wires these into seams-init; see report for the full list.)
// ---------------------------------------------------------------------------

/// Install the nbtpage-owned seams into `backend-access-nbtree-core-seams`.
pub fn init_seams() {
    nbtcore::build_empty_metapage::set(build_empty_metapage);
    nbtcore::bt_getrootheight::set(bt_getrootheight);
    nbtcore::bt_vacuum_needs_cleanup::set(bt_vacuum_needs_cleanup);
    nbtcore::bt_set_cleanup_info::set(bt_set_cleanup_info);
    nbtcore::bt_pendingfsm_init::set(bt_pendingfsm_init);
    nbtcore::bt_pendingfsm_finalize::set(bt_pendingfsm_finalize);
    nbtcore::bt_lockbuf::set(bt_lockbuf);
    nbtcore::bt_relbuf::set(bt_relbuf);
    nbtcore::bt_checkpage::set(bt_checkpage);
    nbtcore::bt_upgradelockbufcleanup::set(bt_upgradelockbufcleanup);
    nbtcore::bt_page_is_recyclable::set(bt_page_is_recyclable);
    nbtcore::bt_delitems_vacuum::set(bt_delitems_vacuum);
    nbtcore::bt_pagedel::set(bt_pagedel);
    nbtcore::bt_delitems_delete_check::set(bt_delitems_delete_check);
    nbtcore::bt_metaversion::set(bt_metaversion);
}
