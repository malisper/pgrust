//! Owned-tree Rust port of `src/backend/access/gin/ginvacuum.c` (PostgreSQL
//! 18.3) — the delete & vacuum routines for the GIN inverted index.
//!
//! The C functions this module provides, ported 1:1 (the buffer pin/lock order,
//! the WAL-before-unlock discipline, and the posting-tree page-deletion
//! atomicity preserved exactly):
//!
//!   * `ginVacuumItemPointers`     — vacuum an uncompressed posting list
//!   * `xlogVacuumPage`            — WAL a vacuumed entry-tree leaf page
//!   * `ginDeletePage`             — delete a posting-tree page
//!   * `ginScanToDelete`           — recursive empty-page descent
//!   * `ginVacuumPostingTreeLeaves`— leftmost-leaf rightlink walk
//!   * `ginVacuumPostingTree`      — vacuum one posting tree
//!   * `ginVacuumEntryPage`        — vacuum one entry-tree page
//!   * `ginbulkdelete`             — the `ambulkdelete` callback
//!   * `ginvacuumcleanup`          — the `amvacuumcleanup` callback
//!   * `GinPageIsRecyclable`       — page recyclability test
//!
//! # In-crate vs. seam
//!
//! The page-content manipulation runs in-crate against the page bytes (read out
//! / written back through the bufmgr `with_buffer_page` boundary, exactly as
//! the sibling `ginbtree` / `gindatapage` crates do). The posting-tree leaf
//! vacuum itself ([`gindatapage::ginVacuumPostingTreeLeaf`])
//! is the data-page owner's; we re-drive it, passing a closure that routes each
//! decoded segment back into [`ginVacuumItemPointers`] against the running
//! [`GinVacuumState`] (no registry, no pointer round-trip).
//!
//! The genuinely-external substrate crosses the established seams: the buffer
//! cache (read/lock/cleanup-lock/mark-dirty/release), WAL emission, the
//! relation-extension lock, the FSM, the next-XID, the index-vacuum dead-TID
//! callback (`vacuum_tid_is_dead`), the metapage stats update
//! (`ginutil::ginUpdateStats`), and — for the not-yet-ported owners — the
//! predicate-lock combine, the global-visibility check, and the pending-list
//! flush (`ginInsertCleanup`), via [`ginvacuum_seams`].
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use mcx::Mcx;

use bufmgr_seams as bufmgr;
use page::{
    PageAddItemExtended, PageGetContents, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexTupleDelete, PageIsNew, PageMut, PageRef,
};
use utils_error::{ereport, PgResult};
use types_error::error::ERROR;

use core_probe::ginpostinglist::{ginCompressPostingList, ginPostingListDecode};
use gindatapage::{
    gin_page_get_flags, gin_page_get_rightlink, gin_page_set_flags, gin_page_set_rightlink,
    ginVacuumPostingTreeLeaf, GinDataPageGetPostingItem,
    GinGetDownlink, GinGetNPosting, GinGetPosting, GinIsPostingTree, GinItupIsCompressed,
    GinPageDeletePostingItem, GinPageIsCompressed, GinPageIsData, GinPageIsDeleted, GinPageIsLeaf,
    GinPageRightMost, GinDataLeafPageGetPostingListSize, gin_page_get_maxoff,
    PostingItemGetBlockNumber,
};
use ginentrypage::GinFormTuple;
use ginutil::{gintuple_get_attrnum, gintuple_get_key, initGinState, ginUpdateStats};

use types_core::primitive::{BlockNumber, ForkNumber, OffsetNumber, Oid, TransactionId};
use types_core::{InvalidBlockNumber, XLogRecPtr};
use gin::{
    GinMaxItemSize, GinMetaPageData, GinState, GinStatsData, GIN_DELETED, GIN_EXCLUSIVE, GIN_LIST,
    GIN_METAPAGE_BLKNO, GIN_ROOT_BLKNO, GIN_SHARE, GIN_UNLOCK,
};
use rel::Relation;
use types_storage::storage::{Buffer, InvalidBuffer};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tuple::heaptuple::{
    IndexTupleData, ItemPointerData, BlockIdData, FIRST_OFFSET_NUMBER as FirstOffsetNumber,
    INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
};

use ginvacuum_seams as gvsx;

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (ginxlog.h / ginblock.h).
// ===========================================================================

/// `RM_GIN_ID` (rmgrlist.h).
const RM_GIN_ID: types_core::RmgrId = 13;
/// `XLOG_GIN_VACUUM_PAGE` info byte (ginxlog.h).
const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;
/// `XLOG_GIN_DELETE_PAGE` info byte (ginxlog.h).
const XLOG_GIN_DELETE_PAGE: u8 = 0x50;
/// `XLOG_GIN_UPDATE_META_PAGE` info byte (ginxlog.h).
const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;

// `REGBUF_*` flags (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x04;
const REGBUF_FORCE_IMAGE: u8 = 0x01;
/// `REGBUF_WILL_INIT` (xloginsert.h).
const REGBUF_WILL_INIT: u8 = 0x02;

/// `sizeof(ItemPointerData)` on disk (6 bytes).
const SIZE_OF_ITEM_POINTER: usize = 6;

/// Byte offset of `pd_prune_xid` within a `PageHeaderData` (native layout):
/// `pd_lsn(8) + pd_checksum(2) + pd_flags(2) + pd_lower(2) + pd_upper(2) +
/// pd_special(2) + pd_pagesize_version(2) = 20`.
const OFF_PD_PRUNE_XID: usize = 20;

// ===========================================================================
// init_seams — this crate installs the two AM-vtable callbacks ginutil declares.
// ===========================================================================

/// Install `ginbulkdelete` / `ginvacuumcleanup` into the `ginutil` AM-routine
/// seams (the `ambulkdelete` / `amvacuumcleanup` vtable slots). The dispatch
/// adapts the seam shape (`callback_state: Option<u64>` handle) to the
/// owned-value bodies here.
pub fn init_seams() {
    ginutil_seams::ginbulkdelete::set(
        |mcx, info, stats, callback_state| ginbulkdelete(mcx, info, stats, callback_state),
    );
    ginutil_seams::ginvacuumcleanup::set(
        |mcx, info, stats| ginvacuumcleanup(mcx, info, stats),
    );
    // The buffer-cache / WAL metapage substrate `ginutil.c` declares (its real
    // body needs the bufmgr / freespace / xloginsert substrate + the GIN-page
    // recyclability test, all of which live here). `ginutil::GinNewBuffer` /
    // `ginGetStats` / `ginUpdateStats` are the thin dispatchers; the bodies are
    // here.
    ginutil_seams::gin_new_buffer::set(gin_new_buffer_impl);
    ginutil_seams::gin_get_stats::set(gin_get_stats_impl);
    ginutil_seams::gin_update_stats::set(
        |index, n_total, n_entry, n_data, n_entries, is_build| {
            gin_update_stats_impl(index, n_total, n_entry, n_data, n_entries, is_build)
        },
    );
}

// ===========================================================================
// GinNewBuffer / ginGetStats / ginUpdateStats substrate (ginutil.c:305/634/655).
//
// These are `ginutil.c` functions whose real bodies need the buffer-cache /
// freespace / WAL substrate (and, for `GinNewBuffer`, the GIN-page
// recyclability test that lives here). `ginutil` declares them as outward seams
// and dispatches; this crate — already carrying every needed dependency — owns
// the bodies and installs the seams.
// ===========================================================================

/// On-disk byte offsets of `GinMetaPageData` fields within the page contents
/// area (`GinPageGetMeta` == `PageGetContents`), native layout.
const OFF_GIN_HEAD: usize = 0;
const OFF_GIN_TAIL: usize = 4;
const OFF_GIN_TAILFREESIZE: usize = 8;
const OFF_GIN_NPENDINGPAGES: usize = 12;
const OFF_GIN_NPENDINGHEAPTUPLES: usize = 16;
const OFF_GIN_NTOTALPAGES: usize = 24;
const OFF_GIN_NENTRYPAGES: usize = 28;
const OFF_GIN_NDATAPAGES: usize = 32;
const OFF_GIN_NENTRIES: usize = 40;
const OFF_GIN_VERSION: usize = 48;
/// `sizeof(GinMetaPageData)` on disk (`MAXALIGN(offsetof(ginVersion)+4)` = 56).
const SIZE_OF_GIN_META_PAGE_DATA: usize = 56;
/// Byte offset of `pd_lower` within `PageHeaderData`.
const OFF_PD_LOWER: usize = 12;

/// `GinPageGetMeta(page)` — read the metadata struct from a page byte image.
fn read_meta(page: &[u8]) -> PgResult<GinMetaPageData> {
    let pr = PageRef::new(page)?;
    let c = PageGetContents(&pr)?;
    let g32 = |o: usize| u32::from_ne_bytes([c[o], c[o + 1], c[o + 2], c[o + 3]]);
    let g64 = |o: usize| {
        i64::from_ne_bytes([
            c[o], c[o + 1], c[o + 2], c[o + 3], c[o + 4], c[o + 5], c[o + 6], c[o + 7],
        ])
    };
    Ok(GinMetaPageData {
        head: g32(OFF_GIN_HEAD),
        tail: g32(OFF_GIN_TAIL),
        tailFreeSize: g32(OFF_GIN_TAILFREESIZE),
        nPendingPages: g32(OFF_GIN_NPENDINGPAGES),
        nPendingHeapTuples: g64(OFF_GIN_NPENDINGHEAPTUPLES),
        nTotalPages: g32(OFF_GIN_NTOTALPAGES),
        nEntryPages: g32(OFF_GIN_NENTRYPAGES),
        nDataPages: g32(OFF_GIN_NDATAPAGES),
        nEntries: g64(OFF_GIN_NENTRIES),
        ginVersion: i32::from_ne_bytes([
            c[OFF_GIN_VERSION],
            c[OFF_GIN_VERSION + 1],
            c[OFF_GIN_VERSION + 2],
            c[OFF_GIN_VERSION + 3],
        ]),
    })
}

/// Offset of the metadata within the page (`PageGetContents` == header size,
/// `MAXALIGN(SizeOfPageHeaderData)` = 24).
const META_OFFSET: usize = 24;

/// Write the metadata struct into a page byte image and set `pd_lower` just past
/// the metadata (so xlog page compression won't drop it), mirroring the
/// `((PageHeader) metapage)->pd_lower = ...` idiom.
fn write_meta(page: &mut [u8], meta: &GinMetaPageData) {
    let put32 = |page: &mut [u8], fo: usize, v: u32| {
        let p = META_OFFSET + fo;
        page[p..p + 4].copy_from_slice(&v.to_ne_bytes());
    };
    let put64 = |page: &mut [u8], fo: usize, v: i64| {
        let p = META_OFFSET + fo;
        page[p..p + 8].copy_from_slice(&v.to_ne_bytes());
    };
    put32(page, OFF_GIN_HEAD, meta.head);
    put32(page, OFF_GIN_TAIL, meta.tail);
    put32(page, OFF_GIN_TAILFREESIZE, meta.tailFreeSize);
    put32(page, OFF_GIN_NPENDINGPAGES, meta.nPendingPages);
    put64(page, OFF_GIN_NPENDINGHEAPTUPLES, meta.nPendingHeapTuples);
    put32(page, OFF_GIN_NTOTALPAGES, meta.nTotalPages);
    put32(page, OFF_GIN_NENTRYPAGES, meta.nEntryPages);
    put32(page, OFF_GIN_NDATAPAGES, meta.nDataPages);
    put64(page, OFF_GIN_NENTRIES, meta.nEntries);
    {
        let p = META_OFFSET + OFF_GIN_VERSION;
        page[p..p + 4].copy_from_slice(&meta.ginVersion.to_ne_bytes());
    }
    let pd_lower = (META_OFFSET + SIZE_OF_GIN_META_PAGE_DATA) as u16;
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&pd_lower.to_ne_bytes());
}

/// Serialize a [`GinMetaPageData`] to its on-disk byte image (for the WAL record
/// body that copies `metadata`).
fn meta_to_bytes(meta: &GinMetaPageData) -> [u8; SIZE_OF_GIN_META_PAGE_DATA] {
    let mut buf = [0u8; SIZE_OF_GIN_META_PAGE_DATA];
    buf[OFF_GIN_HEAD..OFF_GIN_HEAD + 4].copy_from_slice(&meta.head.to_ne_bytes());
    buf[OFF_GIN_TAIL..OFF_GIN_TAIL + 4].copy_from_slice(&meta.tail.to_ne_bytes());
    buf[OFF_GIN_TAILFREESIZE..OFF_GIN_TAILFREESIZE + 4]
        .copy_from_slice(&meta.tailFreeSize.to_ne_bytes());
    buf[OFF_GIN_NPENDINGPAGES..OFF_GIN_NPENDINGPAGES + 4]
        .copy_from_slice(&meta.nPendingPages.to_ne_bytes());
    buf[OFF_GIN_NPENDINGHEAPTUPLES..OFF_GIN_NPENDINGHEAPTUPLES + 8]
        .copy_from_slice(&meta.nPendingHeapTuples.to_ne_bytes());
    buf[OFF_GIN_NTOTALPAGES..OFF_GIN_NTOTALPAGES + 4]
        .copy_from_slice(&meta.nTotalPages.to_ne_bytes());
    buf[OFF_GIN_NENTRYPAGES..OFF_GIN_NENTRYPAGES + 4]
        .copy_from_slice(&meta.nEntryPages.to_ne_bytes());
    buf[OFF_GIN_NDATAPAGES..OFF_GIN_NDATAPAGES + 4]
        .copy_from_slice(&meta.nDataPages.to_ne_bytes());
    buf[OFF_GIN_NENTRIES..OFF_GIN_NENTRIES + 8].copy_from_slice(&meta.nEntries.to_ne_bytes());
    buf[OFF_GIN_VERSION..OFF_GIN_VERSION + 4].copy_from_slice(&meta.ginVersion.to_ne_bytes());
    buf
}

/// `index->rd_locator` serialized as the WAL `RelFileLocator` (12 bytes:
/// spcOid / dbOid / relNumber).
fn relfilelocator_bytes(index: &Relation<'_>) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0..4].copy_from_slice(&index.rd_locator.spcOid.to_ne_bytes());
    b[4..8].copy_from_slice(&index.rd_locator.dbOid.to_ne_bytes());
    b[8..12].copy_from_slice(&index.rd_locator.relNumber.to_ne_bytes());
    b
}

/// `GinNewBuffer(index)` (ginutil.c:305) — allocate a fresh page, recycling via
/// the FSM (`GetFreeIndexPage` + `ConditionalLockBuffer` + `GinPageIsRecyclable`)
/// else extending the index file (`ExtendBufferedRel`, `EB_LOCK_FIRST`). The
/// returned buffer is pinned and exclusive-locked.
fn gin_new_buffer_impl<'mcx>(index: &Relation<'mcx>) -> PgResult<Buffer> {
    // First, try to get a page from the FSM.
    loop {
        let blkno = get_free_index_page(index)?;
        if blkno == InvalidBlockNumber {
            break;
        }

        let buffer = read_buffer(index, blkno)?;

        // Guard against someone else having already recycled this page; the
        // buffer may be locked if so.
        if bufmgr::conditional_lock_buffer::call(buffer)? {
            let mut recyclable = false;
            bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                recyclable = GinPageIsRecyclable(page)?;
                Ok(())
            })?;
            if recyclable {
                return Ok(buffer); // OK to use
            }
            lock_buffer(buffer, GIN_UNLOCK)?;
        }

        // Can't use it, so release the buffer and try again.
        release_buffer(buffer);
    }

    // Must extend the file.
    bufmgr::extend_buffered_rel::call(index, ForkNumber::MAIN_FORKNUM)
}

/// `ginGetStats(index, stats)` (ginutil.c:634) — read the metapage statistics
/// under `GIN_SHARE` and return them.
fn gin_get_stats_impl<'mcx>(index: &Relation<'mcx>) -> PgResult<GinMetaPageData> {
    let metabuffer = read_buffer(index, GIN_METAPAGE_BLKNO)?;
    lock_buffer(metabuffer, GIN_SHARE)?;
    let mut metadata = GinMetaPageData::default();
    bufmgr::with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        metadata = read_meta(page)?;
        Ok(())
    })?;
    unlock_release_buffer(metabuffer);
    Ok(metadata)
}

/// `ginUpdateStats(index, stats, is_build)` (ginutil.c:655) — write the four
/// planner-stat fields into the metapage under `GIN_EXCLUSIVE` (in a critical
/// section), reset `pd_lower` past the metadata, mark dirty, emit
/// `XLOG_GIN_UPDATE_META_PAGE` before unlock when WAL is needed and this is not
/// a build, then unlock-release. `nPendingPages` / `ginVersion` are *not*
/// touched.
fn gin_update_stats_impl<'mcx>(
    index: &Relation<'mcx>,
    n_total_pages: BlockNumber,
    n_entry_pages: BlockNumber,
    n_data_pages: BlockNumber,
    n_entries: i64,
    is_build: bool,
) -> PgResult<()> {
    let metabuffer = read_buffer(index, GIN_METAPAGE_BLKNO)?;
    lock_buffer(metabuffer, GIN_EXCLUSIVE)?;

    // START_CRIT_SECTION();
    let mut metadata = GinMetaPageData::default();
    bufmgr::with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        let mut m = read_meta(page)?;
        m.nTotalPages = n_total_pages;
        m.nEntryPages = n_entry_pages;
        m.nDataPages = n_data_pages;
        m.nEntries = n_entries;
        write_meta(page, &m);
        metadata = m;
        Ok(())
    })?;

    mark_buffer_dirty(metabuffer);

    if relation_needs_wal(index) && !is_build {
        // ginxlogUpdateMeta { RelFileLocator locator; GinMetaPageData metadata;
        //   BlockNumber prevTail; BlockNumber newRightlink; int32 ntuples; }
        let mut rec = Vec::new();
        rec.extend_from_slice(&relfilelocator_bytes(index));
        rec.extend_from_slice(&meta_to_bytes(&metadata));
        rec.extend_from_slice(&InvalidBlockNumber.to_ne_bytes()); // prevTail
        rec.extend_from_slice(&InvalidBlockNumber.to_ne_bytes()); // newRightlink
        rec.extend_from_slice(&0i32.to_ne_bytes()); // ntuples

        xlog_begin_insert()?;
        xlog_register_data(&rec)?;
        xlog_register_buffer(0, metabuffer, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
        let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_UPDATE_META_PAGE)?;
        page_set_lsn(metabuffer, recptr)?;
    }

    unlock_release_buffer(metabuffer);
    // END_CRIT_SECTION();
    Ok(())
}

// ===========================================================================
// GinVacuumState (ginvacuum.c:27).
// ===========================================================================

/// `GinVacuumState` (ginvacuum.c:27): the working state carried through a
/// bulk-delete pass.
///
/// The C `callback` + `callback_state` (the `IndexBulkDeleteCallback`) is the
/// `vacuum_tid_is_dead` seam keyed by `callback_state_handle` (`None` is the
/// cleanup-only NULL callback); `strategy` (the `BufferAccessStrategy`) is held
/// by the buffer-read seam provider. `tmpCxt` is implicit (Rust drops the
/// decoded item vectors at the end of each iteration).
pub struct GinVacuumState<'a, 'mcx> {
    /// `index` — a borrow of the `IndexVacuumInfo` relation (the C `gvs.index`
    /// is a `Relation` pointer into the same relcache entry).
    pub index: &'a Relation<'mcx>,
    /// `result` — the running [`IndexBulkDeleteResult`].
    pub result: IndexBulkDeleteResult,
    /// `ginstate`.
    pub ginstate: GinState<'mcx>,
    /// The `callback_state` handle keying the `vacuum_tid_is_dead` seam, or
    /// `None` for the C NULL callback (cleanup-only).
    pub callback_state: Option<u64>,
    /// `mcx` — the memory context for the GIN-tuple deform/recreate path.
    pub mcx: Mcx<'mcx>,
}

// ===========================================================================
// Page byte helpers local to vacuum.
// ===========================================================================

/// `GinPageIsList(page)` (ginblock.h:118).
#[inline]
fn GinPageIsList(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_LIST != 0
}

/// `GinPageGetDeleteXid(page)` (ginblock.h): the `pd_prune_xid` field.
#[inline]
fn gin_page_get_delete_xid(page: &[u8]) -> TransactionId {
    TransactionId::from_ne_bytes(page[OFF_PD_PRUNE_XID..OFF_PD_PRUNE_XID + 4].try_into().unwrap())
}

/// `GinPageSetDeleteXid(page, xid)` (ginblock.h): set the `pd_prune_xid` field.
#[inline]
fn gin_page_set_delete_xid(page: &mut [u8], xid: TransactionId) {
    page[OFF_PD_PRUNE_XID..OFF_PD_PRUNE_XID + 4].copy_from_slice(&xid.to_ne_bytes());
}

/// `GinDataLeafPageIsEmpty(page)` (ginblock.h:284): for a compressed leaf, the
/// posting-list size is zero; otherwise `maxoff < FirstOffsetNumber`.
fn gin_data_leaf_page_is_empty(page: &[u8]) -> bool {
    if GinPageIsCompressed(page) {
        GinDataLeafPageGetPostingListSize(page) == 0
    } else {
        (gin_page_get_maxoff(page) as i32) < FirstOffsetNumber as i32
    }
}

/// `BufferGetPage(buffer)` copied out as an owned image.
fn page_bytes(buffer: Buffer) -> PgResult<Vec<u8>> {
    let mut out = Vec::new();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = page.to_vec();
        Ok(())
    })?;
    Ok(out)
}

/// `PageGetItem(page, PageGetItemId(page, off))` returning the item bytes.
fn page_get_item_bytes(page: &[u8], off: OffsetNumber) -> PgResult<Vec<u8>> {
    let pr = PageRef::new(page)?;
    let iid = PageGetItemId(&pr, off)?;
    let item_off = iid.lp_off() as usize;
    let item_len = iid.lp_len() as usize;
    Ok(page[item_off..item_off + item_len].to_vec())
}

/// `IndexTupleData` header view over the leading bytes of a tuple image
/// (`GinGetNPosting`/`GinIsPostingTree`/`GinGetDownlink` read `t_tid`/`t_info`).
fn index_tuple_header(tup: &[u8]) -> IndexTupleData {
    IndexTupleData {
        t_tid: ItemPointerData {
            ip_blkid: BlockIdData {
                bi_hi: u16::from_ne_bytes([tup[0], tup[1]]),
                bi_lo: u16::from_ne_bytes([tup[2], tup[3]]),
            },
            ip_posid: u16::from_ne_bytes([tup[4], tup[5]]),
        },
        t_info: u16::from_ne_bytes([tup[6], tup[7]]),
    }
}

/// Read a 6-byte on-disk `ItemPointerData` from `buf`.
fn read_item_pointer(buf: &[u8]) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([buf[0], buf[1]]),
            bi_lo: u16::from_ne_bytes([buf[2], buf[3]]),
        },
        ip_posid: u16::from_ne_bytes([buf[4], buf[5]]),
    }
}

// ---------------------------------------------------------------------------
// Substrate seam helpers (thin name-faithful wrappers).
// ---------------------------------------------------------------------------

fn read_buffer_extended<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer_extended::call(rel, blkno)
}
fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}
fn unlock_release_buffer(buffer: Buffer) {
    bufmgr::unlock_release_buffer::call(buffer)
}
fn release_buffer(buffer: Buffer) {
    bufmgr::release_buffer::call(buffer)
}
fn mark_buffer_dirty(buffer: Buffer) {
    bufmgr::mark_buffer_dirty::call(buffer)
}
fn buffer_get_block_number(buffer: Buffer) -> BlockNumber {
    bufmgr::buffer_get_block_number::call(buffer)
}
fn lock_buffer_for_cleanup(buffer: Buffer) -> PgResult<()> {
    bufmgr::lock_buffer_for_cleanup::call(buffer)
}
fn page_set_lsn(buffer: Buffer, lsn: XLogRecPtr) -> PgResult<()> {
    bufmgr::page_set_lsn::call(buffer, lsn)
}
fn relation_needs_wal(index: &Relation<'_>) -> bool {
    relcache_seams::relation_needs_wal::call(index)
}
fn read_next_transaction_id() -> PgResult<TransactionId> {
    vacuumlazy_seams::read_next_transaction_id::call()
}
fn vacuum_delay_point() -> PgResult<()> {
    vacuumlazy_seams::vacuum_delay_point::call(false)
}
fn am_autovacuum_worker_process() -> bool {
    vacuumlazy_seams::am_autovacuum_worker_process::call()
        .unwrap_or(false)
}

// xloginsert.
fn xlog_begin_insert() -> PgResult<()> {
    xloginsert_seams::xlog_begin_insert::call()
}
fn xlog_register_data(data: &[u8]) -> PgResult<()> {
    xloginsert_seams::xlog_register_data::call(data)
}
fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    xloginsert_seams::xlog_register_buffer::call(block_id, buffer, flags)
}
fn xlog_insert_record(rmid: types_core::RmgrId, info: u8) -> PgResult<XLogRecPtr> {
    xloginsert_seams::xlog_insert_record::call(rmid, info)
}

/// The index-vacuum dead-TID test (the `IndexBulkDeleteCallback`).
fn vacuum_tid_is_dead(tid: ItemPointerData, callback_state: u64) -> bool {
    vacuum_seams::vacuum_tid_is_dead::call(tid, callback_state)
}

// ===========================================================================
// ginVacuumItemPointers (ginvacuum.c:46).
// ===========================================================================

/// `ginVacuumItemPointers(gvs, items, nitem, &nremaining)` (ginvacuum.c:46):
/// vacuum an uncompressed posting list.
///
/// If none of the items need to be removed, returns `None`. Otherwise returns a
/// new array with the remaining items; `nremaining` is the number of remaining
/// items. The dead-TID test is the index-vacuum callback (the
/// `vacuum_tid_is_dead` seam keyed by `gvs.callback_state`).
pub fn ginVacuumItemPointers(
    gvs: &mut GinVacuumState,
    items: &[ItemPointerData],
    nitem: i32,
    nremaining: &mut i32,
) -> Option<Vec<ItemPointerData>> {
    let mut remaining: i32 = 0;
    let mut tmpitems: Option<Vec<ItemPointerData>> = None;

    // Iterate over the TIDs array.
    for i in 0..nitem as usize {
        let dead = match gvs.callback_state {
            Some(cs) => vacuum_tid_is_dead(items[i], cs),
            // The C `gvs->callback` is never NULL inside ginbulkdelete (where
            // ginVacuumItemPointers is reached); a NULL callback never deletes.
            None => false,
        };
        if dead {
            gvs.result.tuples_removed += 1.0;
            if tmpitems.is_none() {
                // First TID to be deleted: allocate the remaining-items array
                // and copy the ones kept so far.
                let mut v = vec![ItemPointerData::default(); nitem as usize];
                v[..i].copy_from_slice(&items[..i]);
                tmpitems = Some(v);
            }
        } else {
            gvs.result.num_index_tuples += 1.0;
            if let Some(t) = tmpitems.as_mut() {
                t[remaining as usize] = items[i];
            }
            remaining += 1;
        }
    }

    *nremaining = remaining;
    tmpitems
}

// ===========================================================================
// xlogVacuumPage (ginvacuum.c:88).
// ===========================================================================

/// `xlogVacuumPage(index, buffer)` (ginvacuum.c:88): create a WAL record for
/// vacuuming an entry-tree leaf page. Always creates a full image. The caller
/// holds the buffer exclusive-locked and has already marked it dirty.
fn xlogVacuumPage(index: &Relation<'_>, buffer: Buffer) -> PgResult<()> {
    // This is only used for entry tree leaf pages.
    // Assert(!GinPageIsData(page) && GinPageIsLeaf(page));

    if !relation_needs_wal(index) {
        return Ok(());
    }

    // Always create a full image; we don't track the changes at a finer level.
    xlog_begin_insert()?;
    xlog_register_buffer(0, buffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD)?;
    let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_VACUUM_PAGE)?;
    page_set_lsn(buffer, recptr)?;
    Ok(())
}

// ===========================================================================
// DataPageDeleteStack (ginvacuum.c:113).
// ===========================================================================

/// `DataPageDeleteStack` (ginvacuum.c:113): one frame of the posting-tree
/// deletion descent. The C doubly-linked child/parent chain becomes an
/// `Option<Box<...>>` child; `leftBuffer == InvalidBuffer` means "no pinned and
/// locked rightmost non-deleted page on the left yet".
#[derive(Default)]
struct DataPageDeleteStack {
    child: Option<Box<DataPageDeleteStack>>,
    /// current block number
    blkno: BlockNumber,
    /// pinned and locked rightmost non-deleted page on left
    left_buffer: Buffer,
    is_root: bool,
}

// ===========================================================================
// ginDeletePage (ginvacuum.c:128).
// ===========================================================================

/// `ginDeletePage(gvs, deleteBlkno, leftBlkno, parentBlkno, myoff,
/// isParentRoot)` (ginvacuum.c:128): delete a posting tree page. MUST be called
/// only when some parent page holds the exclusive cleanup lock. The caller also
/// holds Exclusive locks on the deletable, parent, and left pages.
fn ginDeletePage(
    gvs: &mut GinVacuumState,
    delete_blkno: BlockNumber,
    left_blkno: BlockNumber,
    parent_blkno: BlockNumber,
    myoff: OffsetNumber,
    _is_parent_root: bool,
) -> PgResult<()> {
    let lbuffer = read_buffer_extended(gvs.index, left_blkno)?;
    let dbuffer = read_buffer_extended(gvs.index, delete_blkno)?;
    let pbuffer = read_buffer_extended(gvs.index, parent_blkno)?;

    // rightlink = GinPageGetOpaque(BufferGetPage(dBuffer))->rightlink;
    let dpage0 = page_bytes(dbuffer)?;
    let rightlink = gin_page_get_rightlink(&dpage0);

    // Any insert which would have gone on the leaf block will now go to its
    // right sibling.
    predicate_seams::predicate_lock_page_combine::call(
        gvs.index.rd_id,
        delete_blkno,
        rightlink,
    )?;

    // START_CRIT_SECTION();

    // Unlink the page by changing the left sibling's rightlink.
    bufmgr::with_buffer_page::call(lbuffer, &mut |page: &mut [u8]| {
        gin_page_set_rightlink(page, rightlink);
        Ok(())
    })?;

    // Delete the downlink from the parent.
    bufmgr::with_buffer_page::call(pbuffer, &mut |page: &mut [u8]| {
        debug_assert_eq!(
            PostingItemGetBlockNumber(&GinDataPageGetPostingItem(page, myoff)),
            delete_blkno
        );
        GinPageDeletePostingItem(page, myoff);
        Ok(())
    })?;

    // We shouldn't change the deleted page's rightlink (to keep running search
    // scans workable). Mark the page deleted and remember the last xid which
    // could know its address.
    let delete_xid = read_next_transaction_id()?;
    let mut page_rightlink = InvalidBlockNumber;
    bufmgr::with_buffer_page::call(dbuffer, &mut |page: &mut [u8]| {
        // GinPageSetDeleted(page): flags |= GIN_DELETED.
        let f = gin_page_get_flags(page);
        gin_page_set_flags(page, f | GIN_DELETED);
        // GinPageSetDeleteXid(page, ReadNextTransactionId()).
        gin_page_set_delete_xid(page, delete_xid);
        page_rightlink = gin_page_get_rightlink(page);
        Ok(())
    })?;

    mark_buffer_dirty(pbuffer);
    mark_buffer_dirty(lbuffer);
    mark_buffer_dirty(dbuffer);

    if relation_needs_wal(gvs.index) {
        // ginxlogDeletePage { OffsetNumber parentOffset; BlockNumber rightLink;
        //                     TransactionId deleteXid; }
        let mut rec_data: Vec<u8> = Vec::new();
        rec_data.extend_from_slice(&myoff.to_ne_bytes());
        // pad to 4-byte boundary for rightLink/deleteXid.
        rec_data.extend_from_slice(&[0u8, 0u8]);
        rec_data.extend_from_slice(&page_rightlink.to_ne_bytes());
        rec_data.extend_from_slice(&delete_xid.to_ne_bytes());

        // We can't pass REGBUF_STANDARD for the deleted page (pre-9.4 pd_lower)
        // nor for the left page; the parent's pd_lower was updated, so it's OK.
        xlog_begin_insert()?;
        xlog_register_buffer(0, dbuffer, 0)?;
        xlog_register_buffer(1, pbuffer, REGBUF_STANDARD)?;
        xlog_register_buffer(2, lbuffer, 0)?;
        xlog_register_data(&rec_data)?;
        let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_DELETE_PAGE)?;
        page_set_lsn(dbuffer, recptr)?;
        page_set_lsn(pbuffer, recptr)?;
        page_set_lsn(lbuffer, recptr)?;
    }

    release_buffer(pbuffer);
    release_buffer(lbuffer);
    release_buffer(dbuffer);

    // END_CRIT_SECTION();

    gvs.result.pages_newly_deleted += 1;
    gvs.result.pages_deleted += 1;
    Ok(())
}

// ===========================================================================
// ginScanToDelete (ginvacuum.c:245).
// ===========================================================================

/// `ginScanToDelete(gvs, blkno, isRoot, parent, myoff)` (ginvacuum.c:245):
/// recursively scan the posting tree and delete empty pages. The caller must
/// lock the root for cleanup. During the scan the path from root to current is
/// kept exclusively locked; the left page is kept exclusively locked too
/// (`ginDeletePage` needs it). The C `me`/`parent`/`child` chain is mutated in
/// place across the sibling loop; here the parent frame's `child` slot carries
/// the (possibly updated) frame back, matching the C in-place reuse.
fn ginScanToDelete(
    gvs: &mut GinVacuumState,
    blkno: BlockNumber,
    is_root: bool,
    parent: &mut DataPageDeleteStack,
    myoff: OffsetNumber,
) -> PgResult<bool> {
    let mut me_delete = false;

    // me = isRoot ? parent : (parent->child ?: new child). Take ownership of the
    // child frame out of the parent for the duration of this call, putting it
    // back at the end.
    let mut me: DataPageDeleteStack = if is_root {
        DataPageDeleteStack {
            child: parent.child.take(),
            blkno: parent.blkno,
            left_buffer: parent.left_buffer,
            is_root: parent.is_root,
        }
    } else if let Some(child) = parent.child.take() {
        *child
    } else {
        DataPageDeleteStack {
            child: None,
            blkno: 0,
            left_buffer: InvalidBuffer,
            is_root: false,
        }
    };

    let buffer = read_buffer_extended(gvs.index, blkno)?;

    if !is_root {
        lock_buffer(buffer, GIN_EXCLUSIVE)?;
    }

    // Assert(GinPageIsData(page));
    let mut page = page_bytes(buffer)?;
    let is_leaf = GinPageIsLeaf(&page);

    if !is_leaf {
        me.blkno = blkno;
        // C re-reads the live page each iteration: `i <= GinPageGetOpaque(page)->
        // maxoff` and `GinDataPageGetPostingItem(page, i)` dereference the page
        // pointer freshly, so when a child deletion (`ginDeletePage`) removes the
        // downlink at offset `i` from THIS page it shifts the remaining items down
        // and decrements `maxoff`. We hold only a byte-image copy, so re-fetch it
        // from the buffer each iteration; a stale snapshot/captured `maxoff` would
        // iterate past the real end and read shifted-away downlinks (wrong child
        // block numbers, tripping ginDeletePage's downlink assertion).
        let mut i: OffsetNumber = FirstOffsetNumber;
        while i <= gin_page_get_maxoff(&page) {
            let child_blkno = PostingItemGetBlockNumber(&GinDataPageGetPostingItem(&page, i));
            if ginScanToDelete(gvs, child_blkno, false, &mut me, i)? {
                i -= 1;
            }
            i += 1;
            page = page_bytes(buffer)?;
        }

        // if (GinPageRightMost(page) && BufferIsValid(me->child->leftBuffer))
        if GinPageRightMost(&page) {
            if let Some(child) = me.child.as_mut() {
                if buffer_is_valid(child.left_buffer) {
                    unlock_release_buffer(child.left_buffer);
                    child.left_buffer = InvalidBuffer;
                }
            }
        }
    }

    let isempty = if is_leaf {
        gin_data_leaf_page_is_empty(&page)
    } else {
        (gin_page_get_maxoff(&page) as i32) < FirstOffsetNumber as i32
    };

    if isempty {
        // We never delete the left- or rightmost branch.
        if buffer_is_valid(me.left_buffer) && !GinPageRightMost(&page) {
            // Assert(!isRoot);
            let left_blkno = buffer_get_block_number(me.left_buffer);
            ginDeletePage(gvs, blkno, left_blkno, parent.blkno, myoff, parent.is_root)?;
            me_delete = true;
        }
    }

    if !me_delete {
        if buffer_is_valid(me.left_buffer) {
            unlock_release_buffer(me.left_buffer);
        }
        me.left_buffer = buffer;
    } else {
        if !is_root {
            lock_buffer(buffer, GIN_UNLOCK)?;
        }
        release_buffer(buffer);
    }

    if is_root {
        release_buffer(buffer);
    }

    // Write the frame back into the parent for reuse on the next sibling,
    // exactly as the C code leaves `parent->child` (or, for the root, the root
    // frame) populated.
    if is_root {
        parent.child = me.child.take();
        parent.blkno = me.blkno;
        parent.left_buffer = me.left_buffer;
        parent.is_root = me.is_root;
    } else {
        parent.child = Some(Box::new(me));
    }

    Ok(me_delete)
}

/// `BufferIsValid(buffer)`.
#[inline]
fn buffer_is_valid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

// ===========================================================================
// ginVacuumPostingTreeLeaves (ginvacuum.c:344).
// ===========================================================================

/// `ginVacuumPostingTreeLeaves(gvs, blkno)` (ginvacuum.c:344): scan through the
/// posting tree leaves and delete empty tuples. Returns true if there is at
/// least one empty page.
fn ginVacuumPostingTreeLeaves(gvs: &mut GinVacuumState, mut blkno: BlockNumber) -> PgResult<bool> {
    let mut has_void_page = false;

    // Find the leftmost leaf page of the posting tree and lock it exclusively.
    let mut buffer;
    loop {
        buffer = read_buffer_extended(gvs.index, blkno)?;
        lock_buffer(buffer, GIN_SHARE)?;

        // Assert(GinPageIsData(page));
        let page = page_bytes(buffer)?;
        if GinPageIsLeaf(&page) {
            lock_buffer(buffer, GIN_UNLOCK)?;
            lock_buffer(buffer, GIN_EXCLUSIVE)?;
            break;
        }

        // Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);
        blkno = PostingItemGetBlockNumber(&GinDataPageGetPostingItem(&page, FirstOffsetNumber));
        // Assert(blkno != InvalidBlockNumber);

        unlock_release_buffer(buffer);
    }

    // Iterate all posting tree leaves using rightlinks and vacuum them.
    loop {
        // The leaf vacuum rewrites the page bytes in place (routing each segment
        // through our ginVacuumItemPointers via the closure). `index` is a copy
        // of the shared `&Relation` reference, so the closure can borrow `gvs`
        // mutably without aliasing the `index` argument.
        let index = gvs.index;
        ginVacuumPostingTreeLeaf(index, buffer, &mut |items: &[ItemPointerData]| {
            let mut nremaining = 0;
            let cleaned = ginVacuumItemPointers(gvs, items, items.len() as i32, &mut nremaining);
            (cleaned, nremaining)
        })?;

        let page = page_bytes(buffer)?;
        if gin_data_leaf_page_is_empty(&page) {
            has_void_page = true;
        }

        blkno = gin_page_get_rightlink(&page);

        unlock_release_buffer(buffer);

        if blkno == InvalidBlockNumber {
            break;
        }

        buffer = read_buffer_extended(gvs.index, blkno)?;
        lock_buffer(buffer, GIN_EXCLUSIVE)?;
    }

    Ok(has_void_page)
}

// ===========================================================================
// ginVacuumPostingTree (ginvacuum.c:407).
// ===========================================================================

/// `ginVacuumPostingTree(gvs, rootBlkno)` (ginvacuum.c:407).
pub fn ginVacuumPostingTree(gvs: &mut GinVacuumState, root_blkno: BlockNumber) -> PgResult<()> {
    if ginVacuumPostingTreeLeaves(gvs, root_blkno)? {
        // There is at least one empty page. Rescan the tree, deleting empty
        // pages.
        let buffer = read_buffer_extended(gvs.index, root_blkno)?;

        // Lock the posting tree root for cleanup to ensure no concurrent inserts.
        lock_buffer_for_cleanup(buffer)?;

        let mut root = DataPageDeleteStack {
            child: None,
            blkno: 0,
            left_buffer: InvalidBuffer,
            is_root: true,
        };

        ginScanToDelete(gvs, root_blkno, true, &mut root, InvalidOffsetNumber)?;

        // The C code frees the child stack here; the `Box` chain drops with
        // `root`.

        unlock_release_buffer(buffer);
    }
    Ok(())
}

// ===========================================================================
// ginVacuumEntryPage (ginvacuum.c:454).
// ===========================================================================

/// `ginVacuumEntryPage(gvs, buffer, roots, &nroot)` (ginvacuum.c:454): vacuum a
/// single entry-tree page. Returns the modified page bytes, or `None` if the
/// page wasn't modified. Works against a copy of the original page once the
/// first change occurs. `roots` collects the posting-tree roots found on the
/// page for later vacuuming.
fn ginVacuumEntryPage(
    gvs: &mut GinVacuumState,
    buffer: Buffer,
    roots: &mut Vec<BlockNumber>,
) -> PgResult<Option<Vec<u8>>> {
    let origpage = page_bytes(buffer)?;
    let maxoff = {
        let pr = PageRef::new(&origpage)?;
        PageGetMaxOffsetNumber(&pr)
    };

    // tmppage = origpage; copied lazily on the first modification.
    let mut tmppage: Option<Vec<u8>> = None;

    roots.clear();

    let mut i: OffsetNumber = FirstOffsetNumber;
    while i <= maxoff {
        // itup = PageGetItem(tmppage, PageGetItemId(tmppage, i)).
        let itup_bytes = {
            let cur_page: &[u8] = tmppage.as_deref().unwrap_or(&origpage);
            page_get_item_bytes(cur_page, i)?
        };
        let itup = index_tuple_header(&itup_bytes);

        if GinIsPostingTree(&itup) {
            // Store the posting tree's root for further processing; we can't
            // vacuum it now (deadlock risk with scans/inserts).
            roots.push(GinGetDownlink(&itup));
        } else if GinGetNPosting(&itup) > 0 {
            let nitems: i32;
            let items_orig: Vec<ItemPointerData>;

            // Get the list of item pointers from the tuple.
            if GinItupIsCompressed(&itup) {
                let ptr = GinGetPosting(&itup);
                let mut n = 0;
                items_orig = ginPostingListDecode(&itup_bytes[ptr..], Some(&mut n));
                nitems = n;
            } else {
                let ptr = GinGetPosting(&itup);
                let n = GinGetNPosting(&itup) as i32;
                let mut v: Vec<ItemPointerData> = Vec::new();
                for k in 0..n as usize {
                    let o = ptr + k * SIZE_OF_ITEM_POINTER;
                    v.push(read_item_pointer(&itup_bytes[o..]));
                }
                items_orig = v;
                nitems = n;
            }

            // Remove any items from the list that need to be vacuumed.
            let mut nitems_after = nitems;
            let items = ginVacuumItemPointers(gvs, &items_orig, nitems, &mut nitems_after);

            // If any item pointers were removed, recreate the tuple.
            if let Some(items) = items {
                let nitems = nitems_after;
                let plist: Option<Vec<u8>>;
                let plistsize: usize;

                if nitems > 0 {
                    let compressed = ginCompressPostingList(
                        &items[..nitems as usize],
                        nitems,
                        GinMaxItemSize as i32,
                        None,
                    );
                    plistsize = compressed.bytes.len();
                    plist = Some(compressed.bytes);
                } else {
                    plist = None;
                    plistsize = 0;
                }

                // If we haven't yet, create a temporary copy of the page.
                if tmppage.is_none() {
                    tmppage = Some(origpage.clone());
                }
                let work = tmppage.as_mut().unwrap();

                // Re-fetch itup on the (new) temp page.
                let itup_on_tmp = page_get_item_bytes(work, i)?;

                let attnum = gintuple_get_attrnum(&gvs.ginstate, &itup_on_tmp, gvs.mcx)?;
                let (key, category) = gintuple_get_key(&gvs.ginstate, &itup_on_tmp, gvs.mcx)?;
                let new_itup = GinFormTuple(
                    &gvs.ginstate,
                    gvs.mcx,
                    attnum,
                    key,
                    category,
                    plist.as_deref(),
                    plistsize,
                    nitems,
                    true,
                )?
                .expect("errorTooBig=true never returns None");

                {
                    let mut pm = PageMut::new(work)?;
                    PageIndexTupleDelete(&mut pm, i)?;
                    let added = PageAddItemExtended(&mut pm, &new_itup, i, 0)?;
                    if added != i {
                        return Err(ereport(ERROR)
                            .errmsg(format!(
                                "failed to add item to index page in \"{}\"",
                                gvs.index.rd_rel.relname.as_str()
                            ))
                            .into_error());
                    }
                }
            }
        }
        i += 1;
    }

    Ok(tmppage)
}

// ===========================================================================
// ginbulkdelete (ginvacuum.c:563).
// ===========================================================================

/// `ginbulkdelete(info, stats, callback, callback_state)` (ginvacuum.c:563).
/// `callback_state` is the `vacuum_tid_is_dead` seam handle (the
/// `IndexBulkDeleteCallback` + state). When `stats` is `None`, it is initialized
/// to zeroes and the fast-update pending list is cleaned up first.
pub fn ginbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index = &info.index;
    let mut blkno = GIN_ROOT_BLKNO;

    // initGinState(&gvs.ginstate, index).
    let ginstate = initGinState(index, mcx)?;

    let mut gvs = GinVacuumState {
        index,
        result: IndexBulkDeleteResult::default(),
        ginstate,
        callback_state,
        mcx,
    };

    // first time through?
    let mut stats = match stats {
        Some(s) => s,
        None => {
            // Yes, so initialize stats to zeroes and clean up any pending
            // inserts.
            let mut s = IndexBulkDeleteResult::default();
            let deleted = gvsx::gin_insert_cleanup::call(
                gvs.mcx,
                gvs.index,
                !am_autovacuum_worker_process(),
                false,
                true,
            )?;
            s.pages_deleted += deleted;
            s
        }
    };

    // We'll re-count the tuples each time.
    stats.num_index_tuples = 0.0;
    gvs.result = stats;

    let mut buffer = read_buffer_extended(gvs.index, blkno)?;

    // Find the leftmost leaf page.
    loop {
        // Assert(!GinPageIsData(page));
        lock_buffer(buffer, GIN_SHARE)?;

        let page = page_bytes(buffer)?;
        if GinPageIsLeaf(&page) {
            lock_buffer(buffer, GIN_UNLOCK)?;
            lock_buffer(buffer, GIN_EXCLUSIVE)?;

            if blkno == GIN_ROOT_BLKNO {
                let page2 = page_bytes(buffer)?;
                if !GinPageIsLeaf(&page2) {
                    lock_buffer(buffer, GIN_UNLOCK)?;
                    continue; // check it one more time
                }
            }
            break;
        }

        // Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);
        let itup = index_tuple_header(&page_get_item_bytes(&page, FirstOffsetNumber)?);
        blkno = GinGetDownlink(&itup);
        // Assert(blkno != InvalidBlockNumber);

        unlock_release_buffer(buffer);
        buffer = read_buffer_extended(gvs.index, blkno)?;
    }

    // Right now we found the leftmost page in the entry B-tree.
    let mut root_of_posting_tree: Vec<BlockNumber> = Vec::new();
    loop {
        // Assert(!GinPageIsData(page));
        let res_page = ginVacuumEntryPage(&mut gvs, buffer, &mut root_of_posting_tree)?;

        let page = page_bytes(buffer)?;
        blkno = gin_page_get_rightlink(&page);

        if let Some(res_page) = res_page {
            // START_CRIT_SECTION();
            // PageRestoreTempPage(resPage, page); MarkBufferDirty; xlogVacuumPage.
            bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                page.copy_from_slice(&res_page);
                Ok(())
            })?;
            mark_buffer_dirty(buffer);
            xlogVacuumPage(gvs.index, buffer)?;
            unlock_release_buffer(buffer);
            // END_CRIT_SECTION();
        } else {
            unlock_release_buffer(buffer);
        }

        vacuum_delay_point()?;

        // Vacuum each posting tree root found on this page.
        let roots = core::mem::take(&mut root_of_posting_tree);
        for root in roots {
            ginVacuumPostingTree(&mut gvs, root)?;
            vacuum_delay_point()?;
        }

        if blkno == InvalidBlockNumber {
            break; // rightmost page
        }

        buffer = read_buffer_extended(gvs.index, blkno)?;
        lock_buffer(buffer, GIN_EXCLUSIVE)?;
    }

    Ok(Some(gvs.result))
}

// ===========================================================================
// ginvacuumcleanup (ginvacuum.c:686).
// ===========================================================================

/// `ginvacuumcleanup(info, stats)` (ginvacuum.c:686). The metapage stats update
/// goes through the in-main `ginutil::ginUpdateStats` (its own seam).
pub fn ginvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index = &info.index;

    // In an autovacuum analyze, we want to clean up pending insertions.
    // Otherwise, an ANALYZE-only call is a no-op.
    if info.analyze_only {
        if am_autovacuum_worker_process() {
            // initGinState(&ginstate, index); ginInsertCleanup(...).
            // The owner (ginfast) re-derives the GinState from `index`.
            gvsx::gin_insert_cleanup::call(mcx, index, false, true, true)?;
        }
        return Ok(stats);
    }

    // Set up all-zero stats and clean up pending inserts if ginbulkdelete wasn't
    // called.
    let mut stats = match stats {
        Some(s) => s,
        None => {
            let mut s = IndexBulkDeleteResult::default();
            // The owner (ginfast) re-derives the GinState from `index`.
            let deleted = gvsx::gin_insert_cleanup::call(
                mcx,
                index,
                !am_autovacuum_worker_process(),
                false,
                true,
            )?;
            s.pages_deleted += deleted;
            s
        }
    };

    let mut idx_stat = GinStatsData::default();

    // XXX we always report the heap tuple count as the number of index entries.
    stats.num_index_tuples = info.num_heap_tuples.max(0.0);
    stats.estimated_count = info.estimated_count;

    // Need lock unless it's local to this backend.
    let need_lock = !relation_is_local(index.rd_id)?;

    if need_lock {
        lock_relation_for_extension(index)?;
    }
    let npages = relation_get_number_of_blocks(index)?;
    if need_lock {
        unlock_relation_for_extension(index)?;
    }

    let mut tot_free_pages: BlockNumber = 0;

    let mut blkno = GIN_ROOT_BLKNO;
    while blkno < npages {
        vacuum_delay_point()?;

        let buffer = read_buffer_extended(index, blkno)?;
        lock_buffer(buffer, GIN_SHARE)?;
        let page = page_bytes(buffer)?;

        if GinPageIsRecyclable(&page)? {
            // Assert(blkno != GIN_ROOT_BLKNO);
            record_free_index_page(index, blkno)?;
            tot_free_pages += 1;
        } else if GinPageIsData(&page) {
            idx_stat.nDataPages += 1;
        } else if !GinPageIsList(&page) {
            idx_stat.nEntryPages += 1;

            if GinPageIsLeaf(&page) {
                let pr = PageRef::new(&page)?;
                idx_stat.nEntries += PageGetMaxOffsetNumber(&pr) as i64;
            }
        }

        unlock_release_buffer(buffer);
        blkno += 1;
    }

    // Update the metapage with accurate page and entry counts.
    idx_stat.nTotalPages = npages;
    ginUpdateStats(index, &idx_stat, false)?;

    // Finally, vacuum the FSM.
    index_free_space_map_vacuum(index)?;

    stats.pages_free = tot_free_pages;

    if need_lock {
        lock_relation_for_extension(index)?;
    }
    stats.num_pages = relation_get_number_of_blocks(index)?;
    if need_lock {
        unlock_relation_for_extension(index)?;
    }

    Ok(Some(stats))
}

// ===========================================================================
// GinPageIsRecyclable (ginvacuum.c:800).
// ===========================================================================

/// `GinPageIsRecyclable(page)` (ginvacuum.c:800): whether `page` can safely be
/// recycled.
pub fn GinPageIsRecyclable(page: &[u8]) -> PgResult<bool> {
    {
        let pr = PageRef::new(page)?;
        if PageIsNew(&pr) {
            return Ok(true);
        }
    }

    if !GinPageIsDeleted(page) {
        return Ok(false);
    }

    // delete_xid = GinPageGetDeleteXid(page) == pd_prune_xid.
    let delete_xid = gin_page_get_delete_xid(page);

    if !transaction_id_is_valid(delete_xid) {
        return Ok(true);
    }

    // If no backend could still view delete_xid as in-progress, all scans
    // concurrent with ginDeletePage() must have finished.
    procarray_seams::global_vis_check_removable_xid::call(delete_xid)
}

/// `TransactionIdIsValid(xid)` — `xid != InvalidTransactionId` (0).
#[inline]
fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != 0
}

// ---------------------------------------------------------------------------
// More substrate seam helpers (lmgr / freespace / hio).
// ---------------------------------------------------------------------------

fn relation_is_local(relid: Oid) -> PgResult<bool> {
    hio_seams::relation_is_local::call(relid)
}
fn lock_relation_for_extension<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    // The guard releases on drop; GIN brackets the lock explicitly with
    // UnlockRelationForExtension, so we leak the guard intentionally (the
    // explicit unlock seam performs the release).
    let guard = lmgr_seams::lock_relation_for_extension::call(rel)?;
    core::mem::forget(guard);
    Ok(())
}
fn unlock_relation_for_extension<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    lmgr_seams::unlock_relation_for_extension::call(rel.rd_locator.dbOid, rel.rd_id)
}
fn relation_get_number_of_blocks<'mcx>(rel: &Relation<'mcx>) -> PgResult<BlockNumber> {
    relcache_seams::relation_get_number_of_blocks::call(rel)
}
fn record_free_index_page<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<()> {
    freespace_seams::record_free_index_page::call(rel, blkno)
}
fn get_free_index_page<'mcx>(rel: &Relation<'mcx>) -> PgResult<BlockNumber> {
    freespace_seams::get_free_index_page::call(rel)
}
fn read_buffer<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer::call(rel, blkno)
}
fn index_free_space_map_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    freespace_seams::index_free_space_map_vacuum::call(rel)
}
