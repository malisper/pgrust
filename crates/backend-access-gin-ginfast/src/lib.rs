#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! Port of `src/backend/access/gin/ginfast.c` (PostgreSQL 18.3): the GIN
//! fast-update *pending list*. New index entries are collected into a
//! [`GinTupleCollector`] (`ginHeapTupleFastCollect`) and written to a chain of
//! `GIN_LIST` pages off the metapage (`ginHeapTupleFastInsert`); periodically
//! the pending list is flushed into the regular entry tree by
//! [`ginInsertCleanup`] (re-inserting every key via the landed `ginEntryInsert`
//! spine, then unlinking the consumed pages with [`shiftList`]).
//!
//! This crate OWNS and installs the two seams declared by
//! `backend-access-gin-gininsert-seams` (`gin_get_use_fast_update` and
//! `gin_fast_insert`) that `gininsert.c`'s fast-update branch routes through.
//!
//! The metapage pending-list head/tail management, the per-page lock dance, and
//! the three WAL records (`XLOG_GIN_UPDATE_META_PAGE` /
//! `XLOG_GIN_INSERT_LISTPAGE` / `XLOG_GIN_DELETE_LISTPAGE`) mirror C exactly.
//!
//! # Out of scope
//!
//!   * `gin_clean_pending_list(regclass)` (ginfast.c:1031) — the SQL-callable
//!     wrapper around [`ginInsertCleanup`] (`forceCleanup = true`). It is reached
//!     only through the fmgr SQL builtin dispatch (the `pg_proc` → C-function
//!     table), which is not yet wired in this repo, so there is no caller and no
//!     seam contract for it. Its core (`ginInsertCleanup`) is fully ported here;
//!     port the SQL wrapper when the fmgr builtin dispatch reaches it (it needs
//!     `index_open`/`index_close`, `object_ownercheck`, `aclcheck_error`).

extern crate alloc;

use alloc::vec::Vec;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_freespace_seams as fsm;
use backend_storage_page::{
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIsEmpty, PageMut, PageRef,
};
use backend_utils_error::PgResult;

use mcx::Mcx;
use types_core::primitive::{BlockNumber, OffsetNumber, BLCKSZ};
use types_core::Oid;
use types_error::PgError;
use types_gin::{
    GinMetaPageData, GinNullCategory, GinState, GIN_EXCLUSIVE, GIN_LIST, GIN_LIST_FULLROW,
    GIN_METAPAGE_BLKNO, GIN_SHARE, GIN_UNLOCK,
};
use types_rel::Relation;
use types_storage::lock::{ExclusiveLock, LockRelId};
use types_storage::storage::{Buffer, InvalidBuffer};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_gin_ginutil::{
    ginExtractEntries, GinInitBuffer, GinNewBuffer, gintuple_get_attrnum, gintuple_get_key,
};
use backend_access_gin_ginentrypage::GinFormTuple;
use backend_access_gin_gininsert::ginEntryInsert;

use backend_storage_lmgr_lmgr::{ConditionalLockPage, LockPage, UnlockPage};

mod page;
use page::{
    gin_opaque_from_page, index_tuple_size, meta_to_bytes, or_flags, read_meta, set_flags,
    set_maxoff, set_rightlink, write_meta, GinOpaque, SIZE_OF_GIN_META_PAGE_DATA,
};

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (ginfast.c / ginblock.h / ginxlog.h / gin_private.h).
// ===========================================================================

/// `RM_GIN_ID` (rmgrlist.h) — the GIN resource manager id (local per repo
/// convention; siblings open-code the same value).
const RM_GIN_ID: types_core::RmgrId = 13;

/// `XLOG_GIN_UPDATE_META_PAGE` (ginxlog.h).
const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;
/// `XLOG_GIN_INSERT_LISTPAGE` (ginxlog.h).
const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;
/// `XLOG_GIN_DELETE_LISTPAGE` (ginxlog.h).
const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;

/// `REGBUF_WILL_INIT` (xloginsert.h).
const REGBUF_WILL_INIT: u8 = 0x02;
/// `REGBUF_STANDARD` (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x04;

/// `FirstOffsetNumber` (off.h).
const FirstOffsetNumber: OffsetNumber = 1;
/// `InvalidBlockNumber` (block.h).
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
/// `sizeof(ItemIdData)` (itemid.h) — a 4-byte line pointer.
const SIZE_OF_ITEM_ID_DATA: usize = 4;
/// `MaxAllocSize` (memutils.h) — `0x3fffffff`.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// `GIN_NDELETE_AT_ONCE` (ginfast.c) — `Min(16, XLR_MAX_BLOCK_ID - 1)`.
/// `XLR_MAX_BLOCK_ID` is 32, so this is 16.
const GIN_NDELETE_AT_ONCE: usize = 16;

/// `MAXALIGN(SizeOfPageHeaderData)` = 24; `MAXALIGN(sizeof(GinPageOpaqueData))`
/// = 8 (the opaque is 8 bytes: u32 + u16 + u16).
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
const GIN_PAGE_OPAQUE_SIZE: usize = 8;

/// `GIN_PAGE_FREESIZE` (ginfast.c) — `BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
/// MAXALIGN(sizeof(GinPageOpaqueData))`.
const GIN_PAGE_FREESIZE: usize = BLCKSZ - SIZE_OF_PAGE_HEADER_DATA - GIN_PAGE_OPAQUE_SIZE;

/// `GinListPageSize` (ginblock.h) — `BLCKSZ - SizeOfPageHeaderData -
/// MAXALIGN(sizeof(GinPageOpaqueData))` (note: the header term is NOT
/// MAXALIGN'd here, unlike `GIN_PAGE_FREESIZE`).
const SIZE_OF_PAGE_HEADER_DATA_RAW: usize = 24;
const GIN_LIST_PAGE_SIZE: usize = BLCKSZ - SIZE_OF_PAGE_HEADER_DATA_RAW - GIN_PAGE_OPAQUE_SIZE;

/// `MAXALIGN(len)`.
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7usize
}

// ===========================================================================
// GinTupleCollector (gin_private.h).
// ===========================================================================

/// `GinTupleCollector` (gin_private.h) — the per-heap-tuple collection of GIN
/// index tuples awaiting a fast-update pending-list write. Each tuple is an
/// on-disk `IndexTuple` byte image whose `t_tid` carries the heap TID.
#[derive(Default)]
pub struct GinTupleCollector {
    /// `IndexTuple *tuples` — the collected tuple byte images.
    pub tuples: Vec<Vec<u8>>,
    /// `uint32 sumsize` — total `IndexTupleSize` of the collected tuples.
    pub sumsize: usize,
}

impl GinTupleCollector {
    /// `collector->ntuples`.
    fn ntuples(&self) -> usize {
        self.tuples.len()
    }
}

// ===========================================================================
// KeyArray (ginfast.c) — workspace for processPendingPage.
// ===========================================================================

/// `KeyArray` (ginfast.c) — an expansible `(key, category)` workspace.
struct KeyArray<'mcx> {
    keys: Vec<Datum<'mcx>>,
    categories: Vec<GinNullCategory>,
}

/// `initKeyArray(keys, maxvalues)` (ginfast.c:675).
fn initKeyArray<'mcx>(maxvalues: usize) -> KeyArray<'mcx> {
    KeyArray {
        keys: Vec::with_capacity(maxvalues),
        categories: Vec::with_capacity(maxvalues),
    }
}

/// `addDatum(keys, datum, category)` (ginfast.c:685).
fn addDatum<'mcx>(keys: &mut KeyArray<'mcx>, datum: Datum<'mcx>, category: GinNullCategory) {
    keys.keys.push(datum);
    keys.categories.push(category);
}

// ===========================================================================
// init_seams — install the fast-update seams gininsert routes through.
// ===========================================================================

/// Install the two seams `backend-access-gin-gininsert-seams` declares for the
/// fast-update path (`gin_get_use_fast_update`, `gin_fast_insert`).
pub fn init_seams() {
    backend_access_gin_gininsert_seams::gin_get_use_fast_update::set(gin_get_use_fast_update);
    backend_access_gin_gininsert_seams::gin_fast_insert::set(gin_fast_insert);

    // GIN vacuum (`ginbulkdelete` / `ginvacuumcleanup` / autovacuum-analyze)
    // flushes the fast-update pending list through `ginInsertCleanup`, which
    // lives here. C re-derives the `GinState` from the index immediately before
    // the call (`initGinState(&ginstate, index)`); do the same in the shim.
    backend_access_gin_ginvacuum_seams::gin_insert_cleanup::set(
        |mcx, index, full_clean, fill_fsm, force_cleanup| {
            let ginstate = backend_access_gin_ginutil::initGinState(index, mcx)?;
            ginInsertCleanup(&ginstate, mcx, index, full_clean, fill_fsm, force_cleanup, None)
        },
    );

    // `int gin_pending_list_limit = 0;` (ginfast.c:39) — the process-global
    // `conf->variable` backing for the `gin_pending_list_limit` GUC. The GUC
    // engine seeds it from `boot_val` (4096), users SET it, and
    // `GinGetPendingListCleanupSize` reads it directly (ginutil.c:611 fallback
    // when the per-index `pendingListCleanupSize` reloption is -1). It is NOT a
    // ControlFile value — the GUC table entry points `variable` straight at this
    // global. Backing lives here in the owning crate.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::gin_pending_list_limit.install(GucVarAccessors {
            get: gin_pending_list_limit,
            set: set_gin_pending_list_limit,
        });
    }
}

// `int gin_pending_list_limit = 0;` (ginfast.c:39) — process-local backing for
// the GUC. Modeled as a thread-local cell mirroring the C global (cf. the
// globals.c-backed GUC ints in init-small); seeded to the `boot_val` (4096 kB)
// the GUC table declares so reads before the GUC engine assigns are still
// faithful to PG's default.
std::thread_local! {
    static GIN_PENDING_LIST_LIMIT: core::cell::Cell<i32> = const { core::cell::Cell::new(4096) };
}

/// Read `gin_pending_list_limit` (the GUC `conf->variable`).
fn gin_pending_list_limit() -> i32 {
    GIN_PENDING_LIST_LIMIT.with(|c| c.get())
}

/// Assign `gin_pending_list_limit` (the GUC engine's `assign` path).
fn set_gin_pending_list_limit(v: i32) {
    GIN_PENDING_LIST_LIMIT.with(|c| c.set(v));
}

/// `GinGetUseFastUpdate(index)` (gin_private.h): read the `fastupdate` reloption
/// off `index->rd_options` (the GIN `GinOptions` bytea). Resolved via the
/// `ginutil`-owned seam (which owns the `GinOptions` byte layout); routed here so
/// the gininsert seam contract is fulfilled.
fn gin_get_use_fast_update(index: &Relation<'_>) -> PgResult<bool> {
    backend_access_gin_ginutil_seams::gin_get_use_fast_update::call(index)
}

/// `gin_fast_insert` (gininsert-seams): collect one heap tuple's index entries
/// into a fresh [`GinTupleCollector`] and write the pending pages
/// (`ginHeapTupleFastCollect` for each attribute, then `ginHeapTupleFastInsert`).
/// Mirrors the fast-update branch of `gininsert()` (gininsert.c:884).
fn gin_fast_insert<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    index_oid: Oid,
    values: Vec<Datum<'mcx>>,
    isnull: Vec<bool>,
    ht_ctid: ItemPointerData,
) -> PgResult<()> {
    let ginstate = backend_access_gin_ginutil::initGinState(index, mcx)?;
    let _ = index_oid;
    let natts = ginstate.natts();

    let mut collector = GinTupleCollector::default();
    for i in 0..natts {
        ginHeapTupleFastCollect(
            &ginstate,
            mcx,
            &mut collector,
            (i + 1) as OffsetNumber,
            values[i].clone(),
            isnull[i],
            &ht_ctid,
        )?;
    }

    ginHeapTupleFastInsert(&ginstate, mcx, index, &collector)?;
    Ok(())
}

// ===========================================================================
// writeListPage (ginfast.c:59)
// ===========================================================================

/// `writeListPage(index, buffer, tuples, ntuples, rightlink)` (ginfast.c:59):
/// init `buffer` as a `GIN_LIST` page, add all `tuples`, set its rightlink, mark
/// dirty + WAL, and return the page's exact free space. Consumes (unlocks +
/// unpins) `buffer` before returning.
fn writeListPage(
    index: &Relation<'_>,
    buffer: Buffer,
    tuples: &[Vec<u8>],
    rightlink: BlockNumber,
) -> PgResult<i32> {
    let need_wal = relation_needs_wal(index);
    let mut workspace: Vec<u8> = Vec::new();
    let mut size: usize = 0;

    // START_CRIT_SECTION();
    // Build the page image and the WAL data blob in one pass over the page.
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        GinInitBuffer(page, GIN_LIST as u32)?;
        let mut off = FirstOffsetNumber;
        workspace.clear();
        size = 0;
        for t in tuples {
            let this_size = index_tuple_size(t);
            workspace.extend_from_slice(&t[..this_size]);
            size += this_size;
            let mut pm = PageMut::new(page)?;
            let l = backend_storage_page::PageAddItemExtended(&mut pm, &t[..this_size], off, 0)?;
            if l == 0 {
                return Err(PgError::error("failed to add item to index page"));
            }
            off += 1;
        }
        debug_assert!(size <= BLCKSZ);
        set_rightlink(page, rightlink);
        if rightlink == InvalidBlockNumber {
            // tail page of the list: it stores a complete row.
            or_flags(page, GIN_LIST_FULLROW);
            set_maxoff(page, 1);
        } else {
            set_maxoff(page, 0);
        }
        Ok(())
    })?;

    bufmgr::mark_buffer_dirty::call(buffer);

    if need_wal {
        // ginxlogInsertListPage { BlockNumber rightlink; int32 ntuples; }
        xlog_begin_insert()?;
        let mut hdr = Vec::with_capacity(8);
        hdr.extend_from_slice(&rightlink.to_ne_bytes());
        hdr.extend_from_slice(&(tuples.len() as i32).to_ne_bytes());
        xlog_register_data(&hdr)?;
        xlog_register_buffer(0, buffer, REGBUF_WILL_INIT)?;
        xlog_register_buf_data(0, &workspace[..size])?;
        let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_INSERT_LISTPAGE)?;
        bufmgr::page_set_lsn::call(buffer, recptr)?;
    }

    // PageGetExactFreeSpace(page) before releasing.
    let mut freesize: i32 = 0;
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        let pr = PageRef::new(page)?;
        freesize = backend_storage_page::PageGetExactFreeSpace(&pr) as i32;
        Ok(())
    })?;

    bufmgr::unlock_release_buffer::call(buffer);
    // END_CRIT_SECTION();
    Ok(freesize)
}

// ===========================================================================
// makeSublist (ginfast.c:145)
// ===========================================================================

/// `makeSublist(index, tuples, ntuples, res)` (ginfast.c:145): split `tuples`
/// into a chain of newly allocated `GIN_LIST` pages and fill `res`'s pending-list
/// metadata (head/tail/tailFreeSize/nPendingPages/nPendingHeapTuples=1).
fn makeSublist(
    index: &Relation<'_>,
    tuples: &[Vec<u8>],
    res: &mut GinMetaPageData,
) -> PgResult<()> {
    debug_assert!(!tuples.is_empty());
    let ntuples = tuples.len();

    let mut cur_buffer: Buffer = InvalidBuffer;
    let mut prev_buffer: Buffer = InvalidBuffer;
    let mut size: usize = 0;
    let mut start_tuple: usize = 0;

    let mut i: usize = 0;
    while i < ntuples {
        if cur_buffer == InvalidBuffer {
            cur_buffer = GinNewBuffer(index)?;
            if prev_buffer != InvalidBuffer {
                res.nPendingPages += 1;
                let cur_blkno = bufmgr::buffer_get_block_number::call(cur_buffer);
                writeListPage(index, prev_buffer, &tuples[start_tuple..i], cur_blkno)?;
            } else {
                res.head = bufmgr::buffer_get_block_number::call(cur_buffer);
            }
            prev_buffer = cur_buffer;
            start_tuple = i;
            size = 0;
        }

        let tupsize = maxalign(index_tuple_size(&tuples[i])) + SIZE_OF_ITEM_ID_DATA;

        if size + tupsize > GIN_LIST_PAGE_SIZE {
            // won't fit on this page: reprocess this tuple on a fresh page.
            i -= 1;
            cur_buffer = InvalidBuffer;
        } else {
            size += tupsize;
        }
        i += 1;
    }

    res.tail = bufmgr::buffer_get_block_number::call(cur_buffer);
    res.tailFreeSize =
        writeListPage(index, cur_buffer, &tuples[start_tuple..ntuples], InvalidBlockNumber)? as u32;
    res.nPendingPages += 1;
    // that was only one heap tuple
    res.nPendingHeapTuples = 1;
    Ok(())
}

// ===========================================================================
// ginHeapTupleFastInsert (ginfast.c:219)
// ===========================================================================

/// `ginHeapTupleFastInsert(ginstate, collector)` (ginfast.c:219): write the
/// collected tuples into the metapage's pending list — either appended onto the
/// tail page, or as a freshly built sublist linked in. Triggers
/// [`ginInsertCleanup`] afterwards when the pending list grows past the cleanup
/// threshold.
pub fn ginHeapTupleFastInsert<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    collector: &GinTupleCollector,
) -> PgResult<()> {
    if collector.ntuples() == 0 {
        return Ok(());
    }

    let need_wal = relation_needs_wal(index);

    // ginxlogUpdateMeta header fields (the metadata is filled in at the end).
    let mut data_prev_tail: BlockNumber = InvalidBlockNumber;
    let mut data_new_rightlink: BlockNumber = InvalidBlockNumber;
    let mut data_ntuples: i32 = 0;

    let metabuffer = read_buffer(index, GIN_METAPAGE_BLKNO)?;

    let mut separate_list = false;
    let mut metadata: GinMetaPageData;

    let collector_bytes = collector.sumsize;
    let collector_overhead = collector.ntuples() * SIZE_OF_ITEM_ID_DATA;

    if collector_bytes + collector_overhead > GIN_LIST_PAGE_SIZE {
        separate_list = true;
        metadata = GinMetaPageData::default();
    } else {
        lock_buffer(metabuffer, GIN_EXCLUSIVE)?;
        metadata = read_metabuffer(metabuffer)?;
        if metadata.head == InvalidBlockNumber
            || collector_bytes + collector_overhead > metadata.tailFreeSize as usize
        {
            separate_list = true;
            lock_buffer(metabuffer, GIN_UNLOCK)?;
        }
    }

    // `buffer` is the tail/listpage buffer touched (InvalidBuffer if only the
    // metapage was modified). `data_buffer_image` holds the bytes registered for
    // the data block (Case B).
    let mut buffer: Buffer = InvalidBuffer;
    let mut data_buffer_image: Vec<u8> = Vec::new();

    if separate_list {
        // Total tuples won't fit on the tail page: build a sublist.
        let mut sublist = GinMetaPageData::default();
        makeSublist(index, &collector.tuples, &mut sublist)?;

        lock_buffer(metabuffer, GIN_EXCLUSIVE)?;
        metadata = read_metabuffer(metabuffer)?;
        check_for_serializable_conflict_in(index.rd_id, GIN_METAPAGE_BLKNO)?;

        if metadata.head == InvalidBlockNumber {
            // Main list is empty — install the sublist as the list.
            // START_CRIT_SECTION();
            metadata.head = sublist.head;
            metadata.tail = sublist.tail;
            metadata.tailFreeSize = sublist.tailFreeSize;
            metadata.nPendingPages = sublist.nPendingPages;
            metadata.nPendingHeapTuples = sublist.nPendingHeapTuples;
            if need_wal {
                xlog_begin_insert()?;
            }
        } else {
            // Merge the sublist onto the existing tail page.
            data_prev_tail = metadata.tail;
            data_new_rightlink = sublist.head;

            buffer = read_buffer(index, metadata.tail)?;
            lock_buffer(buffer, GIN_EXCLUSIVE)?;
            // START_CRIT_SECTION();
            bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                set_rightlink(page, sublist.head);
                Ok(())
            })?;
            bufmgr::mark_buffer_dirty::call(buffer);

            metadata.tail = sublist.tail;
            metadata.tailFreeSize = sublist.tailFreeSize;
            metadata.nPendingPages += sublist.nPendingPages;
            metadata.nPendingHeapTuples += sublist.nPendingHeapTuples;
            if need_wal {
                xlog_begin_insert()?;
                xlog_register_buffer(1, buffer, REGBUF_STANDARD)?;
            }
        }
    } else {
        // Append the collected tuples directly into the tail page (metabuffer is
        // already EXCLUSIVE-locked, metadata is current).
        check_for_serializable_conflict_in(index.rd_id, GIN_METAPAGE_BLKNO)?;

        buffer = read_buffer(index, metadata.tail)?;
        lock_buffer(buffer, GIN_EXCLUSIVE)?;

        data_ntuples = collector.ntuples() as i32;
        // START_CRIT_SECTION();
        metadata.nPendingHeapTuples += 1;

        let mut collected = Vec::with_capacity(collector.sumsize);
        bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            let off = {
                let pr = PageRef::new(page)?;
                if PageIsEmpty(&pr) {
                    FirstOffsetNumber
                } else {
                    PageGetMaxOffsetNumber(&pr) + 1
                }
            };
            // GinPageGetOpaque(page)->maxoff++.
            let cur_maxoff = gin_opaque_from_page(page)?.maxoff;
            set_maxoff(page, cur_maxoff + 1);

            collected.clear();
            let mut o = off;
            for t in &collector.tuples {
                let tupsize = index_tuple_size(t);
                let mut pm = PageMut::new(page)?;
                let l = backend_storage_page::PageAddItemExtended(&mut pm, &t[..tupsize], o, 0)?;
                if l == 0 {
                    return Err(PgError::error("failed to add item to index page"));
                }
                collected.extend_from_slice(&t[..tupsize]);
                o += 1;
            }
            // metadata->tailFreeSize = PageGetExactFreeSpace(page).
            let pr = PageRef::new(page)?;
            metadata.tailFreeSize =
                backend_storage_page::PageGetExactFreeSpace(&pr) as u32;
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
        if need_wal {
            xlog_begin_insert()?;
            xlog_register_buffer(1, buffer, REGBUF_STANDARD)?;
            data_buffer_image = collected;
        }
    }

    // Common tail: write the finalized metadata into the metapage.
    bufmgr::with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        write_meta(page, &metadata);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(metabuffer);

    if need_wal {
        // ginxlogUpdateMeta { RelFileLocator locator; GinMetaPageData metadata;
        //   BlockNumber prevTail; BlockNumber newRightlink; int32 ntuples; }
        if data_ntuples > 0 && !data_buffer_image.is_empty() {
            xlog_register_buf_data(1, &data_buffer_image)?;
        }
        let mut rec = Vec::new();
        rec.extend_from_slice(&relfilelocator_bytes(index));
        rec.extend_from_slice(&meta_to_bytes(&metadata));
        rec.extend_from_slice(&data_prev_tail.to_ne_bytes());
        rec.extend_from_slice(&data_new_rightlink.to_ne_bytes());
        rec.extend_from_slice(&data_ntuples.to_ne_bytes());
        xlog_register_buffer(0, metabuffer, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
        xlog_register_data(&rec)?;
        let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_UPDATE_META_PAGE)?;
        bufmgr::page_set_lsn::call(metabuffer, recptr)?;
        if buffer != InvalidBuffer {
            bufmgr::page_set_lsn::call(buffer, recptr)?;
        }
    }

    if buffer != InvalidBuffer {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    // Decide whether a cleanup is now warranted.
    let cleanup_size = gin_get_pending_list_cleanup_size(index)?;
    let need_cleanup =
        metadata.nPendingPages as usize * GIN_PAGE_FREESIZE > cleanup_size as usize * 1024;

    bufmgr::unlock_release_buffer::call(metabuffer);
    // END_CRIT_SECTION();

    if need_cleanup {
        ginInsertCleanup(ginstate, mcx, index, false, true, false, None)?;
    }
    Ok(())
}

// ===========================================================================
// ginHeapTupleFastCollect (ginfast.c:483)
// ===========================================================================

/// `ginHeapTupleFastCollect(ginstate, collector, attnum, value, isNull, ht_ctid)`
/// (ginfast.c:483): extract one indexable item's entries and append them, as
/// posting-tree-less leaf `IndexTuple`s carrying the heap TID in `t_tid`, to the
/// collector.
pub fn ginHeapTupleFastCollect<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    collector: &mut GinTupleCollector,
    attnum: OffsetNumber,
    value: Datum<'mcx>,
    is_null: bool,
    ht_ctid: &ItemPointerData,
) -> PgResult<()> {
    let (entries, categories) = ginExtractEntries(ginstate, attnum, value, is_null, mcx)?;
    let nentries = entries.len();

    if collector.ntuples() + nentries
        > MAX_ALLOC_SIZE / core::mem::size_of::<usize>()
    {
        return Err(PgError::error("too many entries for GIN index"));
    }

    collector.tuples.reserve(nentries);
    for i in 0..nentries {
        let mut itup = GinFormTuple(
            ginstate,
            mcx,
            attnum,
            entries[i].clone(),
            categories[i],
            None,
            0,
            0,
            true,
        )?
        .expect("posting-tree-less GinFormTuple never returns None (errorTooBig)");
        // itup->t_tid = *ht_ctid: stash the heap TID into the tuple header
        // (bytes 0..6 of the IndexTupleData: bi_hi, bi_lo, ip_posid).
        set_itup_tid(&mut itup, ht_ctid);
        collector.sumsize += index_tuple_size(&itup);
        collector.tuples.push(itup);
    }
    Ok(())
}

/// `itup->t_tid = *tid` over a tuple byte image (the 6-byte `ItemPointerData`
/// header at offset 0).
fn set_itup_tid(itup: &mut [u8], tid: &ItemPointerData) {
    itup[0..2].copy_from_slice(&tid.ip_blkid.bi_hi.to_ne_bytes());
    itup[2..4].copy_from_slice(&tid.ip_blkid.bi_lo.to_ne_bytes());
    itup[4..6].copy_from_slice(&tid.ip_posid.to_ne_bytes());
}

// ===========================================================================
// shiftList (ginfast.c:554)
// ===========================================================================

/// `shiftList(index, metabuffer, newHead, fill_fsm, stats)` (ginfast.c:554):
/// delete pending-list pages from the head up to (not including) `newHead`,
/// updating the metapage. `metabuffer` is pinned + exclusive-locked by the
/// caller throughout. Returns the number of pages deleted (C reports via
/// `stats->pages_deleted`; here returned for the caller to accumulate).
fn shiftList(
    index: &Relation<'_>,
    metabuffer: Buffer,
    new_head: BlockNumber,
    fill_fsm: bool,
    pages_deleted: &mut u32,
) -> PgResult<()> {
    let mut metadata = read_metabuffer(metabuffer)?;
    let mut blkno_to_delete = metadata.head;

    loop {
        let mut buffers: Vec<Buffer> = Vec::new();
        let mut freespace: Vec<BlockNumber> = Vec::new();
        let mut n_deleted_heap_tuples: i64 = 0;

        while buffers.len() < GIN_NDELETE_AT_ONCE && blkno_to_delete != new_head {
            freespace.push(blkno_to_delete);
            let b = read_buffer(index, blkno_to_delete)?;
            lock_buffer(b, GIN_EXCLUSIVE)?;
            let op = read_opaque(b)?;
            n_deleted_heap_tuples += op.maxoff as i64;
            blkno_to_delete = op.rightlink;
            buffers.push(b);
        }
        let ndeleted = buffers.len();

        *pages_deleted += ndeleted as u32;

        let need_wal = relation_needs_wal(index);
        if need_wal {
            xlog_ensure_record_space(ndeleted as i32, 0)?;
        }

        // START_CRIT_SECTION();
        metadata.head = blkno_to_delete;
        debug_assert!(metadata.nPendingPages >= ndeleted as u32);
        metadata.nPendingPages -= ndeleted as u32;
        debug_assert!(metadata.nPendingHeapTuples >= n_deleted_heap_tuples);
        metadata.nPendingHeapTuples -= n_deleted_heap_tuples;

        if blkno_to_delete == InvalidBlockNumber {
            metadata.tail = InvalidBlockNumber;
            metadata.tailFreeSize = 0;
            metadata.nPendingPages = 0;
            metadata.nPendingHeapTuples = 0;
        }

        bufmgr::with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
            write_meta(page, &metadata);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(metabuffer);

        for &b in &buffers {
            bufmgr::with_buffer_page::call(b, &mut |page: &mut [u8]| {
                set_flags(page, types_gin::GIN_DELETED);
                Ok(())
            })?;
            bufmgr::mark_buffer_dirty::call(b);
        }

        if need_wal {
            // ginxlogDeleteListPages { GinMetaPageData metadata; int32 ndeleted; }
            xlog_begin_insert()?;
            xlog_register_buffer(0, metabuffer, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
            for (i, &b) in buffers.iter().enumerate() {
                xlog_register_buffer((i + 1) as u8, b, REGBUF_WILL_INIT)?;
            }
            let mut rec = Vec::with_capacity(SIZE_OF_GIN_META_PAGE_DATA + 4);
            rec.extend_from_slice(&meta_to_bytes(&metadata));
            rec.extend_from_slice(&(ndeleted as i32).to_ne_bytes());
            xlog_register_data(&rec)?;
            let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_DELETE_LISTPAGE)?;
            bufmgr::page_set_lsn::call(metabuffer, recptr)?;
            for &b in &buffers {
                bufmgr::page_set_lsn::call(b, recptr)?;
            }
        }

        for &b in &buffers {
            bufmgr::unlock_release_buffer::call(b);
        }
        // END_CRIT_SECTION();

        if fill_fsm {
            for &fb in &freespace {
                fsm::record_free_index_page::call(index, fb)?;
            }
        }

        if blkno_to_delete == new_head {
            break;
        }
    }
    Ok(())
}

// ===========================================================================
// processPendingPage (ginfast.c:709)
// ===========================================================================

/// `processPendingPage(accum, ka, page, startoff)` (ginfast.c:709): collect all
/// pending tuples at offsets `>= startoff` on `page` into `accum`, grouping by
/// `(heap TID, attnum)`. `ka` is reusable workspace. Operates on an
/// already-locked page byte image.
fn processPendingPage<'mcx>(
    accum: &mut backend_access_gin_ginbulk::BuildAccumulator<'mcx>,
    ka: &mut KeyArray<'mcx>,
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    page: &[u8],
    startoff: OffsetNumber,
) -> PgResult<()> {
    ka.keys.clear();
    ka.categories.clear();

    let pr = PageRef::new(page)?;
    let maxoff = PageGetMaxOffsetNumber(&pr);
    debug_assert!(maxoff >= FirstOffsetNumber);

    let mut heapptr: Option<ItemPointerData> = None;
    let mut attrnum: OffsetNumber = 0;

    let mut i = startoff;
    while i <= maxoff {
        let iid = PageGetItemId(&pr, i)?;
        let itup = PageGetItem(&pr, &iid)?;

        let curattnum = gintuple_get_attrnum(ginstate, itup, mcx)?;
        let cur_tid = itup_tid(itup);

        match heapptr {
            None => {
                heapptr = Some(cur_tid);
                attrnum = curattnum;
            }
            Some(hp) => {
                if !(item_pointer_equals(&hp, &cur_tid) && curattnum == attrnum) {
                    accum.ginInsertBAEntries(&hp, attrnum, &ka.keys, &ka.categories)?;
                    ka.keys.clear();
                    ka.categories.clear();
                    heapptr = Some(cur_tid);
                    attrnum = curattnum;
                }
            }
        }

        let (curkey, curcategory) = gintuple_get_key(ginstate, itup, mcx)?;
        addDatum(ka, curkey, curcategory);

        i += 1;
    }

    if let Some(hp) = heapptr {
        accum.ginInsertBAEntries(&hp, attrnum, &ka.keys, &ka.categories)?;
    }
    Ok(())
}

// ===========================================================================
// ginInsertCleanup (ginfast.c:780)
// ===========================================================================

/// `ginInsertCleanup(ginstate, full_clean, fill_fsm, forceCleanup, stats)`
/// (ginfast.c:780): move tuples from the pending list into the regular GIN entry
/// tree. Returns the number of pages deleted (C reports via
/// `stats->pages_deleted`).
pub fn ginInsertCleanup<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    full_clean: bool,
    fill_fsm: bool,
    force_cleanup: bool,
    _stats: Option<()>,
) -> PgResult<u32> {
    let lock_rel = lock_rel_id(index);
    let mut pages_deleted: u32 = 0;

    let work_memory: i32 = if force_cleanup {
        // Called from [auto]vacuum/analyze or gin_clean_pending_list: wait for a
        // concurrent cleanup.
        LockPage(&lock_rel, GIN_METAPAGE_BLKNO, ExclusiveLock)?;
        if am_autovacuum_worker_process()? {
            let avwm = backend_utils_misc_guc_seams::autovacuum_work_mem::call();
            if avwm != -1 {
                avwm
            } else {
                backend_utils_misc_guc_seams::maintenance_work_mem::call()
            }
        } else {
            backend_utils_misc_guc_seams::maintenance_work_mem::call()
        }
    } else {
        // Regular insert: bail if a concurrent cleanup is running.
        if !ConditionalLockPage(&lock_rel, GIN_METAPAGE_BLKNO, ExclusiveLock)? {
            return Ok(0);
        }
        backend_utils_misc_guc_seams::work_mem::call()
    };

    let metabuffer = read_buffer(index, GIN_METAPAGE_BLKNO)?;
    lock_buffer(metabuffer, GIN_SHARE)?;
    let metadata = read_metabuffer(metabuffer)?;

    if metadata.head == InvalidBlockNumber {
        // Nothing to do.
        bufmgr::unlock_release_buffer::call(metabuffer);
        UnlockPage(&lock_rel, GIN_METAPAGE_BLKNO, ExclusiveLock)?;
        return Ok(0);
    }

    let blkno_finish = metadata.tail;
    let mut blkno = metadata.head;
    let mut buffer = read_buffer(index, blkno)?;
    lock_buffer(buffer, GIN_SHARE)?;
    lock_buffer(metabuffer, GIN_UNLOCK)?;

    let mut accum = backend_access_gin_ginbulk::new_accumulator(ginstate, mcx)?;
    let mut datums = initKeyArray(128);
    let mut cleanup_finish = false;
    #[allow(unused_assignments)]
    let mut fsm_vac = false;

    loop {
        // hold pin + SHARE on `buffer`; pin (no lock) on metabuffer.
        if blkno == blkno_finish && !full_clean {
            cleanup_finish = true;
        }

        // Read the page image (SHARE-locked) and process it.
        let (page_image, rightlink, has_full_row, maxoff) = read_listpage(buffer)?;
        processPendingPage(
            &mut accum,
            &mut datums,
            ginstate,
            mcx,
            &page_image,
            FirstOffsetNumber,
        )?;
        accum.take_cmp_error()?;
        vacuum_delay_point()?;

        // Flush decision.
        if rightlink == InvalidBlockNumber
            || (has_full_row
                && accum.allocated_memory() >= work_memory as usize * 1024)
        {
            // Unlock the page to improve concurrency (recheck maxoff later).
            lock_buffer(buffer, GIN_UNLOCK)?;

            // Flush the accumulator into the main index (unlocked).
            for e in accum.drain() {
                ginEntryInsert(
                    ginstate,
                    mcx,
                    index,
                    e.attnum,
                    e.key,
                    e.category,
                    &e.list,
                    e.list.len() as u32,
                    None,
                )?;
                vacuum_delay_point()?;
            }

            // Re-lock to remove pages.
            lock_buffer(metabuffer, GIN_EXCLUSIVE)?;
            lock_buffer(buffer, GIN_SHARE)?;

            // Process any newly-added entries while we were unlocked.
            let (page_image2, rightlink2, _hfr2, maxoff2) = read_listpage(buffer)?;
            if maxoff2 != maxoff {
                accum.reinit();
                processPendingPage(
                    &mut accum,
                    &mut datums,
                    ginstate,
                    mcx,
                    &page_image2,
                    maxoff + 1,
                )?;
                accum.take_cmp_error()?;
                for e in accum.drain() {
                    ginEntryInsert(
                        ginstate,
                        mcx,
                        index,
                        e.attnum,
                        e.key,
                        e.category,
                        &e.list,
                        e.list.len() as u32,
                        None,
                    )?;
                }
            }

            blkno = rightlink2;
            bufmgr::unlock_release_buffer::call(buffer);

            shiftList(index, metabuffer, blkno, fill_fsm, &mut pages_deleted)?;
            fsm_vac = true;

            lock_buffer(metabuffer, GIN_UNLOCK)?;

            if blkno == InvalidBlockNumber || cleanup_finish {
                break;
            }

            // Reset state for the next batch.
            datums = initKeyArray(datums.keys.capacity().max(1));
            accum.reinit();
        } else {
            // Not flushing; advance to the next page.
            blkno = rightlink;
            bufmgr::unlock_release_buffer::call(buffer);
        }

        // Read the next page.
        vacuum_delay_point()?;
        buffer = read_buffer(index, blkno)?;
        lock_buffer(buffer, GIN_SHARE)?;
    }

    UnlockPage(&lock_rel, GIN_METAPAGE_BLKNO, ExclusiveLock)?;
    bufmgr::release_buffer::call(metabuffer);

    if fsm_vac && fill_fsm {
        fsm::index_free_space_map_vacuum::call(index)?;
    }
    Ok(pages_deleted)
}

// ===========================================================================
// Helpers.
// ===========================================================================

/// Read the metapage image off a pinned metabuffer.
fn read_metabuffer(metabuffer: Buffer) -> PgResult<GinMetaPageData> {
    let mut out = GinMetaPageData::default();
    bufmgr::with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        out = read_meta(page)?;
        Ok(())
    })?;
    Ok(out)
}

/// `GinPageGetOpaque(BufferGetPage(buffer))`.
fn read_opaque(buffer: Buffer) -> PgResult<GinOpaque> {
    let mut out = GinOpaque::default();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = gin_opaque_from_page(page)?;
        Ok(())
    })?;
    Ok(out)
}

/// Read a pending-list page's full byte image plus its rightlink, full-row flag
/// and current max offset (the values `ginInsertCleanup` reads off the page).
fn read_listpage(buffer: Buffer) -> PgResult<(Vec<u8>, BlockNumber, bool, OffsetNumber)> {
    let mut image: Vec<u8> = Vec::new();
    let mut rightlink: BlockNumber = 0;
    let mut has_full_row = false;
    let mut maxoff: OffsetNumber = 0;
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        image = page.to_vec();
        let op = gin_opaque_from_page(page)?;
        rightlink = op.rightlink;
        has_full_row = op.flags & GIN_LIST_FULLROW != 0;
        let pr = PageRef::new(page)?;
        maxoff = PageGetMaxOffsetNumber(&pr);
        Ok(())
    })?;
    Ok((image, rightlink, has_full_row, maxoff))
}

/// `itup->t_tid` over a tuple byte image.
fn itup_tid(itup: &[u8]) -> ItemPointerData {
    use types_tuple::heaptuple::BlockIdData;
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([itup[0], itup[1]]),
            bi_lo: u16::from_ne_bytes([itup[2], itup[3]]),
        },
        ip_posid: u16::from_ne_bytes([itup[4], itup[5]]),
    }
}

/// `ItemPointerEquals(a, b)`.
fn item_pointer_equals(a: &ItemPointerData, b: &ItemPointerData) -> bool {
    a.ip_blkid.bi_hi == b.ip_blkid.bi_hi
        && a.ip_blkid.bi_lo == b.ip_blkid.bi_lo
        && a.ip_posid == b.ip_posid
}

/// Build the `LockRelId` for the index's relation-level page locks
/// (`rel->rd_lockInfo.lockRelId`): `relId = rd_id`, `dbId = rd_locator.dbOid`
/// (`InvalidOid` for a shared relation), as `RelationInitLockInfo` computes.
fn lock_rel_id(index: &Relation<'_>) -> LockRelId {
    LockRelId {
        relId: index.rd_id,
        dbId: index.rd_locator.dbOid,
    }
}

/// `index->rd_locator` serialized as the WAL `RelFileLocator`
/// (`{spcOid, dbOid, relNumber}`, three `Oid`/u32 fields).
fn relfilelocator_bytes(index: &Relation<'_>) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0..4].copy_from_slice(&index.rd_locator.spcOid.to_ne_bytes());
    b[4..8].copy_from_slice(&index.rd_locator.dbOid.to_ne_bytes());
    b[8..12].copy_from_slice(&index.rd_locator.relNumber.to_ne_bytes());
    b
}

/// `GinGetPendingListCleanupSize(index)` — the index's
/// `pendingListCleanupSize` reloption (falling back to `gin_pending_list_limit`),
/// resolved by `ginutil` (which owns `GinOptions`).
fn gin_get_pending_list_cleanup_size(index: &Relation<'_>) -> PgResult<i32> {
    backend_access_gin_ginutil_seams::gin_get_pending_list_cleanup_size::call(index)
}

fn relation_needs_wal(index: &Relation<'_>) -> bool {
    backend_utils_cache_relcache_seams::relation_needs_wal::call(index)
}

fn am_autovacuum_worker_process() -> PgResult<bool> {
    backend_access_heap_vacuumlazy_seams::am_autovacuum_worker_process::call()
}

fn vacuum_delay_point() -> PgResult<()> {
    backend_access_heap_vacuumlazy_seams::vacuum_delay_point::call(false)
}

fn check_for_serializable_conflict_in(index_oid: Oid, blkno: BlockNumber) -> PgResult<()> {
    backend_storage_lmgr_predicate_seams::check_for_serializable_conflict_in_page::call(
        index_oid, blkno,
    )
}

fn read_buffer<'mcx>(index: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer::call(index, blkno)
}

fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}

#[inline]
fn xlog_begin_insert() -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_begin_insert::call()
}
#[inline]
fn xlog_register_data(data: &[u8]) -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_register_data::call(data)
}
#[inline]
fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_register_buffer::call(block_id, buffer, flags)
}
#[inline]
fn xlog_register_buf_data(block_id: u8, data: &[u8]) -> PgResult<()> {
    backend_access_transam_xloginsert_seams::xlog_register_buf_data::call(block_id, data)
}
#[inline]
fn xlog_insert_record(rmid: types_core::RmgrId, info: u8) -> PgResult<types_core::XLogRecPtr> {
    backend_access_transam_xloginsert_seams::xlog_insert_record::call(rmid, info)
}
#[inline]
fn xlog_ensure_record_space(max_block_id: i32, ndatas: i32) -> PgResult<()> {
    backend_access_transam_xlog_seams::xlog_ensure_record_space::call(max_block_id, ndatas)
}
