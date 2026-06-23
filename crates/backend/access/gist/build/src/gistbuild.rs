//! Port of `src/backend/access/gist/gistbuild.c` (PostgreSQL 18.3): the GiST
//! index build driver.
//!
//! Two strategies (see the C module header):
//!  1. Sorted build (`gist_indexsortbuild`): all opclasses have sortsupport, so
//!     sort the tuples and pack them bottom-up into leaf pages.
//!  2. Insert build (`gistBuildCallback`): start empty and insert tuple by
//!     tuple, optionally using node buffers at intermediate levels to reduce I/O
//!     (the buffering build, [`crate::gistbuildbuffers`]).
//!
//! ## Memory model
//!
//! C creates `giststate->tempCxt = createTempGistContext()`, resetting it after
//! each tuple. In the owned model the per-tuple scratch rides the `mcx` thread
//! through, exactly as the sibling AM build drivers (spgbuild / brinbuild) do;
//! `createTempGistContext`/`freeGISTstate` are the `mcx` lifetime + drop. The
//! durable index data is written to the shared buffer pages / bulk writer, not
//! the temp context, so eliding the per-tuple reset is behavior-preserving.
//!
//! ## Build-mode reloption
//!
//! The C reads `(GiSTOptions *) index->rd_options` for both `fillfactor` and
//! `buffering_mode`. This repo's relcache carries only `StdRdOptions`
//! (`fillfactor`), so the GiST-specific `buffering_mode` is not available off
//! the relation. `fillfactor` is read via `RelationGetFillFactor`; the
//! `buffering` reloption (the rarely-used explicit on/off knob, used mainly for
//! testing) is treated as unset → the C `GIST_BUFFERING_AUTO` default path. This
//! is the one behaviour the GiST relcache-options carrier (a relcache keystone)
//! would restore.

extern crate alloc;

use mcx::Mcx;

use gist_core::gist_page::{
    gist_page_flags, gistcheckpage, gistfillbuffer, gistinitpage, set_gist_page_rightlink,
    GISTInitBuffer, GistPageIsLeaf,
};
use gist_core::gistutil::{
    gistCompressValues, gistFormTuple, gistNewBuffer, gistchoose, gistextractpage, gistfillitupvec,
    gistgetadjusted, gistjoinvector, gistunion, initGISTstate,
};
use gist_core::gist_insert::{gistdoinsert, gistplacetopage, gistSplit, PlaceToPage};

use indexam_seams as indexam;
use table_tableam_seams as tableam;
use parallel_rt_seams as parallel_rt;
use xloginsert_seams as xloginsert;
use bufmgr_seams as bufmgr;
use page::{
    PageAddItemExtended, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageMut, PageRef,
};
use bulkwrite_seams as bulk;
use miscinit_seams as miscinit;

use types_core::primitive::{
    AttrNumber, BlockNumber, ForkNumber, InvalidBlockNumber, OffsetNumber, RegProcedure, Size,
    BLCKSZ,
};
use types_error::{PgError, PgResult};
use gist::{
    GistSortedBuildLevelState, GISTSTATE, F_LEAF, GIST_DEFAULT_FILLFACTOR, GIST_ROOT_BLKNO,
    GIST_SORTED_BUILD_PAGE_NUM, GIST_SORTSUPPORT_PROC, GistBuildLSN, SplitPageLayout,
};
use rel::Relation;
use types_storage::buf::BUFFER_LOCK_EXCLUSIVE;
use types_storage::buf::BUFFER_LOCK_SHARE;
use types_storage::Buffer;
use types_tableam::amapi::IndexBuildResult;
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use crate::gistbuildbuffers::{
    gistFreeBuildBuffers, gistGetNodeBuffer, gistInitBuildBuffers, gistPopItupFromNodeBuffer,
    gistPushItupToNodeBuffer, gistRelocateBuildBuffersOnSplit, gistUnloadNodeBuffers,
    buffer_overflowed, index_tuple_size, level_has_buffers,
};

use gist::GISTBuildBuffers;

/// `BUFFERING_MODE_SWITCH_CHECK_STEP` (gistbuild.c:52) — step of index tuples
/// for checking whether to switch to buffering build mode.
const BUFFERING_MODE_SWITCH_CHECK_STEP: i64 = 256;

/// `BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET` (gistbuild.c:60) — # of tuples to
/// process before switching to buffering mode when buffering is explicitly on,
/// and between readjusting the buffer size while in buffering mode.
const BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET: i64 = 4096;

/// `SizeOfPageHeaderData` (bufpage.h).
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
/// `sizeof(GISTPageOpaqueData)` (gist.h).
const SIZEOF_GIST_PAGE_OPAQUE_DATA: usize = 16;
/// `sizeof(ItemIdData)` (itemid.h).
const SIZEOF_ITEM_ID_DATA: usize = 4;

/// `GistBuildMode` (gistbuild.c:67) — strategy used to build the index. It can
/// switch between the `GIST_BUFFERING_*` modes on the fly, but the sorted method
/// must be decided up front.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GistBuildMode {
    /// `GIST_SORTED_BUILD` — bottom-up build by sorting.
    SortedBuild,
    /// `GIST_BUFFERING_DISABLED` — regular build mode, won't switch.
    BufferingDisabled,
    /// `GIST_BUFFERING_AUTO` — regular build mode, may switch if the index grows
    /// too big.
    BufferingAuto,
    /// `GIST_BUFFERING_STATS` — gathering tuple-size statistics before switching
    /// to buffering build mode.
    BufferingStats,
    /// `GIST_BUFFERING_ACTIVE` — in buffering build mode.
    BufferingActive,
}

/// `GISTBuildState` (gistbuild.c:82) — working state for `gistbuild` and its
/// callback.
struct GISTBuildState<'mcx> {
    indexrel: Relation<'mcx>,
    heaprel: Relation<'mcx>,
    giststate: GISTSTATE<'mcx>,

    /// `Size freespace` — amount of free space to leave on pages.
    freespace: Size,

    buildMode: GistBuildMode,

    /// `int64 indtuples` — number of tuples indexed.
    indtuples: i64,

    /// `int64 indtuplesSize` — total size of all indexed tuples.
    indtuplesSize: i64,
    /// `GISTBuildBuffers *gfbb` — build-buffers state (buffering build).
    gfbb: Option<GISTBuildBuffers<'mcx>>,
    /// `HTAB *parentMap` — lookup table of the parent of each internal page.
    parentMap: std::collections::HashMap<BlockNumber, BlockNumber>,

    /// `Tuplesortstate *sortstate` — state data for tuplesort.c (sorted build).
    sortstate: Option<mcx::PgBox<'mcx, nodes::nodesort::Tuplesortstate<'mcx>>>,

    /// `BlockNumber pages_allocated` — # of pages allocated by the sorted build.
    pages_allocated: BlockNumber,

    /// `BulkWriteState *bulkstate` — the bulk writer (sorted build).
    bulkstate: Option<bulk::BulkWriteState<'mcx>>,

    /// The threaded allocation context (C `giststate->tempCxt`, and the build's
    /// working context).
    mcx: Mcx<'mcx>,
}

// ===========================================================================
// gistbuild (gistbuild.c:178)
// ===========================================================================

/// `gistbuild(heap, index, indexInfo)` (gistbuild.c:178): main entry point to
/// GiST index build.
pub fn gistbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut nodes::execnodes::IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    // We expect to be called exactly once for any index relation.
    if bufmgr::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        return Err(PgError::error(&alloc::format!(
            "index \"{}\" already contains data",
            index.name()
        )));
    }

    let giststate = initGISTstate(mcx, index)?;

    // Choose build strategy. First check whether the user specified to use
    // buffering mode. (See the module note: buffering_mode is not carried on the
    // repo relcache entry, so the explicit on/off knob is unavailable; treat as
    // the "auto" default.)
    let mut build_mode = GistBuildMode::BufferingAuto;

    // Unless buffering mode was forced (STATS), see if we can use sorting.
    if build_mode != GistBuildMode::BufferingStats {
        let mut hasallsortsupports = true;
        let keyscount = index.indnkeyatts();
        for i in 0..keyscount {
            let oid: RegProcedure = indexam::index_getprocid::call(
                index,
                (i + 1) as AttrNumber,
                GIST_SORTSUPPORT_PROC as u16,
            )?;
            if oid == 0 {
                // !OidIsValid
                hasallsortsupports = false;
                break;
            }
        }
        if hasallsortsupports {
            build_mode = GistBuildMode::SortedBuild;
        }
    }

    // Calculate target amount of free space to leave on pages.
    let fillfactor = index.get_fillfactor(GIST_DEFAULT_FILLFACTOR);
    let freespace = (BLCKSZ as i64 * (100 - fillfactor as i64) / 100) as Size;

    let mut state = GISTBuildState {
        indexrel: index.alias(),
        heaprel: heap.alias(),
        giststate,
        freespace,
        buildMode: build_mode,
        indtuples: 0,
        indtuplesSize: 0,
        gfbb: None,
        parentMap: std::collections::HashMap::new(),
        sortstate: None,
        pages_allocated: 0,
        bulkstate: None,
        mcx,
    };

    let reltuples;

    if state.buildMode == GistBuildMode::SortedBuild {
        // Sort all data, build the index from bottom up.
        let work_mem = vacuumlazy_seams::maintenance_work_mem::call()?;
        let sortstate = tuplesort_seams::tuplesort_begin_index_gist::call(
            mcx,
            heap,
            index,
            work_mem,
            nodes::nodesort::TUPLESORT_NONE,
        )?;
        state.sortstate = Some(mcx::alloc_in(mcx, sortstate)?);

        // Scan the table, adding all tuples to the tuplesort.
        reltuples = {
            let bs = &mut state;
            let index_alias = index.alias();
            tableam::table_index_build_scan::call(
                mcx,
                heap,
                index,
                index_info,
                true,
                true,
                &mut |tid: ItemPointerData,
                      values: &[Datum<'mcx>],
                      isnull: &[bool],
                      _tuple_is_alive: bool|
                      -> PgResult<()> {
                    gistSortedBuildCallback(mcx, &index_alias, &tid, values, isnull, bs)
                },
            )?
        };

        // Perform the sort and build index pages.
        tuplesort_seams::tuplesort_performsort::call(
            state.sortstate.as_mut().expect("sortstate set"),
        )?;

        gist_indexsortbuild(&mut state)?;

        tuplesort_seams::tuplesort_end::call(
            state.sortstate.take().expect("sortstate set"),
        )?;
    } else {
        // Initialize an empty index and insert all tuples, possibly using
        // buffers on intermediate levels.

        // Initialize the root page.
        let buffer = gistNewBuffer(mcx, index, heap)?;
        debug_assert_eq!(bufmgr::buffer_get_block_number::call(buffer), GIST_ROOT_BLKNO);

        miscinit::start_crit_section::call();

        GISTInitBuffer(buffer, F_LEAF)?;

        bufmgr::mark_buffer_dirty::call(buffer);
        bufmgr::page_set_lsn::call(buffer, GistBuildLSN)?;

        bufmgr::unlock_release_buffer::call(buffer);

        miscinit::end_crit_section::call();

        // Scan the table, inserting all the tuples to the index.
        reltuples = {
            let bs = &mut state;
            let index_alias = index.alias();
            tableam::table_index_build_scan::call(
                mcx,
                heap,
                index,
                index_info,
                true,
                true,
                &mut |tid: ItemPointerData,
                      values: &[Datum<'mcx>],
                      isnull: &[bool],
                      _tuple_is_alive: bool|
                      -> PgResult<()> {
                    gistBuildCallback(mcx, &index_alias, &tid, values, isnull, bs)
                },
            )?
        };

        // If buffering was used, flush out all the tuples still in the buffers.
        if state.buildMode == GistBuildMode::BufferingActive {
            // elog(DEBUG1, "all tuples processed, emptying buffers")
            gistEmptyAllBuffers(&mut state)?;
            if let Some(gfbb) = state.gfbb.as_mut() {
                gistFreeBuildBuffers(gfbb)?;
            }
        }

        // We didn't write WAL records as we built the index, so if WAL-logging
        // is required, write all pages to the WAL now.
        if relcache_seams::relation_needs_wal::call(index) {
            let nblocks =
                bufmgr::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?;
            xloginsert::log_newpage_range::call(index, ForkNumber::MAIN_FORKNUM, 0, nblocks, true)?;
        }
    }

    // okay, all heap tuples are indexed.
    // MemoryContextDelete(tempCxt) + freeGISTstate(giststate): the owned model
    // drops the GISTSTATE and the per-tuple scratch with `state`.

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: state.indtuples as f64,
    })
}

// ===========================================================================
// gistSortedBuildCallback (gistbuild.c:365)
// ===========================================================================

/// `gistSortedBuildCallback(index, tid, values, isnull, tupleIsAlive, state)`
/// (gistbuild.c:365): per-tuple callback for the sorted-build
/// `table_index_build_scan`.
fn gistSortedBuildCallback<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    tid: &ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    state: &mut GISTBuildState<'mcx>,
) -> PgResult<()> {
    // Form an index tuple and point it at the heap tuple.
    let compressed_values = gistCompressValues(mcx, &state.giststate, index, values, isnull, true)?;

    tuplesort_seams::tuplesort_putindextuplevalues::call(
        state.sortstate.as_mut().expect("sortstate set"),
        &state.indexrel,
        *tid,
        &compressed_values,
        isnull,
    )?;

    // Update tuple count.
    state.indtuples += 1;
    Ok(())
}

// ===========================================================================
// gist_indexsortbuild (gistbuild.c:399)
// ===========================================================================

/// `gist_indexsortbuild(state)` (gistbuild.c:399): build a GiST index bottom-up
/// from pre-sorted tuples.
fn gist_indexsortbuild<'mcx>(state: &mut GISTBuildState<'mcx>) -> PgResult<()> {
    // Reserve block 0 for the root page.
    state.pages_allocated = 1;

    state.bulkstate = Some(bulk::smgr_bulk_start_rel::call(
        state.mcx,
        &state.indexrel,
        ForkNumber::MAIN_FORKNUM,
    )?);

    // Allocate a temporary buffer for the first leaf page batch.
    let mut first_page: [Option<alloc::vec::Vec<u8>>; GIST_SORTED_BUILD_PAGE_NUM as usize] =
        [None, None, None, None];
    first_page[0] = Some({
        let mut p = alloc::vec![0u8; BLCKSZ];
        gistinitpage(&mut p, F_LEAF)?;
        p
    });
    let mut levelstate = alloc::boxed::Box::new(GistSortedBuildLevelState {
        current_page: 0,
        last_blkno: 0,
        parent: None,
        pages: first_page,
    });

    // Fill index pages with tuples in the sorted order.
    while let Some(itup) = tuplesort_seams::tuplesort_getindextuple::call(
        state.sortstate.as_mut().expect("sortstate set"),
        true,
    )? {
        gist_indexsortbuild_levelstate_add(state, &mut levelstate, &itup)?;
    }

    // Write out the partially full non-root pages. Flush can build a new root;
    // if the number of pages is > 1 then a new root is required.
    while levelstate.parent.is_some() || levelstate.current_page != 0 {
        gist_indexsortbuild_levelstate_flush(state, &mut levelstate)?;
        let parent = levelstate.parent.take();
        // pfree the pages / the levelstate: dropped here.
        match parent {
            Some(p) => levelstate = p,
            None => break,
        }
    }

    // Write out the root.
    {
        let root_page = levelstate.pages[0].as_mut().expect("root page present");
        set_page_lsn_bytes(root_page, GistBuildLSN);
        let mut rootbuf = bulk::smgr_bulk_get_buf::call(
            state.mcx,
            state.bulkstate.as_mut().expect("bulkstate set"),
        )?;
        rootbuf.clear();
        rootbuf.extend_from_slice(root_page);
        bulk::smgr_bulk_write::call(
            state.bulkstate.as_mut().expect("bulkstate set"),
            GIST_ROOT_BLKNO,
            rootbuf,
            true,
        )?;
    }

    bulk::smgr_bulk_finish::call(state.bulkstate.take().expect("bulkstate set"))?;
    Ok(())
}

/// `PageSetLSN(page, lsn)` (bufpage.h) over an owned page byte image: the LSN is
/// the 8-byte `pd_lsn` at offset 0 (`PageXLogRecPtr`), stored as
/// `(xlogid:u32, xrecoff:u32)`.
fn set_page_lsn_bytes(page: &mut [u8], lsn: types_core::XLogRecPtr) {
    let xlogid = (lsn >> 32) as u32;
    let xrecoff = (lsn & 0xffff_ffff) as u32;
    page[0..4].copy_from_slice(&xlogid.to_ne_bytes());
    page[4..8].copy_from_slice(&xrecoff.to_ne_bytes());
}

// ===========================================================================
// gist_indexsortbuild_levelstate_add (gistbuild.c:460)
// ===========================================================================

/// `gist_indexsortbuild_levelstate_add(state, levelstate, itup)`
/// (gistbuild.c:460): add a tuple to a page; if the pages are full, flush them
/// and re-initialize a new page.
fn gist_indexsortbuild_levelstate_add<'mcx>(
    state: &mut GISTBuildState<'mcx>,
    levelstate: &mut GistSortedBuildLevelState,
    itup: &[u8],
) -> PgResult<()> {
    // Check if the tuple can be added to the current page (fillfactor ignored).
    let size_needed = index_tuple_size(itup) + SIZEOF_ITEM_ID_DATA;
    let free = {
        let cur = levelstate.current_page as usize;
        let page = levelstate.pages[cur].as_ref().expect("current page present");
        PageGetFreeSpace(&PageRef::new(page)?)
    };
    if free < size_needed {
        let old_page_flags = {
            let cur = levelstate.current_page as usize;
            gist_page_flags(levelstate.pages[cur].as_ref().expect("current page present"))?
        };

        if levelstate.current_page + 1 == GIST_SORTED_BUILD_PAGE_NUM {
            gist_indexsortbuild_levelstate_flush(state, levelstate)?;
        } else {
            levelstate.current_page += 1;
        }

        let cur = levelstate.current_page as usize;
        if levelstate.pages[cur].is_none() {
            levelstate.pages[cur] = Some(alloc::vec![0u8; BLCKSZ]);
        }
        gistinitpage(levelstate.pages[cur].as_mut().expect("new page present"), old_page_flags)?;
    }

    let cur = levelstate.current_page as usize;
    let page = levelstate.pages[cur].as_mut().expect("current page present");
    gistfillbuffer(page, &[itup.to_vec()], INVALID_OFFSET_NUMBER)?;
    Ok(())
}

/// `InvalidOffsetNumber` (off.h).
const INVALID_OFFSET_NUMBER: OffsetNumber = 0;
/// `FirstOffsetNumber` (off.h).
const FIRST_OFFSET_NUMBER: OffsetNumber = 1;

// ===========================================================================
// gist_indexsortbuild_levelstate_flush (gistbuild.c:492)
// ===========================================================================

/// `gist_indexsortbuild_levelstate_flush(state, levelstate)` (gistbuild.c:492):
/// flush the buffered pages of one level, applying picksplit and writing the
/// result pages, then inserting the downlinks into the parent (creating a new
/// root if needed).
fn gist_indexsortbuild_levelstate_flush<'mcx>(
    state: &mut GISTBuildState<'mcx>,
    levelstate: &mut GistSortedBuildLevelState,
) -> PgResult<()> {
    parallel_rt::check_for_interrupts::call()?;

    let isleaf = GistPageIsLeaf(levelstate.pages[0].as_ref().expect("page 0 present"))?;

    // Get index tuples from the first page.
    let mut itvec = gistextractpage(state.mcx, levelstate.pages[0].as_ref().expect("page 0"))?;
    let dist: alloc::vec::Vec<SplitPageLayout<'mcx>>;
    if levelstate.current_page > 0 {
        // Append tuples from each page.
        for i in 1..(levelstate.current_page + 1) as usize {
            let itvec_local = gistextractpage(state.mcx, levelstate.pages[i].as_ref().expect("page"))?;
            gistjoinvector(&mut itvec, &itvec_local)?;
        }

        // Apply picksplit to the list of all collected tuples.
        let itrefs: alloc::vec::Vec<&[u8]> = itvec.iter().map(|v| v.as_slice()).collect();
        dist = gistSplit(
            state.mcx,
            &state.indexrel,
            // C passes `levelstate->pages[0]`, an in-memory page not yet
            // assigned a block number; gistSplit/gistSplitByKey only use it to
            // stamp `GISTENTRY.page`, which is irrelevant during the bottom-up
            // sorted build, so the unassigned-block sentinel is faithful.
            InvalidBlockNumber,
            &itrefs,
            &state.giststate,
        )?;
    } else {
        // Create split layout from a single page.
        let itrefs: alloc::vec::Vec<&[u8]> = itvec.iter().map(|v| v.as_slice()).collect();
        let union_tuple = gistunion(state.mcx, &state.indexrel, &itrefs, &state.giststate)?;
        let list = gistfillitupvec(state.mcx, &itrefs)?;
        let lenlist = list.len() as i32;
        let num = itvec.len() as i32;
        dist = alloc::vec![SplitPageLayout {
            block: gist::gistxlogPage { blkno: 0, num },
            list,
            lenlist,
            itup: Some(union_tuple),
            page: 0,
            buffer: 0,
        }];
    }

    // Reset page counter.
    levelstate.current_page = 0;

    // Create pages for all partitions in the split result.
    for mut d in dist {
        // check once per page.
        parallel_rt::check_for_interrupts::call()?;

        // Create page and copy data.
        let mut buf = bulk::smgr_bulk_get_buf::call(
            state.mcx,
            state.bulkstate.as_mut().expect("bulkstate set"),
        )?;
        // Reinterpret the bulk-write page as a GiST page.
        let mut target = alloc::vec![0u8; BLCKSZ];
        gistinitpage(&mut target, if isleaf { F_LEAF } else { 0 })?;

        // Place each tuple from the concatenated `list` image onto the page.
        let mut data_off = 0usize;
        {
            let mut pmut = PageMut::new(&mut target)?;
            let mut i = 0i32;
            while i < d.block.num {
                let thistup_sz = index_tuple_size(&d.list[data_off..]);
                let thistup = &d.list[data_off..data_off + thistup_sz];
                let l = PageAddItemExtended(&mut pmut, thistup, (i + FIRST_OFFSET_NUMBER as i32) as OffsetNumber, 0)?;
                if l == INVALID_OFFSET_NUMBER {
                    return Err(PgError::error(&alloc::format!(
                        "failed to add item to index page in \"{}\"",
                        state.indexrel.name()
                    )));
                }
                data_off += thistup_sz;
                i += 1;
            }
        }

        // Set the right link to point to the previous page (debugging aid).
        if levelstate.last_blkno != 0 {
            set_gist_page_rightlink(&mut target, levelstate.last_blkno)?;
        }

        // The page is complete. Assign a block number to it, and pass it to the
        // bulk writer.
        let blkno = state.pages_allocated;
        state.pages_allocated += 1;
        set_page_lsn_bytes(&mut target, GistBuildLSN);
        buf.clear();
        buf.extend_from_slice(&target);
        bulk::smgr_bulk_write::call(
            state.bulkstate.as_mut().expect("bulkstate set"),
            blkno,
            buf,
            true,
        )?;

        // union_tuple->t_tid block number = blkno.
        let mut union_tuple = d.itup.take().expect("split layout has a union tuple");
        itup_set_block_number(&mut union_tuple, blkno);
        levelstate.last_blkno = blkno;

        // Insert the downlink to the parent page. If this was the root, create a
        // new page as the parent, which becomes the new root.
        if levelstate.parent.is_none() {
            let mut p = alloc::vec![0u8; BLCKSZ];
            gistinitpage(&mut p, 0)?;
            let mut parent_pages: [Option<alloc::vec::Vec<u8>>; GIST_SORTED_BUILD_PAGE_NUM as usize] =
                [None, None, None, None];
            parent_pages[0] = Some(p);
            levelstate.parent = Some(alloc::boxed::Box::new(GistSortedBuildLevelState {
                current_page: 0,
                last_blkno: 0,
                parent: None,
                pages: parent_pages,
            }));
        }
        let parent = levelstate.parent.as_mut().expect("parent present");
        gist_indexsortbuild_levelstate_add(state, parent, &union_tuple)?;
    }

    Ok(())
}

/// `ItemPointerSetBlockNumber(&itup->t_tid, blkno)` over an on-disk index tuple
/// byte image: writes the block number into the leading `BlockIdData` (`bi_hi`,
/// `bi_lo` halves) of `t_tid`.
fn itup_set_block_number(itup: &mut [u8], blkno: BlockNumber) {
    let bi_hi = (blkno >> 16) as u16;
    let bi_lo = (blkno & 0xffff) as u16;
    itup[0..2].copy_from_slice(&bi_hi.to_ne_bytes());
    itup[2..4].copy_from_slice(&bi_lo.to_ne_bytes());
}

// ===========================================================================
// gistInitBuffering (gistbuild.c:625)
// ===========================================================================

/// `gistInitBuffering(buildstate)` (gistbuild.c:625): attempt to switch to
/// buffering mode. If there's not enough memory, sets the mode to
/// `BufferingDisabled`; otherwise initializes the build buffers and sets the
/// mode to `BufferingActive`.
fn gistInitBuffering<'mcx>(state: &mut GISTBuildState<'mcx>) -> PgResult<()> {
    let index = state.indexrel.alias();

    // Calc space of index page available for index tuples.
    let page_free_space = (BLCKSZ
        - SIZE_OF_PAGE_HEADER_DATA
        - SIZEOF_GIST_PAGE_OPAQUE_DATA
        - SIZEOF_ITEM_ID_DATA
        - state.freespace) as f64;

    // Average size of already inserted index tuples (gathered statistics).
    let itup_avg_size = state.indtuplesSize as f64 / state.indtuples as f64;

    // Minimal possible size of index tuple by index metadata. Minimal possible
    // size of varlena is VARHDRSZ.
    const SIZEOF_INDEX_TUPLE_DATA: usize = 8;
    const VARHDRSZ: usize = 4;
    let mut itup_min_size = maxalign(SIZEOF_INDEX_TUPLE_DATA) as f64;
    let natts = index.rd_att.natts as usize;
    for i in 0..natts {
        let attlen = index.rd_att.compact_attr(i).attlen;
        if attlen < 0 {
            itup_min_size += VARHDRSZ as f64;
        } else {
            itup_min_size += attlen as f64;
        }
    }

    // Average and maximal number of index tuples that fit on a page.
    let avg_index_tuples_per_page = page_free_space / itup_avg_size;
    let max_index_tuples_per_page = page_free_space / itup_min_size;

    let effective_cache_size =
        guc_tables::vars::effective_cache_size.read() as f64;
    let maintenance_work_mem =
        vacuumlazy_seams::maintenance_work_mem::call()? as f64;

    // Calculate levelStep: the highest level step such that a subtree still fits
    // in cache (see the C comment for the derivation).
    let mut level_step = 1i32;
    loop {
        // size of an average subtree at this levelStep (in pages).
        let subtreesize = (1.0 - libm::pow(avg_index_tuples_per_page, (level_step + 1) as f64))
            / (1.0 - avg_index_tuples_per_page);

        // max number of pages at the lowest level of a subtree.
        let maxlowestlevelpages = libm::pow(max_index_tuples_per_page, level_step as f64);

        // subtree must fit in cache (with safety factor of 4).
        if subtreesize > effective_cache_size / 4.0 {
            break;
        }

        // each node in the lowest level of a subtree has one page in memory.
        if maxlowestlevelpages > (maintenance_work_mem * 1024.0) / BLCKSZ as f64 {
            break;
        }

        // Good, we can handle this levelStep. See if we can go one higher.
        level_step += 1;
    }

    // We just reached an unacceptable value of levelStep; decrease it to the
    // last acceptable value.
    level_step -= 1;

    // If there's not enough cache or maintenance_work_mem, fall back to plain
    // inserts.
    if level_step <= 0 {
        // elog(DEBUG1, "failed to switch to buffered GiST build")
        state.buildMode = GistBuildMode::BufferingDisabled;
        return Ok(());
    }

    // pagesPerBuffer (recalculated also during the build).
    let pages_per_buffer = calculatePagesPerBuffer(state, level_step);

    // Initialize GISTBuildBuffers with these parameters.
    let max_level = gistGetMaxLevel(state.mcx, &index)?;
    state.gfbb = Some(gistInitBuildBuffers(
        state.mcx,
        pages_per_buffer,
        level_step,
        max_level,
    )?);

    gistInitParentMap(state);

    state.buildMode = GistBuildMode::BufferingActive;
    // elog(DEBUG1, "switched to buffered GiST build; ...")
    Ok(())
}

/// `MAXALIGN(x)` (c.h).
#[inline]
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

// ===========================================================================
// calculatePagesPerBuffer (gistbuild.c:788)
// ===========================================================================

/// `calculatePagesPerBuffer(buildstate, levelStep)` (gistbuild.c:788): calculate
/// the `pagesPerBuffer` parameter for the buffering algorithm.
fn calculatePagesPerBuffer<'mcx>(state: &GISTBuildState<'mcx>, level_step: i32) -> i32 {
    // Calc space of index page available for index tuples.
    let page_free_space = (BLCKSZ
        - SIZE_OF_PAGE_HEADER_DATA
        - SIZEOF_GIST_PAGE_OPAQUE_DATA
        - SIZEOF_ITEM_ID_DATA
        - state.freespace) as f64;

    // Average size of already inserted index tuples.
    let itup_avg_size = state.indtuplesSize as f64 / state.indtuples as f64;
    let avg_index_tuples_per_page = page_free_space / itup_avg_size;

    // Recalculate required size of buffers.
    let pages_per_buffer = 2.0 * libm::pow(avg_index_tuples_per_page, level_step as f64);

    libm::rint(pages_per_buffer) as i32
}

// ===========================================================================
// gistBuildCallback (gistbuild.c:821)
// ===========================================================================

/// `gistBuildCallback(index, tid, values, isnull, tupleIsAlive, state)`
/// (gistbuild.c:821): per-tuple callback for the insert-build
/// `table_index_build_scan`.
fn gistBuildCallback<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    tid: &ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    state: &mut GISTBuildState<'mcx>,
) -> PgResult<()> {
    // form an index tuple and point it at the heap tuple.
    let mut itup = gistFormTuple(mcx, &state.giststate, index, values, isnull, true)?;
    // itup->t_tid = *tid: t_tid is the leading 6-byte ItemPointerData.
    let tid_bytes = item_pointer_to_bytes(tid);
    itup[0..6].copy_from_slice(&tid_bytes);

    // Update tuple count and total size.
    state.indtuples += 1;
    state.indtuplesSize += index_tuple_size(&itup) as i64;

    if state.buildMode == GistBuildMode::BufferingActive {
        // We have buffers, so use them.
        gistBufferingBuildInsert(state, &itup)?;
    } else {
        // There are no buffers (yet). Since we already hold the index relation
        // lock, call gistdoinsert directly.
        gistdoinsert(
            mcx,
            index,
            &itup,
            state.freespace,
            &state.giststate,
            &state.heaprel,
            true,
        )?;
    }

    if state.buildMode == GistBuildMode::BufferingActive
        && state.indtuples % BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0
    {
        // Adjust the target buffer size now.
        let level_step = state.gfbb.as_ref().expect("gfbb set").levelStep;
        let pages_per_buffer = calculatePagesPerBuffer(state, level_step);
        state.gfbb.as_mut().expect("gfbb set").pagesPerBuffer = pages_per_buffer;
    }

    // In 'auto' mode, check if the index has grown too large to fit in cache and
    // switch to buffering mode. To avoid excessive smgrnblocks() calls, only
    // check every BUFFERING_MODE_SWITCH_CHECK_STEP tuples. In 'stats' state,
    // switch as soon as we have seen enough tuples to estimate the average size.
    let effective_cache_size =
        guc_tables::vars::effective_cache_size.read();
    let auto_switch = state.buildMode == GistBuildMode::BufferingAuto
        && state.indtuples % BUFFERING_MODE_SWITCH_CHECK_STEP == 0
        && (effective_cache_size as i64)
            < smgrnblocks_main(index)? as i64;
    let stats_switch = state.buildMode == GistBuildMode::BufferingStats
        && state.indtuples >= BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET;
    if auto_switch || stats_switch {
        // Index doesn't fit in effective cache anymore. Try to switch to
        // buffering build mode.
        gistInitBuffering(state)?;
    }

    Ok(())
}

/// `smgrnblocks(RelationGetSmgr(index), MAIN_FORKNUM)` via the smgr seam: read
/// off the relation's physical identity.
fn smgrnblocks_main(index: &Relation<'_>) -> PgResult<BlockNumber> {
    smgr_seams::smgrnblocks::call(
        index.rd_locator,
        index.rd_backend,
        ForkNumber::MAIN_FORKNUM,
    )
}

/// `*tid` -> the 6-byte on-disk `ItemPointerData` image
/// (`BlockIdData ip_blkid` [bi_hi, bi_lo], `OffsetNumber ip_posid`).
fn item_pointer_to_bytes(tid: &ItemPointerData) -> [u8; 6] {
    let mut out = [0u8; 6];
    out[0..2].copy_from_slice(&tid.ip_blkid.bi_hi.to_ne_bytes());
    out[2..4].copy_from_slice(&tid.ip_blkid.bi_lo.to_ne_bytes());
    out[4..6].copy_from_slice(&tid.ip_posid.to_ne_bytes());
    out
}

// ===========================================================================
// gistBufferingBuildInsert (gistbuild.c:908)
// ===========================================================================

/// `gistBufferingBuildInsert(buildstate, itup)` (gistbuild.c:908): insert
/// function for the buffering index build.
fn gistBufferingBuildInsert<'mcx>(state: &mut GISTBuildState<'mcx>, itup: &[u8]) -> PgResult<()> {
    // Insert the tuple to buffers.
    let rootlevel = state.gfbb.as_ref().expect("gfbb set").rootlevel;
    gistProcessItup(state, itup, 0, rootlevel)?;

    // If we filled up (half of a) buffer, process buffer emptying.
    gistProcessEmptyingQueue(state)?;
    Ok(())
}

// ===========================================================================
// gistProcessItup (gistbuild.c:924)
// ===========================================================================

/// `gistProcessItup(buildstate, itup, startblkno, startlevel)`
/// (gistbuild.c:924): run a tuple down the tree until a leaf page or node
/// buffer, inserting it there. Returns `true` if buffer emptying should stop
/// (a child buffer can no longer take tuples).
fn gistProcessItup<'mcx>(
    state: &mut GISTBuildState<'mcx>,
    itup: &[u8],
    startblkno: BlockNumber,
    startlevel: i32,
) -> PgResult<bool> {
    parallel_rt::check_for_interrupts::call()?;

    let mut result = false;
    let mut downlinkoffnum = INVALID_OFFSET_NUMBER;
    let mut parentblkno = InvalidBlockNumber;

    // Loop until we reach a leaf page (level == 0) or a level with buffers (not
    // including the start level, else we'd make no progress).
    let mut blkno = startblkno;
    let mut level = startlevel;
    loop {
        // Have we reached a level with buffers?
        let has_buffers = level_has_buffers(level, state.gfbb.as_ref().expect("gfbb set"));
        if has_buffers && level != startlevel {
            break;
        }
        // Have we reached a leaf page?
        if level == 0 {
            break;
        }

        // Descend to the next level. Choose a child to descend to.
        let buffer = bufmgr::read_buffer::call(&state.indexrel, blkno)?;
        bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;

        let page = bufmgr::buffer_get_page::call(state.mcx, buffer)?;
        let childoffnum = gistchoose(state.mcx, &state.indexrel, &page, itup, &state.giststate)?;
        let (childblkno, idxtuple) = {
            let pref = PageRef::new(&page)?;
            let iid = PageGetItemId(&pref, childoffnum)?;
            let it = PageGetItem(&pref, &iid)?;
            (item_pointer_block_from_itup(it), mcx::slice_in(state.mcx, it)?)
        };

        if level > 1 {
            gistMemorizeParent(state, childblkno, blkno);
        }

        // Check that the key for the target child node is consistent with the
        // key we're inserting; update it if not.
        let newtup = gistgetadjusted(state.mcx, &state.indexrel, &idxtuple, itup, &state.giststate)?;
        if let Some(newtup) = newtup {
            let newtup_refs: [&[u8]; 1] = [&newtup];
            blkno = gistbufferinginserttuples(
                state,
                buffer,
                level,
                &newtup_refs,
                childoffnum,
                InvalidBlockNumber,
                INVALID_OFFSET_NUMBER,
            )?;
            // gistbufferinginserttuples() released the buffer.
        } else {
            bufmgr::unlock_release_buffer::call(buffer);
        }

        // Descend to the child.
        parentblkno = blkno;
        blkno = childblkno;
        downlinkoffnum = childoffnum;
        debug_assert!(level > 0);
        level -= 1;
    }

    if level_has_buffers(level, state.gfbb.as_ref().expect("gfbb set")) {
        // Reached a level with buffers. Place the index tuple to the buffer, and
        // add the buffer to the emptying queue if it overflows.
        let child_node_buffer = {
            let gfbb = state.gfbb.as_mut().expect("gfbb set");
            gistGetNodeBuffer(gfbb, blkno, level)
        };

        // Add index tuple to it.
        {
            let gfbb = state.gfbb.as_mut().expect("gfbb set");
            gistPushItupToNodeBuffer(gfbb, &child_node_buffer, itup)?;
        }

        let gfbb = state.gfbb.as_ref().expect("gfbb set");
        if buffer_overflowed(&child_node_buffer.borrow(), gfbb) {
            result = true;
        }
    } else {
        // Reached a leaf page. Place the tuple here.
        debug_assert_eq!(level, 0);
        let buffer = bufmgr::read_buffer::call(&state.indexrel, blkno)?;
        bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
        let itup_refs: [&[u8]; 1] = [itup];
        gistbufferinginserttuples(
            state,
            buffer,
            level,
            &itup_refs,
            INVALID_OFFSET_NUMBER,
            parentblkno,
            downlinkoffnum,
        )?;
        // gistbufferinginserttuples() released the buffer.
    }

    Ok(result)
}

/// `ItemPointerGetBlockNumber(&idxtuple->t_tid)` from an on-disk index tuple
/// byte image: the block number from `t_tid`'s `BlockIdData` (`bi_hi`, `bi_lo`).
fn item_pointer_block_from_itup(itup: &[u8]) -> BlockNumber {
    let bi_hi = u16::from_ne_bytes([itup[0], itup[1]]) as u32;
    let bi_lo = u16::from_ne_bytes([itup[2], itup[3]]) as u32;
    (bi_hi << 16) | bi_lo
}

// ===========================================================================
// gistbufferinginserttuples (gistbuild.c:1055)
// ===========================================================================

/// `gistbufferinginserttuples(buildstate, buffer, level, itup, ntup, oldoffnum,
/// parentblk, downlinkoffnum)` (gistbuild.c:1055): insert tuples to a page
/// (analogous to gistinserttuples in the regular insertion code). Returns the
/// block number where the first new/updated tuple landed. Caller must hold a
/// lock on `buffer`; this unlocks and unpins it.
#[allow(clippy::too_many_arguments)]
fn gistbufferinginserttuples<'mcx>(
    state: &mut GISTBuildState<'mcx>,
    buffer: Buffer,
    level: i32,
    itup: &[&[u8]],
    oldoffnum: OffsetNumber,
    parentblk: BlockNumber,
    downlinkoffnum: OffsetNumber,
) -> PgResult<BlockNumber> {
    let mut parentblk = parentblk;
    let mut downlinkoffnum = downlinkoffnum;

    let place = {
        let index_alias = state.indexrel.alias();
        let heap_alias = state.heaprel.alias();
        gistplacetopage(PlaceToPage {
            rel: &index_alias,
            freespace: state.freespace,
            giststate: &state.giststate,
            buffer,
            itup,
            oldoffnum,
            left_child_buf: INVALID_BUFFER,
            mark_follow_right: false,
            heap_rel: &heap_alias,
            is_build: true,
        })?
    };
    let is_split = place.is_split;
    let mut splitinfo = place.split_info;
    let placed_to_blk = place.new_blkno;

    // If this is a root split, update the in-memory root path item. This keeps
    // all path stacks complete up to the root, simplifying re-finding the parent.
    if is_split && bufmgr::buffer_get_block_number::call(buffer) == GIST_ROOT_BLKNO {
        debug_assert_eq!(level, state.gfbb.as_ref().expect("gfbb set").rootlevel);
        state.gfbb.as_mut().expect("gfbb set").rootlevel += 1;
        // elog(DEBUG2, "splitting GiST root page, now %d levels deep")

        // All downlinks on the old root page are now on one of the child pages.
        // Visit all the new child pages to memorize the grandchildren's parents.
        if state.gfbb.as_ref().expect("gfbb set").rootlevel > 1 {
            let page = bufmgr::buffer_get_page::call(state.mcx, buffer)?;
            let maxoff = {
                let pref = PageRef::new(&page)?;
                PageGetMaxOffsetNumber(&pref)
            };
            let mut off = FIRST_OFFSET_NUMBER;
            while off <= maxoff {
                let childblkno = {
                    let pref = PageRef::new(&page)?;
                    let iid = PageGetItemId(&pref, off)?;
                    let it = PageGetItem(&pref, &iid)?;
                    item_pointer_block_from_itup(it)
                };
                let childbuf = bufmgr::read_buffer::call(&state.indexrel, childblkno)?;
                bufmgr::lock_buffer::call(childbuf, BUFFER_LOCK_SHARE)?;
                gistMemorizeAllDownlinks(state, childbuf)?;
                bufmgr::unlock_release_buffer::call(childbuf);

                // Also remember that the parent of the new child page is the root
                // block.
                gistMemorizeParent(state, childblkno, GIST_ROOT_BLKNO);
                off += 1;
            }
        }
    }

    if !splitinfo.is_empty() {
        // Insert the downlinks to the parent (analogous to gistfinishsplit, but
        // simpler locking, and we maintain the internal-node buffers and the
        // parent map).

        // Parent may have changed since we memorized this path.
        let parent_buffer =
            gistBufferingFindCorrectParent(
                state,
                bufmgr::buffer_get_block_number::call(buffer),
                level,
                &mut parentblk,
                &mut downlinkoffnum,
            )?;

        // If there's a buffer associated with this page, it needs to be split
        // too. gistRelocateBuildBuffersOnSplit also adjusts the downlinks in
        // splitinfo so they're consistent with the tuples in the buffers.
        {
            let index_alias = state.indexrel.alias();
            // SAFETY: split the gfbb / giststate borrows from `state`.
            let mcx = state.mcx;
            // Need &mut gfbb and &giststate simultaneously; both live in `state`.
            let GISTBuildState {
                gfbb, giststate, ..
            } = state;
            let gfbb = gfbb.as_mut().expect("gfbb set");
            gistRelocateBuildBuffersOnSplit(
                mcx,
                gfbb,
                giststate,
                &index_alias,
                level,
                buffer,
                &mut splitinfo,
            )?;
        }

        // Create an array of all the downlink tuples and update the parent map.
        let ndownlinks = splitinfo.len();
        let mut downlinks: alloc::vec::Vec<mcx::PgVec<'mcx, u8>> =
            alloc::vec::Vec::with_capacity(ndownlinks);
        for si in splitinfo.into_iter() {
            // Remember the parent of each new child page in the parent map. (If
            // the parent page splits when we recurse up to insert the downlinks,
            // the recursive call updates the map again.)
            if level > 0 {
                gistMemorizeParent(
                    state,
                    bufmgr::buffer_get_block_number::call(si.buf),
                    bufmgr::buffer_get_block_number::call(parent_buffer),
                );
            }

            // Also update the parent map for all the downlinks that got moved to
            // a different page. (This also loops the downlinks that stayed, but
            // it does no harm.)
            if level > 1 {
                gistMemorizeAllDownlinks(state, si.buf)?;
            }

            // No concurrent access, so release the lower-level buffers
            // immediately (including the original page).
            bufmgr::unlock_release_buffer::call(si.buf);
            downlinks.push(si.downlink);
        }

        // Insert them into the parent.
        let downlink_refs: alloc::vec::Vec<&[u8]> = downlinks.iter().map(|v| v.as_slice()).collect();
        gistbufferinginserttuples(
            state,
            parent_buffer,
            level + 1,
            &downlink_refs,
            downlinkoffnum,
            InvalidBlockNumber,
            INVALID_OFFSET_NUMBER,
        )?;
        // list_free_deep(splitinfo): owned vectors dropped here.
    } else {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    Ok(placed_to_blk)
}

/// `InvalidBuffer` (buf.h).
const INVALID_BUFFER: Buffer = 0;

// ===========================================================================
// gistBufferingFindCorrectParent (gistbuild.c:1224)
// ===========================================================================

/// `gistBufferingFindCorrectParent(buildstate, childblkno, level, parentblkno,
/// downlinkoffnum)` (gistbuild.c:1224): find the downlink pointing to a child
/// page. Returns the parent buffer (exclusively-locked), updating `*parentblkno`
/// / `*downlinkoffnum` to the real location.
fn gistBufferingFindCorrectParent<'mcx>(
    state: &mut GISTBuildState<'mcx>,
    childblkno: BlockNumber,
    level: i32,
    parentblkno: &mut BlockNumber,
    downlinkoffnum: &mut OffsetNumber,
) -> PgResult<Buffer> {
    let parent: BlockNumber;
    if level > 0 {
        parent = gistGetParent(state, childblkno)?;
    } else {
        // For a leaf page, the caller must supply a correct parent block number.
        if *parentblkno == InvalidBlockNumber {
            return Err(PgError::error(&alloc::format!(
                "no parent buffer provided of child {childblkno}"
            )));
        }
        parent = *parentblkno;
    }

    let buffer = bufmgr::read_buffer::call(&state.indexrel, parent)?;
    bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
    gistcheckpage(state.indexrel.name(), buffer)?;
    let page = bufmgr::buffer_get_page::call(state.mcx, buffer)?;
    let maxoff = {
        let pref = PageRef::new(&page)?;
        PageGetMaxOffsetNumber(&pref)
    };

    // Check if it was not moved.
    if parent == *parentblkno
        && *parentblkno != InvalidBlockNumber
        && *downlinkoffnum != INVALID_OFFSET_NUMBER
        && *downlinkoffnum <= maxoff
    {
        let pref = PageRef::new(&page)?;
        let iid = PageGetItemId(&pref, *downlinkoffnum)?;
        let it = PageGetItem(&pref, &iid)?;
        if item_pointer_block_from_itup(it) == childblkno {
            // Still there.
            return Ok(buffer);
        }
    }

    // Downlink was not at the offset where it used to be. Scan the page to find
    // it. (During a buffering build we track each page's parent, so we should
    // always know what page it's on.)
    let mut off = FIRST_OFFSET_NUMBER;
    while off <= maxoff {
        let pref = PageRef::new(&page)?;
        let iid = PageGetItemId(&pref, off)?;
        let it = PageGetItem(&pref, &iid)?;
        if item_pointer_block_from_itup(it) == childblkno {
            // Found it.
            *downlinkoffnum = off;
            return Ok(buffer);
        }
        off += 1;
    }

    Err(PgError::error(&alloc::format!(
        "failed to re-find parent for block {childblkno}"
    )))
}

// ===========================================================================
// gistProcessEmptyingQueue (gistbuild.c:1298)
// ===========================================================================

/// `gistProcessEmptyingQueue(buildstate)` (gistbuild.c:1298): process the buffer
/// emptying stack until it is empty (cascading emptying).
fn gistProcessEmptyingQueue<'mcx>(state: &mut GISTBuildState<'mcx>) -> PgResult<()> {
    // Iterate while we have elements in the buffer emptying stack.
    while !state.gfbb.as_ref().expect("gfbb set").bufferEmptyingQueue.is_empty() {
        // Get a node buffer from the emptying stack (linitial + delete_first).
        let emptying_node_buffer = {
            let gfbb = state.gfbb.as_mut().expect("gfbb set");
            gfbb.bufferEmptyingQueue.remove(0)
        };
        emptying_node_buffer.borrow_mut().queuedForEmptying = false;

        // We're going to load last pages of the buffers we empty to. Unload any
        // previously loaded buffers.
        {
            let gfbb = state.gfbb.as_mut().expect("gfbb set");
            gistUnloadNodeBuffers(gfbb)?;
        }

        // Pop tuples from the buffer and run them down to lower-level buffers /
        // leaf pages, until one of the lower-level buffers fills up or this
        // buffer runs empty.
        let (node_blocknum, buf_level) = {
            let nb = emptying_node_buffer.borrow();
            (nb.nodeBlocknum, nb.level)
        };
        loop {
            // Get the next index tuple from the buffer.
            let itup = {
                let mcx = state.mcx;
                let gfbb = state.gfbb.as_mut().expect("gfbb set");
                gistPopItupFromNodeBuffer(mcx, gfbb, &emptying_node_buffer)?
            };
            let itup = match itup {
                Some(it) => it,
                None => break,
            };

            // Run it down to the underlying node buffer or leaf page. (The buffer
            // we're emptying may split as a result; emptyingNodeBuffer then points
            // to the left half. We keep flushing from it.)
            if gistProcessItup(state, &itup, node_blocknum, buf_level)? {
                // A lower-level buffer filled up. Stop to avoid overflowing it.
                break;
            }
        }
    }
    Ok(())
}

// ===========================================================================
// gistEmptyAllBuffers (gistbuild.c:1371)
// ===========================================================================

/// `gistEmptyAllBuffers(buildstate)` (gistbuild.c:1371): empty all node buffers,
/// from top to bottom, to flush all remaining tuples at the end of the build.
fn gistEmptyAllBuffers<'mcx>(state: &mut GISTBuildState<'mcx>) -> PgResult<()> {
    // Iterate through the levels from top to bottom.
    let buffers_on_levels_len = state.gfbb.as_ref().expect("gfbb set").buffersOnLevels.len();
    for i in (0..buffers_on_levels_len).rev() {
        // Empty all buffers on this level. New buffers can pop up in the list
        // during processing (from page splits), so a simple walk won't work. We
        // remove a buffer from the list when we see it empty; it can't become
        // non-empty once fully emptied.
        loop {
            let node_buffer = {
                let gfbb = state.gfbb.as_ref().expect("gfbb set");
                match gfbb.buffersOnLevels[i].first() {
                    Some(nb) => alloc::rc::Rc::clone(nb),
                    None => break,
                }
            };

            if node_buffer.borrow().blocksCount != 0 {
                // Add this buffer to the emptying queue, and proceed to empty it.
                if !node_buffer.borrow().queuedForEmptying {
                    node_buffer.borrow_mut().queuedForEmptying = true;
                    let gfbb = state.gfbb.as_mut().expect("gfbb set");
                    gfbb.bufferEmptyingQueue.insert(0, alloc::rc::Rc::clone(&node_buffer));
                }
                gistProcessEmptyingQueue(state)?;
            } else {
                let gfbb = state.gfbb.as_mut().expect("gfbb set");
                gfbb.buffersOnLevels[i].remove(0);
            }
        }
        // elog(DEBUG2, "emptied all buffers at level %d", i)
    }
    Ok(())
}

// ===========================================================================
// gistGetMaxLevel (gistbuild.c:1426)
// ===========================================================================

/// `gistGetMaxLevel(index)` (gistbuild.c:1426): get the depth of the GiST index
/// by traversing down from the root to the leaf level.
fn gistGetMaxLevel<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<i32> {
    let mut max_level = 0;
    let mut blkno = GIST_ROOT_BLKNO;
    loop {
        let buffer = bufmgr::read_buffer::call(index, blkno)?;

        // No concurrent access during index build, so locking is just pro forma.
        bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;

        if GistPageIsLeaf(&page)? {
            // We hit the bottom, so we're done.
            bufmgr::unlock_release_buffer::call(buffer);
            break;
        }

        // Pick the first downlink on the page and follow it (depth is uniform).
        blkno = {
            let pref = PageRef::new(&page)?;
            let iid = PageGetItemId(&pref, FIRST_OFFSET_NUMBER)?;
            let it = PageGetItem(&pref, &iid)?;
            item_pointer_block_from_itup(it)
        };
        bufmgr::unlock_release_buffer::call(buffer);

        // Going down the tree means one more level.
        max_level += 1;
    }
    Ok(max_level)
}

// ===========================================================================
// Parent map (gistbuild.c:1515)
// ===========================================================================

/// `gistInitParentMap(buildstate)` (gistbuild.c:1515): initialize the parent map
/// hash table. (The owned `HashMap` is created with `gfbb`; this clears it.)
fn gistInitParentMap<'mcx>(state: &mut GISTBuildState<'mcx>) {
    state.parentMap.clear();
}

/// `gistMemorizeParent(buildstate, child, parent)` (gistbuild.c:1529): remember
/// `parent` as the parent of `child` in the parent map (HASH_ENTER).
fn gistMemorizeParent<'mcx>(state: &mut GISTBuildState<'mcx>, child: BlockNumber, parent: BlockNumber) {
    state.parentMap.insert(child, parent);
}

/// `gistMemorizeAllDownlinks(buildstate, parentbuf)` (gistbuild.c:1545): scan all
/// downlinks on a page and memorize their parent.
fn gistMemorizeAllDownlinks<'mcx>(state: &mut GISTBuildState<'mcx>, parentbuf: Buffer) -> PgResult<()> {
    let parentblkno = bufmgr::buffer_get_block_number::call(parentbuf);
    let page = bufmgr::buffer_get_page::call(state.mcx, parentbuf)?;

    debug_assert!(!GistPageIsLeaf(&page)?);

    let maxoff = {
        let pref = PageRef::new(&page)?;
        PageGetMaxOffsetNumber(&pref)
    };
    let mut off = FIRST_OFFSET_NUMBER;
    while off <= maxoff {
        let childblkno = {
            let pref = PageRef::new(&page)?;
            let iid = PageGetItemId(&pref, off)?;
            let it = PageGetItem(&pref, &iid)?;
            item_pointer_block_from_itup(it)
        };
        gistMemorizeParent(state, childblkno, parentblkno);
        off += 1;
    }
    Ok(())
}

/// `gistGetParent(buildstate, child)` (gistbuild.c:1566): the parent of `child`
/// from the parent map (HASH_FIND); errors if not found.
fn gistGetParent<'mcx>(state: &GISTBuildState<'mcx>, child: BlockNumber) -> PgResult<BlockNumber> {
    match state.parentMap.get(&child) {
        Some(parent) => Ok(*parent),
        None => Err(PgError::error(&alloc::format!(
            "could not find parent of block {child} in lookup table"
        ))),
    }
}

// ===========================================================================
// gistbuildempty (gist.c) — declared in gistbuild's IndexAmRoutine slot
// ===========================================================================

/// `gistbuildempty(index)` (gist.c:139) — build an empty GiST index in the
/// initialization fork. GiST has no metapage, so this just initializes and
/// WAL-logs a single empty leaf root page.
pub fn gistbuildempty<'mcx>(_mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<()> {
    // Initialize the root page: ExtendBufferedRel(BMR_REL(index), INIT_FORKNUM,
    // NULL, EB_SKIP_EXTENSION_LOCK | EB_LOCK_FIRST). The seam bakes in those
    // flags.
    let buffer = bufmgr::extend_buffered_rel::call(index, ForkNumber::INIT_FORKNUM)?;

    // Initialize and xlog the buffer.
    miscinit::start_crit_section::call();
    GISTInitBuffer(buffer, F_LEAF)?;
    bufmgr::mark_buffer_dirty::call(buffer);
    xloginsert::log_newpage_buffer::call(buffer, true)?;
    miscinit::end_crit_section::call();

    // Unlock and release the buffer.
    bufmgr::unlock_release_buffer::call(buffer);
    Ok(())
}
