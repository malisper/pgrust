//! brin.c build lane (brinbuild + build state + brinsummarize), split from
//! the dispatch crate the way nbtsort is: execindexing sits between indexam
//! and the AM.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::bufmgr_seams::{
    buffer_get_block_number, buffer_get_page, extend_buffered_rel_by, lock_buffer,
    mark_buffer_dirty, release_buffer, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK, EB_LOCK_FIRST,
    EB_SKIP_EXTENSION_LOCK,
};
use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_brin::*;
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, RmgrIds};
use ::types_error::{PgError, PgResult};
use ::types_rel::{AccessShareLock, Relation};
use ::types_storage::buf::BufferAccessStrategy;
use ::types_storage::bufpage::{PageMut, PageRef};
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumber};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use brin::{add_values_to_range, brin_build_desc, brin_get_pages_per_range, union_tuples};
use brin_pageops::{
    brinGetTupleForHeapBlock, brinRevmapInitialize, brinRevmapTerminate,
    brin_can_do_samepage_update, brin_doinsert, brin_doupdate, brin_metapage_init,
    brin_page_cleanup, relation_needs_wal,
};
use brin_tuple::{
    brin_form_placeholder_tuple, brin_form_tuple, brin_memtuple_initialize, brin_new_memtuple,
};

pub struct BrinBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

struct BrinBuildState<'a, 'mcx> {
    bs_irel: &'a Relation<'mcx>,
    bs_numtuples: f64,
    bs_currentInsertBuf: Buffer,
    bs_pagesPerRange: BlockNumber,
    bs_currRangeStart: BlockNumber,
    bs_maxRangeStart: BlockNumber,
    bs_rmAccess: &'a BrinRevmap,
    bs_bdesc: BrinDesc<'mcx>,
    bs_dtuple: BrinMemTuple,
    bs_emptyTuple: Option<PgVec<'mcx, u8>>,
}

fn initialize_brin_buildstate<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    idxRel: &'a Relation<'mcx>,
    revmap: &'a BrinRevmap,
    pagesPerRange: BlockNumber,
    tablePages: BlockNumber,
) -> PgResult<BrinBuildState<'a, 'mcx>> {
    let bdesc = brin_build_desc(mcx, idxRel)?;
    let dtuple = brin_new_memtuple(&bdesc);
    let last_range = if tablePages > 0 {
        ((tablePages - 1) / pagesPerRange) * pagesPerRange
    } else {
        0
    };
    Ok(BrinBuildState {
        bs_irel: idxRel,
        bs_numtuples: 0.0,
        bs_currentInsertBuf: InvalidBuffer,
        bs_pagesPerRange: pagesPerRange,
        bs_currRangeStart: 0,
        // wrapping: the summarize lane passes tablePages=InvalidBlockNumber
        // and never reads bs_maxRangeStart (C wraps the same u32 add).
        bs_maxRangeStart: last_range.wrapping_add(pagesPerRange),
        bs_rmAccess: revmap,
        bs_bdesc: bdesc,
        bs_dtuple: dtuple,
        bs_emptyTuple: None,
    })
}

// terminate_brin_buildstate: give the last insert page's free space to the
// FSM.
fn terminate_brin_buildstate(state: BrinBuildState<'_, '_>) -> PgResult<()> {
    if state.bs_currentInsertBuf != InvalidBuffer {
        // SAFETY: pinned; unlocked freespace read, as C.
        let page =
            unsafe { PageRef::from_raw(buffer_get_page::call(state.bs_currentInsertBuf)) };
        let freesp = page.free_space();
        let blk = buffer_get_block_number::call(state.bs_currentInsertBuf);
        release_buffer::call(state.bs_currentInsertBuf)?;
        freespace::RecordPageWithFreeSpace(state.bs_irel, blk, freesp)?;
        freespace::FreeSpaceMapVacuumRange(state.bs_irel, blk, blk + 1)?;
    }
    Ok(())
}

/// brinbuild (serial; index_build is repo-wide serial).
pub fn brinbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut execindexing::IndexInfo<'mcx>,
) -> PgResult<BrinBuildResult> {
    if bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        index,
        ForkNumber::MAIN_FORKNUM,
    )? != 0
    {
        panic!("index \"{}\" already contains data", index.name());
    }

    let pagesPerRange = brin_get_pages_per_range(index);

    let (meta, _n) = extend_buffered_rel_by::call(
        index,
        ForkNumber::MAIN_FORKNUM,
        None,
        EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK,
        1,
    )?;
    debug_assert!(buffer_get_block_number::call(meta) == BRIN_METAPAGE_BLKNO);

    {
        // SAFETY: pinned + exclusively locked (EB_LOCK_FIRST).
        let mut page = unsafe { PageMut::from_raw(buffer_get_page::call(meta)) };
        brin_metapage_init(&mut page, pagesPerRange, BRIN_CURRENT_VERSION);
        mark_buffer_dirty::call(meta)?;

        if relation_needs_wal(index) {
            let xlrec = xl_brin_createidx(pagesPerRange, BRIN_CURRENT_VERSION);
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RmgrIds::RM_BRIN_ID as u8,
                XLOG_BRIN_CREATE_INDEX,
                0,
                &[&xlrec],
                &[XLogRegBuf {
                    block_id: 0,
                    buffer: meta,
                    flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                    bufdata: &[],
                }],
            )?;
            page.set_lsn(recptr);
        }
    }
    lock_buffer::call(meta, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(meta)?;

    let (revmap, pagesPerRange) = brinRevmapInitialize(index)?;
    let heap_blocks = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        heap,
        ForkNumber::MAIN_FORKNUM,
    )?;
    let mut state = initialize_brin_buildstate(mcx, index, &revmap, pagesPerRange, heap_blocks)?;

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        false,
        |_index_rel, tid, values, isnull, _tuple_is_alive| {
            brinbuildCallback(&mut state, tid, values, isnull)
        },
    )?;

    form_and_insert_tuple(&mut state)?;

    let (prev, max) = (state.bs_currRangeStart, state.bs_maxRangeStart);
    brin_fill_empty_ranges(mcx, &mut state, prev, max)?;

    let idxtuples = state.bs_numtuples;
    terminate_brin_buildstate(state)?;
    brinRevmapTerminate(revmap)?;

    Ok(BrinBuildResult { heap_tuples: reltuples, index_tuples: idxtuples })
}

// brinbuildCallback (serial arm).
fn brinbuildCallback(
    state: &mut BrinBuildState<'_, '_>,
    tid: &ItemPointerData,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<()> {
    let thisblock = ItemPointerGetBlockNumber(tid);

    while thisblock > state.bs_currRangeStart + state.bs_pagesPerRange - 1 {
        form_and_insert_tuple(state)?;
        state.bs_currRangeStart += state.bs_pagesPerRange;
        brin_memtuple_initialize(&mut state.bs_dtuple, &state.bs_bdesc);
    }

    add_values_to_range(&state.bs_bdesc, &mut state.bs_dtuple, values, isnull)?;
    Ok(())
}

/// brinbuildempty (brin.c): unlogged indexes' INIT_FORKNUM metapage.
pub fn brinbuildempty(index: &Relation<'_>) -> PgResult<()> {
    let (metabuf, _n) = extend_buffered_rel_by::call(
        index,
        ForkNumber::INIT_FORKNUM,
        None,
        EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK,
        1,
    )?;
    init_small::globals::StartCriticalSection();
    {
        // SAFETY: pinned + exclusively locked (EB_LOCK_FIRST).
        let mut page = unsafe { PageMut::from_raw(buffer_get_page::call(metabuf)) };
        brin_metapage_init(&mut page, brin_get_pages_per_range(index), BRIN_CURRENT_VERSION);
    }
    mark_buffer_dirty::call(metabuf)?;
    ::xloginsert_seams::log_newpage_buffer::call(metabuf, true)?;
    init_small::globals::EndCriticalSection();
    lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(metabuf)?;
    Ok(())
}

fn form_and_insert_tuple(state: &mut BrinBuildState<'_, '_>) -> PgResult<()> {
    // Minmax-multi serialization leaves scratch-dead datums in bs_dtuple
    // bv_values[0]: reinitialize before any read (C brin_memtuple_initialize).
    let scratch = MemoryContext::new_bump("brin form tuple");
    let tup = brin_form_tuple(
        scratch.mcx(),
        &state.bs_bdesc,
        state.bs_currRangeStart,
        &mut state.bs_dtuple,
    )?;
    brin_doinsert(
        state.bs_irel,
        state.bs_pagesPerRange,
        &state.bs_rmAccess,
        &mut state.bs_currentInsertBuf,
        state.bs_currRangeStart,
        &tup,
    )?;
    state.bs_numtuples += 1.0;
    drop(tup);
    Ok(())
}

// brin_fill_empty_ranges + brin_build_empty_tuple: the empty tuple is built
// once (in the build's mcx) and its blkno patched per range.
fn brin_fill_empty_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut BrinBuildState<'_, 'mcx>,
    prevRange: BlockNumber,
    nextRange: BlockNumber,
) -> PgResult<()> {
    let mut blkno = if prevRange == InvalidBlockNumber {
        0
    } else {
        prevRange + state.bs_pagesPerRange
    };

    while blkno < nextRange {
        if state.bs_emptyTuple.is_none() {
            let mut dtuple = brin_new_memtuple(&state.bs_bdesc);
            state.bs_emptyTuple =
                Some(brin_form_tuple(mcx, &state.bs_bdesc, blkno, &mut dtuple)?);
        }

        let BrinBuildState {
            bs_irel,
            bs_pagesPerRange,
            bs_currentInsertBuf,
            bs_rmAccess,
            bs_emptyTuple,
            bs_numtuples,
            ..
        } = state;
        let tup = bs_emptyTuple.as_mut().expect("built above");
        brin_tuple_set_blkno(tup, blkno);
        brin_doinsert(
            bs_irel,
            *bs_pagesPerRange,
            bs_rmAccess,
            bs_currentInsertBuf,
            blkno,
            tup,
        )?;
        *bs_numtuples += 1.0;

        blkno += state.bs_pagesPerRange;
    }
    Ok(())
}

// summarize_range (brin.c): placeholder tuple first, so insertions concurrent
// with the scan update it; then union the scan result with the (possibly
// concurrently-updated) placeholder in a retry loop.
fn summarize_range<'mcx>(
    mcx: Mcx<'mcx>,
    indexInfo: &mut execindexing::IndexInfo<'mcx>,
    state: &mut BrinBuildState<'_, 'mcx>,
    heapRel: &Relation<'mcx>,
    heapBlk: BlockNumber,
    heapNumBlks: BlockNumber,
) -> PgResult<()> {
    let scratch = MemoryContext::new_bump("brin summarize");

    let mut phbuf: Buffer = InvalidBuffer;
    let mut phtup = brin_form_placeholder_tuple(scratch.mcx(), &state.bs_bdesc, heapBlk)?;
    let mut offset = brin_doinsert(
        state.bs_irel,
        state.bs_pagesPerRange,
        state.bs_rmAccess,
        &mut phbuf,
        heapBlk,
        &phtup,
    )?;

    debug_assert!(heapBlk % state.bs_pagesPerRange == 0);
    let scanNumBlks = if heapBlk + state.bs_pagesPerRange > heapNumBlks {
        // Possibly-partial final range: recompute the table size after the
        // placeholder insert (later extenders update the placeholder), and
        // clamp — the table may have grown beyond this range already.
        core::cmp::min(
            bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                heapRel,
                ForkNumber::MAIN_FORKNUM,
            )? - heapBlk,
            state.bs_pagesPerRange,
        )
    } else {
        state.bs_pagesPerRange
    };

    state.bs_currRangeStart = heapBlk;
    let irel = state.bs_irel;
    // C's "any visible" mode: in-progress tuples must not be missed.
    execindexing::table_index_build_range_scan(
        mcx,
        heapRel,
        irel,
        indexInfo,
        false,
        true,
        heapBlk,
        scanNumBlks,
        |_index_rel, tid, values, isnull, _tuple_is_alive| {
            brinbuildCallback(state, tid, values, isnull)
        },
    )?;

    loop {
        postgres_seams::check_for_interrupts::call()?;

        let form = MemoryContext::new_bump("brin summarize newtup");
        let newtup =
            brin_form_tuple(form.mcx(), &state.bs_bdesc, heapBlk, &mut state.bs_dtuple)?;
        let samepage = brin_can_do_samepage_update(phbuf, phtup.len(), newtup.len());
        let didupdate = brin_doupdate(
            state.bs_irel,
            state.bs_pagesPerRange,
            state.bs_rmAccess,
            heapBlk,
            phbuf,
            offset,
            &phtup,
            &newtup,
            samepage,
        )?;
        if didupdate {
            break;
        }

        let got = brinGetTupleForHeapBlock(
            state.bs_irel,
            state.bs_rmAccess,
            heapBlk,
            &mut phbuf,
            &mut phtup,
            BUFFER_LOCK_SHARE,
            false,
        )?;
        let Some(off) = got else {
            return Err(Box::new(PgError::error("missing placeholder tuple")));
        };
        offset = off;

        union_tuples(&state.bs_bdesc, &mut state.bs_dtuple, &phtup)?;
    }

    release_buffer::call(phbuf)?;
    Ok(())
}

/// brinsummarize (brin.c): summarize unsummarized ranges. pageRange
/// BRIN_ALL_BLOCKRANGES walks the whole table; include_partial also
/// summarizes the partial range at the end of the table.
pub fn brinsummarize<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    heapRel: &Relation<'mcx>,
    pageRange: BlockNumber,
    include_partial: bool,
    mut numSummarized: Option<&mut f64>,
    mut numExisting: Option<&mut f64>,
) -> PgResult<()> {
    let (revmap, pagesPerRange) = brinRevmapInitialize(index)?;

    let mut heapNumBlocks = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        heapRel,
        ForkNumber::MAIN_FORKNUM,
    )?;
    let mut startBlk = if pageRange == BRIN_ALL_BLOCKRANGES {
        0
    } else {
        let s = (pageRange / pagesPerRange) * pagesPerRange;
        // wrapping: user-reachable via brin_summarize_range(idx, 4294967294);
        // C wraps the u32 add, clamping heapNumBlocks to 0 -> early return.
        heapNumBlocks = core::cmp::min(heapNumBlocks, s.wrapping_add(pagesPerRange));
        s
    };
    if startBlk > heapNumBlocks {
        brinRevmapTerminate(revmap)?;
        return Ok(());
    }

    let mut state: Option<BrinBuildState<'_, 'mcx>> = None;
    let mut indexInfo: Option<execindexing::IndexInfo<'mcx>> = None;
    let mut buf: Buffer = InvalidBuffer;
    let tupcxt = MemoryContext::new_bump("brinsummarize tup");
    let mut tup: PgVec<'_, u8> = PgVec::new_in(tupcxt.mcx());

    while startBlk < heapNumBlocks {
        if !include_partial && startBlk + pagesPerRange > heapNumBlocks {
            break;
        }
        postgres_seams::check_for_interrupts::call()?;

        let got = brinGetTupleForHeapBlock(
            index,
            &revmap,
            startBlk,
            &mut buf,
            &mut tup,
            BUFFER_LOCK_SHARE,
            false,
        )?;
        if got.is_none() {
            // No revmap entry for this heap range: summarize it.
            if state.is_none() {
                debug_assert!(indexInfo.is_none());
                state = Some(initialize_brin_buildstate(
                    mcx,
                    index,
                    &revmap,
                    pagesPerRange,
                    InvalidBlockNumber,
                )?);
                indexInfo = Some(execindexing::BuildIndexInfo(mcx, index)?);
            }
            let st = state.as_mut().expect("initialized above");
            summarize_range(
                mcx,
                indexInfo.as_mut().expect("initialized above"),
                st,
                heapRel,
                startBlk,
                heapNumBlocks,
            )?;

            brin_memtuple_initialize(&mut st.bs_dtuple, &st.bs_bdesc);

            if let Some(n) = numSummarized.as_deref_mut() {
                *n += 1.0;
            }
        } else if let Some(n) = numExisting.as_deref_mut() {
            *n += 1.0;
        }

        startBlk += pagesPerRange;
    }

    if buf != InvalidBuffer {
        release_buffer::call(buf)?;
    }
    // C terminates the revmap before the buildstate; the state borrows the
    // revmap here, so give the FSM its page first — pins/locks are disjoint.
    if let Some(st) = state.take() {
        terminate_brin_buildstate(st)?;
    }
    brinRevmapTerminate(revmap)?;
    Ok(())
}

pub fn init_seams() {
    indexam_seams::brin_vacuum_cleanup::set(brinvacuumcleanup);
}

/// brin_vacuum_scan (brin.c): physical-order pass repairing uncataloged
/// pages (lost to a crash after extension) and refreshing the FSM.
fn brin_vacuum_scan(idxrel: &Relation<'_>, strategy: &BufferAccessStrategy) -> PgResult<()> {
    let nblocks = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        idxrel,
        ForkNumber::MAIN_FORKNUM,
    )?;
    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;
        let buf = bufmgr_seams::read_buffer_extended::call(
            idxrel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            strategy.clone(),
        )?;
        brin_page_cleanup(idxrel, buf)?;
        release_buffer::call(buf)?;
    }
    // Upper FSM pages too: propagates brin_page_cleanup's leaf updates and
    // repairs pre-existing out-of-dateness.
    freespace::FreeSpaceMapVacuum(idxrel)
}

/// brinvacuumcleanup (brin.c) minus the analyze_only early return, which the
/// indexam dispatch keeps; reached via indexam_seams (indexam cannot depend
/// on this crate: execindexing -> indexam). Returns (num_pages, increment to
/// num_index_tuples). C passes &stats->num_index_tuples for BOTH brinsummarize
/// out-pointers, so the increment is summarized + existing.
pub fn brinvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    strategy: BufferAccessStrategy,
) -> PgResult<(BlockNumber, f64)> {
    let num_pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        index,
        ForkNumber::MAIN_FORKNUM,
    )?;

    let heapOid = index.rd_index.as_ref().expect("index").indrelid;
    let heapRel = table::table_open(mcx, heapOid, AccessShareLock)?;

    brin_vacuum_scan(index, &strategy)?;

    let mut summarized = 0.0f64;
    let mut existing = 0.0f64;
    brinsummarize(
        mcx,
        index,
        &heapRel,
        BRIN_ALL_BLOCKRANGES,
        false,
        Some(&mut summarized),
        Some(&mut existing),
    )?;

    heapRel.close(AccessShareLock)?;

    Ok((num_pages, summarized + existing))
}
