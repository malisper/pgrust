//! brin.c build lane (brinbuild + build state), split from the dispatch
//! crate the way nbtsort is: execindexing sits between indexam and the AM.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::bufmgr_seams::{
    buffer_get_block_number, buffer_get_page, extend_buffered_rel_by, lock_buffer,
    mark_buffer_dirty, release_buffer, BUFFER_LOCK_UNLOCK, EB_LOCK_FIRST,
    EB_SKIP_EXTENSION_LOCK,
};
use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_brin::*;
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, RmgrIds};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef};
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumber};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use brin::{add_values_to_range, brin_build_desc, brin_get_pages_per_range};
use brin_pageops::{
    brinRevmapInitialize, brinRevmapTerminate, brin_doinsert, brin_metapage_init,
    relation_needs_wal,
};
use brin_tuple::{brin_form_tuple, brin_memtuple_initialize, brin_new_memtuple};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: {what}")
}
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
    bs_rmAccess: BrinRevmap,
    bs_bdesc: BrinDesc<'mcx>,
    bs_dtuple: BrinMemTuple,
    bs_emptyTuple: Option<PgVec<'mcx, u8>>,
}

fn initialize_brin_buildstate<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    idxRel: &'a Relation<'mcx>,
    revmap: BrinRevmap,
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
        bs_maxRangeStart: last_range + pagesPerRange,
        bs_rmAccess: revmap,
        bs_bdesc: bdesc,
        bs_dtuple: dtuple,
        bs_emptyTuple: None,
    })
}

// terminate_brin_buildstate: give the last insert page's free space to the
// FSM.
fn terminate_brin_buildstate(state: BrinBuildState<'_, '_>) -> PgResult<BrinRevmap> {
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
    Ok(state.bs_rmAccess)
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
    let mut state = initialize_brin_buildstate(mcx, index, revmap, pagesPerRange, heap_blocks)?;

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
    let revmap = terminate_brin_buildstate(state)?;
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

/// brinbuildempty: unlogged-index INIT_FORKNUM arm (loud repo-wide).
pub fn brinbuildempty(_index: &Relation<'_>) -> ! {
    unported("brinbuildempty (unlogged-index INIT_FORKNUM lane)")
}

fn form_and_insert_tuple(state: &mut BrinBuildState<'_, '_>) -> PgResult<()> {
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

