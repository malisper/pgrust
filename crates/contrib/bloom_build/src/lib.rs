//! blinsert.c (build half). Split from the bloom crate so indexam -> bloom
//! doesn't cycle through execindexing (pgvector_hnsw_build shape).

use bloom::state::{
    bloom_form_tuple, bloom_init_metapage, bloom_new_buffer, init_bloom_state,
    GENERIC_XLOG_FULL_IMAGE,
};
use bufmgr::UnlockReleaseBuffer;
use execindexing::{table_index_build_scan, IndexInfo};
use generic_xlog::{GenericXLogFinish, GenericXLogStart};
use mcx::Mcx;
use types_bloom::*;
use types_core::{ForkNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_rel::Relation;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

struct BloomBuildState {
    blstate: BloomState,
    indtuples: f64,
    data: Box<[u8; BLCKSZ]>,
    count: i32,
}

fn flush_cached_page<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    buildstate: &mut BloomBuildState,
) -> PgResult<()> {
    let buffer = bloom_new_buffer(index)?;
    let mut state = GenericXLogStart(mcx, index)?;
    let page = state.register_buffer(buffer, GENERIC_XLOG_FULL_IMAGE)?;
    page.copy_from_slice(&buildstate.data[..]);
    GenericXLogFinish(state)?;
    UnlockReleaseBuffer(buffer)
}

fn init_cached_page(buildstate: &mut BloomBuildState) {
    bloom_init_page(&mut buildstate.data[..], 0);
    buildstate.count = 0;
}

pub fn blbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        return Err(PgError::error(format!(
            "index \"{}\" already contains data",
            index.name()
        ))
        .into());
    }

    bloom_init_metapage(mcx, index, ForkNumber::MAIN_FORKNUM)?;

    let mut buildstate = BloomBuildState {
        blstate: init_bloom_state(index)?,
        indtuples: 0.0,
        data: Box::new([0u8; BLCKSZ]),
        count: 0,
    };
    init_cached_page(&mut buildstate);

    let bs = &mut buildstate;
    let reltuples = table_index_build_scan(
        mcx,
        heap,
        index,
        index_info,
        true,
        |_index, tid, values, isnull, _tuple_is_alive| {
            // C's per-tuple tmpCtx reset == the owned tuple Vec dropping here.
            let itup = bloom_form_tuple(&mut bs.blstate, tid, values, isnull)?;
            let size = bs.blstate.size_of_bloom_tuple;
            if page_add_item(&mut bs.data[..], size, &itup) {
                bs.count += 1;
            } else {
                flush_cached_page(mcx, index, bs)?;
                postgres_seams::check_for_interrupts::call()?;
                init_cached_page(bs);
                if !page_add_item(&mut bs.data[..], size, &itup) {
                    return Err(PgError::error(
                        "could not add new bloom tuple to empty page",
                    )
                    .into());
                }
                bs.count += 1;
            }
            bs.indtuples += 1.0;
            Ok(())
        },
    )?;

    // The last page still needs a flush unless the heap was empty.
    if buildstate.count > 0 {
        flush_cached_page(mcx, index, &mut buildstate)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: buildstate.indtuples,
    })
}

pub fn blbuildempty(index: &Relation<'_>) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new_bump("bloom buildempty");
    bloom_init_metapage(ctx.mcx(), index, ForkNumber::INIT_FORKNUM)
}

