//! pgvector 0.8.5 hnswbuild.c: in-memory graph phase in the thread-shared
//! `SharedGraph` (u32 element handles mirror C's graphCtx pointer sharing),
//! flush to disk at maintenance_work_mem, then per-tuple on-disk inserts.

use datum::Datum;
use execindexing::IndexInfo;
use mcx::Mcx;
use pgvector_hnsw::insert::{form_index_value, insert_tuple_on_disk, random_level};
use pgvector_hnsw::layout::hnsw_get_max_level;
use pgvector_hnsw::utils::{hnsw_get_ef_construction, hnsw_get_m, init_support, relation_needs_wal};
use types_core::ForkNumber;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, NOTICE};
use types_hnsw::*;
use types_rel::Relation;
use types_tuple::itemptr::ItemPointerData;

pub(crate) mod algo;
pub(crate) mod flush;
pub(crate) mod graph;
pub(crate) mod parallel;

use crate::flush::flush_pages;
use crate::graph::SharedGraph;
use std::sync::Arc;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

struct BuildState<'a, 'mcx> {
    heap: Option<&'a Relation<'mcx>>,
    index: &'a Relation<'mcx>,
    fork_num: ForkNumber,
    m: i32,
    ef_construction: i32,
    dimensions: i32,
    ml: f64,
    max_level: i32,
    support: HnswSupport,
    graph: Arc<SharedGraph>,
    reltuples: f64,
    /// C: `memoryMargin` in `InsertTuple` — `base == NULL ? 0 : 1024 * 1024`,
    /// i.e. zero for a serial build and 1MB for every participant of a
    /// parallel one.
    memory_margin: usize,
}

// InsertTuple (build path).
fn insert_tuple(
    bs: &mut BuildState<'_, '_>,
    values: &[Datum],
    isnull: &[bool],
    heaptid: &ItemPointerData,
) -> PgResult<bool> {
    if isnull.first().copied().unwrap_or(true) {
        return Ok(false);
    }
    let tmp = mcx::MemoryContext::new_bump("Hnsw build temporary context");
    let tmcx = tmp.mcx();
    let mut support = bs.support.clone();
    let Some(img) = form_index_value(tmcx, values[0], &mut support)? else {
        bs.support = support;
        return Ok(false);
    };
    bs.support = support;

    // C InsertTuple's flushLock protocol (hnswbuild.c:510-574). Lock order is
    // flush_lock → entry_lock everywhere: `flush_pages` takes entry_lock for
    // write and `insert_tuple_in_memory` takes it shared/exclusive, both while
    // this frame holds flush_lock; nothing ever takes flush_lock while holding
    // entry_lock. The Arc clone keeps the guard's lifetime off `bs`, which
    // `flush_pages` needs mutably.
    let graph = Arc::clone(&bs.graph);

    // Ensure the graph is not flushed while we insert.
    let read = graph.flush_lock.read().unwrap_or_else(|e| e.into_inner());

    // Are we in the on-disk phase?
    if graph.flushed() {
        drop(read);
        let mut support = bs.support.clone();
        let r = insert_tuple_on_disk(bs.index, &mut support, &img, heaptid, true);
        bs.support = support;
        return r.map(|_| true);
    }

    // C checks memoryUsed + memoryMargin against memoryTotal BEFORE
    // HnswInitElement draws the level, so the PRNG stream is not consumed by
    // a tuple that diverts to the on-disk path at the flush transition.
    if graph.memory_exhausted(bs.memory_margin) {
        // C: drop the shared flush lock and retake it exclusive, then re-test
        // `flushed` — only the first participant through here flushes (and
        // only it reports the NOTICE).
        drop(read);
        let write = graph.flush_lock.write().unwrap_or_else(|e| e.into_inner());
        if !graph.flushed() {
            elog::ereport(NOTICE)
                .errmsg(format!(
                    "hnsw graph no longer fits into maintenance_work_mem after {} tuples",
                    graph.indtuples() as i64
                ))
                .errdetail("Building will take significantly more time.".to_string())
                .errhint("Increase maintenance_work_mem to speed up builds.".to_string())
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "InsertTuple"))?;
            flush_pages(bs)?;
        }
        drop(write);
        let mut support = bs.support.clone();
        let r = insert_tuple_on_disk(bs.index, &mut support, &img, heaptid, true);
        bs.support = support;
        return r.map(|_| true);
    }

    // HnswInitElement + the value copy; alloc_element does the memory
    // accounting HnswMemoryContextAlloc performs in C.
    let level = random_level(bs.ml, bs.max_level);
    let element = bs.graph.alloc_element(*heaptid, level, &img, bs.m)?;

    algo::insert_tuple_in_memory(
        &bs.graph,
        &mut bs.support,
        bs.m,
        bs.ef_construction,
        element,
    )?;
    // C holds flushLock SHARED across InsertTupleInMemory.
    drop(read);
    Ok(true)
}

fn init_build_state<'a, 'mcx>(
    heap: Option<&'a Relation<'mcx>>,
    index: &'a Relation<'mcx>,
    fork_num: ForkNumber,
) -> PgResult<BuildState<'a, 'mcx>> {
    let type_info = pgvector_hnsw::utils::get_type_info(index)?;
    let max_dims = type_info.max_dimensions;
    let m = hnsw_get_m(index);
    let ef_construction = hnsw_get_ef_construction(index);
    let dimensions = index.rd_att.attr(0).atttypmod;

    if dimensions < 0 {
        return Err(PgError::error("column does not have dimensions")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }
    if dimensions > max_dims {
        return Err(PgError::error(format!(
            "column cannot have more than {max_dims} dimensions for hnsw index"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }
    if ef_construction < 2 * m {
        return Err(
            PgError::error("ef_construction must be greater than or equal to 2 * m")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into(),
        );
    }

    // C calls HnswInitSupport after these checks; init_support also resolves
    // proc 3 a second time (accepted duplicate — see DIVERGENCES in
    // pgvector_hnsw/src/lib.rs).
    let support = init_support(index)?;

    Ok(BuildState {
        heap,
        index,
        fork_num,
        m,
        ef_construction,
        dimensions,
        ml: hnsw_get_ml(m),
        max_level: hnsw_get_max_level(m),
        support,
        graph: Arc::new(SharedGraph::new(
            init_small::globals::maintenance_work_mem() as usize * 1024,
        )),
        reltuples: 0.0,
        memory_margin: 0,
    })
}

/// C: `BuildGraph` — the scan that fills the in-memory graph, parallel when
/// the planner arithmetic allows it, then the flush and the parallel teardown
/// (in C's order: FlushPages runs before HnswEndParallel).
fn build_graph<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    bs: &mut BuildState<'a, 'mcx>,
    heap: &'a Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<()> {
    let request = parallel::compute_parallel_workers(heap, bs.index, index_info);
    let shared = Arc::new(parallel::HnswShared {
        heaprelid: heap.rd_id,
        indexrelid: bs.index.rd_id,
        isconcurrent: index_info.ii_Concurrent,
        done: std::sync::Mutex::new((0, 0.0)),
        workers_done: std::sync::Condvar::new(),
        pscan: std::sync::OnceLock::new(),
        graph: Arc::clone(&bs.graph),
        m: bs.m,
        ef_construction: bs.ef_construction,
        dimensions: bs.dimensions,
        ml: bs.ml,
        max_level: bs.max_level,
    });

    let leader = if request > 0 {
        parallel::begin_parallel(&shared, heap, index_info.ii_Concurrent, request)?
    } else {
        None
    };

    match &leader {
        Some(l) => {
            // C: HnswLeaderParticipateAsWorker, then ParallelHeapScan. The
            // leader's own inserts run through the participant BuildState
            // that `parallel_scan_and_insert` builds (which carries
            // PARALLEL_MEMORY_MARGIN); this outer BuildState only reaches
            // `flush_pages` below, which does not read memory_margin.
            parallel_scan_and_insert_leader(mcx, heap, bs.index, &shared)?;
            bs.reltuples = parallel::parallel_heap_scan(l, &shared)?;
        }
        None => {
            let mut inner_err: Option<Box<PgError>> = None;
            // BuildState is threaded via raw pointer: the callback is FnMut and
            // borrows would alias bs.
            let bs_ptr: *mut BuildState<'_, 'mcx> = bs;
            let reltuples = execindexing::table_index_build_scan(
                mcx,
                heap,
                bs.index,
                index_info,
                true,
                |_index_rel, tid, values, isnull, _alive| {
                    // SAFETY: single-threaded serial build; bs outlives the scan.
                    let bs = unsafe { &mut *bs_ptr };
                    match insert_tuple(bs, values, isnull, tid) {
                        Ok(true) => {
                            bs.graph.inc_indtuples();
                            Ok(())
                        }
                        Ok(false) => Ok(()),
                        Err(e) => {
                            inner_err = Some(e);
                            Err(PgError::error("hnsw build insert failed").into())
                        }
                    }
                },
            );
            match reltuples {
                Ok(n) => bs.reltuples = n,
                Err(e) => return Err(inner_err.unwrap_or(e)),
            }
        }
    }

    if !bs.graph.flushed() {
        flush_pages(bs)?;
    }
    if let Some(l) = leader {
        parallel::end_parallel(l)?;
    }
    Ok(())
}

// The leader's own participation. Split out so the shared borrow of `bs.index`
// does not collide with the `&mut BuildState` the caller holds.
fn parallel_scan_and_insert_leader<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    shared: &Arc<parallel::HnswShared>,
) -> PgResult<()> {
    parallel::parallel_scan_and_insert(mcx, heap, index, shared, true)
}

fn build_index<'mcx>(
    mcx: Mcx<'mcx>,
    heap: Option<&Relation<'mcx>>,
    index: &Relation<'mcx>,
    index_info: Option<&mut IndexInfo<'mcx>>,
    fork_num: ForkNumber,
) -> PgResult<IndexBuildResult> {
    let mut bs = init_build_state(heap, index, fork_num)?;

    if let (Some(heap), Some(index_info)) = (bs.heap, index_info) {
        build_graph(mcx, &mut bs, heap, index_info)?;
    } else if !bs.graph.flushed() {
        flush_pages(&mut bs)?;
    }

    if relation_needs_wal(index) || fork_num == ForkNumber::INIT_FORKNUM {
        let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(index, fork_num)?;
        xloginsert::log_newpage_range(index, fork_num, 0, nblocks, true)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: bs.reltuples,
        index_tuples: bs.graph.indtuples() as f64,
    })
}

pub fn hnswbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    build_index(mcx, Some(heap), index, Some(index_info), ForkNumber::MAIN_FORKNUM)
}

pub fn hnswbuildempty(index: &Relation<'_>) -> PgResult<()> {
    let mcx_owner = mcx::MemoryContext::new_bump("hnsw buildempty");
    build_index(mcx_owner.mcx(), None, index, None, ForkNumber::INIT_FORKNUM)?;
    Ok(())
}

/// Registers the parallel build worker entrypoint (`main_main`'s contrib seam
/// init calls this once at startup).
pub fn init_seams() {
    parallel::init_seams();
}
