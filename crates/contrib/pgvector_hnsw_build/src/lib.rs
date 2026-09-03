//! pgvector 0.8.5 hnswbuild.c: in-memory graph phase in the thread-shared
//! `SharedGraph` (u32 element handles mirror C's graphCtx pointer sharing),
//! flush to disk at maintenance_work_mem, then per-tuple on-disk inserts.

use bufmgr::{LockBuffer, MarkBufferDirty, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use datum::Datum;
use execindexing::IndexInfo;
use mcx::Mcx;
use pgvector_hnsw::insert::{form_index_value, insert_tuple_on_disk, random_level};
use pgvector_hnsw::layout::*;
use pgvector_hnsw::utils::*;
use types_core::{Buffer, ForkNumber};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, NOTICE};
use types_hnsw::*;
use types_rel::Relation;
use types_storage::bufpage::PageMut;
use types_tuple::itemptr::ItemPointerData;

pub(crate) mod algo;
pub(crate) mod graph;

use crate::graph::{lk, SharedElement, SharedGraph};
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
}

// ---- flush to disk ----

fn create_meta_page(bs: &BuildState<'_, '_>) -> PgResult<()> {
    let buf = new_buffer(bs.index, bs.fork_num)?;
    init_page(buf);
    let meta = MetaPage {
        magic_number: HNSW_MAGIC_NUMBER,
        version: HNSW_VERSION,
        dimensions: bs.dimensions as u32,
        m: bs.m as u16,
        ef_construction: bs.ef_construction as u16,
        entry_blkno: INVALID_BLOCK,
        entry_offno: INVALID_OFFSET,
        entry_level: -1,
        insert_page: INVALID_BLOCK,
    };
    {
        let page = buf_page_bytes_mut(buf);
        meta.write(&mut page[PAGE_CONTENTS_OFF..PAGE_CONTENTS_OFF + METAPAGE_SIZE]);
        // pd_lower covers the metapage contents.
        let mut pm = buf_page_mut(buf);
        pm.set_pd_lower((PAGE_CONTENTS_OFF + METAPAGE_SIZE) as u16);
    }
    MarkBufferDirty(buf)?;
    UnlockReleaseBuffer(buf)?;
    Ok(())
}

fn build_append_page(
    index: &Relation<'_>,
    buf: &mut Buffer,
    fork_num: ForkNumber,
) -> PgResult<()> {
    let newbuf = new_buffer(index, fork_num)?;
    page_opaque_set_nextblkno(buf_page_bytes_mut(*buf), bufmgr::BufferGetBlockNumber(newbuf));
    MarkBufferDirty(*buf)?;
    UnlockReleaseBuffer(*buf)?;
    LockBuffer(newbuf, BUFFER_LOCK_UNLOCK)?;
    postgres_seams::check_for_interrupts::call()?;
    LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE)?;
    *buf = newbuf;
    init_page(*buf);
    Ok(())
}

fn serialize_element_tuple(buf: &mut Vec<u8>, e: &SharedElement) {
    let size = element_tuple_size(e.value.len());
    buf.clear();
    buf.resize(size, 0);
    buf[0] = HNSW_ELEMENT_TUPLE_TYPE;
    buf[1] = e.level;
    buf[2] = 0;
    buf[3] = e.version;
    {
        let tids = lk(&e.heaptids);
        for i in 0..HNSW_HEAPTIDS {
            let b = if i < tids.len as usize {
                ipd_to_bytes(&tids.tids[i])
            } else {
                itemptr_encode(INVALID_BLOCK, INVALID_OFFSET)
            };
            buf[4 + i * 6..4 + i * 6 + 6].copy_from_slice(&b);
        }
    }
    let (npage, noffno) = {
        let p = lk(&e.placement);
        (p.neighbor_page, p.neighbor_offno)
    };
    buf[4 + HNSW_HEAPTIDS * 6..4 + HNSW_HEAPTIDS * 6 + 6]
        .copy_from_slice(&itemptr_encode(npage, noffno));
    buf[ELEMENT_DATA_OFFSET..ELEMENT_DATA_OFFSET + e.value.len()].copy_from_slice(&e.value);
}

fn serialize_neighbor_tuple(buf: &mut Vec<u8>, graph: &SharedGraph, e: &SharedElement, m: i32) {
    let size = neighbor_tuple_size(e.level, m);
    buf.clear();
    buf.resize(size, 0);
    buf[0] = HNSW_NEIGHBOR_TUPLE_TYPE;
    buf[1] = e.version;
    // Copy the neighbor ids out under the element's lock, so no second
    // element lock (the neighbors' placement) is taken while it is held.
    let layers: Vec<Vec<u32>> = lk(&e.neighbors)
        .iter()
        .map(|na| na.items.iter().map(|hc| hc.element).collect())
        .collect();
    let mut idx = 0usize;
    for lc in (0..=e.level as i32).rev() {
        let items = &layers[(e.level as i32 - lc) as usize];
        let lm = hnsw_get_layer_m(m, lc);
        for i in 0..lm as usize {
            let b = if i < items.len() {
                let ne = graph.elem(items[i]);
                let p = lk(&ne.placement);
                itemptr_encode(p.blkno, p.offno)
            } else {
                itemptr_encode(INVALID_BLOCK, INVALID_OFFSET)
            };
            buf[NEIGHBOR_TIDS_OFFSET + idx * 6..NEIGHBOR_TIDS_OFFSET + idx * 6 + 6]
                .copy_from_slice(&b);
            idx += 1;
        }
    }
    buf[2..4].copy_from_slice(&(idx as u16).to_ne_bytes());
}

fn page_add(
    index: &Relation<'_>,
    buf: Buffer,
    item: &[u8],
    expected: u16,
) -> PgResult<()> {
    // SAFETY: exclusive lock held on buf.
    let mut pm = unsafe {
        PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf))
    };
    if pm.add_item(item, 0, 0) != Some(expected) {
        return Err(PgError::error(format!(
            "failed to add index item to \"{}\"",
            index.name()
        ))
        .into());
    }
    Ok(())
}

// CreateGraphPages. `order` is C's graph->head walk (non-duplicates only),
// newest-first, taken once by flush_pages and shared with WriteNeighborTuples.
fn create_graph_pages(bs: &mut BuildState<'_, '_>, order: &[u32]) -> PgResult<()> {
    let max_size = HNSW_MAX_SIZE;
    let mut etup: Vec<u8> = Vec::new();
    let mut ntup_placeholder: Vec<u8> = Vec::new();

    let mut buf = new_buffer(bs.index, bs.fork_num)?;
    init_page(buf);

    for &i in order {
        let e = bs.graph.elem(i);
        let etup_size = element_tuple_size(e.value.len());
        let ntup_size = neighbor_tuple_size(e.level, bs.m);
        let combined_size = etup_size + ntup_size + SIZE_OF_ITEM_ID;
        if etup_size > HNSW_TUPLE_ALLOC_SIZE {
            return Err(PgError::error("index tuple too large")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .into());
        }

        // SAFETY: exclusive lock held on buf.
        let free = unsafe {
            types_storage::bufpage::PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf))
        }
        .free_space();
        if free < etup_size || (combined_size <= max_size && free < combined_size) {
            build_append_page(bs.index, &mut buf, bs.fork_num)?;
        }

        let e_offno = {
            // SAFETY: lock held.
            let pr = unsafe {
                types_storage::bufpage::PageRef::from_raw(
                    bufmgr_seams::buffer_get_page::call(buf),
                )
            };
            let blkno = bufmgr::BufferGetBlockNumber(buf);
            let offno = pr.max_offset_number() + 1;
            let mut p = lk(&e.placement);
            p.blkno = blkno;
            p.offno = offno;
            if combined_size <= max_size {
                p.neighbor_page = blkno;
                p.neighbor_offno = offno + 1;
            } else {
                p.neighbor_page = blkno + 1;
                p.neighbor_offno = 1;
            }
            offno
        };

        serialize_element_tuple(&mut etup, &e);
        page_add(bs.index, buf, &etup, e_offno)?;

        // SAFETY: lock held.
        let free = unsafe {
            types_storage::bufpage::PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf))
        }
        .free_space();
        if free < ntup_size {
            build_append_page(bs.index, &mut buf, bs.fork_num)?;
        }
        ntup_placeholder.clear();
        ntup_placeholder.resize(ntup_size, 0);
        ntup_placeholder[0] = HNSW_NEIGHBOR_TUPLE_TYPE;
        let n_offno = lk(&e.placement).neighbor_offno;
        page_add(bs.index, buf, &ntup_placeholder, n_offno)?;
    }

    let insert_page = bufmgr::BufferGetBlockNumber(buf);
    MarkBufferDirty(buf)?;
    UnlockReleaseBuffer(buf)?;

    let entry = bs.graph.entry_point().map(|ep| {
        let e = bs.graph.elem(ep);
        let p = lk(&e.placement);
        (p.blkno, p.offno, e.level as i16)
    });
    update_meta_page(
        bs.index,
        HNSW_UPDATE_ENTRY_ALWAYS,
        entry,
        insert_page,
        bs.fork_num,
        true,
    )
}

// WriteNeighborTuples.
fn write_neighbor_tuples(bs: &mut BuildState<'_, '_>, order: &[u32]) -> PgResult<()> {
    let mut ntup: Vec<u8> = Vec::new();
    for &i in order {
        postgres_seams::check_for_interrupts::call()?;
        let e = bs.graph.elem(i);
        let ntup_size = neighbor_tuple_size(e.level, bs.m);
        let (neighbor_page, neighbor_offno) = {
            let p = lk(&e.placement);
            (p.neighbor_page, p.neighbor_offno)
        };
        serialize_neighbor_tuple(&mut ntup, &bs.graph, &e, bs.m);
        let buf = bufmgr::ReadBufferExtended(
            bs.index,
            bs.fork_num,
            neighbor_page,
            types_storage::storage::ReadBufferMode::Normal,
            None,
        )?;
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;
        // SAFETY: exclusive lock held.
        let mut pm = unsafe {
            PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf))
        };
        if !pm.index_tuple_overwrite(neighbor_offno, &ntup[..ntup_size]) {
            return Err(PgError::error(format!(
                "failed to add index item to \"{}\"",
                bs.index.name()
            ))
            .into());
        }
        MarkBufferDirty(buf)?;
        UnlockReleaseBuffer(buf)?;
    }
    Ok(())
}

fn flush_pages(bs: &mut BuildState<'_, '_>) -> PgResult<()> {
    create_meta_page(bs)?;
    // C iterates graph->head (non-duplicates only), newest-first; we push and
    // take the list once for both passes.
    let order: Vec<u32> = bs.graph.take_head().into_iter().rev().collect();
    create_graph_pages(bs, &order)?;
    write_neighbor_tuples(bs, &order)?;
    bs.graph.set_flushed();
    // C resets graphCtx.
    bs.graph.clear_after_flush();
    Ok(())
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

    if bs.graph.flushed() {
        let mut support = bs.support.clone();
        let r = insert_tuple_on_disk(bs.index, &mut support, &img, heaptid, true);
        bs.support = support;
        return r.map(|_| true);
    }

    // C checks memoryUsed (+ zero serial margin) against memoryTotal BEFORE
    // HnswInitElement draws the level, so the PRNG stream is not consumed by
    // a tuple that diverts to the on-disk path at the flush transition.
    if bs.graph.memory_exhausted() {
        if !bs.graph.flushed() {
            elog::ereport(NOTICE)
                .errmsg(format!(
                    "hnsw graph no longer fits into maintenance_work_mem after {} tuples",
                    bs.graph.indtuples() as i64
                ))
                .errdetail("Building will take significantly more time.".to_string())
                .errhint("Increase maintenance_work_mem to speed up builds.".to_string())
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "InsertTuple"))?;
            flush_pages(bs)?;
        }
        let mut support = bs.support.clone();
        let r = insert_tuple_on_disk(bs.index, &mut support, &img, heaptid, true);
        bs.support = support;
        return r.map(|_| true);
    }

    // HnswInitElement + the value copy; alloc_element does the memory
    // accounting HnswMemoryContextAlloc performs in C.
    let level = random_level(bs.ml, bs.max_level);
    let element = bs.graph.alloc_element(*heaptid, level, &img, bs.m);

    algo::insert_tuple_in_memory(
        &bs.graph,
        &mut bs.support,
        bs.m,
        bs.ef_construction,
        element,
    )?;
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
    })
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
        let mut inner_err: Option<Box<PgError>> = None;
        // BuildState is threaded via raw pointer: the callback is FnMut and
        // borrows would alias bs.
        let bs_ptr: *mut BuildState<'_, 'mcx> = &mut bs;
        let reltuples = execindexing::table_index_build_scan(
            mcx,
            heap,
            index,
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

    if !bs.graph.flushed() {
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
