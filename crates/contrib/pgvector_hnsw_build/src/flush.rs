//! pgvector 0.8.5 hnswbuild.c: flush the in-memory `SharedGraph` to disk
//! (`CreateMetaPage`/`CreateGraphPages`/`WriteNeighborTuples`/`FlushPages`).
//!
//! `flush_pages` runs while other participants may still be reading the
//! graph's entry point via `insert_tuple_in_memory` (parallel build, later
//! task), so it takes `graph.entry_wait_lock` then `graph.entry_lock` for
//! write — the same upgrade dance `insert_tuple_in_memory` uses when it may
//! promote the entry point — before walking `graph.head`, and holds the
//! write lock through `clear_after_flush()` so no participant can observe a
//! partially-reset graph. The caller (`insert_tuple`) never holds
//! `entry_lock` when it calls `flush_pages` (flush happens before
//! `insert_tuple_in_memory`, never interleaved with it), so this cannot
//! deadlock against the lock this function itself takes. The `flush_lock`
//! wrapper around the flush *decision* (C's `flushLock`) is a later task's
//! job; this function stays single-caller.

use crate::graph::{lk, SharedElement, SharedGraph};
use crate::BuildState;
use bufmgr::{LockBuffer, MarkBufferDirty, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use pgvector_hnsw::layout::*;
use pgvector_hnsw::utils::*;
use std::sync::Arc;
use types_core::{Buffer, ForkNumber};
use types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_hnsw::*;
use types_rel::Relation;
use types_storage::bufpage::PageMut;

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

pub(crate) fn serialize_element_tuple(buf: &mut Vec<u8>, e: &SharedElement) {
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

pub(crate) fn serialize_neighbor_tuple(buf: &mut Vec<u8>, graph: &SharedGraph, e: &SharedElement, m: i32) {
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

pub(crate) fn flush_pages(bs: &mut BuildState<'_, '_>) -> PgResult<()> {
    create_meta_page(bs)?;

    // C's FlushPages runs under the caller's flushLock exclusive, which
    // already excludes concurrent inserters; here we additionally take
    // entry_lock ourselves so flush_pages has no implicit precondition on
    // its caller's locking. Same upgrade dance as insert_tuple_in_memory:
    // wait-lock first so new inserters queue behind us, then the exclusive
    // entry lock, held through clear_after_flush(). Clone the Arc so the
    // guard's lifetime doesn't tie up `bs` itself, which create_graph_pages
    // and write_neighbor_tuples below still need mutably.
    let graph = Arc::clone(&bs.graph);
    let wait = lk(&graph.entry_wait_lock);
    let write = graph.entry_lock.write().unwrap_or_else(|e| e.into_inner());
    drop(wait);

    // C iterates graph->head (non-duplicates only), newest-first; we push and
    // take the list once for both passes.
    let order: Vec<u32> = graph.take_head().into_iter().rev().collect();
    create_graph_pages(bs, &order)?;
    write_neighbor_tuples(bs, &order)?;
    graph.set_flushed();
    // C resets graphCtx.
    graph.clear_after_flush();

    drop(write);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgvector_hnsw::layout::{itemptr_is_valid, NeighborTupleView};
    use types_hnsw::HnswSupport;
    use types_tuple::itemptr::ItemPointerData;

    fn support() -> HnswSupport {
        HnswSupport {
            procinfo: types_fmgr::FmgrInfo::new(
                pgvector::funcs::fc_vector_l2_squared_distance,
                1,
                2,
                true,
                false,
            ),
            normprocinfo: None,
            collation: 0,
            type_info: &pgvector::vec::VECTOR_TYPE_INFO,
        }
    }

    // Builds a 50-element 1-D graph through insert_tuple_in_memory (real
    // live neighbor lists, not hand-assembled), then pins that
    // serialize_element_tuple/serialize_neighbor_tuple encode exactly the
    // live state: the neighbor tuple's valid (non-placeholder) entries at
    // layer 0 must equal the live neighbors.lock()[0].items.len(). All
    // elements are forced to level 0 so index 0 is unambiguously their only
    // (base) layer.
    #[test]
    fn flush_serialization_matches_live_neighbor_state() {
        let owner = mcx::MemoryContext::new_bump("flush test");
        let mcx = owner.mcx();
        let graph = SharedGraph::new(usize::MAX);
        let mut sp = support();
        let (m, ef_construction) = (6, 32);

        for i in 0..50u16 {
            let x = i as f32 / 50.0;
            let mut b = pgvector::vec::VecBuilder::new(mcx, 1).unwrap();
            b.set(0, x);
            let e = graph.alloc_element(ItemPointerData::new(1, i + 1), 0, &b.image(), m);
            crate::algo::insert_tuple_in_memory(&graph, &mut sp, m, ef_construction, e).unwrap();
        }

        // Assign each element a synthetic on-disk placement, the way
        // create_graph_pages normally would during flush_pages, so that
        // serialize_neighbor_tuple (which encodes each neighbor's
        // *placement*, not its raw id) has real locations to encode. This
        // lets the test exercise byte-for-byte serialization without a live
        // Postgres relation/buffer.
        for id in 0..50u32 {
            let el = graph.elem(id);
            let mut p = lk(&el.placement);
            p.blkno = id + 1;
            p.offno = 1;
        }

        let e = graph.elem(49);
        let live_count = lk(&e.neighbors)[0].items.len();

        let mut etup: Vec<u8> = Vec::new();
        serialize_element_tuple(&mut etup, &e);
        assert_eq!(etup[1], e.level, "element tuple carries the element's level");

        let mut ntup: Vec<u8> = Vec::new();
        serialize_neighbor_tuple(&mut ntup, &graph, &e, m);
        let view = NeighborTupleView { bytes: &ntup };
        let valid = (0..view.count() as usize)
            .filter(|&i| itemptr_is_valid(view.indextid_bytes(i)))
            .count();
        assert_eq!(
            valid, live_count,
            "neighbor tuple's valid entries must equal the live neighbors.lock()[0].items.len()"
        );
    }
}
