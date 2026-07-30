//! pgvector 0.8.5 hnswbuild.c, serial rendering: in-memory graph phase in a
//! bump arena (u32 element handles mirror C's graphCtx pointer sharing), flush
//! to disk at maintenance_work_mem, then per-tuple on-disk inserts.

use bufmgr::{LockBuffer, MarkBufferDirty, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use datum::Datum;
use execindexing::IndexInfo;
use mcx::{Mcx, PgVec};
use pgvector_hnsw::insert::{form_index_value, insert_tuple_on_disk, random_level};
use pgvector_hnsw::layout::*;
use pgvector_hnsw::utils::*;
use types_core::{BlockNumber, Buffer, ForkNumber};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, NOTICE};
use types_hnsw::*;
use types_rel::Relation;
use types_storage::bufpage::PageMut;
use types_tuple::itemptr::ItemPointerData;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

struct MemCandidate {
    element: u32,
    distance: f32,
    closer: bool,
}

struct MemNeighborArray {
    items: Vec<MemCandidate>,
    closer_set: bool,
}

struct MemElement<'g> {
    heaptids: [ItemPointerData; HNSW_HEAPTIDS],
    heaptids_len: u8,
    level: u8,
    version: u8,
    value: PgVec<'g, u8>,
    neighbors: Vec<MemNeighborArray>,
    // On-disk location assigned by CreateGraphPages.
    blkno: BlockNumber,
    offno: u16,
    neighbor_page: BlockNumber,
    neighbor_offno: u16,
}

struct Graph<'g> {
    mcx: Mcx<'g>,
    // Element arena; ids are indices. Duplicates stay allocated here (as in
    // C's graphCtx) but are only reachable through `head` if non-duplicate.
    elems: Vec<MemElement<'g>>,
    // C's graph->head insertion list: AddElementInMemory links only
    // non-duplicate elements, and the flush walks this list. C prepends and
    // iterates head-first; we push and iterate in reverse (newest-first).
    head: Vec<u32>,
    entry_point: Option<u32>,
    memory_used: usize,
    memory_total: usize,
    flushed: bool,
    indtuples: f64,
}

struct BuildState<'a, 'g, 'mcx> {
    heap: Option<&'a Relation<'mcx>>,
    index: &'a Relation<'mcx>,
    fork_num: ForkNumber,
    m: i32,
    ef_construction: i32,
    dimensions: i32,
    ml: f64,
    max_level: i32,
    support: HnswSupport,
    graph: Graph<'g>,
    reltuples: f64,
}

fn mem_candidate_clone(c: &MemCandidate) -> MemCandidate {
    MemCandidate { element: c.element, distance: c.distance, closer: c.closer }
}

fn get_distance_mem(
    graph: &Graph<'_>,
    support: &mut HnswSupport,
    q: Datum,
    e: u32,
) -> PgResult<f64> {
    let v = Datum::from_usize(graph.elems[e as usize].value.as_ptr() as usize);
    get_distance(support, q, v)
}

// HnswSearchLayer, in-memory form.
#[allow(clippy::too_many_arguments)]
fn search_layer_mem(
    graph: &Graph<'_>,
    support: &mut HnswSupport,
    q: Datum,
    ep: Vec<(u32, f64)>,
    ef: i32,
    lc: i32,
    _m: i32,
) -> PgResult<Vec<(u32, f64)>> {
    let mut visited: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut c_heap: Vec<(f64, u32)> = Vec::new();
    let mut w_heap: Vec<(f64, u32)> = Vec::new();
    let mut wlen: i32 = 0;

    fn push_min(v: &mut Vec<(f64, u32)>, item: (f64, u32)) {
        v.push(item);
        let mut i = v.len() - 1;
        while i > 0 {
            let p = (i - 1) / 2;
            if v[i].0 < v[p].0 {
                v.swap(i, p);
                i = p;
            } else {
                break;
            }
        }
    }
    fn pop_min(v: &mut Vec<(f64, u32)>) -> Option<(f64, u32)> {
        heap_pop(v, |a, b| a < b)
    }
    fn push_max(v: &mut Vec<(f64, u32)>, item: (f64, u32)) {
        v.push(item);
        let mut i = v.len() - 1;
        while i > 0 {
            let p = (i - 1) / 2;
            if v[i].0 > v[p].0 {
                v.swap(i, p);
                i = p;
            } else {
                break;
            }
        }
    }
    fn pop_max(v: &mut Vec<(f64, u32)>) -> Option<(f64, u32)> {
        heap_pop(v, |a, b| a > b)
    }
    fn heap_pop(v: &mut Vec<(f64, u32)>, before: fn(f64, f64) -> bool) -> Option<(f64, u32)> {
        if v.is_empty() {
            return None;
        }
        let last = v.len() - 1;
        v.swap(0, last);
        let out = v.pop();
        let n = v.len();
        let mut i = 0;
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut sm = i;
            if l < n && before(v[l].0, v[sm].0) {
                sm = l;
            }
            if r < n && before(v[r].0, v[sm].0) {
                sm = r;
            }
            if sm == i {
                break;
            }
            v.swap(i, sm);
            i = sm;
        }
        out
    }

    for (e, d) in ep.iter() {
        visited.insert(*e);
        push_min(&mut c_heap, (*d, *e));
        push_max(&mut w_heap, (*d, *e));
        wlen += 1;
    }

    while let Some((c_dist, c_elem)) = pop_min(&mut c_heap) {
        let (f_dist, _) = *w_heap.first().expect("W nonempty");
        if c_dist > f_dist {
            break;
        }
        let layer_idx = (graph.elems[c_elem as usize].level as i32 - lc) as usize;
        let neighbor_ids: Vec<u32> = graph.elems[c_elem as usize].neighbors[layer_idx]
            .items
            .iter()
            .map(|hc| hc.element)
            .collect();

        for e in neighbor_ids {
            if !visited.insert(e) {
                continue;
            }
            let always_add = wlen < ef;
            let (f_dist, _) = *w_heap.first().expect("W nonempty");
            let e_distance = get_distance_mem(graph, support, q, e)?;
            if !(e_distance < f_dist || always_add) {
                continue;
            }
            if (graph.elems[e as usize].level as i32) < lc {
                continue;
            }
            push_min(&mut c_heap, (e_distance, e));
            push_max(&mut w_heap, (e_distance, e));
            wlen += 1;
            if wlen > ef {
                pop_max(&mut w_heap);
            }
        }
    }

    let mut w: Vec<(u32, f64)> = Vec::with_capacity(w_heap.len());
    while let Some((d, e)) = pop_max(&mut w_heap) {
        w.push((e, d));
    }
    Ok(w)
}

// CheckElementCloser (in-memory).
fn check_element_closer_mem(
    graph: &Graph<'_>,
    support: &mut HnswSupport,
    e: &MemCandidate,
    r: &[MemCandidate],
) -> PgResult<bool> {
    let e_value = Datum::from_usize(graph.elems[e.element as usize].value.as_ptr() as usize);
    for ri in r {
        let ri_value =
            Datum::from_usize(graph.elems[ri.element as usize].value.as_ptr() as usize);
        let distance = get_distance(support, e_value, ri_value)? as f32;
        if distance <= e.distance {
            return Ok(false);
        }
    }
    Ok(true)
}

// SelectNeighbors (in-memory). C's candidate list holds pointers into the
// caller's neighbor array, so e->closer updates land in that array; we take
// `c` mutably and write the computed closer flags back by origin index.
#[allow(clippy::too_many_arguments)]
fn select_neighbors_mem(
    graph: &Graph<'_>,
    support: &mut HnswSupport,
    c: &mut [MemCandidate],
    lm: i32,
    closer_set: &mut bool,
    new_candidate: Option<usize>,
    pruned: Option<&mut Option<MemCandidate>>,
    sort_candidates: bool,
    out: &mut Vec<MemCandidate>,
) -> PgResult<()> {
    out.clear();
    if c.len() as i32 <= lm {
        out.extend(c.iter().map(mem_candidate_clone));
        return Ok(());
    }

    let mut w: Vec<MemCandidate> = c.iter().map(mem_candidate_clone).collect();
    let mut w_is_new: Vec<bool> = vec![false; w.len()];
    // Origin index into `c` for closer-flag write-back.
    let mut w_src: Vec<usize> = (0..w.len()).collect();
    if let Some(nc) = new_candidate {
        w_is_new[nc] = true;
    }
    if sort_candidates {
        let mut order: Vec<usize> = (0..w.len()).collect();
        order.sort_by(|&a, &b| {
            w[b].distance
                .partial_cmp(&w[a].distance)
                .unwrap_or(core::cmp::Ordering::Equal)
                .then_with(|| w[b].element.cmp(&w[a].element))
        });
        let neww: Vec<MemCandidate> = order.iter().map(|&i| mem_candidate_clone(&w[i])).collect();
        let newn: Vec<bool> = order.iter().map(|&i| w_is_new[i]).collect();
        let news: Vec<usize> = order.iter().map(|&i| w_src[i]).collect();
        w = neww;
        w_is_new = newn;
        w_src = news;
    }

    let must_calculate = !*closer_set;
    let mut wd: Vec<MemCandidate> = Vec::with_capacity(w.len());
    let mut added: Vec<MemCandidate> = Vec::new();
    let mut removed_any = false;

    while !w.is_empty() && (out.len() as i32) < lm {
        let mut e = w.pop().expect("nonempty");
        let e_is_new = w_is_new.pop().expect("nonempty");
        let e_src = w_src.pop().expect("nonempty");

        if must_calculate {
            e.closer = check_element_closer_mem(graph, support, &e, out)?;
        } else if !added.is_empty() {
            if e.closer {
                e.closer = check_element_closer_mem(graph, support, &e, &added)?;
                if !e.closer {
                    removed_any = true;
                }
            } else if removed_any {
                e.closer = check_element_closer_mem(graph, support, &e, out)?;
                if e.closer {
                    added.push(mem_candidate_clone(&e));
                }
            }
        } else if e_is_new {
            e.closer = check_element_closer_mem(graph, support, &e, out)?;
            if e.closer {
                added.push(mem_candidate_clone(&e));
            }
        }

        // C writes e->closer through the shared pointer; mirror into `c`.
        c[e_src].closer = e.closer;

        if e.closer {
            out.push(e);
        } else {
            wd.push(e);
        }
    }

    *closer_set = sort_candidates;

    let mut wdoff = 0usize;
    while wdoff < wd.len() && (out.len() as i32) < lm {
        out.push(mem_candidate_clone(&wd[wdoff]));
        wdoff += 1;
    }
    if let Some(p) = pruned {
        *p = if wdoff < wd.len() {
            Some(mem_candidate_clone(&wd[wdoff]))
        } else {
            w.first().map(mem_candidate_clone)
        };
    }
    Ok(())
}

// HnswFindElementNeighbors (in-memory).
fn find_element_neighbors_mem(
    bs: &mut BuildState<'_, '_, '_>,
    element: u32,
    entry_point: Option<u32>,
) -> PgResult<()> {
    let level = bs.graph.elems[element as usize].level as i32;
    let q = Datum::from_usize(bs.graph.elems[element as usize].value.as_ptr() as usize);
    let Some(entry_point) = entry_point else { return Ok(()) };

    let mut support = bs.support.clone();
    let entry_level = bs.graph.elems[entry_point as usize].level as i32;
    let ep_dist = get_distance_mem(&bs.graph, &mut support, q, entry_point)?;
    let mut ep: Vec<(u32, f64)> = vec![(entry_point, ep_dist)];

    let mut lc = entry_level;
    while lc >= level + 1 {
        ep = search_layer_mem(&bs.graph, &mut support, q, ep, 1, lc, bs.m)?;
        lc -= 1;
    }

    let level = level.min(entry_level);
    let mut lc = level;
    loop {
        let lm = hnsw_get_layer_m(bs.m, lc);
        let w = search_layer_mem(
            &bs.graph,
            &mut support,
            q,
            ep.clone(),
            bs.ef_construction,
            lc,
            bs.m,
        )?;

        let mut lw: Vec<MemCandidate> = w
            .iter()
            .map(|(e, d)| MemCandidate { element: *e, distance: *d as f32, closer: false })
            .collect();

        let layer_idx = (bs.graph.elems[element as usize].level as i32 - lc) as usize;
        let mut closer_set = bs.graph.elems[element as usize].neighbors[layer_idx].closer_set;
        let mut selected: Vec<MemCandidate> = Vec::new();
        select_neighbors_mem(
            &bs.graph,
            &mut support,
            &mut lw,
            lm,
            &mut closer_set,
            None,
            None,
            false,
            &mut selected,
        )?;
        {
            let na = &mut bs.graph.elems[element as usize].neighbors[layer_idx];
            na.items = selected;
            na.closer_set = closer_set;
        }

        ep = w;
        if lc == 0 {
            break;
        }
        lc -= 1;
    }
    bs.support = support;
    Ok(())
}

// HnswUpdateConnection (in-memory): link `new_element` into `neighbors`.
// C's SelectNeighbors candidate list aliases neighbors->items, so the closer
// flags it computes persist in the array (and the replacement newHc carries
// its computed flag); mirror both write-backs here.
fn update_connection_mem(
    graph: &Graph<'_>,
    support: &mut HnswSupport,
    neighbors: &mut Vec<MemCandidate>,
    closer_set: &mut bool,
    new_element: u32,
    distance: f32,
    lm: i32,
) -> PgResult<()> {
    let new_hc = MemCandidate { element: new_element, distance, closer: false };
    if (neighbors.len() as i32) < lm {
        neighbors.push(new_hc);
        return Ok(());
    }

    // Shrink connections.
    let mut c: Vec<MemCandidate> = neighbors.iter().map(mem_candidate_clone).collect();
    c.push(new_hc);
    let new_idx = c.len() - 1;
    let mut pruned: Option<MemCandidate> = None;
    let mut selected: Vec<MemCandidate> = Vec::new();
    select_neighbors_mem(
        graph,
        support,
        &mut c,
        lm,
        closer_set,
        Some(new_idx),
        Some(&mut pruned),
        true,
        &mut selected,
    )?;
    // Closer flags computed in place land in the neighbor array even on the
    // pruned==NULL early return (c[0..len] is index-aligned with neighbors).
    for (slot, cand) in neighbors.iter_mut().zip(c.iter()) {
        slot.closer = cand.closer;
    }
    // Should not happen (C returns without linking).
    let Some(pruned) = pruned else { return Ok(()) };
    for slot in neighbors.iter_mut() {
        if slot.element == pruned.element {
            *slot = mem_candidate_clone(&c[new_idx]);
            break;
        }
    }
    Ok(())
}

// UpdateNeighborsInMemory.
fn update_neighbors_mem(bs: &mut BuildState<'_, '_, '_>, e: u32) -> PgResult<()> {
    let level = bs.graph.elems[e as usize].level as i32;
    let mut support = bs.support.clone();
    for lc in (0..=level).rev() {
        let lm = hnsw_get_layer_m(bs.m, lc);
        let layer_idx = (level - lc) as usize;
        let items: Vec<MemCandidate> = bs.graph.elems[e as usize].neighbors[layer_idx]
            .items
            .iter()
            .map(mem_candidate_clone)
            .collect();
        for hc in items {
            let n_level = bs.graph.elems[hc.element as usize].level as i32;
            let n_layer_idx = (n_level - lc) as usize;
            let mut neighbors: Vec<MemCandidate> = bs.graph.elems[hc.element as usize].neighbors
                [n_layer_idx]
                .items
                .iter()
                .map(mem_candidate_clone)
                .collect();
            let mut closer_set =
                bs.graph.elems[hc.element as usize].neighbors[n_layer_idx].closer_set;

            update_connection_mem(
                &bs.graph,
                &mut support,
                &mut neighbors,
                &mut closer_set,
                e,
                hc.distance,
                lm,
            )?;

            let na = &mut bs.graph.elems[hc.element as usize].neighbors[n_layer_idx];
            na.items = neighbors;
            na.closer_set = closer_set;
        }
    }
    bs.support = support;
    Ok(())
}

// FindDuplicateInMemory + AddDuplicateInMemory.
fn find_duplicate_mem(bs: &mut BuildState<'_, '_, '_>, element: u32) -> PgResult<bool> {
    let level = bs.graph.elems[element as usize].level as i32;
    let layer0 = level as usize;
    let neighbor_ids: Vec<u32> = bs.graph.elems[element as usize].neighbors[layer0]
        .items
        .iter()
        .map(|hc| hc.element)
        .collect();
    for dup in neighbor_ids {
        let equal = {
            let a = &bs.graph.elems[element as usize].value;
            let b = &bs.graph.elems[dup as usize].value;
            a.as_slice() == b.as_slice()
        };
        if !equal {
            return Ok(false);
        }
        if (bs.graph.elems[dup as usize].heaptids_len as usize) < HNSW_HEAPTIDS {
            let tid = bs.graph.elems[element as usize].heaptids[0];
            let d = &mut bs.graph.elems[dup as usize];
            let n = d.heaptids_len as usize;
            d.heaptids[n] = tid;
            d.heaptids_len += 1;
            return Ok(true);
        }
    }
    Ok(false)
}

// InsertTupleInMemory + UpdateGraphInMemory.
fn insert_tuple_in_memory(bs: &mut BuildState<'_, '_, '_>, element: u32) -> PgResult<()> {
    let entry_point = bs.graph.entry_point;
    find_element_neighbors_mem(bs, element, entry_point)?;
    if find_duplicate_mem(bs, element)? {
        return Ok(());
    }
    // AddElementInMemory: only non-duplicates join the flush list.
    bs.graph.head.push(element);
    update_neighbors_mem(bs, element)?;
    let promote = match entry_point {
        None => true,
        Some(ep) => bs.graph.elems[element as usize].level > bs.graph.elems[ep as usize].level,
    };
    if promote {
        bs.graph.entry_point = Some(element);
    }
    Ok(())
}

// ---- flush to disk ----

fn create_meta_page(bs: &BuildState<'_, '_, '_>) -> PgResult<()> {
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

fn serialize_element_tuple(buf: &mut Vec<u8>, e: &MemElement<'_>) {
    let size = element_tuple_size(e.value.len());
    buf.clear();
    buf.resize(size, 0);
    buf[0] = HNSW_ELEMENT_TUPLE_TYPE;
    buf[1] = e.level;
    buf[2] = 0;
    buf[3] = e.version;
    for i in 0..HNSW_HEAPTIDS {
        let b = if i < e.heaptids_len as usize {
            ipd_to_bytes(&e.heaptids[i])
        } else {
            itemptr_encode(INVALID_BLOCK, INVALID_OFFSET)
        };
        buf[4 + i * 6..4 + i * 6 + 6].copy_from_slice(&b);
    }
    buf[4 + HNSW_HEAPTIDS * 6..4 + HNSW_HEAPTIDS * 6 + 6]
        .copy_from_slice(&itemptr_encode(e.neighbor_page, e.neighbor_offno));
    buf[ELEMENT_DATA_OFFSET..ELEMENT_DATA_OFFSET + e.value.len()].copy_from_slice(&e.value);
}

fn serialize_neighbor_tuple(buf: &mut Vec<u8>, graph: &Graph<'_>, e: &MemElement<'_>, m: i32) {
    let size = neighbor_tuple_size(e.level, m);
    buf.clear();
    buf.resize(size, 0);
    buf[0] = HNSW_NEIGHBOR_TUPLE_TYPE;
    buf[1] = e.version;
    let mut idx = 0usize;
    for lc in (0..=e.level as i32).rev() {
        let na = &e.neighbors[(e.level as i32 - lc) as usize];
        let lm = hnsw_get_layer_m(m, lc);
        for i in 0..lm as usize {
            let b = if i < na.items.len() {
                let ne = &graph.elems[na.items[i].element as usize];
                itemptr_encode(ne.blkno, ne.offno)
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

// CreateGraphPages: iterate newest-first (C list-prepend order).
fn create_graph_pages(bs: &mut BuildState<'_, '_, '_>) -> PgResult<()> {
    let max_size = HNSW_MAX_SIZE;
    let mut etup: Vec<u8> = Vec::new();
    let mut ntup_placeholder: Vec<u8> = Vec::new();

    let mut buf = new_buffer(bs.index, bs.fork_num)?;
    init_page(buf);

    // C iterates graph->head (non-duplicates only), newest-first.
    let order: Vec<usize> = bs.graph.head.iter().rev().map(|&e| e as usize).collect();
    for i in order {
        let (etup_size, ntup_size) = {
            let e = &bs.graph.elems[i];
            (
                element_tuple_size(e.value.len()),
                neighbor_tuple_size(e.level, bs.m),
            )
        };
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

        {
            // SAFETY: lock held.
            let pr = unsafe {
                types_storage::bufpage::PageRef::from_raw(
                    bufmgr_seams::buffer_get_page::call(buf),
                )
            };
            let blkno = bufmgr::BufferGetBlockNumber(buf);
            let offno = pr.max_offset_number() + 1;
            let e = &mut bs.graph.elems[i];
            e.blkno = blkno;
            e.offno = offno;
            if combined_size <= max_size {
                e.neighbor_page = blkno;
                e.neighbor_offno = offno + 1;
            } else {
                e.neighbor_page = blkno + 1;
                e.neighbor_offno = 1;
            }
        }

        serialize_element_tuple(&mut etup, &bs.graph.elems[i]);
        let e_offno = bs.graph.elems[i].offno;
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
        let n_offno = bs.graph.elems[i].neighbor_offno;
        page_add(bs.index, buf, &ntup_placeholder, n_offno)?;
    }

    let insert_page = bufmgr::BufferGetBlockNumber(buf);
    MarkBufferDirty(buf)?;
    UnlockReleaseBuffer(buf)?;

    let entry = bs.graph.entry_point.map(|ep| {
        let e = &bs.graph.elems[ep as usize];
        (e.blkno, e.offno, e.level as i16)
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
fn write_neighbor_tuples(bs: &mut BuildState<'_, '_, '_>) -> PgResult<()> {
    let mut ntup: Vec<u8> = Vec::new();
    // Same head-list walk as create_graph_pages: duplicates have no tuples.
    let order: Vec<usize> = bs.graph.head.iter().rev().map(|&e| e as usize).collect();
    for i in order {
        postgres_seams::check_for_interrupts::call()?;
        let (neighbor_page, neighbor_offno, ntup_size) = {
            let e = &bs.graph.elems[i];
            (
                e.neighbor_page,
                e.neighbor_offno,
                neighbor_tuple_size(e.level, bs.m),
            )
        };
        serialize_neighbor_tuple(&mut ntup, &bs.graph, &bs.graph.elems[i], bs.m);
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

fn flush_pages(bs: &mut BuildState<'_, '_, '_>) -> PgResult<()> {
    create_meta_page(bs)?;
    create_graph_pages(bs)?;
    write_neighbor_tuples(bs)?;
    bs.graph.flushed = true;
    // C resets graphCtx; the arena is dropped with the build state.
    bs.graph.elems.clear();
    bs.graph.head.clear();
    bs.graph.entry_point = None;
    Ok(())
}

// InsertTuple (build path).
fn insert_tuple(
    bs: &mut BuildState<'_, '_, '_>,
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

    if bs.graph.flushed {
        let mut support = bs.support.clone();
        let r = insert_tuple_on_disk(bs.index, &mut support, &img, heaptid, true);
        bs.support = support;
        return r.map(|_| true);
    }

    // C checks memoryUsed (+ zero serial margin) against memoryTotal BEFORE
    // HnswInitElement draws the level, so the PRNG stream is not consumed by
    // a tuple that diverts to the on-disk path at the flush transition.
    if bs.graph.memory_used >= bs.graph.memory_total {
        if !bs.graph.flushed {
            elog::ereport(NOTICE)
                .errmsg(format!(
                    "hnsw graph no longer fits into maintenance_work_mem after {} tuples",
                    bs.graph.indtuples as i64
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

    // Memory accounting mirrors HnswMemoryContextAlloc: element struct +
    // neighbor arrays + value image, approximated by allocation sizes.
    let level = random_level(bs.ml, bs.max_level);
    let mut neighbors_bytes = 0usize;
    for lc in 0..=level as i32 {
        neighbors_bytes +=
            hnsw_get_layer_m(bs.m, lc) as usize * core::mem::size_of::<MemCandidate>() + 16;
    }
    let elem_bytes = core::mem::size_of::<MemElement<'_>>() + neighbors_bytes + img.len();
    bs.graph.memory_used += elem_bytes;

    let gmcx = bs.graph.mcx;
    let mut value: PgVec<'_, u8> = mcx::vec_with_capacity_in_infallible(gmcx, img.len());
    let _ = mcx::vec_append_bytes(&mut value, &img);

    let mut neighbors: Vec<MemNeighborArray> = Vec::with_capacity(level as usize + 1);
    for _ in 0..=level as i32 {
        neighbors.push(MemNeighborArray { items: Vec::new(), closer_set: false });
    }
    let mut heaptids = [ItemPointerData::invalid(); HNSW_HEAPTIDS];
    heaptids[0] = *heaptid;
    bs.graph.elems.push(MemElement {
        heaptids,
        heaptids_len: 1,
        level,
        version: 1,
        value,
        neighbors,
        blkno: INVALID_BLOCK,
        offno: INVALID_OFFSET,
        neighbor_page: INVALID_BLOCK,
        neighbor_offno: INVALID_OFFSET,
    });
    let element = (bs.graph.elems.len() - 1) as u32;

    insert_tuple_in_memory(bs, element)?;
    Ok(true)
}

fn init_build_state<'a, 'g, 'mcx>(
    heap: Option<&'a Relation<'mcx>>,
    index: &'a Relation<'mcx>,
    fork_num: ForkNumber,
    gmcx: Mcx<'g>,
) -> PgResult<BuildState<'a, 'g, 'mcx>> {
    let max_dims = check_type_supported(index)?;
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
        graph: Graph {
            mcx: gmcx,
            elems: Vec::new(),
            head: Vec::new(),
            entry_point: None,
            memory_used: 0,
            memory_total: init_small::globals::maintenance_work_mem() as usize * 1024,
            flushed: false,
            indtuples: 0.0,
        },
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
    let graph_ctx = mcx::MemoryContext::new_bump("Hnsw build graph context");
    let gmcx = graph_ctx.mcx();
    let mut bs = init_build_state(heap, index, fork_num, gmcx)?;

    if let (Some(heap), Some(index_info)) = (bs.heap, index_info) {
        let mut inner_err: Option<Box<PgError>> = None;
        // BuildState is threaded via raw pointer: the callback is FnMut and
        // borrows would alias bs.
        let bs_ptr: *mut BuildState<'_, '_, 'mcx> = &mut bs;
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
                        bs.graph.indtuples += 1.0;
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

    if !bs.graph.flushed {
        flush_pages(&mut bs)?;
    }

    if relation_needs_wal(index) || fork_num == ForkNumber::INIT_FORKNUM {
        let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(index, fork_num)?;
        xloginsert::log_newpage_range(index, fork_num, 0, nblocks, true)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: bs.reltuples,
        index_tuples: bs.graph.indtuples,
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

#[cfg(test)]
mod tests {
    use super::*;

    // C HnswUpdateConnection mutates neighbors->items[i].closer through the
    // shared candidate-list pointers; these tests pin that the in-memory port
    // writes the computed closer flags (and the flagged newHc) back into the
    // neighbor array instead of leaving stale flags for later pruning.

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
        }
    }

    fn graph_1d<'g>(mcx: Mcx<'g>, xs: &[f32]) -> Graph<'g> {
        let mut elems = Vec::new();
        for &x in xs {
            let mut b = pgvector::vec::VecBuilder::new(mcx, 1).unwrap();
            b.set(0, x);
            elems.push(MemElement {
                heaptids: [ItemPointerData::invalid(); HNSW_HEAPTIDS],
                heaptids_len: 0,
                level: 0,
                version: 1,
                value: b.image(),
                neighbors: Vec::new(),
                blkno: INVALID_BLOCK,
                offno: INVALID_OFFSET,
                neighbor_page: INVALID_BLOCK,
                neighbor_offno: INVALID_OFFSET,
            });
        }
        Graph {
            mcx,
            elems,
            head: Vec::new(),
            entry_point: None,
            memory_used: 0,
            memory_total: usize::MAX,
            flushed: false,
            indtuples: 0.0,
        }
    }

    // New candidate survives selection: the pruned slot is replaced by the
    // newHc carrying its computed closer=true, and the surviving original
    // neighbor's recomputed closer=true is written back.
    #[test]
    fn update_connection_writes_back_closer_flags_on_replace() {
        let owner = mcx::MemoryContext::new_bump("test");
        let mcx = owner.mcx();
        // element 0 at 1.0, element 1 at 1.1, element 2 (new) at -1.05;
        // owner is conceptually at 0.0, distances squared below.
        let graph = graph_1d(mcx, &[1.0, 1.1, -1.05]);
        let mut sp = support();
        // Stale flags deliberately wrong (false); C recomputes in place.
        let mut neighbors = vec![
            MemCandidate { element: 0, distance: 1.0, closer: false },
            MemCandidate { element: 1, distance: 1.21, closer: false },
        ];
        let mut closer_set = false;
        update_connection_mem(&graph, &mut sp, &mut neighbors, &mut closer_set, 2, 1.1025, 2)
            .unwrap();
        assert!(closer_set, "sortCandidates=true sets closerSet");
        // Selection keeps 0 (closer) and new 2 (closer vs {0}: d(2,0)^2=4.2 > 1.1025);
        // 1 is never popped (r fills first), pruned = leftover 1 → replaced by newHc.
        let flags: Vec<(u32, bool)> =
            neighbors.iter().map(|n| (n.element, n.closer)).collect();
        assert_eq!(flags, vec![(0, true), (2, true)]);
    }

    // New candidate is itself pruned (kept-neighbor case): no replacement,
    // but the surviving array must carry the freshly computed flags —
    // including a false flag for the not-closer neighbor (wd-kept).
    #[test]
    fn update_connection_writes_back_closer_flags_without_replace() {
        let owner = mcx::MemoryContext::new_bump("test");
        let mcx = owner.mcx();
        // 0 at 1.0, 1 at 1.1, new 2 at 1.2: 1 and 2 are both not-closer to 0;
        // wd-fill keeps 1, prunes the new candidate 2.
        let graph = graph_1d(mcx, &[1.0, 1.1, 1.2]);
        let mut sp = support();
        // Stale flags deliberately wrong (true).
        let mut neighbors = vec![
            MemCandidate { element: 0, distance: 1.0, closer: true },
            MemCandidate { element: 1, distance: 1.21, closer: true },
        ];
        let mut closer_set = false;
        update_connection_mem(&graph, &mut sp, &mut neighbors, &mut closer_set, 2, 1.44, 2)
            .unwrap();
        assert!(closer_set);
        let flags: Vec<(u32, bool)> =
            neighbors.iter().map(|n| (n.element, n.closer)).collect();
        assert_eq!(flags, vec![(0, true), (1, false)]);
    }
}
