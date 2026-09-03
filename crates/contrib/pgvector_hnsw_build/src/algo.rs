//! pgvector 0.8.5 in-memory HNSW algorithms on the thread-shared graph:
//! `hnswbuild.c`'s `InsertTupleInMemory` / `UpdateGraphInMemory` /
//! `UpdateNeighborsInMemory` / `FindDuplicateInMemory` and the in-memory arm of
//! `hnswutils.c`'s `HnswFindElementNeighbors` / `HnswSearchLayer` /
//! `SelectNeighbors` / `HnswUpdateConnection`.
//!
//! Lock protocol, mirroring C exactly:
//! * `graph.entry_wait_lock` is taken and released before reading the entry
//!   point, so a participant that needs the exclusive entry lock can make new
//!   inserters queue behind it (C: `entryWaitLock`).
//! * `graph.entry_lock` is held shared for the whole insert, or exclusive when
//!   the new element's level may promote it to entry point (C: `entryLock`).
//! * A neighbor list is only ever read as a copy taken under the owning
//!   element's `neighbors` mutex (C: `LWLockAcquire(&e->lock, LW_SHARED)` +
//!   `memcpy`), and `update_connection` runs with the *neighbor's* mutex held
//!   exclusively (C: `LWLockAcquire(&neighborElement->lock, LW_EXCLUSIVE)`).
//!
//! Lock ordering: an element mutex is never taken while another element mutex
//! is held (`update_connection` runs under the neighbor's mutex but only reads
//! other elements' immutable `value`s), and `SharedGraph::elem` takes no lock
//! at all (lock-free chunked arena, Task 5b), so an element mutex is the only
//! lock any of this code holds at a time.

use crate::graph::{lk, Candidate, SharedElement, SharedGraph};
use datum::Datum;
use pgvector_hnsw::utils as hnsw_utils;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use types_error::PgResult;
use types_hnsw::{hnsw_get_layer_m, HnswSupport, HNSW_HEAPTIDS};

/// C: `HnswGetValue` on an in-memory element. The element lives in the graph's
/// arena, which outlives the borrow.
fn value_datum(el: &SharedElement) -> Datum {
    Datum::from_usize(el.value.as_ptr() as usize)
}

/// C: `GetElementDistance` on an element already in hand (the hot path fetches
/// each element once and reads distance and level from the same reference).
fn get_distance_el(support: &mut HnswSupport, q: Datum, el: &SharedElement) -> PgResult<f64> {
    hnsw_utils::get_distance(support, q, value_datum(el))
}

/// C: `HnswSearchLayer`, in-memory arm (`index == NULL`), with
/// `HnswLoadUnvisitedFromMemory`'s shared-lock copy of the neighborhood.
pub(crate) fn search_layer(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    q: Datum,
    ep: Vec<(u32, f64)>,
    ef: i32,
    lc: i32,
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
        // HnswLoadUnvisitedFromMemory: copy the neighborhood out under the
        // element's lock, then work off the copy.
        let c_el = graph.elem(c_elem);
        let layer_idx = (c_el.level as i32 - lc) as usize;
        let neighbor_ids: Vec<u32> =
            lk(&c_el.neighbors)[layer_idx].items.iter().map(|hc| hc.element).collect();

        for e in neighbor_ids {
            if !visited.insert(e) {
                continue;
            }
            let always_add = wlen < ef;
            let (f_dist, _) = *w_heap.first().expect("W nonempty");
            // One arena lookup per neighbor: distance and level come off the
            // same reference (C dereferences the same relptr twice).
            let e_el = graph.elem(e);
            let e_distance = get_distance_el(support, q, e_el)?;
            if !(e_distance < f_dist || always_add) {
                continue;
            }
            if (e_el.level as i32) < lc {
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

/// C: `CheckElementCloser`. Reads only immutable element values, so it needs
/// no element lock (and may run with a neighbor's lock held).
fn check_element_closer(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    e: &Candidate,
    r: &[Candidate],
) -> PgResult<bool> {
    let e_value = value_datum(graph.elem(e.element));
    for ri in r {
        // One arena lookup per r element per call (C: one relptr deref).
        let ri_value = value_datum(graph.elem(ri.element));
        let distance = hnsw_utils::get_distance(support, e_value, ri_value)? as f32;
        if distance <= e.distance {
            return Ok(false);
        }
    }
    Ok(true)
}

/// C: `SelectNeighbors`. C's candidate list holds pointers into the caller's
/// neighbor array, so `e->closer` updates land in that array; we take `c`
/// mutably and write the computed closer flags back by origin index.
#[allow(clippy::too_many_arguments)]
pub(crate) fn select_neighbors(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    c: &mut [Candidate],
    lm: i32,
    closer_set: &mut bool,
    new_candidate: Option<usize>,
    pruned: Option<&mut Option<Candidate>>,
    sort_candidates: bool,
    out: &mut Vec<Candidate>,
) -> PgResult<()> {
    out.clear();
    if c.len() as i32 <= lm {
        out.extend_from_slice(c);
        return Ok(());
    }

    let mut w: Vec<Candidate> = c.to_vec();
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
        let neww: Vec<Candidate> = order.iter().map(|&i| w[i]).collect();
        let newn: Vec<bool> = order.iter().map(|&i| w_is_new[i]).collect();
        let news: Vec<usize> = order.iter().map(|&i| w_src[i]).collect();
        w = neww;
        w_is_new = newn;
        w_src = news;
    }

    let must_calculate = !*closer_set;
    let mut wd: Vec<Candidate> = Vec::with_capacity(w.len());
    let mut added: Vec<Candidate> = Vec::new();
    let mut removed_any = false;

    while !w.is_empty() && (out.len() as i32) < lm {
        let mut e = w.pop().expect("nonempty");
        let e_is_new = w_is_new.pop().expect("nonempty");
        let e_src = w_src.pop().expect("nonempty");

        if must_calculate {
            e.closer = check_element_closer(graph, support, &e, out)?;
        } else if !added.is_empty() {
            if e.closer {
                e.closer = check_element_closer(graph, support, &e, &added)?;
                if !e.closer {
                    removed_any = true;
                }
            } else if removed_any {
                e.closer = check_element_closer(graph, support, &e, out)?;
                if e.closer {
                    added.push(e);
                }
            }
        } else if e_is_new {
            e.closer = check_element_closer(graph, support, &e, out)?;
            if e.closer {
                added.push(e);
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
        out.push(wd[wdoff]);
        wdoff += 1;
    }
    if let Some(p) = pruned {
        *p = if wdoff < wd.len() { Some(wd[wdoff]) } else { w.first().copied() };
    }
    Ok(())
}

/// C: `HnswUpdateConnection` — link `new_element` into `neighbors`. C's
/// `SelectNeighbors` candidate list aliases `neighbors->items`, so the closer
/// flags it computes persist in the array (and the replacement `newHc` carries
/// its computed flag); mirror both write-backs here. The caller holds the
/// owning element's lock exclusively.
pub(crate) fn update_connection(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    neighbors: &mut Vec<Candidate>,
    closer_set: &mut bool,
    new_element: u32,
    distance: f32,
    lm: i32,
) -> PgResult<()> {
    let new_hc = Candidate { element: new_element, distance, closer: false };
    if (neighbors.len() as i32) < lm {
        neighbors.push(new_hc);
        return Ok(());
    }

    // Shrink connections.
    let mut c: Vec<Candidate> = neighbors.clone();
    c.push(new_hc);
    let new_idx = c.len() - 1;
    let mut pruned: Option<Candidate> = None;
    let mut selected: Vec<Candidate> = Vec::new();
    select_neighbors(
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
            *slot = c[new_idx];
            break;
        }
    }
    Ok(())
}

/// C: `HnswFindElementNeighbors`, in-memory arm (`index == NULL`,
/// `existing == false`). `element` is not yet linked into the graph, so no
/// other participant can reach it; its neighbor lists are still written under
/// its own lock (C relies on the same invariant and writes them unlocked).
pub(crate) fn find_element_neighbors(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    m: i32,
    ef_construction: i32,
    element: u32,
    entry_point: Option<u32>,
) -> PgResult<()> {
    let el = graph.elem(element);
    let level = el.level as i32;
    let q = value_datum(el);
    let Some(entry_point) = entry_point else { return Ok(()) };

    let ep_el = graph.elem(entry_point);
    let entry_level = ep_el.level as i32;
    let ep_dist = get_distance_el(support, q, ep_el)?;
    let mut ep: Vec<(u32, f64)> = vec![(entry_point, ep_dist)];

    // 1st phase: greedy search down to the insert level.
    let mut lc = entry_level;
    while lc >= level + 1 {
        ep = search_layer(graph, support, q, ep, 1, lc)?;
        lc -= 1;
    }

    let level = level.min(entry_level);
    let mut lc = level;
    loop {
        let lm = hnsw_get_layer_m(m, lc);
        let w = search_layer(graph, support, q, ep.clone(), ef_construction, lc)?;

        let mut lw: Vec<Candidate> = w
            .iter()
            .map(|(e, d)| Candidate { element: *e, distance: *d as f32, closer: false })
            .collect();

        let layer_idx = (el.level as i32 - lc) as usize;
        let mut closer_set = lk(&el.neighbors)[layer_idx].closer_set;
        let mut selected: Vec<Candidate> = Vec::new();
        select_neighbors(
            graph,
            support,
            &mut lw,
            lm,
            &mut closer_set,
            None,
            None,
            false,
            &mut selected,
        )?;
        {
            // AddConnections.
            let mut guard = lk(&el.neighbors);
            let na = &mut guard[layer_idx];
            na.items = selected;
            na.closer_set = closer_set;
        }

        ep = w;
        if lc == 0 {
            break;
        }
        lc -= 1;
    }
    Ok(())
}

/// C: `UpdateNeighborsInMemory`.
pub(crate) fn update_neighbors(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    m: i32,
    e: u32,
) -> PgResult<()> {
    let el = graph.elem(e);
    for lc in (0..=el.level as i32).rev() {
        let lm = hnsw_get_layer_m(m, lc);
        let layer_idx = (el.level as i32 - lc) as usize;
        // C: shared lock on e + memcpy of the layer's neighbor array.
        let items: Vec<Candidate> = lk(&el.neighbors)[layer_idx].items.clone();

        for hc in items {
            let ne = graph.elem(hc.element);
            let n_layer_idx = (ne.level as i32 - lc) as usize;
            // C: LW_EXCLUSIVE on the neighbor for the whole HnswUpdateConnection.
            let mut guard = lk(&ne.neighbors);
            let na = &mut guard[n_layer_idx];
            update_connection(graph, support, &mut na.items, &mut na.closer_set, e, hc.distance, lm)?;
        }
    }
    Ok(())
}

/// C: `FindDuplicateInMemory` + `AddDuplicateInMemory`.
pub(crate) fn find_duplicate(graph: &SharedGraph, element: u32) -> PgResult<bool> {
    let el = graph.elem(element);
    let layer0 = el.level as usize;
    let neighbor_ids: Vec<u32> =
        lk(&el.neighbors)[layer0].items.iter().map(|hc| hc.element).collect();
    // Read out of the (still private) new element before taking a dup's lock,
    // so only ever one element lock is held at a time.
    let tid = lk(&el.heaptids).tids[0];
    for dup in neighbor_ids {
        let d = graph.elem(dup);
        // Exit early since ordered by distance (C: datumIsEqual).
        if el.value != d.value {
            return Ok(false);
        }
        // AddDuplicateInMemory: C takes the dup's element lock exclusively.
        let mut tids = lk(&d.heaptids);
        if (tids.len as usize) < HNSW_HEAPTIDS {
            let n = tids.len as usize;
            tids.tids[n] = tid;
            tids.len += 1;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Either arm of C's `entryLock` acquisition in `InsertTupleInMemory`. The
/// guards are held only for their `Drop` (C: `LWLockRelease(entryLock)`).
enum EntryGuard<'a> {
    Read(#[allow(dead_code)] RwLockReadGuard<'a, ()>),
    Write(#[allow(dead_code)] RwLockWriteGuard<'a, ()>),
}

/// C: `InsertTupleInMemory` (plus the inlined `UpdateGraphInMemory`).
pub(crate) fn insert_tuple_in_memory(
    graph: &SharedGraph,
    support: &mut HnswSupport,
    m: i32,
    ef_construction: i32,
    element: u32,
) -> PgResult<()> {
    // Wait if another participant needs the exclusive entry lock.
    drop(lk(&graph.entry_wait_lock));

    // Get entry point.
    let read = graph.entry_lock.read().unwrap_or_else(|e| e.into_inner());
    let mut entry_point = graph.entry_point();
    let el_level = graph.elem(element).level;
    let promotes = |ep: Option<u32>| match ep {
        None => true,
        Some(ep) => el_level > graph.elem(ep).level,
    };

    // Prevent concurrent inserts when likely updating the entry point.
    let held = if promotes(entry_point) {
        drop(read);
        // Tell other participants to wait and get the exclusive lock.
        let wait = lk(&graph.entry_wait_lock);
        let write = graph.entry_lock.write().unwrap_or_else(|e| e.into_inner());
        drop(wait);
        // Get the latest entry point after the lock is acquired.
        entry_point = graph.entry_point();
        EntryGuard::Write(write)
    } else {
        EntryGuard::Read(read)
    };

    // Find neighbors for element.
    find_element_neighbors(graph, support, m, ef_construction, element, entry_point)?;

    // UpdateGraphInMemory: look for a duplicate first.
    if !find_duplicate(graph, element)? {
        // AddElementInMemory: only non-duplicates join the flush list.
        graph.add_to_head(element);
        // UpdateNeighborsInMemory.
        update_neighbors(graph, support, m, element)?;
        // Update entry point if needed (already have the lock).
        if promotes(entry_point) {
            graph.set_entry_point(Some(element));
        }
    }

    // Release entry lock (also on the error paths above, where C unwinds).
    drop(held);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::SharedGraph;
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

    // A graph of level-0 elements holding 1-dimensional vectors, in id order.
    fn graph_1d(xs: &[f32]) -> SharedGraph {
        let owner = mcx::MemoryContext::new_bump("test");
        let mcx = owner.mcx();
        let g = SharedGraph::new(usize::MAX);
        for (i, &x) in xs.iter().enumerate() {
            let mut b = pgvector::vec::VecBuilder::new(mcx, 1).unwrap();
            b.set(0, x);
            g.alloc_element(ItemPointerData::new(1, i as u16 + 1), 0, &b.image(), 2).unwrap();
        }
        g
    }

    // C HnswUpdateConnection mutates neighbors->items[i].closer through the
    // shared candidate-list pointers; these tests pin that the in-memory port
    // writes the computed closer flags (and the flagged newHc) back into the
    // neighbor array instead of leaving stale flags for later pruning.

    // New candidate survives selection: the pruned slot is replaced by the
    // newHc carrying its computed closer=true, and the surviving original
    // neighbor's recomputed closer=true is written back.
    #[test]
    fn update_connection_writes_back_closer_flags_on_replace() {
        // element 0 at 1.0, element 1 at 1.1, element 2 (new) at -1.05;
        // owner is conceptually at 0.0, distances squared below.
        let graph = graph_1d(&[1.0, 1.1, -1.05]);
        let mut sp = support();
        // Stale flags deliberately wrong (false); C recomputes in place.
        let mut neighbors = vec![
            Candidate { element: 0, distance: 1.0, closer: false },
            Candidate { element: 1, distance: 1.21, closer: false },
        ];
        let mut closer_set = false;
        update_connection(&graph, &mut sp, &mut neighbors, &mut closer_set, 2, 1.1025, 2).unwrap();
        assert!(closer_set, "sortCandidates=true sets closerSet");
        // Selection keeps 0 (closer) and new 2 (closer vs {0}: d(2,0)^2=4.2 > 1.1025);
        // 1 is never popped (r fills first), pruned = leftover 1 → replaced by newHc.
        let flags: Vec<(u32, bool)> = neighbors.iter().map(|n| (n.element, n.closer)).collect();
        assert_eq!(flags, vec![(0, true), (2, true)]);
    }

    // New candidate is itself pruned (kept-neighbor case): no replacement,
    // but the surviving array must carry the freshly computed flags —
    // including a false flag for the not-closer neighbor (wd-kept).
    #[test]
    fn update_connection_writes_back_closer_flags_without_replace() {
        // 0 at 1.0, 1 at 1.1, new 2 at 1.2: 1 and 2 are both not-closer to 0;
        // wd-fill keeps 1, prunes the new candidate 2.
        let graph = graph_1d(&[1.0, 1.1, 1.2]);
        let mut sp = support();
        // Stale flags deliberately wrong (true).
        let mut neighbors = vec![
            Candidate { element: 0, distance: 1.0, closer: true },
            Candidate { element: 1, distance: 1.21, closer: true },
        ];
        let mut closer_set = false;
        update_connection(&graph, &mut sp, &mut neighbors, &mut closer_set, 2, 1.44, 2).unwrap();
        assert!(closer_set);
        let flags: Vec<(u32, bool)> = neighbors.iter().map(|n| (n.element, n.closer)).collect();
        assert_eq!(flags, vec![(0, true), (1, false)]);
    }

    // Four threads insert into one graph under the C lock protocol; afterwards
    // every element must have one neighbor array per layer, no layer over its
    // HnswGetLayerM budget, no self-loops or dangling ids, and the entry point
    // must be an element of maximum level.
    #[test]
    fn four_threads_insert_2000_points_and_graph_invariants_hold() {
        use std::sync::Arc;
        use std::thread;
        let g = Arc::new(SharedGraph::new(usize::MAX));
        let (m, efc) = (16, 64);
        let hs: Vec<_> = (0..4)
            .map(|t| {
                let g = Arc::clone(&g);
                thread::spawn(move || {
                    let owner = mcx::MemoryContext::new_bump("w");
                    let mcx = owner.mcx();
                    let mut sp = support();
                    let mut rng = t as u64 * 7919 + 1;
                    for i in 0..500u16 {
                        rng = rng
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        // Distinct base value per (thread, iteration) pair, plus a
                        // small hash-derived jitter (well under the 1/2000 gap
                        // between adjacent base values) so no two of the 2000
                        // points can collide and spuriously tie for max level.
                        let global_idx = (t as u64) * 500 + i as u64;
                        let jitter = ((rng >> 40) as f32 / 16777216.0) * 0.0001;
                        let x = (global_idx as f32) / 2000.0 + jitter;
                        let mut b = pgvector::vec::VecBuilder::new(mcx, 1).unwrap();
                        b.set(0, x);
                        let level = pgvector_hnsw::insert::random_level(
                            types_hnsw::hnsw_get_ml(m),
                            pgvector_hnsw::layout::hnsw_get_max_level(m),
                        );
                        let e = g
                            .alloc_element(
                                ItemPointerData::new(t as u32 + 1, i + 1),
                                level,
                                &b.image(),
                                m,
                            )
                            .unwrap();
                        insert_tuple_in_memory(&g, &mut sp, m, efc, e).unwrap();
                        g.inc_indtuples();
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(g.len(), 2000);
        let ep = g.entry_point().expect("entry point set");
        let max_level = (0..2000u32).map(|i| g.elem(i).level).max().unwrap();
        assert_eq!(g.elem(ep).level, max_level, "entry point has the max level");
        for i in 0..2000u32 {
            let e = g.elem(i);
            let n = e.neighbors.lock().unwrap();
            assert_eq!(n.len(), e.level as usize + 1);
            for (layer_idx, na) in n.iter().enumerate() {
                let lc = e.level as i32 - layer_idx as i32;
                assert!(na.items.len() as i32 <= types_hnsw::hnsw_get_layer_m(m, lc));
                for c in &na.items {
                    assert!((c.element as usize) < 2000 && c.element != i);
                }
            }
        }
    }
}
