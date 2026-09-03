//! pgvector 0.8.5 hnswbuild.c / hnsw.h in-memory graph, shared between build
//! participants. C keeps the graph in a DSM area with relative pointers and an
//! LWLock per element; here the arena is an Arc-slice behind a RwLock and the
//! per-element lock is a Mutex over the mutable parts. DIVERGENCES (recorded):
//! one implementation serves serial and parallel builds (C: base == NULL vs
//! relptr); indtuples is an integer counter (C: double under a spinlock).
//!
//! Not yet consumed outside this module (Task 2 wires these into `lib.rs`'s
//! build state), so the module is allowed to look unused for now.
#![allow(dead_code)] // consumed from Task 2 onward

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::{Arc, Mutex, RwLock};
use types_core::BlockNumber;
use types_hnsw::{hnsw_get_layer_m, HNSW_HEAPTIDS};
use types_tuple::itemptr::ItemPointerData;
use pgvector_hnsw::layout::{INVALID_BLOCK, INVALID_OFFSET};

/// C: `HnswCandidate`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Candidate {
    pub element: u32,
    pub distance: f32,
    pub closer: bool,
}

/// C: `HnswNeighborArray`.
#[derive(Default)]
pub struct NeighborArray {
    pub items: Vec<Candidate>,
    pub closer_set: bool,
}

/// On-disk location assigned by CreateGraphPages / flush; written by flush
/// only, hence its own lock separate from `neighbors`.
pub struct Placement {
    pub blkno: BlockNumber,
    pub offno: u16,
    pub neighbor_page: BlockNumber,
    pub neighbor_offno: u16,
}

/// C: the `heaptids`/`heaptidsLength` fields of `HnswElementData`.
pub struct HeapTids {
    pub tids: [ItemPointerData; HNSW_HEAPTIDS],
    pub len: u8,
}

/// C: `HnswElementData`. `level`, `version` and `value` are immutable after
/// `alloc_element` (C fills them once under `HnswInitElement`/copy-in and
/// never mutates them again); the remaining fields are guarded individually
/// (C: per-element LWLock covers heaptids + neighbors together, but nothing
/// here requires that they be updated atomically with each other, so they
/// are split into independent Mutexes for finer-grained locking).
pub struct SharedElement {
    pub level: u8,
    pub version: u8,
    pub value: Box<[u8]>,
    pub heaptids: Mutex<HeapTids>,
    pub neighbors: Mutex<Vec<NeighborArray>>,
    pub placement: Mutex<Placement>,
}

/// C: `HnswGraph`, held in the DSM area for parallel builds (or a plain
/// backend-local struct for serial builds). Here one implementation serves
/// both: elements are behind `Arc` so `elem()` can hand out an owned
/// reference without holding the collection lock.
pub struct SharedGraph {
    elems: RwLock<Vec<Arc<SharedElement>>>,
    head: Mutex<Vec<u32>>,
    entry_point: Mutex<Option<u32>>,
    pub entry_lock: RwLock<()>,
    pub entry_wait_lock: Mutex<()>,
    memory_used: AtomicUsize,
    pub memory_total: usize,
    flushed: AtomicBool,
    pub flush_lock: Mutex<()>,
    indtuples: AtomicU64,
}

/// Lock a Mutex, recovering the guard on poison rather than propagating the
/// panic to every other participant (C has no notion of lock poisoning).
fn lk<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl SharedGraph {
    pub fn new(memory_total: usize) -> Self {
        SharedGraph {
            elems: RwLock::new(Vec::new()),
            head: Mutex::new(Vec::new()),
            entry_point: Mutex::new(None),
            entry_lock: RwLock::new(()),
            entry_wait_lock: Mutex::new(()),
            memory_used: AtomicUsize::new(0),
            memory_total,
            flushed: AtomicBool::new(false),
            flush_lock: Mutex::new(()),
            indtuples: AtomicU64::new(0),
        }
    }

    /// C: `HnswInitElement` + `HnswInitNeighbors` + value copy, with the
    /// memory accounting `InsertTuple` performs (HnswMemoryContextAlloc
    /// equivalents). `value` is copied into a `Box<[u8]>` instead of the bump
    /// `Mcx` because worker threads must not allocate from the leader's
    /// memory context (thread-affine). Ids are dense and append-only: the
    /// returned id is always the previous `len()`.
    pub fn alloc_element(&self, heaptid: ItemPointerData, level: u8, value: &[u8], m: i32) -> u32 {
        let mut neighbors_bytes = 0usize;
        let mut neighbors = Vec::with_capacity(level as usize + 1);
        for lc in 0..=level as i32 {
            neighbors_bytes += hnsw_get_layer_m(m, lc) as usize * core::mem::size_of::<Candidate>() + 16;
            neighbors.push(NeighborArray::default());
        }
        let mut tids = [ItemPointerData::invalid(); HNSW_HEAPTIDS];
        tids[0] = heaptid;
        let e = Arc::new(SharedElement {
            level,
            version: 1,
            value: value.to_vec().into_boxed_slice(),
            heaptids: Mutex::new(HeapTids { tids, len: 1 }),
            neighbors: Mutex::new(neighbors),
            placement: Mutex::new(Placement {
                blkno: INVALID_BLOCK,
                offno: INVALID_OFFSET,
                neighbor_page: INVALID_BLOCK,
                neighbor_offno: INVALID_OFFSET,
            }),
        });
        self.memory_used.fetch_add(
            core::mem::size_of::<SharedElement>() + neighbors_bytes + value.len(),
            Relaxed,
        );
        let mut v = self.elems.write().unwrap_or_else(|e| e.into_inner());
        v.push(e);
        (v.len() - 1) as u32
    }

    /// Clones the Arc under the read lock; the returned handle is then usable
    /// without holding any lock on the collection itself.
    pub fn elem(&self, id: u32) -> Arc<SharedElement> {
        Arc::clone(&self.elems.read().unwrap_or_else(|e| e.into_inner())[id as usize])
    }

    pub fn len(&self) -> usize {
        self.elems.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn entry_point(&self) -> Option<u32> {
        *lk(&self.entry_point)
    }
    pub fn set_entry_point(&self, e: Option<u32>) {
        *lk(&self.entry_point) = e;
    }

    pub fn add_to_head(&self, e: u32) {
        lk(&self.head).push(e);
    }
    pub fn take_head(&self) -> Vec<u32> {
        std::mem::take(&mut *lk(&self.head))
    }

    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Relaxed)
    }
    pub fn memory_exhausted(&self) -> bool {
        self.memory_used() >= self.memory_total
    }

    pub fn flushed(&self) -> bool {
        self.flushed.load(Acquire)
    }
    pub fn set_flushed(&self) {
        self.flushed.store(true, Release)
    }

    pub fn indtuples(&self) -> u64 {
        self.indtuples.load(Relaxed)
    }
    pub fn inc_indtuples(&self) {
        self.indtuples.fetch_add(1, Relaxed);
    }

    /// Drops elems/head/entry point like C's graphCtx reset after flush.
    pub fn clear_after_flush(&self) {
        self.elems.write().unwrap_or_else(|e| e.into_inner()).clear();
        lk(&self.head).clear();
        *lk(&self.entry_point) = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tid(n: u16) -> ItemPointerData {
        ItemPointerData::new(1, n)
    }

    #[test]
    fn alloc_is_append_only_and_accounts_memory() {
        let g = SharedGraph::new(1 << 20);
        let a = g.alloc_element(tid(1), 0, &[0u8; 16], 16);
        let b = g.alloc_element(tid(2), 2, &[0u8; 16], 16);
        assert_eq!((a, b), (0, 1));
        assert_eq!(g.elem(b).neighbors.lock().unwrap().len(), 3, "one NeighborArray per layer 0..=level");
        assert!(g.memory_used() > 32);
        assert!(!g.memory_exhausted());
        let small = SharedGraph::new(1);
        small.alloc_element(tid(1), 0, &[0u8; 16], 16);
        assert!(small.memory_exhausted());
    }

    #[test]
    fn concurrent_alloc_yields_dense_unique_ids() {
        let g = Arc::new(SharedGraph::new(usize::MAX));
        let hs: Vec<_> = (0..4)
            .map(|t| {
                let g = Arc::clone(&g);
                thread::spawn(move || {
                    (0..500u16)
                        .map(|i| g.alloc_element(tid(i), 0, &[t as u8; 8], 16))
                        .collect::<Vec<u32>>()
                })
            })
            .collect();
        let mut all: Vec<u32> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
        all.sort_unstable();
        assert_eq!(all, (0..2000u32).collect::<Vec<_>>());
        assert_eq!(g.len(), 2000);
    }

    #[test]
    fn head_entry_point_and_flush_flags() {
        let g = SharedGraph::new(usize::MAX);
        let e = g.alloc_element(tid(1), 1, &[1u8; 8], 16);
        assert_eq!(g.entry_point(), None);
        g.set_entry_point(Some(e));
        g.add_to_head(e);
        g.inc_indtuples();
        assert_eq!((g.entry_point(), g.take_head(), g.indtuples()), (Some(e), vec![e], 1));
        assert!(!g.flushed());
        g.set_flushed();
        assert!(g.flushed());
        g.clear_after_flush();
        assert_eq!((g.len(), g.entry_point()), (0, None));
    }
}
