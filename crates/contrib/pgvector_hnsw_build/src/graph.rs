//! pgvector 0.8.5 hnswbuild.c / hnsw.h in-memory graph, shared between build
//! participants. C keeps the graph in a DSM area with relative pointers and an
//! LWLock per element; here the arena is a lock-free append-only chunked
//! arena (fixed chunk directory of `AtomicPtr<Chunk>`, 1024 elements per
//! chunk) and the per-element lock is a Mutex over the mutable parts.
//! `elem(id)` is therefore a plain Acquire load plus an index — no collection
//! lock and no refcount traffic on the path every `search_layer` step takes.
//! DIVERGENCES (recorded):
//! one implementation serves serial and parallel builds (C: base == NULL vs
//! relptr); indtuples is an integer counter (C: double under a spinlock);
//! memory accounting charges `size_of::<SharedElement>()` per element (the
//! pre-refactor Rust charged `size_of::<MemElement>()`, and C charges its own
//! `HnswElementData`/`HnswCandidate` struct sizes), so the flush point and the
//! "hnsw graph no longer fits into maintenance_work_mem after N tuples" count
//! differ from both the pre-refactor Rust and from C; `clear_after_flush`
//! resets only the logical state (length, head, entry point) and keeps the
//! chunks allocated until the `SharedGraph` is dropped at the end of the
//! build, where C resets `graphCtx` and returns the memory to the OS — up to
//! `maintenance_work_mem` therefore stays allocated during the on-disk phase;
//! and the chunk directory is sized once from `memory_total` (clamped at
//! `MAX_CHUNKS`), so a `maintenance_work_mem` above ~40GB flushes at
//! `MAX_CHUNKS * CHUNK_SIZE` elements rather than at the byte budget.

use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::{Mutex, RwLock};
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

/// Elements per arena chunk. Chunks are allocated one at a time under
/// `alloc_lock` and never reallocated, so a `&SharedElement` handed out by
/// `elem` stays valid (and at a fixed address) until the graph is dropped.
pub(crate) const CHUNK_SIZE: usize = 1024;

/// Upper bound on the chunk directory, i.e. `MAX_CHUNKS * CHUNK_SIZE`
/// elements (~268M, ≈40GB of `maintenance_work_mem` at the minimum charge per
/// element). Beyond this the arena reports itself exhausted and the build
/// flushes, exactly as it does when the byte budget runs out.
const MAX_CHUNKS: usize = 1 << 18;

/// One arena slot. Slots below the published length hold an initialized
/// `SharedElement`; the rest are uninitialized.
type Slot = MaybeUninit<SharedElement>;

/// Number of directory entries needed for `memory_total` bytes of graph. Each
/// element is charged at least `size_of::<SharedElement>()` by
/// `alloc_element`, so that many elements can never fit in the budget; two
/// spare chunks cover the participants that pass the memory check
/// concurrently and then allocate (C: the same window between releasing the
/// allocator lock and `HnswAlloc`).
fn dir_len_for(memory_total: usize) -> usize {
    let max_elems = memory_total / core::mem::size_of::<SharedElement>();
    (max_elems / CHUNK_SIZE + 2).clamp(1, MAX_CHUNKS)
}

fn alloc_chunk() -> *mut Slot {
    let chunk: Box<[Slot]> = (0..CHUNK_SIZE)
        .map(|_| MaybeUninit::uninit())
        .collect::<Vec<Slot>>()
        .into_boxed_slice();
    // Thin pointer to the first slot; the length is the constant CHUNK_SIZE,
    // so `Drop` can rebuild the box from it.
    Box::into_raw(chunk) as *mut Slot
}

/// C: `HnswGraph`, held in the DSM area for parallel builds (or a plain
/// backend-local struct for serial builds). Here one implementation serves
/// both: elements live in an append-only chunked arena, so `elem()` returns a
/// borrowed `&SharedElement` without taking any lock and without touching a
/// refcount (C: a relative pointer into the DSM area, free to dereference).
pub struct SharedGraph {
    /// Chunk directory, allocated once in `new()` and never resized. Entry
    /// `c` is null until an element with id in `c * CHUNK_SIZE ..` is
    /// allocated, and is never cleared afterwards.
    dir: Box<[AtomicPtr<Slot>]>,
    /// Published element count: slots `0..len` are initialized. Written with
    /// Release under `alloc_lock` after the element is in place.
    len: AtomicU32,
    /// `dir.len() * CHUNK_SIZE`.
    capacity: usize,
    /// C: `graph->allocatorLock` — serialises allocation only. Readers never
    /// take it.
    alloc_lock: Mutex<()>,
    /// Set by `clear_after_flush`: the logical graph is gone even though the
    /// storage is still there (see the DIVERGENCE in the module header).
    cleared: AtomicBool,
    head: Mutex<Vec<u32>>,
    entry_point: Mutex<Option<u32>>,
    pub entry_lock: RwLock<()>,
    pub entry_wait_lock: Mutex<()>,
    memory_used: AtomicUsize,
    pub memory_total: usize,
    flushed: AtomicBool,
    /// C: `graph->flushLock`, serialising the in-memory→disk transition
    /// between participants. Held SHARED across an in-memory insert (so a
    /// flush cannot start under one) and EXCLUSIVE around the flush decision
    /// itself. Lock order is flush_lock → entry_lock, never the reverse.
    pub flush_lock: RwLock<()>,
    indtuples: AtomicU64,
}

// `AtomicPtr` is unconditionally Send + Sync, so `SharedGraph` derives both
// automatically — but the arena hands out `&SharedElement` to every
// participant, which is only sound if the element itself is shareable. Pin
// that requirement at compile time rather than assuming it.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<SharedElement>();
    assert_send_sync::<SharedGraph>();
};

/// Lock a Mutex, recovering the guard on poison rather than propagating the
/// panic to every other participant (C has no notion of lock poisoning).
pub(crate) fn lk<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl SharedGraph {
    pub fn new(memory_total: usize) -> Self {
        let ndir = dir_len_for(memory_total);
        let dir: Box<[AtomicPtr<Slot>]> = (0..ndir)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SharedGraph {
            dir,
            len: AtomicU32::new(0),
            capacity: ndir * CHUNK_SIZE,
            alloc_lock: Mutex::new(()),
            cleared: AtomicBool::new(false),
            head: Mutex::new(Vec::new()),
            entry_point: Mutex::new(None),
            entry_lock: RwLock::new(()),
            entry_wait_lock: Mutex::new(()),
            memory_used: AtomicUsize::new(0),
            memory_total,
            flushed: AtomicBool::new(false),
            flush_lock: RwLock::new(()),
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
        let e = SharedElement {
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
        };
        self.memory_used.fetch_add(
            core::mem::size_of::<SharedElement>() + neighbors_bytes + value.len(),
            Relaxed,
        );

        // C: HnswAlloc under graph->allocatorLock. Only the allocation is
        // serialised; readers never take this lock.
        let _guard = lk(&self.alloc_lock);
        let id = self.len.load(Relaxed) as usize;
        assert!(
            id < self.capacity,
            "hnsw graph arena capacity exceeded ({} elements)",
            self.capacity
        );
        let (c, off) = (id / CHUNK_SIZE, id % CHUNK_SIZE);
        let mut chunk = self.dir[c].load(Acquire);
        if chunk.is_null() {
            chunk = alloc_chunk();
            // Release: the chunk allocation happens-before any Acquire load
            // of this directory entry.
            self.dir[c].store(chunk, Release);
        }
        // SAFETY: `off < CHUNK_SIZE` and `chunk` points at a `CHUNK_SIZE`
        // slot array. The slot is beyond the published length, so no other
        // thread may read it; `alloc_lock` excludes other writers.
        unsafe { ptr::write(chunk.add(off) as *mut SharedElement, e) };
        // Release: publishes the write above to every Acquire load of `len`
        // (and to every `elem()` call the caller's id then reaches).
        self.len.store((id + 1) as u32, Release);
        id as u32
    }

    /// Lock-free indexed read. `id` must have been returned by
    /// `alloc_element` on this graph (ids are dense and never reused).
    ///
    /// SAFETY: slot `id < len` was initialized by `alloc_element` before the
    /// Release store of `len` that made the id observable, so the Acquire
    /// load here (of `len` in the caller's chain, and of the directory entry
    /// below) synchronizes with it. Elements are never moved (chunks are
    /// fixed-size heap blocks, the directory is never resized) and never
    /// freed before `Drop`, which needs `&mut self` and therefore cannot run
    /// while this borrow of `&self` is alive.
    #[inline]
    pub fn elem(&self, id: u32) -> &SharedElement {
        let id = id as usize;
        debug_assert!(id < self.len.load(Acquire) as usize, "elem() past the published length");
        let chunk = self.dir[id / CHUNK_SIZE].load(Acquire);
        debug_assert!(!chunk.is_null());
        unsafe { &*(chunk.add(id % CHUNK_SIZE) as *const SharedElement) }
    }

    /// Number of allocated elements (duplicates included), zero once the
    /// graph has been flushed and cleared. Used by tests and by the parallel
    /// build's progress reporting.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        if self.cleared.load(Acquire) {
            return 0;
        }
        self.len.load(Acquire) as usize
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
    /// C: `graph->memoryUsed + memoryMargin >= graph->memoryTotal`. The margin
    /// is 1MB in a parallel build (see `parallel::PARALLEL_MEMORY_MARGIN`) and
    /// zero in a serial one.
    pub fn memory_exhausted(&self, margin: usize) -> bool {
        self.memory_used() + margin >= self.memory_total
            // The arena has no more slots (only reachable when memory_total
            // exceeds what MAX_CHUNKS can hold): flush rather than overrun.
            || self.len.load(Acquire) as usize >= self.capacity
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

    /// C's `MemoryContextReset(graphCtx)` after flush. Resets the logical
    /// graph — length, head and entry point — but keeps the arena storage
    /// (see the DIVERGENCE in the module header): the on-disk phase never
    /// touches the arena, and the elements are dropped with the graph.
    /// `cleared` also makes a `len()`-driven iteration unable to run twice.
    pub fn clear_after_flush(&self) {
        self.cleared.store(true, Release);
        lk(&self.head).clear();
        *lk(&self.entry_point) = None;
    }
}

impl Drop for SharedGraph {
    fn drop(&mut self) {
        // The raw length, not `len()`: `clear_after_flush` hides the elements
        // but they are still there and still need dropping.
        let len = self.len.load(Acquire) as usize;
        for (c, slot) in self.dir.iter_mut().enumerate() {
            let chunk = *slot.get_mut();
            if chunk.is_null() {
                continue;
            }
            let initialized = len.saturating_sub(c * CHUNK_SIZE).min(CHUNK_SIZE);
            // SAFETY: `chunk` came from `alloc_chunk` (a `Box<[Slot]>` of
            // exactly CHUNK_SIZE slots) and is reachable from nowhere else;
            // `&mut self` means no reader can hold a `&SharedElement` into
            // it. The first `initialized` slots hold live elements.
            unsafe {
                for off in 0..initialized {
                    ptr::drop_in_place(chunk.add(off) as *mut SharedElement);
                }
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(chunk, CHUNK_SIZE)));
            }
        }
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
        assert!(!g.memory_exhausted(0));
        let small = SharedGraph::new(1);
        small.alloc_element(tid(1), 0, &[0u8; 16], 16);
        assert!(small.memory_exhausted(0));
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

    #[test]
    fn elements_span_chunk_boundaries() {
        let g = SharedGraph::new(1 << 30);
        for i in 0..=CHUNK_SIZE {
            g.alloc_element(tid(i as u16), (i % 3) as u8, &[(i % 251) as u8; 8], 16);
        }
        assert_eq!(g.len(), CHUNK_SIZE + 1);
        // Explicitly borrowed: `elem` hands out a reference into the arena.
        let first: &SharedElement = g.elem(0);
        let last: &SharedElement = g.elem(CHUNK_SIZE as u32);
        assert_eq!(last.level, (CHUNK_SIZE % 3) as u8);
        assert_eq!(&*last.value, &[(CHUNK_SIZE % 251) as u8; 8]);
        // The second chunk's allocation must not have disturbed the first.
        assert_eq!(first.level, 0);
        assert_eq!(&*first.value, &[0u8; 8]);
        assert!(!std::ptr::eq(first, last));
        // Repeated lookups return the same slot (no copy, no move).
        assert!(std::ptr::eq(g.elem(7), g.elem(7)));
    }

    #[test]
    fn concurrent_alloc_and_readers_see_published_elements() {
        use std::sync::atomic::AtomicBool;
        let g = Arc::new(SharedGraph::new(1 << 30));
        let stop = Arc::new(AtomicBool::new(false));

        // Readers race the allocators: every id below the published length
        // must be a fully initialized element.
        let readers: Vec<_> = (0..4)
            .map(|r| {
                let g = Arc::clone(&g);
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    let mut rng = r as u64 * 104729 + 7;
                    let mut reads = 0u64;
                    while !stop.load(Relaxed) {
                        let n = g.len();
                        if n == 0 {
                            continue;
                        }
                        rng = rng
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let e: &SharedElement = g.elem((rng % n as u64) as u32);
                        assert!(e.level < 4, "level out of range: torn or uninitialized read");
                        assert_eq!(e.value.len(), 8);
                        assert!(e.value[0] < 8);
                        assert_eq!(lk(&e.heaptids).len, 1);
                        assert_eq!(lk(&e.neighbors).len(), e.level as usize + 1);
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        let allocs: Vec<_> = (0..8)
            .map(|t| {
                let g = Arc::clone(&g);
                thread::spawn(move || {
                    (0..2000u32)
                        .map(|i| {
                            g.alloc_element(tid(i as u16), (i % 4) as u8, &[t as u8; 8], 16)
                        })
                        .collect::<Vec<u32>>()
                })
            })
            .collect();

        let mut all: Vec<u32> = allocs.into_iter().flat_map(|h| h.join().unwrap()).collect();
        stop.store(true, Relaxed);
        let reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(reads > 0, "readers never ran");

        all.sort_unstable();
        assert_eq!(all, (0..16000u32).collect::<Vec<_>>());
        assert_eq!(g.len(), 16000);
        for i in 0..16000u32 {
            let e = g.elem(i);
            assert_eq!(e.value.len(), 8);
            assert_eq!(lk(&e.neighbors).len(), e.level as usize + 1);
        }
    }
}
