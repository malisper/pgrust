// aset.c block-pooling backend, minus the per-chunk header (the Allocator gets
// context + Layout). Pooled chunks are 8-aligned; is_dedicated is layout-pure.

use core::alloc::Layout;
use core::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator, Global};

// Guard-page debug allocator: overruns fault at the writing site.
#[cfg(feature = "aset-guard")]
mod guard {
    use core::alloc::Layout;
    use core::ptr::NonNull;

    use allocator_api2::alloc::AllocError;

    fn page_size() -> usize {
        // SAFETY: sysconf is always callable.
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    }

    // [meta page][data pages...][PROT_NONE guard]; Meta records base+len for munmap.
    #[repr(C)]
    struct Meta {
        base: *mut u8,
        len: usize,
    }

    pub(super) fn alloc(layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ps = page_size();
        let size = layout.size().max(1);
        let align = layout.align().max(8);
        let data_pages = (size + ps - 1) / ps;
        let total = ps + data_pages * ps + ps;
        // SAFETY: plain anonymous private mapping; failure checked below.
        let base = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(AllocError);
        }
        let base = base as *mut u8;
        // SAFETY: guard page lies inside the mapping just created.
        let guard = unsafe { base.add(ps + data_pages * ps) };
        unsafe {
            if libc::mprotect(guard as *mut libc::c_void, ps, libc::PROT_NONE) != 0 {
                libc::munmap(base as *mut libc::c_void, total);
                return Err(AllocError);
            }
        }
        let guard_addr = guard as usize;
        let mut start = guard_addr - size;
        start &= !(align - 1);
        let ptr = start as *mut u8;
        // SAFETY: `base` is the writable mapping start; Meta fits a page.
        unsafe {
            core::ptr::write(base as *mut Meta, Meta { base, len: total });
        }
        debug_assert!(ptr >= unsafe { base.add(ps) });
        // SAFETY: `start` is inside the data pages, hence non-null.
        let nn = unsafe { NonNull::new_unchecked(ptr) };
        Ok(NonNull::slice_from_raw_parts(nn, guard_addr - start))
    }

    pub(super) unsafe fn dealloc(ptr: NonNull<u8>, _layout: Layout) {
        let ps = page_size();
        // Meta page immediately precedes the data page containing `ptr`.
        let p = ptr.as_ptr() as usize;
        let data_page_start = p & !(ps - 1);
        let base = (data_page_start - ps) as *mut u8;
        let meta = core::ptr::read(base as *const Meta);
        libc::munmap(meta.base as *mut libc::c_void, meta.len);
    }
}

// ALLOC_MINBITS / ALLOCSET_NUM_FREELISTS / ALLOC_CHUNK_LIMIT (aset.c).
const ALLOC_MINBITS: u32 = 3;
const NUM_FREELISTS: usize = 11;
const ALLOC_CHUNK_LIMIT: usize = 1 << (NUM_FREELISTS - 1 + ALLOC_MINBITS as usize);
const POOL_MAX_ALIGN: usize = 8;

// ALLOCSET_DEFAULT_INITSIZE / ALLOCSET_DEFAULT_MAXSIZE (memutils.h).
pub(crate) const INIT_BLOCK_SIZE: usize = 8 * 1024;
pub(crate) const MAX_BLOCK_SIZE: usize = 8 * 1024 * 1024;

#[cfg(feature = "aset-stats")]
pub(crate) mod stats {
    use core::sync::atomic::AtomicU64;
    pub(crate) static BLOCK_MALLOCS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static KEEPER_REUSES: AtomicU64 = AtomicU64::new(0);
}

pub fn alloc_stats() -> (u64, u64) {
    #[cfg(feature = "aset-stats")]
    {
        use core::sync::atomic::Ordering;
        (
            stats::BLOCK_MALLOCS.load(Ordering::Relaxed),
            stats::KEEPER_REUSES.load(Ordering::Relaxed),
        )
    }
    #[cfg(not(feature = "aset-stats"))]
    {
        (0, 0)
    }
}

const MAX_FREE_CONTEXTS: usize = 100;

struct Block {
    ptr: NonNull<u8>,
    size: usize,
    used: usize,
}

impl Block {
    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, POOL_MAX_ALIGN).unwrap()
    }

    fn alloc(size: usize) -> Result<Block, AllocError> {
        #[cfg(feature = "aset-stats")]
        stats::BLOCK_MALLOCS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let ptr = Global.allocate(Block::layout(size))?;
        Ok(Block { ptr: ptr.cast(), size, used: 0 })
    }

    /// # Safety
    /// At most once per block, with no chunk in it still in use.
    unsafe fn free(self) {
        Global.deallocate(self.ptr, Block::layout(self.size));
    }
}

// Recycled-keeper pool — aset.c's context_freelists[]; all keepers are INIT_BLOCK_SIZE.
struct KeeperPool {
    stack: core::cell::UnsafeCell<alloc::vec::Vec<alloc::vec::Vec<Block>>>,
}

// SAFETY: the single-threaded backend never accesses `stack` concurrently.
unsafe impl Sync for KeeperPool {}

static KEEPER_POOL: KeeperPool = KeeperPool {
    stack: core::cell::UnsafeCell::new(alloc::vec::Vec::new()),
};

impl KeeperPool {
    #[inline]
    fn with<R>(&self, f: impl FnOnce(&mut alloc::vec::Vec<alloc::vec::Vec<Block>>) -> R) -> R {
        // SAFETY: single-threaded backend — no other live access to `stack`.
        f(unsafe { &mut *self.stack.get() })
    }

    fn take(&self) -> alloc::vec::Vec<Block> {
        match self.with(|stack| stack.pop()) {
            Some(blocks) => {
                #[cfg(feature = "aset-stats")]
                stats::KEEPER_REUSES.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                blocks
            }
            None => alloc::vec::Vec::new(),
        }
    }

    // `blocks` is exactly [keeper] with used == 0; a full list drains wholesale first.
    fn recycle(&self, blocks: alloc::vec::Vec<Block>) {
        let overflow = self.with(|stack| {
            let drained = if stack.len() >= MAX_FREE_CONTEXTS {
                core::mem::take(stack)
            } else {
                alloc::vec::Vec::new()
            };
            stack.push(blocks);
            drained
        });
        for v in overflow {
            for b in v {
                // SAFETY: parked keepers own their blocks; this is the sole free.
                unsafe { b.free() };
            }
        }
    }
}

#[inline]
fn take_recycled_blocks() -> alloc::vec::Vec<Block> {
    #[cfg(test)]
    {
        alloc::vec::Vec::new()
    }
    #[cfg(not(test))]
    {
        KEEPER_POOL.take()
    }
}

#[inline]
fn recycle_blocks(blocks: alloc::vec::Vec<Block>) {
    #[cfg(test)]
    {
        for b in blocks {
            // SAFETY: keeper-only list, blocks no longer in use (Drop/&mut).
            unsafe { b.free() };
        }
    }
    #[cfg(not(test))]
    {
        KEEPER_POOL.recycle(blocks);
    }
}

#[inline]
fn is_dedicated(layout: Layout) -> bool {
    layout.align() > POOL_MAX_ALIGN || layout.size().max(1) > ALLOC_CHUNK_LIMIT
}

#[inline]
fn free_list_index(size: usize) -> usize {
    let chunk = size.max(1 << ALLOC_MINBITS).next_power_of_two();
    (chunk.trailing_zeros() - ALLOC_MINBITS) as usize
}

#[inline]
fn idx_and_chunk(size: usize) -> (usize, usize) {
    if size <= (1 << ALLOC_MINBITS) {
        (0, 1 << ALLOC_MINBITS)
    } else {
        let t = (size - 1) >> ALLOC_MINBITS;
        let idx = (usize::BITS - t.leading_zeros()) as usize;
        (idx, 1usize << (idx + ALLOC_MINBITS as usize))
    }
}

#[inline]
fn chunk_size(idx: usize) -> usize {
    1usize << (idx as u32 + ALLOC_MINBITS)
}

pub(crate) struct AllocSet {
    blocks: alloc::vec::Vec<Block>,
    // Freelist heads; the next pointer lives in the chunk's own first 8 bytes.
    freelist: [Option<NonNull<u8>>; NUM_FREELISTS],
    next_block_size: usize,
    mem_allocated: usize,
    // Cached bump window over the active block (C's freeptr/endptr); null until a
    // block exists. Invariant: cur_ptr <= cur_end, in the active block.
    cur_ptr: *mut u8,
    cur_end: *mut u8,
}

impl AllocSet {
    pub(crate) fn new() -> AllocSet {
        let blocks = take_recycled_blocks();
        let mem_allocated = blocks.first().map_or(0, |b| b.size);
        debug_assert!(blocks.is_empty() || (blocks.len() == 1 && blocks[0].used == 0));
        let (cur_ptr, cur_end) = match blocks.first() {
            Some(b) => {
                let base = b.ptr.as_ptr();
                // SAFETY: used <= size, both offsets stay within the block.
                (unsafe { base.add(b.used) }, unsafe { base.add(b.size) })
            }
            None => (core::ptr::null_mut(), core::ptr::null_mut()),
        };
        AllocSet {
            blocks,
            freelist: [None; NUM_FREELISTS],
            next_block_size: INIT_BLOCK_SIZE,
            mem_allocated,
            cur_ptr,
            cur_end,
        }
    }

    #[inline]
    fn sync_active(&mut self) {
        if !self.cur_ptr.is_null() {
            if let Some(active) = self.blocks.last_mut() {
                active.used = self.cur_ptr as usize - active.ptr.as_ptr() as usize;
            }
        }
    }

    pub(crate) fn mem_allocated(&self) -> usize {
        self.mem_allocated
    }

    pub(crate) fn alloc(&mut self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        // ZST: dangling aligned pointer, no pool state touched (as allocator_api2 expects).
        if layout.size() == 0 {
            let dangling = layout.align() as *mut u8;
            // SAFETY: align is a non-zero power of two, hence non-null.
            let nn = unsafe { NonNull::new_unchecked(dangling) };
            return Ok(NonNull::slice_from_raw_parts(nn, 0));
        }
        #[cfg(feature = "aset-guard")]
        {
            return guard::alloc(layout);
        }
        #[cfg(not(feature = "aset-guard"))]
        if is_dedicated(layout) {
            return Global.allocate(layout);
        }
        let (idx, csize) = idx_and_chunk(layout.size());

        if let Some(head) = self.freelist[idx] {
            // SAFETY: a parked chunk's first 8 bytes hold the next-free pointer (>= 8B, 8-aligned).
            let next = unsafe { core::ptr::read(head.as_ptr() as *const Option<NonNull<u8>>) };
            self.freelist[idx] = next;
            return Ok(NonNull::slice_from_raw_parts(head, csize));
        }

        let avail = self.cur_end as usize - self.cur_ptr as usize;
        if csize <= avail {
            let p = self.cur_ptr;
            // SAFETY: csize <= avail keeps the bump within the active block.
            self.cur_ptr = unsafe { p.add(csize) };
            // SAFETY: p is inside a live block, hence non-null.
            return Ok(NonNull::slice_from_raw_parts(unsafe { NonNull::new_unchecked(p) }, csize));
        }

        self.alloc_new_block(csize)
    }

    #[cold]
    #[inline(never)]
    fn alloc_new_block(&mut self, csize: usize) -> Result<NonNull<[u8]>, AllocError> {
        self.sync_active();
        let is_keeper = self.blocks.is_empty();
        let blksize = if is_keeper {
            INIT_BLOCK_SIZE.max(csize)
        } else {
            self.next_block_size.max(csize)
        };
        let mut block = Block::alloc(blksize)?;
        self.mem_allocated += blksize;
        if !is_keeper {
            self.next_block_size = (self.next_block_size * 2).min(MAX_BLOCK_SIZE);
        }
        let p = block.ptr;
        block.used = csize;
        // SAFETY: csize <= blksize; both offsets stay within the new block.
        self.cur_ptr = unsafe { p.as_ptr().add(csize) };
        self.cur_end = unsafe { p.as_ptr().add(blksize) };
        self.blocks.push(block);
        Ok(NonNull::slice_from_raw_parts(p, csize))
    }

    /// # Safety
    /// Live allocation from [`alloc`](Self::alloc) with the same layout.
    pub(crate) unsafe fn dealloc(&mut self, ptr: NonNull<u8>, layout: Layout) {
        // ZST: dangling sentinel; writing a freelist head into it corrupts the heap.
        if layout.size() == 0 {
            return;
        }
        #[cfg(feature = "aset-guard")]
        {
            guard::dealloc(ptr, layout);
            return;
        }
        #[cfg(not(feature = "aset-guard"))]
        if is_dedicated(layout) {
            Global.deallocate(ptr, layout);
            return;
        }
        let idx = free_list_index(layout.size());
        core::ptr::write(ptr.as_ptr() as *mut Option<NonNull<u8>>, self.freelist[idx]);
        self.freelist[idx] = Some(ptr);
    }

    // Always-move realloc (C's in-place lone-block growth is not ported).
    /// # Safety: as [`dealloc`](Self::dealloc); `new_layout` must be valid.
    pub(crate) unsafe fn realloc(
        &mut self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let new = self.alloc(new_layout)?;
        let copy = old_layout.size().min(new_layout.size());
        core::ptr::copy_nonoverlapping(ptr.as_ptr(), new.cast::<u8>().as_ptr(), copy);
        self.dealloc(ptr, old_layout);
        Ok(new)
    }

    pub(crate) fn reset(&mut self) {
        if self.blocks.is_empty() {
            self.freelist = [None; NUM_FREELISTS];
            self.next_block_size = INIT_BLOCK_SIZE;
            return;
        }
        let drained: alloc::vec::Vec<Block> = self.blocks.drain(1..).collect();
        for b in drained {
            self.mem_allocated -= b.size;
            // SAFETY: reset's &mut proves no chunk in these blocks is in use.
            unsafe { b.free() };
        }
        let keeper = &mut self.blocks[0];
        keeper.used = 0;
        let base = keeper.ptr.as_ptr();
        self.cur_ptr = base;
        // SAFETY: offset stays within the keeper block.
        self.cur_end = unsafe { base.add(keeper.size) };
        self.freelist = [None; NUM_FREELISTS];
        self.next_block_size = INIT_BLOCK_SIZE;
        debug_assert_eq!(self.mem_allocated, self.blocks[0].size);
    }
}

impl Drop for AllocSet {
    fn drop(&mut self) {
        if self.blocks.is_empty() {
            return;
        }
        let mut blocks = core::mem::take(&mut self.blocks);
        for b in blocks.drain(1..) {
            // SAFETY: Drop proves no chunk in these blocks is in use.
            unsafe { b.free() };
        }
        let keeper = &mut blocks[0];
        keeper.used = 0;
        if keeper.size == INIT_BLOCK_SIZE {
            recycle_blocks(blocks);
        } else {
            for b in blocks {
                // SAFETY: as above; sole free of a non-default keeper.
                unsafe { b.free() };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_list_index_matches_power_of_two_classes() {
        assert_eq!(free_list_index(1), 0);
        assert_eq!(free_list_index(8), 0);
        assert_eq!(free_list_index(9), 1);
        assert_eq!(free_list_index(16), 1);
        assert_eq!(free_list_index(17), 2);
        assert_eq!(free_list_index(4096), 9);
        assert_eq!(free_list_index(8192), 10);
        assert_eq!(chunk_size(0), 8);
        assert_eq!(chunk_size(10), 8192);
    }

    #[test]
    fn idx_and_chunk_matches_free_list_index_and_chunk_size() {
        for size in 1..=ALLOC_CHUNK_LIMIT {
            let (idx, csize) = idx_and_chunk(size);
            assert_eq!(idx, free_list_index(size), "idx mismatch at size {size}");
            assert_eq!(csize, chunk_size(idx), "csize mismatch at size {size}");
            assert!(csize >= size && idx < NUM_FREELISTS);
        }
    }

    #[test]
    fn cached_bump_carves_contiguously_across_a_block_boundary() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(64, 8).unwrap();
        let per_block = INIT_BLOCK_SIZE / 64;
        let mut ptrs = alloc::vec::Vec::new();
        for _ in 0..(per_block * 3 + 5) {
            let p = a.alloc(l).unwrap();
            let raw = p.cast::<u8>().as_ptr();
            assert_eq!(raw as usize % 8, 0);
            for &q in &ptrs {
                let q: *mut u8 = q;
                assert!(
                    (raw as usize) + 64 <= q as usize || (q as usize) + 64 <= raw as usize,
                    "overlap detected"
                );
            }
            ptrs.push(raw);
        }
        assert!(a.blocks.len() >= 3);
        let active = a.blocks.last().unwrap();
        let abase = active.ptr.as_ptr() as usize;
        assert!(a.cur_ptr as usize >= abase && a.cur_end as usize == abase + active.size);
        assert!(a.cur_ptr as usize <= a.cur_end as usize);
    }

    #[test]
    fn cached_bump_survives_reset_and_interleaved_free() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(48, 8).unwrap();
        let mut held = alloc::vec::Vec::new();
        for i in 0..500 {
            let p = a.alloc(l).unwrap().cast::<u8>();
            if i % 3 == 0 {
                unsafe { a.dealloc(p, l) };
            } else {
                held.push(p);
            }
        }
        a.reset();
        assert_eq!(a.blocks.len(), 1);
        assert_eq!(a.blocks[0].used, 0);
        let keeper_base = a.blocks[0].ptr.as_ptr();
        assert_eq!(a.cur_ptr, keeper_base);
        let first = a.alloc(l).unwrap().cast::<u8>().as_ptr();
        assert_eq!(first, keeper_base, "first post-reset chunk must start the keeper");
        for _ in 0..(INIT_BLOCK_SIZE / 64 + 10) {
            let _ = a.alloc(l).unwrap();
        }
        a.sync_active();
        let total_used: usize = a.blocks.iter().map(|b| b.used).sum();
        assert!(total_used > INIT_BLOCK_SIZE);
    }

    #[test]
    fn recycled_keeper_primes_cached_window() {
        let mut a = AllocSet::new();
        assert!(a.cur_ptr.is_null() && a.cur_end.is_null());
        let l = Layout::from_size_align(32, 8).unwrap();
        let p = a.alloc(l).unwrap().cast::<u8>().as_ptr();
        assert!(!a.cur_ptr.is_null());
        let base = a.blocks[0].ptr.as_ptr();
        assert_eq!(p, base);
        assert_eq!(a.cur_end, unsafe { base.add(a.blocks[0].size) });
    }

    #[test]
    fn dedicated_routing_is_layout_pure() {
        assert!(is_dedicated(Layout::from_size_align(8193, 8).unwrap()));
        assert!(!is_dedicated(Layout::from_size_align(8192, 8).unwrap()));
        assert!(is_dedicated(Layout::from_size_align(16, 16).unwrap()));
        assert!(!is_dedicated(Layout::from_size_align(16, 8).unwrap()));
    }

    #[test]
    fn alloc_dealloc_reuses_same_size_class() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(24, 8).unwrap();
        let p1 = a.alloc(l).unwrap();
        assert!(p1.len() >= 24);
        assert_eq!(p1.cast::<u8>().as_ptr() as usize % 8, 0);
        unsafe { a.dealloc(p1.cast(), l) };
        let p2 = a.alloc(l).unwrap();
        assert_eq!(p1.cast::<u8>().as_ptr(), p2.cast::<u8>().as_ptr());
    }

    #[test]
    fn carving_many_chunks_grows_blocks_and_stays_aligned() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(64, 8).unwrap();
        let mut ptrs = alloc::vec::Vec::new();
        for _ in 0..1000 {
            let p = a.alloc(l).unwrap();
            assert_eq!(p.cast::<u8>().as_ptr() as usize % 8, 0);
            ptrs.push(p);
        }
        assert!(a.mem_allocated() > INIT_BLOCK_SIZE);
        assert!(a.blocks.len() > 1);
        for p in ptrs {
            unsafe { a.dealloc(p.cast(), l) };
        }
    }

    #[test]
    fn reset_keeps_keeper_frees_rest() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(4096, 8).unwrap();
        for _ in 0..10 {
            let _ = a.alloc(l).unwrap();
        }
        assert!(a.blocks.len() > 1);
        a.reset();
        assert_eq!(a.blocks.len(), 1);
        assert_eq!(a.mem_allocated(), INIT_BLOCK_SIZE);
        assert_eq!(a.blocks[0].used, 0);
        let _ = a.alloc(l).unwrap();
    }

    #[test]
    fn zero_sized_alloc_dealloc_is_a_noop_on_dangling_ptr() {
        let mut a = AllocSet::new();
        for align in [1usize, 2, 4, 8] {
            let l = Layout::from_size_align(0, align).unwrap();
            let p = a.alloc(l).unwrap();
            assert_eq!(p.len(), 0);
            assert_eq!(p.cast::<u8>().as_ptr() as usize % align, 0);
            assert_eq!(a.mem_allocated(), 0);
            unsafe { a.dealloc(p.cast(), l) };
        }
        let dangling = unsafe { NonNull::new_unchecked(1usize as *mut u8) };
        unsafe { a.dealloc(dangling, Layout::from_size_align(0, 1).unwrap()) };
        assert!(a.freelist.iter().all(|h| h.is_none()));
    }

    #[test]
    fn keeper_pool_caps_and_recycles() {
        let pool = KeeperPool {
            stack: core::cell::UnsafeCell::new(alloc::vec::Vec::new()),
        };
        pool.recycle(alloc::vec::Vec::new());
        assert_eq!(pool.with(|s| s.len()), 1);
        let _ = pool.take();
        assert_eq!(pool.with(|s| s.len()), 0);
        for _ in 0..MAX_FREE_CONTEXTS {
            pool.recycle(alloc::vec::Vec::new());
        }
        assert_eq!(pool.with(|s| s.len()), MAX_FREE_CONTEXTS);
        pool.recycle(alloc::vec::Vec::new());
        assert_eq!(pool.with(|s| s.len()), 1);
    }

    #[test]
    fn dedicated_large_chunk_roundtrips() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(100_000, 8).unwrap();
        let p = a.alloc(l).unwrap();
        assert!(p.len() >= 100_000);
        assert_eq!(a.mem_allocated(), 0);
        unsafe { a.dealloc(p.cast(), l) };
    }
}
