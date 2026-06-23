//! `aset.c` — the AllocSet block-pooling allocator, ported as the default
//! [`MemoryContext`](crate::MemoryContext) backend.
//!
//! C's `aset.c` amortizes `malloc` by allocating large **blocks** and carving
//! per-request **chunks** out of them by bumping a pointer; freed chunks go onto
//! one of `ALLOCSET_NUM_FREELISTS` power-of-two **size-class freelists** for
//! reuse, and the whole arena is reclaimed block-wise on
//! [`reset`](crate::MemoryContext::reset)/drop. This is why C burns ~3% of CPU
//! on allocation where a `malloc`-per-chunk context burns ~38%.
//!
//! ## What this port keeps and drops vs `aset.c`
//!
//! * **Kept:** block list with a *keeper* block retained across reset, doubling
//!   `nextBlockSize` up to `maxBlockSize`, the 11 power-of-two freelists indexed
//!   by [`free_list_index`], dedicated blocks for over-`allocChunkLimit`
//!   requests, and `AllocSetReset` freeing every block but the keeper.
//! * **Dropped:** the per-chunk `MemoryChunk` header. C needs it to recover the
//!   owning context (and the chunk's size) from a bare `pfree(ptr)`. Our
//!   [`Allocator`] receives the owning context as `self` and the `Layout` on
//!   `deallocate`, so the header is pure overhead here. Block metadata
//!   (`ptr`/`size`/`used`) lives out-of-band in a `Vec<Block>`, leaving the
//!   whole block usable for chunks.
//!
//! ## Alignment contract (the load-bearing safety invariant)
//!
//! Every pooled chunk is **8-byte aligned** (C's `MAXALIGN`), because blocks are
//! requested 8-aligned and chunk sizes are multiples of 8 carved by a running
//! offset that stays a multiple of 8. A request with `align > 8` — or
//! `size > ALLOC_CHUNK_LIMIT` — is **not pooled**: it is served straight from
//! [`Global`] and freed straight to `Global`. Because [`is_dedicated`] is a pure
//! function of the `Layout`, `deallocate` recomputes the identical routing
//! decision `allocate` made, so a dedicated allocation never lands on a freelist
//! and a pooled chunk never reaches `Global::deallocate`. Uniform 8-alignment is
//! what makes same-size-class freelist chunks freely interchangeable.

use core::alloc::Layout;
use core::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator, Global};

// =====================================================================
// Debug instrumentation (feature-gated). See Cargo.toml [features].
// =====================================================================

/// Guard-page debug allocator: every pooled chunk is backed by its own mmap'd
/// region whose end abuts a `PROT_NONE` page, so a write one byte past the
/// requested `size` faults *immediately* at the offending call site. Wasteful
/// (a chunk per >= 2 pages) but pinpoints the corruptor.
#[cfg(feature = "aset-guard")]
mod guard {
    use core::alloc::Layout;
    use core::ptr::NonNull;

    use allocator_api2::alloc::AllocError;

    fn page_size() -> usize {
        // 16 KiB on Apple silicon, 4 KiB on x86. Query once.
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    }

    /// We over-map by one extra page at the FRONT to stash the mmap base+len so
    /// `dealloc` can `munmap` exactly. Layout of the mapping:
    ///   [meta page ...][ data pages ... ][ GUARD page (PROT_NONE) ]
    /// The chunk is placed so its END is flush against the guard page; the meta
    /// (base ptr, total length) is written at the very start of the mapping.
    #[repr(C)]
    struct Meta {
        base: *mut u8,
        len: usize,
    }

    pub(super) fn alloc(layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ps = page_size();
        let size = layout.size().max(1);
        // align the usable size up so the start stays >= 8-aligned when we
        // back it off the guard page.
        let align = layout.align().max(8);
        // data region rounded up to whole pages
        let data_pages = (size + ps - 1) / ps;
        let total = ps /*meta*/ + data_pages * ps + ps /*guard*/;
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
        // guard page = last page
        let guard = unsafe { base.add(ps + data_pages * ps) };
        unsafe {
            if libc::mprotect(guard as *mut libc::c_void, ps, libc::PROT_NONE) != 0 {
                libc::munmap(base as *mut libc::c_void, total);
                return Err(AllocError);
            }
        }
        // place chunk so chunk_end == guard, chunk_start = guard - size, then
        // round start DOWN to `align` (still inside data region since data is
        // whole pages and meta page precedes it).
        let guard_addr = guard as usize;
        let mut start = guard_addr - size;
        start &= !(align - 1);
        let ptr = start as *mut u8;
        // stash meta at the very base
        unsafe {
            core::ptr::write(base as *mut Meta, Meta { base, len: total });
        }
        debug_assert!(ptr >= unsafe { base.add(ps) });
        let nn = unsafe { NonNull::new_unchecked(ptr) };
        Ok(NonNull::slice_from_raw_parts(nn, guard_addr - start))
    }

    pub(super) unsafe fn dealloc(ptr: NonNull<u8>, _layout: Layout) {
        let ps = page_size();
        // base = the meta page that contains `ptr`. ptr is within the data
        // pages which sit right after the meta page; base = page of ptr rounded
        // down then back one page to the meta page... simplest: scan back to a
        // page boundary then read the page before. We stored Meta at base, and
        // base = (data_page_start - ps). data_page_start = round_down(ptr, ps).
        let p = ptr.as_ptr() as usize;
        let data_page_start = p & !(ps - 1);
        let base = (data_page_start - ps) as *mut u8;
        let meta = core::ptr::read(base as *const Meta);
        libc::munmap(meta.base as *mut libc::c_void, meta.len);
    }
}


/// `ALLOC_MINBITS` (aset.c:83) — smallest chunk size is `1 << 3` = 8 bytes.
const ALLOC_MINBITS: u32 = 3;
/// `ALLOCSET_NUM_FREELISTS` (aset.c:84).
const NUM_FREELISTS: usize = 11;
/// `ALLOC_CHUNK_LIMIT` (aset.c:85) — `1 << (NUM_FREELISTS-1+ALLOC_MINBITS)` =
/// 8192. Requests above this get a dedicated block.
const ALLOC_CHUNK_LIMIT: usize = 1 << (NUM_FREELISTS - 1 + ALLOC_MINBITS as usize);
/// Maximum pooled alignment. C aset always returns `MAXALIGN`(8)-aligned chunks;
/// a request needing more is served from `Global` directly so the pool's uniform
/// 8-alignment invariant holds.
const POOL_MAX_ALIGN: usize = 8;

/// `ALLOCSET_DEFAULT_INITSIZE` (memutils.h) — keeper / first block size.
pub(crate) const INIT_BLOCK_SIZE: usize = 8 * 1024;
/// `ALLOCSET_DEFAULT_MAXSIZE` (memutils.h) — block sizes double up to here.
pub(crate) const MAX_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// One block: a `Global` allocation we bump chunks out of. `size` is the exact
/// size requested from `Global` (so we can deallocate with the matching layout);
/// `used` is the bump offset (a multiple of 8).
struct Block {
    ptr: NonNull<u8>,
    size: usize,
    used: usize,
}

impl Block {
    fn layout(size: usize) -> Layout {
        // size is always a positive multiple of POOL_MAX_ALIGN here.
        Layout::from_size_align(size, POOL_MAX_ALIGN).unwrap()
    }

    fn alloc(size: usize) -> Result<Block, AllocError> {
        let ptr = Global.allocate(Block::layout(size))?;
        Ok(Block { ptr: ptr.cast(), size, used: 0 })
    }

    /// SAFETY: must be called at most once per block, and the block's chunks must
    /// no longer be in use (the context's `&mut`/Drop guarantees this).
    unsafe fn free(self) {
        Global.deallocate(self.ptr, Block::layout(self.size));
    }
}

/// `true` if a request of this `Layout` is served from `Global` rather than the
/// pool. Pure function of the layout, so `allocate`/`deallocate` agree.
#[inline]
fn is_dedicated(layout: Layout) -> bool {
    layout.align() > POOL_MAX_ALIGN || layout.size().max(1) > ALLOC_CHUNK_LIMIT
}

/// `AllocSetFreeIndex` (aset.c) — the freelist bucket for a pooled request of
/// `size` bytes. Returns the index whose chunk size `1 << (idx + ALLOC_MINBITS)`
/// is the smallest power of two `>= max(size, 8)`. Caller guarantees the request
/// is pooled (`size <= ALLOC_CHUNK_LIMIT`), so the result is in `0..NUM_FREELISTS`.
#[inline]
fn free_list_index(size: usize) -> usize {
    let chunk = size.max(1 << ALLOC_MINBITS).next_power_of_two();
    (chunk.trailing_zeros() - ALLOC_MINBITS) as usize
}

/// Chunk size (bytes physically carved / reused) for freelist bucket `idx`.
#[inline]
fn chunk_size(idx: usize) -> usize {
    1usize << (idx as u32 + ALLOC_MINBITS)
}

/// A ported `aset.c` AllocSetContext: block list (`blocks[0]` is the keeper) plus
/// the power-of-two size-class freelists. `!Sync` (raw pointers) — one context,
/// one backend process, as in PG.
pub(crate) struct AllocSet {
    /// Block list. `blocks[0]` is the keeper (retained on reset); the last entry
    /// is the active block new chunks are bump-carved from.
    blocks: alloc::vec::Vec<Block>,
    /// Power-of-two size-class freelists. Each `Some(p)` is the head of a
    /// singly-linked list of free chunks whose *next* pointer is stored in the
    /// chunk's own first 8 bytes (chunks are `>= 8` bytes and 8-aligned).
    freelist: [Option<NonNull<u8>>; NUM_FREELISTS],
    /// `set->nextBlockSize` — size of the next non-dedicated block.
    next_block_size: usize,
    /// Total bytes of all live blocks (incl. dedicated are *not* tracked here —
    /// they belong to `Global`). Reported as the arena footprint for stats.
    mem_allocated: usize,
}

impl AllocSet {
    /// `AllocSetContextCreate`. Construction is infallible — the keeper block is
    /// allocated **lazily** on the first [`alloc`](Self::alloc), so an empty
    /// context holds no blocks (footprint 0, matching the old malloc backend) and
    /// `MemoryContext::new`/`new_child` cannot fail on an OOM keeper.
    pub(crate) fn new() -> AllocSet {
        AllocSet {
            blocks: alloc::vec::Vec::new(),
            freelist: [None; NUM_FREELISTS],
            next_block_size: INIT_BLOCK_SIZE,
            mem_allocated: 0,
        }
    }

    /// Total live block bytes — the figure C reports as `totalspace`.
    pub(crate) fn mem_allocated(&self) -> usize {
        self.mem_allocated
    }

    /// `AllocSetAlloc`. Returns a chunk of at least `layout.size()` bytes,
    /// 8-aligned (or `Global`-served for the dedicated case).
    pub(crate) fn alloc(&mut self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        // Zero-sized request: the `Allocator` contract says the returned pointer
        // must not be read/written, only that it is non-null and aligned. Hand
        // back a dangling, correctly-aligned pointer (matching what
        // `alloc::Global` / `NonNull::dangling` do) — never touch the pool, so a
        // later `deallocate` with this dangling pointer is a clean no-op. (In
        // practice `allocator_api2`'s `Box`/`Vec` skip `allocate` for ZSTs and
        // pass a dangling pointer straight to `deallocate`; we must mirror that.)
        if layout.size() == 0 {
            let dangling = layout.align() as *mut u8;
            // align is a non-zero power of two, so this is non-null.
            let nn = unsafe { NonNull::new_unchecked(dangling) };
            return Ok(NonNull::slice_from_raw_parts(nn, 0));
        }
        #[cfg(feature = "aset-guard")]
        {
            // Every allocation (pooled or dedicated) gets its own guard-paged
            // mapping so any overrun faults at the offending write.
            return guard::alloc(layout);
        }
        #[cfg(not(feature = "aset-guard"))]
        if is_dedicated(layout) {
            return Global.allocate(layout);
        }
        let idx = free_list_index(layout.size());
        let csize = chunk_size(idx);

        // 1. Reuse a chunk off this size class's freelist if any.
        if let Some(head) = self.freelist[idx] {
            // The chunk's first 8 bytes hold the next-free pointer.
            let next = unsafe { core::ptr::read(head.as_ptr() as *const Option<NonNull<u8>>) };
            self.freelist[idx] = next;
            return Ok(NonNull::slice_from_raw_parts(head, csize));
        }

        // 2. Bump-carve from the active (last) block if it has room.
        if let Some(active) = self.blocks.last_mut() {
            if csize <= active.size - active.used {
                let p = unsafe { active.ptr.as_ptr().add(active.used) };
                active.used += csize;
                return Ok(NonNull::slice_from_raw_parts(unsafe { NonNull::new_unchecked(p) }, csize));
            }
        }

        // 3. No room (or no block yet): allocate a fresh block and carve from it.
        //    The keeper (first block) is `INIT_BLOCK_SIZE`; later blocks follow
        //    `nextBlockSize`, doubling up to `MAX_BLOCK_SIZE`. Either way the block
        //    is at least `csize`. The old active block's tail free space is
        //    abandoned, exactly as `aset.c` does (recovered only if a chunk in it
        //    is later freed onto a freelist).
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
        self.blocks.push(block);
        Ok(NonNull::slice_from_raw_parts(p, csize))
    }

    /// `AllocSetFree`. A pooled chunk is pushed onto its size-class freelist (its
    /// memory stays in the block, reclaimed on reset/drop); a dedicated chunk goes
    /// back to `Global`.
    ///
    /// SAFETY: `ptr`/`layout` must be a live allocation previously returned by
    /// [`alloc`](Self::alloc) with the *same* `layout` (the `Allocator` contract),
    /// so the routing and freelist index recompute identically.
    pub(crate) unsafe fn dealloc(&mut self, ptr: NonNull<u8>, layout: Layout) {
        // Zero-sized deallocation: `ptr` is a dangling sentinel (e.g. `0x1` for an
        // align-1 ZST) that `Box::drop`/`Vec::drop` pass unconditionally. It owns
        // no pool memory and MUST NOT be dereferenced — mirror `alloc::Global`,
        // which no-ops here. (Writing the freelist head into a dangling `ptr` is
        // exactly the heap-corruption SIGSEGV this guards against.)
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
        // Store the current freelist head in this chunk and become the new head.
        core::ptr::write(ptr.as_ptr() as *mut Option<NonNull<u8>>, self.freelist[idx]);
        self.freelist[idx] = Some(ptr);
    }

    /// Pool-internal realloc for `grow`/`shrink`: allocate a new chunk, copy the
    /// overlap, free the old one. (C's `AllocSetRealloc` can grow a chunk in place
    /// when it is the lone occupant of its block; we keep the simpler always-move
    /// form — correctness first, and growth is not the boolean.sql hot path.)
    ///
    /// SAFETY: as [`dealloc`](Self::dealloc), plus `new_layout` must be valid.
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

    /// `AllocSetReset`: free every block but the keeper, empty the freelists, and
    /// rewind the keeper. The keeper's bytes are retained (not handed back to the
    /// OS) so a context that is reset every tuple does not thrash `malloc`.
    pub(crate) fn reset(&mut self) {
        // The keeper may never have been allocated (lazy): nothing to reclaim,
        // just clear the (already-empty) freelists and rewind the block sizer.
        if self.blocks.is_empty() {
            self.freelist = [None; NUM_FREELISTS];
            self.next_block_size = INIT_BLOCK_SIZE;
            return;
        }
        // Drain non-keeper blocks (index 1..) and free them.
        let drained: alloc::vec::Vec<Block> = self.blocks.drain(1..).collect();
        for b in drained {
            self.mem_allocated -= b.size;
            unsafe { b.free() };
        }
        self.blocks[0].used = 0;
        self.freelist = [None; NUM_FREELISTS];
        self.next_block_size = INIT_BLOCK_SIZE;
        debug_assert_eq!(self.mem_allocated, self.blocks[0].size);
    }
}

impl Drop for AllocSet {
    /// `AllocSetDelete`: free all blocks (keeper included). Dedicated `Global`
    /// allocations are already returned by their own `deallocate`, so nothing is
    /// owed to `Global` here beyond the blocks.
    fn drop(&mut self) {
        for b in self.blocks.drain(..) {
            unsafe { b.free() };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_list_index_matches_power_of_two_classes() {
        assert_eq!(free_list_index(1), 0); // -> 8
        assert_eq!(free_list_index(8), 0); // -> 8
        assert_eq!(free_list_index(9), 1); // -> 16
        assert_eq!(free_list_index(16), 1);
        assert_eq!(free_list_index(17), 2); // -> 32
        assert_eq!(free_list_index(4096), 9);
        assert_eq!(free_list_index(8192), 10); // ALLOC_CHUNK_LIMIT
        assert_eq!(chunk_size(0), 8);
        assert_eq!(chunk_size(10), 8192);
    }

    #[test]
    fn dedicated_routing_is_layout_pure() {
        // size over the chunk limit -> dedicated
        assert!(is_dedicated(Layout::from_size_align(8193, 8).unwrap()));
        assert!(!is_dedicated(Layout::from_size_align(8192, 8).unwrap()));
        // align over 8 -> dedicated
        assert!(is_dedicated(Layout::from_size_align(16, 16).unwrap()));
        assert!(!is_dedicated(Layout::from_size_align(16, 8).unwrap()));
    }

    #[test]
    fn alloc_dealloc_reuses_same_size_class() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(24, 8).unwrap(); // -> 32-byte chunk
        let p1 = a.alloc(l).unwrap();
        assert!(p1.len() >= 24);
        assert_eq!(p1.cast::<u8>().as_ptr() as usize % 8, 0);
        unsafe { a.dealloc(p1.cast(), l) };
        // Next same-class alloc must hand back the very chunk we freed.
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
        // 1000 * 64 = 64KiB > one 8KiB block: must have grown.
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
        // Reusable after reset.
        let _ = a.alloc(l).unwrap();
    }

    #[test]
    fn zero_sized_alloc_dealloc_is_a_noop_on_dangling_ptr() {
        // Reproduces the boolean.sql SIGSEGV: a `Box<ZST, Mcx>` (e.g. the
        // `A_Star` node) is dropped, and `Box::drop` unconditionally calls
        // `deallocate` with `Layout::for_value` (size 0) and a *dangling*
        // pointer (`align`, e.g. 0x1). dealloc must NOT write into it.
        let mut a = AllocSet::new();
        for align in [1usize, 2, 4, 8] {
            let l = Layout::from_size_align(0, align).unwrap();
            // alloc(size 0) returns a dangling, aligned pointer touching no pool.
            let p = a.alloc(l).unwrap();
            assert_eq!(p.len(), 0);
            assert_eq!(p.cast::<u8>().as_ptr() as usize % align, 0);
            // The pool must be untouched (no keeper block carved).
            assert_eq!(a.mem_allocated(), 0);
            // deallocate of the dangling pointer must be a clean no-op.
            unsafe { a.dealloc(p.cast(), l) };
        }
        // Mirror exactly what allocator_api2 does: alloc is skipped, only
        // dealloc runs with a hand-built dangling ZST pointer.
        let dangling = unsafe { NonNull::new_unchecked(1usize as *mut u8) };
        unsafe { a.dealloc(dangling, Layout::from_size_align(0, 1).unwrap()) };
        // Freelists stayed empty — the dangling ptr never entered a freelist.
        assert!(a.freelist.iter().all(|h| h.is_none()));
    }

    #[test]
    fn dedicated_large_chunk_roundtrips() {
        let mut a = AllocSet::new();
        let l = Layout::from_size_align(100_000, 8).unwrap();
        let p = a.alloc(l).unwrap();
        assert!(p.len() >= 100_000);
        // Dedicated allocations are Global-owned: no pool block was created, so
        // the lazy keeper never materialized and the footprint stays 0.
        assert_eq!(a.mem_allocated(), 0);
        unsafe { a.dealloc(p.cast(), l) };
    }
}
