// bump.c backend: per-alloc is MAXALIGN + one window bump; ALL accounting runs at block transitions (C's mem_allocated shape), never per chunk.

use core::alloc::Layout;
use core::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator, Global};

use crate::aset::{
    recycle_blocks, take_recycled_blocks, Block, INIT_BLOCK_SIZE, MAX_BLOCK_SIZE, POOL_MAX_ALIGN,
};
use crate::Acct;

// C's allocChunkLimit policy (8 chunks per max block), kept although Rust blocks carry no header.
const BUMP_BLOCK_HDR_SZ: usize = 40;
const BUMP_CHUNK_LIMIT: usize = {
    let bound = (MAX_BLOCK_SIZE - BUMP_BLOCK_HDR_SZ) / 8;
    let mut limit = MAX_BLOCK_SIZE;
    while limit > bound {
        limit >>= 1;
    }
    limit
};

pub(crate) struct BumpArena {
    // blocks[0] is the keeper (retained over reset, uncharged until claimed).
    blocks: alloc::vec::Vec<Block>,
    // Dedicated oversize/overaligned chunks; freed wholesale at reset.
    oversize: alloc::vec::Vec<(NonNull<u8>, Layout)>,
    // Invariant: windows null until the keeper is claimed, else cur_ptr <= cur_end in the active block.
    cur_ptr: *mut u8,
    cur_end: *mut u8,
    next_block_size: usize,
    mem_allocated: usize,
}

impl BumpArena {
    pub(crate) fn new() -> BumpArena {
        let blocks = take_recycled_blocks();
        debug_assert!(blocks.is_empty() || (blocks.len() == 1 && blocks[0].used == 0));
        let mem_allocated = blocks.first().map_or(0, |b| b.size);
        crate::global_footprint::add(mem_allocated);
        BumpArena {
            blocks,
            oversize: alloc::vec::Vec::new(),
            cur_ptr: core::ptr::null_mut(),
            cur_end: core::ptr::null_mut(),
            next_block_size: INIT_BLOCK_SIZE,
            mem_allocated,
        }
    }

    pub(crate) fn footprint(&self) -> usize {
        self.mem_allocated
    }

    pub(crate) fn nblocks(&self) -> usize {
        self.blocks.len() + self.oversize.len()
    }

    pub(crate) fn window_tail(&self) -> usize {
        self.cur_end as usize - self.cur_ptr as usize
    }

    #[inline]
    pub(crate) fn alloc(
        &mut self,
        layout: Layout,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if layout.align() > POOL_MAX_ALIGN {
            return self.alloc_dedicated(layout, acct);
        }
        let csize = (layout.size() + 7) & !7;
        // wrapping_sub folds the ZST case into the oversize branch.
        if csize.wrapping_sub(1) >= BUMP_CHUNK_LIMIT {
            return self.alloc_unusual(layout, csize, acct);
        }
        let avail = self.cur_end as usize - self.cur_ptr as usize;
        if csize <= avail {
            let p = self.cur_ptr;
            // SAFETY: csize <= avail keeps the bump inside the active block; p non-null there.
            unsafe {
                self.cur_ptr = p.add(csize);
                return Ok(NonNull::slice_from_raw_parts(NonNull::new_unchecked(p), csize));
            }
        }
        self.alloc_from_new_block(csize, acct)
    }

    #[cold]
    #[inline(never)]
    fn alloc_from_new_block(
        &mut self,
        csize: usize,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // Retained keeper (fresh arena or post-reset): claim and charge it now.
        if self.cur_ptr.is_null() {
            if let Some(k) = self.blocks.first() {
                if csize <= k.size {
                    acct.check_limit(k.size)?;
                    acct.commit_block(k.size, self.mem_allocated, self.nblocks());
                    let base = k.ptr.as_ptr();
                    // SAFETY: csize <= k.size; both offsets stay in the keeper.
                    unsafe {
                        self.cur_ptr = base.add(csize);
                        self.cur_end = base.add(k.size);
                    }
                    acct.window_tail.set(k.size - csize);
                    return Ok(NonNull::slice_from_raw_parts(k.ptr, csize));
                }
            }
        }
        let is_keeper = self.blocks.is_empty();
        let mut blksize = if is_keeper {
            INIT_BLOCK_SIZE
        } else {
            let b = self.next_block_size;
            self.next_block_size = (b * 2).min(MAX_BLOCK_SIZE);
            b
        };
        if blksize < csize {
            blksize = csize.next_power_of_two();
        }
        acct.check_limit(blksize)?;
        self.blocks.try_reserve(1).map_err(|_| AllocError)?;
        let block = Block::alloc(blksize)?;
        self.mem_allocated += blksize;
        crate::global_footprint::add(blksize);
        acct.commit_block(blksize, self.mem_allocated, self.nblocks() + 1);
        let p = block.ptr;
        // SAFETY: csize <= blksize; both offsets stay within the new block.
        unsafe {
            self.cur_ptr = p.as_ptr().add(csize);
            self.cur_end = p.as_ptr().add(blksize);
        }
        acct.window_tail.set(blksize - csize);
        self.blocks.push(block);
        Ok(NonNull::slice_from_raw_parts(p, csize))
    }

    // ZST or over-chunk-limit request (C's BumpAllocLarge lane).
    #[cold]
    #[inline(never)]
    fn alloc_unusual(
        &mut self,
        layout: Layout,
        csize: usize,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if csize == 0 {
            let dangling = layout.align() as *mut u8;
            // SAFETY: align is a non-zero power of two, hence non-null.
            let nn = unsafe { NonNull::new_unchecked(dangling) };
            return Ok(NonNull::slice_from_raw_parts(nn, 0));
        }
        self.alloc_dedicated(layout, acct)
    }

    #[cold]
    #[inline(never)]
    fn alloc_dedicated(
        &mut self,
        layout: Layout,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size().max(1);
        let layout = Layout::from_size_align(size, layout.align()).map_err(|_| AllocError)?;
        self.oversize.try_reserve(1).map_err(|_| AllocError)?;
        acct.check_limit(size)?;
        let p = Global.allocate(layout)?;
        self.mem_allocated += size;
        crate::global_footprint::add(size);
        self.oversize.push((p.cast(), layout));
        acct.commit_block(size, self.mem_allocated, self.nblocks());
        acct.window_tail.set(self.window_tail());
        Ok(p)
    }

    /// # Safety
    /// `ptr`: live from this arena's alloc/grow with `old_layout`; `new_layout.size() >= old_layout.size()`.
    pub(crate) unsafe fn grow(
        &mut self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.align() <= POOL_MAX_ALIGN && new_layout.align() <= POOL_MAX_ALIGN {
            let old_csize = (old_layout.size() + 7) & !7;
            let new_csize = (new_layout.size() + 7) & !7;
            // Last-chunk in-place extension (what a growing PgVec hits).
            if ptr.as_ptr().add(old_csize) == self.cur_ptr
                && new_csize - old_csize <= self.cur_end as usize - self.cur_ptr as usize
                && new_csize < BUMP_CHUNK_LIMIT
            {
                self.cur_ptr = ptr.as_ptr().add(new_csize);
                return Ok(NonNull::slice_from_raw_parts(ptr, new_csize));
            }
        }
        let new = self.alloc(new_layout, acct)?;
        core::ptr::copy_nonoverlapping(ptr.as_ptr(), new.cast::<u8>().as_ptr(), old_layout.size());
        Ok(new)
    }

    // Post-reset the window re-opens over the keeper (kept charged by the
    // caller): the next alloc takes the plain bump path, as C's BumpReset
    // leaves the keeper in mem_allocated with freeptr rewound. The common
    // reset (everything fit in the keeper) frees nothing — window rewind only.
    #[inline]
    pub(crate) fn reset(&mut self) {
        if !self.oversize.is_empty() || self.blocks.len() > 1 {
            self.free_extra_blocks();
        }
        match self.blocks.first_mut() {
            Some(k) => {
                k.used = 0;
                let base = k.ptr.as_ptr();
                self.cur_ptr = base;
                // SAFETY: base + k.size is the keeper's one-past-the-end.
                self.cur_end = unsafe { base.add(k.size) };
            }
            None => {
                self.cur_ptr = core::ptr::null_mut();
                self.cur_end = core::ptr::null_mut();
            }
        }
        self.next_block_size = INIT_BLOCK_SIZE;
        debug_assert_eq!(self.mem_allocated, self.blocks.first().map_or(0, |b| b.size));
    }

    #[cold]
    #[inline(never)]
    fn free_extra_blocks(&mut self) {
        for (p, layout) in self.oversize.drain(..) {
            self.mem_allocated -= layout.size();
            crate::global_footprint::sub(layout.size());
            // SAFETY: reset's &mut proves no oversize chunk is still in use.
            unsafe { Global.deallocate(p, layout) };
        }
        if self.blocks.len() > 1 {
            for b in self.blocks.drain(1..) {
                self.mem_allocated -= b.size;
                crate::global_footprint::sub(b.size);
                // SAFETY: reset's &mut proves no chunk in these blocks is in use.
                unsafe { b.free() };
            }
        }
    }
}

impl Drop for BumpArena {
    fn drop(&mut self) {
        self.reset();
        // The keeper's bytes leave this context now, whether freed or parked
        // in the recycle pool.
        crate::global_footprint::sub(self.mem_allocated);
        self.mem_allocated = 0;
        if self.blocks.is_empty() {
            return;
        }
        let blocks = core::mem::take(&mut self.blocks);
        if blocks[0].size == INIT_BLOCK_SIZE {
            recycle_blocks(blocks);
        } else {
            for b in blocks {
                // SAFETY: Drop proves no chunk is in use; sole free.
                unsafe { b.free() };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::{Cell, RefCell};

    fn acct() -> Acct {
        Acct {
            name: Cell::new("bump-test"),
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            self_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(false),
            arena_footprint: Cell::new(0),
            arena_nblocks: Cell::new(0),
            window_tail: Cell::new(0),
            is_bump: true,
            kind: "Bump",
            parent: None,
            children: RefCell::new(alloc::vec::Vec::new()),
        }
    }

    #[test]
    fn bump_carves_contiguously_and_stays_aligned() {
        let mut a = BumpArena::new();
        let acct = acct();
        let l = Layout::from_size_align(24, 8).unwrap();
        let mut last: Option<*mut u8> = None;
        for _ in 0..1000 {
            let p = a.alloc(l, &acct).unwrap().cast::<u8>().as_ptr();
            assert_eq!(p as usize % 8, 0);
            if let Some(q) = last {
                assert_ne!(p, q);
            }
            last = Some(p);
        }
        assert!(a.footprint() >= 1000 * 24);
        assert!(acct.self_used.get() >= 1000 * 24, "block charges cover the chunks");
        assert_eq!(acct.self_used.get() % 8, 0);
    }

    #[test]
    fn charge_happens_at_block_transitions_only() {
        let mut a = BumpArena::new();
        let acct = acct();
        let l = Layout::from_size_align(64, 8).unwrap();
        let _ = a.alloc(l, &acct).unwrap();
        let after_first = acct.self_used.get();
        assert_eq!(after_first, INIT_BLOCK_SIZE, "first alloc charges the whole keeper");
        for _ in 0..(INIT_BLOCK_SIZE / 64 - 1) {
            let _ = a.alloc(l, &acct).unwrap();
        }
        assert_eq!(acct.self_used.get(), after_first, "in-block allocs charge nothing");
        let _ = a.alloc(l, &acct).unwrap();
        assert!(acct.self_used.get() > after_first, "block transition charges");
        assert_eq!(acct.arena_footprint.get(), a.footprint());
        assert_eq!(acct.arena_nblocks.get(), a.nblocks());
    }

    #[test]
    fn reset_keeps_keeper_with_window_open() {
        let mut a = BumpArena::new();
        let acct = acct();
        let l = Layout::from_size_align(4096, 8).unwrap();
        for _ in 0..10 {
            let _ = a.alloc(l, &acct).unwrap();
        }
        assert!(a.blocks.len() > 1);
        a.reset();
        // Post-reset accounting is the caller's (MemoryContext::reset keeps the
        // keeper charged); the arena's window must already be open on it.
        acct.self_used.set(a.blocks[0].size);
        assert_eq!(a.blocks.len(), 1);
        assert_eq!(a.footprint(), a.blocks[0].size);
        let keeper_base = a.blocks[0].ptr.as_ptr();
        assert_eq!(a.cur_ptr, keeper_base, "window re-opens on the keeper at reset");
        let charged = acct.self_used.get();
        let p = a.alloc(l, &acct).unwrap().cast::<u8>().as_ptr();
        assert_eq!(p, keeper_base, "first post-reset chunk starts the keeper");
        assert_eq!(acct.self_used.get(), charged, "no re-charge on the fast path");
    }

    #[test]
    fn zero_sized_alloc_is_dangling_and_uncharged() {
        let mut a = BumpArena::new();
        let acct = acct();
        for align in [1usize, 2, 4, 8] {
            let l = Layout::from_size_align(0, align).unwrap();
            let p = a.alloc(l, &acct).unwrap();
            assert_eq!(p.len(), 0);
            assert_eq!(p.cast::<u8>().as_ptr() as usize % align, 0);
        }
        assert_eq!(acct.self_used.get(), 0);
        assert_eq!(a.footprint(), 0);
    }

    #[test]
    fn oversize_and_overaligned_get_dedicated_chunks() {
        let mut a = BumpArena::new();
        let acct = acct();
        let big = Layout::from_size_align(BUMP_CHUNK_LIMIT + 1, 8).unwrap();
        let p = a.alloc(big, &acct).unwrap();
        assert!(p.len() >= BUMP_CHUNK_LIMIT + 1);
        let al = Layout::from_size_align(64, 64).unwrap();
        let q = a.alloc(al, &acct).unwrap();
        assert_eq!(q.cast::<u8>().as_ptr() as usize % 64, 0);
        assert_eq!(a.oversize.len(), 2);
        assert!(acct.self_used.get() > BUMP_CHUNK_LIMIT);
        a.reset();
        assert!(a.oversize.is_empty());
    }

    #[test]
    fn grow_extends_last_chunk_in_place() {
        let mut a = BumpArena::new();
        let acct = acct();
        let old = Layout::from_size_align(32, 8).unwrap();
        let new = Layout::from_size_align(64, 8).unwrap();
        let p = a.alloc(old, &acct).unwrap().cast::<u8>();
        // SAFETY: p live from alloc with `old`.
        let q = unsafe { a.grow(p, old, new, &acct) }.unwrap();
        assert_eq!(q.cast::<u8>().as_ptr(), p.as_ptr(), "in-place extension");
        let _hole = a.alloc(old, &acct).unwrap();
        let bigger = Layout::from_size_align(128, 8).unwrap();
        // SAFETY: q live from grow with `new`.
        let r = unsafe { a.grow(q.cast(), new, bigger, &acct) }.unwrap();
        assert_ne!(r.cast::<u8>().as_ptr(), p.as_ptr(), "non-tail grow moves");
    }
}
