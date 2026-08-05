// generation.c backend: FIFO-lifetime blocks freed when their chunk count drains; 8B per-chunk
// block-address header (C's MemoryChunk); accounting at block transitions only.

use core::alloc::Layout;
use core::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator, Global};

use crate::aset::{INIT_BLOCK_SIZE, MAX_BLOCK_SIZE, POOL_MAX_ALIGN};
use crate::Acct;

const GEN_CHUNKHDRSZ: usize = 8;
const GEN_BLKHDRSZ: usize = (core::mem::size_of::<GenBlock>() + 7) & !7;

// C's allocChunkLimit policy (Generation_CHUNK_FRACTION = 8 per max block).
const GEN_CHUNK_LIMIT: usize = {
    let bound = (MAX_BLOCK_SIZE - GEN_BLKHDRSZ) / 8;
    let mut limit = MAX_BLOCK_SIZE;
    while limit + GEN_CHUNKHDRSZ > bound {
        limit >>= 1;
    }
    limit
};

#[repr(C)]
struct GenBlock {
    blksize: usize,
    align: u32,
    idx: u32,
    nchunks: u32,
    nfree: u32,
}

#[inline(always)]
fn block_mut<'a>(addr: usize) -> &'a mut GenBlock {
    debug_assert!(addr != 0);
    // SAFETY: `addr` is an exposed, live block base written by alloc paths of this arena.
    unsafe { &mut *core::ptr::with_exposed_provenance_mut::<GenBlock>(addr) }
}

pub(crate) struct GenArena {
    // Exposed block base addresses; `keeper` (if non-zero) is retained over reset.
    blocks: alloc::vec::Vec<usize>,
    keeper: usize,
    freeblock: usize,
    cur_block: usize,
    cur_ptr: *mut u8,
    cur_end: *mut u8,
    next_block_size: usize,
    mem_allocated: usize,
}

impl GenArena {
    pub(crate) fn new() -> GenArena {
        GenArena {
            blocks: alloc::vec::Vec::new(),
            keeper: 0,
            freeblock: 0,
            cur_block: 0,
            cur_ptr: core::ptr::null_mut(),
            cur_end: core::ptr::null_mut(),
            next_block_size: INIT_BLOCK_SIZE,
            mem_allocated: 0,
        }
    }

    pub(crate) fn footprint(&self) -> usize {
        self.mem_allocated
    }

    pub(crate) fn nblocks(&self) -> usize {
        self.blocks.len()
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
        // wrapping_sub folds the ZST case into the unusual branch.
        if csize.wrapping_sub(1) >= GEN_CHUNK_LIMIT {
            return self.alloc_unusual(layout, csize, acct);
        }
        let req = GEN_CHUNKHDRSZ + csize;
        let avail = (self.cur_end as usize).wrapping_sub(self.cur_ptr as usize);
        if req <= avail {
            let hdr = self.cur_ptr;
            // SAFETY: req <= avail keeps the bump inside the current block; hdr non-null there.
            unsafe {
                (hdr as *mut usize).write(self.cur_block);
                let p = hdr.add(GEN_CHUNKHDRSZ);
                self.cur_ptr = p.add(csize);
                block_mut(self.cur_block).nchunks += 1;
                return Ok(NonNull::slice_from_raw_parts(NonNull::new_unchecked(p), csize));
            }
        }
        self.alloc_slow(csize, acct)
    }

    // SAFETY (caller): the window covers req = 8 + csize bytes in cur_block.
    #[inline]
    unsafe fn carve(&mut self, csize: usize) -> NonNull<[u8]> {
        let hdr = self.cur_ptr;
        (hdr as *mut usize).write(self.cur_block);
        let p = hdr.add(GEN_CHUNKHDRSZ);
        self.cur_ptr = p.add(csize);
        block_mut(self.cur_block).nchunks += 1;
        NonNull::slice_from_raw_parts(NonNull::new_unchecked(p), csize)
    }

    #[inline]
    fn set_window(&mut self, addr: usize) {
        let blksize = block_mut(addr).blksize;
        let base = core::ptr::with_exposed_provenance_mut::<u8>(addr);
        self.cur_block = addr;
        // SAFETY: GEN_BLKHDRSZ <= blksize for every block this arena creates.
        unsafe {
            self.cur_ptr = base.add(GEN_BLKHDRSZ);
            self.cur_end = base.add(blksize);
        }
    }

    #[cold]
    #[inline(never)]
    fn alloc_slow(&mut self, csize: usize, acct: &Acct) -> Result<NonNull<[u8]>, AllocError> {
        let req = GEN_CHUNKHDRSZ + csize;
        // Retained keeper (fresh reset): claim and charge it now (bump precedent).
        if self.cur_block == 0 && self.keeper != 0 {
            let ksize = block_mut(self.keeper).blksize;
            if req <= ksize - GEN_BLKHDRSZ {
                acct.check_limit(ksize)?;
                acct.commit_block(ksize, self.mem_allocated, self.blocks.len());
                self.set_window(self.keeper);
                // SAFETY: req fits the just-claimed keeper window.
                return Ok(unsafe { self.carve(csize) });
            }
        }
        if self.freeblock != 0 {
            let fb = self.freeblock;
            if req <= block_mut(fb).blksize - GEN_BLKHDRSZ {
                self.freeblock = 0;
                self.set_window(fb);
                // SAFETY: req fits the empty freeblock's window.
                return Ok(unsafe { self.carve(csize) });
            }
        }
        let is_keeper = self.keeper == 0;
        let mut blksize = if is_keeper {
            INIT_BLOCK_SIZE
        } else {
            let b = self.next_block_size;
            self.next_block_size = (b * 2).min(MAX_BLOCK_SIZE);
            b
        };
        if blksize < req + GEN_BLKHDRSZ {
            blksize = (req + GEN_BLKHDRSZ).next_power_of_two();
        }
        let addr = self.push_block(blksize, POOL_MAX_ALIGN, acct)?;
        if is_keeper {
            self.keeper = addr;
        }
        self.set_window(addr);
        // SAFETY: req + GEN_BLKHDRSZ <= blksize by construction.
        Ok(unsafe { self.carve(csize) })
    }

    fn push_block(
        &mut self,
        blksize: usize,
        align: usize,
        acct: &Acct,
    ) -> Result<usize, AllocError> {
        acct.check_limit(blksize)?;
        self.blocks.try_reserve(1).map_err(|_| AllocError)?;
        let layout = Layout::from_size_align(blksize, align).map_err(|_| AllocError)?;
        let p = Global.allocate(layout)?;
        let addr = p.cast::<u8>().as_ptr().expose_provenance();
        let idx = self.blocks.len() as u32;
        // SAFETY: fresh blksize-byte allocation; the header fits its prefix.
        unsafe {
            core::ptr::with_exposed_provenance_mut::<GenBlock>(addr).write(GenBlock {
                blksize,
                align: align as u32,
                idx,
                nchunks: 0,
                nfree: 0,
            });
        }
        self.mem_allocated += blksize;
        crate::global_footprint::add(blksize);
        self.blocks.push(addr);
        acct.commit_block(blksize, self.mem_allocated, self.blocks.len());
        Ok(addr)
    }

    // ZST or over-chunk-limit request (C's GenerationAllocLarge lane).
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

    // Single-chunk dedicated block; the chunk header still finds it for free.
    #[cold]
    #[inline(never)]
    fn alloc_dedicated(
        &mut self,
        layout: Layout,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let align = layout.align().max(POOL_MAX_ALIGN);
        let csize = (layout.size().max(1) + 7) & !7;
        let data_off = (GEN_BLKHDRSZ + GEN_CHUNKHDRSZ + align - 1) & !(align - 1);
        let blksize = data_off.checked_add(csize).ok_or(AllocError)?;
        let addr = self.push_block(blksize, align, acct)?;
        let b = block_mut(addr);
        b.nchunks = 1;
        let base = core::ptr::with_exposed_provenance_mut::<u8>(addr);
        // SAFETY: data_off + csize == blksize; header slot precedes the chunk.
        unsafe {
            (base.add(data_off - GEN_CHUNKHDRSZ) as *mut usize).write(addr);
            let p = NonNull::new_unchecked(base.add(data_off));
            Ok(NonNull::slice_from_raw_parts(p, csize))
        }
    }

    /// # Safety: `ptr` is live from this arena with `layout` (Allocator contract).
    pub(crate) unsafe fn dealloc(&mut self, ptr: NonNull<u8>, layout: Layout, acct: &Acct) {
        if layout.size() == 0 {
            return;
        }
        let hdr = core::ptr::with_exposed_provenance::<usize>(ptr.as_ptr().addr() - GEN_CHUNKHDRSZ);
        let addr = *hdr;
        let b = block_mut(addr);
        b.nfree += 1;
        debug_assert!(b.nfree <= b.nchunks);
        debug_assert!(addr != self.freeblock);
        if b.nfree < b.nchunks {
            return;
        }
        self.block_emptied(addr, acct);
    }

    #[cold]
    #[inline(never)]
    fn block_emptied(&mut self, addr: usize, acct: &Acct) {
        if addr == self.keeper || addr == self.cur_block {
            let b = block_mut(addr);
            b.nchunks = 0;
            b.nfree = 0;
            if addr == self.cur_block {
                // Rewind the window so the current block is reused in place.
                self.cur_ptr = core::ptr::with_exposed_provenance_mut::<u8>(addr + GEN_BLKHDRSZ);
            }
        } else if self.freeblock == 0 {
            let b = block_mut(addr);
            b.nchunks = 0;
            b.nfree = 0;
            self.freeblock = addr;
        } else {
            let blksize = block_mut(addr).blksize;
            let idx = block_mut(addr).idx as usize;
            self.blocks.swap_remove(idx);
            if let Some(&moved) = self.blocks.get(idx) {
                block_mut(moved).idx = idx as u32;
            }
            self.release_block_bytes(addr);
            acct.uncommit_block(blksize, self.mem_allocated, self.blocks.len());
        }
    }

    fn release_block_bytes(&mut self, addr: usize) {
        let b = block_mut(addr);
        let (blksize, align) = (b.blksize, b.align as usize);
        self.mem_allocated -= blksize;
        crate::global_footprint::sub(blksize);
        // SAFETY: block empty (or arena reset/teardown); size/align from its own header.
        unsafe {
            Global.deallocate(
                NonNull::new_unchecked(core::ptr::with_exposed_provenance_mut::<u8>(addr)),
                Layout::from_size_align_unchecked(blksize, align),
            );
        }
    }

    /// # Safety
    /// `ptr`: live from this arena with `old_layout`; `new_layout.size() >= old_layout.size()`.
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
            if new_csize == old_csize && old_csize != 0 {
                return Ok(NonNull::slice_from_raw_parts(ptr, new_csize));
            }
            // Last-chunk in-place extension (what a growing PgVec hits); the
            // header check rejects a foreign allocation that merely abuts cur_ptr.
            if old_csize != 0
                && ptr.as_ptr().add(old_csize) == self.cur_ptr
                && new_csize - old_csize <= self.cur_end as usize - self.cur_ptr as usize
                && new_csize < GEN_CHUNK_LIMIT
                && *core::ptr::with_exposed_provenance::<usize>(
                    ptr.as_ptr().addr() - GEN_CHUNKHDRSZ,
                ) == self.cur_block
            {
                self.cur_ptr = ptr.as_ptr().add(new_csize);
                return Ok(NonNull::slice_from_raw_parts(ptr, new_csize));
            }
        }
        let new = self.alloc(new_layout, acct)?;
        core::ptr::copy_nonoverlapping(ptr.as_ptr(), new.cast::<u8>().as_ptr(), old_layout.size());
        self.dealloc(ptr, old_layout, acct);
        Ok(new)
    }

    pub(crate) fn reset(&mut self) {
        self.freeblock = 0;
        let keeper = self.keeper;
        for i in 0..self.blocks.len() {
            let addr = self.blocks[i];
            if addr != keeper {
                self.release_block_bytes(addr);
            }
        }
        self.blocks.clear();
        if keeper != 0 {
            self.blocks.push(keeper);
            let b = block_mut(keeper);
            b.idx = 0;
            b.nchunks = 0;
            b.nfree = 0;
        }
        // Null window: the first alloc after reset re-claims (and re-charges) the keeper.
        self.cur_block = 0;
        self.cur_ptr = core::ptr::null_mut();
        self.cur_end = core::ptr::null_mut();
        self.next_block_size = INIT_BLOCK_SIZE;
        debug_assert_eq!(
            self.mem_allocated,
            if keeper != 0 { block_mut(keeper).blksize } else { 0 }
        );
    }
}

impl Drop for GenArena {
    fn drop(&mut self) {
        for i in 0..self.blocks.len() {
            let addr = self.blocks[i];
            self.release_block_bytes(addr);
        }
        self.blocks.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::{Cell, RefCell};

    fn acct() -> Acct {
        Acct {
            name: Cell::new("gen-test"),
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            self_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(false),
            arena_footprint: Cell::new(0),
            arena_nblocks: Cell::new(0),
            window_tail: Cell::new(0),
            is_bump: true,
            kind: "Generation",
            parent: None,
            children: RefCell::new(alloc::vec::Vec::new()),
        }
    }

    fn l(size: usize) -> Layout {
        Layout::from_size_align(size, 8).unwrap()
    }

    #[test]
    fn chunk_limit_matches_c_policy() {
        assert_eq!(GEN_CHUNK_LIMIT, 512 * 1024);
        assert_eq!(GEN_BLKHDRSZ % 8, 0);
    }

    #[test]
    fn fifo_cycle_stays_within_two_blocks() {
        let mut a = GenArena::new();
        let acct = acct();
        let mut ring: alloc::collections::VecDeque<NonNull<u8>> = alloc::collections::VecDeque::new();
        for _ in 0..64 {
            ring.push_back(a.alloc(l(64), &acct).unwrap().cast());
        }
        for _ in 0..20_000 {
            ring.push_back(a.alloc(l(64), &acct).unwrap().cast());
            let old = ring.pop_front().unwrap();
            // SAFETY: `old` is live from this arena with the same layout.
            unsafe { a.dealloc(old, l(64), &acct) };
        }
        assert!(a.nblocks() <= 3, "FIFO churn must recycle, got {} blocks", a.nblocks());
        assert!(a.footprint() <= 4 * INIT_BLOCK_SIZE);
        while let Some(p) = ring.pop_front() {
            unsafe { a.dealloc(p, l(64), &acct) };
        }
    }

    #[test]
    fn emptied_current_block_rewinds_window() {
        let mut a = GenArena::new();
        let acct = acct();
        let p = a.alloc(l(32), &acct).unwrap().cast::<u8>();
        // SAFETY: p live from alloc.
        unsafe { a.dealloc(p, l(32), &acct) };
        let q = a.alloc(l(32), &acct).unwrap().cast::<u8>();
        assert_eq!(p.as_ptr(), q.as_ptr(), "emptied current block reused in place");
        unsafe { a.dealloc(q, l(32), &acct) };
    }

    #[test]
    fn draining_a_full_block_frees_or_parks_it() {
        let mut a = GenArena::new();
        let acct = acct();
        let per_block = INIT_BLOCK_SIZE / (64 + GEN_CHUNKHDRSZ);
        let mut ptrs = alloc::vec::Vec::new();
        for _ in 0..per_block * 6 {
            ptrs.push(a.alloc(l(64), &acct).unwrap().cast::<u8>());
        }
        let peak_blocks = a.nblocks();
        let peak_used = acct.self_used.get();
        assert!(peak_blocks >= 3);
        for p in ptrs {
            // SAFETY: each p live from alloc above.
            unsafe { a.dealloc(p, l(64), &acct) };
        }
        // keeper + current + one parked freeblock survive at most.
        assert!(a.nblocks() <= 3, "drained arena kept {} blocks", a.nblocks());
        assert!(acct.self_used.get() < peak_used, "block frees must uncharge");
        assert_eq!(acct.arena_footprint.get(), a.footprint());
    }

    #[test]
    fn oversize_and_overaligned_get_dedicated_blocks() {
        let mut a = GenArena::new();
        let acct = acct();
        let big = Layout::from_size_align(GEN_CHUNK_LIMIT + 1, 8).unwrap();
        let p = a.alloc(big, &acct).unwrap();
        assert!(p.len() >= GEN_CHUNK_LIMIT + 1);
        let al = Layout::from_size_align(64, 64).unwrap();
        let q = a.alloc(al, &acct).unwrap();
        assert_eq!(q.cast::<u8>().as_ptr() as usize % 64, 0);
        let before = a.footprint();
        // SAFETY: both live from alloc with their layouts.
        unsafe {
            a.dealloc(p.cast(), big, &acct);
            a.dealloc(q.cast(), al, &acct);
        }
        assert!(a.footprint() < before, "dedicated block freed on last chunk free");
    }

    #[test]
    fn grow_extends_last_chunk_in_place_and_moves_otherwise() {
        let mut a = GenArena::new();
        let acct = acct();
        let p = a.alloc(l(32), &acct).unwrap().cast::<u8>();
        // SAFETY: p live with l(32).
        let q = unsafe { a.grow(p, l(32), l(64), &acct) }.unwrap();
        assert_eq!(q.cast::<u8>().as_ptr(), p.as_ptr(), "in-place tail extension");
        let _hole = a.alloc(l(32), &acct).unwrap();
        // SAFETY: q live with l(64).
        let r = unsafe { a.grow(q.cast(), l(64), l(128), &acct) }.unwrap();
        assert_ne!(r.cast::<u8>().as_ptr(), p.as_ptr(), "non-tail grow moves");
        let nchunks = block_mut(a.cur_block).nchunks;
        assert_eq!(nchunks, 3, "moved grow frees the old chunk (alloc,hole,moved)");
    }

    #[test]
    fn reset_keeps_keeper_and_recharges_on_reuse() {
        let mut a = GenArena::new();
        let acct = acct();
        for _ in 0..10 {
            let _ = a.alloc(l(4096), &acct).unwrap();
        }
        assert!(a.nblocks() > 1);
        a.reset();
        acct.self_used.set(0);
        assert_eq!(a.nblocks(), 1);
        assert_eq!(a.footprint(), block_mut(a.keeper).blksize);
        let keeper_data = a.keeper + GEN_BLKHDRSZ + GEN_CHUNKHDRSZ;
        let p = a.alloc(l(4096), &acct).unwrap().cast::<u8>().as_ptr();
        assert_eq!(p.addr(), keeper_data, "first post-reset chunk starts the keeper");
        assert_eq!(acct.self_used.get(), a.footprint(), "keeper re-charged on claim");
    }

    #[test]
    fn zero_sized_alloc_is_dangling_and_uncharged() {
        let mut a = GenArena::new();
        let acct = acct();
        for align in [1usize, 2, 4, 8] {
            let zl = Layout::from_size_align(0, align).unwrap();
            let p = a.alloc(zl, &acct).unwrap();
            assert_eq!(p.len(), 0);
            // SAFETY: ZST dealloc is a no-op by contract.
            unsafe { a.dealloc(p.cast(), zl, &acct) };
        }
        assert_eq!(acct.self_used.get(), 0);
        assert_eq!(a.footprint(), 0);
    }

    #[test]
    fn alignment_and_disjointness_hold_across_blocks() {
        let mut a = GenArena::new();
        let acct = acct();
        let mut ptrs = alloc::vec::Vec::new();
        for _ in 0..1000 {
            let p = a.alloc(l(24), &acct).unwrap().cast::<u8>().as_ptr();
            assert_eq!(p as usize % 8, 0);
            ptrs.push(p as usize);
        }
        ptrs.sort_unstable();
        for w in ptrs.windows(2) {
            assert!(w[0] + 24 <= w[1], "chunk overlap");
        }
    }
}

