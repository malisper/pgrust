// slab.c (PG16 rewrite) backend: fixed-size chunks, fullest-block-first blocklist lanes,
// per-block freelist threaded through free chunk bytes. Blocks are allocated aligned to
// their (power-of-two) size, so free recovers the block by masking the chunk address —
// no per-chunk header (C pays 8 bytes + a decode).

use core::alloc::Layout;
use core::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator, Global};

use crate::aset::POOL_MAX_ALIGN;
use crate::Acct;

const SLAB_BLOCKLIST_COUNT: usize = 3;
const SLAB_MAXIMUM_EMPTY_BLOCKS: usize = 10;
const SLAB_BLKHDRSZ: usize = (core::mem::size_of::<SlabBlock>() + 7) & !7;

#[repr(C)]
struct SlabBlock {
    nfree: u32,
    nunused: u32,
    // Exposed addresses (0 = none): freehead chains through free chunk bytes.
    freehead: usize,
    unused: usize,
    prev: usize,
    next: usize,
}

#[inline(always)]
fn block_mut<'a>(addr: usize) -> &'a mut SlabBlock {
    debug_assert!(addr != 0);
    // SAFETY: `addr` is an exposed, live block base written by this arena's alloc path.
    unsafe { &mut *core::ptr::with_exposed_provenance_mut::<SlabBlock>(addr) }
}

pub(crate) struct SlabArena {
    chunk_size: usize,
    full_chunk_size: usize,
    block_size: usize,
    chunks_per_block: u32,
    blocklist_shift: u32,
    cur_blocklist_index: u32,
    // 4th entry is an always-empty spillway: `& 3` indexing needs no bounds check
    // (blocklist_shift guarantees indexes 0..=2).
    blocklist: [usize; SLAB_BLOCKLIST_COUNT + 1],
    emptyblocks: alloc::vec::Vec<usize>,
    mem_allocated: usize,
    nblocks: usize,
}

impl SlabArena {
    pub(crate) fn new(block_size: usize, chunk_size: usize) -> SlabArena {
        assert!(block_size.is_power_of_two() && block_size >= 1024);
        assert!(chunk_size > 0);
        let full_chunk_size = (chunk_size.max(core::mem::size_of::<usize>()) + 7) & !7;
        let chunks_per_block = (block_size - SLAB_BLKHDRSZ) / full_chunk_size;
        assert!(
            chunks_per_block >= 1,
            "block size {block_size} too small for {chunk_size}-byte chunks",
        );
        let mut blocklist_shift = 0u32;
        while (chunks_per_block >> blocklist_shift) >= SLAB_BLOCKLIST_COUNT - 1 {
            blocklist_shift += 1;
        }
        SlabArena {
            chunk_size,
            full_chunk_size,
            block_size,
            chunks_per_block: chunks_per_block as u32,
            blocklist_shift,
            cur_blocklist_index: 0,
            blocklist: [0; SLAB_BLOCKLIST_COUNT + 1],
            emptyblocks: alloc::vec::Vec::new(),
            mem_allocated: 0,
            nblocks: 0,
        }
    }

    pub(crate) fn footprint(&self) -> usize {
        self.mem_allocated
    }

    pub(crate) fn nblocks(&self) -> usize {
        self.nblocks
    }

    // C's two's-complement trick: 0 stays 0, everything else lands in 1..COUNT.
    #[inline(always)]
    fn blocklist_index(&self, nfree: u32) -> u32 {
        debug_assert!(nfree <= self.chunks_per_block);
        (-((-(nfree as i32)) >> self.blocklist_shift)) as u32
    }

    fn find_next_blocklist_index(&self) -> u32 {
        for i in 1..SLAB_BLOCKLIST_COUNT {
            if self.blocklist[i] != 0 {
                return i as u32;
            }
        }
        0
    }

    #[inline]
    fn push_head(&mut self, i: u32, addr: usize) {
        let next = self.blocklist[(i & 3) as usize];
        {
            let b = block_mut(addr);
            b.prev = 0;
            b.next = next;
        }
        if next != 0 {
            block_mut(next).prev = addr;
        }
        self.blocklist[(i & 3) as usize] = addr;
    }

    #[inline]
    fn unlink(&mut self, i: u32, addr: usize) {
        let (prev, next) = {
            let b = block_mut(addr);
            (b.prev, b.next)
        };
        if prev != 0 {
            block_mut(prev).next = next;
        } else {
            debug_assert_eq!(self.blocklist[(i & 3) as usize], addr);
            self.blocklist[(i & 3) as usize] = next;
        }
        if next != 0 {
            block_mut(next).prev = prev;
        }
    }

    #[inline]
    fn next_free_chunk(&self, addr: usize) -> usize {
        let b = block_mut(addr);
        debug_assert!(b.nfree > 0);
        let chunk;
        if b.freehead != 0 {
            chunk = b.freehead;
            // SAFETY: a free chunk's first 8 bytes hold the next-free address.
            b.freehead = unsafe { *core::ptr::with_exposed_provenance::<usize>(chunk) };
        } else {
            debug_assert!(b.nunused > 0);
            chunk = b.unused;
            b.unused += self.full_chunk_size;
            b.nunused -= 1;
        }
        b.nfree -= 1;
        chunk
    }

    #[inline]
    pub(crate) fn alloc(
        &mut self,
        layout: Layout,
        acct: &Acct,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() != self.chunk_size || layout.align() > POOL_MAX_ALIGN {
            slab_layout_mismatch(layout, self.chunk_size);
        }
        if self.cur_blocklist_index == 0 {
            return self.alloc_from_new_block(acct);
        }
        let i = self.cur_blocklist_index;
        let addr = self.blocklist[(i & 3) as usize];
        debug_assert!(addr != 0);
        debug_assert_eq!(self.blocklist_index(block_mut(addr).nfree), i);
        let chunk = self.next_free_chunk(addr);
        let new_i = self.blocklist_index(block_mut(addr).nfree);
        if new_i != i {
            self.unlink(i, addr);
            self.push_head(new_i, addr);
            if self.blocklist[(i & 3) as usize] == 0 {
                self.cur_blocklist_index = self.find_next_blocklist_index();
            }
        }
        Ok(chunk_slice(chunk, self.full_chunk_size))
    }

    #[cold]
    #[inline(never)]
    fn alloc_from_new_block(&mut self, acct: &Acct) -> Result<NonNull<[u8]>, AllocError> {
        let addr;
        if let Some(a) = self.emptyblocks.pop() {
            debug_assert_eq!(block_mut(a).nfree, self.chunks_per_block);
            addr = a;
        } else {
            acct.check_limit(self.block_size)?;
            self.emptyblocks
                .try_reserve(SLAB_MAXIMUM_EMPTY_BLOCKS)
                .map_err(|_| AllocError)?;
            let layout = Layout::from_size_align(self.block_size, self.block_size)
                .map_err(|_| AllocError)?;
            let p = Global.allocate(layout)?;
            addr = p.cast::<u8>().as_ptr().expose_provenance();
            // SAFETY: fresh block; header fits its prefix (chunks_per_block >= 1).
            unsafe {
                core::ptr::with_exposed_provenance_mut::<SlabBlock>(addr).write(SlabBlock {
                    nfree: self.chunks_per_block,
                    nunused: self.chunks_per_block,
                    freehead: 0,
                    unused: addr + SLAB_BLKHDRSZ,
                    prev: 0,
                    next: 0,
                });
            }
            self.mem_allocated += self.block_size;
            crate::global_footprint::add(self.block_size);
            self.nblocks += 1;
            acct.commit_block(self.block_size, self.mem_allocated, self.nblocks);
        }
        let chunk = self.next_free_chunk(addr);
        let i = self.blocklist_index(block_mut(addr).nfree);
        debug_assert_eq!(self.blocklist[i as usize], 0);
        self.push_head(i, addr);
        self.cur_blocklist_index = i;
        Ok(chunk_slice(chunk, self.full_chunk_size))
    }

    /// # Safety: `ptr` is live from this arena with `layout` (Allocator contract).
    pub(crate) unsafe fn dealloc(&mut self, ptr: NonNull<u8>, layout: Layout, acct: &Acct) {
        debug_assert_eq!(layout.size(), self.chunk_size);
        let chunk = ptr.as_ptr().addr();
        let addr = chunk & !(self.block_size - 1);
        let nfree = {
            let b = block_mut(addr);
            // Push onto the block's freelist; the next link lives in the chunk bytes.
            core::ptr::with_exposed_provenance_mut::<usize>(chunk).write(b.freehead);
            b.freehead = chunk;
            b.nfree += 1;
            debug_assert!(b.nfree <= self.chunks_per_block);
            b.nfree
        };
        let cur_i = self.blocklist_index(nfree - 1);
        let new_i = self.blocklist_index(nfree);
        if cur_i != new_i {
            self.unlink(cur_i, addr);
            self.push_head(new_i, addr);
            if self.cur_blocklist_index >= cur_i {
                self.cur_blocklist_index = self.find_next_blocklist_index();
                debug_assert!(self.cur_blocklist_index > 0);
            }
        }
        if nfree == self.chunks_per_block {
            self.block_emptied(addr, new_i, acct);
        }
    }

    #[cold]
    #[inline(never)]
    fn block_emptied(&mut self, addr: usize, i: u32, acct: &Acct) {
        self.unlink(i, addr);
        if self.emptyblocks.len() < SLAB_MAXIMUM_EMPTY_BLOCKS {
            self.emptyblocks.push(addr);
        } else {
            self.release_block_bytes(addr);
            acct.uncommit_block(self.block_size, self.mem_allocated, self.nblocks);
        }
        if self.cur_blocklist_index == i && self.blocklist[(i & 3) as usize] == 0 {
            self.cur_blocklist_index = self.find_next_blocklist_index();
        }
    }

    fn release_block_bytes(&mut self, addr: usize) {
        self.mem_allocated -= self.block_size;
        crate::global_footprint::sub(self.block_size);
        self.nblocks -= 1;
        // SAFETY: block is empty (or arena reset/teardown); layout matches its allocation.
        unsafe {
            Global.deallocate(
                NonNull::new_unchecked(core::ptr::with_exposed_provenance_mut::<u8>(addr)),
                Layout::from_size_align_unchecked(self.block_size, self.block_size),
            );
        }
    }

    pub(crate) fn reset(&mut self) {
        while let Some(addr) = self.emptyblocks.pop() {
            self.release_block_bytes(addr);
        }
        for i in 0..SLAB_BLOCKLIST_COUNT {
            let mut addr = self.blocklist[i];
            while addr != 0 {
                let next = block_mut(addr).next;
                self.release_block_bytes(addr);
                addr = next;
            }
            self.blocklist[i] = 0;
        }
        self.cur_blocklist_index = 0;
        debug_assert_eq!(self.mem_allocated, 0);
        debug_assert_eq!(self.nblocks, 0);
    }
}

impl Drop for SlabArena {
    fn drop(&mut self) {
        self.reset();
    }
}

#[inline(always)]
fn chunk_slice(chunk: usize, len: usize) -> NonNull<[u8]> {
    // SAFETY: chunk is a non-zero exposed address inside a live block.
    let p = unsafe { NonNull::new_unchecked(core::ptr::with_exposed_provenance_mut::<u8>(chunk)) };
    NonNull::slice_from_raw_parts(p, len)
}

#[cold]
#[inline(never)]
fn slab_layout_mismatch(layout: Layout, chunk_size: usize) -> ! {
    panic!(
        "unexpected alloc chunk size {} align {} (slab expects size {chunk_size}, align <= 8)",
        layout.size(),
        layout.align(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::{Cell, RefCell};

    const SLAB_DEFAULT_BLOCK_SIZE: usize = 8 * 1024;

    fn acct() -> Acct {
        Acct {
            name: Cell::new("slab-test"),
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            self_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(false),
            arena_footprint: Cell::new(0),
            arena_nblocks: Cell::new(0),
            window_tail: Cell::new(0),
            is_bump: true,
            kind: "Slab",
            parent: None,
            children: RefCell::new(alloc::vec::Vec::new()),
        }
    }

    const CHUNK: usize = 72;

    fn l() -> Layout {
        Layout::from_size_align(CHUNK, 8).unwrap()
    }

    #[test]
    fn geometry_matches_c_shape() {
        let a = SlabArena::new(SLAB_DEFAULT_BLOCK_SIZE, CHUNK);
        assert_eq!(a.full_chunk_size, 72);
        assert_eq!(
            a.chunks_per_block as usize,
            (SLAB_DEFAULT_BLOCK_SIZE - SLAB_BLKHDRSZ) / 72
        );
        assert!((a.chunks_per_block >> a.blocklist_shift) < (SLAB_BLOCKLIST_COUNT as u32 - 1));
        assert_eq!(a.blocklist_index(0), 0);
        assert!(a.blocklist_index(1) >= 1);
        assert!((a.blocklist_index(a.chunks_per_block) as usize) < SLAB_BLOCKLIST_COUNT);
    }

    #[test]
    fn alloc_free_cycle_reuses_lifo_chunk() {
        let mut a = SlabArena::new(SLAB_DEFAULT_BLOCK_SIZE, CHUNK);
        let acct = acct();
        let p = a.alloc(l(), &acct).unwrap().cast::<u8>();
        let q = a.alloc(l(), &acct).unwrap().cast::<u8>();
        assert_ne!(p.as_ptr(), q.as_ptr());
        // SAFETY: p live from alloc.
        unsafe { a.dealloc(p, l(), &acct) };
        let r = a.alloc(l(), &acct).unwrap().cast::<u8>();
        assert_eq!(p.as_ptr(), r.as_ptr(), "freed chunk reused before unused tail");
        unsafe {
            a.dealloc(q, l(), &acct);
            a.dealloc(r, l(), &acct);
        }
    }

    #[test]
    fn fills_fullest_block_first_across_blocks() {
        let mut a = SlabArena::new(SLAB_DEFAULT_BLOCK_SIZE, CHUNK);
        let acct = acct();
        let cpb = a.chunks_per_block as usize;
        let mut ptrs = alloc::vec::Vec::new();
        for _ in 0..cpb * 3 + 1 {
            ptrs.push(a.alloc(l(), &acct).unwrap().cast::<u8>());
        }
        assert_eq!(a.nblocks(), 4);
        assert_eq!(acct.self_used.get(), 4 * SLAB_DEFAULT_BLOCK_SIZE);
        // Free one chunk from the first (full) block; next alloc must come from it
        // unless a fuller block exists — here the last block is the fullest lane.
        let first = ptrs[0];
        // SAFETY: live chunk.
        unsafe { a.dealloc(first, l(), &acct) };
        let got = a.alloc(l(), &acct).unwrap().cast::<u8>();
        assert_eq!(
            got.as_ptr(),
            first.as_ptr(),
            "the fullest block (one free chunk) is refilled first, LIFO from its freelist"
        );
        ptrs[0] = got;
        for p in ptrs {
            unsafe { a.dealloc(p, l(), &acct) };
        }
    }

    #[test]
    fn empty_blocks_park_up_to_cap_then_free() {
        let mut a = SlabArena::new(SLAB_DEFAULT_BLOCK_SIZE, CHUNK);
        let acct = acct();
        let cpb = a.chunks_per_block as usize;
        let nblk = SLAB_MAXIMUM_EMPTY_BLOCKS + 3;
        let mut ptrs = alloc::vec::Vec::new();
        for _ in 0..cpb * nblk {
            ptrs.push(a.alloc(l(), &acct).unwrap().cast::<u8>());
        }
        assert_eq!(a.nblocks(), nblk);
        for p in ptrs {
            // SAFETY: live chunk.
            unsafe { a.dealloc(p, l(), &acct) };
        }
        assert_eq!(a.emptyblocks.len(), SLAB_MAXIMUM_EMPTY_BLOCKS);
        assert_eq!(a.nblocks(), SLAB_MAXIMUM_EMPTY_BLOCKS);
        assert_eq!(acct.self_used.get(), SLAB_MAXIMUM_EMPTY_BLOCKS * SLAB_DEFAULT_BLOCK_SIZE);
        let before = a.nblocks();
        let p = a.alloc(l(), &acct).unwrap();
        assert_eq!(a.nblocks(), before, "parked empty block reused without malloc");
        // SAFETY: live chunk.
        unsafe { a.dealloc(p.cast(), l(), &acct) };
        a.reset();
        assert_eq!(a.footprint(), 0);
    }

    #[test]
    fn block_mask_recovers_block_for_every_chunk() {
        let mut a = SlabArena::new(SLAB_DEFAULT_BLOCK_SIZE, CHUNK);
        let acct = acct();
        let cpb = a.chunks_per_block as usize;
        let mask = !(SLAB_DEFAULT_BLOCK_SIZE - 1);
        for _ in 0..cpb {
            let p = a.alloc(l(), &acct).unwrap().cast::<u8>().as_ptr().addr();
            let base = p & mask;
            assert!(p >= base + SLAB_BLKHDRSZ && p + CHUNK <= base + SLAB_DEFAULT_BLOCK_SIZE);
            assert_eq!((p - base - SLAB_BLKHDRSZ) % a.full_chunk_size, 0);
        }
    }

    #[test]
    #[should_panic(expected = "unexpected alloc chunk size")]
    fn wrong_size_panics() {
        let mut a = SlabArena::new(SLAB_DEFAULT_BLOCK_SIZE, CHUNK);
        let acct = acct();
        let _ = a.alloc(Layout::from_size_align(CHUNK + 8, 8).unwrap(), &acct);
    }
}
