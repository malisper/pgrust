// upstream 3f3eefc28892 (18.4): Detect pfree or repalloc of a previously-freed memory chunk.
//
// Debug-build (C MEMORY_CONTEXT_CHECKING) freed-chunk bookkeeping. C marks a
// freed chunk in its header (`requested_size = InvalidAllocSize`) and tests
// that mark in AllocSetFree/Realloc, GenerationFree/Realloc and SlabFree so a
// double pfree errors out instead of building a freelist that hands the same
// chunk out twice. The aset and slab arenas here keep no per-chunk header, so
// their mark is membership in this side set of free-chunk addresses; the
// generation arena zeroes its 8-byte block-address header instead. The whole
// module is compiled out of release builds (the C check is too).

use core::ptr::NonNull;

pub(crate) struct FreedSet(hashbrown::HashSet<usize, rustc_hash::FxBuildHasher>);

impl FreedSet {
    pub(crate) const fn new() -> FreedSet {
        FreedSet(hashbrown::HashSet::with_hasher(rustc_hash::FxBuildHasher))
    }

    /// The chunk went onto a freelist.
    #[inline]
    pub(crate) fn note_free(&mut self, ptr: NonNull<u8>) {
        self.0.insert(ptr.as_ptr().addr());
    }

    /// The chunk came back off a freelist (a bump-carved chunk was never in the set).
    #[inline]
    pub(crate) fn note_alloc(&mut self, addr: usize) {
        self.0.remove(&addr);
    }

    #[inline]
    pub(crate) fn contains(&self, ptr: NonNull<u8>) -> bool {
        self.0.contains(&ptr.as_ptr().addr())
    }

    /// The block's free chunks left the arena with the block.
    pub(crate) fn forget_block(&mut self, base: usize, size: usize) {
        self.0.retain(|&a| a < base || a >= base + size);
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

// C: elog(ERROR, "detected double pfree in %s %p") /
//    elog(ERROR, "detected realloc of freed chunk in %s %p").
#[cold]
#[inline(never)]
pub(crate) fn report(realloc: bool, name: &str, ptr: NonNull<u8>) -> ! {
    if realloc {
        panic!("detected realloc of freed chunk in {name} {:p}", ptr.as_ptr());
    }
    panic!("detected double pfree in {name} {:p}", ptr.as_ptr());
}
