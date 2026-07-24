//! stringhash — length-bucketed string hash map for high-NDV plain-text
//! GROUP BY / DISTINCT keys.
//!
//! Rust equivalent of ClickHouse's `StringHashMap` (the machinery behind CH's
//! fast plain-String GROUP BY; src/Common/HashTable/StringHashTable.h +
//! StringHashMap.h @ v26.6.1.1193-stable). Design points mirrored 1:1:
//!
//! * Four length-bucketed sub-tables: keys of 1..=8 bytes packed into a u64,
//!   9..=16 into two u64s, 17..=24 into three u64s, stored INLINE in the cell
//!   and compared as integers — no memcmp, no pointer chase, one cache miss
//!   per probe. Keys > 24 bytes go to a long-tail table holding
//!   (saved hash, arena offset, len); collisions compare the saved hash
//!   before touching bytes.
//! * The empty string and strings with a trailing NUL byte are not
//!   representable as padded integers (no way to encode the length), so they
//!   route to a dedicated slot / the long-tail table — exactly CH's rule.
//! * Packing assembles the padded integer from loads that stay INSIDE the
//!   key's bytes. CH's half-page trick (an 8-byte load past the key, argued
//!   safe by page arithmetic) is deliberately NOT used: reading outside the
//!   referent is UB in Rust however page-safe the address is, and the
//!   over-read lands in adjacent live data (ASan global-buffer-overflow,
//!   GL-TESTRIGS-1 F-R1-3). The ≤8-byte bucket instead uses the two
//!   overlapped in-bounds halves idiom (wyhash's tail shape): two u32 loads
//!   for 4..=8 bytes, three byte loads for 1..=3 — same padded value, no
//!   bytes outside the key ever touched.
//! * Hash: hardware CRC32C chains for the inline buckets (CH's
//!   `StringHashTableHash`), a 64-bit folded-multiply hash (wyhash-style)
//!   for the long tail (CH uses CityHash64 there).
//! * Open addressing, linear probing, power-of-two capacity, 50% max load,
//!   +1 size-degree growth (CH's `StringHashTableGrower`), initial degree 8.
//!
//! The map is specialized for the group-by use pattern: `insert_or_get`
//! returns a dense group index (payload lives inline in the cell), there is
//! no per-key allocation (long keys bump-append into one arena), and group
//! ids are stable across growth.

/// Dense group index type. u32 caps groups at ~4.29e9 — beyond any
/// work_mem-admissible hash aggregation.
pub type GroupId = u32;

const INITIAL_DEGREE: u32 = 8;

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// One CRC32C accumulation step over a u64 (CH: `__crc32cd` / `_mm_crc32_u64`).
#[inline(always)]
#[cfg(all(target_arch = "aarch64", target_feature = "crc"))]
fn crc_step(seed: u64, x: u64) -> u64 {
    unsafe { core::arch::aarch64::__crc32cd(seed as u32, x) as u64 }
}

#[inline(always)]
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
fn crc_step(seed: u64, x: u64) -> u64 {
    unsafe { core::arch::x86_64::_mm_crc32_u64(seed, x) }
}

/// Portable fallback (also what non-CRC hosts get): 128-bit folded multiply.
#[inline(always)]
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "crc"),
    all(target_arch = "x86_64", target_feature = "sse4.2")
)))]
fn crc_step(seed: u64, x: u64) -> u64 {
    mum(seed ^ 0x9E37_79B9_7F4A_7C15, x ^ 0xA076_1D64_78BD_642F)
}

#[inline(always)]
fn hash8(k: u64) -> u64 {
    crc_step(!0u64, k)
}

#[inline(always)]
fn hash16(a: u64, b: u64) -> u64 {
    crc_step(crc_step(!0u64, a), b)
}

#[inline(always)]
fn hash24(a: u64, b: u64, c: u64) -> u64 {
    crc_step(crc_step(crc_step(!0u64, a), b), c)
}

/// 128-bit multiply, fold high^low (wyhash's mixer).
#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

const WYP0: u64 = 0xA076_1D64_78BD_642F;
const WYP1: u64 = 0xE703_7ED1_A0B4_28DB;
const WYP2: u64 = 0x8EBC_6AF0_9C88_C6E3;

/// True when a hardware CRC step is available (the `crc_step` fast paths).
const HAS_HW_CRC: bool = cfg!(any(
    all(target_arch = "aarch64", target_feature = "crc"),
    all(target_arch = "x86_64", target_feature = "sse4.2")
));

/// 64-bit hash for the long-tail bucket (keys > 24 bytes, plus the rare
/// trailing-NUL short keys). CH uses a serial CRC32C chain here (CRC32Hash
/// in base/StringViewHash.h — "on real data sets works much faster" than
/// CityHash). Ours: THREE interleaved CRC32C accumulators over 24-byte
/// stripes (the serial chain is latency-bound; three lanes recover the ILP),
/// overlapped 8-byte tail, folded-multiply finalizer for 64-bit spread.
#[inline]
fn hash_long(data: &[u8]) -> u64 {
    if HAS_HW_CRC {
        let len = data.len();
        let p = data.as_ptr();
        if len >= 8 {
            let mut a = !0u64;
            let mut b = 0x2AAB_54CC_5AA5_33CCu64;
            let mut c = 0x7744_9911_CC55_AA22u64;
            unsafe {
                let mut i = 0usize;
                while i + 24 <= len {
                    a = crc_step(a, load8(p.add(i)));
                    b = crc_step(b, load8(p.add(i + 8)));
                    c = crc_step(c, load8(p.add(i + 16)));
                    i += 24;
                }
                while i + 8 <= len {
                    a = crc_step(a, load8(p.add(i)));
                    i += 8;
                }
                if i < len {
                    b = crc_step(b, load8(p.add(len - 8))); // overlapped tail
                }
            }
            return mum((a << 32) ^ b ^ (len as u64), c ^ WYP2);
        }
        // 1..=7 bytes (only trailing-NUL keys land here).
        let mut x = 0u64;
        for (i, &byte) in data.iter().enumerate() {
            x |= (byte as u64) << (i * 8);
        }
        return mum(crc_step(!0u64, x) ^ (len as u64), WYP2);
    }
    hash_long_portable(data)
}

/// Portable long hash (wyhash-style) for hosts without hardware CRC.
#[inline]
fn hash_long_portable(data: &[u8]) -> u64 {
    let len = data.len();
    let mut seed = WYP0 ^ (len as u64);
    unsafe {
        let p = data.as_ptr();
        if len >= 16 {
            let mut i = 0usize;
            while i + 16 <= len {
                seed = mum(load8(p.add(i)) ^ WYP1, load8(p.add(i + 8)) ^ seed);
                i += 16;
            }
            if i < len {
                // Overlapped final stripe: the last 16 bytes.
                seed = mum(load8(p.add(len - 16)) ^ WYP1, load8(p.add(len - 8)) ^ seed);
            }
        } else if len >= 8 {
            seed = mum(load8(p) ^ WYP1, load8(p.add(len - 8)) ^ seed);
        } else if len > 0 {
            // 1..=7 bytes: byte-assemble (rare path — only trailing-NUL keys).
            let mut a = 0u64;
            for (i, &b) in data.iter().enumerate() {
                a |= (b as u64) << (i * 8);
            }
            seed = mum(a ^ WYP1, seed);
        }
    }
    mum(seed, WYP2 ^ (len as u64))
}

// ---------------------------------------------------------------------------
// Key packing (CH StringHashTable::dispatch, little-endian arm)
// ---------------------------------------------------------------------------

/// Unaligned little-endian 8-byte load.
#[inline(always)]
unsafe fn load8(p: *const u8) -> u64 {
    u64::from_le((p as *const u64).read_unaligned())
}

/// Inline word-wise byte equality (CH inlines memequalWide here; a libc
/// memcmp call costs more than the compare for 25..100-byte keys). Full
/// 8-byte words plus one overlapped tail load — all in-bounds for len >= 8;
/// byte loop for the rare shorter (trailing-NUL) keys.
#[inline(always)]
unsafe fn eq_bytes(a: *const u8, b: *const u8, len: usize) -> bool {
    if len < 8 {
        for i in 0..len {
            if *a.add(i) != *b.add(i) {
                return false;
            }
        }
        return true;
    }
    let mut i = 0usize;
    while i + 8 <= len {
        if load8(a.add(i)) != load8(b.add(i)) {
            return false;
        }
        i += 8;
    }
    i == len || load8(a.add(len - 8)) == load8(b.add(len - 8))
}


/// Advise transparent hugepages for large allocations (>= 2 MiB). The
/// lane-v2-hugepages change applies the same advice to the executor's
/// existing arenas; the table stays comparable. Alignment is not guaranteed
/// by malloc, so advise the enclosing 2 MiB-aligned span conservatively
/// clipped inward; a no-op on failure.
#[inline]
fn advise_huge(p: *mut u8, bytes: usize) {
    #[cfg(target_os = "linux")]
    {
        const HUGE: usize = 2 * 1024 * 1024;
        if bytes >= HUGE {
            let start = (p as usize).next_multiple_of(HUGE);
            let end = (p as usize + bytes) & !(HUGE - 1);
            if end > start {
                unsafe {
                    libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_HUGEPAGE);
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (p, bytes);
    }
}

/// Power-of-two cell array backed by realloc: growth EXTENDS the allocation
/// in place when the allocator can (mremap for the multi-MB tables), so the
/// already-faulted first half is never copied or re-faulted — CH's resize
/// does exactly this through jemalloc. All cell types are POD with all-zero
/// == empty sentinel.
struct RawCells<C> {
    ptr: std::ptr::NonNull<C>,
    cap: usize,
}

impl<C> RawCells<C> {
    fn new() -> Self {
        RawCells { ptr: std::ptr::NonNull::dangling(), cap: 0 }
    }

    #[cold]
    fn alloc_zeroed(&mut self, n: usize) {
        debug_assert!(self.cap == 0 && n.is_power_of_two());
        unsafe {
            let layout = std::alloc::Layout::array::<C>(n).unwrap();
            let p = std::alloc::alloc_zeroed(layout) as *mut C;
            self.ptr = std::ptr::NonNull::new(p).expect("stringhash: allocation failed");
            self.cap = n;
            advise_huge(p as *mut u8, layout.size());
        }
    }

    /// Double the capacity, zeroing the new upper half. The lower half's
    /// contents (and page mappings) are preserved.
    #[cold]
    fn grow_double(&mut self) {
        self.grow_to(self.cap * 2);
    }

    /// Extend the allocation to `new_cap` (pow2, > cap), zeroing the new
    /// tail. `grow_double` generalized for the dedupsub projection reserve:
    /// one realloc + one tail memset regardless of how many doublings the
    /// jump replaces (the in-place/mremap fast path is identical).
    #[cold]
    fn grow_to(&mut self, new_cap: usize) {
        debug_assert!(new_cap.is_power_of_two() && new_cap > self.cap);
        unsafe {
            let old_layout = std::alloc::Layout::array::<C>(self.cap).unwrap();
            let new_bytes = std::alloc::Layout::array::<C>(new_cap).unwrap().size();
            let p = std::alloc::realloc(self.ptr.as_ptr() as *mut u8, old_layout, new_bytes)
                as *mut C;
            let ptr = std::ptr::NonNull::new(p).expect("stringhash: realloc failed");
            std::ptr::write_bytes(ptr.as_ptr().add(self.cap), 0, new_cap - self.cap);
            self.ptr = ptr;
            self.cap = new_cap;
            advise_huge(self.ptr.as_ptr() as *mut u8, new_bytes);
        }
    }

    #[inline(always)]
    unsafe fn get(&self, i: usize) -> &C {
        &*self.ptr.as_ptr().add(i)
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&mut self, i: usize) -> &mut C {
        &mut *self.ptr.as_ptr().add(i)
    }
}

impl<C> Drop for RawCells<C> {
    fn drop(&mut self) {
        if self.cap != 0 {
            unsafe {
                let layout = std::alloc::Layout::array::<C>(self.cap).unwrap();
                std::alloc::dealloc(self.ptr.as_ptr() as *mut u8, layout);
            }
        }
    }
}

/// Uninitialized byte chunk for the key arena (bytes are written before any
/// read; zeroing would fault every page up front for nothing).
fn alloc_uninit_bytes(n: usize) -> Box<[u8]> {
    unsafe {
        let layout = std::alloc::Layout::array::<u8>(n).unwrap();
        let p = std::alloc::alloc(layout);
        assert!(!p.is_null(), "stringhash: allocation failed");
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(p, n))
    }
}

/// Pack a 1..=8-byte key (no trailing NUL) into a u64, zero-padded high.
/// Every load stays inside the key: two overlapped u32 halves for 4..=8
/// bytes, three byte loads for 1..=3 (wyhash's tail shape). The overlapped
/// bits agree by construction (same memory), so OR reassembles the exact
/// little-endian zero-padded value the old 8-byte load produced — without
/// the load-past-the-key UB it carried (GL-TESTRIGS-1 F-R1-3; the "safe by
/// page arithmetic" over-read is still a read of adjacent live objects).
#[inline(always)]
fn pack8(s: &[u8]) -> u64 {
    let sz = s.len();
    debug_assert!((1..=8).contains(&sz) && s[sz - 1] != 0);
    let p = s.as_ptr();
    unsafe {
        if sz >= 4 {
            let lo = u32::from_le((p as *const u32).read_unaligned()) as u64;
            let hi = u32::from_le((p.add(sz - 4) as *const u32).read_unaligned()) as u64;
            lo | (hi << ((sz - 4) * 8))
        } else {
            let b0 = *p as u64;
            let mid = *p.add(sz >> 1) as u64;
            let last = *p.add(sz - 1) as u64;
            b0 | (mid << ((sz >> 1) * 8)) | (last << ((sz - 1) * 8))
        }
    }
}

/// Pack a 9..=16-byte key: first 8 bytes + last 8 bytes with the overlap
/// shifted out. All loads are in-bounds.
#[inline(always)]
fn pack16(s: &[u8]) -> (u64, u64) {
    let sz = s.len();
    debug_assert!((9..=16).contains(&sz) && s[sz - 1] != 0);
    let shift = (((16 - sz) & 7) * 8) as u32;
    unsafe {
        let a = load8(s.as_ptr());
        let b = load8(s.as_ptr().add(sz - 8)) >> shift;
        (a, b)
    }
}

/// Pack a 17..=24-byte key: first 16 bytes + last 8 with overlap shifted out.
#[inline(always)]
fn pack24(s: &[u8]) -> (u64, u64, u64) {
    let sz = s.len();
    debug_assert!((17..=24).contains(&sz) && s[sz - 1] != 0);
    let shift = (((24 - sz) & 7) * 8) as u32;
    unsafe {
        let a = load8(s.as_ptr());
        let b = load8(s.as_ptr().add(8));
        let c = load8(s.as_ptr().add(sz - 8)) >> shift;
        (a, b, c)
    }
}

// ---------------------------------------------------------------------------
// Inline-key sub-tables (t8 / t16 / t24)
// ---------------------------------------------------------------------------
//
// A macro keeps the three monomorphic hot loops textually identical. The
// empty sentinel is the all-zero LAST word of the key: unrepresentable,
// because packed keys always have a nonzero final byte in their top word
// (CH StringHashMapCell::isZero). The group id rides inline in the cell —
// hit = exactly one cache line touched.

macro_rules! inline_table {
    ($tab:ident, $cell:ident, $keyty:ty, $last:ident, $hashfn:ident) => {
        #[derive(Clone, Copy)]
        struct $cell {
            key: $keyty,
            v: GroupId,
        }

        struct $tab {
            cells: RawCells<$cell>,
            mask: usize,
            len: usize,
        }

        impl $tab {
            fn new() -> Self {
                $tab { cells: RawCells::new(), mask: 0, len: 0 }
            }

            #[inline(always)]
            fn key_last(key: &$keyty) -> u64 {
                inline_table!(@last key, $last)
            }

            /// Insert-or-get. `key`'s last word is nonzero by construction.
            #[inline(always)]
            fn insert(&mut self, key: $keyty, hash: u64, next_id: &mut GroupId) -> (GroupId, bool) {
                if self.cells.cap == 0 {
                    self.cells.alloc_zeroed(1 << INITIAL_DEGREE);
                    self.mask = self.cells.cap - 1;
                }
                let mut pos = (hash as usize) & self.mask;
                loop {
                    // SAFETY: pos is masked to capacity.
                    let c = unsafe { self.cells.get_mut(pos) };
                    if c.key == key {
                        return (c.v, false);
                    }
                    if Self::key_last(&c.key) == 0 {
                        let id = *next_id;
                        *next_id += 1;
                        *c = $cell { key, v: id };
                        self.len += 1;
                        // CH grows on size > max_fill (STRICTLY greater than
                        // 50%); >= fires a doubling early at exact-pow2
                        // cardinalities (2x memory + one extra full rehash).
                        if self.len * 2 > self.cells.cap {
                            self.grow();
                        }
                        return (id, true);
                    }
                    pos = (pos + 1) & self.mask;
                }
            }

            /// Find-only probe (no insert) — the residual-replay path.
            #[inline(always)]
            fn find(&self, key: $keyty, hash: u64) -> Option<GroupId> {
                if self.cells.cap == 0 {
                    return None;
                }
                let mut pos = (hash as usize) & self.mask;
                loop {
                    // SAFETY: pos is masked to capacity.
                    let c = unsafe { self.cells.get(pos) };
                    if c.key == key {
                        return Some(c.v);
                    }
                    if Self::key_last(&c.key) == 0 {
                        return None;
                    }
                    pos = (pos + 1) & self.mask;
                }
            }

            /// CH's HashTable::resize: extend in place (realloc), then move
            /// only the cells whose home chain changed; the wrapped tail of
            /// any collision chain that crossed the old buffer end is
            /// re-processed until the first empty cell.
            #[cold]
            fn grow(&mut self) {
                let old_n = self.cells.cap;
                self.cells.grow_double();
                self.mask = self.cells.cap - 1;
                unsafe {
                    for i in 0..old_n {
                        if Self::key_last(&self.cells.get(i).key) != 0 {
                            self.reinsert(i);
                        }
                    }
                    let mut i = old_n;
                    while i < self.cells.cap && Self::key_last(&self.cells.get(i).key) != 0 {
                        self.reinsert(i);
                        i += 1;
                    }
                }
            }

            /// Move the cell at `i` to its correct position in the resized
            /// table (or leave it if its probe chain still reaches it).
            #[inline]
            unsafe fn reinsert(&mut self, i: usize) {
                let c = *self.cells.get(i);
                let home = ($hashfn(&c) as usize) & self.mask;
                if home == i {
                    return;
                }
                let mut pos = home;
                loop {
                    if pos == i {
                        return; // still reachable on its chain — stays
                    }
                    let slot = self.cells.get_mut(pos);
                    if Self::key_last(&slot.key) == 0 {
                        *slot = c;
                        let old = self.cells.get_mut(i);
                        *old = $cell { key: <$keyty>::default(), v: 0 };
                        return;
                    }
                    pos = (pos + 1) & self.mask;
                }
            }

            fn mem_bytes(&self) -> usize {
                self.cells.cap * std::mem::size_of::<$cell>()
            }
        }
    };
    (@last $key:ident, single) => { *$key };
    (@last $key:ident, second) => { $key[1] };
    (@last $key:ident, third) => { $key[2] };
}

#[inline(always)]
fn rehash8(c: &Cell8) -> u64 {
    hash8(c.key)
}
#[inline(always)]
fn rehash16(c: &Cell16) -> u64 {
    hash16(c.key[0], c.key[1])
}
#[inline(always)]
fn rehash24(c: &Cell24) -> u64 {
    hash24(c.key[0], c.key[1], c.key[2])
}

inline_table!(Tab8, Cell8, u64, single, rehash8);
inline_table!(Tab16, Cell16, [u64; 2], second, rehash16);
inline_table!(Tab24, Cell24, [u64; 3], third, rehash24);

// ---------------------------------------------------------------------------
// Long-tail sub-table (> 24 bytes, plus trailing-NUL keys)
// ---------------------------------------------------------------------------

/// Bump arena for long keys: fixed chunks (geometric to 32 MiB), never
/// reallocated — key pointers stay stable and no half-gigabyte memcpy ever
/// happens on growth (CH's Arena is chunked for the same reason).
struct KeyArena {
    chunks: Vec<Box<[u8]>>,
    used: usize, // in the last chunk
}

const ARENA_FIRST_CHUNK: usize = 64 * 1024;
const ARENA_MAX_CHUNK: usize = 32 * 1024 * 1024;

impl KeyArena {
    fn new() -> Self {
        KeyArena { chunks: Vec::new(), used: 0 }
    }

    #[cold]
    fn new_chunk(&mut self, at_least: usize) {
        let next = self
            .chunks
            .last()
            .map(|c| (c.len() * 2).min(ARENA_MAX_CHUNK))
            .unwrap_or(ARENA_FIRST_CHUNK)
            .max(at_least);
        let chunk = alloc_uninit_bytes(next);
        advise_huge(chunk.as_ptr() as *mut u8, chunk.len());
        self.chunks.push(chunk);
        self.used = 0;
    }

    /// Copy `key` in; the returned pointer is stable for the arena's life.
    #[inline]
    fn push(&mut self, key: &[u8]) -> *const u8 {
        if self.chunks.is_empty() || self.used + key.len() > self.chunks.last().unwrap().len() {
            self.new_chunk(key.len());
        }
        let chunk = self.chunks.last_mut().unwrap();
        unsafe {
            let dst = chunk.as_mut_ptr().add(self.used);
            std::ptr::copy_nonoverlapping(key.as_ptr(), dst, key.len());
            self.used += key.len();
            dst
        }
    }

    fn mem_bytes(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }
}

/// 24-byte cell: saved hash compared before any byte compare; key bytes live
/// in the map's chunked bump arena. `len == 0` marks an empty cell (the true
/// empty string never reaches this table).
#[derive(Clone, Copy)]
struct CellS {
    hash: u64,
    ptr: *const u8,
    len: u32,
    v: GroupId,
}

struct TabS {
    cells: RawCells<CellS>,
    mask: usize,
    len: usize,
    arena: KeyArena,
}

impl TabS {
    fn new() -> Self {
        TabS { cells: RawCells::new(), mask: 0, len: 0, arena: KeyArena::new() }
    }

    #[inline(always)]
    fn insert(&mut self, key: &[u8], hash: u64, next_id: &mut GroupId) -> (GroupId, bool) {
        if self.cells.cap == 0 {
            self.cells.alloc_zeroed(1 << INITIAL_DEGREE);
            self.mask = self.cells.cap - 1;
        }
        debug_assert!(!key.is_empty());
        let mut pos = (hash as usize) & self.mask;
        loop {
            let c = unsafe { *self.cells.get(pos) };
            if c.len == 0 {
                let ptr = self.arena.push(key);
                let id = *next_id;
                *next_id += 1;
                unsafe {
                    *self.cells.get_mut(pos) =
                        CellS { hash, ptr, len: key.len() as u32, v: id };
                }
                self.len += 1;
                if self.len * 2 > self.cells.cap {
                    self.grow();
                }
                return (id, true);
            }
            if c.hash == hash
                && c.len as usize == key.len()
                && unsafe { eq_bytes(c.ptr, key.as_ptr(), key.len()) }
            {
                return (c.v, false);
            }
            pos = (pos + 1) & self.mask;
        }
    }

    /// CH-style in-place resize (see the inline tables' `grow`).
    #[cold]
    fn grow(&mut self) {
        let old_n = self.cells.cap;
        self.cells.grow_double();
        self.mask = self.cells.cap - 1;
        unsafe {
            for i in 0..old_n {
                if self.cells.get(i).len != 0 {
                    self.reinsert(i);
                }
            }
            let mut i = old_n;
            while i < self.cells.cap && self.cells.get(i).len != 0 {
                self.reinsert(i);
                i += 1;
            }
        }
    }

    #[inline]
    unsafe fn reinsert(&mut self, i: usize) {
        let c = *self.cells.get(i);
        let home = (c.hash as usize) & self.mask;
        if home == i {
            return;
        }
        let mut pos = home;
        loop {
            if pos == i {
                return;
            }
            let slot = self.cells.get_mut(pos);
            if slot.len == 0 {
                *slot = c;
                self.cells.get_mut(i).len = 0;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    fn mem_bytes(&self) -> usize {
        self.cells.cap * std::mem::size_of::<CellS>() + self.arena.mem_bytes()
    }
}

// The raw arena pointers are owned by the map itself (self-referential but
// chunk-stable); the map is safe to move across threads as a whole.
unsafe impl Send for TabS {}

// ---------------------------------------------------------------------------
// The map
// ---------------------------------------------------------------------------

pub struct StringHashMap {
    empty_id: GroupId, // GroupId::MAX = unset
    t8: Tab8,
    t16: Tab16,
    t24: Tab24,
    ts: TabS,
    next_id: GroupId,
}

impl Default for StringHashMap {
    fn default() -> Self {
        Self::new()
    }
}

impl StringHashMap {
    pub fn new() -> Self {
        StringHashMap {
            empty_id: GroupId::MAX,
            t8: Tab8::new(),
            t16: Tab16::new(),
            t24: Tab24::new(),
            ts: TabS::new(),
            next_id: 0,
        }
    }

    /// Insert-or-get: returns the key's dense group index and whether the key
    /// was new. Ids are assigned in first-appearance order, 0-based, dense.
    #[inline(always)]
    pub fn insert_or_get(&mut self, key: &[u8]) -> (GroupId, bool) {
        let sz = key.len();
        if sz == 0 {
            if self.empty_id == GroupId::MAX {
                self.empty_id = self.next_id;
                self.next_id += 1;
                return (self.empty_id, true);
            }
            return (self.empty_id, false);
        }
        if key[sz - 1] == 0 {
            // Trailing NUL: unrepresentable as a padded integer key (length
            // would be ambiguous) — generic table, like CH.
            return self.ts.insert(key, hash_long(key), &mut self.next_id);
        }
        match (sz - 1) >> 3 {
            0 => {
                let k = pack8(key);
                self.t8.insert(k, hash8(k), &mut self.next_id)
            }
            1 => {
                let (a, b) = pack16(key);
                self.t16.insert([a, b], hash16(a, b), &mut self.next_id)
            }
            2 => {
                let (a, b, c) = pack24(key);
                self.t24.insert([a, b, c], hash24(a, b, c), &mut self.next_id)
            }
            _ => self.ts.insert(key, hash_long(key), &mut self.next_id),
        }
    }

    /// Number of distinct keys seen.
    pub fn len(&self) -> usize {
        self.next_id as usize
    }

    pub fn is_empty(&self) -> bool {
        self.next_id == 0
    }

    /// Total heap footprint: all sub-table buffers + the long-key arena.
    pub fn mem_bytes(&self) -> usize {
        self.t8.mem_bytes() + self.t16.mem_bytes() + self.t24.mem_bytes() + self.ts.mem_bytes()
    }
}


// ---------------------------------------------------------------------------
// External-arena variant (executor wiring)
// ---------------------------------------------------------------------------
//
// Same length-bucketed design, but the long-tail bucket references key bytes
// in a CALLER-owned arena (`Vec<u8>`) instead of an internal one: the
// hash-grouped arm's `arena` stays the single byte authority (its emission
// comparator, spans, and degrade paths read it), and every insert appends
// the key there and reports the offset for the caller's span bookkeeping.

/// Long-tail bucket over an external arena: cells hold (saved hash, off,
/// len) into the caller's arena. `len == 0` marks empty (the empty string
/// is handled at the map level).
#[derive(Clone, Copy)]
struct CellX {
    hash: u64,
    off: u32,
    len: u32,
    v: GroupId,
}

struct TabX {
    cells: RawCells<CellX>,
    mask: usize,
    len: usize,
}

impl TabX {
    fn new() -> Self {
        TabX { cells: RawCells::new(), mask: 0, len: 0 }
    }

    #[inline(always)]
    fn insert(
        &mut self,
        key: &[u8],
        hash: u64,
        arena: &mut Vec<u8>,
        next_id: &mut GroupId,
    ) -> (GroupId, bool, u32) {
        if self.cells.cap == 0 {
            self.cells.alloc_zeroed(1 << INITIAL_DEGREE);
            self.mask = self.cells.cap - 1;
        }
        debug_assert!(!key.is_empty());
        let mut pos = (hash as usize) & self.mask;
        loop {
            let c = unsafe { *self.cells.get(pos) };
            if c.len == 0 {
                let off = arena.len();
                assert!(
                    off + key.len() <= u32::MAX as usize,
                    "stringhash: external arena exceeds 4 GiB"
                );
                arena.extend_from_slice(key);
                let id = *next_id;
                *next_id += 1;
                unsafe {
                    *self.cells.get_mut(pos) =
                        CellX { hash, off: off as u32, len: key.len() as u32, v: id };
                }
                self.len += 1;
                if self.len * 2 > self.cells.cap {
                    self.grow();
                }
                return (id, true, off as u32);
            }
            if c.hash == hash && c.len as usize == key.len() && unsafe {
                eq_bytes(arena.as_ptr().add(c.off as usize), key.as_ptr(), key.len())
            } {
                return (c.v, false, c.off);
            }
            pos = (pos + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn find(&self, key: &[u8], hash: u64, arena: &[u8]) -> Option<GroupId> {
        if self.cells.cap == 0 {
            return None;
        }
        let mut pos = (hash as usize) & self.mask;
        loop {
            let c = unsafe { *self.cells.get(pos) };
            if c.len == 0 {
                return None;
            }
            if c.hash == hash && c.len as usize == key.len() && unsafe {
                eq_bytes(arena.as_ptr().add(c.off as usize), key.as_ptr(), key.len())
            } {
                return Some(c.v);
            }
            pos = (pos + 1) & self.mask;
        }
    }

    #[cold]
    fn grow(&mut self) {
        let old_n = self.cells.cap;
        self.cells.grow_double();
        self.mask = self.cells.cap - 1;
        unsafe {
            for i in 0..old_n {
                if self.cells.get(i).len != 0 {
                    self.reinsert(i);
                }
            }
            let mut i = old_n;
            while i < self.cells.cap && self.cells.get(i).len != 0 {
                self.reinsert(i);
                i += 1;
            }
        }
    }

    #[inline]
    unsafe fn reinsert(&mut self, i: usize) {
        let c = *self.cells.get(i);
        let home = (c.hash as usize) & self.mask;
        if home == i {
            return;
        }
        let mut pos = home;
        loop {
            if pos == i {
                return;
            }
            let slot = self.cells.get_mut(pos);
            if slot.len == 0 {
                *slot = c;
                self.cells.get_mut(i).len = 0;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    fn mem_bytes(&self) -> usize {
        self.cells.cap * std::mem::size_of::<CellX>()
    }
}

/// Insert-or-get map over an external byte arena (see module section doc).
/// Returned ids are dense, 0-based, in first-appearance order; every INSERT
/// appends the key bytes to `arena` and reports their offset (hits report
/// the ORIGINAL offset).
pub struct ExtIdMap {
    empty: GroupId, // GroupId::MAX = unset; off recorded alongside
    empty_off: u32,
    t8: Tab8,
    t16: Tab16,
    t24: Tab24,
    tx: TabX,
    next_id: GroupId,
}

impl Default for ExtIdMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtIdMap {
    pub fn new() -> Self {
        ExtIdMap {
            empty: GroupId::MAX,
            empty_off: 0,
            t8: Tab8::new(),
            t16: Tab16::new(),
            t24: Tab24::new(),
            tx: TabX::new(),
            next_id: 0,
        }
    }

    /// Returns (dense id, inserted, offset of the key bytes in `arena`).
    #[inline(always)]
    pub fn insert_or_get(&mut self, key: &[u8], arena: &mut Vec<u8>) -> (GroupId, bool, u32) {
        let sz = key.len();
        if sz == 0 {
            if self.empty == GroupId::MAX {
                self.empty = self.next_id;
                self.next_id += 1;
                self.empty_off = arena.len() as u32;
                return (self.empty, true, self.empty_off);
            }
            return (self.empty, false, self.empty_off);
        }
        if key[sz - 1] == 0 {
            return self.tx.insert(key, hash_long(key), arena, &mut self.next_id);
        }
        match (sz - 1) >> 3 {
            0 => {
                let k = pack8(key);
                let (id, ins) = self.t8.insert(k, hash8(k), &mut self.next_id);
                (id, ins, Self::append_if(ins, key, arena))
            }
            1 => {
                let (a, b) = pack16(key);
                let (id, ins) = self.t16.insert([a, b], hash16(a, b), &mut self.next_id);
                (id, ins, Self::append_if(ins, key, arena))
            }
            2 => {
                let (a, b, c) = pack24(key);
                let (id, ins) = self.t24.insert([a, b, c], hash24(a, b, c), &mut self.next_id);
                (id, ins, Self::append_if(ins, key, arena))
            }
            _ => self.tx.insert(key, hash_long(key), arena, &mut self.next_id),
        }
    }

    #[inline(always)]
    fn append_if(inserted: bool, key: &[u8], arena: &mut Vec<u8>) -> u32 {
        if inserted {
            let off = arena.len();
            assert!(
                off + key.len() <= u32::MAX as usize,
                "stringhash: external arena exceeds 4 GiB"
            );
            arena.extend_from_slice(key);
            off as u32
        } else {
            u32::MAX // hits in the inline buckets do not track the offset
        }
    }

    /// Find-only probe (residual replay): no insert, no arena append.
    #[inline(always)]
    pub fn find(&self, key: &[u8], arena: &[u8]) -> Option<GroupId> {
        let sz = key.len();
        if sz == 0 {
            return (self.empty != GroupId::MAX).then_some(self.empty);
        }
        if key[sz - 1] == 0 {
            return self.tx.find(key, hash_long(key), arena);
        }
        match (sz - 1) >> 3 {
            0 => {
                let k = pack8(key);
                self.t8.find(k, hash8(k))
            }
            1 => {
                let (a, b) = pack16(key);
                self.t16.find([a, b], hash16(a, b))
            }
            2 => {
                let (a, b, c) = pack24(key);
                self.t24.find([a, b, c], hash24(a, b, c))
            }
            _ => self.tx.find(key, hash_long(key), arena),
        }
    }

    /// Reserve one dense id for an out-of-band group (the caller's NULL
    /// slot) so map-assigned ids and caller-side groups share one space.
    pub fn reserve_id(&mut self) -> GroupId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn len(&self) -> usize {
        self.next_id as usize
    }

    pub fn is_empty(&self) -> bool {
        self.next_id == 0
    }

    /// Table-buffer footprint (the key bytes live in the caller's arena).
    pub fn mem_bytes(&self) -> usize {
        self.t8.mem_bytes() + self.t16.mem_bytes() + self.t24.mem_bytes() + self.tx.mem_bytes()
    }
}


// ---------------------------------------------------------------------------
// Set variants (DISTINCT dedup): one-miss inline-key probe tables
// ---------------------------------------------------------------------------
//
// The executor's DistinctSet keeps its value arrays (ints / blob+spans —
// spill record formats and parallel export read those); these tables replace
// only its slot->index indirection (two dependent misses per probe) with
// key-in-cell probes (one miss). CH HashMap-class layout: pow2, linear
// probing, 50% max fill, realloc in-place resize.

/// Sparse-clear crossover: `clear_with_*` probes per entry (~a few ns each)
/// instead of memsetting the whole cell array (~0.25 ns/8 B); the targeted
/// clear wins once entries are rarer than one per FACTOR cells. The consumer
/// pattern that needs this is the executor's POOLED per-group DistinctSet
/// (codedgroup emit): one big group inflates the retained capacity, then
/// hundreds of thousands of tiny groups would each pay the full-capacity
/// memset (the train-10 near-unique-text-key regression, +17%).
const CLEAR_SPARSE_FACTOR: usize = 16;

/// Exact i64 set. 8-byte cells (the key itself); 0 is a valid key, tracked
/// by `has_zero`; the all-zero cell is the empty sentinel.
pub struct IntSet {
    cells: RawCells<i64>,
    mask: usize,
    len: usize,
    has_zero: bool,
    /// `clear_with_keys` position scratch (collect-then-zero; retained).
    scratch: Vec<u32>,
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

impl IntSet {
    pub fn new() -> Self {
        IntSet { cells: RawCells::new(), mask: 0, len: 0, has_zero: false, scratch: Vec::new() }
    }

    /// Returns true if `k` was newly inserted.
    #[inline(always)]
    pub fn insert(&mut self, k: i64) -> bool {
        if k == 0 {
            let new = !self.has_zero;
            self.has_zero = true;
            return new;
        }
        if self.cells.cap == 0 {
            self.cells.alloc_zeroed(1 << INITIAL_DEGREE);
            self.mask = self.cells.cap - 1;
        }
        let mut pos = (hash8(k as u64) as usize) & self.mask;
        loop {
            let c = unsafe { self.cells.get_mut(pos) };
            if *c == k {
                return false;
            }
            if *c == 0 {
                *c = k;
                self.len += 1;
                if self.len * 2 > self.cells.cap {
                    self.grow();
                }
                return true;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    /// Prefetch the home cell line `insert(k)` will probe (a hint — no
    /// semantic effect; empty tables no-op). Batched-driver look-ahead
    /// entry (batch-insert lane; SEAM: stringhash2 owns table-internal
    /// batch machinery — this is the minimal read-only hook, flagged for
    /// their review at merge).
    #[inline(always)]
    pub fn prefetch(&self, k: i64) {
        if self.cells.cap == 0 {
            return;
        }
        let pos = (hash8(k as u64) as usize) & self.mask;
        // SAFETY: mask keeps pos in bounds; prefetch is a hint.
        unsafe {
            let p = self.cells.get(pos) as *const i64;
            #[cfg(target_arch = "aarch64")]
            core::arch::asm!(
                "prfm pldl1keep, [{0}]",
                in(reg) p,
                options(nostack, preserves_flags)
            );
            #[cfg(target_arch = "x86_64")]
            core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(
                p as *const i8,
            );
            #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
            let _ = p;
        }
    }

    #[cold]
    fn grow(&mut self) {
        self.grow_to_cap(self.cells.cap * 2);
    }

    /// Grow to `new_cap` cells (pow2, > cap) with ONE realloc + tail zero +
    /// in-place rehash — `grow` generalized so a projection reserve can jump
    /// the whole doubling ladder in one step (dedupsub I3: the ladder's
    /// rehash volume is ~1x final len and measured ~4-5% of narrow-sort DISTINCT cycles).
    /// The two rehash loops are ratio-independent: pass 1 reinserts every
    /// key from the old prefix at its new-mask position; pass 2 fixes the
    /// chain segment that wrapped past the old end (identical to the 2x
    /// version — neither loop assumes new_cap == 2*old).
    #[cold]
    fn grow_to_cap(&mut self, new_cap: usize) {
        let old_n = self.cells.cap;
        debug_assert!(new_cap.is_power_of_two() && new_cap > old_n);
        self.cells.grow_to(new_cap);
        self.mask = self.cells.cap - 1;
        unsafe {
            for i in 0..old_n {
                if *self.cells.get(i) != 0 {
                    self.reinsert(i);
                }
            }
            let mut i = old_n;
            while i < self.cells.cap && *self.cells.get(i) != 0 {
                self.reinsert(i);
                i += 1;
            }
        }
    }

    /// Projection reserve (dedupsub I3): make the table large enough for
    /// `target_len` keys under the 50% max-load law, in one jump. A no-op
    /// when the table is already big enough; an EMPTY set allocates the
    /// final size directly (zero rehash). Never shrinks. Table geometry is
    /// a non-surface: contents, value order, and every probe verdict are
    /// unchanged for any target.
    pub fn reserve_for(&mut self, target_len: usize) {
        let needed = target_len
            .saturating_mul(2)
            .max(1 << INITIAL_DEGREE)
            .next_power_of_two();
        if self.cells.cap == 0 {
            self.cells.alloc_zeroed(needed);
            self.mask = self.cells.cap - 1;
            return;
        }
        if needed > self.cells.cap {
            self.grow_to_cap(needed);
        }
    }

    #[inline]
    unsafe fn reinsert(&mut self, i: usize) {
        let k = *self.cells.get(i);
        let home = (hash8(k as u64) as usize) & self.mask;
        if home == i {
            return;
        }
        let mut pos = home;
        loop {
            if pos == i {
                return;
            }
            let slot = self.cells.get_mut(pos);
            if *slot == 0 {
                *slot = k;
                *self.cells.get_mut(i) = 0;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    /// Empty the set, KEEPING capacity (group-boundary reset).
    pub fn clear(&mut self) {
        if self.cells.cap != 0 {
            unsafe { std::ptr::write_bytes(self.cells.ptr.as_ptr(), 0, self.cells.cap) };
        }
        self.len = 0;
        self.has_zero = false;
    }

    /// `clear` bounded by the CONTENTS instead of the capacity: `keys` must
    /// be exactly the set's elements (any order; a 0 key lives in `has_zero`,
    /// not a cell). Sparse tables (see CLEAR_SPARSE_FACTOR) probe each key's
    /// cell — collect-then-zero, because zeroing while probing would break
    /// the probe chains of keys not yet visited — dense tables fall back to
    /// the memset `clear`.
    pub fn clear_with_keys(&mut self, keys: &[i64]) {
        if self.cells.cap == 0 {
            self.len = 0;
            self.has_zero = false;
            return;
        }
        if self.len * CLEAR_SPARSE_FACTOR >= self.cells.cap {
            self.clear();
            return;
        }
        debug_assert_eq!(keys.iter().filter(|&&k| k != 0).count(), self.len);
        self.scratch.clear();
        for &k in keys {
            if k == 0 {
                continue;
            }
            let mut pos = (hash8(k as u64) as usize) & self.mask;
            loop {
                let c = unsafe { *self.cells.get(pos) };
                if c == k {
                    self.scratch.push(pos as u32);
                    break;
                }
                if c == 0 {
                    // `keys` disagrees with the table (caller bug): stay
                    // correct via the full sweep.
                    debug_assert!(false, "clear_with_keys: key {k} not in table");
                    self.clear();
                    return;
                }
                pos = (pos + 1) & self.mask;
            }
        }
        for &pos in &self.scratch {
            unsafe { *self.cells.get_mut(pos as usize) = 0 };
        }
        self.len = 0;
        self.has_zero = false;
    }

    pub fn mem_bytes(&self) -> usize {
        self.cells.cap * 8 + self.scratch.capacity() * 4
    }
}

/// Byte-content dedup index over a caller-owned blob. 12-byte cells
/// (caller-supplied 32-bit hash, content offset, content length); the caller
/// guarantees content offsets are nonzero (varlena images carry a 4-byte
/// header, so content never starts at 0) — off == 0 is the empty sentinel.
#[derive(Clone, Copy)]
struct CellD {
    hash: u32,
    off: u32,
    len: u32,
}

pub struct BytesDedup {
    cells: RawCells<CellD>,
    mask: usize,
    len: usize,
    /// `clear_with_entries` position scratch (collect-then-zero; retained).
    scratch: Vec<u32>,
}

impl Default for BytesDedup {
    fn default() -> Self {
        Self::new()
    }
}

impl BytesDedup {
    pub fn new() -> Self {
        BytesDedup { cells: RawCells::new(), mask: 0, len: 0, scratch: Vec::new() }
    }

    /// Probe for `content` (placement + prefilter on the caller's `hash`,
    /// e.g. hash_bytes — the same value the caller stores beside the value
    /// arrays). If absent, record it at `content_off` (where the caller is
    /// about to place the content inside `blob`) and return true. `blob` is
    /// the current backing store for previously recorded offsets.
    /// content_off must be nonzero.
    #[inline(always)]
    pub fn insert(&mut self, hash: u32, content: &[u8], blob: &[u8], content_off: u32) -> bool {
        debug_assert!(content_off != 0);
        if self.cells.cap == 0 {
            self.cells.alloc_zeroed(1 << INITIAL_DEGREE);
            self.mask = self.cells.cap - 1;
        }
        let mut pos = (hash as usize) & self.mask;
        loop {
            let c = unsafe { *self.cells.get(pos) };
            if c.off == 0 {
                unsafe {
                    *self.cells.get_mut(pos) =
                        CellD { hash, off: content_off, len: content.len() as u32 };
                }
                self.len += 1;
                if self.len * 2 > self.cells.cap {
                    self.grow();
                }
                return true;
            }
            if c.hash == hash && c.len as usize == content.len() && unsafe {
                eq_bytes(blob.as_ptr().add(c.off as usize), content.as_ptr(), content.len())
            } {
                return false;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    #[cold]
    fn grow(&mut self) {
        let old_n = self.cells.cap;
        self.cells.grow_double();
        self.mask = self.cells.cap - 1;
        unsafe {
            for i in 0..old_n {
                if self.cells.get(i).off != 0 {
                    self.reinsert(i);
                }
            }
            let mut i = old_n;
            while i < self.cells.cap && self.cells.get(i).off != 0 {
                self.reinsert(i);
                i += 1;
            }
        }
    }

    #[inline]
    unsafe fn reinsert(&mut self, i: usize) {
        let c = *self.cells.get(i);
        let home = (c.hash as usize) & self.mask;
        if home == i {
            return;
        }
        let mut pos = home;
        loop {
            if pos == i {
                return;
            }
            let slot = self.cells.get_mut(pos);
            if slot.off == 0 {
                *slot = c;
                self.cells.get_mut(i).off = 0;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    /// Empty the index, KEEPING capacity.
    pub fn clear(&mut self) {
        if self.cells.cap != 0 {
            unsafe {
                std::ptr::write_bytes(self.cells.ptr.as_ptr() as *mut u8, 0, self.cells.cap * std::mem::size_of::<CellD>())
            };
        }
        self.len = 0;
    }

    /// `clear` bounded by the CONTENTS: `entries` must be exactly the
    /// index's (hash, content offset) pairs, any order (offsets are unique
    /// nonzero — the insert contract). Sparse tables probe each entry's
    /// cell (collect-then-zero, as `IntSet::clear_with_keys`); dense tables
    /// fall back to the memset `clear`.
    pub fn clear_with_entries<I: IntoIterator<Item = (u32, u32)>>(&mut self, entries: I) {
        if self.cells.cap == 0 {
            self.len = 0;
            return;
        }
        if self.len * CLEAR_SPARSE_FACTOR >= self.cells.cap {
            self.clear();
            return;
        }
        self.scratch.clear();
        for (hash, off) in entries {
            debug_assert!(off != 0);
            let mut pos = (hash as usize) & self.mask;
            loop {
                let c = unsafe { *self.cells.get(pos) };
                if c.off == off {
                    self.scratch.push(pos as u32);
                    break;
                }
                if c.off == 0 {
                    // `entries` disagrees with the table (caller bug): stay
                    // correct via the full sweep.
                    debug_assert!(false, "clear_with_entries: offset {off} not in table");
                    self.clear();
                    return;
                }
                pos = (pos + 1) & self.mask;
            }
        }
        debug_assert_eq!(self.scratch.len(), self.len);
        for &pos in &self.scratch {
            unsafe { self.cells.get_mut(pos as usize).off = 0 };
        }
        self.len = 0;
    }

    pub fn mem_bytes(&self) -> usize {
        self.cells.cap * std::mem::size_of::<CellD>() + self.scratch.capacity() * 4
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// F-R1-3 pin: pack8 == the little-endian zero-padded key value, for
    /// every length, with the key at the END of its own heap allocation —
    /// any load past the key is a heap-buffer-overflow under the sanitizer
    /// rig (the disease shape: the old 8-byte load read past short keys).
    #[test]
    fn pack8_matches_zero_padded_le_for_all_lengths() {
        for sz in 1..=8usize {
            let v: Vec<u8> = (1..=sz as u8).collect();
            let boxed: Box<[u8]> = v.clone().into_boxed_slice();
            let mut want = [0u8; 8];
            want[..sz].copy_from_slice(&v);
            assert_eq!(pack8(&boxed), u64::from_le_bytes(want), "sz={sz}");
        }
    }

    /// dedupsub I3: a projection reserve mid-stream must lose nothing (a
    /// duplicate re-insert of every held key returns false), keep dedup
    /// exact afterwards, and be a no-op when the table already fits.
    #[test]
    fn intset_reserve_for_jump_preserves_contents() {
        let mut s = IntSet::new();
        for i in 0..1_000i64 {
            assert!(s.insert(i * 3 - 500));
        }
        let cap_small = s.mem_bytes();
        s.reserve_for(100_000);
        assert!(s.mem_bytes() >= 100_000 * 2 * 8, "one jump to final size");
        for i in 0..1_000i64 {
            assert!(!s.insert(i * 3 - 500), "held key lost by the jump rehash");
        }
        for i in 1_000..100_000i64 {
            assert!(s.insert(i * 3 - 500));
        }
        let cap_big = s.mem_bytes();
        s.reserve_for(50_000); // smaller target: never shrinks, no-op
        assert_eq!(s.mem_bytes(), cap_big);
        assert!(cap_big > cap_small);
        // Zero-key + boundary keys survive reserve like any other.
        let mut z = IntSet::new();
        z.insert(0);
        z.insert(i64::MIN);
        z.reserve_for(4_096);
        assert!(!z.insert(0));
        assert!(!z.insert(i64::MIN));
        // Reserve on a fresh set allocates once, then inserts dedup.
        let mut f = IntSet::new();
        f.reserve_for(10_000);
        assert!(f.insert(7));
        assert!(!f.insert(7));
    }

    fn check_against_reference(keys: &[Vec<u8>]) {
        let mut ours = StringHashMap::new();
        let mut reference: HashMap<Vec<u8>, GroupId> = HashMap::new();
        let mut next = 0u32;
        for k in keys {
            let (id, inserted) = ours.insert_or_get(k);
            let (rid, rins) = match reference.get(k) {
                Some(&r) => (r, false),
                None => {
                    reference.insert(k.clone(), next);
                    next += 1;
                    (next - 1, true)
                }
            };
            assert_eq!((id, inserted), (rid, rins), "key {:?}", k);
        }
        assert_eq!(ours.len(), reference.len());
    }

    #[test]
    fn edge_lengths_and_nuls() {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        keys.push(vec![]); // empty
        for len in 1..=40usize {
            // plain
            keys.push((0..len).map(|i| b'a' + (i % 26) as u8).collect());
            // trailing NUL
            let mut k: Vec<u8> = (0..len.saturating_sub(1)).map(|i| b'x' + (i % 3) as u8).collect();
            k.push(0);
            keys.push(k);
            // embedded NUL, nonzero tail
            if len >= 3 {
                let mut k: Vec<u8> = vec![b'q'; len];
                k[len / 2] = 0;
                keys.push(k);
            }
            // prefix pairs that only differ in length ("ab" vs "ab\0" class)
            keys.push((0..len).map(|_| b'z').collect());
        }
        // repeats, shuffled-ish
        let dup = keys.clone();
        keys.extend(dup);
        check_against_reference(&keys);
    }

    #[test]
    fn random_corpus_grows() {
        // Enough distinct keys to force several growths in every bucket.
        let mut s = 0x243F_6A88_85A3_08D3u64;
        let mut lcg = move || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s
        };
        let mut keys = Vec::new();
        for _ in 0..20000 {
            let len = 1 + (lcg() % 40) as usize;
            let mut k = Vec::with_capacity(len);
            for _ in 0..len {
                k.push((lcg() % 255) as u8); // 0..=254 — NULs possible
            }
            if *k.last().unwrap() == 0 {
                *k.last_mut().unwrap() = 7; // keep some trailing NULs via the dedicated case below
            }
            keys.push(k.clone());
            k.push(0);
            keys.push(k); // and its trailing-NUL sibling
        }
        check_against_reference(&keys);
    }

    #[test]
    fn ext_map_matches_reference_and_offsets() {
        use std::collections::HashMap;
        let mut s = 0x0123_4567_89AB_CDEFu64;
        let mut lcg = move || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s
        };
        let mut keys: Vec<Vec<u8>> = vec![vec![]];
        for _ in 0..30000 {
            let len = (lcg() % 40) as usize;
            let mut k: Vec<u8> = (0..len).map(|_| (lcg() % 256) as u8).collect();
            if let Some(last) = k.last_mut() {
                if lcg() % 4 == 0 {
                    *last = 0; // trailing-NUL class
                }
            }
            keys.push(k);
        }
        let dup = keys.clone();
        keys.extend(dup);
        let mut ours = ExtIdMap::new();
        let mut arena: Vec<u8> = Vec::new();
        let mut reference: HashMap<Vec<u8>, (u32, u32)> = HashMap::new(); // key -> (id, off)
        let mut next = 0u32;
        for k in &keys {
            let (id, inserted, off) = ours.insert_or_get(k, &mut arena);
            match reference.get(k) {
                Some(&(rid, roff)) => {
                    assert_eq!((id, inserted), (rid, false), "hit {k:?}");
                    // long-bucket hits report the original offset
                    if k.len() > 24 || k.last() == Some(&0) {
                        assert_eq!(off, roff);
                    }
                    assert_eq!(ours.find(k, &arena), Some(rid));
                }
                None => {
                    assert!(inserted);
                    assert_eq!(id, next);
                    assert_eq!(&arena[off as usize..off as usize + k.len()], &k[..]);
                    reference.insert(k.clone(), (id, off));
                    next += 1;
                }
            }
        }
        assert_eq!(ours.len(), reference.len());
        // find() misses
        assert_eq!(ours.find(b"definitely-not-present-key-xyzzy-0123456789", &arena), None);
    }

    #[test]
    fn int_set_matches_reference() {
        use std::collections::HashSet;
        let mut s = 0xDEAD_BEEF_1234_5678u64;
        let mut lcg = move || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s
        };
        let mut ours = IntSet::new();
        let mut reference = HashSet::new();
        for _ in 0..300_000 {
            let k = (lcg() % 100_000) as i64 - 50_000; // includes 0 and negatives, heavy dups
            assert_eq!(ours.insert(k), reference.insert(k), "key {k}");
        }
        ours.clear();
        let mut reference2 = HashSet::new();
        for k in -100i64..100 {
            assert_eq!(ours.insert(k), reference2.insert(k));
        }
    }

    #[test]
    fn bytes_dedup_matches_reference() {
        use std::collections::HashSet;
        let mut s = 0x0F0F_1234_5678_9ABCu64;
        let mut lcg = move || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s
        };
        let mut dd = BytesDedup::new();
        let mut blob: Vec<u8> = vec![0u8; 4]; // offset 0 reserved (header discipline)
        let mut reference: HashSet<Vec<u8>> = HashSet::new();
        for _ in 0..100_000 {
            let len = (lcg() % 33) as usize;
            let k: Vec<u8> = (0..len).map(|_| (lcg() % 7) as u8 + b'a').collect();
            let h = {
                // any 32-bit content hash works; simple fnv for the test
                let mut x = 0x811c_9dc5u32;
                for &b in &k {
                    x = (x ^ b as u32).wrapping_mul(16777619);
                }
                x
            };
            let off = blob.len() as u32;
            let inserted = dd.insert(h, &k, &blob, off);
            if inserted {
                blob.extend_from_slice(&k);
            }
            assert_eq!(inserted, reference.insert(k.clone()), "key {k:?}");
        }
    }

    #[test]
    fn int_set_sparse_clear() {
        // Pooled-reuse shape: one big fill inflates capacity, then many tiny
        // fills each clear by contents. Every element must be truly gone
        // after each clear (re-insert returns true), including 0/has_zero.
        let mut s = IntSet::new();
        let mut big: Vec<i64> = (0..10_000).map(|i| i * 2654435761 % 1_000_003 - 500_000).collect();
        big.sort_unstable();
        big.dedup();
        for &k in &big {
            s.insert(k);
        }
        let cap_before = s.mem_bytes();
        s.clear_with_keys(&big); // dense enough or not — must fully empty
        for round in 0..1_000 {
            let keys = [round as i64 + 1, -(round as i64) - 1, 0, round as i64 + 1];
            let mut held: Vec<i64> = Vec::new();
            for &k in &keys {
                if s.insert(k) {
                    held.push(k);
                }
            }
            assert_eq!(held.len(), 3, "round {round}");
            s.clear_with_keys(&held);
        }
        // Capacity retained (clears never shrink).
        assert!(s.mem_bytes() >= cap_before);
        // Everything cleared: the big set re-inserts as all-new.
        for &k in &big {
            assert!(s.insert(k), "key {k} survived a sparse clear");
        }
    }

    #[test]
    fn bytes_dedup_sparse_clear() {
        let hash = |k: &[u8]| -> u32 {
            let mut x = 0x811c_9dc5u32;
            for &b in k {
                x = (x ^ b as u32).wrapping_mul(16777619);
            }
            x
        };
        let mut dd = BytesDedup::new();
        let mut blob: Vec<u8> = vec![0u8; 4];
        // Big fill inflates capacity.
        let mut entries: Vec<(u32, u32)> = Vec::new();
        for i in 0..5_000u32 {
            let k = format!("bigkey-{i}");
            let h = hash(k.as_bytes());
            let off = blob.len() as u32;
            assert!(dd.insert(h, k.as_bytes(), &blob, off));
            blob.extend_from_slice(k.as_bytes());
            entries.push((h, off));
        }
        dd.clear_with_entries(entries.iter().copied());
        // Tiny per-group cycles over the retained big table.
        for round in 0..1_000u32 {
            blob.truncate(4);
            let mut held: Vec<(u32, u32)> = Vec::new();
            for k in [format!("k-{round}"), format!("k2-{round}"), format!("k-{round}")] {
                let h = hash(k.as_bytes());
                let off = blob.len() as u32;
                if dd.insert(h, k.as_bytes(), &blob, off) {
                    blob.extend_from_slice(k.as_bytes());
                    held.push((h, off));
                }
            }
            assert_eq!(held.len(), 2, "round {round}");
            dd.clear_with_entries(held.iter().copied());
        }
        // Everything cleared: the big set re-inserts as all-new.
        blob.truncate(4);
        for i in 0..5_000u32 {
            let k = format!("bigkey-{i}");
            let h = hash(k.as_bytes());
            let off = blob.len() as u32;
            assert!(dd.insert(h, k.as_bytes(), &blob, off), "key {k} survived a sparse clear");
            blob.extend_from_slice(k.as_bytes());
        }
    }

    #[test]
    fn dense_ids() {
        let mut m = StringHashMap::new();
        let (a, _) = m.insert_or_get(b"alpha");
        let (b, _) = m.insert_or_get(b"beta-longer-than-eight");
        let (c, _) = m.insert_or_get(b"");
        let (d, _) = m.insert_or_get(b"this key is definitely longer than twenty-four bytes");
        assert_eq!((a, b, c, d), (0, 1, 2, 3));
        assert_eq!(m.insert_or_get(b"alpha"), (0, false));
        assert_eq!(m.len(), 4);
    }
}
