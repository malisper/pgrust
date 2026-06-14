//! Port of `backend/lib/bloomfilter.c` — space-efficient set membership
//! testing (Bloom filter).
//!
//! A Bloom filter is a probabilistic data structure that is used to test an
//! element's membership of a set.  False positives are possible, but false
//! negatives are not; a test of membership of the set returns either "possibly
//! in set" or "definitely not in set".  Elements can be added but not removed.
//!
//! ## Port shape: C-faithful raw-pointer
//!
//! This crate owns the `backend-lib-bloomfilter-seams` contract, whose
//! `bloom_filter` is opaque (`*mut BloomFilter`).  Consumers (`acl.c` role
//! membership, `brin_bloom.c`) hold and pass that raw pointer and never look
//! inside, exactly as in C (`opacity-inherited-never-introduced`: a real C
//! opaque type, not an invented handle).  We therefore mirror the C struct
//! byte-for-byte:
//!
//! ```text
//! struct bloom_filter
//! {
//!     int           k_hash_funcs;
//!     uint64        seed;
//!     uint64        m;
//!     unsigned char bitset[FLEXIBLE_ARRAY_MEMBER];
//! };
//! ```
//!
//! as a `#[repr(C)]` header with a zero-length `bitset` flexible array member.
//! The whole object (header + `bitset_bytes` of bitset) is allocated as one
//! block via the global allocator — the analog of C's single
//! `palloc0(offsetof(bloom_filter, bitset) + bitset_bytes)` — and freed by
//! `bloom_free` (the analog of `pfree`).  Bit addressing (`hash >> 3` byte
//! index, `hash & 7` bit offset) and all arithmetic match the C exactly.
//!
//! ## Memory: palloc0 → fallible allocation surfacing PgError
//!
//! C's `palloc0` reports OOM via `ereport(ERROR, ...)` (a non-local exit).  The
//! seam contract returns `PgResult<*mut BloomFilter>`, so the single block
//! allocation is performed with the global allocator and a null result is
//! turned into a loud [`PgError`] carrying `ERRCODE_OUT_OF_MEMORY`, exactly
//! where C's `palloc0` would `ereport`.
//!
//! ## Hashing / popcount
//!
//! `k_hashes` calls `DatumGetUInt64(hash_any_extended(elem, len, seed))`.
//! `hash_any_extended` is the fmgr-facing wrapper around `hash_bytes_extended`;
//! we route to the ported `hash_bytes_extended` via `common-hashfn-seams`.
//! `bloom_prop_bits_set` calls `pg_popcount`, whose result (whether PostgreSQL
//! takes the `pg_number_of_ones` table path for small buffers or the SIMD path)
//! is identical to summing each byte's population count.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use core::alloc::Layout;

use backend_lib_bloomfilter_seams::BloomFilter;
use common_hashfn_seams as hashfn;
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

/// `MAX_HASH_FUNCS` (bloomfilter.c).
const MAX_HASH_FUNCS: i32 = 10;

/// `BITS_PER_BYTE` (c.h).
const BITS_PER_BYTE: u64 = 8;

/// `struct bloom_filter` (bloomfilter.c).
///
/// Byte-for-byte mirror of the C struct, with the `FLEXIBLE_ARRAY_MEMBER`
/// `bitset` as a zero-length trailing array.  The allocation backing a
/// `*mut bloom_filter` is `offsetof(bloom_filter, bitset) + bitset_bytes` bytes
/// long; the bitset bytes live immediately after the header.
#[repr(C)]
struct bloom_filter {
    /// K hash functions are used, seeded by caller's seed.
    k_hash_funcs: i32,
    /// Caller's seed.
    seed: u64,
    /// `m` is bitset size, in bits.  Must be a power of two <= 2^32.
    m: u64,
    /// `bitset[FLEXIBLE_ARRAY_MEMBER]`.
    bitset: [u8; 0],
}

/// `offsetof(bloom_filter, bitset)` — start of the flexible bitset within the
/// block.
#[inline]
fn bitset_offset() -> usize {
    // memoffset-free: the field offset is the size of the header up to the
    // (zero-length) flexible array, which `#[repr(C)]` lays out the same way C
    // does.
    let dummy = bloom_filter {
        k_hash_funcs: 0,
        seed: 0,
        m: 0,
        bitset: [],
    };
    // SAFETY: both pointers are derived from the same live object `dummy`.
    let base = &dummy as *const bloom_filter as usize;
    let field = dummy.bitset.as_ptr() as usize;
    field - base
}

/// Allocate (and zero) the single block that backs a `bloom_filter` with
/// `bitset_bytes` of bitset.  The analog of C's `palloc0(offsetof(...) +
/// bitset_bytes)`.  Returns the typed header pointer.
///
/// On allocation failure surfaces the OOM the C `palloc0` would `ereport`.
fn alloc_filter(bitset_bytes: usize) -> PgResult<*mut bloom_filter> {
    let total = bitset_offset() + bitset_bytes;
    let layout = Layout::from_size_align(total, core::mem::align_of::<bloom_filter>())
        .expect("bloom_filter layout");
    // SAFETY: `total` is non-zero (offsetof header > 0), the layout is valid.
    let raw = unsafe { alloc::alloc::alloc_zeroed(layout) };
    if raw.is_null() {
        return Err(PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY));
    }
    Ok(raw as *mut bloom_filter)
}

/// Reconstruct the [`Layout`] used to allocate the block backing `filter`, so
/// `bloom_free` can deallocate it.  `m` bits => `m / BITS_PER_BYTE` bitset
/// bytes (the same value `bloom_create` allocated, since `m` is a power of two
/// multiple of 8).
///
/// # Safety
/// `filter` must point at a live block produced by [`alloc_filter`].
unsafe fn filter_layout(filter: *mut bloom_filter) -> Layout {
    let bitset_bytes = ((*filter).m / BITS_PER_BYTE) as usize;
    let total = bitset_offset() + bitset_bytes;
    Layout::from_size_align(total, core::mem::align_of::<bloom_filter>()).expect("bloom_filter layout")
}

/// Pointer to the first bitset byte of `filter`.
///
/// # Safety
/// `filter` must point at a live block produced by [`alloc_filter`].
#[inline]
unsafe fn bitset_ptr(filter: *mut bloom_filter) -> *mut u8 {
    (filter as *mut u8).add(bitset_offset())
}

/// Create Bloom filter in caller's memory context.  We aim for a false positive
/// rate of between 1% and 2% when bitset size is not constrained by memory
/// availability.
///
/// `total_elems` is an estimate of the final size of the set.  `bloom_work_mem`
/// is sized in KB.  The bitset is always sized as a power of two number of
/// bits, and the largest possible bitset is 512MB (2^32 bits).  The filter is
/// seeded with `seed`.
fn bloom_create(total_elems: i64, bloom_work_mem: i32, seed: u64) -> PgResult<*mut bloom_filter> {
    /*
     * Aim for two bytes per element; this is sufficient to get a false
     * positive rate below 1%, independent of the size of the bitset or total
     * number of elements.  Also, if rounding down the size of the bitset to
     * the next lowest power of two turns out to be a significant drop, the
     * false positive rate still won't exceed 2% in almost all cases.
     */
    let mut bitset_bytes: u64 = u64::min(
        (bloom_work_mem as u64).wrapping_mul(1024),
        (total_elems.wrapping_mul(2)) as u64,
    );
    bitset_bytes = u64::max(1024 * 1024, bitset_bytes);

    /*
     * Size in bits should be the highest power of two <= target.  bitset_bits
     * is uint64 because PG_UINT32_MAX is 2^32 - 1, not 2^32
     */
    let bloom_power = my_bloom_power(bitset_bytes.wrapping_mul(BITS_PER_BYTE));
    let bitset_bits: u64 = 1u64 << bloom_power;
    bitset_bytes = bitset_bits / BITS_PER_BYTE;

    /* Allocate bloom filter with unset bitset */
    let filter = alloc_filter(bitset_bytes as usize)?;
    // SAFETY: `filter` is a freshly allocated, zeroed, correctly sized block.
    unsafe {
        (*filter).k_hash_funcs = optimal_k(bitset_bits, total_elems);
        (*filter).seed = seed;
        (*filter).m = bitset_bits;
    }

    Ok(filter)
}

/// Free Bloom filter.
///
/// # Safety
/// `filter` must point at a live block produced by [`bloom_create`].
unsafe fn bloom_free(filter: *mut bloom_filter) {
    let layout = filter_layout(filter);
    alloc::alloc::dealloc(filter as *mut u8, layout);
}

/// Add element to Bloom filter.
///
/// # Safety
/// `filter` must be live; `elem`/`len` describe a readable byte slice.
unsafe fn bloom_add_element(filter: *mut bloom_filter, elem: *const u8, len: usize) {
    let mut hashes = [0u32; MAX_HASH_FUNCS as usize];

    k_hashes(filter, &mut hashes, elem, len);

    let bitset = bitset_ptr(filter);
    /* Map a bit-wise address to a byte-wise address + bit offset */
    let k = (*filter).k_hash_funcs;
    let mut i = 0;
    while i < k {
        let h = hashes[i as usize];
        let byte = bitset.add((h >> 3) as usize);
        *byte |= 1u8 << (h & 7);
        i += 1;
    }
}

/// Test if Bloom filter definitely lacks element.
///
/// Returns true if the element is definitely not in the set of elements
/// observed by [`bloom_add_element`].  Otherwise, returns false, indicating
/// that element is probably present in set.
///
/// # Safety
/// `filter` must be live; `elem`/`len` describe a readable byte slice.
unsafe fn bloom_lacks_element(filter: *mut bloom_filter, elem: *const u8, len: usize) -> bool {
    let mut hashes = [0u32; MAX_HASH_FUNCS as usize];

    k_hashes(filter, &mut hashes, elem, len);

    let bitset = bitset_ptr(filter);
    /* Map a bit-wise address to a byte-wise address + bit offset */
    let k = (*filter).k_hash_funcs;
    let mut i = 0;
    while i < k {
        let h = hashes[i as usize];
        let byte = *bitset.add((h >> 3) as usize);
        if byte & (1u8 << (h & 7)) == 0 {
            return true;
        }
        i += 1;
    }

    false
}

/// What proportion of bits are currently set?
///
/// Returns proportion, expressed as a multiplier of filter size.  That should
/// generally be close to 0.5.
///
/// # Safety
/// `filter` must point at a live block produced by [`bloom_create`].
unsafe fn bloom_prop_bits_set(filter: *mut bloom_filter) -> f64 {
    let bitset_bytes = ((*filter).m / BITS_PER_BYTE) as i32;
    // SAFETY: `bitset_ptr(filter)` is the start of `bitset_bytes` live bytes.
    let buf = core::slice::from_raw_parts(bitset_ptr(filter), bitset_bytes as usize);
    let bits_set = pg_popcount(buf);

    bits_set as f64 / (*filter).m as f64
}

/// Which element in the sequence of powers of two is less than or equal to
/// `target_bitset_bits`?
///
/// Bitset is never allowed to exceed 2^32 bits (512MB); this is sufficient for
/// all current callers and lets us use 32-bit hash functions.
fn my_bloom_power(mut target_bitset_bits: u64) -> i32 {
    let mut bloom_power: i32 = -1;

    while target_bitset_bits > 0 && bloom_power < 32 {
        bloom_power += 1;
        target_bitset_bits >>= 1;
    }

    bloom_power
}

/// Determine optimal number of hash functions based on size of filter in bits,
/// and projected total number of elements.  The optimal number is the number
/// that minimizes the false positive rate.
fn optimal_k(bitset_bits: u64, total_elems: i64) -> i32 {
    // C: `(int) rint(log(2.0) * bitset_bits / total_elems)`.  `rint` rounds to
    // nearest, ties to even (default FP environment).
    let k = rint(core::f64::consts::LN_2 * bitset_bits as f64 / total_elems as f64) as i32;

    i32::max(1, i32::min(k, MAX_HASH_FUNCS))
}

/// Generate k hash values for element.
///
/// Caller passes array, which is filled-in with k values determined by hashing
/// caller's element.  Only 2 real independent hash functions are actually used;
/// enhanced double hashing supports up to `MAX_HASH_FUNCS` derived hashes.
///
/// # Safety
/// `filter` must be live; `elem`/`len` describe a readable byte slice;
/// `hashes` has at least `MAX_HASH_FUNCS` entries.
unsafe fn k_hashes(
    filter: *mut bloom_filter,
    hashes: &mut [u32; MAX_HASH_FUNCS as usize],
    elem: *const u8,
    len: usize,
) {
    /* Use 64-bit hashing to get two independent 32-bit hashes */
    let elem_slice = if len == 0 {
        &[][..]
    } else {
        core::slice::from_raw_parts(elem, len)
    };
    let hash: u64 = hash_any_extended(elem_slice, (*filter).seed);
    let mut x = hash as u32;
    let mut y = (hash >> 32) as u32;
    let m = (*filter).m;

    x = mod_m(x, m);
    y = mod_m(y, m);

    /* Accumulate hashes */
    hashes[0] = x;
    let k = (*filter).k_hash_funcs;
    let mut i = 1;
    while i < k {
        x = mod_m(x.wrapping_add(y), m);
        y = mod_m(y.wrapping_add(i as u32), m);

        hashes[i as usize] = x;
        i += 1;
    }
}

/// Calculate "val MOD m" inexpensively.
///
/// Assumes that m (which is bitset size) is a power of two.
#[inline]
fn mod_m(val: u32, m: u64) -> u32 {
    debug_assert!(m <= u32::MAX as u64 + 1);
    debug_assert!(((m - 1) & m) == 0);

    (val as u64 & m.wrapping_sub(1)) as u32
}

/// `DatumGetUInt64(hash_any_extended(elem, len, seed))` (bloomfilter.c).
///
/// `hash_any_extended` is the fmgr wrapper around `hash_bytes_extended`; we
/// route straight to the ported `hash_bytes_extended` (identical 64-bit Bob
/// Jenkins hash) through `common-hashfn-seams`.
#[inline]
fn hash_any_extended(elem: &[u8], seed: u64) -> u64 {
    hashfn::hash_bytes_extended::call(elem, seed)
}

/// `pg_popcount(buf, bytes)` (port/pg_bitutils.h): count of set bits across the
/// buffer.  Both the `pg_number_of_ones` table path and the SIMD path produce
/// this value.
#[inline]
fn pg_popcount(buf: &[u8]) -> u64 {
    buf.iter().map(|b| b.count_ones() as u64).sum()
}

/// `rint(x)` — round to nearest integer, ties to even (libm semantics).
#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

// ===========================================================================
// Seam wiring.
// ===========================================================================

/// Install every seam in `backend-lib-bloomfilter-seams`.
///
/// The seam contract carries the opaque `*mut BloomFilter`; we cast our real
/// `*mut bloom_filter` block pointer to/from it (the same allocation, just the
/// opaque view the consumer holds).
pub fn init_seams() {
    use backend_lib_bloomfilter_seams as seam;

    seam::bloom_create::set(|total_elems, bloom_work_mem, seed| {
        bloom_create(total_elems, bloom_work_mem, seed).map(|f| f as *mut BloomFilter)
    });
    seam::bloom_free::set(|filter| {
        // SAFETY: `filter` was produced by `bloom_create` above.
        unsafe { bloom_free(filter as *mut bloom_filter) }
    });
    seam::bloom_add_element::set(|filter, elem, len| {
        // SAFETY: contract: live filter + readable `elem`/`len`.
        unsafe { bloom_add_element(filter as *mut bloom_filter, elem, len) }
    });
    seam::bloom_lacks_element::set(|filter, elem, len| {
        // SAFETY: contract: live filter + readable `elem`/`len`.
        unsafe { bloom_lacks_element(filter as *mut bloom_filter, elem, len) }
    });
    seam::bloom_prop_bits_set::set(|filter| {
        // SAFETY: `filter` was produced by `bloom_create` above.
        unsafe { bloom_prop_bits_set(filter as *mut bloom_filter) }
    });
}

#[cfg(test)]
mod tests;
