//! Seam declarations for the space-efficient Bloom filter substrate
//! (`lib/bloomfilter.c`, catalog unit `backend-lib-bloomfilter`).
//!
//! # Safe owned model (re-ported from the SAFE src-idiomatic version)
//!
//! The C `struct bloom_filter` allocates its control object and bitset as a
//! single `palloc0` block using a `FLEXIBLE_ARRAY_MEMBER` and addresses bits
//! with raw pointer arithmetic. The original C-faithful port mirrored that with
//! a `#[repr(C)]` header + zero-length flexible array, a manually
//! `alloc_zeroed`/`dealloc`'d block, and an opaque `*mut BloomFilter` crossing
//! the seam — i.e. raw pointers and `unsafe` throughout.
//!
//! Following the SAFE src-idiomatic port, [`BloomFilter`] is now a real owned
//! value: the three control fields (`k_hash_funcs`, `seed`, `m`) as plain
//! scalars and the bitset as an owned `Vec<u8>`. There is no
//! `FLEXIBLE_ARRAY_MEMBER`, no raw pointer, and no `unsafe`. A live filter
//! crosses the seam by value / by reference and is freed by being dropped (the
//! C `bloom_free`/`pfree` analog). Bit addressing (`hash >> 3` byte index,
//! `hash & 7` bit offset) is unchanged from C; it now indexes a safe slice.
//!
//! The struct fields are owned by this dependency-free seam crate (so a filter
//! can cross the seam by value); the `bloomfilter.c` logic that reads/writes
//! them lives in the owning `backend-lib-bloomfilter` crate, installed from its
//! `init_seams()`. Until then a call panics loudly.

#![allow(non_camel_case_types)]

use types_error::PgResult;

/// `bloom_filter` (`lib/bloomfilter.h`) — a space-efficient Bloom filter.
///
/// The C struct is
///
/// ```text
/// struct bloom_filter
/// {
///     int      k_hash_funcs;   /* number of hash functions */
///     uint64   seed;           /* random seed */
///     uint64   m;              /* number of bits in the bitset (power of 2) */
///     unsigned char bitset[FLEXIBLE_ARRAY_MEMBER]; /* bitset, sized in bytes */
/// };
/// ```
///
/// Here the flexible bitset array becomes an owned `Vec<u8>` and the control
/// fields are plain owned values. Fields are public so the dependency-free
/// owning crate (`backend-lib-bloomfilter`) can construct and mutate the
/// filter; consumers treat it as an opaque owned value (create it, pass it,
/// drop it).
pub struct BloomFilter {
    /// Number of hash functions (an `int` in C).
    pub k_hash_funcs: i32,
    /// Random seed.
    pub seed: u64,
    /// Number of bits in the bitset (always a power of two).
    pub m: u64,
    /// Bitset, sized in bytes (`m / BITS_PER_BYTE` bytes).
    pub bitset: Vec<u8>,
}

seam_core::seam!(
    /// `bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)`
    /// (bloomfilter.c) — create a Bloom filter sized for `total_elems`
    /// elements using up to `bloom_work_mem` KB, salted with `seed`. `Err`
    /// carries the `ereport(ERROR)` for an allocation failure (the C `palloc0`
    /// OOM exit).
    pub fn bloom_create(
        total_elems: i64,
        bloom_work_mem: i32,
        seed: u64,
    ) -> PgResult<BloomFilter>
);

seam_core::seam!(
    /// `bloom_add_element(bloom_filter *filter, unsigned char *elem,
    /// size_t len)` (bloomfilter.c) — add the element `elem` to the filter.
    pub fn bloom_add_element(filter: &mut BloomFilter, elem: &[u8])
);

seam_core::seam!(
    /// `bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
    /// size_t len)` (bloomfilter.c) — `true` iff the element `elem` is
    /// definitely not in the filter (no false negatives; possible false
    /// `false`).
    pub fn bloom_lacks_element(filter: &BloomFilter, elem: &[u8]) -> bool
);

seam_core::seam!(
    /// `bloom_prop_bits_set(bloom_filter *filter)` (bloomfilter.c) — proportion
    /// of bits currently set, expressed as a multiplier of filter size
    /// (generally close to 0.5). Instrumentation only.
    pub fn bloom_prop_bits_set(filter: &BloomFilter) -> f64
);
