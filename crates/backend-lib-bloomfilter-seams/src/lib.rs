//! Seam declarations for the space-efficient Bloom filter substrate
//! (`lib/bloomfilter.c`, catalog unit `backend-lib-bloomfilter`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. `bloom_filter` is an opaque struct whose layout
//! lives in `bloomfilter.c`, so a live filter crosses the seam as the raw
//! `*mut BloomFilter` pointer the C code holds — never dereferenced by
//! consumers (`opacity-inherited-never-introduced`: a real C opaque type, not
//! an invented handle).

#![allow(non_camel_case_types)]

use types_error::PgResult;

/// `bloom_filter` (`lib/bloomfilter.h`) — opaque Bloom filter. The internals
/// are owned by the `bloomfilter.c` substrate; consumers only hold and pass
/// the pointer, so the body stays opaque.
#[repr(C)]
pub struct BloomFilter {
    _private: [u8; 0],
}

seam_core::seam!(
    /// `bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)`
    /// (bloomfilter.c) — create a Bloom filter sized for `total_elems`
    /// elements using up to `bloom_work_mem` KB, salted with `seed`. `Err`
    /// carries the `ereport(ERROR)` for an allocation failure.
    pub fn bloom_create(
        total_elems: i64,
        bloom_work_mem: i32,
        seed: u64,
    ) -> PgResult<*mut BloomFilter>
);

seam_core::seam!(
    /// `bloom_free(bloom_filter *filter)` (bloomfilter.c) — free a filter
    /// created by [`bloom_create`].
    pub fn bloom_free(filter: *mut BloomFilter)
);

seam_core::seam!(
    /// `bloom_add_element(bloom_filter *filter, unsigned char *elem,
    /// size_t len)` (bloomfilter.c) — add the `len`-byte element `elem` to the
    /// filter.
    pub fn bloom_add_element(filter: *mut BloomFilter, elem: *const u8, len: usize)
);

seam_core::seam!(
    /// `bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
    /// size_t len)` (bloomfilter.c) — `true` iff the `len`-byte element `elem`
    /// is definitely not in the filter (no false negatives; possible false
    /// `false`).
    pub fn bloom_lacks_element(
        filter: *mut BloomFilter,
        elem: *const u8,
        len: usize,
    ) -> bool
);

seam_core::seam!(
    /// `bloom_prop_bits_set(bloom_filter *filter)` (bloomfilter.c) — proportion
    /// of bits currently set, expressed as a multiplier of filter size
    /// (generally close to 0.5). Instrumentation only.
    pub fn bloom_prop_bits_set(filter: *mut BloomFilter) -> f64
);
