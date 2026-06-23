//! Port of `backend/lib/bloomfilter.c` — space-efficient set membership
//! testing (Bloom filter).
//!
//! A Bloom filter is a probabilistic data structure that is used to test an
//! element's membership of a set.  False positives are possible, but false
//! negatives are not; a test of membership of the set returns either "possibly
//! in set" or "definitely not in set".  Elements can be added but not removed.
//!
//! ## Port shape: SAFE owned value (re-ported from the SAFE src-idiomatic)
//!
//! The C `struct bloom_filter` allocates its control object and its bitset as a
//! single `palloc0` block using a `FLEXIBLE_ARRAY_MEMBER` (`bitset[]`), and
//! addresses bits with raw pointer arithmetic.  The previous port of this crate
//! mirrored that byte-for-byte with a `#[repr(C)]` header + zero-length flexible
//! array, a manually `alloc_zeroed`/`dealloc`'d block, and an opaque
//! `*mut BloomFilter` crossing the seam — raw pointers and `unsafe` throughout.
//!
//! This re-port follows the SAFE src-idiomatic version: the filter is the owned
//! [`BloomFilter`] value (owned by `backend-lib-bloomfilter-seams`) holding the
//! three control fields (`k_hash_funcs`, `seed`, `m`) as plain scalars and the
//! bitset as an owned `Vec<u8>`.  There is no `FLEXIBLE_ARRAY_MEMBER`, no raw
//! pointer, and the whole crate is `#![forbid(unsafe_code)]`.  Bit addressing
//! (`hash >> 3` byte index, `hash & 7` bit offset) is unchanged from C; it now
//! indexes a safe slice.
//!
//! ## Memory: palloc0 → fallible Vec allocation surfacing PgError
//!
//! C's `bloom_create()` builds the filter in the caller's current memory
//! context with `palloc0`; the recommended way to free it is `bloom_free()` (a
//! `pfree`), or by destroying the context.  C's `palloc0` reports OOM via
//! `ereport(ERROR, ...)` (a non-local exit).  This port (matching the rbtree
//! re-port convention for this repo) drops the memory-context charge model and
//! uses a plain `Vec<u8>`: the zero-initialised bitset is allocated OOM-safely
//! (`try_reserve_exact`), and on failure a loud [`PgError`] surfaces carrying
//! `ERRCODE_OUT_OF_MEMORY`, exactly where C's `palloc0` would `ereport`.  The
//! filter is freed simply by being dropped (the `bloom_free`/`pfree` analog).
//!
//! ## Hashing / popcount
//!
//! `k_hashes` calls `DatumGetUInt64(hash_any_extended(elem, len, seed))`.
//! `hash_any_extended` is the fmgr-facing wrapper around `hash_bytes_extended`;
//! we route to the ported `hash_bytes_extended` via `common-hashfn-seams`.
//! `bloom_prop_bits_set` calls `pg_popcount`, whose result (whether PostgreSQL
//! takes the `pg_number_of_ones` table path for small buffers or the SIMD path)
//! is identical to summing each byte's population count.

#![forbid(unsafe_code)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// `clippy::result_large_err`: the allocating constructor `bloom_create` returns
// the shared `PgResult` (== `Result<_, PgError>`) to model the C
// `palloc`/`elog(ERROR, ...)` non-local exit faithfully.  `PgError`'s size is
// fixed by the `types-error` crate and the un-boxed `PgResult` is the
// project-wide error contract every sibling crate matches.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::collections::TryReserveError;
use alloc::vec::Vec;

use ::bloomfilter_seams::BloomFilter;
use hashfn_seams as hashfn;
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

/// `MAX_HASH_FUNCS` (bloomfilter.c).
const MAX_HASH_FUNCS: usize = 10;

/// `BITS_PER_BYTE` (c.h).
const BITS_PER_BYTE: u64 = 8;

/// Create Bloom filter in caller's memory context.  We aim for a false positive
/// rate of between 1% and 2% when bitset size is not constrained by memory
/// availability.
///
/// `total_elems` is an estimate of the final size of the set.  It should be
/// approximately correct, but the implementation can cope well with it being
/// off by perhaps a factor of five or more.
///
/// `bloom_work_mem` is sized in KB, in line with the general work_mem
/// convention.  This determines the size of the underlying bitset (trivial
/// bookkeeping space isn't counted).  The bitset is always sized as a power of
/// two number of bits, and the largest possible bitset is 512MB (2^32 bits).
///
/// The Bloom filter is seeded with `seed`.
fn bloom_create(total_elems: i64, bloom_work_mem: i32, seed: u64) -> PgResult<BloomFilter> {
    let mut bitset_bytes: u64;

    /*
     * Aim for two bytes per element; this is sufficient to get a false
     * positive rate below 1%, independent of the size of the bitset or total
     * number of elements.  Also, if rounding down the size of the bitset to
     * the next lowest power of two turns out to be a significant drop, the
     * false positive rate still won't exceed 2% in almost all cases.
     */
    bitset_bytes = u64::min(
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

    /* Allocate bloom filter with unset (zeroed) bitset */
    let bitset = zeroed_bitset(bitset_bytes as usize).map_err(oom)?;

    Ok(BloomFilter {
        k_hash_funcs: optimal_k(bitset_bits, total_elems),
        seed,
        m: bitset_bits,
        bitset,
    })
}

/// Add element to Bloom filter.
fn bloom_add_element(filter: &mut BloomFilter, elem: &[u8]) {
    let hashes = k_hashes(filter.k_hash_funcs, filter.seed, filter.m, elem);

    /* Map a bit-wise address to a byte-wise address + bit offset */
    for &hash in hashes.iter().take(filter.k_hash_funcs as usize) {
        filter.bitset[(hash >> 3) as usize] |= 1 << (hash & 7);
    }
}

/// Test if Bloom filter definitely lacks element.
///
/// Returns true if the element is definitely not in the set of elements
/// observed by [`bloom_add_element`].  Otherwise, returns false, indicating
/// that element is probably present in set.
fn bloom_lacks_element(filter: &BloomFilter, elem: &[u8]) -> bool {
    let hashes = k_hashes(filter.k_hash_funcs, filter.seed, filter.m, elem);

    /* Map a bit-wise address to a byte-wise address + bit offset */
    for &hash in hashes.iter().take(filter.k_hash_funcs as usize) {
        if filter.bitset[(hash >> 3) as usize] & (1 << (hash & 7)) == 0 {
            return true;
        }
    }

    false
}

/// What proportion of bits are currently set?
///
/// Returns proportion, expressed as a multiplier of filter size.  That should
/// generally be close to 0.5, even when we have more than enough memory to
/// ensure a false positive rate within target 1% to 2% band, since more hash
/// functions are used as more memory is available per element.
fn bloom_prop_bits_set(filter: &BloomFilter) -> f64 {
    let bits_set = pg_popcount(&filter.bitset);

    bits_set as f64 / filter.m as f64
}

/// Allocate a zero-initialised bitset of `bytes` bytes.
///
/// The analog of C's `palloc0(offsetof(bloom_filter, bitset) + bitset_bytes)`
/// for the bitset portion: a contiguous run of zero bytes.  Allocated
/// OOM-safely (`try_reserve_exact`) and zero-filled.
fn zeroed_bitset(bytes: usize) -> Result<Vec<u8>, TryReserveError> {
    let mut zeros: Vec<u8> = Vec::new();
    zeros.try_reserve_exact(bytes)?;
    zeros.resize(bytes, 0);
    Ok(zeros)
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
    let k = (core::f64::consts::LN_2 * bitset_bits as f64 / total_elems as f64).round_ties_even()
        as i32;

    i32::max(1, i32::min(k, MAX_HASH_FUNCS as i32))
}

/// Generate k hash values for element.
///
/// Returns an array filled-in with k values determined by hashing the element.
/// Only 2 real independent hash functions are actually used; enhanced double
/// hashing supports up to `MAX_HASH_FUNCS` derived hashes.
fn k_hashes(k_hash_funcs: i32, seed: u64, m: u64, elem: &[u8]) -> [u32; MAX_HASH_FUNCS] {
    let mut hashes = [0u32; MAX_HASH_FUNCS];

    /* Use 64-bit hashing to get two independent 32-bit hashes */
    let hash: u64 = hash_any_extended(elem, seed);
    let mut x = hash as u32;
    let mut y = (hash >> 32) as u32;

    x = mod_m(x, m);
    y = mod_m(y, m);

    /* Accumulate hashes */
    hashes[0] = x;
    let mut i: i32 = 1;
    while i < k_hash_funcs {
        x = mod_m(x.wrapping_add(y), m);
        y = mod_m(y.wrapping_add(i as u32), m);

        hashes[i as usize] = x;
        i += 1;
    }

    hashes
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

/// Translate a `try_reserve` failure into the project's out-of-memory error.
/// The C code relies on `palloc0`'s `ereport(ERROR, ...)` for OOM; here the
/// fallible bitset allocation surfaces as a loud [`PgError`] instead.
fn oom(_e: TryReserveError) -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

// ===========================================================================
// Seam wiring.
// ===========================================================================

/// Install every seam in `backend-lib-bloomfilter-seams`.
///
/// The seam contract carries the owned [`BloomFilter`] value (by value on
/// create, by reference on the operations); the filter is freed by being
/// dropped, so there is no `bloom_free` seam.
pub fn init_seams() {
    use bloomfilter_seams as seam;

    seam::bloom_create::set(bloom_create);
    seam::bloom_add_element::set(bloom_add_element);
    seam::bloom_lacks_element::set(bloom_lacks_element);
    seam::bloom_prop_bits_set::set(bloom_prop_bits_set);
}

#[cfg(test)]
mod tests;
