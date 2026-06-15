#![forbid(unsafe_code)]
#![allow(non_snake_case)]
// `clippy::result_large_err`: the allocating constructors return the shared
// `PgResult` (== `Result<_, PgError>`) to model C's `palloc0`/`elog(ERROR, ...)`
// non-local exit faithfully. `PgError`'s size is fixed by the `types-error`
// crate and the un-boxed `PgResult` is the project-wide error contract every
// sibling crate matches; boxing it locally would diverge from those signatures.
#![allow(clippy::result_large_err)]
//! HyperLogLog cardinality estimator.
//!
//! Logic-exact port of `src/backend/lib/hyperloglog.c` from PostgreSQL 18.3 —
//! the approximate cardinality estimator (Hideaki Ohno's implementation), used
//! internally by abbreviated-key sorting in `tuplesort` and by `nodeAgg`'s
//! hash-aggregation spill path to estimate per-partition group cardinality.
//!
//! Every function from `hyperloglog.c` is ported 1:1: [`initHyperLogLog`],
//! [`initHyperLogLogError`], [`addHyperLogLog`], [`estimateHyperLogLog`],
//! [`freeHyperLogLog`], and the file-static helper `rho`. The branch order,
//! constants, the `elog(ERROR, ...)` message text, and the floating-point
//! expressions match the C source exactly so estimates are bit-identical.
//!
//! # Ownership model (the idiomatic difference from C)
//!
//! The C `hyperLogLogState` holds a raw `uint8 *hashesArr` that `initHyperLogLog`
//! `palloc0`s and `freeHyperLogLog` `pfree`s, addressing registers by raw index.
//! This port replaces that raw pointer with an owned [`PgVec<u8>`] register array
//! charged to a [`mcx::MemoryContext`], and the control fields (`registerWidth`,
//! `nRegisters`, `alphaMM`, `arrSize`) as plain owned values. There is no raw
//! pointer, no `extern "C"`, and the whole crate is `#![forbid(unsafe_code)]`.
//! Register addressing (`hash >> k` index, `Max(count, reg)` update) is unchanged
//! from C; it now indexes a safe slice.
//!
//! # Type/ops split (no handle, no registry)
//!
//! The estimator *state* — the C `hyperLogLogState` struct — lives in the
//! foundational [`types_nodes::nodeagg::HyperLogLog`] crate as pure data, so
//! struct holders below this `backend-lib-*` layer (the `nodeAgg` spill path's
//! `HashAggSpill.hll_card`, the varlena abbreviated-key sort state) can hold the
//! counter *by value*, exactly as C holds `hyperLogLogState` inline. The
//! *operations* — `initHyperLogLog`/`addHyperLogLog`/`estimateHyperLogLog`/
//! `freeHyperLogLog` — live here and borrow that struct (`&mut HyperLogLog`).
//! No opaque handle, no process-wide registry, no seam: the consumer holds the
//! real owned struct and calls these functions on it directly.

use backend_utils_error::elog;
use mcx::{Mcx, PgVec};
use types_error::{PgResult, ERROR};
use types_nodes::nodeagg::HyperLogLog;

/// `BITS_PER_BYTE` from `c.h`.
const BITS_PER_BYTE: usize = 8;
/// `POW_2_32` from `hyperloglog.c`.
const POW_2_32: f64 = 4_294_967_296.0;
/// `NEG_POW_2_32` from `hyperloglog.c`.
const NEG_POW_2_32: f64 = -4_294_967_296.0;
/// `sizeof(uint32)` — the hash word width the estimator addresses, in bytes.
const SIZEOF_UINT32: usize = 4;

/// Initialize HyperLogLog track state, by bit width.
///
/// `bwidth` is bit width (so register size will be 2 to the power of bwidth).
/// Must be between 4 and 16 inclusive. `mcx` is the memory context the register
/// array is charged to (the analog of "the current memory context" at the C
/// call site).
///
/// 1:1 with `initHyperLogLog` in `hyperloglog.c`: every field is computed and
/// `hashesArr` is `palloc0`'d into `mcx`.
pub fn initHyperLogLog(mcx: Mcx<'_>, bwidth: u8) -> PgResult<HyperLogLog<'_>> {
    // Transcribed verbatim from C (`if (bwidth < 4 || bwidth > 16)`); the
    // `manual_range_contains` lint would rewrite this to a `RangeInclusive`,
    // which obscures the 1:1 mapping, so it is suppressed here.
    #[allow(clippy::manual_range_contains)]
    if bwidth < 4 || bwidth > 16 {
        elog(ERROR, "bit width must be between 4 and 16 inclusive")?;
        // `elog(ERROR, ...)` is a non-local exit in C; here it returns an
        // `Err`, so the `?` above already propagated. This is unreachable.
        unreachable!("elog(ERROR, ...) returns Err");
    }

    let register_width = bwidth;
    let n_registers: usize = 1usize << bwidth;
    let arr_size = std::mem::size_of::<u8>() * n_registers + 1;

    /*
     * Initialize hashes array to zero, not negative infinity, per discussion
     * of the coupon collector problem in the HyperLogLog paper
     */
    let hashes_arr = zeroed_array(mcx, arr_size)?;

    /*
     * "alpha" is a value that for each possible number of registers (m) is
     * used to correct a systematic multiplicative bias present in m ^ 2 Z (Z
     * is "the indicator function" through which we finally compute E,
     * estimated cardinality).
     */
    let alpha = match n_registers {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / n_registers as f64),
    };

    /*
     * Precalculate alpha m ^ 2, later used to generate "raw" HyperLogLog
     * estimate E
     */
    let alpha_mm = alpha * n_registers as f64 * n_registers as f64;

    Ok(HyperLogLog {
        registerWidth: register_width,
        nRegisters: n_registers,
        alphaMM: alpha_mm,
        hashesArr: hashes_arr,
        arrSize: arr_size,
    })
}

/// Initialize HyperLogLog track state, by error rate.
///
/// Mirrors the C `initHyperLogLogError()` entry point: it finds the lowest
/// `bwidth` for which `e = 1.04 / sqrt(m) < error` (`m = 2^bwidth`), then
/// initializes the counter with it. 1:1 with `initHyperLogLogError`.
pub fn initHyperLogLogError(mcx: Mcx<'_>, error: f64) -> PgResult<HyperLogLog<'_>> {
    let mut bwidth: u8 = 4;

    while bwidth < 16 {
        let m = (1usize << bwidth) as f64;

        if 1.04 / m.sqrt() < error {
            break;
        }
        bwidth += 1;
    }

    initHyperLogLog(mcx, bwidth)
}

/// Adds element to the estimator, from caller-supplied hash.
///
/// 1:1 with `addHyperLogLog` in `hyperloglog.c`. It is critical that the hash
/// value passed be an actual hash value (typically from `hash_any()`): the
/// algorithm relies on a uniform distribution of bits.
pub fn addHyperLogLog(cState: &mut HyperLogLog<'_>, hash: u32) {
    let register_width = cState.registerWidth;

    /* Use the first "k" (registerWidth) bits as a zero based index */
    let index = hash >> (BITS_PER_BYTE * SIZEOF_UINT32 - register_width as usize);

    /* Compute the rank of the remaining 32 - "k" (registerWidth) bits */
    let count = rho(
        hash << register_width,
        (BITS_PER_BYTE * SIZEOF_UINT32 - register_width as usize) as u8,
    );

    let register = &mut cState.hashesArr[index as usize];
    *register = max_u8(count, *register);
}

/// Estimates cardinality, based on elements added so far.
///
/// 1:1 with `estimateHyperLogLog` in `hyperloglog.c`, including the small-/
/// large-range corrections and the order of branches. Uses `pow`/`ln` to
/// mirror C's `pow`/`log`.
pub fn estimateHyperLogLog(cState: &HyperLogLog<'_>) -> f64 {
    let registers = &cState.hashesArr[..cState.nRegisters];
    let mut sum = 0.0;

    for &register in registers {
        sum += 1.0 / 2.0_f64.powf(register as f64);
    }

    /* result set to "raw" HyperLogLog estimate (E in the HyperLogLog paper) */
    let mut result = cState.alphaMM / sum;

    if result <= (5.0 / 2.0) * cState.nRegisters as f64 {
        /* Small range correction */
        let mut zero_count: i32 = 0;

        for &register in registers {
            if register == 0 {
                zero_count += 1;
            }
        }

        if zero_count != 0 {
            result =
                cState.nRegisters as f64 * (cState.nRegisters as f64 / zero_count as f64).ln();
        }
    } else if result > (1.0 / 30.0) * POW_2_32 {
        /* Large range correction */
        result = NEG_POW_2_32 * (1.0 - (result / POW_2_32)).ln();
    }

    result
}

/// Free HyperLogLog track state.
///
/// Mirrors the C `freeHyperLogLog()` entry point. C `Assert`s `hashesArr != NULL`
/// then `pfree`s it; here dropping the value releases the register array's charge
/// to its context.
pub fn freeHyperLogLog(cState: HyperLogLog<'_>) {
    drop(cState)
}

/// Worker for [`addHyperLogLog`].
///
/// Calculates the position of the first set bit in the first `b` bits of `x`,
/// reading from most significant to least significant. 1:1 with the file-static
/// `rho` in `hyperloglog.c`:
///
/// ```text
/// rho(x = 0b1000000000)   returns 1
/// rho(x = 0b0010000000)   returns 3
/// rho(x = 0b0000000000)   returns b + 1
/// ```
///
/// C computes `j = 32 - pg_leftmost_one_pos32(x)`.
fn rho(x: u32, b: u8) -> u8 {
    if x == 0 {
        return b + 1;
    }

    let j: u8 = (32 - pg_leftmost_one_pos32(x)) as u8;

    if j > b {
        return b + 1;
    }

    j
}

/// `pg_leftmost_one_pos32` from `port/pg_bitutils.h`.
///
/// Returns the 0-based position of the most significant set bit in `word`,
/// counting from the least significant bit. `word` must not be zero (matching the
/// C `Assert(word != 0)`); `rho` guarantees `x != 0` before calling. For a
/// non-zero `word`, the MSB position is `31 - leading_zeros`, identical to C's
/// `31 - __builtin_clz(word)`.
#[inline]
fn pg_leftmost_one_pos32(word: u32) -> u32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros()
}

/// Allocate a zero-initialised register array of `bytes` bytes, charged to
/// `mcx`.
///
/// The analog of C's `palloc0(arrSize)`: a contiguous run of zero bytes. The
/// allocation is fallible (`mcx::vec_with_capacity_in` enforces `palloc`'s
/// `MaxAllocSize` gate and surfaces the context's OOM error), exactly where C's
/// `palloc0` would `ereport(ERROR, ...)` on failure.
fn zeroed_array(mcx: Mcx<'_>, bytes: usize) -> PgResult<PgVec<'_, u8>> {
    let mut zeros = mcx::vec_with_capacity_in(mcx, bytes)?;
    zeros.resize(bytes, 0);
    Ok(zeros)
}

/// `Max(a, b)` for `uint8` (C `Max` macro), used by [`addHyperLogLog`].
#[inline]
fn max_u8(a: u8, b: u8) -> u8 {
    if a > b {
        a
    } else {
        b
    }
}

#[cfg(test)]
mod tests;
