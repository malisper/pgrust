//! Relation block sampling routines (`src/backend/utils/misc/sampling.c`).
//!
//! Block-level sampling (Knuth's Algorithm S), reservoir sampling (Vitter's
//! Algorithm Z), and the deprecated `anl_*` API that shares one per-backend
//! reservoir state across callers (thread-local here).
//!
//! mcx note: `sampling.c` never pallocs — both state structs are pure
//! scalars, caller-placed, so nothing here takes an `Mcx` handle or carries
//! a context lifetime.

#![allow(non_snake_case)]

use std::cell::RefCell;

use ::prng::{global_prng, PgPrng};
use ::types_core::{uint32, BlockNumber};

/// State for block-level sampling Algorithm S (Knuth 3.4.2)
/// (`utils/sampling.h` `BlockSamplerData`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BlockSamplerData {
    /// number of blocks, known in advance
    pub N: BlockNumber,
    /// desired sample size
    pub n: i32,
    /// current block number
    pub t: BlockNumber,
    /// blocks selected so far
    pub m: i32,
    /// random generator state
    pub randstate: PgPrng,
}

/// State for reservoir sampling Algorithm Z (Vitter 1985)
/// (`utils/sampling.h` `ReservoirStateData`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ReservoirStateData {
    pub W: f64,
    /// random generator state
    pub randstate: PgPrng,
}

/// `BlockSampler_Init` -- prepare for random sampling of blocknumbers.
///
/// BlockSampler provides algorithm for block level sampling of a relation
/// as discussed on pgsql-hackers 2004-04-02 (subject "Large DB").
/// It selects a random sample of `samplesize` blocks out of the `nblocks`
/// blocks in the table. If the table has less than `samplesize` blocks, all
/// blocks are selected.
///
/// Since we know the total number of blocks in advance, we can use the
/// straightforward Algorithm S from Knuth 3.4.2, rather than Vitter's
/// algorithm.
///
/// Returns the number of blocks that [`BlockSampler_Next`] will return.
pub fn BlockSampler_Init(
    bs: &mut BlockSamplerData,
    nblocks: BlockNumber,
    samplesize: i32,
    randseed: uint32,
) -> BlockNumber {
    bs.N = nblocks; /* measured table size */

    // If we decide to reduce samplesize for tables that have less or not much
    // more than samplesize blocks, here is the place to do it.
    bs.n = samplesize;
    bs.t = 0; /* blocks scanned so far */
    bs.m = 0; /* blocks selected so far */

    sampler_random_init_state(randseed, &mut bs.randstate);

    // Min(bs->n, bs->N): the comparison is done in BlockNumber (uint32) space,
    // matching the implicit C conversion in the Min() use site.
    (bs.n as BlockNumber).min(bs.N)
}

pub fn BlockSampler_HasMore(bs: &BlockSamplerData) -> bool {
    (bs.t < bs.N) && (bs.m < bs.n)
}

pub fn BlockSampler_Next(bs: &mut BlockSamplerData) -> BlockNumber {
    let mut K: BlockNumber = bs.N.wrapping_sub(bs.t); /* remaining blocks */
    let k: i32 = bs.n - bs.m; /* blocks still to sample */

    // hence K > 0 and k > 0
    debug_assert!(BlockSampler_HasMore(bs));

    if (k as BlockNumber) >= K {
        // need all the rest
        bs.m += 1;
        let block = bs.t;
        bs.t = bs.t.wrapping_add(1);
        return block;
    }

    // It is not obvious that this code matches Knuth's Algorithm S.
    // Knuth says to skip the current block with probability 1 - k/K.
    // If we are to skip, we should advance t (hence decrease K), and
    // repeat the same probabilistic test for the next block.  The naive
    // implementation thus requires a sampler_random_fract() call for each
    // block number.  But we can reduce this to one sampler_random_fract()
    // call per selected block, by noting that each time the while-test
    // succeeds, we can reinterpret V as a uniform random number in the range
    // 0 to p. Therefore, instead of choosing a new V, we just adjust p to be
    // the appropriate fraction of its former value, and our next loop
    // makes the appropriate probabilistic test.
    //
    // We have initially K > k > 0.  If the loop reduces K to equal k,
    // the next while-test must fail since p will become exactly zero
    // (we assume there will not be roundoff error in the division).
    // (Note: Knuth suggests a "<=" loop condition, but we use "<" just
    // to be doubly sure about roundoff error.)  Therefore K cannot become
    // less than k, which means that we cannot fail to select enough blocks.
    let V = sampler_random_fract(&mut bs.randstate);
    let mut p = 1.0 - (k as f64) / (K as f64);
    while V < p {
        // skip
        bs.t = bs.t.wrapping_add(1);
        K = K.wrapping_sub(1); /* keep K == N - t */

        // adjust p to be new cutoff point in reduced range
        p *= 1.0 - (k as f64) / (K as f64);
    }

    // select
    bs.m += 1;
    let block = bs.t;
    bs.t = bs.t.wrapping_add(1);
    block
}

// These two routines embody Algorithm Z from "Random sampling with a
// reservoir" by Jeffrey S. Vitter, in ACM Trans. Math. Softw. 11, 1
// (Mar. 1985), Pages 37-57.  Vitter describes his algorithm in terms
// of the count S of records to skip before processing another record.
// It is computed primarily based on t, the number of records already read.
// The only extra state needed between calls is W, a random state variable.
//
// reservoir_init_selection_state computes the initial W value.
//
// Given that we've already read t records (t >= n), reservoir_get_next_S
// determines the number of records to skip before the next record is
// processed.

pub fn reservoir_init_selection_state(rs: &mut ReservoirStateData, n: i32) {
    // Reservoir sampling is not used anywhere where it would need to return
    // repeatable results so we can initialize it randomly.
    let seed = global_prng(PgPrng::next_u32);
    sampler_random_init_state(seed, &mut rs.randstate);

    // Initial value of W (for use when Algorithm Z is first applied)
    rs.W = (-sampler_random_fract(&mut rs.randstate).ln() / n as f64).exp();
}

pub fn reservoir_get_next_S(rs: &mut ReservoirStateData, mut t: f64, n: i32) -> f64 {
    let S;

    // The magic constant here is T from Vitter's paper
    if t <= (22.0 * n as f64) {
        // Process records using Algorithm X until t is large enough
        let V = sampler_random_fract(&mut rs.randstate); /* Generate V */
        let mut s = 0.0;
        t += 1.0;
        // Note: "num" in Vitter's code is always equal to t - n
        let mut quot = (t - n as f64) / t;
        // Find min S satisfying (4.1)
        while quot > V {
            s += 1.0;
            t += 1.0;
            quot *= (t - n as f64) / t;
        }
        S = s;
    } else {
        // Now apply Algorithm Z
        let mut W = rs.W;
        let term = t - n as f64 + 1.0;

        loop {
            // Generate U and X
            let U = sampler_random_fract(&mut rs.randstate);
            let X = t * (W - 1.0);
            let s = X.floor(); /* S is tentatively set to floor(X) */
            // Test if U <= h(S)/cg(X) in the manner of (6.3)
            let tmp = (t + 1.0) / term;
            let lhs = ((((U * tmp * tmp) * (term + s)) / (t + X)).ln() / n as f64).exp();
            let rhs = (((t + X) / (term + s)) * term) / t;
            if lhs <= rhs {
                W = rhs / lhs;
                S = s;
                break;
            }
            // Test if U <= f(S)/cg(X)
            let mut y = (((U * (t + 1.0)) / term) * (t + s + 1.0)) / (t + X);
            let (mut denom, numer_lim) = if (n as f64) < s {
                (t, term + s)
            } else {
                (t - n as f64 + s, t + 1.0)
            };
            let mut numer = t + s;
            while numer >= numer_lim {
                y *= numer / denom;
                denom -= 1.0;
                numer -= 1.0;
            }
            // Generate W in advance
            W = (-sampler_random_fract(&mut rs.randstate).ln() / n as f64).exp();
            if (y.ln() / n as f64).exp() <= (t + X) / t {
                S = s;
                break;
            }
        }

        rs.W = W;
    }

    S
}

// ----------------------------------------------------------------------------
// Random number generator used by sampling
// ----------------------------------------------------------------------------

pub fn sampler_random_init_state(seed: uint32, randstate: &mut PgPrng) {
    randstate.seed(seed as u64);
}

/// Select a random value R uniformly distributed in (0 - 1).
pub fn sampler_random_fract(randstate: &mut PgPrng) -> f64 {
    // pg_prng_double returns a value in [0.0 - 1.0), so we must reject 0.0
    loop {
        let res = randstate.next_f64();
        if res != 0.0 {
            return res;
        }
    }
}

// ----------------------------------------------------------------------------
// Backwards-compatible API for block sampling.
//
// This code is now deprecated, but since it's still in use by many FDWs, we
// should keep it for awhile at least.  The functionality is the same as
// sampler_random_fract/reservoir_init_selection_state/reservoir_get_next_S,
// except that a common random state is used across all callers.
// ----------------------------------------------------------------------------

thread_local! {
    /// `(oldrs, oldrs_initialized)` — the reservoir state shared across the
    /// deprecated `anl_*` API. The boolean records whether the random state
    /// has been seeded. C's `static ReservoirStateData oldrs` is per-backend
    /// state, so it is thread-local here, never a shared static.
    static OLD_RESERVOIR_STATE: RefCell<(ReservoirStateData, bool)> = const {
        RefCell::new((
            ReservoirStateData {
                W: 0.0,
                randstate: PgPrng::from_raw(0, 0),
            },
            false,
        ))
    };
}

pub fn anl_random_fract() -> f64 {
    // and compute a random fraction
    with_old_reservoir_state(|rs| sampler_random_fract(&mut rs.randstate))
}

pub fn anl_init_selection_state(n: i32) -> f64 {
    // Initial value of W (for use when Algorithm Z is first applied)
    with_old_reservoir_state(|rs| (-sampler_random_fract(&mut rs.randstate).ln() / n as f64).exp())
}

pub fn anl_get_next_S(t: f64, n: i32, stateptr: &mut f64) -> f64 {
    // Note: unlike anl_random_fract/anl_init_selection_state, the C function
    // has no oldrs_initialized guard -- it uses oldrs.randstate as-is.
    OLD_RESERVOIR_STATE.with_borrow_mut(|state| {
        let rs = &mut state.0;

        rs.W = *stateptr;
        let result = reservoir_get_next_S(rs, t, n);
        *stateptr = rs.W;
        result
    })
}

/// Run `f` against the shared `oldrs` reservoir state, initializing the random
/// state on first use (the C `oldrs_initialized` guard).
fn with_old_reservoir_state<R>(f: impl FnOnce(&mut ReservoirStateData) -> R) -> R {
    OLD_RESERVOIR_STATE.with_borrow_mut(|state| {
        // initialize if first time through
        if !state.1 {
            let seed = global_prng(PgPrng::next_u32);
            sampler_random_init_state(seed, &mut state.0.randstate);
            state.1 = true;
        }

        f(&mut state.0)
    })
}

/// This crate declares no inward seams; nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_sampler_returns_all_blocks_when_sample_is_larger_than_relation() {
        let mut sampler = BlockSamplerData::default();

        assert_eq!(BlockSampler_Init(&mut sampler, 3, 10, 7), 3);

        let mut blocks = Vec::new();
        while BlockSampler_HasMore(&sampler) {
            blocks.push(BlockSampler_Next(&mut sampler));
        }

        assert_eq!(blocks, vec![0, 1, 2]);
        assert_eq!(sampler.t, 3);
        assert_eq!(sampler.m, 3);
    }

    #[test]
    fn block_sampler_is_deterministic_for_seed() {
        let mut first = BlockSamplerData::default();
        let mut second = BlockSamplerData::default();
        BlockSampler_Init(&mut first, 100, 10, 42);
        BlockSampler_Init(&mut second, 100, 10, 42);

        let mut first_blocks = Vec::new();
        let mut second_blocks = Vec::new();

        while BlockSampler_HasMore(&first) {
            first_blocks.push(BlockSampler_Next(&mut first));
        }
        while BlockSampler_HasMore(&second) {
            second_blocks.push(BlockSampler_Next(&mut second));
        }

        assert_eq!(first_blocks, second_blocks);
        assert_eq!(first_blocks.len(), 10);
        assert!(first_blocks.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn sampler_random_fract_never_returns_zero_for_seeded_state() {
        let mut state = PgPrng::seeded(0);

        for _ in 0..100 {
            let value = sampler_random_fract(&mut state);
            assert!(value > 0.0);
            assert!(value < 1.0);
        }
    }

    #[test]
    fn reservoir_sampling_updates_state() {
        let mut state = ReservoirStateData::default();
        sampler_random_init_state(123, &mut state.randstate);
        state.W = (-sampler_random_fract(&mut state.randstate).ln() / 10.0).exp();

        let before = state.W;
        let skips = reservoir_get_next_S(&mut state, 1_000.0, 10);

        assert!(skips >= 0.0);
        assert_ne!(state.W, before);
    }

    #[test]
    fn reservoir_init_selection_state_seeds_and_sets_w() {
        let mut state = ReservoirStateData::default();
        reservoir_init_selection_state(&mut state, 10);

        // W is exp(-ln(fract)/n) for fract in (0,1), so W > 1.
        assert!(state.W > 1.0);
    }

    #[test]
    fn anl_api_round_trips_state() {
        // anl_init_selection_state computes the initial W; the value must be
        // usable as the stateptr to anl_get_next_S without panicking.
        let mut w = anl_init_selection_state(10);
        assert!(w > 1.0);

        let skips = anl_get_next_S(1_000.0, 10, &mut w);
        assert!(skips >= 0.0);

        let fract = anl_random_fract();
        assert!(fract > 0.0 && fract < 1.0);
    }

    #[test]
    fn anl_state_is_per_thread() {
        // The deprecated oldrs state is per-backend in C; here per-thread.
        // Seeding it on this thread must not mark it initialized (or leak
        // PRNG position) on any other thread.
        let _ = anl_init_selection_state(10);
        assert!(OLD_RESERVOIR_STATE.with_borrow(|s| s.1));

        std::thread::spawn(|| {
            assert!(
                OLD_RESERVOIR_STATE.with_borrow(|s| !s.1),
                "fresh thread saw another thread's oldrs initialization"
            );
            // First use on this thread runs its own seeding path.
            let w = anl_init_selection_state(10);
            assert!(w > 1.0);
        })
        .join()
        .unwrap();
    }
}
