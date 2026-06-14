//! `geqo_random.c` — random number generator.
//!
//! In C these read `root->join_search_private->random_state`; idiomatically the
//! [`GeqoPrivateData`](crate::GeqoPrivateData) (which owns the
//! [`pg_prng::PgPrng`]) is passed by `&mut`.

use crate::GeqoPrivateData;

/// `geqo_set_seed(root, seed)` — `pg_prng_fseed(&private->random_state, seed)`.
pub fn geqo_set_seed(private: &mut GeqoPrivateData, seed: f64) {
    private.random_state.seed_from_f64(seed);
}

/// `geqo_rand(root)` — returns a random float in `[0.0, 1.0)`
/// (`pg_prng_double(&private->random_state)`).
pub fn geqo_rand(private: &mut GeqoPrivateData) -> f64 {
    private.random_state.next_f64()
}

/// `geqo_randint(root, upper, lower)` — returns an integer in `[lower, upper]`
/// inclusive. "In current usage, lower is never negative so we can just use
/// pg_prng_uint64_range directly."
pub fn geqo_randint(private: &mut GeqoPrivateData, upper: i32, lower: i32) -> i32 {
    private.random_state.u64_range(lower as u64, upper as u64) as i32
}
