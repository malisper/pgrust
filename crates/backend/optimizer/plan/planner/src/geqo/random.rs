//! geqo_random.c — the run's PRNG (C keeps it in join_search_private).

use super::GeqoState;

pub(super) fn geqo_set_seed(state: &mut GeqoState, seed: f64) {
    state.rng.fseed(seed);
}

pub(super) fn geqo_rand(state: &mut GeqoState) -> f64 {
    state.rng.next_f64()
}

// [lower, upper] inclusive; lower is never negative in GEQO usage.
pub(super) fn geqo_randint(state: &mut GeqoState, upper: i32, lower: i32) -> i32 {
    state.rng.u64_range(lower as u64, upper as u64) as i32
}
