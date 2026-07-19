//! Weighted choice over INTEGER cumulative weights (determinism law A3: no
//! float accumulation in choice arithmetic).

use rand::Rng;
use rand::RngCore;

/// Pick an index with probability weight[i]/sum. `None` if all weights are 0.
pub fn weighted_index(rng: &mut dyn RngCore, weights: &[u64]) -> Option<usize> {
    let total: u64 = weights.iter().sum();
    if total == 0 {
        return None;
    }
    let mut pick = rng.gen_range(0..total);
    for (i, &w) in weights.iter().enumerate() {
        if pick < w {
            return Some(i);
        }
        pick -= w;
    }
    unreachable!("cumulative weights exhausted")
}

/// Uniform draw in [lo, hi] inclusive.
pub fn range_incl(rng: &mut dyn RngCore, lo: u64, hi: u64) -> u64 {
    debug_assert!(lo <= hi);
    rng.gen_range(lo..=hi)
}
