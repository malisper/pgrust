//! geqo_selection.c — linear-bias selection of two parents.

use super::copy::geqo_copy;
use super::random::geqo_rand;
use super::{Chromosome, GeqoState, Pool};

pub(super) fn geqo_selection(
    state: &mut GeqoState,
    momma: &mut Chromosome,
    daddy: &mut Chromosome,
    pool: &Pool,
    bias: f64,
) {
    let first = linear_rand(state, pool.size, bias);
    let mut second = linear_rand(state, pool.size, bias);
    // Ensure distinct parents unless the pool has a single member.
    if pool.size > 1 {
        while first == second {
            second = linear_rand(state, pool.size, bias);
        }
    }
    geqo_copy(momma, &pool.data[first as usize], pool.string_length);
    geqo_copy(daddy, &pool.data[second as usize], pool.string_length);
}

// f(x) = bias - 2(bias - 1)x; retry on a value outside [0, max) (roundoff or a
// geqo_rand() of exactly 1.0).
fn linear_rand(state: &mut GeqoState, pool_size: i32, bias: f64) -> i32 {
    let max = pool_size as f64;
    loop {
        let mut sqrtval = (bias * bias) - 4.0 * (bias - 1.0) * geqo_rand(state);
        if sqrtval > 0.0 {
            sqrtval = sqrtval.sqrt();
        }
        let idx = max * (bias - sqrtval) / 2.0 / (bias - 1.0);
        if !(idx < 0.0 || idx >= max) {
            return idx as i32;
        }
    }
}
