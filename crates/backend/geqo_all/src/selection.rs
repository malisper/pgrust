//! `geqo_selection.c` — linear selection scheme.

use crate::copy::geqo_copy;
use crate::pool::Pool;
use crate::random::geqo_rand;
use crate::{Chromosome, GeqoPrivateData};

/// `geqo_selection(root, momma, daddy, pool, bias)` — select two genes from the
/// pool according to the linear bias, ensuring they differ (unless the pool has
/// only one member), and copy them into `momma`/`daddy`.
pub fn geqo_selection(
    private: &mut GeqoPrivateData,
    momma: &mut Chromosome,
    daddy: &mut Chromosome,
    pool: &Pool,
    bias: f64,
) {
    let first = linear_rand(private, pool.size, bias);
    let mut second = linear_rand(private, pool.size, bias);

    /*
     * Ensure we have selected different genes, except if pool size is only
     * one, when we can't.
     */
    if pool.size > 1 {
        while first == second {
            second = linear_rand(private, pool.size, bias);
        }
    }

    geqo_copy(momma, &pool.data[first as usize], pool.string_length);
    geqo_copy(daddy, &pool.data[second as usize], pool.string_length);
}

/// `linear_rand(root, pool_size, bias)` — generate a random integer in
/// `0 .. pool_size` using the linear bias (`f(x) = bias - 2(bias - 1)x`).
fn linear_rand(private: &mut GeqoPrivateData, pool_size: i32, bias: f64) -> i32 {
    let max = pool_size as f64; /* index between 0 and pool_size */
    let index;

    /*
     * geqo_rand() is not supposed to return 1.0, but if it does then we will
     * get exactly max from this equation, whereas we need 0 <= index < max.
     * Also it seems possible that roundoff error might deliver values
     * slightly outside the range; in particular avoid passing a value
     * slightly less than 0 to sqrt().  If we get a bad value just try again.
     */
    loop {
        let mut sqrtval = (bias * bias) - 4.0 * (bias - 1.0) * geqo_rand(private);
        if sqrtval > 0.0 {
            sqrtval = sqrtval.sqrt();
        }
        let idx = max * (bias - sqrtval) / 2.0 / (bias - 1.0);
        if !(idx < 0.0 || idx >= max) {
            index = idx;
            break;
        }
    }

    index as i32
}
