//! `geqo_ox1.c` — order crossover [OX1] (Davis). C: `#if defined(OX1)`.
//! Ported as an always-available function (see crate docs).

use crate::random::geqo_randint;
use crate::recombination::City;
use crate::{Gene, GeqoPrivateData};

/// `ox1(root, tour1, tour2, offspring, num_gene, city_table)` — order
/// crossover (Davis).
pub fn ox1(
    private: &mut GeqoPrivateData,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    /* initialize city table */
    for k in 1..=num_gene as usize {
        city_table[k].used = 0;
    }

    /* select portion to copy from tour1 */
    let mut left = geqo_randint(private, num_gene - 1, 0);
    let mut right = geqo_randint(private, num_gene - 1, 0);

    if left > right {
        let temp = left;
        left = right;
        right = temp;
    }

    /* copy portion from tour1 to offspring */
    for k in left..=right {
        offspring[k as usize] = tour1[k as usize];
        city_table[tour1[k as usize] as usize].used = 1;
    }

    let mut k = (right + 1) % num_gene; /* index into offspring */
    let mut p = k; /* index into tour2 */

    /* copy stuff from tour2 to offspring */
    while k != left {
        if city_table[tour2[p as usize] as usize].used == 0 {
            offspring[k as usize] = tour2[p as usize];
            k = (k + 1) % num_gene;
            city_table[tour2[p as usize] as usize].used = 1;
        }
        p = (p + 1) % num_gene; /* increment tour2-index */
    }
}
