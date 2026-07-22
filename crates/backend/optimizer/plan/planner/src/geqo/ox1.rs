//! `geqo_ox1.c` — order crossover [OX1] (Davis). C: `#if defined(OX1)`.
#![allow(dead_code)]

use super::random::geqo_randint;
use super::recombination::City;
use super::{Gene, GeqoState};

/// `ox1(tour1, tour2, offspring, num_gene, city_table)` — order crossover.
pub(super) fn ox1(
    state: &mut GeqoState,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    for k in 1..=num_gene as usize {
        city_table[k].used = 0;
    }

    let mut left = geqo_randint(state, num_gene - 1, 0);
    let mut right = geqo_randint(state, num_gene - 1, 0);
    if left > right {
        core::mem::swap(&mut left, &mut right);
    }

    for k in left..=right {
        offspring[k as usize] = tour1[k as usize];
        city_table[tour1[k as usize] as usize].used = 1;
    }

    let mut k = (right + 1) % num_gene;
    let mut p = k;
    while k != left {
        if city_table[tour2[p as usize] as usize].used == 0 {
            offspring[k as usize] = tour2[p as usize];
            k = (k + 1) % num_gene;
            city_table[tour2[p as usize] as usize].used = 1;
        }
        p = (p + 1) % num_gene;
    }
}
