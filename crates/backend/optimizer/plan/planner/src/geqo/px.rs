//! `geqo_px.c` — position crossover [PX] (Syswerda). C: `#if defined(PX)`.
#![allow(dead_code)]

use super::random::geqo_randint;
use super::recombination::City;
use super::{Gene, GeqoState};

/// `px(tour1, tour2, offspring, num_gene, city_table)` — position crossover.
pub(super) fn px(
    state: &mut GeqoState,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    for i in 1..=num_gene as usize {
        city_table[i].used = 0;
    }

    let num_positions = geqo_randint(state, 2 * num_gene / 3, num_gene / 3);
    for _ in 0..num_positions {
        let pos = geqo_randint(state, num_gene - 1, 0) as usize;
        offspring[pos] = tour1[pos];
        city_table[tour1[pos] as usize].used = 1;
    }

    let mut tour2_index = 0usize;
    let mut offspring_index = 0usize;
    while offspring_index < num_gene as usize {
        if city_table[tour1[offspring_index] as usize].used == 0 {
            if city_table[tour2[tour2_index] as usize].used == 0 {
                offspring[offspring_index] = tour2[tour2_index];
                tour2_index += 1;
                offspring_index += 1;
            } else {
                tour2_index += 1;
            }
        } else {
            offspring_index += 1;
        }
    }
}
