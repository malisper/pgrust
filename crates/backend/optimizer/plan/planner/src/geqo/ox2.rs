//! `geqo_ox2.c` — order crossover [OX2] (Syswerda). C: `#if defined(OX2)`.
#![allow(dead_code)]

use super::random::geqo_randint;
use super::recombination::City;
use super::{Gene, GeqoState};

/// `ox2(tour1, tour2, offspring, num_gene, city_table)` — order crossover.
pub(super) fn ox2(
    state: &mut GeqoState,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    for k in 1..=num_gene as usize {
        city_table[k].used = 0;
        city_table[k - 1].select_list = -1;
    }

    let num_positions = geqo_randint(state, 2 * num_gene / 3, num_gene / 3);
    for _ in 0..num_positions {
        let pos = geqo_randint(state, num_gene - 1, 0) as usize;
        city_table[pos].select_list = tour1[pos];
        city_table[tour1[pos] as usize].used = 1;
    }

    let mut count = 0;
    let mut k = 0usize;
    while count < num_positions {
        if city_table[k].select_list == -1 {
            let mut j = k + 1;
            // C tests select_list before the bound; the table has num_gene + 1
            // entries so index num_gene is in bounds (C access order preserved).
            while city_table[j].select_list == -1 && j < num_gene as usize {
                j += 1;
            }
            city_table[k].select_list = city_table[j].select_list;
            city_table[j].select_list = -1;
            count += 1;
        } else {
            count += 1;
        }
        k += 1;
    }

    let mut select = 0usize;
    for k in 0..num_gene as usize {
        if city_table[tour2[k] as usize].used != 0 {
            offspring[k] = city_table[select].select_list as Gene;
            select += 1;
        } else {
            offspring[k] = tour2[k];
        }
    }
}
