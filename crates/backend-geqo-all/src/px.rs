//! `geqo_px.c` — position crossover [PX] (Syswerda). C: `#if defined(PX)`.
//! Ported as an always-available function (see crate docs).

use crate::random::geqo_randint;
use crate::recombination::City;
use crate::{Gene, GeqoPrivateData};

/// `px(root, tour1, tour2, offspring, num_gene, city_table)` — position
/// crossover.
pub fn px(
    private: &mut GeqoPrivateData,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) {
    /* initialize city table */
    for i in 1..=num_gene as usize {
        city_table[i].used = 0;
    }

    /* choose random positions that will be inherited directly from parent */
    let num_positions = geqo_randint(private, 2 * num_gene / 3, num_gene / 3);

    /* choose random position */
    for _i in 0..num_positions {
        let pos = geqo_randint(private, num_gene - 1, 0) as usize;

        offspring[pos] = tour1[pos]; /* transfer cities to child */
        city_table[tour1[pos] as usize].used = 1; /* mark city used */
    }

    let mut tour2_index = 0usize;
    let mut offspring_index = 0usize;

    /* px main part */
    while offspring_index < num_gene as usize {
        /* next position in offspring filled */
        if city_table[tour1[offspring_index] as usize].used == 0 {
            /* next city in tour1 not used */
            if city_table[tour2[tour2_index] as usize].used == 0 {
                /* inherit from tour1 */
                offspring[offspring_index] = tour2[tour2_index];

                tour2_index += 1;
                offspring_index += 1;
            } else {
                /* next city in tour2 has been used */
                tour2_index += 1;
            }
        } else {
            /* next position in offspring is filled */
            offspring_index += 1;
        }
    }
}
