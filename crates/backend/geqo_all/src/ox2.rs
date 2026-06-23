//! `geqo_ox2.c` — order crossover [OX2] (Syswerda). C: `#if defined(OX2)`.
//! Ported as an always-available function (see crate docs).

use crate::random::geqo_randint;
use crate::recombination::City;
use crate::{Gene, GeqoPrivateData};

/// `ox2(root, tour1, tour2, offspring, num_gene, city_table)` — order
/// crossover (Syswerda).
pub fn ox2(
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
        city_table[k - 1].select_list = -1;
    }

    /* determine the number of positions to be inherited from tour1 */
    let num_positions = geqo_randint(private, 2 * num_gene / 3, num_gene / 3);

    /* make a list of selected cities */
    for _k in 0..num_positions {
        let pos = geqo_randint(private, num_gene - 1, 0) as usize;
        city_table[pos].select_list = tour1[pos];
        city_table[tour1[pos] as usize].used = 1; /* mark used */
    }

    let mut count = 0;
    let mut k = 0usize;

    /* consolidate the select list to adjacent positions */
    while count < num_positions {
        if city_table[k].select_list == -1 {
            let mut j = k + 1;
            /* C tests `city_table[j].select_list == -1` before `j < num_gene`;
             * the table has `num_gene + 1` entries so index `num_gene` is in
             * bounds, matching the C access order exactly. */
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
            select += 1; /* next city in the select list */
        } else {
            /* city isn't used yet, so inherit from tour2 */
            offspring[k] = tour2[k];
        }
    }
}
