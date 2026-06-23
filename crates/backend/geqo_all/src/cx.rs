//! `geqo_cx.c` — cycle crossover [CX] (Oliver et al). C: `#if defined(CX)`.
//! Ported as an always-available function (see crate docs).

use crate::random::geqo_randint;
use crate::recombination::City;
use crate::{Gene, GeqoPrivateData};

/// `cx(root, tour1, tour2, offspring, num_gene, city_table)` — cycle crossover.
/// Returns the number of differences between mom (`tour1`) and `offspring` when
/// it fails to produce a complete tour (`num_diffs`).
pub fn cx(
    private: &mut GeqoPrivateData,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) -> i32 {
    let mut count = 0;
    let mut num_diffs = 0;

    /* initialize city table */
    for i in 1..=num_gene as usize {
        city_table[i].used = 0;
        city_table[tour2[i - 1] as usize].tour2_position = (i - 1) as i32;
        city_table[tour1[i - 1] as usize].tour1_position = (i - 1) as i32;
    }

    /* choose random cycle starting position */
    let start_pos = geqo_randint(private, num_gene - 1, 0) as usize;

    /* child inherits first city */
    offspring[start_pos] = tour1[start_pos];

    /* begin cycle with tour1 */
    let mut curr_pos = start_pos;
    city_table[tour1[start_pos] as usize].used = 1;

    count += 1;

    /* cx main part */

    /* STEP 1 */
    while tour2[curr_pos] != tour1[start_pos] {
        city_table[tour2[curr_pos] as usize].used = 1;
        curr_pos = city_table[tour2[curr_pos] as usize].tour1_position as usize;
        offspring[curr_pos] = tour1[curr_pos];
        count += 1;
    }

    /* STEP 2 */
    /* failed to create a complete tour */
    if count < num_gene {
        for i in 1..=num_gene as usize {
            if city_table[i].used == 0 {
                let pos = city_table[i].tour2_position as usize;
                offspring[pos] = tour2[pos];
                count += 1;
            }
        }
    }

    /* STEP 3 */
    /* still failed to create a complete tour */
    if count < num_gene {
        /* count the number of differences between mom and offspring */
        for i in 0..num_gene as usize {
            if tour1[i] != offspring[i] {
                num_diffs += 1;
            }
        }
    }

    num_diffs
}
