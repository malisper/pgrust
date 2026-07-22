//! `geqo_cx.c` — cycle crossover [CX] (Oliver et al). C: `#if defined(CX)`.
#![allow(dead_code)]

use super::random::geqo_randint;
use super::recombination::City;
use super::{Gene, GeqoState};

/// `cx(tour1, tour2, offspring, num_gene, city_table)` — cycle crossover.
/// Returns the number of mom-vs-offspring differences when it fails to make a
/// complete tour.
pub(super) fn cx(
    state: &mut GeqoState,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
    city_table: &mut [City],
) -> i32 {
    let mut count = 0;
    let mut num_diffs = 0;

    for i in 1..=num_gene as usize {
        city_table[i].used = 0;
        city_table[tour2[i - 1] as usize].tour2_position = (i - 1) as i32;
        city_table[tour1[i - 1] as usize].tour1_position = (i - 1) as i32;
    }

    let start_pos = geqo_randint(state, num_gene - 1, 0) as usize;
    offspring[start_pos] = tour1[start_pos];
    let mut curr_pos = start_pos;
    city_table[tour1[start_pos] as usize].used = 1;
    count += 1;

    while tour2[curr_pos] != tour1[start_pos] {
        city_table[tour2[curr_pos] as usize].used = 1;
        curr_pos = city_table[tour2[curr_pos] as usize].tour1_position as usize;
        offspring[curr_pos] = tour1[curr_pos];
        count += 1;
    }

    if count < num_gene {
        for i in 1..=num_gene as usize {
            if city_table[i].used == 0 {
                let pos = city_table[i].tour2_position as usize;
                offspring[pos] = tour2[pos];
                count += 1;
            }
        }
    }

    if count < num_gene {
        for i in 0..num_gene as usize {
            if tour1[i] != offspring[i] {
                num_diffs += 1;
            }
        }
    }
    num_diffs
}
