//! geqo_recombination.c — the init_tour shuffle and the City table.

use super::random::geqo_randint;
use super::{Gene, GeqoState};

// Random legal TSP tour (permutation of 1..=num_gene) via inside-out
// Fisher-Yates.
pub(super) fn init_tour(state: &mut GeqoState, tour: &mut [Gene], num_gene: i32) {
    if num_gene > 0 {
        tour[0] = 1;
    }
    for i in 1..num_gene as usize {
        let j = geqo_randint(state, i as i32, 0) as usize;
        if i != j {
            tour[i] = tour[j];
        }
        tour[j] = (i + 1) as Gene;
    }
}

#[cfg(feature = "geqo_nondefault_operators")]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct City {
    pub tour2_position: i32,
    pub tour1_position: i32,
    pub used: i32,
    pub select_list: i32,
}

// num_gene + 1 rows so nodes 1..n index directly (0 unused). Not wired into the
// ERX driver; switching operators is a build-time choice, as in C.
#[cfg(feature = "geqo_nondefault_operators")]
#[allow(dead_code)]
pub(super) fn alloc_city_table(num_gene: i32) -> Vec<City> {
    vec![City::default(); (num_gene + 1) as usize]
}
