//! `geqo_recombination.c` — misc recombination procedures + the `City` table.

use crate::random::geqo_randint;
use crate::{Gene, GeqoPrivateData};
use alloc::vec;
use alloc::vec::Vec;

/// `init_tour(root, tour, num_gene)` — randomly generate a legal TSP tour (each
/// point visited once), using the inside-out Fisher-Yates shuffle to fill
/// `tour[]` with a random permutation of `1 .. num_gene`.
pub fn init_tour(private: &mut GeqoPrivateData, tour: &mut [Gene], num_gene: i32) {
    if num_gene > 0 {
        tour[0] = 1 as Gene;
    }

    for i in 1..num_gene as usize {
        let j = geqo_randint(private, i as i32, 0) as usize;
        /* i != j check avoids fetching uninitialized array element */
        if i != j {
            tour[i] = tour[j];
        }
        tour[j] = (i + 1) as Gene;
    }
}

/* city table is used in CX/PX/OX1/OX2 recombination methods */

/// `City` (`geqo_recombination.h`): one row of the city table used by the
/// CX/PX/OX1/OX2 operators.
#[derive(Clone, Copy, Debug, Default)]
pub struct City {
    pub tour2_position: i32,
    pub tour1_position: i32,
    pub used: i32,
    pub select_list: i32,
}

/// `alloc_city_table(root, num_gene)` — allocate the city table. C palloc's
/// `num_gene + 1` entries "so that nodes numbered 1..n can be indexed directly;
/// 0 will not be used".
pub fn alloc_city_table(num_gene: i32) -> Vec<City> {
    vec![City::default(); (num_gene + 1) as usize]
}

// free_city_table is RAII (the Vec is dropped); the C `pfree` has no analogue.
