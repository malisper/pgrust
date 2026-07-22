//! `geqo_mutation.c` — TSP mutation (C: `#if defined(CX)`, used only in CX).
#![allow(dead_code)]

use super::random::geqo_randint;
use super::{Gene, GeqoState};

/// `geqo_mutation(tour, num_gene)` — up to `num_gene/3` random pairwise swaps.
pub(super) fn geqo_mutation(state: &mut GeqoState, tour: &mut [Gene], num_gene: i32) {
    let mut num_swaps = geqo_randint(state, num_gene / 3, 0);
    while num_swaps > 0 {
        let swap1 = geqo_randint(state, num_gene - 1, 0);
        let mut swap2 = geqo_randint(state, num_gene - 1, 0);
        while swap1 == swap2 {
            swap2 = geqo_randint(state, num_gene - 1, 0);
        }
        tour.swap(swap1 as usize, swap2 as usize);
        num_swaps -= 1;
    }
}
