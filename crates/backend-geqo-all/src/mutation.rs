//! `geqo_mutation.c` — TSP mutation routine (C: `#if defined(CX)`, used only in
//! CX mode). Ported as an always-available function (see crate docs).

use crate::random::geqo_randint;
use crate::{Gene, GeqoPrivateData};

/// `geqo_mutation(root, tour, num_gene)` — perform up to `num_gene/3` random
/// pairwise swaps within `tour`.
pub fn geqo_mutation(private: &mut GeqoPrivateData, tour: &mut [Gene], num_gene: i32) {
    let mut num_swaps = geqo_randint(private, num_gene / 3, 0);

    while num_swaps > 0 {
        let swap1 = geqo_randint(private, num_gene - 1, 0);
        let mut swap2 = geqo_randint(private, num_gene - 1, 0);

        while swap1 == swap2 {
            swap2 = geqo_randint(private, num_gene - 1, 0);
        }

        let temp = tour[swap1 as usize];
        tour[swap1 as usize] = tour[swap2 as usize];
        tour[swap2 as usize] = temp;

        num_swaps -= 1;
    }
}
