//! `geqo_pool.c` — Genetic Algorithm (GA) pool stuff.

use crate::copy::geqo_copy;
use crate::eval::geqo_eval;
use crate::recombination::init_tour;
use crate::{Chromosome, Gene, GeqoPrivateData};
use alloc::vec;
use alloc::vec::Vec;
use types_pathnodes::PlannerInfo;
use types_core::primitive::Cost;

/// `DBL_MAX` (`<float.h>`) — the sentinel `geqo_eval` returns for an invalid
/// join order.
const DBL_MAX: Cost = f64::MAX;

/// `Pool` (`geqo_gene.h`): a population of [`Chromosome`]s. The C `Chromosome
/// *data` array becomes an owned `Vec<Chromosome>`.
#[derive(Clone, Debug)]
pub struct Pool {
    pub data: Vec<Chromosome>,
    pub size: i32,
    pub string_length: i32,
}

/// `alloc_pool(root, pool_size, string_length)` — allocate the GA pool. Each
/// chromosome's tour gets `string_length + 1` genes (the C `+1` slack).
pub fn alloc_pool(pool_size: i32, string_length: i32) -> Pool {
    let mut data = Vec::with_capacity(pool_size as usize);
    for _ in 0..pool_size {
        data.push(Chromosome {
            string: vec![0 as Gene; (string_length + 1) as usize],
            worth: 0.0,
        });
    }

    Pool {
        data,
        size: pool_size,
        string_length,
    }
}

// free_pool is RAII (the owned Vecs are dropped).

/// `random_init_pool(root, pool)` — initialize the genetic pool, discarding
/// invalid individuals (those whose [`geqo_eval`] returns `DBL_MAX`). Gives up
/// after 10000 consecutive invalid tries with no valid individual yet.
pub fn random_init_pool(root: &mut PlannerInfo, private: &mut GeqoPrivateData, pool: &mut Pool) {
    let string_length = pool.string_length;
    let mut i = 0i32;
    let mut bad = 0i32;

    while i < pool.size {
        let idx = i as usize;
        init_tour(private, &mut pool.data[idx].string, string_length);
        let worth = geqo_eval(root, private, &pool.data[idx].string, string_length);
        pool.data[idx].worth = worth;
        if pool.data[idx].worth < DBL_MAX {
            i += 1;
        } else {
            bad += 1;
            if i == 0 && bad >= 10000 {
                panic!("geqo failed to make a valid plan");
            }
        }
    }
    // GEQO_DEBUG: count of invalid tours not logged (debug-only path).
}

/// `sort_pool(root, pool)` — sort the pool by `worth`, smallest to largest
/// (`qsort` with [`compare`]).
pub fn sort_pool(pool: &mut Pool) {
    pool.data.sort_by(compare);
}

/// `compare(arg1, arg2)` — qsort comparison: order by `worth` ascending.
fn compare(chromo1: &Chromosome, chromo2: &Chromosome) -> core::cmp::Ordering {
    if chromo1.worth == chromo2.worth {
        core::cmp::Ordering::Equal
    } else if chromo1.worth > chromo2.worth {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Less
    }
}

/// `alloc_chromo(root, string_length)` — allocate a chromosome with a
/// `string_length + 1` gene string.
pub fn alloc_chromo(string_length: i32) -> Chromosome {
    Chromosome {
        string: vec![0 as Gene; (string_length + 1) as usize],
        worth: 0.0,
    }
}

// free_chromo is RAII (the owned Vec is dropped).

/// `spread_chromo(root, chromo, pool)` — insert a new chromosome into the pool,
/// displacing the worst gene; assumes best→worst = smallest→largest.
pub fn spread_chromo(chromo: &Chromosome, pool: &mut Pool) {
    let size = pool.size as usize;

    /* new chromo is so bad we can't use it */
    if chromo.worth > pool.data[size - 1].worth {
        return;
    }

    /* do a binary search to find the index of the new chromo */
    let mut top = 0usize;
    let mut mid = size / 2;
    let mut bot = size - 1;
    let mut index: isize = -1;

    while index == -1 {
        /* these 4 cases find a new location */
        if chromo.worth <= pool.data[top].worth {
            index = top as isize;
        } else if chromo.worth == pool.data[mid].worth {
            index = mid as isize;
        } else if chromo.worth == pool.data[bot].worth {
            index = bot as isize;
        } else if bot - top <= 1 {
            index = bot as isize;
        }
        /*
         * these 2 cases move the search indices since a new location has not
         * yet been found.
         */
        else if chromo.worth < pool.data[mid].worth {
            bot = mid;
            mid = top + ((bot - top) / 2);
        } else {
            /* (chromo->worth > pool->data[mid].worth) */
            top = mid;
            mid = top + ((bot - top) / 2);
        }
    }

    let index = index as usize;

    /* now we have index for chromo */

    /*
     * move every gene from index on down one position to make room for chromo
     */
    /* copy new gene into pool storage; always replace worst gene in pool */
    {
        let string_length = pool.string_length;
        let (worst, rest) = pool.data.split_at_mut(size - 1);
        let _ = worst;
        geqo_copy(&mut rest[0], chromo, string_length);
    }

    /*
     * The swap dance moves owned gene strings (Vec<Gene>) by value, exactly as
     * C swaps the `Gene *string` pointers: every entry from `index` downward is
     * shifted one slot toward the worst (last) entry, which already holds the
     * freshly-copied new chromosome.
     */
    let mut swap_string = core::mem::take(&mut pool.data[size - 1].string);
    let mut swap_worth = pool.data[size - 1].worth;

    for i in index..size {
        let tmp_string = core::mem::take(&mut pool.data[i].string);
        let tmp_worth = pool.data[i].worth;

        pool.data[i].string = swap_string;
        pool.data[i].worth = swap_worth;

        swap_string = tmp_string;
        swap_worth = tmp_worth;
    }
}
