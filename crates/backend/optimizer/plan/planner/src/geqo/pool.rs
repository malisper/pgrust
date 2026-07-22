//! `geqo_pool.c` — GA pool allocation, seeding, sort, and displacement.

use types_core::primitive::Cost;
use types_error::PgResult;
use types_pathnodes::RelId;

use super::copy::geqo_copy;
use super::eval::geqo_eval;
use super::recombination::init_tour;
use super::{Chromosome, GeqoState, Pool};
use crate::run::PlannerRun;

// Fitness sentinel for an invalid join order (C's DBL_MAX).
const DBL_MAX: Cost = f64::MAX;

// Each tour gets string_length + 1 genes (C's +1 slack).
pub(super) fn alloc_pool(pool_size: i32, string_length: i32) -> Pool {
    let mut data = Vec::with_capacity(pool_size as usize);
    for _ in 0..pool_size {
        data.push(Chromosome {
            string: vec![0 as super::Gene; (string_length + 1) as usize],
            worth: 0.0,
        });
    }
    Pool { data, size: pool_size, string_length }
}

pub(super) fn alloc_chromo(string_length: i32) -> Chromosome {
    Chromosome { string: vec![0 as super::Gene; (string_length + 1) as usize], worth: 0.0 }
}

// Seed the pool, discarding invalid individuals (fitness DBL_MAX); give up
// after 10000 consecutive bad tries with no valid individual yet.
pub(super) fn random_init_pool<'mcx>(
    run: &mut PlannerRun<'mcx>,
    state: &mut GeqoState,
    initial_rels: &[RelId],
    pool: &mut Pool,
) -> PgResult<()> {
    let string_length = pool.string_length;
    let mut i = 0i32;
    let mut bad = 0i32;
    while i < pool.size {
        let idx = i as usize;
        init_tour(state, &mut pool.data[idx].string, string_length);
        let worth = geqo_eval(run, initial_rels, &pool.data[idx].string, string_length)?;
        pool.data[idx].worth = worth;
        if worth < DBL_MAX {
            i += 1;
        } else {
            bad += 1;
            if i == 0 && bad >= 10000 {
                return Err(Box::new(types_error::PgError::error(
                    "geqo failed to make a valid plan".to_string(),
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn sort_pool(pool: &mut Pool) {
    pool.data.sort_by(compare);
}

fn compare(a: &Chromosome, b: &Chromosome) -> core::cmp::Ordering {
    if a.worth == b.worth {
        core::cmp::Ordering::Equal
    } else if a.worth > b.worth {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Less
    }
}

// Insert chromo at its sorted position, displacing the worst (last) member;
// best->worst = smallest->largest.
pub(super) fn spread_chromo(chromo: &Chromosome, pool: &mut Pool) {
    let size = pool.size as usize;

    // Too bad to use.
    if chromo.worth > pool.data[size - 1].worth {
        return;
    }

    // Binary search for the insertion index.
    let mut top = 0usize;
    let mut mid = size / 2;
    let mut bot = size - 1;
    let mut index: isize = -1;
    while index == -1 {
        if chromo.worth <= pool.data[top].worth {
            index = top as isize;
        } else if chromo.worth == pool.data[mid].worth {
            index = mid as isize;
        } else if chromo.worth == pool.data[bot].worth {
            index = bot as isize;
        } else if bot - top <= 1 {
            index = bot as isize;
        } else if chromo.worth < pool.data[mid].worth {
            bot = mid;
            mid = top + ((bot - top) / 2);
        } else {
            top = mid;
            mid = top + ((bot - top) / 2);
        }
    }
    let index = index as usize;

    // Copy into the worst slot, then shift entries from `index` down toward it
    // — the swap-dance moves owned gene strings by value exactly as C swaps the
    // Gene *string pointers.
    {
        let string_length = pool.string_length;
        let (_worst, rest) = pool.data.split_at_mut(size - 1);
        geqo_copy(&mut rest[0], chromo, string_length);
    }
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
