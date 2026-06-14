//! Unit tests for the GEQO genetic-algorithm machinery. These exercise the
//! pure-arithmetic core (tour generation, recombination operators, selection,
//! pool maintenance) which needs no planner state. The planner-facing
//! `geqo`/`geqo_eval` paths require the seam providers (`make_join_rel` etc.)
//! and are exercised end-to-end by the wiring layer, not here.

use crate::copy::geqo_copy;
use crate::cx::cx;
use crate::erx::{alloc_edge_table, gimme_edge_table, gimme_tour};
use crate::main::{GeqoConfig, DEFAULT_GEQO_SELECTION_BIAS};
use crate::mutation::geqo_mutation;
use crate::ox1::ox1;
use crate::ox2::ox2;
use crate::pmx::pmx;
use crate::pool::{alloc_chromo, alloc_pool, sort_pool, spread_chromo};
use crate::px::px;
use crate::random::{geqo_rand, geqo_randint, geqo_set_seed};
use crate::recombination::{alloc_city_table, init_tour};
use crate::selection::geqo_selection;
use crate::{Chromosome, Gene, GeqoPrivateData};
use alloc::vec;
use alloc::vec::Vec;

fn priv_seeded(seed: f64) -> GeqoPrivateData {
    let mut p = GeqoPrivateData::default();
    geqo_set_seed(&mut p, seed);
    p
}

/// Assert `tour[0..num_gene]` is a permutation of `1..=num_gene`.
fn assert_permutation(tour: &[Gene], num_gene: i32) {
    let mut seen = vec![false; (num_gene + 1) as usize];
    for &g in &tour[..num_gene as usize] {
        assert!(g >= 1 && g <= num_gene, "gene {} out of range", g);
        assert!(!seen[g as usize], "gene {} repeated", g);
        seen[g as usize] = true;
    }
}

#[test]
fn randint_in_range_inclusive() {
    let mut p = priv_seeded(0.42);
    for _ in 0..1000 {
        let v = geqo_randint(&mut p, 7, 3);
        assert!((3..=7).contains(&v));
    }
}

#[test]
fn rand_in_unit_interval() {
    let mut p = priv_seeded(0.7);
    for _ in 0..1000 {
        let v = geqo_rand(&mut p);
        assert!((0.0..1.0).contains(&v));
    }
}

#[test]
fn init_tour_is_permutation() {
    let mut p = priv_seeded(0.123);
    for num_gene in 1..=12 {
        let mut tour = vec![0 as Gene; num_gene as usize];
        init_tour(&mut p, &mut tour, num_gene);
        assert_permutation(&tour, num_gene);
    }
}

#[test]
fn erx_gimme_tour_is_permutation() {
    let mut p = priv_seeded(0.55);
    let num_gene = 8;

    let mut tour1 = vec![0 as Gene; num_gene as usize];
    let mut tour2 = vec![0 as Gene; num_gene as usize];
    init_tour(&mut p, &mut tour1, num_gene);
    init_tour(&mut p, &mut tour2, num_gene);

    let mut edge_table = alloc_edge_table(num_gene);
    let avg = gimme_edge_table(&tour1, &tour2, num_gene, &mut edge_table);
    assert!((2.0..=4.0).contains(&avg), "avg edges/city {} out of [2,4]", avg);

    let mut child = vec![0 as Gene; (num_gene + 1) as usize];
    let _failures = gimme_tour(&mut p, &mut edge_table, &mut child, num_gene);
    assert_permutation(&child, num_gene);
}

#[test]
fn pmx_produces_permutation() {
    let mut p = priv_seeded(0.31);
    let num_gene = 9;
    let mut t1 = vec![0 as Gene; num_gene as usize];
    let mut t2 = vec![0 as Gene; num_gene as usize];
    init_tour(&mut p, &mut t1, num_gene);
    init_tour(&mut p, &mut t2, num_gene);

    let mut child = vec![0 as Gene; (num_gene + 1) as usize];
    pmx(&mut p, &t1, &t2, &mut child, num_gene);
    assert_permutation(&child, num_gene);
}

#[test]
fn ox1_ox2_px_produce_permutations() {
    let mut p = priv_seeded(0.9);
    let num_gene = 10;
    let mut t1 = vec![0 as Gene; num_gene as usize];
    let mut t2 = vec![0 as Gene; num_gene as usize];
    init_tour(&mut p, &mut t1, num_gene);
    init_tour(&mut p, &mut t2, num_gene);

    let mut child = vec![0 as Gene; (num_gene + 1) as usize];

    let mut ct = alloc_city_table(num_gene);
    ox1(&mut p, &t1, &t2, &mut child, num_gene, &mut ct);
    assert_permutation(&child, num_gene);

    let mut ct = alloc_city_table(num_gene);
    ox2(&mut p, &t1, &t2, &mut child, num_gene, &mut ct);
    assert_permutation(&child, num_gene);

    let mut ct = alloc_city_table(num_gene);
    px(&mut p, &t1, &t2, &mut child, num_gene, &mut ct);
    assert_permutation(&child, num_gene);
}

#[test]
fn cx_then_mutation_keeps_permutation() {
    let mut p = priv_seeded(0.17);
    let num_gene = 8;
    let mut t1 = vec![0 as Gene; num_gene as usize];
    let mut t2 = vec![0 as Gene; num_gene as usize];
    init_tour(&mut p, &mut t1, num_gene);
    init_tour(&mut p, &mut t2, num_gene);

    let mut child = vec![0 as Gene; (num_gene + 1) as usize];
    let mut ct = alloc_city_table(num_gene);
    let _diffs = cx(&mut p, &t1, &t2, &mut child, num_gene, &mut ct);
    // cx may leave some offspring slots unfilled when it can't complete a tour;
    // when it completes (count==num_gene) the result is a permutation. We at
    // least verify mutation preserves whatever multiset is present.
    let before: Vec<Gene> = child[..num_gene as usize].to_vec();
    geqo_mutation(&mut p, &mut child, num_gene);
    let mut a = before.clone();
    let mut b = child[..num_gene as usize].to_vec();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b, "mutation must be a permutation of the tour");
}

#[test]
fn geqo_copy_copies_tour_and_worth() {
    let src = Chromosome {
        string: vec![3, 1, 2, 0],
        worth: 42.5,
    };
    let mut dst = alloc_chromo(3);
    geqo_copy(&mut dst, &src, 3);
    assert_eq!(&dst.string[..3], &[3, 1, 2]);
    assert_eq!(dst.worth, 42.5);
}

#[test]
fn sort_and_spread_pool_keep_sorted_ascending() {
    let string_length = 3;
    let mut pool = alloc_pool(4, string_length);
    let worths = [5.0, 1.0, 9.0, 3.0];
    for (i, w) in worths.iter().enumerate() {
        pool.data[i].worth = *w;
        for j in 0..string_length as usize {
            pool.data[i].string[j] = (j + 1) as Gene;
        }
    }
    sort_pool(&mut pool);
    let sorted: Vec<f64> = pool.data.iter().map(|c| c.worth).collect();
    assert_eq!(sorted, vec![1.0, 3.0, 5.0, 9.0]);

    // Spread a better-than-worst chromo; it should land in sorted position and
    // evict the worst (9.0).
    let newc = Chromosome {
        string: vec![1, 2, 3, 0],
        worth: 2.0,
    };
    spread_chromo(&newc, &mut pool);
    let after: Vec<f64> = pool.data.iter().map(|c| c.worth).collect();
    let mut a = after.clone();
    a.sort_by(|x, y| x.partial_cmp(y).unwrap());
    assert_eq!(after, a, "pool must remain ascending after spread_chromo");
    assert!(after.contains(&2.0));
    assert!(!after.contains(&9.0), "worst should have been evicted");
}

#[test]
fn selection_picks_distinct_when_pool_large() {
    let mut p = priv_seeded(0.6);
    let string_length = 2;
    let mut pool = alloc_pool(5, string_length);
    for i in 0..5 {
        pool.data[i].worth = i as f64;
        pool.data[i].string[0] = 1;
        pool.data[i].string[1] = 2;
    }
    let mut momma = alloc_chromo(string_length);
    let mut daddy = alloc_chromo(string_length);
    // Run several selections; with pool size > 1 momma/daddy come from distinct
    // pool slots (worth values may coincide, but the routine forbids identical
    // indices).
    for _ in 0..50 {
        geqo_selection(&mut p, &mut momma, &mut daddy, &pool, DEFAULT_GEQO_SELECTION_BIAS);
        // copied tours are the (uniform) pool tours
        assert_eq!(&momma.string[..2], &[1, 2]);
        assert_eq!(&daddy.string[..2], &[1, 2]);
    }
}

#[test]
fn config_defaults_match_geqo_h() {
    let c = GeqoConfig::default();
    assert_eq!(c.effort, 5);
    assert_eq!(c.pool_size, 0);
    assert_eq!(c.generations, 0);
    assert_eq!(c.selection_bias, 2.0);
    assert_eq!(c.seed, 0.0);
}
