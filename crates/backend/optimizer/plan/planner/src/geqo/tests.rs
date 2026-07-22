//! Unit coverage for the pure GA components (no PlannerRun): the size formulas,
//! tour shuffle, ERX, pool displacement, and the determinism pin (fixed
//! geqo_seed => reproducible RNG stream => reproducible plan). The end-to-end
//! "GEQO plan executes to the same rows as standard search" witness is the
//! join.sql regress leg (set geqo_threshold=2), validated on the fleet.

use super::erx::{alloc_edge_table, gimme_edge_table, gimme_tour};
use super::random::{geqo_randint, geqo_set_seed};
use super::recombination::init_tour;
use super::{Chromosome, GeqoState, Pool};

fn state(seed: f64) -> GeqoState {
    let mut s = GeqoState { rng: pg_prng::PgPrng::default() };
    geqo_set_seed(&mut s, seed);
    s
}

fn is_permutation(tour: &[super::Gene], num_gene: i32) -> bool {
    let mut seen = vec![false; (num_gene + 1) as usize];
    for &g in &tour[..num_gene as usize] {
        if g < 1 || g > num_gene || seen[g as usize] {
            return false;
        }
        seen[g as usize] = true;
    }
    true
}

#[test]
fn gimme_pool_size_matches_c_formula() {
    // Boot defaults (cluster TLS): pool_size=0, effort=5.
    super::set_geqo_pool_size(0);
    super::set_geqo_effort(5);
    // 2^(n+1) clamped to [10*effort, 50*effort] = [50, 250].
    assert_eq!(super::gimme_pool_size(3), 50); // 2^4=16 < 50 -> minsize
    assert_eq!(super::gimme_pool_size(7), 250); // 2^8=256 > 250 -> maxsize
    assert_eq!(super::gimme_pool_size(6), 128); // 2^7=128 in range
    // A configured pool size >= 2 overrides the default.
    super::set_geqo_pool_size(37);
    assert_eq!(super::gimme_pool_size(6), 37);
    // A configured size of 1 is illegal and ignored (falls back to default).
    super::set_geqo_pool_size(1);
    assert_eq!(super::gimme_pool_size(6), 128);
    super::set_geqo_pool_size(0);
}

#[test]
fn gimme_number_generations_default_is_pool_size() {
    super::set_geqo_generations(0);
    assert_eq!(super::gimme_number_generations(128), 128);
    super::set_geqo_generations(42);
    assert_eq!(super::gimme_number_generations(128), 42);
    super::set_geqo_generations(0);
}

#[test]
fn init_tour_is_a_permutation() {
    let mut st = state(0.0);
    for num_gene in [1, 2, 5, 12, 20] {
        let mut tour = vec![0; (num_gene + 1) as usize];
        init_tour(&mut st, &mut tour, num_gene);
        assert!(is_permutation(&tour, num_gene), "num_gene={num_gene}");
    }
}

#[test]
fn erx_tour_is_a_permutation() {
    let mut st = state(0.5);
    let num_gene = 12;
    let mut t1 = vec![0; (num_gene + 1) as usize];
    let mut t2 = vec![0; (num_gene + 1) as usize];
    init_tour(&mut st, &mut t1, num_gene);
    init_tour(&mut st, &mut t2, num_gene);
    let mut et = alloc_edge_table(num_gene);
    gimme_edge_table(&t1, &t2, num_gene, &mut et);
    let mut kid = vec![0; (num_gene + 1) as usize];
    gimme_tour(&mut st, &mut et, &mut kid, num_gene);
    assert!(is_permutation(&kid, num_gene));
}

// Determinism pin: identical seed => identical draws => identical tours =>
// identical plan.
#[test]
fn same_seed_reproduces_the_rng_stream() {
    let run = |seed: f64| {
        let mut st = state(seed);
        let num_gene = 14;
        let mut t1 = vec![0; (num_gene + 1) as usize];
        let mut t2 = vec![0; (num_gene + 1) as usize];
        init_tour(&mut st, &mut t1, num_gene);
        init_tour(&mut st, &mut t2, num_gene);
        let mut et = alloc_edge_table(num_gene);
        gimme_edge_table(&t1, &t2, num_gene, &mut et);
        let mut kid = vec![0; (num_gene + 1) as usize];
        gimme_tour(&mut st, &mut et, &mut kid, num_gene);
        // A trailing scalar draw so selection-style consumption is covered too.
        let r = geqo_randint(&mut st, 100, 0);
        (t1, t2, kid, r)
    };
    assert_eq!(run(0.0), run(0.0), "seed 0.0 must be reproducible");
    assert_eq!(run(0.42), run(0.42), "seed 0.42 must be reproducible");
    assert_ne!(run(0.0).2, run(0.99).2, "different seeds should differ");
}

#[test]
fn spread_chromo_keeps_pool_sorted() {
    let string_length = 4;
    let mk = |worth: f64| Chromosome { string: vec![0; string_length as usize + 1], worth };
    let mut pool = Pool {
        data: vec![mk(1.0), mk(3.0), mk(5.0), mk(7.0), mk(9.0)],
        size: 5,
        string_length,
    };
    // Insert a mid-ranked individual; it should land at index 2, worst drops.
    super::pool::spread_chromo(&mk(4.0), &mut pool);
    let worths: Vec<f64> = pool.data.iter().map(|c| c.worth).collect();
    assert_eq!(worths, vec![1.0, 3.0, 4.0, 5.0, 7.0]);
    // A too-bad individual is rejected.
    super::pool::spread_chromo(&mk(100.0), &mut pool);
    let worths: Vec<f64> = pool.data.iter().map(|c| c.worth).collect();
    assert_eq!(worths, vec![1.0, 3.0, 4.0, 5.0, 7.0]);
    // A new best lands at the front.
    super::pool::spread_chromo(&mk(0.5), &mut pool);
    let worths: Vec<f64> = pool.data.iter().map(|c| c.worth).collect();
    assert_eq!(worths, vec![0.5, 1.0, 3.0, 4.0, 5.0]);
}

#[test]
fn geqo_gucs_are_backed_and_live() {
    // The 7 GUCs read for real: the cluster getters return boot defaults and
    // reflect writes (the install() wiring feeds SET through these setters).
    super::set_enable_geqo(true);
    super::set_geqo_threshold(12);
    assert!(super::enable_geqo());
    assert_eq!(super::geqo_threshold(), 12);
    super::set_geqo_threshold(2);
    assert_eq!(super::geqo_threshold(), 2);
    super::set_geqo_threshold(12);
    super::set_geqo_seed(0.75);
    assert_eq!(super::geqo_seed(), 0.75);
    super::set_geqo_seed(0.0);
}
