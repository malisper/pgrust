//! H5 rung B tests: EXPLAIN canonicalization (fingerprint stability — literal
//! changes must not mint species, plan-shape changes must) and the
//! Good-Turing f1/n arithmetic against closed-form expectations on synthetic
//! known distributions (uniform, zipf).

use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use simharness::metrics::{canonicalize_explain, SpeciesCensus};

fn lines(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

// ------------------------------------------------------- canonicalization

#[test]
fn literal_changes_do_not_mint_species() {
    // Same plan shape, different literals: the literal lives on the Filter:
    // detail line, which is dropped.
    let a = lines(&[
        "Sort",
        "  Sort Key: t1.id",
        "  ->  Seq Scan on t1",
        "        Filter: (c1 = 7)",
    ]);
    let b = lines(&[
        "Sort",
        "  Sort Key: t1.id",
        "  ->  Seq Scan on t1",
        "        Filter: (c1 = 93)",
    ]);
    assert_eq!(canonicalize_explain(&a), canonicalize_explain(&b));
    assert_eq!(canonicalize_explain(&a), "Sort(Seq Scan)");
}

#[test]
fn relation_and_index_names_are_stripped() {
    let a = lines(&["Index Only Scan using i1 on t1"]);
    let b = lines(&["Index Only Scan using i9 on t7"]);
    assert_eq!(canonicalize_explain(&a), canonicalize_explain(&b));
    assert_eq!(canonicalize_explain(&a), "Index Only Scan");
}

#[test]
fn plan_shape_changes_do_mint_species() {
    let seq = lines(&["Seq Scan on t1"]);
    let idx = lines(&["Index Scan using i1 on t1"]);
    assert_ne!(canonicalize_explain(&seq), canonicalize_explain(&idx));

    let hash_join = lines(&[
        "Hash Join",
        "  Hash Cond: (a.id = b.id)",
        "  ->  Seq Scan on t1 a",
        "  ->  Hash",
        "        ->  Seq Scan on t2 b",
    ]);
    let nl_join = lines(&[
        "Nested Loop",
        "  ->  Seq Scan on t1 a",
        "  ->  Index Scan using t2_pkey on t2 b",
        "        Index Cond: (id = a.id)",
    ]);
    assert_eq!(canonicalize_explain(&hash_join), "Hash Join(Seq Scan,Hash(Seq Scan))");
    assert_eq!(canonicalize_explain(&nl_join), "Nested Loop(Seq Scan,Index Scan)");
    assert_ne!(canonicalize_explain(&hash_join), canonicalize_explain(&nl_join));
}

#[test]
fn parallel_and_agg_modifiers_are_shape() {
    let serial = lines(&["Aggregate", "  ->  Seq Scan on t1"]);
    let parallel = lines(&[
        "Finalize Aggregate",
        "  ->  Gather",
        "        Workers Planned: 2",
        "        ->  Partial Aggregate",
        "              ->  Parallel Seq Scan on t1",
    ]);
    assert_eq!(canonicalize_explain(&serial), "Aggregate(Seq Scan)");
    assert_eq!(
        canonicalize_explain(&parallel),
        "Finalize Aggregate(Gather(Partial Aggregate(Parallel Seq Scan)))"
    );
    assert_ne!(canonicalize_explain(&serial), canonicalize_explain(&parallel));
}

#[test]
fn cost_parentheticals_are_stripped_when_present() {
    // COSTS OFF is the collection default, but the canonicalizer must not
    // depend on it.
    let with_costs = lines(&[
        "Sort  (cost=1.05..1.06 rows=5 width=8)",
        "  ->  Seq Scan on t1  (cost=0.00..1.04 rows=5 width=8)",
    ]);
    assert_eq!(canonicalize_explain(&with_costs), "Sort(Seq Scan)");
}

#[test]
fn deep_left_join_nest_shape() {
    let p = lines(&[
        "Sort",
        "  Sort Key: a.id",
        "  ->  Hash Left Join",
        "        Hash Cond: (a.id = b.id)",
        "        Filter: (COALESCE(b.id, 0) = 0)",
        "        ->  Seq Scan on t1 a",
        "        ->  Hash",
        "              ->  Hash Left Join",
        "                    Hash Cond: (b.id = c.id)",
        "                    ->  Seq Scan on t2 b",
        "                    ->  Hash",
        "                          ->  Seq Scan on t3 c",
    ]);
    assert_eq!(
        canonicalize_explain(&p),
        "Sort(Hash Left Join(Seq Scan,Hash(Hash Left Join(Seq Scan,Hash(Seq Scan)))))"
    );
}

#[test]
fn empty_input_is_explicit() {
    assert_eq!(canonicalize_explain(&[]), "<no-plan>");
}

// ---------------------------------------------------------- f1/n estimator

fn sample_census(weights: &[f64], n: u64, seed: u64) -> SpeciesCensus {
    let total: f64 = weights.iter().sum();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut c = SpeciesCensus::default();
    for _ in 0..n {
        // Inverse-CDF draw over the weight vector.
        let u = (rng.next_u64() as f64 / u64::MAX as f64) * total;
        let mut acc = 0.0;
        let mut pick = weights.len() - 1;
        for (i, w) in weights.iter().enumerate() {
            acc += w;
            if u < acc {
                pick = i;
                break;
            }
        }
        c.add_sighting(&format!("species-{pick}"));
    }
    c
}

/// Closed-form E[f1] = sum_i n * p_i * (1-p_i)^(n-1); E[U] = E[f1]/n.
fn expected_u(weights: &[f64], n: u64) -> f64 {
    let total: f64 = weights.iter().sum();
    let nf = n as f64;
    let ef1: f64 = weights
        .iter()
        .map(|w| {
            let p = w / total;
            nf * p * (1.0 - p).powf(nf - 1.0)
        })
        .sum();
    ef1 / nf
}

#[test]
fn good_turing_uniform_matches_closed_form() {
    // Uniform over S=2000 species, n=1000 draws: E[U] = (1-1/2000)^999 ~ 0.61.
    let weights = vec![1.0; 2000];
    let n = 1000;
    let expect = expected_u(&weights, n);
    let c = sample_census(&weights, n, 42);
    let got = c.good_turing_u().unwrap();
    assert!(
        (got - expect).abs() < 0.05,
        "uniform: U={got:.4} expected~{expect:.4}"
    );
    // Arithmetic identity: U * n == f1 exactly.
    assert_eq!((got * n as f64).round() as u64, c.f1());
}

#[test]
fn good_turing_zipf_matches_closed_form() {
    // Zipf over S=100 species (p_i ~ 1/i), n=2000.
    let weights: Vec<f64> = (1..=100).map(|i| 1.0 / i as f64).collect();
    let n = 2000;
    let expect = expected_u(&weights, n);
    let c = sample_census(&weights, n, 7);
    let got = c.good_turing_u().unwrap();
    assert!(
        (got - expect).abs() < 0.02,
        "zipf: U={got:.4} expected~{expect:.4}"
    );
}

#[test]
fn good_turing_degenerate_cases() {
    // All-distinct: every sighting is a singleton -> U = 1.
    let mut c = SpeciesCensus::default();
    for i in 0..50 {
        c.add_sighting(&format!("s{i}"));
    }
    assert_eq!(c.f1(), 50);
    assert_eq!(c.good_turing_u(), Some(1.0));
    // All-same: one species, f1 = 0 after the second sighting -> U = 0.
    let mut c = SpeciesCensus::default();
    for _ in 0..50 {
        c.add_sighting("only");
    }
    assert_eq!(c.f1(), 0);
    assert_eq!(c.good_turing_u(), Some(0.0));
    assert_eq!(c.f2(), 0); // seen 50 times, not twice
    // n = 0: no estimate, never a fake zero.
    let c = SpeciesCensus::default();
    assert_eq!(c.good_turing_u(), None);
    assert_eq!(c.chao1(), None);
}

#[test]
fn f1_f2_and_chao1_bookkeeping() {
    let mut c = SpeciesCensus::default();
    for fp in ["a", "a", "b", "c", "c", "d"] {
        c.add_sighting(fp);
    }
    assert_eq!(c.n, 6);
    assert_eq!(c.distinct(), 4);
    assert_eq!(c.f1(), 2); // b, d
    assert_eq!(c.f2(), 2); // a, c
    // Chao1 = S + f1^2/(2 f2) = 4 + 4/4 = 5.
    assert_eq!(c.chao1(), Some(5.0));
    // Accumulation curve checkpoints are monotone in both coordinates.
    let mut c = SpeciesCensus::default();
    let mut rng = ChaCha8Rng::seed_from_u64(1);
    for i in 0..200 {
        c.add_sighting(&format!("s{}", rng.next_u64() % 30));
        if i % 10 == 0 {
            c.checkpoint();
        }
    }
    for w in c.curve.windows(2) {
        assert!(w[1].0 >= w[0].0 && w[1].1 >= w[0].1);
    }
}
