use super::*;

#[test]
fn gcd_matches_c() {
    assert_eq!(gcd(12, 18), 6);
    assert_eq!(gcd(18, 12), 6);
    assert_eq!(gcd(7, 13), 1);
    assert_eq!(gcd(0, 5), 5);
    assert_eq!(gcd(5, 0), 5);
    assert_eq!(gcd(1, 1), 1);
}

#[test]
fn relative_prime_bounds_and_coprimality() {
    let mut r = sampler_random_init_state(42);
    assert_eq!(random_relative_prime(0, &mut r), 1);
    assert_eq!(random_relative_prime(1, &mut r), 1);
    for n in [2u32, 12, 97, 4096, 65537] {
        for seed in 0..8u32 {
            let mut r = sampler_random_init_state(seed);
            let p = random_relative_prime(n, &mut r);
            assert!(p > 0 && p < n, "step {p} out of range for n={n}");
            assert_eq!(gcd(p, n), 1, "step {p} not coprime to {n}");
        }
    }
}

#[test]
fn fract_is_deterministic_nonzero_unit_interval() {
    let mut a = sampler_random_init_state(0);
    let mut b = sampler_random_init_state(0);
    for _ in 0..64 {
        let v = sampler_random_fract(&mut a);
        assert_eq!(v, sampler_random_fract(&mut b));
        assert!(v > 0.0 && v < 1.0);
    }
    let mut c = sampler_random_init_state(1);
    assert_ne!(sampler_random_fract(&mut a), sampler_random_fract(&mut c));
}

fn walk(s: &mut SystemRowsSampler, nblocks: u32, donetuples: i64) -> Vec<u32> {
    let mut v = vec![];
    loop {
        let b = s.next_sample_block(nblocks, donetuples);
        if b == InvalidBlockNumber {
            return v;
        }
        v.push(b);
    }
}

#[test]
fn block_walk_visits_every_block_exactly_once() {
    for nblocks in [1u32, 2, 13, 37, 64] {
        let mut s = SystemRowsSampler::default();
        s.begin_sample_scan(i64::MAX, 12345).unwrap();
        let mut blocks = walk(&mut s, nblocks, 0);
        assert_eq!(blocks.len(), nblocks as usize);
        blocks.sort_unstable();
        blocks.dedup();
        assert_eq!(blocks.len(), nblocks as usize, "walk missed or repeated a block");
    }
}

#[test]
fn same_seed_same_walk_and_rescan_reuses_pattern() {
    let run = |seed: u32| {
        let mut s = SystemRowsSampler::default();
        s.begin_sample_scan(1 << 40, seed).unwrap();
        walk(&mut s, 64, 0)
    };
    assert_eq!(run(7), run(7));
    assert_ne!(run(7), run(8));

    // A rescan (fresh begin, even another executor seed) repeats the walk.
    let mut s = SystemRowsSampler::default();
    s.begin_sample_scan(1 << 40, 7).unwrap();
    let first = walk(&mut s, 64, 0);
    s.begin_sample_scan(1 << 40, 999).unwrap();
    assert_eq!(first, walk(&mut s, 64, 0));
}

#[test]
fn donetuples_cutoff() {
    let mut s = SystemRowsSampler::default();
    s.begin_sample_scan(10, 1).unwrap();
    assert_ne!(s.next_sample_block(8, 0), InvalidBlockNumber);
    assert_eq!(s.next_sample_block(8, 10), InvalidBlockNumber);

    let mut s = SystemRowsSampler::default();
    s.begin_sample_scan(5, 1).unwrap();
    assert_eq!(s.next_sample_tuple(30, 5), InvalidOffsetNumber);
    assert_eq!(s.next_sample_tuple(2, 0), FirstOffsetNumber);
    assert_eq!(s.next_sample_tuple(2, 1), 2);
    assert_eq!(s.next_sample_tuple(2, 2), InvalidOffsetNumber);
}

#[test]
fn zero_rows_and_empty_relation() {
    let mut s = SystemRowsSampler::default();
    s.begin_sample_scan(0, 1).unwrap();
    assert_eq!(s.next_sample_block(8, 0), InvalidBlockNumber);

    let mut s = SystemRowsSampler::default();
    s.begin_sample_scan(100, 3).unwrap();
    assert_eq!(s.next_sample_block(0, 0), InvalidBlockNumber);
}

#[test]
fn negative_count_is_2202h() {
    let mut s = SystemRowsSampler::default();
    let err = s.begin_sample_scan(-1, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
    assert_eq!(err.message(), "sample size must not be negative");
}

#[test]
fn sample_size_estimates_match_c() {
    assert_eq!(sample_scan_get_sample_size(Some(-5), 100, 10000.0), (10, 1000.0));
    assert_eq!(sample_scan_get_sample_size(None, 100, 10000.0), (10, 1000.0));
    assert_eq!(sample_scan_get_sample_size(Some(50), 100, 10000.0), (1, 50.0));
    assert_eq!(sample_scan_get_sample_size(Some(50000), 100, 10000.0), (100, 10000.0));
    assert_eq!(sample_scan_get_sample_size(Some(7), 0, 0.0), (1, 1.0));
}
