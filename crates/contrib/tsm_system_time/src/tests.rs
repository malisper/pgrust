use super::*;

#[test]
fn gcd_matches_c() {
    assert_eq!(gcd(12, 18), 6);
    assert_eq!(gcd(7, 13), 1);
    assert_eq!(gcd(0, 5), 5);
}

#[test]
fn relative_prime_bounds_and_coprimality() {
    let mut r = sampler_random_init_state(42);
    assert_eq!(random_relative_prime(0, &mut r), 1);
    assert_eq!(random_relative_prime(1, &mut r), 1);
    for n in [2u32, 12, 97, 4096] {
        for seed in 0..8u32 {
            let mut r = sampler_random_init_state(seed);
            let p = random_relative_prime(n, &mut r);
            assert!(p > 0 && p < n && gcd(p, n) == 1);
        }
    }
}

fn walk(s: &mut SystemTimeSampler, nblocks: u32) -> Vec<u32> {
    let mut v = vec![];
    loop {
        let b = s.next_sample_block(nblocks);
        if b == InvalidBlockNumber {
            return v;
        }
        v.push(b);
    }
}

#[test]
fn zero_budget_returns_no_blocks() {
    let mut s = SystemTimeSampler::default();
    s.begin_sample_scan(0.0, 5).unwrap();
    assert_eq!(s.next_sample_block(31), InvalidBlockNumber);
}

#[test]
fn large_budget_visits_every_block_exactly_once() {
    for nblocks in [1u32, 2, 13, 37] {
        let mut s = SystemTimeSampler::default();
        s.begin_sample_scan(1e9, 12345).unwrap();
        let mut blocks = walk(&mut s, nblocks);
        assert_eq!(blocks.len(), nblocks as usize);
        blocks.sort_unstable();
        blocks.dedup();
        assert_eq!(blocks.len(), nblocks as usize, "walk missed or repeated a block");
    }
}

#[test]
fn tuple_walk_in_order() {
    let mut s = SystemTimeSampler::default();
    s.begin_sample_scan(1e9, 3).unwrap();
    for off in 1..=5u16 {
        assert_eq!(s.next_sample_tuple(5), off);
    }
    assert_eq!(s.next_sample_tuple(5), InvalidOffsetNumber);
}

#[test]
fn empty_relation() {
    let mut s = SystemTimeSampler::default();
    s.begin_sample_scan(1000.0, 3).unwrap();
    assert_eq!(s.next_sample_block(0), InvalidBlockNumber);
}

#[test]
fn negative_or_nan_time_is_2202h() {
    for bad in [-1.0, f64::NAN] {
        let mut s = SystemTimeSampler::default();
        let err = s.begin_sample_scan(bad, 0).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
        assert_eq!(err.message(), "sample collection time must not be negative");
    }
}

#[test]
fn sample_size_estimates_match_c() {
    assert_eq!(sample_scan_get_sample_size(Some(40.0), 4.0, 100, 10000.0), (10, 1000.0));
    assert_eq!(sample_scan_get_sample_size(None, 4.0, 100, 10000.0), (100, 10000.0));
    assert_eq!(sample_scan_get_sample_size(Some(f64::NAN), 4.0, 100, 10000.0), (100, 10000.0));
    assert_eq!(sample_scan_get_sample_size(Some(8.0), 0.0, 100, 10000.0), (8, 800.0));
    assert_eq!(sample_scan_get_sample_size(Some(7.0), 4.0, 0, 0.0), (1, 1.0));
}
