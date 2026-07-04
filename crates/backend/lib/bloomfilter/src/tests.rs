use super::*;
use ::mcx::MemoryContext;
use std::collections::HashSet;

fn dummy(prefix: char, i: i64) -> String {
    format!("{prefix}{i}")
}

// test_bloomfilter.c defaults: power=23, nelements=2^23/10, 1% threshold.
const POWER: i64 = 23;
const NELEMENTS: i64 = (1 << POWER) / 10;

fn create_and_test_bloom(ctx: &MemoryContext, seed: u64) -> (i64, f64) {
    let bloom_work_mem = ((1i64 << POWER) / (8 * 1024)) as i32;
    let mut filter = BloomFilter::create_in(ctx.mcx(), NELEMENTS, bloom_work_mem, seed).unwrap();

    for i in 0..NELEMENTS {
        filter.add_element(dummy('i', i).as_bytes());
    }
    for i in 0..NELEMENTS {
        assert!(!filter.lacks_element(dummy('i', i).as_bytes()), "false negative at i{i}");
    }
    let mut nfalsepos = 0i64;
    for i in 0..NELEMENTS {
        if !filter.lacks_element(dummy('M', i).as_bytes()) {
            nfalsepos += 1;
        }
    }
    (nfalsepos, filter.prop_bits_set())
}

#[test]
fn false_positive_rate_within_band() {
    let ctx = MemoryContext::new("bloom-test");
    for seed in [0u64, 17, 2147483647] {
        let (nfalsepos, prop) = create_and_test_bloom(&ctx, seed);
        assert!(
            nfalsepos as f64 <= NELEMENTS as f64 * 0.01,
            "seed {seed}: {nfalsepos} false positives over {NELEMENTS}"
        );
        assert!((0.3..0.7).contains(&prop), "seed {seed}: prop_bits_set {prop}");
    }
}

#[test]
fn seeded_determinism() {
    let ctx = MemoryContext::new("bloom-test");
    let mk = |seed: u64| {
        let mut f = BloomFilter::create_in(ctx.mcx(), 10_000, 1024, seed).unwrap();
        for i in 0..10_000 {
            f.add_element(dummy('i', i).as_bytes());
        }
        f
    };
    let a = mk(42);
    let b = mk(42);
    let c = mk(43);
    assert_eq!(a.bitset(), b.bitset());
    assert_ne!(a.bitset(), c.bitset());
    assert_eq!(a.k_hash_funcs(), b.k_hash_funcs());
}

#[test]
fn sizing_and_clamping() {
    let ctx = MemoryContext::new("bloom-test");

    // total_elems-bound request below the 1MB floor.
    let f = BloomFilter::create_in(ctx.mcx(), 1_000, 1_000_000, 0).unwrap();
    assert_eq!(f.bitset_bits(), 1 << 23);
    assert_eq!(f.bitset().len(), 1024 * 1024);
    assert_eq!(f.k_hash_funcs(), 10);

    // work_mem-bound: k clamps to 1 when elements vastly exceed bits.
    let f = BloomFilter::create_in(ctx.mcx(), 100_000_000, 1024, 0).unwrap();
    assert_eq!(f.bitset_bits(), 1 << 23);
    assert_eq!(f.k_hash_funcs(), 1);

    // Non-power-of-two work_mem rounds the bitset down to a power of two.
    let f = BloomFilter::create_in(ctx.mcx(), i64::MAX / 4, 3 * 1024, 0).unwrap();
    assert_eq!(f.bitset_bits(), 1 << 24);
    assert_eq!(f.bitset().len(), 2 * 1024 * 1024);

    let f = BloomFilter::create_in(ctx.mcx(), 1, 1024, 0).unwrap();
    assert_eq!(f.prop_bits_set(), 0.0);
}

#[test]
fn bloom_power_matches_c() {
    assert_eq!(my_bloom_power(1), 0);
    assert_eq!(my_bloom_power(2), 1);
    assert_eq!(my_bloom_power(3), 1);
    assert_eq!(my_bloom_power(1 << 23), 23);
    assert_eq!(my_bloom_power((1 << 23) + 1), 23);
    assert_eq!(my_bloom_power(1 << 32), 32);
    assert_eq!(my_bloom_power(u64::MAX), 32);
}

#[test]
fn optimal_k_matches_c_rint() {
    assert_eq!(optimal_k(1 << 23, 838_861), 7);
    assert_eq!(optimal_k(1 << 23, 100_000_000), 1);
    assert_eq!(optimal_k(1 << 23, 1_000), 10);
    // rint ties-to-even: ln2 * m / n == 2.5 rounds to 2.
    let n = (core::f64::consts::LN_2 * (1u64 << 20) as f64 / 2.5) as i64;
    let k = core::f64::consts::LN_2 * (1u64 << 20) as f64 / n as f64;
    if k == 2.5 {
        assert_eq!(optimal_k(1 << 20, n), 2);
    }
}

#[test]
fn k_hashes_shape() {
    let mut hashes = [0u32; MAX_HASH_FUNCS];
    let m = 1u64 << 23;
    k_hashes(10, 0xdead_beef, m, b"i12345", &mut hashes);
    let h = hashfn::hash_bytes_extended(b"i12345", 0xdead_beef);
    let mut x = mod_m(h as u32, m);
    let mut y = mod_m((h >> 32) as u32, m);
    assert_eq!(hashes[0], x);
    for (i, &got) in hashes.iter().enumerate().skip(1) {
        x = mod_m(x.wrapping_add(y), m);
        y = mod_m(y.wrapping_add(i as u32), m);
        assert_eq!(got, x);
        assert!(u64::from(got) < m);
    }
}

#[test]
fn model_property_no_false_negatives() {
    let ctx = MemoryContext::new("bloom-test");
    let mut filter = BloomFilter::create_in(ctx.mcx(), 50_000, 1024, 7).unwrap();
    let mut model: HashSet<Vec<u8>> = HashSet::new();

    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        state
    };
    for _ in 0..50_000 {
        let len = 1 + (next() % 24) as usize;
        let elem: Vec<u8> = (0..len).map(|_| next() as u8).collect();
        filter.add_element(&elem);
        model.insert(elem);
    }
    for elem in &model {
        assert!(!filter.lacks_element(elem));
    }
}
