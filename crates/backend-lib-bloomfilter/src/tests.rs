//! Tests for the Bloom filter port.
//!
//! These exercise the internal safe owned-value implementation directly (the
//! seam layer is a thin pass-through). The `create_and_test_bloom` golden test
//! mirrors `src/test/modules/test_bloomfilter/test_bloomfilter.c`.

use super::*;
use alloc::format;
use alloc::vec::Vec;
use std::sync::Once;

/// Install the real `hash_bytes_extended` (the in-repo `common-hashfn` owner),
/// so the golden false-positive-rate test exercises the genuine Bob Jenkins
/// hash exactly as C's `hash_any_extended` does.
fn install_hash() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        common_hashfn::init_seams();
    });
}

// ---- Pure helpers (no allocation) ----------------------------------------

#[test]
fn my_bloom_power_matches_c() {
    // -1 for zero, highest power of two <= target otherwise, capped at 32.
    assert_eq!(my_bloom_power(0), -1);
    assert_eq!(my_bloom_power(1), 0);
    assert_eq!(my_bloom_power(2), 1);
    assert_eq!(my_bloom_power(3), 1);
    assert_eq!(my_bloom_power(8), 3);
    assert_eq!(my_bloom_power(1 << 31), 31);
    assert_eq!(my_bloom_power(1 << 32), 32);
    assert_eq!(my_bloom_power(u64::MAX), 32);
}

#[test]
fn optimal_k_clamps_to_band() {
    // ln(2) * m / n, rounded to nearest even, clamped to [1, 10].
    assert_eq!(optimal_k(1 << 23, 1000), 10);
    assert_eq!(optimal_k(1024, 100000), 1);
    // acl.c's exact argument (total_elems == 10240 against a 2^23-bit bitset).
    assert_eq!(optimal_k(1 << 23, 10240), 10);
    // A case landing strictly inside the band: ln2 * 2^23 / 2_000_000 ~= 3.
    assert_eq!(optimal_k(1 << 23, 2_000_000), 3);
}

#[test]
fn mod_m_is_power_of_two_mask() {
    assert_eq!(mod_m(0x1234_5678, 1 << 16), 0x5678);
    assert_eq!(mod_m(0xFFFF_FFFF, 1 << 32), 0xFFFF_FFFF);
}

// ---- End-to-end via the owned-value API ----------------------------------

#[test]
fn create_uses_postgres_size_rules() {
    let filter = bloom_create(1000, 4096, 0).unwrap();
    assert_eq!(filter.m, 1 << 23);
    assert_eq!(filter.k_hash_funcs, 10);
    assert_eq!(filter.seed, 0);
    // bitset is exactly m / 8 zero bytes.
    assert_eq!(filter.bitset.len(), (filter.m / BITS_PER_BYTE) as usize);
    assert!(filter.bitset.iter().all(|&b| b == 0));
}

#[test]
fn added_elements_are_not_lacking() {
    install_hash();
    let mut filter = bloom_create(10_000, 4096, 42).unwrap();
    let abc = b"abc";

    assert!(bloom_lacks_element(&filter, abc));
    bloom_add_element(&mut filter, abc);
    assert!(!bloom_lacks_element(&filter, abc));
    assert!(bloom_prop_bits_set(&filter) > 0.0);
}

#[test]
fn no_false_negatives_over_many_elements() {
    install_hash();
    let mut filter = bloom_create(1000, 4096, 7).unwrap();

    for i in 0..1000_u32 {
        let b = i.to_le_bytes();
        bloom_add_element(&mut filter, &b);
    }
    for i in 0..1000_u32 {
        let b = i.to_le_bytes();
        assert!(!bloom_lacks_element(&filter, &b));
    }
}

#[test]
fn empty_filter_has_zero_bits_set() {
    let filter = bloom_create(1000, 4096, 0).unwrap();
    assert_eq!(bloom_prop_bits_set(&filter), 0.0);
}

#[test]
fn empty_element_is_hashable() {
    install_hash();
    // A zero-length element hashes cleanly through the safe slice path.
    let mut filter = bloom_create(1000, 4096, 0).unwrap();
    assert!(bloom_lacks_element(&filter, &[]));
    bloom_add_element(&mut filter, &[]);
    assert!(!bloom_lacks_element(&filter, &[]));
}

// ---- Golden test from test_bloomfilter.c ---------------------------------

/// `FPOSITIVE_THRESHOLD` (1%) from `test_bloomfilter.c`.
const FPOSITIVE_THRESHOLD: f64 = 0.01;

/// 1:1 port of `populate_with_dummy_strings()`: element `i` is `"i" || i`.
fn populate_with_dummy_strings(filter: &mut BloomFilter, nelements: i64) {
    for i in 0..nelements {
        let element = format!("i{i}");
        bloom_add_element(filter, element.as_bytes());
    }
}

/// 1:1 port of `nfalsepos_for_missing_strings()`: probe `i` is `"M" || i`.
fn nfalsepos_for_missing_strings(filter: &BloomFilter, nelements: i64) -> i64 {
    let mut nfalsepos: i64 = 0;
    for i in 0..nelements {
        let element = format!("M{i}");
        if !bloom_lacks_element(filter, element.as_bytes()) {
            nfalsepos += 1;
        }
    }
    nfalsepos
}

/// Port of `create_and_test_bloom()`.
fn create_and_test_bloom(power: i32, nelements: i64, seed: u64) -> i64 {
    // bloom_work_mem = ((int64) 1 << power) / (8 * 1024);
    let bloom_work_mem: i32 = (((1_i64) << power) / (8 * 1024)) as i32;

    let mut filter = bloom_create(nelements, bloom_work_mem, seed).unwrap();
    populate_with_dummy_strings(&mut filter, nelements);
    nfalsepos_for_missing_strings(&filter, nelements)
}

/// Golden test mirroring `test_bloomfilter.sql`'s sole exercised call:
///
///     SELECT test_bloomfilter(power => 23, nelements => 838861,
///                             seed => -1, tests => 1);
///
/// whose clean `.out` (no WARNING) certifies the false-positive rate stays
/// under 1%. We pin a deterministic seed.
#[test]
fn golden_test_bloomfilter_under_threshold() {
    install_hash();
    let power = 23;
    let nelements: i64 = 838861;
    let seed: u64 = 1;

    let nfalsepos = create_and_test_bloom(power, nelements, seed);

    assert!(
        (nfalsepos as f64) <= nelements as f64 * FPOSITIVE_THRESHOLD,
        "false positive rate exceeded 1% threshold: {nfalsepos} of {nelements}"
    );
}

#[test]
fn seam_roundtrip() {
    install_hash();
    // The installed seams carry the owned filter by value / by reference.
    init_seams();
    use backend_lib_bloomfilter_seams as seam;

    let mut f = seam::bloom_create::call(1000, 4096, 9).unwrap();
    let data: Vec<u8> = b"seamtest".to_vec();
    assert!(seam::bloom_lacks_element::call(&f, &data));
    seam::bloom_add_element::call(&mut f, &data);
    assert!(!seam::bloom_lacks_element::call(&f, &data));
    assert!(seam::bloom_prop_bits_set::call(&f) > 0.0);
    // Freed by drop.
}
