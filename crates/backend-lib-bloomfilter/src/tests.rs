//! Tests for the Bloom filter port.
//!
//! These exercise the internal raw-pointer implementation directly (the seam
//! layer is a thin cast wrapper). The `create_and_test_bloom` golden test
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

#[test]
fn bitset_offset_matches_c_struct() {
    // C struct: int(4) + pad(4) + uint64(8) + uint64(8) = 24 before the
    // flexible bitset member, matching c2rust's `offsetof == 24`.
    assert_eq!(bitset_offset(), 24);
}

// ---- End-to-end via the raw-pointer API ----------------------------------

#[test]
fn create_uses_postgres_size_rules() {
    let filter = bloom_create(1000, 4096, 0).unwrap();
    unsafe {
        assert_eq!((*filter).m, 1 << 23);
        assert_eq!((*filter).k_hash_funcs, 10);
        assert_eq!((*filter).seed, 0);
        bloom_free(filter);
    }
}

#[test]
fn added_elements_are_not_lacking() {
    install_hash();
    unsafe {
        let filter = bloom_create(10_000, 4096, 42).unwrap();
        let abc = b"abc";

        assert!(bloom_lacks_element(filter, abc.as_ptr(), abc.len()));
        bloom_add_element(filter, abc.as_ptr(), abc.len());
        assert!(!bloom_lacks_element(filter, abc.as_ptr(), abc.len()));
        assert!(bloom_prop_bits_set(filter) > 0.0);

        bloom_free(filter);
    }
}

#[test]
fn no_false_negatives_over_many_elements() {
    install_hash();
    unsafe {
        let filter = bloom_create(1000, 4096, 7).unwrap();

        for i in 0..1000_u32 {
            let b = i.to_le_bytes();
            bloom_add_element(filter, b.as_ptr(), b.len());
        }
        for i in 0..1000_u32 {
            let b = i.to_le_bytes();
            assert!(!bloom_lacks_element(filter, b.as_ptr(), b.len()));
        }

        bloom_free(filter);
    }
}

#[test]
fn empty_filter_has_zero_bits_set() {
    unsafe {
        let filter = bloom_create(1000, 4096, 0).unwrap();
        assert_eq!(bloom_prop_bits_set(filter), 0.0);
        bloom_free(filter);
    }
}

#[test]
fn empty_element_is_hashable() {
    install_hash();
    // len == 0 must not deref a dangling pointer.
    unsafe {
        let filter = bloom_create(1000, 4096, 0).unwrap();
        assert!(bloom_lacks_element(filter, core::ptr::NonNull::dangling().as_ptr(), 0));
        bloom_add_element(filter, core::ptr::NonNull::dangling().as_ptr(), 0);
        assert!(!bloom_lacks_element(filter, core::ptr::NonNull::dangling().as_ptr(), 0));
        bloom_free(filter);
    }
}

// ---- Golden test from test_bloomfilter.c ---------------------------------

/// `FPOSITIVE_THRESHOLD` (1%) from `test_bloomfilter.c`.
const FPOSITIVE_THRESHOLD: f64 = 0.01;

/// 1:1 port of `populate_with_dummy_strings()`: element `i` is `"i" || i`.
unsafe fn populate_with_dummy_strings(filter: *mut bloom_filter, nelements: i64) {
    for i in 0..nelements {
        let element = format!("i{i}");
        let bytes = element.as_bytes();
        bloom_add_element(filter, bytes.as_ptr(), bytes.len());
    }
}

/// 1:1 port of `nfalsepos_for_missing_strings()`: probe `i` is `"M" || i`.
unsafe fn nfalsepos_for_missing_strings(filter: *mut bloom_filter, nelements: i64) -> i64 {
    let mut nfalsepos: i64 = 0;
    for i in 0..nelements {
        let element = format!("M{i}");
        let bytes = element.as_bytes();
        if !bloom_lacks_element(filter, bytes.as_ptr(), bytes.len()) {
            nfalsepos += 1;
        }
    }
    nfalsepos
}

/// Port of `create_and_test_bloom()`.
unsafe fn create_and_test_bloom(power: i32, nelements: i64, seed: u64) -> i64 {
    // bloom_work_mem = ((int64) 1 << power) / (8 * 1024);
    let bloom_work_mem: i32 = (((1_i64) << power) / (8 * 1024)) as i32;

    let filter = bloom_create(nelements, bloom_work_mem, seed).unwrap();
    populate_with_dummy_strings(filter, nelements);
    let nfalsepos = nfalsepos_for_missing_strings(filter, nelements);
    bloom_free(filter);
    nfalsepos
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

    let nfalsepos = unsafe { create_and_test_bloom(power, nelements, seed) };

    assert!(
        (nfalsepos as f64) <= nelements as f64 * FPOSITIVE_THRESHOLD,
        "false positive rate exceeded 1% threshold: {nfalsepos} of {nelements}"
    );
}

#[test]
fn seam_roundtrip() {
    install_hash();
    // The installed seams cast to/from the opaque pointer and call through.
    init_seams();
    use backend_lib_bloomfilter_seams as seam;

    let f = seam::bloom_create::call(1000, 4096, 9).unwrap();
    let data: Vec<u8> = b"seamtest".to_vec();
    assert!(seam::bloom_lacks_element::call(f, data.as_ptr(), data.len()));
    seam::bloom_add_element::call(f, data.as_ptr(), data.len());
    assert!(!seam::bloom_lacks_element::call(f, data.as_ptr(), data.len()));
    seam::bloom_free::call(f);
}
