//! Unit tests for the bloom-filter sizing/codec/bitmap arithmetic (the
//! in-crate, seam-free logic of brin_bloom.c).

use super::*;

/// The hashfn seam the bloom filter calls must be installed for the
/// add/contains tests; install the real owner.
fn install_hashfn() {
    use core::sync::atomic::{AtomicBool, Ordering};
    static DONE: AtomicBool = AtomicBool::new(false);
    if DONE
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        common_hashfn::init_seams();
    }
}

#[test]
fn filter_size_matches_c_arithmetic() {
    // 100 distinct, 1% fp: -(100*ln(0.01))/(ln 2)^2 = ceil(958.5...) -> nbits,
    // rounded to whole bytes, k = round(ln2 * nbits / ndistinct).
    let (nbytes, nbits, nhashes) = bloom_filter_size(100, 0.01);
    assert!(nbits > 0 && nbits % 8 == 0);
    assert_eq!(nbits, nbytes * 8);
    assert!(nhashes >= 1);
    // ndistinct=100, p=0.01 -> ~959 bits -> 120 bytes -> 960 bits, k ~= 7.
    assert_eq!(nbytes, 120);
    assert_eq!(nbits, 960);
    assert_eq!(nhashes, 7);
}

#[test]
fn serialize_roundtrip() {
    let f = BloomFilter::init(64, 0.01).unwrap();
    let bytes = f.serialize();
    // 4-byte varlena header carries the full length.
    assert_eq!(
        u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize,
        bytes.len()
    );
    let g = BloomFilter::deserialize(&bytes);
    assert_eq!(f, g);
}

#[test]
fn add_then_contains() {
    install_hashfn();
    let mut f = BloomFilter::init(64, 0.01).unwrap();
    let mut updated = false;
    f.add_value(0x1234_5678, &mut updated);
    assert!(updated);
    assert!(f.contains_value(0x1234_5678));
    assert!(f.nbits_set > 0);
}

#[test]
fn second_add_of_same_value_sets_no_new_bits() {
    install_hashfn();
    let mut f = BloomFilter::init(64, 0.01).unwrap();
    let mut updated = false;
    f.add_value(42, &mut updated);
    let bits_after_first = f.nbits_set;
    let mut updated2 = false;
    f.add_value(42, &mut updated2);
    assert!(!updated2);
    assert_eq!(f.nbits_set, bits_after_first);
}

#[test]
fn union_ors_bitmaps_and_recounts() {
    install_hashfn();
    let mut a = BloomFilter::init(64, 0.01).unwrap();
    let mut b = BloomFilter::init(64, 0.01).unwrap();
    let mut u = false;
    a.add_value(1, &mut u);
    b.add_value(2, &mut u);
    let nbytes = (a.nbits / 8) as usize;
    for i in 0..nbytes {
        a.data[i] |= b.data[i];
    }
    a.nbits_set = a.data[..nbytes].iter().map(|x| x.count_ones()).sum();
    assert!(a.contains_value(1));
    assert!(a.contains_value(2));
}

#[test]
fn ndistinct_negative_is_relative_to_maxtuples() {
    // opts default n_distinct_per_range = -0.1 -> 10% of maxtuples, clamped.
    let opts = BloomOptions {
        n_distinct_per_range: BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
        false_positive_rate: BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
    };
    let n = brin_bloom_get_ndistinct(PAGES_PER_RANGE_DEFAULT, Some(&opts));
    // 10% of MaxHeapTuplesPerPage*128, well above the min, below maxtuples.
    let maxtuples = (MAX_HEAP_TUPLES_PER_PAGE as f64) * (PAGES_PER_RANGE_DEFAULT as f64);
    assert!(n as f64 <= maxtuples);
    assert!(n >= BLOOM_MIN_NDISTINCT_PER_RANGE as i32);
}

#[test]
fn ndistinct_clamped_to_min() {
    // Tiny positive ndistinct is clamped up to BLOOM_MIN_NDISTINCT_PER_RANGE.
    let opts = BloomOptions {
        n_distinct_per_range: 1.0,
        false_positive_rate: BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
    };
    let n = brin_bloom_get_ndistinct(PAGES_PER_RANGE_DEFAULT, Some(&opts));
    assert_eq!(n, BLOOM_MIN_NDISTINCT_PER_RANGE as i32);
}

#[test]
fn summary_out_text() {
    let f = BloomFilter::init(64, 0.01).unwrap();
    let s = brin_bloom_summary_out(&f.serialize());
    assert!(s.starts_with("{mode: hashed  nhashes:"));
    assert!(s.ends_with('}'));
}
