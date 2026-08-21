//! SEAL-SPEED-2 D3 — sampled-election suite: the deterministic-sample law
//! (pure function of the input, never RNG), the exact-parameter law (a
//! sampled BYTE_FOR election's width byte is NEVER an estimate — the
//! winner-facts pass computes it over every frame), CONST/NoValues parity
//! with the census, the sampled-float never-false-elect law, and the
//! carried-facts validity of the sampled winner.

use super::{roundtrip_int_corpus, GranuleData};
use crate::election::{
    elect_float_carry, elect_float_sampled_carry, elect_int_carry, elect_int_sampled_carry,
    sample_ords, Election, SampleFamilies,
};
use pgrc2_format::class::StorageClass;
use pgrc2_format::enc::EncodingId;

#[test]
fn sample_ords_deterministic_distinct_sorted() {
    for count in [0usize, 1, 5, 8, 9, 100, 101, 799, 800, 801, 4096, 100_000] {
        let a = sample_ords(count);
        let b = sample_ords(count);
        assert_eq!(a, b, "pure function of count ({count})");
        assert!(a.windows(2).all(|w| w[0] < w[1]), "strictly increasing ({count})");
        assert!(a.iter().all(|&o| o < count.max(1) || count == 0), "in range ({count})");
        if count <= 8 {
            assert_eq!(a.len(), count, "small counts sample everything");
        } else {
            assert!(a.len() >= 8, "minimum run floor ({count})");
            assert!(!a.is_empty() && a[0] == 0, "first unit always sampled ({count})");
            // ~1% target above the floor.
            assert!(a.len() <= count.div_ceil(100).max(8), "run budget ({count})");
        }
    }
}

#[test]
fn sampled_int_parity_and_exact_params() {
    let mut seed = 0xD3_u64;
    for shape in 0..12u32 {
        let g0 = roundtrip_int_corpus(shape, 8192, &mut seed);
        let g1 = roundtrip_int_corpus(shape.wrapping_add(1), 8192, &mut seed);
        let g2 = roundtrip_int_corpus(shape.wrapping_add(2), 3000, &mut seed);
        for &signed in &[true, false] {
            let class = StorageClass::ByvalWord { width: 8, signed };
            let inputs = [g0.input(class), g1.input(class), g2.input(class)];
            for &(fused, cold) in &[(false, false), (true, false), (false, true), (true, true)] {
                let (census, census_carry) = elect_int_carry(&inputs, 8, signed, fused, cold);
                let (sampled, carry) = elect_int_sampled_carry(&inputs, 8, signed, fused, cold);
                // Determinism: the sampled election is a pure function.
                let (sampled2, _) = elect_int_sampled_carry(&inputs, 8, signed, fused, cold);
                assert_eq!(sampled, sampled2, "deterministic sample");
                assert_eq!(
                    carry.frames.len(),
                    census_carry.frames.len(),
                    "carry covers every frame"
                );
                match (census, sampled) {
                    // CONST / NoValues parity is EXACT (the winner-facts
                    // pass tracks constancy over every row).
                    (
                        Election::Elected { encoding: EncodingId::Const, .. },
                        Election::Elected { encoding, .. },
                    ) => {
                        assert_eq!(encoding, EncodingId::Const, "CONST parity");
                    }
                    (Election::Demoted { reason: r1, .. }, Election::Demoted { reason: r2, .. }) => {
                        // Demotion parity holds on these corpora when both
                        // demote; reasons must agree.
                        assert_eq!(r1, r2, "demotion reason parity");
                    }
                    (
                        Election::Elected { encoding: e1, width: w1, candidate_bytes: c1, .. },
                        Election::Elected { encoding: e2, width: w2, candidate_bytes: c2, .. },
                    ) => {
                        // Same family ⇒ identical exact parameters (widths
                        // and sizes come from the full winner pass, never
                        // the sample).
                        if e1 == e2 {
                            assert_eq!(w1, w2, "exact width, shape={shape}");
                            assert_eq!(c1, c2, "exact size, shape={shape}");
                        }
                    }
                    // A family/gate flip near the margin is the D3 regret
                    // class — legal, bounded by the adoption gate.
                    _ => {}
                }
                // Width safety on a sampled BYTE_FOR election: the elected
                // width must fit EVERY frame's range (the correctness law
                // the winner-facts pass exists for).
                if let Election::Elected {
                    encoding: EncodingId::ByteFor,
                    width,
                    ..
                } = sampled
                {
                    for f in &carry.frames {
                        assert!(
                            crate::bytefor::width_for_range(f.range) <= width,
                            "sampled width covers every frame (shape={shape})"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn sampled_int_const_and_allnull_parity() {
    let class = StorageClass::ByvalWord { width: 8, signed: true };
    let konst = GranuleData { rows: 8192, datums: vec![7; 8192], validity: None };
    let inputs = [konst.input(class)];
    let (census, _) = elect_int_carry(&inputs, 8, true, true, true);
    let (sampled, _) = elect_int_sampled_carry(&inputs, 8, true, true, true);
    assert_eq!(census, sampled, "CONST short-circuit parity");

    let all_null = GranuleData {
        rows: 4096,
        datums: vec![0; 4096],
        validity: Some(vec![0u64; 64]),
    };
    let inputs = [all_null.input(class)];
    let (census, _) = elect_int_carry(&inputs, 8, true, true, true);
    let (sampled, _) = elect_int_sampled_carry(&inputs, 8, true, true, true);
    assert_eq!(census, sampled, "NoValues parity");
}

/// A near-constant stream with outlier spikes ONLY outside the sampled
/// frames: the family is chosen from the sample, but the width/size must
/// still be exact — the adversarial shape for any estimate-parameter bug.
#[test]
fn sampled_int_unsampled_outliers_stay_exact() {
    let rows = 128 * 1024u32; // 128 frames
    let mut datums = vec![100u64; rows as usize];
    let ords = sample_ords((rows / 1024) as usize);
    // Poison one frame that is NOT in the sample: a range needing delta
    // width 2 where every sampled frame needs width 1.
    let poisoned = (0..(rows / 1024) as usize)
        .find(|o| !ords.contains(o))
        .expect("an unsampled frame exists");
    datums[poisoned * 1024 + 7] = 100 + 65_535;
    let gd = GranuleData { rows, datums, validity: None };
    let class = StorageClass::ByvalWord { width: 8, signed: false };
    let inputs = [gd.input(class)];
    // Flat posture (BYTE_FOR the only arm) so the family is forced and the
    // width path is the test subject.
    let (sampled, carry) = elect_int_sampled_carry(&inputs, 8, false, false, false);
    let Election::Elected { encoding: EncodingId::ByteFor, width, candidate_bytes, .. } = sampled
    else {
        panic!("fixture must elect BYTE_FOR: {sampled:?}");
    };
    // The poisoned frame's range demands width 2; the elected width must
    // cover it even though the sample never saw it (sample max is 1).
    let widest = carry
        .frames
        .iter()
        .map(|f| crate::bytefor::width_for_range(f.range))
        .max()
        .unwrap();
    assert_eq!(width, widest, "width is the full-pass max, not the sample's");
    assert_eq!(width, 2, "the poisoned frame's width");
    // And the exact size prices that width.
    let expect: usize = inputs
        .iter()
        .map(|g| crate::bytefor::payload_bytes(g.rows, width))
        .sum();
    assert_eq!(candidate_bytes, expect, "exact size at the exact width");
    // Census agreement: one arm, exact gate — identical election.
    let (census, _) = elect_int_carry(&inputs, 8, false, false, false);
    assert_eq!(sampled, census, "single-arm sampled == census");
}

#[test]
fn sampled_float_never_falsely_elects() {
    use super::roundtrip_float_corpus;
    let mut seed = 0xF10A7_u64;
    for shape in 0..8u32 {
        let g0 = roundtrip_float_corpus(shape, 8192, &mut seed);
        let g1 = roundtrip_float_corpus(shape.wrapping_add(1), 3000, &mut seed);
        let inputs = [g0.input(StorageClass::F64), g1.input(StorageClass::F64)];
        let (census, _) = elect_float_carry(&inputs, StorageClass::F64);
        let (sampled, sc) = elect_float_sampled_carry(&inputs, StorageClass::F64);
        let (sampled2, _) = elect_float_sampled_carry(&inputs, StorageClass::F64);
        assert_eq!(sampled, sampled2, "deterministic sample");
        match (census, sampled) {
            (Election::Demoted { .. }, Election::Elected { .. }) => {
                panic!("sampled float elected what the census demoted (final gate is exact)");
            }
            (
                Election::Elected { encoding: e1, candidate_bytes: c1, .. },
                Election::Elected { encoding: e2, candidate_bytes: c2, .. },
            ) => {
                // Survivors run the census arm in full: identical outcome.
                assert_eq!(e1, e2, "same stamp, shape={shape}");
                assert_eq!(c1, c2, "same exact bytes, shape={shape}");
                assert!(sc.is_some(), "carry rides the elected sampled arm");
            }
            // Sample-gate rejection of a census win = the regret class
            // (missed election, verbatim ships — valid).
            _ => {}
        }
    }
}

#[test]
fn sample_families_parse_vocabulary() {
    assert_eq!(SampleFamilies::parse(""), SampleFamilies::NONE);
    assert_eq!(SampleFamilies::parse("0"), SampleFamilies::NONE);
    assert_eq!(SampleFamilies::parse("1"), SampleFamilies::ALL);
    let f = SampleFamilies::parse("int,fsst");
    assert!(f.int && f.fsst && !f.float);
    let f = SampleFamilies::parse(" float ");
    assert!(f.float && !f.int && !f.fsst);
    assert_eq!(SampleFamilies::parse("bogus"), SampleFamilies::NONE);
}

/// Near-sorted cold columns (DELTA_FOR's home turf): the sampled arm's
/// DF estimate lands inside the verify margin, the exact chain runs, and
/// the election is EXACTLY the census's — the smoke-attributed regret
/// class (DELTA_FOR family mispicks) is structurally closed.
#[test]
fn sampled_int_df_competitive_matches_census() {
    let mut seed = 0x5EA1_u64;
    for shape in [4u32, 3, 0] {
        let g0 = roundtrip_int_corpus(shape, 8192, &mut seed);
        let g1 = roundtrip_int_corpus(shape, 8192, &mut seed);
        let inputs = [
            g0.input(StorageClass::ByvalWord { width: 8, signed: true }),
            g1.input(StorageClass::ByvalWord { width: 8, signed: true }),
        ];
        let (census, census_carry) = elect_int_carry(&inputs, 8, true, true, true);
        let (sampled, carry) = elect_int_sampled_carry(&inputs, 8, true, true, true);
        if let Election::Elected { encoding: EncodingId::DeltaFor, .. } = census {
            assert_eq!(sampled, census, "DF-winning shape {shape} must match census");
            // And the DF facts must be the exact chain's.
            for (a, b) in carry.frames.iter().zip(census_carry.frames.iter()) {
                assert_eq!(a.df_width, b.df_width, "df_width parity, shape {shape}");
                assert_eq!(a.df_first, b.df_first, "df_first parity, shape {shape}");
            }
        }
    }
}
