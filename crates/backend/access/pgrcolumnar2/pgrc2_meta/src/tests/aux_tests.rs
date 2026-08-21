//! Aux-plane batteries: PSMA build+probe coverage properties ("candidate
//! ranges always cover matches" — the slice clause), the bloom arming
//! policy (unclustered ∧ NDV floor) and no-false-negatives law, the NDV
//! register merge algebra (associative, commutative, idempotent), and the
//! section-body wire round-trips through the probe-side locators.

use super::{Rng, TestColumn, TestValue};
use crate::bloom::{
    bloom_armed, bloom_block_for, bloom_insert, bloom_may_contain, BLOOM_BYTES_PER_GRANULE_DEFAULT,
    BLOOM_K_DEFAULT, BLOOM_NDV_FLOOR,
};
use crate::builder::ColumnMeta;
use crate::format::abi::ColumnMetaBuilder;
use crate::format::class::{CollationClass, StorageClass};
use crate::format::meta::{Sortedness, PSMA_BLOCK_LEN};
use crate::format::part::SectionKind;
use crate::ndv::{Hll, NDV_PRECISION};
use crate::profile::{MetaProfile, TypeSemantics};
use crate::psma::{
    psma_block_for, psma_candidates_eq, psma_candidates_range, psma_index, psma_shift, PsmaAcc,
};

fn int_class() -> StorageClass {
    StorageClass::ByvalWord {
        width: 8,
        signed: true,
    }
}

fn int_profile() -> MetaProfile {
    MetaProfile::derive(int_class(), CollationClass::C, TypeSemantics::SignedInt).unwrap()
}

// ---------------------------------------------------------------------------
// PSMA
// ---------------------------------------------------------------------------

#[test]
fn psma_eq_candidates_always_cover_matches() {
    let mut rng = Rng::new(0xA0C5_0001);
    for round in 0..30 {
        // Adversarial spreads: tight ranges, huge ranges, duplicates.
        let (lo, span): (i64, u64) = match round % 4 {
            0 => (-50, 100),
            1 => (i64::MIN / 2, 1 << 40),
            2 => (0, 3),
            _ => (i64::MAX - 1000, 900),
        };
        let keys: Vec<i64> = (0..500)
            .map(|_| lo.wrapping_add((rng.next() % (span + 1)) as i64))
            .collect();
        let kmin = *keys.iter().min().unwrap();
        let kmax = *keys.iter().max().unwrap();
        let shift = psma_shift(kmin, kmax);
        let mut acc = PsmaAcc::default();
        for (row, &k) in keys.iter().enumerate() {
            acc.observe(psma_index(kmin, shift, k), row as u32);
        }
        let mut block = Vec::new();
        acc.encode_into(&mut block);
        assert_eq!(block.len(), PSMA_BLOCK_LEN);
        // Probes: every present key + absent keys + out-of-zone keys.
        let mut probes: Vec<i64> = keys.iter().copied().take(64).collect();
        probes.push(kmin);
        probes.push(kmax);
        probes.push(kmin.saturating_sub(1));
        probes.push(kmax.saturating_add(1));
        for _ in 0..32 {
            probes.push(lo.wrapping_add((rng.next() % (2 * span + 1)) as i64));
        }
        let mut checked = 0;
        for &p in &probes {
            let (a, b) = psma_candidates_eq(&block, kmin, kmax, p).expect("well-formed block");
            // COVERAGE LAW: every row holding p lies inside [a, b).
            for (row, &k) in keys.iter().enumerate() {
                if k == p {
                    assert!(
                        (a as usize) <= row && row < (b as usize),
                        "candidate range [{a},{b}) missed row {row} holding the probe"
                    );
                }
            }
            checked += 1;
        }
        assert!(checked > 0);
    }
}

#[test]
fn psma_range_candidates_always_cover_matches() {
    let mut rng = Rng::new(0xA0C5_0002);
    let keys: Vec<i64> = (0..400).map(|_| (rng.next() % 10_000) as i64).collect();
    let kmin = *keys.iter().min().unwrap();
    let kmax = *keys.iter().max().unwrap();
    let shift = psma_shift(kmin, kmax);
    let mut acc = PsmaAcc::default();
    for (row, &k) in keys.iter().enumerate() {
        acc.observe(psma_index(kmin, shift, k), row as u32);
    }
    let mut block = Vec::new();
    acc.encode_into(&mut block);
    for _ in 0..100 {
        let a = (rng.next() % 12_000) as i64 - 1000;
        let b = a + (rng.next() % 4_000) as i64;
        let (clo, chi) = psma_candidates_range(&block, kmin, kmax, a, b).expect("well-formed");
        for (row, &k) in keys.iter().enumerate() {
            if k >= a && k <= b {
                assert!(
                    (clo as usize) <= row && row < (chi as usize),
                    "range candidates [{clo},{chi}) missed row {row}"
                );
            }
        }
    }
}

#[test]
fn psma_malformed_blocks_decline() {
    assert!(psma_candidates_eq(&[0u8; 10], 0, 100, 5).is_none());
    assert!(psma_candidates_range(&[0u8; 1023], 0, 100, 1, 2).is_none());
}

#[test]
fn psma_section_roundtrip_through_locator() {
    // Three granules: g0 constant (unarmed), g1 spread (armed), g2 spread
    // (armed) — the armed bitmap + rank walk must find exactly g1/g2.
    let mut b = ColumnMeta::new(int_profile());
    let granules: Vec<Vec<Option<TestValue>>> = vec![
        (0..10).map(|_| Some(TestValue::Word(7))).collect(),
        (0..10).map(|i| Some(TestValue::Word(i * 3))).collect(),
        (0..10).map(|i| Some(TestValue::Word(1000 - i))).collect(),
    ];
    for (g, rows) in granules.iter().enumerate() {
        let col = TestColumn::new(int_class(), rows);
        b.observe_granule(&col.input(), g as u32);
        b.seal_granule(g as u32);
    }
    b.seal_band(0);
    b.seal_part();
    let sections = b.aux_sections();
    let psma = &sections
        .iter()
        .find(|(k, _)| *k == SectionKind::Psma)
        .expect("psma section exists")
        .1;
    assert_eq!(
        psma_block_for(psma, 3, 0).expect("well-formed"),
        None,
        "constant granule is unarmed"
    );
    let b1 = psma_block_for(psma, 3, 1)
        .expect("well-formed")
        .expect("armed");
    let b2 = psma_block_for(psma, 3, 2)
        .expect("well-formed")
        .expect("armed");
    assert_eq!(b1.len(), PSMA_BLOCK_LEN);
    assert_eq!(b2.len(), PSMA_BLOCK_LEN);
    assert_ne!(b1, b2);
    // Out-of-range granule ordinal is a typed refusal.
    assert!(psma_block_for(psma, 3, 3).is_err());
}

// ---------------------------------------------------------------------------
// bloom
// ---------------------------------------------------------------------------

#[test]
fn bloom_never_false_negative() {
    let mut rng = Rng::new(0xA0C5_0003);
    let mut block = vec![0u8; BLOOM_BYTES_PER_GRANULE_DEFAULT as usize];
    let values: Vec<Vec<u8>> = (0..2000)
        .map(|_| {
            let len = rng.below(20) as usize;
            (0..len).map(|_| rng.next() as u8).collect()
        })
        .collect();
    for v in &values {
        bloom_insert(&mut block, BLOOM_K_DEFAULT, v);
    }
    for v in &values {
        assert!(
            bloom_may_contain(&block, BLOOM_K_DEFAULT, v),
            "a bloom false negative is the wrong-results class"
        );
    }
}

#[test]
fn bloom_absence_is_useful() {
    // Sanity that the filter actually filters (not a soundness law): at
    // 2 KiB / k=4 over 1000 short keys, absent probes mostly miss.
    let mut block = vec![0u8; BLOOM_BYTES_PER_GRANULE_DEFAULT as usize];
    for i in 0..1000u64 {
        bloom_insert(&mut block, BLOOM_K_DEFAULT, &i.to_le_bytes());
    }
    let misses = (1_000_000u64..1_002_000)
        .filter(|i| !bloom_may_contain(&block, BLOOM_K_DEFAULT, &i.to_le_bytes()))
        .count();
    assert!(
        misses > 1800,
        "fp rate degenerate: only {misses}/2000 misses"
    );
}

#[test]
fn bloom_arming_policy() {
    // unclustered ∧ NDV floor.
    assert!(bloom_armed(Sortedness::Unknown, BLOOM_NDV_FLOOR));
    assert!(bloom_armed(Sortedness::Unknown, BLOOM_NDV_FLOOR + 100));
    assert!(!bloom_armed(Sortedness::Unknown, BLOOM_NDV_FLOOR - 1));
    assert!(!bloom_armed(Sortedness::Ascending, 1000));
    assert!(!bloom_armed(Sortedness::Descending, 1000));
    assert!(!bloom_armed(Sortedness::Constant, 1000));
}

/// #598 leg 2, the seal side of the collation gate: a nondeterministic-
/// collation text column arms NOTHING equality-shaped — no bloom section
/// even over the exact shape that always arms under a deterministic
/// collation (unclustered, NDV over the floor). A byte-hash bloom's
/// absence proof requires byte-eq == value-eq, which a nondeterministic
/// collation does not grant: equal-under-collation, byte-different probes
/// would read definite-absent.
#[test]
fn nondeterministic_collation_seals_no_bloom() {
    let text = |coll| {
        MetaProfile::derive(
            StorageClass::VarlenaVerbatim,
            coll,
            TypeSemantics::TextCollated,
        )
        .unwrap()
    };
    // 6000 distinct values: comfortably above the OD-11 4096-NDV arming
    // floor even under the HLL estimator's ±3% band, so the control's
    // arming is decided by the collation gate alone.
    let mut vals: Vec<Option<TestValue>> = (0..6000)
        .map(|i| Some(TestValue::Bytes(format!("v{i:05}").into_bytes())))
        .collect();
    Rng::new(0xA0C5_0007).shuffle(&mut vals);
    let drive = |profile: MetaProfile| {
        let mut b = ColumnMeta::new(profile);
        let col = TestColumn::new(StorageClass::VarlenaVerbatim, &vals);
        b.observe_granule(&col.input(), 0);
        b.seal_granule(0);
        b.seal_band(0);
        b.seal_part();
        b.aux_sections()
    };
    // Control: the same data under a deterministic collation DOES arm —
    // so the absence below is the collation gate, not the arming policy.
    let det = drive(text(CollationClass::OtherDeterministic));
    assert!(
        det.iter().any(|(k, _)| *k == SectionKind::Bloom),
        "control: a deterministic collation arms the bloom over this shape"
    );
    // Nondeterministic: no bloom section (and no NDV — value hashing off).
    let nondet_profile = text(CollationClass::Nondeterministic);
    assert!(!nondet_profile.eq_bloomable, "the profile lattice leg");
    assert!(!nondet_profile.ndv);
    let nondet = drive(nondet_profile);
    assert!(
        !nondet.iter().any(|(k, _)| *k == SectionKind::Bloom),
        "a nondeterministic collation must never seal a byte-hash bloom"
    );
}

#[test]
fn bloom_arming_end_to_end() {
    // Sorted granule: bloom built but NOT armed (dropped at seal). 8192
    // distinct values — far above the OD-11 4096 floor, so the refusal
    // below is the sortedness leg, not the floor.
    let mut b = ColumnMeta::new(int_profile());
    let rows: Vec<Option<TestValue>> = (0..8192).map(|i| Some(TestValue::Word(i))).collect();
    let col = TestColumn::new(int_class(), &rows);
    b.observe_granule(&col.input(), 0);
    b.seal_granule(0);
    b.seal_band(0);
    b.seal_part();
    assert!(
        !b.aux_sections()
            .iter()
            .any(|(k, _)| *k == SectionKind::Bloom),
        "a clustered granule must not arm its bloom"
    );
    // Shuffled: armed.
    let mut shuffled = rows.clone();
    Rng::new(0xA0C5_0004).shuffle(&mut shuffled);
    let mut b = ColumnMeta::new(int_profile());
    let col = TestColumn::new(int_class(), &shuffled);
    b.observe_granule(&col.input(), 0);
    b.seal_granule(0);
    b.seal_band(0);
    b.seal_part();
    let sections = b.aux_sections();
    let bloom = &sections
        .iter()
        .find(|(k, _)| *k == SectionKind::Bloom)
        .expect("unclustered granule arms its bloom")
        .1;
    let (k, block) = bloom_block_for(bloom, 1, 0)
        .expect("well-formed")
        .expect("armed");
    assert_eq!(k, BLOOM_K_DEFAULT);
    assert_eq!(block.len(), BLOOM_BYTES_PER_GRANULE_DEFAULT as usize);
    // Every present value answers maybe-present.
    for row in &shuffled {
        if let Some(TestValue::Word(w)) = row {
            assert!(bloom_may_contain(block, k, &(*w as i64).to_le_bytes()));
        }
    }
    // Sub-floor shuffled granule: NOT armed. NDV 2000 sits in the HLL's
    // near-exact linear-counting regime, WOULD have armed under v3's
    // floor-8, and must not under the OD-11 4096 floor — the raise's
    // born-RED direction witnessed at the arming grain.
    let rows: Vec<Option<TestValue>> = (0..8192).map(|i| Some(TestValue::Word(i % 2000))).collect();
    let mut shuffled = rows.clone();
    Rng::new(0xA0C5_0005).shuffle(&mut shuffled);
    let mut b = ColumnMeta::new(int_profile());
    let col = TestColumn::new(int_class(), &shuffled);
    b.observe_granule(&col.input(), 0);
    b.seal_granule(0);
    b.seal_band(0);
    b.seal_part();
    assert!(
        !b.aux_sections()
            .iter()
            .any(|(k, _)| *k == SectionKind::Bloom),
        "below the NDV floor the bloom must not arm"
    );
}

#[test]
fn bloom_locator_refuses_malformed_bodies() {
    assert!(bloom_block_for(&[1, 2, 3], 1, 0).is_err()); // truncated header
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes()); // k = 0
    body.extend_from_slice(&8u32.to_le_bytes());
    body.push(0x01);
    assert!(bloom_block_for(&body, 1, 0).is_err());
    // Armed bit set but block truncated.
    let mut body = Vec::new();
    body.extend_from_slice(&4u32.to_le_bytes());
    body.extend_from_slice(&2048u32.to_le_bytes());
    body.push(0x01);
    assert!(bloom_block_for(&body, 1, 0).is_err());
}

// ---------------------------------------------------------------------------
// NDV registers
// ---------------------------------------------------------------------------

fn hll_from(seed: u64, n: usize, salt: u64) -> Hll {
    let mut h = Hll::default();
    let mut rng = Rng::new(seed);
    for _ in 0..n {
        let v = rng.next() ^ salt;
        h.observe(&v.to_le_bytes());
    }
    h
}

#[test]
fn ndv_merge_is_commutative_associative_idempotent() {
    let a = hll_from(1, 500, 0);
    let b = hll_from(2, 300, 0xABCD);
    let c = hll_from(3, 700, 0x1234_5678);
    // Commutative.
    let mut ab = a.clone();
    ab.merge(&b);
    let mut ba = b.clone();
    ba.merge(&a);
    assert_eq!(ab, ba);
    // Associative.
    let mut ab_c = ab.clone();
    ab_c.merge(&c);
    let mut bc = b.clone();
    bc.merge(&c);
    let mut a_bc = a.clone();
    a_bc.merge(&bc);
    assert_eq!(ab_c, a_bc);
    // Idempotent.
    let mut aa = a.clone();
    aa.merge(&a);
    assert_eq!(aa, a);
}

#[test]
fn ndv_estimates_are_sane() {
    // Estimates are estimates — generous deterministic bounds, never a
    // metadata-answered-aggregate obligation.
    for (n, seed) in [(10usize, 7u64), (100, 8), (1000, 9), (5000, 10)] {
        let h = hll_from(seed, n, 0);
        let est = h.estimate() as f64;
        let n = n as f64;
        assert!(
            est > n * 0.7 && est < n * 1.4,
            "estimate {est} too far from true {n}"
        );
    }
    // Duplicates do not inflate: 10k copies of 5 values ≈ 5.
    let mut h = Hll::default();
    for i in 0..10_000u64 {
        h.observe(&(i % 5).to_le_bytes());
    }
    let est = h.estimate();
    assert!((3..=7).contains(&est), "duplicate stream estimated {est}");
}

#[test]
fn ndv_section_roundtrip_and_cross_part_merge() {
    let a = hll_from(11, 800, 0);
    let mut body = Vec::new();
    a.encode_section(&mut body);
    assert_eq!(body.len(), 8 + (1usize << NDV_PRECISION));
    let (hdr, back) = Hll::decode_section(&body).expect("round-trips");
    assert_eq!(hdr.algo, 1);
    assert_eq!(hdr.precision, NDV_PRECISION);
    assert_eq!(hdr.reg_len, 1u32 << NDV_PRECISION);
    assert_eq!(back, a);
    // Cross-part merge: decode two parts' sections, merge, estimate the
    // union (the v8 mergeable-form law in action).
    let b = hll_from(12, 800, 0xFFFF);
    let mut body_b = Vec::new();
    b.encode_section(&mut body_b);
    let (_, mut union) = Hll::decode_section(&body).unwrap();
    let (_, hb) = Hll::decode_section(&body_b).unwrap();
    union.merge(&hb);
    let est = union.estimate() as f64;
    assert!(
        est > 1600.0 * 0.7 && est < 1600.0 * 1.4,
        "union estimate {est}"
    );
    // Malformed sections are typed refusals.
    assert!(Hll::decode_section(&body[..7]).is_err());
    let mut bad = body.clone();
    bad[0] = 9; // unknown algo
    assert!(Hll::decode_section(&bad).is_err());
    let mut bad = body.clone();
    bad[1] = NDV_PRECISION + 1; // precision/len disagreement
    assert!(Hll::decode_section(&bad).is_err());
}

// ---------------------------------------------------------------------------
// XC-5 engagement census (M3-L2): the counters exist WITH the sections —
// consult wiring witnessed against a real sealed granule's bloom + PSMA.
// ---------------------------------------------------------------------------

#[test]
fn xc5_census_counts_bloom_and_psma_engagement() {
    use crate::census::{evaluate_censused, psma_candidates_eq_censused, MetaEngagement};
    use crate::key::signed_word_key;
    use crate::lower::{lower_const, ConstInput};
    use crate::format::meta::Verdict;
    use crate::verdict::{BloomEvidence, GrainFacts, ZonePredicate};

    // 8192 distinct shuffled evens: bloom armed (over the OD-11 floor,
    // unclustered), PSMA armed (exact keys, kmin < kmax).
    let rows: Vec<Option<TestValue>> =
        (0..8192).map(|i| Some(TestValue::Word((i * 2) as u64))).collect();
    let mut shuffled = rows.clone();
    Rng::new(0xA0C5_0009).shuffle(&mut shuffled);
    let mut b = ColumnMeta::new(int_profile());
    let col = TestColumn::new(int_class(), &shuffled);
    b.observe_granule(&col.input(), 0);
    let rec = b.seal_granule(0);
    b.seal_band(0);
    b.seal_part();
    let sections = b.aux_sections();
    let bloom_body = &sections
        .iter()
        .find(|(k, _)| *k == SectionKind::Bloom)
        .expect("armed")
        .1;
    let (k, block) = bloom_block_for(bloom_body, 1, 0)
        .expect("well-formed")
        .expect("armed");

    // Bloom leg: an ABSENT odd value inside [min, max] whose block probe
    // is negative (selected deterministically — engagement, not FP luck).
    let probe_val: i64 = (1..16384i64)
        .step_by(2)
        .find(|v| !crate::bloom::bloom_may_contain(block, k, &v.to_le_bytes()))
        .expect("a bloom-negative absent odd exists");
    let mut census = MetaEngagement::default();
    let facts = GrainFacts { rows: 8192 };
    let c = lower_const(&int_profile(), ConstInput::Word(probe_val as u64))
        .lowered()
        .unwrap();
    let v = evaluate_censused(
        &int_profile(),
        facts,
        &rec,
        &ZonePredicate::Eq(c),
        Some(BloomEvidence { k, block }),
        &mut census,
    );
    assert_eq!(v, Verdict::AllFail);
    assert_eq!(census.bloom_probes, 1);
    assert_eq!(
        census.bloom_definite_absent, 1,
        "keys alone say Mixed here — the skip is the BLOOM's and the census attributes it"
    );

    // A PRESENT value: probe counted, no definite-absent inflation.
    let v2 = evaluate_censused(
        &int_profile(),
        facts,
        &rec,
        &ZonePredicate::Eq(
            lower_const(&int_profile(), ConstInput::Word(4096))
                .lowered()
                .unwrap(),
        ),
        Some(BloomEvidence { k, block }),
        &mut census,
    );
    assert_ne!(v2, Verdict::AllFail);
    assert_eq!(census.bloom_probes, 2);
    assert_eq!(census.bloom_definite_absent, 1);

    // PSMA leg: the armed block narrows an equality probe's row window.
    let psma_body = &sections
        .iter()
        .find(|(k2, _)| *k2 == SectionKind::Psma)
        .expect("psma armed")
        .1;
    let pblock = psma_block_for(psma_body, 1, 0)
        .expect("well-formed")
        .expect("armed");
    let r = psma_candidates_eq_censused(
        pblock,
        rec.min_key,
        rec.max_key,
        signed_word_key(4096u64).raw(),
        8192,
        &mut census,
    );
    assert!(r.is_some());
    assert_eq!(census.psma_probes, 1);
    assert_eq!(
        census.psma_windows_narrowed, 1,
        "a 256-bucket table over 8192 rows narrows every in-range eq probe"
    );
    // Fold identity: two workers' counters sum.
    let mut total = MetaEngagement::default();
    total.fold(&census);
    total.fold(&census);
    assert_eq!(total.bloom_probes, 4);
}
