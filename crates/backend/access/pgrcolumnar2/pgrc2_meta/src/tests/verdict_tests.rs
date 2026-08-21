//! Verdict battery: the coarse-key law's two enforcement teeth (type +
//! released assert) born-RED, the unit-grain verdict-vs-decode
//! differential (born-RED in BOTH directions — false AllPass and false
//! AllFail are the wrong-results class), metamorphic permutation
//! invariance, the null gate, the collation gate end-to-end, bloom
//! evidence, and the measured-only law.

use super::{pg_f64_cmp, Rng, TestColumn, TestValue};
use crate::builder::ColumnMeta;
use crate::format::abi::ColumnMetaBuilder;
use crate::format::class::{CollationClass, StorageClass};
use crate::format::meta::{KeyKind, StatsRecord, Verdict};
use crate::format::part::SectionKind;
use crate::format::wire::varlena_header_4b_u;
use crate::lower::{lower_const, ConstInput, LoweredConst};
use crate::profile::{MetaProfile, TypeSemantics};
use crate::verdict::{
    check_verdict_against_decode, coarse_eq, evaluate, finalize, BloomEvidence, CoarseEqVerdict,
    DifferentialViolation, GrainFacts, ZonePredicate,
};

fn int_profile() -> MetaProfile {
    MetaProfile::derive(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        CollationClass::C,
        TypeSemantics::SignedInt,
    )
    .unwrap()
}

fn text_profile(coll: CollationClass) -> MetaProfile {
    MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        coll,
        TypeSemantics::TextCollated,
    )
    .unwrap()
}

fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let mut img = varlena_header_4b_u(payload.len() as u32)
        .to_le_bytes()
        .to_vec();
    img.extend_from_slice(payload);
    img
}

/// Build one granule and return (record, bloom section body if any).
fn build_granule(profile: MetaProfile, col: &TestColumn) -> (StatsRecord, Option<Vec<u8>>) {
    let mut b = ColumnMeta::new(profile);
    b.observe_granule(&col.input(), 0);
    let rec = b.seal_granule(0);
    b.seal_band(0);
    b.seal_part();
    let bloom = b
        .aux_sections()
        .into_iter()
        .find(|(k, _)| *k == SectionKind::Bloom)
        .map(|(_, body)| body);
    (rec, bloom)
}

/// Evaluate + differential-check in one move; returns the verdict. The
/// `checked == rows` assertion is the differential's second tooth (it
/// fails if the check did not actually run over the grain).
fn checked_evaluate(
    profile: &MetaProfile,
    rec: &StatsRecord,
    rows: u32,
    bloom: Option<BloomEvidence<'_>>,
    probe: &ZonePredicate<'_>,
    passes: impl FnMut(u32) -> Option<bool>,
) -> Verdict {
    let v = evaluate(profile, GrainFacts { rows: rows as u64 }, rec, probe, bloom);
    let checked = check_verdict_against_decode(v, rows, passes)
        .expect("verdict-vs-decode differential must hold");
    assert_eq!(checked, rows, "the differential must cover every row");
    v
}

// ---------------------------------------------------------------------------
// the coarse-key law: both teeth, born-RED
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "coarse-key law")]
fn finalize_refuses_coarse_eq_allpass() {
    // The RELEASED assert (tooth 2): a seeded illegal triple must fire in
    // any build profile.
    let _ = finalize(true, true, Verdict::AllPass);
}

#[test]
fn finalize_passes_legal_verdicts() {
    // Every legal neighbor of the guarded triple flows through.
    assert_eq!(finalize(true, false, Verdict::AllPass), Verdict::AllPass);
    assert_eq!(finalize(false, true, Verdict::AllPass), Verdict::AllPass);
    assert_eq!(finalize(true, true, Verdict::AllFail), Verdict::AllFail);
    assert_eq!(finalize(true, true, Verdict::Mixed), Verdict::Mixed);
}

#[test]
fn coarse_eq_domain_has_no_allpass() {
    // Tooth 1 (the type wall), exercised exhaustively over a key grid:
    // every coarse Eq answer is AllFail or Mixed BY TYPE — and the enum
    // itself has no AllPass to construct.
    let keys: Vec<i64> = vec![i64::MIN, -5, -1, 0, 1, 5, i64::MAX];
    let mut cases = 0;
    for &lo in &keys {
        for &hi in &keys {
            if lo > hi {
                continue;
            }
            for &c in &keys {
                let v = coarse_eq(coarse_from_raw(lo), coarse_from_raw(hi), coarse_from_raw(c));
                match v {
                    CoarseEqVerdict::AllFail => assert!(c < lo || c > hi),
                    CoarseEqVerdict::Mixed => assert!(c >= lo && c <= hi),
                }
                cases += 1;
            }
        }
    }
    assert!(cases > 0);
}

/// Coarse keys for the grid: through a real coarse transform (an 8-byte
/// big-endian image reproduces any raw i64 exactly).
fn coarse_from_raw(raw: i64) -> crate::key::CoarseKey {
    let be = ((raw as u64) ^ (1u64 << 63)).to_be_bytes();
    let k = crate::key::memcmp_var_prefix_key(&be);
    assert_eq!(k.raw(), raw);
    k
}

#[test]
fn coarse_eq_never_allpass_end_to_end() {
    // Semantic pin: a C-text granule where EVERY row is the same value
    // "prefix00a". An exact column would prove Eq AllPass; the coarse
    // prefix key must NOT (the tie could hide "prefix00b").
    let profile = text_profile(CollationClass::C);
    let rows: Vec<Option<TestValue>> = (0..10)
        .map(|_| Some(TestValue::Bytes(b"prefix00a".to_vec())))
        .collect();
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (rec, _) = build_granule(profile, &col);
    assert_eq!(rec.key_kind, KeyKind::Coarse.as_u8());
    assert_eq!(rec.min_key, rec.max_key, "all keys tie");
    let img = varlena_image(b"prefix00a");
    let c = lower_const(&profile, ConstInput::VarlenaImage(&img))
        .lowered()
        .unwrap();
    let v = checked_evaluate(
        &profile,
        &rec,
        col.rows(),
        None,
        &ZonePredicate::Eq(c),
        |_| Some(true),
    );
    assert_eq!(v, Verdict::Mixed, "coarse Eq must not claim AllPass");
    // The dangerous neighbor: same keys, DIFFERENT value past the prefix —
    // an AllPass here would return wrong rows; the differential proves the
    // Mixed verdict is required.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"prefix00a".to_vec())),
        Some(TestValue::Bytes(b"prefix00b".to_vec())),
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (rec, _) = build_granule(profile, &col);
    let v = checked_evaluate(
        &profile,
        &rec,
        col.rows(),
        None,
        &ZonePredicate::Eq(c),
        |r| Some(r == 0),
    );
    assert_eq!(v, Verdict::Mixed);
    // And the exact-side contrast: an int granule of one repeated value
    // DOES prove Eq AllPass (the capability the law protects).
    let profile = int_profile();
    let rows: Vec<Option<TestValue>> = (0..5).map(|_| Some(TestValue::Word(42))).collect();
    let col = TestColumn::new(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        &rows,
    );
    let (rec, _) = build_granule(profile, &col);
    let c = lower_const(&profile, ConstInput::Word(42))
        .lowered()
        .unwrap();
    let v = checked_evaluate(
        &profile,
        &rec,
        col.rows(),
        None,
        &ZonePredicate::Eq(c),
        |_| Some(true),
    );
    assert_eq!(
        v,
        Verdict::AllPass,
        "exact keys must keep the AllPass power"
    );
}

#[test]
fn coarse_between_degenerate_cannot_allpass() {
    // BETWEEN c AND c (inclusive) is Eq in range clothing; the coarse
    // strict-proof logic must structurally refuse AllPass.
    let profile = text_profile(CollationClass::C);
    let rows: Vec<Option<TestValue>> = (0..4)
        .map(|_| Some(TestValue::Bytes(b"prefix00a".to_vec())))
        .collect();
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (rec, _) = build_granule(profile, &col);
    let img = varlena_image(b"prefix00a");
    let c = lower_const(&profile, ConstInput::VarlenaImage(&img))
        .lowered()
        .unwrap();
    let v = checked_evaluate(
        &profile,
        &rec,
        col.rows(),
        None,
        &ZonePredicate::Between {
            lo: c,
            lo_inc: true,
            hi: c,
            hi_inc: true,
        },
        |_| Some(true),
    );
    assert_eq!(v, Verdict::Mixed);
}

// ---------------------------------------------------------------------------
// the differential: born-RED in both directions
// ---------------------------------------------------------------------------

#[test]
fn differential_fires_on_false_allpass_and_false_allfail() {
    // False AllPass: a tampered record claims min==max==c over data that
    // contains other values.
    let profile = int_profile();
    let vals = [7i64, 8, 9];
    let rows: Vec<Option<TestValue>> = vals
        .iter()
        .map(|&v| Some(TestValue::Word(v as u64)))
        .collect();
    let col = TestColumn::new(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        &rows,
    );
    let (mut rec, _) = build_granule(profile, &col);
    rec.min_key = 7;
    rec.max_key = 7; // TAMPER: pretend the granule is constant 7
    let c = lower_const(&profile, ConstInput::Word(7))
        .lowered()
        .unwrap();
    let v = evaluate(
        &profile,
        GrainFacts { rows: 3 },
        &rec,
        &ZonePredicate::Eq(c),
        None,
    );
    assert_eq!(v, Verdict::AllPass, "the tamper produced the wrong verdict");
    let err = check_verdict_against_decode(v, 3, |r| Some(vals[r as usize] == 7)).unwrap_err();
    assert_eq!(err, DifferentialViolation::FalseAllPass { row: 1 });
    // False AllFail: a tampered record excludes a present value.
    let (mut rec, _) = build_granule(profile, &col);
    rec.min_key = 100;
    rec.max_key = 200; // TAMPER: the range excludes everything present
    let v = evaluate(
        &profile,
        GrainFacts { rows: 3 },
        &rec,
        &ZonePredicate::Eq(c),
        None,
    );
    assert_eq!(v, Verdict::AllFail);
    let err = check_verdict_against_decode(v, 3, |r| Some(vals[r as usize] == 7)).unwrap_err();
    assert_eq!(err, DifferentialViolation::FalseAllFail { row: 0 });
}

// ---------------------------------------------------------------------------
// unit-grain verdict-vs-decode batteries (exact + coarse + bloom)
// ---------------------------------------------------------------------------

#[test]
fn int_probe_battery_holds_the_differential() {
    let profile = int_profile();
    let mut rng = Rng::new(0x7E5D_0001);
    let mut tally = [0u32; 3];
    for _ in 0..24 {
        let vals: Vec<Option<i64>> = (0..40)
            .map(|_| {
                if rng.chance(1, 6) {
                    None
                } else {
                    Some((rng.next() as i64) % 50)
                }
            })
            .collect();
        let rows: Vec<Option<TestValue>> = vals
            .iter()
            .map(|v| v.map(|x| TestValue::Word(x as u64)))
            .collect();
        let col = TestColumn::new(
            StorageClass::ByvalWord {
                width: 8,
                signed: true,
            },
            &rows,
        );
        let (rec, bloom_body) = build_granule(profile, &col);
        let n = col.rows();
        // Probe constants: present values, absent values, extremes.
        let consts: Vec<i64> = vec![
            vals.iter().flatten().copied().next().unwrap_or(0),
            -1,
            0,
            49,
            60,
            i64::MIN,
            i64::MAX,
            (rng.next() as i64) % 80,
        ];
        for &cv in &consts {
            let c = lower_const(&profile, ConstInput::Word(cv as u64))
                .lowered()
                .unwrap();
            let bloom = bloom_body
                .as_deref()
                .and_then(|b| crate::bloom::bloom_block_for(b, 1, 0).expect("well-formed"))
                .map(|(k, block)| BloomEvidence { k, block });
            let probes: Vec<(ZonePredicate<'_>, Box<dyn Fn(i64) -> bool>)> = vec![
                (ZonePredicate::Eq(c), Box::new(move |v| v == cv)),
                (ZonePredicate::Lt(c), Box::new(move |v| v < cv)),
                (ZonePredicate::Le(c), Box::new(move |v| v <= cv)),
                (ZonePredicate::Gt(c), Box::new(move |v| v > cv)),
                (ZonePredicate::Ge(c), Box::new(move |v| v >= cv)),
                (
                    ZonePredicate::Between {
                        lo: c,
                        lo_inc: true,
                        hi: c,
                        hi_inc: true,
                    },
                    Box::new(move |v| v == cv),
                ),
            ];
            for (probe, val_passes) in probes {
                let v = checked_evaluate(&profile, &rec, n, bloom, &probe, |r| {
                    vals[r as usize].map(&*val_passes)
                });
                tally[match v {
                    Verdict::AllPass => 0,
                    Verdict::AllFail => 1,
                    Verdict::Mixed => 2,
                }] += 1;
            }
        }
    }
    // The battery must actually exercise pruning power (second tooth).
    assert!(
        tally[1] > 0,
        "no AllFail ever proven — the battery is toothless"
    );
    assert!(tally[2] > 0, "no Mixed ever seen");
}

#[test]
fn float_probe_battery_with_nan_data_holds_the_differential() {
    let profile =
        MetaProfile::derive(StorageClass::F64, CollationClass::C, TypeSemantics::Float).unwrap();
    let mut rng = Rng::new(0x7E5D_0002);
    for _ in 0..12 {
        let vals: Vec<Option<f64>> = (0..30)
            .map(|_| {
                if rng.chance(1, 8) {
                    None
                } else if rng.chance(1, 10) {
                    Some(f64::NAN)
                } else if rng.chance(1, 10) {
                    Some(-0.0)
                } else {
                    Some(((rng.next() as i64) % 1000) as f64 / 8.0)
                }
            })
            .collect();
        let rows: Vec<Option<TestValue>> = vals
            .iter()
            .map(|v| v.map(|x| TestValue::Word(x.to_bits())))
            .collect();
        let col = TestColumn::new(StorageClass::F64, &rows);
        let (rec, _) = build_granule(profile, &col);
        let n = col.rows();
        for cv in [
            0.0f64,
            -0.0,
            3.5,
            -125.0,
            1e9,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            let c = lower_const(&profile, ConstInput::Word(cv.to_bits()))
                .lowered()
                .unwrap();
            // PG float comparison truths for the oracle closure.
            let probes: Vec<(ZonePredicate<'_>, Box<dyn Fn(f64) -> bool>)> = vec![
                (
                    ZonePredicate::Eq(c),
                    Box::new(move |v| pg_f64_cmp(v, cv) == core::cmp::Ordering::Equal),
                ),
                (
                    ZonePredicate::Lt(c),
                    Box::new(move |v| pg_f64_cmp(v, cv) == core::cmp::Ordering::Less),
                ),
                (
                    ZonePredicate::Ge(c),
                    Box::new(move |v| pg_f64_cmp(v, cv) != core::cmp::Ordering::Less),
                ),
            ];
            for (probe, val_passes) in probes {
                checked_evaluate(&profile, &rec, n, None, &probe, |r| {
                    vals[r as usize].map(&*val_passes)
                });
            }
        }
    }
}

#[test]
fn coarse_text_probe_battery_holds_the_differential() {
    // Adversarial C-text corpus: shared 8-byte prefixes, boundary lengths.
    let profile = text_profile(CollationClass::C);
    let mut rng = Rng::new(0x7E5D_0003);
    let lexicon: Vec<&[u8]> = vec![
        b"",
        b"a",
        b"abcdefg",
        b"abcdefgh",
        b"abcdefghAAA",
        b"abcdefghZZZ",
        b"prefix00a",
        b"prefix00b",
        b"zzzzzzzzz",
    ];
    for _ in 0..16 {
        let vals: Vec<Option<&[u8]>> = (0..25)
            .map(|_| {
                if rng.chance(1, 7) {
                    None
                } else {
                    Some(lexicon[rng.below(lexicon.len() as u64) as usize])
                }
            })
            .collect();
        let rows: Vec<Option<TestValue>> = vals
            .iter()
            .map(|v| v.map(|b| TestValue::Bytes(b.to_vec())))
            .collect();
        let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
        let (rec, bloom_body) = build_granule(profile, &col);
        let n = col.rows();
        for cv in &lexicon {
            let img = varlena_image(cv);
            let c = lower_const(&profile, ConstInput::VarlenaImage(&img))
                .lowered()
                .unwrap();
            let bloom = bloom_body
                .as_deref()
                .and_then(|b| crate::bloom::bloom_block_for(b, 1, 0).expect("well-formed"))
                .map(|(k, block)| BloomEvidence { k, block });
            let cvv: Vec<u8> = cv.to_vec();
            let probes: Vec<(ZonePredicate<'_>, Box<dyn Fn(&[u8]) -> bool>)> = vec![
                (ZonePredicate::Eq(c), {
                    let cvv = cvv.clone();
                    Box::new(move |v: &[u8]| v == &cvv[..])
                }),
                (ZonePredicate::Lt(c), {
                    let cvv = cvv.clone();
                    Box::new(move |v: &[u8]| v < &cvv[..])
                }),
                (ZonePredicate::Le(c), {
                    let cvv = cvv.clone();
                    Box::new(move |v: &[u8]| v <= &cvv[..])
                }),
                (ZonePredicate::Gt(c), {
                    let cvv = cvv.clone();
                    Box::new(move |v: &[u8]| v > &cvv[..])
                }),
                (ZonePredicate::Ge(c), {
                    let cvv = cvv.clone();
                    Box::new(move |v: &[u8]| v >= &cvv[..])
                }),
            ];
            for (probe, val_passes) in probes {
                checked_evaluate(&profile, &rec, n, bloom, &probe, |r| {
                    vals[r as usize].map(|v| val_passes(v))
                });
            }
        }
    }
}

#[test]
fn bloom_proves_allfail_inside_the_range() {
    // A value absent from the granule but INSIDE [min,max]: keys say
    // Mixed; the bloom proves AllFail — and the differential ratifies it.
    let profile = int_profile();
    // 8192 distinct evens: above the OD-11 4096-NDV arming floor.
    let vals: Vec<i64> = (0..8192).map(|i| i * 2).collect(); // evens 0..16382
    let rows: Vec<Option<TestValue>> = vals
        .iter()
        .map(|&v| Some(TestValue::Word(v as u64)))
        .collect();
    let col = TestColumn::new(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        &rows,
    );
    // Shuffle so the granule is unclustered (bloom arming policy).
    let mut shuffled = rows.clone();
    Rng::new(0x7E5D_0004).shuffle(&mut shuffled);
    let col2 = TestColumn::new(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        &shuffled,
    );
    let (rec, bloom_body) = build_granule(profile, &col2);
    let bloom_body = bloom_body.expect("unclustered high-NDV int granule arms its bloom");
    let (k, block) = crate::bloom::bloom_block_for(&bloom_body, 1, 0)
        .expect("well-formed")
        .expect("armed");
    let c = lower_const(&profile, ConstInput::Word(63))
        .lowered()
        .unwrap(); // odd: absent
    let v = checked_evaluate(
        &profile,
        &rec,
        col.rows(),
        Some(BloomEvidence { k, block }),
        &ZonePredicate::Eq(c),
        |r| {
            let val = match &shuffled[r as usize] {
                Some(TestValue::Word(w)) => *w as i64,
                _ => unreachable!(),
            };
            Some(val == 63)
        },
    );
    assert_eq!(
        v,
        Verdict::AllFail,
        "the bloom must prove absence inside the range"
    );
    // Without bloom evidence the keys alone say Mixed.
    let v = evaluate(
        &profile,
        GrainFacts {
            rows: col.rows() as u64,
        },
        &rec,
        &ZonePredicate::Eq(c),
        None,
    );
    assert_eq!(v, Verdict::Mixed);
}

// ---------------------------------------------------------------------------
// metamorphic: permuted granule contents
// ---------------------------------------------------------------------------

#[test]
fn permuted_contents_produce_identical_verdicts() {
    let profile = int_profile();
    let mut rng = Rng::new(0x7E5D_0005);
    let mut rows: Vec<Option<TestValue>> = (0..60)
        .map(|_| {
            if rng.chance(1, 5) {
                None
            } else {
                Some(TestValue::Word((rng.next() % 30) as u64))
            }
        })
        .collect();
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let col = TestColumn::new(class, &rows);
    let (rec_a, _) = build_granule(profile, &col);
    // Three permutations: records must be identical field-for-field except
    // sortedness (the only order-sensitive stat), and every probe verdict
    // must agree.
    for seed in [1u64, 2, 3] {
        Rng::new(seed).shuffle(&mut rows);
        let col_b = TestColumn::new(class, &rows);
        let (rec_b, _) = build_granule(profile, &col_b);
        let mut a = rec_a;
        let mut b = rec_b;
        a.sortedness = 0;
        b.sortedness = 0;
        assert_eq!(a, b, "permutation changed an order-insensitive stat");
        for cv in [-1i64, 0, 5, 29, 40] {
            let c = lower_const(&profile, ConstInput::Word(cv as u64))
                .lowered()
                .unwrap();
            for probe in [
                ZonePredicate::Eq(c),
                ZonePredicate::Lt(c),
                ZonePredicate::Ge(c),
                ZonePredicate::IsNull,
                ZonePredicate::IsNotNull,
            ] {
                let facts = GrainFacts {
                    rows: col.rows() as u64,
                };
                assert_eq!(
                    evaluate(&profile, facts, &rec_a, &probe, None),
                    evaluate(&profile, facts, &rec_b, &probe, None),
                    "verdicts must be permutation-invariant"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// null gate + null-witness verdicts
// ---------------------------------------------------------------------------

#[test]
fn null_gate_blocks_allpass_and_null_probes_answer() {
    let profile = int_profile();
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    // All non-null rows equal 5, but one NULL row exists: Eq 5 must be
    // Mixed (SQL ternary — the null row does not pass).
    let rows = vec![Some(TestValue::Word(5)), None, Some(TestValue::Word(5))];
    let col = TestColumn::new(class, &rows);
    let (rec, _) = build_granule(profile, &col);
    let c = lower_const(&profile, ConstInput::Word(5))
        .lowered()
        .unwrap();
    let v = checked_evaluate(&profile, &rec, 3, None, &ZonePredicate::Eq(c), |r| {
        if r == 1 {
            None
        } else {
            Some(true)
        }
    });
    assert_eq!(v, Verdict::Mixed, "the null gate must block AllPass");
    // IsNull / IsNotNull from the nonnull witness.
    let v = checked_evaluate(&profile, &rec, 3, None, &ZonePredicate::IsNull, |r| {
        Some(r == 1)
    });
    assert_eq!(v, Verdict::Mixed);
    // All-null granule: value predicates AllFail; IsNull AllPass.
    let rows = vec![None, None];
    let col = TestColumn::new(class, &rows);
    let (rec, _) = build_granule(profile, &col);
    let v = checked_evaluate(&profile, &rec, 2, None, &ZonePredicate::Eq(c), |_| None);
    assert_eq!(v, Verdict::AllFail);
    let v = checked_evaluate(&profile, &rec, 2, None, &ZonePredicate::IsNull, |_| {
        Some(true)
    });
    assert_eq!(v, Verdict::AllPass);
    let v = checked_evaluate(&profile, &rec, 2, None, &ZonePredicate::IsNotNull, |_| {
        Some(false)
    });
    assert_eq!(v, Verdict::AllFail);
    // No-null granule: AllPass reachable; IsNotNull AllPass.
    let rows = vec![Some(TestValue::Word(5)), Some(TestValue::Word(5))];
    let col = TestColumn::new(class, &rows);
    let (rec, _) = build_granule(profile, &col);
    let v = checked_evaluate(&profile, &rec, 2, None, &ZonePredicate::Eq(c), |_| {
        Some(true)
    });
    assert_eq!(v, Verdict::AllPass);
    let v = checked_evaluate(&profile, &rec, 2, None, &ZonePredicate::IsNotNull, |_| {
        Some(true)
    });
    assert_eq!(v, Verdict::AllPass);
}

// ---------------------------------------------------------------------------
// InSet
// ---------------------------------------------------------------------------

#[test]
fn inset_verdicts() {
    let profile = int_profile();
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let rows = vec![Some(TestValue::Word(4)), Some(TestValue::Word(4))];
    let col = TestColumn::new(class, &rows);
    let (rec, _) = build_granule(profile, &col);
    let lower = |v: i64| {
        lower_const(&profile, ConstInput::Word(v as u64))
            .lowered()
            .unwrap()
    };
    // Empty set: never true.
    let members: Vec<LoweredConst<'_>> = vec![];
    let v = checked_evaluate(
        &profile,
        &rec,
        2,
        None,
        &ZonePredicate::InSet(&members),
        |_| Some(false),
    );
    assert_eq!(v, Verdict::AllFail);
    // A member equals the constant granule: AllPass.
    let members = vec![lower(9), lower(4)];
    let v = checked_evaluate(
        &profile,
        &rec,
        2,
        None,
        &ZonePredicate::InSet(&members),
        |_| Some(true),
    );
    assert_eq!(v, Verdict::AllPass);
    // All members outside the range: AllFail.
    let members = vec![lower(100), lower(-7)];
    let v = checked_evaluate(
        &profile,
        &rec,
        2,
        None,
        &ZonePredicate::InSet(&members),
        |_| Some(false),
    );
    assert_eq!(v, Verdict::AllFail);
}

// ---------------------------------------------------------------------------
// measured-only + collation gate + reserved-slot handling
// ---------------------------------------------------------------------------

#[test]
fn measured_only_ignores_unmeasured_fields() {
    // An Opaque column's record is tampered with plausible-looking keys:
    // evaluation must consult NOTHING (profile says keys were never
    // computed) and return Mixed.
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::Opaque,
    )
    .unwrap();
    let mut rec = StatsRecord::absent();
    rec.nonnull = 4;
    rec.key_kind = KeyKind::Exact.as_u8(); // TAMPER (foreign record)
    rec.min_key = 0;
    rec.max_key = 0;
    let text_p = text_profile(CollationClass::C);
    let img = varlena_image(b"x");
    let c = lower_const(&text_p, ConstInput::VarlenaImage(&img))
        .lowered()
        .unwrap();
    let v = evaluate(
        &profile,
        GrainFacts { rows: 4 },
        &rec,
        &ZonePredicate::Eq(c),
        None,
    );
    assert_eq!(
        v,
        Verdict::Mixed,
        "unmeasured stats must never be consulted"
    );
}

#[test]
fn enum_rank_reserved_reads_as_absent() {
    // The O-5 reserved slot: a record carrying it (a future writer) is
    // consulted as Absent by this crate — sound either way the
    // transcription lands.
    let profile = int_profile();
    let mut rec = StatsRecord::absent();
    rec.nonnull = 2;
    rec.key_kind = KeyKind::EnumRankReserved.as_u8();
    rec.min_key = 1;
    rec.max_key = 1;
    let c = lower_const(&profile, ConstInput::Word(1))
        .lowered()
        .unwrap();
    let v = evaluate(
        &profile,
        GrainFacts { rows: 2 },
        &rec,
        &ZonePredicate::Eq(c),
        None,
    );
    assert_eq!(v, Verdict::Mixed);
}

#[test]
fn collation_gate_end_to_end() {
    // 6000 distinct values: above the OD-11 4096-NDV arming floor even
    // under the HLL estimator's band, so the collation gate alone decides.
    let corpus: Vec<Option<TestValue>> = (0..6000)
        .map(|i| Some(TestValue::Bytes(format!("w{i:04}").into_bytes())))
        .collect();
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &corpus);
    // Other-deterministic collation: no ORDER metadata (Lt stays Mixed
    // whatever the data), equality blooms still serve.
    let p = text_profile(CollationClass::OtherDeterministic);
    let (rec, bloom_body) = build_granule(p, &col);
    assert_eq!(
        rec.key_kind,
        KeyKind::Absent.as_u8(),
        "no keys under non-C collation"
    );
    let img = varlena_image(b"w3000");
    let c = lower_const(&p, ConstInput::VarlenaImage(&img))
        .lowered()
        .unwrap();
    let v = checked_evaluate(&p, &rec, col.rows(), None, &ZonePredicate::Lt(c), |r| {
        Some(
            matches!(&corpus[r as usize], Some(TestValue::Bytes(b)) if b.as_slice() < b"w3000".as_slice()),
        )
    });
    assert_eq!(v, Verdict::Mixed, "no order verdicts without C collation");
    let bloom_body = bloom_body.expect("deterministic collation arms blooms");
    let (k, block) = crate::bloom::bloom_block_for(&bloom_body, 1, 0)
        .expect("well-formed")
        .expect("armed");
    // A 6000-NDV granule loads the 2KB block to a real FP rate, so pick a
    // probe that is BOTH absent from the corpus AND bloom-negative
    // (deterministic: the hash family is fixed) — the leg under test is
    // "evidence is consulted", not the filter's FP luck.
    let probe = (0..256u32)
        .map(|i| format!("missing-{i}"))
        .find(|s| !crate::bloom::bloom_may_contain(block, k, s.as_bytes()))
        .expect("a bloom-negative absent probe exists within 256 tries");
    let img = varlena_image(probe.as_bytes());
    let c = lower_const(&p, ConstInput::VarlenaImage(&img))
        .lowered()
        .unwrap();
    let v = checked_evaluate(
        &p,
        &rec,
        col.rows(),
        Some(BloomEvidence { k, block }),
        &ZonePredicate::Eq(c),
        |_| Some(false),
    );
    assert_eq!(v, Verdict::AllFail, "equality metadata survives the gate");
    // Nondeterministic collation: NOTHING — even with bloom evidence
    // supplied, evaluation must not consult it.
    let p = text_profile(CollationClass::Nondeterministic);
    let (rec, bloom_body) = build_granule(p, &col);
    assert!(
        bloom_body.is_none(),
        "nondeterministic text must not build blooms"
    );
    let c = LoweredConst::from_parts(None, Some(&b"missing"[..]));
    let v = checked_evaluate(
        &p,
        &rec,
        col.rows(),
        Some(BloomEvidence { k, block }),
        &ZonePredicate::Eq(c),
        |_| Some(false),
    );
    assert_eq!(v, Verdict::Mixed, "the gate must ignore bloom evidence");
}

// ---------------------------------------------------------------------------
// metadata-ANSWERED MIN/MAX (Ruling 4 item 12 — the metaagg footer legs)
// ---------------------------------------------------------------------------

/// The happy path: a real-built int granule answers exact, null-skipping
/// min/max keys, cross-checked against the decode-truth oracle.
#[test]
fn min_max_answer_matches_decode_truth_and_skips_nulls() {
    use crate::verdict::{min_max_answer, MinMaxKeys};
    let profile = int_profile();
    // Nulls interleaved; the value extremes differ from word extremes to
    // prove sign handling (-7 min, 9000 max).
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Word(5)),
        None,
        Some(TestValue::Word((-7i64) as u64)),
        Some(TestValue::Word(9000)),
        None,
        Some(TestValue::Word(0)),
    ];
    let col = TestColumn::new(profile.class, &rows);
    let (rec, _) = build_granule(profile, &col);
    assert_eq!(
        min_max_answer(&profile, &rec),
        Some(MinMaxKeys::Keys { min: -7, max: 9000 }),
        "min/max ride the non-null values only (SQL null law)"
    );
    // Decode-truth oracle: recompute from the raw rows.
    let truth_min = -7i64;
    let truth_max = 9000i64;
    match min_max_answer(&profile, &rec).unwrap() {
        MinMaxKeys::Keys { min, max } => {
            assert_eq!(
                (
                    profile.key.exact_key_to_datum_word(min).unwrap() as i64,
                    profile.key.exact_key_to_datum_word(max).unwrap() as i64,
                ),
                (truth_min, truth_max),
                "key inversion reproduces the datum words"
            );
        }
        MinMaxKeys::NoRows => panic!("nonnull > 0 cannot be NoRows"),
    }
}

/// An all-null grain answers NoRows (skip, not poison): nonnull is exact
/// under every builder vintage — spec §6.6.
#[test]
fn min_max_answer_all_null_grain_is_norows_even_unwitnessed() {
    use crate::verdict::{min_max_answer, MinMaxKeys};
    let profile = int_profile();
    let col = TestColumn::new(profile.class, &[None, None, None]);
    let (rec, _) = build_granule(profile, &col);
    assert_eq!(min_max_answer(&profile, &rec), Some(MinMaxKeys::NoRows));
    // The SAME answer with the witness stripped: NoRows needs no witness.
    let mut standin = rec;
    standin.flags = 0;
    assert_eq!(min_max_answer(&profile, &standin), Some(MinMaxKeys::NoRows));
}

/// THE TRAP (the #598 shape, min/max edition): a stand-in-vintage record
/// carries exact nonnull with ZEROED aggregates — min_key == max_key == 0
/// over real data. Without the witness gate the answer face would return
/// MIN = MAX = 0 over a granule whose true extremes are elsewhere: a
/// silently wrong query result. Absence poisons.
#[test]
fn standin_record_min_max_is_the_598_trap_absence_poisons() {
    use crate::verdict::{min_max_answer, MinMaxKeys};
    let profile = int_profile();
    let col = TestColumn::new(
        profile.class,
        &[
            Some(TestValue::Word(41)),
            Some(TestValue::Word((-3i64) as u64)),
        ],
    );
    let (real, _) = build_granule(profile, &col);
    assert!(
        matches!(min_max_answer(&profile, &real), Some(MinMaxKeys::Keys { .. })),
        "the real-built record answers"
    );
    // The stand-in shape: exact nonnull, zeroed aggregates, flags == 0,
    // key_kind spoofed Exact (worst case — a foreign writer that stamped
    // the kind without computing; key_kind alone must NOT be trusted for
    // ANSWERS even though pruning may trust it for verdicts).
    let mut standin = StatsRecord::absent();
    standin.nonnull = real.nonnull;
    standin.key_kind = KeyKind::Exact.as_u8();
    assert_eq!(
        min_max_answer(&profile, &standin),
        None,
        "witness-less min/max over nonnull rows must decline (born-RED trap)"
    );
}

/// The value-faithful law: float keys are EXACT (order/equality-faithful)
/// yet must decline — PG float equality classes (±0.0, NaN payloads) span
/// byte images, so no key inversion can reproduce the scan's byte choice.
#[test]
fn float_min_max_declines_exact_but_not_value_faithful() {
    use crate::verdict::min_max_answer;
    let profile = MetaProfile::derive(StorageClass::F64, CollationClass::C, TypeSemantics::Float)
        .unwrap();
    assert_eq!(profile.key.kind(), KeyKind::Exact, "float keys ARE exact");
    assert!(!profile.key.value_faithful(), "…but not value-faithful");
    let col = TestColumn::new(
        profile.class,
        &[
            Some(TestValue::Word(1.5f64.to_bits())),
            Some(TestValue::Word((-0.0f64).to_bits())),
        ],
    );
    let (rec, _) = build_granule(profile, &col);
    assert_eq!(min_max_answer(&profile, &rec), None);
}

/// Coarse keys (C-collated text prefixes) decline: a prefix cannot name
/// the value, let alone its bytes.
#[test]
fn coarse_text_min_max_declines() {
    use crate::verdict::min_max_answer;
    let profile = text_profile(CollationClass::C);
    let col = TestColumn::new(
        profile.class,
        &[
            Some(TestValue::Bytes(b"alpha".to_vec())),
            Some(TestValue::Bytes(b"omega".to_vec())),
        ],
    );
    let (rec, _) = build_granule(profile, &col);
    assert_eq!(min_max_answer(&profile, &rec), None);
}

/// A poisoned grain (a value defeated its derivation) reads key_kind
/// Absent and declines even with the witness present.
#[test]
fn poisoned_grain_min_max_declines() {
    use crate::verdict::min_max_answer;
    let profile = int_profile();
    let col = TestColumn::new(profile.class, &[Some(TestValue::Word(3))]);
    let (mut rec, _) = build_granule(profile, &col);
    rec.key_kind = KeyKind::Absent.as_u8();
    assert_eq!(
        min_max_answer(&profile, &rec),
        None,
        "witnessed but keyless (builder degrade) declines"
    );
}
