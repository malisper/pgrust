//! Builder battery: typed footer aggregates vs the DECODE oracle (the
//! stats a naive recomputation over reference-codec-decoded values
//! produces), grain merges (granule → band → part) with the boundary
//! sortedness laws, the packed-numeric degrade coupling, the #80
//! byte+char pins, and the driver-protocol released asserts.

use super::{pg_f64_cmp, roundtrip_through_reference, Rng, TestColumn, TestValue};
use crate::builder::ColumnMeta;
use crate::format::abi::{ColumnMetaBuilder, EncodeInput};
use crate::format::class::{CollationClass, StorageClass};
use crate::format::meta::{KeyKind, Sortedness, StatsRecord};
use crate::profile::{MetaProfile, TypeSemantics};
use crate::verdict::{sum_answer, zero_count_answer};

fn drive_one_granule(profile: MetaProfile, col: &TestColumn) -> (ColumnMeta, StatsRecord) {
    let mut b = ColumnMeta::new(profile);
    b.observe_granule(&col.input(), 0);
    let rec = b.seal_granule(0);
    (b, rec)
}

/// Naive decode-side oracle for word-class stats: recompute everything from
/// the DECODED datums (reference-codec loop), independently of the builder.
struct WordOracle {
    nonnull: u32,
    min_key: i64,
    max_key: i64,
    sum: i128,
    zeros: u64,
}

fn word_oracle(
    col: &TestColumn,
    key: impl Fn(u64) -> i64,
    sum: impl Fn(u64) -> i128,
    zero: impl Fn(u64) -> bool,
) -> WordOracle {
    let dec = roundtrip_through_reference(col);
    let mut o = WordOracle {
        nonnull: 0,
        min_key: i64::MAX,
        max_key: i64::MIN,
        sum: 0,
        zeros: 0,
    };
    for r in 0..col.rows() {
        if !col.valid(r) {
            continue;
        }
        let w = dec.datums[r as usize];
        o.nonnull += 1;
        let k = key(w);
        o.min_key = o.min_key.min(k);
        o.max_key = o.max_key.max(k);
        o.sum += sum(w);
        if zero(w) {
            o.zeros += 1;
        }
    }
    o
}

#[test]
fn signed_int_granule_stats_match_decode_oracle() {
    let mut rng = Rng::new(0xB11D_0001);
    let rows: Vec<Option<TestValue>> = (0..300)
        .map(|_| {
            if rng.chance(1, 5) {
                None
            } else {
                Some(TestValue::Word((rng.next() as i64 % 10_000) as u64))
            }
        })
        .collect();
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let col = TestColumn::new(class, &rows);
    let profile = MetaProfile::derive(class, CollationClass::C, TypeSemantics::SignedInt).unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    let o = word_oracle(&col, |w| w as i64, |w| w as i64 as i128, |w| w == 0);
    assert_eq!(rec.nonnull, o.nonnull, "two-witness: stats-side nonnull");
    assert_eq!(rec.key_kind, KeyKind::Exact.as_u8());
    assert_eq!(rec.min_key, o.min_key);
    assert_eq!(rec.max_key, o.max_key);
    assert_eq!(rec.sum_i128, o.sum);
    assert_eq!(rec.zero_count, o.zeros);
    // Validity popcount == stats nonnull (the builder-side two-witness leg).
    let pop: u32 = col
        .validity
        .as_ref()
        .map(|v| v.iter().map(|w| w.count_ones()).sum())
        .unwrap_or(col.rows());
    assert_eq!(rec.nonnull, pop);
}

#[test]
fn unsigned_flip_and_bool_stats_match_decode_oracle() {
    // pg_lsn-flavored u64 (flip embed).
    let mut rng = Rng::new(0xB11D_0002);
    let rows: Vec<Option<TestValue>> = (0..200)
        .map(|_| {
            if rng.chance(1, 7) {
                None
            } else {
                Some(TestValue::Word(rng.next()))
            }
        })
        .collect();
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: false,
    };
    let col = TestColumn::new(class, &rows);
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::UnsignedInt).unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    let o = word_oracle(
        &col,
        |w| crate::key::unsigned_flip_key(w).raw(),
        |w| w as i128,
        |w| w == 0,
    );
    assert_eq!(rec.min_key, o.min_key);
    assert_eq!(rec.max_key, o.max_key);
    assert_eq!(rec.sum_i128, o.sum);
    // bool: sum counts trues, zero counts falses.
    let rows: Vec<Option<TestValue>> = (0..100)
        .map(|_| {
            if rng.chance(1, 9) {
                None
            } else {
                Some(TestValue::Word(rng.below(2)))
            }
        })
        .collect();
    let col = TestColumn::new(StorageClass::Bool, &rows);
    let profile =
        MetaProfile::derive(StorageClass::Bool, CollationClass::C, TypeSemantics::Bool).unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    let o = word_oracle(&col, |w| (w != 0) as i64, |w| (w != 0) as i128, |w| w == 0);
    assert_eq!(rec.sum_i128, o.sum);
    assert_eq!(rec.zero_count, o.zeros);
    assert_eq!(rec.min_key, o.min_key);
    assert_eq!(rec.max_key, o.max_key);
}

#[test]
fn float_granule_stats_match_decode_oracle_with_nan_and_zeros() {
    // Adversarial: NaNs (both signs), ±0.0, infinities — the stored-side
    // key plane must stay exact under PG float semantics.
    let mut vals: Vec<Option<TestValue>> = vec![
        Some(TestValue::Word(f64::NAN.to_bits())),
        Some(TestValue::Word((-f64::NAN).to_bits())),
        Some(TestValue::Word((-0.0f64).to_bits())),
        Some(TestValue::Word(0.0f64.to_bits())),
        Some(TestValue::Word(f64::NEG_INFINITY.to_bits())),
        Some(TestValue::Word(f64::INFINITY.to_bits())),
        None,
    ];
    let mut rng = Rng::new(0xB11D_0003);
    for _ in 0..100 {
        vals.push(Some(TestValue::Word(f64::from_bits(rng.next()).to_bits())));
    }
    let col = TestColumn::new(StorageClass::F64, &vals);
    let profile =
        MetaProfile::derive(StorageClass::F64, CollationClass::C, TypeSemantics::Float).unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    let o = word_oracle(
        &col,
        |w| crate::key::f64_key_from_datum(w).raw(),
        |_| 0,
        |w| f64::from_bits(w) == 0.0,
    );
    assert_eq!(rec.min_key, o.min_key);
    assert_eq!(rec.max_key, o.max_key);
    assert_eq!(rec.zero_count, o.zeros, "±0.0 both count as zero");
    assert_eq!(rec.sum_i128, 0, "floats never fill the i128 sum");
    // NaN present ⇒ the max key is the canonical NaN key (greatest).
    assert_eq!(rec.max_key, i64::MAX);
    // Oracle min via PG comparator agrees with the key plane's min.
    let decoded_min = (0..col.rows())
        .filter(|&r| col.valid(r))
        .map(|r| f64::from_bits(col.datums[r as usize]))
        .min_by(|a, b| pg_f64_cmp(*a, *b))
        .unwrap();
    assert_eq!(rec.min_key, crate::key::f64_order_key(decoded_min).raw());
}

#[test]
fn text_stats_pin_both_units() {
    // The #80 lesson: byte AND char lengths, pinned on multibyte content.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes("héllo".as_bytes().to_vec())), // 6 B, 5 chars
        Some(TestValue::Bytes("日本語".as_bytes().to_vec())), // 9 B, 3 chars
        Some(TestValue::Bytes("🦀".as_bytes().to_vec())),    // 4 B, 1 char
        Some(TestValue::Bytes(b"".to_vec())),                // 0 B, 0 chars
        None,
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(rec.byte_len_min, 0);
    assert_eq!(rec.byte_len_max, 9);
    assert_eq!(rec.byte_len_sum, 6 + 9 + 4);
    assert_eq!(rec.char_len_min, 0);
    assert_eq!(rec.char_len_max, 5, "char unit must not mirror bytes");
    assert_eq!(rec.nonnull, 4);
    // Oracle cross-check with an independent char counter.
    for s in ["héllo", "日本語", "🦀", ""] {
        assert_eq!(
            s.chars().count() as u32,
            {
                let b = s.as_bytes();
                b.iter().filter(|&&x| (x & 0xC0) != 0x80).count() as u32
            },
            "test oracle self-check"
        );
    }
    // bytea: byte stats only, char fields stay zero.
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::MemcmpOrdered,
    )
    .unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(rec.byte_len_max, 9);
    assert_eq!(rec.char_len_min, 0);
    assert_eq!(rec.char_len_max, 0, "bytea carries no char stats");
}

#[test]
fn all_ascii_witness_p2() {
    use crate::format::meta::STATSF_ALL_ASCII;
    // v4 P-2 (ledger FT-2): multibyte content DENIES the witness...
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"plain".to_vec())),
        Some(TestValue::Bytes("héllo".as_bytes().to_vec())),
        None,
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(
        rec.flags & STATSF_ALL_ASCII,
        0,
        "multibyte value must falsify the all-ASCII witness"
    );
    // ...pure-ASCII content (incl. empty strings + nulls) MINTS it.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"plain".to_vec())),
        Some(TestValue::Bytes(b"".to_vec())),
        None,
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    assert_ne!(
        rec.flags & STATSF_ALL_ASCII,
        0,
        "pure-ASCII granule must mint the witness"
    );
    // bytea (MemcmpOrdered, bytes-only LenStats) never mints it — the
    // witness is a CHAR-unit license and bytea has no char unit.
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::MemcmpOrdered,
    )
    .unwrap();
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(
        rec.flags & STATSF_ALL_ASCII,
        0,
        "bytes-only profile must not carry a char-unit license"
    );
}

#[test]
fn packed_numeric_degrade_couples_keys_and_sums() {
    use super::numeric_payload;
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::PackedNumeric { scale: 2 },
    )
    .unwrap();
    // Clean granule: everything packs.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(numeric_payload(150, 2))), // 1.50
        Some(TestValue::Bytes(numeric_payload(-25, 2))), // -0.25
        Some(TestValue::Bytes(numeric_payload(0, 2))),   // 0.00
        None,
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(rec.key_kind, KeyKind::Exact.as_u8());
    assert_eq!((rec.min_key, rec.max_key), (-25, 150));
    assert_eq!(rec.sum_i128, 125);
    assert_eq!(rec.zero_count, 1);
    assert_eq!(sum_answer(&profile, &rec), Some(125));
    assert_eq!(zero_count_answer(&profile, &rec), Some(1));
    // Poisoned granule: one value does not rescale → keys Absent AND the
    // coupled aggregates read as uncomputed.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(numeric_payload(150, 2))),
        Some(TestValue::Bytes(numeric_payload(1005, 3))), // 1.005: no fit at scale 2
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(
        rec.key_kind,
        KeyKind::Absent.as_u8(),
        "pack failure degrades keys"
    );
    assert_eq!(rec.sum_i128, 0, "coupled sum is zeroed, not partial");
    assert_eq!(rec.zero_count, 0);
    assert_eq!(
        sum_answer(&profile, &rec),
        None,
        "measured-only: sum unreadable"
    );
    assert_eq!(zero_count_answer(&profile, &rec), None);
    assert_eq!(rec.nonnull, 2, "counts survive the degrade");
}

// ---------------------------------------------------------------------------
// the computed-stats witness (#598 leg 1)
// ---------------------------------------------------------------------------

/// BORN-RED (#598): a stand-in-vintage record — exact `nonnull`, every
/// other field `StatsRecord::absent()` (flags 0, sum 0, zero_count 0) — is
/// exactly what every pre-wire part carries on disk, and those parts stay
/// admissible forever (TypeSemantics is deliberately outside
/// schema_fingerprint; blessed v1 bank keys are frozen). The probe side
/// derives the REAL profile from the catalog, so for the word lanes the
/// PROFILE says "the builder computes SUM" while THIS record's builder did
/// not — only an on-part witness can say so. The answer face must DECLINE
/// (None → the caller decodes), never read the zeroed fields as facts.
#[test]
fn word_lane_answers_decline_without_the_computed_stats_witness() {
    let profile = int_profile();
    let rec = StatsRecord {
        nonnull: 60_000, // 60k real rows behind the zeroed aggregates
        ..StatsRecord::absent()
    };
    assert_eq!(
        sum_answer(&profile, &rec),
        None,
        "a witness-less record must decline, not answer Sum(0) over 60_000 rows"
    );
    assert_eq!(
        zero_count_answer(&profile, &rec),
        None,
        "same defect for ZeroCount(0)"
    );
}

/// The witness the REAL builder mints: present at EVERY grain (granule,
/// band, part — both record-producing sites), and the answers it unlocks
/// match the flatten oracle. The COUNT asymmetry rides the same records:
/// `count_nonnull_answer` answers identically with and without the
/// witness, because `nonnull` is exact under every builder vintage.
#[test]
fn real_builder_mints_the_witness_at_every_grain() {
    use crate::format::meta::STATSF_COMPUTED;
    use crate::verdict::{computed_stats_witness, count_nonnull_answer};
    let mut rng = Rng::new(0xB11D_0006);
    // 9 granules -> 2 bands; nulls and GUARANTEED zeros in the mix so the
    // zero-count oracle is not vacuously zero.
    let granules: Vec<Vec<Option<i64>>> = (0..9)
        .map(|g| {
            (0..40)
                .map(|i| {
                    if rng.chance(1, 5) {
                        None
                    } else if (g + i) % 13 == 0 {
                        Some(0)
                    } else {
                        Some(rng.next() as i64 % 1000 - 500)
                    }
                })
                .collect()
        })
        .collect();
    let (grecs, brecs, part) = drive_part(int_profile(), &granules);
    for (i, rec) in grecs
        .iter()
        .chain(brecs.iter())
        .chain(std::iter::once(&part))
        .enumerate()
    {
        assert!(
            computed_stats_witness(rec),
            "witness missing at record {i} (granules, then bands, then part)"
        );
        assert_ne!(rec.flags & STATSF_COMPUTED, 0);
    }
    // Witness-unlocked answers == the flatten oracle.
    let all: Vec<Option<i64>> = granules.iter().flatten().copied().collect();
    let sum: i128 = all.iter().flatten().map(|&v| v as i128).sum();
    let zeros = all.iter().flatten().filter(|&&v| v == 0).count() as u64;
    let nonnull = all.iter().flatten().count() as u64;
    assert!(zeros > 0, "the oracle must exercise real zeros");
    let profile = int_profile();
    assert_eq!(sum_answer(&profile, &part), Some(sum));
    assert_eq!(zero_count_answer(&profile, &part), Some(zeros));
    // The COUNT asymmetry: exact WITH the witness...
    assert_eq!(count_nonnull_answer(&part), nonnull);
    // ...and equally exact WITHOUT it (the stand-in vintage) — the law
    // that ships the COUNT metadata-answer slice before SUM.
    let standin_vintage = StatsRecord {
        nonnull: part.nonnull,
        ..StatsRecord::absent()
    };
    assert_eq!(count_nonnull_answer(&standin_vintage), nonnull);
}

// ---------------------------------------------------------------------------
// sortedness (granule grain)
// ---------------------------------------------------------------------------

fn int_col(vals: &[Option<i64>]) -> TestColumn {
    let rows: Vec<Option<TestValue>> = vals
        .iter()
        .map(|v| v.map(|x| TestValue::Word(x as u64)))
        .collect();
    TestColumn::new(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        &rows,
    )
}

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

#[test]
fn granule_sortedness_exact() {
    let cases: Vec<(Vec<Option<i64>>, Sortedness)> = vec![
        (
            vec![Some(1), Some(2), Some(2), Some(5)],
            Sortedness::Ascending,
        ),
        (
            vec![Some(5), Some(2), Some(2), Some(1)],
            Sortedness::Descending,
        ),
        (vec![Some(3), Some(3), Some(3)], Sortedness::Constant),
        (vec![Some(1), Some(9), Some(2)], Sortedness::Unknown),
        // Nulls are positionally skipped: the nonnull sequence governs.
        (
            vec![Some(1), None, Some(2), None, Some(7)],
            Sortedness::Ascending,
        ),
        (vec![Some(4)], Sortedness::Constant),
        (vec![None, None], Sortedness::Unknown),
    ];
    for (vals, expect) in cases {
        let col = int_col(&vals);
        let (_, rec) = drive_one_granule(int_profile(), &col);
        assert_eq!(
            Sortedness::from_u8(rec.sortedness).unwrap(),
            expect,
            "sortedness of {vals:?}"
        );
    }
}

#[test]
fn granule_sortedness_coarse_ties_claim_nothing() {
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    // Strictly ascending prefixes: claimable.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"aaa".to_vec())),
        Some(TestValue::Bytes(b"bbb".to_vec())),
        Some(TestValue::Bytes(b"ccc".to_vec())),
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(
        Sortedness::from_u8(rec.sortedness).unwrap(),
        Sortedness::Ascending
    );
    // ASCENDING VALUES whose keys tie past byte 8: Unknown (a tie could
    // equally hide an inversion — the coarse sortedness law).
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"prefix00a".to_vec())),
        Some(TestValue::Bytes(b"prefix00b".to_vec())),
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(
        Sortedness::from_u8(rec.sortedness).unwrap(),
        Sortedness::Unknown
    );
    // DESCENDING values under tied keys: also Unknown — never Ascending.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"prefix00b".to_vec())),
        Some(TestValue::Bytes(b"prefix00a".to_vec())),
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (_, rec) = drive_one_granule(profile, &col);
    assert_eq!(
        Sortedness::from_u8(rec.sortedness).unwrap(),
        Sortedness::Unknown
    );
    // Coarse constant-looking granule (identical keys): never Constant.
    let rows: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"prefix00a".to_vec())),
        Some(TestValue::Bytes(b"prefix00a".to_vec())),
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let (_, rec) = drive_one_granule(profile, &col);
    assert_ne!(
        Sortedness::from_u8(rec.sortedness).unwrap(),
        Sortedness::Constant,
        "coarse keys must never claim Constant"
    );
}

// ---------------------------------------------------------------------------
// grain merges (band / part)
// ---------------------------------------------------------------------------

/// Drive `granules` (each a value vector) through a full band + part seal.
fn drive_part(
    profile: MetaProfile,
    granules: &[Vec<Option<i64>>],
) -> (Vec<StatsRecord>, Vec<StatsRecord>, StatsRecord) {
    let mut b = ColumnMeta::new(profile);
    let cols: Vec<TestColumn> = granules.iter().map(|g| int_col(g)).collect();
    let mut grecs = Vec::new();
    for (i, col) in cols.iter().enumerate() {
        b.observe_granule(&col.input(), i as u32);
        grecs.push(b.seal_granule(i as u32));
    }
    let bands = granules.len().div_ceil(8);
    let mut brecs = Vec::new();
    for band in 0..bands {
        brecs.push(b.seal_band(band as u32));
    }
    let part = b.seal_part();
    (grecs, brecs, part)
}

#[test]
fn band_and_part_merges_match_oracle() {
    let mut rng = Rng::new(0xB11D_0004);
    // 10 granules → 2 bands (8 + 2).
    let granules: Vec<Vec<Option<i64>>> = (0..10)
        .map(|_| {
            (0..50)
                .map(|_| {
                    if rng.chance(1, 6) {
                        None
                    } else {
                        Some(rng.next() as i64 % 100_000)
                    }
                })
                .collect()
        })
        .collect();
    let (grecs, brecs, part) = drive_part(int_profile(), &granules);
    assert_eq!(brecs.len(), 2);
    // Oracle: flatten.
    let all: Vec<Option<i64>> = granules.iter().flatten().copied().collect();
    let nonnull = all.iter().flatten().count() as u32;
    let min = all.iter().flatten().min().copied().unwrap();
    let max = all.iter().flatten().max().copied().unwrap();
    let sum: i128 = all.iter().flatten().map(|&v| v as i128).sum();
    assert_eq!(part.nonnull, nonnull);
    assert_eq!(part.min_key, min);
    assert_eq!(part.max_key, max);
    assert_eq!(part.sum_i128, sum);
    // Band 0 == merge of granules 0..8 by the same oracle.
    let band0: Vec<Option<i64>> = granules[..8].iter().flatten().copied().collect();
    assert_eq!(brecs[0].nonnull, band0.iter().flatten().count() as u32);
    assert_eq!(
        brecs[0].min_key,
        band0.iter().flatten().min().copied().unwrap()
    );
    assert_eq!(
        brecs[0].max_key,
        band0.iter().flatten().max().copied().unwrap()
    );
    // Granule records survived unchanged.
    assert_eq!(grecs.len(), 10);
}

#[test]
fn merge_sortedness_boundary_laws() {
    // Ascending granules with ordered boundaries → Ascending band.
    let asc = vec![
        vec![Some(1), Some(3)],
        vec![Some(3), Some(7)], // exact boundary tie is fine
        vec![Some(8), Some(9)],
    ];
    let (_, brecs, part) = drive_part(int_profile(), &asc);
    assert_eq!(
        Sortedness::from_u8(brecs[0].sortedness).unwrap(),
        Sortedness::Ascending
    );
    assert_eq!(
        Sortedness::from_u8(part.sortedness).unwrap(),
        Sortedness::Ascending
    );
    // Boundary violation → Unknown even though each granule is sorted.
    let broken = vec![vec![Some(1), Some(9)], vec![Some(2), Some(11)]];
    let (_, brecs, _) = drive_part(int_profile(), &broken);
    assert_eq!(
        Sortedness::from_u8(brecs[0].sortedness).unwrap(),
        Sortedness::Unknown
    );
    // Constant chain on one key → Constant.
    let constant = vec![vec![Some(4), Some(4)], vec![Some(4)]];
    let (_, brecs, _) = drive_part(int_profile(), &constant);
    assert_eq!(
        Sortedness::from_u8(brecs[0].sortedness).unwrap(),
        Sortedness::Constant
    );
    // All-null granules are skipped in the chain.
    let with_gap = vec![
        vec![Some(1), Some(2)],
        vec![None, None],
        vec![Some(2), Some(5)],
    ];
    let (_, brecs, _) = drive_part(int_profile(), &with_gap);
    assert_eq!(
        Sortedness::from_u8(brecs[0].sortedness).unwrap(),
        Sortedness::Ascending
    );
}

#[test]
fn merge_sortedness_coarse_boundary_tie_is_unknown() {
    // Coarse (C-text) granules, each strictly ascending, but the BOUNDARY
    // keys tie (same 8-byte prefix): the band must refuse the claim.
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let mut b = ColumnMeta::new(profile);
    let g0: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"aaa".to_vec())),
        Some(TestValue::Bytes(b"prefix00a".to_vec())),
    ];
    let g1: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"prefix00b".to_vec())), // ties g0's last key
        Some(TestValue::Bytes(b"zzz".to_vec())),
    ];
    let c0 = TestColumn::new(StorageClass::VarlenaVerbatim, &g0);
    let c1 = TestColumn::new(StorageClass::VarlenaVerbatim, &g1);
    b.observe_granule(&c0.input(), 0);
    let r0 = b.seal_granule(0);
    b.observe_granule(&c1.input(), 1);
    let r1 = b.seal_granule(1);
    assert_eq!(
        Sortedness::from_u8(r0.sortedness).unwrap(),
        Sortedness::Ascending
    );
    assert_eq!(
        Sortedness::from_u8(r1.sortedness).unwrap(),
        Sortedness::Ascending
    );
    let band = b.seal_band(0);
    assert_eq!(
        Sortedness::from_u8(band.sortedness).unwrap(),
        Sortedness::Unknown,
        "a coarse boundary tie can hide an inversion"
    );
}

#[test]
fn split_observes_accumulate_like_one() {
    // Metamorphic: one granule fed in three chunks == fed at once.
    let mut rng = Rng::new(0xB11D_0005);
    let vals: Vec<Option<i64>> = (0..90)
        .map(|_| {
            if rng.chance(1, 8) {
                None
            } else {
                Some(rng.next() as i64 % 1000)
            }
        })
        .collect();
    let col = int_col(&vals);
    let (_, whole) = drive_one_granule(int_profile(), &col);
    let mut b = ColumnMeta::new(int_profile());
    for chunk in [&vals[..30], &vals[30..70], &vals[70..]] {
        let part_col = int_col(chunk);
        // Rebuild an input whose rows offset accumulates naturally.
        let input = EncodeInput {
            class: part_col.class,
            rows: part_col.rows(),
            datums: &part_col.datums,
            validity: part_col.validity.as_deref(),
        };
        b.observe_granule(&input, 0);
    }
    let split = b.seal_granule(0);
    assert_eq!(
        whole, split,
        "split observation must equal whole observation"
    );
}

// ---------------------------------------------------------------------------
// driver-protocol teeth (released asserts)
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "skipped a granule")]
fn observe_out_of_order_panics() {
    let col = int_col(&[Some(1)]);
    let mut b = ColumnMeta::new(int_profile());
    b.observe_granule(&col.input(), 1); // granule 0 was never sealed
}

#[test]
#[should_panic(expected = "seal_band before its granules")]
fn seal_band_before_granules_panics() {
    let mut b = ColumnMeta::new(int_profile());
    b.seal_band(0);
}

#[test]
#[should_panic(expected = "aux_sections before seal_part")]
fn aux_before_part_panics() {
    let mut b = ColumnMeta::new(int_profile());
    b.aux_sections();
}

// ---------------------------------------------------------------------------
// the TWO driver protocols (the M3-D integration defect this pins)
// ---------------------------------------------------------------------------

/// M3-D's `seal_part` does NOT interleave: it observes EVERY granule during
/// the encode pass and only then seals granule 0..n. This builder was
/// written against the interleaved protocol and asserted its way out of the
/// real one — a mismatch the `StandinMetaBuilder` hid for as long as it was
/// the thing actually wired at seal.
///
/// Both orders are now accepted, and they must produce IDENTICAL records at
/// every grain: the statistics are a pure function of the observed values,
/// not of when the driver asked for them.
#[test]
fn observe_all_then_seal_all_equals_interleaved() {
    let granules: Vec<TestColumn> = (0..3)
        .map(|g: i64| {
            let vals: Vec<Option<i64>> = (0..500)
                .map(|i| {
                    let v = g * 1000 + i;
                    if i % 11 == 0 { None } else { Some(v) }
                })
                .collect();
            int_col(&vals)
        })
        .collect();

    // Protocol 1: observe g, seal g, observe g+1, seal g+1, ...
    let mut a = ColumnMeta::new(int_profile());
    let mut a_recs = Vec::new();
    for (g, col) in granules.iter().enumerate() {
        a.observe_granule(&col.input(), g as u32);
        a_recs.push(a.seal_granule(g as u32));
    }
    let a_band = a.seal_band(0);
    let a_part = a.seal_part();
    let a_aux = a.aux_sections();

    // Protocol 2: observe every granule, THEN seal every granule.
    let mut b = ColumnMeta::new(int_profile());
    for (g, col) in granules.iter().enumerate() {
        b.observe_granule(&col.input(), g as u32);
    }
    let b_recs: Vec<StatsRecord> = (0..granules.len())
        .map(|g| b.seal_granule(g as u32))
        .collect();
    let b_band = b.seal_band(0);
    let b_part = b.seal_part();
    let b_aux = b.aux_sections();

    assert_eq!(a_recs, b_recs, "per-granule records differ by protocol");
    assert_eq!(a_band, b_band, "band record differs by protocol");
    assert_eq!(a_part, b_part, "part record differs by protocol");
    assert_eq!(a_aux, b_aux, "aux section bodies differ by protocol");

    // And the records are actually meaningful (not two identical nothings).
    assert_eq!(a_recs[0].key_kind, KeyKind::Exact.as_u8());
    assert_eq!(a_part.min_key, 1, "value 0 is null (i % 11 == 0)");
    assert_eq!(a_part.max_key, 2499);
    assert_eq!(a_part.sortedness, Sortedness::Ascending.as_u8());
}

/// The surviving ordering tooth: seal_granule is still refused out of order
/// under either protocol.
#[test]
#[should_panic(expected = "seal_granule out of order")]
fn seal_granule_out_of_order_panics() {
    let col = int_col(&[Some(1)]);
    let mut b = ColumnMeta::new(int_profile());
    b.observe_granule(&col.input(), 0);
    b.seal_granule(1);
}

// ---------------------------------------------------------------------------
// DICT-DEDUP: the distribution feed (born-RED charter)
// ---------------------------------------------------------------------------

/// Feed vs accumulator through the REAL builder: same values, same
/// distribution — and the feed must not perturb any fold (records + aux
/// bytes identical; the feed only replaces WHO maintains the distinct set).
#[test]
fn distribution_feed_matches_accumulator_and_leaves_folds_alone() {
    let mut rng = Rng::new(0xD1C7_FEED);
    let rows: Vec<Option<TestValue>> = (0..600)
        .map(|_| {
            if rng.chance(1, 13) {
                None
            } else {
                Some(TestValue::Bytes(
                    format!("value-{}", rng.next() % 97).into_bytes(),
                ))
            }
        })
        .collect();
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &rows);
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    assert!(profile.ndv, "precondition: the accumulator arm is armed");

    // The counted distinct set the dict build would hand over (byte-sorted,
    // exact counts over non-null values).
    let mut counts: std::collections::BTreeMap<Vec<u8>, u64> = std::collections::BTreeMap::new();
    for r in &rows {
        if let Some(TestValue::Bytes(b)) = r {
            *counts.entry(b.clone()).or_insert(0) += 1;
        }
    }
    let feed: Vec<(Vec<u8>, u64)> = counts.into_iter().collect();

    let mut acc = ColumnMeta::new(profile);
    let mut fed = ColumnMeta::new(profile);
    fed.set_distribution_feed(feed);
    acc.observe_granule(&col.input(), 0);
    fed.observe_granule(&col.input(), 0);
    let (ra, rf) = (acc.seal_granule(0), fed.seal_granule(0));
    assert_eq!(ra, rf, "the feed must not perturb granule folds");
    assert_eq!(acc.seal_band(0), fed.seal_band(0));
    assert_eq!(acc.seal_part(), fed.seal_part());
    assert_eq!(acc.aux_sections(), fed.aux_sections(), "aux bytes perturbed");
    let (da, df) = (acc.distribution(), fed.distribution());
    assert!(da.is_some());
    assert_eq!(da, df, "feed-served distribution diverged from the accumulator");
}

/// The collation gate rides the feed automatically: an ndv-unsound profile
/// keeps declining `distribution()`, feed or no feed (never serves numbers
/// whose value identity is unsound).
#[test]
fn distribution_feed_refused_where_ndv_unsound() {
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::Nondeterministic,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    assert!(!profile.ndv, "precondition: nondeterministic collation disarms ndv");
    let col = TestColumn::new(
        StorageClass::VarlenaVerbatim,
        &[Some(TestValue::Bytes(b"x".to_vec()))],
    );
    let mut b = ColumnMeta::new(profile);
    b.set_distribution_feed(vec![(b"x".to_vec(), 1)]);
    b.observe_granule(&col.input(), 0);
    b.seal_granule(0);
    b.seal_band(0);
    b.seal_part();
    assert!(b.distribution().is_none(), "unsound profile served a sketch");
}

/// Driver protocol: the feed replaces accumulation, so it must precede all
/// observation (a post-observation feed is a code bug, refused released).
#[test]
#[should_panic(expected = "set_distribution_feed after observation")]
fn distribution_feed_after_observation_panics() {
    let col = TestColumn::new(
        StorageClass::VarlenaVerbatim,
        &[Some(TestValue::Bytes(b"x".to_vec()))],
    );
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let mut b = ColumnMeta::new(profile);
    b.observe_granule(&col.input(), 0);
    b.set_distribution_feed(vec![(b"x".to_vec(), 1)]);
}

// ---------------------------------------------------------------------------
// D-STATS batched-shell equivalence battery (born-RED charter)
//
// The batched fold kernels (`crate::batch`) must be OUTPUT-IDENTICAL to
// the per-value incumbent walk on every corpus: StatsRecords at all three
// grains, aux section bytes (Psma/Bloom/NdvRegisters), and the sidecar
// distribution are sealed part/sidecar bytes, so any divergence here is a
// byte-law break, never a tuning delta. The seeded-divergence tooth below
// proves this battery can fail.
// ---------------------------------------------------------------------------

/// Everything a builder emits (the complete downstream-consumed surface).
#[derive(Debug, PartialEq)]
struct ShellOut {
    grecs: Vec<StatsRecord>,
    bands: Vec<StatsRecord>,
    part: StatsRecord,
    aux: Vec<(crate::format::part::SectionKind, Vec<u8>)>,
    dist: Option<crate::format::sidecar::ColDistribution>,
}

/// Drive one arm over multi-granule input. `span` = None drives the
/// classic `observe_granule` protocol; `Some(k)` drives the SEAL-SPEED-2
/// streaming face in k-row spans (the fused encoders' frame walk shape).
fn drive_shell(
    profile: MetaProfile,
    granules: &[TestColumn],
    batch: bool,
    span: Option<u32>,
    feed: Option<Vec<(Vec<u8>, u64)>>,
) -> ShellOut {
    let mut b = ColumnMeta::with_batch_fold(profile, batch);
    if let Some(f) = feed {
        b.set_distribution_feed(f);
    }
    for (g, col) in granules.iter().enumerate() {
        let input = col.input();
        match span {
            None => b.observe_granule(&input, g as u32),
            Some(k) => {
                b.begin_granule_rows(g as u32);
                let mut first = 0u32;
                while first < input.rows {
                    let n = k.min(input.rows - first);
                    b.observe_rows(&input, first, n);
                    first += n;
                }
                b.end_granule_rows(input.rows);
            }
        }
    }
    let n = granules.len() as u32;
    let grecs: Vec<StatsRecord> = (0..n).map(|g| b.seal_granule(g)).collect();
    let bands: Vec<StatsRecord> = (0..n.div_ceil(crate::format::geom::GRANULES_PER_BAND))
        .map(|band| b.seal_band(band))
        .collect();
    let part = b.seal_part();
    let aux = b.aux_sections();
    let dist = b.distribution();
    ShellOut {
        grecs,
        bands,
        part,
        aux,
        dist,
    }
}

/// The four-way pin: batched==per-value under BOTH observation protocols.
fn assert_shells_equal(profile: MetaProfile, granules: &[TestColumn], what: &str) {
    let reference = drive_shell(profile, granules, false, None, None);
    for (batch, span, arm) in [
        (false, Some(1024), "per-value spans"),
        (true, None, "batched granule"),
        (true, Some(1024), "batched spans"),
        (true, Some(37), "batched ragged spans"),
    ] {
        let got = drive_shell(profile, granules, batch, span, None);
        assert_eq!(got, reference, "{what}: {arm} diverged from per-value granule");
    }
}

fn word_granules(
    class: StorageClass,
    seed: u64,
    make: impl Fn(&mut Rng, u32) -> Option<u64>,
) -> Vec<TestColumn> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::new();
    for rows in [8192u32, 8192, 300] {
        let vals: Vec<Option<TestValue>> = (0..rows)
            .map(|i| make(&mut rng, i).map(TestValue::Word))
            .collect();
        out.push(TestColumn::new(class, &vals));
    }
    // An ALL-NULL granule (validity-only fold) at the tail.
    out.push(TestColumn::new(
        class,
        &vec![None; 100][..],
    ));
    out
}

#[test]
fn batched_equals_pervalue_signed_int() {
    let class = StorageClass::ByvalWord { width: 8, signed: true };
    let profile = MetaProfile::derive(class, CollationClass::C, TypeSemantics::SignedInt).unwrap();
    // Duplicate-heavy + full-range magnitudes + a sorted prefix (asc/ties
    // transitions) + NULL bursts.
    let granules = word_granules(class, 0xD57A_0001, |rng, i| {
        if rng.chance(1, 6) {
            None
        } else if i < 2000 {
            Some(i as u64) // sorted ascending prefix with no ties
        } else if rng.chance(1, 3) {
            Some((rng.next() as i64 % 50) as u64) // dup-heavy, sign-crossing
        } else {
            Some(rng.next()) // full-range
        }
    });
    assert_shells_equal(profile, &granules, "signed int");
}

#[test]
fn batched_equals_pervalue_unsigned_bool() {
    let class = StorageClass::ByvalWord { width: 8, signed: false };
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::UnsignedInt).unwrap();
    let granules = word_granules(class, 0xD57A_0002, |rng, _| {
        if rng.chance(1, 9) { None } else { Some(rng.next()) }
    });
    assert_shells_equal(profile, &granules, "unsigned flip");

    let profile =
        MetaProfile::derive(StorageClass::Bool, CollationClass::C, TypeSemantics::Bool).unwrap();
    let granules = word_granules(StorageClass::Bool, 0xD57A_0003, |rng, _| {
        if rng.chance(1, 5) { None } else { Some(rng.below(2)) }
    });
    assert_shells_equal(profile, &granules, "bool");
}

#[test]
fn batched_equals_pervalue_floats_nan_zero_classes() {
    // The float equality classes that span byte images: ±0.0, several NaN
    // payloads, infinities — the exact key/zero-count hazards.
    let profile =
        MetaProfile::derive(StorageClass::F64, CollationClass::C, TypeSemantics::Float).unwrap();
    let specials = [
        0.0f64.to_bits(),
        (-0.0f64).to_bits(),
        f64::NAN.to_bits(),
        f64::NAN.to_bits() | 0xDEAD, // a different NaN payload
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        f64::MIN_POSITIVE.to_bits(),
    ];
    let granules = word_granules(StorageClass::F64, 0xD57A_0004, |rng, _| {
        if rng.chance(1, 7) {
            None
        } else if rng.chance(1, 3) {
            Some(specials[rng.below(specials.len() as u64) as usize])
        } else {
            Some(((rng.next() as i64 % 100_000) as f64 / 100.0).to_bits())
        }
    });
    assert_shells_equal(profile, &granules, "f64");

    let profile =
        MetaProfile::derive(StorageClass::F32, CollationClass::C, TypeSemantics::Float).unwrap();
    let granules = word_granules(StorageClass::F32, 0xD57A_0005, |rng, _| {
        if rng.chance(1, 7) {
            None
        } else if rng.chance(1, 4) {
            Some((-0.0f32).to_bits() as u64)
        } else {
            Some(((rng.next() as i32 % 1000) as f32).to_bits() as u64)
        }
    });
    assert_shells_equal(profile, &granules, "f32");
}

#[test]
fn batched_equals_pervalue_fixed_and_exotic_keys() {
    // uuid-shaped Fixed{16} memcmp (coarse prefix keys).
    let class = StorageClass::Fixed { len: 16 };
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::MemcmpOrdered).unwrap();
    let mut rng = Rng::new(0xD57A_0006);
    let mk16 = |rng: &mut Rng| {
        let mut v = vec![0u8; 16];
        v[..8].copy_from_slice(&rng.next().to_le_bytes());
        v[8..].copy_from_slice(&rng.next().to_le_bytes());
        v
    };
    let granules: Vec<TestColumn> = (0..2)
        .map(|_| {
            let vals: Vec<Option<TestValue>> = (0..3000)
                .map(|_| {
                    if rng.chance(1, 8) { None } else { Some(TestValue::Bytes(mk16(&mut rng))) }
                })
                .collect();
            TestColumn::new(class, &vals)
        })
        .collect();
    assert_shells_equal(profile, &granules, "fixed16 memcmp");

    // interval Fixed{16} (saturating coarse embed; random images can
    // defeat the parse — the poison arm).
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::IntervalCmp).unwrap();
    let granules: Vec<TestColumn> = (0..2)
        .map(|_| {
            let vals: Vec<Option<TestValue>> = (0..1000)
                .map(|_| {
                    if rng.chance(1, 8) { None } else { Some(TestValue::Bytes(mk16(&mut rng))) }
                })
                .collect();
            TestColumn::new(class, &vals)
        })
        .collect();
    assert_shells_equal(profile, &granules, "interval fixed16");

    // timetz Fixed{12}.
    let class12 = StorageClass::Fixed { len: 12 };
    let profile =
        MetaProfile::derive(class12, CollationClass::C, TypeSemantics::TimetzUtc).unwrap();
    let granules: Vec<TestColumn> = (0..2)
        .map(|_| {
            let vals: Vec<Option<TestValue>> = (0..1000)
                .map(|_| {
                    if rng.chance(1, 8) {
                        None
                    } else {
                        let mut v = vec![0u8; 12];
                        v[..8].copy_from_slice(&(rng.below(86_400_000_000)).to_le_bytes());
                        v[8..].copy_from_slice(&((rng.next() as i32 % 60_000).to_le_bytes()));
                        Some(TestValue::Bytes(v))
                    }
                })
                .collect();
            TestColumn::new(class12, &vals)
        })
        .collect();
    assert_shells_equal(profile, &granules, "timetz fixed12");
}

#[test]
fn batched_equals_pervalue_text_families() {
    let class = StorageClass::VarlenaVerbatim;
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::TextCollated).unwrap();
    let mut rng = Rng::new(0xD57A_0007);
    let cyrillic = ["товар", "пример", "яблоко", "grüße", "naïve", "emoji-😀-tail"];
    let long = vec![0xC2u8, 0xA9].repeat(200); // 400 B valid UTF-8, > MCV max
    let granules: Vec<TestColumn> = (0..3)
        .map(|g| {
            let rows = if g == 2 { 300 } else { 4000 };
            let vals: Vec<Option<TestValue>> = (0..rows)
                .map(|_| {
                    if rng.chance(1, 6) {
                        None
                    } else if rng.chance(1, 12) {
                        Some(TestValue::Bytes(Vec::new())) // empty payload
                    } else if rng.chance(1, 10) {
                        Some(TestValue::Bytes(long.clone()))
                    } else if rng.chance(1, 3) {
                        Some(TestValue::Bytes(
                            cyrillic[rng.below(cyrillic.len() as u64) as usize]
                                .as_bytes()
                                .to_vec(),
                        ))
                    } else {
                        Some(TestValue::Bytes(
                            format!("url/{}/page", rng.below(500)).into_bytes(),
                        ))
                    }
                })
                .collect();
            TestColumn::new(class, &vals)
        })
        .collect();
    assert_shells_equal(profile, &granules, "text collation-C");

    // Deterministic non-C: no keys, blooms + NDV + both length units stay.
    let profile = MetaProfile::derive(
        class,
        CollationClass::OtherDeterministic,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    assert_shells_equal(profile, &granules, "text deterministic non-C");

    // All-ASCII corpus (the P-2 witness must mint on both shells).
    let ascii_granules: Vec<TestColumn> = (0..2)
        .map(|_| {
            let vals: Vec<Option<TestValue>> = (0..500)
                .map(|_| {
                    if rng.chance(1, 5) {
                        None
                    } else {
                        Some(TestValue::Bytes(format!("k{}", rng.below(40)).into_bytes()))
                    }
                })
                .collect();
            TestColumn::new(class, &vals)
        })
        .collect();
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::TextCollated).unwrap();
    assert_shells_equal(profile, &ascii_granules, "text all-ascii");
}

#[test]
fn batched_equals_pervalue_packed_numeric_with_poison() {
    use super::{numeric_payload, numeric_payload_nan};
    let class = StorageClass::VarlenaVerbatim;
    let profile = MetaProfile::derive(
        class,
        CollationClass::C,
        TypeSemantics::PackedNumeric { scale: 2 },
    )
    .unwrap();
    let mut rng = Rng::new(0xD57A_0008);
    // Granule 0: clean (every value rescales at scale 2, zeros present).
    // Granule 1: poisoned mid-granule (scale-3 value + NaN) — the coupled
    // degrade must land identically on both shells.
    let clean: Vec<Option<TestValue>> = (0..2000)
        .map(|_| {
            if rng.chance(1, 6) {
                None
            } else {
                Some(TestValue::Bytes(numeric_payload(
                    (rng.next() as i64 % 40_000) as i128 - 20_000,
                    2,
                )))
            }
        })
        .collect();
    let poisoned: Vec<Option<TestValue>> = (0..2000)
        .map(|i| {
            if rng.chance(1, 6) {
                None
            } else if i == 700 {
                Some(TestValue::Bytes(numeric_payload(1005, 3))) // no fit at 2
            } else if i == 900 {
                Some(TestValue::Bytes(numeric_payload_nan()))
            } else {
                Some(TestValue::Bytes(numeric_payload(
                    (rng.next() as i64 % 1000) as i128,
                    2,
                )))
            }
        })
        .collect();
    let granules = vec![
        TestColumn::new(class, &clean),
        TestColumn::new(class, &poisoned),
    ];
    assert_shells_equal(profile, &granules, "packed numeric");
}

#[test]
fn batched_equals_pervalue_with_distribution_feed() {
    // Feed accepted (dict-elected shape): the accumulator is disarmed on
    // both shells and the folds still match.
    let class = StorageClass::VarlenaVerbatim;
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::TextCollated).unwrap();
    let mut rng = Rng::new(0xD57A_0009);
    let vals: Vec<Option<TestValue>> = (0..3000)
        .map(|_| {
            if rng.chance(1, 5) {
                None
            } else {
                Some(TestValue::Bytes(format!("v{:03}", rng.below(80)).into_bytes()))
            }
        })
        .collect();
    let granules = vec![TestColumn::new(class, &vals)];
    let feed = counted_feed(&granules);
    let a = drive_shell(profile, &granules, true, None, Some(feed.clone()));
    let b = drive_shell(profile, &granules, false, None, Some(feed));
    assert_eq!(a, b, "feed-armed shells diverged");
}

/// The byte-sorted counted distinct set of a corpus (the DICT-DEDUP feed's
/// exact shape: strictly byte-ascending, exact non-null counts).
fn counted_feed(granules: &[TestColumn]) -> Vec<(Vec<u8>, u64)> {
    let mut counts: std::collections::BTreeMap<Vec<u8>, u64> = std::collections::BTreeMap::new();
    for col in granules {
        for r in 0..col.rows() {
            if let Some(super::OracleValue::Bytes(b)) = col.value(r) {
                *counts.entry(b.to_vec()).or_insert(0) += 1;
            }
        }
    }
    counts.into_iter().collect()
}

// ---------------------------------------------------------------------------
// D-STATS dict fold-from-codes identity (the charter's dedicated cell)
// ---------------------------------------------------------------------------

/// Build (value granules, code granules) over a text corpus: codes are the
/// byte-rank ranks of the feed (index == global code), row-dense with 0 in
/// null slots — exactly the seal's dict encode currency.
fn dict_corpus(
    seed: u64,
    granule_rows: &[u32],
    ndv: u64,
) -> (Vec<TestColumn>, Vec<Vec<u64>>, Vec<(Vec<u8>, u64)>) {
    let mut rng = Rng::new(seed);
    let cyrillic = ["товар", "пример", "яблоко", "The quick brown fox", ""];
    let granules: Vec<TestColumn> = granule_rows
        .iter()
        .map(|&rows| {
            let vals: Vec<Option<TestValue>> = (0..rows)
                .map(|_| {
                    if rng.chance(1, 5) {
                        None
                    } else if rng.chance(1, 7) {
                        Some(TestValue::Bytes(
                            cyrillic[rng.below(cyrillic.len() as u64) as usize]
                                .as_bytes()
                                .to_vec(),
                        ))
                    } else {
                        Some(TestValue::Bytes(
                            format!("entry-{:04}", rng.below(ndv)).into_bytes(),
                        ))
                    }
                })
                .collect();
            TestColumn::new(StorageClass::VarlenaVerbatim, &vals)
        })
        .collect();
    let feed = counted_feed(&granules);
    let rank_of = |b: &[u8]| -> u64 {
        feed.binary_search_by(|(e, _)| e.as_slice().cmp(b)).expect("in feed") as u64
    };
    let codes: Vec<Vec<u64>> = granules
        .iter()
        .map(|col| {
            (0..col.rows())
                .map(|r| match col.value(r) {
                    Some(super::OracleValue::Bytes(b)) => rank_of(b),
                    _ => 0,
                })
                .collect()
        })
        .collect();
    (granules, codes, feed)
}

/// Drive the codes face over the corpus; the builder must accept it.
fn drive_codes(
    profile: MetaProfile,
    granules: &[TestColumn],
    codes: &[Vec<u64>],
    feed: Vec<(Vec<u8>, u64)>,
) -> ShellOut {
    let mut b = ColumnMeta::with_batch_fold(profile, true);
    b.set_distribution_feed(feed);
    assert!(
        b.dict_code_observe_supported(),
        "codes face must be offered on the dict-elected shape"
    );
    for (g, col) in granules.iter().enumerate() {
        let input = EncodeInput {
            class: StorageClass::VarlenaVerbatim,
            rows: col.rows(),
            datums: &codes[g],
            validity: col.validity.as_deref(),
        };
        b.observe_granule_dict_codes(&input, g as u32);
    }
    let n = granules.len() as u32;
    let grecs: Vec<StatsRecord> = (0..n).map(|g| b.seal_granule(g)).collect();
    let bands: Vec<StatsRecord> = (0..n.div_ceil(crate::format::geom::GRANULES_PER_BAND))
        .map(|band| b.seal_band(band))
        .collect();
    let part = b.seal_part();
    let aux = b.aux_sections();
    let dist = b.distribution();
    ShellOut { grecs, bands, part, aux, dist }
}

#[test]
fn dict_codes_fold_equals_value_walk() {
    let (granules, codes, feed) = dict_corpus(0xD1C7_C0DE, &[4000, 4000, 250], 300);
    for (collation, what) in [
        (CollationClass::C, "collation C (keys armed)"),
        (CollationClass::OtherDeterministic, "deterministic non-C (no keys)"),
    ] {
        let profile = MetaProfile::derive(
            StorageClass::VarlenaVerbatim,
            collation,
            TypeSemantics::TextCollated,
        )
        .unwrap();
        let from_codes = drive_codes(profile, &granules, &codes, feed.clone());
        // Against BOTH value-walk shells, feed armed the same way.
        let batched = drive_shell(profile, &granules, true, None, Some(feed.clone()));
        let pervalue = drive_shell(profile, &granules, false, None, Some(feed.clone()));
        assert_eq!(from_codes, batched, "codes vs batched value walk: {what}");
        assert_eq!(from_codes, pervalue, "codes vs per-value walk: {what}");
    }
}

#[test]
fn dict_codes_fold_all_null_and_constant_granules() {
    // All-null granule between value granules + a constant granule (one
    // distinct code) — sortedness Constant is exact-only; coarse keys must
    // NOT claim it, on either path.
    let class = StorageClass::VarlenaVerbatim;
    let all_null = TestColumn::new(class, &vec![None; 64][..]);
    let constant: Vec<Option<TestValue>> = (0..64)
        .map(|_| Some(TestValue::Bytes(b"only".to_vec())))
        .collect();
    let constant = TestColumn::new(class, &constant);
    let mixed: Vec<Option<TestValue>> = (0..64)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(TestValue::Bytes(format!("w{}", i % 5).into_bytes()))
            }
        })
        .collect();
    let mixed = TestColumn::new(class, &mixed);
    let granules = vec![mixed, all_null, constant];
    let feed = counted_feed(&granules);
    let rank_of = |b: &[u8]| -> u64 {
        feed.binary_search_by(|(e, _)| e.as_slice().cmp(b)).expect("in feed") as u64
    };
    let codes: Vec<Vec<u64>> = granules
        .iter()
        .map(|col| {
            (0..col.rows())
                .map(|r| match col.value(r) {
                    Some(super::OracleValue::Bytes(b)) => rank_of(b),
                    _ => 0,
                })
                .collect()
        })
        .collect();
    let profile = MetaProfile::derive(class, CollationClass::C, TypeSemantics::TextCollated)
        .unwrap();
    let from_codes = drive_codes(profile, &granules, &codes, feed.clone());
    let pervalue = drive_shell(profile, &granules, false, None, Some(feed));
    assert_eq!(from_codes, pervalue, "codes fold: null/constant granules");
}

#[test]
fn dict_codes_face_declines_without_feed_or_batch() {
    let profile = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    // No feed: decline (D2 inherit / feed-off control).
    let b = ColumnMeta::with_batch_fold(profile, true);
    assert!(!b.dict_code_observe_supported(), "no feed must decline");
    // Batch off: decline (the kill switch restores the value walk wholesale).
    let mut b = ColumnMeta::with_batch_fold(profile, false);
    b.set_distribution_feed(vec![(b"x".to_vec(), 1)]);
    assert!(!b.dict_code_observe_supported(), "batch-off must decline");
    // ndv-unsound profile never accepts the feed, so it declines too.
    let nd = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::Nondeterministic,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let mut b = ColumnMeta::with_batch_fold(nd, true);
    b.set_distribution_feed(vec![(b"x".to_vec(), 1)]);
    assert!(!b.dict_code_observe_supported(), "unsound profile must decline");
}

// ---------------------------------------------------------------------------
// the born-RED tooth: a seeded accumulator leak MUST fail the battery
// ---------------------------------------------------------------------------

/// Distinct values engineered to occupy pairwise-DISTINCT HLL registers,
/// so dropping any one of them is guaranteed visible in the registers (no
/// dominated-register coincidence can mask the seeded leak).
fn distinct_register_values(n: usize) -> Vec<Vec<u8>> {
    let mut vals = Vec::new();
    let mut regs = std::collections::HashSet::new();
    let mut i = 0u32;
    while vals.len() < n {
        let sv = format!("seeded-{i:05}").into_bytes();
        let (h1, _) = crate::hash::meta_hash128(&sv);
        let idx = h1 >> (64 - crate::ndv::NDV_PRECISION as u32);
        if regs.insert(idx) {
            vals.push(sv);
        }
        i += 1;
    }
    vals
}

#[test]
fn seeded_divergence_fails_the_equivalence_battery() {
    let class = StorageClass::VarlenaVerbatim;
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::TextCollated).unwrap();
    let rows: Vec<Option<TestValue>> = distinct_register_values(5)
        .into_iter()
        .map(|b| Some(TestValue::Bytes(b)))
        .collect();
    let granules = vec![TestColumn::new(class, &rows)];
    // Armed: the batched hash lane skips the last HLL update — the arms
    // MUST diverge (this is the proof the battery has teeth; the corpus
    // guarantees the dropped register is not covered by another value).
    crate::batch::seed::arm(true);
    let seeded = drive_shell(profile, &granules, true, None, None);
    crate::batch::seed::arm(false);
    let reference = drive_shell(profile, &granules, false, None, None);
    assert_ne!(
        seeded, reference,
        "the seeded accumulator leak was NOT caught — the battery has no teeth"
    );
    // Disarmed: green again.
    let clean = drive_shell(profile, &granules, true, None, None);
    assert_eq!(clean, reference, "disarmed shells must re-converge");
}

#[test]
fn seeded_divergence_fails_the_codes_battery() {
    let class = StorageClass::VarlenaVerbatim;
    let profile =
        MetaProfile::derive(class, CollationClass::C, TypeSemantics::TextCollated).unwrap();
    // Rows = the engineered distinct set in order (last distinct's
    // register unique by construction — the codes face's skip-last leak
    // cannot be masked).
    let values = distinct_register_values(5);
    let rows: Vec<Option<TestValue>> = values
        .iter()
        .map(|b| Some(TestValue::Bytes(b.clone())))
        .collect();
    let granules = vec![TestColumn::new(class, &rows)];
    let feed = counted_feed(&granules);
    let rank_of = |b: &[u8]| -> u64 {
        feed.binary_search_by(|(e, _)| e.as_slice().cmp(b)).expect("in feed") as u64
    };
    let codes: Vec<Vec<u64>> = vec![values.iter().map(|b| rank_of(b)).collect()];
    crate::batch::seed::arm(true);
    let seeded = drive_codes(profile, &granules, &codes, feed.clone());
    crate::batch::seed::arm(false);
    let reference = drive_shell(profile, &granules, false, None, Some(feed.clone()));
    assert_ne!(seeded, reference, "codes-face seeded leak was NOT caught");
    let clean = drive_codes(profile, &granules, &codes, feed);
    assert_eq!(clean, reference, "disarmed codes face must re-converge");
}
