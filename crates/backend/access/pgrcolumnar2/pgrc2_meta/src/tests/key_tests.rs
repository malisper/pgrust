//! Zone-transform properties across every charter-§5 class (the M3-E slice
//! clause "exact zone transforms vs decode oracle across every §5 class"):
//! exact transforms must be order-isomorphic AND equality-faithful against
//! an independent PG-semantics comparator; coarse transforms must be
//! monotone-with-ties in both directions. Adversarial corpora sit on every
//! seam (±0.0, NaN bit patterns, prefix ties at the 8-byte boundary,
//! saturation extremes). The decode-oracle leg re-derives keys from datums
//! decoded through the reference codec and pins them equal.

use core::cmp::Ordering;

use super::{
    memcmp_order, numeric_payload, numeric_payload_nan, numeric_payload_ninf, numeric_payload_pinf,
    numeric_payload_short, pg_f64_cmp, roundtrip_through_reference, Rng, TestColumn, TestValue,
};
use crate::format::class::StorageClass;
use crate::key::{
    bool_key, f32_order_key, f64_order_key, interval_cmp_key, memcmp_fixed_exact_key,
    memcmp_fixed_prefix_key, memcmp_var_prefix_key, numeric_pack_at_scale, signed_word_key,
    timetz_utc_key, unsigned_flip_key, unsigned_small_key, NumericPack,
};

/// Exact law: `cmp(a,b) == key(a).cmp(key(b))` for ALL pairs — order
/// isomorphism and equality faithfulness in one assertion. Returns the
/// pair count (the second tooth: callers assert it is nonzero).
fn check_exact<T>(vals: &[T], key: impl Fn(&T) -> i64, cmp: impl Fn(&T, &T) -> Ordering) -> usize {
    let mut pairs = 0;
    for a in vals {
        for b in vals {
            assert_eq!(
                cmp(a, b),
                key(a).cmp(&key(b)),
                "exact transform broke order/equality faithfulness"
            );
            pairs += 1;
        }
    }
    pairs
}

/// Coarse law: monotone-with-ties (`a ≤ b ⇒ k(a) ≤ k(b)`) and its
/// contrapositive strict form (`k(a) < k(b) ⇒ a < b`).
fn check_coarse<T>(vals: &[T], key: impl Fn(&T) -> i64, cmp: impl Fn(&T, &T) -> Ordering) -> usize {
    let mut pairs = 0;
    for a in vals {
        for b in vals {
            let (ka, kb) = (key(a), key(b));
            match cmp(a, b) {
                Ordering::Less | Ordering::Equal => {
                    assert!(ka <= kb, "coarse transform broke monotonicity")
                }
                Ordering::Greater => assert!(ka >= kb, "coarse transform broke monotonicity"),
            }
            if ka < kb {
                assert_eq!(
                    cmp(a, b),
                    Ordering::Less,
                    "strict keys must prove strict order"
                );
            }
            pairs += 1;
        }
    }
    pairs
}

#[test]
fn signed_word_exact() {
    let mut vals: Vec<i64> = vec![
        i64::MIN,
        i64::MIN + 1,
        -2,
        -1,
        0,
        1,
        2,
        i64::MAX - 1,
        i64::MAX,
    ];
    let mut rng = Rng::new(0x5157_0001);
    for _ in 0..64 {
        vals.push(rng.next() as i64);
    }
    let n = check_exact(&vals, |v| signed_word_key(*v as u64).raw(), |a, b| a.cmp(b));
    assert!(n > 0);
}

#[test]
fn unsigned_small_exact() {
    let mut vals: Vec<u32> = vec![0, 1, u32::MAX - 1, u32::MAX];
    let mut rng = Rng::new(0x5157_0002);
    for _ in 0..64 {
        vals.push(rng.next() as u32);
    }
    let n = check_exact(
        &vals,
        |v| unsigned_small_key(*v as u64).raw(),
        |a, b| a.cmp(b),
    );
    assert!(n > 0);
}

#[test]
fn unsigned_flip_exact_full_domain() {
    let mut vals: Vec<u64> = vec![0, 1, i64::MAX as u64, 1 << 63, u64::MAX - 1, u64::MAX];
    let mut rng = Rng::new(0x5157_0003);
    for _ in 0..64 {
        vals.push(rng.next());
    }
    let n = check_exact(&vals, |v| unsigned_flip_key(*v).raw(), |a, b| a.cmp(b));
    assert!(n > 0);
}

#[test]
fn bool_exact() {
    let vals = [false, true];
    let n = check_exact(&vals, |v| bool_key(*v as u64).raw(), |a, b| a.cmp(b));
    assert!(n > 0);
}

fn adversarial_f64() -> Vec<f64> {
    let mut vals = vec![
        f64::NEG_INFINITY,
        f64::MIN,
        -1.0e300,
        -1.5,
        -1.0,
        -f64::MIN_POSITIVE,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        1.0,
        1.5,
        1.0e300,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
        -f64::NAN,
        // Distinct NaN payloads (quiet + signaling shapes, both signs).
        f64::from_bits(0x7FF0_0000_0000_0001),
        f64::from_bits(0x7FF8_0000_0000_0042),
        f64::from_bits(0xFFF8_0000_0000_0007),
        // Subnormals.
        f64::from_bits(1),
        f64::from_bits(0x8000_0000_0000_0001),
    ];
    let mut rng = Rng::new(0x5157_0004);
    for _ in 0..48 {
        vals.push(f64::from_bits(rng.next()));
    }
    vals
}

#[test]
fn f64_order_key_exact_under_pg_semantics() {
    // PG float8_cmp: NaN = NaN > everything, -0.0 = +0.0 — the transform
    // must be exact for THIS order, including every NaN bit pattern.
    let vals = adversarial_f64();
    let n = check_exact(
        &vals,
        |v| f64_order_key(*v).raw(),
        |a, b| pg_f64_cmp(*a, *b),
    );
    assert!(n > 0);
}

#[test]
fn f32_order_key_exact_under_pg_semantics() {
    let mut vals: Vec<f32> = vec![
        f32::NEG_INFINITY,
        f32::MIN,
        -1.0,
        -0.0,
        0.0,
        f32::MIN_POSITIVE,
        1.0,
        f32::MAX,
        f32::INFINITY,
        f32::NAN,
        -f32::NAN,
        f32::from_bits(0x7F80_0001),
        f32::from_bits(0xFFC0_0007),
    ];
    let mut rng = Rng::new(0x5157_0005);
    for _ in 0..48 {
        vals.push(f32::from_bits(rng.next() as u32));
    }
    let n = check_exact(
        &vals,
        |v| f32_order_key(*v).raw(),
        |a, b| pg_f64_cmp(*a as f64, *b as f64),
    );
    assert!(n > 0);
}

#[test]
fn memcmp_fixed_exact_at_or_below_8() {
    // macaddr (6 B) and macaddr8 (8 B): whole-image embeds are exact.
    for len in [1usize, 3, 6, 8] {
        let mut vals: Vec<Vec<u8>> = vec![vec![0u8; len], vec![0xFF; len]];
        let mut rng = Rng::new(0x5157_0006 + len as u64);
        for _ in 0..48 {
            vals.push((0..len).map(|_| rng.next() as u8).collect());
        }
        // Adversarial: equal high bytes, differing last byte.
        let mut a = vec![0xAB; len];
        let mut b = vec![0xAB; len];
        if let Some(last) = b.last_mut() {
            *last = 0xAC;
        }
        if let Some(last) = a.last_mut() {
            *last = 0xAA;
        }
        vals.push(a);
        vals.push(b);
        let n = check_exact(
            &vals,
            |v| memcmp_fixed_exact_key(v).raw(),
            |a, b| memcmp_order(a, b),
        );
        assert!(n > 0);
    }
}

#[test]
fn uuid_prefix_coarse() {
    // 16-byte images sharing 8-byte prefixes: coarse, monotone-with-ties.
    let mut vals: Vec<Vec<u8>> = Vec::new();
    let mut rng = Rng::new(0x5157_0007);
    for _ in 0..32 {
        vals.push((0..16).map(|_| rng.next() as u8).collect());
    }
    // Shared-prefix families (ties that differ past byte 8).
    let base: Vec<u8> = (0..16).map(|i| i as u8).collect();
    for tail in 0..4u8 {
        let mut v = base.clone();
        v[15] = tail;
        v[8] = tail.wrapping_mul(37);
        vals.push(v);
    }
    let n = check_coarse(
        &vals,
        |v| memcmp_fixed_prefix_key(v).raw(),
        |a, b| memcmp_order(a, b),
    );
    assert!(n > 0);
}

#[test]
fn var_prefix_coarse_bytea_and_c_text() {
    let mut vals: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"\x00".to_vec(),
        b"a".to_vec(),
        b"ab".to_vec(),
        b"abcdefg".to_vec(),
        b"abcdefgh".to_vec(),     // exactly 8
        b"abcdefgh\x00".to_vec(), // ties with the 8-byte prefix
        b"abcdefghi".to_vec(),    // ties with the 8-byte prefix
        b"abcdefghzzzz".to_vec(), // same 8-prefix, different tail
        b"abcdefgi".to_vec(),     // differs at byte 8
        vec![0xFF; 12],
        vec![0xFF; 8],
    ];
    let mut rng = Rng::new(0x5157_0008);
    for _ in 0..40 {
        let len = rng.below(14) as usize;
        vals.push((0..len).map(|_| rng.next() as u8).collect());
    }
    let n = check_coarse(
        &vals,
        |v| memcmp_var_prefix_key(v).raw(),
        |a, b| memcmp_order(a, b),
    );
    assert!(n > 0);
}

#[test]
fn interval_coarse_with_pg_cmp_value_semantics() {
    // Oracle: PG interval_cmp_value span as i128 (no saturation).
    fn image(time: i64, day: i32, month: i32) -> Vec<u8> {
        let mut v = Vec::with_capacity(16);
        v.extend_from_slice(&time.to_le_bytes());
        v.extend_from_slice(&day.to_le_bytes());
        v.extend_from_slice(&month.to_le_bytes());
        v
    }
    fn span(img: &[u8]) -> i128 {
        let time = i64::from_le_bytes(img[0..8].try_into().unwrap());
        let day = i32::from_le_bytes(img[8..12].try_into().unwrap());
        let month = i32::from_le_bytes(img[12..16].try_into().unwrap());
        time as i128 + (month as i128) * 30 * 86_400_000_000 + (day as i128) * 86_400_000_000
    }
    let mut vals = vec![
        image(0, 0, 0),
        image(1, 0, 0),
        image(-1, 0, 0),
        image(0, 30, 0),
        image(0, 0, 1), // 1 month == 30 days: the cmp-value TIE
        image(0, 29, 0),
        image(86_400_000_000, 29, 0),
        image(i64::MAX, i32::MAX, i32::MAX), // saturates
        image(i64::MIN, i32::MIN, i32::MIN), // saturates
        image(0, 0, i32::MAX),               // saturates
        image(0, 0, i32::MIN),
    ];
    let mut rng = Rng::new(0x5157_0009);
    for _ in 0..40 {
        vals.push(image(
            rng.next() as i64 % 1_000_000_000_000,
            (rng.next() as i32) % 10_000,
            (rng.next() as i32) % 1_000,
        ));
    }
    let n = check_coarse(
        &vals,
        |v| interval_cmp_key(v).expect("well-formed").raw(),
        |a, b| span(a).cmp(&span(b)),
    );
    assert!(n > 0);
    // The PG equality fact: 1 month and 30 days share one key (a tie —
    // range-only by construction).
    assert_eq!(
        interval_cmp_key(&image(0, 30, 0)).unwrap(),
        interval_cmp_key(&image(0, 0, 1)).unwrap()
    );
    // Malformed images decline.
    assert!(interval_cmp_key(&[0u8; 15]).is_none());
}

#[test]
fn timetz_coarse_with_pg_primary_comparand() {
    fn image(time: i64, zone: i32) -> Vec<u8> {
        let mut v = Vec::with_capacity(12);
        v.extend_from_slice(&time.to_le_bytes());
        v.extend_from_slice(&zone.to_le_bytes());
        v
    }
    fn comparand(img: &[u8]) -> i128 {
        let time = i64::from_le_bytes(img[0..8].try_into().unwrap());
        let zone = i32::from_le_bytes(img[8..12].try_into().unwrap());
        time as i128 + (zone as i128) * 1_000_000
    }
    let mut vals = vec![
        image(0, 0),
        image(3_600_000_000, -3600), // 01:00 at +01 == 00:00 UTC: ties with (0, 0)
        image(0, 3600),
        image(86_400_000_000, 0),
        image(86_400_000_000, 57_600),
        image(0, -57_600),
    ];
    let mut rng = Rng::new(0x5157_000A);
    for _ in 0..40 {
        vals.push(image(
            (rng.next() % 86_400_000_001) as i64,
            ((rng.next() as i32) % 57_601).saturating_mul(1),
        ));
    }
    let n = check_coarse(
        &vals,
        |v| timetz_utc_key(v).expect("well-formed").raw(),
        |a, b| comparand(a).cmp(&comparand(b)),
    );
    assert!(n > 0);
    assert!(timetz_utc_key(&[0u8; 11]).is_none());
}

// ---------------------------------------------------------------------------
// packed numeric: exact-rescale-or-abstain at the transform grain
// ---------------------------------------------------------------------------

#[test]
fn numeric_pack_roundtrips_generated_images() {
    let mut rng = Rng::new(0x5157_000B);
    for _ in 0..200 {
        let v = (rng.next() as i64 as i128) >> (rng.below(40) as u32);
        let scale = rng.below(10) as u32;
        let payload = numeric_payload(v, scale);
        assert_eq!(
            numeric_pack_at_scale(&payload, scale as i32),
            NumericPack::Packed(v as i64),
            "value {v} at scale {scale} must round-trip"
        );
    }
}

#[test]
fn numeric_pack_short_form_parses_like_long_form() {
    for (v, s) in [
        (0i128, 0u32),
        (1, 0),
        (-1, 0),
        (12_345, 2),
        (-9_999, 4),
        (150, 2),
    ] {
        let short = numeric_payload_short(v, s);
        let long = numeric_payload(v, s);
        assert_eq!(
            numeric_pack_at_scale(&short, s as i32),
            numeric_pack_at_scale(&long, s as i32),
            "short and long forms must agree for {v} at {s}"
        );
        assert_eq!(
            numeric_pack_at_scale(&short, s as i32),
            NumericPack::Packed(v as i64)
        );
    }
}

#[test]
fn numeric_pack_cross_scale_exactness() {
    // 1.50 (scale 2) at scale 4 → 15000 exactly.
    let p = numeric_payload(150, 2);
    assert_eq!(numeric_pack_at_scale(&p, 4), NumericPack::Packed(15_000));
    // 1.50 at scale 1 → 15 exactly (trailing zero drops exactly).
    assert_eq!(numeric_pack_at_scale(&p, 1), NumericPack::Packed(15));
    // 1.55 at scale 1 → NOT representable (would round).
    let p = numeric_payload(155, 2);
    assert_eq!(numeric_pack_at_scale(&p, 1), NumericPack::Unrepresentable);
    // Overflow at scale.
    let p = numeric_payload(i64::MAX as i128, 0);
    assert_eq!(numeric_pack_at_scale(&p, 0), NumericPack::Packed(i64::MAX));
    assert_eq!(numeric_pack_at_scale(&p, 2), NumericPack::Unrepresentable);
}

#[test]
fn numeric_pack_specials_and_malformed_are_typed() {
    assert_eq!(
        numeric_pack_at_scale(&numeric_payload_nan(), 0),
        NumericPack::Special
    );
    assert_eq!(
        numeric_pack_at_scale(&numeric_payload_pinf(), 0),
        NumericPack::Special
    );
    assert_eq!(
        numeric_pack_at_scale(&numeric_payload_ninf(), 0),
        NumericPack::Special
    );
    // Truncated header.
    assert_eq!(numeric_pack_at_scale(&[0x12], 0), NumericPack::Malformed);
    // Odd digit-region length.
    let mut p = numeric_payload(150, 2);
    p.push(0);
    assert_eq!(numeric_pack_at_scale(&p, 2), NumericPack::Malformed);
    // Digit out of base range.
    let mut p = numeric_payload(150, 2);
    let last = p.len() - 2;
    p[last..].copy_from_slice(&10_000u16.to_le_bytes());
    assert_eq!(numeric_pack_at_scale(&p, 2), NumericPack::Malformed);
}

#[test]
fn numeric_pack_key_is_exact_across_scales() {
    // Order isomorphism over a value set with mixed magnitudes, all packed
    // at one shared scale (the elected-lane situation).
    let scale = 3i32;
    let vals: Vec<i64> = vec![
        i64::MIN / 1000,
        -1_000_000,
        -1_001,
        -1_000,
        -1,
        0,
        1,
        999,
        1_000,
        123_456_789,
        i64::MAX / 1000,
    ];
    let n = check_exact(
        &vals,
        |v| match numeric_pack_at_scale(&numeric_payload(*v as i128, scale as u32), scale) {
            NumericPack::Packed(p) => p,
            other => panic!("must pack: {other:?}"),
        },
        |a, b| a.cmp(b),
    );
    assert!(n > 0);
}

// ---------------------------------------------------------------------------
// the decode-oracle leg: keys derived from DECODED datums == keys from
// the input values, for a word class and both pointer classes
// ---------------------------------------------------------------------------

#[test]
fn keys_survive_the_reference_decode_loop() {
    // Signed word.
    let vals: Vec<Option<TestValue>> = vec![
        Some(TestValue::Word(-5i64 as u64)),
        None,
        Some(TestValue::Word(7)),
        Some(TestValue::Word(i64::MIN as u64)),
    ];
    let col = TestColumn::new(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        &vals,
    );
    let dec = roundtrip_through_reference(&col);
    for r in 0..col.rows() {
        if !col.valid(r) {
            continue;
        }
        assert_eq!(
            crate::key::signed_word_key(col.datums[r as usize]),
            crate::key::signed_word_key(dec.datums[r as usize]),
            "decoded datum changed the key at row {r}"
        );
    }
    // uuid (Fixed 16, coarse prefix).
    let mut rng = Rng::new(0x5157_000C);
    let vals: Vec<Option<TestValue>> = (0..16)
        .map(|i| {
            if i % 5 == 3 {
                None
            } else {
                Some(TestValue::Bytes(
                    (0..16).map(|_| rng.next() as u8).collect(),
                ))
            }
        })
        .collect();
    let col = TestColumn::new(StorageClass::Fixed { len: 16 }, &vals);
    let dec = roundtrip_through_reference(&col);
    for r in 0..col.rows() {
        if !col.valid(r) {
            continue;
        }
        // SAFETY: decoded pointer datums point into the held arena.
        let img = unsafe { core::slice::from_raw_parts(dec.datums[r as usize] as *const u8, 16) };
        let from_input = match col.value(r).unwrap() {
            super::OracleValue::Bytes(b) => crate::key::memcmp_fixed_prefix_key(b),
            _ => unreachable!(),
        };
        assert_eq!(from_input, crate::key::memcmp_fixed_prefix_key(img));
    }
    // C-text prefix (VarlenaVerbatim, coarse).
    let vals: Vec<Option<TestValue>> = vec![
        Some(TestValue::Bytes(b"".to_vec())),
        Some(TestValue::Bytes(b"abcdefghij".to_vec())),
        None,
        Some(TestValue::Bytes(b"abcdefgh".to_vec())),
        Some(TestValue::Bytes(vec![0xFF; 9])),
    ];
    let col = TestColumn::new(StorageClass::VarlenaVerbatim, &vals);
    let dec = roundtrip_through_reference(&col);
    for r in 0..col.rows() {
        if !col.valid(r) {
            continue;
        }
        let p = dec.datums[r as usize] as *const u8;
        // SAFETY: decoded varlena datums are varlena-shaped in the arena.
        let header = u32::from_le_bytes(
            unsafe { core::slice::from_raw_parts(p, 4) }
                .try_into()
                .unwrap(),
        );
        let payload_len = (header >> 2) as usize - 4;
        let payload = unsafe { core::slice::from_raw_parts(p.add(4), payload_len) };
        let from_input = match col.value(r).unwrap() {
            super::OracleValue::Bytes(b) => crate::key::memcmp_var_prefix_key(b),
            _ => unreachable!(),
        };
        assert_eq!(from_input, crate::key::memcmp_var_prefix_key(payload));
    }
}

// ---------------------------------------------------------------------------
// value-faithful inversion (the metaagg footer MIN/MAX law)
// ---------------------------------------------------------------------------

/// Round-trip: for every value-faithful derivation, key(word) inverts back
/// to the word — including domain extremes; non-faithful derivations
/// refuse inversion outright.
#[test]
fn value_faithful_inversion_round_trips() {
    use crate::key::{
        bool_key, signed_word_key, unsigned_flip_key, unsigned_small_key, KeyDerivation,
    };
    // SignedWord: sign-extended datum words, extremes included.
    let d = KeyDerivation::SignedWord;
    assert!(d.value_faithful());
    for w in [
        0u64,
        1,
        (-1i64) as u64,
        i64::MAX as u64,
        i64::MIN as u64,
        (-40923i64) as u64,
    ] {
        let k = signed_word_key(w);
        assert_eq!(d.exact_key_to_datum_word(k.raw()), Some(w));
    }
    // UnsignedWordSmall: identity on 0..=u32::MAX; out-of-image keys refuse.
    let d = KeyDerivation::UnsignedWordSmall;
    assert!(d.value_faithful());
    for w in [0u64, 1, u32::MAX as u64] {
        let k = unsigned_small_key(w);
        assert_eq!(d.exact_key_to_datum_word(k.raw()), Some(w));
    }
    assert_eq!(d.exact_key_to_datum_word(-1), None, "outside the embed image");
    assert_eq!(d.exact_key_to_datum_word(u32::MAX as i64 + 1), None);
    // UnsignedWordFlip: the sign-bit flip is an involution on all of u64.
    let d = KeyDerivation::UnsignedWordFlip;
    assert!(d.value_faithful());
    for w in [0u64, 1, u64::MAX, 1u64 << 63, (1u64 << 63) - 1] {
        let k = unsigned_flip_key(w);
        assert_eq!(d.exact_key_to_datum_word(k.raw()), Some(w));
    }
    // Bool: 0/1 only; anything else is a foreign record.
    let d = KeyDerivation::Bool;
    assert!(d.value_faithful());
    assert_eq!(d.exact_key_to_datum_word(bool_key(0).raw()), Some(0));
    assert_eq!(d.exact_key_to_datum_word(bool_key(7).raw()), Some(1));
    assert_eq!(d.exact_key_to_datum_word(2), None);
    // NOT faithful: exact-but-class-collapsing and coarse derivations.
    for d in [
        KeyDerivation::Float32,
        KeyDerivation::Float64,
        KeyDerivation::PackedNumeric { scale: 2 },
        KeyDerivation::MemcmpFixed { len: 6 },
        KeyDerivation::MemcmpFixed { len: 16 },
        KeyDerivation::MemcmpVarPrefix,
        KeyDerivation::IntervalCmpSat,
        KeyDerivation::TimetzUtc,
        KeyDerivation::None,
    ] {
        assert!(!d.value_faithful(), "{d:?} must not claim value-faithful");
        assert_eq!(d.exact_key_to_datum_word(0), None, "{d:?} must refuse inversion");
    }
}
