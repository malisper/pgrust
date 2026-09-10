// PROVENANCE (O-1 vendoring, lane M3-B): vendored VERBATIM from
// origin/appbench-types @ 4000b2794c773909aa985d87e4d38ee842cffaab — donor tests carried;
// no semantic edits during the move (adaptations are separate commits).

//! Fixed-scale scaled-integer pack for numeric — the A6b fit/reconstruct
//! core for pgrcolumnar's numeric fast lane
//! (docs/design/appbench-types-milestone-a.md §6-A6b).
//!
//! keypack.rs solves the neighbouring problem (value-canonical grouping
//! keys), but its byte-identity gate requires MINIMAL display scale and so
//! refuses typmod-scaled values like '1.50'. Columnar NUMERIC(P,S) chunks
//! are exactly that shape: every value displays at the SAME dscale S. This
//! module fixes S per chunk instead: a value fits iff it is finite, its
//! dscale == S exactly, and |value·10^S| stays within the i64 mantissa
//! budget. Reconstruction (int128_to_var + dscale = S + weight shift +
//! make_result) is then byte-identical by the same canonical-digit argument
//! keypack proves.
//!
//! The uniform-dscale rule is a byte-identity requirement, not a value
//! question: '1.5' would fit S=2 as 150·10^-2, but its stored image displays
//! "1.5" while reconstruction at S=2 displays "1.50" — output bytes are
//! user-visible, so it refuses.
//!
//! Packed-i64 ordering equals `cmp_numerics` on any set of fitting values
//! (at a shared S, value ↦ value·10^S is order-preserving), so i64 zone maps
//! over packed chunks are sound.

use crate::var::{int128_to_var, make_result, NumericImage, NumericVar};
use crate::{Num, NBASE, NUMERIC_DSCALE_MAX, NUMERIC_NEG, NUMERIC_POS};
use types_error::PgResult;

/// Mantissa-magnitude budget for the fast lane: keypack's shipped 2^55-1
/// bound (milestone D5 ruling), reusing its proven no-wrap argument for
/// consumers that pack the i64 into wider composite forms.
pub const FIXED_SCALE_MANT_ABS_MAX: i64 = (1i64 << 55) - 1;

// Base-10000 digit-count bound for the i128 walk (keypack's argument): a
// canonical 6-digit image scales to at least 10^17 (> the 2^55-1 budget)
// even after the <=3-decimal-digit exact division below, so longer images
// can never fit — and 6 keeps the accumulator far from i128 overflow.
const FIT_MAX_NDIGITS: usize = 6;

/// Exact fit test: `Some(value·10^scale)` iff `num` is finite, its dscale ==
/// `scale` exactly (uniform-dscale rule — module doc), and the scaled
/// magnitude is within [`FIXED_SCALE_MANT_ABS_MAX`]. NaN/±Inf and
/// non-canonical stored digit forms refuse.
///
/// EXACTNESS CONSTRAINT: this must remain a pure digit walk. numeric_int8 /
/// var_to_int64 / var_to_int128 implement C cast semantics and ROUND to the
/// nearest integer — routed through them, an image carrying digit content
/// below `scale` would pack to a nearby WRONG value instead of refusing.
pub fn fixed_scale_fit(num: Num<'_>, scale: i32) -> Option<i64> {
    if num.is_special() {
        return None;
    }
    // num.dscale() is mask-bounded to [0, NUMERIC_DSCALE_MAX], so an
    // out-of-range `scale` can never match and refuses here too.
    if num.dscale() != scale {
        return None;
    }
    let digits = num.digits();
    let nd = digits.len();
    if nd == 0 {
        return Some(0);
    }
    if nd > FIT_MAX_NDIGITS {
        return None;
    }
    // Reconstruction canonicalizes through make_result (leading/trailing
    // zero base-10000 digits stripped); a non-canonical stored image would
    // not round-trip byte-identically, so refuse it (keypack's gate).
    if digits[0] == 0 || digits[nd - 1] == 0 {
        return None;
    }
    let mut m: i128 = 0;
    for &d in digits {
        if !(0..NBASE as i16).contains(&d) {
            return None;
        }
        m = m * 10000 + d as i128;
    }
    // value = m × 10^e (m > 0), so value·10^scale = m × 10^p.
    let e = (num.weight() - (nd as i32 - 1)) * 4;
    let p = e + scale;
    let budget = FIXED_SCALE_MANT_ABS_MAX as i128;
    let v = if p >= 0 {
        // v >= m and v >= 10^p: refuse both factors before multiplying so
        // the product cannot overflow i128 (m < 10^24 here).
        if m > budget || p > 16 {
            return None;
        }
        m * 10i128.pow(p as u32)
    } else {
        // Digits extend below 10^-scale: exact only when they are trailing
        // decimal zeros inside the last base-10000 digit. That digit is
        // nonzero (canonical), so m admits at most 3 factors of ten —
        // deeper digit content is unrepresentable at this scale and
        // refuses (never rounds).
        let k = -p as u32;
        if k > 3 {
            return None;
        }
        let pow = 10i128.pow(k);
        if m % pow != 0 {
            return None;
        }
        m / pow
    };
    if v > budget {
        return None;
    }
    debug_assert!(num.sign() == NUMERIC_POS || num.sign() == NUMERIC_NEG);
    Some(if num.sign() == NUMERIC_NEG {
        -(v as i64)
    } else {
        v as i64
    })
}

/// Reconstruct the numeric image of a packed mantissa: value = v·10^-scale
/// displayed at dscale = scale. For every (v, scale) [`fixed_scale_fit`]
/// produces, the result is byte-identical to the packed datum: make_result
/// re-canonicalizes the digit form the fit gate proved canonical, and dscale
/// is pinned to the shared scale the gate matched.
pub fn fixed_scale_unpack(v: i64, scale: i32) -> PgResult<NumericImage> {
    debug_assert!((0..=NUMERIC_DSCALE_MAX).contains(&scale));
    debug_assert!(v.unsigned_abs() <= FIXED_SCALE_MANT_ABS_MAX as u64);
    let mut var = NumericVar::new();
    if v == 0 {
        var.set_zero();
        var.dscale = scale;
        return make_result(var.view());
    }
    // value = (v × 10^shift) × 10000^-((scale+shift)/4): pad the fraction to
    // a whole base-10000 digit, then shift whole digits via the weight
    // (keypack's reconstruction, with dscale fixed at the shared scale).
    let shift = (4 - (scale % 4)) % 4;
    let mval = (v as i128) * 10i128.pow(shift as u32);
    int128_to_var(mval, &mut var);
    var.weight -= (scale + shift) / 4;
    var.dscale = scale;
    make_result(var.view())
}

/// [`fixed_scale_unpack`] widened to i128 mantissas — the AGGREGATE-side
/// reconstruction (a SUM over packed scale-`scale` mantissas is itself a
/// scale-`scale` mantissa, exact in i128 for any row count the engine
/// serves: |Σ| <= n·(2^55−1) < 2^119 for n < 2^64). PG's sum(numeric)
/// result carries dscale = max input dscale = the shared scale, so the
/// image built here is byte-identical to C's accumulated sum image.
///
/// The fraction pad (×10^shift, shift <= 3) is checked: it cannot
/// overflow below |v| < 2^124, far above any witnessed sum — the error
/// arm exists so the law stays total, never wrapping.
pub fn fixed_scale_unpack_i128(v: i128, scale: i32) -> PgResult<NumericImage> {
    debug_assert!((0..=NUMERIC_DSCALE_MAX).contains(&scale));
    let mut var = NumericVar::new();
    if v == 0 {
        var.set_zero();
        var.dscale = scale;
        return make_result(var.view());
    }
    let shift = (4 - (scale % 4)) % 4;
    let mval = v
        .checked_mul(10i128.pow(shift as u32))
        .ok_or_else(|| Box::new(crate::numeric_overflow_error()))?;
    int128_to_var(mval, &mut var);
    var.weight -= (scale + shift) / 4;
    var.dscale = scale;
    make_result(var.view())
}

/// Why a chunk refused the fast lane (the writer's demotion reason).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixedScaleRefusal {
    /// NaN or ±Inf present.
    Special,
    /// A value's dscale differs from the elected shared scale.
    MixedDscale,
    /// A value's scaled magnitude exceeds [`FIXED_SCALE_MANT_ABS_MAX`], or
    /// its stored image is non-canonical / carries digit content below the
    /// shared scale (both unreachable for PG-produced datums).
    Overflow,
}

/// A fully packed chunk: every value fit the shared `scale`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedScaleChunk {
    pub scale: i32,
    pub packed: Vec<i64>,
}

/// Chunk election for the pgrcolumnar writer: elect S = the FIRST value's
/// dscale, pack the whole chunk at it, or report why the chunk must demote
/// to the varlena lane. Empty input elects scale 0 vacuously.
pub fn fixed_scale_elect<'a, I>(values: I) -> Result<FixedScaleChunk, FixedScaleRefusal>
where
    I: IntoIterator<Item = Num<'a>>,
{
    let mut iter = values.into_iter();
    let mut packed = Vec::with_capacity(iter.size_hint().0);
    let first = match iter.next() {
        Some(num) => num,
        None => return Ok(FixedScaleChunk { scale: 0, packed }),
    };
    if first.is_special() {
        return Err(FixedScaleRefusal::Special);
    }
    let scale = first.dscale();
    for num in core::iter::once(first).chain(iter) {
        if num.is_special() {
            return Err(FixedScaleRefusal::Special);
        }
        if num.dscale() != scale {
            return Err(FixedScaleRefusal::MixedDscale);
        }
        match fixed_scale_fit(num, scale) {
            Some(v) => packed.push(v),
            None => return Err(FixedScaleRefusal::Overflow),
        }
    }
    Ok(FixedScaleChunk { scale, packed })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{cmp_numerics, make_numeric_typmod, numeric_apply_typmod};

    // Parse via the production numeric_in path so fit/unpack are tested
    // against PG-canonical images.
    fn img(s: &str) -> NumericImage {
        crate::io::numeric_in(s, -1, None)
            .expect("parse")
            .expect("non-soft parse")
    }

    /// NUMERIC(P,S)-coerced image — the production shape the fast lane sees.
    fn img_ps(s: &str, precision: i32, scale: i32) -> NumericImage {
        numeric_apply_typmod(img(s).num(), make_numeric_typmod(precision, scale))
            .expect("typmod fits")
    }

    const BUDGET: i64 = FIXED_SCALE_MANT_ABS_MAX; // 36028797018963967

    fn lcg(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state
    }

    /// Decimal string with exactly `scale` fraction digits, so numeric_in
    /// yields dscale == scale.
    fn decimal_string(mant: i64, scale: i32) -> String {
        let neg = mant < 0;
        let mut a = mant.unsigned_abs().to_string();
        let s = scale as usize;
        if a.len() <= s {
            a = format!("{}{a}", "0".repeat(s + 1 - a.len()));
        }
        let dot = a.len() - s;
        let body = if s == 0 {
            a
        } else {
            format!("{}.{}", &a[..dot], &a[dot..])
        };
        if neg {
            format!("-{body}")
        } else {
            body
        }
    }

    #[test]
    fn typmod_scaled_values_roundtrip_byte_identically() {
        for (s, p, sc, want) in [
            ("1.5", 10, 2, 150),
            ("-1.5", 10, 2, -150),
            ("0", 10, 2, 0),
            ("1", 10, 2, 100),
            ("1.05", 10, 2, 105),
            ("0.10", 10, 2, 10),
            ("123.456", 20, 4, 1234560),
            ("59", 5, 0, 59),
            ("10000", 10, 0, 10000),
            ("0.0001", 10, 4, 1),
            ("360287970189639.67", 20, 2, BUDGET),
            ("-360287970189639.67", 20, 2, -BUDGET),
        ] {
            let image = img_ps(s, p, sc);
            assert_eq!(image.num().dscale(), sc, "typmod dscale for {s}");
            let v = fixed_scale_fit(image.num(), sc)
                .unwrap_or_else(|| panic!("{s} must fit at scale {sc}"));
            assert_eq!(v, want, "packed value for {s}");
            let back = fixed_scale_unpack(v, sc).expect("unpack");
            assert_eq!(back.as_bytes(), image.as_bytes(), "roundtrip bytes for {s}");
        }
    }

    #[test]
    fn keypack_minimal_dscale_gate_is_lifted() {
        // The reason this module exists: keypack refuses typmod-scaled
        // display forms; the fixed lane packs them.
        let image = img_ps("1.5", 10, 2);
        assert_eq!(
            crate::keypack::numeric_key_pack(image.num(), (1u64 << 55) - 1),
            None
        );
        assert_eq!(fixed_scale_fit(image.num(), 2), Some(150));
    }

    #[test]
    fn uniform_dscale_rule_refuses_value_level_fits() {
        // '1.5' the VALUE fits scale 2, but its image displays "1.5" —
        // refuse (output bytes are user-visible).
        assert_eq!(fixed_scale_fit(img("1.5").num(), 2), None);
        // Never rounds: dscale 3 at S=2 refuses, not Some(150|151).
        assert_eq!(fixed_scale_fit(img("1.505").num(), 2), None);
        // Mismatch in the other direction refuses too.
        assert_eq!(fixed_scale_fit(img_ps("1.5", 10, 2).num(), 3), None);
        // Zero follows the same rule: dscale must match S exactly.
        assert_eq!(fixed_scale_fit(img("0").num(), 2), None);
        assert_eq!(fixed_scale_fit(img_ps("0", 10, 2).num(), 2), Some(0));
    }

    #[test]
    fn budget_boundary_is_exact() {
        assert_eq!(
            fixed_scale_fit(img("36028797018963967").num(), 0),
            Some(BUDGET)
        );
        assert_eq!(fixed_scale_fit(img("36028797018963968").num(), 0), None);
        assert_eq!(
            fixed_scale_fit(img("-36028797018963967").num(), 0),
            Some(-BUDGET)
        );
        assert_eq!(fixed_scale_fit(img("-36028797018963968").num(), 0), None);
        assert_eq!(
            fixed_scale_fit(img_ps("360287970189639.67", 20, 2).num(), 2),
            Some(BUDGET)
        );
        assert_eq!(
            fixed_scale_fit(img_ps("360287970189639.68", 20, 2).num(), 2),
            None
        );
        assert_eq!(fixed_scale_fit(img("1e30").num(), 0), None);
    }

    #[test]
    fn specials_refuse() {
        for s in ["NaN", "Infinity", "-Infinity"] {
            for scale in [0, 2] {
                assert_eq!(fixed_scale_fit(img(s).num(), scale), None, "{s} at {scale}");
            }
        }
    }

    #[test]
    fn long_header_dscale_roundtrips() {
        // dscale 100 > NUMERIC_SHORT_DSCALE_MAX exercises the long-header
        // make_result arm on both sides.
        let s = format!("0.{}1", "0".repeat(99));
        let image = img(&s);
        assert_eq!(image.num().dscale(), 100);
        let v = fixed_scale_fit(image.num(), 100).expect("must fit");
        assert_eq!(v, 1);
        let back = fixed_scale_unpack(v, 100).expect("unpack");
        assert_eq!(back.as_bytes(), image.as_bytes());
    }

    #[test]
    fn property_roundtrip_over_random_mantissas() {
        let mut st: u64 = 0x243F6A8885A308D3;
        for i in 0..4000u32 {
            let r1 = lcg(&mut st);
            let r2 = lcg(&mut st);
            let mut mag = (r1 % (BUDGET as u64 + 1)) as i64;
            if i % 3 == 0 {
                // Bias a third of samples small: exercises zero padding,
                // sub-1 values, and the exact-division fit arm.
                mag %= 1000;
            }
            let mant = if r2 & 1 == 1 { -mag } else { mag };
            let scale = ((r2 >> 1) % 9) as i32;
            let s = decimal_string(mant, scale);
            let image = img(&s);
            assert_eq!(image.num().dscale(), scale, "dscale of {s}");
            assert_eq!(
                fixed_scale_fit(image.num(), scale),
                Some(mant),
                "fit of {s} at {scale}"
            );
            let back = fixed_scale_unpack(mant, scale).expect("unpack");
            assert_eq!(back.as_bytes(), image.as_bytes(), "roundtrip bytes for {s}");
        }
    }

    #[test]
    fn packed_order_agrees_with_cmp_numerics() {
        // Zone-map soundness: i64 order over packed values == cmp_numerics.
        let scale = 2;
        let mut vals: Vec<i64> = vec![
            -BUDGET, -1234567, -100, -1, 0, 1, 99, 100, 101, 1234567, BUDGET,
        ];
        let mut st: u64 = 0x13198A2E03707344;
        for _ in 0..200 {
            let r1 = lcg(&mut st);
            let r2 = lcg(&mut st);
            let mag = (r1 % (BUDGET as u64 + 1)) as i64;
            vals.push(if r2 & 1 == 1 { -mag } else { mag });
        }
        let items: Vec<(i64, NumericImage)> = vals
            .into_iter()
            .map(|v| {
                let image = img(&decimal_string(v, scale));
                assert_eq!(fixed_scale_fit(image.num(), scale), Some(v));
                (v, image)
            })
            .collect();
        for (vi, ni) in &items {
            for (vj, nj) in &items {
                let want = cmp_numerics(ni.num(), nj.num()).signum();
                assert_eq!(
                    vi.cmp(vj) as i32,
                    want,
                    "packed order vs cmp_numerics for {vi} vs {vj}"
                );
            }
        }
    }

    #[test]
    fn election_packs_uniform_chunk() {
        let imgs: Vec<NumericImage> = ["1.50", "2.75", "0.00", "-0.25", "360287970189639.67"]
            .iter()
            .map(|s| img_ps(s, 20, 2))
            .collect();
        let chunk = fixed_scale_elect(imgs.iter().map(|i| i.num())).expect("uniform chunk");
        assert_eq!(chunk.scale, 2);
        assert_eq!(chunk.packed, vec![150, 275, 0, -25, BUDGET]);
        // Roundtrip the whole chunk.
        for (v, image) in chunk.packed.iter().zip(&imgs) {
            let back = fixed_scale_unpack(*v, chunk.scale).expect("unpack");
            assert_eq!(back.as_bytes(), image.as_bytes());
        }
    }

    #[test]
    fn election_elects_first_values_dscale() {
        let ints: Vec<NumericImage> = ["7", "10", "59"].iter().map(|s| img(s)).collect();
        let chunk = fixed_scale_elect(ints.iter().map(|i| i.num())).expect("int chunk");
        assert_eq!(chunk.scale, 0);
        assert_eq!(chunk.packed, vec![7, 10, 59]);
    }

    #[test]
    fn unpack_i128_matches_unpack_and_numeric_in() {
        // i64-range mantissas: byte-identical to the narrow unpack.
        let mut st: u64 = 0x452821E638D01377;
        for _ in 0..2000u32 {
            let r1 = lcg(&mut st);
            let r2 = lcg(&mut st);
            let mag = (r1 % (BUDGET as u64 + 1)) as i64;
            let mant = if r2 & 1 == 1 { -mag } else { mag };
            let scale = ((r2 >> 1) % 9) as i32;
            let narrow = fixed_scale_unpack(mant, scale).expect("narrow");
            let wide = fixed_scale_unpack_i128(mant as i128, scale).expect("wide");
            assert_eq!(
                wide.as_bytes(),
                narrow.as_bytes(),
                "mant {mant} scale {scale}"
            );
        }
        // Beyond-i64 sums: byte-identical to numeric_in of the decimal
        // string (the SUM answer law: dscale = the shared scale).
        for (m, s, want) in [
            (
                123456789012345678901234567i128,
                2,
                "1234567890123456789012345.67",
            ),
            (
                -123456789012345678901234567i128,
                2,
                "-1234567890123456789012345.67",
            ),
            (i128::from(i64::MAX) * 1000, 3, "9223372036854775807.000"),
            (10i128.pow(30), 0, "1000000000000000000000000000000"),
            (0, 4, "0.0000"),
            (-5, 6, "-0.000005"),
        ] {
            let wide = fixed_scale_unpack_i128(m, s).expect("wide");
            let byin = img(want);
            assert_eq!(wide.as_bytes(), byin.as_bytes(), "m {m} scale {s}");
            let mut out = Vec::new();
            crate::io::numeric_out_into(wide.num(), &mut out);
            assert_eq!(String::from_utf8(out).unwrap(), want);
        }
    }

    #[test]
    fn election_refusal_reasons() {
        let a = img("1.5"); // dscale 1 elects S=1
        let b = img_ps("2.5", 10, 2); // dscale 2
        assert_eq!(
            fixed_scale_elect([a.num(), b.num()]),
            Err(FixedScaleRefusal::MixedDscale)
        );

        let one = img("1");
        let nan = img("NaN");
        assert_eq!(
            fixed_scale_elect([one.num(), nan.num()]),
            Err(FixedScaleRefusal::Special)
        );
        let inf = img("Infinity");
        assert_eq!(
            fixed_scale_elect([inf.num(), one.num()]),
            Err(FixedScaleRefusal::Special)
        );

        let big = img("99999999999999999999");
        assert_eq!(
            fixed_scale_elect([one.num(), big.num()]),
            Err(FixedScaleRefusal::Overflow)
        );

        assert_eq!(
            fixed_scale_elect(core::iter::empty::<Num<'_>>()),
            Ok(FixedScaleChunk {
                scale: 0,
                packed: vec![]
            })
        );
    }
}
