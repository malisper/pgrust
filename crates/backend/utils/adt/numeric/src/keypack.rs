//! Canonical (mantissa, exp10) key form for numeric grouping keys — the
//! lane-v2 compact-table "numeric key kind" (a follow-up of the multikey
//! landing, whose v1 admission refused numeric because 1.0 ≡ 1.00 under
//! `numeric_eq` but not by bytes).
//!
//! The pack side decomposes a numeric VALUE into the unique canonical pair
//! `value = mantissa × 10^exp10` with `mantissa` not divisible by 10 (and
//! `mantissa == 0 ⇒ exp10 == 0`), so `pack(a) == pack(b) ⇔ numeric_eq(a, b)`
//! — exactly the injectivity contract packed multi-key grouping needs.
//! Specials (NaN/±Inf) get their own variants: `numeric_eq` treats NaN equal
//! to NaN, so a single NaN key form is correct.
//!
//! BYTE-IDENTITY GATE: the C hash table stores the first-arrival datum and
//! outputs its display scale, so read-back reconstruction is byte-identical
//! only when every packed input already displays at its canonical minimal
//! scale (`dscale == max(0, -exp10)`, e.g. `1.5` but never `1.50`) and its
//! stored digit form is canonical (no leading/trailing zero base-10000
//! digits — every PG-produced numeric is). `numeric_key_pack` returns `None`
//! for anything else — the caller must refuse/demote to the C path, NOT
//! pack lossily. `EXTRACT(minute FROM ts)`-class values (small integers,
//! dscale 0) always pass.

use crate::var::{int128_to_var, make_result, NumericImage, NumericVar};
use crate::{Num, NUMERIC_NEG, NUMERIC_POS};
use types_error::PgResult;

/// The canonical key form. `Finite.exp10` is bounded to i8's non-reserved
/// range by [`numeric_key_pack`] so callers can encode it in one byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericKeyForm {
    /// `value = mantissa × 10^exp10`; mantissa not divisible by 10 unless 0;
    /// `mantissa == 0 ⇒ exp10 == 0`; `-127 <= exp10 <= 127`.
    Finite { mantissa: i64, exp10: i32 },
    NaN,
    PInf,
    NInf,
}

/// Largest exponent magnitude [`numeric_key_pack`] admits (i8 range with
/// `-128` reserved for the caller's special-value encodings).
pub const NUMERIC_KEY_EXP_MAX: i32 = 127;

// Base-10000 digit count bound: 6 digits cover up to 24 decimal digits —
// beyond any admissible i64 mantissa even before trailing-zero stripping,
// and small enough that the i128 accumulator below can never overflow.
const KEY_MAX_NDIGITS: usize = 6;

/// Decompose `num` into its canonical key form when (a) the value is exactly
/// representable with `|mantissa| <= mant_abs_max` and `|exp10| <=`
/// [`NUMERIC_KEY_EXP_MAX`], and (b) reconstruction from that form is
/// byte-identical to `num`'s stored image (canonical digit form + minimal
/// display scale — module doc). `None` = not packable; the caller refuses
/// or demotes to the C path.
pub fn numeric_key_pack(num: Num<'_>, mant_abs_max: u64) -> Option<NumericKeyForm> {
    if num.is_special() {
        return Some(if num.is_nan() {
            NumericKeyForm::NaN
        } else if num.is_pinf() {
            NumericKeyForm::PInf
        } else {
            NumericKeyForm::NInf
        });
    }
    let digits = num.digits();
    let nd = digits.len();
    if nd == 0 {
        // Zero: canonical display is "0" (dscale 0); "0.00"-class datums
        // must keep the C path to preserve their output bytes.
        return (num.dscale() == 0).then_some(NumericKeyForm::Finite { mantissa: 0, exp10: 0 });
    }
    if nd > KEY_MAX_NDIGITS {
        return None;
    }
    // Canonical stored digit form: PG strips leading/trailing zero base-10000
    // digits everywhere (strip_var before make_result); a non-canonical image
    // would not reconstruct byte-identically, so refuse it.
    if digits[0] == 0 || digits[nd - 1] == 0 {
        return None;
    }
    let mut m: i128 = 0;
    for &d in digits {
        if !(0..crate::NBASE as i16).contains(&d) {
            return None;
        }
        m = m * 10000 + d as i128;
    }
    // value = m × 10^e at this point (m > 0).
    let mut e: i32 = (num.weight() - (nd as i32 - 1)) * 4;
    while m % 10 == 0 {
        m /= 10;
        e += 1;
    }
    // Minimal display scale: exactly the fractional digits the canonical
    // mantissa carries. A larger stored dscale (trailing display zeros,
    // "1.50") would reconstruct differently — refuse.
    if num.dscale() != 0.max(-e) {
        return None;
    }
    if m as u128 > mant_abs_max as u128
        || !(-NUMERIC_KEY_EXP_MAX..=NUMERIC_KEY_EXP_MAX).contains(&e)
    {
        return None;
    }
    let mantissa = if num.sign() == NUMERIC_NEG { -(m as i64) } else { m as i64 };
    debug_assert!(num.sign() == NUMERIC_POS || num.sign() == NUMERIC_NEG);
    Some(NumericKeyForm::Finite { mantissa, exp10: e })
}

/// Reconstruct the numeric image of a packed key form. For every form
/// [`numeric_key_pack`] produces, the result is byte-identical to the packed
/// datum (the pack side's canonicality gates guarantee it).
pub fn numeric_key_unpack(key: NumericKeyForm) -> PgResult<NumericImage> {
    let (m, e) = match key {
        NumericKeyForm::NaN => return Ok(NumericImage::nan()),
        NumericKeyForm::PInf => return Ok(NumericImage::pinf()),
        NumericKeyForm::NInf => return Ok(NumericImage::ninf()),
        NumericKeyForm::Finite { mantissa, exp10 } => (mantissa, exp10),
    };
    let mut var = NumericVar::new();
    if m == 0 {
        var.set_zero();
        return make_result(var.view());
    }
    if e >= 0 {
        // value = (m × 10^(e % 4)) × 10000^(e / 4): pad the mantissa into
        // base-10000 alignment, then shift whole digits via the weight.
        let mval = (m as i128) * 10i128.pow((e % 4) as u32);
        int128_to_var(mval, &mut var);
        var.weight += e / 4;
        var.dscale = 0;
    } else {
        // value = (m × 10^shift) × 10000^-((s + shift) / 4), shift padding
        // the fraction to a whole base-10000 digit; dscale = s (the minimal
        // display scale the pack gate proved).
        let s = -e;
        let shift = (4 - (s % 4)) % 4;
        let mval = (m as i128) * 10i128.pow(shift as u32);
        int128_to_var(mval, &mut var);
        var.weight -= (s + shift) / 4;
        var.dscale = s;
    }
    make_result(var.view())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Parse via the production numeric_in path so pack/unpack are tested
    // against PG-canonical images.
    fn img(s: &str) -> NumericImage {
        crate::io::numeric_in(s, -1, None).expect("parse").expect("non-soft parse")
    }

    fn pack_str(s: &str, mant_abs_max: u64) -> Option<NumericKeyForm> {
        numeric_key_pack(img(s).num(), mant_abs_max)
    }

    const M56: u64 = (1u64 << 55) - 1;
    const M24: u64 = (1u64 << 23) - 1;

    #[test]
    fn canonical_values_roundtrip_byte_identically() {
        for s in [
            "0", "1", "-1", "59", "9999", "10000", "12345678", "-12345678", "1.5", "-1.5",
            "0.25", "-0.25", "0.0001", "-0.0001", "123456.789", "3.14159", "-0.07", "300000",
            "8388607", "-8388607", "10000000000", "0.0000025",
        ] {
            let image = img(s);
            let key =
                numeric_key_pack(image.num(), M56).unwrap_or_else(|| panic!("{s} must pack"));
            let back = numeric_key_unpack(key).expect("unpack");
            assert_eq!(back.as_bytes(), image.as_bytes(), "roundtrip bytes for {s}");
        }
    }

    #[test]
    fn specials_roundtrip() {
        for (s, k) in [
            ("NaN", NumericKeyForm::NaN),
            ("Infinity", NumericKeyForm::PInf),
            ("-Infinity", NumericKeyForm::NInf),
        ] {
            let image = img(s);
            assert_eq!(numeric_key_pack(image.num(), M56), Some(k), "{s}");
            let back = numeric_key_unpack(k).expect("unpack");
            assert_eq!(back.as_bytes(), image.as_bytes(), "special bytes for {s}");
        }
    }

    #[test]
    fn equal_values_pack_equal_distinct_values_pack_distinct() {
        let keys: Vec<_> = ["1", "1.5", "15", "150", "0.15", "-1.5", "0"]
            .iter()
            .map(|s| pack_str(s, M56).unwrap())
            .collect();
        for i in 0..keys.len() {
            for j in 0..keys.len() {
                assert_eq!(keys[i] == keys[j], i == j, "keys {i} vs {j}");
            }
        }
    }

    #[test]
    fn non_minimal_dscale_refuses() {
        // Display-scale-bearing equal values must NOT pack (first-arrival
        // output bytes would be unreconstructible).
        for s in ["1.0", "1.50", "0.00", "0.250", "59.000"] {
            assert_eq!(pack_str(s, M56), None, "{s} must refuse");
        }
        // ...while their minimal twins pack.
        for s in ["1", "1.5", "0", "0.25", "59"] {
            assert!(pack_str(s, M56).is_some(), "{s} must pack");
        }
    }

    #[test]
    fn mantissa_boundary_is_exact() {
        let max = M24 as i64; // 8388607
        assert_eq!(
            pack_str("8388607", M24),
            Some(NumericKeyForm::Finite { mantissa: max, exp10: 0 })
        );
        assert_eq!(
            pack_str("-8388607", M24),
            Some(NumericKeyForm::Finite { mantissa: -max, exp10: 0 })
        );
        // One past the boundary refuses...
        assert_eq!(pack_str("8388608", M24), None);
        assert_eq!(pack_str("-8388608", M24), None);
        // ...but a strippable trailing zero keeps larger round values in
        // range (83886070 = 8388607 × 10^1).
        assert_eq!(
            pack_str("83886070", M24),
            Some(NumericKeyForm::Finite { mantissa: max, exp10: 1 })
        );
    }

    #[test]
    fn exponent_boundary_is_exact() {
        assert_eq!(
            pack_str("1e127", M56),
            Some(NumericKeyForm::Finite { mantissa: 1, exp10: 127 })
        );
        assert_eq!(pack_str("1e128", M56), None);
        assert_eq!(
            pack_str("1e-127", M56),
            Some(NumericKeyForm::Finite { mantissa: 1, exp10: -127 })
        );
        assert_eq!(pack_str("1e-128", M56), None);
    }

    #[test]
    fn minute_domain_packs_at_width4() {
        let mut seen = std::collections::HashSet::new();
        for v in 0..60i64 {
            let key = pack_str(&v.to_string(), M24).unwrap();
            // Canonical form strips trailing zeros (10 → (1, 1)); assert
            // injectivity + byte-exact roundtrip over the whole domain.
            assert!(seen.insert(format!("{key:?}")), "distinct key for {v}");
            let back = numeric_key_unpack(key).unwrap();
            assert_eq!(back.as_bytes(), img(&v.to_string()).as_bytes());
        }
    }
}
