//! numeric.c sortsupport section: numeric_fast_cmp + the 64-bit abbreviation
//! family (numeric_abbrev_convert/_convert_var/_abort). SIZEOF_DATUM == 8 arm
//! only; abbreviated words are C-exact (negated, NaN = i64::MIN sorts highest
//! under the backwards signed compare in tuplesort's NumericAbbrev arm).

use core::mem::MaybeUninit;

use crate::{Num, VarView, NUMERIC_POS};
use hyperloglog::HyperLogLog;

pub const NUMERIC_ABBREV_NAN: i64 = i64::MIN;
pub const NUMERIC_ABBREV_PINF: i64 = -i64::MAX;
pub const NUMERIC_ABBREV_NINF: i64 = i64::MAX;

// VARATT_SHORT_MAX - VARHDRSZ_SHORT: only short-varlena payloads can carry
// misaligned digits (long-form numerics are typalign 'i' in tuples and
// 8-aligned from datumCopy).
const SHORT_PAYLOAD_MAX: usize = 126;

#[repr(align(8))]
struct AlignBuf([u8; 128]);

// C's nss->buf / DatumGetNumeric palloc: realign a packed image so digits()
// reads whole i16s; the payload bytes themselves are format-identical.
#[inline]
fn aligned<'a>(payload: &'a [u8], buf: &'a mut MaybeUninit<AlignBuf>) -> Num<'a> {
    if payload.as_ptr() as usize & 1 == 0 {
        return Num::from_payload(payload);
    }
    assert!(
        payload.len() <= SHORT_PAYLOAD_MAX,
        "numeric sortsupport: misaligned long-form numeric image"
    );
    // SAFETY: in-bounds copy into the 8-aligned 128-byte buffer; the returned
    // slice borrows buf for 'a.
    unsafe {
        let dst = buf.as_mut_ptr().cast::<u8>();
        core::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
        Num::from_payload(core::slice::from_raw_parts(dst, payload.len()))
    }
}

/// `numeric_fast_cmp` over varlena payloads (header stripped, short or long
/// numeric format). C pays a detoast palloc/pfree per short input; the stack
/// realign copy is the cheaper equivalent with identical results.
pub fn numeric_fast_cmp(x: &[u8], y: &[u8]) -> i32 {
    let (mut bx, mut by) = (MaybeUninit::uninit(), MaybeUninit::uninit());
    crate::cmp_numerics(aligned(x, &mut bx), aligned(y, &mut by))
}

pub struct NumericAbbrevState {
    input_count: i64,
    estimating: bool,
    abbr_card: HyperLogLog,
}

impl NumericAbbrevState {
    pub fn new() -> NumericAbbrevState {
        NumericAbbrevState {
            input_count: 0,
            estimating: true,
            abbr_card: HyperLogLog::new(10),
        }
    }

    /// `numeric_abbrev_convert` (64-bit arm); returns the abbreviated key
    /// word. Specials never feed the cardinality estimator (C parity).
    pub fn convert(&mut self, payload: &[u8]) -> u64 {
        self.input_count += 1;
        let mut buf = MaybeUninit::uninit();
        let value = aligned(payload, &mut buf);
        let result = if value.is_special() {
            if value.is_pinf() {
                NUMERIC_ABBREV_PINF
            } else if value.is_ninf() {
                NUMERIC_ABBREV_NINF
            } else {
                NUMERIC_ABBREV_NAN
            }
        } else {
            self.convert_var(value.view())
        };
        result as u64
    }

    // numeric_abbrev_convert_var, NUMERIC_ABBREV_BITS == 64:
    // 0 + 7-bit excess-44 word weight + 4 x 14-bit digit words, negated
    // relative to the original so NaN (i64::MIN) sorts highest.
    fn convert_var(&mut self, var: VarView<'_>) -> i64 {
        let ndigits = var.ndigits;
        let weight = var.weight;
        let mut result: i64;

        if ndigits == 0 || weight < -44 {
            result = 0;
        } else if weight > 83 {
            result = i64::MAX;
        } else {
            result = ((weight + 44) as i64) << 56;
            let d = var.digits;
            if ndigits >= 4 {
                result |= d[3] as i64;
            }
            if ndigits >= 3 {
                result |= (d[2] as i64) << 14;
            }
            if ndigits >= 2 {
                result |= (d[1] as i64) << 28;
            }
            result |= (d[0] as i64) << 42;
        }

        if var.sign == NUMERIC_POS {
            result = -result;
        }

        if self.estimating {
            let tmp = (result as u32) ^ ((result as u64 >> 32) as u32);
            self.abbr_card.add(hashfn::hash_bytes_uint32(tmp));
        }

        result
    }

    /// `numeric_abbrev_abort`: commit once past 100k distinct abbrevs, abort
    /// below 1 distinct per ~10k non-null inputs (+0.5 fudge).
    pub fn abort(&mut self, memtupcount: i32) -> bool {
        if memtupcount < 10000 || self.input_count < 10000 || !self.estimating {
            return false;
        }
        let abbr_card = self.abbr_card.estimate();
        if abbr_card > 100000.0 {
            self.estimating = false;
            return false;
        }
        abbr_card < self.input_count as f64 / 10000.0 + 0.5
    }
}

impl Default for NumericAbbrevState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var::NumericImage;

    fn img(s: &str) -> NumericImage {
        match s {
            "NaN" => NumericImage::nan(),
            "Infinity" => NumericImage::pinf(),
            "-Infinity" => NumericImage::ninf(),
            _ => crate::io::numeric_in(s, -1, None).unwrap().unwrap(),
        }
    }

    fn corpus() -> Vec<NumericImage> {
        [
            "NaN", "Infinity", "-Infinity", "0", "0.0", "-0.000", "1", "-1", "2.5", "-2.5",
            "9999", "10000", "9999.9999", "0.0001", "-0.0001", "123456789012345.678901",
            "-123456789012345.678901", "1e83", "1e84", "1e100", "-1e100", "1e-44", "1e-45",
            "1e-200", "-1e-200", "1e300", "-1e300", "42", "42.000", "41.9999999999999999",
            "3.14159265358979", "-3.14159265358979", "700000000", "700000001",
            "0.5", "0.4999999999999", "123.456", "123.4560001", "-9999999999.999999",
        ]
        .iter()
        .map(|s| img(s))
        .collect()
    }

    // Odd-offset copy: exercises the realign path.
    fn odd(payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(payload.len() + 2);
        v.push(0);
        if (v.as_ptr() as usize + 1) & 1 == 0 {
            v.push(0);
        }
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn fast_cmp_matches_cmp_numerics_incl_misaligned() {
        let vals = corpus();
        for a in &vals {
            for b in &vals {
                let expect = crate::cmp_numerics(a.num(), b.num());
                assert_eq!(numeric_fast_cmp(a.payload(), b.payload()), expect);
                let (oa, ob) = (odd(a.payload()), odd(b.payload()));
                let (sa, sb) = (&oa[oa.len() - a.payload().len()..], &ob[ob.len() - b.payload().len()..]);
                if sa.as_ptr() as usize & 1 == 1 || sb.as_ptr() as usize & 1 == 1 {
                    assert_eq!(numeric_fast_cmp(sa, sb), expect);
                }
            }
        }
    }

    #[test]
    fn abbrev_orders_like_cmp_numerics() {
        // Backwards signed compare over abbrevs must refine cmp_numerics:
        // where abbrevs differ, order must match; NaN > +Inf-class ties allowed
        // only as equal abbrevs.
        let mut st = NumericAbbrevState::new();
        let vals = corpus();
        let abbrevs: Vec<i64> = vals.iter().map(|v| st.convert(v.payload()) as i64).collect();
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = if abbrevs[i] < abbrevs[j] {
                    1
                } else if abbrevs[i] > abbrevs[j] {
                    -1
                } else {
                    0
                };
                if a != 0 {
                    assert_eq!(
                        a,
                        crate::cmp_numerics(vals[i].num(), vals[j].num()),
                        "{i} vs {j}"
                    );
                }
            }
        }
    }

    #[test]
    fn abbrev_specials_are_c_exact() {
        let mut st = NumericAbbrevState::new();
        assert_eq!(st.convert(img("NaN").payload()) as i64, i64::MIN);
        assert_eq!(st.convert(img("Infinity").payload()) as i64, -i64::MAX);
        assert_eq!(st.convert(img("-Infinity").payload()) as i64, i64::MAX);
        assert_eq!(st.convert(img("0").payload()) as i64, 0);
        // 42 = digit word 42, weight 0: -(44<<56 | 42<<42).
        assert_eq!(
            st.convert(img("42").payload()) as i64,
            -((44i64 << 56) | (42i64 << 42))
        );
        // Weight clamps are in NBASE-10000 words: > 83 (~10^332) and < -44
        // (~10^-176).
        assert_eq!(st.convert(img("1e400").payload()) as i64, -i64::MAX);
        assert_eq!(st.convert(img("-1e400").payload()) as i64, i64::MAX);
        assert_eq!(st.convert(img("1e-200").payload()) as i64, 0);
        assert_eq!(st.convert(img("-1e-200").payload()) as i64, 0);
        assert_eq!(
            st.convert(img("1e84").payload()) as i64,
            -(((84 / 4 + 44) as i64) << 56 | 1i64 << 42)
        );
    }

    #[test]
    fn abort_gates() {
        let one = img("7");
        let mut st = NumericAbbrevState::new();
        st.convert(one.payload());
        assert!(!st.abort(9999));

        let mut st = NumericAbbrevState::new();
        for _ in 0..20000 {
            st.convert(one.payload());
        }
        assert!(st.abort(20000), "cardinality 1 < 20000/10000 + 0.5");

        // Specials alone never feed the estimator; input_count still moves.
        let mut st = NumericAbbrevState::new();
        for _ in 0..20000 {
            st.convert(img("NaN").payload());
        }
        assert!(st.abort(20000));
    }
}
