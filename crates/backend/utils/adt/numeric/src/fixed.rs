//! Fixed-buffer mirrors of the numeric arithmetic core.
//!
//! Proofs-campaign support (pgrust-correct Kani C≡Rust equivalence).
//! proofs/TRIAGE.md records that all-finite symbolic numeric ARITHMETIC
//! harnesses wall on `DigitBuf::realloc_uninit`: the heap Vec + TLS
//! DIGIT_POOL machinery stays in the SAT formula regardless of digit/weight
//! bounds. Following the jsonb `*_fixed` precedent (commit 99875c3430), this
//! module mirrors the innermost digit-loop kernels onto a caller-provided
//! const-generic stack array:
//!
//! - same algorithm as the allocating kernels in `arith.rs`, line for line
//!   (safe indexing replaces the raw-pointer walks; identical arithmetic);
//! - Option-returning: `None` = result does not fit `N` digits, caller falls
//!   back to the allocating path (result contents are unspecified on `None`);
//! - the allocating path is untouched.
//!
//! `cmp_abs`/`cmp_var` need no mirror: they are already allocation-free.
//! The full pairwise `mul_var` kernel and `div_var` use TLS scratch and are
//! out of scope here; `mul_var_fixed` covers exactly the cases `mul_var`
//! routes to its short kernel and returns `None` otherwise.

use crate::arith::cmp_abs;
use crate::var::VarView;
use crate::{NumericDigit, NBASE, NUMERIC_NEG, NUMERIC_POS};

/// Stack-array working variable: `NumericVar` with the `DigitBuf` replaced
/// by a plain `[NumericDigit; N]`. Same layout invariant as `NumericVar`:
/// digits live at `buf[offset .. offset+ndigits]` and `offset >= 1` after an
/// alloc, so one spare zero digit sits below for rounding carry-out.
///
/// Proofs campaign: this type keeps heap/DIGIT_POOL machinery out of Kani
/// formulas (TRIAGE "all-finite symbolic numeric ARITHMETIC walls on
/// DigitBuf::realloc_uninit").
pub struct FixedVar<const N: usize> {
    pub ndigits: i32,
    pub weight: i32,
    pub sign: u16,
    pub dscale: i32,
    buf: [NumericDigit; N],
    offset: u32,
}

impl<const N: usize> FixedVar<N> {
    pub fn new() -> FixedVar<N> {
        FixedVar {
            ndigits: 0,
            weight: 0,
            sign: NUMERIC_POS,
            dscale: 0,
            buf: [0; N],
            offset: 0,
        }
    }

    /// `NumericVar::alloc` mirror; `None` when `ndigits + 1` (the spare
    /// carry digit) exceeds `N`.
    fn alloc(&mut self, ndigits: i32) -> Option<()> {
        debug_assert!(ndigits >= 0);
        if ndigits as usize + 1 > N {
            return None;
        }
        self.buf[0] = 0;
        self.offset = 1;
        self.ndigits = ndigits;
        Some(())
    }

    /// `NumericVar::set_zero` mirror (dscale left untouched, as there).
    pub fn set_zero(&mut self) {
        self.offset = 0;
        self.ndigits = 0;
        self.weight = 0;
        self.sign = NUMERIC_POS;
    }

    #[inline]
    pub fn digits(&self) -> &[NumericDigit] {
        &self.buf[self.offset as usize..self.offset as usize + self.ndigits as usize]
    }

    #[inline]
    fn digits_mut(&mut self) -> &mut [NumericDigit] {
        &mut self.buf[self.offset as usize..self.offset as usize + self.ndigits as usize]
    }

    #[inline]
    pub fn view(&self) -> VarView<'_> {
        VarView {
            ndigits: self.ndigits,
            weight: self.weight,
            sign: self.sign,
            dscale: self.dscale,
            digits: self.digits(),
        }
    }

    /// `NumericVar::strip` mirror (safe indexing; identical walk).
    pub fn strip(&mut self) {
        let mut n = self.ndigits as usize;
        let base = self.offset as usize;
        let mut start = 0usize;
        let mut weight_drop = 0;
        while n > 0 && self.buf[base + start] == 0 {
            start += 1;
            weight_drop += 1;
            n -= 1;
        }
        while n > 0 && self.buf[base + start + n - 1] == 0 {
            n -= 1;
        }
        self.weight -= weight_drop;
        if n == 0 {
            self.sign = NUMERIC_POS;
            self.weight = 0;
        }
        self.offset += start as u32;
        self.ndigits = n as i32;
    }
}

impl<const N: usize> Default for FixedVar<N> {
    fn default() -> Self {
        FixedVar::new()
    }
}

fn zero_with_dscale_fixed<const N: usize>(result: &mut FixedVar<N>, dscale: i32) {
    result.set_zero();
    result.dscale = dscale;
}

/// `add_abs` (arith.rs) mirror onto a `FixedVar`. Proofs campaign: fixed
/// buffer keeps `DigitBuf::realloc_uninit` out of the formula (TRIAGE
/// numeric-arithmetic wall entry). `None` = doesn't fit `N`; fall back to
/// the allocating kernel.
pub fn add_abs_fixed<const N: usize>(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut FixedVar<N>,
) -> Option<()> {
    let res_weight = var1.weight.max(var2.weight) + 1;
    let res_dscale = var1.dscale.max(var2.dscale);

    let rscale1 = var1.ndigits - var1.weight - 1;
    let rscale2 = var2.ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    result.alloc(res_ndigits)?;

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut carry = 0i32;
    {
        let res_digits = result.digits_mut();
        for i in (0..res_ndigits).rev() {
            i1 -= 1;
            i2 -= 1;
            if i1 >= 0 && i1 < var1.ndigits {
                carry += var1.digits[i1 as usize] as i32;
            }
            if i2 >= 0 && i2 < var2.ndigits {
                carry += var2.digits[i2 as usize] as i32;
            }

            if carry >= NBASE {
                res_digits[i as usize] = (carry - NBASE) as NumericDigit;
                carry = 1;
            } else {
                res_digits[i as usize] = carry as NumericDigit;
                carry = 0;
            }
        }
    }
    debug_assert_eq!(carry, 0);

    result.weight = res_weight;
    result.dscale = res_dscale;
    result.strip();
    Some(())
}

/// `sub_abs` (arith.rs) mirror onto a `FixedVar`; requires
/// ABS(var1) >= ABS(var2) exactly as there. Proofs campaign: see
/// [`add_abs_fixed`] / TRIAGE numeric-arithmetic wall entry.
pub fn sub_abs_fixed<const N: usize>(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut FixedVar<N>,
) -> Option<()> {
    let res_weight = var1.weight;
    let res_dscale = var1.dscale.max(var2.dscale);

    let rscale1 = var1.ndigits - var1.weight - 1;
    let rscale2 = var2.ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    result.alloc(res_ndigits)?;

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut borrow = 0i32;
    {
        let res_digits = result.digits_mut();
        for i in (0..res_ndigits).rev() {
            i1 -= 1;
            i2 -= 1;
            if i1 >= 0 && i1 < var1.ndigits {
                borrow += var1.digits[i1 as usize] as i32;
            }
            if i2 >= 0 && i2 < var2.ndigits {
                borrow -= var2.digits[i2 as usize] as i32;
            }

            if borrow < 0 {
                res_digits[i as usize] = (borrow + NBASE) as NumericDigit;
                borrow = -1;
            } else {
                res_digits[i as usize] = borrow as NumericDigit;
                borrow = 0;
            }
        }
    }
    debug_assert_eq!(borrow, 0);

    result.weight = res_weight;
    result.dscale = res_dscale;
    result.strip();
    Some(())
}

/// `add_var` (arith.rs) mirror onto a `FixedVar`: identical sign dispatch
/// over the fixed kernels (`cmp_abs` is allocation-free and shared). Proofs
/// campaign: see [`add_abs_fixed`] / TRIAGE numeric-arithmetic wall entry.
pub fn add_var_fixed<const N: usize>(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut FixedVar<N>,
) -> Option<()> {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_POS {
            add_abs_fixed(var1, var2, result)?;
            result.sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => zero_with_dscale_fixed(result, var1.dscale.max(var2.dscale)),
                1 => {
                    sub_abs_fixed(var1, var2, result)?;
                    result.sign = NUMERIC_POS;
                }
                _ => {
                    sub_abs_fixed(var2, var1, result)?;
                    result.sign = NUMERIC_NEG;
                }
            }
        }
    } else if var2.sign == NUMERIC_POS {
        match cmp_abs(var1, var2) {
            0 => zero_with_dscale_fixed(result, var1.dscale.max(var2.dscale)),
            1 => {
                sub_abs_fixed(var1, var2, result)?;
                result.sign = NUMERIC_NEG;
            }
            _ => {
                sub_abs_fixed(var2, var1, result)?;
                result.sign = NUMERIC_POS;
            }
        }
    } else {
        add_abs_fixed(var1, var2, result)?;
        result.sign = NUMERIC_NEG;
    }
    Some(())
}

/// `sub_var` (arith.rs) mirror onto a `FixedVar`. Proofs campaign: see
/// [`add_abs_fixed`] / TRIAGE numeric-arithmetic wall entry.
pub fn sub_var_fixed<const N: usize>(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut FixedVar<N>,
) -> Option<()> {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_NEG {
            add_abs_fixed(var1, var2, result)?;
            result.sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => zero_with_dscale_fixed(result, var1.dscale.max(var2.dscale)),
                1 => {
                    sub_abs_fixed(var1, var2, result)?;
                    result.sign = NUMERIC_POS;
                }
                _ => {
                    sub_abs_fixed(var2, var1, result)?;
                    result.sign = NUMERIC_NEG;
                }
            }
        }
    } else if var2.sign == NUMERIC_NEG {
        match cmp_abs(var1, var2) {
            0 => zero_with_dscale_fixed(result, var1.dscale.max(var2.dscale)),
            1 => {
                sub_abs_fixed(var1, var2, result)?;
                result.sign = NUMERIC_NEG;
            }
            _ => {
                sub_abs_fixed(var2, var1, result)?;
                result.sign = NUMERIC_POS;
            }
        }
    } else {
        add_abs_fixed(var1, var2, result)?;
        result.sign = NUMERIC_NEG;
    }
    Some(())
}

/// `mul_var` (arith.rs) mirror onto a `FixedVar`, covering exactly the cases
/// the allocating `mul_var` routes to its exact short kernel (shorter input
/// <= 6 digits and `rscale == var1.dscale + var2.dscale`) plus the zero
/// shortcut. Returns `None` (allocating fallback) for the full pairwise
/// kernel, which needs TLS scratch. Behavior on `Some` is identical to
/// `mul_var` with the same arguments. Proofs campaign: see
/// [`add_abs_fixed`] / TRIAGE numeric-arithmetic wall entry.
pub fn mul_var_fixed<const N: usize>(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut FixedVar<N>,
    rscale: i32,
) -> Option<()> {
    // var1 must be the shorter input (same normalization as mul_var).
    let (var1, var2) = if var1.ndigits > var2.ndigits {
        (var2, var1)
    } else {
        (var1, var2)
    };

    if var1.ndigits == 0 {
        zero_with_dscale_fixed(result, rscale);
        return Some(());
    }

    if var1.ndigits <= 6 && rscale == var1.dscale + var2.dscale {
        return mul_var_short_fixed(var1, var2, result);
    }

    None
}

/// `mul_var_short` (arith.rs) mirror onto a `FixedVar`: var1 has 1-6 digits,
/// var2 at least as many; exact product. Same PRODSUM ladder, safe indexing.
/// Proofs campaign: see [`add_abs_fixed`] / TRIAGE numeric-arithmetic wall
/// entry.
fn mul_var_short_fixed<const N: usize>(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut FixedVar<N>,
) -> Option<()> {
    let var1ndigits = var1.ndigits as usize;
    let var2ndigits = var2.ndigits as usize;
    let d1 = var1.digits;
    let d2 = var2.digits;

    debug_assert!((1..=6).contains(&var1ndigits));
    debug_assert!(var2ndigits >= var1ndigits);

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };
    let res_weight = var1.weight + var2.weight + 1;
    let res_ndigits = var1ndigits + var2ndigits;

    result.alloc(res_ndigits as i32)?;
    let mut carry: u32 = 0;

    macro_rules! prodsum {
        ($n:literal, $i1:expr, $i2:expr) => {{
            let i1 = $i1;
            let i2 = $i2;
            let mut t: u32 = d1[i1] as u32 * d2[i2] as u32;
            if $n >= 2 {
                t += d1[i1 + 1] as u32 * d2[i2 - 1] as u32;
            }
            if $n >= 3 {
                t += d1[i1 + 2] as u32 * d2[i2 - 2] as u32;
            }
            if $n >= 4 {
                t += d1[i1 + 3] as u32 * d2[i2 - 3] as u32;
            }
            if $n >= 5 {
                t += d1[i1 + 4] as u32 * d2[i2 - 4] as u32;
            }
            if $n >= 6 {
                t += d1[i1 + 5] as u32 * d2[i2 - 5] as u32;
            }
            t
        }};
    }

    {
        let res = result.digits_mut();

        macro_rules! tail_digit {
            ($n:literal, $i1:expr, $pos:expr) => {{
                let term = prodsum!($n, $i1, var2ndigits - 1) + carry;
                res[$pos] = (term % NBASE as u32) as NumericDigit;
                carry = term / NBASE as u32;
            }};
        }

        match var1ndigits {
            1 => {
                for i in (0..var2ndigits).rev() {
                    let term = prodsum!(1, 0, i) + carry;
                    res[i + 1] = (term % NBASE as u32) as NumericDigit;
                    carry = term / NBASE as u32;
                }
                res[0] = carry as NumericDigit;
            }
            2 => {
                tail_digit!(1, 1, res_ndigits - 1);
                for i in (1..var2ndigits).rev() {
                    let term = prodsum!(2, 0, i) + carry;
                    res[i + 1] = (term % NBASE as u32) as NumericDigit;
                    carry = term / NBASE as u32;
                }
            }
            3 => {
                tail_digit!(1, 2, res_ndigits - 1);
                tail_digit!(2, 1, res_ndigits - 2);
                for i in (2..var2ndigits).rev() {
                    let term = prodsum!(3, 0, i) + carry;
                    res[i + 1] = (term % NBASE as u32) as NumericDigit;
                    carry = term / NBASE as u32;
                }
            }
            4 => {
                tail_digit!(1, 3, res_ndigits - 1);
                tail_digit!(2, 2, res_ndigits - 2);
                tail_digit!(3, 1, res_ndigits - 3);
                for i in (3..var2ndigits).rev() {
                    let term = prodsum!(4, 0, i) + carry;
                    res[i + 1] = (term % NBASE as u32) as NumericDigit;
                    carry = term / NBASE as u32;
                }
            }
            5 => {
                tail_digit!(1, 4, res_ndigits - 1);
                tail_digit!(2, 3, res_ndigits - 2);
                tail_digit!(3, 2, res_ndigits - 3);
                tail_digit!(4, 1, res_ndigits - 4);
                for i in (4..var2ndigits).rev() {
                    let term = prodsum!(5, 0, i) + carry;
                    res[i + 1] = (term % NBASE as u32) as NumericDigit;
                    carry = term / NBASE as u32;
                }
            }
            _ => {
                tail_digit!(1, 5, res_ndigits - 1);
                tail_digit!(2, 4, res_ndigits - 2);
                tail_digit!(3, 3, res_ndigits - 3);
                tail_digit!(4, 2, res_ndigits - 4);
                tail_digit!(5, 1, res_ndigits - 5);
                for i in (5..var2ndigits).rev() {
                    let term = prodsum!(6, 0, i) + carry;
                    res[i + 1] = (term % NBASE as u32) as NumericDigit;
                    carry = term / NBASE as u32;
                }
            }
        }

        // Remaining var1ndigits most significant digits (fallthrough ladder).
        if var1ndigits >= 6 {
            let term = prodsum!(5, 0, 4) + carry;
            res[5] = (term % NBASE as u32) as NumericDigit;
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 5 {
            let term = prodsum!(4, 0, 3) + carry;
            res[4] = (term % NBASE as u32) as NumericDigit;
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 4 {
            let term = prodsum!(3, 0, 2) + carry;
            res[3] = (term % NBASE as u32) as NumericDigit;
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 3 {
            let term = prodsum!(2, 0, 1) + carry;
            res[2] = (term % NBASE as u32) as NumericDigit;
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 2 {
            let term = prodsum!(1, 0, 0) + carry;
            res[1] = (term % NBASE as u32) as NumericDigit;
            res[0] = (term / NBASE as u32) as NumericDigit;
        }
    }

    result.weight = res_weight;
    result.sign = res_sign;
    result.dscale = var1.dscale + var2.dscale;
    result.strip();
    Some(())
}
