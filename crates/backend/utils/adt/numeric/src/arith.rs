use core::cell::RefCell;

use types_error::PgResult;

use crate::var::{NumericVar, VarView};
use crate::{
    division_by_zero_error, NumericDigit, DEC_DIGITS, DIV_GUARD_DIGITS, MUL_GUARD_DIGITS, NBASE,
    NBASE_SQR, NUMERIC_MAX_DISPLAY_SCALE, NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MIN_SIG_DIGITS,
    NUMERIC_NEG, NUMERIC_POS,
};

// mul_var/div_var working arrays; retained TLS scratch (rule 7). Single-entry:
// neither kernel re-enters itself, so borrow_mut panicking is the loud guard.
std::thread_local! {
    static MUL_SCRATCH: RefCell<(Vec<u64>, Vec<u32>)> = const { RefCell::new((Vec::new(), Vec::new())) };
    static DIV_SCRATCH: RefCell<(Vec<i64>, Vec<i32>)> = const { RefCell::new((Vec::new(), Vec::new())) };
}

pub fn cmp_var(var1: VarView<'_>, var2: VarView<'_>) -> i32 {
    cmp_var_common(
        var1.digits,
        var1.ndigits,
        var1.weight,
        var1.sign,
        var2.digits,
        var2.ndigits,
        var2.weight,
        var2.sign,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn cmp_var_common(
    var1digits: &[NumericDigit],
    var1ndigits: i32,
    var1weight: i32,
    var1sign: u16,
    var2digits: &[NumericDigit],
    var2ndigits: i32,
    var2weight: i32,
    var2sign: u16,
) -> i32 {
    if var1ndigits == 0 {
        if var2ndigits == 0 {
            return 0;
        }
        if var2sign == NUMERIC_NEG {
            return 1;
        }
        return -1;
    }
    if var2ndigits == 0 {
        if var1sign == NUMERIC_POS {
            return 1;
        }
        return -1;
    }

    if var1sign == NUMERIC_POS {
        if var2sign == NUMERIC_NEG {
            return 1;
        }
        return cmp_abs_common(
            var1digits, var1ndigits, var1weight, var2digits, var2ndigits, var2weight,
        );
    }

    if var2sign == NUMERIC_POS {
        return -1;
    }

    cmp_abs_common(
        var2digits, var2ndigits, var2weight, var1digits, var1ndigits, var1weight,
    )
}

pub fn cmp_abs(var1: VarView<'_>, var2: VarView<'_>) -> i32 {
    cmp_abs_common(
        var1.digits,
        var1.ndigits,
        var1.weight,
        var2.digits,
        var2.ndigits,
        var2.weight,
    )
}

pub fn cmp_abs_common(
    var1digits: &[NumericDigit],
    var1ndigits: i32,
    mut var1weight: i32,
    var2digits: &[NumericDigit],
    var2ndigits: i32,
    mut var2weight: i32,
) -> i32 {
    debug_assert!(var1digits.len() >= var1ndigits as usize);
    debug_assert!(var2digits.len() >= var2ndigits as usize);
    let d1 = var1digits.as_ptr();
    let d2 = var2digits.as_ptr();
    let mut i1 = 0i32;
    let mut i2 = 0i32;

    // SAFETY throughout: i1/i2 are read only while < var{1,2}ndigits, which
    // are within the slices per the asserts above.
    unsafe {
        while var1weight > var2weight && i1 < var1ndigits {
            if *d1.add(i1 as usize) != 0 {
                return 1;
            }
            i1 += 1;
            var1weight -= 1;
        }
        while var2weight > var1weight && i2 < var2ndigits {
            if *d2.add(i2 as usize) != 0 {
                return -1;
            }
            i2 += 1;
            var2weight -= 1;
        }

        if var1weight == var2weight {
            while i1 < var1ndigits && i2 < var2ndigits {
                // C (numeric.c cmp_abs_common) subtracts in int; widen so
                // out-of-invariant digits (corrupt storage) compare exactly
                // as C does instead of wrapping at i16 (proofs finding #6).
                let stat = *d1.add(i1 as usize) as i32 - *d2.add(i2 as usize) as i32;
                i1 += 1;
                i2 += 1;
                if stat != 0 {
                    return if stat > 0 { 1 } else { -1 };
                }
            }
        }

        while i1 < var1ndigits {
            if *d1.add(i1 as usize) != 0 {
                return 1;
            }
            i1 += 1;
        }
        while i2 < var2ndigits {
            if *d2.add(i2 as usize) != 0 {
                return -1;
            }
            i2 += 1;
        }
    }

    0
}

fn add_abs(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) {
    let res_weight = var1.weight.max(var2.weight) + 1;
    let res_dscale = var1.dscale.max(var2.dscale);

    let rscale1 = var1.ndigits - var1.weight - 1;
    let rscale2 = var2.ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    result.alloc(res_ndigits);

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut carry = 0i32;
    {
        let res_digits = result.digits_mut().as_mut_ptr();
        let d1 = var1.digits.as_ptr();
        let d2 = var2.digits.as_ptr();
        for i in (0..res_ndigits).rev() {
            i1 -= 1;
            i2 -= 1;
            // SAFETY: reads guarded to [0, ndigits); writes cover exactly the
            // res_ndigits digits just allocated.
            unsafe {
                if i1 >= 0 && i1 < var1.ndigits {
                    carry += *d1.add(i1 as usize) as i32;
                }
                if i2 >= 0 && i2 < var2.ndigits {
                    carry += *d2.add(i2 as usize) as i32;
                }

                if carry >= NBASE {
                    *res_digits.add(i as usize) = (carry - NBASE) as NumericDigit;
                    carry = 1;
                } else {
                    *res_digits.add(i as usize) = carry as NumericDigit;
                    carry = 0;
                }
            }
        }
    }
    debug_assert_eq!(carry, 0);

    result.weight = res_weight;
    result.dscale = res_dscale;
    result.strip();
}

// Requires ABS(var1) >= ABS(var2).
fn sub_abs(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) {
    let res_weight = var1.weight;
    let res_dscale = var1.dscale.max(var2.dscale);

    let rscale1 = var1.ndigits - var1.weight - 1;
    let rscale2 = var2.ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    result.alloc(res_ndigits);

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut borrow = 0i32;
    {
        let res_digits = result.digits_mut().as_mut_ptr();
        let d1 = var1.digits.as_ptr();
        let d2 = var2.digits.as_ptr();
        for i in (0..res_ndigits).rev() {
            i1 -= 1;
            i2 -= 1;
            // SAFETY: reads guarded to [0, ndigits); writes cover exactly the
            // res_ndigits digits just allocated.
            unsafe {
                if i1 >= 0 && i1 < var1.ndigits {
                    borrow += *d1.add(i1 as usize) as i32;
                }
                if i2 >= 0 && i2 < var2.ndigits {
                    borrow -= *d2.add(i2 as usize) as i32;
                }

                if borrow < 0 {
                    *res_digits.add(i as usize) = (borrow + NBASE) as NumericDigit;
                    borrow = -1;
                } else {
                    *res_digits.add(i as usize) = borrow as NumericDigit;
                    borrow = 0;
                }
            }
        }
    }
    debug_assert_eq!(borrow, 0);

    result.weight = res_weight;
    result.dscale = res_dscale;
    result.strip();
}

fn zero_with_dscale(result: &mut NumericVar, dscale: i32) {
    result.set_zero();
    result.dscale = dscale;
}

pub fn add_var(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_POS {
            add_abs(var1, var2, result);
            result.sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => zero_with_dscale(result, var1.dscale.max(var2.dscale)),
                1 => {
                    sub_abs(var1, var2, result);
                    result.sign = NUMERIC_POS;
                }
                _ => {
                    sub_abs(var2, var1, result);
                    result.sign = NUMERIC_NEG;
                }
            }
        }
    } else if var2.sign == NUMERIC_POS {
        match cmp_abs(var1, var2) {
            0 => zero_with_dscale(result, var1.dscale.max(var2.dscale)),
            1 => {
                sub_abs(var1, var2, result);
                result.sign = NUMERIC_NEG;
            }
            _ => {
                sub_abs(var2, var1, result);
                result.sign = NUMERIC_POS;
            }
        }
    } else {
        add_abs(var1, var2, result);
        result.sign = NUMERIC_NEG;
    }
}

pub fn sub_var(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_NEG {
            add_abs(var1, var2, result);
            result.sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => zero_with_dscale(result, var1.dscale.max(var2.dscale)),
                1 => {
                    sub_abs(var1, var2, result);
                    result.sign = NUMERIC_POS;
                }
                _ => {
                    sub_abs(var2, var1, result);
                    result.sign = NUMERIC_NEG;
                }
            }
        }
    } else if var2.sign == NUMERIC_NEG {
        match cmp_abs(var1, var2) {
            0 => zero_with_dscale(result, var1.dscale.max(var2.dscale)),
            1 => {
                sub_abs(var1, var2, result);
                result.sign = NUMERIC_NEG;
            }
            _ => {
                sub_abs(var2, var1, result);
                result.sign = NUMERIC_POS;
            }
        }
    } else {
        add_abs(var1, var2, result);
        result.sign = NUMERIC_NEG;
    }
}

pub fn mul_var(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar, rscale: i32) {
    // var1 must be the shorter input (fewer outer-loop iterations).
    let (var1, var2) = if var1.ndigits > var2.ndigits {
        (var2, var1)
    } else {
        (var1, var2)
    };

    let var1ndigits = var1.ndigits;
    let var2ndigits = var2.ndigits;
    let var1digits = var1.digits;
    let var2digits = var2.digits;

    if var1ndigits == 0 {
        return zero_with_dscale(result, rscale);
    }

    if var1ndigits <= 6 && rscale == var1.dscale + var2.dscale {
        return mul_var_short(var1, var2, result);
    }

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };

    let var1ndigitpairs = (var1ndigits + 1) / 2;
    let var2ndigitpairs = (var2ndigits + 1) / 2;

    let mut res_ndigits = var1ndigits + var2ndigits;
    let mut res_ndigitpairs = res_ndigits / 2 + 1;
    let pair_offset = res_ndigitpairs - var1ndigitpairs - var2ndigitpairs + 1;
    let res_weight = var1.weight + var2.weight + 1 + 2 * res_ndigitpairs
        - res_ndigits
        - (var1ndigits & 1)
        - (var2ndigits & 1);

    let maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS + MUL_GUARD_DIGITS;
    let maxdigitpairs = maxdigits / 2 + 1;

    res_ndigitpairs = res_ndigitpairs.min(maxdigitpairs);
    res_ndigits = 2 * res_ndigitpairs;

    if res_ndigitpairs <= pair_offset {
        return zero_with_dscale(result, rscale);
    }
    let var1ndigitpairs = var1ndigitpairs.min(res_ndigitpairs - pair_offset);
    let var2ndigitpairs = var2ndigitpairs.min(res_ndigitpairs - pair_offset);

    MUL_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        let (dig, var2digitpairs) = &mut *scratch;
        dig.clear();
        dig.reserve(res_ndigitpairs as usize);
        var2digitpairs.clear();
        var2digitpairs.reserve(var2ndigitpairs as usize);
        // SAFETY: capacities reserved; u64/u32 have no invalid bit patterns.
        // Every dig[] word is written before read: the head memset below
        // covers [0, i1+pair_offset) and the first product loop covers the
        // tail exactly (i2limit = res_ndigitpairs - i1 - pair_offset by the
        // pair_offset identity) — C zero-fills only the head the same way.
        unsafe {
            dig.set_len(res_ndigitpairs as usize);
            var2digitpairs.set_len(var2ndigitpairs as usize);
        }

        let v2p = var2digitpairs.as_mut_ptr();
        // SAFETY: writes are at i2 < var2ndigitpairs; digit reads 2*i2(+1)
        // are < var2ndigits per the last-pair case split.
        unsafe {
            for i2 in 0..var2ndigitpairs - 1 {
                *v2p.add(i2 as usize) = (var2digits[2 * i2 as usize] as i32 * NBASE
                    + var2digits[2 * i2 as usize + 1] as i32)
                    as u32;
            }
            let i2 = var2ndigitpairs - 1;
            *v2p.add(i2 as usize) = if 2 * i2 + 1 < var2ndigits {
                (var2digits[2 * i2 as usize] as i32 * NBASE
                    + var2digits[2 * i2 as usize + 1] as i32) as u32
            } else {
                (var2digits[2 * i2 as usize] as i32 * NBASE) as u32
            };
        }

        let mut i1 = var1ndigitpairs - 1;
        let mut var1digitpair: u32 = if 2 * i1 + 1 < var1ndigits {
            (var1digits[2 * i1 as usize] as i32 * NBASE + var1digits[2 * i1 as usize + 1] as i32)
                as u32
        } else {
            (var1digits[2 * i1 as usize] as i32 * NBASE) as u32
        };
        let mut maxdig: u64 = var1digitpair as u64;

        let i2limit = var2ndigitpairs.min(res_ndigitpairs - i1 - pair_offset);
        {
            let base = (i1 + pair_offset) as usize;
            // SAFETY: base <= res_ndigitpairs (pair_offset identity above).
            unsafe { core::ptr::write_bytes(dig.as_mut_ptr(), 0, base) };
            let dseg = &mut dig[base..base + i2limit as usize];
            for (d, &v2) in dseg.iter_mut().zip(&var2digitpairs[..i2limit as usize]) {
                *d = var1digitpair as u64 * v2 as u64;
            }
        }

        while i1 > 0 {
            i1 -= 1;
            var1digitpair = (var1digits[2 * i1 as usize] as i32 * NBASE
                + var1digits[2 * i1 as usize + 1] as i32) as u32;
            if var1digitpair == 0 {
                continue;
            }

            maxdig += var1digitpair as u64;
            if maxdig > (u64::MAX - u64::MAX / NBASE_SQR as u64) / (NBASE_SQR as u64 - 1) {
                let digp = dig.as_mut_ptr();
                let mut carry: u64 = 0;
                for i in (0..res_ndigitpairs as usize).rev() {
                    // SAFETY: i < res_ndigitpairs = dig.len().
                    unsafe {
                        let mut newdig = *digp.add(i) + carry;
                        if newdig >= NBASE_SQR as u64 {
                            carry = newdig / NBASE_SQR as u64;
                            newdig -= carry * NBASE_SQR as u64;
                        } else {
                            carry = 0;
                        }
                        *digp.add(i) = newdig;
                    }
                }
                debug_assert_eq!(carry, 0);
                maxdig = 1 + var1digitpair as u64;
            }

            let i2limit = var2ndigitpairs.min(res_ndigitpairs - i1 - pair_offset);
            let base = (i1 + pair_offset) as usize;
            let dseg = &mut dig[base..base + i2limit as usize];
            for (d, &v2) in dseg.iter_mut().zip(&var2digitpairs[..i2limit as usize]) {
                *d += var1digitpair as u64 * v2 as u64;
            }
        }

        result.alloc(res_ndigits);
        {
            let res_digits = result.digits_mut().as_mut_ptr();
            let digp = dig.as_ptr();
            let mut carry: u64 = 0;
            for i in (0..res_ndigitpairs as usize).rev() {
                // SAFETY: i < res_ndigitpairs = dig.len(); writes at 2*i and
                // 2*i+1 are < res_ndigits = 2*res_ndigitpairs.
                unsafe {
                    let mut newdig = *digp.add(i) + carry;
                    if newdig >= NBASE_SQR as u64 {
                        carry = newdig / NBASE_SQR as u64;
                        newdig -= carry * NBASE_SQR as u64;
                    } else {
                        carry = 0;
                    }
                    *res_digits.add(2 * i + 1) = (newdig as u32 % NBASE as u32) as NumericDigit;
                    *res_digits.add(2 * i) = (newdig as u32 / NBASE as u32) as NumericDigit;
                }
            }
            debug_assert_eq!(carry, 0);
        }

        result.weight = res_weight;
        result.sign = res_sign;
        result.round(rscale);
        result.strip();
    })
}

// var1 has 1-6 digits, var2 at least as many; exact product.
fn mul_var_short(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) {
    let var1ndigits = var1.ndigits as usize;
    let var2ndigits = var2.ndigits as usize;
    // SAFETY throughout: every d1 index is < var1ndigits and every d2 index
    // is < var2ndigits, by the same case analysis as the C PRODSUM ladder
    // (var1ndigits in 1..=6, var2ndigits >= var1ndigits).
    let d1 = var1.digits.as_ptr();
    let d2 = var2.digits.as_ptr();

    debug_assert!((1..=6).contains(&var1ndigits));
    debug_assert!(var2ndigits >= var1ndigits);

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };
    let res_weight = var1.weight + var2.weight + 1;
    let res_ndigits = var1ndigits + var2ndigits;

    result.alloc(res_ndigits as i32);
    let mut carry: u32 = 0;

    macro_rules! prodsum {
        ($n:literal, $i1:expr, $i2:expr) => {{
            let i1 = $i1;
            let i2 = $i2;
            #[allow(unused_unsafe)]
            unsafe {
                let mut t: u32 = *d1.add(i1) as u32 * *d2.add(i2) as u32;
                if $n >= 2 {
                    t += *d1.add(i1 + 1) as u32 * *d2.add(i2 - 1) as u32;
                }
                if $n >= 3 {
                    t += *d1.add(i1 + 2) as u32 * *d2.add(i2 - 2) as u32;
                }
                if $n >= 4 {
                    t += *d1.add(i1 + 3) as u32 * *d2.add(i2 - 3) as u32;
                }
                if $n >= 5 {
                    t += *d1.add(i1 + 4) as u32 * *d2.add(i2 - 4) as u32;
                }
                if $n >= 6 {
                    t += *d1.add(i1 + 5) as u32 * *d2.add(i2 - 5) as u32;
                }
                t
            }
        }};
    }

    {
        let res = result.digits_mut().as_mut_ptr();

        // SAFETY: positions written are < res_ndigits by the C case analysis.
        macro_rules! wr {
            ($pos:expr, $val:expr) => {
                unsafe { *res.add($pos) = $val }
            };
        }

        macro_rules! tail_digit {
            ($n:literal, $i1:expr, $pos:expr) => {{
                let term = prodsum!($n, $i1, var2ndigits - 1) + carry;
                wr!($pos, (term % NBASE as u32) as NumericDigit);
                carry = term / NBASE as u32;
            }};
        }

        match var1ndigits {
            1 => {
                for i in (0..var2ndigits).rev() {
                    let term = prodsum!(1, 0, i) + carry;
                    wr!(i + 1, (term % NBASE as u32) as NumericDigit);
                    carry = term / NBASE as u32;
                }
                wr!(0, carry as NumericDigit);
            }
            2 => {
                tail_digit!(1, 1, res_ndigits - 1);
                for i in (1..var2ndigits).rev() {
                    let term = prodsum!(2, 0, i) + carry;
                    wr!(i + 1, (term % NBASE as u32) as NumericDigit);
                    carry = term / NBASE as u32;
                }
            }
            3 => {
                tail_digit!(1, 2, res_ndigits - 1);
                tail_digit!(2, 1, res_ndigits - 2);
                for i in (2..var2ndigits).rev() {
                    let term = prodsum!(3, 0, i) + carry;
                    wr!(i + 1, (term % NBASE as u32) as NumericDigit);
                    carry = term / NBASE as u32;
                }
            }
            4 => {
                tail_digit!(1, 3, res_ndigits - 1);
                tail_digit!(2, 2, res_ndigits - 2);
                tail_digit!(3, 1, res_ndigits - 3);
                for i in (3..var2ndigits).rev() {
                    let term = prodsum!(4, 0, i) + carry;
                    wr!(i + 1, (term % NBASE as u32) as NumericDigit);
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
                    wr!(i + 1, (term % NBASE as u32) as NumericDigit);
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
                    wr!(i + 1, (term % NBASE as u32) as NumericDigit);
                    carry = term / NBASE as u32;
                }
            }
        }

        // Remaining var1ndigits most significant digits (fallthrough ladder).
        if var1ndigits >= 6 {
            let term = prodsum!(5, 0, 4) + carry;
            wr!(5, (term % NBASE as u32) as NumericDigit);
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 5 {
            let term = prodsum!(4, 0, 3) + carry;
            wr!(4, (term % NBASE as u32) as NumericDigit);
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 4 {
            let term = prodsum!(3, 0, 2) + carry;
            wr!(3, (term % NBASE as u32) as NumericDigit);
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 3 {
            let term = prodsum!(2, 0, 1) + carry;
            wr!(2, (term % NBASE as u32) as NumericDigit);
            carry = term / NBASE as u32;
        }
        if var1ndigits >= 2 {
            let term = prodsum!(1, 0, 0) + carry;
            wr!(1, (term % NBASE as u32) as NumericDigit);
            wr!(0, (term / NBASE as u32) as NumericDigit);
        }
    }

    result.weight = res_weight;
    result.sign = res_sign;
    result.dscale = var1.dscale + var2.dscale;
    result.strip();
}

pub fn div_var(
    var1: VarView<'_>,
    var2: VarView<'_>,
    result: &mut NumericVar,
    rscale: i32,
    round: bool,
    mut exact: bool,
) -> PgResult<()> {
    let var1ndigits = var1.ndigits;
    let var2ndigits = var2.ndigits;

    if var2ndigits == 0 || var2.digits[0] == 0 {
        return Err(division_by_zero_error().into());
    }

    if var2ndigits <= 2 {
        let mut idivisor = var2.digits[0] as i32;
        let mut idivisor_weight = var2.weight;
        if var2ndigits == 2 {
            idivisor = idivisor * NBASE + var2.digits[1] as i32;
            idivisor_weight -= 1;
        }
        if var2.sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }
        return div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
    }
    if var2ndigits <= 4 {
        let mut idivisor = var2.digits[0] as i64;
        let mut idivisor_weight = var2.weight;
        for i in 1..var2ndigits {
            idivisor = idivisor * NBASE as i64 + var2.digits[i as usize] as i64;
            idivisor_weight -= 1;
        }
        if var2.sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }
        return div_var_int64(var1, idivisor, idivisor_weight, result, rscale, round);
    }

    if var1ndigits == 0 {
        zero_with_dscale(result, rscale);
        return Ok(());
    }

    if var2ndigits <= 2 * (DIV_GUARD_DIGITS + 2) {
        exact = true;
    }

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };
    let res_weight = var1.weight - var2.weight + 1;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = res_ndigits.max(1);
    if round {
        res_ndigits += 1;
    }
    if !exact {
        res_ndigits += DIV_GUARD_DIGITS;
    }

    let mut var1ndigitpairs = (var1ndigits + 1) / 2;
    let mut var2ndigitpairs = (var2ndigits + 1) / 2;
    let res_ndigitpairs = (res_ndigits + 1) / 2;
    let res_ndigits = 2 * res_ndigitpairs;

    let div_ndigitpairs;
    if exact {
        div_ndigitpairs = res_ndigitpairs + var2ndigitpairs;
        var1ndigitpairs = var1ndigitpairs.min(div_ndigitpairs);
    } else {
        div_ndigitpairs = res_ndigitpairs;
        var1ndigitpairs = var1ndigitpairs.min(div_ndigitpairs);
        var2ndigitpairs = var2ndigitpairs.min(div_ndigitpairs);
    }

    DIV_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        let (dividend, divisor) = &mut *scratch;
        // One extra dividend digit so the loop can touch dividend[qi+1].
        dividend.clear();
        dividend.resize(div_ndigitpairs as usize + 1, 0);
        divisor.clear();
        divisor.reserve(var2ndigitpairs as usize);

        let var1digits = var1.digits;
        let var2digits = var2.digits;

        for i in 0..var1ndigitpairs - 1 {
            dividend[i as usize] = (var1digits[2 * i as usize] as i32 * NBASE
                + var1digits[2 * i as usize + 1] as i32) as i64;
        }
        let i = var1ndigitpairs - 1;
        dividend[i as usize] = if 2 * i + 1 < var1ndigits {
            (var1digits[2 * i as usize] as i32 * NBASE + var1digits[2 * i as usize + 1] as i32)
                as i64
        } else {
            (var1digits[2 * i as usize] as i32 * NBASE) as i64
        };

        for i in 0..var2ndigitpairs - 1 {
            divisor.push(
                var2digits[2 * i as usize] as i32 * NBASE + var2digits[2 * i as usize + 1] as i32,
            );
        }
        let i = var2ndigitpairs - 1;
        if 2 * i + 1 < var2ndigits {
            divisor.push(
                var2digits[2 * i as usize] as i32 * NBASE + var2digits[2 * i as usize + 1] as i32,
            );
        } else {
            divisor.push(var2digits[2 * i as usize] as i32 * NBASE);
        }

        let mut fdivisor = divisor[0] as f64 * NBASE_SQR as f64;
        if var2ndigitpairs > 1 {
            fdivisor += divisor[1] as f64;
        }
        let fdivisorinverse = 1.0 / fdivisor;

        let mut maxdiv: i64 = 1;

        for qi in 0..res_ndigitpairs as usize {
            let mut fdividend = dividend[qi] as f64 * NBASE_SQR as f64;
            fdividend += dividend[qi + 1] as f64;

            let fquotient = fdividend * fdivisorinverse;
            let mut qdigit: i32 = if fquotient >= 0.0 {
                fquotient as i32
            } else {
                fquotient as i32 - 1
            };

            if qdigit != 0 {
                maxdiv += (qdigit as i64).abs();
                if maxdiv > (i64::MAX - i64::MAX / NBASE_SQR as i64 - 1) / (NBASE_SQR as i64 - 1) {
                    let mut carry: i64 = 0;
                    let top = (qi + var2ndigitpairs as usize - 2).min(div_ndigitpairs as usize - 1);
                    let mut i = top;
                    while i > qi {
                        let mut newdig = dividend[i] + carry;
                        if newdig < 0 {
                            carry = -((-newdig - 1) / NBASE_SQR as i64) - 1;
                            newdig -= carry * NBASE_SQR as i64;
                        } else if newdig >= NBASE_SQR as i64 {
                            carry = newdig / NBASE_SQR as i64;
                            newdig -= carry * NBASE_SQR as i64;
                        } else {
                            carry = 0;
                        }
                        dividend[i] = newdig;
                        i -= 1;
                    }
                    dividend[qi] += carry;

                    maxdiv = 1;

                    let mut fdividend = dividend[qi] as f64 * NBASE_SQR as f64;
                    fdividend += dividend[qi + 1] as f64;
                    let fquotient = fdividend * fdivisorinverse;
                    qdigit = if fquotient >= 0.0 {
                        fquotient as i32
                    } else {
                        fquotient as i32 - 1
                    };

                    maxdiv += (qdigit as i64).abs();
                }

                if qdigit != 0 {
                    let istop = (var2ndigitpairs as usize).min(div_ndigitpairs as usize - qi);
                    let dseg = &mut dividend[qi..qi + istop];
                    for (d, &v2) in dseg.iter_mut().zip(&divisor[..istop]) {
                        *d -= qdigit as i64 * v2 as i64;
                    }
                }
            }

            // Cancelling wrap-around is intentional (C relies on the same).
            dividend[qi + 1] =
                dividend[qi + 1].wrapping_add(dividend[qi].wrapping_mul(NBASE_SQR as i64));
            dividend[qi] = qdigit as i64;
        }

        let qi = res_ndigitpairs as usize;

        if exact {
            let mut carry: i64 = 0;
            for i in (0..=var2ndigitpairs as usize - 2).rev() {
                let mut newdig = dividend[qi + i] + carry;
                if newdig < 0 {
                    carry = -((-newdig - 1) / NBASE_SQR as i64) - 1;
                    newdig -= carry * NBASE_SQR as i64;
                } else if newdig >= NBASE_SQR as i64 {
                    carry = newdig / NBASE_SQR as i64;
                    newdig -= carry * NBASE_SQR as i64;
                } else {
                    carry = 0;
                }
                dividend[qi + i + 1] = newdig;
            }
            dividend[qi] = carry;

            if dividend[qi] < 0 {
                loop {
                    let mut carry: i64 = 0;
                    for i in (1..var2ndigitpairs as usize).rev() {
                        let newdig = dividend[qi + i] + divisor[i] as i64 + carry;
                        if newdig >= NBASE_SQR as i64 {
                            dividend[qi + i] = newdig - NBASE_SQR as i64;
                            carry = 1;
                        } else {
                            dividend[qi + i] = newdig;
                            carry = 0;
                        }
                    }
                    dividend[qi] += divisor[0] as i64 + carry;

                    dividend[qi - 1] -= 1;

                    if dividend[qi] >= 0 {
                        break;
                    }
                }
            } else {
                loop {
                    let mut less = false;
                    for i in 0..var2ndigitpairs as usize {
                        match dividend[qi + i].cmp(&(divisor[i] as i64)) {
                            core::cmp::Ordering::Less => {
                                less = true;
                                break;
                            }
                            core::cmp::Ordering::Greater => break,
                            core::cmp::Ordering::Equal => {}
                        }
                    }
                    if less {
                        break;
                    }

                    let mut carry: i64 = 0;
                    for i in (1..var2ndigitpairs as usize).rev() {
                        let newdig = dividend[qi + i] - divisor[i] as i64 + carry;
                        if newdig < 0 {
                            dividend[qi + i] = newdig + NBASE_SQR as i64;
                            carry = -1;
                        } else {
                            dividend[qi + i] = newdig;
                            carry = 0;
                        }
                    }
                    dividend[qi] = dividend[qi] - divisor[0] as i64 + carry;

                    dividend[qi - 1] += 1;
                }
            }
        }

        result.alloc(res_ndigits);
        {
            let res_digits = result.digits_mut();
            let mut carry: i64 = 0;
            for i in (0..res_ndigitpairs as usize).rev() {
                let mut newdig = dividend[i] + carry;
                if newdig < 0 {
                    carry = -((-newdig - 1) / NBASE_SQR as i64) - 1;
                    newdig -= carry * NBASE_SQR as i64;
                } else if newdig >= NBASE_SQR as i64 {
                    carry = newdig / NBASE_SQR as i64;
                    newdig -= carry * NBASE_SQR as i64;
                } else {
                    carry = 0;
                }
                res_digits[2 * i + 1] = (newdig as u32 % NBASE as u32) as NumericDigit;
                res_digits[2 * i] = (newdig as u32 / NBASE as u32) as NumericDigit;
            }
            debug_assert_eq!(carry, 0);
        }

        result.weight = res_weight;
        result.sign = res_sign;

        if round {
            result.round(rscale);
        } else {
            result.trunc(rscale);
        }
        result.strip();
        Ok(())
    })
}

pub fn div_var_int(
    var: VarView<'_>,
    ival: i32,
    ival_weight: i32,
    result: &mut NumericVar,
    rscale: i32,
    round: bool,
) -> PgResult<()> {
    if ival == 0 {
        return Err(division_by_zero_error().into());
    }

    if var.ndigits == 0 {
        zero_with_dscale(result, rscale);
        return Ok(());
    }

    let res_sign = if var.sign == NUMERIC_POS {
        if ival > 0 {
            NUMERIC_POS
        } else {
            NUMERIC_NEG
        }
    } else if ival > 0 {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };
    let res_weight = var.weight - ival_weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = res_ndigits.max(1);
    if round {
        res_ndigits += 1;
    }

    result.alloc(res_ndigits);

    let divisor = ival.unsigned_abs();
    let var_digits = var.digits;
    let var_ndigits = var.ndigits;

    {
        let res_digits = result.digits_mut().as_mut_ptr();
        let vd = var_digits.as_ptr();
        // SAFETY: writes cover exactly the res_ndigits digits just allocated;
        // reads are index-guarded to [0, var_ndigits).
        unsafe {
            if divisor <= u32::MAX / NBASE as u32 {
                let mut carry: u32 = 0;
                for i in 0..res_ndigits {
                    carry = carry * NBASE as u32
                        + if i < var_ndigits {
                            *vd.add(i as usize) as u32
                        } else {
                            0
                        };
                    *res_digits.add(i as usize) = (carry / divisor) as NumericDigit;
                    carry %= divisor;
                }
            } else {
                let mut carry: u64 = 0;
                for i in 0..res_ndigits {
                    carry = carry * NBASE as u64
                        + if i < var_ndigits {
                            *vd.add(i as usize) as u64
                        } else {
                            0
                        };
                    *res_digits.add(i as usize) = (carry / divisor as u64) as NumericDigit;
                    carry %= divisor as u64;
                }
            }
        }
    }

    result.weight = res_weight;
    result.sign = res_sign;

    if round {
        result.round(rscale);
    } else {
        result.trunc(rscale);
    }
    result.strip();
    Ok(())
}

pub fn div_var_int64(
    var: VarView<'_>,
    ival: i64,
    ival_weight: i32,
    result: &mut NumericVar,
    rscale: i32,
    round: bool,
) -> PgResult<()> {
    if ival == 0 {
        return Err(division_by_zero_error().into());
    }

    if var.ndigits == 0 {
        zero_with_dscale(result, rscale);
        return Ok(());
    }

    let res_sign = if var.sign == NUMERIC_POS {
        if ival > 0 {
            NUMERIC_POS
        } else {
            NUMERIC_NEG
        }
    } else if ival > 0 {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };
    let res_weight = var.weight - ival_weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = res_ndigits.max(1);
    if round {
        res_ndigits += 1;
    }

    result.alloc(res_ndigits);

    let divisor = ival.unsigned_abs();
    let var_digits = var.digits;
    let var_ndigits = var.ndigits;

    {
        let res_digits = result.digits_mut().as_mut_ptr();
        let vd = var_digits.as_ptr();
        // SAFETY: writes cover exactly the res_ndigits digits just allocated;
        // reads are index-guarded to [0, var_ndigits).
        unsafe {
            if divisor <= u64::MAX / NBASE as u64 {
                let mut carry: u64 = 0;
                for i in 0..res_ndigits {
                    carry = carry * NBASE as u64
                        + if i < var_ndigits {
                            *vd.add(i as usize) as u64
                        } else {
                            0
                        };
                    *res_digits.add(i as usize) = (carry / divisor) as NumericDigit;
                    carry %= divisor;
                }
            } else {
                let mut carry: u128 = 0;
                for i in 0..res_ndigits {
                    carry = carry * NBASE as u128
                        + if i < var_ndigits {
                            *vd.add(i as usize) as u128
                        } else {
                            0
                        };
                    *res_digits.add(i as usize) = (carry / divisor as u128) as NumericDigit;
                    carry %= divisor as u128;
                }
            }
        }
    }

    result.weight = res_weight;
    result.sign = res_sign;

    if round {
        result.round(rscale);
    } else {
        result.trunc(rscale);
    }
    result.strip();
    Ok(())
}

pub fn select_div_scale(var1: VarView<'_>, var2: VarView<'_>) -> i32 {
    let mut weight1 = 0;
    let mut firstdigit1: NumericDigit = 0;
    for i in 0..var1.ndigits {
        firstdigit1 = var1.digits[i as usize];
        if firstdigit1 != 0 {
            weight1 = var1.weight - i;
            break;
        }
    }

    let mut weight2 = 0;
    let mut firstdigit2: NumericDigit = 0;
    for i in 0..var2.ndigits {
        firstdigit2 = var2.digits[i as usize];
        if firstdigit2 != 0 {
            weight2 = var2.weight - i;
            break;
        }
    }

    let mut qweight = weight1 - weight2;
    if firstdigit1 <= firstdigit2 {
        qweight -= 1;
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = rscale.max(var1.dscale);
    rscale = rscale.max(var2.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
    rscale
}
