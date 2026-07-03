use types_error::PgResult;

use crate::arith::{add_var, mul_var};
use crate::ops::numeric_avg_div;
use crate::var::{make_result, NumericImage, NumericVar, VarView};
use crate::{Num, NumericDigit, NBASE, NUMERIC_NEG, NUMERIC_POS};

/// C's NumericSumAccum: 32-bit digit limbs with lazy carry, positive and
/// negative inputs accumulated separately. Owned by aggregate state (agg
/// lifetime), so std Vec with retained capacity mirrors C's agg_context
/// buffers.
#[derive(Default)]
pub struct NumericSumAccum {
    ndigits: i32,
    weight: i32,
    dscale: i32,
    num_uncarried: i32,
    have_carry_space: bool,
    pos_digits: Vec<i32>,
    neg_digits: Vec<i32>,
}

impl NumericSumAccum {
    pub fn new() -> NumericSumAccum {
        NumericSumAccum::default()
    }

    pub fn reset(&mut self) {
        self.dscale = 0;
        for d in self.pos_digits.iter_mut() {
            *d = 0;
        }
        for d in self.neg_digits.iter_mut() {
            *d = 0;
        }
        self.num_uncarried = 0;
    }

    pub fn add(&mut self, val: VarView<'_>) {
        if self.num_uncarried == NBASE - 1 {
            self.carry();
        }

        self.rescale(val);

        let accum_digits = if val.sign == NUMERIC_POS {
            &mut self.pos_digits
        } else {
            &mut self.neg_digits
        };

        let mut i = (self.weight - val.weight) as usize;
        for &d in val.digits {
            accum_digits[i] += d as i32;
            i += 1;
        }

        self.num_uncarried += 1;
    }

    fn carry(&mut self) {
        if self.num_uncarried == 0 {
            return;
        }

        debug_assert!(self.pos_digits[0] == 0 && self.neg_digits[0] == 0);

        let ndigits = self.ndigits as usize;

        let mut newdig = 0i32;
        let mut carry = 0i32;
        for i in (0..ndigits).rev() {
            newdig = self.pos_digits[i] + carry;
            if newdig >= NBASE {
                carry = newdig / NBASE;
                newdig -= carry * NBASE;
            } else {
                carry = 0;
            }
            self.pos_digits[i] = newdig;
        }
        if newdig > 0 {
            self.have_carry_space = false;
        }

        let mut newdig = 0i32;
        let mut carry = 0i32;
        for i in (0..ndigits).rev() {
            newdig = self.neg_digits[i] + carry;
            if newdig >= NBASE {
                carry = newdig / NBASE;
                newdig -= carry * NBASE;
            } else {
                carry = 0;
            }
            self.neg_digits[i] = newdig;
        }
        if newdig > 0 {
            self.have_carry_space = false;
        }

        self.num_uncarried = 0;
    }

    fn rescale(&mut self, val: VarView<'_>) {
        let old_weight = self.weight;
        let old_ndigits = self.ndigits;
        let mut accum_weight = old_weight;
        let mut accum_ndigits = old_ndigits;

        if val.weight >= accum_weight {
            accum_weight = val.weight + 1;
            accum_ndigits += accum_weight - old_weight;
        } else if !self.have_carry_space {
            accum_weight += 1;
            accum_ndigits += 1;
        }

        let accum_rscale = accum_ndigits - accum_weight - 1;
        let val_rscale = val.ndigits - val.weight - 1;
        if val_rscale > accum_rscale {
            accum_ndigits += val_rscale - accum_rscale;
        }

        if accum_ndigits != old_ndigits || accum_weight != old_weight {
            let weightdiff = (accum_weight - old_weight) as usize;

            let mut new_pos = vec![0i32; accum_ndigits as usize];
            let mut new_neg = vec![0i32; accum_ndigits as usize];
            if !self.pos_digits.is_empty() {
                new_pos[weightdiff..weightdiff + old_ndigits as usize]
                    .copy_from_slice(&self.pos_digits);
                new_neg[weightdiff..weightdiff + old_ndigits as usize]
                    .copy_from_slice(&self.neg_digits);
            }
            self.pos_digits = new_pos;
            self.neg_digits = new_neg;

            self.weight = accum_weight;
            self.ndigits = accum_ndigits;

            debug_assert!(self.pos_digits[0] == 0 && self.neg_digits[0] == 0);
            self.have_carry_space = true;
        }

        if val.dscale > self.dscale {
            self.dscale = val.dscale;
        }
    }

    pub fn finalize(&mut self, result: &mut NumericVar) {
        if self.ndigits == 0 {
            result.set_zero();
            result.dscale = 0;
            return;
        }

        self.carry();

        let mut pos_var = NumericVar::new();
        pos_var.alloc(self.ndigits);
        pos_var.weight = self.weight;
        pos_var.dscale = self.dscale;
        pos_var.sign = NUMERIC_POS;

        let mut neg_var = NumericVar::new();
        neg_var.alloc(self.ndigits);
        neg_var.weight = self.weight;
        neg_var.dscale = self.dscale;
        neg_var.sign = NUMERIC_NEG;

        {
            let pd = pos_var.digits_mut();
            for (dst, src) in pd.iter_mut().zip(&self.pos_digits) {
                debug_assert!(*src < NBASE);
                *dst = *src as NumericDigit;
            }
        }
        {
            let nd = neg_var.digits_mut();
            for (dst, src) in nd.iter_mut().zip(&self.neg_digits) {
                debug_assert!(*src < NBASE);
                *dst = *src as NumericDigit;
            }
        }

        add_var(pos_var.view(), neg_var.view(), result);
        result.strip();
    }

    pub fn copy_from(&mut self, src: &NumericSumAccum) {
        self.pos_digits = src.pos_digits.clone();
        self.neg_digits = src.neg_digits.clone();
        self.num_uncarried = src.num_uncarried;
        self.ndigits = src.ndigits;
        self.weight = src.weight;
        self.dscale = src.dscale;
        self.have_carry_space = src.have_carry_space;
    }

    pub fn combine(&mut self, other: &mut NumericSumAccum) {
        let mut tmp = NumericVar::new();
        other.finalize(&mut tmp);
        self.add(tmp.view());
    }
}

pub struct NumericAggState {
    pub calc_sum_x2: bool,
    pub n: i64,
    pub sum_x: NumericSumAccum,
    pub sum_x2: NumericSumAccum,
    pub max_scale: i32,
    pub max_scale_count: i64,
    pub nan_count: i64,
    pub pinf_count: i64,
    pub ninf_count: i64,
}

impl NumericAggState {
    pub fn new(calc_sum_x2: bool) -> NumericAggState {
        NumericAggState {
            calc_sum_x2,
            n: 0,
            sum_x: NumericSumAccum::new(),
            sum_x2: NumericSumAccum::new(),
            max_scale: 0,
            max_scale_count: 0,
            nan_count: 0,
            pinf_count: 0,
            ninf_count: 0,
        }
    }

    pub fn total_count(&self) -> i64 {
        self.n + self.nan_count + self.pinf_count + self.ninf_count
    }
}

pub fn do_numeric_accum(state: &mut NumericAggState, newval: Num<'_>) {
    if newval.is_special() {
        if newval.is_pinf() {
            state.pinf_count += 1;
        } else if newval.is_ninf() {
            state.ninf_count += 1;
        } else {
            state.nan_count += 1;
        }
        return;
    }

    let x = newval.view();

    // Track the highest input dscale seen (inverse-transition support).
    if x.dscale > state.max_scale {
        state.max_scale = x.dscale;
        state.max_scale_count = 1;
    } else if x.dscale == state.max_scale {
        state.max_scale_count += 1;
    }

    if state.calc_sum_x2 {
        let mut x2 = NumericVar::new();
        mul_var(x, x, &mut x2, x.dscale * 2);
        state.n += 1;
        state.sum_x.add(x);
        state.sum_x2.add(x2.view());
    } else {
        state.n += 1;
        state.sum_x.add(x);
    }
}

pub fn do_numeric_discard(state: &mut NumericAggState, newval: Num<'_>) -> bool {
    if newval.is_special() {
        if newval.is_pinf() {
            state.pinf_count -= 1;
        } else if newval.is_ninf() {
            state.ninf_count -= 1;
        } else {
            state.nan_count -= 1;
        }
        return true;
    }

    let x = newval.view();

    if x.dscale == state.max_scale {
        if state.max_scale_count > 1 || state.max_scale == 0 {
            state.max_scale_count -= 1;
        } else if state.n == 1 {
            state.max_scale = 0;
            state.max_scale_count = 0;
        } else {
            // Correct new max_scale is unknowable; force re-aggregation.
            return false;
        }
    }

    let x2 = if state.calc_sum_x2 {
        let mut x2 = NumericVar::new();
        mul_var(x, x, &mut x2, x.dscale * 2);
        Some(x2)
    } else {
        None
    };

    state.n -= 1;
    if state.n > 0 {
        let mut neg_x = x;
        neg_x.sign = if x.sign == NUMERIC_POS {
            NUMERIC_NEG
        } else {
            NUMERIC_POS
        };
        state.sum_x.add(neg_x);

        if let Some(x2) = x2 {
            let mut v = x2.view();
            v.sign = NUMERIC_NEG;
            state.sum_x2.add(v);
        }
    } else {
        debug_assert_eq!(state.n, 0);
        state.sum_x.reset();
        if state.calc_sum_x2 {
            state.sum_x2.reset();
        }
    }

    true
}

pub fn do_numeric_accum_int64(state: &mut NumericAggState, newval: i64) {
    let img = crate::ops::int64_to_numeric(newval);
    do_numeric_accum(state, img.num());
}

/// SUM(numeric) final. None = SQL NULL.
pub fn numeric_sum(state: Option<&mut NumericAggState>) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    if state.total_count() == 0 {
        return Ok(None);
    }

    if state.nan_count > 0 || (state.pinf_count > 0 && state.ninf_count > 0) {
        return Ok(Some(NumericImage::nan()));
    }
    if state.pinf_count > 0 {
        return Ok(Some(NumericImage::pinf()));
    }
    if state.ninf_count > 0 {
        return Ok(Some(NumericImage::ninf()));
    }

    let mut sum = NumericVar::new();
    state.sum_x.finalize(&mut sum);
    Ok(Some(make_result(sum.view())?))
}

/// AVG(numeric) final. None = SQL NULL.
pub fn numeric_avg(state: Option<&mut NumericAggState>) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    if state.total_count() == 0 {
        return Ok(None);
    }

    if state.nan_count > 0 || (state.pinf_count > 0 && state.ninf_count > 0) {
        return Ok(Some(NumericImage::nan()));
    }
    if state.pinf_count > 0 {
        return Ok(Some(NumericImage::pinf()));
    }
    if state.ninf_count > 0 {
        return Ok(Some(NumericImage::ninf()));
    }

    let mut sum = NumericVar::new();
    state.sum_x.finalize(&mut sum);
    let sum_img = make_result(sum.view())?;
    Ok(Some(numeric_avg_div(sum_img.num(), state.n)?))
}

/// C's Int128AggState (HAVE_INT128 poly aggregate fast path).
#[derive(Default, Clone, Copy)]
pub struct Int128AggState {
    pub calc_sum_x2: bool,
    pub n: i64,
    pub sum_x: i128,
    pub sum_x2: i128,
}

impl Int128AggState {
    pub fn new(calc_sum_x2: bool) -> Int128AggState {
        Int128AggState {
            calc_sum_x2,
            ..Default::default()
        }
    }
}

#[inline]
pub fn do_int128_accum(state: &mut Int128AggState, newval: i128) {
    if state.calc_sum_x2 {
        state.sum_x2 += newval * newval;
    }
    state.sum_x += newval;
    state.n += 1;
}

#[inline]
pub fn do_int128_discard(state: &mut Int128AggState, newval: i128) {
    if state.calc_sum_x2 {
        state.sum_x2 -= newval * newval;
    }
    state.sum_x -= newval;
    state.n -= 1;
}

pub fn numeric_poly_sum(state: Option<&Int128AggState>) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    if state.n == 0 {
        return Ok(None);
    }
    let mut result = NumericVar::new();
    crate::var::int128_to_var(state.sum_x, &mut result);
    Ok(Some(make_result(result.view())?))
}

pub fn numeric_poly_avg(state: Option<&Int128AggState>) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    if state.n == 0 {
        return Ok(None);
    }
    let mut sum = NumericVar::new();
    crate::var::int128_to_var(state.sum_x, &mut sum);
    let sum_img = make_result(sum.view())?;
    Ok(Some(numeric_avg_div(sum_img.num(), state.n)?))
}
