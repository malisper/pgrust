use ::mcx::{Allocator, Mcx};
use types_error::PgResult;

use crate::arith::{add_var, cmp_var, div_var, mul_var, select_div_scale, sub_var};
use crate::math::sqrt_var;
use crate::ops::numeric_avg_div;
use crate::var::{int64_to_var, make_result, NumericImage, NumericVar, VarView};
use crate::{Num, NumericDigit, NBASE, NUMERIC_NEG, NUMERIC_POS};

/// C's NumericSumAccum: 32-bit digit limbs with lazy carry, positive and
/// negative inputs accumulated separately. Digit buffers live in the agg
/// context arena the state itself occupies (C pallocs them in agg_context and
/// pfrees on rescale; the arena reclaims wholesale instead), so the state
/// stays drop-free — every method taking `Mcx` must get that same context.
pub struct NumericSumAccum {
    ndigits: i32,
    weight: i32,
    dscale: i32,
    num_uncarried: i32,
    have_carry_space: bool,
    pos_digits: *mut i32,
    neg_digits: *mut i32,
}

const _: () = assert!(!core::mem::needs_drop::<NumericSumAccum>());

impl Default for NumericSumAccum {
    fn default() -> Self {
        NumericSumAccum::new()
    }
}

fn alloc_zeroed_digits(mcx: Mcx<'_>, n: usize) -> PgResult<*mut i32> {
    let layout = core::alloc::Layout::array::<i32>(n).expect("digit buffer layout");
    let raw: core::ptr::NonNull<u8> =
        mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
    let p = raw.as_ptr().cast::<i32>();
    // SAFETY: fresh allocation of n i32 slots.
    unsafe { core::ptr::write_bytes(p, 0, n) };
    Ok(p)
}

impl NumericSumAccum {
    pub fn new() -> NumericSumAccum {
        NumericSumAccum {
            ndigits: 0,
            weight: 0,
            dscale: 0,
            num_uncarried: 0,
            have_carry_space: false,
            pos_digits: core::ptr::null_mut(),
            neg_digits: core::ptr::null_mut(),
        }
    }

    #[inline]
    fn pos(&mut self) -> &mut [i32] {
        if self.ndigits == 0 {
            return &mut [];
        }
        // SAFETY: non-zero ndigits implies live same-arena buffers of that
        // length (alloc_zeroed_digits in rescale); sole access path.
        unsafe { core::slice::from_raw_parts_mut(self.pos_digits, self.ndigits as usize) }
    }

    #[inline]
    fn neg(&mut self) -> &mut [i32] {
        if self.ndigits == 0 {
            return &mut [];
        }
        // SAFETY: as `pos`.
        unsafe { core::slice::from_raw_parts_mut(self.neg_digits, self.ndigits as usize) }
    }

    pub fn reset(&mut self) {
        self.dscale = 0;
        self.pos().fill(0);
        self.neg().fill(0);
        self.num_uncarried = 0;
    }

    /// C `accum_sum_add`; `mcx` is the owning agg context (rescale target).
    pub fn add(&mut self, mcx: Mcx<'_>, val: VarView<'_>) -> PgResult<()> {
        if self.num_uncarried == NBASE - 1 {
            self.carry();
        }

        self.rescale(mcx, val)?;

        let start = (self.weight - val.weight) as usize;
        let accum_digits = if val.sign == NUMERIC_POS { self.pos() } else { self.neg() };
        for (i, &d) in val.digits.iter().enumerate() {
            accum_digits[start + i] += d as i32;
        }

        self.num_uncarried += 1;
        Ok(())
    }

    fn carry(&mut self) {
        if self.num_uncarried == 0 {
            return;
        }

        let ndigits = self.ndigits as usize;
        debug_assert!(ndigits == 0 || (self.pos()[0] == 0 && self.neg()[0] == 0));

        let mut spilled = false;
        for digits in [self.pos_digits, self.neg_digits] {
            if ndigits == 0 {
                break;
            }
            // SAFETY: as `pos` — live same-arena buffers of ndigits length.
            let digits = unsafe { core::slice::from_raw_parts_mut(digits, ndigits) };
            let mut newdig = 0i32;
            let mut carry = 0i32;
            for i in (0..ndigits).rev() {
                newdig = digits[i] + carry;
                if newdig >= NBASE {
                    carry = newdig / NBASE;
                    newdig -= carry * NBASE;
                } else {
                    carry = 0;
                }
                digits[i] = newdig;
            }
            if newdig > 0 {
                spilled = true;
            }
        }
        if spilled {
            self.have_carry_space = false;
        }

        self.num_uncarried = 0;
    }

    fn rescale(&mut self, mcx: Mcx<'_>, val: VarView<'_>) -> PgResult<()> {
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

            let new_pos = alloc_zeroed_digits(mcx, accum_ndigits as usize)?;
            let new_neg = alloc_zeroed_digits(mcx, accum_ndigits as usize)?;
            if old_ndigits > 0 {
                // SAFETY: fresh buffers of accum_ndigits >= weightdiff +
                // old_ndigits slots; old buffers live per the arena contract.
                // C pfrees the old pair; the bump arena reclaims at reset.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        self.pos_digits,
                        new_pos.add(weightdiff),
                        old_ndigits as usize,
                    );
                    core::ptr::copy_nonoverlapping(
                        self.neg_digits,
                        new_neg.add(weightdiff),
                        old_ndigits as usize,
                    );
                }
            }
            self.pos_digits = new_pos;
            self.neg_digits = new_neg;

            self.weight = accum_weight;
            self.ndigits = accum_ndigits;

            debug_assert!(self.pos()[0] == 0 && self.neg()[0] == 0);
            self.have_carry_space = true;
        }

        if val.dscale > self.dscale {
            self.dscale = val.dscale;
        }
        Ok(())
    }

    /// C `accum_sum_final`.
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
            for (dst, src) in pd.iter_mut().zip(self.pos().iter()) {
                debug_assert!(*src < NBASE);
                *dst = *src as NumericDigit;
            }
        }
        {
            let nd = neg_var.digits_mut();
            for (dst, src) in nd.iter_mut().zip(self.neg().iter()) {
                debug_assert!(*src < NBASE);
                *dst = *src as NumericDigit;
            }
        }

        add_var(pos_var.view(), neg_var.view(), result);
        result.strip();
    }

    /// C `accum_sum_copy`.
    pub fn copy_from(&mut self, mcx: Mcx<'_>, src: &mut NumericSumAccum) -> PgResult<()> {
        let n = src.ndigits as usize;
        if n > 0 {
            let pos = alloc_zeroed_digits(mcx, n)?;
            let neg = alloc_zeroed_digits(mcx, n)?;
            // SAFETY: fresh n-slot buffers; src buffers live per the arena
            // contract.
            unsafe {
                core::ptr::copy_nonoverlapping(src.pos_digits, pos, n);
                core::ptr::copy_nonoverlapping(src.neg_digits, neg, n);
            }
            self.pos_digits = pos;
            self.neg_digits = neg;
        } else {
            self.pos_digits = core::ptr::null_mut();
            self.neg_digits = core::ptr::null_mut();
        }
        self.num_uncarried = src.num_uncarried;
        self.ndigits = src.ndigits;
        self.weight = src.weight;
        self.dscale = src.dscale;
        self.have_carry_space = src.have_carry_space;
        Ok(())
    }

    /// C `accum_sum_combine`.
    pub fn combine(&mut self, mcx: Mcx<'_>, other: &mut NumericSumAccum) -> PgResult<()> {
        let mut tmp = NumericVar::new();
        other.finalize(&mut tmp);
        self.add(mcx, tmp.view())
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

const _: () = assert!(!core::mem::needs_drop::<NumericAggState>());

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

/// `mcx` is the agg context owning the state (C `state->agg_context`).
pub fn do_numeric_accum(
    state: &mut NumericAggState,
    mcx: Mcx<'_>,
    newval: Num<'_>,
) -> PgResult<()> {
    if newval.is_special() {
        if newval.is_pinf() {
            state.pinf_count += 1;
        } else if newval.is_ninf() {
            state.ninf_count += 1;
        } else {
            state.nan_count += 1;
        }
        return Ok(());
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
        state.sum_x.add(mcx, x)?;
        state.sum_x2.add(mcx, x2.view())?;
    } else {
        state.n += 1;
        state.sum_x.add(mcx, x)?;
    }
    Ok(())
}

/// `Ok(false)` = un-aggregation impossible (C's re-aggregate signal).
pub fn do_numeric_discard(
    state: &mut NumericAggState,
    mcx: Mcx<'_>,
    newval: Num<'_>,
) -> PgResult<bool> {
    if newval.is_special() {
        if newval.is_pinf() {
            state.pinf_count -= 1;
        } else if newval.is_ninf() {
            state.ninf_count -= 1;
        } else {
            state.nan_count -= 1;
        }
        return Ok(true);
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
            return Ok(false);
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
        state.sum_x.add(mcx, neg_x)?;

        if let Some(x2) = x2 {
            let mut v = x2.view();
            v.sign = NUMERIC_NEG;
            state.sum_x2.add(mcx, v)?;
        }
    } else {
        debug_assert_eq!(state.n, 0);
        state.sum_x.reset();
        if state.calc_sum_x2 {
            state.sum_x2.reset();
        }
    }

    Ok(true)
}

pub fn do_numeric_accum_int64(
    state: &mut NumericAggState,
    mcx: Mcx<'_>,
    newval: i64,
) -> PgResult<()> {
    let img = crate::ops::int64_to_numeric(newval);
    do_numeric_accum(state, mcx, img.num())
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

// The arithmetic tail of C numeric_stddev_internal, shared with the poly lane.
fn stddev_from_sums(
    n: i64,
    vsum_x: &NumericVar,
    vsum_x2: NumericVar,
    variance: bool,
    sample: bool,
) -> PgResult<NumericImage> {
    let v_n = int64_to_var(n);
    let one = int64_to_var(1);
    let mut v_nminus1 = NumericVar::new();
    sub_var(v_n.view(), one.view(), &mut v_nminus1);

    let rscale = vsum_x.dscale * 2;

    let mut vsum_x_sq = NumericVar::new();
    mul_var(vsum_x.view(), vsum_x.view(), &mut vsum_x_sq, rscale);
    let mut n_sum_x2 = NumericVar::new();
    mul_var(v_n.view(), vsum_x2.view(), &mut n_sum_x2, rscale);
    let mut numerator = NumericVar::new();
    sub_var(n_sum_x2.view(), vsum_x_sq.view(), &mut numerator);

    let zero = int64_to_var(0);
    if cmp_var(numerator.view(), zero.view()) <= 0 {
        // Roundoff error can produce a negative numerator (C comment).
        return make_result(zero.view());
    }

    let mut denom = NumericVar::new();
    if sample {
        mul_var(v_n.view(), v_nminus1.view(), &mut denom, 0);
    } else {
        mul_var(v_n.view(), v_n.view(), &mut denom, 0);
    }
    let rscale = select_div_scale(numerator.view(), denom.view());
    let mut result = NumericVar::new();
    div_var(numerator.view(), denom.view(), &mut result, rscale, true, true)?;
    if !variance {
        let arg = core::mem::replace(&mut result, NumericVar::new());
        sqrt_var(arg.view(), &mut result, rscale)?;
    }

    make_result(result.view())
}

/// C `numeric_stddev_internal`. None = SQL NULL.
pub fn numeric_stddev_internal(
    state: Option<&mut NumericAggState>,
    variance: bool,
    sample: bool,
) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    let tot_count = state.total_count();
    if tot_count == 0 || (sample && tot_count <= 1) {
        return Ok(None);
    }

    // Any NaN or infinity input produces NaN output (C float8 analogy).
    if state.nan_count > 0 || state.pinf_count > 0 || state.ninf_count > 0 {
        return Ok(Some(NumericImage::nan()));
    }

    let mut vsum_x = NumericVar::new();
    let mut vsum_x2 = NumericVar::new();
    state.sum_x.finalize(&mut vsum_x);
    state.sum_x2.finalize(&mut vsum_x2);
    Ok(Some(stddev_from_sums(state.n, &vsum_x, vsum_x2, variance, sample)?))
}

/// C `numeric_poly_stddev_internal` (HAVE_INT128). None = SQL NULL.
pub fn numeric_poly_stddev_internal(
    state: Option<&Int128AggState>,
    variance: bool,
    sample: bool,
) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    if state.n == 0 || (sample && state.n <= 1) {
        return Ok(None);
    }

    let mut vsum_x = NumericVar::new();
    let mut vsum_x2 = NumericVar::new();
    crate::var::int128_to_var(state.sum_x, &mut vsum_x);
    crate::var::int128_to_var(state.sum_x2, &mut vsum_x2);
    Ok(Some(stddev_from_sums(state.n, &vsum_x, vsum_x2, variance, sample)?))
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
