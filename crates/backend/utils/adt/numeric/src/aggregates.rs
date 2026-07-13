use ::mcx::{Allocator, Mcx};
use stringinfo::StringInfo;
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

    /// Bytes of both digit buffers (pos + neg), 0 when empty.
    pub fn digits_bytes(&self) -> usize {
        2 * self.ndigits.max(0) as usize * core::mem::size_of::<i32>()
    }

    /// Field clone with the digit buffers copied to `dst` (pos first, then
    /// neg) — the handed-table relocation of a state out of its arena.
    ///
    /// # Safety
    /// `dst` is 4-aligned with [`Self::digits_bytes`] bytes writable, and the
    /// source buffers are live per the arena contract.
    pub unsafe fn relocated_into(&self, dst: *mut i32) -> NumericSumAccum {
        let n = self.ndigits.max(0) as usize;
        let (pos, neg) = if n > 0 {
            // SAFETY: caller contract — dst holds 2n slots, sources live.
            unsafe {
                core::ptr::copy_nonoverlapping(self.pos_digits, dst, n);
                core::ptr::copy_nonoverlapping(self.neg_digits, dst.add(n), n);
            }
            (dst, unsafe { dst.add(n) })
        } else {
            (core::ptr::null_mut(), core::ptr::null_mut())
        };
        NumericSumAccum {
            ndigits: self.ndigits,
            weight: self.weight,
            dscale: self.dscale,
            num_uncarried: self.num_uncarried,
            have_carry_space: self.have_carry_space,
            pos_digits: pos,
            neg_digits: neg,
        }
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

    /// Digit-buffer bytes a [`Self::relocated_into`] copy needs.
    pub fn digits_bytes(&self) -> usize {
        self.sum_x.digits_bytes() + self.sum_x2.digits_bytes()
    }

    /// Field clone with all digit buffers copied to `digits` (sum_x's pair
    /// first) — the handed-table relocation of a state out of its arena.
    ///
    /// # Safety
    /// `digits` is 4-aligned with [`Self::digits_bytes`] bytes writable, and
    /// the source buffers are live per the arena contract.
    pub unsafe fn relocated_into(&self, digits: *mut i32) -> NumericAggState {
        // SAFETY: caller contract, split across the two accumulators.
        let (sum_x, sum_x2) = unsafe {
            let sum_x = self.sum_x.relocated_into(digits);
            let x2_dst = digits.add(2 * self.sum_x.ndigits.max(0) as usize);
            (sum_x, self.sum_x2.relocated_into(x2_dst))
        };
        NumericAggState {
            calc_sum_x2: self.calc_sum_x2,
            n: self.n,
            sum_x,
            sum_x2,
            max_scale: self.max_scale,
            max_scale_count: self.max_scale_count,
            nan_count: self.nan_count,
            pinf_count: self.pinf_count,
            ninf_count: self.ninf_count,
        }
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

// C numeric.c:7843/7859 numericvar_serialize/numericvar_deserialize: int32
// header fields (unlike numeric_send's int16 — intermediate values may exceed
// the numeric type's ranges), digits unvalidated.
pub fn numericvar_serialize(buf: &mut StringInfo<'_>, var: VarView<'_>) -> PgResult<()> {
    pqformat::pq_sendint32(buf, var.ndigits as u32)?;
    pqformat::pq_sendint32(buf, var.weight as u32)?;
    pqformat::pq_sendint32(buf, var.sign as u32)?;
    pqformat::pq_sendint32(buf, var.dscale as u32)?;
    for &d in var.digits {
        pqformat::pq_sendint16(buf, d as u16)?;
    }
    Ok(())
}

pub fn numericvar_deserialize(buf: &mut StringInfo<'_>, var: &mut NumericVar) -> PgResult<()> {
    let len = pqformat::pq_getmsgint(buf, 4)? as i32;
    var.alloc(len);
    var.weight = pqformat::pq_getmsgint(buf, 4)? as i32;
    var.sign = pqformat::pq_getmsgint(buf, 4)? as u16;
    var.dscale = pqformat::pq_getmsgint(buf, 4)? as i32;
    for slot in var.digits_mut() {
        *slot = pqformat::pq_getmsgint(buf, 2)? as u16 as NumericDigit;
    }
    Ok(())
}

// C numeric.c:5323 numeric_avg_serialize / 5433 numeric_serialize bodies past
// the AggCheckCallContext gate; they differ only in the sumX2 leg.
pub fn numeric_agg_state_serialize(
    state: &mut NumericAggState,
    with_sum_x2: bool,
    buf: &mut StringInfo<'_>,
) -> PgResult<()> {
    let mut tmp = NumericVar::new();
    pqformat::pq_sendint64(buf, state.n as u64)?;
    state.sum_x.finalize(&mut tmp);
    numericvar_serialize(buf, tmp.view())?;
    if with_sum_x2 {
        state.sum_x2.finalize(&mut tmp);
        numericvar_serialize(buf, tmp.view())?;
    }
    pqformat::pq_sendint32(buf, state.max_scale as u32)?;
    pqformat::pq_sendint64(buf, state.max_scale_count as u64)?;
    pqformat::pq_sendint64(buf, state.nan_count as u64)?;
    pqformat::pq_sendint64(buf, state.pinf_count as u64)?;
    pqformat::pq_sendint64(buf, state.ninf_count as u64)?;
    Ok(())
}

// C numeric.c:5375 numeric_avg_deserialize / 5489 numeric_deserialize. C
// allocates with makeNumericAggStateCurrentContext(false) in both variants;
// `mcx` is the context the caller stores the state in (digit buffers must
// share it).
pub fn numeric_agg_state_deserialize(
    buf: &mut StringInfo<'_>,
    mcx: Mcx<'_>,
    with_sum_x2: bool,
) -> PgResult<NumericAggState> {
    let mut result = NumericAggState::new(false);
    let mut tmp = NumericVar::new();
    result.n = pqformat::pq_getmsgint64(buf)?;
    numericvar_deserialize(buf, &mut tmp)?;
    result.sum_x.add(mcx, tmp.view())?;
    if with_sum_x2 {
        numericvar_deserialize(buf, &mut tmp)?;
        result.sum_x2.add(mcx, tmp.view())?;
    }
    result.max_scale = pqformat::pq_getmsgint(buf, 4)? as i32;
    result.max_scale_count = pqformat::pq_getmsgint64(buf)?;
    result.nan_count = pqformat::pq_getmsgint64(buf)?;
    result.pinf_count = pqformat::pq_getmsgint64(buf)?;
    result.ninf_count = pqformat::pq_getmsgint64(buf)?;
    pqformat::pq_getmsgend(buf)?;
    Ok(result)
}

// C numeric.c:5159 numeric_combine / 5251 numeric_avg_combine, non-NULL
// state1 arm. `mcx` is state1's owning agg context.
pub fn numeric_agg_combine(
    state1: &mut NumericAggState,
    state2: &mut NumericAggState,
    mcx: Mcx<'_>,
    with_sum_x2: bool,
) -> PgResult<()> {
    state1.n += state2.n;
    state1.nan_count += state2.nan_count;
    state1.pinf_count += state2.pinf_count;
    state1.ninf_count += state2.ninf_count;
    if state2.n > 0 {
        if state2.max_scale > state1.max_scale {
            state1.max_scale = state2.max_scale;
            state1.max_scale_count = state2.max_scale_count;
        } else if state2.max_scale == state1.max_scale {
            state1.max_scale_count += state2.max_scale_count;
        }
        state1.sum_x.combine(mcx, &mut state2.sum_x)?;
        if with_sum_x2 {
            state1.sum_x2.combine(mcx, &mut state2.sum_x2)?;
        }
    }
    Ok(())
}

// C numeric_combine/numeric_avg_combine NULL-state1 arm: field copy into the
// freshly made agg-context state.
pub fn numeric_agg_copy(
    dst: &mut NumericAggState,
    src: &mut NumericAggState,
    mcx: Mcx<'_>,
    with_sum_x2: bool,
) -> PgResult<()> {
    dst.n = src.n;
    dst.nan_count = src.nan_count;
    dst.pinf_count = src.pinf_count;
    dst.ninf_count = src.ninf_count;
    dst.max_scale = src.max_scale;
    dst.max_scale_count = src.max_scale_count;
    dst.sum_x.copy_from(mcx, &mut src.sum_x)?;
    if with_sum_x2 {
        dst.sum_x2.copy_from(mcx, &mut src.sum_x2)?;
    }
    Ok(())
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

// C numeric.c:5800 numeric_poly_serialize / 5998 int8_avg_serialize,
// HAVE_INT128 arm: int128 sums cross the wire as NumericVar images
// (int128_to_numericvar) so the combine format is platform-independent.
pub fn int128_agg_state_serialize(
    state: &Int128AggState,
    with_sum_x2: bool,
    buf: &mut StringInfo<'_>,
) -> PgResult<()> {
    let mut tmp = NumericVar::new();
    pqformat::pq_sendint64(buf, state.n as u64)?;
    crate::var::int128_to_var(state.sum_x, &mut tmp);
    numericvar_serialize(buf, tmp.view())?;
    if with_sum_x2 {
        crate::var::int128_to_var(state.sum_x2, &mut tmp);
        numericvar_serialize(buf, tmp.view())?;
    }
    Ok(())
}

// C numeric.c:5858 numeric_poly_deserialize / 6047 int8_avg_deserialize.
// C ignores numericvar_to_int128's overflow bool, leaving the palloc0'd sum
// at 0; both variants allocate with calcSumX2=false.
pub fn int128_agg_state_deserialize(
    buf: &mut StringInfo<'_>,
    with_sum_x2: bool,
) -> PgResult<Int128AggState> {
    let mut result = Int128AggState::new(false);
    let mut tmp = NumericVar::new();
    result.n = pqformat::pq_getmsgint64(buf)?;
    numericvar_deserialize(buf, &mut tmp)?;
    result.sum_x = crate::var::var_to_int128(tmp.view()).unwrap_or(0);
    if with_sum_x2 {
        numericvar_deserialize(buf, &mut tmp)?;
        result.sum_x2 = crate::var::var_to_int128(tmp.view()).unwrap_or(0);
    }
    pqformat::pq_getmsgend(buf)?;
    Ok(result)
}

pub fn numeric_poly_avg(state: Option<&Int128AggState>) -> PgResult<Option<NumericImage>> {
    let Some(state) = state else { return Ok(None) };
    if state.n == 0 {
        return Ok(None);
    }
    Ok(Some(crate::ops::int128_avg_div(state.sum_x, state.n)?))
}
