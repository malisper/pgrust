//! Family: aggregates — the lazy [`NumericSumAccum`] sum accumulator
//! (`accum_sum_*`), the Youngs-Cramer [`NumericAggState`] for `sum`/`avg`/
//! `variance`/`stddev` over real `numeric[]` arrays (`do_numeric_accum`/
//! `discard` + the `numeric_*`/`numeric_poly_*` transition/inverse/combine/
//! serialize/deserialize/final functions), the 128-bit [`Int128AggState`] fast
//! path (`do_int128_accum`/`discard` + `int{2,4,8}_accum`), the int
//! sum/avg transitions, plus abbreviated-key sort-support (`numeric_sortsupport`/
//! `numeric_abbrev_*`) and the value hashers (`hash_numeric`/
//! `hash_numeric_extended`).
//!
//! Accumulators bear charged `PgVec`s (the `'mcx` lifetime); allocating
//! transitions take an explicit `Mcx<'mcx>` and return [`PgResult`] where the C
//! `ereport`s. The HyperLogLog cardinality estimator is held by value in the
//! abbreviated-key sort state ([`NumericSortSupportState`]) and operated on
//! directly via `backend-lib-hyperloglog` (mirroring varlena). Only the
//! function-pointer / `ssup_extra` install into the trimmed `SortSupportData`
//! node is a genuine external reached via seam (the unported tuplesort
//! abbreviation machinery).

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::types_numeric::var::{NumericAggState, NumericSign, NumericSumAccum, NumericVar};
use types_numeric::{
    numeric_digit_at, numeric_digits, numeric_is_ninf, numeric_is_pinf, numeric_is_special,
    numeric_ndigits, numeric_weight, Int128AggState, NumericDigit, NumericSortSupport, DEC_DIGITS,
    NBASE, NUMERIC_ABBREV_NAN, NUMERIC_ABBREV_NINF, NUMERIC_ABBREV_PINF,
};

use hyperloglog as hll;
use ::nodes::nodeagg::HyperLogLog;

use crate::{convert, kernel_transcendental, kernel_var, ops_sql};

// ---------------------------------------------------------------------------
// NumericSumAccum (numeric.c accum_sum_*).
// ---------------------------------------------------------------------------

/// `accum_sum_add(accum, val)` (numeric.c:12334): accumulate a new value.
pub fn accum_sum_add(accum: &mut NumericSumAccum<'_>, val: &NumericVar<'_>) -> PgResult<()> {
    // If we have accumulated too many values since the last carry propagation,
    // do it now, to avoid overflowing.
    if accum.num_uncarried == NBASE - 1 {
        accum_sum_carry(accum);
    }

    // Adjust the weight or scale of the old value to accommodate the new value.
    accum_sum_rescale(accum, val)?;

    // Choose the positive or negative limb array.
    let val_ndigits = val.ndigits();
    let val_digits = val.logical_digits();

    let accum_digits = if val.sign == NumericSign::Pos {
        &mut accum.pos_digits
    } else {
        &mut accum.neg_digits
    };

    let mut i = (accum.weight - val.weight) as usize;
    for &d in val_digits.iter().take(val_ndigits) {
        accum_digits[i] += i32::from(d);
        i += 1;
    }

    accum.num_uncarried += 1;
    Ok(())
}

/// `accum_sum_rescale(accum, val)` (numeric.c:12455): re-scale the accumulator
/// to accommodate a new value, enlarging the limb buffers when needed.
pub fn accum_sum_rescale(accum: &mut NumericSumAccum<'_>, val: &NumericVar<'_>) -> PgResult<()> {
    let old_weight = accum.weight;
    let old_ndigits = accum.ndigits;

    let mut accum_weight = old_weight;
    let mut accum_ndigits = old_ndigits;

    let val_ndigits = val.ndigits() as i32;

    // Does the new value have a larger weight?  If so, enlarge the buffers and
    // shift the existing value to the new weight by adding leading zeros.  We
    // enforce that the accumulator always has a weight one larger than needed
    // for the inputs, so there is space for an extra digit at final carry.
    if val.weight >= accum_weight {
        accum_weight = val.weight + 1;
        accum_ndigits += accum_weight - old_weight;
    }
    // Even though the new value is small, we might've used up the space
    // reserved for the carry digit in the last call to accum_sum_carry().  If
    // so, enlarge to make room for another one.
    else if !accum.have_carry_space {
        accum_weight += 1;
        accum_ndigits += 1;
    }

    // Is the new value wider on the right side?
    let accum_rscale = accum_ndigits - accum_weight - 1;
    let val_rscale = val_ndigits - val.weight - 1;
    if val_rscale > accum_rscale {
        accum_ndigits += val_rscale - accum_rscale;
    }

    if accum_ndigits != old_ndigits || accum_weight != old_weight {
        let weightdiff = (accum_weight - old_weight) as usize;
        let new_len = accum_ndigits as usize;

        let pos_mcx = *accum.pos_digits.allocator();
        let neg_mcx = *accum.neg_digits.allocator();
        let mut new_pos_digits = alloc_zeroed_limbs(pos_mcx, new_len)?;
        let mut new_neg_digits = alloc_zeroed_limbs(neg_mcx, new_len)?;

        // The old limbs are copied in starting at offset weightdiff.  (When the
        // accumulator was empty, old_ndigits == 0 and nothing is copied.)
        let copy_len = old_ndigits as usize;
        new_pos_digits[weightdiff..weightdiff + copy_len]
            .copy_from_slice(&accum.pos_digits[..copy_len]);
        new_neg_digits[weightdiff..weightdiff + copy_len]
            .copy_from_slice(&accum.neg_digits[..copy_len]);

        accum.pos_digits = new_pos_digits;
        accum.neg_digits = new_neg_digits;

        accum.weight = accum_weight;
        accum.ndigits = accum_ndigits;

        debug_assert!(accum.pos_digits[0] == 0 && accum.neg_digits[0] == 0);
        accum.have_carry_space = true;
    }

    if val.dscale > accum.dscale {
        accum.dscale = val.dscale;
    }

    Ok(())
}

/// `accum_sum_carry(accum)` (numeric.c:12382): propagate carries.
pub fn accum_sum_carry(accum: &mut NumericSumAccum<'_>) {
    // If no new values have been added since last carry propagation, nothing
    // to do.
    if accum.num_uncarried == 0 {
        return;
    }

    // We maintain that the weight of the accumulator is always one larger than
    // needed to hold the current value, before carrying, so there is enough
    // space for the possible extra digit when carry is propagated.
    debug_assert!(accum.pos_digits[0] == 0 && accum.neg_digits[0] == 0);

    let ndigits = accum.ndigits as usize;

    // Propagate carry in the positive sum.
    let mut newdig: i32 = 0;
    {
        let dig = &mut accum.pos_digits;
        let mut carry: i32 = 0;
        for i in (0..ndigits).rev() {
            newdig = dig[i] + carry;
            if newdig >= NBASE {
                carry = newdig / NBASE;
                newdig -= carry * NBASE;
            } else {
                carry = 0;
            }
            dig[i] = newdig;
        }
    }
    // Did we use up the digit reserved for carry propagation?
    if newdig > 0 {
        accum.have_carry_space = false;
    }

    // And the same for the negative sum.
    newdig = 0;
    {
        let dig = &mut accum.neg_digits;
        let mut carry: i32 = 0;
        for i in (0..ndigits).rev() {
            newdig = dig[i] + carry;
            if newdig >= NBASE {
                carry = newdig / NBASE;
                newdig -= carry * NBASE;
            } else {
                carry = 0;
            }
            dig[i] = newdig;
        }
    }
    if newdig > 0 {
        accum.have_carry_space = false;
    }

    accum.num_uncarried = 0;
}

/// `accum_sum_reset(accum)` (numeric.c:12318): reset the accumulator's value to
/// zero.  The limb buffers are not freed.
pub fn accum_sum_reset(accum: &mut NumericSumAccum<'_>) {
    accum.dscale = 0;
    let n = accum.ndigits as usize;
    for i in 0..n {
        accum.pos_digits[i] = 0;
        accum.neg_digits[i] = 0;
    }
}

/// `accum_sum_final(accum, result)` (numeric.c:12544): final carry propagation,
/// then add together the positive and negative sums.
pub fn accum_sum_final<'mcx>(
    mcx: Mcx<'mcx>,
    accum: &NumericSumAccum<'_>,
) -> PgResult<NumericVar<'mcx>> {
    if accum.ndigits == 0 {
        return Ok(kernel_var::const_zero(mcx));
    }

    // accum_sum_carry() mutates the accumulator; operate on a private copy so
    // this routine matches the C contract that the caller's accumulator is not
    // required to be in any particular memory context (and to keep `&self`).
    let mut work = accum_sum_copy(mcx, accum);

    // Perform final carry.
    accum_sum_carry(&mut work);

    // Create NumericVars representing the positive and negative sums.
    let ndigits = work.ndigits as usize;

    let mut pos_var = kernel_var::alloc_var(mcx, ndigits)?;
    let mut neg_var = kernel_var::alloc_var(mcx, ndigits)?;

    pos_var.weight = work.weight;
    neg_var.weight = work.weight;
    pos_var.dscale = work.dscale;
    neg_var.dscale = work.dscale;
    pos_var.sign = NumericSign::Pos;
    neg_var.sign = NumericSign::Neg;

    {
        let pos_digits = &mut pos_var.digits;
        let neg_digits = &mut neg_var.digits;
        let pos_off = pos_var.headroom;
        let neg_off = neg_var.headroom;
        for i in 0..ndigits {
            debug_assert!(work.pos_digits[i] < NBASE);
            pos_digits[pos_off + i] = work.pos_digits[i] as NumericDigit;

            debug_assert!(work.neg_digits[i] < NBASE);
            neg_digits[neg_off + i] = work.neg_digits[i] as NumericDigit;
        }
    }

    // And add them together.
    let mut result = kernel_var::add_var(mcx, &pos_var, &neg_var)?;

    // Remove leading/trailing zeroes.
    kernel_var::strip_var(&mut result);

    Ok(result)
}

/// `accum_sum_copy(dst, src)` (numeric.c:12595): copy an accumulator's state.
pub fn accum_sum_copy<'mcx>(mcx: Mcx<'mcx>, src: &NumericSumAccum<'_>) -> NumericSumAccum<'mcx> {
    let n = src.ndigits as usize;

    let mut pos_digits = PgVec::with_capacity_in(n, mcx);
    let mut neg_digits = PgVec::with_capacity_in(n, mcx);
    pos_digits.extend_from_slice(&src.pos_digits[..n]);
    neg_digits.extend_from_slice(&src.neg_digits[..n]);

    NumericSumAccum {
        ndigits: src.ndigits,
        weight: src.weight,
        dscale: src.dscale,
        num_uncarried: src.num_uncarried,
        have_carry_space: src.have_carry_space,
        pos_digits,
        neg_digits,
    }
}

/// `accum_sum_combine(accum, accum2)` (numeric.c:12612): add the current value
/// of `accum2` into `accum`.
pub fn accum_sum_combine<'mcx>(
    mcx: Mcx<'mcx>,
    accum: &mut NumericSumAccum<'_>,
    accum2: &NumericSumAccum<'_>,
) -> PgResult<()> {
    let tmp_var = accum_sum_final(mcx, accum2)?;
    accum_sum_add(accum, &tmp_var)
}

// ---------------------------------------------------------------------------
// NumericAggState transitions (Youngs-Cramer) over on-disk numeric values.
// ---------------------------------------------------------------------------

/// `do_numeric_accum(state, newval)` (numeric.c:4976): accumulate a new input
/// value for numeric aggregate functions.  `newval` is the whole on-disk
/// `numeric` byte image.
pub fn do_numeric_accum(
    mcx: Mcx<'_>,
    state: &mut NumericAggState<'_>,
    newval: &[u8],
) -> PgResult<()> {
    // Count NaN/infinity inputs separately from all else.
    if numeric_is_special(newval) {
        if numeric_is_pinf(newval) {
            state.p_inf_count += 1;
        } else if numeric_is_ninf(newval) {
            state.n_inf_count += 1;
        } else {
            state.nan_count += 1;
        }
        return Ok(());
    }

    // Load processed number.
    let x = convert::set_var_from_num(mcx, newval)?;

    // Track the highest input dscale seen, to support inverse transitions (see
    // do_numeric_discard).
    if x.dscale > state.max_scale {
        state.max_scale = x.dscale;
        state.max_scale_count = 1;
    } else if x.dscale == state.max_scale {
        state.max_scale_count += 1;
    }

    // If we need X^2, calculate it.
    let x2 = if state.calc_sum_x2 {
        Some(kernel_var::mul_var(mcx, &x, &x, x.dscale * 2)?)
    } else {
        None
    };

    state.n += 1;

    // Accumulate sums.
    accum_sum_add(&mut state.sum_x, &x)?;

    if let Some(x2) = &x2 {
        accum_sum_add(&mut state.sum_x2, x2)?;
    }

    Ok(())
}

/// `do_numeric_discard(state, newval)` (numeric.c:5046): attempt to remove an
/// input value from the aggregated state.  Returns `false` (the C path) when
/// the value cannot be removed.
pub fn do_numeric_discard(
    mcx: Mcx<'_>,
    state: &mut NumericAggState<'_>,
    newval: &[u8],
) -> PgResult<bool> {
    // Count NaN/infinity inputs separately from all else.
    if numeric_is_special(newval) {
        if numeric_is_pinf(newval) {
            state.p_inf_count -= 1;
        } else if numeric_is_ninf(newval) {
            state.n_inf_count -= 1;
        } else {
            state.nan_count -= 1;
        }
        return Ok(true);
    }

    // Load processed number.
    let mut x = convert::set_var_from_num(mcx, newval)?;

    // state->sumX's dscale is the maximum dscale of any of the inputs.
    // Removing the last input with that dscale would require us to recompute
    // the maximum dscale of the remaining inputs, which we cannot do unless no
    // more non-NaN inputs remain at all.  So report failure in that case.
    if x.dscale == state.max_scale {
        if state.max_scale_count > 1 || state.max_scale == 0 {
            // Some remaining inputs have same dscale, or dscale hasn't gotten
            // above zero anyway.
            state.max_scale_count -= 1;
        } else if state.n == 1 {
            // No remaining non-NaN inputs at all, so reset maxScale.
            state.max_scale = 0;
            state.max_scale_count = 0;
        } else {
            // Correct new maxScale is uncertain, must fail.
            return Ok(false);
        }
    }

    // If we need X^2, calculate it.
    let x2 = if state.calc_sum_x2 {
        Some(kernel_var::mul_var(mcx, &x, &x, x.dscale * 2)?)
    } else {
        None
    };

    let prev_n = state.n;
    state.n -= 1;
    if prev_n > 1 {
        // Negate X, to subtract it from the sum.
        x.sign = if x.sign == NumericSign::Pos {
            NumericSign::Neg
        } else {
            NumericSign::Pos
        };
        accum_sum_add(&mut state.sum_x, &x)?;

        if let Some(mut x2) = x2 {
            // Negate X^2.  X^2 is always positive.
            x2.sign = NumericSign::Neg;
            accum_sum_add(&mut state.sum_x2, &x2)?;
        }
    } else {
        // Zero the sums.
        debug_assert!(state.n == 0);

        accum_sum_reset(&mut state.sum_x);
        if state.calc_sum_x2 {
            accum_sum_reset(&mut state.sum_x2);
        }
    }

    Ok(true)
}

/// `numeric_avg(state)` (numeric.c:6247): AVG(numeric) final.  `Ok(None)` is the
/// C `PG_RETURN_NULL()` (no non-null inputs).
pub fn numeric_avg<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // If there were no non-null inputs, return NULL.
    if state.total_count() == 0 {
        return Ok(None);
    }

    if state.nan_count > 0 {
        // There was at least one NaN input.
        return Ok(Some(make_special(mcx, NumericSign::NaN)?));
    }

    // Adding plus and minus infinities gives NaN.
    if state.p_inf_count > 0 && state.n_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::NaN)?));
    }
    if state.p_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::PInf)?));
    }
    if state.n_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::NInf)?));
    }

    let n_datum = convert::int64_to_numeric(mcx, state.n)?;

    let sum_x_var = accum_sum_final(mcx, &state.sum_x)?;
    let sum_x_datum = convert::make_result(mcx, &sum_x_var)?;

    Ok(Some(ops_sql::numeric_div(mcx, &sum_x_datum, &n_datum)?))
}

/// `numeric_sum(state)` (numeric.c:6282): SUM(numeric) final.
pub fn numeric_sum<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // If there were no non-null inputs, return NULL.
    if state.total_count() == 0 {
        return Ok(None);
    }

    if state.nan_count > 0 {
        // There was at least one NaN input.
        return Ok(Some(make_special(mcx, NumericSign::NaN)?));
    }

    // Adding plus and minus infinities gives NaN.
    if state.p_inf_count > 0 && state.n_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::NaN)?));
    }
    if state.p_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::PInf)?));
    }
    if state.n_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::NInf)?));
    }

    let sum_x_var = accum_sum_final(mcx, &state.sum_x)?;
    let result = convert::make_result(mcx, &sum_x_var)?;

    Ok(Some(result))
}

/// `numeric_var_pop`/`var_samp`/`stddev_pop`/`stddev_samp` share the
/// `numeric_stddev_internal` core (numeric.c:6325): variance/stddev final,
/// sample vs population.  `Ok(None)` is the C `*is_null = true`.
pub fn numeric_stddev_internal<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
    variance: bool,
    sample: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Sample stddev and variance are undefined when N <= 1; population stddev
    // is undefined when N == 0.  Return NULL in either case (NaNs and
    // infinities count as normal inputs for this purpose).
    let tot_count = state.total_count();
    if tot_count == 0 {
        return Ok(None);
    }

    if sample && tot_count <= 1 {
        return Ok(None);
    }

    // Deal with NaN and infinity cases.  By analogy to the float8 functions,
    // any infinity input produces NaN output.
    if state.nan_count > 0 || state.p_inf_count > 0 || state.n_inf_count > 0 {
        return Ok(Some(make_special(mcx, NumericSign::NaN)?));
    }

    // OK, normal calculation applies.
    let v_n = kernel_transcendental::int64_to_numericvar(mcx, state.n)?;
    let mut vsum_x = accum_sum_final(mcx, &state.sum_x)?;
    let mut vsum_x2 = accum_sum_final(mcx, &state.sum_x2)?;

    let const_one = kernel_var::const_one(mcx);
    let const_zero = kernel_var::const_zero(mcx);

    let mut v_nminus1 = kernel_var::sub_var(mcx, &v_n, &const_one)?;

    // Compute rscale for mul_var calls.
    let rscale = vsum_x.dscale * 2;

    // vsumX = sumX * sumX
    let vsum_x_sq = kernel_var::mul_var(mcx, &vsum_x, &vsum_x, rscale)?;
    vsum_x = vsum_x_sq;
    // vsumX2 = N * sumX2
    let vsum_x2_n = kernel_var::mul_var(mcx, &v_n, &vsum_x2, rscale)?;
    vsum_x2 = vsum_x2_n;
    // N * sumX2 - sumX * sumX
    vsum_x2 = kernel_var::sub_var(mcx, &vsum_x2, &vsum_x)?;

    let res = if kernel_var::cmp_var(&vsum_x2, &const_zero) != core::cmp::Ordering::Greater {
        // Watch out for roundoff error producing a negative numerator.
        convert::make_result(mcx, &const_zero)?
    } else {
        if sample {
            // N * (N - 1)
            v_nminus1 = kernel_var::mul_var(mcx, &v_n, &v_nminus1, 0)?;
        } else {
            // N * N
            v_nminus1 = kernel_var::mul_var(mcx, &v_n, &v_n, 0)?;
        }
        let rscale = kernel_var::select_div_scale(&vsum_x2, &v_nminus1);
        // variance
        let mut vsum = kernel_var::div_var(mcx, &vsum_x2, &v_nminus1, rscale, true, true)?;
        if !variance {
            // stddev
            vsum = kernel_transcendental::sqrt_var(mcx, &vsum, rscale)?;
        }
        convert::make_result(mcx, &vsum)?
    };

    Ok(Some(res))
}

/// `numeric_combine(state1, state2)` (numeric.c:5159): combine two `sumX`+`sumX2`
/// transition states.  When `state1` is `None` (the C NULL), `state2` is copied.
pub fn numeric_combine<'mcx>(
    mcx: Mcx<'mcx>,
    state1: Option<NumericAggState<'_>>,
    state2: &NumericAggState<'_>,
) -> PgResult<NumericAggState<'mcx>> {
    // state2 == NULL is handled by the caller (returns state1 unchanged); here
    // state2 is always present.

    // Manually copy all fields from state2 to state1 when state1 is NULL.
    let mut state1 = match state1 {
        None => {
            let mut s1 = NumericAggState::new(mcx, true);
            s1.n = state2.n;
            s1.nan_count = state2.nan_count;
            s1.p_inf_count = state2.p_inf_count;
            s1.n_inf_count = state2.n_inf_count;
            s1.max_scale = state2.max_scale;
            s1.max_scale_count = state2.max_scale_count;

            s1.sum_x = accum_sum_copy(mcx, &state2.sum_x);
            s1.sum_x2 = accum_sum_copy(mcx, &state2.sum_x2);

            return Ok(s1);
        }
        Some(s1) => clone_agg_state(mcx, &s1),
    };

    state1.n += state2.n;
    state1.nan_count += state2.nan_count;
    state1.p_inf_count += state2.p_inf_count;
    state1.n_inf_count += state2.n_inf_count;

    if state2.n > 0 {
        // These are currently only needed for moving aggregates, but do the
        // right thing anyway.
        if state2.max_scale > state1.max_scale {
            state1.max_scale = state2.max_scale;
            state1.max_scale_count = state2.max_scale_count;
        } else if state2.max_scale == state1.max_scale {
            state1.max_scale_count += state2.max_scale_count;
        }

        // Accumulate sums.
        accum_sum_combine(mcx, &mut state1.sum_x, &state2.sum_x)?;
        accum_sum_combine(mcx, &mut state1.sum_x2, &state2.sum_x2)?;
    }

    Ok(state1)
}

/// `numeric_serialize(state)` (numeric.c:5433): serialize a transition state
/// (requiring sumX2) for parallel transfer.  Mirrors `pq_begintypsend` /
/// `pq_sendint64` / `numericvar_serialize` / `pq_endtypsend` over a `bytea`
/// payload (big-endian wire ints, varlena header).
pub fn numeric_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = begin_typsend(mcx);

    // N
    send_int64(&mut buf, state.n);

    // sumX
    let tmp_var = accum_sum_final(mcx, &state.sum_x)?;
    io_serialize_var(&mut buf, &tmp_var);

    // sumX2
    let tmp_var = accum_sum_final(mcx, &state.sum_x2)?;
    io_serialize_var(&mut buf, &tmp_var);

    // maxScale
    send_int32(&mut buf, state.max_scale);

    // maxScaleCount
    send_int64(&mut buf, state.max_scale_count);

    // NaNcount
    send_int64(&mut buf, state.nan_count);

    // pInfcount
    send_int64(&mut buf, state.p_inf_count);

    // nInfcount
    send_int64(&mut buf, state.n_inf_count);

    end_typsend(&mut buf);
    Ok(buf)
}

/// `numeric_deserialize(buf)` (numeric.c:5488): deserialize a transition state
/// (requiring sumX2).  `buf` is the `bytea` payload (the wire body, no varlena
/// header — matching `VARDATA_ANY`).
pub fn numeric_deserialize<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<NumericAggState<'mcx>> {
    let mut pos = 0usize;

    let mut result = NumericAggState::new(mcx, false);

    // N
    result.n = get_int64(buf, &mut pos);

    // sumX
    let tmp_var = crate::io::numericvar_deserialize(mcx, buf, &mut pos)?;
    accum_sum_add(&mut result.sum_x, &tmp_var)?;

    // sumX2
    let tmp_var = crate::io::numericvar_deserialize(mcx, buf, &mut pos)?;
    accum_sum_add(&mut result.sum_x2, &tmp_var)?;

    // maxScale
    result.max_scale = get_int32(buf, &mut pos);

    // maxScaleCount
    result.max_scale_count = get_int64(buf, &mut pos);

    // NaNcount
    result.nan_count = get_int64(buf, &mut pos);

    // pInfcount
    result.p_inf_count = get_int64(buf, &mut pos);

    // nInfcount
    result.n_inf_count = get_int64(buf, &mut pos);

    Ok(result)
}

/// `numeric_avg_combine(state1, state2)` (numeric.c:5097): combine two AVG/SUM
/// transition states (no sumX2).  When `state1` is `None` (the C NULL),
/// `state2` is copied.  The caller handles a NULL `state2` (returns `state1`).
pub fn numeric_avg_combine<'mcx>(
    mcx: Mcx<'mcx>,
    state1: Option<NumericAggState<'_>>,
    state2: &NumericAggState<'_>,
) -> PgResult<NumericAggState<'mcx>> {
    // Manually copy all fields from state2 to state1 when state1 is NULL.
    let mut state1 = match state1 {
        None => {
            let mut s1 = NumericAggState::new(mcx, false);
            s1.n = state2.n;
            s1.nan_count = state2.nan_count;
            s1.p_inf_count = state2.p_inf_count;
            s1.n_inf_count = state2.n_inf_count;
            s1.max_scale = state2.max_scale;
            s1.max_scale_count = state2.max_scale_count;

            s1.sum_x = accum_sum_copy(mcx, &state2.sum_x);

            return Ok(s1);
        }
        Some(s1) => clone_agg_state(mcx, &s1),
    };

    state1.n += state2.n;
    state1.nan_count += state2.nan_count;
    state1.p_inf_count += state2.p_inf_count;
    state1.n_inf_count += state2.n_inf_count;

    if state2.n > 0 {
        // These are currently only needed for moving aggregates, but do the
        // right thing anyway.
        if state2.max_scale > state1.max_scale {
            state1.max_scale = state2.max_scale;
            state1.max_scale_count = state2.max_scale_count;
        } else if state2.max_scale == state1.max_scale {
            state1.max_scale_count += state2.max_scale_count;
        }

        // Accumulate sums.
        accum_sum_combine(mcx, &mut state1.sum_x, &state2.sum_x)?;
    }

    Ok(state1)
}

/// `numeric_avg_serialize(state)` (numeric.c:5377): serialize an AVG/SUM
/// transition state (no sumX2) for parallel transfer.  Mirrors `pq_begintypsend`
/// / `pq_sendint64` / `numericvar_serialize` / `pq_endtypsend` over a `bytea`
/// payload (big-endian wire ints, varlena header).
pub fn numeric_avg_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = begin_typsend(mcx);

    // N
    send_int64(&mut buf, state.n);

    // sumX
    let tmp_var = accum_sum_final(mcx, &state.sum_x)?;
    io_serialize_var(&mut buf, &tmp_var);

    // maxScale
    send_int32(&mut buf, state.max_scale);

    // maxScaleCount
    send_int64(&mut buf, state.max_scale_count);

    // NaNcount
    send_int64(&mut buf, state.nan_count);

    // pInfcount
    send_int64(&mut buf, state.p_inf_count);

    // nInfcount
    send_int64(&mut buf, state.n_inf_count);

    end_typsend(&mut buf);
    Ok(buf)
}

/// `numeric_avg_deserialize(buf)` (numeric.c:5421): deserialize an AVG/SUM
/// transition state (no sumX2).  `buf` is the `bytea` payload (the wire body, no
/// varlena header — matching `VARDATA_ANY`).
pub fn numeric_avg_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
) -> PgResult<NumericAggState<'mcx>> {
    let mut pos = 0usize;

    let mut result = NumericAggState::new(mcx, false);

    // N
    result.n = get_int64(buf, &mut pos);

    // sumX
    let tmp_var = crate::io::numericvar_deserialize(mcx, buf, &mut pos)?;
    accum_sum_add(&mut result.sum_x, &tmp_var)?;

    // maxScale
    result.max_scale = get_int32(buf, &mut pos);

    // maxScaleCount
    result.max_scale_count = get_int64(buf, &mut pos);

    // NaNcount
    result.nan_count = get_int64(buf, &mut pos);

    // pInfcount
    result.p_inf_count = get_int64(buf, &mut pos);

    // nInfcount
    result.n_inf_count = get_int64(buf, &mut pos);

    Ok(result)
}

// ---------------------------------------------------------------------------
// Int128AggState fast path (numeric.c do_int128_accum + int*_accum + the
// numeric_poly_* finals).
// ---------------------------------------------------------------------------

/// `makeInt128AggState(fcinfo, calcSumX2)` (numeric.c:5599): a fresh, all-zero
/// 128-bit transition state.
pub fn make_int128_agg_state(calc_sum_x2: bool) -> Int128AggState {
    Int128AggState {
        calc_sum_x2,
        n: 0,
        sum_x: 0,
        sum_x2: 0,
    }
}

/// `do_int128_accum(state, newval)` (numeric.c:5637): accumulate a new input.
pub fn do_int128_accum(state: &mut Int128AggState, newval: i128) {
    if state.calc_sum_x2 {
        state.sum_x2 += newval * newval;
    }

    state.sum_x += newval;
    state.n += 1;
}

/// `do_int128_discard(state, newval)` (numeric.c:5650): remove an input.
pub fn do_int128_discard(state: &mut Int128AggState, newval: i128) {
    if state.calc_sum_x2 {
        state.sum_x2 -= newval * newval;
    }

    state.sum_x -= newval;
    state.n -= 1;
}

/// `numeric_poly_combine(state1, state2)` (numeric.c:5179): combine two 128-bit
/// poly transition states.  On `HAVE_INT128` the sums are plain int128 adds.
/// When `state1` is `None` (the C NULL), `state2` is copied; the caller handles
/// a NULL `state2` (returns `state1`).
pub fn numeric_poly_combine(
    state1: Option<Int128AggState>,
    state2: &Int128AggState,
) -> Int128AggState {
    let mut state1 = match state1 {
        None => {
            // makePolyNumAggState(fcinfo, true); copy N + both sums.
            let mut s1 = make_int128_agg_state(true);
            s1.n = state2.n;
            s1.sum_x = state2.sum_x;
            s1.sum_x2 = state2.sum_x2;
            return s1;
        }
        Some(s1) => s1,
    };

    if state2.n > 0 {
        state1.n += state2.n;
        state1.sum_x += state2.sum_x;
        state1.sum_x2 += state2.sum_x2;
    }
    state1
}

/// `numeric_poly_serialize(state)` (numeric.c:5247): serialize a 128-bit poly
/// transition state for parallel transfer.  On `HAVE_INT128` the int128 sums are
/// converted to `NumericVar` so the wire format is platform-independent.
pub fn numeric_poly_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    state: &Int128AggState,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = begin_typsend(mcx);

    // N
    send_int64(&mut buf, state.n);

    // sumX
    let tmp_var = int128_to_numericvar(mcx, state.sum_x)?;
    io_serialize_var(&mut buf, &tmp_var);

    // sumX2
    let tmp_var = int128_to_numericvar(mcx, state.sum_x2)?;
    io_serialize_var(&mut buf, &tmp_var);

    end_typsend(&mut buf);
    Ok(buf)
}

/// `numeric_poly_deserialize(buf)` (numeric.c:5310): deserialize a 128-bit poly
/// transition state.  `buf` is the `bytea` payload (the wire body, no varlena
/// header — matching `VARDATA_ANY`).  On `HAVE_INT128` the `NumericVar` sums are
/// converted back to int128.
pub fn numeric_poly_deserialize<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<Int128AggState> {
    let mut pos = 0usize;

    // makePolyNumAggStateCurrentContext(false).
    let mut result = make_int128_agg_state(false);

    // N
    result.n = get_int64(buf, &mut pos);

    // sumX
    let tmp_var = crate::io::numericvar_deserialize(mcx, buf, &mut pos)?;
    result.sum_x = convert::numericvar_to_int128(&tmp_var)?.unwrap_or(0);

    // sumX2
    let tmp_var = crate::io::numericvar_deserialize(mcx, buf, &mut pos)?;
    result.sum_x2 = convert::numericvar_to_int128(&tmp_var)?.unwrap_or(0);

    Ok(result)
}

/// `int8_avg_combine(state1, state2)` (numeric.c:5938): combine two 128-bit poly
/// transition states for aggregates which don't require sumX2 (AVG(int8)).  Same
/// shape as [`numeric_poly_combine`] but the fresh state is created with
/// `calc_sum_x2 = false` and only N + sumX are copied/accumulated.
pub fn int8_avg_combine(
    state1: Option<Int128AggState>,
    state2: &Int128AggState,
) -> Int128AggState {
    let mut state1 = match state1 {
        None => {
            // makePolyNumAggState(fcinfo, false); copy N + sumX only.
            let mut s1 = make_int128_agg_state(false);
            s1.n = state2.n;
            s1.sum_x = state2.sum_x;
            return s1;
        }
        Some(s1) => s1,
    };

    if state2.n > 0 {
        state1.n += state2.n;
        state1.sum_x += state2.sum_x;
    }
    state1
}

/// `int8_avg_serialize(state)` (numeric.c:5998): serialize a 128-bit poly
/// transition state for AVG(int8) parallel transfer.  Like
/// [`numeric_poly_serialize`] but without sumX2 (only N + sumX).
pub fn int8_avg_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    state: &Int128AggState,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = begin_typsend(mcx);

    // N
    send_int64(&mut buf, state.n);

    // sumX
    let tmp_var = int128_to_numericvar(mcx, state.sum_x)?;
    io_serialize_var(&mut buf, &tmp_var);

    end_typsend(&mut buf);
    Ok(buf)
}

/// `int8_avg_deserialize(buf)` (numeric.c:6047): deserialize a 128-bit poly
/// transition state for AVG(int8).  Like [`numeric_poly_deserialize`] but
/// without sumX2 (only N + sumX).
pub fn int8_avg_deserialize<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<Int128AggState> {
    let mut pos = 0usize;

    // makePolyNumAggStateCurrentContext(false).
    let mut result = make_int128_agg_state(false);

    // N
    result.n = get_int64(buf, &mut pos);

    // sumX
    let tmp_var = crate::io::numericvar_deserialize(mcx, buf, &mut pos)?;
    result.sum_x = convert::numericvar_to_int128(&tmp_var)?.unwrap_or(0);

    Ok(result)
}

/// `int2_accum(state, newval)` (numeric.c:5669): SUM/AVG(int2) transition on the
/// 128-bit fast path (`HAVE_INT128`).
pub fn int2_accum(state: Option<Int128AggState>, newval: i16) -> PgResult<Int128AggState> {
    let mut state = state.unwrap_or_else(|| make_int128_agg_state(true));
    do_int128_accum(&mut state, i128::from(newval));
    Ok(state)
}

/// `int4_accum(state, newval)` (numeric.c:5692): SUM/AVG(int4) transition on the
/// 128-bit fast path (`HAVE_INT128`).
pub fn int4_accum(state: Option<Int128AggState>, newval: i32) -> PgResult<Int128AggState> {
    let mut state = state.unwrap_or_else(|| make_int128_agg_state(true));
    do_int128_accum(&mut state, i128::from(newval));
    Ok(state)
}

/// `int8_accum(state, newval)` (numeric.c:5715): SUM/AVG(int8) transition, which
/// always uses the `NumericAggState` accumulators (not the 128-bit path).
pub fn int8_accum<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<NumericAggState<'_>>,
    newval: i64,
) -> PgResult<NumericAggState<'mcx>> {
    let mut state = match state {
        Some(s) => clone_agg_state(mcx, &s),
        None => NumericAggState::new(mcx, true),
    };
    let num = convert::int64_to_numeric(mcx, newval)?;
    do_numeric_accum(mcx, &mut state, &num)?;
    Ok(state)
}

/// `numeric_poly_sum(state)` (numeric.c:6189): SUM final over the 128-bit state
/// (`HAVE_INT128`); `int128_to_numericvar(sumX)` then `make_result`.
pub fn numeric_poly_sum<'mcx>(
    mcx: Mcx<'mcx>,
    state: &Int128AggState,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // If there were no non-null inputs, return NULL.
    if state.n == 0 {
        return Ok(None);
    }

    let result = int128_to_numericvar(mcx, state.sum_x)?;
    let res = convert::make_result(mcx, &result)?;

    Ok(Some(res))
}

/// `numeric_poly_avg(state)` (numeric.c:6219): AVG final over the 128-bit state.
pub fn numeric_poly_avg<'mcx>(
    mcx: Mcx<'mcx>,
    state: &Int128AggState,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if state.n == 0 {
        return Ok(None);
    }
    // sumX_var = int128_to_numericvar(state->sumX); numericvar_to_numeric(...)
    let sum_x_var = int128_to_numericvar(mcx, state.sum_x)?;
    let sum_x_datum = convert::make_result(mcx, &sum_x_var)?;
    let n_datum = convert::int64_to_numeric(mcx, state.n)?;
    Ok(Some(ops_sql::numeric_div(mcx, &sum_x_datum, &n_datum)?))
}

/// `numeric_poly_stddev_internal(state, variance, sample, is_null)`
/// (numeric.c:6135): build a `NumericAggState` from the 128-bit sums and run the
/// numeric variance/stddev. `Ok(None)` is the C `*is_null = true`.
pub fn numeric_poly_stddev_internal<'mcx>(
    mcx: Mcx<'mcx>,
    state: &Int128AggState,
    variance: bool,
    sample: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Initialize an empty agg state (calc_sum_x2 so accum_sum_final is valid).
    let mut numstate = NumericAggState::new(mcx, true);

    // numstate.N = state->N;
    numstate.n = state.n;

    // int128_to_numericvar(state->sumX, &tmp); accum_sum_add(&numstate.sumX, &tmp);
    let tmp_x = int128_to_numericvar(mcx, state.sum_x)?;
    accum_sum_add(&mut numstate.sum_x, &tmp_x)?;

    // int128_to_numericvar(state->sumX2, &tmp); accum_sum_add(&numstate.sumX2, &tmp);
    let tmp_x2 = int128_to_numericvar(mcx, state.sum_x2)?;
    accum_sum_add(&mut numstate.sum_x2, &tmp_x2)?;

    numeric_stddev_internal(mcx, &numstate, variance, sample)
}

/// `int128_to_numericvar(val, var)` (numeric.c:8414): convert a 128-bit integer
/// to a `NumericVar`.  Private aggregate-only helper (only the `numeric_poly_*`
/// finals need it).
fn int128_to_numericvar<'mcx>(mcx: Mcx<'mcx>, val: i128) -> PgResult<NumericVar<'mcx>> {
    // int128 can require at most 39 decimal digits; add one for safety.
    let cap = (40 / DEC_DIGITS) as usize;
    let mut var = kernel_var::alloc_var(mcx, cap)?;

    let mut uval: u128 = if val < 0 {
        var.sign = NumericSign::Neg;
        val.unsigned_abs()
    } else {
        var.sign = NumericSign::Pos;
        val as u128
    };
    var.dscale = 0;

    if val == 0 {
        // ndigits == 0, weight == 0: an empty logical run (headroom == len).
        var.headroom = var.digits.len();
        var.weight = 0;
        return Ok(var);
    }

    // alloc_var laid out `cap` logical digits with headroom 0; write the value
    // from the right end and then set `headroom` to expose only the trailing
    // `ndigits` significant digits (matching C's `digits = ptr; ndigits = ...`).
    let total = var.digits.len();
    let mut idx = total;
    let mut ndigits = 0usize;
    loop {
        idx -= 1;
        ndigits += 1;
        let newuval = uval / NBASE as u128;
        var.digits[idx] = (uval - newuval * NBASE as u128) as NumericDigit;
        uval = newuval;
        if uval == 0 {
            break;
        }
    }
    var.headroom = total - ndigits;
    var.weight = ndigits as i32 - 1;

    Ok(var)
}

// ---------------------------------------------------------------------------
// avg(int2)/avg(int4) and the moving-aggregate sum(int2)/sum(int4) (numeric.c
// Int8TransTypeData). The transition datatype is a two-element int8 array
// holding {count, sum}; finals int8_avg / int2int4_sum read it back.
//
// C reads the array image with `ARR_DATA_PTR` (validating it is a 1-D,
// non-null, exactly-2-element int8 array) and, inside an aggregate transition,
// scribbles on it in place (`AggCheckCallContext`) to save a palloc. This repo
// has no ambient context: each transition / combine takes the detoasted array
// image (`&[u8]`) and a target `Mcx<'mcx>`, and ALWAYS returns a freshly
// constructed array image. That is behavior-identical to C — the in-place leg
// is purely an allocation optimization over the same final {count, sum}
// values, which the executor's by-ref transValue reparent reintroduces when it
// owns the buffer. Mirrors the float8[] aggregate model in
// backend-utils-adt-float::aggregates.
// ---------------------------------------------------------------------------

use ::arrayfuncs::construct::construct_array;
use ::arrayfuncs::foundation::{
    self, arr_dim, arr_elemtype, arr_hasnull, arr_ndim, fetch_att, INT8OID,
};
use ::datum::Datum as ByValDatum;

/// INT8 array element storage attributes (`pg_type`): 8-byte, pass-by-value,
/// `'d'` alignment — matching `construct_array`'s INT8OID switch arm.
const INT8_ELMLEN: i32 = 8;
const INT8_ELMBYVAL: bool = foundation::FLOAT8PASSBYVAL;
const INT8_ELMALIGN: u8 = b'd';

/// The `Int8TransTypeData` two-element `{count, sum}` decoded from a transition
/// array image. C validates `ARR_HASNULL(transarray) || ARR_SIZE(transarray) !=
/// ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData)` and otherwise
/// `elog(ERROR, "expected 2-element int8 array")`.
fn check_int8_trans_array(transarray: &[u8]) -> PgResult<(i64, i64)> {
    use ::types_error::PgError;
    // ARR_HASNULL || ARR_SIZE != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData)
    // is equivalent (for a well-formed non-null array) to: 1-D, no nulls,
    // INT8OID element type, exactly 2 elements.
    if arr_ndim(transarray) != 1
        || arr_dim(transarray, 0) != 2
        || arr_hasnull(transarray)
        || arr_elemtype(transarray) != INT8OID
    {
        return Err(PgError::error("expected 2-element int8 array"));
    }

    let mut p = foundation::arr_data_ptr_off(transarray);
    let count = fetch_att(transarray, p, INT8_ELMBYVAL, INT8_ELMLEN).as_i64();
    p = foundation::att_addlength_pointer(p, INT8_ELMLEN, transarray, p);
    p = foundation::att_align_nominal(p, INT8_ELMALIGN);
    let sum = fetch_att(transarray, p, INT8_ELMBYVAL, INT8_ELMLEN).as_i64();
    Ok((count, sum))
}

/// Build a fresh `{count, sum}` int8[2] transition array image, charged to
/// `mcx` (the shared `PG_RETURN_ARRAYTYPE_P` tail —
/// `construct_array_builtin(transdatums, 2, INT8OID)`).
fn return_int8_trans_array<'mcx>(
    mcx: Mcx<'mcx>,
    count: i64,
    sum: i64,
) -> PgResult<PgVec<'mcx, u8>> {
    let elems = [ByValDatum::from_i64(count), ByValDatum::from_i64(sum)];
    construct_array(mcx, &elems, INT8OID, INT8_ELMLEN, INT8_ELMBYVAL, INT8_ELMALIGN)
}

/// `int2_avg_accum(transarray, int2)` (numeric.c:6776): AVG(int2) / moving
/// SUM(int2) transition. `count++; sum += newval`.
pub fn int2_avg_accum<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval: i16,
) -> PgResult<PgVec<'mcx, u8>> {
    let (count, sum) = check_int8_trans_array(transarray)?;
    return_int8_trans_array(mcx, count + 1, sum + i64::from(newval))
}

/// `int4_avg_accum(transarray, int4)` (numeric.c:6804): AVG(int4) / moving
/// SUM(int4) transition. `count++; sum += newval`.
pub fn int4_avg_accum<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let (count, sum) = check_int8_trans_array(transarray)?;
    return_int8_trans_array(mcx, count + 1, sum + i64::from(newval))
}

/// `int4_avg_combine(transarray1, transarray2)` (numeric.c:6833): combine two
/// `{count, sum}` transition states. Shared by avg(int2)/avg(int4).
pub fn int4_avg_combine<'mcx>(
    mcx: Mcx<'mcx>,
    transarray1: &[u8],
    transarray2: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let (count1, sum1) = check_int8_trans_array(transarray1)?;
    let (count2, sum2) = check_int8_trans_array(transarray2)?;
    return_int8_trans_array(mcx, count1 + count2, sum1 + sum2)
}

/// `int2_avg_accum_inv(transarray, int2)` (numeric.c:6863): moving-aggregate
/// inverse transition for AVG(int2)/SUM(int2). `count--; sum -= newval`.
pub fn int2_avg_accum_inv<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval: i16,
) -> PgResult<PgVec<'mcx, u8>> {
    let (count, sum) = check_int8_trans_array(transarray)?;
    return_int8_trans_array(mcx, count - 1, sum - i64::from(newval))
}

/// `int4_avg_accum_inv(transarray, int4)` (numeric.c:6891): moving-aggregate
/// inverse transition for AVG(int4)/SUM(int4). `count--; sum -= newval`.
pub fn int4_avg_accum_inv<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let (count, sum) = check_int8_trans_array(transarray)?;
    return_int8_trans_array(mcx, count - 1, sum - i64::from(newval))
}

/// `int8_avg(transarray)` (numeric.c:6919): AVG(int2)/AVG(int4) final.
/// `Ok(None)` is `PG_RETURN_NULL()` (SQL AVG of no values is NULL).
pub fn int8_avg<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let (count, sum) = check_int8_trans_array(transarray)?;

    // SQL defines AVG of no values to be NULL.
    if count == 0 {
        return Ok(None);
    }

    // DirectFunctionCall2(numeric_div, int64_to_numeric(sum), int64_to_numeric(count))
    let countd = convert::int64_to_numeric(mcx, count)?;
    let sumd = convert::int64_to_numeric(mcx, sum)?;
    Ok(Some(ops_sql::numeric_div(mcx, sumd.as_slice(), countd.as_slice())?))
}

/// `int2int4_sum(transarray)` (numeric.c:6946): SUM(int2)/SUM(int4) final in
/// moving-aggregate mode (both return int8). `Ok(None)` is `PG_RETURN_NULL()`.
/// Returns the by-value `int8` sum.
pub fn int2int4_sum(transarray: &[u8]) -> PgResult<Option<i64>> {
    let (count, sum) = check_int8_trans_array(transarray)?;

    // SQL defines SUM of no values to be NULL.
    if count == 0 {
        return Ok(None);
    }

    Ok(Some(sum))
}

// ---------------------------------------------------------------------------
// Sort-support (numeric.c numeric_sortsupport/abbrev_*). Node registration +
// HyperLogLog estimator are genuine externals behind sort-support seams.
// ---------------------------------------------------------------------------

/// `numeric_abbrev_convert(original)` (numeric.c:2171): produce the abbreviated
/// key for an on-disk numeric value, updating the [`NumericSortSupport`]
/// estimator state.  `mcx` is needed to decode finite values to a `NumericVar`
/// (the C `PG_DETOAST_DATUM_PACKED` + short-varlena buffer reuse is the
/// fmgr/toast deferral surface and is elided here).
pub fn numeric_abbrev_convert<'mcx>(
    mcx: Mcx<'mcx>,
    original: &[u8],
    nss: &mut NumericSortSupportState<'mcx>,
) -> PgResult<i64> {
    nss.payload.input_count += 1;

    if numeric_is_special(original) {
        let result = if numeric_is_pinf(original) {
            NUMERIC_ABBREV_PINF
        } else if numeric_is_ninf(original) {
            NUMERIC_ABBREV_NINF
        } else {
            NUMERIC_ABBREV_NAN
        };
        Ok(result)
    } else {
        let var = convert::set_var_from_num(mcx, original)?;
        numeric_abbrev_convert_var(&var, nss)
    }
}

/// `numeric_abbrev_convert_var(var, nss)` (numeric.c:2384, the 64-bit variant):
/// pack a finite `NumericVar` into its 64-bit abbreviated key (negated, with
/// excess-44 weight in the top bits).  `nss.payload.estimating` controls HLL
/// updates; the HyperLogLog accumulation runs directly on the counter held by
/// value in the sort state (mirrors varlena's `varstr_abbrev_convert`).
fn numeric_abbrev_convert_var<'mcx>(
    var: &NumericVar<'_>,
    nss: &mut NumericSortSupportState<'mcx>,
) -> PgResult<i64> {
    let ndigits = var.ndigits() as i32;
    let weight = var.weight;
    let mut result: i64;

    if ndigits == 0 || weight < -44 {
        result = 0;
    } else if weight > 83 {
        result = i64::MAX;
    } else {
        let digits = var.logical_digits();
        result = (i64::from(weight) + 44) << 56;

        // Mirror C's fall-through `switch (ndigits)`: pack the first up-to-4
        // digit words into 14-bit slots.
        match ndigits {
            1 => {
                result |= i64::from(digits[0]) << 42;
            }
            2 => {
                result |= i64::from(digits[1]) << 28;
                result |= i64::from(digits[0]) << 42;
            }
            3 => {
                result |= i64::from(digits[2]) << 14;
                result |= i64::from(digits[1]) << 28;
                result |= i64::from(digits[0]) << 42;
            }
            _ => {
                // default: ndigits >= 4
                result |= i64::from(digits[3]);
                result |= i64::from(digits[2]) << 14;
                result |= i64::from(digits[1]) << 28;
                result |= i64::from(digits[0]) << 42;
            }
        }
    }

    // The abbrev is negated relative to the original.
    if var.sign == NumericSign::Pos {
        result = result.wrapping_neg();
    }

    if nss.payload.estimating {
        // C: uint32 tmp = (uint32) result ^ (uint32) ((uint64) result >> 32);
        //    addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
        // `hash_uint32(k)` is `hash_bytes_uint32(k)` (common/hashfn.h:42); the
        // HLL accumulation runs directly on the by-value counter.
        let tmp = (result as u32) ^ (((result as u64) >> 32) as u32);
        let hash = hashfn::hash_bytes_uint32(tmp);
        hll::addHyperLogLog(&mut nss.abbr_card, hash);
    }

    Ok(result)
}

/// `numeric_fast_cmp(x, y)` (numeric.c:2300): the full comparator used by
/// sort-support — `cmp_numerics(x, y)`.  The C detoast is the fmgr/toast
/// deferral surface; this takes the whole on-disk byte images and compares via
/// the byte-image comparison core.
pub fn numeric_fast_cmp(a: &[u8], b: &[u8]) -> i32 {
    match ops_sql::numeric_cmp(a, b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// `numeric_cmp_abbrev(x, y)` (numeric.c:2322): compare two abbreviated keys.
/// NOTE WELL: intentionally backwards, because the abbreviation is negated
/// relative to the original value (to handle NaN/infinity cases). Pure i64
/// compare; infallible.
pub fn numeric_cmp_abbrev(x: i64, y: i64) -> i32 {
    if x < y {
        1
    } else if x > y {
        -1
    } else {
        0
    }
}

/// `numeric_abbrev_abort(memtupcount, ssup)` (numeric.c:2233): decide whether
/// to abort abbreviation. The HyperLogLog cardinality read runs directly on the
/// counter held by value in the sort state; the threshold logic and the
/// `estimating` toggle are in-crate. Returns `true` if abbreviation should be
/// aborted. Pure decision; never ereports.
///
/// `trace_sort` LOG `elog`s in the C are diagnostics with no SQL-visible
/// effect and are elided.
pub fn numeric_abbrev_abort(memtupcount: i32, nss: &mut NumericSortSupportState<'_>) -> bool {
    if memtupcount < 10000 || nss.payload.input_count < 10000 || !nss.payload.estimating {
        return false;
    }

    let abbr_card = hll::estimateHyperLogLog(&nss.abbr_card);

    // If we have >100k distinct values, stop even counting at that point.
    if abbr_card > 100000.0 {
        nss.payload.estimating = false;
        return false;
    }

    // Target minimum cardinality is 1 per ~10k of non-null inputs, with a 0.5
    // row fudge factor.
    if abbr_card < nss.payload.input_count as f64 / 10000.0 + 0.5 {
        return true;
    }

    false
}

// ---------------------------------------------------------------------------
// numeric_sortsupport (numeric.c:2130). Sets up the abbreviated-key sort.
//
// The comparator/abbrev function *bodies* are this unit's own logic (above:
// `numeric_fast_cmp`, `numeric_abbrev_convert`, `numeric_cmp_abbrev`,
// `numeric_abbrev_abort`). What `numeric_sortsupport` does is INSTALL them into
// the `SortSupport` node's function-pointer slots and, when abbreviation is
// enabled, allocate the `NumericSortSupport`/HyperLogLog buffer into the sort's
// `ssup_extra` in `ssup->ssup_cxt`.
//
// The `NumericSortSupport` payload (`input_count`/`estimating`) *and* its
// HyperLogLog cardinality counter (`abbr_card`) are this unit's own state — held
// by value in the [`NumericSortSupportState`] sort state below, exactly as C
// holds `NumericSortSupport` (with its inline `hyperLogLogState abbr_card`) in
// `ssup_extra`, and mirroring varlena's `VarStringSortSupport`. The HLL ops run
// directly on that counter (no seam). What still routes OUT to the (unported)
// tuplesort abbreviation machinery is purely the *function-pointer / ssup_extra
// install into the `SortSupportData` node* — minting the comparator/converter/
// abort tokens and storing the state — since the trimmed `SortSupportData`
// deliberately carries only `comparator`. Those installs panic until the
// tuplesort abbreviation owner lands.
// ---------------------------------------------------------------------------

use ::types_sortsupport::SortSupportData;

/// The `ssup->ssup_extra` payload for numeric abbreviated-key sorting
/// (numeric.c:340-347 `NumericSortSupport`): the in-crate computation fields
/// ([`NumericSortSupport`]) plus the HyperLogLog cardinality estimator held by
/// value (C `hyperLogLogState abbr_card`). The short-varlena reuse buffer (C
/// `nss->buf`) is the fmgr/toast deferral surface and is elided. Mirrors
/// varlena's `VarStringSortSupport`: the counter lives here and the
/// init/add/estimate ops are called directly from `backend-lib-hyperloglog`.
pub struct NumericSortSupportState<'mcx> {
    /// `input_count` / `estimating` — the trimmed [`NumericSortSupport`]
    /// payload.
    pub payload: NumericSortSupport,
    /// `hyperLogLogState abbr_card` — abbreviated-key cardinality counter.
    pub abbr_card: HyperLogLog<'mcx>,
}

seam_core::seam!(
    /// `ssup->comparator = numeric_fast_cmp;` — register this unit's full
    /// comparator (`numeric_fast_cmp`/`cmp_numerics`) as the sort comparator,
    /// minting the comparator token the sort engine interprets. Owner: the
    /// sortsupport comparator-resolution machinery.
    pub fn install_numeric_comparator(ssup: &mut SortSupportData<'_>)
);

seam_core::seam!(
    /// The abbreviation wiring of `numeric_sortsupport` when `ssup->abbreviate`:
    /// store the freshly-built [`NumericSortSupportState`] (payload seeded
    /// `input_count = 0`, `estimating = true`, plus `initHyperLogLog(&abbr_card,
    /// 10)`, all done in-crate by the caller) as `ssup->ssup_extra`, then set
    /// `abbrev_full_comparator = comparator; comparator = numeric_cmp_abbrev;
    /// abbrev_converter = numeric_abbrev_convert; abbrev_abort =
    /// numeric_abbrev_abort`. The `ssup_extra`/abbrev-slot fields live on the
    /// (unported) tuplesort abbreviation owner. Owner: the tuplesort
    /// abbreviation machinery.
    pub fn install_numeric_abbrev<'mcx>(
        ssup: &mut SortSupportData<'mcx>,
        nss: NumericSortSupportState<'mcx>,
    )
);

/// `numeric_sortsupport(PG_FUNCTION_ARGS)` (numeric.c:2130): set up sort support
/// for `numeric`, enabling the abbreviated-key optimization when requested.
///
/// `ssup` is the `SortSupport` node (C `(SortSupport) PG_GETARG_POINTER(0)`).
/// Returns `PG_RETURN_VOID()`.
pub fn numeric_sortsupport(ssup: &mut SortSupportData<'_>) -> PgResult<()> {
    // ssup->comparator = numeric_fast_cmp;
    install_numeric_comparator::call(ssup);

    if ssup.abbreviate {
        // oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);
        // nss = palloc(sizeof(NumericSortSupport));
        // nss->buf = palloc(VARATT_SHORT_MAX + VARHDRSZ + 1);  (toast surface; elided)
        // nss->input_count = 0; nss->estimating = true;
        // initHyperLogLog(&nss->abbr_card, 10);
        let nss = NumericSortSupportState {
            payload: NumericSortSupport {
                input_count: 0,
                estimating: true,
            },
            abbr_card: hll::initHyperLogLog(ssup.ssup_cxt, 10)?,
        };

        // ssup->ssup_extra = nss;
        // ssup->abbrev_full_comparator = ssup->comparator;
        // ssup->comparator = numeric_cmp_abbrev;
        // ssup->abbrev_converter = numeric_abbrev_convert;
        // ssup->abbrev_abort = numeric_abbrev_abort;
        // MemoryContextSwitchTo(oldcontext);
        install_numeric_abbrev::call(ssup, nss);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Hashing (numeric.c hash_numeric/hash_numeric_extended).
// ---------------------------------------------------------------------------

/// `hash_numeric(key)` (numeric.c:2816): hash a numeric value.  `num` is the
/// whole on-disk byte image.
pub fn hash_numeric(num: &[u8]) -> u32 {
    // If it's NaN or infinity, don't try to hash the rest of the fields.
    if numeric_is_special(num) {
        return 0;
    }

    let varsize = num.len();
    let nd = numeric_ndigits(num, varsize);
    let digits = numeric_digits(num);

    let mut weight = numeric_weight(num);
    let mut start_offset = 0usize;
    let mut end_offset = 0usize;

    // Omit any leading zeros from the input to the hash (paranoia: the numeric
    // implementation should already suppress them).  Offsets are in units of
    // NumericDigits, not bytes.
    for i in 0..nd {
        if numeric_digit_at(digits, i) != 0 {
            break;
        }
        start_offset += 1;
        // The weight is effectively the # of digits before the decimal point,
        // so decrement it for each leading zero we skip.
        weight -= 1;
    }

    // If there are no non-zero digits, the value is zero regardless of other
    // fields.
    if nd == start_offset {
        return u32::MAX; // PG_RETURN_UINT32(-1)
    }

    for i in (0..nd).rev() {
        if numeric_digit_at(digits, i) != 0 {
            break;
        }
        end_offset += 1;
    }

    debug_assert!(start_offset + end_offset < nd);

    // We don't hash on scale (two numerics can compare equal with different
    // scales) nor sign.
    let hash_len = nd - start_offset - end_offset;
    let byte_start = start_offset * core::mem::size_of::<NumericDigit>();
    let byte_len = hash_len * core::mem::size_of::<NumericDigit>();
    let digit_hash = hashfn::hash_bytes(&digits[byte_start..byte_start + byte_len]);

    // Mix in the weight, via XOR (C XORs a Datum with an int `weight`; only the
    // low 32 bits participate in the returned uint32).
    digit_hash ^ (weight as u32)
}

/// `hash_numeric_extended(key, seed)` (numeric.c:2896): 64-bit seeded hash.
pub fn hash_numeric_extended(num: &[u8], seed: u64) -> u64 {
    // If it's NaN or infinity, don't try to hash the rest of the fields.
    if numeric_is_special(num) {
        return seed;
    }

    let varsize = num.len();
    let nd = numeric_ndigits(num, varsize);
    let digits = numeric_digits(num);

    let mut weight = numeric_weight(num);
    let mut start_offset = 0usize;
    let mut end_offset = 0usize;

    for i in 0..nd {
        if numeric_digit_at(digits, i) != 0 {
            break;
        }
        start_offset += 1;
        weight -= 1;
    }

    if nd == start_offset {
        return seed.wrapping_sub(1); // PG_RETURN_UINT64(seed - 1)
    }

    for i in (0..nd).rev() {
        if numeric_digit_at(digits, i) != 0 {
            break;
        }
        end_offset += 1;
    }

    debug_assert!(start_offset + end_offset < nd);

    let hash_len = nd - start_offset - end_offset;
    let byte_start = start_offset * core::mem::size_of::<NumericDigit>();
    let byte_len = hash_len * core::mem::size_of::<NumericDigit>();
    let digit_hash = hashfn::hash_bytes_extended(&digits[byte_start..byte_start + byte_len], seed);

    // result = digit_hash ^ weight (the C XORs a uint64 with int `weight`,
    // sign-extended to 64 bits).
    digit_hash ^ (weight as i64 as u64)
}

// ---------------------------------------------------------------------------
// Internal helpers.
// ---------------------------------------------------------------------------

/// Allocate a zeroed, charged `PgVec<'mcx, i32>` of length `n` (the
/// accumulator limb buffer; the C `palloc0`).  OOM-safe: validated bound +
/// fallible reserve, surfacing OOM as the `numeric value out of range` error.
fn alloc_zeroed_limbs<'mcx>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, i32>> {
    use types_error::{PgError, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
    let mut v = ::mcx::vec_with_capacity_in::<i32>(mcx, n).map_err(|_| {
        PgError::error("value overflows numeric format")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
    })?;
    v.resize(n, 0);
    Ok(v)
}

/// Build the on-disk image of a special value (`NaN`/`+Inf`/`-Inf`) via
/// `make_result(&const_*)`.
fn make_special<'mcx>(mcx: Mcx<'mcx>, sign: NumericSign) -> PgResult<PgVec<'mcx, u8>> {
    let v = NumericVar::special(mcx, sign);
    convert::make_result(mcx, &v)
}

/// Deep-copy a [`NumericAggState`] into a fresh state in `mcx` (the analogue of
/// reusing the C state pointer in the aggregate context).
fn clone_agg_state<'mcx>(mcx: Mcx<'mcx>, src: &NumericAggState<'_>) -> NumericAggState<'mcx> {
    NumericAggState {
        calc_sum_x2: src.calc_sum_x2,
        n: src.n,
        sum_x: accum_sum_copy(mcx, &src.sum_x),
        sum_x2: accum_sum_copy(mcx, &src.sum_x2),
        max_scale: src.max_scale,
        max_scale_count: src.max_scale_count,
        nan_count: src.nan_count,
        p_inf_count: src.p_inf_count,
        n_inf_count: src.n_inf_count,
    }
}

// --- pq StringInfo-style wire helpers (big-endian), bytea framing -----------
//
// numeric_serialize uses pq_begintypsend / pq_sendint{32,64} / pq_endtypsend.
// pq_begintypsend reserves VARHDRSZ (4) leading bytes; pq_endtypsend writes the
// total varlena length there with SET_VARSIZE.  We reproduce that framing so
// the serialized state is the full on-disk `bytea` image.

use ::datum::VARHDRSZ;

fn begin_typsend<'mcx>(mcx: Mcx<'mcx>) -> PgVec<'mcx, u8> {
    let mut buf = PgVec::new_in(mcx);
    // Reserve the varlena length word (filled in by end_typsend).
    buf.extend_from_slice(&[0u8; VARHDRSZ]);
    buf
}

fn end_typsend(buf: &mut PgVec<'_, u8>) {
    // SET_VARSIZE: store the total length (including the 4-byte header) in the
    // first word, in the 4-byte-aligned ("long") varlena format (native-endian
    // length word — matches the in-memory varlena representation read back via
    // VARSIZE / VARDATA_ANY).
    let total = buf.len() as u32;
    let hdr = (total << 2).to_ne_bytes();
    buf[0..VARHDRSZ].copy_from_slice(&hdr);
}

fn send_int32(buf: &mut PgVec<'_, u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn send_int64(buf: &mut PgVec<'_, u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn io_serialize_var(buf: &mut PgVec<'_, u8>, var: &NumericVar<'_>) {
    crate::io::numericvar_serialize(buf, var);
}

fn get_int32(buf: &[u8], pos: &mut usize) -> i32 {
    let bytes: [u8; 4] = buf[*pos..*pos + 4].try_into().unwrap();
    *pos += 4;
    i32::from_be_bytes(bytes)
}

fn get_int64(buf: &[u8], pos: &mut usize) -> i64 {
    let bytes: [u8; 8] = buf[*pos..*pos + 8].try_into().unwrap();
    *pos += 8;
    i64::from_be_bytes(bytes)
}
