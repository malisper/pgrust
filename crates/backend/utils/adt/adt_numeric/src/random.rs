//! Family: `random` — the numeric `random(rmin, rmax)` kernel
//! (`numeric.c` `random_numeric` / `random_var`).
//!
//! These are this unit's OWN logic: range-bound validation, the rejection
//! sampler that draws a uniform value in `[0, rmax - rmin]` NBASE-digit by
//! NBASE-digit and shifts it by `rmin`. The only external is the random source,
//! the real `prng` generator ([`prng::PgPrng`]) threaded in by value — the
//! C `pg_prng_state *state` from the SQL `random()` setup. `pg_prng_uint64_range`
//! is [`PgPrng::u64_range`].

use mcx::Mcx;
use prng::PgPrng;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use types_numeric::var::{NumericSign, NumericVar};
use types_numeric::{
    numeric_is_nan, numeric_is_special, NumericDigit, DEC_DIGITS, NBASE,
};

use mcx::PgVec;

use crate::convert::{make_result, set_var_from_num};
use crate::kernel_var::{add_var, alloc_var, cmp_var, set_var_from_var, strip_var, sub_var};

/// `random_numeric(state, rmin, rmax)` (numeric.c:4347): return a random numeric
/// value uniformly distributed in the inclusive range `[rmin, rmax]`.
///
/// `rmin`/`rmax` are on-disk `numeric` byte images (the SQL `random()` fmgr
/// wrapper detoasts them; that detoast is the fmgr/toast boundary, elided here).
/// `state` is the backend's `pg_prng_state` threaded in by value.
pub fn random_numeric<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut PgPrng,
    rmin: &[u8],
    rmax: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    // Range bounds must not be NaN/infinity.
    if numeric_is_special(rmin) {
        if numeric_is_nan(rmin) {
            return Err(PgError::error("lower bound cannot be NaN")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        } else {
            return Err(PgError::error("lower bound cannot be infinity")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }
    if numeric_is_special(rmax) {
        if numeric_is_nan(rmax) {
            return Err(PgError::error("upper bound cannot be NaN")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        } else {
            return Err(PgError::error("upper bound cannot be infinity")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }

    // Return a random value in the range [rmin, rmax].
    let rmin_var = set_var_from_num(mcx, rmin)?;
    let rmax_var = set_var_from_num(mcx, rmax)?;

    let result = random_var(mcx, state, &rmin_var, &rmax_var)?;

    make_result(mcx, &result)
}

/// `random_var(state, rmin, rmax, result)` (numeric.c:11681): the rejection
/// sampler. Selects a value uniformly from `[0, rlen = rmax - rmin]` and shifts
/// it by `rmin`. `rscale` is the larger of the two bounds' display scales.
pub fn random_var<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut PgPrng,
    rmin: &NumericVar<'_>,
    rmax: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let rscale = rmin.dscale.max(rmax.dscale);

    // Compute rlen = rmax - rmin and check the range bounds.
    let rlen = sub_var(mcx, rmax, rmin)?;

    if rlen.sign == NumericSign::Neg {
        return Err(
            PgError::error("lower bound must be less than or equal to upper bound")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    // Special case for an empty range.
    if rlen.ndigits() == 0 {
        let mut result = set_var_from_var(mcx, rmin)?;
        result.dscale = rscale;
        return Ok(result);
    }

    // Otherwise, select a random value in the range [0, rlen = rmax - rmin],
    // and shift it to the required range by adding rmin.

    // Required result digits.
    let res_ndigits = rlen.weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;

    // To get the required rscale, the final result digit must be a multiple of
    // pow10 = 10^n, where n = (-rscale) mod DEC_DIGITS.
    let n = ((rscale + DEC_DIGITS - 1) / DEC_DIGITS) * DEC_DIGITS - rscale;
    let mut pow10: i32 = 1;
    for _ in 0..n {
        pow10 *= 10;
    }

    // To choose a random value uniformly from the range [0, rlen], choose from
    // the slightly larger range [0, rlen2], where rlen2 is formed from rlen by
    // copying the first 4 NBASE digits and setting all remaining decimal digits
    // to "9". rlen64 is a 64-bit integer formed from the first 4 NBASE digits
    // copied from rlen.
    let rlen_digits = rlen.logical_digits();
    let rlen_ndigits = rlen.ndigits() as i32;
    let mut rlen64: u64 = rlen_digits[0] as u64;
    let mut rlen64_ndigits: i32 = 1;
    while rlen64_ndigits < res_ndigits && rlen64_ndigits < 4 {
        rlen64 *= NBASE as u64;
        if rlen64_ndigits < rlen_ndigits {
            rlen64 += rlen_digits[rlen64_ndigits as usize] as u64;
        }
        rlen64_ndigits += 1;
    }

    // Loop until we get a result <= rlen.
    let result = loop {
        // C re-allocs (and zeroes) the result buffer at the top of each
        // do-while iteration.
        let mut result = alloc_var(mcx, res_ndigits as usize)?;
        result.sign = NumericSign::Pos;
        result.weight = rlen.weight;
        result.dscale = rscale;
        // res_digits = result->digits; logical region is digits[headroom..].
        let base = result.headroom;

        // Set the first rlen64_ndigits using a random value in [0, rlen64].
        //
        // If this is the whole result, and rscale is not a multiple of
        // DEC_DIGITS (pow10 from above is not 1), then we need this to be a
        // multiple of pow10.
        let mut rand: u64 = if rlen64_ndigits == res_ndigits && pow10 != 1 {
            state.u64_range(0, rlen64 / pow10 as u64) * pow10 as u64
        } else {
            state.u64_range(0, rlen64)
        };

        let mut i = (rlen64_ndigits - 1) as i64;
        while i >= 0 {
            result.digits[base + i as usize] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            i -= 1;
        }

        // Set the remaining digits to random values in range [0, NBASE), noting
        // that the last digit needs to be a multiple of pow10.
        let mut whole_ndigits = res_ndigits;
        if pow10 != 1 {
            whole_ndigits -= 1;
        }

        // Set whole digits in groups of 4 for best performance.
        let mut i = rlen64_ndigits;
        while i < whole_ndigits - 3 {
            rand = state.u64_range(0, (NBASE as u64) * (NBASE as u64) * (NBASE as u64) * (NBASE as u64) - 1);
            result.digits[base + i as usize] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            i += 1;
            result.digits[base + i as usize] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            i += 1;
            result.digits[base + i as usize] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            i += 1;
            result.digits[base + i as usize] = rand as NumericDigit;
            i += 1;
        }

        // Remaining whole digits.
        while i < whole_ndigits {
            rand = state.u64_range(0, NBASE as u64 - 1);
            result.digits[base + i as usize] = rand as NumericDigit;
            i += 1;
        }

        // Final partial digit (multiple of pow10).
        if i < res_ndigits {
            rand = state.u64_range(0, NBASE as u64 / pow10 as u64 - 1) * pow10 as u64;
            result.digits[base + i as usize] = rand as NumericDigit;
        }

        // Remove leading/trailing zeroes.
        strip_var(&mut result);

        // If result > rlen, try again; otherwise we are done.
        if cmp_var(&result, &rlen) <= core::cmp::Ordering::Equal {
            break result;
        }
    };

    // Offset the result to the required range.
    let result = add_var(mcx, &result, rmin)?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{numeric_in, numeric_out};
    use mcx::MemoryContext;

    fn rnum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgVec<'mcx, u8> {
        numeric_in(mcx, s, -1).expect("parse numeric literal")
    }

    #[test]
    fn random_numeric_stays_in_range_and_keeps_scale() {
        let ctx = MemoryContext::new("random-test");
        let mcx = ctx.mcx();
        let mut prng = PgPrng::seeded(12345);
        let rmin = rnum(mcx, "1.00");
        let rmax = rnum(mcx, "10.00");
        for _ in 0..200 {
            let r =
                random_numeric(mcx, &mut prng, &rmin, &rmax).expect("random_numeric in valid range");
            // In range [1.00, 10.00].
            assert!(crate::ops_sql::numeric_cmp(&r, &rmin) != core::cmp::Ordering::Less);
            assert!(crate::ops_sql::numeric_cmp(&r, &rmax) != core::cmp::Ordering::Greater);
            // dscale == 2 (max of the two bounds' scales).
            let s = numeric_out(mcx, &r).expect("numeric_out");
            let frac = s.split('.').nth(1).map_or(0, |f| f.len());
            assert_eq!(frac, 2, "value {s} should carry 2 fractional digits");
        }
    }

    #[test]
    fn random_numeric_equal_bounds_returns_bound() {
        let ctx = MemoryContext::new("random-test");
        let mcx = ctx.mcx();
        let mut prng = PgPrng::seeded(7);
        let b = rnum(mcx, "42.5");
        let r = random_numeric(mcx, &mut prng, &b, &b).expect("equal bounds");
        assert_eq!(numeric_out(mcx, &r).unwrap(), "42.5");
    }

    #[test]
    fn random_numeric_rejects_inverted_range() {
        let ctx = MemoryContext::new("random-test");
        let mcx = ctx.mcx();
        let mut prng = PgPrng::seeded(3);
        let lo = rnum(mcx, "10");
        let hi = rnum(mcx, "1");
        assert!(random_numeric(mcx, &mut prng, &lo, &hi).is_err());
    }

    #[test]
    fn random_numeric_rejects_nan_and_inf_bounds() {
        let ctx = MemoryContext::new("random-test");
        let mcx = ctx.mcx();
        let mut prng = PgPrng::seeded(9);
        let ok = rnum(mcx, "1");
        let nan = rnum(mcx, "NaN");
        let inf = rnum(mcx, "Infinity");
        assert!(random_numeric(mcx, &mut prng, &nan, &ok).is_err());
        assert!(random_numeric(mcx, &mut prng, &ok, &inf).is_err());
    }
}
