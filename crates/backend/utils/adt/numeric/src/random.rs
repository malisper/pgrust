use ::pg_prng::PgPrng;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::arith::{add_var, cmp_var, sub_var};
use crate::var::{make_result, NumericImage, NumericVar, VarView};
use crate::{Num, NumericDigit, DEC_DIGITS, NBASE, NUMERIC_NEG, NUMERIC_POS};

#[cold]
#[inline(never)]
fn bound_error(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

fn check_bound(num: Num<'_>, which: &'static str) -> PgResult<()> {
    if num.is_special() {
        if num.is_nan() {
            return Err(if which == "lower" {
                bound_error("lower bound cannot be NaN")
            } else {
                bound_error("upper bound cannot be NaN")
            }
            .into());
        }
        return Err(if which == "lower" {
            bound_error("lower bound cannot be infinity")
        } else {
            bound_error("upper bound cannot be infinity")
        }
        .into());
    }
    Ok(())
}

pub fn random_numeric(
    state: &mut PgPrng,
    rmin: Num<'_>,
    rmax: Num<'_>,
) -> PgResult<NumericImage> {
    check_bound(rmin, "lower")?;
    check_bound(rmax, "upper")?;

    let mut result = NumericVar::new();
    random_var(state, rmin.view(), rmax.view(), &mut result)?;
    make_result(result.view())
}

fn random_var(
    state: &mut PgPrng,
    rmin: VarView<'_>,
    rmax: VarView<'_>,
    result: &mut NumericVar,
) -> PgResult<()> {
    let rscale = rmin.dscale.max(rmax.dscale);

    let mut rlen = NumericVar::new();
    sub_var(rmax, rmin, &mut rlen);

    if rlen.sign == NUMERIC_NEG {
        return Err(bound_error("lower bound must be less than or equal to upper bound").into());
    }

    if rlen.ndigits == 0 {
        result.set_from_view(rmin);
        result.dscale = rscale;
        return Ok(());
    }

    let res_ndigits = rlen.weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;

    let n = ((rscale + DEC_DIGITS - 1) / DEC_DIGITS) * DEC_DIGITS - rscale;
    let mut pow10: u64 = 1;
    for _ in 0..n {
        pow10 *= 10;
    }

    // rlen64 = first up-to-4 NBASE digits of rlen; the value chosen from
    // [0, rlen2 >= rlen] is rejected and redrawn when > rlen (P < 1e-13).
    let rlen_digits = rlen.digits();
    let mut rlen64 = rlen_digits[0] as u64;
    let mut rlen64_ndigits: i32 = 1;
    while rlen64_ndigits < res_ndigits && rlen64_ndigits < 4 {
        rlen64 *= NBASE as u64;
        if rlen64_ndigits < rlen.ndigits {
            rlen64 += rlen_digits[rlen64_ndigits as usize] as u64;
        }
        rlen64_ndigits += 1;
    }

    loop {
        result.alloc(res_ndigits);
        result.sign = NUMERIC_POS;
        result.weight = rlen.weight;
        result.dscale = rscale;
        let res_digits = result.digits_mut();

        let mut rand = if rlen64_ndigits == res_ndigits && pow10 != 1 {
            state.u64_range(0, rlen64 / pow10) * pow10
        } else {
            state.u64_range(0, rlen64)
        };

        for i in (0..rlen64_ndigits as usize).rev() {
            res_digits[i] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
        }

        let mut whole_ndigits = res_ndigits as usize;
        if pow10 != 1 {
            whole_ndigits -= 1;
        }

        let mut i = rlen64_ndigits as usize;
        while i + 3 < whole_ndigits {
            let mut rand =
                state.u64_range(0, (NBASE as u64) * (NBASE as u64) * (NBASE as u64) * (NBASE as u64) - 1);
            res_digits[i] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            res_digits[i + 1] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            res_digits[i + 2] = (rand % NBASE as u64) as NumericDigit;
            rand /= NBASE as u64;
            res_digits[i + 3] = rand as NumericDigit;
            i += 4;
        }

        while i < whole_ndigits {
            res_digits[i] = state.u64_range(0, NBASE as u64 - 1) as NumericDigit;
            i += 1;
        }

        if i < res_ndigits as usize {
            res_digits[i] = (state.u64_range(0, NBASE as u64 / pow10 - 1) * pow10) as NumericDigit;
        }

        result.strip();

        if cmp_var(result.view(), rlen.view()) <= 0 {
            break;
        }
    }

    let sum = {
        let mut sum = NumericVar::new();
        add_var(result.view(), rmin, &mut sum);
        sum
    };
    *result = sum;
    Ok(())
}
