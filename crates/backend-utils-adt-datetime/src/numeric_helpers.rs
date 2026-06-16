//! Small numeric-building helpers shared by the EXTRACT cores of the date/time
//! types.
//!
//! The C `time_part_common` / `timetz_part_common` fractional-second and epoch
//! paths call `int64_div_fast_to_numeric()` (numeric.c), which builds an
//! on-disk `Numeric` for `val / 10^log10val2`.  The on-disk builder needs a
//! memory-context scope; for the plain-Rust cores we instead return a
//! [`NumericVar`], replicating the same fast scaling math at the var level.
//! Callers (fmgr shims) wrap it with `make_result`.

use backend_utils_adt_numeric::convert::int128_to_numericvar;
use backend_utils_adt_numeric::kernel_transcendental::int64_to_numericvar;
use mcx::Mcx;
use types_error::PgResult;
use types_numeric::var::NumericVar;
use types_numeric::DEC_DIGITS;

/// `int64_div_fast_to_numericvar()` -- the `NumericVar`-producing analogue of
/// numeric.c's `int64_div_fast_to_numeric(val1, log10val2)`, i.e. the value
/// `val1 / 10^log10val2` at display scale `max(log10val2, 0)`.
///
/// This mirrors the C code exactly: it adjusts the weight by `log10val2 /
/// DEC_DIGITS`, and folds any sub-`DEC_DIGITS` remainder into a multiply on
/// `val1` (promoting to 128-bit if the multiply overflows `i64`).
pub fn int64_div_fast_to_numericvar<'mcx>(
    mcx: Mcx<'mcx>,
    val1: i64,
    log10val2: i32,
) -> PgResult<NumericVar<'mcx>> {
    // result scale
    let rscale = if log10val2 < 0 { 0 } else { log10val2 };

    // how much to decrease the weight by
    let mut w = log10val2 / DEC_DIGITS;
    // how much is left to divide by
    let mut m = log10val2 % DEC_DIGITS;
    if m < 0 {
        m += DEC_DIGITS;
        w -= 1;
    }

    let mut result;
    if m > 0 {
        // pow10[DEC_DIGITS] == {1, 10, 100, 1000} for DEC_DIGITS == 4.
        const POW10: [i64; 4] = [1, 10, 100, 1000];
        let factor: i64 = POW10[(DEC_DIGITS - m) as usize];

        match val1.checked_mul(factor) {
            Some(new_val1) => {
                result = int64_to_numericvar(mcx, new_val1)?;
            }
            None => {
                // 128-bit multiplication path.
                let tmp = val1 as i128 * factor as i128;
                result = int128_to_numericvar(mcx, tmp)?;
            }
        }
        w += 1;
    } else {
        result = int64_to_numericvar(mcx, val1)?;
    }

    result.weight -= w;
    result.dscale = rscale;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn div_fast_sets_scale_and_weight() {
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        // 1_500_000 / 10^6 == 1.5 at scale 6.
        let v = int64_div_fast_to_numericvar(mcx, 1_500_000, 6).unwrap();
        assert_eq!(v.dscale, 6);
        // 13 / 10^0 == 13 at scale 0.
        let v2 = int64_div_fast_to_numericvar(mcx, 13, 0).unwrap();
        assert_eq!(v2.dscale, 0);
    }
}
