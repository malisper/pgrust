use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::arith::{add_var, cmp_var, div_var, sub_var};
use crate::var::{make_result, NumericImage, NumericVar, CONST_ONE, CONST_ZERO};
use crate::{Num, NUMERIC_NEG, NUMERIC_POS};

fn invalid_param(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn reject_special(num: Num<'_>, nan_msg: &'static str, inf_msg: &'static str) -> PgResult<()> {
    if num.is_special() {
        return Err(if num.is_nan() {
            invalid_param(nan_msg)
        } else {
            invalid_param(inf_msg)
        });
    }
    Ok(())
}

pub struct GenerateSeriesNumeric {
    current: NumericVar,
    stop: NumericVar,
    step: NumericVar,
}

impl GenerateSeriesNumeric {
    pub fn new(start: Num<'_>, stop: Num<'_>, step: Option<Num<'_>>) -> PgResult<Self> {
        reject_special(start, "start value cannot be NaN", "start value cannot be infinity")?;
        reject_special(stop, "stop value cannot be NaN", "stop value cannot be infinity")?;
        let steploc = match step {
            Some(s) => {
                reject_special(s, "step size cannot be NaN", "step size cannot be infinity")?;
                let v = NumericVar::from_view(s.view());
                if cmp_var(v.view(), CONST_ZERO) == 0 {
                    return Err(invalid_param("step size cannot equal zero"));
                }
                v
            }
            None => NumericVar::from_view(CONST_ONE),
        };
        Ok(GenerateSeriesNumeric {
            current: NumericVar::from_view(start.view()),
            stop: NumericVar::from_view(stop.view()),
            step: steploc,
        })
    }

    pub fn next(&mut self) -> PgResult<Option<NumericImage>> {
        let cmp = cmp_var(self.current.view(), self.stop.view());
        let more = (self.step.sign == NUMERIC_POS && cmp <= 0)
            || (self.step.sign == NUMERIC_NEG && cmp >= 0);
        if !more {
            return Ok(None);
        }
        let result = make_result(self.current.view())?;
        let mut next = NumericVar::new();
        add_var(self.current.view(), self.step.view(), &mut next);
        self.current = next;
        Ok(Some(result))
    }
}

// C's generate_series_numeric_support rows arm: floor((stop-start)/step) + 1
// when step's sign matches stop-start, else zero rows.
pub fn generate_series_numeric_rows(
    start: Num<'_>,
    stop: Num<'_>,
    step: Option<Num<'_>>,
) -> PgResult<Option<f64>> {
    if start.is_special() || stop.is_special() {
        return Ok(None);
    }
    let stepv = match step {
        Some(s) => {
            if s.is_special() {
                return Ok(None);
            }
            NumericVar::from_view(s.view())
        }
        None => NumericVar::from_view(CONST_ONE),
    };
    if cmp_var(stepv.view(), CONST_ZERO) == 0 {
        return Ok(None);
    }
    let mut res = NumericVar::new();
    sub_var(stop.view(), start.view(), &mut res);
    if stepv.sign != res.sign {
        return Ok(Some(0.0));
    }
    if step.is_some() {
        let mut quo = NumericVar::new();
        div_var(res.view(), stepv.view(), &mut quo, 0, false, false)?;
        res = quo;
    } else {
        res.trunc(0);
    }
    Ok(Some(crate::math::var_to_f64(res.view()) + 1.0))
}
