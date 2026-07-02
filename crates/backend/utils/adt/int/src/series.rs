use alloc::boxed::Box;

use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::pg_add_s32_overflow;

// generate_series_step_int4's cross-call state (`generate_series_fctx`); the
// funcapi SRF frame that owns it is backend-utils-fmgr-funcapi's unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerateSeriesInt4 {
    pub current: i32,
    pub finish: i32,
    pub step: i32,
}

#[cold]
#[inline(never)]
pub(crate) fn zero_step() -> Box<PgError> {
    Box::new(
        PgError::error("step size cannot equal zero")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

impl GenerateSeriesInt4 {
    pub fn new(start: i32, finish: i32, step: i32) -> PgResult<Self> {
        if step == 0 {
            return Err(zero_step());
        }
        Ok(GenerateSeriesInt4 {
            current: start,
            finish,
            step,
        })
    }

    // SRF per-call body: emit current and advance; a next-value overflow zeroes
    // the step so the emission just made is the final one.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<i32> {
        let result = self.current;
        if (self.step > 0 && self.current <= self.finish)
            || (self.step < 0 && self.current >= self.finish)
        {
            let mut nextval = 0;
            if pg_add_s32_overflow(self.current, self.step, &mut nextval) {
                self.step = 0;
            } else {
                self.current = nextval;
            }
            Some(result)
        } else {
            None
        }
    }
}

// generate_series_int4_support's SupportRequestRows estimate; Const/NULL
// classification of the argument nodes is the planner's job.
pub fn generate_series_int4_rows(start: f64, finish: f64, step: f64) -> Option<f64> {
    if step != 0.0 {
        Some(((finish - start + step) / step).floor())
    } else {
        None
    }
}
