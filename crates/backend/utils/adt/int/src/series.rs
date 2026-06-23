//! `int.c`'s set-returning series generator and its planner-support row
//! estimate, ported as pure logic.
//!
//!   * `generate_series_int4` / `generate_series_step_int4` (int.c:1531/1537):
//!     the cross-call SRF state and the per-call step are PURE and ported here
//!     as [`GenerateSeriesInt4`] (the `generate_series_fctx` struct + the
//!     iteration body, including the `pg_add_s32_overflow` final-value guard).
//!     Only the funcapi `SRF_FIRSTCALL_INIT`/`SRF_RETURN_NEXT`/`SRF_RETURN_DONE`
//!     glue and the multi-call memory context are the deferred fmgr layer's job.
//!   * `generate_series_int4_support` (int.c:1613): the planner row-count
//!     estimate.  Its `floor((finish-start+step)/step)` math is ported in
//!     [`generate_series_int4_rows`]; classifying the `Const`/NULL argument
//!     nodes is the optimizer/`nodes` layer's job and is fed in as resolved
//!     `Option<Option<f64>>`.

use utils_error::{ereport, PgResult};
use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

use crate::overflow::pg_add_s32_overflow;

/// `generate_series_fctx` (int.c:47): the cross-call state of
/// `generate_series_step_int4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerateSeriesInt4 {
    pub current: i32,
    pub finish: i32,
    pub step: i32,
}

impl GenerateSeriesInt4 {
    /// `SRF_IS_FIRSTCALL()` setup (int.c:1546-1580): validate the step and seed
    /// the state.  `step` defaults to 1 when only two args were given (the
    /// `PG_NARGS() == 3` check is the caller's; pass the resolved step).  A zero
    /// step raises ERRCODE_INVALID_PARAMETER_VALUE "step size cannot equal
    /// zero".
    pub fn new(start: i32, finish: i32, step: i32) -> PgResult<Self> {
        if step == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("step size cannot equal zero")
                .into_error());
        }
        Ok(GenerateSeriesInt4 {
            current: start,
            finish,
            step,
        })
    }

    /// One iteration of `generate_series_step_int4` (int.c:1583-1607): returns
    /// the value for this call (or `None` when the series is exhausted, i.e.
    /// `SRF_RETURN_DONE`).  On a producing call, `current` is advanced by `step`
    /// and, if that overflows, `step` is zeroed so the next call terminates --
    /// exactly the C "this is the final result" behavior.
    ///
    /// (Named `next` to mirror the SRF "next value" semantics; it is the
    /// `SRF_PERCALL` body, not an `Iterator`.)
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

/// `generate_series_int4_support` row estimate (int.c:1659-1672):
/// `floor((finish - start + step) / step)` in double arithmetic, for non-zero
/// step.  Returns `None` for a zero step (no estimate) -- matching the C guard.
pub fn generate_series_int4_rows(start: f64, finish: f64, step: f64) -> Option<f64> {
    if step != 0.0 {
        Some(((finish - start + step) / step).floor())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_series_steps_up_and_terminates() {
        let mut g = GenerateSeriesInt4::new(1, 3, 1).unwrap();
        assert_eq!(g.next(), Some(1));
        assert_eq!(g.next(), Some(2));
        assert_eq!(g.next(), Some(3));
        assert_eq!(g.next(), None);
    }

    #[test]
    fn generate_series_steps_down() {
        let mut g = GenerateSeriesInt4::new(3, 1, -1).unwrap();
        assert_eq!(g.next(), Some(3));
        assert_eq!(g.next(), Some(2));
        assert_eq!(g.next(), Some(1));
        assert_eq!(g.next(), None);
    }

    #[test]
    fn generate_series_overflow_is_final_value() {
        let mut g = GenerateSeriesInt4::new(i32::MAX - 1, i32::MAX, 1).unwrap();
        assert_eq!(g.next(), Some(i32::MAX - 1));
        assert_eq!(g.next(), Some(i32::MAX));
        assert_eq!(g.next(), None);
    }

    #[test]
    fn zero_step_errors() {
        let err = GenerateSeriesInt4::new(1, 10, 0).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(err.message(), "step size cannot equal zero");
    }

    #[test]
    fn row_estimate() {
        assert_eq!(generate_series_int4_rows(1.0, 10.0, 1.0), Some(10.0));
        assert_eq!(generate_series_int4_rows(1.0, 10.0, 2.0), Some(5.0));
        assert_eq!(generate_series_int4_rows(1.0, 10.0, 0.0), None);
    }
}
