//! Seam declarations for `utils/adt/float.c` / `utils/float.h` — the IEEE
//! float8 arithmetic primitives with PostgreSQL's over/underflow detection.
//!
//! The owning unit (`backend-utils-adt-float`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `float8_mul(a, b)` (`utils/float.h`): IEEE multiply with PostgreSQL's
    /// over/underflow detection (`CHECKFLOATVAL`). Overflow surfaces as
    /// `Err(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)` ("value out of range:
    /// overflow"). Owner: `backend-utils-adt-float`.
    pub fn float8_mul(a: f64, b: f64) -> PgResult<f64>
);

seam_core::seam!(
    /// `float8_div(a, b)` (`utils/float.h`): IEEE divide with PostgreSQL's
    /// over/underflow and divide-by-zero detection. Owner:
    /// `backend-utils-adt-float`.
    pub fn float8_div(a: f64, b: f64) -> PgResult<f64>
);
