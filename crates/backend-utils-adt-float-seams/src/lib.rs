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

// ---------------------------------------------------------------------------
// libm transcendentals not exposed by Rust's `std` (`<math.h>`): `erf`, `erfc`,
// `tgamma`, `lgamma`. float.c calls these directly from the C standard library.
// They have no PostgreSQL owner crate; until a `common-libm` provider lands the
// `derf`/`derfc`/`dgamma`/`dlgamma` cores route through these outward seams and
// panic loudly (mirror-pg-and-panic). Pure FP routines, no failure surface.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `erf(x)` (`<math.h>`): the Gauss error function.
    pub fn erf(x: f64) -> f64
);

seam_core::seam!(
    /// `erfc(x)` (`<math.h>`): the complementary error function.
    pub fn erfc(x: f64) -> f64
);

seam_core::seam!(
    /// `tgamma(x)` (`<math.h>`): the true gamma function.
    pub fn tgamma(x: f64) -> f64
);

seam_core::seam!(
    /// `lgamma(x)` (`<math.h>`): the natural log of `|gamma(x)|` (the C
    /// `signgam` side output is unused by float.c's `dlgamma`).
    pub fn lgamma(x: f64) -> f64
);
