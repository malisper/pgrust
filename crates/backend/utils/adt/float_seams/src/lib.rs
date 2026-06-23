//! Seam declarations for `utils/adt/float.c` / `utils/float.h` — the IEEE
//! float8 arithmetic primitives with PostgreSQL's over/underflow detection.
//!
//! The owning unit (`backend-utils-adt-float`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use ::types_error::PgResult;

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
// Further float8 primitives needed across dependency cycles (e.g. by the
// geometric types in `geo_ops.c`). Owner: `backend-utils-adt-float`.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `float8_pl(a, b)` (`utils/float.h`): IEEE add with PostgreSQL's
    /// over/underflow detection. Owner: `backend-utils-adt-float`.
    pub fn float8_pl(a: f64, b: f64) -> PgResult<f64>
);

seam_core::seam!(
    /// `float8_mi(a, b)` (`utils/float.h`): IEEE subtract with PostgreSQL's
    /// over/underflow detection. Owner: `backend-utils-adt-float`.
    pub fn float8_mi(a: f64, b: f64) -> PgResult<f64>
);

seam_core::seam!(
    /// `float8_eq(a, b)` (`utils/float.h`): NaN-aware IEEE equality (all NaNs
    /// equal, sort after non-NaN). Owner: `backend-utils-adt-float`.
    pub fn float8_eq(a: f64, b: f64) -> bool
);

seam_core::seam!(
    /// `float8_lt(a, b)` (`utils/float.h`): NaN-aware IEEE `<`. Owner:
    /// `backend-utils-adt-float`.
    pub fn float8_lt(a: f64, b: f64) -> bool
);

seam_core::seam!(
    /// `float8_gt(a, b)` (`utils/float.h`): NaN-aware IEEE `>`. Owner:
    /// `backend-utils-adt-float`.
    pub fn float8_gt(a: f64, b: f64) -> bool
);

seam_core::seam!(
    /// `float8_min(a, b)` (`utils/float.h`): NaN-aware minimum. Owner:
    /// `backend-utils-adt-float`.
    pub fn float8_min(a: f64, b: f64) -> f64
);

seam_core::seam!(
    /// `float8_max(a, b)` (`utils/float.h`): NaN-aware maximum. Owner:
    /// `backend-utils-adt-float`.
    pub fn float8_max(a: f64, b: f64) -> f64
);

seam_core::seam!(
    /// `get_float8_infinity()` (`utils/float.h`): `+Inf`. Owner:
    /// `backend-utils-adt-float`.
    pub fn get_float8_infinity() -> f64
);

seam_core::seam!(
    /// `get_float8_nan()` (`utils/float.h`): NaN. Owner:
    /// `backend-utils-adt-float`.
    pub fn get_float8_nan() -> f64
);

seam_core::seam!(
    /// `float_overflow_error()` (float.c:85): the shared overflow `ereport`.
    /// Owner: `backend-utils-adt-float`.
    pub fn float_overflow_error() -> ::types_error::PgError
);

seam_core::seam!(
    /// `float_underflow_error()` (float.c:93): the shared underflow `ereport`.
    /// Owner: `backend-utils-adt-float`.
    pub fn float_underflow_error() -> ::types_error::PgError
);

// ---------------------------------------------------------------------------
// Text I/O for float8, needed by the geometric input/output functions in
// `geo_ops.c` (which call `float8in_internal` / `float8out_internal` across the
// dependency cycle). Owner: `backend-utils-adt-float`.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `float8in_internal(num, &endptr, type_name, orig_string)` (float.c:394),
    /// in the "report stopping point" mode (`endptr_p != NULL`): parse a float8
    /// off the front of `num`, returning the parsed value plus the byte offset
    /// (into `num`) just past the trailing whitespace. Syntax / range errors
    /// surface as `Err`. Owner: `backend-utils-adt-float`.
    pub fn float8in_internal_endptr(
        num: String,
        type_name: String,
        orig_string: String,
    ) -> PgResult<(f64, usize)>
);

seam_core::seam!(
    /// `float8out_internal(num)` (float.c:536): shortest round-trip decimal text
    /// of a float8 (default `extra_float_digits`). Owner:
    /// `backend-utils-adt-float`.
    pub fn float8out_internal(num: f64) -> String
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
