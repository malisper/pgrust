//! Seam declarations for the GUC numeric value parsers
//! (`utils/misc/guc.c`): `parse_int` / `parse_real`.
//!
//! The reloptions parser calls `parse_int(value, &result, 0, NULL)` and
//! `parse_real(value, &result, 0, NULL)` (flags `0`, no hint message). Both
//! return a C `bool` success and never `ereport` — they are infallible and
//! pure, so the seams return `Option<_>` (`Some` on success, `None` on a parse
//! failure) and take no `Mcx`.
//!
//! The owning unit (`backend-utils-misc-guc`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `parse_int(value, &result, 0, NULL)` (guc.c): parse an integer GUC
    /// value (with optional unit suffix). `None` is the C `false` return.
    pub fn parse_int(value: String) -> Option<i32>
);

seam_core::seam!(
    /// `parse_real(value, &result, 0, NULL)` (guc.c): parse a floating-point
    /// GUC value (with optional unit suffix). `None` is the C `false` return.
    pub fn parse_real(value: String) -> Option<f64>
);
