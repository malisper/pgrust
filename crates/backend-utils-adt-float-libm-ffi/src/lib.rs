//! libm provider for `backend-utils-adt-float-seams`.
//!
//! `utils/adt/float.c` calls the C standard-library math primitives `erf`,
//! `erfc`, `tgamma` and `lgamma` directly (`<math.h>`). Rust's `std` does not
//! expose these, so the float owner (`backend-utils-adt-float`) routes its
//! `derf`/`derfc`/`dgamma`/`dlgamma` cores through the four outward seams in
//! `backend-utils-adt-float-seams`, which loud-panic until a provider binds
//! them (the sanctioned external-library pattern — mirror-PG-and-panic with the
//! `#ifdef` off).
//!
//! This crate is that provider. It binds the SAME system math library
//! PostgreSQL binds (`libm`, in `libSystem` on macOS / `libm.so` on Linux) so
//! the results are bit-identical to the C build — NOT the `libm`/`statrs` Rust
//! crates, which are independent reimplementations. The c2rust translation of
//! float.c emits exactly this `extern "C"` block
//! (`../pgrust/c2rust-runs/backend-utils-adt-float/src/float.rs:49-52`):
//!
//! ```c
//! fn erf(_: c_double) -> c_double;
//! fn erfc(_: c_double) -> c_double;
//! fn lgamma(_: c_double) -> c_double;
//! fn tgamma(_: c_double) -> c_double;
//! ```
//!
//! The owner inspects the returned value (`is_infinite()`, NaN handling) for
//! the over/underflow surface exactly as float.c's no-errno fallback path does,
//! so these seams carry no failure surface — they are the raw libm results.

// SAFETY: these are the standard C `<math.h>` transcendentals, resolved from
// the system math library (libSystem on macOS, libm.so on Linux glibc/musl).
// Each is a pure `double -> double` function with no side effects relevant to
// the owner (the `signgam` global written by `lgamma` is unused by float.c's
// `dlgamma`, matching the C source).
extern "C" {
    fn erf(x: f64) -> f64;
    fn erfc(x: f64) -> f64;
    fn tgamma(x: f64) -> f64;
    fn lgamma(x: f64) -> f64;
}

fn erf_seam(x: f64) -> f64 {
    // SAFETY: pure libm call, no invalid arguments possible for f64.
    unsafe { erf(x) }
}

fn erfc_seam(x: f64) -> f64 {
    // SAFETY: pure libm call.
    unsafe { erfc(x) }
}

fn tgamma_seam(x: f64) -> f64 {
    // SAFETY: pure libm call.
    unsafe { tgamma(x) }
}

fn lgamma_seam(x: f64) -> f64 {
    // SAFETY: pure libm call. The `signgam` side output is unused by the owner.
    unsafe { lgamma(x) }
}

/// Bind the four float8 libm seams (`erf`/`erfc`/`tgamma`/`lgamma`) to the
/// system math library. Mirrors a normal owner's `init_seams()`; called once
/// from the startup aggregator (`seams-init`).
pub fn init_seams() {
    backend_utils_adt_float_seams::erf::set(erf_seam);
    backend_utils_adt_float_seams::erfc::set(erfc_seam);
    backend_utils_adt_float_seams::tgamma::set(tgamma_seam);
    backend_utils_adt_float_seams::lgamma::set(lgamma_seam);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_and_matches_known_values() {
        init_seams();
        // erf(0) = 0, erfc(0) = 1.
        assert!((backend_utils_adt_float_seams::erf::call(0.0) - 0.0).abs() < 1e-15);
        assert!((backend_utils_adt_float_seams::erfc::call(0.0) - 1.0).abs() < 1e-15);
        // tgamma(5) = 4! = 24.
        assert!((backend_utils_adt_float_seams::tgamma::call(5.0) - 24.0).abs() < 1e-10);
        // lgamma(1) = ln(0!) = 0.
        assert!((backend_utils_adt_float_seams::lgamma::call(1.0) - 0.0).abs() < 1e-12);
        // lgamma(5) = ln(24).
        assert!(
            (backend_utils_adt_float_seams::lgamma::call(5.0) - 24.0_f64.ln()).abs() < 1e-10
        );
    }
}
