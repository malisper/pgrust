//! Kani C≡Rust equivalence: the scalar cast family — int<->int width casts,
//! bool<->int4, float4<->float8 widening/narrowing, and float<->int
//! conversions (pg_proc oids 235-238, 311-319, 313/314, 480-483, 652/653,
//! 714, 754, 2557/2558).
//!
//! Rust side: the SHIPPED fmgr wrappers — `adt_float::builtins::fc_*`,
//! `adt_int::builtins::fc_*`, `adt_int8::builtins::fc_*` — invoked through a
//! real `LocalFcinfo<1>` frame (proof_support::call1), so each proof covers
//! the whole shipped path: datum unwrap (args_n + as_*) → core →
//! Datum::from_*. C side: vendored REL_18_STABLE float.c/int.c/int8.c casts
//! + the c.h FLOAT*_FITS_IN_INT* macros (c/pg_casts.c).
//!
//! Domains: fully symbolic over the full input type everywhere — every NaN
//! payload, ±Inf, ±0, subnormals for the float inputs; no assumes anywhere
//! (all casts are total: fallible ones error rather than trap).
//!
//! Float outputs are compared by to_bits() — bit-exact, so dtof narrowing
//! rounding and int→float conversion rounding are inside the theorem.
//!
//! Fallible casts (dtof, dtoi2/4/8, ftoi2/4/8, int84, int82, i4toi2) prove
//! VERDICT parity over the full domain: Rust Ok ⇔ C non-error, equal values
//! on Ok, and the shipped sqlstate (22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
//! applied by the real `with_sqlstate` call) + ERROR level on Err against
//! C's ereport shim flag (cash_pl precedent). The error MESSAGE plumbing is
//! stubbed (`PgError::error` → proof_support::stub_pg_error_error, minus
//! `Location::caller()`/message text, which Kani cannot execute): value-space
//! + verdict + sqlstate are in the theorem, message text is not. Each
//! fallible harness carries `kani::cover!` witnesses that BOTH arms are
//! reachable (vacuity insurance).
//!
//! rint: C rounds float→int inputs with rint() (round-half-even in the
//! default mode CBMC models); Rust ships `round_ties_even()`. The theorem
//! checks they agree bit-for-bit, including C's verbatim
//! `float4 num = rint(num)` double→float narrowing in ftoi2/ftoi4/ftoi8.
//!
//! Negative control: control_dtoi4_trunc_vs_rint pits shipped fc_dtoi4
//! against a deliberately wrong C variant (truncation instead of rint) —
//! MUST fail (counterexample at any fractional input, e.g. 1.5). Run it
//! with the DEFAULT solver (kissat is non-incremental and never terminates
//! on failing harnesses); expected-green harnesses run with kissat.

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use proof_support::{call1, call1_ok};
    use types_error::{ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};

    use std::os::raw::c_int;

    extern "C" {
        // float.c
        fn pg_ftod(num: f32) -> f64;
        fn pg_dtof(num: f64, presult: *mut f32) -> c_int;
        fn pg_dtoi4(num: f64, presult: *mut i32) -> c_int;
        fn pg_dtoi2(num: f64, presult: *mut i16) -> c_int;
        fn pg_i4tod(num: i32) -> f64;
        fn pg_i2tod(num: i16) -> f64;
        fn pg_ftoi4(num: f32, presult: *mut i32) -> c_int;
        fn pg_ftoi2(num: f32, presult: *mut i16) -> c_int;
        fn pg_i4tof(num: i32) -> f32;
        fn pg_i2tof(num: i16) -> f32;
        // int8.c
        fn pg_int48(arg: i32) -> i64;
        fn pg_int84(arg: i64, presult: *mut i32) -> c_int;
        fn pg_int28(arg: i16) -> i64;
        fn pg_int82(arg: i64, presult: *mut i16) -> c_int;
        fn pg_i8tod(arg: i64) -> f64;
        fn pg_dtoi8(num: f64, presult: *mut i64) -> c_int;
        fn pg_i8tof(arg: i64) -> f32;
        fn pg_ftoi8(num: f32, presult: *mut i64) -> c_int;
        // int.c
        fn pg_i2toi4(arg1: i16) -> i32;
        fn pg_i4toi2(arg1: i32, presult: *mut i16) -> c_int;
        fn pg_int4_bool(arg: i32) -> c_int;
        fn pg_bool_int4(arg: c_int) -> i32;
        // control-only (NOT Postgres code)
        fn pg_control_dtoi4_trunc(num: f64, presult: *mut i32) -> c_int;
    }

    // ---------- infallible casts: value parity (floats by to_bits) ----------

    /// int/bool-output infallible casts.
    macro_rules! infallible_int {
        ($($h:ident: $fc:path, $pg:ident, $in:ty, $extract:ident as $cast:ty;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $in = kani::any();
                let r = call1_ok($fc, a);
                let c = unsafe { $pg(a) };
                assert!(r.$extract() as $cast == c);
            }
        )*};
    }

    infallible_int! {
        eq_i2toi4:    adt_int::builtins::fc_i2toi4,     pg_i2toi4,    i16, as_i32 as i32;
        eq_int48:     adt_int8::builtins::fc_int48,     pg_int48,     i32, as_i64 as i64;
        eq_int28:     adt_int8::builtins::fc_int28,     pg_int28,     i16, as_i64 as i64;
        eq_int4_bool: adt_int::builtins::fc_int4_bool,  pg_int4_bool, i32, as_bool as c_int;
    }

    #[kani::proof]
    fn eq_bool_int4() {
        let a: bool = kani::any();
        let r = call1_ok(adt_int::builtins::fc_bool_int4, a);
        let c = unsafe { pg_bool_int4(a as c_int) };
        assert!(r.as_i32() == c);
    }

    /// float-output infallible casts: bit-exact via to_bits. Inputs ride
    /// through the same Datum ctor the shipped callers use ($ctor).
    macro_rules! infallible_float {
        ($($h:ident: $fc:path, $pg:ident, $in:ty, $ctor:ident, $extract:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $in = kani::any();
                let r = call1_ok($fc, Datum::$ctor(a));
                let c = unsafe { $pg(a) };
                assert!(r.$extract().to_bits() == c.to_bits());
            }
        )*};
    }

    infallible_float! {
        eq_ftod:  adt_float::builtins::fc_ftod,  pg_ftod,  f32, from_f32, as_f64;
        eq_i4tod: adt_float::builtins::fc_i4tod, pg_i4tod, i32, from_i32, as_f64;
        eq_i2tod: adt_float::builtins::fc_i2tod, pg_i2tod, i16, from_i16, as_f64;
        eq_i4tof: adt_float::builtins::fc_i4tof, pg_i4tof, i32, from_i32, as_f32;
        eq_i2tof: adt_float::builtins::fc_i2tof, pg_i2tof, i16, from_i16, as_f32;
        eq_i8tod: adt_int8::builtins::fc_i8tod,  pg_i8tod, i64, from_i64, as_f64;
        eq_i8tof: adt_int8::builtins::fc_i8tof,  pg_i8tof, i64, from_i64, as_f32;
    }

    // ---------- fallible casts: value + verdict + sqlstate parity ----------

    /// Fallible cast with an integer output: C error flag 0/1; Rust
    /// Ok ⇔ flag==0 with equal value, Err ⇔ flag==1 with the shipped
    /// sqlstate/level (set by the real with_sqlstate call, not the stub).
    macro_rules! fallible_int {
        ($($h:ident: $fc:path, $pg:ident, $in:ty => $out:ty, $extract:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            fn $h() {
                let a: $in = kani::any();
                let mut cval: $out = 0;
                let cerr = unsafe { $pg(a, &mut cval) };
                kani::cover!(cerr == 0, "success arm reachable");
                kani::cover!(cerr != 0, "error arm reachable");
                match call1($fc, a) {
                    Ok(d) => {
                        assert!(cerr == 0);
                        assert!(d.$extract() == cval);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                    }
                }
            }
        )*};
    }

    fallible_int! {
        eq_int84:  adt_int8::builtins::fc_int84,  pg_int84,  i64 => i32, as_i32;
        eq_int82:  adt_int8::builtins::fc_int82,  pg_int82,  i64 => i16, as_i16;
        eq_i4toi2: adt_int::builtins::fc_i4toi2,  pg_i4toi2, i32 => i16, as_i16;
    }

    macro_rules! fallible_float_to_int {
        ($($h:ident: $fc:path, $pg:ident, $in:ty, $ctor:ident => $out:ty, $extract:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            fn $h() {
                let a: $in = kani::any();
                let mut cval: $out = 0;
                let cerr = unsafe { $pg(a, &mut cval) };
                kani::cover!(cerr == 0, "success arm reachable");
                kani::cover!(cerr != 0, "error arm reachable");
                match call1($fc, Datum::$ctor(a)) {
                    Ok(d) => {
                        assert!(cerr == 0);
                        assert!(d.$extract() == cval);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                    }
                }
            }
        )*};
    }

    fallible_float_to_int! {
        eq_dtoi4: adt_float::builtins::fc_dtoi4, pg_dtoi4, f64, from_f64 => i32, as_i32;
        eq_dtoi2: adt_float::builtins::fc_dtoi2, pg_dtoi2, f64, from_f64 => i16, as_i16;
        eq_ftoi4: adt_float::builtins::fc_ftoi4, pg_ftoi4, f32, from_f32 => i32, as_i32;
        eq_ftoi2: adt_float::builtins::fc_ftoi2, pg_ftoi2, f32, from_f32 => i16, as_i16;
        eq_dtoi8: adt_int8::builtins::fc_dtoi8,  pg_dtoi8, f64, from_f64 => i64, as_i64;
        eq_ftoi8: adt_int8::builtins::fc_ftoi8,  pg_ftoi8, f32, from_f32 => i64, as_i64;
    }

    /// dtof: float4 output (bit-exact), C flag distinguishes overflow(1) /
    /// underflow(2) — both raise ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE in C
    /// (float_overflow_error/float_underflow_error), matching the shipped
    /// Rust error constructors.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
    fn eq_dtof() {
        let a: f64 = kani::any();
        let mut cval: f32 = 0.0;
        let cerr = unsafe { pg_dtof(a, &mut cval) };
        kani::cover!(cerr == 0, "success arm reachable");
        kani::cover!(cerr == 1, "overflow arm reachable");
        kani::cover!(cerr == 2, "underflow arm reachable");
        match call1(adt_float::builtins::fc_dtof, Datum::from_f64(a)) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_f32().to_bits() == cval.to_bits());
            }
            Err(e) => {
                assert!(cerr == 1 || cerr == 2);
                assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
            }
        }
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_dtoi4 (rint semantics) vs a C variant
    /// that truncates. MUST fail with a counterexample at a fractional input
    /// (e.g. 1.5 → Rust 2, control 1). Run with the DEFAULT solver.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
    fn control_dtoi4_trunc_vs_rint() {
        let a: f64 = kani::any();
        let mut cval: i32 = 0;
        let cerr = unsafe { pg_control_dtoi4_trunc(a, &mut cval) };
        match call1(adt_float::builtins::fc_dtoi4, Datum::from_f64(a)) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_i32() == cval);
            }
            Err(_) => {
                assert!(cerr == 1);
            }
        }
    }
}
