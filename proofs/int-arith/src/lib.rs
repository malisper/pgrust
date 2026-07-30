//! Kani C≡Rust equivalence: the integer arithmetic family from int.c /
//! int8.c — int{2,4,8,24,42,28,82,48,84}{pl,mi,mul,div}, int{2,4,8}mod,
//! int{2,4,8}{um,up,abs,larger,smaller}.
//!
//! Rust side: the SHIPPED fmgr wrappers — `adt_int::builtins::fc_*` /
//! `adt_int8::builtins::fc_*` — invoked through a real `LocalFcinfo` frame,
//! so each proof covers the whole shipped path: datum unwrap (args_n +
//! as_i16/as_i32/as_i64) → core → Datum::from_i16/from_i32/from_i64.
//! C side: vendored int.c + int8.c + common/int.h overflow builtins
//! (c/pg_int_arith.c).
//!
//! Domains: full symbolic iNN × iNN everywhere (all by-value scalars, no
//! fences — every function is total over its domain because both sides
//! detect overflow/zero-divide rather than perform it).
//!
//! Fallible ops prove VERDICT parity over the full domain: Rust Ok ⇔ C
//! non-error with equal values, and on Err the shipped sqlstate against C's
//! ereport errcode class (22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for
//! "integer/smallint/bigint out of range", 22012 ERRCODE_DIVISION_BY_ZERO
//! for "division by zero" — int.c/int8.c use exactly these two codes). The
//! error MESSAGE plumbing is stubbed (`PgError::error` → field-identical
//! constructor minus `Location::caller()`/message text, which Kani cannot
//! execute); the shipped `.with_sqlstate(..)` calls stay load-bearing:
//! value-space + verdict + sqlstate are in the theorem, message text is not.
//!
//! div/mod SYMBOLIC-DIVISOR harnesses (eq_*div/eq_*mod) are the known-hard
//! circuit: eq_int2div and eq_int24div PROVE (25-27s, kissat
//! + --no-assertion-reach-checks; narrow dividend keeps the divider
//! tractable); the rest wall >30s under both solvers. The spot_* harnesses
//! cover the divergence-prone special divisors (0 → 22012; -1 → negation /
//! INT_MIN overflow / mod≡0) with a CONCRETE divisor and fully symbolic
//! dividend, which avoids the divider circuit entirely and always proves.
//!
//! Solver findings (measured 2026-07-28): the WHOLE mul family — including
//! full-symbolic 64×64 int8mul — proves in 1-3s under kissat
//! + --no-assertion-reach-checks, while CaDiCaL wedges >30s on every
//! 64-bit-result fallible harness; and Box<PgError> DROP GLUE on the Err arm
//! (not the arithmetic) was the original 64-bit wall — every Err arm ends
//! with core::mem::forget(e) (ERROR-DROP trap, see proofs/varbit-rows;
//! 16s → 0.2s on eq_int8um). Assertion reach-checks cost a full external-SAT
//! pass each under kissat; these harnesses have NO kani::assume fences (full
//! domain), so reach-checks are vacuity-irrelevant and stay off.
//!
//! Negative control: control_int4larger_vs_c_smaller pits fc_int4larger
//! against C int4smaller — must FAIL (counterexample at any a != b). Run it
//! with the DEFAULT solver, expected-green harnesses with kissat.
//!
//! WAVE-3 EXTENSION (2026-07-28): bit ops (int2/int4/int8
//! and/or/xor/not/shl/shr), in_range_int{2,4,8}_int{2,4,8}, casts
//! (i2toi4/i4toi2/int48/int84/int28/int82/i8tooid/oidtoi8), int4inc,
//! int8inc/int8dec (+_any delegates).
//!
//! SHIFT THEOREMS (TRIAGE "INT SHIFT UB PLANE" pre-ruling): C `arg1 << arg2`
//! is UB for out-of-range counts, so each shift row carries TWO harnesses:
//!   eq_*   — strict equivalence vs verbatim C, count FENCED to the ruling's
//!            domain (0..=31 int4, 0..=15 int2, 0..=63 int8);
//!   model_* — shipped wrapper vs the RATIFIED masked platform model
//!            (pg_*_model in c/pg_int_arith.c) over the FULL i32 count
//!            domain (defined-in-Rust, UB-in-C; matches observed x86/ARM64
//!            C PostgreSQL behavior, ground-truthed PG 18.4 ARM64). The
//!            out-of-range arm is NOT a divergence — ratified UB plane.
//!
//! in_range: C status 3 == ereport(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_
//! SIZE) (22013, offset < 0); overflow short-circuit and both compare
//! directions are value-space in-theorem. Fallible-arm reachability for the
//! new sections lives in the cover_* harnesses (both Ok and Err arms
//! kani::cover!-witnessed; run with the DEFAULT solver — kissat is for
//! expected-green only). Extra negative controls for the new sections:
//! control_int4shl_vs_c_shr_model and control_in_range_int4_int4_lessflip
//! (both MUST FAIL).

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use types_error::{
        PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INTERNAL_ERROR,
        ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
    };
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        // int.c — infallible
        fn pg_int2up(arg: i16) -> i16;
        fn pg_int4up(arg: i32) -> i32;
        fn pg_int2larger(a: i16, b: i16) -> i16;
        fn pg_int2smaller(a: i16, b: i16) -> i16;
        fn pg_int4larger(a: i32, b: i32) -> i32;
        fn pg_int4smaller(a: i32, b: i32) -> i32;
        // int.c — fallible (0 ok / 1 = 22003 / 2 = 22012)
        fn pg_int2um(a: i16, r: *mut i16) -> c_int;
        fn pg_int4um(a: i32, r: *mut i32) -> c_int;
        fn pg_int2abs(a: i16, r: *mut i16) -> c_int;
        fn pg_int4abs(a: i32, r: *mut i32) -> c_int;
        fn pg_int2pl(a: i16, b: i16, r: *mut i16) -> c_int;
        fn pg_int2mi(a: i16, b: i16, r: *mut i16) -> c_int;
        fn pg_int2mul(a: i16, b: i16, r: *mut i16) -> c_int;
        fn pg_int2div(a: i16, b: i16, r: *mut i16) -> c_int;
        fn pg_int2mod(a: i16, b: i16, r: *mut i16) -> c_int;
        fn pg_int4pl(a: i32, b: i32, r: *mut i32) -> c_int;
        fn pg_int4mi(a: i32, b: i32, r: *mut i32) -> c_int;
        fn pg_int4mul(a: i32, b: i32, r: *mut i32) -> c_int;
        fn pg_int4div(a: i32, b: i32, r: *mut i32) -> c_int;
        fn pg_int4mod(a: i32, b: i32, r: *mut i32) -> c_int;
        fn pg_int24pl(a: i16, b: i32, r: *mut i32) -> c_int;
        fn pg_int24mi(a: i16, b: i32, r: *mut i32) -> c_int;
        fn pg_int24mul(a: i16, b: i32, r: *mut i32) -> c_int;
        fn pg_int24div(a: i16, b: i32, r: *mut i32) -> c_int;
        fn pg_int42pl(a: i32, b: i16, r: *mut i32) -> c_int;
        fn pg_int42mi(a: i32, b: i16, r: *mut i32) -> c_int;
        fn pg_int42mul(a: i32, b: i16, r: *mut i32) -> c_int;
        fn pg_int42div(a: i32, b: i16, r: *mut i32) -> c_int;
        // int8.c — infallible
        fn pg_int8up(arg: i64) -> i64;
        fn pg_int8larger(a: i64, b: i64) -> i64;
        fn pg_int8smaller(a: i64, b: i64) -> i64;
        // int8.c — fallible
        fn pg_int8um(a: i64, r: *mut i64) -> c_int;
        fn pg_int8abs(a: i64, r: *mut i64) -> c_int;
        fn pg_int8pl(a: i64, b: i64, r: *mut i64) -> c_int;
        fn pg_int8mi(a: i64, b: i64, r: *mut i64) -> c_int;
        fn pg_int8mul(a: i64, b: i64, r: *mut i64) -> c_int;
        fn pg_int8div(a: i64, b: i64, r: *mut i64) -> c_int;
        fn pg_int8mod(a: i64, b: i64, r: *mut i64) -> c_int;
        fn pg_int84pl(a: i64, b: i32, r: *mut i64) -> c_int;
        fn pg_int84mi(a: i64, b: i32, r: *mut i64) -> c_int;
        fn pg_int84mul(a: i64, b: i32, r: *mut i64) -> c_int;
        fn pg_int84div(a: i64, b: i32, r: *mut i64) -> c_int;
        fn pg_int48pl(a: i32, b: i64, r: *mut i64) -> c_int;
        fn pg_int48mi(a: i32, b: i64, r: *mut i64) -> c_int;
        fn pg_int48mul(a: i32, b: i64, r: *mut i64) -> c_int;
        fn pg_int48div(a: i32, b: i64, r: *mut i64) -> c_int;
        fn pg_int82pl(a: i64, b: i16, r: *mut i64) -> c_int;
        fn pg_int82mi(a: i64, b: i16, r: *mut i64) -> c_int;
        fn pg_int82mul(a: i64, b: i16, r: *mut i64) -> c_int;
        fn pg_int82div(a: i64, b: i16, r: *mut i64) -> c_int;
        fn pg_int28pl(a: i16, b: i64, r: *mut i64) -> c_int;
        fn pg_int28mi(a: i16, b: i64, r: *mut i64) -> c_int;
        fn pg_int28mul(a: i16, b: i64, r: *mut i64) -> c_int;
        fn pg_int28div(a: i16, b: i64, r: *mut i64) -> c_int;
        // wave 3: int.c casts / inc
        fn pg_i2toi4(a: i16) -> i32;
        fn pg_i4toi2(a: i32, r: *mut i16) -> c_int;
        fn pg_int4inc(a: i32, r: *mut i32) -> c_int;
        // wave 3: int.c / int8.c bit ops (shl/shr verbatim = UB-fenced)
        fn pg_int4and(a: i32, b: i32) -> i32;
        fn pg_int4or(a: i32, b: i32) -> i32;
        fn pg_int4xor(a: i32, b: i32) -> i32;
        fn pg_int4not(a: i32) -> i32;
        fn pg_int4shl(a: i32, b: i32) -> i32;
        fn pg_int4shr(a: i32, b: i32) -> i32;
        fn pg_int2and(a: i16, b: i16) -> i16;
        fn pg_int2or(a: i16, b: i16) -> i16;
        fn pg_int2xor(a: i16, b: i16) -> i16;
        fn pg_int2not(a: i16) -> i16;
        fn pg_int2shl(a: i16, b: i32) -> i16;
        fn pg_int2shr(a: i16, b: i32) -> i16;
        fn pg_int8and(a: i64, b: i64) -> i64;
        fn pg_int8or(a: i64, b: i64) -> i64;
        fn pg_int8xor(a: i64, b: i64) -> i64;
        fn pg_int8not(a: i64) -> i64;
        fn pg_int8shl(a: i64, b: i32) -> i64;
        fn pg_int8shr(a: i64, b: i32) -> i64;
        // wave 3: RATIFIED masked platform model (NOT vendored C)
        fn pg_int4shl_model(a: i32, b: i32) -> i32;
        fn pg_int4shr_model(a: i32, b: i32) -> i32;
        fn pg_int2shl_model(a: i16, b: i32) -> i16;
        fn pg_int2shr_model(a: i16, b: i32) -> i16;
        fn pg_int8shl_model(a: i64, b: i32) -> i64;
        fn pg_int8shr_model(a: i64, b: i32) -> i64;
        // wave 3: in_range (0 ok / 3 = 22013)
        fn pg_in_range_int4_int4(v: i32, b: i32, o: i32, sub: bool, less: bool, r: *mut bool) -> c_int;
        fn pg_in_range_int4_int2(v: i32, b: i32, o: i16, sub: bool, less: bool, r: *mut bool) -> c_int;
        fn pg_in_range_int4_int8(v: i32, b: i32, o: i64, sub: bool, less: bool, r: *mut bool) -> c_int;
        fn pg_in_range_int2_int4(v: i16, b: i16, o: i32, sub: bool, less: bool, r: *mut bool) -> c_int;
        fn pg_in_range_int2_int2(v: i16, b: i16, o: i16, sub: bool, less: bool, r: *mut bool) -> c_int;
        fn pg_in_range_int2_int8(v: i16, b: i16, o: i64, sub: bool, less: bool, r: *mut bool) -> c_int;
        fn pg_in_range_int8_int8(v: i64, b: i64, o: i64, sub: bool, less: bool, r: *mut bool) -> c_int;
        // wave 3: int8.c inc/dec + conversions
        fn pg_int8inc(a: i64, r: *mut i64) -> c_int;
        fn pg_int8dec(a: i64, r: *mut i64) -> c_int;
        fn pg_int8inc_any(a: i64, r: *mut i64) -> c_int;
        fn pg_int8dec_any(a: i64, r: *mut i64) -> c_int;
        fn pg_int48(a: i32) -> i64;
        fn pg_int84(a: i64, r: *mut i32) -> c_int;
        fn pg_int28(a: i16) -> i64;
        fn pg_int82(a: i64, r: *mut i16) -> c_int;
        fn pg_i8tooid(a: i64, r: *mut u32) -> c_int;
        fn pg_oidtoi8(a: u32) -> i64;
        // wave 3: recv/send over pqformat.c (0 ok / 4 = 08P01)
        fn pg_int2recv(data: *const u8, len: i32, cursor: *mut i32, out: *mut i16) -> c_int;
        fn pg_int4recv(data: *const u8, len: i32, cursor: *mut i32, out: *mut i32) -> c_int;
        fn pg_int8recv(data: *const u8, len: i32, cursor: *mut i32, out: *mut i64) -> c_int;
        fn pg_int2send(a: i16, out: *mut u8) -> i32;
        fn pg_int4send(a: i32, out: *mut u8) -> i32;
        fn pg_int8send(a: i64, out: *mut u8) -> i32;
    }

    /// Stub for `PgError::error`: field-identical to the shipped
    /// `new_impl(ERROR, ..)` result except `message` (text left out of the
    /// proof) and `location` (`Location::caller()` is Kani-unsupported;
    /// shipped code fills `Some(..)`, the stub leaves `None` — the field is
    /// not asserted on). `sqlstate` starts at the same
    /// `default_sqlstate_for_level(ERROR)` value so the shipped
    /// `.with_sqlstate(..)` in *_out_of_range / division_by_zero stays
    /// load-bearing.
    fn stub_pg_error_error(_message: impl Into<String>) -> PgError {
        PgError {
            level: ERROR,
            sqlstate: ERRCODE_INTERNAL_ERROR,
            message: String::new(),
            message_raw: None,
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    // ---------- infallible unary (up) ----------

    macro_rules! infallible1 {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty) $from:ident $get:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ta = kani::any();
                let c = unsafe { $pg(a) };
                let mut f = LocalFcinfo::<1>::new(0);
                f.args[0] = NullableDatum::value(Datum::$from(a));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(_) => panic!("infallible int fn errored"),
                };
                assert!(d.$get() == c);
            }
        )*};
    }

    infallible1! {
        eq_int2up: adt_int::fc_int2up / pg_int2up (i16) from_i16 as_i16;
        eq_int4up: adt_int::fc_int4up / pg_int4up (i32) from_i32 as_i32;
        eq_int8up: adt_int8::fc_int8up / pg_int8up (i64) from_i64 as_i64;
    }

    // ---------- infallible binary (larger / smaller) ----------

    macro_rules! infallible2 {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty, $tb:ty) $froma:ident $fromb:ident $get:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ta = kani::any();
                let b: $tb = kani::any();
                let c = unsafe { $pg(a, b) };
                let mut f = LocalFcinfo::<2>::new(0);
                f.args[0] = NullableDatum::value(Datum::$froma(a));
                f.args[1] = NullableDatum::value(Datum::$fromb(b));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(_) => panic!("infallible int fn errored"),
                };
                assert!(d.$get() == c);
            }
        )*};
    }

    infallible2! {
        eq_int2larger: adt_int::fc_int2larger / pg_int2larger (i16, i16) from_i16 from_i16 as_i16;
        eq_int2smaller: adt_int::fc_int2smaller / pg_int2smaller (i16, i16) from_i16 from_i16 as_i16;
        eq_int4larger: adt_int::fc_int4larger / pg_int4larger (i32, i32) from_i32 from_i32 as_i32;
        eq_int4smaller: adt_int::fc_int4smaller / pg_int4smaller (i32, i32) from_i32 from_i32 as_i32;
        eq_int8larger: adt_int8::fc_int8larger / pg_int8larger (i64, i64) from_i64 from_i64 as_i64;
        eq_int8smaller: adt_int8::fc_int8smaller / pg_int8smaller (i64, i64) from_i64 from_i64 as_i64;
    }

    // ---------- fallible unary (um / abs): value + verdict + sqlstate ----------

    macro_rules! fallible1 {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty) $from:ident $get:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: $ta = kani::any();
                let mut cval: $ta = 0;
                let cerr = unsafe { $pg(a, &mut cval) };
                let mut f = LocalFcinfo::<1>::new(0);
                f.args[0] = NullableDatum::value(Datum::$from(a));
                match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => {
                        assert!(cerr == 0);
                        assert!(d.$get() == cval);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        // Box<PgError> drop glue walls the solver (ERROR-DROP
                        // trap, see proofs/varbit-rows); teardown is not part
                        // of the claim.
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    fallible1! {
        eq_int2um: adt_int::fc_int2um / pg_int2um (i16) from_i16 as_i16;
        eq_int4um: adt_int::fc_int4um / pg_int4um (i32) from_i32 as_i32;
        eq_int8um: adt_int8::fc_int8um / pg_int8um (i64) from_i64 as_i64;
        eq_int2abs: adt_int::fc_int2abs / pg_int2abs (i16) from_i16 as_i16;
        eq_int4abs: adt_int::fc_int4abs / pg_int4abs (i32) from_i32 as_i32;
        eq_int8abs: adt_int8::fc_int8abs / pg_int8abs (i64) from_i64 as_i64;
    }

    // ---------- fallible binary: value + verdict + sqlstate, full domain ----------
    //
    // C status 1 == ereport(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), status 2 ==
    // ereport(ERRCODE_DIVISION_BY_ZERO); the sqlstate on the Rust side is set
    // by the SHIPPED with_sqlstate call, not the stub.

    macro_rules! fallible2 {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty, $tb:ty) -> $tr:ty : $froma:ident $fromb:ident $get:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: $ta = kani::any();
                let b: $tb = kani::any();
                fallible2_body!($h, $krate, $fc, $pg, a, b, $tr, $froma, $fromb, $get);
            }
        )*};
    }

    // Same theorem body with a CONCRETE divisor (spot proofs for the
    // divergence-prone special divisors of the div/mod family).
    macro_rules! fallible2_spot {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty, $tb:ty = $bval:expr) -> $tr:ty : $froma:ident $fromb:ident $get:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: $ta = kani::any();
                let b: $tb = $bval;
                fallible2_body!($h, $krate, $fc, $pg, a, b, $tr, $froma, $fromb, $get);
            }
        )*};
    }

    macro_rules! fallible2_body {
        ($h:ident, $krate:ident, $fc:ident, $pg:ident, $a:ident, $b:ident, $tr:ty, $froma:ident, $fromb:ident, $get:ident) => {{
            let mut cval: $tr = 0;
            let cerr = unsafe { $pg($a, $b, &mut cval) };
            let mut f = LocalFcinfo::<2>::new(0);
            f.args[0] = NullableDatum::value(Datum::$froma($a));
            f.args[1] = NullableDatum::value(Datum::$fromb($b));
            match $krate::builtins::$fc(None, &mut f) {
                Ok(d) => {
                    assert!(cerr == 0);
                    assert!(d.$get() == cval);
                }
                Err(e) => {
                    assert!(cerr == 1 || cerr == 2);
                    if cerr == 1 {
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                    } else {
                        assert!(e.sqlstate == ERRCODE_DIVISION_BY_ZERO);
                    }
                    assert!(e.level == ERROR);
                    // ERROR-DROP trap: Box<PgError> drop glue is not part of
                    // the claim (see proofs/varbit-rows).
                    core::mem::forget(e);
                }
            }
        }};
    }

    // pl / mi: fast-class (adder circuits).
    fallible2! {
        eq_int2pl: adt_int::fc_int2pl / pg_int2pl (i16, i16) -> i16 : from_i16 from_i16 as_i16;
        eq_int2mi: adt_int::fc_int2mi / pg_int2mi (i16, i16) -> i16 : from_i16 from_i16 as_i16;
        eq_int4pl: adt_int::fc_int4pl / pg_int4pl (i32, i32) -> i32 : from_i32 from_i32 as_i32;
        eq_int4mi: adt_int::fc_int4mi / pg_int4mi (i32, i32) -> i32 : from_i32 from_i32 as_i32;
        eq_int24pl: adt_int::fc_int24pl / pg_int24pl (i16, i32) -> i32 : from_i16 from_i32 as_i32;
        eq_int24mi: adt_int::fc_int24mi / pg_int24mi (i16, i32) -> i32 : from_i16 from_i32 as_i32;
        eq_int42pl: adt_int::fc_int42pl / pg_int42pl (i32, i16) -> i32 : from_i32 from_i16 as_i32;
        eq_int42mi: adt_int::fc_int42mi / pg_int42mi (i32, i16) -> i32 : from_i32 from_i16 as_i32;
        eq_int8pl: adt_int8::fc_int8pl / pg_int8pl (i64, i64) -> i64 : from_i64 from_i64 as_i64;
        eq_int8mi: adt_int8::fc_int8mi / pg_int8mi (i64, i64) -> i64 : from_i64 from_i64 as_i64;
        eq_int84pl: adt_int8::fc_int84pl / pg_int84pl (i64, i32) -> i64 : from_i64 from_i32 as_i64;
        eq_int84mi: adt_int8::fc_int84mi / pg_int84mi (i64, i32) -> i64 : from_i64 from_i32 as_i64;
        eq_int48pl: adt_int8::fc_int48pl / pg_int48pl (i32, i64) -> i64 : from_i32 from_i64 as_i64;
        eq_int48mi: adt_int8::fc_int48mi / pg_int48mi (i32, i64) -> i64 : from_i32 from_i64 as_i64;
        eq_int82pl: adt_int8::fc_int82pl / pg_int82pl (i64, i16) -> i64 : from_i64 from_i16 as_i64;
        eq_int82mi: adt_int8::fc_int82mi / pg_int82mi (i64, i16) -> i64 : from_i64 from_i16 as_i64;
        eq_int28pl: adt_int8::fc_int28pl / pg_int28pl (i16, i64) -> i64 : from_i16 from_i64 as_i64;
        eq_int28mi: adt_int8::fc_int28mi / pg_int28mi (i16, i64) -> i64 : from_i16 from_i64 as_i64;
    }

    // mul: symbolic×symbolic multiplier circuits — WATCH-LIST, probe each.
    fallible2! {
        eq_int2mul: adt_int::fc_int2mul / pg_int2mul (i16, i16) -> i16 : from_i16 from_i16 as_i16;
        eq_int4mul: adt_int::fc_int4mul / pg_int4mul (i32, i32) -> i32 : from_i32 from_i32 as_i32;
        eq_int24mul: adt_int::fc_int24mul / pg_int24mul (i16, i32) -> i32 : from_i16 from_i32 as_i32;
        eq_int42mul: adt_int::fc_int42mul / pg_int42mul (i32, i16) -> i32 : from_i32 from_i16 as_i32;
        eq_int8mul: adt_int8::fc_int8mul / pg_int8mul (i64, i64) -> i64 : from_i64 from_i64 as_i64;
        eq_int84mul: adt_int8::fc_int84mul / pg_int84mul (i64, i32) -> i64 : from_i64 from_i32 as_i64;
        eq_int48mul: adt_int8::fc_int48mul / pg_int48mul (i32, i64) -> i64 : from_i32 from_i64 as_i64;
        eq_int82mul: adt_int8::fc_int82mul / pg_int82mul (i64, i16) -> i64 : from_i64 from_i16 as_i64;
        eq_int28mul: adt_int8::fc_int28mul / pg_int28mul (i16, i64) -> i64 : from_i16 from_i64 as_i64;
    }

    // div / mod: symbolic-divisor division — the known-hard circuit; probe.
    fallible2! {
        eq_int2div: adt_int::fc_int2div / pg_int2div (i16, i16) -> i16 : from_i16 from_i16 as_i16;
        eq_int4div: adt_int::fc_int4div / pg_int4div (i32, i32) -> i32 : from_i32 from_i32 as_i32;
        eq_int24div: adt_int::fc_int24div / pg_int24div (i16, i32) -> i32 : from_i16 from_i32 as_i32;
        eq_int42div: adt_int::fc_int42div / pg_int42div (i32, i16) -> i32 : from_i32 from_i16 as_i32;
        eq_int8div: adt_int8::fc_int8div / pg_int8div (i64, i64) -> i64 : from_i64 from_i64 as_i64;
        eq_int84div: adt_int8::fc_int84div / pg_int84div (i64, i32) -> i64 : from_i64 from_i32 as_i64;
        eq_int48div: adt_int8::fc_int48div / pg_int48div (i32, i64) -> i64 : from_i32 from_i64 as_i64;
        eq_int82div: adt_int8::fc_int82div / pg_int82div (i64, i16) -> i64 : from_i64 from_i16 as_i64;
        eq_int28div: adt_int8::fc_int28div / pg_int28div (i16, i64) -> i64 : from_i16 from_i64 as_i64;
        eq_int2mod: adt_int::fc_int2mod / pg_int2mod (i16, i16) -> i16 : from_i16 from_i16 as_i16;
        eq_int4mod: adt_int::fc_int4mod / pg_int4mod (i32, i32) -> i32 : from_i32 from_i32 as_i32;
        eq_int8mod: adt_int8::fc_int8mod / pg_int8mod (i64, i64) -> i64 : from_i64 from_i64 as_i64;
    }

    // Spot proofs: concrete special divisors (b = 0, -1, +1), fully symbolic
    // dividend. These are exactly where ports diverge (INT_MIN/-1 overflow,
    // mod by -1 ≡ 0, zero-divide errcode) and involve no symbolic divider
    // circuit, so they stand even where the eq_* div/mod harnesses wall.
    fallible2_spot! {
        spot_int4div_b0: adt_int::fc_int4div / pg_int4div (i32, i32 = 0) -> i32 : from_i32 from_i32 as_i32;
        spot_int4div_bneg1: adt_int::fc_int4div / pg_int4div (i32, i32 = -1) -> i32 : from_i32 from_i32 as_i32;
        spot_int4div_b1: adt_int::fc_int4div / pg_int4div (i32, i32 = 1) -> i32 : from_i32 from_i32 as_i32;
        spot_int42div_b0: adt_int::fc_int42div / pg_int42div (i32, i16 = 0) -> i32 : from_i32 from_i16 as_i32;
        spot_int42div_bneg1: adt_int::fc_int42div / pg_int42div (i32, i16 = -1) -> i32 : from_i32 from_i16 as_i32;
        spot_int42div_b1: adt_int::fc_int42div / pg_int42div (i32, i16 = 1) -> i32 : from_i32 from_i16 as_i32;
        spot_int8div_b0: adt_int8::fc_int8div / pg_int8div (i64, i64 = 0) -> i64 : from_i64 from_i64 as_i64;
        spot_int8div_bneg1: adt_int8::fc_int8div / pg_int8div (i64, i64 = -1) -> i64 : from_i64 from_i64 as_i64;
        spot_int8div_b1: adt_int8::fc_int8div / pg_int8div (i64, i64 = 1) -> i64 : from_i64 from_i64 as_i64;
        spot_int84div_b0: adt_int8::fc_int84div / pg_int84div (i64, i32 = 0) -> i64 : from_i64 from_i32 as_i64;
        spot_int84div_bneg1: adt_int8::fc_int84div / pg_int84div (i64, i32 = -1) -> i64 : from_i64 from_i32 as_i64;
        spot_int84div_b1: adt_int8::fc_int84div / pg_int84div (i64, i32 = 1) -> i64 : from_i64 from_i32 as_i64;
        spot_int48div_b0: adt_int8::fc_int48div / pg_int48div (i32, i64 = 0) -> i64 : from_i32 from_i64 as_i64;
        spot_int48div_bneg1: adt_int8::fc_int48div / pg_int48div (i32, i64 = -1) -> i64 : from_i32 from_i64 as_i64;
        spot_int48div_b1: adt_int8::fc_int48div / pg_int48div (i32, i64 = 1) -> i64 : from_i32 from_i64 as_i64;
        spot_int82div_b0: adt_int8::fc_int82div / pg_int82div (i64, i16 = 0) -> i64 : from_i64 from_i16 as_i64;
        spot_int82div_bneg1: adt_int8::fc_int82div / pg_int82div (i64, i16 = -1) -> i64 : from_i64 from_i16 as_i64;
        spot_int82div_b1: adt_int8::fc_int82div / pg_int82div (i64, i16 = 1) -> i64 : from_i64 from_i16 as_i64;
        spot_int28div_b0: adt_int8::fc_int28div / pg_int28div (i16, i64 = 0) -> i64 : from_i16 from_i64 as_i64;
        spot_int28div_bneg1: adt_int8::fc_int28div / pg_int28div (i16, i64 = -1) -> i64 : from_i16 from_i64 as_i64;
        spot_int28div_b1: adt_int8::fc_int28div / pg_int28div (i16, i64 = 1) -> i64 : from_i16 from_i64 as_i64;
        spot_int2mod_b0: adt_int::fc_int2mod / pg_int2mod (i16, i16 = 0) -> i16 : from_i16 from_i16 as_i16;
        spot_int2mod_bneg1: adt_int::fc_int2mod / pg_int2mod (i16, i16 = -1) -> i16 : from_i16 from_i16 as_i16;
        spot_int2mod_b1: adt_int::fc_int2mod / pg_int2mod (i16, i16 = 1) -> i16 : from_i16 from_i16 as_i16;
        spot_int4mod_b0: adt_int::fc_int4mod / pg_int4mod (i32, i32 = 0) -> i32 : from_i32 from_i32 as_i32;
        spot_int4mod_bneg1: adt_int::fc_int4mod / pg_int4mod (i32, i32 = -1) -> i32 : from_i32 from_i32 as_i32;
        spot_int4mod_b1: adt_int::fc_int4mod / pg_int4mod (i32, i32 = 1) -> i32 : from_i32 from_i32 as_i32;
        spot_int8mod_b0: adt_int8::fc_int8mod / pg_int8mod (i64, i64 = 0) -> i64 : from_i64 from_i64 as_i64;
        spot_int8mod_bneg1: adt_int8::fc_int8mod / pg_int8mod (i64, i64 = -1) -> i64 : from_i64 from_i64 as_i64;
        spot_int8mod_b1: adt_int8::fc_int8mod / pg_int8mod (i64, i64 = 1) -> i64 : from_i64 from_i64 as_i64;
    }

    // ================== wave 3: bit ops / casts / in_range ==================

    // ---------- infallible unary with distinct result type (casts) ----------

    macro_rules! infallible1_cast {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty) $from:ident $get:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ta = kani::any();
                let c = unsafe { $pg(a) };
                let mut f = LocalFcinfo::<1>::new(0);
                f.args[0] = NullableDatum::value(Datum::$from(a));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(_) => panic!("infallible int fn errored"),
                };
                assert!(d.$get() == c);
            }
        )*};
    }

    infallible1_cast! {
        eq_i2toi4: adt_int::fc_i2toi4 / pg_i2toi4 (i16) from_i16 as_i32;
        eq_int48: adt_int8::fc_int48 / pg_int48 (i32) from_i32 as_i64;
        eq_int28: adt_int8::fc_int28 / pg_int28 (i16) from_i16 as_i64;
        eq_oidtoi8: adt_int8::fc_oidtoi8 / pg_oidtoi8 (u32) from_oid as_i64;
        eq_int2not: adt_int::fc_int2not / pg_int2not (i16) from_i16 as_i16;
        eq_int4not: adt_int::fc_int4not / pg_int4not (i32) from_i32 as_i32;
        eq_int8not: adt_int8::fc_int8not / pg_int8not (i64) from_i64 as_i64;
    }

    // ---------- infallible binary bit ops (and / or / xor) ----------

    infallible2! {
        eq_int2and: adt_int::fc_int2and / pg_int2and (i16, i16) from_i16 from_i16 as_i16;
        eq_int2or: adt_int::fc_int2or / pg_int2or (i16, i16) from_i16 from_i16 as_i16;
        eq_int2xor: adt_int::fc_int2xor / pg_int2xor (i16, i16) from_i16 from_i16 as_i16;
        eq_int4and: adt_int::fc_int4and / pg_int4and (i32, i32) from_i32 from_i32 as_i32;
        eq_int4or: adt_int::fc_int4or / pg_int4or (i32, i32) from_i32 from_i32 as_i32;
        eq_int4xor: adt_int::fc_int4xor / pg_int4xor (i32, i32) from_i32 from_i32 as_i32;
        eq_int8and: adt_int8::fc_int8and / pg_int8and (i64, i64) from_i64 from_i64 as_i64;
        eq_int8or: adt_int8::fc_int8or / pg_int8or (i64, i64) from_i64 from_i64 as_i64;
        eq_int8xor: adt_int8::fc_int8xor / pg_int8xor (i64, i64) from_i64 from_i64 as_i64;
    }

    // ---------- shifts: fenced strict arm + ratified platform-model arm ----------
    //
    // eq_*: strict vs verbatim C, count fenced to the ruling's domain (the
    // fence is trivially satisfiable, so reach-check vacuity is not a risk).
    // model_*: full-i32 count domain vs the masked platform model (ratified
    // UB plane, TRIAGE pre-ruling; NOT a parity claim against C's UB).

    macro_rules! shift_fenced {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty, max=$max:expr) $from:ident $get:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ta = kani::any();
                let n: i32 = kani::any();
                kani::assume(n >= 0 && n <= $max);
                let c = unsafe { $pg(a, n) };
                let mut f = LocalFcinfo::<2>::new(0);
                f.args[0] = NullableDatum::value(Datum::$from(a));
                f.args[1] = NullableDatum::value(Datum::from_i32(n));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(_) => panic!("infallible int fn errored"),
                };
                assert!(d.$get() == c);
            }
        )*};
    }

    macro_rules! shift_model {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty) $from:ident $get:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ta = kani::any();
                let n: i32 = kani::any();
                let c = unsafe { $pg(a, n) };
                let mut f = LocalFcinfo::<2>::new(0);
                f.args[0] = NullableDatum::value(Datum::$from(a));
                f.args[1] = NullableDatum::value(Datum::from_i32(n));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(_) => panic!("infallible int fn errored"),
                };
                assert!(d.$get() == c);
            }
        )*};
    }

    shift_fenced! {
        eq_int2shl: adt_int::fc_int2shl / pg_int2shl (i16, max=15) from_i16 as_i16;
        eq_int2shr: adt_int::fc_int2shr / pg_int2shr (i16, max=15) from_i16 as_i16;
        eq_int4shl: adt_int::fc_int4shl / pg_int4shl (i32, max=31) from_i32 as_i32;
        eq_int4shr: adt_int::fc_int4shr / pg_int4shr (i32, max=31) from_i32 as_i32;
        eq_int8shl: adt_int8::fc_int8shl / pg_int8shl (i64, max=63) from_i64 as_i64;
        eq_int8shr: adt_int8::fc_int8shr / pg_int8shr (i64, max=63) from_i64 as_i64;
    }

    shift_model! {
        model_int2shl: adt_int::fc_int2shl / pg_int2shl_model (i16) from_i16 as_i16;
        model_int2shr: adt_int::fc_int2shr / pg_int2shr_model (i16) from_i16 as_i16;
        model_int4shl: adt_int::fc_int4shl / pg_int4shl_model (i32) from_i32 as_i32;
        model_int4shr: adt_int::fc_int4shr / pg_int4shr_model (i32) from_i32 as_i32;
        model_int8shl: adt_int8::fc_int8shl / pg_int8shl_model (i64) from_i64 as_i64;
        model_int8shr: adt_int8::fc_int8shr / pg_int8shr_model (i64) from_i64 as_i64;
    }

    // ---------- fallible unary casts / inc / dec ----------

    macro_rules! fallible1_cast {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty) -> $tr:ty : $from:ident $get:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: $ta = kani::any();
                let mut cval: $tr = 0;
                let cerr = unsafe { $pg(a, &mut cval) };
                let mut f = LocalFcinfo::<1>::new(0);
                f.args[0] = NullableDatum::value(Datum::$from(a));
                match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => {
                        assert!(cerr == 0);
                        assert!(d.$get() == cval);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        // ERROR-DROP trap: teardown is not part of the claim.
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    fallible1_cast! {
        eq_i4toi2: adt_int::fc_i4toi2 / pg_i4toi2 (i32) -> i16 : from_i32 as_i16;
        eq_int4inc: adt_int::fc_int4inc / pg_int4inc (i32) -> i32 : from_i32 as_i32;
        eq_int84: adt_int8::fc_int84 / pg_int84 (i64) -> i32 : from_i64 as_i32;
        eq_int82: adt_int8::fc_int82 / pg_int82 (i64) -> i16 : from_i64 as_i16;
        eq_i8tooid: adt_int8::fc_i8tooid / pg_i8tooid (i64) -> u32 : from_i64 as_oid;
        eq_int8inc: adt_int8::fc_int8inc / pg_int8inc (i64) -> i64 : from_i64 as_i64;
        eq_int8dec: adt_int8::fc_int8dec / pg_int8dec (i64) -> i64 : from_i64 as_i64;
    }

    // int8inc_any / int8dec_any: shipped wrappers are arity 2 (the counted
    // "any" argument is ignored, C body = `return int8inc(fcinfo)`); the
    // second arg stays fully symbolic to prove the ignore.
    macro_rules! fallible_agg_inc {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: i64 = kani::any();
                let any_arg: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(a, &mut cval) };
                let mut f = LocalFcinfo::<2>::new(0);
                f.args[0] = NullableDatum::value(Datum::from_i64(a));
                f.args[1] = NullableDatum::value(Datum::from_i64(any_arg));
                match adt_int8::builtins::$fc(None, &mut f) {
                    Ok(d) => {
                        assert!(cerr == 0);
                        assert!(d.as_i64() == cval);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    fallible_agg_inc! {
        eq_int8inc_any: fc_int8inc_any / pg_int8inc_any;
        eq_int8dec_any: fc_int8dec_any / pg_int8dec_any;
    }

    // ---------- in_range family: value + verdict + sqlstate, full domain ----------
    //
    // C status 3 == ereport(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
    // (22013, offset < 0). The overflow short-circuit (`sub ? !less : less`)
    // and both compare directions are value-space in-theorem.

    macro_rules! in_range {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($tv:ty, $to:ty) $fromv:ident $fromo:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let val: $tv = kani::any();
                let base: $tv = kani::any();
                let offset: $to = kani::any();
                let sub: bool = kani::any();
                let less: bool = kani::any();
                let mut cres: bool = false;
                let cerr = unsafe { $pg(val, base, offset, sub, less, &mut cres) };
                let mut f = LocalFcinfo::<5>::new(0);
                f.args[0] = NullableDatum::value(Datum::$fromv(val));
                f.args[1] = NullableDatum::value(Datum::$fromv(base));
                f.args[2] = NullableDatum::value(Datum::$fromo(offset));
                f.args[3] = NullableDatum::value(Datum::from_bool(sub));
                f.args[4] = NullableDatum::value(Datum::from_bool(less));
                match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => {
                        assert!(cerr == 0);
                        assert!(d.as_bool() == cres);
                    }
                    Err(e) => {
                        assert!(cerr == 3);
                        assert!(e.sqlstate == ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    in_range! {
        eq_in_range_int4_int4: adt_int::fc_in_range_int4_int4 / pg_in_range_int4_int4 (i32, i32) from_i32 from_i32;
        eq_in_range_int4_int2: adt_int::fc_in_range_int4_int2 / pg_in_range_int4_int2 (i32, i16) from_i32 from_i16;
        eq_in_range_int4_int8: adt_int::fc_in_range_int4_int8 / pg_in_range_int4_int8 (i32, i64) from_i32 from_i64;
        eq_in_range_int2_int4: adt_int::fc_in_range_int2_int4 / pg_in_range_int2_int4 (i16, i32) from_i16 from_i32;
        eq_in_range_int2_int2: adt_int::fc_in_range_int2_int2 / pg_in_range_int2_int2 (i16, i16) from_i16 from_i16;
        eq_in_range_int2_int8: adt_int::fc_in_range_int2_int8 / pg_in_range_int2_int8 (i16, i64) from_i16 from_i64;
        eq_in_range_int8_int8: adt_int8::fc_in_range_int8_int8 / pg_in_range_int8_int8 (i64, i64) from_i64 from_i64;
    }

    // ---------- cover harnesses: both-arm reachability witnesses ----------
    //
    // Fallible-op gate insurance (gate-blindness class): every Ok and Err arm
    // of the new sections is kani::cover!-witnessed. No asserts here — run
    // with the DEFAULT solver (incremental; kissat re-solves per property).

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn cover_casts_both_arms() {
        let a: i32 = kani::any();
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_i32(a));
        match adt_int::builtins::fc_i4toi2(None, &mut f) {
            Ok(_) => kani::cover!(true, "i4toi2 Ok reachable"),
            Err(e) => {
                kani::cover!(true, "i4toi2 Err reachable");
                core::mem::forget(e);
            }
        }
        let b: i64 = kani::any();
        let mut g = LocalFcinfo::<1>::new(0);
        g.args[0] = NullableDatum::value(Datum::from_i64(b));
        match adt_int8::builtins::fc_int84(None, &mut g) {
            Ok(_) => kani::cover!(true, "int84 Ok reachable"),
            Err(e) => {
                kani::cover!(true, "int84 Err reachable");
                core::mem::forget(e);
            }
        }
        let c: i64 = kani::any();
        let mut h = LocalFcinfo::<1>::new(0);
        h.args[0] = NullableDatum::value(Datum::from_i64(c));
        match adt_int8::builtins::fc_int82(None, &mut h) {
            Ok(_) => kani::cover!(true, "int82 Ok reachable"),
            Err(e) => {
                kani::cover!(true, "int82 Err reachable");
                core::mem::forget(e);
            }
        }
        let d: i64 = kani::any();
        let mut i = LocalFcinfo::<1>::new(0);
        i.args[0] = NullableDatum::value(Datum::from_i64(d));
        match adt_int8::builtins::fc_i8tooid(None, &mut i) {
            Ok(_) => kani::cover!(true, "i8tooid Ok reachable"),
            Err(e) => {
                kani::cover!(true, "i8tooid Err reachable");
                core::mem::forget(e);
            }
        }
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn cover_inc_dec_both_arms() {
        let a: i32 = kani::any();
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_i32(a));
        match adt_int::builtins::fc_int4inc(None, &mut f) {
            Ok(_) => kani::cover!(true, "int4inc Ok reachable"),
            Err(e) => {
                kani::cover!(true, "int4inc Err reachable");
                core::mem::forget(e);
            }
        }
        let b: i64 = kani::any();
        let mut g = LocalFcinfo::<2>::new(0);
        g.args[0] = NullableDatum::value(Datum::from_i64(b));
        g.args[1] = NullableDatum::value(Datum::from_i64(kani::any()));
        match adt_int8::builtins::fc_int8inc_any(None, &mut g) {
            Ok(_) => kani::cover!(true, "int8inc_any Ok reachable"),
            Err(e) => {
                kani::cover!(true, "int8inc_any Err reachable");
                core::mem::forget(e);
            }
        }
        let c: i64 = kani::any();
        let mut h = LocalFcinfo::<2>::new(0);
        h.args[0] = NullableDatum::value(Datum::from_i64(c));
        h.args[1] = NullableDatum::value(Datum::from_i64(kani::any()));
        match adt_int8::builtins::fc_int8dec_any(None, &mut h) {
            Ok(_) => kani::cover!(true, "int8dec_any Ok reachable"),
            Err(e) => {
                kani::cover!(true, "int8dec_any Err reachable");
                core::mem::forget(e);
            }
        }
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn cover_in_range_both_arms() {
        // One representative per width plane (int4_int4, int2_int8 delegate,
        // int8_int8): the remaining variants share these exact code paths.
        let v: i32 = kani::any();
        let o: i32 = kani::any();
        let mut f = LocalFcinfo::<5>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_i32(v));
        f.args[1] = NullableDatum::value(Datum::from_i32(kani::any()));
        f.args[2] = NullableDatum::value(Datum::from_i32(o));
        f.args[3] = NullableDatum::value(Datum::from_bool(kani::any()));
        f.args[4] = NullableDatum::value(Datum::from_bool(kani::any()));
        match adt_int::builtins::fc_in_range_int4_int4(None, &mut f) {
            Ok(_) => kani::cover!(true, "in_range_int4_int4 Ok reachable"),
            Err(e) => {
                kani::cover!(true, "in_range_int4_int4 Err reachable");
                core::mem::forget(e);
            }
        }
        let v2: i16 = kani::any();
        let o2: i64 = kani::any();
        let mut g = LocalFcinfo::<5>::new(0);
        g.args[0] = NullableDatum::value(Datum::from_i16(v2));
        g.args[1] = NullableDatum::value(Datum::from_i16(kani::any()));
        g.args[2] = NullableDatum::value(Datum::from_i64(o2));
        g.args[3] = NullableDatum::value(Datum::from_bool(kani::any()));
        g.args[4] = NullableDatum::value(Datum::from_bool(kani::any()));
        match adt_int::builtins::fc_in_range_int2_int8(None, &mut g) {
            Ok(_) => kani::cover!(true, "in_range_int2_int8 Ok reachable"),
            Err(e) => {
                kani::cover!(true, "in_range_int2_int8 Err reachable");
                core::mem::forget(e);
            }
        }
        let v3: i64 = kani::any();
        let o3: i64 = kani::any();
        let mut h = LocalFcinfo::<5>::new(0);
        h.args[0] = NullableDatum::value(Datum::from_i64(v3));
        h.args[1] = NullableDatum::value(Datum::from_i64(kani::any()));
        h.args[2] = NullableDatum::value(Datum::from_i64(o3));
        h.args[3] = NullableDatum::value(Datum::from_bool(kani::any()));
        h.args[4] = NullableDatum::value(Datum::from_bool(kani::any()));
        match adt_int8::builtins::fc_in_range_int8_int8(None, &mut h) {
            Ok(_) => kani::cover!(true, "in_range_int8_int8 Ok reachable"),
            Err(e) => {
                kani::cover!(true, "in_range_int8_int8 Err reachable");
                core::mem::forget(e);
            }
        }
    }

    // ---------- recv/send: shipped wire path over a real StringInfo ----------
    //
    // Harness scaffolding (not part of the claim): proof_support mcx-stubs
    // recipe — Mcx::allocate -> static bump (tiny-proof-heap 2 KiB),
    // env::var -> "0", OnceLock::get_or_init -> recompute, std::fmt::format
    // stubbed (cold mcx-oom / enlarge_error arms drag fmt machinery into
    // symex even when dead); StringInfo/ctx mem::forget at harness end
    // (teardown out of scope). Theorem qualifier: "modulo static-buffer
    // allocator model".
    //
    // MEASURED (2026-07-28, shared box, kissat): the three eq_*send
    // harnesses PROVE — int2send 49.2s / int4send 65.6s / int8send 112.8s —
    // RELEASE-GATE tier (>30s per-commit cap); the send control fails
    // exactly on the image byte assert, so the through-datum image read has
    // intact provenance. The three eq_*recv harnesses are a symex WALL
    // (>240s, no verdict): the datum->&mut StringInfo pointer round-trip
    // (fc arg_stringinfo recovers the pointer from a usize Datum) keeps
    // kani::mem provenance checks live on every StringInfo field access —
    // send never round-trips a pointer datum and completes. Harnesses kept
    // for a refactor/idle-box revisit; cover_recv_both_arms shares the wall.
    //
    // recv (walled): full symbolic message bytes (cap 12), symbolic data
    // length AND symbolic cursor (incl. cursor > len and short-buffer
    // planes): value + cursor advance + verdict + sqlstate 08P01 parity vs
    // vendored pq_copymsgbytes/pq_getmsgint[64]. send (proved): full
    // symbolic value; the ENTIRE wire image (4B varlena header + BE
    // payload) is byte-compared.

    use proof_support::{mcx_stubs, stubs};
    use types_error::ERRCODE_PROTOCOL_VIOLATION;

    macro_rules! recv_harness {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident -> $tr:ty : $get:ident, n=$n:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // copy loops <= CAP+1; slack unwind = dead loop copies (TRIAGE)
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            // cold error arms (mcx oom / enlarge_error) drag std fmt machinery
            // into symex even when dead — message text is out of the proof
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const CAP: usize = $n + 4;
                let data: [u8; CAP] = kani::any();
                let dlen: usize = kani::any();
                kani::assume(dlen <= CAP);
                let cur: usize = kani::any();
                kani::assume(cur <= CAP); // cursor > len plane included

                let mut ccur: i32 = cur as i32;
                let mut cout: $tr = 0;
                let cst = unsafe { $pg(data.as_ptr(), dlen as i32, &mut ccur, &mut cout) };

                let ctx = mcx::MemoryContext::new_bump("kani-int-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
                    Ok(s) => s,
                    Err(e) => { core::mem::forget(e); panic!("stub alloc failed") }
                };
                if let Err(e) = si.append_bytes(&data[..dlen]) {
                    core::mem::forget(e);
                    panic!("append within capacity failed");
                }
                si.cursor = cur;
                let mut f = LocalFcinfo::<1>::new(0);
                f.args[0] = NullableDatum::value(Datum::from_usize(&mut si as *mut stringinfo::StringInfo as usize));
                match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => {
                        assert!(cst == 0);
                        assert!(d.$get() == cout);
                        assert!(si.cursor == ccur as usize);
                    }
                    Err(e) => {
                        assert!(cst == 4);
                        assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        )*};
    }

    recv_harness! {
        eq_int2recv: adt_int::fc_int2recv / pg_int2recv -> i16 : as_i16, n=2, unwind=8;
        eq_int4recv: adt_int::fc_int4recv / pg_int4recv -> i32 : as_i32, n=4, unwind=10;
        eq_int8recv: adt_int8::fc_int8recv / pg_int8recv -> i64 : as_i64, n=8, unwind=14;
    }

    macro_rules! send_harness {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($ta:ty) $from:ident, total=$total:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // image compare <= total+1; slack unwind = dead loop copies
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let a: $ta = kani::any();
                let mut cbuf = [0u8; $total];
                let clen = unsafe { $pg(a, cbuf.as_mut_ptr()) };

                let ctx = mcx::MemoryContext::new_bump("kani-int-send");
                let mut f = LocalFcinfo::<1>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] = NullableDatum::value(Datum::$from(a));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => { core::mem::forget(e); panic!("send errored") }
                };
                // varlena_result leaks the image (mem::forget); the datum
                // points at its first byte inside the stub bump buffer.
                let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, $total) };
                assert!(clen == $total as i32);
                let mut i = 0;
                while i < $total {
                    assert!(img[i] == cbuf[i]);
                    i += 1;
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    send_harness! {
        eq_int2send: adt_int::fc_int2send / pg_int2send (i16) from_i16, total=6, unwind=8;
        eq_int4send: adt_int::fc_int4send / pg_int4send (i32) from_i32, total=8, unwind=10;
        eq_int8send: adt_int8::fc_int8send / pg_int8send (i64) from_i64, total=12, unwind=14;
    }

    // recv both-arm reachability (Ok needs cursor+N <= len; Err needs the
    // short-message plane) — gate insurance for the fallible wire path.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn cover_recv_both_arms() {
        const CAP: usize = 6;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let cur: usize = kani::any();
        kani::assume(cur <= CAP);
        let ctx = mcx::MemoryContext::new_bump("kani-int-recv-cover");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => { core::mem::forget(e); panic!("stub alloc failed") }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        si.cursor = cur;
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(&mut si as *mut stringinfo::StringInfo as usize));
        match adt_int::builtins::fc_int4recv(None, &mut f) {
            Ok(_) => kani::cover!(true, "int4recv Ok reachable"),
            Err(e) => {
                kani::cover!(true, "int4recv Err reachable");
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// Deliberate mismatch: shipped fc_int4send vs C int2send's image (wrong
    /// width and header). MUST fail. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_int4send_vs_c_int2send() {
        let a: i32 = kani::any();
        let mut cbuf = [0u8; 6];
        let _clen = unsafe { pg_int2send(a as i16, cbuf.as_mut_ptr()) };
        let ctx = mcx::MemoryContext::new_bump("kani-int-send-ctl");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_i32(a));
        let d = match adt_int::builtins::fc_int4send(None, &mut f) {
            Ok(d) => d,
            Err(e) => { core::mem::forget(e); panic!("send errored") }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 6) };
        let mut i = 0;
        while i < 6 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    // ---------- wave-3 negative controls: new sections must be able to fail ----------

    /// Deliberate mismatch: shipped fc_int4shl vs the C shr model. MUST fail
    /// (counterexample at e.g. a=1, n=1). DEFAULT solver.
    #[kani::proof]
    fn control_int4shl_vs_c_shr_model() {
        let a: i32 = kani::any();
        let n: i32 = kani::any();
        let c = unsafe { pg_int4shr_model(a, n) };
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_i32(a));
        f.args[1] = NullableDatum::value(Datum::from_i32(n));
        let d = match adt_int::builtins::fc_int4shl(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("infallible int fn errored"),
        };
        assert!(d.as_i32() == c);
    }

    /// Deliberate mismatch: C in_range called with `less` FLIPPED. MUST fail.
    /// DEFAULT solver.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn control_in_range_int4_int4_lessflip() {
        let val: i32 = kani::any();
        let base: i32 = kani::any();
        let offset: i32 = kani::any();
        let sub: bool = kani::any();
        let less: bool = kani::any();
        let mut cres: bool = false;
        let cerr = unsafe { pg_in_range_int4_int4(val, base, offset, sub, !less, &mut cres) };
        let mut f = LocalFcinfo::<5>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_i32(val));
        f.args[1] = NullableDatum::value(Datum::from_i32(base));
        f.args[2] = NullableDatum::value(Datum::from_i32(offset));
        f.args[3] = NullableDatum::value(Datum::from_bool(sub));
        f.args[4] = NullableDatum::value(Datum::from_bool(less));
        match adt_int::builtins::fc_in_range_int4_int4(None, &mut f) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_bool() == cres);
            }
            Err(e) => {
                assert!(cerr == 3);
                core::mem::forget(e);
            }
        }
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_int4larger vs C int4smaller. MUST fail
    /// with a counterexample at any a != b. Run with the DEFAULT solver
    /// (kissat is non-incremental and effectively never terminates on failing
    /// harnesses).
    #[kani::proof]
    fn control_int4larger_vs_c_smaller() {
        let a: i32 = kani::any();
        let b: i32 = kani::any();
        let c = unsafe { pg_int4smaller(a, b) };
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_i32(a));
        f.args[1] = NullableDatum::value(Datum::from_i32(b));
        let d = match adt_int::builtins::fc_int4larger(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("infallible int fn errored"),
        };
        assert!(d.as_i32() == c);
    }
}
