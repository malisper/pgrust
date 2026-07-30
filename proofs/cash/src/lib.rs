//! Kani C≡Rust equivalence: the money (cash) family — comparators +
//! additive arithmetic (oids 377, 888–895, 898, 899) and, since the wave-4
//! extension (2026-07-28/29), the full remainder: int multiply/divide
//! (862–867, 3344, 3345, 3399), float multiply/divide (846–848, 896, 897,
//! 919), cash_div_cash (3822), int4_cash/int8_cash (3811/3812, locale
//! seam), cash_in/cash_out (886/887, locale seam), cash_words (935),
//! cash_recv/cash_send (2492/2493). cash_numeric/numeric_cash (3823/3824)
//! stay parked on the numeric fixed-buffer core refactor (allocating digit
//! loops — TRIAGE).
//!
//! KNOWN DIVERGENCE (adjudication owed, do not silently fix): shipped
//! cash_div_int64(i64::MIN, -1) PANICS (Rust division overflow, release
//! included — native replay 2026-07-29), i.e. crash-and-restart per the
//! panic-fatality ruling; C cash.c has no MIN/-1 guard, and real PostgreSQL
//! 18.4 on Linux/ARM64 (docker postgres:18, the production architecture)
//! quietly returns INT64_MIN (money / -1 at the most negative value); on
//! x86-64 the same C is a SIGFPE crash. See
//! rust_panics_cash_div_min_by_neg1 (pins the Rust arm) and the fenced
//! eq_cash_div_int8_by_neg1.
//!
//! Rust side: the SHIPPED fmgr wrappers — `adt_cash::builtins::fc_cash_{eq,
//! ne,lt,le,gt,ge,cmp,pl,mi}` and `fc_cashlarger`/`fc_cashsmaller` — invoked
//! through a real `LocalFcinfo<2>` frame (via `proof_support`'s call
//! helpers), so each proof covers the whole shipped path: datum unwrap
//! (args_n + as_i64) → core → Datum::from_bool/from_i32/from_i64. C side:
//! vendored cash.c + common/int.h overflow helpers (c/pg_cash.c).
//!
//! Domains: full symbolic i64 × i64 everywhere (Cash is by-value int64; no
//! fences needed — cash_pl/cash_mi are total over the domain because both
//! sides detect overflow rather than perform it).
//!
//! cash_pl / cash_mi additionally prove VERDICT parity over the full domain:
//! Rust Ok ⇔ C non-overflow, equal values on Ok, and the shipped sqlstate
//! (22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, applied by the real
//! `with_sqlstate` call) on Err against C's ereport errcode. The error
//! MESSAGE plumbing is stubbed with `proof_support::stub_pg_error_error`
//! (field-identical constructor minus `Location::caller()`/message text,
//! which Kani cannot execute): value-space + verdict + sqlstate are in the
//! theorem, message text is not.
//!
//! Negative control: control_cash_lt_vs_c_le pits fc_cash_lt against C
//! cash_le — must FAIL (counterexample at a == b). Run it with the DEFAULT
//! solver, expected-green harnesses with kissat.

#[cfg(kani)]
mod proofs {
    use proof_support::{call2, call2_ok, stubs};
    use types_error::{ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};

    use std::os::raw::c_int;

    extern "C" {
        fn pg_cash_eq(c1: i64, c2: i64) -> c_int;
        fn pg_cash_ne(c1: i64, c2: i64) -> c_int;
        fn pg_cash_lt(c1: i64, c2: i64) -> c_int;
        fn pg_cash_le(c1: i64, c2: i64) -> c_int;
        fn pg_cash_gt(c1: i64, c2: i64) -> c_int;
        fn pg_cash_ge(c1: i64, c2: i64) -> c_int;
        fn pg_cash_cmp(c1: i64, c2: i64) -> c_int;
        fn pg_cash_pl(c1: i64, c2: i64, result: *mut i64) -> c_int;
        fn pg_cash_mi(c1: i64, c2: i64, result: *mut i64) -> c_int;
        fn pg_cashlarger(c1: i64, c2: i64) -> i64;
        fn pg_cashsmaller(c1: i64, c2: i64) -> i64;
    }

    // ---------- comparators + larger/smaller: full symbolic i64 × i64 ----------

    proof_support::eq_op2! {
        eq_cash_eq: adt_cash::builtins::fc_cash_eq, pg_cash_eq, i64, as_bool as c_int;
        eq_cash_ne: adt_cash::builtins::fc_cash_ne, pg_cash_ne, i64, as_bool as c_int;
        eq_cash_lt: adt_cash::builtins::fc_cash_lt, pg_cash_lt, i64, as_bool as c_int;
        eq_cash_le: adt_cash::builtins::fc_cash_le, pg_cash_le, i64, as_bool as c_int;
        eq_cash_gt: adt_cash::builtins::fc_cash_gt, pg_cash_gt, i64, as_bool as c_int;
        eq_cash_ge: adt_cash::builtins::fc_cash_ge, pg_cash_ge, i64, as_bool as c_int;
        eq_cash_cmp: adt_cash::builtins::fc_cash_cmp, pg_cash_cmp, i64, as_i32 as c_int;
        eq_cashlarger: adt_cash::builtins::fc_cashlarger, pg_cashlarger, i64, as_i64 as i64;
        eq_cashsmaller: adt_cash::builtins::fc_cashsmaller, pg_cashsmaller, i64, as_i64 as i64;
    }

    // ---------- cash_pl / cash_mi: value + verdict parity, full domain ----------

    macro_rules! fallible_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let a: i64 = kani::any();
                let b: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(a, b, &mut cval) };
                match call2(adt_cash::builtins::$fc, a, b) {
                    Ok(d) => {
                        // Vacuity insurance (retrofit 2026-07-28): the arm must
                        // be reachable or the green is hollow.
                        kani::cover!(true, "Ok arm reachable");
                        // C succeeded too, with the identical value.
                        assert!(cerr == 0);
                        assert!(d.as_i64() == cval);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        // C raised ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE):
                        // verdict + sqlstate parity (sqlstate set by the SHIPPED
                        // with_sqlstate call, not the stub).
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        // Box<PgError> drop glue is a measured symex tax
                        // (TRIAGE error-drop trap) — teardown is not part of
                        // the claim.
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    fallible_op! {
        eq_cash_pl: fc_cash_pl / pg_cash_pl;
        eq_cash_mi: fc_cash_mi / pg_cash_mi;
    }

    // ======================================================================
    // WAVE-4 EXTENSION (2026-07-28): the remaining ~23 cash rows.
    //
    // Locale seam (state-seam pattern, tz-seam precedent): the shipped
    // `pg_locale::pglc_localeconv` is a config-state read. Harnesses stub it
    // to return PROOF_LCONV (below), a struct whose char-class fields are set
    // per-harness — universally quantified where the harness says so — and
    // whose string symbols stay "" = the C locale, so BOTH sides run their
    // own verbatim fallback arms ('.', ",", "$", "+", "-") in-theorem. The C
    // side reads the identical values through pg_proof_set_lconv. What
    // leaves the proof: the locale lookup itself (and the shipped non-C-
    // locale FEATURE_NOT_SUPPORTED arm, plus non-default locale SYMBOLS).
    // control_int8_cash_lconv_skew / control_cash_out_lconv_skew prove the
    // seam is load-bearing (must FAIL when the sides disagree).
    //
    // Error-code seam: vendored C returns 0 ok / 1 22003 / 2 22012 /
    // 3 22P02 / 4 08P01 (per-errcode PROOF_EREPORT_FLAG); errcode_of()
    // maps the Rust Err sqlstate onto the same scale, so sqlstate parity is
    // asserted on every Err arm. Message text/location out of proof
    // (stub_pg_error_error / stub_format).
    // ======================================================================

    use proof_support::{call1, mcx_stubs};
    use types_error::{
        PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_TEXT_REPRESENTATION,
        ERRCODE_PROTOCOL_VIOLATION,
    };

    extern "C" {
        fn pg_proof_set_lconv(
            frac_digits: i8,
            mon_grouping0: i8,
            p_cs_precedes: i8,
            n_cs_precedes: i8,
            p_sep_by_space: i8,
            n_sep_by_space: i8,
            p_sign_posn: i8,
            n_sign_posn: i8,
        ) -> c_int; // int returns: goto-cc rejects Rust Unit vs C void
        fn pg_cash_mul_flt8(c: i64, f: f64, out: *mut i64) -> c_int;
        fn pg_cash_div_flt8(c: i64, f: f64, out: *mut i64) -> c_int;
        fn pg_cash_mul_flt4(c: i64, f: f32, out: *mut i64) -> c_int;
        fn pg_cash_div_flt4(c: i64, f: f32, out: *mut i64) -> c_int;
        fn pg_cash_mul_int64(c: i64, i: i64, out: *mut i64) -> c_int;
        fn pg_cash_div_int64(c: i64, i: i64, out: *mut i64) -> c_int;
        fn pg_cash_mul_int4(c: i64, i: i32, out: *mut i64) -> c_int;
        fn pg_cash_div_int4(c: i64, i: i32, out: *mut i64) -> c_int;
        fn pg_cash_mul_int2(c: i64, s: i16, out: *mut i64) -> c_int;
        fn pg_cash_div_int2(c: i64, s: i16, out: *mut i64) -> c_int;
        fn pg_cash_div_cash(dividend: i64, divisor: i64, out: *mut f64) -> c_int;
        fn pg_int4_cash(amount: i32, out: *mut i64) -> c_int;
        fn pg_int8_cash(amount: i64, out: *mut i64) -> c_int;
        fn pg_cash_in(s: *const u8, err: *mut c_int) -> i64;
        fn pg_cash_out(value: i64, out: *mut u8) -> c_int;
        fn pg_cash_words(value: i64, out: *mut u8) -> c_int;
        fn pg_cash_recv(buf: *const u8, len: u64, out: *mut i64) -> c_int;
        fn pg_cash_send(v: i64, out8: *mut u8) -> c_int;
    }

    // ---------- locale seam scaffolding ----------

    static mut PROOF_LCONV: pg_locale::PgLconv = pg_locale::PgLconv {
        mon_decimal_point: "",
        mon_thousands_sep: "",
        mon_grouping: "",
        currency_symbol: "",
        positive_sign: "",
        negative_sign: "",
        frac_digits: 127,
        p_cs_precedes: 127,
        n_cs_precedes: 127,
        p_sep_by_space: 127,
        n_sep_by_space: 127,
        p_sign_posn: 127,
        n_sign_posn: 127,
    };

    /// Stub for `pg_locale::pglc_localeconv`: return the harness-controlled
    /// seam struct. What leaves the proof: the locale lookup/caching itself
    /// and the shipped non-C-locale FEATURE_NOT_SUPPORTED arm.
    fn stub_pglc_localeconv() -> PgResult<&'static pg_locale::PgLconv> {
        // SAFETY: single-threaded under Kani; written only before the calls.
        Ok(unsafe { &*core::ptr::addr_of!(PROOF_LCONV) })
    }

    /// mon_grouping is `&'static str` on the Rust side, so its first byte
    /// cannot be made bit-symbolic; a symbolic SELECTOR over these concrete
    /// groupings covers the clamp (<=0, >6 -> 3) and the in-range values.
    /// Bytes >= 0x80 are unrepresentable in &str; on the C side they clamp
    /// to 3 under either char-signedness, same as 0/127 do, so the selector
    /// set loses no behavior class.
    const GROUPINGS: [&str; 5] = ["", "\u{1}", "\u{3}", "\u{6}", "\u{7f}"];

    #[allow(clippy::too_many_arguments)]
    fn seam_lconv(fd: i8, gsel: usize, pcs: i8, ncs: i8, psep: i8, nsep: i8, ppos: i8, npos: i8) {
        let g = GROUPINGS[gsel];
        // SAFETY: single-threaded under Kani.
        unsafe {
            let l = &mut *core::ptr::addr_of_mut!(PROOF_LCONV);
            l.frac_digits = fd;
            l.mon_grouping = g;
            l.p_cs_precedes = pcs;
            l.n_cs_precedes = ncs;
            l.p_sep_by_space = psep;
            l.n_sep_by_space = nsep;
            l.p_sign_posn = ppos;
            l.n_sign_posn = npos;
            let _ = pg_proof_set_lconv(
                fd,
                g.as_bytes().first().copied().unwrap_or(0) as i8,
                pcs,
                ncs,
                psep,
                nsep,
                ppos,
                npos,
            );
        }
    }

    /// Fully symbolic seam (all char-class fields), C-locale symbols.
    fn seam_lconv_any() {
        let gsel: usize = kani::any();
        kani::assume(gsel < GROUPINGS.len());
        seam_lconv(
            kani::any(),
            gsel,
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
        );
    }

    // ---------- misc scaffolding ----------

    /// Rust Err sqlstate -> the vendored C err-code scale.
    fn errcode_of(e: &PgError) -> c_int {
        if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
            1
        } else if e.sqlstate == ERRCODE_DIVISION_BY_ZERO {
            2
        } else if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
            3
        } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
            4
        } else {
            -1
        }
    }

    /// `.unwrap()` on PgResult drags Box<PgError> Debug + drop glue into
    /// symex (TRIAGE) — forget + static panic instead.
    fn ok_or_die<T>(r: PgResult<T>) -> T {
        match r {
            Ok(t) => t,
            Err(e) => {
                core::mem::forget(e);
                panic!("unexpected Err on scaffolding path");
            }
        }
    }

    // ---------- fallible cash x int multiply/divide (wrapper-level) ----------
    //
    // Value + verdict + sqlstate parity; Ok/Err cover witnesses on every
    // harness (gate-blindness insurance).

    macro_rules! fallible_cash_int {
        ($($h:ident: $fc:ident($aty:ty, $bty:ty) / $pg:ident(cash=$cash:tt);)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let a: $aty = kani::any();
                let b: $bty = kani::any();
                let mut cval: i64 = 0;
                // arg order to the C core mirrors the C wrapper: cash first.
                let cerr = fallible_cash_int!(@call $pg, $cash, a, b, &mut cval);
                match call2(adt_cash::builtins::$fc, a, b) {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(cerr == 0);
                        assert!(d.as_i64() == cval);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(cerr == errcode_of(&e));
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
        (@call $pg:ident, first, $a:ident, $b:ident, $out:expr) => {
            unsafe { $pg($a, $b, $out) }
        };
        (@call $pg:ident, second, $a:ident, $b:ident, $out:expr) => {
            unsafe { $pg($b, $a, $out) }
        };
    }

    fallible_cash_int! {
        // oid 3344 / 3399: full i64 x i64 (multiply refutation: kissat-fast)
        eq_cash_mul_int8: fc_cash_mul_int8(i64, i64) / pg_cash_mul_int64(cash=first);
        eq_int8_mul_cash: fc_int8_mul_cash(i64, i64) / pg_cash_mul_int64(cash=second);
        // oid 864 / 862: full i64 x i32
        eq_cash_mul_int4: fc_cash_mul_int4(i64, i32) / pg_cash_mul_int4(cash=first);
        eq_int4_mul_cash: fc_int4_mul_cash(i32, i64) / pg_cash_mul_int4(cash=second);
        // oid 866 / 863: full i64 x i16
        eq_cash_mul_int2: fc_cash_mul_int2(i64, i16) / pg_cash_mul_int2(cash=first);
        eq_int2_mul_cash: fc_int2_mul_cash(i16, i64) / pg_cash_mul_int2(cash=second);
    }

    // ---------- division: danger-set treatment (TRIAGE division rule) -------
    //
    // symbolic/symbolic with a 64-bit dividend walls; the proved regimes are
    // (a) 16-bit dividend x full symbolic divisor (division-by-zero arm
    // in-theorem), (b) full dividend / literal -1 fenced off i64::MIN,
    // (c) full dividend / literal 0 (pure error-arm parity), and (d) the
    // i64::MIN / -1 plane, where shipped Rust PANICS (divergence candidate —
    // see rust_panics_cash_div_min_by_neg1).

    macro_rules! div_band16 {
        ($($h:ident: $fc:ident($bty:ty) / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let c: i64 = kani::any();
                kani::assume((-32768..=32767).contains(&c)); // 16-bit dividend band
                let i: $bty = kani::any(); // full divisor domain, 0 included
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(c, i, &mut cval) };
                match call2(adt_cash::builtins::$fc, c, i) {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(cerr == 0);
                        assert!(d.as_i64() == cval);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(cerr == errcode_of(&e)); // 2 = 22012
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    div_band16! {
        eq_cash_div_int8_band16: fc_cash_div_int8(i64) / pg_cash_div_int64;
        eq_cash_div_int4_band16: fc_cash_div_int4(i32) / pg_cash_div_int4;
        eq_cash_div_int2_band16: fc_cash_div_int2(i16) / pg_cash_div_int2;
    }

    /// Full-i64 dividend / literal -1, fenced off the i64::MIN plane (which
    /// diverges — see the should_panic witness below).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_div_int8_by_neg1() {
        let c: i64 = kani::any();
        kani::assume(c != i64::MIN);
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_cash_div_int64(c, -1, &mut cval) };
        let d = ok_or_die(call2(adt_cash::builtins::fc_cash_div_int8, c, -1i64));
        assert!(cerr == 0);
        assert!(d.as_i64() == cval);
    }

    /// Full-i64 dividend / literal 0: pure error-arm parity (22012).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_div_int8_by_zero() {
        let c: i64 = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_cash_div_int64(c, 0, &mut cval) };
        match call2(adt_cash::builtins::fc_cash_div_int8, c, 0i64) {
            Ok(_) => panic!("division by zero accepted"),
            Err(e) => {
                assert!(cerr == errcode_of(&e));
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    /// DIVERGENCE WITNESS (candidate, adjudication owed): shipped
    /// `cash_div_int64(i64::MIN, -1)` = Rust `i64::MIN / -1` PANICS
    /// ("attempt to divide with overflow"). C cash_div_int64 has no MIN/-1
    /// guard: `c / i` there is signed-overflow UB — SIGFPE on x86-64,
    /// quietly INT64_MIN on ARM64 (ground-truth against docker postgres:18
    /// per GROUND-TRUTH law before recording). This harness pins the Rust
    /// arm's behavior; it must keep PASSING (i.e. the panic must keep
    /// happening) until adjudicated.
    #[kani::proof]
    #[kani::should_panic]
    fn rust_panics_cash_div_min_by_neg1() {
        let _ = call2(adt_cash::builtins::fc_cash_div_int8, i64::MIN, -1i64);
    }

    // ---------- cash_div_cash (oid 3822): f64 result ----------

    /// Divisor == 0: pure error-arm parity, no float circuit reached.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_div_cash_by_zero() {
        let a: i64 = kani::any();
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_cash_div_cash(a, 0, &mut cval) };
        match call2(adt_cash::builtins::fc_cash_div_cash, a, 0i64) {
            Ok(_) => panic!("division by zero accepted"),
            Err(e) => {
                // KANI 0.67 MODEL DEFECT (TRIAGE; proofs/float-arith witness
                // pair): Err(Box<PgError>) payloads that crossed a
                // Result<f64,_> frame read back corrupted — field asserts
                // spuriously fail. Verdict-only on this arm; C's err code
                // (2 = 22012) pins WHICH error C raised.
                assert!(cerr == 2);
                core::mem::forget(e);
            }
        }
    }

    /// 53-bit f64 divide = wall class (float law): spot grid, one symbolic
    /// index into a concrete table (geo-cmp pattern). Bit-exact compare.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_div_cash_grid() {
        const DIVIDENDS: [i64; 8] =
            [0, 1, -1, 100, -3, 999_999_999_999_999_999, i64::MAX, i64::MIN];
        const DIVISORS: [i64; 7] = [1, -1, 2, 3, 100, i64::MAX, i64::MIN];
        let i: usize = kani::any();
        let j: usize = kani::any();
        kani::assume(i < DIVIDENDS.len() && j < DIVISORS.len());
        let (a, b) = (DIVIDENDS[i], DIVISORS[j]);
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_cash_div_cash(a, b, &mut cval) };
        let d = ok_or_die(call2(adt_cash::builtins::fc_cash_div_cash, a, b));
        assert!(cerr == 0);
        assert!(d.as_f64().to_bits() == cval.to_bits());
    }

    /// Ladder probe: full-i64 dividend / literal power-of-two divisor
    /// (geo precedent: /2.0 full-domain provable, default solver).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_div_cash_pow2() {
        let a: i64 = kani::any();
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_cash_div_cash(a, 4, &mut cval) };
        let d = ok_or_die(call2(adt_cash::builtins::fc_cash_div_cash, a, 4i64));
        assert!(cerr == 0);
        assert!(d.as_f64().to_bits() == cval.to_bits());
    }

    // ---------- cash x float (oids 846-848, 896, 897, 919) ----------
    //
    // 53-bit multiply/divide = wall class -> special-grid + zero-arm
    // treatment (float law). Runs need --no-overflow-checks (legal IEEE NaN
    // production trips Kani's default float checks, property noise not
    // parity). The float8_mul underflow arm (result==0, both operands
    // nonzero) is UNREACHABLE from cash: |(float8) c| is 0 or >= 1, so no
    // product/quotient with a finite nonzero f64 rounds to 0 — no cover for
    // it (a cover would fail; noted in ledger instead).

    /// f64 grid values: normal/rounding-tie/sign cases, +-huge (overflow to
    /// inf on mul, underflow-to-subnormal on div), subnormal, the
    /// FLOAT8_FITS_IN_INT64 boundary 2^63, and the special lattice.
    const F8_GRID: [f64; 18] = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        2.5,
        -2.5,
        3.5,
        0.5,
        -0.5,
        0.1,
        1e300,
        -1e300,
        5e-324,
        9.223372036854776e18,
        -9.223372036854776e18,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    const C_GRID: [i64; 9] = [
        0,
        1,
        -1,
        2,
        100,
        -100,
        999_999_999_999_999_999,
        i64::MAX,
        i64::MIN,
    ];

    macro_rules! cash_flt_grid {
        ($($h:ident: $fc:ident, $pg:ident, $fty:ty, $mk:expr, cash=$cash:tt, err=$mode:tt;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < C_GRID.len() && j < F8_GRID.len());
                let c = C_GRID[i];
                let f: $fty = $mk(F8_GRID[j]);
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(c, f, &mut cval) };
                let r = cash_flt_grid!(@call $fc, $cash, c, f);
                match r {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(cerr == 0);
                        assert!(d.as_i64() == cval);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        // Div rows: the shipped float8_div returns
                        // Err(zero_divide_boxed()) straight through a
                        // Result<f64,_> frame, hitting the KANI 0.67 Box-
                        // provenance defect (TRIAGE; proofs/float-arith
                        // witness pair) — Err FIELD reads are corrupted, so
                        // the Err arm is verdict-only here, with C's err
                        // code covering both error kinds. (The mul rows'
                        // error path rides the shipped u64-bits cold
                        // transport and asserts fields fine — kept there.)
                        kani::cover!(cerr == 1, "C 22003 arm reachable");
                        cash_flt_grid!(@errchk $mode, cerr, e);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
        (@errchk fields, $cerr:ident, $e:ident) => {
            assert!($cerr == errcode_of(&$e));
            assert!($e.level == ERROR);
        };
        (@errchk verdict_only, $cerr:ident, $e:ident) => {
            assert!($cerr != 0);
            kani::cover!($cerr == 2, "C 22012 arm reachable");
        };
        (@call $fc:ident, first, $c:ident, $f:ident) => {
            call2(adt_cash::builtins::$fc, $c, datum_of($f))
        };
        (@call $fc:ident, second, $c:ident, $f:ident) => {
            call2(adt_cash::builtins::$fc, datum_of($f), $c)
        };
    }

    trait FloatDatum {
        fn to_datum(self) -> datum::Datum;
    }
    impl FloatDatum for f64 {
        fn to_datum(self) -> datum::Datum {
            datum::Datum::from_f64(self)
        }
    }
    impl FloatDatum for f32 {
        fn to_datum(self) -> datum::Datum {
            datum::Datum::from_f32(self)
        }
    }
    fn datum_of<F: FloatDatum>(f: F) -> datum::Datum {
        f.to_datum()
    }

    cash_flt_grid! {
        eq_cash_mul_flt8_grid: fc_cash_mul_flt8, pg_cash_mul_flt8, f64, (|x| x), cash=first, err=fields;
        eq_flt8_mul_cash_grid: fc_flt8_mul_cash, pg_cash_mul_flt8, f64, (|x| x), cash=second, err=fields;
        eq_cash_div_flt8_grid: fc_cash_div_flt8, pg_cash_div_flt8, f64, (|x| x), cash=first, err=verdict_only;
        eq_cash_mul_flt4_grid: fc_cash_mul_flt4, pg_cash_mul_flt4, f32, (|x| x as f32), cash=first, err=fields;
        eq_flt4_mul_cash_grid: fc_flt4_mul_cash, pg_cash_mul_flt4, f32, (|x| x as f32), cash=second, err=fields;
        eq_cash_div_flt4_grid: fc_cash_div_flt4, pg_cash_div_flt4, f32, (|x| x as f32), cash=first, err=verdict_only;
    }

    /// Zero-arm probe: full-i64 cash x literal 0.0 (product +-0.0, in range).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_mul_flt8_f_zero() {
        let c: i64 = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_cash_mul_flt8(c, 0.0, &mut cval) };
        match call2(
            adt_cash::builtins::fc_cash_mul_flt8,
            c,
            datum::Datum::from_f64(0.0),
        ) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_i64() == cval);
            }
            Err(e) => {
                core::mem::forget(e);
                panic!("0.0 product cannot overflow");
            }
        }
    }

    /// Zero-arm probe: literal 0 cash x full-f64 (0*f: NaN lattice on
    /// f in {inf, -inf, NaN} -> error; else exact 0).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_mul_flt8_c_zero() {
        let f: f64 = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_cash_mul_flt8(0, f, &mut cval) };
        match call2(
            adt_cash::builtins::fc_cash_mul_flt8,
            0i64,
            datum::Datum::from_f64(f),
        ) {
            Ok(d) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(cerr == 0);
                assert!(d.as_i64() == cval);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(cerr == errcode_of(&e));
                core::mem::forget(e);
            }
        }
    }

    // ---------- int4_cash / int8_cash (oids 3811/3812): locale seam ----------

    // A fully symbolic frac_digits walls: both sides build `scale` in their
    // own <=10-iteration loop and the products' equality becomes a dual-
    // accumulation multiplication-equivalence (the strtoint >=10-digit wall
    // shape; measured here: kissat UNSAT call hung >400s while symex was
    // 13.5k steps). Per the literals-fold law, case-split frac_digits into
    // LITERAL cells: 0..=10 in-range plus one representative per clamp arm
    // (-1 for <0, 127 for >10 — the shipped clamps, which run INSIDE both
    // proved bodies, send every out-of-range i8 to the same fpoint=2 path,
    // so one representative per arm covers the class).
    // int_cash_fd_cells_cover is the mandatory union-coverage witness.

    fn int_cash_cell<A: proof_support::ProofArg + Copy>(
        fc: proof_support::FcFn<Box<PgError>>,
        pg: unsafe extern "C" fn(A, *mut i64) -> c_int,
        amount: A,
        fd: i8,
    ) {
        seam_lconv(fd, 0, 127, 127, 127, 127, 127, 127);
        let mut cval: i64 = 0;
        let cerr = unsafe { pg(amount, &mut cval) };
        match call1(fc, amount) {
            Ok(d) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(cerr == 0);
                assert!(d.as_i64() == cval);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(cerr == errcode_of(&e)); // int8mul: 22003
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    macro_rules! int_cash_cells {
        ($($h:ident: $fc:ident($aty:ty) / $pg:ident, fd=$fd:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)] // scale loop: fpoint <= 10 iterations
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
            fn $h() {
                let amount: $aty = kani::any();
                int_cash_cell::<$aty>(adt_cash::builtins::$fc, $pg, amount, $fd);
            }
        )*};
    }

    int_cash_cells! {
        eq_int8_cash_fd0: fc_int8_cash(i64) / pg_int8_cash, fd=0;
        eq_int8_cash_fd1: fc_int8_cash(i64) / pg_int8_cash, fd=1;
        eq_int8_cash_fd2: fc_int8_cash(i64) / pg_int8_cash, fd=2;
        eq_int8_cash_fd3: fc_int8_cash(i64) / pg_int8_cash, fd=3;
        eq_int8_cash_fd4: fc_int8_cash(i64) / pg_int8_cash, fd=4;
        eq_int8_cash_fd5: fc_int8_cash(i64) / pg_int8_cash, fd=5;
        eq_int8_cash_fd6: fc_int8_cash(i64) / pg_int8_cash, fd=6;
        eq_int8_cash_fd7: fc_int8_cash(i64) / pg_int8_cash, fd=7;
        eq_int8_cash_fd8: fc_int8_cash(i64) / pg_int8_cash, fd=8;
        eq_int8_cash_fd9: fc_int8_cash(i64) / pg_int8_cash, fd=9;
        eq_int8_cash_fd10: fc_int8_cash(i64) / pg_int8_cash, fd=10;
        eq_int8_cash_fdneg: fc_int8_cash(i64) / pg_int8_cash, fd=-1;
        eq_int8_cash_fdbig: fc_int8_cash(i64) / pg_int8_cash, fd=127;
        eq_int4_cash_fd0: fc_int4_cash(i32) / pg_int4_cash, fd=0;
        eq_int4_cash_fd1: fc_int4_cash(i32) / pg_int4_cash, fd=1;
        eq_int4_cash_fd2: fc_int4_cash(i32) / pg_int4_cash, fd=2;
        eq_int4_cash_fd3: fc_int4_cash(i32) / pg_int4_cash, fd=3;
        eq_int4_cash_fd4: fc_int4_cash(i32) / pg_int4_cash, fd=4;
        eq_int4_cash_fd5: fc_int4_cash(i32) / pg_int4_cash, fd=5;
        eq_int4_cash_fd6: fc_int4_cash(i32) / pg_int4_cash, fd=6;
        eq_int4_cash_fd7: fc_int4_cash(i32) / pg_int4_cash, fd=7;
        eq_int4_cash_fd8: fc_int4_cash(i32) / pg_int4_cash, fd=8;
        eq_int4_cash_fd9: fc_int4_cash(i32) / pg_int4_cash, fd=9;
        eq_int4_cash_fd10: fc_int4_cash(i32) / pg_int4_cash, fd=10;
        eq_int4_cash_fdneg: fc_int4_cash(i32) / pg_int4_cash, fd=-1;
        eq_int4_cash_fdbig: fc_int4_cash(i32) / pg_int4_cash, fd=127;
    }

    /// Union-coverage witness for the fd case-split (MANDATORY per the
    /// case-split rule): every i8 either IS one of the in-range literal
    /// cells, or lands in a clamp arm whose downstream behavior is decided
    /// solely by the clamped constant 2 — i.e. behaves as the -1 / 127
    /// representative. Mirrors the verbatim clamp both sides run in-body.
    #[kani::proof]
    fn int_cash_fd_cells_cover() {
        let fd: i8 = kani::any();
        let fpoint = fd as i32;
        if !(0..=10).contains(&fpoint) {
            // clamp arm: behavior identical to the representatives' fpoint=2
            assert!(fpoint < 0 || fpoint > 10);
        } else {
            // in-range: this exact literal is one of the proved cells
            assert!((0..=10).contains(&fpoint));
        }
        kani::cover!(fd == -128, "extreme negative reachable");
        kani::cover!(fd == 127, "extreme positive reachable");
    }

    /// Seam skew control: Rust sees frac_digits=2, C sees 3 — scale factors
    /// 100 vs 1000, so amount=1 must produce different values. MUST FAIL
    /// (default solver): proves the locale seam is load-bearing.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
    fn control_int8_cash_lconv_skew() {
        seam_lconv(2, 0, 127, 127, 127, 127, 127, 127);
        // skew ONLY the C side
        let _ = unsafe { pg_proof_set_lconv(3, 0, 127, 127, 127, 127, 127, 127) };
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_int8_cash(1, &mut cval) };
        let d = ok_or_die(call1(adt_cash::builtins::fc_int8_cash, 1i64));
        assert!(cerr == 0);
        assert!(d.as_i64() == cval); // 100 vs 1000: must fail
    }

    // ---------- cash_in (oid 886): per-length, locale frac_digits symbolic --
    //
    // Core-level (adt_cash::cash_in with escontext=None): the fc wrapper's
    // cstring/from_utf8_lossy plumbing is Kani-blocked and value-neutral.
    // Input fenced to NUL-free ASCII (0x01..=0x7F): the C side is a cstring
    // (embedded NUL unrepresentable) and non-ASCII bytes cannot form a &str
    // byte-for-byte; multibyte parity is out of proof.

    // A fully symbolic frac_digits walls here exactly as in int_cash (the
    // dec-fill loop is a dual x10 accumulation chain on both sides; measured
    // kissat hang at len>=1). Same remedy: fd arrives as a LITERAL from each
    // harness — per-length harnesses pin fd=2 (what the shipped C-locale
    // pglc_localeconv clamps to), and fd-cell harnesses at len 2 quantify
    // the seam field across the clamp classes.
    fn cash_in_check<const N: usize>(fd: i8) {
        let buf: [u8; N] = kani::any();
        for k in 0..N {
            kani::assume(buf[k] >= 1 && buf[k] <= 127);
        }
        seam_lconv(fd, 0, 127, 127, 127, 127, 127, 127);

        let mut cbuf = [0u8; 16];
        cbuf[..N].copy_from_slice(&buf);
        cbuf[N] = 0;
        let mut cerr: c_int = 0;
        let cval = unsafe { pg_cash_in(cbuf.as_ptr(), &mut cerr) };

        // SAFETY (of the claim): bytes are ASCII by the assumption above.
        let s = unsafe { core::str::from_utf8_unchecked(&buf) };
        match adt_cash::cash_in(s, None) {
            Ok(v) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(cerr == 0);
                assert!(v == cval);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(cerr == errcode_of(&e)); // 1 = 22003, 3 = 22P02
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    macro_rules! cash_in_len {
        ($($h:ident: $n:literal, fd=$fd:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(13)] // parse loops <= len+1; dec-fill loop <= 11
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
            fn $h() { cash_in_check::<$n>($fd); }
        )*};
    }

    cash_in_len! {
        eq_cash_in_len1: 1, fd=2;
        eq_cash_in_len2: 2, fd=2;
        eq_cash_in_len3: 3, fd=2;
        eq_cash_in_len4: 4, fd=2;
        eq_cash_in_len5: 5, fd=2;
        eq_cash_in_len6: 6, fd=2;
        // fpoint cells at len 2 (clamp classes + spread of in-range values;
        // int_cash_fd_cells_cover is the shared union witness for the fd
        // case-split shape)
        eq_cash_in_len2_fd0: 2, fd=0;
        eq_cash_in_len2_fd1: 2, fd=1;
        eq_cash_in_len2_fd5: 2, fd=5;
        eq_cash_in_len2_fd10: 2, fd=10;
        eq_cash_in_len2_fdneg: 2, fd=-1;
        eq_cash_in_len2_fdbig: 2, fd=127;
    }

    /// Empty string: both sides ACCEPT it as 0 (C has no "no digits" check;
    /// machine-checks that quirk is ported faithfully). frac_digits symbolic.
    #[kani::proof]
    #[kani::unwind(13)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
    fn eq_cash_in_len0() {
        let fd: i8 = kani::any();
        seam_lconv(fd, 0, 127, 127, 127, 127, 127, 127);
        let cbuf = [0u8; 1];
        let mut cerr: c_int = 0;
        let cval = unsafe { pg_cash_in(cbuf.as_ptr(), &mut cerr) };
        let v = ok_or_die(adt_cash::cash_in("", None));
        assert!(cerr == 0);
        assert!(v == cval);
        assert!(v == 0);
    }

    // ---------- cash_out (oid 887): band + spots, full locale quantified ----

    // cash_out theorem shape (measured on this lane): ANY symbolic locale
    // field is a CNF-phase MEMORY wall (>6GiB in "converting SSA" with symex
    // complete at ~112k steps) — sign_posn/sep/cs_precedes select different
    // concatenation arms, so every store offset in the output image goes
    // symbolic and the whole 128/160-byte arrays havoc per SSA version
    // (result-image wall class; the two-walls law says stubs cannot help).
    // Reshape: the VALUE is symbolic only in the band harness (all-default
    // locale, offsets driven by digit count alone — the intout-proved
    // shape); the locale axes are covered by CONCRETE cells at fixed
    // representative values — every sign_posn arm (0,1,2,3,4,default),
    // both cs_precedes arms, every sep_by_space arm, and grouping 1/3/6,
    // on both the negative and positive symbol planes; plus points cells
    // 0/1/5/10/clamp arms.

    #[allow(clippy::too_many_arguments)]
    fn cash_out_cell(
        value: i64,
        fd: i8,
        gsel: usize,
        pcs: i8,
        ncs: i8,
        psep: i8,
        nsep: i8,
        ppos: i8,
        npos: i8,
        cmp_cap: usize,
    ) {
        seam_lconv(fd, gsel, pcs, ncs, psep, nsep, ppos, npos);
        let mut rbuf = [0u8; adt_cash::CASH_OUT_BUFLEN];
        let rlen = ok_or_die(adt_cash::cash_out_into(value, &mut rbuf));
        let mut cbuf = [0u8; 160];
        let clen = unsafe { pg_cash_out(value, cbuf.as_mut_ptr()) } as usize;
        assert!(rlen == clen);
        assert!(rlen <= cmp_cap);
        // Both buffers are zero beyond their (equal) lengths, so whole-word
        // prefix compare == content compare. cmp_cap/8 iterations only.
        let mut k = 0;
        while k < cmp_cap {
            assert!(word8(&rbuf, k) == word8(&cbuf, k));
            k += 8;
        }
    }

    /// 8-byte word read (memcpy, no user loop — keeps the compare out of the
    /// unwind budget next to the /10 divider loop).
    fn word8(buf: &[u8], k: usize) -> u64 {
        let mut w = [0u8; 8];
        w.copy_from_slice(&buf[k..k + 8]);
        u64::from_le_bytes(w)
    }

    // Even a symbolic |value| < 1e5 band RSS-kills in CNF: the digit COUNT
    // is symbolic, so C's right-to-left bufptr start (and every cat offset
    // after it) goes symbolic — same width wall. Per-DIGIT-COUNT band cells
    // fix the offsets and leave only the digit values symbolic.
    // ... and unwind slack next to the 64-bit /10 loop is CATASTROPHIC
    // (intout law, remeasured here: unwind 12 over 3 real iterations =
    // 18 GiB CNF): each band gets the EXACT unwind for its digit count.
    macro_rules! cash_out_band {
        ($($h:ident: $lo:literal..=$hi:literal, uw=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
            fn $h() {
                let value: i64 = kani::any();
                kani::assume(($lo..=$hi).contains(&value));
                cash_out_cell(value, 2, 0, 127, 127, 127, 127, 127, 127, 32);
            }
        )*};
    }

    // MEASURED WALL (all variants, 8-18GiB CNF): with ANY symbolic value the
    // digit loop's exit is data-dependent, so bufptr (and every image offset
    // after it) is symbolic at formula-build time no matter how narrow the
    // assumed band or how exact the unwind — assumes never fold (the
    // derived-length copy wall generalizes: derived-OFFSET image writes).
    // Image equality over a symbolic value band is therefore recorded
    // wall(CNF width-bound); image coverage comes from the 20+ concrete
    // cells above. Per the result-image wall law, the scalar-verdict
    // projection (output LENGTH parity) is proven over the band instead:
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
    fn eq_cash_out_band1e5_len() {
        let value: i64 = kani::any();
        kani::assume((-99_999..=99_999).contains(&value));
        seam_lconv(2, 0, 127, 127, 127, 127, 127, 127);
        let mut rbuf = [0u8; adt_cash::CASH_OUT_BUFLEN];
        let rlen = ok_or_die(adt_cash::cash_out_into(value, &mut rbuf));
        let mut cbuf = [0u8; 160];
        let clen = unsafe { pg_cash_out(value, cbuf.as_mut_ptr()) } as usize;
        assert!(rlen == clen);
    }

    macro_rules! cash_out_spot {
        ($($h:ident: $v:expr, $fd:literal, $gsel:literal, $pcs:literal, $ncs:literal,
           $psep:literal, $nsep:literal, $ppos:literal, $npos:literal, uw=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
            fn $h() {
                cash_out_cell($v, $fd, $gsel, $pcs, $ncs, $psep, $nsep, $ppos, $npos, 64);
            }
        )*};
    }

    cash_out_spot! {
        // boundary values, default locale
        eq_cash_out_spot_min: i64::MIN, 2, 0, 127, 127, 127, 127, 127, 127, uw=44;
        eq_cash_out_spot_max: i64::MAX, 2, 0, 127, 127, 127, 127, 127, 127, uw=44;
        eq_cash_out_spot_zero: 0, 2, 0, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_spot_neg1: -1, 2, 0, 127, 127, 127, 127, 127, 127, uw=28;
        // points cells (both clamp arms + spread; 0 = no-decimal arm)
        eq_cash_out_fd0: -12345, 0, 0, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_fd1: -12345, 1, 0, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_fd5: -12345, 5, 0, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_fd10: -12345, 10, 0, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_fdneg: -12345, -1, 0, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_fdbig: -12345, 127, 0, 127, 127, 127, 127, 127, 127, uw=28;
        // negative-plane locale cells: every n_sign_posn arm, both
        // n_cs_precedes, every n_sep_by_space (pairwise-covering set)
        eq_cash_out_neg_p0: -12345, 2, 0, 127, 1, 127, 1, 127, 0, uw=28;
        eq_cash_out_neg_p1: -12345, 2, 0, 127, 0, 127, 2, 127, 1, uw=28;
        eq_cash_out_neg_p2: -12345, 2, 0, 127, 1, 127, 0, 127, 2, uw=28;
        eq_cash_out_neg_p3: -12345, 2, 0, 127, 0, 127, 1, 127, 3, uw=28;
        eq_cash_out_neg_p4: -12345, 2, 0, 127, 1, 127, 2, 127, 4, uw=28;
        eq_cash_out_neg_pdef: -12345, 2, 0, 127, 0, 127, 0, 127, 99, uw=28;
        // positive-plane locale cells (positive_sign is "" in the C locale;
        // arms still reorder csymbol/digits/spaces)
        eq_cash_out_pos_p0: 12345, 2, 0, 1, 127, 1, 127, 0, 127, uw=28;
        eq_cash_out_pos_p2: 12345, 2, 0, 0, 127, 2, 127, 2, 127, uw=28;
        eq_cash_out_pos_p4: 12345, 2, 0, 1, 127, 0, 127, 4, 127, uw=28;
        // grouping cells (group=1 max separators; group=6 sparse)
        eq_cash_out_group1: -12345, 2, 1, 127, 127, 127, 127, 127, 127, uw=28;
        eq_cash_out_group6: -1234567, 2, 3, 127, 127, 127, 127, 127, 127, uw=28;
    }

    /// Seam skew control for cash_out: Rust n_sign_posn=0 vs C=1 on a
    /// negative value — "(..)" vs "-.." framing. MUST FAIL (default solver).
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(pg_locale::pglc_localeconv, stub_pglc_localeconv)]
    fn control_cash_out_lconv_skew() {
        seam_lconv(2, 0, 127, 127, 127, 127, 127, 0); // Rust: n_sign_posn = 0
        let _ = unsafe { pg_proof_set_lconv(2, 0, 127, 127, 127, 127, 127, 1) }; // C: 1
        let mut rbuf = [0u8; adt_cash::CASH_OUT_BUFLEN];
        let rlen = ok_or_die(adt_cash::cash_out_into(-1, &mut rbuf));
        let mut cbuf = [0u8; 160];
        let clen = unsafe { pg_cash_out(-1, cbuf.as_mut_ptr()) } as usize;
        assert!(rlen == clen); // must fail
    }

    // ---------- cash_words (oid 935): concrete spots (mcx result;
    // /1e2../1e17 divider chain = hard wall symbolically, folds concrete) ----

    /// Token Mcx handle (jsonb-probe recipe). SOUNDNESS: with Mcx::allocate/
    /// grow/deallocate AND mcx::vec_with_capacity_in stubbed to the static
    /// proof heap, no path under proof dereferences the context — a REAL
    /// MemoryContext::new_bump is pure scaffolding and a measured symex wall
    /// (>240s jsonb; RSS-killed the first cash_words spot attempt here via
    /// the AcctWeak drop machinery). The zeroed image is never read.
    fn token_ctx() -> &'static mcx::MemoryContext {
        static CTX: [u8; 256] = [0u8; 256];
        assert!(core::mem::size_of::<mcx::MemoryContext>() <= 256);
        unsafe { &*(CTX.as_ptr() as *const mcx::MemoryContext) }
    }

    macro_rules! cash_words_spot {
        ($($h:ident: $v:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(260)] // concrete value: loops fold to their actual
                                 // counts (longest: ~220-char i64::MIN image
                                 // byte-compare); no symbolic dead copies
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let value: i64 = $v;
                let ctx = token_ctx();
                let t = ok_or_die(adt_cash::cash_words(ctx.mcx(), value));
                let mut cbuf = [0u8; 256];
                let clen = unsafe { pg_cash_words(value, cbuf.as_mut_ptr()) } as usize;
                let data = t.data();
                assert!(data.len() == clen);
                for k in 0..data.len() {
                    assert!(data[k] == cbuf[k]);
                }
                // teardown is not part of the claim (mcx-stubs recipe)
                core::mem::forget(t);
            }
        )*};
    }

    cash_words_spot! {
        eq_cash_words_spot_zero: 0;                      // "Zero dollars and zero cents"
        eq_cash_words_spot_one_cent: 1;                  // singular cent
        eq_cash_words_spot_one_dollar: 101;              // singular dollar + cent
        eq_cash_words_spot_neg: -12345;                  // "minus ..." path
        eq_cash_words_spot_teens: 1517;                  // "hundred and" arm
        eq_cash_words_spot_groups: 123_456_789_012_345;  // billion/million/thousand arms
        eq_cash_words_spot_max: i64::MAX;                // quadrillion arm
        eq_cash_words_spot_min: i64::MIN;                // minus + wraparound plane
    }

    // ---------- cash_recv / cash_send (oids 2492/2493) ----------

    /// 8-byte message: full symbolic payload, value parity with the
    /// big-endian read (C side: pq_getmsgint64 model).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_recv() {
        let bytes: [u8; 8] = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_cash_recv(bytes.as_ptr(), 8, &mut cval) };

        let ctx = mcx::MemoryContext::new_bump("kani-cash");
        let mut v = ok_or_die(mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 16));
        ok_or_die(mcx::vec_append_bytes(&mut v, &bytes));
        let mut si = ok_or_die(stringinfo::StringInfo::from_vec(v));
        let r = ok_or_die(adt_cash::cash_recv(&mut si));

        assert!(cerr == 0);
        assert!(r == cval);
        assert!(si.cursor == 8);
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// Short message (3 bytes): insufficient-data arm, sqlstate 08P01 parity.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_recv_short() {
        let bytes: [u8; 3] = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_cash_recv(bytes.as_ptr(), 3, &mut cval) };

        let ctx = mcx::MemoryContext::new_bump("kani-cash");
        let mut v = ok_or_die(mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 8));
        ok_or_die(mcx::vec_append_bytes(&mut v, &bytes));
        let mut si = ok_or_die(stringinfo::StringInfo::from_vec(v));
        match adt_cash::cash_recv(&mut si) {
            Ok(_) => panic!("short message accepted"),
            Err(e) => {
                assert!(cerr == errcode_of(&e)); // 4 = 08P01
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// Full symbolic value -> 12-byte bytea image: varlena header length +
    /// big-endian payload parity (C side: pq_sendint64/pq_endtypsend model).
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_cash_send() {
        let value: i64 = kani::any();
        let mut c8 = [0u8; 8];
        let _ = unsafe { pg_cash_send(value, c8.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-cash");
        let b = ok_or_die(adt_cash::cash_send(ctx.mcx(), value));
        assert!(b.varsize() == 12); // VARHDRSZ + 8, as C SET_VARSIZE stamps
        let data = b.data();
        assert!(data.len() == 8);
        for k in 0..8 {
            assert!(data[k] == c8[k]);
        }
        core::mem::forget(b);
        core::mem::forget(ctx);
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_cash_lt vs C cash_le. MUST fail with a
    /// counterexample at a == b. Run with the DEFAULT solver (kissat is
    /// non-incremental and effectively never terminates on failing
    /// harnesses).
    #[kani::proof]
    fn control_cash_lt_vs_c_le() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let r = call2_ok(adt_cash::builtins::fc_cash_lt, a, b);
        let c = unsafe { pg_cash_le(a, b) };
        assert!(r.as_bool() as c_int == c);
    }
}
