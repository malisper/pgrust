//! Kani C≡Rust equivalence: float unary + arithmetic operators (27 pg_proc
//! rows: oids 202-207, 216-221, 279-286, 1394/1395, 1608-1610, 1913/1914)
//! + the ROUNDING/CLASSIFICATION wave (2026-07-28): dround 228/1342,
//! dtrunc 229/1343, dceil 2308/2320, dfloor 2309, dsign 2310, dsqrt
//! 230/1344 (probe), in_range_float8_float8 4139, in_range_float4_float8
//! 4140.
//!
//! ROUNDING-WAVE NOTES (harnesses eq_dround/dceil/dfloor/dsign/dtrunc,
//! eq_dsqrt + spots_dsqrt, eq_in_range_float8_float8/float4_float8,
//! controls control_dtrunc_vs_c_round + control_in_range_f8_vs_noreject —
//! compile-gated only, NOT yet solved; see runqueue.txt):
//! - rounding five + in_range pair: full-domain expected-GREEN (rint/ceil/
//!   floor/compare + one f64 add/sub — no 53-bit multiply/divide).
//! - dsqrt: probe (sqrt at 53-bit width, CBMC-native); spots_dsqrt is the
//!   standing fallback grid, VERDICT-ONLY on its constant-reachable Err arm
//!   (Kani Err(Box) f64 defect below); eq_dsqrt's symbolic Err arm keeps
//!   sqlstate 2201F/22003 parity in-theorem.
//! - CANONICAL-NAN SHIM: screened NOT NEEDED — no vendored section reaches
//!   the NAN macro or get_float8_nan (propagation-only; models correctly).
//!   tests/semantics_check.rs pins canonical-NaN propagation natively.
//! - No loops anywhere: no unwind bounds needed.
//!
//! FLOAT-ARITHMETIC COST PROBE: the float-cmp family proved comparison
//! circuits cheap (0.1-0.3s); this crate measures the ARITHMETIC circuits
//! (add/sub/mul/div at 32 and 64 bits) — the per-harness solve times are a
//! primary deliverable calibrating TRIAGE.md's "float arithmetic = wall"
//! entry.
//!
//! Rust side: the SHIPPED fmgr wrappers — `adt_float::builtins::fc_float4abs`
//! .. `fc_float84div`, `fc_degrees`, `fc_radians`, `fc_dpi` — invoked through
//! a real `LocalFcinfo` frame, so each proof covers datum unwrap → core →
//! Datum::from_f32/from_f64. C side: vendored REL_18_STABLE float.c wrappers
//! + float.h arithmetic inlines (c/pg_float_arith.c).
//!
//! Fallible ops (pl/mi/mul/div/degrees/radians) prove the cash pattern:
//! VERDICT parity over the full symbolic domain — Rust Ok ⇔ C no-ereport
//! with bit-identical values, and on the Err arm sqlstate parity against the
//! C error taxonomy (overflow/underflow → 22003
//! ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, zero-divide → 22012
//! ERRCODE_DIVISION_BY_ZERO; the sqlstates are applied by the SHIPPED
//! with_sqlstate calls, not the stub). Error MESSAGE plumbing is stubbed
//! (`PgError::error` → field-identical constructor minus
//! `Location::caller()`/message text, which Kani cannot execute):
//! value-space + verdict + sqlstate are in the theorem, message text is not.
//! C's overflow vs underflow distinction (flag 1 vs 2) maps to the same
//! sqlstate; the harness distinguishes them anyway so a taxonomy swap would
//! still be caught... via the sqlstate only where they differ (zero-divide).
//!
//! Results are floats: compared by to_bits() — bit-exact, so -0 vs +0,
//! NaN payload propagation, and denormal rounding are inside the theorem.
//!
//! Domains: FULLY SYMBOLIC f32/f64 (every NaN payload, ±Inf, ±0,
//! denormals). No assumes: the operators are total (both sides adjudicate
//! overflow rather than trap). Where a fully-symbolic pair WALLS (>30s),
//! the op keeps a `spots_*` harness instead: a symbolically-indexed sweep
//! over the special-value grid (±0, ±Inf, ±NaN payloads, denormal
//! min/max, normal min/max, ±1, ±2, 0.5) × the same grid — every special
//! pair is in the theorem, the open-region remainder is recorded as wall.
//! Div at f8 width additionally gets a fully-symbolic ZERO-FENCED harness
//! (divisor assumed ±0: the early-return zero-divide adjudication carries
//! no divide circuit and proves cheap at full width) — payload reads there
//! are verdict-only because of the Kani defect witnessed below; the
//! sqlstate/level parity of the same shared zero arm is proven at f4
//! (eq_float4div_zero).
//!
//! Every fallible harness carries kani::cover! vacuity witnesses on both
//! arms (SATISFIED = arm genuinely explored). The mixed float48/84 pl/mi
//! Err covers are UNREACHABLE — correctly: (f64)f32 ± f64 cannot overflow
//! from finite inputs (3.4e38 is far below f64::MAX's half-ulp ~1e292), and
//! the C side agrees (verdict parity asserts both directions regardless).
//!
//! RUN: every harness needs
//!   timeout 30 cargo kani -Z c-ffi -Z stubbing \
//!     --c-lib c/pg_float_arith.c --harness <h> --solver kissat \
//!     --no-overflow-checks
//! `--no-overflow-checks` is REQUIRED: it disables CBMC's automatic
//! "NaN on addition/…" property checks, which fire on BOTH sides' ordinary
//! IEEE arithmetic (NaN production is legal Postgres semantics here, and
//! the shimmed C deliberately continues past flagged errors). It does not
//! weaken the parity theorem (our own asserts carry it).
//!
//! MEASURED COST CURVE (kissat, M-series laptop, 2026-07-28; 30s cap):
//!   unary (abs/um/up both widths, dpi)      0.13-0.15s  GREEN
//!   f32 add/sub (pl/mi)                     ~6-7s       GREEN
//!   f32 mul                                 ~13-14s     GREEN
//!   f64/mixed add/sub (8pl/8mi/48/84)       ~12-20s     GREEN
//!   f64 mul (8mul/48mul/84mul)              >30s        WALL
//!   div, any width, full symbolic           >30s        WALL
//!   ×/÷ by CONSTANT at f64 (degrees/radians)>30s        WALL
//!   half-symbolic mul f32 (sym × 16-grid)   ~21s        GREEN
//!   half-symbolic mul/div f64, div f32      >30s        WALL
//!   spots grids (16×16 specials) mul, 4div  10-22s      GREEN
//!   zero-fenced div (b==±0, all widths)     1.4-5s      GREEN
//! Boundary: the 24-bit significand multiplier is the largest affordable
//! float circuit; 53-bit multiply/divide (and anything containing one,
//! including ×constant) is past the 30s wall. Walled harnesses are kept
//! below for re-probing (a suite runner must skip: eq_float4div,
//! eq_float8mul, eq_float8div, eq_float48mul, eq_float48div, eq_float84mul,
//! eq_float84div, eq_degrees, eq_radians, half_float4div, half_float8mul,
//! half_float8div — all confirmed >30s on BOTH kissat and default).
//!
//! Negative controls (run with the DEFAULT solver; kissat never terminates
//! on failing harnesses):
//!   - control_float4um_vs_c_abs: shipped fc_float4um vs C fabsf — MUST
//!     fail (any x with -x != |x| bitwise, e.g. x = +0).
//!   - control_float4pl_vs_ieee: shipped fc_float4pl vs plain IEEE + with
//!     no overflow ereport — MUST fail on the finite+finite=Inf arm,
//!     witnessing that the solver actually explores the error arm.
//!   - kani_defect_witness_f64 MUST fail / kani_defect_control_f32 MUST
//!     pass (see the witness-pair comment below).

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use types_error::{
        PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INTERNAL_ERROR,
        ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
        ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION,
        ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
    };
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        // unary, infallible
        fn pg_float4abs(a: f32) -> f32;
        fn pg_float4um(a: f32) -> f32;
        fn pg_float4up(a: f32) -> f32;
        fn pg_float8abs(a: f64) -> f64;
        fn pg_float8um(a: f64) -> f64;
        fn pg_float8up(a: f64) -> f64;
        fn pg_dpi() -> f64;

        // fallible arithmetic: returns 0 ok / 1 overflow / 2 underflow /
        // 3 zero-divide; value via *out (valid only when 0).
        fn pg_float4pl(a: f32, b: f32, out: *mut f32) -> c_int;
        fn pg_float4mi(a: f32, b: f32, out: *mut f32) -> c_int;
        fn pg_float4mul(a: f32, b: f32, out: *mut f32) -> c_int;
        fn pg_float4div(a: f32, b: f32, out: *mut f32) -> c_int;
        fn pg_float8pl(a: f64, b: f64, out: *mut f64) -> c_int;
        fn pg_float8mi(a: f64, b: f64, out: *mut f64) -> c_int;
        fn pg_float8mul(a: f64, b: f64, out: *mut f64) -> c_int;
        fn pg_float8div(a: f64, b: f64, out: *mut f64) -> c_int;
        fn pg_float48pl(a: f32, b: f64, out: *mut f64) -> c_int;
        fn pg_float48mi(a: f32, b: f64, out: *mut f64) -> c_int;
        fn pg_float48mul(a: f32, b: f64, out: *mut f64) -> c_int;
        fn pg_float48div(a: f32, b: f64, out: *mut f64) -> c_int;
        fn pg_float84pl(a: f64, b: f32, out: *mut f64) -> c_int;
        fn pg_float84mi(a: f64, b: f32, out: *mut f64) -> c_int;
        fn pg_float84mul(a: f64, b: f32, out: *mut f64) -> c_int;
        fn pg_float84div(a: f64, b: f32, out: *mut f64) -> c_int;
        fn pg_degrees(a: f64, out: *mut f64) -> c_int;
        fn pg_radians(a: f64, out: *mut f64) -> c_int;

        // rounding/sign family (float.c "RANDOM FLOAT8 OPERATORS"),
        // infallible unary
        fn pg_dround(a: f64) -> f64;
        fn pg_dceil(a: f64) -> f64;
        fn pg_dfloor(a: f64) -> f64;
        fn pg_dsign(a: f64) -> f64;
        fn pg_dtrunc(a: f64) -> f64;

        // dsqrt: 0 ok / 1 overflow / 2 underflow / 4 negative arg
        // (ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION); value via *out
        // (valid only when 0).
        fn pg_dsqrt(a: f64, out: *mut f64) -> c_int;

        // in_range window support: 0 ok (bool result via *out) /
        // 5 NaN-or-negative offset reject
        // (ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE).
        fn pg_in_range_float8_float8(
            val: f64, base: f64, offset: f64, sub: c_int, less: c_int,
            out: *mut c_int,
        ) -> c_int;
        fn pg_in_range_float4_float8(
            val: f32, base: f32, offset: f64, sub: c_int, less: c_int,
            out: *mut c_int,
        ) -> c_int;

        // Negative controls only — NOT Postgres code.
        fn pg_float4send(num: f32, out: *mut u8) -> i32;
        fn pg_float8send(num: f64, out: *mut u8) -> i32;
        fn pg_width_bucket_float8(
            operand: f64, bound1: f64, bound2: f64, count: i32,
            out: *mut i32,
        ) -> c_int;
        fn pg_float4um_wrong(a: f32) -> f32;
        fn pg_float4pl_ieee(a: f32, b: f32, out: *mut f32) -> c_int;
        fn pg_dtrunc_wrong(a: f64) -> f64;
        fn pg_in_range_f8_noreject(
            val: f64, base: f64, offset: f64, sub: c_int, less: c_int,
            out: *mut c_int,
        ) -> c_int;
    }

    type FcFn = fn(
        Option<&mut types_fmgr::FmgrInfo>,
        &mut types_fmgr::FunctionCallInfoBaseData,
    ) -> Result<Datum, Box<PgError>>;

    /// Run a shipped fc_* wrapper on a 1-arg frame.
    fn call1(fc: FcFn, a: Datum) -> Result<Datum, Box<PgError>> {
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(a);
        fc(None, &mut f)
    }

    /// Run a shipped fc_* wrapper on a 2-arg frame.
    fn call2(fc: FcFn, a: Datum, b: Datum) -> Result<Datum, Box<PgError>> {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        fc(None, &mut f)
    }

    /// Run a shipped fc_* wrapper on a 5-arg frame (in_range shape).
    fn call5(
        fc: FcFn,
        a: Datum,
        b: Datum,
        c: Datum,
        d: Datum,
        e: Datum,
    ) -> Result<Datum, Box<PgError>> {
        let mut f = LocalFcinfo::<5>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        f.args[2] = NullableDatum::value(c);
        f.args[3] = NullableDatum::value(d);
        f.args[4] = NullableDatum::value(e);
        fc(None, &mut f)
    }

    fn ok(r: Result<Datum, Box<PgError>>) -> Datum {
        match r {
            Ok(d) => d,
            Err(_) => panic!("infallible float fn errored"),
        }
    }

    /// Fully symbolic floats via bits: every NaN payload explored.
    fn any_f32() -> f32 {
        f32::from_bits(kani::any())
    }
    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    /// Stub for `PgError::error` (cash precedent): field-identical to the
    /// shipped `new_impl(ERROR, ..)` result except `message` (text left out
    /// of the proof) and `location` (`Location::caller()` is
    /// Kani-unsupported; shipped fills `Some(..)`, stub leaves `None` — not
    /// asserted on). `sqlstate` starts at the same
    /// `default_sqlstate_for_level(ERROR)` value so the shipped
    /// `.with_sqlstate(..)` in float_overflow_error / float_underflow_error /
    /// float_zero_divide_error stays load-bearing.
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

    // ---------- unary ops: full symbolic domain, bit-exact ----------

    macro_rules! unary_op {
        ($($h:ident: $fc:ident / $pg:ident ($g:ident; $pack:ident, $unpack:ident);)*) => {$(
            #[kani::proof]
            fn $h() {
                let a = $g();
                let r = ok(call1(adt_float::builtins::$fc, Datum::$pack(a)));
                let c = unsafe { $pg(a) };
                assert!(r.$unpack().to_bits() == c.to_bits());
            }
        )*};
    }

    unary_op! {
        eq_float4abs: fc_float4abs / pg_float4abs (any_f32; from_f32, as_f32);
        eq_float4um: fc_float4um / pg_float4um (any_f32; from_f32, as_f32);
        eq_float4up: fc_float4up / pg_float4up (any_f32; from_f32, as_f32);
        eq_float8abs: fc_float8abs / pg_float8abs (any_f64; from_f64, as_f64);
        eq_float8um: fc_float8um / pg_float8um (any_f64; from_f64, as_f64);
        eq_float8up: fc_float8up / pg_float8up (any_f64; from_f64, as_f64);
    }

    /// dpi: zero-arg constant. Bit-exact M_PI parity.
    #[kani::proof]
    fn eq_dpi() {
        let mut f = LocalFcinfo::<0>::new(0);
        let r = match adt_float::builtins::fc_dpi(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("dpi errored"),
        };
        let c = unsafe { pg_dpi() };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    // ---------- rounding/sign family: full symbolic domain, bit-exact ----------
    //
    // dround/dceil/dfloor/dsign/dtrunc are infallible unary rounding/
    // classification circuits (rint/ceil/floor/compare — no 53-bit
    // multiply/divide anywhere), so the float cost law puts them in the
    // FULL-DOMAIN class: fully symbolic f64 including every NaN payload,
    // ±Inf, ±0, denormals. rint ≡ round_ties_even is already
    // machine-checked in proofs/casts; these theorems add the shipped
    // fmgr wrapper + Datum pack/unpack on top. NaN handling is pure
    // PROPAGATION on both sides (no NAN macro / get_float8_nan in the
    // vendored C — canonical-NAN shim screened NOT NEEDED; see the C
    // header + tests/semantics_check.rs native pin).

    unary_op! {
        eq_dround: fc_dround / pg_dround (any_f64; from_f64, as_f64);
        eq_dceil: fc_dceil / pg_dceil (any_f64; from_f64, as_f64);
        eq_dfloor: fc_dfloor / pg_dfloor (any_f64; from_f64, as_f64);
        eq_dsign: fc_dsign / pg_dsign (any_f64; from_f64, as_f64);
        eq_dtrunc: fc_dtrunc / pg_dtrunc (any_f64; from_f64, as_f64);
    }

    // ---------- fallible arithmetic: value + verdict + sqlstate parity ----------

    /// C flag → expected shipped sqlstate.
    fn want_sqlstate(cerr: c_int) -> types_error::SqlState {
        if cerr == 3 {
            ERRCODE_DIVISION_BY_ZERO
        } else if cerr == 4 {
            ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION
        } else if cerr == 5 {
            ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
        } else {
            ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
        }
    }

    macro_rules! fallible_bin {
        ($($h:ident: $fc:ident / $pg:ident ($ga:ident : $ta:ty, $gb:ident : $tb:ty;
             $pa:ident, $pb:ident; $ot:ty, $unpack:ident);)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: $ta = $ga();
                let b: $tb = $gb();
                let mut cval: $ot = 0.0;
                let cerr = unsafe { $pg(a, b, &mut cval) };
                match call2(adt_float::builtins::$fc, Datum::$pa(a), Datum::$pb(b)) {
                    Ok(d) => {
                        kani::cover!(cerr == 0); // vacuity witness: Ok arm explored
                        assert!(cerr == 0);
                        assert!(d.$unpack().to_bits() == cval.to_bits());
                    }
                    Err(e) => {
                        kani::cover!(cerr != 0); // vacuity witness: Err arm explored
                        assert!(cerr != 0);
                        assert!(e.sqlstate == want_sqlstate(cerr));
                        assert!(e.level == ERROR);
                    }
                }
            }
        )*};
    }

    fallible_bin! {
        eq_float4pl: fc_float4pl / pg_float4pl (any_f32: f32, any_f32: f32; from_f32, from_f32; f32, as_f32);
        eq_float4mi: fc_float4mi / pg_float4mi (any_f32: f32, any_f32: f32; from_f32, from_f32; f32, as_f32);
        eq_float4mul: fc_float4mul / pg_float4mul (any_f32: f32, any_f32: f32; from_f32, from_f32; f32, as_f32);
        eq_float4div: fc_float4div / pg_float4div (any_f32: f32, any_f32: f32; from_f32, from_f32; f32, as_f32);
        eq_float8pl: fc_float8pl / pg_float8pl (any_f64: f64, any_f64: f64; from_f64, from_f64; f64, as_f64);
        eq_float8mi: fc_float8mi / pg_float8mi (any_f64: f64, any_f64: f64; from_f64, from_f64; f64, as_f64);
        eq_float8mul: fc_float8mul / pg_float8mul (any_f64: f64, any_f64: f64; from_f64, from_f64; f64, as_f64);
        eq_float8div: fc_float8div / pg_float8div (any_f64: f64, any_f64: f64; from_f64, from_f64; f64, as_f64);
        eq_float48pl: fc_float48pl / pg_float48pl (any_f32: f32, any_f64: f64; from_f32, from_f64; f64, as_f64);
        eq_float48mi: fc_float48mi / pg_float48mi (any_f32: f32, any_f64: f64; from_f32, from_f64; f64, as_f64);
        eq_float48mul: fc_float48mul / pg_float48mul (any_f32: f32, any_f64: f64; from_f32, from_f64; f64, as_f64);
        eq_float48div: fc_float48div / pg_float48div (any_f32: f32, any_f64: f64; from_f32, from_f64; f64, as_f64);
        eq_float84pl: fc_float84pl / pg_float84pl (any_f64: f64, any_f32: f32; from_f64, from_f32; f64, as_f64);
        eq_float84mi: fc_float84mi / pg_float84mi (any_f64: f64, any_f32: f32; from_f64, from_f32; f64, as_f64);
        eq_float84mul: fc_float84mul / pg_float84mul (any_f64: f64, any_f32: f32; from_f64, from_f32; f64, as_f64);
        eq_float84div: fc_float84div / pg_float84div (any_f64: f64, any_f32: f32; from_f64, from_f32; f64, as_f64);
    }

    macro_rules! fallible_un {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a = any_f64();
                let mut cval: f64 = 0.0;
                let cerr = unsafe { $pg(a, &mut cval) };
                match call1(adt_float::builtins::$fc, Datum::from_f64(a)) {
                    Ok(d) => {
                        kani::cover!(cerr == 0); // vacuity witness
                        assert!(cerr == 0);
                        assert!(d.as_f64().to_bits() == cval.to_bits());
                    }
                    Err(e) => {
                        kani::cover!(cerr != 0); // vacuity witness
                        assert!(cerr != 0);
                        assert!(e.sqlstate == want_sqlstate(cerr));
                        assert!(e.level == ERROR);
                    }
                }
            }
        )*};
    }

    fallible_un! {
        eq_degrees: fc_degrees / pg_degrees;
        eq_radians: fc_radians / pg_radians;
    }

    // ---------- dsqrt: PROBE harness (class on the result) ----------
    //
    // IEEE sqrt is CBMC-native, and the guard circuit is
    // compare/isinf/is-zero only — but sqrt sits at 53-bit significand
    // width, so this is a PROBE per NEXT-200: full-symbolic first; if it
    // walls, the spot grid below is the standing fallback (record
    // `proved(specials grid; full-sym wall)`). The full-symbolic Err arm
    // (arg<0 → 2201F, plus overflow/underflow taxonomy — both
    // C-unreachable for sqrt on finite non-negative inputs, verdict
    // parity asserts that agreement) is symbolic-reachable, so
    // sqlstate/level parity is safe to read (the Kani Err(Box) defect
    // needs a CONSTANT-reachable arm).
    fallible_un! {
        eq_dsqrt: fc_dsqrt / pg_dsqrt;
    }

    /// Spot-grid fallback for dsqrt: same theorem over the 16-value
    /// special grid. The Err arm is CONSTANT-reachable here (grid holds
    /// -1.0 etc.), which trips the Kani Err(Box<PgError>) f64-payload
    /// defect (witness pair below) — so this harness is VERDICT-ONLY on
    /// the error arm: C flag must be 4 (negative arg; overflow/underflow
    /// are unreachable from the grid), payload forgotten. sqlstate/level
    /// parity for the same shipped error constructor is proven in
    /// eq_dsqrt (and natively in tests/semantics_check.rs).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn spots_dsqrt() {
        let a = sp_f64();
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_dsqrt(a, &mut cval) };
        match call1(adt_float::builtins::fc_dsqrt, Datum::from_f64(a)) {
            Ok(d) => {
                kani::cover!(cerr == 0); // vacuity witness
                assert!(cerr == 0);
                let r = d.as_f64();
                if a.is_nan() {
                    // CBMC's sqrt MODEL has no NaN-payload semantics (same
                    // defect family as its NAN constant) and fabricates
                    // payload divergences here. In-model: both-NaN only.
                    // Payload/quieting parity is pinned NATIVELY (real
                    // clang C vs shipped Rust, bit-exact) in
                    // tests/dsqrt_grid_native.rs.
                    assert!(r.is_nan() && cval.is_nan());
                } else if a.is_subnormal() {
                    // SECOND CBMC sqrt-model defect arm (decoded 2026-07-29
                    // from the fleet FAILED, concrete index 7 = min
                    // denormal 0x1): CBMC's sqrt model mis-rounds SUBNORMAL
                    // inputs — real silicon gives the exact
                    // 0x1e60000000000000 (= 2^-537), proven bit-exact
                    // C-vs-Rust in tests/dsqrt_grid_native.rs. In-model:
                    // classification only (positive finite both sides);
                    // bit-exactness for this cell is pinned NATIVELY.
                    assert!(r.is_finite() && r > 0.0);
                    assert!(cval.is_finite() && cval > 0.0);
                } else {
                    assert!(r.to_bits() == cval.to_bits());
                }
            }
            Err(e) => {
                kani::cover!(cerr != 0); // vacuity witness
                assert!(cerr == 4);
                std::mem::forget(e); // Kani defect: payload untouchable
            }
        }
    }

    /// Bisect helper for the spots_dsqrt in-model FAILED: one loop
    /// iteration per grid row; CBMC reports each unwind copy's assertion
    /// separately, naming the failing index.
    #[kani::proof]
    #[kani::unwind(17)]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn dsqrt_bisect_grid() {
        for k in 0..16usize {
            let a = f64::from_bits(SP64[k]);
            let mut cval: f64 = 0.0;
            let cerr = unsafe { pg_dsqrt(a, &mut cval) };
            if cerr != 0 { continue; }
            let r = match call1(adt_float::builtins::fc_dsqrt, Datum::from_f64(a)) {
                Ok(d) => d.as_f64(),
                Err(e) => { std::mem::forget(e); continue }
            };
            let same = (r.is_nan() && cval.is_nan()) || r.to_bits() == cval.to_bits();
            assert!(same);
        }
    }

    // ---------- in_range window support: value + verdict + sqlstate ----------
    //
    // Per the float cost law these are GREEN-class at full width: the
    // circuit is isnan/isinf tests + ONE f64 add/sub + compares — no
    // multiply/divide. All five args fully symbolic (every NaN payload on
    // val/base/offset, both bool flags). Result is bool (compared as
    // as_bool vs C's int out), Err transport is Result<bool-Datum,_> —
    // not the defective Result<f64,_> shape — and the reject arm is
    // symbolic-reachable, so sqlstate/level parity stays in-theorem.

    macro_rules! eq_in_range {
        ($($h:ident: $fc:ident / $pg:ident ($gv:ident : $tv:ty; $pv:ident);)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let val: $tv = $gv();
                let base: $tv = $gv();
                let offset = any_f64();
                let sub: bool = kani::any();
                let less: bool = kani::any();
                let mut cout: c_int = 0;
                let cerr = unsafe {
                    $pg(val, base, offset, sub as c_int, less as c_int, &mut cout)
                };
                match call5(
                    adt_float::builtins::$fc,
                    Datum::$pv(val),
                    Datum::$pv(base),
                    Datum::from_f64(offset),
                    Datum::from_bool(sub),
                    Datum::from_bool(less),
                ) {
                    Ok(d) => {
                        kani::cover!(cerr == 0); // vacuity witness
                        assert!(cerr == 0);
                        assert!(d.as_bool() == (cout != 0));
                    }
                    Err(e) => {
                        kani::cover!(cerr != 0); // vacuity witness
                        assert!(cerr == 5);
                        assert!(e.sqlstate == want_sqlstate(cerr));
                        assert!(e.level == ERROR);
                    }
                }
            }
        )*};
    }

    eq_in_range! {
        eq_in_range_float8_float8: fc_in_range_float8_float8 / pg_in_range_float8_float8 (any_f64: f64; from_f64);
        eq_in_range_float4_float8: fc_in_range_float4_float8 / pg_in_range_float4_float8 (any_f32: f32; from_f32);
    }

    // ---------- width_bucket_float8 (oid 320, added 2026-07-29) ----------
    //
    // Ledger-note plan executed: every arm EXCEPT the bucket-value divide is
    // comparison-only (count<=0 / NaN / infinite-bound / bound1==bound2
    // rejects, operand-outside-bounds results 0 and count+1 incl the
    // pg_add_s32_overflow reject) — proved FULLY SYMBOLIC with a fence
    // keeping only the 53-bit multiply/divide arm out. That arm gets the
    // standard spot grid: ONE symbolic index into a concrete cell table
    // (geo-cmp rule — never a loop through the wrapper), cells covering
    // both directions, the /2 overflow-difference path, and a
    // subtraction-rounding cell where the quotient rounds to exactly 1.0
    // (the r>=count clamp branch). Err transport is Result<Datum,_> with an
    // i32 payload core (PgResult<i32>) — NOT the defective Result<f64,_>
    // shape — so sqlstate/level parity stays in-theorem on both reject
    // classes (2201G and 22003).

    /// Run a shipped fc_* wrapper on a 4-arg frame (width_bucket shape).
    fn call4(fc: FcFn, a: Datum, b: Datum, c: Datum, d: Datum) -> Result<Datum, Box<PgError>> {
        let mut f = LocalFcinfo::<4>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        f.args[2] = NullableDatum::value(c);
        f.args[3] = NullableDatum::value(d);
        fc(None, &mut f)
    }

    /// Shared theorem body: shipped fc_width_bucket_float8 vs vendored C.
    fn wb_check(operand: f64, bound1: f64, bound2: f64, count: i32) {
        let mut cout: i32 = 0;
        let cerr =
            unsafe { pg_width_bucket_float8(operand, bound1, bound2, count, &mut cout) };
        match call4(
            adt_float::builtins::fc_width_bucket_float8,
            Datum::from_f64(operand),
            Datum::from_f64(bound1),
            Datum::from_f64(bound2),
            Datum::from_i32(count),
        ) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_i32() == cout);
            }
            Err(e) => {
                assert!(cerr == 6 || cerr == 7);
                if cerr == 6 {
                    assert!(e.sqlstate == ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION);
                } else {
                    assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                }
                assert!(e.level == ERROR);
            }
        }
    }

    /// All non-divide arms, fully symbolic. The fence excludes exactly the
    /// executions that would reach the bucket-value divide (reject checks
    /// fire first in both implementations, so the divide is reachable only
    /// when no reject applies AND operand falls strictly inside the bucket
    /// range).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn eq_width_bucket_reject() {
        let operand = any_f64();
        let bound1 = any_f64();
        let bound2 = any_f64();
        let count: i32 = kani::any();
        let no_reject = count > 0
            && !operand.is_nan()
            && !bound1.is_nan()
            && !bound2.is_nan()
            && !bound1.is_infinite()
            && !bound2.is_infinite();
        let in_div_up = bound1 < bound2 && operand >= bound1 && operand < bound2;
        let in_div_dn = bound1 > bound2 && operand <= bound1 && operand > bound2;
        kani::assume(!(no_reject && (in_div_up || in_div_dn)));
        // vacuity witnesses: value arm (0 / count+1), both reject classes
        kani::cover!(no_reject && count < i32::MAX);
        kani::cover!(!no_reject);
        kani::cover!(no_reject && count == i32::MAX && (in_div_up || in_div_dn) == false);
        wb_check(operand, bound1, bound2, count);
    }

    /// Bucket-value divide arm: concrete cell grid, one symbolic index.
    /// Cells: up/down directions, first/middle/last buckets, denormal and
    /// large-magnitude operands, the /2 arm (bound difference overflows to
    /// Inf), and a subtraction-rounding q==1.0 clamp cell
    /// (operand 9.5, bounds [-1e16, 10]: numerator 1e16+9.5 rounds to
    /// 1e16+10 == denominator, quotient exactly 1.0, r>=count clamp fires).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn grid_width_bucket_float8() {
        const CELLS: [(f64, f64, f64, i32); 14] = [
            (5.0, 0.0, 10.0, 5),                  // middle bucket, up
            (0.0, 0.0, 10.0, 5),                  // first bucket edge, up
            (9.999, 0.0, 10.0, 5),                // last bucket, up
            (5.0, 10.0, 0.0, 5),                  // middle bucket, down
            (10.0, 10.0, 0.0, 5),                 // first bucket edge, down
            (0.001, 10.0, 0.0, 5),                // last bucket, down
            (5e-324, 0.0, 1.0, 3),                // denormal operand
            (0.0, -1e308, 1e308, 7),              // /2 arm, up (diff = Inf)
            (0.0, 1e308, -1e308, 7),              // /2 arm, down
            (9.5, -1e16, 10.0, 5),                // q rounds to 1.0: clamp
            (1.0, 0.0, 3.0, 2147483647),          // count = i32::MAX, in-range
            (2.5, 0.0, 10.0, 1),                  // count = 1
            (-1e307, -1e308, 1e308, 3),           // /2 arm, non-center operand
            (0.5, 0.0, 1.0, 1000000),             // large count, exact half
        ];
        let i: usize = kani::any();
        kani::assume(i < CELLS.len());
        let (operand, bound1, bound2, count) = CELLS[i];
        wb_check(operand, bound1, bound2, count);
    }

    // ---------- float4send / float8send (oids 2425/2427, 2026-07-29) ----------
    //
    // int-arith send precedent verbatim: shipped fc_*send through a real
    // LocalFcinfo + bump context under the proof_support mcx-stubs recipe
    // (allocation strategy leaves the proof — "modulo static-buffer
    // allocator model"); ENTIRE wire image incl the 4B varlena header
    // byte-compared against the vendored pq_sendfloat4/8 emission. Floats
    // ride the same union bit-copy both sides, so every NaN payload is
    // in-theorem. float4recv/float8recv are NOT harnessed: identical
    // pointer-datum recv ABI to int2/4/8recv, a measured symex wall class
    // (kani::mem provenance on every StringInfo field access) — recorded
    // wall by precedent, re-opens on a slice-core refactor.

    use proof_support::{mcx_stubs, stubs};

    macro_rules! send_harness_f {
        ($($h:ident: $fc:ident / $pg:ident ($ta:ty) $from:ident, total=$total:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // image compare <= total+1; slack = dead loop copies
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let bits: <$ta as FloatBits>::Bits = kani::any();
                let a = <$ta>::from_bits(bits);
                let mut cbuf = [0u8; $total];
                let clen = unsafe { $pg(a, cbuf.as_mut_ptr()) };

                let ctx = mcx::MemoryContext::new_bump("kani-float-send");
                let mut f = LocalFcinfo::<1>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] = NullableDatum::value(Datum::$from(a));
                let d = match adt_float::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => { core::mem::forget(e); panic!("send errored") }
                };
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

    /// Helper trait so the macro can draw fully-symbolic bit patterns.
    trait FloatBits {
        type Bits;
    }
    impl FloatBits for f32 {
        type Bits = u32;
    }
    impl FloatBits for f64 {
        type Bits = u64;
    }

    send_harness_f! {
        eq_float4send: fc_float4send / pg_float4send (f32) from_f32, total=8, unwind=10;
        eq_float8send: fc_float8send / pg_float8send (f64) from_f64, total=12, unwind=14;
    }

    /// Deliberate mismatch: shipped fc_float8send vs C float4send's image
    /// (wrong width and header). MUST fail. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_float8send_vs_c_float4send() {
        let bits: u64 = kani::any();
        let a = f64::from_bits(bits);
        let mut cbuf = [0u8; 8];
        let _clen = unsafe { pg_float4send(a as f32, cbuf.as_mut_ptr()) };
        let ctx = mcx::MemoryContext::new_bump("kani-float-send-ctl");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_f64(a));
        let d = match adt_float::builtins::fc_float8send(None, &mut f) {
            Ok(d) => d,
            Err(e) => { core::mem::forget(e); panic!("send errored") }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 8) };
        let mut i = 0;
        while i < 8 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    // ---------- special-value spot grids (for ops whose fully-symbolic
    // pair walls: same theorem restricted to the special-pair grid) ----------

    /// f32 special values: ±0, ±Inf, quiet/signaling NaNs both signs,
    /// denormal min/max, normal min/max, ±1, ±2, 0.5, an ordinary value.
    const SP32: [u32; 16] = [
        0x0000_0000, // +0
        0x8000_0000, // -0
        0x7F80_0000, // +Inf
        0xFF80_0000, // -Inf
        0x7FC0_0000, // +qNaN
        0xFFC0_0001, // -qNaN (payload)
        0x7F80_0001, // +sNaN
        0x0000_0001, // min denormal
        0x807F_FFFF, // -max denormal
        0x0080_0000, // min normal
        0x7F7F_FFFF, // max finite
        0x3F80_0000, // 1.0
        0xBF80_0000, // -1.0
        0x4000_0000, // 2.0
        0x3F00_0000, // 0.5
        0x40490FDB,  // pi
    ];

    /// f64 specials, same taxonomy.
    const SP64: [u64; 16] = [
        0x0000_0000_0000_0000, // +0
        0x8000_0000_0000_0000, // -0
        0x7FF0_0000_0000_0000, // +Inf
        0xFFF0_0000_0000_0000, // -Inf
        0x7FF8_0000_0000_0000, // +qNaN
        0xFFF8_0000_0000_0001, // -qNaN (payload)
        0x7FF0_0000_0000_0001, // +sNaN
        0x0000_0000_0000_0001, // min denormal
        0x800F_FFFF_FFFF_FFFF, // -max denormal
        0x0010_0000_0000_0000, // min normal
        0x7FEF_FFFF_FFFF_FFFF, // max finite
        0x3FF0_0000_0000_0000, // 1.0
        0xBFF0_0000_0000_0000, // -1.0
        0x4000_0000_0000_0000, // 2.0
        0x3FE0_0000_0000_0000, // 0.5
        0x4009_21FB_5444_2D18, // pi
    ];

    fn sp_f32() -> f32 {
        let i: usize = kani::any();
        kani::assume(i < SP32.len());
        f32::from_bits(SP32[i])
    }
    fn sp_f64() -> f64 {
        let i: usize = kani::any();
        kani::assume(i < SP64.len());
        f64::from_bits(SP64[i])
    }

    // Div grids for f8-width outputs (spots_float8div / spots_float48div /
    // spots_float84div) are EXCLUDED: their error arms are
    // constant-reachable and hit the Kani Err(Box) f64-payload defect
    // documented at the witness pair below (measured: 84div FAILED on
    // garbage payload reads; 8div/48div walled). Re-add when
    // kani_defect_witness_f64 starts passing.
    fallible_bin! {
        spots_float4mul: fc_float4mul / pg_float4mul (sp_f32: f32, sp_f32: f32; from_f32, from_f32; f32, as_f32);
        spots_float4div: fc_float4div / pg_float4div (sp_f32: f32, sp_f32: f32; from_f32, from_f32; f32, as_f32);
        spots_float8mul: fc_float8mul / pg_float8mul (sp_f64: f64, sp_f64: f64; from_f64, from_f64; f64, as_f64);
        spots_float48mul: fc_float48mul / pg_float48mul (sp_f32: f32, sp_f64: f64; from_f32, from_f64; f64, as_f64);
        spots_float84mul: fc_float84mul / pg_float84mul (sp_f64: f64, sp_f32: f32; from_f64, from_f32; f64, as_f64);
    }

    /// Half-symbolic mul: one fully-symbolic operand × special grid.
    /// Probes whether symbolic×constant multiply is affordable (the
    /// degrees/radians shape) even where symbolic×symbolic walls.
    fallible_bin! {
        half_float4mul: fc_float4mul / pg_float4mul (any_f32: f32, sp_f32: f32; from_f32, from_f32; f32, as_f32);
        half_float8mul: fc_float8mul / pg_float8mul (any_f64: f64, sp_f64: f64; from_f64, from_f64; f64, as_f64);
        half_float4div: fc_float4div / pg_float4div (any_f32: f32, sp_f32: f32; from_f32, from_f32; f32, as_f32);
        half_float8div: fc_float8div / pg_float8div (any_f64: f64, sp_f64: f64; from_f64, from_f64; f64, as_f64);
    }

    // ---------- zero-divide arm, fully symbolic (b fenced to ±0) ----------
    //
    // The divisor fence kills the division circuit (the early-return path
    // never divides), so the ENTIRE zero-divide adjudication — including
    // the NaN/0 non-error row and sqlstate 22012 vs the C ereport — is a
    // cheap full-width theorem even where symbolic÷symbolic walls.

    fn any_f32_zero() -> f32 {
        let b = any_f32();
        kani::assume(b == 0.0); // ±0 both satisfy
        b
    }
    fn any_f64_zero() -> f64 {
        let b = any_f64();
        kani::assume(b == 0.0);
        b
    }

    fallible_bin! {
        eq_float4div_zero: fc_float4div / pg_float4div (any_f32: f32, any_f32_zero: f32; from_f32, from_f32; f32, as_f32);
    }

    // f8-width zero-divide arms: VERDICT-ONLY parity. Reading (or dropping)
    // the Err(Box<PgError>) payload of a Result<f64,_>-returning div is
    // corrupted by the Kani defect witnessed below, so the payload is
    // mem::forget'ten: Ok/Err verdict, Ok bit-values, and C's error CODE
    // (must be 3 = zero-divide) stay in the theorem; Rust sqlstate/level on
    // this arm are excluded — they ARE proven at f4 width
    // (eq_float4div_zero), and all four widths funnel the identical
    // float8_div/float4_div zero arm.
    macro_rules! fallible_bin_verdict {
        ($($h:ident: $fc:ident / $pg:ident ($ga:ident : $ta:ty, $gb:ident : $tb:ty;
             $pa:ident, $pb:ident; $ot:ty, $unpack:ident; $cerr_want:literal);)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            fn $h() {
                let a: $ta = $ga();
                let b: $tb = $gb();
                let mut cval: $ot = 0.0;
                let cerr = unsafe { $pg(a, b, &mut cval) };
                match call2(adt_float::builtins::$fc, Datum::$pa(a), Datum::$pb(b)) {
                    Ok(d) => {
                        kani::cover!(cerr == 0); // vacuity witness
                        assert!(cerr == 0);
                        assert!(d.$unpack().to_bits() == cval.to_bits());
                    }
                    Err(e) => {
                        kani::cover!(cerr != 0); // vacuity witness
                        assert!(cerr == $cerr_want);
                        std::mem::forget(e); // Kani defect: payload untouchable
                    }
                }
            }
        )*};
    }

    fallible_bin_verdict! {
        eq_float8div_zero: fc_float8div / pg_float8div (any_f64: f64, any_f64_zero: f64; from_f64, from_f64; f64, as_f64; 3);
        eq_float48div_zero: fc_float48div / pg_float48div (any_f32: f32, any_f64_zero: f64; from_f32, from_f64; f64, as_f64; 3);
        eq_float84div_zero: fc_float84div / pg_float84div (any_f64: f64, any_f32_zero: f32; from_f64, from_f32; f64, as_f64; 3);
    }

    // ---------- Kani-defect witness pair (documented, NOT a parity gate) ----
    //
    // Kani 0.67.0 + CBMC miscodegens reads through the Err(Box<PgError>)
    // payload of Result<f64, Box<PgError>> when the error arm is
    // CONSTANT-REACHABLE (concrete or constant-folded inputs): the Box
    // pointer loses provenance and field reads return garbage, with
    // spurious __rust_dealloc/drop_in_place safety failures. The identical
    // Result<f32, _> shape is fine, and fully-SYMBOLIC harnesses through
    // the shipped fns are fine (their Err arms are cover!-witnessed above).
    // Consequence here: concrete special-pair grids for f8-width DIV error
    // arms cannot be trusted; witness pair below documents the defect.
    //
    // kani_defect_witness_f64 MUST FAIL (harness-local Rust-only code, no C,
    // semantics verified by tests/semantics_check.rs). If it starts PASSING,
    // the Kani bug is fixed — re-run the spots_* div grids and promote them.
    // kani_defect_control_f32 is the same code at f32 and MUST PASS.

    #[cold]
    #[inline(never)]
    fn local_boxed() -> Box<PgError> {
        Box::new(PgError::error("z").with_sqlstate(ERRCODE_DIVISION_BY_ZERO))
    }

    #[inline]
    fn local_div_f64(val1: f64, val2: f64) -> Result<f64, Box<PgError>> {
        if val2 == 0.0 && !val1.is_nan() {
            return Err(local_boxed());
        }
        Ok(val1 / val2)
    }

    #[inline]
    fn local_div_f32(val1: f32, val2: f32) -> Result<f32, Box<PgError>> {
        if val2 == 0.0 && !val1.is_nan() {
            return Err(local_boxed());
        }
        Ok(val1 / val2)
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn kani_defect_witness_f64() {
        match local_div_f64(2.0, -0.0) {
            Ok(_) => assert!(false),
            Err(e) => assert!(e.sqlstate == ERRCODE_DIVISION_BY_ZERO && e.level == ERROR),
        }
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn kani_defect_control_f32() {
        match local_div_f32(2.0, -0.0) {
            Ok(_) => assert!(false),
            Err(e) => assert!(e.sqlstate == ERRCODE_DIVISION_BY_ZERO && e.level == ERROR),
        }
    }

    // ---------- negative controls: rig must be able to fail ----------

    /// Shipped fc_float4um vs C fabsf. MUST fail (e.g. x = 1.0: -1 != 1).
    /// Run with the DEFAULT solver.
    #[kani::proof]
    fn control_float4um_vs_c_abs() {
        let a = any_f32();
        let r = ok(call1(adt_float::builtins::fc_float4um, Datum::from_f32(a)));
        let c = unsafe { pg_float4um_wrong(a) };
        assert!(r.as_f32().to_bits() == c.to_bits());
    }

    /// Shipped fc_dtrunc vs C rint (round-nearest-even instead of
    /// truncate-toward-zero). MUST fail (e.g. 1.5: trunc 1.0 vs rint 2.0)
    /// — the rounding-section rig can fail. Run with the DEFAULT solver.
    #[kani::proof]
    fn control_dtrunc_vs_c_round() {
        let a = any_f64();
        let r = ok(call1(adt_float::builtins::fc_dtrunc, Datum::from_f64(a)));
        let c = unsafe { pg_dtrunc_wrong(a) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    /// Shipped fc_in_range_float8_float8 vs a control C that skips the
    /// NaN/negative-offset reject and all NaN/Inf special handling. MUST
    /// fail (e.g. offset = NaN: Rust errors 22013, control returns ok) —
    /// witnesses the solver reaches the reject arm and the NaN lattice.
    /// Run with the DEFAULT solver.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn control_in_range_f8_vs_noreject() {
        let val = any_f64();
        let base = any_f64();
        let offset = any_f64();
        let sub: bool = kani::any();
        let less: bool = kani::any();
        let mut cout: c_int = 0;
        let cerr = unsafe {
            pg_in_range_f8_noreject(val, base, offset, sub as c_int, less as c_int, &mut cout)
        };
        match call5(
            adt_float::builtins::fc_in_range_float8_float8,
            Datum::from_f64(val),
            Datum::from_f64(base),
            Datum::from_f64(offset),
            Datum::from_bool(sub),
            Datum::from_bool(less),
        ) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_bool() == (cout != 0));
            }
            Err(_) => {
                // control C never errors: any Rust Err is a mismatch.
                assert!(false);
            }
        }
    }

    /// Shipped fc_float4pl vs plain IEEE + (no overflow ereport). MUST fail
    /// on the finite+finite=Inf overflow arm — witnesses the solver reaches
    /// the error arm. Run with the DEFAULT solver.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn control_float4pl_vs_ieee() {
        let a = any_f32();
        let b = any_f32();
        let mut cval: f32 = 0.0;
        let cerr = unsafe { pg_float4pl_ieee(a, b, &mut cval) };
        match call2(adt_float::builtins::fc_float4pl, Datum::from_f32(a), Datum::from_f32(b)) {
            Ok(d) => {
                assert!(cerr == 0);
                assert!(d.as_f32().to_bits() == cval.to_bits());
            }
            Err(_) => {
                // control C never errors: any Rust Err is a mismatch.
                assert!(false);
            }
        }
    }
}
