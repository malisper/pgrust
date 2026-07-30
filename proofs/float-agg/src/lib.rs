//! Kani C≡Rust equivalence: float8[] Youngs-Cramer AGGREGATE FINALS +
//! transition zero-arm planes (tail-triage group 1, lane float-agg —
//! 21 ledger rows):
//!   finals (t3): float8_avg 1830, float8_var_pop 2512, float8_var_samp
//!     1831, float8_stddev_pop 2513, float8_stddev_samp 1832
//!   finals (t6): float8_regr_sxx 2807, float8_regr_syy 2808,
//!     float8_regr_sxy 2809, float8_regr_avgx 2810, float8_regr_avgy 2811,
//!     float8_regr_r2 2812, float8_regr_slope 2813, float8_regr_intercept
//!     2814, float8_covar_pop 2815, float8_covar_samp 2816, float8_corr 2817
//!   transitions (zero-arm planes only): float4_accum 208, float8_accum 222,
//!     float8_combine 276, float8_regr_accum 2806, float8_regr_combine 3342
//!
//! Rust side: the SHIPPED pure cores `adt_float::aggregates::*`
//! ([f64; N] transvalue -> Option<f64> / PgResult<[f64; N]>) — the exact
//! functions the fc_* wrappers dispatch (builtins.rs fc_float_final! /
//! float8_trans_result). C side: c/pg_float_agg.c, verbatim REL_18_STABLE
//! float.c bodies (see its header for every shim).
//!
//! THEOREM STRUCTURE (per the float cost law, TRIAGE.md):
//!   - eq_check_float8_array_t3/_t6: the transition-array SHAPE CHECK
//!     (header-word predicates + data extraction) proven over symbolic
//!     headers — this is the piece the core-level final theorems shim out
//!     of the C prologue; together they cover the wrapper chain modulo the
//!     fcinfo varlena fetch. Err arm is VERDICT-ONLY (message text via
//!     stubbed PgError::error/format; Box forgotten — Kani Err(Box)
//!     payload-read defect, see proofs/float-arith witness pair).
//!   - eq_finals_nullity_t3/_t6: ONE shared nullity-parity theorem per
//!     transvalue width — FULLY SYMBOLIC [f64; N] (every NaN payload, ±Inf,
//!     ±0, denormals), asserting Some/None <-> value/NULL verdict parity
//!     for all finals at once. The N-threshold family (n==0 vs n<=1 vs
//!     n<1 vs n<2 distinct boundaries) is fully in-theorem here.
//!   - eq_regr_sxx/_syy/_sxy: FULL theorems (verdict + bit-exact value)
//!     over fully symbolic [f64; 6] — the finals with NO arithmetic
//!     (ledger: contradicts the blanket "regr = state" exclusion; SHIP
//!     FIRST).
//!   - eq_regr_r2_horizontal: syy pinned by LITERAL 0.0 (assumes never
//!     constant-fold; literals do) — the constant-1.0 arm proved outright,
//!     no divide circuit reachable.
//!   - grid_*: the 53-bit divide/sqrt finals get the special-grid
//!     treatment (float law: 53-bit x/÷ walls full-symbolic; spots grids
//!     prove 10-22s) — symbolic INDICES into concrete N_GRID x S_GRID
//!     tables, full verdict + bit-exact value parity per cell. Unused
//!     transvalue slots are LITERAL 0.0 (dead-symbolic-bytes law).
//!   - plane_*: transition-fn zero-arm planes — accum first-row
//!     (trans = literal [0.0; N] prunes the N0>0 FMA/divide arm; the
//!     NaN/Inf routing through get_float8_nan is fully in-theorem) and
//!     combine n1==0/n2==0 identity planes (literal zero N prunes the
//!     general arm and its float8_pl/divide chain). The general arms are
//!     NOT harnessed: 53-bit multiply/divide wall + CBMC does not model
//!     the fp-contraction the shipped mul_add mirrors (pg_hypot spec-gap
//!     class, proofs/geo-cmp) — fabricated last-bit divergences, not
//!     parity. Err arms are statically unreachable on these planes
//!     (forget + static panic, never .unwrap()).
//!   - control_var_samp_vs_c_var_pop: MUST FAIL (rig non-vacuity; run
//!     with the DEFAULT solver — kissat never terminates on failing
//!     harnesses).
//!
//! CANONICAL-NAN SHIM: REQUIRED and present in c/pg_float_agg.c — the
//! accum planes reach get_float8_nan(), and CBMC's NAN constant is a
//! non-canonical signaling pattern (tool defect, geo-cmp report). With the
//! shim both sides produce 0x7ff8000000000000 and NaN comparisons are
//! bit-exact in-theorem.
//!
//! RUN (from proofs/float-agg/): every harness needs
//!   timeout 75 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_float_agg.c \
//!     --no-overflow-checks --harness <h> --exact [--solver kissat]
//! --no-overflow-checks is REQUIRED (float-family rule: CBMC's automatic
//! NaN-production checks fire on legal IEEE arithmetic on both sides).
//! Exact queue + expected verdicts in runqueue.txt. ONE run at a time
//! under the RSS watchdog; judge near-30s on kani-reported Verification
//! Time (multi-lane load inflates wall clock).

/// Deterministic in-model sqrt MODEL (dsqrt dual-mode artifact fix).
///
/// MECHANISM (isolated 2026-07-29, probe_sqrt_self_determinism): Kani/CBMC's
/// f64::sqrt is NONDETERMINISTIC PER CALL — `x.sqrt() != x.sqrt()` is
/// satisfiable in the model — so any theorem comparing two independently
/// computed sqrt results can fabricate a FAILED even when both sides are
/// identical on silicon (native_diff_float_agg: 0 diffs over the exact grid
/// domains + 8M sweep). No shared-symbol routing can fix nondeterminism;
/// the cure is ONE deterministic model applied to BOTH sides:
///   - C side: sqrt #define-routed to pg_proof_sqrt whenever
///     PG_PROOF_NATIVE is NOT defined (goto-cc/kani builds; build.rs
///     defines it for native cc builds, which keep libm sqrt — measured:
///     goto-cc does NOT define __CPROVER__ during --c-lib preprocessing,
///     so the guard must be build-controlled);
///   - Rust side: sqrt-bearing harnesses stub f64::sqrt to this model.
/// The model preserves IEEE routing (NaN/negative -> canonical quiet NaN,
/// +inf -> +inf, +-0 -> +-0) and is monotone on positive finites (exponent
/// halving seed); its VALUES are explicitly not silicon sqrt — sqrt value
/// parity is owned by the native differential. Claims read
/// "modulo deterministic sqrt model".
#[cfg(kani)]
pub fn det_sqrt_model(x: f64) -> f64 {
    if x.is_nan() || x < 0.0 {
        f64::NAN
    } else if x == f64::INFINITY || x == 0.0 {
        x
    } else {
        f64::from_bits((x.to_bits() >> 1) + 0x1FF8000000000000)
    }
}

/// C-seam entry for the deterministic sqrt model (see det_sqrt_model).
#[cfg(kani)]
#[no_mangle]
pub extern "C" fn pg_proof_sqrt(x: f64) -> f64 {
    det_sqrt_model(x)
}

#[cfg(kani)]
mod proofs {
    use proof_support::stubs;
    use std::os::raw::c_int;

    /// MECHANISM-ISOLATION WITNESS (dsqrt dual-mode artifact, for the
    /// CBMC/Kani upstream reports): two syntactically identical f64::sqrt
    /// calls on the same value. Expected PROVED if the in-model sqrt is a
    /// deterministic function; a FAILED here = the model is
    /// nondeterministic per call site (which would explain the grid_*
    /// sqrt-bearing FAILEDs regardless of canonicalization). DEFAULT
    /// solver (may fail; kissat-not-for-failures trap).
    #[kani::proof]
    fn probe_sqrt_self_determinism() {
        let x = f64::from_bits(kani::any());
        let a = x.sqrt();
        let b = x.sqrt();
        assert!(a.to_bits() == b.to_bits());
    }

    /// Companion witness: the deterministic replacement model IS
    /// deterministic in-model (two calls, same value — expected PROVED;
    /// contrast with probe_sqrt_self_determinism's FAILED on the
    /// intrinsic).
    #[kani::proof]
    fn probe_det_sqrt_model_determinism() {
        let x = f64::from_bits(kani::any());
        let a = crate::pg_proof_sqrt(x);
        let b = crate::det_sqrt_model(x);
        assert!(a.to_bits() == b.to_bits());
    }

    extern "C" {
        // shape check: returns data ptr, NULL + *err=9 on reject
        fn pg_check_float8_array(image: *const u8, n: c_int, err: *mut c_int) -> *const f64;

        // transitions: 0 ok / 2 overflow (values via out[N], valid when 0)
        fn pg_float8_accum(trans: *const f64, newval: f64, out: *mut f64) -> c_int;
        fn pg_float4_accum(trans: *const f64, newval: f32, out: *mut f64) -> c_int;
        fn pg_float8_combine(t1: *const f64, t2: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_accum(
            trans: *const f64,
            newval_y: f64,
            newval_x: f64,
            out: *mut f64,
        ) -> c_int;
        fn pg_float8_regr_combine(t1: *const f64, t2: *const f64, out: *mut f64) -> c_int;

        // finals: 0 = value in *out / 1 = SQL NULL
        fn pg_float8_avg(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_var_pop(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_var_samp(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_stddev_pop(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_stddev_samp(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_sxx(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_syy(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_sxy(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_avgx(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_avgy(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_covar_pop(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_covar_samp(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_corr(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_r2(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_slope(trans: *const f64, out: *mut f64) -> c_int;
        fn pg_float8_regr_intercept(trans: *const f64, out: *mut f64) -> c_int;
    }

    /// Fully symbolic f64 via bits: every NaN payload explored.
    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }
    fn any_f32() -> f32 {
        f32::from_bits(kani::any())
    }

    /// Option<f64> vs (C null-flag, C out) — verdict both directions +
    /// bit-exact value on the value arm. assert! (not assert_eq!): no Debug
    /// machinery on failure paths.
    fn opt_eq(r: Option<f64>, cflag: c_int, cval: f64) {
        match r {
            None => assert!(cflag == 1),
            Some(v) => {
                assert!(cflag == 0);
                assert!(v.to_bits() == cval.to_bits());
            }
        }
    }

    /// PgResult<[f64; N]> Ok arm vs C (flag, out[N]) on planes where the Err
    /// arm is statically unreachable (literal-zero pruning). Err is forgotten
    /// (Box drop-glue trap) and fails loudly via a static-str panic.
    fn trans_eq<const N: usize>(
        r: Result<[f64; N], Box<types_error::PgError>>,
        cflag: c_int,
        cout: &[f64; N],
    ) {
        match r {
            Ok(rt) => {
                assert!(cflag == 0);
                let mut k = 0;
                while k < N {
                    assert!(rt[k].to_bits() == cout[k].to_bits());
                    k += 1;
                }
            }
            Err(e) => {
                core::mem::forget(e);
                panic!("Err arm reached on a zero plane");
            }
        }
    }

    // ---------- check_float8_array: the transition-array shape check ----------

    /// 8-aligned array image (C reads it as ArrayType).
    #[repr(C, align(8))]
    struct Img<const B: usize>([u8; B]);

    /// vl_len_ (unread by both sides' predicates) and lbound (unread by
    /// both) stay LITERAL zero — dead-symbolic-bytes law.
    fn build_img<const B: usize, const N: usize>(
        ndim: i32,
        dataoffset: i32,
        elemtype: i32,
        dims0: i32,
        vals: &[f64; N],
    ) -> Img<B> {
        let mut img = Img::<B>([0u8; B]);
        img.0[4..8].copy_from_slice(&ndim.to_ne_bytes());
        img.0[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
        img.0[12..16].copy_from_slice(&elemtype.to_ne_bytes());
        img.0[16..20].copy_from_slice(&dims0.to_ne_bytes());
        let mut k = 0;
        while k < N {
            img.0[24 + 8 * k..32 + 8 * k].copy_from_slice(&vals[k].to_ne_bytes());
            k += 1;
        }
        img
    }

    macro_rules! check_harness {
        ($h:ident, $n:literal, $bytes:literal, $unwind:literal) => {
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let ndim: i32 = kani::any();
                let dataoffset: i32 = kani::any();
                let elemtype: i32 = kani::any();
                let dims0: i32 = kani::any();
                let mut vals = [0f64; $n];
                let mut k = 0;
                while k < $n {
                    vals[k] = any_f64();
                    k += 1;
                }
                let img = build_img::<$bytes, $n>(ndim, dataoffset, elemtype, dims0, &vals);

                let mut cerr: c_int = 0;
                let cptr =
                    unsafe { pg_check_float8_array(img.0.as_ptr(), $n as c_int, &mut cerr) };
                let r = adt_float::aggregates::check_float8_array::<$n>(&img.0, "proof");

                match r {
                    Ok(rv) => {
                        kani::cover!(true); // accept arm reachable
                        assert!(cerr == 0);
                        // C's accept-arm data pointer is the fixed
                        // MAXALIGN(24)-byte offset the Rust reader uses.
                        assert!(cptr as usize == img.0.as_ptr() as usize + 24);
                        let mut k = 0;
                        while k < $n {
                            assert!(rv[k].to_bits() == vals[k].to_bits());
                            k += 1;
                        }
                    }
                    Err(e) => {
                        kani::cover!(true); // reject arm reachable
                        core::mem::forget(e); // verdict-only (Err(Box) defect)
                        assert!(cerr == 9);
                        assert!(cptr.is_null());
                    }
                }
            }
        };
    }

    check_harness!(eq_check_float8_array_t3, 3, 48, 10);
    check_harness!(eq_check_float8_array_t6, 6, 72, 10);

    // ---------- shared nullity-parity theorems (fully symbolic) ----------

    /// t3 finals: verdict (Some/None vs value/NULL) parity over fully
    /// symbolic [f64; 3]. Values are NOT asserted here (divide/sqrt circuits
    /// stay unconstrained); the grid_* harnesses own value parity.
    #[kani::proof]
    fn eq_finals_nullity_t3() {
        let t = [any_f64(), any_f64(), any_f64()];
        let mut sink = 0f64;

        let c = unsafe { pg_float8_avg(t.as_ptr(), &mut sink) };
        let r = adt_float::aggregates::float8_avg(t);
        assert!(r.is_none() == (c == 1));
        kani::cover!(r.is_none());
        kani::cover!(r.is_some());

        let c = unsafe { pg_float8_var_pop(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_var_pop(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_var_samp(t.as_ptr(), &mut sink) };
        let r = adt_float::aggregates::float8_var_samp(t);
        assert!(r.is_none() == (c == 1));
        kani::cover!(r.is_none());
        kani::cover!(r.is_some());

        let c = unsafe { pg_float8_stddev_pop(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_stddev_pop(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_stddev_samp(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_stddev_samp(t).is_none() == (c == 1));
    }

    /// t6 finals: the full nullity lattice (n<1 / n<2 thresholds, corr/r2/
    /// slope/intercept sxx==0 / syy==0 arms) over fully symbolic [f64; 6].
    #[kani::proof]
    fn eq_finals_nullity_t6() {
        let t = [
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
        ];
        let mut sink = 0f64;

        let c = unsafe { pg_float8_regr_sxx(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_sxx(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_regr_syy(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_syy(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_regr_sxy(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_sxy(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_regr_avgx(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_avgx(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_regr_avgy(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_avgy(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_covar_pop(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_covar_pop(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_covar_samp(t.as_ptr(), &mut sink) };
        let r = adt_float::aggregates::float8_covar_samp(t);
        assert!(r.is_none() == (c == 1));
        kani::cover!(r.is_none());
        kani::cover!(r.is_some());

        let c = unsafe { pg_float8_corr(t.as_ptr(), &mut sink) };
        let r = adt_float::aggregates::float8_corr(t);
        assert!(r.is_none() == (c == 1));
        kani::cover!(r.is_none());
        kani::cover!(r.is_some());

        let c = unsafe { pg_float8_regr_r2(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_r2(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_regr_slope(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_slope(t).is_none() == (c == 1));

        let c = unsafe { pg_float8_regr_intercept(t.as_ptr(), &mut sink) };
        assert!(adt_float::aggregates::float8_regr_intercept(t).is_none() == (c == 1));
    }

    // ---------- no-arithmetic finals: FULL theorems, fully symbolic ----------

    macro_rules! full_final_t6 {
        ($($h:ident: $core:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let t = [
                    any_f64(), any_f64(), any_f64(),
                    any_f64(), any_f64(), any_f64(),
                ];
                let mut c_out = 0f64;
                let c = unsafe { $pg(t.as_ptr(), &mut c_out) };
                let r = adt_float::aggregates::$core(t);
                opt_eq(r, c, c_out);
                kani::cover!(r.is_none());
                kani::cover!(r.is_some());
            }
        )*};
    }

    full_final_t6! {
        eq_regr_sxx: float8_regr_sxx / pg_float8_regr_sxx;
        eq_regr_syy: float8_regr_syy / pg_float8_regr_syy;
        eq_regr_sxy: float8_regr_sxy / pg_float8_regr_sxy;
    }

    /// regr_r2 horizontal-line arm: syy pinned by LITERAL 0.0 (prunes the
    /// (sxy*sxy)/(sxx*syy) divide statically) — the constant-1.0 arm plus
    /// the n<1/sxx==0 lattice proved over symbolic n/sxx. sx/sy/sxy are
    /// unread on this plane: literal 0.0 (dead-symbolic-bytes law).
    #[kani::proof]
    fn eq_regr_r2_horizontal() {
        let t = [any_f64(), 0.0, any_f64(), 0.0, 0.0, 0.0];
        let mut c_out = 0f64;
        let c = unsafe { pg_float8_regr_r2(t.as_ptr(), &mut c_out) };
        let r = adt_float::aggregates::float8_regr_r2(t);
        opt_eq(r, c, c_out);
        kani::cover!(r == Some(1.0));
        kani::cover!(r.is_none());
    }

    // ---------- value grids for the divide/sqrt finals ----------
    //
    // 53-bit divide (and sqrt-of-divide) walls full-symbolic (float law);
    // each grid cell draws its relevant transvalue slots through SYMBOLIC
    // INDICES into concrete tables — every (n, special) pair is in the
    // theorem, the open-region remainder is recorded as the standing
    // non-covered regime (spots-grid precedent: float-arith 16x16 grids
    // prove in 10-22s).

    const N_GRID: [f64; 6] = [0.0, 1.0, 2.0, 3.0, 1e15, 1e300];
    const S_GRID: [f64; 12] = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.5,
        3.0,
        1e308,
        -1e308,
        5e-324, // min denormal
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ];

    fn pick(table: &[f64]) -> f64 {
        let i: usize = kani::any();
        kani::assume(i < table.len());
        table[i]
    }

    /// t3 grid: slots = [n, sx, sxx] with only the final's read slots drawn
    /// from the grids; unread slots literal 0.0.
    macro_rules! grid_final_t3 {
        ($($h:ident: $core:ident / $pg:ident [$n:expr, $sx:expr, $sxx:expr];)*) => {$(
            #[kani::proof]
            // Deterministic sqrt model on BOTH sides (dsqrt dual-mode fix;
            // see crate::det_sqrt_model). No-op for sqrt-free finals.
            #[kani::stub(f64::sqrt, crate::det_sqrt_model)]
            fn $h() {
                // Force codegen of the C-seam sqrt symbol (referenced only
                // from the vendored C, invisible to Kani's reachability).
                let _keep = crate::pg_proof_sqrt as extern "C" fn(f64) -> f64;
                let t = [$n, $sx, $sxx];
                let mut c_out = 0f64;
                let c = unsafe { $pg(t.as_ptr(), &mut c_out) };
                let r = adt_float::aggregates::$core(t);
                opt_eq(r, c, c_out);
                kani::cover!(r.is_none());
                kani::cover!(r.is_some());
            }
        )*};
    }

    grid_final_t3! {
        grid_avg:         float8_avg         / pg_float8_avg         [pick(&N_GRID), pick(&S_GRID), 0.0];
        grid_var_pop:     float8_var_pop     / pg_float8_var_pop     [pick(&N_GRID), 0.0, pick(&S_GRID)];
        grid_var_samp:    float8_var_samp    / pg_float8_var_samp    [pick(&N_GRID), 0.0, pick(&S_GRID)];
        grid_stddev_pop:  float8_stddev_pop  / pg_float8_stddev_pop  [pick(&N_GRID), 0.0, pick(&S_GRID)];
        grid_stddev_samp: float8_stddev_samp / pg_float8_stddev_samp [pick(&N_GRID), 0.0, pick(&S_GRID)];
    }

    /// t6 grid: slots = [n, sx, sxx, sy, syy, sxy], unread slots literal.
    macro_rules! grid_final_t6 {
        ($($h:ident: $core:ident / $pg:ident
            [$n:expr, $sx:expr, $sxx:expr, $sy:expr, $syy:expr, $sxy:expr];)*) => {$(
            #[kani::proof]
            // Deterministic sqrt model on BOTH sides (dsqrt dual-mode fix;
            // see crate::det_sqrt_model). No-op for sqrt-free finals.
            #[kani::stub(f64::sqrt, crate::det_sqrt_model)]
            fn $h() {
                let _keep = crate::pg_proof_sqrt as extern "C" fn(f64) -> f64;
                let t = [$n, $sx, $sxx, $sy, $syy, $sxy];
                let mut c_out = 0f64;
                let c = unsafe { $pg(t.as_ptr(), &mut c_out) };
                let r = adt_float::aggregates::$core(t);
                opt_eq(r, c, c_out);
                kani::cover!(r.is_none());
                kani::cover!(r.is_some());
            }
        )*};
    }

    grid_final_t6! {
        grid_regr_avgx: float8_regr_avgx / pg_float8_regr_avgx
            [pick(&N_GRID), pick(&S_GRID), 0.0, 0.0, 0.0, 0.0];
        grid_regr_avgy: float8_regr_avgy / pg_float8_regr_avgy
            [pick(&N_GRID), 0.0, 0.0, pick(&S_GRID), 0.0, 0.0];
        grid_covar_pop: float8_covar_pop / pg_float8_covar_pop
            [pick(&N_GRID), 0.0, 0.0, 0.0, 0.0, pick(&S_GRID)];
        grid_covar_samp: float8_covar_samp / pg_float8_covar_samp
            [pick(&N_GRID), 0.0, 0.0, 0.0, 0.0, pick(&S_GRID)];
        grid_regr_slope: float8_regr_slope / pg_float8_regr_slope
            [pick(&N_GRID), 0.0, pick(&S_GRID), 0.0, 0.0, pick(&S_GRID)];
        // richer circuits (two divides / sqrt-of-product / chained divide):
        // expected release-gate tier; ladder note in runqueue.txt.
        grid_regr_r2: float8_regr_r2 / pg_float8_regr_r2
            [pick(&N_GRID), 0.0, pick(&S_GRID), 0.0, pick(&S_GRID), pick(&S_GRID)];
        grid_corr: float8_corr / pg_float8_corr
            [pick(&N_GRID), 0.0, pick(&S_GRID), 0.0, pick(&S_GRID), pick(&S_GRID)];
        grid_regr_intercept: float8_regr_intercept / pg_float8_regr_intercept
            [pick(&N_GRID), pick(&S_GRID), pick(&S_GRID), pick(&S_GRID), 0.0, pick(&S_GRID)];
    }

    // ---------- transition zero-arm planes ----------

    /// float8_accum first-row plane: trans = LITERAL [0.0; 3] (prunes the
    /// N0>0 arm: no FMA, no divide, no overflow ereport reachable); newval
    /// fully symbolic. The NaN/Inf -> Sxx = get_float8_nan() routing is
    /// fully in-theorem (canonical-NAN shim makes it bit-exact).
    #[kani::proof]
    fn plane_float8_accum_first() {
        let newval = any_f64();
        let t = [0.0f64, 0.0, 0.0];
        let mut c_out = [0f64; 3];
        let c = unsafe { pg_float8_accum(t.as_ptr(), newval, c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float8_accum(t, newval);
        trans_eq(r, c, &c_out);
        kani::cover!(newval.is_nan());
        kani::cover!(newval.is_infinite());
        kani::cover!(newval.is_finite());
    }

    /// float4_accum first-row plane (f32 widen + same routing).
    #[kani::proof]
    fn plane_float4_accum_first() {
        let newval = any_f32();
        let t = [0.0f64, 0.0, 0.0];
        let mut c_out = [0f64; 3];
        let c = unsafe { pg_float4_accum(t.as_ptr(), newval, c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float4_accum(t, newval);
        trans_eq(r, c, &c_out);
        kani::cover!(newval.is_nan());
        kani::cover!(newval.is_finite());
    }

    /// float8_regr_accum first-row plane: trans = LITERAL [0.0; 6]; both
    /// newvals fully symbolic — the per-argument NaN/Inf routing matrix
    /// (sxx/sxy vs syy/sxy) is fully in-theorem. Y-before-X argument order
    /// (the shipped comment claim) is pinned by value parity.
    #[kani::proof]
    fn plane_regr_accum_first() {
        let y = any_f64();
        let x = any_f64();
        let t = [0.0f64, 0.0, 0.0, 0.0, 0.0, 0.0];
        let mut c_out = [0f64; 6];
        let c = unsafe { pg_float8_regr_accum(t.as_ptr(), y, x, c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float8_regr_accum(t, y, x);
        trans_eq(r, c, &c_out);
        kani::cover!(x.is_nan() && y.is_finite());
        kani::cover!(y.is_nan() && x.is_finite());
        kani::cover!(x.is_finite() && y.is_finite());
    }

    /// float8_combine identity planes: the parallel-worker n==0 arms.
    /// The zero side's N is LITERAL 0.0 (prunes the general arm and its
    /// float8_pl / divide chain); the other transvalue is fully symbolic.
    #[kani::proof]
    fn plane_float8_combine_n1zero() {
        let t1 = [0.0f64, any_f64(), any_f64()];
        let t2 = [any_f64(), any_f64(), any_f64()];
        let mut c_out = [0f64; 3];
        let c = unsafe { pg_float8_combine(t1.as_ptr(), t2.as_ptr(), c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float8_combine(t1, t2);
        trans_eq(r, c, &c_out);
    }

    #[kani::proof]
    fn plane_float8_combine_n2zero() {
        let t1 = [any_f64(), any_f64(), any_f64()];
        // n1 != 0 so the n2==0 arm (not the n1==0 arm) adjudicates; n2 is
        // the literal zero.
        kani::assume(t1[0] != 0.0);
        let t2 = [0.0f64, any_f64(), any_f64()];
        let mut c_out = [0f64; 3];
        let c = unsafe { pg_float8_combine(t1.as_ptr(), t2.as_ptr(), c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float8_combine(t1, t2);
        trans_eq(r, c, &c_out);
    }

    #[kani::proof]
    fn plane_regr_combine_n1zero() {
        let t1 = [0.0f64, any_f64(), any_f64(), any_f64(), any_f64(), any_f64()];
        let t2 = [
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
        ];
        let mut c_out = [0f64; 6];
        let c = unsafe { pg_float8_regr_combine(t1.as_ptr(), t2.as_ptr(), c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float8_regr_combine(t1, t2);
        trans_eq(r, c, &c_out);
    }

    #[kani::proof]
    fn plane_regr_combine_n2zero() {
        let t1 = [
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
            any_f64(),
        ];
        kani::assume(t1[0] != 0.0);
        let t2 = [0.0f64, any_f64(), any_f64(), any_f64(), any_f64(), any_f64()];
        let mut c_out = [0f64; 6];
        let c = unsafe { pg_float8_regr_combine(t1.as_ptr(), t2.as_ptr(), c_out.as_mut_ptr()) };
        let r = adt_float::aggregates::float8_regr_combine(t1, t2);
        trans_eq(r, c, &c_out);
    }

    // ---------- negative control (MUST FAIL; DEFAULT solver) ----------

    /// Shipped float8_var_samp vs C float8_var_pop: diverges at n == 0.5
    /// (var_pop returns a value, var_samp NULL) without touching any
    /// divide result — a cheap decodable counterexample proving the rig
    /// is non-vacuous.
    #[kani::proof]
    fn control_var_samp_vs_c_var_pop() {
        let t = [any_f64(), 0.0, any_f64()];
        let mut c_out = 0f64;
        let c = unsafe { pg_float8_var_pop(t.as_ptr(), &mut c_out) };
        let r = adt_float::aggregates::float8_var_samp(t);
        opt_eq(r, c, c_out);
    }
}
