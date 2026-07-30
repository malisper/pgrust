//! Kani C≡Rust equivalence: the float comparator family (30 pg_proc rows).
//!
//! FLOAT FEASIBILITY PROBE: CBMC models IEEE-754 bit-exactly; this family is
//! the first float workload in the proofs suite, and the per-harness solve
//! times are a deliverable (they calibrate the float entry in TRIAGE.md,
//! previously out-of-scope by assumption).
//!
//! Rust side: the SHIPPED fmgr wrappers — `adt_float::builtins::fc_float4eq`
//! .. `fc_btfloat84cmp`, `fc_float{4,8}{larger,smaller}` — invoked through a
//! real `LocalFcinfo<2>` frame, so the proof covers datum unwrap (arg f32/f64
//! by value) → NaN-aware core → Datum::from_bool/from_i32/from_f32/from_f64.
//! C side: vendored float.c + float.h inline helpers (c/pg_float_cmp.c).
//!
//! Domains: FULLY SYMBOLIC f32/f64 pairs — every NaN payload (quiet and
//! signaling, both signs), ±Inf, ±0, subnormals. No assumes anywhere: the
//! comparators are total. NaN ordering is exactly where a port would silently
//! diverge (Postgres sorts NaN last and all-NaNs-equal, NOT IEEE semantics),
//! so the NaN subspace being inside the theorem is the point.
//!
//! Mixed-width float48/84: C widens float4→float8 (exact); the harness
//! asserts the shipped Rust `as f64` path agrees on the full f32×f64 space.
//!
//! larger/smaller return floats: compared by to_bits() — bit-exact, so ±0
//! selection and NaN payload propagation are inside the theorem too.
//!
//! Negative control: control_float4eq_vs_ieee pits shipped fc_float4eq
//! against plain IEEE == (pg_float4eq_ieee, control-only C). MUST fail with
//! a NaN counterexample — proof the rig is non-vacuous AND that the solver
//! explores the NaN subspace. Run it with the DEFAULT solver (kissat is
//! non-incremental and never terminates on failing harnesses);
//! expected-green harnesses run with kissat.

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        fn pg_float4eq(a: f32, b: f32) -> c_int;
        fn pg_float4ne(a: f32, b: f32) -> c_int;
        fn pg_float4lt(a: f32, b: f32) -> c_int;
        fn pg_float4le(a: f32, b: f32) -> c_int;
        fn pg_float4gt(a: f32, b: f32) -> c_int;
        fn pg_float4ge(a: f32, b: f32) -> c_int;
        fn pg_btfloat4cmp(a: f32, b: f32) -> c_int;

        fn pg_float8eq(a: f64, b: f64) -> c_int;
        fn pg_float8ne(a: f64, b: f64) -> c_int;
        fn pg_float8lt(a: f64, b: f64) -> c_int;
        fn pg_float8le(a: f64, b: f64) -> c_int;
        fn pg_float8gt(a: f64, b: f64) -> c_int;
        fn pg_float8ge(a: f64, b: f64) -> c_int;
        fn pg_btfloat8cmp(a: f64, b: f64) -> c_int;

        fn pg_float48eq(a: f32, b: f64) -> c_int;
        fn pg_float48ne(a: f32, b: f64) -> c_int;
        fn pg_float48lt(a: f32, b: f64) -> c_int;
        fn pg_float48le(a: f32, b: f64) -> c_int;
        fn pg_float48gt(a: f32, b: f64) -> c_int;
        fn pg_float48ge(a: f32, b: f64) -> c_int;
        fn pg_btfloat48cmp(a: f32, b: f64) -> c_int;

        fn pg_float84eq(a: f64, b: f32) -> c_int;
        fn pg_float84ne(a: f64, b: f32) -> c_int;
        fn pg_float84lt(a: f64, b: f32) -> c_int;
        fn pg_float84le(a: f64, b: f32) -> c_int;
        fn pg_float84gt(a: f64, b: f32) -> c_int;
        fn pg_float84ge(a: f64, b: f32) -> c_int;
        fn pg_btfloat84cmp(a: f64, b: f32) -> c_int;

        fn pg_float4larger(a: f32, b: f32) -> f32;
        fn pg_float4smaller(a: f32, b: f32) -> f32;
        fn pg_float8larger(a: f64, b: f64) -> f64;
        fn pg_float8smaller(a: f64, b: f64) -> f64;

        // Negative control only — NOT Postgres code.
        fn pg_float4eq_ieee(a: f32, b: f32) -> c_int;
    }

    /// Run a shipped fc_* wrapper on a 2-arg frame; the comparators never
    /// error, so the Err arm is statically dead.
    fn call<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
        b: Datum,
    ) -> Datum {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("comparator errored"),
        }
    }

    /// Fully symbolic f32/f64 (via bits, so every NaN payload is explored;
    /// kani::any::<f32>() is equivalent but the bit route makes it explicit).
    fn any_f32() -> f32 {
        f32::from_bits(kani::any())
    }
    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    /// bool-returning ops: (harness: fc / pg, lhs-gen, rhs-gen, lhs-pack, rhs-pack)
    macro_rules! bool_op {
        ($($h:ident: $fc:ident / $pg:ident ($ga:ident, $gb:ident; $pa:ident, $pb:ident);)*) => {$(
            #[kani::proof]
            fn $h() {
                let a = $ga();
                let b = $gb();
                let r = call(adt_float::builtins::$fc, Datum::$pa(a), Datum::$pb(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    bool_op! {
        eq_float4eq: fc_float4eq / pg_float4eq (any_f32, any_f32; from_f32, from_f32);
        eq_float4ne: fc_float4ne / pg_float4ne (any_f32, any_f32; from_f32, from_f32);
        eq_float4lt: fc_float4lt / pg_float4lt (any_f32, any_f32; from_f32, from_f32);
        eq_float4le: fc_float4le / pg_float4le (any_f32, any_f32; from_f32, from_f32);
        eq_float4gt: fc_float4gt / pg_float4gt (any_f32, any_f32; from_f32, from_f32);
        eq_float4ge: fc_float4ge / pg_float4ge (any_f32, any_f32; from_f32, from_f32);

        eq_float8eq: fc_float8eq / pg_float8eq (any_f64, any_f64; from_f64, from_f64);
        eq_float8ne: fc_float8ne / pg_float8ne (any_f64, any_f64; from_f64, from_f64);
        eq_float8lt: fc_float8lt / pg_float8lt (any_f64, any_f64; from_f64, from_f64);
        eq_float8le: fc_float8le / pg_float8le (any_f64, any_f64; from_f64, from_f64);
        eq_float8gt: fc_float8gt / pg_float8gt (any_f64, any_f64; from_f64, from_f64);
        eq_float8ge: fc_float8ge / pg_float8ge (any_f64, any_f64; from_f64, from_f64);

        eq_float48eq: fc_float48eq / pg_float48eq (any_f32, any_f64; from_f32, from_f64);
        eq_float48ne: fc_float48ne / pg_float48ne (any_f32, any_f64; from_f32, from_f64);
        eq_float48lt: fc_float48lt / pg_float48lt (any_f32, any_f64; from_f32, from_f64);
        eq_float48le: fc_float48le / pg_float48le (any_f32, any_f64; from_f32, from_f64);
        eq_float48gt: fc_float48gt / pg_float48gt (any_f32, any_f64; from_f32, from_f64);
        eq_float48ge: fc_float48ge / pg_float48ge (any_f32, any_f64; from_f32, from_f64);

        eq_float84eq: fc_float84eq / pg_float84eq (any_f64, any_f32; from_f64, from_f32);
        eq_float84ne: fc_float84ne / pg_float84ne (any_f64, any_f32; from_f64, from_f32);
        eq_float84lt: fc_float84lt / pg_float84lt (any_f64, any_f32; from_f64, from_f32);
        eq_float84le: fc_float84le / pg_float84le (any_f64, any_f32; from_f64, from_f32);
        eq_float84gt: fc_float84gt / pg_float84gt (any_f64, any_f32; from_f64, from_f32);
        eq_float84ge: fc_float84ge / pg_float84ge (any_f64, any_f32; from_f64, from_f32);
    }

    /// i32-returning btree comparators.
    macro_rules! cmp_op {
        ($($h:ident: $fc:ident / $pg:ident ($ga:ident, $gb:ident; $pa:ident, $pb:ident);)*) => {$(
            #[kani::proof]
            fn $h() {
                let a = $ga();
                let b = $gb();
                let r = call(adt_float::builtins::$fc, Datum::$pa(a), Datum::$pb(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_i32() == c);
            }
        )*};
    }

    cmp_op! {
        eq_btfloat4cmp: fc_btfloat4cmp / pg_btfloat4cmp (any_f32, any_f32; from_f32, from_f32);
        eq_btfloat8cmp: fc_btfloat8cmp / pg_btfloat8cmp (any_f64, any_f64; from_f64, from_f64);
        eq_btfloat48cmp: fc_btfloat48cmp / pg_btfloat48cmp (any_f32, any_f64; from_f32, from_f64);
        eq_btfloat84cmp: fc_btfloat84cmp / pg_btfloat84cmp (any_f64, any_f32; from_f64, from_f32);
    }

    /// larger/smaller return a float: compare BIT PATTERNS, so ±0 selection
    /// and NaN payload propagation are part of the theorem.
    macro_rules! sel_op {
        ($($h:ident: $fc:ident / $pg:ident ($g:ident; $pack:ident, $unpack:ident);)*) => {$(
            #[kani::proof]
            fn $h() {
                let a = $g();
                let b = $g();
                let r = call(adt_float::builtins::$fc, Datum::$pack(a), Datum::$pack(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.$unpack().to_bits() == c.to_bits());
            }
        )*};
    }

    sel_op! {
        eq_float4larger: fc_float4larger / pg_float4larger (any_f32; from_f32, as_f32);
        eq_float4smaller: fc_float4smaller / pg_float4smaller (any_f32; from_f32, as_f32);
        eq_float8larger: fc_float8larger / pg_float8larger (any_f64; from_f64, as_f64);
        eq_float8smaller: fc_float8smaller / pg_float8smaller (any_f64; from_f64, as_f64);
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_float4eq (NaN-aware, all NaNs equal)
    /// vs plain IEEE == . MUST fail with a NaN counterexample, witnessing
    /// both non-vacuity and NaN-subspace exploration. Run with the DEFAULT
    /// solver.
    #[kani::proof]
    fn control_float4eq_vs_ieee() {
        let a = any_f32();
        let b = any_f32();
        let r = call(adt_float::builtins::fc_float4eq, Datum::from_f32(a), Datum::from_f32(b));
        let c = unsafe { pg_float4eq_ieee(a, b) };
        assert!(r.as_bool() as c_int == c);
    }
}
