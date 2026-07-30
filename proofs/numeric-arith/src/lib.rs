//! Kani C≡Rust equivalence: FIRST theorems on pgrust numeric ARITHMETIC —
//! add_var / sub_var / mul_var (zero + short-kernel planes).
//!
//! Rust side (shipped code, path-dep — never copied):
//!   adt_numeric::fixed::{add_var_fixed, sub_var_fixed, mul_var_fixed} on
//!   FixedVar<16> — the fixed-buffer mirrors of the allocating kernels
//!   (fixed.rs, landed 38f13030d1).  The allocating kernels themselves are
//!   a measured symex wall (DigitBuf::realloc_uninit keeps heap/DIGIT_POOL
//!   machinery in the formula — proofs/TRIAGE.md numeric-arithmetic wall
//!   entry); the mirrors are line-for-line the same algorithm on a
//!   caller-provided stack array.  The native test at the bottom ties the
//!   mirrors to the shipped allocating add_var/sub_var/mul_var exhaustively
//!   on a structured grid (mirror-consistency witness, tested tier).
//!
//! C side: c/pg_numeric_arith.c — verbatim REL_18_STABLE numeric.c
//! add_var/sub_var/add_abs/sub_abs/cmp_abs/cmp_abs_common/strip_var/
//! zero_var/mul_var_short + mul_var's zero/short-delegation head
//! (provenance + shims there).
//!
//! Fences and claims (mirror into the ledger):
//!  - VAR-LEVEL: theorems quantify over spec-level NumericVar tuples
//!    (sign, weight, dscale, digits[], ndigits) — the layer BELOW the
//!    packed image codec (that codec is proven separately in
//!    proofs/numeric-probe).  Both sides are fed the same tuple; the whole
//!    digit pipeline (index arithmetic, carry/borrow propagation, strip,
//!    sign dispatch, dscale merge) is inside the theorem.
//!  - DIGIT-RANGE FENCE: each digit in [0, 9999] (outside it the value is
//!    meaningless and C's int arithmetic diverges from i16 by
//!    representation, not logic).  Leading/trailing ZERO digits are
//!    ALLOWED (no strip fence) — the C strip loops run live.
//!  - PLANE: ndigits <= 4 per side, weight in [-4, 4], dscale in [0, 6]
//!    for add/sub (result ndigits <= 13 < N=16, so the fixed mirror's
//!    fallback arm `alloc -> None` is provably unreachable — the harness
//!    PANICS on None, a loud trap, never a silent fence).  mul cells per
//!    nd cap below.
//!  - MUL PLANE (SHIM A3, loud on both sides): zero shortcut + short
//!    kernel (shorter input 1..=6 digits AND rscale == dscale1 + dscale2),
//!    exactly the cases mul_var_fixed covers by design; the full pairwise
//!    kernel is out-of-plane — C side sets a trap flag the harness asserts
//!    ZERO, Rust side would return None -> panic.  A dedicated
//!    fallback-ROUTING harness proves the two sides agree on the plane
//!    boundary itself (rscale != d1+d2 => C trap == 1 AND Rust None).
//!  - The result comparison is full-field: sign, weight, dscale, ndigits,
//!    and every result digit.
//!
//! Negative control (DEFAULT solver, must FAIL): C sees weight2+1 on add.

#[cfg(kani)]
mod proofs {
    use adt_numeric::{
        add_var_fixed, mul_var_fixed, sub_var_fixed, FixedVar, VarView, NUMERIC_NEG, NUMERIC_POS,
    };
    use std::os::raw::c_int;

    extern "C" {
        fn pg_arith_add_var(
            sign1: c_int, weight1: c_int, dscale1: c_int, digits1: *const i16, nd1: c_int,
            sign2: c_int, weight2: c_int, dscale2: c_int, digits2: *const i16, nd2: c_int,
            out_sign: *mut c_int, out_weight: *mut c_int, out_dscale: *mut c_int,
            out_digits: *mut i16, out_nd: *mut c_int,
        ) -> c_int;
        fn pg_arith_sub_var(
            sign1: c_int, weight1: c_int, dscale1: c_int, digits1: *const i16, nd1: c_int,
            sign2: c_int, weight2: c_int, dscale2: c_int, digits2: *const i16, nd2: c_int,
            out_sign: *mut c_int, out_weight: *mut c_int, out_dscale: *mut c_int,
            out_digits: *mut i16, out_nd: *mut c_int,
        ) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_arith_mul_var(
            sign1: c_int, weight1: c_int, dscale1: c_int, digits1: *const i16, nd1: c_int,
            sign2: c_int, weight2: c_int, dscale2: c_int, digits2: *const i16, nd2: c_int,
            rscale: c_int, trap: *mut c_int,
            out_sign: *mut c_int, out_weight: *mut c_int, out_dscale: *mut c_int,
            out_digits: *mut i16, out_nd: *mut c_int,
        ) -> c_int;
    }

    /// Max digits per side; per-harness caps are assumes below it.
    const NDMAX: usize = 4;
    /// FixedVar capacity: add/sub res_ndigits <= (nd - wmin - 1)max +
    /// (wmax + 1) + 1 = 7 + 5 + 1 = 13, mul <= 4 + 4 + (spare) — both fit
    /// with the +1 spare carry digit under 16.
    const N: usize = 16;

    /// One symbolic spec-level numeric var within the digit-range fence.
    /// Unused digit slots are LITERAL ZERO (dead-symbolic-bytes law).
    struct SVar {
        digits: [i16; NDMAX],
        nd: usize,
        weight: i32,
        sign: u16,
        dscale: i32,
    }

    fn sym_var(nd_cap: usize, w_bound: i32, ds_bound: i32) -> SVar {
        let nd: usize = kani::any();
        kani::assume(nd <= nd_cap);
        let mut digits: [i16; NDMAX] = kani::any();
        for i in 0..NDMAX {
            if i < nd {
                kani::assume(digits[i] >= 0 && digits[i] < 10000);
            } else {
                digits[i] = 0;
            }
        }
        let weight: i32 = kani::any();
        kani::assume(weight >= -w_bound && weight <= w_bound);
        let neg: bool = kani::any();
        let dscale: i32 = kani::any();
        kani::assume(dscale >= 0 && dscale <= ds_bound);
        SVar {
            digits,
            nd,
            weight,
            sign: if neg { NUMERIC_NEG } else { NUMERIC_POS },
            dscale,
        }
    }

    impl SVar {
        fn view(&self) -> VarView<'_> {
            VarView {
                ndigits: self.nd as i32,
                weight: self.weight,
                sign: self.sign,
                dscale: self.dscale,
                digits: &self.digits[..self.nd],
            }
        }
    }

    struct COut {
        sign: c_int,
        weight: c_int,
        dscale: c_int,
        digits: [i16; N],
        nd: c_int,
    }

    /// Full-field match as ONE assertion: under kissat every assert is a
    /// separate external SAT solve (~58s each measured here on the 21-assert
    /// split form) — fuse the claim into a single property.
    fn assert_match(c: &COut, r: &FixedVar<N>) {
        let mut ok = r.sign as c_int == c.sign
            && r.weight == c.weight
            && r.dscale == c.dscale
            && r.ndigits == c.nd;
        let rd = r.digits();
        let mut i = 0;
        while i < rd.len() {
            ok = ok && rd[i] == c.digits[i];
            i += 1;
        }
        assert!(ok);
    }

    macro_rules! addsub_harness {
        ($($name:ident: $cfn:ident, $rfn:ident, $w2_skew:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(15)]
            fn $name() {
                let a = sym_var(NDMAX, 4, 6);
                let b = sym_var(NDMAX, 4, 6);
                let mut c = COut { sign: 0, weight: 0, dscale: 0, digits: [0; N], nd: 0 };
                unsafe {
                    $cfn(
                        a.sign as c_int, a.weight, a.dscale, a.digits.as_ptr(), a.nd as c_int,
                        b.sign as c_int, b.weight + $w2_skew, b.dscale,
                        b.digits.as_ptr(), b.nd as c_int,
                        &mut c.sign, &mut c.weight, &mut c.dscale,
                        c.digits.as_mut_ptr(), &mut c.nd,
                    );
                }
                let mut r = FixedVar::<N>::new();
                // loud trap: within the plane the fixed mirror MUST fit N
                if $rfn(a.view(), b.view(), &mut r).is_none() {
                    panic!("fixed mirror fallback arm fired inside the fenced plane");
                }
                assert_match(&c, &r);
            }
        )*};
    }

    addsub_harness! {
        eq_add_var_nd4: pg_arith_add_var, add_var_fixed, 0;
        eq_sub_var_nd4: pg_arith_sub_var, sub_var_fixed, 0;
    }

    // Regime-reachability witnesses, HOISTED into one harness (each inline
    // cover = one extra SAT call under kissat): same builder, same fences,
    // so reachability transfers to the eq harnesses.
    #[kani::proof]
    #[kani::unwind(15)]
    fn cover_addsub_regimes() {
        let a = sym_var(NDMAX, 4, 6);
        let b = sym_var(NDMAX, 4, 6);
        let mut r = FixedVar::<N>::new();
        let ok = add_var_fixed(a.view(), b.view(), &mut r).is_some();
        kani::cover!(ok && a.sign != b.sign && r.ndigits == 0); // exact cancel (zero arm)
        kani::cover!(ok && a.sign != b.sign && r.sign == NUMERIC_NEG); // sub_abs swap arm
        kani::cover!(ok && a.sign == b.sign && r.weight == a.weight.max(b.weight) + 1
            && r.ndigits > 0); // carry out the top (add_abs growth survives strip)
        kani::cover!(ok && a.nd > 0 && a.digits[0] == 0); // leading zero in (strip live)
        kani::cover!(ok && a.weight != b.weight); // digit alignment offset
        let mut r2 = FixedVar::<N>::new();
        let ok2 = sub_var_fixed(a.view(), b.view(), &mut r2).is_some();
        kani::cover!(ok2 && a.sign == b.sign && r2.ndigits == 0); // sub tie arm
        kani::cover!(ok2 && a.sign == NUMERIC_POS && b.sign == NUMERIC_NEG); // add_abs arm
    }

    // ---- mul: zero + short-kernel plane, per-nd-cap cells (wall curve;
    // the largest green cell is the standing gate — smaller cells are
    // strictly nested, no union coverage needed) ----

    macro_rules! mul_harness {
        ($($name:ident: $cap:expr, unwind = $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            fn $name() {
                let a = sym_var($cap, 4, 6);
                let b = sym_var($cap, 4, 6);
                let rscale = a.dscale + b.dscale; // exact-product plane
                let mut trap: c_int = 0;
                let mut c = COut { sign: 0, weight: 0, dscale: 0, digits: [0; N], nd: 0 };
                unsafe {
                    pg_arith_mul_var(
                        a.sign as c_int, a.weight, a.dscale, a.digits.as_ptr(), a.nd as c_int,
                        b.sign as c_int, b.weight, b.dscale, b.digits.as_ptr(), b.nd as c_int,
                        rscale, &mut trap,
                        &mut c.sign, &mut c.weight, &mut c.dscale,
                        c.digits.as_mut_ptr(), &mut c.nd,
                    );
                }
                assert!(trap == 0); // C-side loud trap: plane must not escape
                let mut r = FixedVar::<N>::new();
                match mul_var_fixed(a.view(), b.view(), &mut r, rscale) {
                    Some(()) => {}
                    None => panic!("mul_var_fixed fell back inside the short-kernel plane"),
                }
                assert_match(&c, &r);
            }
        )*};
    }

    mul_harness! {
        eq_mul_var_nd2: 2, unwind = 8;
        eq_mul_var_nd3: 3, unwind = 10;
        eq_mul_var_nd4: 4, unwind = 12;
    }

    /// Plane-BOUNDARY routing parity: for rscale != dscale1 + dscale2 with
    /// nonzero inputs, C's verbatim delegation head routes to the full
    /// pairwise kernel (trap == 1) and mul_var_fixed returns None — the
    /// fallback fence itself is machine-checked to sit at the same place on
    /// both sides.
    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_mul_fallback_routing() {
        let a = sym_var(2, 4, 6);
        let b = sym_var(2, 4, 6);
        kani::assume(a.nd >= 1 && b.nd >= 1); // zero shortcut is rscale-blind
        let rscale: i32 = kani::any();
        kani::assume(rscale >= 0 && rscale <= 20);
        kani::assume(rscale != a.dscale + b.dscale);
        let mut trap: c_int = 0;
        let mut c = COut { sign: 0, weight: 0, dscale: 0, digits: [0; N], nd: 0 };
        unsafe {
            pg_arith_mul_var(
                a.sign as c_int, a.weight, a.dscale, a.digits.as_ptr(), a.nd as c_int,
                b.sign as c_int, b.weight, b.dscale, b.digits.as_ptr(), b.nd as c_int,
                rscale, &mut trap,
                &mut c.sign, &mut c.weight, &mut c.dscale,
                c.digits.as_mut_ptr(), &mut c.nd,
            );
        }
        assert!(trap == 1);
        let mut r = FixedVar::<N>::new();
        assert!(mul_var_fixed(a.view(), b.view(), &mut r, rscale).is_none());
    }

    /// mul regime witnesses (hoisted).
    #[kani::proof]
    #[kani::unwind(12)]
    fn cover_mul_regimes() {
        let a = sym_var(NDMAX, 4, 6);
        let b = sym_var(NDMAX, 4, 6);
        let rscale = a.dscale + b.dscale;
        let mut r = FixedVar::<N>::new();
        let ok = mul_var_fixed(a.view(), b.view(), &mut r, rscale).is_some();
        kani::cover!(ok && (a.nd == 0 || b.nd == 0) && r.ndigits == 0); // zero shortcut
        kani::cover!(ok && a.nd == NDMAX && b.nd == NDMAX); // deepest PRODSUM ladder
        kani::cover!(ok && a.nd > b.nd); // normalization swap arm
        kani::cover!(ok && r.ndigits > 0 && a.sign != b.sign && r.sign == NUMERIC_NEG);
        kani::cover!(ok && r.ndigits == a.nd as i32 + b.nd as i32 - 1); // leading digit stripped
    }

    // ---- negative control: rig is non-vacuous (DEFAULT solver, MUST FAIL:
    // C sees weight2+1, which shifts var2's digit alignment) ----
    addsub_harness! {
        control_add_var_weight_skew: pg_arith_add_var, add_var_fixed, 1;
    }
}

/// Native mirror-consistency witness (tested tier): the fixed-buffer
/// mirrors under proof are LINE-FOR-LINE the shipped allocating kernels;
/// this test pins that claim by running both on a structured exhaustive
/// grid and comparing full result fields.  With it, the Kani theorems
/// (fixed ≡ verbatim C) transfer to the shipped allocating path modulo
/// this witnessed consistency.
#[cfg(test)]
mod mirror_consistency {
    use adt_numeric::{
        add_var, add_var_fixed, mul_var, mul_var_fixed, sub_var, sub_var_fixed, FixedVar,
        NumericVar, VarView, NUMERIC_NEG, NUMERIC_POS,
    };

    const DIGSETS: [&[i16]; 8] = [
        &[],
        &[1],
        &[9999],
        &[0, 5],
        &[1, 0],
        &[9999, 9999],
        &[3, 0, 7],
        &[9999, 0, 0, 1],
    ];

    fn views() -> Vec<(Vec<i16>, i32, u16, i32)> {
        let mut v = Vec::new();
        for ds in DIGSETS {
            for w in -2i32..=2 {
                for sign in [NUMERIC_POS, NUMERIC_NEG] {
                    for dscale in [0i32, 3] {
                        v.push((ds.to_vec(), w, sign, dscale));
                    }
                }
            }
        }
        v
    }

    fn check(op: &str, a: VarView<'_>, b: VarView<'_>, alloc: &NumericVar, fixed: &FixedVar<16>) {
        assert_eq!(alloc.sign, fixed.sign, "{op} sign {a:?} {b:?}");
        assert_eq!(alloc.weight, fixed.weight, "{op} weight {a:?} {b:?}");
        assert_eq!(alloc.dscale, fixed.dscale, "{op} dscale {a:?} {b:?}");
        assert_eq!(alloc.ndigits, fixed.ndigits, "{op} ndigits {a:?} {b:?}");
        assert_eq!(alloc.digits(), fixed.digits(), "{op} digits {a:?} {b:?}");
    }

    #[test]
    fn fixed_mirrors_match_allocating_kernels() {
        let vs = views();
        let mut pairs = 0u32;
        let mut mul_in_plane = 0u32;
        for (d1, w1, s1, ds1) in &vs {
            for (d2, w2, s2, ds2) in &vs {
                let a = VarView {
                    ndigits: d1.len() as i32,
                    weight: *w1,
                    sign: *s1,
                    dscale: *ds1,
                    digits: d1,
                };
                let b = VarView {
                    ndigits: d2.len() as i32,
                    weight: *w2,
                    sign: *s2,
                    dscale: *ds2,
                    digits: d2,
                };

                let mut alloc = NumericVar::new();
                add_var(a, b, &mut alloc);
                let mut fixed = FixedVar::<16>::new();
                add_var_fixed(a, b, &mut fixed).expect("add fits 16");
                check("add", a, b, &alloc, &fixed);

                let mut alloc = NumericVar::new();
                sub_var(a, b, &mut alloc);
                let mut fixed = FixedVar::<16>::new();
                sub_var_fixed(a, b, &mut fixed).expect("sub fits 16");
                check("sub", a, b, &alloc, &fixed);

                // exact-product plane (the fixed mirror's whole mul domain)
                let rscale = ds1 + ds2;
                let mut fixed = FixedVar::<16>::new();
                if mul_var_fixed(a, b, &mut fixed, rscale).is_some() {
                    let mut alloc = NumericVar::new();
                    mul_var(a, b, &mut alloc, rscale);
                    check("mul", a, b, &alloc, &fixed);
                    mul_in_plane += 1;
                }
                pairs += 1;
            }
        }
        assert_eq!(pairs, (vs.len() * vs.len()) as u32);
        // vacuity guard: the short-kernel plane must actually cover the grid
        assert!(mul_in_plane == pairs, "grid inputs all lie in the short plane");
    }
}
