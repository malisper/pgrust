//! Kani C≡Rust equivalence: the geometric comparator / containment family
//! (point and box relational operators, geo_ops.c).
//!
//! THE PARITY SURFACE here is the fuzzy-compare semantics: EPSILON = 1.0E-06
//! and the exact comparison shapes (`A == B || fabs(A-B) <= EPSILON`,
//! `A + EPSILON < B`, ...) from geo_decls.h, plus point_eq_point's
//! any-NaN-goes-exact special case. A port that used exact IEEE compares —
//! or put EPSILON on the wrong side of an asymmetric compare — would
//! diverge inside the epsilon band; both the constant and the direction are
//! inside every theorem because the FP helpers are fully expanded in both
//! circuits. The negative control (control_point_left_vs_ieee) pits the
//! shipped fuzzy FPlt against exact IEEE `<` and MUST fail with a
//! counterexample in the epsilon band — witness that the fuzz is
//! load-bearing, not vacuously equal.
//!
//! Rust side: the SHIPPED fmgr wrappers `adt_geo::builtins::fc_*`, invoked
//! through a real `LocalFcinfo` frame with by-ref point (16B) / box (32B)
//! datum images — so the by-ref datum unwrap (pointer word -> arg_fixed ->
//! from_datum_bytes) is inside the theorem (first by-ref fixed-len family;
//! datetime-cmp screened by-value only). box_center is proved at the CORE
//! (`adt_geo::boxes::box_cn`) because the shipped wrapper's result ride is
//! a result-mcx palloc (Mcx trap, not part of the comparator claim).
//!
//! C side: c/pg_geo_cmp.c — geo_ops.c bodies verbatim @ REL_18_STABLE with
//! geo_decls.h/float.h inlines; ereports shimmed to a first-error-wins flag
//! (all reachable errors are sqlstate 22003).
//!
//! Domains:
//!   - All pure-compare operators (points; box position/containment/same/
//!     overlap): FULLY SYMBOLIC f64 coordinates via from_bits — every NaN
//!     payload, ±Inf, ±0, subnormals. Total functions, no assumes.
//!   - Area-based comparators (box_lt/gt/eq/le/ge) and box_area contain a
//!     53-bit symbolic×symbolic multiply (box_wd*box_ht) = WALL per the
//!     float-arith cost law. Treatment: (a) full-symbolic probe recorded
//!     honestly, (b) zero-area / degenerate planes with a LITERAL-zero
//!     multiplicand where feasible, (c) a concrete boundary GRID harness
//!     (areas equal / ±exactly-EPSILON / just-inside / zero / negative /
//!     ±Inf / NaN / overflow-error / underflow-error cells, all pairs) —
//!     spot theorems, not full-domain coverage; bounds recorded per row.
//!   - box_width/box_height: float8_mi only (add/sub class) — full domain,
//!     value bit-exact (to_bits) + error-verdict + sqlstate parity.
//!
//! Fallible harnesses stub `types_error::PgError::error` (proof_support,
//! field-identical minus Location/message-text machinery — value-space
//! only) and `mem::forget` the Err payload (Box<PgError> drop glue is a
//! measured symex wall). Both arms carry kani::cover! witnesses.
//!
//! Run: cd proofs/geo-cmp && timeout 30 cargo kani -Z c-ffi \
//!        --c-lib c/pg_geo_cmp.c --no-overflow-checks --solver kissat \
//!        --harness <h> --exact
//! (--no-overflow-checks: Kani's default NaN-production check fires on
//! legal IEEE Inf-Inf inside FPeq/FPne — property noise, not parity.
//! Negative control runs with the DEFAULT solver, no kissat.)

mod ext;
mod ext2;

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use proof_support::{call1, call2, stubs};
    use types_core::geo::{Point, BOX};
    use types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;

    use std::os::raw::c_int;

    extern "C" {
        fn pg_point_left(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_right(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_above(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_below(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_vert(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_horiz(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_eq(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_point_ne(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;

        #[allow(clippy::too_many_arguments)]
        fn pg_box_same(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_overlap(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_left(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_overleft(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_right(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_overright(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_below(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_overbelow(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_above(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_overabove(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_contained(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_contain(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_below_eq(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_above_eq(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_box_contain_pt(bhx: f64, bhy: f64, blx: f64, bly: f64, px: f64, py: f64) -> c_int;

        fn pg_box_lt(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;
        fn pg_box_gt(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;
        fn pg_box_eq(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;
        fn pg_box_le(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;
        fn pg_box_ge(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;

        fn pg_box_area(hx: f64, hy: f64, lx: f64, ly: f64, result: *mut f64) -> c_int;
        fn pg_box_width(hx: f64, hy: f64, lx: f64, ly: f64, result: *mut f64) -> c_int;
        fn pg_box_height(hx: f64, hy: f64, lx: f64, ly: f64, result: *mut f64) -> c_int;
        fn pg_box_center(hx: f64, hy: f64, lx: f64, ly: f64, cx: *mut f64, cy: *mut f64) -> c_int;

        // Negative control only — NOT Postgres code.
        fn pg_point_left_ieee(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    /// Shipped-wrapper call with two by-ref point datums (16-byte images).
    fn rust_point_op(
        fc: proof_support::FcFn<Box<types_error::PgError>>,
        p1: Point,
        p2: Point,
    ) -> bool {
        let i1 = p1.to_datum_bytes();
        let i2 = p2.to_datum_bytes();
        match call2(fc, i1.as_ptr(), i2.as_ptr()) {
            Ok(d) => d.as_bool(),
            Err(_) => panic!("infallible geo comparator errored"),
        }
    }

    /// Shipped-wrapper call with two by-ref box datums (32-byte images).
    fn rust_box_op(
        fc: proof_support::FcFn<Box<types_error::PgError>>,
        b1: BOX,
        b2: BOX,
    ) -> Result<bool, Box<types_error::PgError>> {
        let i1 = b1.to_datum_bytes();
        let i2 = b2.to_datum_bytes();
        call2(fc, i1.as_ptr(), i2.as_ptr()).map(Datum::as_bool)
    }

    fn pt(x: f64, y: f64) -> Point {
        Point { x, y }
    }

    fn bx(hx: f64, hy: f64, lx: f64, ly: f64) -> BOX {
        BOX {
            high: pt(hx, hy),
            low: pt(lx, ly),
        }
    }

    // ---------- point comparators: fully symbolic, total ----------

    macro_rules! point_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let r = rust_point_op(adt_geo::builtins::$fc, pt(x1, y1), pt(x2, y2));
                let c = unsafe { $pg(x1, y1, x2, y2) };
                assert!(r as c_int == c);
            }
        )*};
    }

    point_op! {
        eq_point_left: fc_point_left / pg_point_left;
        eq_point_right: fc_point_right / pg_point_right;
        eq_point_above: fc_point_above / pg_point_above;
        eq_point_below: fc_point_below / pg_point_below;
        eq_point_vert: fc_point_vert / pg_point_vert;
        eq_point_horiz: fc_point_horiz / pg_point_horiz;
        eq_point_eq: fc_point_eq / pg_point_eq;
        eq_point_ne: fc_point_ne / pg_point_ne;
    }

    // ---------- box position/containment/same/overlap: fully symbolic ----------

    macro_rules! box_bool_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (ahx, ahy, alx, aly) = (any_f64(), any_f64(), any_f64(), any_f64());
                let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
                let r = match rust_box_op(
                    adt_geo::builtins::$fc,
                    bx(ahx, ahy, alx, aly),
                    bx(bhx, bhy, blx, bly),
                ) {
                    Ok(v) => v,
                    Err(_) => panic!("infallible geo comparator errored"),
                };
                let c = unsafe { $pg(ahx, ahy, alx, aly, bhx, bhy, blx, bly) };
                assert!(r as c_int == c);
            }
        )*};
    }

    box_bool_op! {
        eq_box_same: fc_box_same / pg_box_same;
        eq_box_overlap: fc_box_overlap / pg_box_overlap;
        eq_box_left: fc_box_left / pg_box_left;
        eq_box_overleft: fc_box_overleft / pg_box_overleft;
        eq_box_right: fc_box_right / pg_box_right;
        eq_box_overright: fc_box_overright / pg_box_overright;
        eq_box_below: fc_box_below / pg_box_below;
        eq_box_overbelow: fc_box_overbelow / pg_box_overbelow;
        eq_box_above: fc_box_above / pg_box_above;
        eq_box_overabove: fc_box_overabove / pg_box_overabove;
        eq_box_contained: fc_box_contained / pg_box_contained;
        eq_box_contain: fc_box_contain / pg_box_contain;
        eq_box_below_eq: fc_box_below_eq / pg_box_below_eq;
        eq_box_above_eq: fc_box_above_eq / pg_box_above_eq;
    }

    /// box_contain_pt(box, pt) — the deliberately EXACT (non-fuzzy)
    /// containment; also discharges on_pb (same shipped core + same C body,
    /// args swapped at the fmgr layer on both sides).
    #[kani::proof]
    fn eq_box_contain_pt() {
        let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (px, py) = (any_f64(), any_f64());
        let ib = bx(bhx, bhy, blx, bly).to_datum_bytes();
        let ip = pt(px, py).to_datum_bytes();
        let r = match call2(adt_geo::builtins::fc_box_contain_pt, ib.as_ptr(), ip.as_ptr()) {
            Ok(d) => d.as_bool(),
            Err(_) => panic!("infallible geo comparator errored"),
        };
        let c = unsafe { pg_box_contain_pt(bhx, bhy, blx, bly, px, py) };
        assert!(r as c_int == c);
    }

    // ---------- fallible ops: verdict + sqlstate + value parity ----------

    /// Adjudicate a fallible Rust arm against the C err flag. All errors
    /// reachable in this family are 22003 (overflow=1 / underflow=2);
    /// value-space only (PgError::error stubbed; message/Location out of
    /// proof). Err payload is forgotten (Box<PgError> drop glue = measured
    /// symex wall). Returns Some(value) on the clean arm.
    fn adjudicate<T>(r: Result<T, Box<types_error::PgError>>, cerr: c_int) -> Option<T> {
        match r {
            Ok(v) => {
                kani::cover!(true); // vacuity witness: Ok arm explored
                assert!(cerr == 0);
                Some(v)
            }
            Err(e) => {
                kani::cover!(true); // vacuity witness: Err arm explored
                assert!(cerr != 0);
                assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                core::mem::forget(e);
                None
            }
        }
    }

    // box_width / box_height: float8_mi only — full symbolic domain.
    macro_rules! box_f64_op1 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
                let img = bx(hx, hy, lx, ly).to_datum_bytes();
                let r = call1(adt_geo::builtins::$fc, img.as_ptr());
                let mut cval: f64 = 0.0;
                let cerr = unsafe { $pg(hx, hy, lx, ly, &mut cval) };
                if let Some(d) = adjudicate(r, cerr) {
                    assert!(d.as_f64().to_bits() == cval.to_bits());
                }
            }
        )*};
    }

    box_f64_op1! {
        eq_box_width: fc_box_width / pg_box_width;
        eq_box_height: fc_box_height / pg_box_height;
    }

    // box_area full-symbolic probe: symbolic×symbolic 53-bit multiply —
    // expected WALL per the float-arith cost law; run to record honestly.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn probe_box_area_full() {
        let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let img = bx(hx, hy, lx, ly).to_datum_bytes();
        let r = call1(adt_geo::builtins::fc_box_area, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_box_area(hx, hy, lx, ly, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// box_area on the zero-height plane: low.y == high.y == 0.0 LITERALLY
    /// (an assumed zero does not constant-fold — I128 multiply rule), so the
    /// area multiply has a literal-0.0 multiplicand; width stays fully
    /// symbolic (the float8_mi overflow arm remains reachable and covered).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_box_area_zero_plane() {
        let (hx, lx) = (any_f64(), any_f64());
        let img = bx(hx, 0.0, lx, 0.0).to_datum_bytes();
        let r = call1(adt_geo::builtins::fc_box_area, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_box_area(hx, 0.0, lx, 0.0, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // box_lt full-symbolic probe (stands for the whole area-comparator
    // family shape): expected WALL, run to record.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn probe_box_lt_full() {
        let (ahx, ahy, alx, aly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let r = rust_box_op(
            adt_geo::builtins::fc_box_lt,
            bx(ahx, ahy, alx, aly),
            bx(bhx, bhy, blx, bly),
        );
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_box_lt(ahx, ahy, alx, aly, bhx, bhy, blx, bly, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // ---------- area comparators: zero-plane theorems ----------
    // Both boxes on the zero-height plane (heights LITERAL 0.0, widths
    // fully symbolic): both areas are ±0.0/NaN products — the epsilon
    // compare of the two areas plus the full error lattice of float8_mi/mul
    // stays in-theorem without a symbolic×symbolic multiplier.

    macro_rules! area_zero_plane {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (ahx, alx, bhx, blx) = (any_f64(), any_f64(), any_f64(), any_f64());
                let r = rust_box_op(
                    adt_geo::builtins::$fc,
                    bx(ahx, 0.0, alx, 0.0),
                    bx(bhx, 0.0, blx, 0.0),
                );
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(ahx, 0.0, alx, 0.0, bhx, 0.0, blx, 0.0, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    area_zero_plane! {
        eq_box_lt_zero_plane: fc_box_lt / pg_box_lt;
        eq_box_gt_zero_plane: fc_box_gt / pg_box_gt;
        eq_box_eq_zero_plane: fc_box_eq / pg_box_eq;
        eq_box_le_zero_plane: fc_box_le / pg_box_le;
        eq_box_ge_zero_plane: fc_box_ge / pg_box_ge;
    }

    // ---------- area comparators: concrete boundary grid ----------
    // GRID x GRID all-pairs spot theorems at the adjudication boundaries:
    // areas equal / apart by exactly EPSILON at the FPlt boundary /
    // well-inside the band / zero / negative / ±Inf / NaN, plus cells whose
    // AREA COMPUTATION errors (overflow: finite width overflowing to Inf;
    // underflow: nonzero*nonzero rounding to 0) so the Err arm is covered
    // concretely. Everything is compile-time concrete: symex evaluates,
    // no symbolic multiplier enters the formula.

    const GRID: [[f64; 4]; 10] = [
        [1.0, 1.0, 0.0, 0.0],              // area 1
        [2.0, 1.0, 0.0, 0.0],              // area 2
        [1.0 + 1.0e-6, 1.0, 0.0, 0.0],     // area ~1+EPSILON (band edge)
        [1.0 + 3.0e-6, 1.0, 0.0, 0.0],     // area ~1+3*EPSILON (outside band)
        [1.0, 1.0, 2.0, 0.0],              // width -1 -> area -1 (unnormalized)
        [0.0, 5.0, 0.0, 0.0],              // area 0
        [f64::INFINITY, 1.0, 0.0, 0.0],    // area +Inf (no error: Inf input)
        [1.0e300, 1.0e300, -1.0e300, -1.0e300], // width 2e300 -> OVERFLOW error
        [1.0e-200, 1.0e-200, 0.0, 0.0],    // area 1e-400 -> 0 -> UNDERFLOW error
        [f64::NAN, 1.0, 0.0, 0.0],         // NaN area
    ];

    // Shape note (measured): looping the concrete pairs through the shipped
    // wrapper grows the symex formula superlinearly (outer iteration 8 of 10
    // at 300s) — instead each harness draws ONE symbolic index pair into the
    // concrete table (universally quantified over all 100 cells, one fc
    // call in the formula; the multiplier operands are 10-valued).
    macro_rules! area_grid {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < GRID.len() && j < GRID.len());
                let a = GRID[i];
                let b = GRID[j];
                let r = rust_box_op(
                    adt_geo::builtins::$fc,
                    bx(a[0], a[1], a[2], a[3]),
                    bx(b[0], b[1], b[2], b[3]),
                );
                let mut cres: c_int = 0;
                let cerr = unsafe {
                    $pg(a[0], a[1], a[2], a[3], b[0], b[1], b[2], b[3], &mut cres)
                };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    area_grid! {
        eq_box_lt_grid: fc_box_lt / pg_box_lt;
        eq_box_gt_grid: fc_box_gt / pg_box_gt;
        eq_box_eq_grid: fc_box_eq / pg_box_eq;
        eq_box_le_grid: fc_box_le / pg_box_le;
        eq_box_ge_grid: fc_box_ge / pg_box_ge;
    }

    /// box_area on the boundary grid (same cells, value bit-exactness).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_box_area_grid() {
        let i: usize = kani::any();
        kani::assume(i < GRID.len());
        let a = GRID[i];
        let img = bx(a[0], a[1], a[2], a[3]).to_datum_bytes();
        let r = call1(adt_geo::builtins::fc_box_area, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_box_area(a[0], a[1], a[2], a[3], &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // ---------- box_center (core box_cn: float8_pl + float8_div by 2.0) ----------
    // Proved at the CORE (adt_geo::boxes::box_cn) — the shipped wrapper's
    // only addition is a result-mcx palloc of the 16-byte Point image
    // (byref_result), which is allocation plumbing, not comparator logic.
    // Probe first: /2.0 is a divide by a literal power of two; the cost law
    // walls 53-bit divides in general, so record what happens.

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn probe_box_cn_full() {
        let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let r = adt_geo::boxes::box_cn(&bx(hx, hy, lx, ly));
        let (mut cx, mut cy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_box_center(hx, hy, lx, ly, &mut cx, &mut cy) };
        if let Some(p) = adjudicate(r, cerr) {
            assert!(p.x.to_bits() == cx.to_bits());
            assert!(p.y.to_bits() == cy.to_bits());
        }
    }

    /// box_cn boundary spots (concrete): exact halves, overflow of the
    /// float8_pl (1e308+1e308 -> Inf with finite inputs = error), Inf
    /// passthrough, NaN, subnormal halving to zero (underflow arm of
    /// float8_div: min_subnormal/2 rounds to 0 with val1 != 0).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_box_cn_spots() {
        const SPOTS: [[f64; 4]; 8] = [
            [1.0, 3.0, 0.0, 1.0],
            [-5.5, 2.25, 4.5, -7.75],
            [1.0e308, 0.0, 1.0e308, 0.0],          // pl overflow -> error
            [f64::INFINITY, 1.0, 0.0, 0.0],        // Inf passthrough, no error
            [f64::NAN, 1.0, 2.0, 3.0],             // NaN center
            [5.0e-324, 0.0, 0.0, 0.0],             // min_subnormal/2 -> 0: div underflow error
            [-0.0, -0.0, 0.0, 0.0],                // signed-zero handling
            [1.0e-6, 0.0, -1.0e-6, 0.0],           // epsilon-scale coords
        ];
        let i: usize = kani::any();
        kani::assume(i < SPOTS.len());
        let s = SPOTS[i];
        let r = adt_geo::boxes::box_cn(&bx(s[0], s[1], s[2], s[3]));
        let (mut cx, mut cy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_box_center(s[0], s[1], s[2], s[3], &mut cx, &mut cy) };
        if let Some(p) = adjudicate(r, cerr) {
            assert!(p.x.to_bits() == cx.to_bits());
            assert!(p.y.to_bits() == cy.to_bits());
        }
    }

    // ---------- negative control: the rig must be able to fail ----------
    // Shipped fuzzy FPlt vs exact IEEE `<`: MUST produce a counterexample
    // inside the epsilon band (e.g. x1 < x2 <= x1 + EPSILON) — witnessing
    // both that the harness is non-vacuous and that the EPSILON constant
    // and comparison direction are load-bearing in-theorem.
    // Run with the DEFAULT solver (kissat never terminates on failures).
    #[kani::proof]
    fn control_point_left_vs_ieee() {
        let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let r = rust_point_op(adt_geo::builtins::fc_point_left, pt(x1, y1), pt(x2, y2));
        let c = unsafe { pg_point_left_ieee(x1, y1, x2, y2) };
        assert!(r as c_int == c);
    }
}
