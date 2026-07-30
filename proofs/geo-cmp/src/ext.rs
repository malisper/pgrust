//! Kani C≡Rust equivalence — geo-cmp EXTENSION: circle / lseg / point
//! comparator, predicate, accessor and arithmetic siblings (geo_ops.c).
//! Same rig as the base module (see lib.rs doc): shipped fc_* wrappers via
//! LocalFcinfo with by-ref datum images (Point 16B, LSEG 32B, CIRCLE 24B);
//! C side = c/pg_geo_cmp.c extension section, bodies verbatim @
//! REL_18_STABLE, ereports -> pg_geo_errflag.
//!
//! Domain regimes:
//!   - Pure-compare / add-sub-class operators: FULLY SYMBOLIC f64 (every
//!     NaN payload, ±Inf, ±0, subnormals). circle_same, lseg_vertical/
//!     horizontal/eq/ne (total, no assumes); circle position ops and
//!     point/circle add/sub (fallible: float8_pl/mi overflow arm covered).
//!   - point_dt (pg_hypot) users: fenced to the sqrt-free hypot paths by
//!     an AXIS-ALIGNED SLICE — the y (or x) coordinate is the SAME
//!     symbolic variable on both endpoints, so the dy operand of pg_hypot
//!     is literally 0.0/NaN and the general x*sqrt(1+yx²) path is
//!     unreachable. This is load-bearing twice: (a) 53-bit divide+sqrt
//!     is a solver wall, (b) the general path is a MODEL-SPEC GAP — the C
//!     model does not FMA-contract 1.0+yx*yx while shipped Rust fuses
//!     explicitly (f64::mul_add) to match aarch64 C codegen, so in-model
//!     comparison there would fabricate divergences real silicon doesn't
//!     have. Ledger bounds say "axis-aligned slice; general hypot path out
//!     of proof".
//!   - point_sl users (point_slope, lseg_parallel, lseg_perp): fenced to
//!     the fuzzy-vertical / fuzzy-horizontal early-return arms
//!     (kani::assume(FPeq(x1,x2) || FPeq(y1,y2)) per point pair); the
//!     symbolic÷symbolic divide arm is out of proof (53-bit wall law).
//!   - Area comparators circle_eq/ne/lt/gt/le/ge and circle_area:
//!     radius²·π is a symbolic×symbolic 53-bit multiply = WALL (box-area
//!     precedent). Treatment: symbolic-index RADIUS GRID (equal /
//!     ±epsilon-band edges / zero / negative / ±Inf / NaN / overflow-error
//!     / underflow-error cells) with FULLY SYMBOLIC centers, plus one
//!     full-symbolic probe recorded honestly.
//!
//! Fallible harnesses stub types_error::PgError::error (field-identical
//! minus Location/message text — value-space only) and mem::forget the
//! Err payload; both arms carry kani::cover! witnesses where reachable.
//!
//! Run: cd proofs/geo-cmp && timeout 30 cargo kani -Z c-ffi \
//!        --c-lib c/pg_geo_cmp.c --no-overflow-checks --solver kissat \
//!        --harness <h> --exact

#[cfg(kani)]
mod proofs_ext {
    use datum::Datum;
    use proof_support::{call1, call2, stubs};
    use types_core::geo::{Point, CIRCLE, LSEG};
    use types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;

    use std::os::raw::c_int;

    extern "C" {
        // point arithmetic / measurement (errflag return, value out-param)
        fn pg_point_distance(x1: f64, y1: f64, x2: f64, y2: f64, result: *mut f64) -> c_int;
        fn pg_point_slope(x1: f64, y1: f64, x2: f64, y2: f64, result: *mut f64) -> c_int;
        fn pg_point_add(x1: f64, y1: f64, x2: f64, y2: f64, ox: *mut f64, oy: *mut f64) -> c_int;
        fn pg_point_sub(x1: f64, y1: f64, x2: f64, y2: f64, ox: *mut f64, oy: *mut f64) -> c_int;

        // lseg
        fn pg_lseg_construct(x1: f64, y1: f64, x2: f64, y2: f64, out4: *mut f64) -> c_int;
        fn pg_lseg_vertical(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        fn pg_lseg_horizontal(x1: f64, y1: f64, x2: f64, y2: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_lseg_eq(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64) -> c_int;
        fn pg_lseg_ne(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64) -> c_int;
        fn pg_lseg_lt(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64, result: *mut c_int) -> c_int;
        fn pg_lseg_le(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64, result: *mut c_int) -> c_int;
        fn pg_lseg_gt(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64, result: *mut c_int) -> c_int;
        fn pg_lseg_ge(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64, result: *mut c_int) -> c_int;
        fn pg_lseg_length(x1: f64, y1: f64, x2: f64, y2: f64, result: *mut f64) -> c_int;
        fn pg_lseg_center(x1: f64, y1: f64, x2: f64, y2: f64, cx: *mut f64, cy: *mut f64) -> c_int;
        fn pg_lseg_parallel(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64, result: *mut c_int) -> c_int;
        fn pg_lseg_perp(ax1: f64, ay1: f64, ax2: f64, ay2: f64, bx1: f64, by1: f64, bx2: f64, by2: f64, result: *mut c_int) -> c_int;

        // circle predicates
        fn pg_circle_same(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64) -> c_int;
        fn pg_circle_overlap(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_overleft(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_left(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_right(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_overright(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_contained(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_contain(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_below(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_above(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_overbelow(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_overabove(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_eq(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_ne(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_lt(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_gt(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_le(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;
        fn pg_circle_ge(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, result: *mut c_int) -> c_int;

        // circle arithmetic / accessors
        fn pg_circle_add_pt(cx: f64, cy: f64, r: f64, px: f64, py: f64, ox: *mut f64, oy: *mut f64, orad: *mut f64) -> c_int;
        fn pg_circle_sub_pt(cx: f64, cy: f64, r: f64, px: f64, py: f64, ox: *mut f64, oy: *mut f64, orad: *mut f64) -> c_int;
        fn pg_circle_area(cx: f64, cy: f64, r: f64, result: *mut f64) -> c_int;
        fn pg_circle_diameter(cx: f64, cy: f64, r: f64, result: *mut f64) -> c_int;
        fn pg_circle_radius(cx: f64, cy: f64, r: f64, result: *mut f64) -> c_int;
        fn pg_circle_distance(c1x: f64, c1y: f64, r1: f64, c2x: f64, c2y: f64, r2: f64, out: *mut f64) -> c_int;
        fn pg_circle_contain_pt(cx: f64, cy: f64, r: f64, px: f64, py: f64, result: *mut c_int) -> c_int;
        fn pg_pt_contained_circle(px: f64, py: f64, cx: f64, cy: f64, r: f64, result: *mut c_int) -> c_int;
        fn pg_dist_pc(px: f64, py: f64, cx: f64, cy: f64, r: f64, out: *mut f64) -> c_int;
        fn pg_dist_cpoint(cx: f64, cy: f64, r: f64, px: f64, py: f64, out: *mut f64) -> c_int;
        fn pg_circle_center(cx: f64, cy: f64, r: f64, ox: *mut f64, oy: *mut f64) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    fn pt(x: f64, y: f64) -> Point {
        Point { x, y }
    }

    fn circ(cx: f64, cy: f64, r: f64) -> CIRCLE {
        CIRCLE {
            center: pt(cx, cy),
            radius: r,
        }
    }

    fn seg(x1: f64, y1: f64, x2: f64, y2: f64) -> LSEG {
        LSEG {
            p: [pt(x1, y1), pt(x2, y2)],
        }
    }

    /// Adjudicate a fallible Rust arm against the C err flag (base-module
    /// convention). All errors reachable in this slice are 22003
    /// (overflow/underflow); value-space only (PgError::error stubbed).
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

    /// Infallible-within-domain adjudication: no cover on the Err arm
    /// (structurally unreachable — a failing cover there is not a vacuity
    /// signal but a broken harness, so panic instead).
    fn expect_ok<T>(r: Result<T, Box<types_error::PgError>>, cerr: c_int) -> T {
        assert!(cerr == 0);
        match r {
            Ok(v) => v,
            Err(_) => panic!("infallible geo op errored"),
        }
    }

    // =================================================================
    // circle: pure-compare and add/sub-class position ops, full symbolic
    // =================================================================

    /// circle_same: pure compares (NaN-radii-equal special case in-theorem).
    #[kani::proof]
    fn eq_circle_same() {
        let (c1x, c1y, r1) = (any_f64(), any_f64(), any_f64());
        let (c2x, c2y, r2) = (any_f64(), any_f64(), any_f64());
        let i1 = circ(c1x, c1y, r1).to_datum_bytes();
        let i2 = circ(c2x, c2y, r2).to_datum_bytes();
        let r = match call2(adt_geo::builtins::fc_circle_same, i1.as_ptr(), i2.as_ptr()) {
            Ok(d) => d.as_bool(),
            Err(_) => panic!("infallible geo comparator errored"),
        };
        let c = unsafe { pg_circle_same(c1x, c1y, r1, c2x, c2y, r2) };
        assert!(r as c_int == c);
    }

    // circle position ops: float8_pl/mi (add/sub class) + fuzzy compare —
    // full symbolic domain, verdict + errflag + sqlstate parity.
    macro_rules! circle_pos_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (c1x, c1y, r1) = (any_f64(), any_f64(), any_f64());
                let (c2x, c2y, r2) = (any_f64(), any_f64(), any_f64());
                let i1 = circ(c1x, c1y, r1).to_datum_bytes();
                let i2 = circ(c2x, c2y, r2).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(c1x, c1y, r1, c2x, c2y, r2, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    circle_pos_op! {
        eq_circle_overleft: fc_circle_overleft / pg_circle_overleft;
        eq_circle_left: fc_circle_left / pg_circle_left;
        eq_circle_right: fc_circle_right / pg_circle_right;
        eq_circle_overright: fc_circle_overright / pg_circle_overright;
        eq_circle_below: fc_circle_below / pg_circle_below;
        eq_circle_above: fc_circle_above / pg_circle_above;
        eq_circle_overbelow: fc_circle_overbelow / pg_circle_overbelow;
        eq_circle_overabove: fc_circle_overabove / pg_circle_overabove;
    }

    // =================================================================
    // circle: point_dt users — collinear-centers slice (shared symbolic y)
    // =================================================================
    // The two centers (or center and point) share ONE symbolic y, so
    // float8_mi(y, y) is 0.0 (finite y) / NaN (Inf or NaN y) and pg_hypot
    // takes only its sqrt-free arms. Radii and x's fully symbolic; the
    // float8_mi(x1,x2) overflow arm keeps the Err arm reachable.

    macro_rules! circle_dt_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (c1x, r1, c2x, r2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let y = any_f64(); // shared: axis-aligned slice
                let i1 = circ(c1x, y, r1).to_datum_bytes();
                let i2 = circ(c2x, y, r2).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(c1x, y, r1, c2x, y, r2, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    circle_dt_op! {
        eq_circle_overlap_hslice: fc_circle_overlap / pg_circle_overlap;
        eq_circle_contain_hslice: fc_circle_contain / pg_circle_contain;
        eq_circle_contained_hslice: fc_circle_contained / pg_circle_contained;
    }

    /// circle_distance on the collinear-centers slice (value bit-exact,
    /// negative-clamp in-theorem).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_distance_hslice() {
        let (c1x, r1, c2x, r2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let y = any_f64();
        let i1 = circ(c1x, y, r1).to_datum_bytes();
        let i2 = circ(c2x, y, r2).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_circle_distance, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_circle_distance(c1x, y, r1, c2x, y, r2, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// circle_contain_pt / pt_contained_circle / dist_pc / dist_cpoint:
    /// point shares its y with the circle center (axis-aligned slice).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_contain_pt_hslice() {
        let (cx, r, px) = (any_f64(), any_f64(), any_f64());
        let y = any_f64();
        let ic = circ(cx, y, r).to_datum_bytes();
        let ip = pt(px, y).to_datum_bytes();
        let rr = call2(adt_geo::builtins::fc_circle_contain_pt, ic.as_ptr(), ip.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_circle_contain_pt(cx, y, r, px, y, &mut cres) };
        if let Some(v) = adjudicate(rr, cerr) {
            assert!(v as c_int == cres);
        }
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_pt_contained_circle_hslice() {
        let (cx, r, px) = (any_f64(), any_f64(), any_f64());
        let y = any_f64();
        let ip = pt(px, y).to_datum_bytes();
        let ic = circ(cx, y, r).to_datum_bytes();
        let rr = call2(adt_geo::builtins::fc_pt_contained_circle, ip.as_ptr(), ic.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_pt_contained_circle(px, y, cx, y, r, &mut cres) };
        if let Some(v) = adjudicate(rr, cerr) {
            assert!(v as c_int == cres);
        }
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_dist_pc_hslice() {
        let (cx, r, px) = (any_f64(), any_f64(), any_f64());
        let y = any_f64();
        let ip = pt(px, y).to_datum_bytes();
        let ic = circ(cx, y, r).to_datum_bytes();
        let rr = call2(adt_geo::builtins::fc_dist_pc, ip.as_ptr(), ic.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_dist_pc(px, y, cx, y, r, &mut cval) };
        if let Some(d) = adjudicate(rr, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_dist_cpoint_hslice() {
        let (cx, r, px) = (any_f64(), any_f64(), any_f64());
        let y = any_f64();
        let ic = circ(cx, y, r).to_datum_bytes();
        let ip = pt(px, y).to_datum_bytes();
        let rr = call2(adt_geo::builtins::fc_dist_cpoint, ic.as_ptr(), ip.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_dist_cpoint(cx, y, r, px, y, &mut cval) };
        if let Some(d) = adjudicate(rr, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // =================================================================
    // circle: area comparators — symbolic-index radius grid + full probe
    // =================================================================
    // Areas depend only on the radii; centers stay FULLY SYMBOLIC. Radii
    // are drawn by symbolic index from a concrete boundary table (base
    // module GRID precedent: one symbolic index, never a loop through the
    // wrapper).

    const RGRID: [f64; 10] = [
        1.0,             // area pi
        2.0,             // area 4pi
        1.0 + 1.0e-7,    // area delta ~6.3e-7 < EPSILON (inside band)
        1.0 + 1.0e-6,    // area delta ~6.3e-6 > EPSILON (outside band)
        0.0,             // area 0
        -1.0,            // negative radius, area still +pi
        f64::INFINITY,   // area +Inf (no error: Inf input)
        1.0e200,         // r*r -> Inf with finite input = OVERFLOW error
        1.0e-200,        // r*r -> 0, nonzero input = UNDERFLOW error
        f64::NAN,        // NaN area
    ];

    macro_rules! circle_area_cmp_grid {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < RGRID.len() && j < RGRID.len());
                let (c1x, c1y, c2x, c2y) = (any_f64(), any_f64(), any_f64(), any_f64());
                let (r1, r2) = (RGRID[i], RGRID[j]);
                let i1 = circ(c1x, c1y, r1).to_datum_bytes();
                let i2 = circ(c2x, c2y, r2).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(c1x, c1y, r1, c2x, c2y, r2, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    circle_area_cmp_grid! {
        eq_circle_eq_grid: fc_circle_eq / pg_circle_eq;
        eq_circle_ne_grid: fc_circle_ne / pg_circle_ne;
        eq_circle_lt_grid: fc_circle_lt / pg_circle_lt;
        eq_circle_gt_grid: fc_circle_gt / pg_circle_gt;
        eq_circle_le_grid: fc_circle_le / pg_circle_le;
        eq_circle_ge_grid: fc_circle_ge / pg_circle_ge;
    }

    /// circle_area on the radius grid (value bit-exact).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_area_grid() {
        let i: usize = kani::any();
        kani::assume(i < RGRID.len());
        let (cx, cy) = (any_f64(), any_f64());
        let img = circ(cx, cy, RGRID[i]).to_datum_bytes();
        let r = call1(adt_geo::builtins::fc_circle_area, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_circle_area(cx, cy, RGRID[i], &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // circle_area full-symbolic probe: symbolic x symbolic 53-bit multiply
    // (r*r) — expected WALL per the float-arith cost law; run to record.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn probe_circle_area_full() {
        let (cx, cy, r) = (any_f64(), any_f64(), any_f64());
        let img = circ(cx, cy, r).to_datum_bytes();
        let rr = call1(adt_geo::builtins::fc_circle_area, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_circle_area(cx, cy, r, &mut cval) };
        if let Some(d) = adjudicate(rr, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // =================================================================
    // circle: accessors and translation arithmetic
    // =================================================================

    /// circle_radius: pure field read through the full wrapper stack.
    #[kani::proof]
    fn eq_circle_radius() {
        let (cx, cy, r) = (any_f64(), any_f64(), any_f64());
        let img = circ(cx, cy, r).to_datum_bytes();
        let rr = call1(adt_geo::builtins::fc_circle_radius, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_circle_radius(cx, cy, r, &mut cval) };
        let d = expect_ok(rr, cerr);
        assert!(d.as_f64().to_bits() == cval.to_bits());
    }

    /// circle_diameter: float8_mul(r, 2.0) — literal power-of-two
    /// multiplicand (box_center /2.0 refinement suggests full domain is
    /// tractable; probe records the answer).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_diameter_full() {
        let (cx, cy, r) = (any_f64(), any_f64(), any_f64());
        let img = circ(cx, cy, r).to_datum_bytes();
        let rr = call1(adt_geo::builtins::fc_circle_diameter, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_circle_diameter(cx, cy, r, &mut cval) };
        if let Some(d) = adjudicate(rr, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// circle_center at the CORE (shipped wrapper's result ride is a
    /// result-mcx palloc — Mcx trap, not comparator logic; box_cn
    /// precedent). Pure copy, full domain.
    #[kani::proof]
    fn eq_circle_center_core() {
        let (cx, cy, r) = (any_f64(), any_f64(), any_f64());
        let p = adt_geo::circle::circle_center(&circ(cx, cy, r));
        let (mut ox, mut oy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_circle_center(cx, cy, r, &mut ox, &mut oy) };
        assert!(cerr == 0);
        assert!(p.x.to_bits() == ox.to_bits());
        assert!(p.y.to_bits() == oy.to_bits());
    }

    // circle_add_pt / circle_sub_pt at the CORE (wrapper ride = result-mcx
    // palloc of the 24-byte CIRCLE): float8_pl/mi per center coordinate +
    // radius copy — full domain.
    macro_rules! circle_addsub_core {
        ($($h:ident: $core:path, $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (cx, cy, r, px, py) =
                    (any_f64(), any_f64(), any_f64(), any_f64(), any_f64());
                let rr = $core(&circ(cx, cy, r), &pt(px, py));
                let (mut ox, mut oy, mut orad) = (0.0f64, 0.0f64, 0.0f64);
                let cerr = unsafe { $pg(cx, cy, r, px, py, &mut ox, &mut oy, &mut orad) };
                if let Some(c) = adjudicate(rr, cerr) {
                    assert!(c.center.x.to_bits() == ox.to_bits());
                    assert!(c.center.y.to_bits() == oy.to_bits());
                    assert!(c.radius.to_bits() == orad.to_bits());
                }
            }
        )*};
    }

    circle_addsub_core! {
        eq_circle_add_pt_core: adt_geo::circle::circle_add_pt, pg_circle_add_pt;
        eq_circle_sub_pt_core: adt_geo::circle::circle_sub_pt, pg_circle_sub_pt;
    }

    // =================================================================
    // lseg: pure predicates, full symbolic
    // =================================================================

    macro_rules! lseg_pred1 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let img = seg(x1, y1, x2, y2).to_datum_bytes();
                let r = match call1(adt_geo::builtins::$fc, img.as_ptr()) {
                    Ok(d) => d.as_bool(),
                    Err(_) => panic!("infallible geo predicate errored"),
                };
                let c = unsafe { $pg(x1, y1, x2, y2) };
                assert!(r as c_int == c);
            }
        )*};
    }

    lseg_pred1! {
        eq_lseg_vertical: fc_lseg_vertical / pg_lseg_vertical;
        eq_lseg_horizontal: fc_lseg_horizontal / pg_lseg_horizontal;
    }

    macro_rules! lseg_pred2 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (ax1, ay1, ax2, ay2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let (bx1, by1, bx2, by2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let i1 = seg(ax1, ay1, ax2, ay2).to_datum_bytes();
                let i2 = seg(bx1, by1, bx2, by2).to_datum_bytes();
                let r = match call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr()) {
                    Ok(d) => d.as_bool(),
                    Err(_) => panic!("infallible geo predicate errored"),
                };
                let c = unsafe { $pg(ax1, ay1, ax2, ay2, bx1, by1, bx2, by2) };
                assert!(r as c_int == c);
            }
        )*};
    }

    lseg_pred2! {
        eq_lseg_eq: fc_lseg_eq / pg_lseg_eq;
        eq_lseg_ne: fc_lseg_ne / pg_lseg_ne;
    }

    // =================================================================
    // lseg: length comparators — horizontal-segments slice
    // =================================================================
    // Each segment's endpoints share ONE symbolic y (per segment), so both
    // point_dt calls take the sqrt-free hypot arms; x's fully symbolic.

    macro_rules! lseg_len_cmp {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (ax1, ax2, bx1, bx2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let ya = any_f64(); // shared within segment a
                let yb = any_f64(); // shared within segment b
                let i1 = seg(ax1, ya, ax2, ya).to_datum_bytes();
                let i2 = seg(bx1, yb, bx2, yb).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(ax1, ya, ax2, ya, bx1, yb, bx2, yb, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    lseg_len_cmp! {
        eq_lseg_lt_hslice: fc_lseg_lt / pg_lseg_lt;
        eq_lseg_le_hslice: fc_lseg_le / pg_lseg_le;
        eq_lseg_gt_hslice: fc_lseg_gt / pg_lseg_gt;
        eq_lseg_ge_hslice: fc_lseg_ge / pg_lseg_ge;
    }

    /// lseg_length: horizontal slice (value bit-exact).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_lseg_length_hslice() {
        let (x1, x2) = (any_f64(), any_f64());
        let y = any_f64();
        let img = seg(x1, y, x2, y).to_datum_bytes();
        let r = call1(adt_geo::builtins::fc_lseg_length, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_lseg_length(x1, y, x2, y, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// lseg_length: vertical slice (shared x; hypot's swap arm exercised).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_lseg_length_vslice() {
        let (y1, y2) = (any_f64(), any_f64());
        let x = any_f64();
        let img = seg(x, y1, x, y2).to_datum_bytes();
        let r = call1(adt_geo::builtins::fc_lseg_length, img.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_lseg_length(x, y1, x, y2, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // =================================================================
    // lseg: slope-based predicates — early-return-arms fence
    // =================================================================
    // point_sl/point_invsl return through their fuzzy-vertical /
    // fuzzy-horizontal early arms when FPeq fires on either coordinate
    // pair; the fence keeps every point pair in those arms. The symbolic
    // divide arm (53-bit wall + reachable only outside the fence) is out
    // of proof.

    fn fpeq(a: f64, b: f64) -> bool {
        a == b || (a - b).abs() <= 1.0e-6
    }

    macro_rules! lseg_slope_cmp {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (ax1, ay1, ax2, ay2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let (bx1, by1, bx2, by2) = (any_f64(), any_f64(), any_f64(), any_f64());
                kani::assume(fpeq(ax1, ax2) || fpeq(ay1, ay2));
                kani::assume(fpeq(bx1, bx2) || fpeq(by1, by2));
                let i1 = seg(ax1, ay1, ax2, ay2).to_datum_bytes();
                let i2 = seg(bx1, by1, bx2, by2).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(ax1, ay1, ax2, ay2, bx1, by1, bx2, by2, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    lseg_slope_cmp! {
        eq_lseg_parallel_arms: fc_lseg_parallel / pg_lseg_parallel;
        eq_lseg_perp_arms: fc_lseg_perp / pg_lseg_perp;
    }

    /// lseg_center at the CORE (wrapper ride = result-mcx palloc of the
    /// Point; box_cn precedent): float8_pl + /2.0 power-of-two divide,
    /// full domain (box_center refinement: provable, calibration tier).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_lseg_center_core() {
        let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let r = adt_geo::lseg::lseg_center(&seg(x1, y1, x2, y2));
        let (mut cx, mut cy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_lseg_center(x1, y1, x2, y2, &mut cx, &mut cy) };
        if let Some(p) = adjudicate(r, cerr) {
            assert!(p.x.to_bits() == cx.to_bits());
            assert!(p.y.to_bits() == cy.to_bits());
        }
    }

    /// lseg_construct at the CORE (wrapper ride = result-mcx palloc of the
    /// LSEG): pure copy, full domain.
    #[kani::proof]
    fn eq_lseg_construct_core() {
        let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let l = adt_geo::lseg::lseg_construct(&pt(x1, y1), &pt(x2, y2));
        let mut out = [0.0f64; 4];
        let cerr = unsafe { pg_lseg_construct(x1, y1, x2, y2, out.as_mut_ptr()) };
        assert!(cerr == 0);
        assert!(l.p[0].x.to_bits() == out[0].to_bits());
        assert!(l.p[0].y.to_bits() == out[1].to_bits());
        assert!(l.p[1].x.to_bits() == out[2].to_bits());
        assert!(l.p[1].y.to_bits() == out[3].to_bits());
    }

    // =================================================================
    // point: distance / slope / add / sub
    // =================================================================

    /// point_distance: horizontal slice (shared y).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_point_distance_hslice() {
        let (x1, x2) = (any_f64(), any_f64());
        let y = any_f64();
        let i1 = pt(x1, y).to_datum_bytes();
        let i2 = pt(x2, y).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_point_distance, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_point_distance(x1, y, x2, y, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// point_distance: vertical slice (shared x).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_point_distance_vslice() {
        let (y1, y2) = (any_f64(), any_f64());
        let x = any_f64();
        let i1 = pt(x, y1).to_datum_bytes();
        let i2 = pt(x, y2).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_point_distance, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_point_distance(x, y1, x, y2, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// point_slope: early-return arms (fuzzy-vertical -> +Inf,
    /// fuzzy-horizontal -> 0.0); divide arm out of proof.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_point_slope_arms() {
        let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
        kani::assume(fpeq(x1, x2) || fpeq(y1, y2));
        let i1 = pt(x1, y1).to_datum_bytes();
        let i2 = pt(x2, y2).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_point_slope, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_point_slope(x1, y1, x2, y2, &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // point_add / point_sub at the CORE (wrapper ride = result-mcx palloc
    // of the Point): float8_pl/mi per coordinate — full domain.
    macro_rules! point_addsub_core {
        ($($h:ident: $core:path, $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
                let r = $core(&pt(x1, y1), &pt(x2, y2));
                let (mut ox, mut oy) = (0.0f64, 0.0f64);
                let cerr = unsafe { $pg(x1, y1, x2, y2, &mut ox, &mut oy) };
                if let Some(p) = adjudicate(r, cerr) {
                    assert!(p.x.to_bits() == ox.to_bits());
                    assert!(p.y.to_bits() == oy.to_bits());
                }
            }
        )*};
    }

    point_addsub_core! {
        eq_point_add_core: adt_geo::point::point_add_point, pg_point_add;
        eq_point_sub_core: adt_geo::point::point_sub_point, pg_point_sub;
    }

    // =================================================================
    // grid spot theorems for slice-walled shapes
    // =================================================================
    // The two-point_dt slices (lseg length comparators, circle_distance,
    // circle_contained) and the dual slope fences (lseg_parallel/perp)
    // wall at the ladder max even axis-aligned — chained symbolic adder
    // circuits. Concrete-cell tables with ONE SYMBOLIC INDEX per operand
    // (base-module GRID rule) keep the adjudication boundaries and the
    // error lattice in-theorem, including the point_sl DIVIDE arm at
    // concrete operands (unreachable in the fenced harnesses above).

    // Horizontal segments (y = 0), as (x1, x2): length = |x1 - x2|.
    const SGRID: [[f64; 2]; 8] = [
        [0.0, 1.0],                 // length 1
        [0.0, 2.0],                 // length 2
        [0.0, 1.0 + 1.0e-7],        // length in the FPlt epsilon band vs 1
        [0.0, 1.0 + 3.0e-6],        // just outside the band vs 1
        [5.0, 5.0],                 // length 0
        [-1.0e308, 1.0e308],        // dx overflow -> error inside point_dt
        [0.0, f64::INFINITY],       // infinite length (no error: Inf input)
        [f64::NAN, 1.0],            // NaN length
    ];

    macro_rules! lseg_len_cmp_grid {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < SGRID.len() && j < SGRID.len());
                let a = SGRID[i];
                let b = SGRID[j];
                let i1 = seg(a[0], 0.0, a[1], 0.0).to_datum_bytes();
                let i2 = seg(b[0], 0.0, b[1], 0.0).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(a[0], 0.0, a[1], 0.0, b[0], 0.0, b[1], 0.0, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    lseg_len_cmp_grid! {
        eq_lseg_lt_grid: fc_lseg_lt / pg_lseg_lt;
        eq_lseg_le_grid: fc_lseg_le / pg_lseg_le;
        eq_lseg_gt_grid: fc_lseg_gt / pg_lseg_gt;
        eq_lseg_ge_grid: fc_lseg_ge / pg_lseg_ge;
    }

    // Collinear circles on y = 0, as (cx, r).
    const CGRID: [[f64; 2]; 8] = [
        [0.0, 1.0],
        [3.0, 1.0],                 // dist(0,3)=3: gap 1 vs r sums
        [2.0, 1.0],                 // touching
        [1.0, 0.5],                 // overlapping -> negative gap (clamp)
        [0.0, 1.0e308],             // radius-sum overflow -> error
        [-1.0e308, 1.0],            // center-distance overflow -> error
        [1.0, f64::INFINITY],       // Inf radius -> -Inf gap -> clamp 0
        [1.0, f64::NAN],            // NaN radius
    ];

    /// circle_distance on the collinear grid (value bit-exact incl clamp).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_distance_grid() {
        let i: usize = kani::any();
        let j: usize = kani::any();
        kani::assume(i < CGRID.len() && j < CGRID.len());
        let a = CGRID[i];
        let b = CGRID[j];
        let i1 = circ(a[0], 0.0, a[1]).to_datum_bytes();
        let i2 = circ(b[0], 0.0, b[1]).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_circle_distance, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_circle_distance(a[0], 0.0, a[1], b[0], 0.0, b[1], &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// circle_contained on the collinear grid (mirror circle_contain
    /// proved full-slice; contained itself walls at the ladder max).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_contained_grid() {
        let i: usize = kani::any();
        let j: usize = kani::any();
        kani::assume(i < CGRID.len() && j < CGRID.len());
        let a = CGRID[i];
        let b = CGRID[j];
        let i1 = circ(a[0], 0.0, a[1]).to_datum_bytes();
        let i2 = circ(b[0], 0.0, b[1]).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_circle_contained, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_circle_contained(a[0], 0.0, a[1], b[0], 0.0, b[1], &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // Segments for slope predicates, as (x1, y1, x2, y2): vertical,
    // horizontal, unit/two/negative diagonals (concrete DIVIDE arm),
    // a slope inside the FPeq epsilon band of 1, Inf and NaN members.
    const SLGRID: [[f64; 4]; 8] = [
        [0.0, 0.0, 0.0, 1.0],               // vertical -> slope Inf
        [0.0, 0.0, 1.0, 0.0],               // horizontal -> slope 0
        [0.0, 0.0, 2.0, 2.0],               // slope 1 (divide arm)
        [0.0, 0.0, 2.0, -2.0],              // slope -1 (divide arm)
        [0.0, 0.0, 1.0, 2.0],               // slope 2 (divide arm)
        [0.0, 0.0, 2.0, 2.0 + 1.6e-6],      // slope 1+8e-7: FPeq-band vs 1
        [0.0, 0.0, f64::INFINITY, 1.0],     // Inf coords
        [f64::NAN, 0.0, 1.0, 1.0],          // NaN coords
    ];

    macro_rules! lseg_slope_grid {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < SLGRID.len() && j < SLGRID.len());
                let a = SLGRID[i];
                let b = SLGRID[j];
                let i1 = seg(a[0], a[1], a[2], a[3]).to_datum_bytes();
                let i2 = seg(b[0], b[1], b[2], b[3]).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
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

    lseg_slope_grid! {
        eq_lseg_parallel_grid: fc_lseg_parallel / pg_lseg_parallel;
        eq_lseg_perp_grid: fc_lseg_perp / pg_lseg_perp;
    }

    /// point_slope on the slope grid: covers the DIVIDE arm (concrete
    /// operands) that the arms-fenced harness excludes; value bit-exact.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_point_slope_grid() {
        let i: usize = kani::any();
        kani::assume(i < SLGRID.len());
        let a = SLGRID[i];
        let i1 = pt(a[0], a[1]).to_datum_bytes();
        let i2 = pt(a[2], a[3]).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_point_slope, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_point_slope(a[0], a[1], a[2], a[3], &mut cval) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // =================================================================
    // negative control: the extension rig must be able to fail
    // =================================================================
    // Shipped circle_same (fuzzy FPeq on the radius) vs a WRONG C twin?
    // Not needed — the base module's control_point_left_vs_ieee already
    // witnesses the family rig. Instead this control makes the errflag
    // adjudication falsifiable: it wires circle_overleft's Rust arm
    // against pg_circle_overRIGHT's C arm and MUST fail.
    // Run with the DEFAULT solver (kissat never terminates on failures).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn control_circle_overleft_vs_overright() {
        let (c1x, c1y, r1) = (any_f64(), any_f64(), any_f64());
        let (c2x, c2y, r2) = (any_f64(), any_f64(), any_f64());
        let i1 = circ(c1x, c1y, r1).to_datum_bytes();
        let i2 = circ(c2x, c2y, r2).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_circle_overleft, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_circle_overright(c1x, c1y, r1, c2x, c2y, r2, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }
}
