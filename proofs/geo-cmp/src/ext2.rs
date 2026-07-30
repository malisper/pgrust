//! Kani C≡Rust equivalence — geo-cmp EXTENSION 2: box constructors and
//! arithmetic, line predicates, on_XX / inter_XX containment predicates,
//! and the conversion constructors (geo_ops.c @ REL_18_STABLE).
//!
//! Same rig as the base module (lib.rs doc): shipped fc_* wrappers via
//! LocalFcinfo with by-ref datum images (Point 16B, LSEG/BOX 32B, LINE
//! 24B, CIRCLE 24B); C side = c/pg_geo_cmp.c EXTENSION 2 section, bodies
//! verbatim, ereports -> pg_geo_errflag (4 = 22023 invalid-parameter,
//! new in this wave; 1/2/3 = 22003/22003/22012 as before).
//!
//! NEW in this wave — BY-REF RESULT wrappers (ret_box/ret_lseg/ret_line/
//! ret_circle/ret_point ride a result-mcx palloc): proved at the FULL
//! SHIPPED WRAPPER via the mcx-stubs recipe (int-arith send precedent):
//! Mcx::allocate -> static bump buffer, env::var -> "0", OnceLock ->
//! recompute, std::fmt::format stubbed, ctx forgotten at harness end;
//! the result datum's image is read back through the raw pointer (wave-6
//! rule: image read-back does not trip pointer provenance). Theorem
//! qualifier: "modulo static-buffer allocator model".
//!
//! Domain regimes (per the standing geo treatments):
//!   - Exact/fuzzy compare + add/sub-class shapes: FULLY SYMBOLIC f64.
//!     on_sb, point_box, points_box, boxes_bound_box, box_intersect
//!     (incl NULL-verdict parity), box_diagonal, cr_circle,
//!     construct_point, box_add/box_sub, line_vertical/line_horizontal.
//!   - 53-bit symbolic multiply/divide shapes (point_mul/point_div,
//!     box_mul/box_div, line_perp/line_eq general arms, on_pl/on_sl
//!     A*x+B*y form): LITERAL-constant planes (literal ±power-of-two /
//!     zero multiplicands are free; assumed constants do NOT fold) with
//!     everything else fully symbolic, plus ONE-SYMBOLIC-INDEX concrete
//!     grids for the general arms and the error lattice.
//!   - point_dt users (box_distance, box_circle): AXIS-ALIGNED SLICE
//!     (shared symbolic y) — pg_hypot general path is BOTH a 53-bit wall
//!     and the known mul_add model-spec gap, so it stays out of every
//!     theorem (base-module fence law). Grid cells are also constructed
//!     axis-aligned everywhere point_dt is reachable (on_ps, inter_sb).
//!   - inter_sb: full-symbolic probe recorded honestly + concrete grid
//!     (disjoint / endpoint-inside / edge-crossing / border-touch /
//!     epsilon-band / NaN / Inf cells).
//!
//! Fallible harnesses stub types_error::PgError::error (value-space
//! only) and mem::forget the Err payload; both arms carry kani::cover!
//! witnesses where genuinely reachable.
//!
//! Run: ./run-one.sh <harness> <timeout-s> [--solver kissat]
//! (negative control control_box_add_vs_c_box_sub runs DEFAULT solver.)

#[cfg(kani)]
mod proofs_ext2 {
    use datum::{Datum, NullableDatum};
    use proof_support::{call1, call2, mcx_stubs, stubs, FcFn};
    use types_core::geo::{Point, BOX, LINE, LSEG};
    use types_error::{
        PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_PARAMETER_VALUE,
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    };
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        // containment / intersection predicates
        fn pg_on_pl(px: f64, py: f64, l1a: f64, l1b: f64, l1c: f64, result: *mut c_int) -> c_int;
        fn pg_on_ps(px: f64, py: f64, sx1: f64, sy1: f64, sx2: f64, sy2: f64, result: *mut c_int) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_on_sl(sx1: f64, sy1: f64, sx2: f64, sy2: f64, l1a: f64, l1b: f64, l1c: f64, result: *mut c_int) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_on_sb(sx1: f64, sy1: f64, sx2: f64, sy2: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_inter_sb(sx1: f64, sy1: f64, sx2: f64, sy2: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut c_int) -> c_int;

        // constructors / arithmetic (image out-params, errflag return)
        fn pg_points_box(x1: f64, y1: f64, x2: f64, y2: f64, out4: *mut f64) -> c_int;
        fn pg_box_add(ahx: f64, ahy: f64, alx: f64, aly: f64, px: f64, py: f64, out4: *mut f64) -> c_int;
        fn pg_box_sub(ahx: f64, ahy: f64, alx: f64, aly: f64, px: f64, py: f64, out4: *mut f64) -> c_int;
        fn pg_box_mul(ahx: f64, ahy: f64, alx: f64, aly: f64, px: f64, py: f64, out4: *mut f64) -> c_int;
        fn pg_box_div(ahx: f64, ahy: f64, alx: f64, aly: f64, px: f64, py: f64, out4: *mut f64) -> c_int;
        fn pg_point_mul(x1: f64, y1: f64, x2: f64, y2: f64, ox: *mut f64, oy: *mut f64) -> c_int;
        fn pg_point_div(x1: f64, y1: f64, x2: f64, y2: f64, ox: *mut f64, oy: *mut f64) -> c_int;
        fn pg_construct_point(x: f64, y: f64, ox: *mut f64, oy: *mut f64) -> c_int;
        fn pg_point_box(px: f64, py: f64, out4: *mut f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_boxes_bound_box(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, out4: *mut f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_box_intersect(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, out4: *mut f64, isnull: *mut c_int) -> c_int;
        fn pg_box_diagonal(ahx: f64, ahy: f64, alx: f64, aly: f64, out4: *mut f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_box_distance(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64, result: *mut f64) -> c_int;
        fn pg_box_circle(ahx: f64, ahy: f64, alx: f64, aly: f64, ocx: *mut f64, ocy: *mut f64, orad: *mut f64) -> c_int;
        fn pg_cr_circle(px: f64, py: f64, radius: f64, ocx: *mut f64, ocy: *mut f64, orad: *mut f64) -> c_int;

        // line predicates
        fn pg_line_vertical(l1a: f64, l1b: f64, l1c: f64) -> c_int;
        fn pg_line_horizontal(l1a: f64, l1b: f64, l1c: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_line_perp(l1a: f64, l1b: f64, l1c: f64, l2a: f64, l2b: f64, l2c: f64, result: *mut c_int) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_line_eq(l1a: f64, l1b: f64, l1c: f64, l2a: f64, l2b: f64, l2c: f64, result: *mut c_int) -> c_int;
        fn pg_line_construct_pp(x1: f64, y1: f64, x2: f64, y2: f64, out3: *mut f64) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
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

    fn seg(x1: f64, y1: f64, x2: f64, y2: f64) -> LSEG {
        LSEG {
            p: [pt(x1, y1), pt(x2, y2)],
        }
    }

    #[allow(non_snake_case)]
    fn line(A: f64, B: f64, C: f64) -> LINE {
        LINE { A, B, C }
    }

    /// C errflag -> expected sqlstate (extension-2 flag convention).
    fn expected_sqlstate(cerr: c_int) -> types_error::SqlState {
        match cerr {
            3 => ERRCODE_DIVISION_BY_ZERO,
            4 => ERRCODE_INVALID_PARAMETER_VALUE,
            _ => ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        }
    }

    /// Adjudicate a fallible Rust arm against the C err flag (base-module
    /// convention, extended sqlstate map). Value-space only (PgError::error
    /// stubbed); Err payload forgotten (drop-glue wall).
    fn adjudicate<T>(r: Result<T, Box<PgError>>, cerr: c_int) -> Option<T> {
        match r {
            Ok(v) => {
                kani::cover!(true); // vacuity witness: Ok arm explored
                assert!(cerr == 0);
                Some(v)
            }
            Err(e) => {
                kani::cover!(true); // vacuity witness: Err arm explored
                assert!(cerr != 0);
                assert!(e.sqlstate == expected_sqlstate(cerr));
                core::mem::forget(e);
                None
            }
        }
    }

    /// VERDICT-ONLY adjudication for chains whose Err crosses a
    /// Result<f64,_> internally (float8_div in point_div_point): reading
    /// Err payload FIELDS there trips the known Kani 0.67 Box-provenance
    /// defect (cash-lane law; eq_point_div_pow2plane FAILED on a
    /// corrupted e.sqlstate read while the 3200-point native differential
    /// shows zero divergences — tests/pdiv_native.rs). Sqlstate parity for
    /// this family is carried by the native differential census instead.
    fn adjudicate_verdict<T>(r: Result<T, Box<PgError>>, cerr: c_int) -> Option<T> {
        match r {
            Ok(v) => {
                kani::cover!(true); // vacuity witness: Ok arm explored
                assert!(cerr == 0);
                Some(v)
            }
            Err(e) => {
                kani::cover!(true); // vacuity witness: Err arm explored
                assert!(cerr != 0);
                core::mem::forget(e);
                None
            }
        }
    }

    /// Infallible-within-domain adjudication (no Err cover — a reachable
    /// Err here is a broken harness, not a vacuity signal).
    fn expect_ok<T>(r: Result<T, Box<PgError>>, cerr: c_int) -> T {
        assert!(cerr == 0);
        match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("infallible geo op errored")
            }
        }
    }

    // ---------- by-ref-result call plumbing (mcx-stubs recipe) ----------

    /// Shipped-wrapper call with a result-mcx armed (needed by the
    /// ret_point/ret_box/ret_lseg/ret_line/ret_circle wrappers). Returns
    /// (result, fcinfo.isnull). The bump context is forgotten (teardown
    /// is not part of the claim and walls symex).
    fn call_mcx<const N: usize>(
        fc: FcFn<Box<PgError>>,
        args: [Datum; N],
    ) -> (Result<Datum, Box<PgError>>, bool) {
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext2");
        let mut f = LocalFcinfo::<N>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        let mut i = 0;
        while i < N {
            f.args[i] = NullableDatum::value(args[i]);
            i += 1;
        }
        let r = fc(None, &mut f);
        let isnull = f.isnull;
        core::mem::forget(ctx);
        (r, isnull)
    }

    /// Read the k-th f64 of a by-ref result image (wave-6 rule: datum
    /// image read-back does not trip pointer provenance).
    fn img_f64(d: Datum, k: usize) -> f64 {
        unsafe { *(d.as_usize() as *const f64).add(k) }
    }

    fn assert_img4(d: Datum, c: &[f64; 4]) {
        assert!(img_f64(d, 0).to_bits() == c[0].to_bits());
        assert!(img_f64(d, 1).to_bits() == c[1].to_bits());
        assert!(img_f64(d, 2).to_bits() == c[2].to_bits());
        assert!(img_f64(d, 3).to_bits() == c[3].to_bits());
    }

    fn assert_img2(d: Datum, cx: f64, cy: f64) {
        assert!(img_f64(d, 0).to_bits() == cx.to_bits());
        assert!(img_f64(d, 1).to_bits() == cy.to_bits());
    }

    // =================================================================
    // on_sb: exact >=/<= compares — fully symbolic, total
    // =================================================================

    #[kani::proof]
    fn eq_on_sb() {
        let (sx1, sy1, sx2, sy2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let is = seg(sx1, sy1, sx2, sy2).to_datum_bytes();
        let ib = bx(bhx, bhy, blx, bly).to_datum_bytes();
        let r = match call2(adt_geo::builtins::fc_on_sb, is.as_ptr(), ib.as_ptr()) {
            Ok(d) => d.as_bool(),
            Err(_) => panic!("infallible geo predicate errored"),
        };
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_on_sb(sx1, sy1, sx2, sy2, bhx, bhy, blx, bly, &mut cres) };
        assert!(cerr == 0);
        assert!(r as c_int == cres);
    }

    // =================================================================
    // on_pl / on_sl: A*x + B*y + C — literal-(A,B) planes (symbolic
    // multiplicands with literal 0/±1 coefficients are free; the general
    // symbolic-A,B arm is a 53-bit wall and stays out of proof)
    // =================================================================

    macro_rules! on_pl_plane {
        ($($h:ident: $a:expr, $b:expr;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (px, py, c) = (any_f64(), any_f64(), any_f64());
                let ip = pt(px, py).to_datum_bytes();
                let il = line($a, $b, c).to_datum_bytes();
                let r = call2(adt_geo::builtins::fc_on_pl, ip.as_ptr(), il.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { pg_on_pl(px, py, $a, $b, c, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    on_pl_plane! {
        eq_on_pl_hplane: 0.0, -1.0;   // horizontal line y = C
        eq_on_pl_vplane: -1.0, 0.0;   // vertical line x = C
        eq_on_pl_dplane: 1.0, -1.0;   // unit-diagonal line y = x + C
    }

    macro_rules! on_sl_plane {
        ($($h:ident: $a:expr, $b:expr;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (sx1, sy1, sx2, sy2, c) =
                    (any_f64(), any_f64(), any_f64(), any_f64(), any_f64());
                let is = seg(sx1, sy1, sx2, sy2).to_datum_bytes();
                let il = line($a, $b, c).to_datum_bytes();
                let r = call2(adt_geo::builtins::fc_on_sl, is.as_ptr(), il.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { pg_on_sl(sx1, sy1, sx2, sy2, $a, $b, c, &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    on_sl_plane! {
        eq_on_sl_hplane: 0.0, -1.0;
        eq_on_sl_vplane: -1.0, 0.0;
        eq_on_sl_dplane: 1.0, -1.0;
    }

    // =================================================================
    // on_ps: 3x point_dt — one-symbolic-index grid, axis-aligned cells
    // (general hypot path = mul_add model-spec gap, fenced by cell
    // construction: every reachable point_dt has dx == 0 or dy == 0)
    // =================================================================

    // [px, py, x1, y1, x2, y2]
    const OPGRID: [[f64; 6]; 9] = [
        [0.5, 0.0, 0.0, 0.0, 1.0, 0.0],            // midpoint on segment
        [0.0, 0.0, 0.0, 0.0, 1.0, 0.0],            // endpoint
        [2.0, 0.0, 0.0, 0.0, 1.0, 0.0],            // collinear outside
        [0.5, 0.0, 0.5, 1.0, 0.5, 2.0],            // vertical seg, point off it
        [0.5, 0.0, 0.5, -1.0, 0.5, 1.0],           // vertical seg, point on it
        [0.5 + 1.0e-7, 0.0, 0.0, 0.0, 1.0, 0.0],   // FPeq epsilon band
        [f64::NAN, 0.0, 0.0, 0.0, 1.0, 0.0],       // NaN distances
        [f64::INFINITY, 0.0, 0.0, 0.0, 1.0, 0.0],  // Inf distances
        [-1.0e308, 0.0, 1.0e308, 0.0, 0.0, 0.0],   // dx overflow -> error
    ];

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_on_ps_grid() {
        let i: usize = kani::any();
        kani::assume(i < OPGRID.len());
        let a = OPGRID[i];
        let ip = pt(a[0], a[1]).to_datum_bytes();
        let is = seg(a[2], a[3], a[4], a[5]).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_on_ps, ip.as_ptr(), is.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_on_ps(a[0], a[1], a[2], a[3], a[4], a[5], &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // =================================================================
    // inter_sb: full-symbolic probe (expected wall, recorded honestly)
    // + one-symbolic-index concrete grid (axis-aligned cells)
    // =================================================================

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn probe_inter_sb_full() {
        let (sx1, sy1, sx2, sy2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let is = seg(sx1, sy1, sx2, sy2).to_datum_bytes();
        let ib = bx(bhx, bhy, blx, bly).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_inter_sb, is.as_ptr(), ib.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_inter_sb(sx1, sy1, sx2, sy2, bhx, bhy, blx, bly, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // [sx1, sy1, sx2, sy2, bhx, bhy, blx, bly] — segments horizontal or
    // vertical so every reachable point_dt / interpt keeps an exact-zero
    // coordinate difference (hypot general path fenced by construction).
    const ISGRID: [[f64; 8]; 9] = [
        [0.2, 0.5, 0.8, 0.5, 1.0, 1.0, 0.0, 0.0],   // wholly inside
        [-1.0, 0.5, 2.0, 0.5, 1.0, 1.0, 0.0, 0.0],  // crosses through
        [-1.0, 2.0, 2.0, 2.0, 1.0, 1.0, 0.0, 0.0],  // above: bbox disjoint
        [5.0, 5.0, 6.0, 5.0, 1.0, 1.0, 0.0, 0.0],   // far disjoint
        [-1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 0.0, 0.0],  // along top border
        [0.5, -1.0, 0.5, 2.0, 1.0, 1.0, 0.0, 0.0],  // vertical crossing
        [-1.0, 1.0 + 5.0e-7, 2.0, 1.0 + 5.0e-7, 1.0, 1.0, 0.0, 0.0], // eps above
        [f64::NAN, 0.5, 0.5, 0.5, 1.0, 1.0, 0.0, 0.0], // NaN
        [0.5, f64::INFINITY, 0.5, -1.0, 1.0, 1.0, 0.0, 0.0], // Inf vertical
    ];

    // Per-cell spot harnesses: the symbolic-index form path-explodes in
    // symex through the 4-edge interpt chain (killed at 240s) — a LITERAL
    // cell index concretizes the whole chain (wave-6 literal-fold rule).
    macro_rules! inter_sb_cell {
        ($($h:ident: $i:expr;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let a = ISGRID[$i];
                let is = seg(a[0], a[1], a[2], a[3]).to_datum_bytes();
                let ib = bx(a[4], a[5], a[6], a[7]).to_datum_bytes();
                let r = call2(adt_geo::builtins::fc_inter_sb, is.as_ptr(), ib.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe {
                    pg_inter_sb(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], &mut cres)
                };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    inter_sb_cell! {
        eq_inter_sb_cell0: 0;
        eq_inter_sb_cell1: 1;
        eq_inter_sb_cell2: 2;
        eq_inter_sb_cell3: 3;
        eq_inter_sb_cell4: 4;
        eq_inter_sb_cell5: 5;
        eq_inter_sb_cell6: 6;
        eq_inter_sb_cell7: 7;
        eq_inter_sb_cell8: 8;
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_inter_sb_grid() {
        let i: usize = kani::any();
        kani::assume(i < ISGRID.len());
        let a = ISGRID[i];
        let is = seg(a[0], a[1], a[2], a[3]).to_datum_bytes();
        let ib = bx(a[4], a[5], a[6], a[7]).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_inter_sb, is.as_ptr(), ib.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_inter_sb(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // =================================================================
    // line predicates
    // =================================================================

    /// line_vertical / line_horizontal: FPzero on one field — fully
    /// symbolic LINE through the full wrapper stack, total.
    macro_rules! line_pred1 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (a, b, c) = (any_f64(), any_f64(), any_f64());
                let il = line(a, b, c).to_datum_bytes();
                let r = match call1(adt_geo::builtins::$fc, il.as_ptr()) {
                    Ok(d) => d.as_bool(),
                    Err(_) => panic!("infallible geo predicate errored"),
                };
                let cres = unsafe { $pg(a, b, c) };
                assert!(r as c_int == cres);
            }
        )*};
    }

    line_pred1! {
        eq_line_vertical: fc_line_vertical / pg_line_vertical;
        eq_line_horizontal: fc_line_horizontal / pg_line_horizontal;
    }

    // line_perp: four early arms via literal-selector planes (a literal
    // 0.0 / 1.0 coefficient resolves the FPzero branch concretely and
    // prunes the 53-bit general arm from symex — assumed fences do not);
    // general (A1*A2)/(B1*B2) arm via the concrete grid below.

    // Arm 1: FPzero(l1.A) -> FPzero(l2.B): l1.A literal 0, l1.B/l2 fully sym.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_line_perp_arm1() {
        let (l1b, l1c, l2a, l2b, l2c) = (any_f64(), any_f64(), any_f64(), any_f64(), any_f64());
        let i1 = line(0.0, l1b, l1c).to_datum_bytes();
        let i2 = line(l2a, l2b, l2c).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_line_perp, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_line_perp(0.0, l1b, l1c, l2a, l2b, l2c, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // Arm 2: !FPzero(l1.A) && FPzero(l2.A) -> FPzero(l1.B): literal selectors.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_line_perp_arm2() {
        let (l1b, l1c, l2b, l2c) = (any_f64(), any_f64(), any_f64(), any_f64());
        let i1 = line(1.0, l1b, l1c).to_datum_bytes();
        let i2 = line(0.0, l2b, l2c).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_line_perp, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_line_perp(1.0, l1b, l1c, 0.0, l2b, l2c, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // Arm 3: A's nonzero, FPzero(l1.B) -> FPzero(l2.A restated): literals.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_line_perp_arm3() {
        let (l1c, l2b, l2c) = (any_f64(), any_f64(), any_f64());
        let i1 = line(1.0, 0.0, l1c).to_datum_bytes();
        let i2 = line(1.0, l2b, l2c).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_line_perp, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_line_perp(1.0, 0.0, l1c, 1.0, l2b, l2c, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // Arm 4: A's and l1.B nonzero, FPzero(l2.B) -> FPzero(l1.A restated).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_line_perp_arm4() {
        let (l1c, l2c) = (any_f64(), any_f64());
        let i1 = line(1.0, 1.0, l1c).to_datum_bytes();
        let i2 = line(1.0, 0.0, l2c).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_line_perp, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_line_perp(1.0, 1.0, l1c, 1.0, 0.0, l2c, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // Concrete LINE grid: general-arm cells (ratio -1 exact / in the FPeq
    // band / off), proportional pairs, zero-cascade cells (A then B then C
    // then all-zero -> ratio 1.0), overflow/underflow error cells, NaN.
    const LGRID: [[f64; 3]; 10] = [
        [1.0, -1.0, 0.0],              // y = x
        [1.0, 1.0, 0.0],               // y = -x (perp to #0: ratio -1 exact)
        [2.0, -2.0, 0.0],              // proportional to #0
        [1.0 + 5.0e-7, -1.0, 0.0],     // inside FPeq band of #0
        [1.0, 1.0 + 3.0e-6, 4.0],      // ratio just outside -1 band vs #0
        [0.0, -1.0, 3.0],              // horizontal
        [-1.0, 0.0, 3.0],              // vertical
        [0.0, 0.0, 0.0],               // degenerate all-zero (ratio = 1.0)
        [1.0e300, -1.0e-300, 2.0],     // overflow/underflow products
        [f64::NAN, -1.0, 0.0],         // NaN
    ];

    macro_rules! line_grid_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < LGRID.len() && j < LGRID.len());
                let a = LGRID[i];
                let b = LGRID[j];
                let i1 = line(a[0], a[1], a[2]).to_datum_bytes();
                let i2 = line(b[0], b[1], b[2]).to_datum_bytes();
                let r = call2(adt_geo::builtins::$fc, i1.as_ptr(), i2.as_ptr())
                    .map(Datum::as_bool);
                let mut cres: c_int = 0;
                let cerr = unsafe { $pg(a[0], a[1], a[2], b[0], b[1], b[2], &mut cres) };
                if let Some(v) = adjudicate(r, cerr) {
                    assert!(v as c_int == cres);
                }
            }
        )*};
    }

    line_grid_op! {
        eq_line_perp_grid: fc_line_perp / pg_line_perp;
        eq_line_eq_grid: fc_line_eq / pg_line_eq;
    }

    /// line_eq NaN plane: l1.A literally NaN (prunes the ratio arm
    /// concretely — the NaN-exact comparison triple is the whole circuit);
    /// all five other parameters fully symbolic.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_line_eq_nan_plane() {
        let (l1b, l1c, l2a, l2b, l2c) = (any_f64(), any_f64(), any_f64(), any_f64(), any_f64());
        let i1 = line(f64::NAN, l1b, l1c).to_datum_bytes();
        let i2 = line(l2a, l2b, l2c).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_line_eq, i1.as_ptr(), i2.as_ptr())
            .map(Datum::as_bool);
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_line_eq(f64::NAN, l1b, l1c, l2a, l2b, l2c, &mut cres) };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    // =================================================================
    // by-ref-result constructors: mcx-stubs recipe harnesses
    // =================================================================

    /// point_box: pure copy Point -> BOX{high:pt, low:pt}.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_point_box_img() {
        let (px, py) = (any_f64(), any_f64());
        let ip = pt(px, py).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_point_box,
            [Datum::from_usize(ip.as_ptr() as usize)],
        );
        let mut cout = [0.0f64; 4];
        let cerr = unsafe { pg_point_box(px, py, cout.as_mut_ptr()) };
        let d = expect_ok(r, cerr);
        assert_img4(d, &cout);
    }

    /// points_box: box_construct's exact float8_gt normalization.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_points_box_img() {
        let (x1, y1, x2, y2) = (any_f64(), any_f64(), any_f64(), any_f64());
        let i1 = pt(x1, y1).to_datum_bytes();
        let i2 = pt(x2, y2).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_points_box,
            [
                Datum::from_usize(i1.as_ptr() as usize),
                Datum::from_usize(i2.as_ptr() as usize),
            ],
        );
        let mut cout = [0.0f64; 4];
        let cerr = unsafe { pg_points_box(x1, y1, x2, y2, cout.as_mut_ptr()) };
        let d = expect_ok(r, cerr);
        assert_img4(d, &cout);
    }

    /// boxes_bound_box: float8_max/min (NaN-aware exact compares).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_boxes_bound_box_img() {
        let (ahx, ahy, alx, aly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let i1 = bx(ahx, ahy, alx, aly).to_datum_bytes();
        let i2 = bx(bhx, bhy, blx, bly).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_boxes_bound_box,
            [
                Datum::from_usize(i1.as_ptr() as usize),
                Datum::from_usize(i2.as_ptr() as usize),
            ],
        );
        let mut cout = [0.0f64; 4];
        let cerr = unsafe {
            pg_boxes_bound_box(ahx, ahy, alx, aly, bhx, bhy, blx, bly, cout.as_mut_ptr())
        };
        let d = expect_ok(r, cerr);
        assert_img4(d, &cout);
    }

    /// box_intersect: box_ov verdict incl the SQL NULL arm (isnull
    /// parity), image on the non-null arm; fully symbolic.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_box_intersect_img() {
        let (ahx, ahy, alx, aly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (bhx, bhy, blx, bly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let i1 = bx(ahx, ahy, alx, aly).to_datum_bytes();
        let i2 = bx(bhx, bhy, blx, bly).to_datum_bytes();
        let (r, isnull) = call_mcx(
            adt_geo::builtins::fc_box_intersect,
            [
                Datum::from_usize(i1.as_ptr() as usize),
                Datum::from_usize(i2.as_ptr() as usize),
            ],
        );
        let mut cout = [0.0f64; 4];
        let mut cisnull: c_int = 0;
        let cerr = unsafe {
            pg_box_intersect(
                ahx, ahy, alx, aly, bhx, bhy, blx, bly, cout.as_mut_ptr(), &mut cisnull,
            )
        };
        let d = expect_ok(r, cerr);
        assert!(isnull as c_int == cisnull);
        if !isnull {
            kani::cover!(true); // non-null arm reached
            assert_img4(d, &cout);
        } else {
            kani::cover!(true); // NULL arm reached
        }
    }

    /// box_diagonal: pure corner copy into an LSEG image.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_box_diagonal_img() {
        let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let ib = bx(hx, hy, lx, ly).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_box_diagonal,
            [Datum::from_usize(ib.as_ptr() as usize)],
        );
        let mut cout = [0.0f64; 4];
        let cerr = unsafe { pg_box_diagonal(hx, hy, lx, ly, cout.as_mut_ptr()) };
        let d = expect_ok(r, cerr);
        assert_img4(d, &cout);
    }

    /// cr_circle: pure struct build (point + radius -> CIRCLE image).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_cr_circle_img() {
        let (px, py, rad) = (any_f64(), any_f64(), any_f64());
        let ip = pt(px, py).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_cr_circle,
            [Datum::from_usize(ip.as_ptr() as usize), Datum::from_f64(rad)],
        );
        let (mut ccx, mut ccy, mut crad) = (0.0f64, 0.0f64, 0.0f64);
        let cerr = unsafe { pg_cr_circle(px, py, rad, &mut ccx, &mut ccy, &mut crad) };
        let d = expect_ok(r, cerr);
        assert_img2(d, ccx, ccy);
        assert!(img_f64(d, 2).to_bits() == crad.to_bits());
    }

    /// construct_point: by-value f64 args -> Point image, pure copy.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_construct_point_img() {
        let (x, y) = (any_f64(), any_f64());
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_construct_point,
            [Datum::from_f64(x), Datum::from_f64(y)],
        );
        let (mut ox, mut oy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_construct_point(x, y, &mut ox, &mut oy) };
        let d = expect_ok(r, cerr);
        assert_img2(d, ox, oy);
    }

    // box_add / box_sub: float8_pl/mi per corner coordinate — fully
    // symbolic, fallible (overflow arm covered).
    macro_rules! box_addsub_img {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
                let (px, py) = (any_f64(), any_f64());
                let ib = bx(hx, hy, lx, ly).to_datum_bytes();
                let ip = pt(px, py).to_datum_bytes();
                let (r, _) = call_mcx(
                    adt_geo::builtins::$fc,
                    [
                        Datum::from_usize(ib.as_ptr() as usize),
                        Datum::from_usize(ip.as_ptr() as usize),
                    ],
                );
                let mut cout = [0.0f64; 4];
                let cerr = unsafe { $pg(hx, hy, lx, ly, px, py, cout.as_mut_ptr()) };
                if let Some(d) = adjudicate(r, cerr) {
                    assert_img4(d, &cout);
                }
            }
        )*};
    }

    box_addsub_img! {
        eq_box_add_img: fc_box_add / pg_box_add;
        eq_box_sub_img: fc_box_sub / pg_box_sub;
    }

    // =================================================================
    // complex multiply/divide family: literal power-of-two planes
    // (multiplier drawn by ONE SYMBOLIC INDEX from a literal ±pow2/0/
    // Inf/NaN single-component table; the other operand FULLY SYMBOLIC)
    // + concrete pair grids for the general arms
    // =================================================================

    // Single-component multiplier cells: literal ±pow2 / 0 keep every
    // multiply free and (for div) make the divisor a literal pow2.
    const P2GRID: [[f64; 2]; 8] = [
        [1.0, 0.0],
        [0.0, 1.0],
        [-2.0, 0.0],
        [0.0, -0.5],
        [0.5, 0.0],
        [0.0, 0.0],           // div: zero-divide arm
        [f64::INFINITY, 0.0],
        [f64::NAN, 0.0],
    ];

    macro_rules! point_muldiv_pow2 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (x1, y1) = (any_f64(), any_f64());
                let j: usize = kani::any();
                kani::assume(j < P2GRID.len());
                let m = P2GRID[j];
                let i1 = pt(x1, y1).to_datum_bytes();
                let i2 = pt(m[0], m[1]).to_datum_bytes();
                let (r, _) = call_mcx(
                    adt_geo::builtins::$fc,
                    [
                        Datum::from_usize(i1.as_ptr() as usize),
                        Datum::from_usize(i2.as_ptr() as usize),
                    ],
                );
                let (mut ox, mut oy) = (0.0f64, 0.0f64);
                let cerr = unsafe { $pg(x1, y1, m[0], m[1], &mut ox, &mut oy) };
                if let Some(d) = adjudicate_verdict(r, cerr) {
                    assert_img2(d, ox, oy);
                }
            }
        )*};
    }

    point_muldiv_pow2! {
        eq_point_mul_pow2plane: fc_point_mul / pg_point_mul;
        eq_point_div_pow2plane: fc_point_div / pg_point_div;
    }

    macro_rules! box_muldiv_pow2 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
                let j: usize = kani::any();
                kani::assume(j < P2GRID.len());
                let m = P2GRID[j];
                let ib = bx(hx, hy, lx, ly).to_datum_bytes();
                let ip = pt(m[0], m[1]).to_datum_bytes();
                let (r, _) = call_mcx(
                    adt_geo::builtins::$fc,
                    [
                        Datum::from_usize(ib.as_ptr() as usize),
                        Datum::from_usize(ip.as_ptr() as usize),
                    ],
                );
                let mut cout = [0.0f64; 4];
                let cerr = unsafe { $pg(hx, hy, lx, ly, m[0], m[1], cout.as_mut_ptr()) };
                if let Some(d) = adjudicate_verdict(r, cerr) {
                    assert_img4(d, &cout);
                }
            }
        )*};
    }

    box_muldiv_pow2! {
        eq_box_mul_pow2plane: fc_box_mul / pg_box_mul;
        eq_box_div_pow2plane: fc_box_div / pg_box_div;
    }

    // The symbolic-index pow2 plane path-explodes for the BOX forms (2x
    // point_mul_point + box_construct; kissat 300s / default 480s walls
    // measured 2026-07-29) — enumerate the SAME 8-cell multiplier table
    // with LITERAL indices instead (wave-6 literal-fold rule; per-cell
    // split is exhaustive over the table by construction, box fully
    // symbolic in every cell).
    macro_rules! box_muldiv_pow2_cell {
        ($($h:ident: $fc:ident / $pg:ident / $i:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
                let m = P2GRID[$i];
                let ib = bx(hx, hy, lx, ly).to_datum_bytes();
                let ip = pt(m[0], m[1]).to_datum_bytes();
                let (r, _) = call_mcx(
                    adt_geo::builtins::$fc,
                    [
                        Datum::from_usize(ib.as_ptr() as usize),
                        Datum::from_usize(ip.as_ptr() as usize),
                    ],
                );
                let mut cout = [0.0f64; 4];
                let cerr = unsafe { $pg(hx, hy, lx, ly, m[0], m[1], cout.as_mut_ptr()) };
                if let Some(d) = adjudicate_verdict(r, cerr) {
                    assert_img4(d, &cout);
                }
            }
        )*};
    }

    box_muldiv_pow2_cell! {
        eq_box_mul_p2c0: fc_box_mul / pg_box_mul / 0;
        eq_box_mul_p2c1: fc_box_mul / pg_box_mul / 1;
        eq_box_mul_p2c2: fc_box_mul / pg_box_mul / 2;
        eq_box_mul_p2c3: fc_box_mul / pg_box_mul / 3;
        eq_box_mul_p2c4: fc_box_mul / pg_box_mul / 4;
        eq_box_mul_p2c5: fc_box_mul / pg_box_mul / 5;
        eq_box_mul_p2c6: fc_box_mul / pg_box_mul / 6;
        eq_box_mul_p2c7: fc_box_mul / pg_box_mul / 7;
        eq_box_div_p2c0: fc_box_div / pg_box_div / 0;
        eq_box_div_p2c1: fc_box_div / pg_box_div / 1;
        eq_box_div_p2c2: fc_box_div / pg_box_div / 2;
        eq_box_div_p2c3: fc_box_div / pg_box_div / 3;
        eq_box_div_p2c4: fc_box_div / pg_box_div / 4;
        eq_box_div_p2c5: fc_box_div / pg_box_div / 5;
        eq_box_div_p2c6: fc_box_div / pg_box_div / 6;
        eq_box_div_p2c7: fc_box_div / pg_box_div / 7;
    }

    // Concrete pair grid: general (non-pow2) multiplier cells + the
    // error lattice at fully concrete operands.
    const PGRID: [[f64; 2]; 10] = [
        [1.0, 0.0],
        [0.0, 1.0],
        [2.0, 3.0],
        [-1.5, 0.5],
        [0.0, 0.0],                // div-by-(0,0): 22012
        [1.0e300, 0.0],            // product overflow
        [1.0e-300, 1.0e-300],      // product underflow
        [f64::INFINITY, 1.0],
        [f64::NAN, 1.0],
        [1.0 + 1.0e-7, 0.0],
    ];

    macro_rules! point_muldiv_grid {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let i: usize = kani::any();
                let j: usize = kani::any();
                kani::assume(i < PGRID.len() && j < PGRID.len());
                let a = PGRID[i];
                let b = PGRID[j];
                let i1 = pt(a[0], a[1]).to_datum_bytes();
                let i2 = pt(b[0], b[1]).to_datum_bytes();
                let (r, _) = call_mcx(
                    adt_geo::builtins::$fc,
                    [
                        Datum::from_usize(i1.as_ptr() as usize),
                        Datum::from_usize(i2.as_ptr() as usize),
                    ],
                );
                let (mut ox, mut oy) = (0.0f64, 0.0f64);
                let cerr = unsafe { $pg(a[0], a[1], b[0], b[1], &mut ox, &mut oy) };
                if let Some(d) = adjudicate_verdict(r, cerr) {
                    assert_img2(d, ox, oy);
                }
            }
        )*};
    }

    point_muldiv_grid! {
        eq_point_mul_grid: fc_point_mul / pg_point_mul;
        eq_point_div_grid: fc_point_div / pg_point_div;
    }

    // =================================================================
    // point_dt users: axis-aligned slices (shared symbolic y)
    // =================================================================

    /// box_distance on the shared-y slice: all four y's are ONE symbolic
    /// variable, so both centers get bit-identical y's and the point_dt
    /// dy is exactly 0.0 (finite) / NaN (Inf, NaN) — sqrt-free hypot arms
    /// only. x's and the float8_pl overflow arm fully symbolic.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_box_distance_hslice() {
        let (ahx, alx, bhx, blx) = (any_f64(), any_f64(), any_f64(), any_f64());
        let y = any_f64();
        let i1 = bx(ahx, y, alx, y).to_datum_bytes();
        let i2 = bx(bhx, y, blx, y).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_box_distance, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe { pg_box_distance(ahx, y, alx, y, bhx, y, blx, y, &mut cval) };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    /// box_circle on the shared-y slice (center.y == high.y bit-exactly on
    /// the finite plane, so the radius point_dt is sqrt-free); CIRCLE
    /// result image via the mcx recipe.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_box_circle_hslice() {
        let (hx, lx) = (any_f64(), any_f64());
        let y = any_f64();
        let ib = bx(hx, y, lx, y).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_box_circle,
            [Datum::from_usize(ib.as_ptr() as usize)],
        );
        let (mut ccx, mut ccy, mut crad) = (0.0f64, 0.0f64, 0.0f64);
        let cerr = unsafe { pg_box_circle(hx, y, lx, y, &mut ccx, &mut ccy, &mut crad) };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert_img2(d, ccx, ccy);
            assert!(img_f64(d, 2).to_bits() == crad.to_bits());
        }
    }

    // Fallback grids for the point_dt users: the fully/partially symbolic
    // hslices wall BOTH solvers at 550s (box_cn's four /2.0 divides + one
    // point_dt exceed the 43-204s single-point_dt budget; measured
    // 2026-07-29). One-symbolic-index CONCRETE grids, every cell
    // axis-aligned (dx==0 or dy==0) or NaN/Inf so the general hypot path
    // (mul_add model-spec gap) stays unreachable.

    // [ahx, ahy, alx, aly, bhx, bhy, blx, bly] — centers share y (or x).
    const BDGRID: [[f64; 8]; 8] = [
        [2.0, 1.0, 0.0, 1.0, 6.0, 1.0, 4.0, 1.0],   // shared-y, d=4
        [2.0, 1.0, 0.0, 1.0, 2.0, 1.0, 0.0, 1.0],   // identical, d=0
        [1.0, 4.0, 0.0, 2.0, 1.0, 10.0, 0.0, 8.0],  // shared-x centers, d=6
        [1.0, 0.0, 0.0, 0.0, 1.0 + 1.0e-7, 0.0, 1.0e-7, 0.0], // eps offset
        [f64::NAN, 1.0, 0.0, 1.0, 6.0, 1.0, 4.0, 1.0], // NaN center
        [f64::INFINITY, 1.0, 0.0, 1.0, 6.0, 1.0, 4.0, 1.0], // Inf center
        [1.0e308, 1.0, 1.0e308, 1.0, -1.0e308, 1.0, -1.0e308, 1.0], // dx overflow -> 22003
        [0.0, -3.0, -2.0, -3.0, -6.0, -3.0, -8.0, -3.0], // negative shared-y
    ];

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_box_distance_grid() {
        let i: usize = kani::any();
        kani::assume(i < BDGRID.len());
        let a = BDGRID[i];
        let i1 = bx(a[0], a[1], a[2], a[3]).to_datum_bytes();
        let i2 = bx(a[4], a[5], a[6], a[7]).to_datum_bytes();
        let r = call2(adt_geo::builtins::fc_box_distance, i1.as_ptr(), i2.as_ptr());
        let mut cval: f64 = 0.0;
        let cerr = unsafe {
            pg_box_distance(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], &mut cval)
        };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert!(d.as_f64().to_bits() == cval.to_bits());
        }
    }

    // [ahx, ahy, alx, aly] — hy == ly (degenerate height: the radius
    // point_dt keeps dy == 0) or NaN/Inf arms.
    const BCGRID: [[f64; 4]; 8] = [
        [4.0, 1.0, 0.0, 1.0],                 // center (2,1), radius 2
        [1.0, 1.0, 1.0, 1.0],                 // degenerate point box, r=0
        [0.5, -2.0, -0.5, -2.0],              // negative y, r=0.5
        [1.0 + 1.0e-7, 3.0, 1.0, 3.0],        // epsilon width
        [f64::NAN, 1.0, 0.0, 1.0],            // NaN coordinate
        [f64::INFINITY, 1.0, 0.0, 1.0],       // Inf coordinate
        [1.0e308, 1.0, -1.0e308, 1.0],        // hx+lx = 0 center, huge radius
        [1.0e308, 1.0e308, 1.0e308, 1.0e308], // center-sum overflow -> 22003
    ];

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_box_circle_grid() {
        let i: usize = kani::any();
        kani::assume(i < BCGRID.len());
        let a = BCGRID[i];
        let ib = bx(a[0], a[1], a[2], a[3]).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_box_circle,
            [Datum::from_usize(ib.as_ptr() as usize)],
        );
        let (mut ccx, mut ccy, mut crad) = (0.0f64, 0.0f64, 0.0f64);
        let cerr = unsafe { pg_box_circle(a[0], a[1], a[2], a[3], &mut ccx, &mut ccy, &mut crad) };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert_img2(d, ccx, ccy);
            assert!(img_f64(d, 2).to_bits() == crad.to_bits());
        }
    }

    // =================================================================
    // line_construct_pp: slope early arms + distinct-points ereport
    // =================================================================

    /// Shared-x plane: both points carry ONE symbolic x, so point_sl's
    /// FPeq(x,x) fires on the whole finite plane (vertical arm; NaN x
    /// falls through with NaN products handled by the same circuits both
    /// sides). Distinct-points check (22023 Err arm) fully in-theorem.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn probe_line_construct_pp_vshared() {
        let (y1, y2) = (any_f64(), any_f64());
        let x = any_f64();
        let i1 = pt(x, y1).to_datum_bytes();
        let i2 = pt(x, y2).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_line_construct_pp,
            [
                Datum::from_usize(i1.as_ptr() as usize),
                Datum::from_usize(i2.as_ptr() as usize),
            ],
        );
        let mut cout = [0.0f64; 3];
        let cerr = unsafe { pg_line_construct_pp(x, y1, x, y2, cout.as_mut_ptr()) };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert!(img_f64(d, 0).to_bits() == cout[0].to_bits());
            assert!(img_f64(d, 1).to_bits() == cout[1].to_bits());
            assert!(img_f64(d, 2).to_bits() == cout[2].to_bits());
        }
    }

    // Concrete point-pair grid: vertical / horizontal / general-slope
    // (divide + C = y - m*x arms at concrete operands) / equal ->
    // 22023 error / epsilon-band-equal -> error / NaN / Inf cells.
    const PPGRID: [[f64; 4]; 9] = [
        [0.0, 0.0, 0.0, 1.0],               // vertical: x = C
        [0.0, 0.0, 1.0, 0.0],               // horizontal: y = C
        [0.0, 0.0, 2.0, 2.0],               // slope 1
        [1.0, 5.0, 3.0, 1.0],               // slope -2, nonzero intercept
        [1.0, 1.0, 1.0, 1.0],               // equal -> 22023
        [1.0, 1.0, 1.0 + 5.0e-7, 1.0],      // FPeq-band equal -> 22023
        [0.0, 0.0, f64::INFINITY, 1.0],     // Inf coordinate
        [f64::NAN, 0.0, 1.0, 1.0],          // NaN coordinate
        [0.0, 0.0, 1.0e-7, 3.0],            // fuzzy-vertical (eps band)
    ];

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_line_construct_pp_grid() {
        let i: usize = kani::any();
        kani::assume(i < PPGRID.len());
        let a = PPGRID[i];
        let i1 = pt(a[0], a[1]).to_datum_bytes();
        let i2 = pt(a[2], a[3]).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_line_construct_pp,
            [
                Datum::from_usize(i1.as_ptr() as usize),
                Datum::from_usize(i2.as_ptr() as usize),
            ],
        );
        let mut cout = [0.0f64; 3];
        let cerr = unsafe { pg_line_construct_pp(a[0], a[1], a[2], a[3], cout.as_mut_ptr()) };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert!(img_f64(d, 0).to_bits() == cout[0].to_bits());
            assert!(img_f64(d, 1).to_bits() == cout[1].to_bits());
            assert!(img_f64(d, 2).to_bits() == cout[2].to_bits());
        }
    }

    // =================================================================
    // negative control: the extension-2 rig must be able to fail
    // =================================================================

    /// Shipped fc_box_add vs the C box_sub model: MUST fail with a
    /// decodable counterexample (e.g. p = (1,0)). Witnesses that the
    /// by-ref result image comparison is falsifiable end-to-end
    /// (allocator stubs, image read-back, err adjudication).
    /// Run with the DEFAULT solver (kissat never terminates on failures).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_box_add_vs_c_box_sub() {
        let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let (px, py) = (any_f64(), any_f64());
        let ib = bx(hx, hy, lx, ly).to_datum_bytes();
        let ip = pt(px, py).to_datum_bytes();
        let (r, _) = call_mcx(
            adt_geo::builtins::fc_box_add,
            [
                Datum::from_usize(ib.as_ptr() as usize),
                Datum::from_usize(ip.as_ptr() as usize),
            ],
        );
        let mut cout = [0.0f64; 4];
        let cerr = unsafe { pg_box_sub(hx, hy, lx, ly, px, py, cout.as_mut_ptr()) };
        if let Some(d) = adjudicate_verdict(r, cerr) {
            assert_img4(d, &cout);
        }
    }
}
