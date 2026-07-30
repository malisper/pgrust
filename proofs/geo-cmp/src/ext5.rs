//! Kani C≡Rust equivalence — geo-cmp EXTENSION 5: the scalar-verdict
//! PLANES of the path/poly ladder rows whose general bodies are ratified
//! walls (min-over-segments, lseg_inside_poly recursion, na*nb interpt,
//! shoelace 53-bit multiplies).
//!
//! Same trusted-varlena-builder rig as ext3 (harness-built images through
//! the shipped fc_* wrappers; C side staged structs, bodies verbatim).
//! OUT-OF-PLANE TRAPS: the C deep helpers unreachable inside each plane
//! set pg_geo_errflag = 99 — every harness asserts the errflag, so a
//! leaking plane FAILS loudly (literal-planes law).
//!
//! Planes:
//!   - path_area OPEN plane (closed literal 0): SQL-NULL verdict parity,
//!     points fully symbolic, npts 1..=3 symbolic — the shoelace body is
//!     behind the literal-false closed check on both sides.
//!   - path_distance NO-SEGMENT plane (p2 open n=1 literal): inner loop
//!     yields nothing -> have_min stays false -> SQL NULL on both sides;
//!     p1 fully symbolic (npts 1..=3, full-i32 closed, symbolic points).
//!   - path_inter n=1 OPEN plane: single-point bound boxes (loop-free),
//!     box_ov fuzzy test in-theorem, segment enumeration structurally
//!     empty -> false on both sides; fully symbolic points.
//!   - poly_contain / poly_contained / poly_overlap bbox-fail planes:
//!     ONE literal separating coordinate makes the boundbox quick-check
//!     fail (FPge(0,10) / FPle(10,0) constant-folds and prunes the deep
//!     walk on both sides); the other 7 boundbox coords and all points
//!     fully symbolic; n=1 cells; result pinned false.
//!
//! Run: ./run-one.sh "ext5::proofs_ext5::<harness>" <timeout-s> [--solver kissat]

#[cfg(kani)]
mod proofs_ext5 {
    use adt_geo::{PathRef, PolyRef};
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs, FcFn};
    use types_error::PgError;
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        fn pg_path_area_w(closed: c_int, npts: c_int, xy: *const f64, out: *mut f64, isnull: *mut c_int) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_path_distance_w(c1: c_int, n1: c_int, xy1: *const f64, c2: c_int, n2: c_int, xy2: *const f64, out: *mut f64, isnull: *mut c_int) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_path_inter_w(c1: c_int, n1: c_int, xy1: *const f64, c2: c_int, n2: c_int, xy2: *const f64, result: *mut c_int) -> c_int;
        fn pg_poly_contain_w(na: c_int, bba: *const f64, pa: *const f64, nb: c_int, bbb: *const f64, pb: *const f64, result: *mut c_int) -> c_int;
        fn pg_poly_contained_w(na: c_int, bba: *const f64, pa: *const f64, nb: c_int, bbb: *const f64, pb: *const f64, result: *mut c_int) -> c_int;
        fn pg_poly_overlap_w(na: c_int, bba: *const f64, pa: *const f64, nb: c_int, bbb: *const f64, pb: *const f64, result: *mut c_int) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    // trusted varlena builders (ext3 fence)
    const CAP: usize = 3;
    const PATH_BUF: usize = 4 + 12 + 16 * CAP;
    const POLY_BUF: usize = 4 + 36 + 16 * CAP;

    fn path_img(npts: i32, closed: i32, pts: &[(f64, f64)]) -> [u8; PATH_BUF] {
        let mut b = [0u8; PATH_BUF];
        let total = (16 + 16 * npts as usize) as u32;
        b[0..4].copy_from_slice(&(total << 2).to_ne_bytes());
        b[4..8].copy_from_slice(&npts.to_ne_bytes());
        b[8..12].copy_from_slice(&closed.to_ne_bytes());
        let mut i = 0;
        while i < pts.len() {
            let off = 16 + 16 * i;
            b[off..off + 8].copy_from_slice(&pts[i].0.to_ne_bytes());
            b[off + 8..off + 16].copy_from_slice(&pts[i].1.to_ne_bytes());
            i += 1;
        }
        b
    }

    fn poly_img(npts: i32, bb: [f64; 4], pts: &[(f64, f64)]) -> [u8; POLY_BUF] {
        let mut b = [0u8; POLY_BUF];
        let total = (40 + 16 * npts as usize) as u32;
        b[0..4].copy_from_slice(&(total << 2).to_ne_bytes());
        b[4..8].copy_from_slice(&npts.to_ne_bytes());
        b[8..16].copy_from_slice(&bb[0].to_ne_bytes());
        b[16..24].copy_from_slice(&bb[1].to_ne_bytes());
        b[24..32].copy_from_slice(&bb[2].to_ne_bytes());
        b[32..40].copy_from_slice(&bb[3].to_ne_bytes());
        let mut i = 0;
        while i < pts.len() {
            let off = 40 + 16 * i;
            b[off..off + 8].copy_from_slice(&pts[i].0.to_ne_bytes());
            b[off + 8..off + 16].copy_from_slice(&pts[i].1.to_ne_bytes());
            i += 1;
        }
        b
    }

    /// 2-arg fc call returning (result, isnull).
    fn call2n(fc: FcFn<Box<PgError>>, a: *const u8, b: *const u8) -> (Result<Datum, Box<PgError>>, bool) {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(a as usize));
        f.args[1] = NullableDatum::value(Datum::from_usize(b as usize));
        let r = fc(None, &mut f);
        let isnull = f.isnull;
        (r, isnull)
    }

    fn call1n(fc: FcFn<Box<PgError>>, a: *const u8) -> (Result<Datum, Box<PgError>>, bool) {
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(a as usize));
        let r = fc(None, &mut f);
        let isnull = f.isnull;
        (r, isnull)
    }

    fn expect_ok(r: Result<Datum, Box<PgError>>) -> Datum {
        match r {
            Ok(d) => d,
            Err(_) => panic!("plane harness reached an error arm"),
        }
    }

    /// path_area OPEN plane: SQL-NULL verdict on both sides.
    #[kani::proof]
    #[kani::unwind(5)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_path_area_open_plane() {
        let n: i32 = kani::any();
        kani::assume(n >= 1 && n <= CAP as i32);
        let pts = [(any_f64(), any_f64()), (any_f64(), any_f64()), (any_f64(), any_f64())];
        let img = path_img(n, 0, &pts); // closed = literal 0 (open)
        let (r, isnull) = call1n(adt_geo::builtins::fc_path_area, img.as_ptr());
        let flat = [pts[0].0, pts[0].1, pts[1].0, pts[1].1, pts[2].0, pts[2].1];
        let (mut cout, mut cnull) = (0.0f64, 0);
        let cerr = unsafe { pg_path_area_w(0, n, flat.as_ptr(), &mut cout, &mut cnull) };
        assert!(cerr == 0); // trap-free plane
        let _ = expect_ok(r);
        assert!(cnull == 1);
        assert!(isnull); // SQL NULL parity
    }

    /// path_distance NO-SEGMENT plane (p2 open n=1): SQL NULL both sides.
    #[kani::proof]
    #[kani::unwind(5)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_path_distance_nullplane() {
        let n1: i32 = kani::any();
        kani::assume(n1 >= 1 && n1 <= CAP as i32);
        let c1: i32 = kani::any();
        let pts1 = [(any_f64(), any_f64()), (any_f64(), any_f64()), (any_f64(), any_f64())];
        let p2pt = (any_f64(), any_f64());
        let i1 = path_img(n1, c1, &pts1);
        let i2 = path_img(1, 0, &[p2pt]); // open single point: no segments
        // CORE-level (fc pointer round-trip defeats the literal fold and
        // drags the fenced-out closept body into symex; the wrapper's
        // varlena decode is proven by the ext3 rows)
        let p1 = PathRef::from_payload(&i1[4..4 + 12 + 16 * n1 as usize]);
        let p2 = PathRef::from_payload(&i2[4..4 + 12 + 16]);
        let r = adt_geo::path::path_distance(&p1, &p2);
        let flat1 = [pts1[0].0, pts1[0].1, pts1[1].0, pts1[1].1, pts1[2].0, pts1[2].1];
        let flat2 = [p2pt.0, p2pt.1];
        let (mut cout, mut cnull) = (0.0f64, 0);
        let cerr = unsafe {
            pg_path_distance_w(c1, n1, flat1.as_ptr(), 0, 1, flat2.as_ptr(), &mut cout, &mut cnull)
        };
        assert!(cerr == 0); // the closept trap must NOT fire
        assert!(cnull == 1);
        match r {
            Ok(v) => assert!(v.is_none()), // SQL NULL parity
            Err(_) => panic!("plane harness reached an error arm"),
        }
    }

    /// path_inter n=1 OPEN plane: single-point bound boxes + box_ov fuzzy
    /// test in-theorem; segment enumeration structurally empty.
    #[kani::proof]
    #[kani::unwind(5)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_path_inter_n1_open() {
        let a = (any_f64(), any_f64());
        let b = (any_f64(), any_f64());
        let i1 = path_img(1, 0, &[a]);
        let i2 = path_img(1, 0, &[b]);
        // CORE-level (same literal-fold rationale as path_distance)
        let p1 = PathRef::from_payload(&i1[4..4 + 12 + 16]);
        let p2 = PathRef::from_payload(&i2[4..4 + 12 + 16]);
        let r = adt_geo::path::path_inter(&p1, &p2);
        let fa = [a.0, a.1];
        let fb = [b.0, b.1];
        let mut cres: c_int = 0;
        let cerr = unsafe { pg_path_inter_w(0, 1, fa.as_ptr(), 0, 1, fb.as_ptr(), &mut cres) };
        assert!(cerr == 0);
        match r {
            Ok(v) => assert!(v as c_int == cres),
            Err(_) => panic!("plane harness reached an error arm"),
        }
        assert!(cres == 0); // spec pin: open single points never intersect
    }

    /// poly bbox-fail planes: one literal separating coordinate, the rest
    /// (7 boundbox coords + both points) fully symbolic; n=1 cells.
    macro_rules! poly_plane {
        ($($h:ident: $fc:ident / $pg:ident, bba_field: $ai:literal, bbb_field: $bi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(3)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                // boundbox layout: [high.x, high.y, low.x, low.y]
                let mut bba = [any_f64(), any_f64(), any_f64(), any_f64()];
                let mut bbb = [any_f64(), any_f64(), any_f64(), any_f64()];
                bba[$ai] = 0.0;      // literal
                bbb[$bi] = 10.0;     // literal separator
                let pa = (any_f64(), any_f64());
                let pb = (any_f64(), any_f64());
                // CORE-level via from_parts: literal boundbox STRUCT fields
                // fold and prune the guarded deep walk (the byte-image
                // decode defeats the fold — measured; the image decode is
                // proven by the ext3 rows)
                let pab = [pa.0.to_ne_bytes(), pa.1.to_ne_bytes()].concat();
                let pbb = [pb.0.to_ne_bytes(), pb.1.to_ne_bytes()].concat();
                let ra = PolyRef::from_parts(
                    types_core::geo::BOX {
                        high: types_core::geo::Point { x: bba[0], y: bba[1] },
                        low: types_core::geo::Point { x: bba[2], y: bba[3] },
                    },
                    1,
                    &pab,
                );
                let rb = PolyRef::from_parts(
                    types_core::geo::BOX {
                        high: types_core::geo::Point { x: bbb[0], y: bbb[1] },
                        low: types_core::geo::Point { x: bbb[2], y: bbb[3] },
                    },
                    1,
                    &pbb,
                );
                let r = adt_geo::poly::$fc(&ra, &rb);
                let fa = [pa.0, pa.1];
                let fb = [pb.0, pb.1];
                let mut cres: c_int = 0;
                let cerr = unsafe {
                    $pg(1, bba.as_ptr(), fa.as_ptr(), 1, bbb.as_ptr(), fb.as_ptr(), &mut cres)
                };
                assert!(cerr == 0); // deep-walk traps must NOT fire
                match r {
                    Ok(v) => assert!(v as c_int == cres),
                    Err(_) => panic!("plane harness reached an error arm"),
                }
                assert!(cres == 0); // spec pin: quick-check fails => false
            }
        )*};
    }

    poly_plane! {
        // contain(a,b) = contain_poly(contains=a, contained=b):
        // FPge(a.high.x = 0, b.high.x = 10) fails (both literal)
        eq_poly_contain_bboxfail: poly_contain / pg_poly_contain_w, bba_field: 0, bbb_field: 0;
        // contained(a,b) = contain_poly(contains=b, contained=a):
        // FPle(b.low.x = 10, a.low.x = 0) fails (both literal)
        eq_poly_contained_bboxfail: poly_contained / pg_poly_contained_w, bba_field: 2, bbb_field: 2;
        // overlap: box_ov needs FPle(b.low.x = 10, a.high.x = 0) — fails
        eq_poly_overlap_bboxfail: poly_overlap / pg_poly_overlap_w, bba_field: 0, bbb_field: 2;
    }

    // =================================================================
    // box_poly: the one geo constructor outside the image-width wall
    // (npts LITERALLY 4 => concrete 104B output frame). Full shipped
    // wrapper via the mcx-stubs recipe; C body verbatim incl the
    // box_construct boundbox normalization (Rust recomputes it through
    // bound_box — agreement over full f64 incl NaN is the theorem).
    // =================================================================

    extern "C" {
        fn pg_box_poly_w(hx: f64, hy: f64, lx: f64, ly: f64, out: *mut u8) -> c_int;
    }

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_box_poly_img() {
        let (hx, hy, lx, ly) = (any_f64(), any_f64(), any_f64(), any_f64());
        let ib = {
            let mut b = [0u8; 32];
            b[0..8].copy_from_slice(&hx.to_ne_bytes());
            b[8..16].copy_from_slice(&hy.to_ne_bytes());
            b[16..24].copy_from_slice(&lx.to_ne_bytes());
            b[24..32].copy_from_slice(&ly.to_ne_bytes());
            b
        };
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext5");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(ib.as_ptr() as usize));
        let r = (adt_geo::builtins::fc_box_poly)(None, &mut f);
        core::mem::forget(ctx);
        let mut cout = [0u8; 104];
        let cerr = unsafe { pg_box_poly_w(hx, hy, lx, ly, cout.as_mut_ptr()) };
        assert!(cerr == 0);
        let d = expect_ok(r);
        // 104B result image read back in 16B chunks + one 8B tail
        // (6 + 1 iterations: inside the tight unwind(8) bound)
        fn word_at(d: Datum, k: usize) -> u64 {
            // SAFETY: by-ref result datum points at a live 104B image.
            unsafe {
                let p = (d.as_usize() as *const u8).add(k);
                let mut w = [0u8; 8];
                core::ptr::copy_nonoverlapping(p, w.as_mut_ptr(), 8);
                u64::from_ne_bytes(w)
            }
        }
        let cw = |k: usize| u64::from_ne_bytes(cout[k..k + 8].try_into().unwrap());
        let mut k = 0;
        while k + 16 <= 104 {
            assert!(word_at(d, k) == cw(k));
            assert!(word_at(d, k + 8) == cw(k + 8));
            k += 16;
        }
        assert!(word_at(d, 96) == cw(96));
    }
}
