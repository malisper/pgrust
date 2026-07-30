//! Kani C≡Rust equivalence — geo-cmp EXTENSION 3: the geo-varlena lane.
//! PATH / POLYGON header accessors, path_n_* count comparators, the
//! polygon boundbox position predicates, poly_same, and poly_box
//! (geo_ops.c @ REL_18_STABLE; C side = c/pg_geo_cmp.c EXTENSION 3
//! section, bodies verbatim).
//!
//! NEW rig element — the TRUSTED VARLENA BUILDER fence: PATH/POLYGON are
//! varlena types, so the harness builds a real 4B-uncompressed varlena
//! image (header = total<<2, little-endian; geo_decls.h payload layout:
//! PATH npts/closed/dummy then points at offset 12, POLYGON
//! npts/boundbox then points at offset 36) and hands its pointer to the
//! SHIPPED fc_* wrapper, so arg_varlena_packed + PathRef/PolyRef payload
//! decode are inside the theorem. The C side receives the same header
//! fields / point coordinates directly (staged stack structs — detoast
//! plumbing is out of proof). The fence: only well-formed images (npts
//! consistent with the image extent, 4B uncompressed header) are in the
//! domain — that is exactly the C caller contract post
//! PG_GETARG_PATH_P/POLYGON_P. control_poly_npoints_skew is the
//! must-fail witness that the builder is load-bearing.
//!
//! Domain regimes:
//!   - header accessors / path_n_*: SYMBOLIC npts (0..=cap) and fully
//!     symbolic i32 closed word (any nonzero is true on both sides).
//!   - poly boundbox predicates: boundbox = 4 fully symbolic f64 per
//!     side (exact <,<=,>,>= — NaN semantics in-theorem); npts literal 1,
//!     point slots literal zero (dead-symbolic-bytes law).
//!   - poly_same: per-n cells (n = 1, 2, 3) with fully symbolic points
//!     (FPeq fuzzy + NaN-exact arms in-theorem) + an n-mismatch plane;
//!     unwind exact-fit at cap+1.
//!   - poly_box: by-ref BOX result via the mcx-stubs recipe (ext2
//!     precedent); theorem qualifier "modulo static-buffer allocator
//!     model".
//!
//! Run: ./run-one.sh "ext3::proofs_ext3::<harness>" <timeout-s> [--solver kissat]
//! (control_poly_npoints_skew runs the DEFAULT solver and must FAIL.)

#[cfg(kani)]
mod proofs_ext3 {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs, FcFn};
    use types_error::PgError;
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        // path count comparators / header accessors
        fn pg_path_n_lt(n1: c_int, n2: c_int) -> c_int;
        fn pg_path_n_gt(n1: c_int, n2: c_int) -> c_int;
        fn pg_path_n_eq(n1: c_int, n2: c_int) -> c_int;
        fn pg_path_n_le(n1: c_int, n2: c_int) -> c_int;
        fn pg_path_n_ge(n1: c_int, n2: c_int) -> c_int;
        fn pg_path_isclosed(closed: c_int) -> c_int;
        fn pg_path_isopen(closed: c_int) -> c_int;
        fn pg_path_npoints(npts: c_int) -> c_int;

        // polygon boundbox predicates / accessors
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_left(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_overleft(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_right(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_overright(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_below(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_overbelow(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_above(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_poly_overabove(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_same(na: c_int, nb: c_int, pa: *const f64, pb: *const f64) -> c_int;
        fn pg_poly_npoints(npts: c_int) -> c_int;
        fn pg_poly_box(hx: f64, hy: f64, lx: f64, ly: f64, out4: *mut f64) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    // ---------------- trusted varlena builders (the fence) ----------------

    const PATH_CAP: usize = 3;
    const POLY_CAP: usize = 3;
    /// 4B varlena header + PATH payload (npts/closed/dummy + cap points).
    const PATH_BUF: usize = 4 + 12 + 16 * PATH_CAP;
    /// 4B varlena header + POLYGON payload (npts/boundbox + cap points).
    const POLY_BUF: usize = 4 + 36 + 16 * POLY_CAP;

    /// Well-formed uncompressed 4B-header PATH image: npts consistent with
    /// the declared extent (npts <= cap enforced by callers), point slots
    /// beyond npts stay literal zero (dead-symbolic-bytes law).
    fn path_img(npts: i32, closed: i32, pts: &[(f64, f64)]) -> [u8; PATH_BUF] {
        let mut b = [0u8; PATH_BUF];
        let total = (16 + 16 * npts as usize) as u32;
        b[0..4].copy_from_slice(&(total << 2).to_ne_bytes());
        b[4..8].copy_from_slice(&npts.to_ne_bytes());
        b[8..12].copy_from_slice(&closed.to_ne_bytes());
        // b[12..16] = dummy pad, literal zero
        let mut i = 0;
        while i < pts.len() {
            let off = 16 + 16 * i;
            b[off..off + 8].copy_from_slice(&pts[i].0.to_ne_bytes());
            b[off + 8..off + 16].copy_from_slice(&pts[i].1.to_ne_bytes());
            i += 1;
        }
        b
    }

    /// Well-formed uncompressed 4B-header POLYGON image (same fence).
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

    /// Invoke a shipped 2-arg fc builtin on two by-ref varlena images.
    fn call2v(fc: FcFn<Box<PgError>>, a: *const u8, b: *const u8) -> Result<Datum, Box<PgError>> {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(a as usize));
        f.args[1] = NullableDatum::value(Datum::from_usize(b as usize));
        fc(None, &mut f)
    }

    /// Invoke a shipped 1-arg fc builtin on one by-ref varlena image.
    fn call1v(fc: FcFn<Box<PgError>>, a: *const u8) -> Result<Datum, Box<PgError>> {
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(a as usize));
        fc(None, &mut f)
    }

    /// This slice is infallible within the fenced domain: an Err arm is a
    /// broken harness, not a vacuity signal — panic (no cover).
    fn expect_ok(r: Result<Datum, Box<PgError>>) -> Datum {
        match r {
            Ok(d) => d,
            Err(_) => panic!("infallible geo varlena op errored"),
        }
    }

    // =================================================================
    // path_n_* count comparators: symbolic npts both sides + wrapper
    // varlena decode in-theorem
    // =================================================================

    macro_rules! path_n_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(5)]
            fn $h() {
                let na: i32 = kani::any();
                let nb: i32 = kani::any();
                kani::assume(na >= 0 && na <= PATH_CAP as i32);
                kani::assume(nb >= 0 && nb <= PATH_CAP as i32);
                let ca: i32 = kani::any(); // closed word: full i32
                let cb: i32 = kani::any();
                let ia = path_img(na, ca, &[]);
                let ib = path_img(nb, cb, &[]);
                let r = expect_ok(call2v(adt_geo::builtins::$fc, ia.as_ptr(), ib.as_ptr()));
                let c = unsafe { $pg(na, nb) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    path_n_op! {
        eq_path_n_lt: fc_path_n_lt / pg_path_n_lt;
        eq_path_n_gt: fc_path_n_gt / pg_path_n_gt;
        eq_path_n_eq: fc_path_n_eq / pg_path_n_eq;
        eq_path_n_le: fc_path_n_le / pg_path_n_le;
        eq_path_n_ge: fc_path_n_ge / pg_path_n_ge;
    }

    // =================================================================
    // path / poly header accessors
    // =================================================================

    /// path_isclosed: any nonzero int32 closed word is true on both sides.
    #[kani::proof]
    #[kani::unwind(5)]
    fn eq_path_isclosed() {
        let n: i32 = kani::any();
        kani::assume(n >= 0 && n <= PATH_CAP as i32);
        let closed: i32 = kani::any();
        let img = path_img(n, closed, &[]);
        let r = expect_ok(call1v(adt_geo::builtins::fc_path_isclosed, img.as_ptr()));
        let c = unsafe { pg_path_isclosed(closed) };
        assert!(r.as_bool() as c_int == c);
    }

    #[kani::proof]
    #[kani::unwind(5)]
    fn eq_path_isopen() {
        let n: i32 = kani::any();
        kani::assume(n >= 0 && n <= PATH_CAP as i32);
        let closed: i32 = kani::any();
        let img = path_img(n, closed, &[]);
        let r = expect_ok(call1v(adt_geo::builtins::fc_path_isopen, img.as_ptr()));
        let c = unsafe { pg_path_isopen(closed) };
        assert!(r.as_bool() as c_int == c);
    }

    #[kani::proof]
    #[kani::unwind(5)]
    fn eq_path_npoints() {
        let n: i32 = kani::any();
        kani::assume(n >= 0 && n <= PATH_CAP as i32);
        let closed: i32 = kani::any();
        let img = path_img(n, closed, &[]);
        let r = expect_ok(call1v(adt_geo::builtins::fc_path_npoints, img.as_ptr()));
        let c = unsafe { pg_path_npoints(n) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    #[kani::unwind(5)]
    fn eq_poly_npoints() {
        let n: i32 = kani::any();
        kani::assume(n >= 0 && n <= POLY_CAP as i32);
        let bb = [any_f64(), any_f64(), any_f64(), any_f64()];
        let img = poly_img(n, bb, &[]);
        let r = expect_ok(call1v(adt_geo::builtins::fc_poly_npoints, img.as_ptr()));
        let c = unsafe { pg_poly_npoints(n) };
        assert!(r.as_i32() == c);
    }

    // =================================================================
    // polygon boundbox position predicates: exact compares, fully
    // symbolic boundboxes (NaN semantics in-theorem)
    // =================================================================

    macro_rules! poly_pos_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(5)]
            fn $h() {
                let a = [any_f64(), any_f64(), any_f64(), any_f64()];
                let b = [any_f64(), any_f64(), any_f64(), any_f64()];
                let ia = poly_img(1, a, &[(0.0, 0.0)]);
                let ib = poly_img(1, b, &[(0.0, 0.0)]);
                let r = expect_ok(call2v(adt_geo::builtins::$fc, ia.as_ptr(), ib.as_ptr()));
                let c = unsafe { $pg(a[0], a[1], a[2], a[3], b[0], b[1], b[2], b[3]) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    poly_pos_op! {
        eq_poly_left: fc_poly_left / pg_poly_left;
        eq_poly_overleft: fc_poly_overleft / pg_poly_overleft;
        eq_poly_right: fc_poly_right / pg_poly_right;
        eq_poly_overright: fc_poly_overright / pg_poly_overright;
        eq_poly_below: fc_poly_below / pg_poly_below;
        eq_poly_overbelow: fc_poly_overbelow / pg_poly_overbelow;
        eq_poly_above: fc_poly_above / pg_poly_above;
        eq_poly_overabove: fc_poly_overabove / pg_poly_overabove;
    }

    // =================================================================
    // poly_same: per-n cells (fully symbolic points; plist_same cyclic
    // forward/backward match + FPeq/NaN-exact arms in-theorem)
    // =================================================================

    macro_rules! poly_same_cell {
        ($($h:ident: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(5)]
            fn $h() {
                const N: usize = $n;
                let mut pa = [(0.0f64, 0.0f64); N];
                let mut pb = [(0.0f64, 0.0f64); N];
                let mut i = 0;
                while i < N {
                    pa[i] = (any_f64(), any_f64());
                    pb[i] = (any_f64(), any_f64());
                    i += 1;
                }
                // boundboxes are not read by poly_same; literal zeros.
                let ia = poly_img(N as i32, [0.0; 4], &pa);
                let ib = poly_img(N as i32, [0.0; 4], &pb);
                let r = expect_ok(call2v(adt_geo::builtins::fc_poly_same, ia.as_ptr(), ib.as_ptr()));
                let c = unsafe {
                    pg_poly_same(N as c_int, N as c_int, pa.as_ptr() as *const f64, pb.as_ptr() as *const f64)
                };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    poly_same_cell! {
        eq_poly_same_n1: 1;
        eq_poly_same_n2: 2;
        eq_poly_same_n3: 3;
    }

    /// n-mismatch plane: npts differ, both sides answer false before any
    /// point is read (points literal zero).
    #[kani::proof]
    #[kani::unwind(5)]
    fn eq_poly_same_nmismatch() {
        let na: i32 = kani::any();
        let nb: i32 = kani::any();
        kani::assume(na >= 1 && na <= POLY_CAP as i32);
        kani::assume(nb >= 1 && nb <= POLY_CAP as i32);
        kani::assume(na != nb);
        let ia = poly_img(na, [0.0; 4], &[(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]);
        let ib = poly_img(nb, [0.0; 4], &[(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]);
        let r = expect_ok(call2v(adt_geo::builtins::fc_poly_same, ia.as_ptr(), ib.as_ptr()));
        let zeros = [0.0f64; 2 * POLY_CAP];
        let c = unsafe { pg_poly_same(na, nb, zeros.as_ptr(), zeros.as_ptr()) };
        assert!(r.as_bool() as c_int == c);
        assert!(c == 0); // spec pin: mismatched counts are never the same
    }

    // =================================================================
    // poly_box: stored boundbox copy, by-ref BOX result (mcx-stubs
    // recipe; "modulo static-buffer allocator model")
    // =================================================================

    /// Read the k-th f64 of a by-ref result image (wave-6 rule: datum
    /// image read-back does not trip pointer provenance).
    fn img_f64(d: Datum, k: usize) -> f64 {
        // SAFETY: by-ref result datum points at a live image in the stub
        // allocator's static buffer.
        unsafe {
            let p = (d.as_usize() as *const u8).add(8 * k);
            let mut b = [0u8; 8];
            core::ptr::copy_nonoverlapping(p, b.as_mut_ptr(), 8);
            f64::from_ne_bytes(b)
        }
    }

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_poly_box_img() {
        let bb = [any_f64(), any_f64(), any_f64(), any_f64()];
        let img = poly_img(1, bb, &[(0.0, 0.0)]);
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext3");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
        let r = (adt_geo::builtins::fc_poly_box)(None, &mut f);
        core::mem::forget(ctx);
        let mut cout = [0.0f64; 4];
        let cerr = unsafe { pg_poly_box(bb[0], bb[1], bb[2], bb[3], cout.as_mut_ptr()) };
        assert!(cerr == 0);
        let d = expect_ok(r);
        let mut k = 0;
        while k < 4 {
            assert!(img_f64(d, k).to_bits() == cout[k].to_bits());
            k += 1;
        }
    }

    // =================================================================
    // negative control: the varlena BUILDER is load-bearing. Rust reads
    // npts n, C is fed n+1 — the verdicts must diverge somewhere in the
    // domain, so this harness must FAIL (run with the DEFAULT solver).
    // =================================================================

    #[kani::proof]
    #[kani::unwind(5)]
    fn control_poly_npoints_skew() {
        let n: i32 = kani::any();
        kani::assume(n >= 0 && n < POLY_CAP as i32);
        let img = poly_img(n, [0.0; 4], &[]);
        let r = expect_ok(call1v(adt_geo::builtins::fc_poly_npoints, img.as_ptr()));
        let c = unsafe { pg_poly_npoints(n + 1) };
        assert!(r.as_i32() == c); // MUST FAIL: builder skew is visible
    }
}
