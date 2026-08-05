//! Kani C≡Rust equivalence — geo-cmp EXTENSION 4: the geo-wire lane
//! (binary send/recv, geo_ops.c @ REL_18_STABLE; C side = c/pg_geo_cmp.c
//! EXTENSION 4 section, bodies verbatim over a fixed PQ_BUF frame shim).
//!
//! Theorem shape (CORE-LEVEL, per the standing recv-wall ruling): the
//! recv direction constructs the StringInfo IN-HARNESS over a fully
//! symbolic exact-size wire frame and calls the shipped io::*_recv core
//! (the shipped fc wrapper is blocked by the pointer-datum recv wall,
//! rows stay qualified core-only); the send direction calls the shipped
//! io::*_send core and compares the COMPLETE returned bytea image
//! (4B varlena header + big-endian payload — fixed length, so this is
//! not a result-image wall) byte-for-byte against the C image. Both
//! directions ride the mcx-stubs recipe ("modulo static-buffer allocator
//! model"); wire byte order and the f64<->u64 bit pun are in-theorem on
//! both sides.
//!
//! Fences: recv frames are EXACT-size, so the insufficient-data ereport
//! arm is out of proof on both sides. line_recv's FPzero(A)&&FPzero(B)
//! 22P03 reject and circle_recv's radius<0 reject (NaN accepted) are
//! IN-theorem (errflag 5 <-> Err sqlstate 22P03, value-space only,
//! PgError::error stubbed). path_send/poly_send are per-n cells (n=1,2;
//! symbolic n would make the store offsets symbolic - the known image
//! width wall).
//!
//! Run: ./run-one.sh "ext4::proofs_ext4::<harness>" <timeout-s> [--solver kissat]

#[cfg(kani)]
mod proofs_ext4 {
    use adt_geo::{PathRef, PolyRef};
    use proof_support::{mcx_stubs, stubs};
    use types_core::geo::{Point, CIRCLE, LINE, LSEG};
    use types_error::{PgError, ERRCODE_INVALID_BINARY_REPRESENTATION};

    use std::os::raw::c_int;

    extern "C" {
        fn pg_point_recv_w(input: *const u8, ox: *mut f64, oy: *mut f64) -> c_int;
        fn pg_box_recv_w(input: *const u8, out4: *mut f64) -> c_int;
        fn pg_lseg_recv_w(input: *const u8, out4: *mut f64) -> c_int;
        fn pg_line_recv_w(input: *const u8, out3: *mut f64) -> c_int;
        fn pg_circle_recv_w(input: *const u8, out3: *mut f64) -> c_int;

        fn pg_point_send_w(x: f64, y: f64, out: *mut u8, olen: *mut c_int) -> c_int;
        fn pg_box_send_w(hx: f64, hy: f64, lx: f64, ly: f64, out: *mut u8, olen: *mut c_int) -> c_int;
        fn pg_lseg_send_w(x1: f64, y1: f64, x2: f64, y2: f64, out: *mut u8, olen: *mut c_int) -> c_int;
        #[allow(non_snake_case)]
        fn pg_line_send_w(A: f64, B: f64, C: f64, out: *mut u8, olen: *mut c_int) -> c_int;
        fn pg_circle_send_w(cx: f64, cy: f64, r: f64, out: *mut u8, olen: *mut c_int) -> c_int;
        fn pg_path_send_w(closed: c_int, npts: c_int, xy: *const f64, out: *mut u8, olen: *mut c_int) -> c_int;
        fn pg_poly_send_w(npts: c_int, xy: *const f64, out: *mut u8, olen: *mut c_int) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    /// In-harness StringInfo over an exact wire frame (recv direction).
    fn si_from<'m>(mcx: mcx::Mcx<'m>, frame: &[u8]) -> stringinfo::StringInfo<'m> {
        let mut si = match stringinfo::StringInfo::with_capacity_in(mcx, frame.len() + 1) {
            Ok(s) => s,
            Err(_) => panic!("stringinfo alloc failed"),
        };
        match si.append_bytes(frame) {
            Ok(()) => si,
            Err(_) => panic!("stringinfo append failed"),
        }
    }

    fn expect_ok<T>(r: Result<T, Box<PgError>>) -> T {
        match r {
            Ok(v) => v,
            Err(_) => panic!("infallible geo wire op errored"),
        }
    }

    // =================================================================
    // recv: exact symbolic frames, core-level
    // =================================================================

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_point_recv_core() {
        let frame: [u8; 16] = kani::any();
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
        let mut si = si_from(ctx.mcx(), &frame);
        let r = expect_ok(adt_geo::io::point_recv(&mut si));
        let (mut cx, mut cy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_point_recv_w(frame.as_ptr(), &mut cx, &mut cy) };
        assert!(cerr == 0);
        assert!(r.x.to_bits() == cx.to_bits());
        assert!(r.y.to_bits() == cy.to_bits());
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// box_recv: the float8_lt corner reorder (NaN-aware) is in-theorem.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_box_recv_core() {
        let frame: [u8; 32] = kani::any();
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
        let mut si = si_from(ctx.mcx(), &frame);
        let r = expect_ok(adt_geo::io::box_recv(&mut si));
        let mut c = [0.0f64; 4];
        let cerr = unsafe { pg_box_recv_w(frame.as_ptr(), c.as_mut_ptr()) };
        assert!(cerr == 0);
        assert!(r.high.x.to_bits() == c[0].to_bits());
        assert!(r.high.y.to_bits() == c[1].to_bits());
        assert!(r.low.x.to_bits() == c[2].to_bits());
        assert!(r.low.y.to_bits() == c[3].to_bits());
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_lseg_recv_core() {
        let frame: [u8; 32] = kani::any();
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
        let mut si = si_from(ctx.mcx(), &frame);
        let r: LSEG = expect_ok(adt_geo::io::lseg_recv(&mut si));
        let mut c = [0.0f64; 4];
        let cerr = unsafe { pg_lseg_recv_w(frame.as_ptr(), c.as_mut_ptr()) };
        assert!(cerr == 0);
        assert!(r.p[0].x.to_bits() == c[0].to_bits());
        assert!(r.p[0].y.to_bits() == c[1].to_bits());
        assert!(r.p[1].x.to_bits() == c[2].to_bits());
        assert!(r.p[1].y.to_bits() == c[3].to_bits());
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// line_recv: FPzero(A)&&FPzero(B) 22P03 reject arm in-theorem
    /// (value-space only; both arms cover-witnessed).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_line_recv_core() {
        let frame: [u8; 24] = kani::any();
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
        let mut si = si_from(ctx.mcx(), &frame);
        let r: Result<LINE, Box<PgError>> = adt_geo::io::line_recv(&mut si);
        let mut c = [0.0f64; 3];
        let cerr = unsafe { pg_line_recv_w(frame.as_ptr(), c.as_mut_ptr()) };
        match r {
            Ok(l) => {
                kani::cover!(true); // accept arm reachable
                assert!(cerr == 0);
                assert!(l.A.to_bits() == c[0].to_bits());
                assert!(l.B.to_bits() == c[1].to_bits());
                assert!(l.C.to_bits() == c[2].to_bits());
            }
            Err(e) => {
                kani::cover!(true); // reject arm reachable
                assert!(cerr == 5);
                assert!(e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// circle_recv: radius<0 reject (NaN radius ACCEPTED) in-theorem.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_circle_recv_core() {
        let frame: [u8; 24] = kani::any();
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
        let mut si = si_from(ctx.mcx(), &frame);
        let r: Result<CIRCLE, Box<PgError>> = adt_geo::io::circle_recv(&mut si);
        let mut c = [0.0f64; 3];
        let cerr = unsafe { pg_circle_recv_w(frame.as_ptr(), c.as_mut_ptr()) };
        match r {
            Ok(ci) => {
                kani::cover!(true);
                assert!(cerr == 0);
                assert!(ci.center.x.to_bits() == c[0].to_bits());
                assert!(ci.center.y.to_bits() == c[1].to_bits());
                assert!(ci.radius.to_bits() == c[2].to_bits());
            }
            Err(e) => {
                kani::cover!(true);
                assert!(cerr == 5);
                assert!(e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    // =================================================================
    // send: fixed-length full-image compare (header + BE payload)
    // =================================================================

    /// Compare the shipped Varlena image against the C image byte-for-byte.
    /// Chunked (u64-word + byte-tail) image compare: keeps every loop
    /// within the tight unwind(8) bound the mcx registry demands.
    fn assert_img(v: &datum::Varlena<'_>, cout: &[u8], colen: c_int) {
        let img = v.as_bytes();
        let n = img.len();
        assert!(n == colen as usize);
        let mut k = 0;
        while k + 8 <= n {
            let a = u64::from_ne_bytes(img[k..k + 8].try_into().unwrap());
            let b = u64::from_ne_bytes(cout[k..k + 8].try_into().unwrap());
            assert!(a == b);
            k += 8;
        }
        while k < n {
            assert!(img[k] == cout[k]);
            k += 1;
        }
    }

    macro_rules! send_op {
        ($($h:ident($n:literal): $core:ident($($arg:ident),*) / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                $(let $arg = any_f64();)*
                let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
                let val = build_val_
                    ::$core($($arg),*);
                let v = expect_ok(adt_geo::io::$core(ctx.mcx(), &val));
                let mut cout = [0u8; 64];
                let mut colen: c_int = 0;
                let cerr = unsafe { $pg($($arg,)* cout.as_mut_ptr(), &mut colen) };
                assert!(cerr == 0);
                assert_img(&v, &cout, colen);
                core::mem::forget(v);
                core::mem::forget(ctx);
            }
        )*};
    }

    /// Value constructors for the send macro (harness plumbing).
    mod build_val_ {
        use super::{Point, CIRCLE, LINE, LSEG};

        pub fn point_send(x: f64, y: f64) -> Point {
            Point { x, y }
        }
        pub fn box_send(hx: f64, hy: f64, lx: f64, ly: f64) -> types_core::geo::BOX {
            types_core::geo::BOX {
                high: Point { x: hx, y: hy },
                low: Point { x: lx, y: ly },
            }
        }
        pub fn lseg_send(x1: f64, y1: f64, x2: f64, y2: f64) -> LSEG {
            LSEG {
                p: [Point { x: x1, y: y1 }, Point { x: x2, y: y2 }],
            }
        }
        #[allow(non_snake_case)]
        pub fn line_send(A: f64, B: f64, C: f64) -> LINE {
            LINE { A, B, C }
        }
        pub fn circle_send(cx: f64, cy: f64, r: f64) -> CIRCLE {
            CIRCLE {
                center: Point { x: cx, y: cy },
                radius: r,
            }
        }
    }

    send_op! {
        eq_point_send_img(2): point_send(x, y) / pg_point_send_w;
        eq_box_send_img(4): box_send(hx, hy, lx, ly) / pg_box_send_w;
        eq_lseg_send_img(4): lseg_send(x1, y1, x2, y2) / pg_lseg_send_w;
        eq_line_send_img(3): line_send(a, b, c) / pg_line_send_w;
        eq_circle_send_img(3): circle_send(cx, cy, r) / pg_circle_send_w;
    }

    // =================================================================
    // path_send / poly_send: per-n cells over harness-built varlena
    // images (trusted-builder fence, as ext3)
    // =================================================================

    const PATH_BUF: usize = 4 + 12 + 16 * 2;
    const POLY_BUF: usize = 4 + 36 + 16 * 2;

    fn path_img2(npts: i32, closed: i32, pts: &[(f64, f64)]) -> [u8; PATH_BUF] {
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

    fn poly_img2(npts: i32, pts: &[(f64, f64)]) -> [u8; POLY_BUF] {
        let mut b = [0u8; POLY_BUF];
        let total = (40 + 16 * npts as usize) as u32;
        b[0..4].copy_from_slice(&(total << 2).to_ne_bytes());
        b[4..8].copy_from_slice(&npts.to_ne_bytes());
        // boundbox literal zero: not read by poly_send
        let mut i = 0;
        while i < pts.len() {
            let off = 40 + 16 * i;
            b[off..off + 8].copy_from_slice(&pts[i].0.to_ne_bytes());
            b[off + 8..off + 16].copy_from_slice(&pts[i].1.to_ne_bytes());
            i += 1;
        }
        b
    }

    macro_rules! path_send_cell {
        ($($h:ident: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const N: usize = $n;
                let closed: i32 = kani::any();
                let mut pts = [(0.0f64, 0.0f64); N];
                let mut i = 0;
                while i < N {
                    pts[i] = (any_f64(), any_f64());
                    i += 1;
                }
                let img = path_img2(N as i32, closed, &pts);
                let pref = PathRef::from_payload(&img[4..4 + 12 + 16 * N]);
                let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
                let v = expect_ok(adt_geo::io::path_send(ctx.mcx(), &pref));
                let flat = {
                    let mut f = [0.0f64; 2 * N];
                    let mut i = 0;
                    while i < N {
                        f[2 * i] = pts[i].0;
                        f[2 * i + 1] = pts[i].1;
                        i += 1;
                    }
                    f
                };
                let mut cout = [0u8; 64];
                let mut colen: c_int = 0;
                let cerr = unsafe {
                    pg_path_send_w(closed, N as c_int, flat.as_ptr(), cout.as_mut_ptr(), &mut colen)
                };
                assert!(cerr == 0);
                assert_img(&v, &cout, colen);
                core::mem::forget(v);
                core::mem::forget(ctx);
            }
        )*};
    }

    path_send_cell! {
        eq_path_send_n1: 1;
        eq_path_send_n2: 2;
    }

    macro_rules! poly_send_cell {
        ($($h:ident: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const N: usize = $n;
                let mut pts = [(0.0f64, 0.0f64); N];
                let mut i = 0;
                while i < N {
                    pts[i] = (any_f64(), any_f64());
                    i += 1;
                }
                let img = poly_img2(N as i32, &pts);
                let pref = PolyRef::from_payload(&img[4..4 + 36 + 16 * N]);
                let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
                let v = expect_ok(adt_geo::io::poly_send(ctx.mcx(), &pref));
                let flat = {
                    let mut f = [0.0f64; 2 * N];
                    let mut i = 0;
                    while i < N {
                        f[2 * i] = pts[i].0;
                        f[2 * i + 1] = pts[i].1;
                        i += 1;
                    }
                    f
                };
                let mut cout = [0u8; 64];
                let mut colen: c_int = 0;
                let cerr = unsafe {
                    pg_poly_send_w(N as c_int, flat.as_ptr(), cout.as_mut_ptr(), &mut colen)
                };
                assert!(cerr == 0);
                assert_img(&v, &cout, colen);
                core::mem::forget(v);
                core::mem::forget(ctx);
            }
        )*};
    }

    poly_send_cell! {
        eq_poly_send_n1: 1;
        eq_poly_send_n2: 2;
    }

    // =================================================================
    // negative control: wire byte ORDER is load-bearing — feed the C
    // side a byte-swapped frame; the equality must FAIL (DEFAULT solver)
    // =================================================================

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_point_recv_byteorder_skew() {
        let frame: [u8; 16] = kani::any();
        let mut swapped = frame;
        swapped[0..8].reverse();
        swapped[8..16].reverse();
        let ctx = mcx::MemoryContext::new_bump("kani-geo-ext4");
        let mut si = si_from(ctx.mcx(), &frame);
        let r = expect_ok(adt_geo::io::point_recv(&mut si));
        let (mut cx, mut cy) = (0.0f64, 0.0f64);
        let cerr = unsafe { pg_point_recv_w(swapped.as_ptr(), &mut cx, &mut cy) };
        assert!(cerr == 0);
        assert!(r.x.to_bits() == cx.to_bits()); // MUST FAIL
        core::mem::forget(si);
        core::mem::forget(ctx);
    }
}
