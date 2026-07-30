//! Kani C≡Rust equivalence: GiST support procedures for 2-D geometric types
//! (gistproc.c — the gist-geo family riding the proved geo-cmp comparator
//! corpus).  Ledger rows: 2578 gist_box_consistent, 2581 gist_box_penalty,
//! 2583 gist_box_union, 2584 gist_box_same, 1030 gist_point_compress,
//! 3282 gist_point_fetch, 2179 gist_point_consistent, 3064
//! gist_point_distance, 3998 gist_box_distance, 3280 gist_circle_distance,
//! 3288 gist_poly_distance, 2585 gist_poly_consistent, 2586
//! gist_poly_compress, 2591 gist_circle_consistent, 2592
//! gist_circle_compress, 6347 gist_translate_cmptype_common.
//!
//! Rust side: the SHIPPED fmgr wrappers, reached through the shipped
//! `gistproc::GISTPROC_BUILTINS` dispatch table by CONSTANT index with an
//! oid/name assert (table wiring is in-theorem without a scan loop), invoked
//! on real `LocalFcinfo` frames.  GISTENTRY inputs are stack structs whose
//! `key` datum points at 32-byte BOX / 16-byte Point / 24-byte CIRCLE /
//! polygon-varlena images, so the gist fmgr protocol (entry deref, by-ref
//! key decode, recheck/result out-params, strategy datum unwrap) is inside
//! every theorem.  By-ref RESULTS (union / compress / fetch) use the
//! mcx-stubs + token-ctx + set_result_mcx recipe (wave-6/cash precedent);
//! the returned datum's image is read back and compared byte-for-byte —
//! "modulo static-buffer allocator model (allocation strategy out of
//! scope)".
//!
//! C side: c/pg_gist_geo.c — gistproc.c/gistutil.c bodies verbatim @
//! REL_18_STABLE over the geo_ops.c/float.h/geo_decls.h helper corpus
//! already vendored by proofs/geo-cmp; ereports -> pg_geo_errflag; NAN
//! macro pinned to the canonical quiet NaN (point_dt -> pg_hypot reaches
//! get_float8_nan(); measured CBMC header-constant defect).
//!
//! Domain regimes (per the geo cost laws in proofs/TRIAGE.md):
//!   - Consistent lattices (box/point/poly/circle): FULLY SYMBOLIC f64
//!     coordinates via from_bits + FULLY SYMBOLIC strategy u16 — the
//!     strategy-number switch, the RTOldBelow/RTOldAbove remaps, the
//!     leaf/internal split and the unrecognized-strategy error arm are all
//!     in-theorem.  gist_point_consistent is fenced away from strategy
//!     groups 2/3 (polygon/circle arms: varlena detoast + exact leaf
//!     containment — ladder follow-up per ledger row 2179); the C shim
//!     traps those groups with flag 99 so plane vacuity fails LOUDLY.
//!   - box_penalty: size_box contains a symbolic×symbolic 53-bit multiply
//!     (float-arith wall law).  Treatment: full-symbolic probe recorded
//!     honestly, a degenerate-height plane (LITERAL 0.0 y coordinates —
//!     assumed zeros do not fold), and a one-symbolic-index concrete pair
//!     GRID covering the zero-size / NaN-high / NaN-low / Inf / overflow /
//!     underflow / f64->f32 rounding cells.
//!   - Distances: computeDistance's in-box and strip arms are compare +
//!     float8_mi (fast class, fully symbolic under arm fences); the leaf
//!     arm holds ONE point_dt and is proved on the axis-aligned slice
//!     (shared symbolic y -> sqrt-free hypot paths; the general hypot path
//!     is a known model-spec gap and stays out of every theorem); the
//!     vertex arm holds FOUR point_dt calls -> one-symbolic-index concrete
//!     grid only (dual-point_dt cost law).
//!   - Union / compress / fetch: pure select/copy circuits + mcx image
//!     results; union is split per-n (2/3) — symbolic entry counts make
//!     builder addresses symbolic (jsonb law).
//!
//! SCREENED DIVERGENCE PLANE (adjudication at solve time, do NOT record
//! green over it): C gist_box_same compares with NaN-AWARE float8_eq
//! (NaN == NaN -> true); shipped fc_gist_box_same uses raw f64 `==`
//! (crates/backend/access/gist/gistproc/src/lib.rs:238-247).  A NaN
//! coordinate shared by both keys is C-equal but Rust-unequal.
//! probe_gist_box_same_nan_plane is the concrete expected-FAIL witness;
//! eq_gist_box_same is fenced non-NaN and claims only that plane.
//!
//! Fallible harnesses stub types_error::PgError::error (field-identical
//! minus Location/message text — value-space only) and std::fmt::format
//! (the unrecognized-strategy message), and mem::forget the Err payload
//! (Box<PgError> drop glue is a measured symex wall).  Verdict parity is
//! asserted against pg_geo_errflag; sqlstate 22003 parity on the float
//! error arms (flags 1/2); strategy/inconsistent elogs (flags 9/8) are
//! verdict-only (C elog carries no sqlstate distinct from XX000).
//!
//! Run (ZERO solves in this pre-build lane — see runqueue.txt):
//!   cd proofs/gist-geo && timeout 75 cargo kani -Z c-ffi -Z stubbing \
//!     --c-lib c/pg_gist_geo.c --no-overflow-checks --solver kissat \
//!     --harness proofs::<h> --exact
//! (--no-overflow-checks: float-family law — Kani's NaN-production check
//! fires on legal IEEE arithmetic.  Controls and the NaN-plane witness run
//! with the DEFAULT solver: kissat never terminates on failing harnesses.)

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use types_core::geo::{Point, BOX, CIRCLE};
    use types_error::{PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
    use types_fmgr::{LocalFcinfo, PGFunction};
    use types_gist::{GistEntryVector, GISTENTRY};

    use std::os::raw::c_int;

    extern "C" {
        fn pg_gist_box_consistent(
            key_isnull: c_int,
            khx: f64, khy: f64, klx: f64, kly: f64,
            query_isnull: c_int,
            qhx: f64, qhy: f64, qlx: f64, qly: f64,
            strategy: u16, page_is_leaf: c_int,
            recheck: *mut c_int, result: *mut c_int,
        ) -> c_int;
        fn pg_gist_box_union(
            numranges: c_int, coords: *const f64, sizep: *mut c_int,
            out: *mut f64,
        ) -> c_int;
        fn pg_gist_box_penalty(
            ohx: f64, ohy: f64, olx: f64, oly: f64,
            nhx: f64, nhy: f64, nlx: f64, nly: f64,
            result: *mut f32,
        ) -> c_int;
        fn pg_gist_box_same(
            b1_isnull: c_int,
            ahx: f64, ahy: f64, alx: f64, aly: f64,
            b2_isnull: c_int,
            bhx: f64, bhy: f64, blx: f64, bly: f64,
            result: *mut c_int,
        ) -> c_int;
        fn pg_gist_point_compress_leaf(px: f64, py: f64, out_box: *mut f64) -> c_int;
        fn pg_gist_point_fetch(
            khx: f64, khy: f64, klx: f64, kly: f64, out_pt: *mut f64,
        ) -> c_int;
        fn pg_gist_point_consistent(
            strategy: u16, page_is_leaf: c_int,
            khx: f64, khy: f64, klx: f64, kly: f64,
            qx: f64, qy: f64,
            qbhx: f64, qbhy: f64, qblx: f64, qbly: f64,
            recheck: *mut c_int, result: *mut c_int,
        ) -> c_int;
        fn pg_gist_point_distance(
            strategy: u16, page_is_leaf: c_int,
            khx: f64, khy: f64, klx: f64, kly: f64,
            qx: f64, qy: f64, dist: *mut f64,
        ) -> c_int;
        fn pg_gist_box_distance(
            strategy: u16,
            khx: f64, khy: f64, klx: f64, kly: f64,
            qx: f64, qy: f64, dist: *mut f64,
        ) -> c_int;
        fn pg_gist_circle_distance(
            strategy: u16,
            khx: f64, khy: f64, klx: f64, kly: f64,
            qx: f64, qy: f64, recheck: *mut c_int, dist: *mut f64,
        ) -> c_int;
        fn pg_gist_poly_distance(
            strategy: u16,
            khx: f64, khy: f64, klx: f64, kly: f64,
            qx: f64, qy: f64, recheck: *mut c_int, dist: *mut f64,
        ) -> c_int;
        fn pg_gist_poly_consistent(
            key_isnull: c_int,
            khx: f64, khy: f64, klx: f64, kly: f64,
            query_isnull: c_int,
            bbhx: f64, bbhy: f64, bblx: f64, bbly: f64,
            strategy: u16, recheck: *mut c_int, result: *mut c_int,
        ) -> c_int;
        fn pg_gist_circle_consistent(
            key_isnull: c_int,
            khx: f64, khy: f64, klx: f64, kly: f64,
            query_isnull: c_int,
            cx: f64, cy: f64, r: f64,
            strategy: u16, recheck: *mut c_int, result: *mut c_int,
        ) -> c_int;
        fn pg_gist_circle_compress_leaf(cx: f64, cy: f64, r: f64, out_box: *mut f64) -> c_int;
        fn pg_gist_poly_compress_leaf(
            bbhx: f64, bbhy: f64, bblx: f64, bbly: f64, out_box: *mut f64,
        ) -> c_int;
        fn pg_gist_translate_cmptype_common(cmptype: i32) -> c_int;
    }

    // ---------- shared rig ----------

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

    /// Shipped dispatch-table lookup by CONSTANT index (no scan loop) with
    /// the oid AND name wiring asserted in-theorem.
    fn fcn(idx: usize, oid: u32, name: &str) -> PGFunction {
        let b = &gistproc::GISTPROC_BUILTINS[idx];
        assert!(b.foid == oid);
        assert!(b.name.as_bytes() == name.as_bytes());
        b.func
    }

    /// Token Mcx handle (cash/jsonb-probe recipe).  SOUNDNESS: with
    /// Mcx::allocate stubbed to the static proof heap, no path under proof
    /// dereferences the context image — a REAL MemoryContext::new_bump is
    /// pure scaffolding and a measured symex wall.  The zeroed image is
    /// never read.
    fn token_ctx() -> &'static mcx::MemoryContext {
        static CTX: [u8; 256] = [0u8; 256];
        assert!(core::mem::size_of::<mcx::MemoryContext>() <= 256);
        unsafe { &*(CTX.as_ptr() as *const mcx::MemoryContext) }
    }

    /// Build a LocalFcinfo frame and run a shipped wrapper.  `arm_mcx`
    /// arms the result-ownership context (needed whenever the wrapper's
    /// path calls fcinfo.result_mcx(): union/compress/fetch results, and
    /// poly_at's detoast entry point).
    fn call<const N: usize>(f: PGFunction, args: [Datum; N], arm_mcx: bool) -> PgResult<Datum> {
        let mut fc = LocalFcinfo::<N>::new(0);
        if arm_mcx {
            // SAFETY: the token context is a static; it outlives the call.
            unsafe { fc.set_result_mcx(token_ctx().mcx()) };
        }
        let mut i = 0;
        while i < N {
            fc.args[i] = NullableDatum::value(args[i]);
            i += 1;
        }
        f(None, &mut fc)
    }

    fn entry(key: Datum, offset: u16, leafkey: bool, page_is_leaf: bool) -> GISTENTRY {
        GISTENTRY {
            key,
            offset,
            leafkey,
            page_is_leaf,
            rel_natts: 0,
        }
    }

    fn dp<T>(p: *const T) -> Datum {
        Datum::from_usize(p as usize)
    }

    /// Out-param pointer datum (the callee writes through it).
    fn dpm<T>(p: *mut T) -> Datum {
        Datum::from_usize(p as usize)
    }

    /// Adjudicate a fallible Rust arm against the C err flag.  Flags 1/2 are
    /// the 22003 float overflow/underflow arms (sqlstate asserted); flags
    /// 8/9 are elog(ERROR) arms (verdict-only — value-space claim); flag 99
    /// is the C-side out-of-plane trap and FAILS the harness loudly.
    /// Err payload is forgotten (Box<PgError> drop glue = measured wall).
    fn adjudicate<T>(r: Result<T, Box<types_error::PgError>>, cerr: c_int) -> Option<T> {
        assert!(cerr != 99); // fenced-out plane reached: harness defect
        match r {
            Ok(v) => {
                kani::cover!(true); // vacuity witness: Ok arm explored
                assert!(cerr == 0);
                Some(v)
            }
            Err(e) => {
                kani::cover!(true); // vacuity witness: Err arm explored
                assert!(cerr != 0);
                if cerr == 1 || cerr == 2 {
                    assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                }
                core::mem::forget(e);
                None
            }
        }
    }

    fn expect_ok<T>(r: Result<T, Box<types_error::PgError>>, cerr: c_int) -> T {
        assert!(cerr == 0);
        match r {
            Ok(v) => v,
            Err(_) => panic!("infallible gist support fn errored"),
        }
    }

    /// Read a 32-byte BOX image back from a by-ref result datum (through-
    /// datum image reads do not trip pointer provenance — wave-6 law).
    unsafe fn box_image_at(d: Datum) -> BOX {
        BOX::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 32))
    }

    unsafe fn point_image_at(d: Datum) -> Point {
        Point::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 16))
    }

    // Polygon varlena image: 4-byte uncompressed header | npts=1 | boundbox
    // | one point.  Unaligned-safe on the Rust side (PolyRef reader
    // contract); total 56 bytes.
    fn poly_image(bb: &BOX, p: &Point) -> [u8; 56] {
        let mut img = [0u8; 56];
        img[0..4].copy_from_slice(&(56u32 << 2).to_ne_bytes()); // SET_VARSIZE 4B-u
        img[4..8].copy_from_slice(&1i32.to_ne_bytes()); // npts
        img[8..40].copy_from_slice(&bb.to_datum_bytes());
        img[40..56].copy_from_slice(&p.to_datum_bytes());
        img
    }

    const IDX_POINT_COMPRESS: usize = 1; // oid 1030
    const IDX_POINT_CONSISTENT: usize = 2; // oid 2179
    const IDX_BOX_CONSISTENT: usize = 3; // oid 2578
    const IDX_BOX_PENALTY: usize = 4; // oid 2581
    const IDX_BOX_UNION: usize = 6; // oid 2583
    const IDX_BOX_SAME: usize = 7; // oid 2584
    const IDX_POLY_CONSISTENT: usize = 8; // oid 2585
    const IDX_POLY_COMPRESS: usize = 9; // oid 2586
    const IDX_CIRCLE_CONSISTENT: usize = 10; // oid 2591
    const IDX_CIRCLE_COMPRESS: usize = 11; // oid 2592
    const IDX_POINT_DISTANCE: usize = 12; // oid 3064
    const IDX_CIRCLE_DISTANCE: usize = 13; // oid 3280
    const IDX_POINT_FETCH: usize = 14; // oid 3282
    const IDX_POLY_DISTANCE: usize = 15; // oid 3288
    const IDX_BOX_DISTANCE: usize = 17; // oid 3998
    const IDX_TRANSLATE: usize = 18; // oid 6347

    /// Run gist_box_consistent through the shipped wrapper (5-arg gist
    /// consistent protocol frame).
    fn rust_box_consistent(
        key: Option<&BOX>,
        query: Option<&BOX>,
        strategy: u16,
        page_is_leaf: bool,
        recheck: &mut bool,
    ) -> PgResult<bool> {
        let f = fcn(IDX_BOX_CONSISTENT, 2578, "gist_box_consistent");
        let kimg = key.map(|b| b.to_datum_bytes());
        let qimg = query.map(|b| b.to_datum_bytes());
        let e = entry(
            kimg.as_ref().map_or(Datum::from_usize(0), |i| dp(i.as_ptr())),
            0,
            false,
            page_is_leaf,
        );
        let r = call(
            f,
            [
                dp(&e as *const GISTENTRY),
                qimg.as_ref().map_or(Datum::from_usize(0), |i| dp(i.as_ptr())),
                Datum::from_u16(strategy),
                Datum::from_u16(0), // subtype, never read
                dpm(recheck as *mut bool),
            ],
            false,
        );
        r.map(Datum::as_bool)
    }

    // =================================================================
    // gist_box_consistent (oid 2578): 12-strategy lattice, wrapper-level
    // =================================================================

    /// Leaf arm: fully symbolic key/query boxes AND strategy; the
    /// unrecognized-strategy error arm is in-theorem (verdict parity).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_box_consistent_leaf() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let strategy: u16 = kani::any();
        let mut recheck = true; // wrapper must clear it
        let r = rust_box_consistent(Some(&k), Some(&q), strategy, true, &mut recheck);
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_box_consistent(
                0, k.high.x, k.high.y, k.low.x, k.low.y,
                0, q.high.x, q.high.y, q.low.x, q.low.y,
                strategy, 1, &mut crech, &mut cres,
            )
        };
        assert!(recheck as c_int == crech);
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    /// Internal arm: same domain through rtree_internal_consistent.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_box_consistent_internal() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let strategy: u16 = kani::any();
        let mut recheck = true;
        let r = rust_box_consistent(Some(&k), Some(&q), strategy, false, &mut recheck);
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_box_consistent(
                0, k.high.x, k.high.y, k.low.x, k.low.y,
                0, q.high.x, q.high.y, q.low.x, q.low.y,
                strategy, 0, &mut crech, &mut cres,
            )
        };
        assert!(recheck as c_int == crech);
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    /// NULL lattice: key and/or query null -> false before the strategy
    /// switch (strategy fully symbolic and never dispatched).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_box_consistent_nulls() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let (knull, qnull): (bool, bool) = (kani::any(), kani::any());
        kani::assume(knull || qnull);
        let strategy: u16 = kani::any();
        let leaf: bool = kani::any();
        let mut recheck = true;
        let r = rust_box_consistent(
            (!knull).then_some(&k),
            (!qnull).then_some(&q),
            strategy,
            leaf,
            &mut recheck,
        );
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_box_consistent(
                knull as c_int, k.high.x, k.high.y, k.low.x, k.low.y,
                qnull as c_int, q.high.x, q.high.y, q.low.x, q.low.y,
                strategy, leaf as c_int, &mut crech, &mut cres,
            )
        };
        assert!(recheck as c_int == crech);
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    /// NEGATIVE CONTROL (MUST FAIL, default solver): shipped LEAF arm wired
    /// against the C INTERNAL arm at RTLeft — the leaf/internal split must
    /// be load-bearing in-theorem (e.g. key.high.x < query.low.x - eps but
    /// key.low.x >= query.low.x - eps).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_gist_box_consistent_leaf_vs_internal() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let mut recheck = true;
        let r = rust_box_consistent(Some(&k), Some(&q), 1, true, &mut recheck);
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_box_consistent(
                0, k.high.x, k.high.y, k.low.x, k.low.y,
                0, q.high.x, q.high.y, q.low.x, q.low.y,
                1, /* page_is_leaf = */ 0, &mut crech, &mut cres,
            )
        };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres); // must fail
        }
    }

    // =================================================================
    // gist_box_same (oid 2584)
    // =================================================================

    fn rust_box_same(b1: Option<&BOX>, b2: Option<&BOX>) -> (bool, bool) {
        let f = fcn(IDX_BOX_SAME, 2584, "gist_box_same");
        let i1 = b1.map(|b| b.to_datum_bytes());
        let i2 = b2.map(|b| b.to_datum_bytes());
        let mut result = true;
        let rptr = dp(&mut result as *mut bool);
        let d = match call(
            f,
            [
                i1.as_ref().map_or(Datum::from_usize(0), |i| dp(i.as_ptr())),
                i2.as_ref().map_or(Datum::from_usize(0), |i| dp(i.as_ptr())),
                rptr,
            ],
            false,
        ) {
            Ok(d) => d,
            Err(_) => panic!("infallible gist_box_same errored"),
        };
        (result, d.as_usize() == rptr.as_usize())
    }

    /// Non-NaN plane: exact-equality lattice, result out-param + returned
    /// pointer datum parity.  (The NaN plane is a screened divergence — see
    /// module doc and the probe below.)
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_gist_box_same_nonnan() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        kani::assume(
            !k.high.x.is_nan() && !k.high.y.is_nan() && !k.low.x.is_nan() && !k.low.y.is_nan(),
        );
        kani::assume(
            !q.high.x.is_nan() && !q.high.y.is_nan() && !q.low.x.is_nan() && !q.low.y.is_nan(),
        );
        let (r, ret_is_arg2) = rust_box_same(Some(&k), Some(&q));
        let mut cres: c_int = 0;
        let cerr = unsafe {
            pg_gist_box_same(
                0, k.high.x, k.high.y, k.low.x, k.low.y,
                0, q.high.x, q.high.y, q.low.x, q.low.y,
                &mut cres,
            )
        };
        assert!(cerr == 0);
        assert!(ret_is_arg2);
        assert!(r as c_int == cres);
    }

    /// NULL lattice: (None, None) -> true, one-sided -> false.
    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_gist_box_same_nulls() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let (n1, n2): (bool, bool) = (kani::any(), kani::any());
        kani::assume(n1 || n2);
        let (r, ret_is_arg2) = rust_box_same((!n1).then_some(&k), (!n2).then_some(&q));
        let mut cres: c_int = 0;
        let cerr = unsafe {
            pg_gist_box_same(
                n1 as c_int, k.high.x, k.high.y, k.low.x, k.low.y,
                n2 as c_int, q.high.x, q.high.y, q.low.x, q.low.y,
                &mut cres,
            )
        };
        assert!(cerr == 0);
        assert!(ret_is_arg2);
        assert!(r as c_int == cres);
    }

    /// DIVERGENCE WITNESS (EXPECTED FAIL, default solver): concrete NaN
    /// coordinate in both keys — C float8_eq says equal (NaN == NaN), the
    /// shipped raw f64 `==` says unequal.  Decodable single-path
    /// counterexample; adjudication owed before any fix.
    #[kani::proof]
    #[kani::unwind(34)]
    fn probe_gist_box_same_nan_plane() {
        let k = bx(1.0, 2.0, f64::NAN, 0.0);
        let q = bx(1.0, 2.0, f64::NAN, 0.0);
        let (r, _) = rust_box_same(Some(&k), Some(&q));
        let mut cres: c_int = 0;
        let cerr = unsafe {
            pg_gist_box_same(
                0, k.high.x, k.high.y, k.low.x, k.low.y,
                0, q.high.x, q.high.y, q.low.x, q.low.y,
                &mut cres,
            )
        };
        assert!(cerr == 0);
        assert!(r as c_int == cres); // must fail: C true, Rust false
    }

    // =================================================================
    // gist_box_penalty (oid 2581): 53-bit multiply -> probe + plane + grid
    // =================================================================

    /// Shipped-wrapper penalty call; returns (f32 result, returned datum ==
    /// arg2 pointer) on the Ok arm.
    fn rust_box_penalty(orig: &BOX, new: &BOX) -> PgResult<(f32, bool)> {
        let f = fcn(IDX_BOX_PENALTY, 2581, "gist_box_penalty");
        let io = orig.to_datum_bytes();
        let inw = new.to_datum_bytes();
        let eo = entry(dp(io.as_ptr()), 0, false, false);
        let en = entry(dp(inw.as_ptr()), 0, false, false);
        let mut result: f32 = 0.0;
        let rptr = dp(&mut result as *mut f32);
        let d = call(
            f,
            [dp(&eo as *const GISTENTRY), dp(&en as *const GISTENTRY), rptr],
            false,
        )?;
        Ok((result, d.as_usize() == rptr.as_usize()))
    }

    /// Full-symbolic probe: symbolic×symbolic 53-bit multiply in size_box —
    /// expected WALL per the float-arith cost law; run to record honestly.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn probe_gist_box_penalty_full() {
        let o = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let n = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let r = rust_box_penalty(&o, &n);
        let mut cres: f32 = 0.0;
        let cerr = unsafe {
            pg_gist_box_penalty(
                o.high.x, o.high.y, o.low.x, o.low.y,
                n.high.x, n.high.y, n.low.x, n.low.y,
                &mut cres,
            )
        };
        if let Some((v, ret_ok)) = adjudicate(r, cerr) {
            assert!(ret_ok);
            assert!(v.to_bits() == cres.to_bits());
        }
    }

    /// Degenerate-height plane: all four y coordinates LITERAL 0.0 (assumed
    /// zeros do not fold — I128 rule), so both size_box calls take the
    /// zero-size early return; the x compares of rt_box_union/size_box stay
    /// fully symbolic.  Penalty is 0.0 - 0.0 on every path.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_box_penalty_degenerate_y() {
        let o = bx(any_f64(), 0.0, any_f64(), 0.0);
        let n = bx(any_f64(), 0.0, any_f64(), 0.0);
        let r = rust_box_penalty(&o, &n);
        let mut cres: f32 = 0.0;
        let cerr = unsafe {
            pg_gist_box_penalty(
                o.high.x, 0.0, o.low.x, 0.0,
                n.high.x, 0.0, n.low.x, 0.0,
                &mut cres,
            )
        };
        if let Some((v, ret_ok)) = adjudicate(r, cerr) {
            assert!(ret_ok);
            assert!(v.to_bits() == cres.to_bits());
        }
    }

    // Concrete boundary cells as (hx, hy, lx, ly): ordinary sizes, the
    // zero-size early return, NaN in high (Inf-size arm) and in low
    // (NaN-aware float8_le screen), Inf spans, a multiply-OVERFLOW cell
    // (finite coords, size -> Inf = error), a multiply-UNDERFLOW cell, and
    // an f32-rounding cell (penalty not representable in f32).
    const PGRID: [[f64; 4]; 10] = [
        [1.0, 1.0, 0.0, 0.0],                        // area 1
        [3.0, 2.0, 1.0, 1.0],                        // area 2
        [0.0, 5.0, 0.0, 0.0],                        // zero width -> size 0
        [1.0, -1.0, 0.0, 0.0],                       // high.y <= low.y -> size 0
        [f64::NAN, 1.0, 0.0, 0.0],                   // NaN high -> size +Inf
        [1.0, 1.0, f64::NAN, 0.0],                   // NaN low (le screen arm)
        [f64::INFINITY, 1.0, 0.0, 0.0],              // Inf span, no error
        [1.0e300, 1.0e300, -1.0e300, -1.0e300],      // mul overflow -> error
        [1.0e-200, 1.0e-200, 0.0, 0.0],              // mul underflow -> error
        [1.0e-30, 1.0 + 1.0e-9, 0.0, 1.0],           // f32 downcast rounding
    ];

    /// One-symbolic-index pair grid (never a loop through the wrapper —
    /// measured superlinear symex growth): universally quantified over all
    /// 100 (orig, new) cells; multiply operands are 10-valued.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn grid_gist_box_penalty() {
        let i: usize = kani::any();
        let j: usize = kani::any();
        kani::assume(i < PGRID.len() && j < PGRID.len());
        let a = PGRID[i];
        let b = PGRID[j];
        let o = bx(a[0], a[1], a[2], a[3]);
        let n = bx(b[0], b[1], b[2], b[3]);
        let r = rust_box_penalty(&o, &n);
        let mut cres: f32 = 0.0;
        let cerr = unsafe {
            pg_gist_box_penalty(a[0], a[1], a[2], a[3], b[0], b[1], b[2], b[3], &mut cres)
        };
        if let Some((v, ret_ok)) = adjudicate(r, cerr) {
            assert!(ret_ok);
            assert!(v.to_bits() == cres.to_bits());
        }
    }

    // =================================================================
    // gist_box_union (oid 2583): per-n split (2/3), mcx image result
    // =================================================================

    // adjust_box fold over n symbolic boxes; result image read back from
    // the static-buffer allocation ("modulo static-buffer allocator model").
    macro_rules! union_harness {
        ($($h:ident: n=$n:literal, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const N: usize = $n;
                let mut coords = [0.0f64; 4 * N];
                for c in coords.iter_mut() {
                    *c = any_f64();
                }
                let mut images = [[0u8; 32]; N];
                for i in 0..N {
                    images[i] = bx(
                        coords[4 * i],
                        coords[4 * i + 1],
                        coords[4 * i + 2],
                        coords[4 * i + 3],
                    )
                    .to_datum_bytes();
                }
                let mut vector = Vec::with_capacity(N);
                for i in 0..N {
                    vector.push(entry(dp(images[i].as_ptr()), 0, false, false));
                }
                let ev = GistEntryVector {
                    n: N as i32,
                    vector,
                };
                let mut size: i32 = 0;
                let sptr = dp(&mut size as *mut i32);
                let f = fcn(IDX_BOX_UNION, 2583, "gist_box_union");
                let d = expect_ok(
                    call(f, [dp(&ev as *const GistEntryVector), sptr], true),
                    0,
                );
                let rbox = unsafe { box_image_at(d) };

                let mut csize: c_int = 0;
                let mut cout = [0.0f64; 4];
                let cerr = unsafe {
                    pg_gist_box_union(N as c_int, coords.as_ptr(), &mut csize, cout.as_mut_ptr())
                };
                assert!(cerr == 0);
                assert!(size == csize);
                assert!(size == 32);
                assert!(rbox.high.x.to_bits() == cout[0].to_bits());
                assert!(rbox.high.y.to_bits() == cout[1].to_bits());
                assert!(rbox.low.x.to_bits() == cout[2].to_bits());
                assert!(rbox.low.y.to_bits() == cout[3].to_bits());
            }
        )*};
    }

    union_harness! {
        eq_gist_box_union_n2: n=2, unwind=72;
        eq_gist_box_union_n3: n=3, unwind=72;
    }

    // =================================================================
    // gist_point_compress (oid 1030) / gist_point_fetch (oid 3282)
    // =================================================================

    /// Leaf arm: point key -> degenerate box (high == low) + fresh GISTENTRY
    /// image in the result mcx; entry fields and the 32-byte box image are
    /// compared ("modulo static-buffer allocator model").
    #[kani::proof]
    #[kani::unwind(72)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_compress_leaf() {
        let p = pt(any_f64(), any_f64());
        let offset: u16 = kani::any();
        let page_is_leaf: bool = kani::any();
        let img = p.to_datum_bytes();
        let e = entry(dp(img.as_ptr()), offset, true, page_is_leaf);
        let f = fcn(IDX_POINT_COMPRESS, 1030, "gist_point_compress");
        let d = expect_ok(call(f, [dp(&e as *const GISTENTRY)], true), 0);
        // SAFETY: result datum points at a GISTENTRY image in the stub heap.
        let re = unsafe { &*(d.as_usize() as *const GISTENTRY) };
        assert!(re.offset == offset);
        assert!(!re.leafkey);
        assert!(re.page_is_leaf == page_is_leaf);
        let rbox = unsafe { box_image_at(re.key) };

        let mut cout = [0.0f64; 4];
        let cerr = unsafe { pg_gist_point_compress_leaf(p.x, p.y, cout.as_mut_ptr()) };
        assert!(cerr == 0);
        assert!(rbox.high.x.to_bits() == cout[0].to_bits());
        assert!(rbox.high.y.to_bits() == cout[1].to_bits());
        assert!(rbox.low.x.to_bits() == cout[2].to_bits());
        assert!(rbox.low.y.to_bits() == cout[3].to_bits());
    }

    /// No-op arm (leafkey = false): both sides return the input entry
    /// unchanged (C: `retval = entry` — identity in the C body); the theorem
    /// is the shipped identity, pointer-exact.
    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_gist_point_compress_noop() {
        let b = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let img = b.to_datum_bytes();
        let e = entry(dp(img.as_ptr()), kani::any(), false, kani::any());
        let eptr = dp(&e as *const GISTENTRY);
        let f = fcn(IDX_POINT_COMPRESS, 1030, "gist_point_compress");
        let d = expect_ok(call(f, [eptr], false), 0);
        assert!(d.as_usize() == eptr.as_usize());
    }

    /// Fetch: box key -> point (high.x, high.y) + fresh GISTENTRY image.
    #[kani::proof]
    #[kani::unwind(72)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_fetch() {
        let b = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let offset: u16 = kani::any();
        let page_is_leaf: bool = kani::any();
        let img = b.to_datum_bytes();
        let e = entry(dp(img.as_ptr()), offset, false, page_is_leaf);
        let f = fcn(IDX_POINT_FETCH, 3282, "gist_point_fetch");
        let d = expect_ok(call(f, [dp(&e as *const GISTENTRY)], true), 0);
        // SAFETY: result datum points at a GISTENTRY image in the stub heap.
        let re = unsafe { &*(d.as_usize() as *const GISTENTRY) };
        assert!(re.offset == offset);
        assert!(!re.leafkey);
        assert!(re.page_is_leaf == page_is_leaf);
        let rpt = unsafe { point_image_at(re.key) };

        let mut cout = [0.0f64; 2];
        let cerr =
            unsafe { pg_gist_point_fetch(b.high.x, b.high.y, b.low.x, b.low.y, cout.as_mut_ptr()) };
        assert!(cerr == 0);
        assert!(rpt.x.to_bits() == cout[0].to_bits());
        assert!(rpt.y.to_bits() == cout[1].to_bits());
    }

    // =================================================================
    // gist_point_consistent (oid 2179): groups 0/1 + error arm
    // (groups 2/3 = polygon/circle arms are a fenced ladder follow-up;
    //  the C shim traps them with flag 99 -> loud harness failure)
    // =================================================================

    fn rust_point_consistent(
        key: &BOX,
        query: Datum,
        strategy: u16,
        page_is_leaf: bool,
        recheck: &mut bool,
    ) -> PgResult<bool> {
        let f = fcn(IDX_POINT_CONSISTENT, 2179, "gist_point_consistent");
        let kimg = key.to_datum_bytes();
        let e = entry(dp(kimg.as_ptr()), 0, false, page_is_leaf);
        call(
            f,
            [
                dp(&e as *const GISTENTRY),
                query,
                Datum::from_u16(strategy),
                Datum::from_u16(0),
                dpm(recheck as *mut bool),
            ],
            false,
        )
        .map(Datum::as_bool)
    }

    /// Group 0 (point ops) incl. the RTOldBelow/RTOldAbove remaps and the
    /// in-group unrecognized strategies (error verdict parity); leaf and
    /// internal RTSame arms both in-theorem via symbolic page_is_leaf.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_consistent_group0() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        let strategy: u16 = kani::any();
        kani::assume(strategy < 20 || strategy == 29 || strategy == 30);
        let leaf: bool = kani::any();
        let qimg = q.to_datum_bytes();
        let mut recheck = true;
        let r = rust_point_consistent(&k, dp(qimg.as_ptr()), strategy, leaf, &mut recheck);
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_point_consistent(
                strategy, leaf as c_int,
                k.high.x, k.high.y, k.low.x, k.low.y,
                q.x, q.y,
                0.0, 0.0, 0.0, 0.0,
                &mut crech, &mut cres,
            )
        };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(recheck as c_int == crech);
            assert!(v as c_int == cres);
        }
    }

    /// Group 1 (point <@ box, on_pb): the deliberately NON-FUZZY overlap
    /// test; query is a box datum.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_consistent_group1() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let strategy: u16 = kani::any();
        kani::assume((20..40).contains(&strategy));
        let leaf: bool = kani::any();
        let qimg = q.to_datum_bytes();
        let mut recheck = true;
        let r = rust_point_consistent(&k, dp(qimg.as_ptr()), strategy, leaf, &mut recheck);
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_point_consistent(
                strategy, leaf as c_int,
                k.high.x, k.high.y, k.low.x, k.low.y,
                0.0, 0.0,
                q.high.x, q.high.y, q.low.x, q.low.y,
                &mut crech, &mut cres,
            )
        };
        if let Some(v) = adjudicate(r, cerr) {
            assert!(recheck as c_int == crech);
            assert!(v as c_int == cres);
        }
    }

    /// Strategy group >= 4: the wrapper-level unrecognized-strategy error
    /// arm (verdict parity; query datum never dereferenced on this path).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_consistent_badgroup() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let strategy: u16 = kani::any();
        kani::assume(strategy >= 80);
        let leaf: bool = kani::any();
        let mut recheck = true;
        let r = rust_point_consistent(&k, Datum::from_usize(0), strategy, leaf, &mut recheck);
        let (mut crech, mut cres): (c_int, c_int) = (1, 0);
        let cerr = unsafe {
            pg_gist_point_consistent(
                strategy, leaf as c_int,
                k.high.x, k.high.y, k.low.x, k.low.y,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                &mut crech, &mut cres,
            )
        };
        assert!(cerr != 0 && cerr != 99);
        assert!(r.is_err());
        if let Err(e) = r {
            core::mem::forget(e);
        }
    }

    // =================================================================
    // gist_point_distance (oid 3064): arm-fenced slices + vertex grid
    // =================================================================

    fn rust_point_distance(
        key: &BOX,
        q: &Point,
        strategy: u16,
        page_is_leaf: bool,
    ) -> PgResult<f64> {
        let f = fcn(IDX_POINT_DISTANCE, 3064, "gist_point_distance");
        let kimg = key.to_datum_bytes();
        let qimg = q.to_datum_bytes();
        let e = entry(dp(kimg.as_ptr()), 0, false, page_is_leaf);
        let mut recheck = false;
        call(
            f,
            [
                dp(&e as *const GISTENTRY),
                dp(qimg.as_ptr()),
                Datum::from_u16(strategy),
                Datum::from_u16(0),
                dpm(&mut recheck as *mut bool),
            ],
            false,
        )
        .map(Datum::as_f64)
    }

    /// Leaf arm on the axis-aligned slice: leaf keys are degenerate boxes
    /// (high == low == the point) and the query shares ONE symbolic y, so
    /// the single point_dt takes only sqrt-free hypot paths (general hypot
    /// path = model-spec gap, out of every theorem).  float8_mi overflow
    /// keeps the Err arm reachable; NAN pin in-theorem.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_distance_leaf_axis() {
        let (kx, qx) = (any_f64(), any_f64());
        let y = any_f64(); // shared: axis-aligned slice
        let k = bx(kx, y, kx, y); // degenerate leaf box
        let q = pt(qx, y);
        let r = rust_point_distance(&k, &q, 15, true); // RTKNNSearch (group 0)
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_point_distance(15, 1, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut cdist)
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// Internal, point-inside-box arm: exact compares only, distance 0.0.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_distance_inbox() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        kani::assume(q.x <= k.high.x && q.x >= k.low.x && q.y <= k.high.y && q.y >= k.low.y);
        let r = rust_point_distance(&k, &q, 15, false);
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_point_distance(15, 0, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut cdist)
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// Internal, over/below strip: x in range, not inside -> float8_mi arms
    /// + the "inconsistent point values" elog arm (reachable with NaN y —
    /// verdict parity, flag 8).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_distance_ystrip() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        kani::assume(q.x <= k.high.x && q.x >= k.low.x);
        kani::assume(!(q.y <= k.high.y && q.y >= k.low.y));
        let r = rust_point_distance(&k, &q, 15, false);
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_point_distance(15, 0, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut cdist)
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// Internal, left/right strip: y in range, x outside.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_distance_xstrip() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        kani::assume(!(q.x <= k.high.x && q.x >= k.low.x));
        kani::assume(q.y <= k.high.y && q.y >= k.low.y);
        let r = rust_point_distance(&k, &q, 15, false);
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_point_distance(15, 0, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut cdist)
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    // Vertex-arm cells (box hx,hy,lx,ly, point x,y): point strictly outside
    // both coordinate ranges -> FOUR point_dt calls, all concrete (dual-
    // point_dt shapes go straight to one-symbolic-index grids).  Cells pin
    // NaN routing (canonical-NAN shim), Inf, an overflow-error dx, and the
    // vertex-minimum selection on ordinary values.
    const VGRID: [[f64; 6]; 8] = [
        [2.0, 3.0, 1.0, 1.0, 0.0, 0.0],                    // low-left vertex
        [2.0, 3.0, 1.0, 1.0, 5.0, 7.0],                    // high-right vertex
        [2.0, 3.0, 1.0, 1.0, 0.0, 7.0],                    // top-left vertex
        [2.0, 3.0, 1.0, 1.0, 5.0, 0.0],                    // bottom-right vertex
        [2.0, 3.0, 1.0, 1.0, f64::NAN, f64::NAN],          // NaN point -> NaN dist
        [f64::NAN, f64::NAN, f64::NAN, f64::NAN, 0.0, 5.0], // NaN box
        [2.0, 3.0, 1.0, 1.0, f64::INFINITY, -5.0],         // Inf point -> Inf dist
        [1.0e308, 1.0e308, 1.0e307, 1.0e307, -1.0e308, -1.0e308], // dx overflow -> error
    ];

    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn grid_gist_point_distance_vertex() {
        let i: usize = kani::any();
        kani::assume(i < VGRID.len());
        let c = VGRID[i];
        let k = bx(c[0], c[1], c[2], c[3]);
        let q = pt(c[4], c[5]);
        let r = rust_point_distance(&k, &q, 15, false);
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_point_distance(15, 0, c[0], c[1], c[2], c[3], c[4], c[5], &mut cdist)
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// Strategy group != 0: the error arm (verdict parity both sides).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_point_distance_badstrategy() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        let strategy: u16 = kani::any();
        kani::assume(strategy >= 20);
        let leaf: bool = kani::any();
        let r = rust_point_distance(&k, &q, strategy, leaf);
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_point_distance(
                strategy, leaf as c_int,
                k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut cdist,
            )
        };
        assert!(cerr != 0);
        assert!(r.is_err());
        if let Err(e) = r {
            core::mem::forget(e);
        }
    }

    // =================================================================
    // bbox distances: gist_box_distance (3998), gist_circle_distance
    // (3280), gist_poly_distance (3288) — computeDistance(false, ..) via
    // gist_bbox_distance; circle/poly additionally claim *recheck = true
    // =================================================================

    fn rust_bbox_distance(
        idx: usize,
        oid: u32,
        name: &str,
        key: &BOX,
        q: &Point,
        strategy: u16,
        recheck: &mut bool,
    ) -> PgResult<f64> {
        let f = fcn(idx, oid, name);
        let kimg = key.to_datum_bytes();
        let qimg = q.to_datum_bytes();
        let e = entry(dp(kimg.as_ptr()), 0, false, kani::any()); // page level must not matter
        call(
            f,
            [
                dp(&e as *const GISTENTRY),
                dp(qimg.as_ptr()),
                Datum::from_u16(strategy),
                Datum::from_u16(0),
                dpm(recheck as *mut bool),
            ],
            false,
        )
        .map(Datum::as_f64)
    }

    /// gist_box_distance, strip/in-box arms (x-range or y-range holds).
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_box_distance_strips() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        kani::assume(
            (q.x <= k.high.x && q.x >= k.low.x) || (q.y <= k.high.y && q.y >= k.low.y),
        );
        let mut recheck = false;
        let r = rust_bbox_distance(IDX_BOX_DISTANCE, 3998, "gist_box_distance", &k, &q, 15, &mut recheck);
        let mut cdist = 0.0f64;
        let cerr = unsafe {
            pg_gist_box_distance(15, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut cdist)
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// gist_box_distance, vertex arm on the concrete grid.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn grid_gist_box_distance_vertex() {
        let i: usize = kani::any();
        kani::assume(i < VGRID.len());
        let c = VGRID[i];
        let k = bx(c[0], c[1], c[2], c[3]);
        let q = pt(c[4], c[5]);
        let mut recheck = false;
        let r = rust_bbox_distance(IDX_BOX_DISTANCE, 3998, "gist_box_distance", &k, &q, 15, &mut recheck);
        let mut cdist = 0.0f64;
        let cerr =
            unsafe { pg_gist_box_distance(15, c[0], c[1], c[2], c[3], c[4], c[5], &mut cdist) };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// gist_circle_distance: bbox lower bound + the *recheck = true claim.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_circle_distance_strips() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        kani::assume(
            (q.x <= k.high.x && q.x >= k.low.x) || (q.y <= k.high.y && q.y >= k.low.y),
        );
        let mut recheck = false;
        let r = rust_bbox_distance(
            IDX_CIRCLE_DISTANCE, 3280, "gist_circle_distance", &k, &q, 15, &mut recheck,
        );
        let (mut crech, mut cdist): (c_int, f64) = (0, 0.0);
        let cerr = unsafe {
            pg_gist_circle_distance(
                15, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut crech, &mut cdist,
            )
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(recheck as c_int == crech); // both true: lossy method
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    /// gist_poly_distance: same shape, *recheck = true claim.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_poly_distance_strips() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let q = pt(any_f64(), any_f64());
        kani::assume(
            (q.x <= k.high.x && q.x >= k.low.x) || (q.y <= k.high.y && q.y >= k.low.y),
        );
        let mut recheck = false;
        let r = rust_bbox_distance(
            IDX_POLY_DISTANCE, 3288, "gist_poly_distance", &k, &q, 15, &mut recheck,
        );
        let (mut crech, mut cdist): (c_int, f64) = (0, 0.0);
        let cerr = unsafe {
            pg_gist_poly_distance(
                15, k.high.x, k.high.y, k.low.x, k.low.y, q.x, q.y, &mut crech, &mut cdist,
            )
        };
        if let Some(d) = adjudicate(r, cerr) {
            assert!(recheck as c_int == crech);
            assert!(d.to_bits() == cdist.to_bits());
        }
    }

    // =================================================================
    // gist_poly_consistent (oid 2585) / gist_poly_compress (oid 2586):
    // the Rust side decodes a REAL polygon varlena image in-theorem
    // (4B header, npts=1, symbolic boundbox + point); the C side receives
    // the boundbox per the shim contract.
    // =================================================================

    #[kani::proof]
    #[kani::unwind(72)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_poly_consistent() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let bb = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let p0 = pt(any_f64(), any_f64());
        let strategy: u16 = kani::any();
        let (knull, qnull): (bool, bool) = (kani::any(), kani::any());
        let kimg = k.to_datum_bytes();
        let qimg = poly_image(&bb, &p0);
        let e = entry(
            if knull { Datum::from_usize(0) } else { dp(kimg.as_ptr()) },
            0,
            false,
            kani::any(),
        );
        let f = fcn(IDX_POLY_CONSISTENT, 2585, "gist_poly_consistent");
        let mut recheck = false;
        let r = call(
            f,
            [
                dp(&e as *const GISTENTRY),
                if qnull { Datum::from_usize(0) } else { dp(qimg.as_ptr()) },
                Datum::from_u16(strategy),
                Datum::from_u16(0),
                dpm(&mut recheck as *mut bool),
            ],
            true, // poly_at evaluates fcinfo.result_mcx()
        )
        .map(Datum::as_bool);
        let (mut crech, mut cres): (c_int, c_int) = (0, 0);
        let cerr = unsafe {
            pg_gist_poly_consistent(
                knull as c_int, k.high.x, k.high.y, k.low.x, k.low.y,
                qnull as c_int, bb.high.x, bb.high.y, bb.low.x, bb.low.y,
                strategy, &mut crech, &mut cres,
            )
        };
        assert!(recheck as c_int == crech); // always true (inexact method)
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    /// Poly compress, leaf arm: boundbox extracted from the decoded varlena
    /// image into a fresh box + GISTENTRY image.
    #[kani::proof]
    #[kani::unwind(72)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_poly_compress_leaf() {
        let bb = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let p0 = pt(any_f64(), any_f64());
        let offset: u16 = kani::any();
        let page_is_leaf: bool = kani::any();
        let qimg = poly_image(&bb, &p0);
        let e = entry(dp(qimg.as_ptr()), offset, true, page_is_leaf);
        let f = fcn(IDX_POLY_COMPRESS, 2586, "gist_poly_compress");
        let d = expect_ok(call(f, [dp(&e as *const GISTENTRY)], true), 0);
        // SAFETY: result datum points at a GISTENTRY image in the stub heap.
        let re = unsafe { &*(d.as_usize() as *const GISTENTRY) };
        assert!(re.offset == offset);
        assert!(!re.leafkey);
        assert!(re.page_is_leaf == page_is_leaf);
        let rbox = unsafe { box_image_at(re.key) };

        let mut cout = [0.0f64; 4];
        let cerr = unsafe {
            pg_gist_poly_compress_leaf(bb.high.x, bb.high.y, bb.low.x, bb.low.y, cout.as_mut_ptr())
        };
        assert!(cerr == 0);
        assert!(rbox.high.x.to_bits() == cout[0].to_bits());
        assert!(rbox.high.y.to_bits() == cout[1].to_bits());
        assert!(rbox.low.x.to_bits() == cout[2].to_bits());
        assert!(rbox.low.y.to_bits() == cout[3].to_bits());
    }

    /// Poly compress, no-op arm: identity (C: `retval = entry`).
    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_gist_poly_compress_noop() {
        let b = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let img = b.to_datum_bytes();
        let e = entry(dp(img.as_ptr()), kani::any(), false, kani::any());
        let eptr = dp(&e as *const GISTENTRY);
        let f = fcn(IDX_POLY_COMPRESS, 2586, "gist_poly_compress");
        let d = expect_ok(call(f, [eptr], false), 0);
        assert!(d.as_usize() == eptr.as_usize());
    }

    // =================================================================
    // gist_circle_consistent (oid 2591) / gist_circle_compress (oid 2592)
    // =================================================================

    /// Circle consistent: bbox computation (float8_pl/mi — fast class, Err
    /// arm covered) + rtree_internal_consistent, all fully symbolic.
    #[kani::proof]
    #[kani::unwind(34)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_circle_consistent() {
        let k = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let c = CIRCLE {
            center: pt(any_f64(), any_f64()),
            radius: any_f64(),
        };
        let strategy: u16 = kani::any();
        let (knull, qnull): (bool, bool) = (kani::any(), kani::any());
        let kimg = k.to_datum_bytes();
        let qimg = c.to_datum_bytes();
        let e = entry(
            if knull { Datum::from_usize(0) } else { dp(kimg.as_ptr()) },
            0,
            false,
            kani::any(),
        );
        let f = fcn(IDX_CIRCLE_CONSISTENT, 2591, "gist_circle_consistent");
        let mut recheck = false;
        let r = call(
            f,
            [
                dp(&e as *const GISTENTRY),
                if qnull { Datum::from_usize(0) } else { dp(qimg.as_ptr()) },
                Datum::from_u16(strategy),
                Datum::from_u16(0),
                dpm(&mut recheck as *mut bool),
            ],
            false,
        )
        .map(Datum::as_bool);
        let (mut crech, mut cres): (c_int, c_int) = (0, 0);
        let cerr = unsafe {
            pg_gist_circle_consistent(
                knull as c_int, k.high.x, k.high.y, k.low.x, k.low.y,
                qnull as c_int, c.center.x, c.center.y, c.radius,
                strategy, &mut crech, &mut cres,
            )
        };
        assert!(recheck as c_int == crech); // always true (inexact method)
        if let Some(v) = adjudicate(r, cerr) {
            assert!(v as c_int == cres);
        }
    }

    /// Circle compress, leaf arm: circle -> bbox + GISTENTRY image; the
    /// float8_pl/mi overflow arm keeps Err reachable (verdict + sqlstate).
    #[kani::proof]
    #[kani::unwind(72)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_gist_circle_compress_leaf() {
        let c = CIRCLE {
            center: pt(any_f64(), any_f64()),
            radius: any_f64(),
        };
        let offset: u16 = kani::any();
        let page_is_leaf: bool = kani::any();
        let img = c.to_datum_bytes();
        let e = entry(dp(img.as_ptr()), offset, true, page_is_leaf);
        let f = fcn(IDX_CIRCLE_COMPRESS, 2592, "gist_circle_compress");
        let r = call(f, [dp(&e as *const GISTENTRY)], true);
        let mut cout = [0.0f64; 4];
        let cerr = unsafe {
            pg_gist_circle_compress_leaf(c.center.x, c.center.y, c.radius, cout.as_mut_ptr())
        };
        if let Some(d) = adjudicate(r, cerr) {
            // SAFETY: result datum points at a GISTENTRY image (stub heap).
            let re = unsafe { &*(d.as_usize() as *const GISTENTRY) };
            assert!(re.offset == offset);
            assert!(!re.leafkey);
            assert!(re.page_is_leaf == page_is_leaf);
            let rbox = unsafe { box_image_at(re.key) };
            assert!(rbox.high.x.to_bits() == cout[0].to_bits());
            assert!(rbox.high.y.to_bits() == cout[1].to_bits());
            assert!(rbox.low.x.to_bits() == cout[2].to_bits());
            assert!(rbox.low.y.to_bits() == cout[3].to_bits());
        }
    }

    /// Circle compress, no-op arm: identity.
    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_gist_circle_compress_noop() {
        let b = bx(any_f64(), any_f64(), any_f64(), any_f64());
        let img = b.to_datum_bytes();
        let e = entry(dp(img.as_ptr()), kani::any(), false, kani::any());
        let eptr = dp(&e as *const GISTENTRY);
        let f = fcn(IDX_CIRCLE_COMPRESS, 2592, "gist_circle_compress");
        let d = expect_ok(call(f, [eptr], false), 0);
        assert!(d.as_usize() == eptr.as_usize());
    }

    // =================================================================
    // gist_translate_cmptype_common (oid 6347): 8-arm i32 -> u16 table
    // =================================================================

    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_gist_translate_cmptype_common() {
        let cmptype: i32 = kani::any();
        let f = fcn(IDX_TRANSLATE, 6347, "gist_translate_cmptype_common");
        let d = expect_ok(call(f, [Datum::from_i32(cmptype)], false), 0);
        let c = unsafe { pg_gist_translate_cmptype_common(cmptype) };
        assert!(d.as_u16() as c_int == c);
    }
}
