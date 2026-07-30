//! EXTENSION 3 (2026-07-30, geopoly lane): polygon/path POSITION operators
//! and the path/poly header-read converters.
//!
//! THE PARITY SURFACE: unlike the box_* position operators (EPSILON-fuzzy
//! FPlt/FPle/FPgt/FPge — proved in lib.rs), the poly_* position operators
//! are RAW IEEE comparisons on the stored boundbox coordinates (geo_ops.c
//! poly_left..poly_overabove: `polya->boundbox.high.x < polyb->boundbox.low.x`
//! etc., no EPSILON). A port that reused the box comparator shape would
//! diverge inside the epsilon band; the negative control
//! (control_poly_left_vs_fuzzy) pits shipped poly_left against exactly that
//! wrong shape and MUST fail in-band. NaN planes are fully in-theorem: all
//! boundbox coordinates are symbolic via from_bits (every NaN payload,
//! ±Inf, ±0, subnormals), and raw C comparisons on NaN are false on both
//! sides.
//!
//! Rust side: the SHIPPED fmgr wrappers `adt_geo::builtins::fc_poly_*` /
//! `fc_path_*`, invoked through a real `LocalFcinfo` frame whose args are
//! INLINE 4-BYTE-HEADER UNCOMPRESSED varlena images built by the harness —
//! so the varlena unwrap (pointer word -> arg_varlena_packed 4B-u fast
//! path -> payload slice -> PolyRef/PathRef::from_payload header parse,
//! including the npts-driven point-slice bounds) is inside the theorem.
//! Out of proof: short-header (1B) and toasted/compressed images (those
//! arms detoast through Mcx — not part of the comparator claim), and
//! npts > MAXN (fenced; image capacity).
//!
//! C side: c/pg_geo_poly.c — geo_ops.c bodies verbatim @ REL_18_STABLE;
//! fmgr unwrapping shimmed to plain signatures taking the header fields
//! the bodies read (see that file's provenance header). The C bodies never
//! touch the NAN macro (comparisons only); the family's canonical-NAN shim
//! rides in pg_geo_cmp.c, which is always linked alongside.
//!
//! Domains:
//!   - poly position ops: both boundboxes fully symbolic (8 sym f64),
//!     npts = 1 literal, vertex bytes concrete zero (the bodies never read
//!     vertices; npts enters only through the in-theorem slice bounds).
//!   - path_isclosed/isopen: npts symbolic in [1,MAXN], closed = FULL
//!     symbolic i32 — the nonzero-closed plane (closed=2, -1, ...) is
//!     in-theorem on both sides (C `(x) ? 1 : 0` BoolGetDatum vs Rust
//!     `!= 0`).
//!   - path_npoints/poly_npoints: npts symbolic in [1,MAXN]; i32 result.
//!
//! Run: cd proofs/geo-cmp && ./run-one.sh 'ext3::proofs_ext3::<h>' 60 \
//!        --c-lib c/pg_geo_poly.c --solver kissat
//! (run-one.sh already passes -Z c-ffi -Z stubbing --c-lib c/pg_geo_cmp.c
//! --no-overflow-checks --exact. Negative control runs with the DEFAULT
//! solver, no kissat.)

#[cfg(kani)]
mod proofs_ext3 {
    use datum::Datum;
    use proof_support::call2;
    use types_core::geo::{Point, BOX, PATH_HEADER_SIZE, POLYGON_HEADER_SIZE};

    use std::os::raw::c_int;

    extern "C" {
        fn pg_poly_left(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_overleft(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_right(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_overright(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_below(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_overbelow(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_above(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
        fn pg_poly_overabove(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;

        fn pg_path_isclosed(npts: i32, closed: i32) -> c_int;
        fn pg_path_isopen(npts: i32, closed: i32) -> c_int;
        fn pg_path_npoints(npts: i32, closed: i32) -> i32;
        fn pg_poly_npoints(npts: i32) -> i32;

        // Negative control only — NOT Postgres code.
        fn pg_poly_left_fuzzy(ahx: f64, ahy: f64, alx: f64, aly: f64, bhx: f64, bhy: f64, blx: f64, bly: f64) -> c_int;
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }

    /// Image capacity in points; npts fences are `1..=MAXN`.
    const MAXN: usize = 3;
    const PT: usize = 16;
    const POLY_CAP: usize = POLYGON_HEADER_SIZE + MAXN * PT;
    const PATH_CAP: usize = PATH_HEADER_SIZE + MAXN * PT;

    /// Inline 4B-uncompressed POLYGON varlena image (LE header word =
    /// total_size << 2). Vertex bytes stay zero: the position-operator
    /// bodies never read them; npts still drives the in-theorem
    /// from_payload slice bounds.
    fn poly_image(npts: usize, bb: &BOX) -> [u8; POLY_CAP] {
        let mut img = [0u8; POLY_CAP];
        let total = (POLYGON_HEADER_SIZE + npts * PT) as u32;
        img[0..4].copy_from_slice(&(total << 2).to_le_bytes());
        img[4..8].copy_from_slice(&(npts as i32).to_ne_bytes());
        img[8..40].copy_from_slice(&bb.to_datum_bytes());
        img
    }

    /// Inline 4B-uncompressed PATH varlena image. `closed` is the raw
    /// int32 header field (NOT normalized — the nonzero plane is the
    /// point).
    fn path_image(npts: usize, closed: i32) -> [u8; PATH_CAP] {
        let mut img = [0u8; PATH_CAP];
        let total = (PATH_HEADER_SIZE + npts * PT) as u32;
        img[0..4].copy_from_slice(&(total << 2).to_le_bytes());
        img[4..8].copy_from_slice(&(npts as i32).to_ne_bytes());
        img[8..12].copy_from_slice(&closed.to_ne_bytes());
        img
    }

    fn any_box() -> BOX {
        BOX {
            high: Point { x: any_f64(), y: any_f64() },
            low: Point { x: any_f64(), y: any_f64() },
        }
    }

    /// Shipped-wrapper call with two inline POLYGON varlena datums.
    fn rust_poly_op(
        fc: proof_support::FcFn<Box<types_error::PgError>>,
        ba: &BOX,
        bb: &BOX,
    ) -> bool {
        let ia = poly_image(1, ba);
        let ib = poly_image(1, bb);
        match call2(fc, ia.as_ptr(), ib.as_ptr()) {
            Ok(d) => d.as_bool(),
            Err(_) => panic!("infallible poly position operator errored"),
        }
    }

    // ---------- poly position operators: boundboxes fully symbolic ----------

    macro_rules! poly_pos_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (ba, bb) = (any_box(), any_box());
                let r = rust_poly_op(adt_geo::builtins::$fc, &ba, &bb);
                let c = unsafe {
                    $pg(ba.high.x, ba.high.y, ba.low.x, ba.low.y,
                        bb.high.x, bb.high.y, bb.low.x, bb.low.y)
                };
                assert!(r as c_int == c);
                kani::cover!(r);
                kani::cover!(!r);
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

    // ---------- path/poly header reads ----------

    fn any_npts() -> usize {
        let npts: usize = kani::any();
        kani::assume(npts >= 1 && npts <= MAXN);
        npts
    }

    #[kani::proof]
    fn eq_path_isclosed() {
        let npts = any_npts();
        let closed: i32 = kani::any(); // full i32: nonzero-closed plane in-theorem
        let img = path_image(npts, closed);
        let d = proof_support::call1(adt_geo::builtins::fc_path_isclosed, img.as_ptr())
            .unwrap_or_else(|_| panic!("path_isclosed errored"));
        let r = d.as_bool();
        let c = unsafe { pg_path_isclosed(npts as i32, closed) };
        assert!(r as c_int == c);
        kani::cover!(r);
        kani::cover!(!r);
    }

    #[kani::proof]
    fn eq_path_isopen() {
        let npts = any_npts();
        let closed: i32 = kani::any();
        let img = path_image(npts, closed);
        let d = proof_support::call1(adt_geo::builtins::fc_path_isopen, img.as_ptr())
            .unwrap_or_else(|_| panic!("path_isopen errored"));
        let r = d.as_bool();
        let c = unsafe { pg_path_isopen(npts as i32, closed) };
        assert!(r as c_int == c);
        kani::cover!(r);
        kani::cover!(!r);
    }

    #[kani::proof]
    fn eq_path_npoints() {
        let npts = any_npts();
        let closed: i32 = kani::any();
        let img = path_image(npts, closed);
        let d = proof_support::call1(adt_geo::builtins::fc_path_npoints, img.as_ptr())
            .unwrap_or_else(|_| panic!("path_npoints errored"));
        let r = d.as_i32();
        let c = unsafe { pg_path_npoints(npts as i32, closed) };
        assert!(r == c);
        kani::cover!(r == 1);
        kani::cover!(r == MAXN as i32);
    }

    #[kani::proof]
    fn eq_poly_npoints() {
        let npts = any_npts();
        let bb = any_box(); // symbolic boundbox: witness it does not affect npts
        let img = poly_image(npts, &bb);
        let d = proof_support::call1(adt_geo::builtins::fc_poly_npoints, img.as_ptr())
            .unwrap_or_else(|_| panic!("poly_npoints errored"));
        let r = d.as_i32();
        let c = unsafe { pg_poly_npoints(npts as i32) };
        assert!(r == c);
        kani::cover!(r == 1);
        kani::cover!(r == MAXN as i32);
    }

    // ---------- negative control (run with DEFAULT solver) ----------

    /// MUST FAIL: shipped poly_left is a raw IEEE `<` on boundbox coords;
    /// pg_poly_left_fuzzy is the plausibly-wrong EPSILON-fuzzy box_left
    /// shape. Counterexample lives in the epsilon band — witness the
    /// exactness of the poly position family is in-theorem.
    #[kani::proof]
    fn control_poly_left_vs_fuzzy() {
        let (ba, bb) = (any_box(), any_box());
        let r = rust_poly_op(adt_geo::builtins::fc_poly_left, &ba, &bb);
        let c = unsafe {
            pg_poly_left_fuzzy(ba.high.x, ba.high.y, ba.low.x, ba.low.y,
                               bb.high.x, bb.high.y, bb.low.x, bb.low.y)
        };
        assert!(r as c_int == c);
    }
}
