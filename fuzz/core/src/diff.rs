//! Differential fuzz drivers: shipped Rust vs vendored PostgreSQL C.
//!
//! Targets the `excluded(wall)` proofs-ledger rows that Kani/CBMC cannot
//! reach (strtod-class parse cascades, Ryu shortest-decimal result images,
//! 53-bit geometric predicates). The C side is verbatim vendored REL_18
//! PostgreSQL compiled by build.rs (csrc/pg_float_io.c, csrc/pg_geo_io.c,
//! csrc/ryu/*); both sides run in-process and every case is compared
//! three-way: value (exact bits / exact byte image), error-vs-no-error,
//! and error code. Any mismatch panics, which libFuzzer converts into a
//! minimized crash artifact — the reproducer.
//!
//! NaN caveat (documented, deliberate): for *parse* results where both
//! sides yield NaN, payload bits are not compared — PostgreSQL's NaN
//! payload comes from the platform strtod and is not a portable semantic
//! (the C oracle here runs on the host libc). Everything else is bit- or
//! byte-exact, including output images of NaN/Infinity.

use std::ffi::{c_char, CString};

use types_error::{
    PgError, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

extern "C" {
    fn pg_diff_float8in(num: *const c_char) -> f64;
    fn pg_diff_float4in(num: *const c_char) -> f32;
    fn pg_diff_float8out(num: f64, buf32: *mut c_char) -> i32;
    fn pg_diff_float4out(num: f32, buf32: *mut c_char) -> i32;
    fn pg_diff_point_out(x: f64, y: f64, buf: *mut c_char, buflen: i32) -> i32;
    fn pg_diff_on_ppath(px: f64, py: f64, closed: i32, npts: i32, xys: *const f64) -> i32;
    static mut pg_diff_errcode: i32;
}

/// Oracle error classes (see the errcode shim in csrc/pg_float_io.c).
const C_ERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const C_ERR_OUT_OF_RANGE: i32 = 2; /* 22003 */

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode }
}

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else {
        99
    }
}

// ---------------------------------------------------------------------------
// Target: float_in_diff — float4in / float8in (strtod-class parse cascade).
// ---------------------------------------------------------------------------
//
// Input layout: [selector][text...]. selector bit0: 0 = float8in,
// 1 = float4in. The text must be interior-NUL-free valid UTF-8 — the only
// shape reachable through the shipped Rust `&str` API (the server validates
// client encoding long before datatype input) and the same C cstring the
// oracle parses.

pub fn float_in_diff(data: &[u8]) {
    let Some((&sel, text)) = data.split_first() else {
        return;
    };
    if text.len() > 1024 || text.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(text) else {
        return;
    };
    // ORACLE PLATFORM CARVE (2026-07-30): macOS strtod consumes
    // "nan(<anything>)" including bytes glibc rejects from the n-char-seq,
    // so on this host the C oracle accepts strings real PostgreSQL 18
    // (glibc, confirmed via docker) rejects with 22P02. Shipped Rust
    // matches glibc/PG (pinned by tests::float8in_nan_ncharseq_matches_
    // glibc_pg); skip every nan( form so the fuzzer hunts real
    // divergences instead of rediscovering the libc delta.
    let lower = s.to_ascii_lowercase();
    if lower.contains("nan(") {
        return;
    }
    let cs = CString::new(text).unwrap();

    if sel & 1 == 0 {
        let cval = unsafe { pg_diff_float8in(cs.as_ptr()) };
        let cerr = c_errcode();
        match adt_float::float8in(s, None) {
            Ok(r) => {
                let same =
                    cerr == 0 && (r.to_bits() == cval.to_bits() || (r.is_nan() && cval.is_nan()));
                assert!(
                    same,
                    "float8in DIVERGENCE input={s:?}: C=(err {cerr}, {:016x} {cval:e}) Rust=Ok({:016x} {r:e})",
                    cval.to_bits(),
                    r.to_bits()
                );
            }
            Err(e) => {
                let rerr = rust_err_class(&e);
                assert!(
                    cerr == rerr,
                    "float8in DIVERGENCE input={s:?}: C err {cerr} (val {cval:e}) vs Rust err {rerr} ({})",
                    e.message
                );
            }
        }
    } else {
        let cval = unsafe { pg_diff_float4in(cs.as_ptr()) };
        let cerr = c_errcode();
        match adt_float::float4in(s, None) {
            Ok(r) => {
                let same =
                    cerr == 0 && (r.to_bits() == cval.to_bits() || (r.is_nan() && cval.is_nan()));
                assert!(
                    same,
                    "float4in DIVERGENCE input={s:?}: C=(err {cerr}, {:08x} {cval:e}) Rust=Ok({:08x} {r:e})",
                    cval.to_bits(),
                    r.to_bits()
                );
            }
            Err(e) => {
                let rerr = rust_err_class(&e);
                assert!(
                    cerr == rerr,
                    "float4in DIVERGENCE input={s:?}: C err {cerr} (val {cval:e}) vs Rust err {rerr} ({})",
                    e.message
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Target: float_out_diff — float4out / float8out (Ryu shortest-decimal
// result image, default extra_float_digits=1 arm). Exact byte-image parity.
// ---------------------------------------------------------------------------
//
// Input layout: [selector][raw bits...]. selector bit0: 0 = float8out
// (8 bytes), 1 = float4out (4 bytes). Extra bytes ignored so libFuzzer can
// grow/shrink freely.

pub fn float_out_diff(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let mut cbuf = [0u8; 32];
    if sel & 1 == 0 {
        if rest.len() < 8 {
            return;
        }
        let v = f64::from_le_bytes(rest[..8].try_into().unwrap());
        let clen = unsafe { pg_diff_float8out(v, cbuf.as_mut_ptr().cast()) } as usize;
        let mut rbuf = [0u8; 64];
        let rlen = adt_float::float8out(v, &mut rbuf);
        assert!(
            &cbuf[..clen] == &rbuf[..rlen],
            "float8out DIVERGENCE bits={:016x}: C={:?} Rust={:?}",
            v.to_bits(),
            std::str::from_utf8(&cbuf[..clen]),
            std::str::from_utf8(&rbuf[..rlen])
        );
    } else {
        if rest.len() < 4 {
            return;
        }
        let v = f32::from_le_bytes(rest[..4].try_into().unwrap());
        let clen = unsafe { pg_diff_float4out(v, cbuf.as_mut_ptr().cast()) } as usize;
        let mut rbuf = [0u8; 64];
        let rlen = adt_float::float4out(v, &mut rbuf);
        assert!(
            &cbuf[..clen] == &rbuf[..rlen],
            "float4out DIVERGENCE bits={:08x}: C={:?} Rust={:?}",
            v.to_bits(),
            std::str::from_utf8(&cbuf[..clen]),
            std::str::from_utf8(&rbuf[..rlen])
        );
    }
}

// ---------------------------------------------------------------------------
// Target: geo_diff — point_out (wall: CNF width, result image) and
// on_ppath (wall: 53-bit predicate), value + error + errcode parity.
// ---------------------------------------------------------------------------
//
// Input layout: [selector][payload...]. selector bit0:
//   0 = point_out: payload = 16 bytes (x,y le doubles). Exact image parity.
//   1 = on_ppath: payload = [closed][pt 16 bytes][path pts 16 bytes each,
//       1..=64]. Bool/error parity.

const PATH_HEADER_PAYLOAD: usize = 12; /* npts i32 + closed i32 + pad4 */

pub fn geo_diff(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    if sel & 1 == 0 {
        if rest.len() < 16 {
            return;
        }
        let x = f64::from_le_bytes(rest[0..8].try_into().unwrap());
        let y = f64::from_le_bytes(rest[8..16].try_into().unwrap());
        let mut cbuf = [0u8; 512];
        let clen = unsafe { pg_diff_point_out(x, y, cbuf.as_mut_ptr().cast(), 512) };
        let pt = types_core::geo::Point { x, y };
        let mut out = Vec::with_capacity(64);
        adt_geo::io::point_out(&pt, &mut out);
        // point_out has no error path on either side for any double bits.
        assert!(
            clen >= 0 && &cbuf[..clen as usize] == out.as_slice(),
            "point_out DIVERGENCE x={:016x} y={:016x}: C={:?} Rust={:?}",
            x.to_bits(),
            y.to_bits(),
            std::str::from_utf8(&cbuf[..clen.max(0) as usize]),
            std::str::from_utf8(&out)
        );
    } else {
        let Some((&closed, pts_bytes)) = rest.split_first() else {
            return;
        };
        if pts_bytes.len() < 32 {
            return; /* need the probe point + at least one path point */
        }
        let px = f64::from_le_bytes(pts_bytes[0..8].try_into().unwrap());
        let py = f64::from_le_bytes(pts_bytes[8..16].try_into().unwrap());
        let path_bytes = &pts_bytes[16..];
        let npts = (path_bytes.len() / 16).min(64);
        let closed = (closed & 1) as i32;

        // C oracle side.
        let mut xys = Vec::with_capacity(npts * 2);
        for i in 0..npts {
            xys.push(f64::from_le_bytes(
                path_bytes[i * 16..i * 16 + 8].try_into().unwrap(),
            ));
            xys.push(f64::from_le_bytes(
                path_bytes[i * 16 + 8..i * 16 + 16].try_into().unwrap(),
            ));
        }
        let cres = unsafe { pg_diff_on_ppath(px, py, closed, npts as i32, xys.as_ptr()) };
        let cerr = c_errcode();

        // Rust side: build the PATH varlena payload PathRef expects.
        let mut payload = Vec::with_capacity(PATH_HEADER_PAYLOAD + npts * 16);
        payload.extend_from_slice(&(npts as i32).to_ne_bytes());
        payload.extend_from_slice(&closed.to_ne_bytes());
        payload.extend_from_slice(&[0u8; 4]);
        payload.extend_from_slice(&path_bytes[..npts * 16]);
        let path = adt_geo::PathRef::from_payload(&payload);
        let pt = types_core::geo::Point { x: px, y: py };
        match adt_geo::proximity::on_ppath(&pt, &path) {
            Ok(b) => assert!(
                cres == b as i32,
                "on_ppath DIVERGENCE pt=({:016x},{:016x}) closed={closed} npts={npts} \
                 pts={xys:?}: C={cres} (err {cerr}) Rust=Ok({b})",
                px.to_bits(),
                py.to_bits()
            ),
            Err(e) => {
                let rerr = rust_err_class(&e);
                assert!(
                    cres == -1 && cerr == rerr,
                    "on_ppath DIVERGENCE pt=({:016x},{:016x}) closed={closed} npts={npts} \
                     pts={xys:?}: C={cres} (err {cerr}) Rust=Err({rerr} {})",
                    px.to_bits(),
                    py.to_bits(),
                    e.message
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke tests: drive each differential over an edge-case
// corpus so `cargo test` exercises the C link + comparators without
// cargo-fuzz. These are the same corpora gen_seeds.sh seeds libFuzzer with.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    pub const FLOAT_STR_CORPUS: &[&str] = &[
        "0", "-0", "0.0", "1", "1.5", "-1.5", " 1.5 ", "\t1e10\n", "1e-45", "1e309", "-1e309",
        "1e-309", "1e-323", "5e-324", "2.5e-324", "4.9e-324", "1e-400", "1e400",
        "1.7976931348623157e308", "1.7976931348623159e308", "2.2250738585072014e-308",
        "2.2250738585072011e-308", "3.4028235e38", "3.4028236e38", "1.4e-45", "7e-46",
        "7.038531e-26", "0.1", "0.2", "0.3", "1/3", "..5", "5..", "1e", "1e+", "1e-", "e5", ".",
        "+", "-", "", " ", "NaN", "nan", "NAN", "-NaN", "+nan", "nan(1234)", "nan()", "Infinity",
        "-Infinity", "+Infinity", "inf", "-inf", "INF", "infinity junk", "1.5 junk", "1.5junk",
        "0x1p3", "0x1.8p1", "0x1p-1074", "0x1p-1075", "0x1p1024", "0x", "0x1p", "0xp3",
        "9007199254740993", "9007199254740992.5", "123456789012345678901234567890",
        "0.000000000000000000000000000001", "1_000", "1,5", "١٢٣", "\u{00a0}1", "1\u{2009}",
    ];

    #[test]
    fn float_in_corpus() {
        for s in FLOAT_STR_CORPUS {
            let mut d = vec![0u8];
            d.extend_from_slice(s.as_bytes());
            float_in_diff(&d); /* float8in */
            d[0] = 1;
            float_in_diff(&d); /* float4in */
        }
    }

    pub const F64_BITS_CORPUS: &[u64] = &[
        0x0000000000000000, /* +0 */
        0x8000000000000000, /* -0 */
        0x0000000000000001, /* min subnormal */
        0x000fffffffffffff, /* max subnormal */
        0x0010000000000000, /* min normal */
        0x7fefffffffffffff, /* max finite */
        0x7ff0000000000000, /* +inf */
        0xfff0000000000000, /* -inf */
        0x7ff8000000000000, /* qNaN */
        0x7ff0000000000001, /* sNaN */
        0xfff800000000dead, /* payload NaN */
        0x3ff0000000000000, /* 1.0 */
        0x4024000000000000, /* 10.0 */
        0x3fb999999999999a, /* 0.1 */
        0x4340000000000000, /* 2^53 */
        0x4340000000000001, /* 2^53 + 2 */
        0x40c3880000000000, /* 10001 */
        0x44b52d02c7e14af6, /* 1e23 (boundary-famous) */
        0x44b52d02c7e14af7,
    ];

    #[test]
    fn float_out_corpus() {
        for &bits in F64_BITS_CORPUS {
            let mut d = vec![0u8];
            d.extend_from_slice(&bits.to_le_bytes());
            float_out_diff(&d);
            let mut d4 = vec![1u8];
            d4.extend_from_slice(&((bits >> 32) as u32).to_le_bytes());
            float_out_diff(&d4);
        }
        // sweep all f32 exponent boundaries via bit patterns
        for e in 0..=255u32 {
            for m in [0u32, 1, 0x7fffff] {
                let bits = (e << 23) | m;
                let mut d = vec![1u8];
                d.extend_from_slice(&bits.to_le_bytes());
                float_out_diff(&d);
            }
        }
    }

    /// DIVERGENCE-CANDIDATE WITNESS (found by float_in_diff fuzzing,
    /// minimized artifact crash-2fea..: input "nan(1\x18)").
    /// ADJUDICATION: platform artifact of the macOS-libc ORACLE, not a
    /// pgrust defect — macOS strtod consumes a nan(...) n-char-seq
    /// containing arbitrary bytes up to ')', while glibc (and therefore
    /// real PostgreSQL 18, confirmed against docker postgres:18) stops
    /// after "nan", leaving "(1\x18)" as trailing junk => 22P02. Shipped
    /// Rust matches the glibc/PG behavior; this test pins it.
    #[test]
    fn float8in_nan_ncharseq_matches_glibc_pg() {
        let r = adt_float::float8in("nan(1\u{18})", None);
        assert_eq!(
            r.err().map(|e| rust_err_class(&e)),
            Some(C_ERR_INVALID_TEXT),
            "pgrust must reject nan(<invalid n-char-seq>) like glibc PG"
        );
        // well-formed n-char-seq stays accepted on both
        assert!(adt_float::float8in("nan(123)", None).unwrap().is_nan());
    }

    /// DIVERGENCE-CANDIDATE WITNESS (found by geo_diff fuzzing, artifact
    /// crash-a8aa..): open-path on_ppath, distances a and b each finite
    /// (~1e308) but a+b overflows. C on_ppath computes
    /// FPeq(float8_pl(a, b), ...) => real PostgreSQL 18 raises 22003
    /// ("value out of range: overflow"); shipped Rust computes the plain
    /// unchecked `a + b` (inf) and returns Ok(false).
    /// FIXED (fix/on-ppath-float8-pl): proximity::on_ppath now uses the
    /// checked float8_pl, so Rust raises 22003 exactly like C/PG18; the
    /// geo_diff carve for this divergence has been removed.
    #[test]
    fn on_ppath_overflow_divergence_witness() {
        let pt = types_core::geo::Point { x: 0.0, y: 1e308 };
        let mut payload = Vec::new();
        payload.extend_from_slice(&2i32.to_ne_bytes());
        payload.extend_from_slice(&0i32.to_ne_bytes()); /* open */
        payload.extend_from_slice(&[0u8; 4]);
        for (x, y) in [(0.0f64, 0.0f64), (1.0, 0.0)] {
            payload.extend_from_slice(&x.to_le_bytes());
            payload.extend_from_slice(&y.to_le_bytes());
        }
        let path = adt_geo::PathRef::from_payload(&payload);
        // Fixed behavior: 22003 overflow error, matching C/PG18.
        let err = adt_geo::proximity::on_ppath(&pt, &path).unwrap_err();
        assert_eq!(rust_err_class(&err), C_ERR_OUT_OF_RANGE);
        assert_eq!(err.message, "value out of range: overflow");
        // C oracle behavior (matches real PG18): 22003 error.
        let mut xys = [0.0f64, 0.0, 1.0, 0.0];
        let cres = unsafe { pg_diff_on_ppath(0.0, 1e308, 0, 2, xys.as_mut_ptr()) };
        assert_eq!((cres, c_errcode()), (-1, C_ERR_OUT_OF_RANGE));
    }

    #[test]
    fn geo_corpus() {
        let vals: &[f64] = &[
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.5,
            1e-7,
            -1e-7,
            1e300,
            -1e300,
            5e-324,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            1e6,
            3.5,
        ];
        // point_out over the value grid
        for &x in vals {
            for &y in vals {
                let mut d = vec![0u8];
                d.extend_from_slice(&x.to_le_bytes());
                d.extend_from_slice(&y.to_le_bytes());
                geo_diff(&d);
            }
        }
        // on_ppath: triangle paths built from the grid, open and closed
        for &v in vals {
            for closed in [0u8, 1] {
                let mut d = vec![1u8, closed];
                for &(x, y) in &[(v, 0.0), (0.0, 1.0), (1.0, 1.0), (v, v)] {
                    let (x, y): (f64, f64) = (x, y);
                    d.extend_from_slice(&x.to_le_bytes());
                    d.extend_from_slice(&y.to_le_bytes());
                }
                geo_diff(&d);
            }
        }
    }
}
