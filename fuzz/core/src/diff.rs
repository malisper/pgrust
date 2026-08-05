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
    PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROTOCOL_VIOLATION,
};

extern "C" {
    fn pg_diff_float8in(num: *const c_char) -> f64;
    fn pg_diff_float4in(num: *const c_char) -> f32;
    fn pg_diff_float8out(num: f64, buf32: *mut c_char) -> i32;
    fn pg_diff_float4out(num: f32, buf32: *mut c_char) -> i32;
    fn pg_diff_point_out(x: f64, y: f64, buf: *mut c_char, buflen: i32) -> i32;
    fn pg_diff_on_ppath(px: f64, py: f64, closed: i32, npts: i32, xys: *const f64) -> i32;
    fn pg_diff_float_math(fn_id: i32, a: f64, b: f64, out: *mut f64) -> i32;
    // Accessor because the C-side errcode is _Thread_local (parallel test
    // threads raced on the old shared global) and stable Rust cannot bind
    // a C thread-local as an extern static.
    fn pg_diff_errcode_get() -> i32;
    // Vendored PG 18.3 Ryu (csrc/ryu/): exported non-static, callable
    // directly for the ryu-crate `*_buf` NUL-terminating wrapper arms the
    // server float-out path never calls (lane-0B, common/ryu done-gate).
    fn double_to_shortest_decimal_buf(f: f64, result: *mut c_char) -> i32;
    fn float_to_shortest_decimal_buf(f: f32, result: *mut c_char) -> i32;
    // p1-lanead (float_misc_diff): extra_float_digits <= 0 output arm
    // (pg_strfromd %.*g path) + float4/8 recv/send wire images.
    fn pg_diff_float8out_efd(num: f64, efd: i32, buf32: *mut c_char) -> i32;
    fn pg_diff_float4out_efd(num: f32, efd: i32, buf32: *mut c_char) -> i32;
    fn pg_diff_float4recv(data: *const c_char, len: i32, out: *mut f32) -> i32;
    fn pg_diff_float8recv(data: *const c_char, len: i32, out: *mut f64) -> i32;
    fn pg_diff_float4send(num: f32, out4: *mut c_char);
    fn pg_diff_float8send(num: f64, out8: *mut c_char);
    // Vendored check_float8_array (csrc/pg_float_agg_check.c); image must be
    // >= 24 + 8n bytes (the PG varlena guarantee).
    fn pg_diff_check_float8_array(image: *const c_char, n: i32, out: *mut f64) -> i32;
}

/// Oracle error classes (see the errcode shims in csrc/pg_float_io.c and
/// csrc/pg_float_math.c).
const C_ERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const C_ERR_OUT_OF_RANGE: i32 = 2; /* 22003 */
const C_ERR_INVALID_LOG_ARG: i32 = 3; /* 2201E */
const C_ERR_INVALID_POWER_ARG: i32 = 4; /* 2201F */
const C_ERR_DIVISION_BY_ZERO: i32 = 5; /* 22012 */
const C_ERR_PROTOCOL_VIOLATION: i32 = 6; /* 08P01 */

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else if e.sqlstate == ERRCODE_INVALID_ARGUMENT_FOR_LOG {
        C_ERR_INVALID_LOG_ARG
    } else if e.sqlstate == ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION {
        C_ERR_INVALID_POWER_ARG
    } else if e.sqlstate == ERRCODE_DIVISION_BY_ZERO {
        C_ERR_DIVISION_BY_ZERO
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        C_ERR_PROTOCOL_VIOLATION
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
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
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
    // p1-lanead: the carve is HOST-CONDITIONAL — on glibc (the fleet, the
    // platform real PG runs on) the oracle agrees with PG, so nan( forms
    // are fuzzed there instead of being globally dark.
    #[cfg(target_os = "macos")]
    {
        let lower = s.to_ascii_lowercase();
        if lower.contains("nan(") {
            return;
        }
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
// (8 bytes), 1 = float4out (4 bytes). selector bit1 (lane-0B): additionally
// drive the ryu-crate `*_to_shortest_decimal_buf` NUL-terminating wrappers
// (pub API the server float-out path never calls) against the vendored C
// Ryu's identical wrappers — byte image + returned index + NUL terminator
// parity. Extra bytes ignored so libFuzzer can grow/shrink freely.

pub fn float_out_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    if sel & 2 != 0 {
        // DOUBLE_SHORTEST_DECIMAL_LEN = 25, FLOAT_SHORTEST_DECIMAL_LEN = 16.
        let mut cbuf = [0xaau8; 32];
        let mut rbuf = [0xaau8; 32];
        if sel & 1 == 0 {
            if rest.len() < 8 {
                return;
            }
            let v = f64::from_le_bytes(rest[..8].try_into().unwrap());
            let clen = unsafe { double_to_shortest_decimal_buf(v, cbuf.as_mut_ptr().cast()) };
            let rlen = ryu::double_to_shortest_decimal_buf(v, &mut rbuf);
            assert!(
                clen as usize == rlen && cbuf[..=rlen] == rbuf[..=rlen] && rbuf[rlen] == 0,
                "ryu d2s_buf DIVERGENCE bits={:016x}: C(len={})={:?} Rust(len={})={:?}",
                v.to_bits(),
                clen,
                std::str::from_utf8(&cbuf[..clen.max(0) as usize]),
                rlen,
                std::str::from_utf8(&rbuf[..rlen])
            );
        } else {
            if rest.len() < 4 {
                return;
            }
            let v = f32::from_le_bytes(rest[..4].try_into().unwrap());
            let clen = unsafe { float_to_shortest_decimal_buf(v, cbuf.as_mut_ptr().cast()) };
            let rlen = ryu::float_to_shortest_decimal_buf(v, &mut rbuf);
            assert!(
                clen as usize == rlen && cbuf[..=rlen] == rbuf[..=rlen] && rbuf[rlen] == 0,
                "ryu f2s_buf DIVERGENCE bits={:08x}: C(len={})={:?} Rust(len={})={:?}",
                v.to_bits(),
                clen,
                std::str::from_utf8(&cbuf[..clen.max(0) as usize]),
                rlen,
                std::str::from_utf8(&rbuf[..rlen])
            );
        }
        return;
    }
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
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
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
// Targets: float_math_diff / float_math2_diff — the libm-backed float8 math
// family (proofs-ledger blocked(libm)/blocked(seam:libm-oracle) rows).
// Unlike a Kani seam, both sides here call the REAL platform libm, so the
// actual numerics are exercised; the comparison is three-way and exact:
// result bits, error-vs-no-error, errcode class (22003 overflow/underflow +
// domain, 2201E log args, 2201F power args).
// ---------------------------------------------------------------------------
//
// Function-id table shared with csrc/pg_float_math.c pg_diff_fmath_table.
// ORDER IS LOAD-BEARING: 0..=27 unary (alphabetical), 28..=30 two-argument.

type Math1 = fn(f64) -> types_error::PgResult<f64>;
type Math2 = fn(f64, f64) -> types_error::PgResult<f64>;

fn dsinh_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dsinh(x)) /* infallible in both C and Rust */
}
fn dasinh_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dasinh(x)) /* infallible in both C and Rust */
}

pub const FLOAT_MATH1: &[(&str, Math1)] = &[
    ("dacos", adt_float::dacos),     /* 0 */
    ("dacosd", adt_float::dacosd),   /* 1 */
    ("dacosh", adt_float::dacosh),   /* 2 */
    ("dasin", adt_float::dasin),     /* 3 */
    ("dasind", adt_float::dasind),   /* 4 */
    ("dasinh", dasinh_ok),           /* 5 */
    ("datan", adt_float::datan),     /* 6 */
    ("datand", adt_float::datand),   /* 7 */
    ("datanh", adt_float::datanh),   /* 8 */
    ("dcbrt", adt_float::dcbrt),     /* 9 */
    ("dcos", adt_float::dcos),       /* 10 */
    ("dcosd", adt_float::dcosd),     /* 11 */
    ("dcosh", adt_float::dcosh),     /* 12 */
    ("dcot", adt_float::dcot),       /* 13 */
    ("dcotd", adt_float::dcotd),     /* 14 */
    ("derf", adt_float::derf),       /* 15 */
    ("derfc", adt_float::derfc),     /* 16 */
    ("dexp", adt_float::dexp),       /* 17 */
    ("dgamma", adt_float::dgamma),   /* 18 */
    ("dlgamma", adt_float::dlgamma), /* 19 */
    ("dlog1", adt_float::dlog1),     /* 20 */
    ("dlog10", adt_float::dlog10),   /* 21 */
    ("dsin", adt_float::dsin),       /* 22 */
    ("dsind", adt_float::dsind),     /* 23 */
    ("dsinh", dsinh_ok),             /* 24 */
    ("dtan", adt_float::dtan),       /* 25 */
    ("dtand", adt_float::dtand),     /* 26 */
    ("dtanh", adt_float::dtanh),     /* 27 */
];

pub const FLOAT_MATH2: &[(&str, Math2)] = &[
    ("datan2", adt_float::datan2),   /* C id 28 */
    ("datan2d", adt_float::datan2d), /* C id 29 */
    ("dpow", adt_float::dpow),       /* C id 30 */
];

/// Core comparator: run C oracle id `fn_id` and the paired Rust fn on the
/// same argument(s); panic (= libFuzzer divergence artifact) on any
/// value-bits / error-presence / errcode-class mismatch. NaN payload bits
/// are not compared when both sides yield NaN (both take payloads straight
/// from the same platform libm; canonical-vs-passthrough is pinned by the
/// smoke grid instead) — everything else is bit-exact.
fn float_math_compare(name: &str, fn_id: i32, a: f64, b: f64, rres: types_error::PgResult<f64>) {
    // The verbatim C oracle's INIT_DEGREE_CONSTANTS() gate is a plain
    // static bool over plain double globals — a data race when parallel
    // tests first hit degree-based functions concurrently (C Postgres is
    // one-thread-per-backend and never sees this; observed as a spurious
    // dcotd "divergence" under `cargo test` default parallelism). Warm the
    // constants exactly once, before any concurrent callers.
    static DEGREE_WARMUP: std::sync::Once = std::sync::Once::new();
    DEGREE_WARMUP.call_once(|| {
        let mut w = 0.0f64;
        // dsind (a degree-family fn): forces init_degree_constants().
        unsafe { pg_diff_float_math(23, 45.0, 0.0, &mut w) };
    });
    let mut cval = 0.0f64;
    let cerr = unsafe { pg_diff_float_math(fn_id, a, b, &mut cval) };
    assert!(cerr >= 0, "bad fn_id {fn_id}");
    match rres {
        Ok(r) => {
            let same = cerr == 0 && (r.to_bits() == cval.to_bits() || (r.is_nan() && cval.is_nan()));
            assert!(
                same,
                "{name} DIVERGENCE a={a:e}[{:016x}] b={b:e}[{:016x}]: \
                 C=(err {cerr}, {:016x} {cval:e}) Rust=Ok({:016x} {r:e})",
                a.to_bits(),
                b.to_bits(),
                cval.to_bits(),
                r.to_bits()
            );
        }
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cerr == rerr,
                "{name} DIVERGENCE a={a:e}[{:016x}] b={b:e}[{:016x}]: \
                 C=(err {cerr}, val {cval:e}) Rust=Err({rerr} {})",
                a.to_bits(),
                b.to_bits(),
                e.message
            );
        }
    }
}

// Input layout: [selector][8 bytes le f64]. selector % 28 picks the unary
// function. Extra bytes ignored so libFuzzer can grow/shrink freely.
pub fn float_math_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    if rest.len() < 8 {
        return;
    }
    let x = f64::from_le_bytes(rest[..8].try_into().unwrap());
    let id = (sel as usize) % FLOAT_MATH1.len();
    let (name, f) = FLOAT_MATH1[id];
    float_math_compare(name, id as i32, x, 0.0, f(x));
}

// Input layout: [selector][16 bytes le f64 pair]. selector % 3 picks the
// two-argument function (C ids 28..=30).
pub fn float_math2_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    if rest.len() < 16 {
        return;
    }
    let a = f64::from_le_bytes(rest[..8].try_into().unwrap());
    let b = f64::from_le_bytes(rest[8..16].try_into().unwrap());
    let id = (sel as usize) % FLOAT_MATH2.len();
    let (name, f) = FLOAT_MATH2[id];
    float_math_compare(name, (FLOAT_MATH1.len() + id) as i32, a, b, f(a, b));
}

// ---------------------------------------------------------------------------
// Target: float_misc_diff — the float.c remainder the four float targets do
// not reach (p1-lanead): the rounding/sqrt/degrees unary family (C ids
// 31..38, appended to pg_diff_fmath_table), the extra_float_digits <= 0
// output arm (pg_strfromd %.*g path — the only caller of the shipped
// format_g), and the float4/8 recv/send wire images (proofs-ledger
// wall(symex: pointer-datum recv ABI) rows).
// ---------------------------------------------------------------------------
//
// Input layout: [selector][payload...]. selector % 13 picks the arm:
//   0..=7: unary math (payload = 8 bytes le f64) — degrees, radians, dsqrt,
//          dsign, dtrunc, dround, dceil, dfloor (C ids 31..38).
//   8: float4recv (payload = raw bytes, len capped at 8; short buffers
//      exercise the 08P01 arm). 9: float8recv (cap 16).
//   10: float4out efd arm: payload = [efd byte][4 bytes le f32];
//       efd = 3 - (byte % 19) spans the GUC range [-15, 3].
//   11: float8out efd arm: payload = [efd byte][8 bytes le f64].
//   12: float4send + float8send images (payload = 8 bytes le f64; the f32
//       leg casts).
//   13: check_float8_array (t3/t6 by payload bit0) over a raw image vs the
//       vendored C body — verdict + extracted values. Images shorter than
//       24+8n are asserted Rust-side only (in PG the varlena is always at
//       least VARSIZE bytes; see csrc/pg_float_agg_check.c header).
//   14: write_float8_transarray roundtrip: the written image must be
//       ACCEPTED by the vendored C check and yield the same value bits —
//       witnesses the writer emits exactly a C-valid transarray.
//   15: float8out via the SHARED stub:guc facility (fuzz/STUBS.md demo
//       wiring): payload = [efd byte][8 bytes le f64]; the efd pin is set
//       on BOTH sides through stubs::guc::pin_extra_float_digits and both
//       sides run their GUC-reading output paths (no efd argument).
//   Extra bytes ignored so libFuzzer can grow/shrink freely.

fn dsign_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dsign(x)) /* infallible in both C and Rust */
}
fn dtrunc_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dtrunc(x))
}
fn dround_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dround(x))
}
fn dceil_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dceil(x))
}
fn dfloor_ok(x: f64) -> types_error::PgResult<f64> {
    Ok(adt_float::dfloor(x))
}

/// (name, C fn_id in pg_diff_fmath_table, fn). APPEND-ONLY ids; 0..30 are
/// owned by FLOAT_MATH1/FLOAT_MATH2 and never renumbered.
pub const FLOAT_MATH1B: &[(&str, i32, Math1)] = &[
    ("degrees", 31, adt_float::degrees),
    ("radians", 32, adt_float::radians),
    ("dsqrt", 33, adt_float::dsqrt),
    ("dsign", 34, dsign_ok),
    ("dtrunc", 35, dtrunc_ok),
    ("dround", 36, dround_ok),
    ("dceil", 37, dceil_ok),
    ("dfloor", 38, dfloor_ok),
];

pub fn float_misc_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    match sel % 16 {
        arm @ 0..=7 => {
            if rest.len() < 8 {
                return;
            }
            let x = f64::from_le_bytes(rest[..8].try_into().unwrap());
            let (name, id, f) = FLOAT_MATH1B[arm as usize];
            float_math_compare(name, id, x, 0.0, f(x));
        }
        8 => {
            let buf = &rest[..rest.len().min(8)];
            let mut cval = 0.0f32;
            let cerr =
                unsafe { pg_diff_float4recv(buf.as_ptr().cast(), buf.len() as i32, &mut cval) };
            match adt_float::float4recv(buf) {
                Ok(r) => assert!(
                    cerr == 0 && r.to_bits() == cval.to_bits(),
                    "float4recv DIVERGENCE buf={buf:02x?}: C=(err {cerr}, {:08x}) Rust=Ok({:08x})",
                    cval.to_bits(),
                    r.to_bits()
                ),
                Err(e) => {
                    let rerr = rust_err_class(&e);
                    assert!(
                        cerr == rerr,
                        "float4recv DIVERGENCE buf={buf:02x?}: C err {cerr} vs Rust err {rerr} ({})",
                        e.message
                    );
                }
            }
        }
        9 => {
            let buf = &rest[..rest.len().min(16)];
            let mut cval = 0.0f64;
            let cerr =
                unsafe { pg_diff_float8recv(buf.as_ptr().cast(), buf.len() as i32, &mut cval) };
            match adt_float::float8recv(buf) {
                Ok(r) => assert!(
                    cerr == 0 && r.to_bits() == cval.to_bits(),
                    "float8recv DIVERGENCE buf={buf:02x?}: C=(err {cerr}, {:016x}) Rust=Ok({:016x})",
                    cval.to_bits(),
                    r.to_bits()
                ),
                Err(e) => {
                    let rerr = rust_err_class(&e);
                    assert!(
                        cerr == rerr,
                        "float8recv DIVERGENCE buf={buf:02x?}: C err {cerr} vs Rust err {rerr} ({})",
                        e.message
                    );
                }
            }
        }
        10 => {
            if rest.len() < 5 {
                return;
            }
            let efd = 3 - (rest[0] % 19) as i32; /* GUC range [-15, 3] */
            let v = f32::from_le_bytes(rest[1..5].try_into().unwrap());
            let mut cbuf = [0u8; 40];
            let clen = unsafe { pg_diff_float4out_efd(v, efd, cbuf.as_mut_ptr().cast()) } as usize;
            let mut rbuf = [0u8; 64];
            let rlen = adt_float::float4out_with(v, efd, &mut rbuf);
            assert!(
                &cbuf[..clen] == &rbuf[..rlen],
                "float4out(efd={efd}) DIVERGENCE bits={:08x}: C={:?} Rust={:?}",
                v.to_bits(),
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(&rbuf[..rlen])
            );
        }
        11 => {
            if rest.len() < 9 {
                return;
            }
            let efd = 3 - (rest[0] % 19) as i32;
            let v = f64::from_le_bytes(rest[1..9].try_into().unwrap());
            let mut cbuf = [0u8; 40];
            let clen = unsafe { pg_diff_float8out_efd(v, efd, cbuf.as_mut_ptr().cast()) } as usize;
            let mut rbuf = [0u8; 64];
            let rlen = adt_float::float8out_internal_with(v, efd, &mut rbuf);
            assert!(
                &cbuf[..clen] == &rbuf[..rlen],
                "float8out(efd={efd}) DIVERGENCE bits={:016x}: C={:?} Rust={:?}",
                v.to_bits(),
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(&rbuf[..rlen])
            );
        }
        13 => {
            let Some((&nsel, image)) = rest.split_first() else {
                return;
            };
            let n: usize = if nsel & 1 == 0 { 3 } else { 6 };
            let image = &image[..image.len().min(96)];
            let need = 24 + 8 * n;
            let r3;
            let r6;
            let rres: Result<&[f64], _> = if n == 3 {
                r3 = adt_float::aggregates::check_float8_array::<3>(image, "fuzz");
                r3.as_ref().map(|a| &a[..]).map_err(|e| e)
            } else {
                r6 = adt_float::aggregates::check_float8_array::<6>(image, "fuzz");
                r6.as_ref().map(|a| &a[..]).map_err(|e| e)
            };
            if image.len() < need {
                /* C varlena guarantee: never feed the oracle a short image */
                assert!(
                    rres.is_err(),
                    "check_float8_array accepted a short image (len {} < {need})",
                    image.len()
                );
                return;
            }
            let mut cvals = [0.0f64; 6];
            let cerr = unsafe {
                pg_diff_check_float8_array(image.as_ptr().cast(), n as i32, cvals.as_mut_ptr())
            };
            match rres {
                Ok(vals) => {
                    let same = cerr == 0
                        && vals
                            .iter()
                            .zip(&cvals[..n])
                            .all(|(a, b)| a.to_bits() == b.to_bits());
                    assert!(
                        same,
                        "check_float8_array(t{n}) DIVERGENCE image={:02x?}: C=(err {cerr}, {:?}) Rust=Ok({vals:?})",
                        &image[..need],
                        &cvals[..n]
                    );
                }
                Err(_) => assert!(
                    cerr == 7,
                    "check_float8_array(t{n}) DIVERGENCE image={:02x?}: C accepted, Rust rejected",
                    &image[..need]
                ),
            }
        }
        15 => {
            // stub:guc demonstration wiring (fuzz/STUBS.md): the
            // extra_float_digits pin goes through the SHARED stub facility
            // instead of argument-passing — the driver pins the GUC on both
            // sides from the fuzz byte (Rust: the adt_float session cell the
            // shipped float8out reads; C: pg_stub_extra_float_digits, read
            // by the verbatim float8out_internal_efd via the stubshims
            // wrapper), then both sides run their GUC-READING entry points
            // with no efd argument in sight.
            if rest.len() < 9 {
                return;
            }
            let efd = crate::stubs::guc::pin_extra_float_digits(rest[0]);
            let v = f64::from_le_bytes(rest[1..9].try_into().unwrap());
            let mut cbuf = [0u8; 40];
            let clen =
                unsafe { crate::stubs::pg_stub_float8out_guc(v, cbuf.as_mut_ptr().cast()) }
                    as usize;
            let mut rbuf = [0u8; 64];
            let rlen = adt_float::float8out(v, &mut rbuf);
            assert!(
                cbuf[..clen] == rbuf[..rlen],
                "float8out(stub:guc efd={efd}) DIVERGENCE bits={:016x}: C={:?} Rust={:?}",
                v.to_bits(),
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(&rbuf[..rlen])
            );
        }
        14 => {
            let Some((&nsel, vbytes)) = rest.split_first() else {
                return;
            };
            let n: usize = if nsel & 1 == 0 { 3 } else { 6 };
            if vbytes.len() < 8 * n {
                return;
            }
            let mut vals = [0.0f64; 6];
            for (i, v) in vals[..n].iter_mut().enumerate() {
                *v = f64::from_le_bytes(vbytes[8 * i..8 * i + 8].try_into().unwrap());
            }
            let mut img = [0u8; 72];
            let size = adt_float::aggregates::write_float8_transarray(&vals[..n], &mut img);
            assert_eq!(size, adt_float::aggregates::float8_transarray_size(n));
            let mut cvals = [0.0f64; 6];
            let cerr = unsafe {
                pg_diff_check_float8_array(img.as_ptr().cast(), n as i32, cvals.as_mut_ptr())
            };
            assert!(
                cerr == 0
                    && vals[..n]
                        .iter()
                        .zip(&cvals[..n])
                        .all(|(a, b)| a.to_bits() == b.to_bits()),
                "write_float8_transarray(t{n}) NOT C-VALID: err {cerr} vals={:?} cvals={:?}",
                &vals[..n],
                &cvals[..n]
            );
        }
        _ => {
            if rest.len() < 8 {
                return;
            }
            let v8 = f64::from_le_bytes(rest[..8].try_into().unwrap());
            let v4 = v8 as f32;
            let mut c4 = [0u8; 4];
            let mut c8 = [0u8; 8];
            unsafe {
                pg_diff_float4send(v4, c4.as_mut_ptr().cast());
                pg_diff_float8send(v8, c8.as_mut_ptr().cast());
            }
            let r4 = adt_float::float4send(v4);
            let r8 = adt_float::float8send(v8);
            assert!(
                r4 == c4 && r8 == c8,
                "float send DIVERGENCE bits={:016x}: C4={c4:02x?} R4={r4:02x?} C8={c8:02x?} R8={r8:02x?}",
                v8.to_bits()
            );
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

    /// Serialize this module's tests against each other. The vendored float
    /// C oracle carries process-global mutable statics with no C-side
    /// synchronization (C Postgres is one-thread-per-backend); running these
    /// tests concurrently at high `--test-threads` corrupts them — observed
    /// as a deterministic spurious dcotd "divergence" (C returned -1.6e-303
    /// for cotd(-1e308) whose true value is 0.4877…) at >=8 threads on the
    /// wave-3 train, while every single-threaded and pairwise run is green.
    /// A follow-up owns finding the actual racing writer; this lock makes
    /// the CI signal deterministic without masking single-run divergences.
    use crate::c_oracle_serial;

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
        let _serial = c_oracle_serial();
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
        let _serial = c_oracle_serial();
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

    /// Lane-0B ryu `*_buf` wrapper arm (selector bit1): stable-build smoke
    /// over the same corpora as float_out_corpus, plus the trailing-zero /
    /// round-even shapes the shortest-repr edge arms need.
    #[test]
    fn ryu_buf_corpus() {
        let _serial = c_oracle_serial();
        for &bits in F64_BITS_CORPUS {
            let mut d = vec![2u8];
            d.extend_from_slice(&bits.to_le_bytes());
            float_out_diff(&d);
            let mut d4 = vec![3u8];
            d4.extend_from_slice(&((bits >> 32) as u32).to_le_bytes());
            float_out_diff(&d4);
        }
        for e in 0..=255u32 {
            for m in [0u32, 1, 0x7fffff] {
                let bits = (e << 23) | m;
                let mut d = vec![3u8];
                d.extend_from_slice(&bits.to_le_bytes());
                float_out_diff(&d);
            }
        }
        // exact powers of ten / trailing-zero mantissas (vm trailing-zeros
        // loop + round-even arms), both widths
        for v in [1e2f64, 5e2, 1.25e3, 1e15, 1e16, 2.5, 0.5, 123.0, 500.0] {
            let mut d = vec![2u8];
            d.extend_from_slice(&v.to_le_bytes());
            float_out_diff(&d);
            let mut d4 = vec![3u8];
            d4.extend_from_slice(&(v as f32).to_le_bytes());
            float_out_diff(&d4);
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
        let _serial = c_oracle_serial();
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
        let _serial = c_oracle_serial();
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
        let _serial = c_oracle_serial();
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

    /// Deliberate seed grid for the float-math differentials: domain
    /// boundaries (acos/asin/atanh at ±1, log at 0/negatives, acosh at 1,
    /// tan near π/2), exp/gamma overflow-underflow edges, denormals, ±0,
    /// ±Inf, NaN (canonical + payload), and the degree-variant wrap points
    /// (30/45/60/90/180/270/360 and neighbors) where PostgreSQL has
    /// exact-value special cases. gen_seeds.sh writes the same grid as the
    /// libFuzzer starter corpus.
    pub const FLOAT_MATH_VAL_CORPUS: &[f64] = &[
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.5,
        -0.5,
        0.9999999999999999,
        -0.9999999999999999,
        1.0000000000000002,
        -1.0000000000000002,
        1.5,
        -1.5,
        2.0,
        -2.0,
        -3.0, /* tgamma/lgamma pole */
        -2.5,
        0.1,
        30.0,
        45.0,
        60.0,
        90.0,
        180.0,
        270.0,
        360.0,
        -30.0,
        -45.0,
        -90.0,
        -180.0,
        -270.0,
        -360.0,
        720.5,
        -719.5,
        29.999999999999996,
        90.00000000000001,
        179.99999999999997,
        1.5707963267948966,  /* nearest f64 to π/2 */
        1.5707963267948968,
        3.141592653589793,   /* π */
        6.283185307179586,   /* 2π */
        0.7853981633974483,  /* π/4 */
        709.782712893384,    /* exp overflow edge */
        709.7827128933841,
        -745.1332191019412,  /* exp underflow edge */
        -745.1332191019413,
        710.0,
        -746.0,
        171.62437695630272,  /* tgamma overflow edge */
        171.62437695630274,
        172.0,
        -171.5,
        -2.7476826467e-324,  /* rounds to a denormal */
        5e-324,
        -5e-324,
        2.2250738585072014e-308,
        -2.2250738585072014e-308,
        1e-308,
        1e308,
        -1e308,
        1.7976931348623157e308,
        -1.7976931348623157e308,
        9.007199254740992e15, /* 2^53 */
        1e22,
        -1e22,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ];

    /// DIVERGENCE-CANDIDATE WITNESS (found by float_math_diff fuzzing,
    /// minimized artifact crash-c603..: dasind of f64 bits
    /// bfe000000000003f ≈ -0.500000000000007).
    /// ADJUDICATION: compiler/platform artifact, not a pgrust wrapper
    /// defect. Three observed result-bit values for the same input:
    ///   - shipped Rust (never FP-contracts):          c03e000000000082
    ///   - C oracle, clang -ffp-contract=on (arm64):   c03e000000000081
    ///     (asind_q1's `90.0 - (acos_x/acos_0_5)*60.0` fused to fmsub)
    ///   - real PostgreSQL 18.4 aarch64 glibc docker:  c03e000000000085
    ///     (glibc acos itself differs from macOS libm by ulps here)
    /// With -ffp-contract=off the C oracle is bit-identical to Rust on
    /// this host, so build.rs pins that flag: the fuzz claim is "same
    /// wrapper logic over the same libm". Residual production note: on
    /// FMA targets where the C compiler contracts (aarch64 gcc default),
    /// C PostgreSQL's degree-trig can sit 1 ulp from pgrust even with a
    /// matched libm; baseline x86-64 PG builds cannot contract and match.
    #[test]
    fn dasind_fp_contraction_witness() {
        let _serial = c_oracle_serial();
        let x = f64::from_bits(0xbfe000000000003f);
        let r = adt_float::dasind(x).unwrap();
        // The PORTABLE witness is the differential plane: if either compiler
        // re-contracts the degree-constant chain, the two sides split.
        let mut cval = 0.0f64;
        let cerr = unsafe { pg_diff_float_math(4, x, 0.0, &mut cval) };
        assert_eq!(cerr, 0, "oracle errored on the contraction-witness input");
        assert_eq!(
            cval.to_bits(),
            r.to_bits(),
            "C and Rust dasind bits split — FP contraction regressed on one side"
        );
        // The absolute-bits pin holds only where it was minted: asind_q1
        // routes through libm asin/acos and glibc's last-ulp differs from
        // Apple's, so on gcc/linux-aarch64 BOTH sides agree on a different
        // bit pattern and this pin was red on every fleet rail baseline
        // regardless of mutants (fix/mutants-rail 2026-08-02). Platform-
        // variant pins must be cfg-gated to their minting platform; the
        // cross-compare above is the enforcement everywhere else.
        #[cfg(target_os = "macos")]
        assert_eq!(r.to_bits(), 0xc03e000000000082, "pinned uncontracted result");
    }

    #[test]
    fn float_math_corpus() {
        let _serial = c_oracle_serial();
        let payload_nan = f64::from_bits(0xfff800000000dead);
        let mut vals = FLOAT_MATH_VAL_CORPUS.to_vec();
        vals.push(payload_nan);
        // Unary family: full value grid for every function id.
        for id in 0..FLOAT_MATH1.len() {
            for &x in &vals {
                let mut d = vec![id as u8];
                d.extend_from_slice(&x.to_le_bytes());
                float_math_diff(&d);
            }
        }
        // Two-arg family: full value-pair grid (covers the POSIX pow corner
        // lattice: NaN^0, 1^NaN, 0^neg, neg^nonint, ±Inf arms, ±1^±Inf).
        for id in 0..FLOAT_MATH2.len() {
            for &a in &vals {
                for &b in &vals {
                    let mut d = vec![id as u8];
                    d.extend_from_slice(&a.to_le_bytes());
                    d.extend_from_slice(&b.to_le_bytes());
                    float_math2_diff(&d);
                }
            }
        }
    }

    #[test]
    fn float_corpus_replays_clean() {
        let _serial = c_oracle_serial();
        // CI regression rail: every committed corpus input replays clean
        // through its comparator (the float family's banked corpora).
        for (dir, f) in [
            ("/../corpus/float_in_diff", float_in_diff as fn(&[u8])),
            ("/../corpus/float_out_diff", float_out_diff),
            ("/../corpus/float_math_diff", float_math_diff),
            ("/../corpus/float_math2_diff", float_math2_diff),
            ("/../corpus/float_misc_diff", float_misc_diff),
        ] {
            let dir = format!("{}{}", env!("CARGO_MANIFEST_DIR"), dir);
            let mut n = 0;
            for e in std::fs::read_dir(&dir).expect("corpus dir missing") {
                let p = e.unwrap().path();
                if p.is_file() {
                    f(&std::fs::read(&p).unwrap());
                    n += 1;
                }
            }
            assert!(n >= 30, "{dir}: expected >=30 seeds, found {n}");
        }
    }

    #[test]
    fn float_misc_corpus() {
        let _serial = c_oracle_serial();
        let payload_nan = f64::from_bits(0xfff800000000dead);
        let mut vals = FLOAT_MATH_VAL_CORPUS.to_vec();
        vals.push(payload_nan);
        // Unary rounding/sqrt/degrees family, full value grid.
        for arm in 0..FLOAT_MATH1B.len() {
            for &x in &vals {
                let mut d = vec![arm as u8];
                d.extend_from_slice(&x.to_le_bytes());
                float_misc_diff(&d);
            }
        }
        // recv arms: every wire length 0..=16 (short-buffer 08P01 arms) over
        // the bit-pattern corpus.
        for &bits in F64_BITS_CORPUS {
            let be = bits.to_be_bytes();
            for n in 0..=8usize {
                let mut d = vec![8u8];
                d.extend_from_slice(&be[..n.min(4)]);
                float_misc_diff(&d);
                let mut d8 = vec![9u8];
                d8.extend_from_slice(&be[..n]);
                float_misc_diff(&d8);
            }
            // send images
            let mut ds = vec![12u8];
            ds.extend_from_slice(&bits.to_le_bytes());
            float_misc_diff(&ds);
        }
        // efd output arms: every extra_float_digits in [-15, 3] x bit corpus.
        for efd_byte in 0..19u8 {
            for &bits in F64_BITS_CORPUS {
                let mut d = vec![11u8, efd_byte];
                d.extend_from_slice(&bits.to_le_bytes());
                float_misc_diff(&d);
                let mut d4 = vec![10u8, efd_byte];
                d4.extend_from_slice(&((bits >> 32) as u32).to_le_bytes());
                float_misc_diff(&d4);
                // stub:guc demo arm (15): same corpus through the SHARED
                // both-sides GUC pin instead of argument passing.
                let mut ds = vec![15u8, efd_byte];
                ds.extend_from_slice(&bits.to_le_bytes());
                float_misc_diff(&ds);
            }
        }
        // transarray arms: writer roundtrip + check over crafted images
        // (valid headers, each single-field corruption, short images).
        for &nsel in &[0u8, 1u8] {
            let n = if nsel & 1 == 0 { 3usize } else { 6 };
            let mut d = vec![14u8, nsel];
            for i in 0..n {
                d.extend_from_slice(&(i as f64 + 0.5).to_le_bytes());
            }
            float_misc_diff(&d);
            // valid image via the writer, then corrupt each header word
            let mut vals = [1.5f64; 6];
            vals[0] = f64::NAN;
            let mut img = [0u8; 72];
            let size = adt_float::aggregates::write_float8_transarray(&vals[..n], &mut img);
            let base = &img[..size];
            let mut ok = vec![13u8, nsel];
            ok.extend_from_slice(base);
            float_misc_diff(&ok);
            for off in [4usize, 8, 12, 16, 20] {
                let mut bad = base.to_vec();
                bad[off] ^= 0xff;
                let mut d = vec![13u8, nsel];
                d.extend_from_slice(&bad);
                float_misc_diff(&d);
            }
            for cut in [0usize, 10, 23, 24, size - 1] {
                let mut d = vec![13u8, nsel];
                d.extend_from_slice(&base[..cut]);
                float_misc_diff(&d);
            }
        }
    }
}
