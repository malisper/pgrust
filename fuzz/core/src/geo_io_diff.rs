//! geo_io_diff: differential fuzz driver — shipped Rust `adt_geo` text I/O vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_geo_io.c, I/O-family extension section). Crate under test:
//! crates/backend/utils/adt/geo (io.rs + the in/out cores of builtins.rs).
//!
//! Complements the original geo_diff target (point_out image + on_ppath):
//! this target owns the REMAINING I/O surface: {point,box,lseg,line,path,
//! poly,circle}_in text parse and {box,lseg,line,path,poly,circle}_out
//! images.
//!
//! Comparison planes: parse verdict + errcode class (1=22P02 invalid-text,
//! 2=22003 out-of-range, 3=54000 too-many-points, 4=22012 zero-divide),
//! parsed struct value BITS (bit-exact f64 — NaN/-0.0 included), and the
//! exact out-image bytes (both from parsed values and from raw fuzzed
//! double bits).
//!
//! Input layout: [sel][mode][payload]; sel % 7 = type (0 point, 1 box,
//! 2 lseg, 3 line, 4 path, 5 poly, 6 circle); mode bit0:
//!   0 = text-in: payload = UTF-8 NUL-free text (<=1024B; PG cstrings are
//!       NUL-free and server-encoding valid). On success both parses must
//!       agree bit-exactly AND their out-images must match byte-for-byte.
//!   1 = raw-out: payload = raw le f64 bits for the type's fields (path:
//!       one closed byte first; path/poly capped at 512 points). Exercises
//!       the out functions over ALL double bit patterns, not just
//!       parser-reachable ones.
//!
//! The float parse core is float8in_internal on BOTH sides (platform strtod
//! in the C oracle, exactly as real PostgreSQL; the shipped Rust port on the
//! Rust side) — parity for the parse core itself is float_in_diff's charter,
//! and any residual divergence surfacing here is real geo-visible signal.
//!
//! SKIPPED (this target): recv/send binary I/O (separate wire-format arms;
//! candidate follow-up target), fmgr fc_* wrappers of the geo family
//! (builtins.rs coverage rides the ops-family target plan).

use core::ffi::c_char;
use std::ffi::CString;

use types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};
use types_error::{
    PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

extern "C" {
    fn pg_diff_errcode_get() -> i32;
    fn pg_diff_geo_point_in(str: *const c_char, out: *mut f64) -> i32;
    fn pg_diff_geo_box_in(str: *const c_char, out: *mut f64) -> i32;
    fn pg_diff_geo_lseg_in(str: *const c_char, out: *mut f64) -> i32;
    fn pg_diff_geo_line_in(str: *const c_char, out: *mut f64) -> i32;
    fn pg_diff_geo_circle_in(str: *const c_char, out: *mut f64) -> i32;
    fn pg_diff_geo_path_in(
        str: *const c_char,
        npts: *mut i32,
        closed: *mut i32,
        xys: *mut f64,
        maxpts: i32,
    ) -> i32;
    fn pg_diff_geo_poly_in(
        str: *const c_char,
        npts: *mut i32,
        bound: *mut f64,
        xys: *mut f64,
        maxpts: i32,
    ) -> i32;
    fn pg_diff_geo_box_out(inp: *const f64, buf: *mut u8, buflen: i32) -> i32;
    fn pg_diff_geo_lseg_out(inp: *const f64, buf: *mut u8, buflen: i32) -> i32;
    fn pg_diff_geo_line_out(inp: *const f64, buf: *mut u8, buflen: i32) -> i32;
    fn pg_diff_geo_circle_out(inp: *const f64, buf: *mut u8, buflen: i32) -> i32;
    fn pg_diff_geo_path_out(
        npts: i32,
        closed: i32,
        xys: *const f64,
        buf: *mut u8,
        buflen: i32,
    ) -> i32;
    fn pg_diff_geo_poly_out(npts: i32, xys: *const f64, buf: *mut u8, buflen: i32) -> i32;
}

const MAX_TEXT: usize = 1024;
const MAX_PTS: usize = 512;
const CBUF: usize = 32768;

fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        1
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        2
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        3
    } else if e.sqlstate == ERRCODE_DIVISION_BY_ZERO {
        4
    } else {
        99
    }
}

/// Bit-exact f64 slice equality (NaN payloads and -0.0 significant).
fn bits_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

fn take_text(payload: &[u8]) -> Option<(&str, CString)> {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(payload).ok()?;
    let c = CString::new(payload).unwrap();
    Some((s, c))
}

fn f64s(payload: &[u8], n: usize) -> Option<Vec<f64>> {
    if payload.len() < n * 8 {
        return None;
    }
    Some(
        (0..n)
            .map(|i| f64::from_le_bytes(payload[i * 8..i * 8 + 8].try_into().unwrap()))
            .collect(),
    )
}

pub fn geo_io_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&mode, payload)) = rest.split_first() else {
        return;
    };
    let raw_out = mode & 1 != 0;
    match sel % 7 {
        0 => point_arm(payload, raw_out),
        1 => box_arm(payload, raw_out),
        2 => lseg_arm(payload, raw_out),
        3 => line_arm(payload, raw_out),
        4 => path_arm(payload, raw_out),
        5 => poly_arm(payload, raw_out),
        _ => circle_arm(payload, raw_out),
    }
}

// ---------------------------------------------------------------------------
// Fixed-arity types: shared in-parse comparator returning both parses.
// ---------------------------------------------------------------------------

/// Runs one fixed-arity text-in comparison; on shared success returns the
/// (bit-identical) parsed doubles.
fn in_compare(
    name: &str,
    text: &str,
    cst: i32,
    cvals: &[f64],
    r: Result<Vec<f64>, Box<PgError>>,
) -> Option<Vec<f64>> {
    let cerr = unsafe { pg_diff_errcode_get() };
    match r {
        Ok(vals) => {
            assert!(
                cst == 0 && bits_eq(&vals, cvals),
                "{name}_in DIVERGENCE input={text:?}: C=(st {cst} err {cerr} {cvals:?}) \
                 Rust=Ok({vals:?})"
            );
            Some(vals)
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                cst != 0 && cerr == rc,
                "{name}_in DIVERGENCE input={text:?}: C=(st {cst} err {cerr}) \
                 Rust=Err(class {rc} sqlstate {:?} {})",
                e.sqlstate,
                e.message
            );
            None
        }
    }
}

/// Compare a C out-image against the Rust out-image.
fn out_compare(name: &str, ctx: &dyn core::fmt::Debug, clen: i32, cbuf: &[u8], rust: &[u8]) {
    let cerr = unsafe { pg_diff_errcode_get() };
    assert!(
        clen >= 0 && &cbuf[..clen as usize] == rust,
        "{name}_out DIVERGENCE {ctx:?}: C=(len {clen} err {cerr} {:?}) Rust={:?}",
        std::str::from_utf8(&cbuf[..clen.max(0) as usize]),
        std::str::from_utf8(rust)
    );
}

fn point_arm(payload: &[u8], raw_out: bool) {
    if raw_out {
        // point_out over raw bits is the original geo_diff's arm 0; skip.
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cvals = [0f64; 2];
    let cst = unsafe { pg_diff_geo_point_in(cs.as_ptr(), cvals.as_mut_ptr()) };
    let r = adt_geo::io::point_in(s, None).map(|p| vec![p.x, p.y]);
    if let Some(v) = in_compare("point", s, cst, &cvals, r) {
        // out roundtrip is geo_diff arm 0's plane; still cheap to cross-check
        // through the Rust image only when parse succeeded on both sides.
        let _ = v;
    }
}

fn box_arm(payload: &[u8], raw_out: bool) {
    let mut cbuf = [0u8; CBUF];
    if raw_out {
        let Some(v) = f64s(payload, 4) else { return };
        let clen = unsafe { pg_diff_geo_box_out(v.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32) };
        let b = BOX {
            high: Point { x: v[0], y: v[1] },
            low: Point { x: v[2], y: v[3] },
        };
        let mut out = Vec::new();
        adt_geo::io::box_out(&b, &mut out);
        out_compare("box", &v, clen, &cbuf, &out);
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cvals = [0f64; 4];
    let cst = unsafe { pg_diff_geo_box_in(cs.as_ptr(), cvals.as_mut_ptr()) };
    let r = adt_geo::io::box_in(s, None).map(|b| vec![b.high.x, b.high.y, b.low.x, b.low.y]);
    if let Some(v) = in_compare("box", s, cst, &cvals, r) {
        let clen = unsafe { pg_diff_geo_box_out(v.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32) };
        let b = BOX {
            high: Point { x: v[0], y: v[1] },
            low: Point { x: v[2], y: v[3] },
        };
        let mut out = Vec::new();
        adt_geo::io::box_out(&b, &mut out);
        out_compare("box(rt)", &s, clen, &cbuf, &out);
    }
}

fn lseg_arm(payload: &[u8], raw_out: bool) {
    let mut cbuf = [0u8; CBUF];
    let run_out = |v: &[f64], ctx: &dyn core::fmt::Debug, cbuf: &mut [u8; CBUF]| {
        let clen = unsafe { pg_diff_geo_lseg_out(v.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32) };
        let l = LSEG {
            p: [
                Point { x: v[0], y: v[1] },
                Point { x: v[2], y: v[3] },
            ],
        };
        let mut out = Vec::new();
        adt_geo::io::lseg_out(&l, &mut out);
        out_compare("lseg", ctx, clen, cbuf, &out);
    };
    if raw_out {
        let Some(v) = f64s(payload, 4) else { return };
        run_out(&v, &v, &mut cbuf);
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cvals = [0f64; 4];
    let cst = unsafe { pg_diff_geo_lseg_in(cs.as_ptr(), cvals.as_mut_ptr()) };
    let r =
        adt_geo::io::lseg_in(s, None).map(|l| vec![l.p[0].x, l.p[0].y, l.p[1].x, l.p[1].y]);
    if let Some(v) = in_compare("lseg", s, cst, &cvals, r) {
        run_out(&v, &s, &mut cbuf);
    }
}

fn line_arm(payload: &[u8], raw_out: bool) {
    let mut cbuf = [0u8; CBUF];
    let run_out = |v: &[f64], ctx: &dyn core::fmt::Debug, cbuf: &mut [u8; CBUF]| {
        let clen = unsafe { pg_diff_geo_line_out(v.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32) };
        let l = LINE { A: v[0], B: v[1], C: v[2] };
        let mut out = Vec::new();
        adt_geo::io::line_out(&l, &mut out);
        out_compare("line", ctx, clen, cbuf, &out);
    };
    if raw_out {
        let Some(v) = f64s(payload, 3) else { return };
        run_out(&v, &v, &mut cbuf);
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cvals = [0f64; 3];
    let cst = unsafe { pg_diff_geo_line_in(cs.as_ptr(), cvals.as_mut_ptr()) };
    let r = adt_geo::io::line_in(s, None).map(|l| vec![l.A, l.B, l.C]);
    if let Some(v) = in_compare("line", s, cst, &cvals, r) {
        run_out(&v, &s, &mut cbuf);
    }
}

fn circle_arm(payload: &[u8], raw_out: bool) {
    let mut cbuf = [0u8; CBUF];
    let run_out = |v: &[f64], ctx: &dyn core::fmt::Debug, cbuf: &mut [u8; CBUF]| {
        let clen =
            unsafe { pg_diff_geo_circle_out(v.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32) };
        let c = CIRCLE {
            center: Point { x: v[0], y: v[1] },
            radius: v[2],
        };
        let mut out = Vec::new();
        adt_geo::io::circle_out(&c, &mut out);
        out_compare("circle", ctx, clen, cbuf, &out);
    };
    if raw_out {
        let Some(v) = f64s(payload, 3) else { return };
        run_out(&v, &v, &mut cbuf);
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cvals = [0f64; 3];
    let cst = unsafe { pg_diff_geo_circle_in(cs.as_ptr(), cvals.as_mut_ptr()) };
    let r = adt_geo::io::circle_in(s, None).map(|c| vec![c.center.x, c.center.y, c.radius]);
    if let Some(v) = in_compare("circle", s, cst, &cvals, r) {
        run_out(&v, &s, &mut cbuf);
    }
}

// ---------------------------------------------------------------------------
// Variable-arity types: path / poly. The Rust in-functions build varlena
// images; compare via the PathRef/PolyRef views + the out image.
// ---------------------------------------------------------------------------

fn path_arm(payload: &[u8], raw_out: bool) {
    let mut cbuf = [0u8; CBUF];
    if raw_out {
        let Some((&closed, dbytes)) = payload.split_first() else {
            return;
        };
        let npts = (dbytes.len() / 16).min(MAX_PTS);
        if npts == 0 {
            return;
        }
        let Some(xys) = f64s(dbytes, npts * 2) else { return };
        let closed = (closed & 1) as i32;
        let clen = unsafe {
            pg_diff_geo_path_out(npts as i32, closed, xys.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32)
        };
        // Rust image: [npts i32][closed i32][pad4][points]
        let mut img = Vec::with_capacity(12 + npts * 16);
        img.extend_from_slice(&(npts as i32).to_ne_bytes());
        img.extend_from_slice(&closed.to_ne_bytes());
        img.extend_from_slice(&[0u8; 4]);
        for v in &xys {
            img.extend_from_slice(&v.to_le_bytes());
        }
        let pr = adt_geo::PathRef::from_payload(&img);
        let mut out = Vec::new();
        adt_geo::io::path_out(&pr, &mut out).expect("fuzz geo inputs are far below MaxAllocSize");
        out_compare("path", &(npts, closed), clen, &cbuf, &out);
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cn = 0i32;
    let mut cclosed = 0i32;
    let mut cxys = vec![0f64; MAX_PTS * 2];
    let cst = unsafe {
        pg_diff_geo_path_in(cs.as_ptr(), &mut cn, &mut cclosed, cxys.as_mut_ptr(), MAX_PTS as i32)
    };
    let cerr = unsafe { pg_diff_errcode_get() };
    let cx = mcx::MemoryContext::new("geo_io_fuzz");
    let res = adt_geo::io::path_in(cx.mcx(), s, None);
    match res {
        Ok(vt) => {
            let pr = adt_geo::PathRef::from_payload(vt.data());
            let rn = adt_geo::Pts::n(&pr);
            let rclosed = pr.closed as i32;
            let rxys: Vec<f64> = (0..rn)
                .flat_map(|i| {
                    let p = adt_geo::Pts::pt(&pr, i);
                    [p.x, p.y]
                })
                .collect();
            assert!(
                cst == 0
                    && cn as usize == rn
                    && cclosed == rclosed
                    && bits_eq(&rxys, &cxys[..rn * 2]),
                "path_in DIVERGENCE input={s:?}: C=(st {cst} err {cerr} n {cn} closed \
                 {cclosed}) Rust=Ok(n {rn} closed {rclosed})"
            );
            let clen = unsafe {
                pg_diff_geo_path_out(cn, cclosed, cxys.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32)
            };
            let mut out = Vec::new();
            adt_geo::io::path_out(&pr, &mut out).expect("fuzz geo inputs are far below MaxAllocSize");
            out_compare("path(rt)", &s, clen, &cbuf, &out);
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                cst != 0 && cerr == rc,
                "path_in DIVERGENCE input={s:?}: C=(st {cst} err {cerr}) Rust=Err(class {rc} {})",
                e.message
            );
        }
    }
}

fn poly_arm(payload: &[u8], raw_out: bool) {
    let mut cbuf = [0u8; CBUF];
    if raw_out {
        let npts = (payload.len() / 16).min(MAX_PTS);
        if npts == 0 {
            return;
        }
        let Some(xys) = f64s(payload, npts * 2) else { return };
        let clen = unsafe {
            pg_diff_geo_poly_out(npts as i32, xys.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32)
        };
        let mut img = Vec::with_capacity(36 + npts * 16);
        img.extend_from_slice(&(npts as i32).to_ne_bytes());
        img.extend_from_slice(&[0u8; 32]); // boundbox: not read by poly_out
        for v in &xys {
            img.extend_from_slice(&v.to_le_bytes());
        }
        let pr = adt_geo::PolyRef::from_payload(&img);
        let mut out = Vec::new();
        adt_geo::io::poly_out(&pr, &mut out).expect("fuzz geo inputs are far below MaxAllocSize");
        out_compare("poly", &npts, clen, &cbuf, &out);
        return;
    }
    let Some((s, cs)) = take_text(payload) else {
        return;
    };
    let mut cn = 0i32;
    let mut cbound = [0f64; 4];
    let mut cxys = vec![0f64; MAX_PTS * 2];
    let cst = unsafe {
        pg_diff_geo_poly_in(
            cs.as_ptr(),
            &mut cn,
            cbound.as_mut_ptr(),
            cxys.as_mut_ptr(),
            MAX_PTS as i32,
        )
    };
    let cerr = unsafe { pg_diff_errcode_get() };
    let cx = mcx::MemoryContext::new("geo_io_fuzz");
    let res = adt_geo::io::poly_in(cx.mcx(), s, None);
    match res {
        Ok(vt) => {
            let pr = adt_geo::PolyRef::from_payload(vt.data());
            let rn = adt_geo::Pts::n(&pr);
            let rxys: Vec<f64> = (0..rn)
                .flat_map(|i| {
                    let p = adt_geo::Pts::pt(&pr, i);
                    [p.x, p.y]
                })
                .collect();
            let rbound = [
                pr.boundbox.high.x,
                pr.boundbox.high.y,
                pr.boundbox.low.x,
                pr.boundbox.low.y,
            ];
            // C bound order in the struct image: high then low (BOX layout).
            let cb = [cbound[0], cbound[1], cbound[2], cbound[3]];
            assert!(
                cst == 0 && cn as usize == rn && bits_eq(&rxys, &cxys[..rn * 2])
                    && bits_eq(&rbound, &cb),
                "poly_in DIVERGENCE input={s:?}: C=(st {cst} err {cerr} n {cn} bound {cb:?}) \
                 Rust=Ok(n {rn} bound {rbound:?})"
            );
            let clen = unsafe {
                pg_diff_geo_poly_out(cn, cxys.as_ptr(), cbuf.as_mut_ptr(), CBUF as i32)
            };
            let mut out = Vec::new();
            adt_geo::io::poly_out(&pr, &mut out).expect("fuzz geo inputs are far below MaxAllocSize");
            out_compare("poly(rt)", &s, clen, &cbuf, &out);
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                cst != 0 && cerr == rc,
                "poly_in DIVERGENCE input={s:?}: C=(st {cst} err {cerr}) Rust=Err(class {rc} {})",
                e.message
            );
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/geo_io_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/geo_io_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                geo_io_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // text-in ok + error shapes per type.
        geo_io_diff(b"\x00\x00(1,2)"); // point ok
        geo_io_diff(b"\x00\x00(1,2,3)"); // point err
        geo_io_diff(b"\x01\x00(1,2),(3,4)"); // box ok (corner reorder path)
        geo_io_diff(b"\x01\x00(3,4),(1,2)");
        geo_io_diff(b"\x01\x00nope"); // box err
        geo_io_diff(b"\x02\x00[(1,2),(3,4)]"); // lseg ok
        geo_io_diff(b"\x03\x00{1,2,3}"); // line ABC form
        geo_io_diff(b"\x03\x00{0,0,3}"); // line err: A=B=0
        geo_io_diff(b"\x03\x00(1,2),(3,4)"); // line 2-point form
        geo_io_diff(b"\x03\x00(1,2),(1,2)"); // line err: same points
        geo_io_diff(b"\x04\x00((1,2),(3,4),(5,6))"); // path closed
        geo_io_diff(b"\x04\x00[(1,2),(3,4)]"); // path open
        geo_io_diff(b"\x04\x001,2"); // path bare pair
        geo_io_diff(b"\x05\x00((1,2),(3,4),(5,6))"); // poly ok
        geo_io_diff(b"\x05\x00((1,2),(3,4)"); // poly err
        geo_io_diff(b"\x06\x00<(1,2),3>"); // circle ok
        geo_io_diff(b"\x06\x00<(1,2),-3>"); // circle err: negative radius
        geo_io_diff(b"\x06\x001,2,3"); // circle quick form
        // raw-out: NaN / -0.0 / inf bit patterns through each out image.
        let specials: [f64; 4] = [f64::NAN, -0.0, f64::INFINITY, 1.5e-300];
        let mut p = vec![1u8, 1u8];
        for v in specials {
            p.extend_from_slice(&v.to_le_bytes());
        }
        geo_io_diff(&p); // box raw
        p[0] = 2;
        geo_io_diff(&p); // lseg raw
        p[0] = 3;
        geo_io_diff(&p); // line raw (3 doubles used)
        p[0] = 6;
        geo_io_diff(&p); // circle raw
        let mut pp = vec![4u8, 1u8, 1u8];
        for v in specials {
            pp.extend_from_slice(&v.to_le_bytes());
        }
        geo_io_diff(&pp); // path raw (closed=1, 2 pts)
        pp[0] = 5;
        pp.remove(2); // poly raw has no closed byte
        geo_io_diff(&pp);
    }
}
