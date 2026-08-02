//! jsonbio_diff: differential fuzz driver — shipped Rust `adt_jsonb` io/cast
//! surface vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df)
//! C (csrc/pg_jsonbio_io.c + csrc/jsonbfam/). Crate under test:
//! crates/backend/utils/adt/jsonb (io.rs, build.rs, container.rs, iter.rs,
//! builtins.rs io/cast wrappers) plus the adt_json jsonapi lexer/parser and
//! adt_numeric in/out they drive.
//!
//! The RUST SIDE RUNS THE SHIPPED fc_* WRAPPERS (builtins.rs) on a native
//! LocalFcinfo frame — the exact catalog entry points — so the wrapper lines
//! execute under the C-parity oracle every iteration.
//!
//! Comparison planes: value bytes (container binary image, out/pretty text,
//! send wire bytes, cast scalar bits, numeric_out text), error-verdict, and
//! errcode/sqlstate class. Message text out of scope.
//!
//! Errcode class contract (must match csrc/jsonbfam/shim/postgres.h):
//!   1=22P02 2=22003 3=22023 4=54000 5=22P05 6=internal/other 7=08P01
//!   8=54001 9=22025. Parse failure inside a derived-op C entry returns
//!   100+class; the Rust arms only drive those after their own parse
//!   succeeded, so any 100+ return asserts as a divergence. -1 = SQL NULL
//!   (cast arms, jsonb null input).
//!
//! Input layout: [sel][payload]; sel % 5 =
//!   0 in_full: payload = JSON text -> jsonb_in verdict+sqlstate; on shared
//!     success: container image bytes, jsonb_out text, jsonb_send bytes.
//!     (covers oids 3806 jsonb_in, 3804 jsonb_out, 3803 jsonb_send)
//!   1 recv: payload = raw wire bytes (version byte + text) -> jsonb_recv
//!     (oid 3805) verdict + image.
//!   2 op1: payload = [op][json]; op%4 = jsonb_typeof 3210 /
//!     jsonb_array_length 3207 / jsonb_pretty 3306 / jsonb_strip_nulls 3262,
//!     driven only when the Rust parse succeeds.
//!   3 cast: payload = [which][json]; which%7 = jsonb_bool 3556 / int2 3450 /
//!     int4 3451 / int8 3452 / float4 3453 / float8 2580 / numeric 3449.
//!   4 build_noargs: payload bit0 -> jsonb_build_array_noargs 3272 /
//!     jsonb_build_object_noargs 3274 image compare.
//!
//! INPUT CARVES (documented non-surfaces, driver-enforced identically on
//! both sides):
//!   - text payloads capped at 2048 bytes, NUL-free, valid UTF-8 (PG
//!     cstrings are NUL-free and server-encoding validated upstream of
//!     jsonb_in; encoding pinned UTF8; the oracle has no non-UTF8 arm);
//!   - bracket nesting depth pre-screened to <= 64 ('{'/'[' counted raw,
//!     including inside strings — conservative): the 54001
//!     stack-depth-exceeded surface is environment-sized (real PG:
//!     max_stack_depth GUC vs C stack; oracle: no stack accounting), so
//!     deep-nesting parity is out of this target's scope;
//!   - jsonb_recv wire payloads capped at 2048 bytes.
//!
//! SKIPPED (excluded rows per the claim's carve): SRF family (srfs.rs),
//! populate/record family (typcache), agg transitions (aggs.rs),
//! to_jsonb/variadic build (typcache/funcapi), subscripting (subs.rs), GIN
//! glue (gin.rs). The two-doc ops/mutate family (cmp/contains/getfield/
//! concat/set/delete/...) is the follow-up jsonbops_diff target's charter.

use core::ffi::c_char;
use std::ffi::CString;

use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_UNTRANSLATABLE_CHARACTER,
};
use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_jsonb_in_full(
        str: *const c_char,
        img: *mut u8, imgcap: i32, imglen: *mut i32,
        out: *mut u8, outcap: i32, outlen: *mut i32,
        send: *mut u8, sendcap: i32, sendlen: *mut i32,
    ) -> i32;
    fn pg_diff_jsonb_recv(
        wire: *const u8, wirelen: i32,
        img: *mut u8, imgcap: i32, imglen: *mut i32,
    ) -> i32;
    fn pg_diff_jsonb_op1(
        op: i32, flag: i32, str: *const c_char,
        out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_jsonb_cast(
        which: i32, str: *const c_char,
        out: *mut u8, outcap: i32, outlen: *mut i32,
    ) -> i32;
    fn pg_diff_jsonb_build_noargs(
        isobj: i32,
        img: *mut u8, imgcap: i32, imglen: *mut i32,
    ) -> i32;
}

const MAX_TEXT: usize = 2048;
const MAX_DEPTH: usize = 64;
const CBUF: usize = 1 << 16;

/// Rust-side sqlstate -> the C oracle's class constants (see module header).
pub(crate) fn err_class(e: &PgError) -> i32 {
    let ss = e.sqlstate;
    if ss == ERRCODE_INVALID_TEXT_REPRESENTATION {
        1
    } else if ss == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        2
    } else if ss == ERRCODE_INVALID_PARAMETER_VALUE {
        3
    } else if ss == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        4
    } else if ss == ERRCODE_UNTRANSLATABLE_CHARACTER {
        5
    } else if ss == ERRCODE_PROTOCOL_VIOLATION {
        7
    } else if ss == types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR {
        10
    } else if ss == types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED {
        11
    } else {
        6
    }
}

/// Text-payload gate: both sides only ever see inputs passing this screen
/// (INPUT CARVES in the module header).
pub(crate) fn take_json(payload: &[u8]) -> Option<CString> {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return None;
    }
    std::str::from_utf8(payload).ok()?;
    let mut depth = 0usize;
    let mut max_depth = 0usize;
    for &b in payload {
        match b {
            b'{' | b'[' => {
                depth += 1;
                max_depth = max_depth.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    if max_depth > MAX_DEPTH {
        return None;
    }
    CString::new(payload).ok()
}

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
pub(crate) fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Read a varlena result datum's payload bytes (4-byte or short header).
pub(crate) fn varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live images in the armed arena, read
    // before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// Read a cstring result datum.
pub(crate) fn cstring_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc cstring results are NUL-terminated palloc'd strings.
    unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const c_char) }.to_bytes()
}


/// Grow a C out-buffer for the -2 retry protocol (see the errcode contract).
/// Renders from a <=2KB doc are bounded well under this hard cap.
pub(crate) fn grow(buf: &mut Vec<u8>, needed: i32) {
    let target = (needed.max(0) as usize + 4096).max(buf.len() * 2);
    assert!(target <= (1 << 28), "oracle render exceeded 256MB hard cap");
    buf.resize(target, 0);
}

/// Session environment pin, mirrored by the C oracle's shims: UTF8 database
/// encoding, mbutils seams installed (set-once is process-global and panics
/// on double-install; encoding is thread-local so set per exec — sibling
/// targets share the test process). Shared with jsonbops_diff.
pub(crate) fn init_session_env() {
    {
        use std::sync::Once;
        static SEAMS: Once = Once::new();
        // catch_unwind tolerates another lane's harness installing the
        // mbutils seams first (double-install panics; all lanes share one
        // test binary — arrayfuncs_diff::init_seams convention). Also keeps
        // the Once unpoisoned for every later caller.
        SEAMS.call_once(|| {
            let _ = std::panic::catch_unwind(mbutils::init_seams);
        });
    }
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("PG_UTF8 is valid");
}

pub fn jsonbio_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_session_env();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 5 {
        0 => in_full_arm(payload),
        1 => recv_arm(payload),
        2 => op1_arm(payload),
        3 => cast_arm(payload),
        _ => build_arm(payload),
    }
}

fn in_full_arm(payload: &[u8]) {
    let Some(cs) = take_json(payload) else { return };
    let mut img = vec![0u8; CBUF];
    let mut out = vec![0u8; CBUF];
    let mut snd = vec![0u8; CBUF];
    let (mut il, mut ol, mut sl) = (0i32, 0i32, 0i32);
    let crc = loop {
        let rc = unsafe {
            pg_diff_jsonb_in_full(
                cs.as_ptr(),
                img.as_mut_ptr(), img.len() as i32, &mut il,
                out.as_mut_ptr(), out.len() as i32, &mut ol,
                snd.as_mut_ptr(), snd.len() as i32, &mut sl,
            )
        };
        if rc != -2 {
            break rc;
        }
        grow(&mut img, il.max(ol).max(sl));
        grow(&mut out, il.max(ol).max(sl));
        grow(&mut snd, il.max(ol).max(sl));
    };
    let cx = mcx::MemoryContext::new("jsonbio_fuzz");
    let m = cx.mcx();
    let (r, isnull) = fc_call(
        adt_jsonb::builtins::fc_jsonb_in,
        m,
        [Datum::from_usize(cs.as_ptr() as usize)],
    );
    match r {
        Ok(jb) => {
            assert!(!isnull, "fc_jsonb_in returned SQL NULL without escontext");
            let rimg = varlena_data(jb);
            assert!(
                crc == 0 && rimg == &img[..il as usize],
                "jsonb_in IMAGE DIVERGENCE input={cs:?}: C=(rc {crc} len {il}) Rust=Ok(len {})",
                rimg.len()
            );
            let (txt, _) = fc_call(adt_jsonb::builtins::fc_jsonb_out, m, [jb]);
            let rtxt = cstring_data(txt.expect("jsonb_out on a valid image"));
            assert!(
                rtxt == &out[..ol as usize],
                "jsonb_out DIVERGENCE input={cs:?}: C={:?} Rust={:?}",
                std::str::from_utf8(&out[..ol as usize]),
                std::str::from_utf8(rtxt)
            );
            let (sd, _) = fc_call(adt_jsonb::builtins::fc_jsonb_send, m, [jb]);
            let rsnd = varlena_data(sd.expect("jsonb_send on a valid image"));
            assert!(
                rsnd == &snd[..sl as usize],
                "jsonb_send DIVERGENCE input={cs:?}: C len {sl} Rust len {}",
                rsnd.len()
            );
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                crc == rc,
                "jsonb_in VERDICT DIVERGENCE input={cs:?}: C=rc {crc} Rust=Err(class {rc} {})",
                e.message
            );
        }
    }
}

fn recv_arm(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    // wire text after the version byte flows through the same cstring-ish
    // parse; keep the corpus in the validated-input regime for text bytes
    // (the version/length plumbing itself is fully fuzzed).
    if payload.len() > 1 && std::str::from_utf8(&payload[1..]).is_err() {
        return;
    }
    if payload.contains(&0) {
        return;
    }
    let mut img = vec![0u8; CBUF];
    let mut il = 0i32;
    let crc = loop {
        let rc = unsafe {
            pg_diff_jsonb_recv(payload.as_ptr(), payload.len() as i32,
                               img.as_mut_ptr(), img.len() as i32, &mut il)
        };
        if rc != -2 {
            break rc;
        }
        grow(&mut img, il);
    };
    let cx = mcx::MemoryContext::new("jsonbio_fuzz");
    let m = cx.mcx();
    let Ok(mut si) = stringinfo::StringInfo::new_in(m) else { return };
    if si.append_bytes(payload).is_err() {
        return;
    }
    si.cursor = 0;
    let (r, _) = fc_call(
        adt_jsonb::builtins::fc_jsonb_recv,
        m,
        [Datum::from_usize(&mut si as *mut _ as usize)],
    );
    match r {
        Ok(jb) => {
            let rimg = varlena_data(jb);
            assert!(
                crc == 0 && rimg == &img[..il as usize],
                "jsonb_recv IMAGE DIVERGENCE wire={payload:?}: C=(rc {crc} len {il}) Rust=Ok(len {})",
                rimg.len()
            );
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                crc == rc,
                "jsonb_recv VERDICT DIVERGENCE wire={payload:?}: C=rc {crc} Rust=Err(class {rc} {})",
                e.message
            );
        }
    }
}

/// Derived-op prelude: run the shipped fc_jsonb_in; only proceed when it
/// succeeds (arm 0 owns parse-verdict parity).
fn parse_rust(m: mcx::Mcx<'_>, cs: &CString) -> Option<Datum> {
    let (r, _) = fc_call(
        adt_jsonb::builtins::fc_jsonb_in,
        m,
        [Datum::from_usize(cs.as_ptr() as usize)],
    );
    r.ok()
}

fn op1_arm(payload: &[u8]) {
    let Some((&op, rest)) = payload.split_first() else { return };
    let flag = i32::from((op >> 2) & 1); // strip_in_arrays plane (op 3 only)
    let op = (op % 4) as i32;
    let Some(cs) = take_json(rest) else { return };
    let cx = mcx::MemoryContext::new("jsonbio_fuzz");
    let m = cx.mcx();
    let Some(jb) = parse_rust(m, &cs) else { return };
    let mut out = vec![0u8; CBUF];
    let mut ol = 0i32;
    let crc = loop {
        let rc = unsafe {
            pg_diff_jsonb_op1(op, flag, cs.as_ptr(), out.as_mut_ptr(), out.len() as i32, &mut ol)
        };
        if rc != -2 {
            break rc;
        }
        grow(&mut out, ol);
    };
    assert!(
        crc < 100,
        "op{op} C PARSE DIVERGENCE (Rust parsed, C rc {crc}) input={cs:?}"
    );
    let f: PGFunction = match op {
        0 => adt_jsonb::builtins::fc_jsonb_typeof,
        1 => adt_jsonb::builtins::fc_jsonb_array_length,
        2 => adt_jsonb::builtins::fc_jsonb_pretty,
        _ => adt_jsonb::builtins::fc_jsonb_strip_nulls,
    };
    let (r, isnull) = if op == 3 {
        // catalog jsonb_strip_nulls is 2-arg (strip_in_arrays, PG17+)
        fc_call(f, m, [jb, Datum::from_bool(flag != 0)])
    } else {
        fc_call::<1>(f, m, [jb])
    };
    match r {
        Ok(d) => {
            assert!(!isnull, "op{op} unexpected SQL NULL input={cs:?}");
            let rust: Vec<u8> = match op {
                1 => d.as_i32().to_le_bytes().to_vec(),
                _ => varlena_data(d).to_vec(),
            };
            assert!(
                crc == 0 && rust == out[..ol as usize],
                "op{op} VALUE DIVERGENCE input={cs:?}: C=(rc {crc} len {ol}) Rust len {}",
                rust.len()
            );
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                crc == rc,
                "op{op} VERDICT DIVERGENCE input={cs:?}: C=rc {crc} Rust=Err(class {rc} {})",
                e.message
            );
        }
    }
}

fn cast_arm(payload: &[u8]) {
    let Some((&which, rest)) = payload.split_first() else { return };
    let which = (which % 7) as i32;
    let Some(cs) = take_json(rest) else { return };
    let cx = mcx::MemoryContext::new("jsonbio_fuzz");
    let m = cx.mcx();
    let Some(jb) = parse_rust(m, &cs) else { return };
    let mut out = vec![0u8; CBUF];
    let mut ol = 0i32;
    let crc = loop {
        let rc = unsafe {
            pg_diff_jsonb_cast(which, cs.as_ptr(), out.as_mut_ptr(), out.len() as i32, &mut ol)
        };
        if rc != -2 {
            break rc;
        }
        grow(&mut out, ol);
    };
    assert!(
        crc < 100,
        "cast{which} C PARSE DIVERGENCE (Rust parsed, C rc {crc}) input={cs:?}"
    );
    let f: PGFunction = match which {
        0 => adt_jsonb::builtins::fc_jsonb_bool,
        1 => adt_jsonb::builtins::fc_jsonb_int2,
        2 => adt_jsonb::builtins::fc_jsonb_int4,
        3 => adt_jsonb::builtins::fc_jsonb_int8,
        4 => adt_jsonb::builtins::fc_jsonb_float4,
        5 => adt_jsonb::builtins::fc_jsonb_float8,
        _ => adt_jsonb::builtins::fc_jsonb_numeric,
    };
    let (r, isnull) = fc_call(f, m, [jb]);
    match r {
        Ok(d) => {
            if isnull {
                assert!(
                    crc == -1,
                    "cast{which} NULL DIVERGENCE input={cs:?}: C=rc {crc} Rust=SQL NULL"
                );
                return;
            }
            let rust: Vec<u8> = match which {
                0 => vec![u8::from(d.as_bool())],
                1 => d.as_i16().to_le_bytes().to_vec(),
                2 => d.as_i32().to_le_bytes().to_vec(),
                3 => d.as_i64().to_le_bytes().to_vec(),
                4 => d.as_f32().to_bits().to_le_bytes().to_vec(),
                5 => d.as_f64().to_bits().to_le_bytes().to_vec(),
                _ => {
                    // numeric: compare via the shipped numeric_out text
                    let img = varlena_data(d);
                    let mut s = Vec::new();
                    adt_numeric::numeric_out_into(adt_numeric::Num::from_payload(img), &mut s);
                    s
                }
            };
            assert!(
                crc == 0 && rust == out[..ol as usize],
                "cast{which} VALUE DIVERGENCE input={cs:?}: C=(rc {crc} {:?}) Rust={rust:?}",
                &out[..ol.max(0) as usize]
            );
        }
        Err(e) => {
            let rc = err_class(&e);
            assert!(
                crc == rc,
                "cast{which} VERDICT DIVERGENCE input={cs:?}: C=rc {crc} Rust=Err(class {rc} {})",
                e.message
            );
        }
    }
}

fn build_arm(payload: &[u8]) {
    let isobj = payload.first().map_or(0, |b| i32::from(b & 1));
    let mut img = vec![0u8; 256];
    let mut il = 0i32;
    let crc = unsafe { pg_diff_jsonb_build_noargs(isobj, img.as_mut_ptr(), 256, &mut il) };
    let cx = mcx::MemoryContext::new("jsonbio_fuzz");
    let m = cx.mcx();
    let f: PGFunction = if isobj == 1 {
        adt_jsonb::builtins::fc_jsonb_build_object_noargs
    } else {
        adt_jsonb::builtins::fc_jsonb_build_array_noargs
    };
    let (r, _) = fc_call::<0>(f, m, []);
    let rimg = varlena_data(r.expect("build_noargs cannot fail"));
    assert!(
        crc == 0 && rimg == &img[..il as usize],
        "build_noargs({isobj}) IMAGE DIVERGENCE: C=(rc {crc} len {il}) Rust len {}",
        rimg.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/jsonbio_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/jsonbio_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                jsonbio_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // arm 0: parse+render, ok and error shapes
        jsonbio_diff(b"\x00{\"a\": [1, 2.5e2, true, null, \"x\\u00e9\"]}");
        jsonbio_diff(b"\x00{bad");
        jsonbio_diff(b"\x00[1,2,3]");
        jsonbio_diff(b"\x00\"scalar\"");
        jsonbio_diff(b"\x00-1.5e-100");
        jsonbio_diff(b"\x001e309"); // numeric holds it; float casts would err
        jsonbio_diff(b"\x00[\"\\u0000\"]"); // 22P05 untranslatable
        jsonbio_diff(b"\x00{\"k\":1,\"k\":2}"); // duplicate keys: last wins
        jsonbio_diff(b"\x00{\"b\":1,\"a\":2,\"aa\":3}"); // key sort order
        jsonbio_diff(b"\x00[\"\\ud83d\\ude00\"]"); // surrogate pair
        jsonbio_diff(b"\x00[\"\\ud83d\"]"); // lone surrogate: error
        // arm 1: recv ok / bad version / truncated / empty
        jsonbio_diff(b"\x01\x01[true]");
        jsonbio_diff(b"\x01\x09[true]");
        jsonbio_diff(b"\x01");
        // arm 2: single-doc ops
        jsonbio_diff(b"\x02\x00[1,2]"); // typeof
        jsonbio_diff(b"\x02\x00\"x\""); // typeof scalar
        jsonbio_diff(b"\x02\x01[1,2]"); // array_length
        jsonbio_diff(b"\x02\x01\"x\""); // array_length on scalar: 22023
        jsonbio_diff(b"\x02\x01{\"a\":1}"); // array_length on object: 22023
        jsonbio_diff(b"\x02\x02{\"a\":{\"b\":[1]}}"); // pretty
        jsonbio_diff(b"\x02\x03{\"a\":null,\"b\":{\"c\":null}}"); // strip_nulls
        jsonbio_diff(b"\x02\x03\"x\""); // strip_nulls scalar passthrough
        // arm 3: casts
        jsonbio_diff(b"\x03\x00true");
        jsonbio_diff(b"\x03\x01123");
        jsonbio_diff(b"\x03\x0140000"); // int2 overflow 22003
        jsonbio_diff(b"\x03\x02123.6"); // int4 rounds
        jsonbio_diff(b"\x03\x039223372036854775807");
        jsonbio_diff(b"\x03\x041.5");
        jsonbio_diff(b"\x03\x051e309"); // float8 overflow 22003
        jsonbio_diff(b"\x03\x062.500"); // numeric keeps scale
        jsonbio_diff(b"\x03\x06null"); // SQL NULL
        jsonbio_diff(b"\x03\x00\"x\""); // cannot cast: 22023
        jsonbio_diff(b"\x03\x03[1]"); // cannot cast array
        // arm 4: build noargs
        jsonbio_diff(b"\x04\x00");
        jsonbio_diff(b"\x04\x01");
    }
}
