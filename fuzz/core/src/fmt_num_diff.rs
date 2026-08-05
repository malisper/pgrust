//! fmt_num_diff: differential fuzz driver — shipped Rust `adt_formatting`
//! NUM (to_char numeric family + to_number) vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C. The C oracle lives in
//! csrc/pg_fmt_dch_io.c (single TU shared with fmt_dch_diff — the NUM SQL
//! entries call the static NUM_processor/NUM_cache there; the numeric.c /
//! int.c / int8.c / float.c / numutils.c dependency closure is vendored
//! verbatim in csrc/pg_numeric_*.inc / csrc/pg_formatting_num_18_3.inc).
//! csrc/pg_fmt_num_io.c is intentionally NOT compiled.
//!
//! Comparison planes: value bytes, error-verdict, errcode/sqlstate class;
//! message text out of scope. Environment pins identical to fmt_dch_diff
//! (UTF8, C collation, C locale — PGLC_localeconv is the process C locale
//! on both sides, so L/G/D pictures compare under C-locale lconv).
//!
//! Input layout: [selector][payload]; selector % 6 picks the arm:
//!   0 numeric_to_char   (oid 1772): u8 num_len + num decimal string + fmt.
//!     BOTH sides run the same pipeline numeric_in(num_str) -> to_char, so
//!     the numeric-input plane is compared too (a parse divergence surfaces
//!     as a verdict/class divergence).
//!   1 int4_to_char      (oid 1773): i32 LE + fmt.
//!   2 int8_to_char      (oid 1774): i64 LE + fmt.
//!   3 float4_to_char    (oid 1775): f32 LE bits + fmt.
//!   4 float8_to_char    (oid 1776): f64 LE bits + fmt.
//!   5 numeric_to_number (oid 1777): u16 LE input_len + input + fmt; the
//!     resulting numeric is rendered through each side's OWN numeric_out
//!     and the canonical decimal strings compared.
//!
//! Primary comparison = the fc_* wrapper plane (fmgr_builtins.rs), matching
//! the SQL surface; numeric values enter through io::numeric_in exactly as
//! C's DirectFunctionCall3(numeric_in, ...).
//!
//! SKIPPED (documented): interior-NUL / invalid-UTF-8 inputs (not
//! server-reachable); soft-error shapes (both sides hard-error, like
//! fmt_dch_diff).

use datum::{Datum, NullableDatum};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction};

extern "C" {
    fn pg_diff_errcode_get() -> i32;
    fn pg_diff_fmt_numeric_to_char(
        num_str: *const u8,
        num_len: i32,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_int4_to_char(
        v: i32,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_int8_to_char(
        v: i64,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_float4_to_char(
        v: f32,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_float8_to_char(
        v: f64,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_numeric_to_number(
        txt: *const u8,
        txt_len: i32,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
}

const COLLID: Oid = types_core::catalog::C_COLLATION_OID;
const FMT_MAX: usize = 256;
const OUT_CAP: usize = 16384;

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

/// Same class table as fmt_dch_diff (csrc/pg_fmt_dch_io.c owns it).
fn c_err_to_sqlstate(c: i32) -> Option<types_error::SqlState> {
    use types_error::*;
    Some(match c {
        101 => ERRCODE_SYNTAX_ERROR,
        102 => ERRCODE_INVALID_DATETIME_FORMAT,
        103 => ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
        104 => ERRCODE_DATETIME_FIELD_OVERFLOW,
        105 => ERRCODE_INVALID_TEXT_REPRESENTATION,
        106 => ERRCODE_FEATURE_NOT_SUPPORTED,
        107 => ERRCODE_INDETERMINATE_COLLATION,
        108 => ERRCODE_INVALID_PARAMETER_VALUE,
        109 => ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        110 => ERRCODE_DIVISION_BY_ZERO,
        111 => ERRCODE_INTERVAL_FIELD_OVERFLOW,
        114 => ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        115 => ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
        116 => ERRCODE_CONFIG_FILE_ERROR,
        117 => ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
        118 => ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
        119 => ERRCODE_INVALID_ARGUMENT_FOR_LOG,
        _ => return None,
    })
}

fn assert_err_parity(arm: &str, ctx: &dyn core::fmt::Debug, cerr: i32, e: &PgError) {
    match c_err_to_sqlstate(cerr) {
        Some(expect) => assert!(
            e.sqlstate == expect,
            "{arm} ERRCODE DIVERGENCE {ctx:?}: C class {cerr} vs Rust sqlstate {:?} ({})",
            e.sqlstate,
            e.message
        ),
        None => panic!(
            "{arm} ERRCODE DIVERGENCE {ctx:?}: C raised unmapped class {cerr}, Rust ({})",
            e.message
        ),
    }
}

fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(COLLID);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(flinfo, &mut fcinfo);
    (r, fcinfo.isnull)
}

fn text_image(payload: &[u8]) -> Vec<u8> {
    let len = payload.len() + 4;
    let mut v = Vec::with_capacity(len);
    v.extend_from_slice(&((len as u32) << 2).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

fn datum_text_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: wrapper varlena results are live 4B-header images in the
    // driver-owned result mcx for the duration of the exec.
    unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data()
}

fn text_ok(b: &[u8], cap: usize) -> bool {
    b.len() <= cap && !b.contains(&0) && core::str::from_utf8(b).is_ok()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn fmt_num_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let _oracle_guard = crate::fmt_dch_diff::oracle_lock();
    crate::fmt_dch_diff::pin_environment();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 6 {
        0 => numeric_to_char_diff(payload),
        1 => int_to_char_diff(payload, false),
        2 => int_to_char_diff(payload, true),
        3 => float_to_char_diff(payload, false),
        4 => float_to_char_diff(payload, true),
        _ => numeric_to_number_diff(payload),
    }
}

/// Shared verdict/value/error comparison for the to_char arms.
fn compare_tochar(
    arm: &str,
    ctx: &dyn core::fmt::Debug,
    cst: i32,
    cerr: i32,
    cbytes: &[u8],
    wres: &PgResult<Datum>,
    wnull: bool,
) {
    match (cst, wres) {
        (1, Ok(_)) => assert!(wnull, "{arm} NULL DIVERGENCE {ctx:?}: C NULL vs Rust non-null"),
        (0, Ok(d)) => {
            assert!(!wnull, "{arm} NULL DIVERGENCE {ctx:?}: Rust NULL vs C ok");
            let rbytes = datum_text_bytes(*d);
            assert!(
                rbytes == cbytes,
                "{arm} VALUE DIVERGENCE {ctx:?}: C={:?} Rust={:?}",
                String::from_utf8_lossy(cbytes),
                String::from_utf8_lossy(rbytes)
            );
        }
        (-1, Err(e)) => assert_err_parity(arm, ctx, cerr, e),
        _ => panic!(
            "{arm} VERDICT DIVERGENCE {ctx:?}: C status {cst} (err {cerr}) vs Rust {:?}",
            wres.as_ref().map(|_| ()).map_err(|e| e.message.clone())
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: numeric_to_char (oid 1772).
// ---------------------------------------------------------------------------

fn numeric_to_char_diff(payload: &[u8]) {
    let Some((&nlen, rest)) = payload.split_first() else {
        return;
    };
    let nlen = nlen as usize;
    if nlen > rest.len() {
        return;
    }
    let (num_str, fmt) = rest.split_at(nlen);
    if !text_ok(num_str, 64) || !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut out = [0u8; OUT_CAP];
    let mut out_len: i32 = 0;
    let cst = unsafe {
        pg_diff_fmt_numeric_to_char(
            num_str.as_ptr(),
            num_str.len() as i32,
            fmt.as_ptr(),
            fmt.len() as i32,
            out.as_mut_ptr(),
            OUT_CAP as i32,
            &mut out_len,
        )
    };
    if cst == -2 {
        return;
    }
    let cerr = c_errcode();
    let cbytes = &out[..out_len.max(0) as usize];

    let ctxmgr = mcx::MemoryContext::new("fmt_num_diff");
    let m = ctxmgr.mcx();

    // Same pipeline as C: numeric_in then the fc to_char wrapper.
    let s = core::str::from_utf8(num_str).unwrap();
    let img = match adt_numeric::io::numeric_in(s, -1, None) {
        Ok(Some(img)) => img,
        Ok(None) => unreachable!("hard-error shape returned soft escape"),
        Err(e) => {
            // C must have failed (in its numeric_in) with the same class.
            assert!(
                cst == -1,
                "numeric_to_char INPUT DIVERGENCE num={s:?}: Rust numeric_in Err({}) vs C status {cst}",
                e.message
            );
            assert_err_parity("numeric_to_char(numeric_in)", &(s, fmt), cerr, &e);
            return;
        }
    };
    let fmt_img = text_image(fmt);
    let (wres, wnull) = fc_call::<2>(
        adt_formatting::fmgr_builtins::fc_numeric_to_char,
        None,
        m,
        [
            Datum::from_usize(img.as_bytes().as_ptr() as usize),
            Datum::from_usize(fmt_img.as_ptr() as usize),
        ],
    );
    compare_tochar("numeric_to_char", &(s, fmt), cst, cerr, cbytes, &wres, wnull);
}

// ---------------------------------------------------------------------------
// Arms 1/2: int4_to_char / int8_to_char (oids 1773 / 1774).
// ---------------------------------------------------------------------------

fn int_to_char_diff(payload: &[u8], wide: bool) {
    let arm = if wide { "int8_to_char" } else { "int4_to_char" };
    let need = if wide { 8 } else { 4 };
    if payload.len() < need {
        return;
    }
    let (v64, v32);
    let fmt = if wide {
        v64 = i64::from_le_bytes(payload[..8].try_into().unwrap());
        v32 = 0;
        &payload[8..]
    } else {
        v32 = i32::from_le_bytes(payload[..4].try_into().unwrap());
        v64 = v32 as i64;
        &payload[4..]
    };
    if !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut out = [0u8; OUT_CAP];
    let mut out_len: i32 = 0;
    let cst = unsafe {
        if wide {
            pg_diff_fmt_int8_to_char(
                v64,
                fmt.as_ptr(),
                fmt.len() as i32,
                out.as_mut_ptr(),
                OUT_CAP as i32,
                &mut out_len,
            )
        } else {
            pg_diff_fmt_int4_to_char(
                v32,
                fmt.as_ptr(),
                fmt.len() as i32,
                out.as_mut_ptr(),
                OUT_CAP as i32,
                &mut out_len,
            )
        }
    };
    if cst == -2 {
        return;
    }
    let cerr = c_errcode();
    let cbytes = &out[..out_len.max(0) as usize];

    let ctxmgr = mcx::MemoryContext::new("fmt_num_diff");
    let m = ctxmgr.mcx();
    let fmt_img = text_image(fmt);
    let (f, a0): (PGFunction, Datum) = if wide {
        (adt_formatting::fmgr_builtins::fc_int8_to_char, Datum::from_i64(v64))
    } else {
        (adt_formatting::fmgr_builtins::fc_int4_to_char, Datum::from_i32(v32))
    };
    let (wres, wnull) = fc_call::<2>(f, None, m, [a0, Datum::from_usize(fmt_img.as_ptr() as usize)]);
    compare_tochar(arm, &(v64, fmt), cst, cerr, cbytes, &wres, wnull);
}

// ---------------------------------------------------------------------------
// Arms 3/4: float4_to_char / float8_to_char (oids 1775 / 1776).
// ---------------------------------------------------------------------------

fn float_to_char_diff(payload: &[u8], wide: bool) {
    let arm = if wide { "float8_to_char" } else { "float4_to_char" };
    let need = if wide { 8 } else { 4 };
    if payload.len() < need {
        return;
    }
    let (f64v, f32v);
    let fmt = if wide {
        f64v = f64::from_le_bytes(payload[..8].try_into().unwrap());
        f32v = 0.0f32;
        &payload[8..]
    } else {
        f32v = f32::from_le_bytes(payload[..4].try_into().unwrap());
        f64v = 0.0f64;
        &payload[4..]
    };
    if !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut out = [0u8; OUT_CAP];
    let mut out_len: i32 = 0;
    let cst = unsafe {
        if wide {
            pg_diff_fmt_float8_to_char(
                f64v,
                fmt.as_ptr(),
                fmt.len() as i32,
                out.as_mut_ptr(),
                OUT_CAP as i32,
                &mut out_len,
            )
        } else {
            pg_diff_fmt_float4_to_char(
                f32v,
                fmt.as_ptr(),
                fmt.len() as i32,
                out.as_mut_ptr(),
                OUT_CAP as i32,
                &mut out_len,
            )
        }
    };
    if cst == -2 {
        return;
    }
    let cerr = c_errcode();
    let cbytes = &out[..out_len.max(0) as usize];

    let ctxmgr = mcx::MemoryContext::new("fmt_num_diff");
    let m = ctxmgr.mcx();
    let fmt_img = text_image(fmt);
    let (f, a0): (PGFunction, Datum) = if wide {
        (adt_formatting::fmgr_builtins::fc_float8_to_char, Datum::from_f64(f64v))
    } else {
        (adt_formatting::fmgr_builtins::fc_float4_to_char, Datum::from_f32(f32v))
    };
    let (wres, wnull) = fc_call::<2>(f, None, m, [a0, Datum::from_usize(fmt_img.as_ptr() as usize)]);
    compare_tochar(
        arm,
        &(if wide { f64v.to_bits() } else { f32v.to_bits() as u64 }, fmt),
        cst,
        cerr,
        cbytes,
        &wres,
        wnull,
    );
}

// ---------------------------------------------------------------------------
// Arm 5: numeric_to_number (oid 1777).
// ---------------------------------------------------------------------------

fn numeric_to_number_diff(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let ilen = u16::from_le_bytes(payload[..2].try_into().unwrap()) as usize;
    let rest = &payload[2..];
    if ilen > rest.len() {
        return;
    }
    let (input, fmt) = rest.split_at(ilen);
    if !text_ok(input, 256) || !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut out = [0u8; OUT_CAP];
    let mut out_len: i32 = 0;
    let cst = unsafe {
        pg_diff_fmt_numeric_to_number(
            input.as_ptr(),
            input.len() as i32,
            fmt.as_ptr(),
            fmt.len() as i32,
            out.as_mut_ptr(),
            OUT_CAP as i32,
            &mut out_len,
        )
    };
    if cst == -2 {
        return;
    }
    let cerr = c_errcode();
    let cbytes = &out[..out_len.max(0) as usize];

    let ctxmgr = mcx::MemoryContext::new("fmt_num_diff");
    let m = ctxmgr.mcx();
    let input_img = text_image(input);
    let fmt_img = text_image(fmt);
    let (wres, wnull) = fc_call::<2>(
        adt_formatting::fmgr_builtins::fc_numeric_to_number,
        None,
        m,
        [
            Datum::from_usize(input_img.as_ptr() as usize),
            Datum::from_usize(fmt_img.as_ptr() as usize),
        ],
    );
    match (cst, &wres) {
        (1, Ok(_)) => assert!(
            wnull,
            "numeric_to_number NULL DIVERGENCE input={input:?} fmt={fmt:?}"
        ),
        (0, Ok(d)) => {
            assert!(
                !wnull,
                "numeric_to_number NULL DIVERGENCE input={input:?} fmt={fmt:?}: Rust NULL vs C ok"
            );
            // Render the Rust-side numeric through the shipped numeric_out.
            let payload_bytes = datum_text_bytes(*d); /* numeric payload past 4B header */
            let num = adt_numeric::Num::from_payload(payload_bytes);
            let mut rbuf = Vec::new();
            adt_numeric::io::numeric_out_into(num, &mut rbuf);
            assert!(
                rbuf == cbytes,
                "numeric_to_number VALUE DIVERGENCE input={:?} fmt={:?}: C={:?} Rust={:?}",
                String::from_utf8_lossy(input),
                String::from_utf8_lossy(fmt),
                String::from_utf8_lossy(cbytes),
                String::from_utf8_lossy(&rbuf)
            );
        }
        (-1, Err(e)) => assert_err_parity("numeric_to_number", &(input, fmt), cerr, e),
        _ => panic!(
            "numeric_to_number VERDICT DIVERGENCE input={:?} fmt={:?}: C status {cst} (err {cerr}) vs Rust {:?}",
            String::from_utf8_lossy(input),
            String::from_utf8_lossy(fmt),
            wres.as_ref().map(|_| ()).map_err(|e| e.message.clone())
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sel: u8, payload: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(payload);
        fmt_num_diff(&v);
    }

    fn nc(num: &[u8], fmt: &[u8]) -> Vec<u8> {
        let mut p = vec![num.len() as u8];
        p.extend_from_slice(num);
        p.extend_from_slice(fmt);
        p
    }

    #[test]
    fn smoke_numeric_to_char() {
        run(0, &nc(b"12345.6789", b"99999.9999"));
        run(0, &nc(b"-1.5", b"FM9990.099"));
        run(0, &nc(b"0.5", b"SG990.9MI PL"));
        run(0, &nc(b"12345", b"L99G999D99"));
        run(0, &nc(b"485", b"RN rn"));
        run(0, &nc(b"12345.67", b"9.99EEEE"));
        run(0, &nc(b"NaN", b"999"));
        run(0, &nc(b"Infinity", b"999"));
        run(0, &nc(b"1e100", b"9V99"));
        run(0, &nc(b"junk", b"999")); // numeric_in error parity
        run(0, &nc(b"123", b"999TH th B"));
        run(0, &nc(b"0", b"FM9999999999999999.999999999999999"));
    }

    #[test]
    fn smoke_int_to_char() {
        let mut p = 485i32.to_le_bytes().to_vec();
        p.extend_from_slice(b"RN 999TH PR");
        run(1, &p);
        let mut p = i32::MIN.to_le_bytes().to_vec();
        p.extend_from_slice(b"9999999999PR");
        run(1, &p);
        let mut p = i64::MIN.to_le_bytes().to_vec();
        p.extend_from_slice(b"9999999999999999999S");
        run(2, &p);
        let mut p = 0i64.to_le_bytes().to_vec();
        p.extend_from_slice(b"B9999");
        run(2, &p);
    }

    #[test]
    fn smoke_float_to_char() {
        let mut p = 1.5f32.to_le_bytes().to_vec();
        p.extend_from_slice(b"9.99EEEE 990.099");
        run(3, &p);
        let mut p = 1.5f64.to_le_bytes().to_vec();
        p.extend_from_slice(b"9.99EEEE 990.099");
        run(4, &p);
        let mut p = f64::NAN.to_le_bytes().to_vec();
        p.extend_from_slice(b"999.9");
        run(4, &p);
        let mut p = f64::INFINITY.to_le_bytes().to_vec();
        p.extend_from_slice(b"9.9EEEE");
        run(4, &p);
    }

    #[test]
    fn smoke_numeric_to_number() {
        let mk = |input: &[u8], fmt: &[u8]| {
            let mut p = (input.len() as u16).to_le_bytes().to_vec();
            p.extend_from_slice(input);
            p.extend_from_slice(fmt);
            p
        };
        run(5, &mk(b"12,345.67", b"99G999D99"));
        run(5, &mk(b"-1234", b"S9999"));
        run(5, &mk(b"<1234>", b"9999PR"));
        run(5, &mk(b"485", b"RN"));
        run(5, &mk(b"1.2e3", b"9.9EEEE"));
        run(5, &mk(b"junk", b"999"));
        run(5, &mk(b"$1234", b"L9999"));
    }

    /// Replay every committed seed through the driver.
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/fmt_num_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/fmt_num_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                fmt_num_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}
