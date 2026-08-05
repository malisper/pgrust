//! Target: numutils_diff — the p1-laneaj adt/numutils batch (pg_strtoint16/
//! 32/64(_safe), uint32in_subr, uint64in_subr, pg_ultoa_n, pg_ulltoa_n,
//! pg_ltoa, pg_lltoa, pg_itoa, pg_ultostr_zeropad, pg_ultostr) shipped Rust
//! vs vendored PostgreSQL 18.3 C (csrc/pg_numutils.c) in-process.
//!
//! Comparison planes (harness contract):
//!   - parse family: returned value (on success) + error-verdict (ok vs soft
//!     error) + errcode/SQLSTATE (22003 out-of-range vs 22P02
//!     invalid-syntax); uint*in_subr additionally the remaining-tail offset
//!     on the endloc arm.
//!   - emit family: emitted bytes + returned length, exactly.
//! Any mismatch panics, so a libFuzzer crash artifact is a C/Rust divergence
//! reproducer.
//!
//! Domain carves (documented, ratified non-surfaces):
//!   - C strings cannot represent interior NUL: every text input is truncated
//!     at the first NUL before BOTH sides (the strfam_diff carve).
//!   - The shipped Rust parse entry points take &str (type-enforced valid-
//!     UTF-8 domain); non-UTF-8 fuzz bytes are skipped for them (the C side
//!     accepts any bytes, but the shipped Rust surface cannot be called with
//!     them — the domain difference is the TYPE, not logic). Same carve as
//!     strfam's &str entries.
//!   - uint32in_subr/uint64in_subr parse core: the C oracle's is the platform
//!     strtoul(base 0), exactly as real PostgreSQL (which defers to libc);
//!     ground-truthing vs glibc is the postgres:18.3 Docker replay step.
//!   - typname is pinned to "oid" on both sides: it only enters the message
//!     text, which is out of the harness contract.
//!   - pg_ultostr_zeropad minwidth: C Asserts minwidth > 0 (compiled out in
//!     release) and both sides' callers only pass small positive widths; the
//!     driver draws minwidth from 1..=15 and rejects the 0 byte (an invalid
//!     minwidth is C UB — unbounded left-pad — not a comparable behavior).

use std::ffi::CString;
use std::os::raw::{c_char, c_int};

use types_error::{
    PgError, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

extern "C" {
    fn pg_diff_numutils_strtoint16(s: *const c_char, out: *mut i16) -> c_int;
    fn pg_diff_numutils_strtoint32(s: *const c_char, out: *mut i32) -> c_int;
    fn pg_diff_numutils_strtoint64(s: *const c_char, out: *mut i64) -> c_int;
    fn pg_diff_numutils_uint32in_subr(
        s: *const c_char,
        use_endloc: c_int,
        out: *mut u32,
        end_off: *mut usize,
    ) -> c_int;
    fn pg_diff_numutils_uint64in_subr(
        s: *const c_char,
        use_endloc: c_int,
        out: *mut u64,
        end_off: *mut usize,
    ) -> c_int;
    fn pg_diff_numutils_ultoa_n(value: u32, a: *mut c_char) -> c_int;
    fn pg_diff_numutils_ulltoa_n(value: u64, a: *mut c_char) -> c_int;
    fn pg_diff_numutils_ltoa(value: i32, a: *mut c_char) -> c_int;
    fn pg_diff_numutils_lltoa(value: i64, a: *mut c_char) -> c_int;
    fn pg_diff_numutils_itoa(i: i16, a: *mut c_char) -> c_int;
    fn pg_diff_numutils_ultostr(a: *mut c_char, value: u32) -> c_int;
    fn pg_diff_numutils_ultostr_zeropad(a: *mut c_char, value: u32, minwidth: i32) -> c_int;
}

/* C errcode ids in pg_numutils.c */
const C_ERR_INVALID_SYNTAX: c_int = 1; /* 22P02 */
const C_ERR_OUT_OF_RANGE: c_int = 2; /* 22003 */

/// Map the C oracle's errcode id to the shipped SQLSTATE and assert the
/// Rust-side PgError carries it.
fn assert_errcode(c_rc: c_int, e: &PgError, what: &str, input: &str) {
    let want = match c_rc {
        C_ERR_INVALID_SYNTAX => ERRCODE_INVALID_TEXT_REPRESENTATION,
        C_ERR_OUT_OF_RANGE => ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        _ => panic!("{what}: unknown C errcode {c_rc} on {input:?}"),
    };
    assert_eq!(
        e.sqlstate(),
        want,
        "{what}: SQLSTATE diverged on {input:?} (C errcode id {c_rc})"
    );
}

/// NUL-truncate (the C-string domain; see header) then require UTF-8 (the
/// shipped &str domain; see header).
fn parse_input(bytes: &[u8]) -> Option<(&str, CString)> {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = std::str::from_utf8(&bytes[..end]).ok()?;
    let c = CString::new(s).expect("truncated at first NUL");
    Some((s, c))
}

macro_rules! diff_strtoint {
    ($name:ident, $ity:ty, $cfn:ident, $rust_safe:path, $rust_plain:path, $what:literal) => {
        fn $name(payload: &[u8]) {
            let Some((s, c_in)) = parse_input(payload) else {
                return;
            };
            let mut c_val: $ity = 0;
            // SAFETY: NUL-terminated input; out is a valid slot.
            let c_rc = unsafe { $cfn(c_in.as_ptr(), &mut c_val) };

            // Soft-error arm (escontext with details): value + verdict + code.
            let mut esc = SoftErrorContext::new(true);
            let r = $rust_safe(s, Some(&mut esc)).expect("soft path never hard-errors");
            // Hard-error arm (escontext = None): same verdict + code.
            let r_plain = $rust_plain(s);

            if c_rc == 0 {
                assert!(
                    !esc.error_occurred(),
                    "{}: verdict diverged on {s:?}: C ok={c_val}, Rust soft error {:?}",
                    $what,
                    esc.error()
                );
                assert_eq!(r, c_val, "{}: value diverged on {s:?}", $what);
                match r_plain {
                    Ok(v) => assert_eq!(v, c_val, "{}: plain value diverged on {s:?}", $what),
                    Err(e) => panic!("{}: plain verdict diverged on {s:?}: {e}", $what),
                }
            } else {
                let e = esc
                    .take_error()
                    .unwrap_or_else(|| panic!("{}: verdict diverged on {s:?}: C errcode {c_rc}, Rust ok={r}", $what));
                assert_errcode(c_rc, &e, $what, s);
                // Error-location plane (client-keyed: pg8000 pins F/R on
                // integer-input errors — see numutils crate comment). The C
                // reference is __func__ inside vendored numutils.c, which is
                // exactly the *_safe entry name; pinned by rule here.
                let loc = e.location().unwrap_or_else(|| {
                    panic!("{}: error location missing on {s:?}", $what)
                });
                assert_eq!(
                    loc.funcname.as_deref(),
                    Some(concat!($what, "_safe")),
                    "{}: error R (funcname) diverged on {s:?}",
                    $what
                );
                assert_eq!(
                    loc.filename.as_deref(),
                    Some("numutils.c"),
                    "{}: error F (filename) diverged on {s:?}",
                    $what
                );
                // ereturn dummy: C returns 0 on the soft path; Rust matches.
                assert_eq!(r, 0, "{}: soft-error dummy diverged on {s:?}", $what);
                match r_plain {
                    Ok(v) => panic!("{}: plain verdict diverged on {s:?}: C errcode {c_rc}, Rust ok={v}", $what),
                    Err(e) => assert_errcode(c_rc, &e, $what, s),
                }
            }
        }
    };
}

diff_strtoint!(
    diff_strtoint16,
    i16,
    pg_diff_numutils_strtoint16,
    numutils::pg_strtoint16_safe,
    numutils::pg_strtoint16,
    "pg_strtoint16"
);
diff_strtoint!(
    diff_strtoint32,
    i32,
    pg_diff_numutils_strtoint32,
    numutils::pg_strtoint32_safe,
    numutils::pg_strtoint32,
    "pg_strtoint32"
);
diff_strtoint!(
    diff_strtoint64,
    i64,
    pg_diff_numutils_strtoint64,
    numutils::pg_strtoint64_safe,
    numutils::pg_strtoint64,
    "pg_strtoint64"
);

macro_rules! diff_uintin_subr {
    ($name:ident, $uty:ty, $cfn:ident, $rust:path, $what:literal) => {
        fn $name(payload: &[u8]) {
            let Some((&flags, rest)) = payload.split_first() else {
                return;
            };
            let endloc = flags & 1 != 0;
            let Some((s, c_in)) = parse_input(rest) else {
                return;
            };
            let mut c_val: $uty = 0;
            let mut c_end: usize = 0;
            // SAFETY: NUL-terminated input; out slots valid.
            let c_rc = unsafe { $cfn(c_in.as_ptr(), endloc as c_int, &mut c_val, &mut c_end) };

            let mut esc = SoftErrorContext::new(true);
            let (r_val, r_rest) =
                $rust(s, endloc, "oid", Some(&mut esc)).expect("soft path never hard-errors");

            if c_rc == 0 {
                assert!(
                    !esc.error_occurred(),
                    "{}: verdict diverged on {s:?} (endloc={endloc}): C ok={c_val}, Rust soft error {:?}",
                    $what,
                    esc.error()
                );
                assert_eq!(r_val, c_val, "{}: value diverged on {s:?} (endloc={endloc})", $what);
                if endloc {
                    let r_off = s.len() - r_rest.len();
                    assert_eq!(
                        r_off, c_end,
                        "{}: endloc tail offset diverged on {s:?}",
                        $what
                    );
                }
            } else {
                let e = esc.take_error().unwrap_or_else(|| {
                    panic!(
                        "{}: verdict diverged on {s:?} (endloc={endloc}): C errcode {c_rc}, Rust ok={r_val}",
                        $what
                    )
                });
                assert_errcode(c_rc, &e, $what, s);
            }
        }
    };
}

diff_uintin_subr!(
    diff_uint32in_subr,
    u32,
    pg_diff_numutils_uint32in_subr,
    numutils::uint32in_subr,
    "uint32in_subr"
);
diff_uintin_subr!(
    diff_uint64in_subr,
    u64,
    pg_diff_numutils_uint64in_subr,
    numutils::uint64in_subr,
    "uint64in_subr"
);

/// Little-endian value draw, zero-padded when the payload is short (so every
/// input length reaches the emit family).
fn le_bytes<const N: usize>(payload: &[u8]) -> [u8; N] {
    let mut b = [0u8; N];
    let n = payload.len().min(N);
    b[..n].copy_from_slice(&payload[..n]);
    b
}

const EMIT_BUF: usize = 40;

fn diff_ultoa_n(v: u32) {
    let mut c_buf = [0u8; EMIT_BUF];
    // SAFETY: buffer holds any u32 decimal image (max 10 bytes).
    let c_len = unsafe { pg_diff_numutils_ultoa_n(v, c_buf.as_mut_ptr().cast()) } as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_ultoa_n(v, &mut r_buf);
    assert_eq!(r_len, c_len, "pg_ultoa_n: length diverged on {v}");
    assert_eq!(&r_buf[..r_len], &c_buf[..c_len], "pg_ultoa_n: bytes diverged on {v}");
}

fn diff_ulltoa_n(v: u64) {
    let mut c_buf = [0u8; EMIT_BUF];
    // SAFETY: buffer holds any u64 decimal image (max 20 bytes).
    let c_len = unsafe { pg_diff_numutils_ulltoa_n(v, c_buf.as_mut_ptr().cast()) } as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_ulltoa_n(v, &mut r_buf);
    assert_eq!(r_len, c_len, "pg_ulltoa_n: length diverged on {v}");
    assert_eq!(&r_buf[..r_len], &c_buf[..c_len], "pg_ulltoa_n: bytes diverged on {v}");
}

/// C pg_ltoa/pg_lltoa/pg_itoa NUL-terminate; the shipped Rust ones don't
/// (documented crate difference). Compare returned length + the length-many
/// emitted bytes, and assert the C NUL as a sanity floor.
fn diff_ltoa(v: i32) {
    let mut c_buf = [0xAAu8; EMIT_BUF];
    // SAFETY: buffer holds sign + any i32 decimal image + NUL (max 12 bytes).
    let c_len = unsafe { pg_diff_numutils_ltoa(v, c_buf.as_mut_ptr().cast()) } as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_ltoa(v, &mut r_buf);
    assert_eq!(r_len, c_len, "pg_ltoa: length diverged on {v}");
    assert_eq!(&r_buf[..r_len], &c_buf[..c_len], "pg_ltoa: bytes diverged on {v}");
    assert_eq!(c_buf[c_len], 0, "pg_ltoa: C oracle NUL missing on {v}");
}

fn diff_lltoa(v: i64) {
    let mut c_buf = [0xAAu8; EMIT_BUF];
    // SAFETY: buffer holds sign + any i64 decimal image + NUL (max 21 bytes).
    let c_len = unsafe { pg_diff_numutils_lltoa(v, c_buf.as_mut_ptr().cast()) } as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_lltoa(v, &mut r_buf);
    assert_eq!(r_len, c_len, "pg_lltoa: length diverged on {v}");
    assert_eq!(&r_buf[..r_len], &c_buf[..c_len], "pg_lltoa: bytes diverged on {v}");
    assert_eq!(c_buf[c_len], 0, "pg_lltoa: C oracle NUL missing on {v}");
}

fn diff_itoa(v: i16) {
    let mut c_buf = [0xAAu8; EMIT_BUF];
    // SAFETY: buffer holds sign + any i16 decimal image + NUL (max 7 bytes).
    let c_len = unsafe { pg_diff_numutils_itoa(v, c_buf.as_mut_ptr().cast()) } as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_itoa(v, &mut r_buf);
    assert_eq!(r_len, c_len, "pg_itoa: length diverged on {v}");
    assert_eq!(&r_buf[..r_len], &c_buf[..c_len], "pg_itoa: bytes diverged on {v}");
    assert_eq!(c_buf[c_len], 0, "pg_itoa: C oracle NUL missing on {v}");
}

fn diff_ultostr(v: u32) {
    let mut c_buf = [0u8; EMIT_BUF];
    // SAFETY: buffer holds any u32 decimal image.
    let c_len = unsafe { pg_diff_numutils_ultostr(c_buf.as_mut_ptr().cast(), v) } as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_ultostr(&mut r_buf, v);
    assert_eq!(r_len, c_len, "pg_ultostr: length diverged on {v}");
    assert_eq!(&r_buf[..r_len], &c_buf[..c_len], "pg_ultostr: bytes diverged on {v}");
}

fn diff_ultostr_zeropad(v: u32, minwidth: i32) {
    debug_assert!((1..=15).contains(&minwidth));
    let mut c_buf = [0u8; EMIT_BUF];
    // SAFETY: buffer holds max(minwidth, 10) bytes; minwidth <= 15.
    let c_len =
        unsafe { pg_diff_numutils_ultostr_zeropad(c_buf.as_mut_ptr().cast(), v, minwidth) }
            as usize;
    let mut r_buf = [0u8; EMIT_BUF];
    let r_len = numutils::pg_ultostr_zeropad(&mut r_buf, v, minwidth);
    assert_eq!(r_len, c_len, "pg_ultostr_zeropad: length diverged on ({v}, {minwidth})");
    assert_eq!(
        &r_buf[..r_len],
        &c_buf[..c_len],
        "pg_ultostr_zeropad: bytes diverged on ({v}, {minwidth})"
    );
}

/// Entry: first byte selects the family member (float_in_diff pattern).
pub fn numutils_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    match sel % 12 {
        0 => diff_strtoint16(rest),
        1 => diff_strtoint32(rest),
        2 => diff_strtoint64(rest),
        3 => diff_uint32in_subr(rest),
        4 => diff_uint64in_subr(rest),
        5 => diff_ultoa_n(u32::from_le_bytes(le_bytes::<4>(rest))),
        6 => diff_ulltoa_n(u64::from_le_bytes(le_bytes::<8>(rest))),
        7 => diff_ltoa(i32::from_le_bytes(le_bytes::<4>(rest))),
        8 => diff_lltoa(i64::from_le_bytes(le_bytes::<8>(rest))),
        9 => diff_itoa(i16::from_le_bytes(le_bytes::<2>(rest))),
        10 => diff_ultostr(u32::from_le_bytes(le_bytes::<4>(rest))),
        _ => {
            // value (4 LE bytes) + minwidth byte; 0 rejected (see header),
            // the rest folded into 1..=15.
            let v = u32::from_le_bytes(le_bytes::<4>(rest));
            let mw = rest.get(4).copied().unwrap_or(2);
            if mw == 0 {
                return;
            }
            diff_ultostr_zeropad(v, i32::from((mw - 1) % 15 + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CI replay rail: every committed corpus unit replays clean through the
    /// differential on stable (the banked corpus is the regression suite).
    #[test]
    fn numutils_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/numutils_diff");
        let mut n = 0usize;
        for entry in std::fs::read_dir(dir).expect("committed corpus present") {
            let p = entry.unwrap().path();
            if p.is_file() {
                numutils_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n > 100, "corpus unexpectedly small: {n} units");
    }

    /// Deterministic smoke: every selector, both verdict planes each.
    #[test]
    fn numutils_smoke() {
        let _serial = crate::c_oracle_serial();
        let strings: &[&[u8]] = &[
            b"0", b"-0", b"+1", b" 42 ", b"0x7fff", b"0o777", b"0b1010", b"1_000", b"_1", b"1_",
            b"32767", b"32768", b"-32768", b"-32769", b"2147483647", b"2147483648",
            b"-2147483648", b"-2147483649", b"9223372036854775807", b"9223372036854775808",
            b"-9223372036854775808", b"-9223372036854775809", b"18446744073709551615",
            b"18446744073709551616", b"99999999999999999999", b"0x", b"0x_", b"0xdeadbeefcafebabe0",
            b"\x0b7\x0b", b" \t\n\r\x0c-000123 ", b"", b" ", b"+", b"-", b"07", b"0x10zzz",
            b"12\x0034",
        ];
        for sel in 0u8..=4 {
            for s in strings {
                let mut d = vec![sel];
                if sel >= 3 {
                    d.push(0); // endloc = false
                }
                d.extend_from_slice(s);
                numutils_diff(&d);
                if sel >= 3 {
                    let mut d = vec![sel, 1]; // endloc = true
                    d.extend_from_slice(s);
                    numutils_diff(&d);
                }
            }
        }
        // Emit family: representative draws incl. short payloads.
        for sel in 5u8..=11 {
            numutils_diff(&[sel]);
            numutils_diff(&[sel, 0x39]);
            numutils_diff(&[sel, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2]);
            numutils_diff(&[sel, 0x00, 0xCA, 0x9A, 0x3B, 11]); // 10^9, mw 11
        }
        // zeropad minwidth 0 reject byte + the mw==2 fast arm.
        numutils_diff(&[11, 5, 0, 0, 0, 0]);
        numutils_diff(&[11, 99, 0, 0, 0, 2]);
    }

    /// a0 EXHAUSTIVE-DIFF (every cargo test): ALL 2^16 i16 through pg_itoa
    /// vs C pg_itoa, plus parse-back through pg_strtoint16 on both sides
    /// (canonical roundtrip).
    #[test]
    fn test_itoa_exhaustive_i16() {
        let _serial = crate::c_oracle_serial();
        let mut r_buf = [0u8; EMIT_BUF];
        for v in i16::MIN..=i16::MAX {
            diff_itoa(v);
            let r_len = numutils::pg_itoa(v, &mut r_buf);
            let s = std::str::from_utf8(&r_buf[..r_len]).unwrap();
            assert_eq!(numutils::pg_strtoint16(s).unwrap(), v, "Rust roundtrip on {v}");
            let c_in = CString::new(s).unwrap();
            let mut c_val = 0i16;
            // SAFETY: NUL-terminated input; out slot valid.
            let c_rc = unsafe { pg_diff_numutils_strtoint16(c_in.as_ptr(), &mut c_val) };
            assert_eq!(c_rc, 0, "C roundtrip verdict on {v}");
            assert_eq!(c_val, v, "C roundtrip on {v}");
        }
    }

    /// Canonical-value parse sweep: stride-sampled i32 plus INT16/INT32
    /// boundaries +-2 and powers of 10 +-1, formatted via C pg_ltoa and
    /// parsed through strtoint32 on both sides; the INT16/INT64 boundaries
    /// go through strtoint16/strtoint64 too.
    #[test]
    fn test_strtoint_canonical_i32_sampled() {
        let _serial = crate::c_oracle_serial();
        let mut vals: Vec<i64> = Vec::new();
        // stride sample of i32 (coprime stride => ~42k spread draws)
        let mut v = i32::MIN as i64;
        while v <= i32::MAX as i64 {
            vals.push(v);
            v += 100_003;
        }
        for b in [i16::MIN as i64, i16::MAX as i64, i32::MIN as i64, i32::MAX as i64] {
            for d in -2i64..=2 {
                vals.push(b + d);
            }
        }
        let mut p = 1i128;
        for _ in 0..=18 {
            for d in -1i128..=1 {
                if let Ok(v) = i64::try_from(p + d) {
                    vals.push(v);
                    vals.push(-v);
                }
            }
            p *= 10;
        }
        let mut c_buf = [0u8; EMIT_BUF];
        for &v in &vals {
            // strtoint32 leg over the C-canonical i32 image
            if let Ok(v32) = i32::try_from(v) {
                // SAFETY: buffer holds sign + image + NUL.
                let c_len =
                    unsafe { pg_diff_numutils_ltoa(v32, c_buf.as_mut_ptr().cast()) } as usize;
                let mut d = vec![1u8];
                d.extend_from_slice(&c_buf[..c_len]);
                numutils_diff(&d);
                let s = std::str::from_utf8(&c_buf[..c_len]).unwrap();
                assert_eq!(numutils::pg_strtoint32(s).unwrap(), v32, "canonical parse {v32}");
                if let Ok(v16) = i16::try_from(v) {
                    assert_eq!(numutils::pg_strtoint16(s).unwrap(), v16);
                    let mut d = vec![0u8];
                    d.extend_from_slice(&c_buf[..c_len]);
                    numutils_diff(&d);
                }
            }
            // strtoint64 leg (also covers the out-of-i32-range boundary draws)
            let c_len = unsafe { pg_diff_numutils_lltoa(v, c_buf.as_mut_ptr().cast()) } as usize;
            let mut d = vec![2u8];
            d.extend_from_slice(&c_buf[..c_len]);
            numutils_diff(&d);
            let s = std::str::from_utf8(&c_buf[..c_len]).unwrap();
            assert_eq!(numutils::pg_strtoint64(s).unwrap(), v, "canonical parse {v}");
        }
        // i64 boundaries +-2 as strings (magnitudes past i64 via i128)
        for b in [i64::MIN as i128, i64::MAX as i128] {
            for d in -2i128..=2 {
                let s = format!("{}", b + d);
                let mut data = vec![2u8];
                data.extend_from_slice(s.as_bytes());
                numutils_diff(&data);
            }
        }
    }

    /// Decimal-length band sweep: every 10^k boundary +-1 for u32 and u64
    /// through the whole emit family (and zeropad minwidth 1..=12).
    #[test]
    fn test_ultoa_bands() {
        let _serial = crate::c_oracle_serial();
        let mut u32s: Vec<u32> = vec![0, 1, u32::MAX, u32::MAX - 1];
        let mut p = 1u64;
        while p <= u32::MAX as u64 {
            for d in [-1i64, 0, 1] {
                let v = p as i64 + d;
                if (0..=u32::MAX as i64).contains(&v) {
                    u32s.push(v as u32);
                }
            }
            p *= 10;
        }
        for &v in &u32s {
            diff_ultoa_n(v);
            diff_ultostr(v);
            for mw in 1..=12 {
                diff_ultostr_zeropad(v, mw);
            }
            diff_ltoa(v as i32);
            diff_ltoa((v as i32).wrapping_neg());
        }
        let mut u64s: Vec<u64> = vec![0, 1, u64::MAX, u64::MAX - 1];
        let mut p = 1u128;
        while p <= u64::MAX as u128 {
            for d in [-1i128, 0, 1] {
                let v = p as i128 + d;
                if (0..=u64::MAX as i128).contains(&v) {
                    u64s.push(v as u64);
                }
            }
            p *= 10;
        }
        for &v in &u64s {
            diff_ulltoa_n(v);
            diff_lltoa(v as i64);
            diff_lltoa((v as i64).wrapping_neg());
        }
        diff_lltoa(i64::MIN);
        diff_lltoa(i64::MAX);
        diff_ltoa(i32::MIN);
        diff_ltoa(i32::MAX);
        diff_itoa(i16::MIN);
        diff_itoa(i16::MAX);
    }

    fn exhaustive_gate(name: &str) -> bool {
        if std::env::var_os("NUMUTILS_EXHAUSTIVE").is_some_and(|v| v == "1") {
            true
        } else {
            println!("{name}: SKIPPED (full 2^32 sweep; set NUMUTILS_EXHAUSTIVE=1 — fleet job)");
            false
        }
    }

    /// a0 EXHAUSTIVE-DIFF, fleet-gated: full u32 domain through pg_ultoa_n
    /// (pg_ultostr rides the same emission core; see routes tsv).
    #[test]
    fn test_ultoa_exhaustive_u32() {
        let _serial = crate::c_oracle_serial();
        if !exhaustive_gate("test_ultoa_exhaustive_u32") {
            return;
        }
        let mut c_buf = [0u8; EMIT_BUF];
        let mut r_buf = [0u8; EMIT_BUF];
        let mut v: i64 = 0;
        while v <= u32::MAX as i64 {
            let u = v as u32;
            // SAFETY: buffer holds any u32 decimal image.
            let c_len = unsafe { pg_diff_numutils_ultoa_n(u, c_buf.as_mut_ptr().cast()) } as usize;
            let r_len = numutils::pg_ultoa_n(u, &mut r_buf);
            if r_len != c_len || r_buf[..r_len] != c_buf[..c_len] {
                panic!("pg_ultoa_n diverged on {u}");
            }
            v += 1;
        }
    }

    /// a0 EXHAUSTIVE-DIFF, fleet-gated: full i32 domain through pg_ltoa.
    #[test]
    fn test_ltoa_exhaustive_i32() {
        let _serial = crate::c_oracle_serial();
        if !exhaustive_gate("test_ltoa_exhaustive_i32") {
            return;
        }
        let mut c_buf = [0u8; EMIT_BUF];
        let mut r_buf = [0u8; EMIT_BUF];
        let mut v: i64 = i32::MIN as i64;
        while v <= i32::MAX as i64 {
            let i = v as i32;
            // SAFETY: buffer holds sign + image + NUL.
            let c_len = unsafe { pg_diff_numutils_ltoa(i, c_buf.as_mut_ptr().cast()) } as usize;
            let r_len = numutils::pg_ltoa(i, &mut r_buf);
            if r_len != c_len || r_buf[..r_len] != c_buf[..c_len] {
                panic!("pg_ltoa diverged on {i}");
            }
            v += 1;
        }
    }

    /// a0 EXHAUSTIVE-DIFF, fleet-gated: full u32 domain through
    /// pg_ultostr_zeropad at minwidth 2 (the special-cased fast arm) plus a
    /// per-value cycling minwidth 1..=15 (every (len, minwidth) ordering).
    #[test]
    fn test_zeropad_exhaustive() {
        let _serial = crate::c_oracle_serial();
        if !exhaustive_gate("test_zeropad_exhaustive") {
            return;
        }
        let mut c_buf = [0u8; EMIT_BUF];
        let mut r_buf = [0u8; EMIT_BUF];
        let mut v: i64 = 0;
        while v <= u32::MAX as i64 {
            let u = v as u32;
            for mw in [2, (u % 15) as i32 + 1] {
                // SAFETY: buffer holds max(minwidth, 10) bytes.
                let c_len = unsafe {
                    pg_diff_numutils_ultostr_zeropad(c_buf.as_mut_ptr().cast(), u, mw)
                } as usize;
                let r_len = numutils::pg_ultostr_zeropad(&mut r_buf, u, mw);
                if r_len != c_len || r_buf[..r_len] != c_buf[..c_len] {
                    panic!("pg_ultostr_zeropad diverged on ({u}, {mw})");
                }
            }
            v += 1;
        }
    }
}
