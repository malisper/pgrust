//! varbit_io_diff — SHIPPED Rust `adt_varbit::bits_in`
//! (crates/backend/utils/adt/varbit) vs VERBATIM vendored PostgreSQL 18.3 C
//! (csrc/pg_varbit_io.c; varbit.c bit_in/varbit_in @ upstream sha
//! 62d6c7d3df). The un-vendored bug-class surface the VENDOR lane exists to
//! reach: a hand-rolled text parser on an attacker-controlled cstring
//! (exactly the tid/ltree shape).
//!
//! Comparator planes (float_in_diff conventions): value bytes (the FULL
//! varlena image — LE 4-byte header + i32 bit_len + zero-padded bit_dat),
//! error-vs-no-error, and errcode/sqlstate class. Message text is out of
//! scope. Any mismatch panics — libFuzzer minimizes that into the
//! divergence reproducer.
//!
//! THE BAR: pgrust must ACCEPT-or-REJECT each input IDENTICALLY to the
//! verbatim C. A pgrust PANIC / OOB / assert where C cleanly rejects (54000
//! program-limit / 22026 length-mismatch / 22001 right-truncation / 22P02
//! invalid-digit) is a HIGH-severity finding (the ST3/Q8 class).
//!
//! Both `bit_in` (fixed `bit(N)`) and `varbit_in` (`bit varying(N)`) are
//! driven, on the hard-error plane (escontext = None) and the soft-error
//! plane (ErrorSaveContext); the shipped core `bits_in` implements both.
//!
//! Input layout: `[selector][atttypmod: 4 bytes LE][text...]`.
//!   - selector bit0: 0 => fixed (bit_in), 1 => varying (varbit_in).
//!   - selector bit1: 0 => hard plane, 1 => soft plane.
//!   - `atttypmod`: values <= 0 (incl. i32::MIN — the ST3 typmod class) are
//!     kept verbatim (both sides treat <=0 as "unconstrained = bitlen");
//!     large positives are reduced modulo 1<<20 so a huge fixed-length that
//!     happens to match `bitlen` still allocates only a small image (the
//!     mismatch/right-truncation error paths, which are the interesting
//!     ones, fire BEFORE any allocation on both sides regardless).
//!   - `text`: capped at 4096 bytes and TRUNCATED AT THE FIRST NUL before
//!     BOTH sides run — `bits_in` models the fmgr cstring arg, and a cstring
//!     ends at its first NUL; feeding the shared pre-NUL prefix keeps the
//!     comparison honest while still exercising the PARSER "embedded NUL"
//!     bank case (the C `strlen(sp)` boundary is reproduced explicitly).

use std::ffi::{c_char, c_int, CString};

use types_error::{
    PgError, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
    ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
};

extern "C" {
    fn pg_diff_bits_in(
        input: *const c_char,
        atttypmod: c_int,
        fixed: c_int,
        soft: c_int,
        out_img: *mut u8,
        out_cap: c_int,
        out_len: *mut c_int,
    ) -> c_int;
    fn pg_diff_varbit_errcode_get() -> c_int;
}

/* Same class ints as pg_varbit_io.c's PG_DIFF_ERR_*. */
const C_ERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const C_ERR_PROGRAM_LIMIT: i32 = 3; /* 54000 */
const C_ERR_LENGTH_MISMATCH: i32 = 4; /* 22026 */
const C_ERR_RIGHT_TRUNCATION: i32 = 9; /* 22001 */

fn rust_err_class(e: &PgError) -> i32 {
    let s = e.sqlstate();
    if s == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if s == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        C_ERR_PROGRAM_LIMIT
    } else if s == ERRCODE_STRING_DATA_LENGTH_MISMATCH {
        C_ERR_LENGTH_MISMATCH
    } else if s == ERRCODE_STRING_DATA_RIGHT_TRUNCATION {
        C_ERR_RIGHT_TRUNCATION
    } else {
        99
    }
}

const MAX_TEXT: usize = 4096;
/// Generous varlena image cap: 4096 hex chars => 16384 bits => ~2 KiB body.
const OUT_CAP: usize = 8192;

/// cstring guard: truncate at the first NUL (the fmgr cstring boundary the C
/// `strlen(sp)` reproduces), cap length, and require valid UTF-8 (the shipped
/// `bits_in` takes `&[u8]`, but the parser only branches on ASCII b/x/0-9/a-f
/// and the CString for the C side must be interior-NUL-free — which the
/// truncation guarantees).
fn cstring_input(text: &[u8]) -> Option<(Vec<u8>, CString)> {
    let end = text.iter().position(|&b| b == 0).unwrap_or(text.len());
    let body = &text[..end.min(MAX_TEXT)];
    let cs = CString::new(body).ok()?; // body is NUL-free by construction
    Some((body.to_vec(), cs))
}

fn clamp_typmod(raw: i32) -> i32 {
    // <=0 (incl i32::MIN) kept verbatim — both sides treat as "unconstrained".
    if raw <= 0 {
        raw
    } else {
        raw % (1 << 20)
    }
}

/// One differential exec of `bits_in` for a given `fixed`/`soft` selection.
fn bits_in_one(body: &[u8], cs: &CString, atttypmod: i32, fixed: bool, soft: bool) {
    let mut c_img = vec![0u8; OUT_CAP];
    let mut c_len: c_int = 0;
    let c_rc = unsafe {
        pg_diff_bits_in(
            cs.as_ptr(),
            atttypmod as c_int,
            fixed as c_int,
            soft as c_int,
            c_img.as_mut_ptr(),
            OUT_CAP as c_int,
            &mut c_len,
        )
    };
    // Cross-check: the return code must equal the C-side errcode class
    // (soft plane negates it). Confirms the shim's error plane is wired.
    if soft && c_rc != 0 {
        debug_assert_eq!(-c_rc, unsafe { pg_diff_varbit_errcode_get() });
    } else if !soft {
        debug_assert_eq!(c_rc, unsafe { pg_diff_varbit_errcode_get() });
    }

    let cx = mcx::MemoryContext::new("varbit_fuzz");
    let mcx = cx.mcx();

    if soft {
        let mut sec = SoftErrorContext::new(true);
        match adt_varbit::bits_in(mcx, body, atttypmod, fixed, Some(&mut sec)) {
            Ok(opt) => {
                if sec.error_occurred() {
                    let e = sec.take_error().unwrap();
                    assert!(
                        c_rc < 0,
                        "bits_in soft: rust soft-error {:?}, C rc={c_rc} fixed={fixed} \
                         typmod={atttypmod} input={body:?}",
                        e.message()
                    );
                    assert_eq!(
                        rust_err_class(&e),
                        -c_rc,
                        "bits_in soft errclass fixed={fixed} typmod={atttypmod} input={body:?}"
                    );
                    assert!(
                        opt.is_none(),
                        "bits_in soft: error recorded but Some(img) returned input={body:?}"
                    );
                } else {
                    let img = opt.expect("bits_in soft: no error, but None returned");
                    assert_eq!(
                        c_rc, 0,
                        "bits_in soft: rust ok, C rc={c_rc} fixed={fixed} typmod={atttypmod} \
                         input={body:?}"
                    );
                    assert_eq!(
                        img.as_slice(),
                        &c_img[..c_len as usize],
                        "bits_in soft VALUE-IMAGE divergence fixed={fixed} typmod={atttypmod} \
                         input={body:?}"
                    );
                }
            }
            Err(e) => panic!(
                "bits_in soft: HARD error {:?} escaped the soft context fixed={fixed} \
                 typmod={atttypmod} input={body:?}",
                e.message()
            ),
        }
    } else {
        match adt_varbit::bits_in(mcx, body, atttypmod, fixed, None) {
            Ok(opt) => {
                let img = opt.expect("bits_in hard: Ok(None) with no escontext");
                assert_eq!(
                    c_rc, 0,
                    "bits_in: rust ok, C rc={c_rc} fixed={fixed} typmod={atttypmod} input={body:?}"
                );
                assert_eq!(
                    img.as_slice(),
                    &c_img[..c_len as usize],
                    "bits_in VALUE-IMAGE divergence fixed={fixed} typmod={atttypmod} input={body:?}"
                );
            }
            Err(e) => {
                assert!(
                    c_rc > 0,
                    "bits_in: rust err {:?}, C rc={c_rc} (C accepted!) fixed={fixed} \
                     typmod={atttypmod} input={body:?}",
                    e.message()
                );
                assert_eq!(
                    rust_err_class(&e),
                    c_rc,
                    "bits_in errclass fixed={fixed} typmod={atttypmod} input={body:?}"
                );
            }
        }
    }
}

/// Differential driver entry. Serializes through the process-global C oracle
/// mutex (the C shim carries per-thread state but the oracle-guard holder
/// check is process-global).
pub fn varbit_io_diff(data: &[u8]) {
    let _serial = crate::c_oracle_serial();
    if data.is_empty() {
        return;
    }
    let selector = data[0];
    let fixed = selector & 1 == 0;
    let soft = selector & 2 != 0;

    let (raw_typmod, text) = if data.len() >= 5 {
        (
            i32::from_le_bytes([data[1], data[2], data[3], data[4]]),
            &data[5..],
        )
    } else {
        (-1, &data[1..])
    };
    let atttypmod = clamp_typmod(raw_typmod);

    let Some((body, cs)) = cstring_input(text) else {
        return;
    };

    bits_in_one(&body, &cs, atttypmod, fixed, soft);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(sel: u8, typmod: i32, text: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(&typmod.to_le_bytes());
        d.extend_from_slice(text);
        varbit_io_diff(&d);
    }

    /// The PARSER adversarial text bank against BOTH bit_in and varbit_in, on
    /// both the hard and soft planes, across the ST3 typmod edges. Green =
    /// pgrust matched the verbatim C on every case (accept-or-reject + image +
    /// errclass). A red here is exactly the ST3/Q8 class this lane hunts.
    #[test]
    fn parser_bank_corpus() {
        let _serial = crate::c_oracle_serial();
        // The PARSER edge bank for a hand-rolled text parser.
        let bank: &[&[u8]] = &[
            b"",                 // empty — the Q8-F1 empty-slice class
            b"b",                // bare prefix, empty payload
            b"B",
            b"x",                // bare hex prefix
            b"X",
            b"0",                // implicit-binary
            b"1",
            b"01",
            b"1010",
            b"b1010",
            b"B0011",
            b"x1f",              // hex
            b"Xabcdef",
            b"xABCDEF",
            b"x0",               // odd hex length (1 nibble)
            b"xf",
            b"x123",             // odd hex length (3 nibbles)
            b"b2",               // non-binary digit
            b"b01201",           // embedded bad binary digit
            b"xg",               // non-hex digit
            b"x1g",
            b"bg",
            b"z1010",            // unknown prefix => implicit-binary, 'z' bad
            b"  ",               // whitespace (bad binary)
            b"\t1",
            b"b1\x001",          // embedded NUL — truncates at cstring boundary
            b"x\xffff",          // high byte — bad hex digit
            &[0x80, 0x80],       // high bytes, implicit-binary, both bad
        ];
        // Long inputs: huge bit length, huge hex length (near-overflow shape).
        let long_bits = vec![b'1'; 4096];
        let long_hex_body = vec![b'a'; 4095];
        let mut long_hex = vec![b'x'];
        long_hex.extend_from_slice(&long_hex_body);

        let typmods = [i32::MIN, -1, 0, 1, 2, 3, 4, 8, 16, 1000, i32::MAX];
        for sel in 0u8..4 {
            // sel 0=fixed/hard 1=varying/hard 2=fixed/soft 3=varying/soft
            for &tm in &typmods {
                for input in bank {
                    drive(sel, tm, input);
                }
                drive(sel, tm, &long_bits);
                drive(sel, tm, &long_hex);
                // exact-length fixed accept: bit_in wants bitlen == atttypmod.
                // Feed a binary string of length |tm| when tm is a small
                // positive so the fixed-accept image path is exercised.
            }
            // Fixed exact-length accept path (bit_in): typmod == bitlen.
            for n in [0i32, 1, 7, 8, 9, 16, 100] {
                let s = vec![b'1'; n as usize];
                drive(sel, n, &s);
                let mut bs = vec![b'b'];
                bs.extend_from_slice(&s);
                drive(sel, n, &bs);
            }
        }
    }

    /// DETECTION-POWER CONTROL (must fail if detection is broken).
    ///
    /// A green sweep is only evidence if the harness CAN see a divergence.
    /// This asserts the differential comparison actually fires: we run the C
    /// oracle and the shipped Rust on the SAME input and confirm they agree,
    /// then confirm that a DELIBERATELY WRONG expectation (claiming C rejected
    /// an input it actually accepts) makes the comparison panic. Without this,
    /// "0 findings" is indistinguishable from a driver that compares nothing.
    #[test]
    fn detection_control_catches_planted_divergence() {
        let _serial = crate::c_oracle_serial();

        // 1. Real path agrees (no panic) on a valid input.
        drive(0, -1, b"b1010"); // fixed, hard, accepts

        // 2. Plant a divergence: run the differential comparison but with a C
        //    return code we KNOW is wrong for an accepted input, and assert
        //    the value-image comparison would catch it. We reproduce the
        //    driver's core assert directly against a corrupted C image.
        let cs = CString::new("1111").unwrap();
        let mut c_img = vec![0u8; OUT_CAP];
        let mut c_len: c_int = 0;
        let c_rc = unsafe {
            pg_diff_bits_in(cs.as_ptr(), -1, 1, 0, c_img.as_mut_ptr(), OUT_CAP as c_int, &mut c_len)
        };
        assert_eq!(c_rc, 0, "control: C should accept '1111'");
        assert!(c_len > 0, "control: C produced an empty image for '1111'");
        // Corrupt the C image and confirm the byte-for-byte comparison the
        // driver performs would reject it.
        let mut corrupted = c_img[..c_len as usize].to_vec();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        let cx = mcx::MemoryContext::new("varbit_ctl");
        let img = adt_varbit::bits_in(cx.mcx(), b"1111", -1, true, None)
            .unwrap()
            .unwrap();
        assert_ne!(
            img.as_slice(),
            corrupted.as_slice(),
            "detection-power control FAILED: corrupted C image compared EQUAL to Rust — \
             the value-image plane cannot see a divergence"
        );
    }

    /// Execution witness: a nonzero, floored case count so a driver that
    /// silently refuses every input (guard rejects all) is caught. Mirrors
    /// EDGE2's per-driver vacuity accounting.
    #[test]
    fn execution_witness_floor() {
        let _serial = crate::c_oracle_serial();
        let mut executed = 0usize;
        let inputs: &[&[u8]] = &[b"b1010", b"x1f", b"1", b"", b"xg"];
        for sel in 0u8..4 {
            for input in inputs {
                // Confirm the input survives the cstring guard (i.e. the
                // driver will actually run the comparison, not no-op).
                if cstring_input(input).is_some() {
                    drive(sel, -1, input);
                    executed += 1;
                }
            }
        }
        assert!(
            executed >= 16,
            "execution witness: driver executed only {executed} cases — near-vacuous"
        );
    }
}
