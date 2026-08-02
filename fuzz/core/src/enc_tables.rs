//! enc_tables_diff — composite dual-exec differential target for the
//! p1-laneg batch: common/base64 (pg_b64_*), adt/adt_ascii (pg_to_ascii
//! kernel, ascii_safe_strlcpy, fc_to_ascii_* wrappers) and common/keywords
//! (ScanKeywordLookup / GetScanKeyword / keyword_text).
//!
//! Oracle: verbatim vendored PostgreSQL 18.3 C (csrc/pg_enc_tables.c,
//! postgres-src 62d6c7d3df), in-process. Comparison planes: value
//! bytes/ints (including the full dst image, so the C "-1 zeroes dst"
//! secret-hygiene contract is compared byte-for-byte), error-verdict, and
//! errcode class. Any mismatch panics = libFuzzer divergence artifact.
//!
//! Wrapper plumbing planes (documented, deliberate):
//!   - fc_to_ascii_enc's invalid-encoding-code gate: C's wrapper check is
//!     `PG_VALID_ENCODING(enc)` (0 <= enc < _PG_LAST_ENCODING_ = 42, a
//!     two-comparison macro). The comparator pins the Rust wrapper's
//!     22P... ERRCODE_UNDEFINED_OBJECT verdict to exactly
//!     `!pg_valid_encoding(enc)` — rule-pinned rather than oracle-diffed
//!     (vendoring the SQL-function wrapper would drag in fmgr/varlena
//!     machinery, i.e. environment, not computation).
//!   - fc_to_ascii_encname's name->encoding mapping runs through the
//!     shipped mbutils::pg_char_to_encoding on the Rust side only; the
//!     resolved enc is then fed to BOTH kernels. The encnames table parity
//!     is owned by mb/encnames (proofs/encnames), out of this crate.
//!   - fc_to_ascii_default reads the session encoding cell; the driver
//!     sets it explicitly per exec (mbutils::SetDatabaseEncoding) and
//!     feeds the same enc to the C kernel.
//!
//! Input layout: [family][payload...], family = data[0] % 5:
//!   0 = pg_b64_encode  (payload = [dst_shrink][src bytes])
//!   1 = pg_b64_decode  (payload = [dst_shrink][src bytes])
//!   2 = pg_to_ascii kernel + ascii_safe_strlcpy
//!       (payload = [enc_sel][destsiz_sel][bytes])
//!   3 = keyword lookup (payload = [n_lo][n_hi][word bytes])
//!   4 = fc_to_ascii_* wrappers (payload = [wrapper_sel][enc_sel][text])

use std::ffi::CString;
use std::os::raw::{c_char, c_int};

use datum::{Datum, NullableDatum};
use mcx::MemoryContext;
use types_error::{PgError, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_OBJECT};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};
use wchar::{pg_valid_encoding, PG_LATIN1, PG_LATIN2, PG_LATIN9, PG_UTF8, PG_WIN1250};

extern "C" {
    // csrc/pg_enc_tables.c (verbatim 18.3 bodies; see its provenance header)
    fn pg_b64_encode(src: *const u8, len: c_int, dst: *mut c_char, dstlen: c_int) -> c_int;
    fn pg_b64_decode(src: *const c_char, len: c_int, dst: *mut u8, dstlen: c_int) -> c_int;
    fn pg_b64_enc_len(srclen: c_int) -> c_int;
    fn pg_b64_dec_len(srclen: c_int) -> c_int;
    fn pg_diff_to_ascii(src: *mut u8, src_end: *mut u8, dest: *mut u8, enc: c_int);
    fn pg_enc_tables_errcode_get() -> c_int;
    fn ascii_safe_strlcpy(dest: *mut c_char, src: *const c_char, destsiz: usize);
    fn pg_diff_scan_keyword_lookup(str_: *const c_char) -> c_int;
    fn pg_diff_get_scan_keyword(n: c_int) -> *const c_char;
    fn pg_diff_scan_keywords_num() -> c_int;
    fn pg_diff_scan_keywords_max_len() -> c_int;
}

const SRC_CAP: usize = 512;
const FILL: u8 = 0x5a;

// ---------------------------------------------------------------------------
// Family 0/1: base64 encode/decode, full-dst-image + verdict parity.
// dst_shrink pulls dstlen below the *_len sizing to reach the overflow →
// zero-the-buffer error arms on both sides.
// ---------------------------------------------------------------------------

fn b64_encode_case(payload: &[u8]) {
    let Some((&shrink, src)) = payload.split_first() else {
        return;
    };
    let src = &src[..src.len().min(SRC_CAP)];
    let len = src.len() as i32;

    let c_cap = unsafe { pg_b64_enc_len(len) };
    let r_cap = pg_b64::pg_b64_enc_len(len);
    assert!(c_cap == r_cap, "pg_b64_enc_len DIVERGENCE len={len}: C={c_cap} Rust={r_cap}");

    let dstlen = (c_cap - (shrink % 9) as i32).max(0);
    let mut c_dst = vec![FILL; dstlen as usize];
    let mut r_dst = vec![FILL; dstlen as usize];

    let c_rc = unsafe { pg_b64_encode(src.as_ptr(), len, c_dst.as_mut_ptr().cast(), dstlen) };
    let r_rc = pg_b64::pg_b64_encode(src, len, &mut r_dst, dstlen);
    assert!(
        c_rc == r_rc && c_dst == r_dst,
        "pg_b64_encode DIVERGENCE src={src:02x?} dstlen={dstlen}: \
         C=(rc {c_rc}, {c_dst:02x?}) Rust=(rc {r_rc}, {r_dst:02x?})"
    );
}

fn b64_decode_case(payload: &[u8]) {
    let Some((&shrink, src)) = payload.split_first() else {
        return;
    };
    let src = &src[..src.len().min(SRC_CAP)];
    let len = src.len() as i32;

    let c_cap = unsafe { pg_b64_dec_len(len) };
    let r_cap = pg_b64::pg_b64_dec_len(len);
    assert!(c_cap == r_cap, "pg_b64_dec_len DIVERGENCE len={len}: C={c_cap} Rust={r_cap}");

    let dstlen = (c_cap - (shrink % 9) as i32).max(0);
    let mut c_dst = vec![FILL; dstlen as usize];
    let mut r_dst = vec![FILL; dstlen as usize];

    let c_rc = unsafe { pg_b64_decode(src.as_ptr().cast(), len, c_dst.as_mut_ptr(), dstlen) };
    let r_rc = pg_b64::pg_b64_decode(src, len, &mut r_dst, dstlen);
    assert!(
        c_rc == r_rc && c_dst == r_dst,
        "pg_b64_decode DIVERGENCE src={src:02x?} dstlen={dstlen}: \
         C=(rc {c_rc}, {c_dst:02x?}) Rust=(rc {r_rc}, {r_dst:02x?})"
    );
}

// ---------------------------------------------------------------------------
// Family 2: pg_to_ascii kernel (all four supported encodings + the
// unsupported-encoding error plane) and ascii_safe_strlcpy.
// ---------------------------------------------------------------------------

/// enc grid: the four supported encodings, plus valid-but-unsupported and
/// out-of-range codes for the error plane.
const ENC_GRID: [i32; 8] = [
    PG_LATIN1,
    PG_LATIN2,
    PG_LATIN9,
    PG_WIN1250,
    PG_UTF8,
    0,  /* PG_SQL_ASCII */
    63, /* out of range */
    -3, /* negative */
];

fn to_ascii_case(payload: &[u8]) {
    let (&enc_sel, payload) = match payload.split_first() {
        Some(x) => x,
        None => return,
    };
    let (&siz_sel, bytes) = match payload.split_first() {
        Some(x) => x,
        None => return,
    };
    let bytes = &bytes[..bytes.len().min(SRC_CAP)];
    let enc = ENC_GRID[(enc_sel % 8) as usize];

    // kernel
    let mut c_src = bytes.to_vec();
    let mut c_dest = vec![FILL; bytes.len()];
    let c_end = unsafe { c_src.as_mut_ptr().add(c_src.len()) };
    unsafe { pg_diff_to_ascii(c_src.as_mut_ptr(), c_end, c_dest.as_mut_ptr(), enc) };
    let c_err = unsafe { pg_enc_tables_errcode_get() };

    let mut r_dest = vec![FILL; bytes.len()];
    match adt_ascii::pg_to_ascii(bytes, &mut r_dest, enc) {
        Ok(()) => assert!(
            c_err == 0 && c_dest == r_dest,
            "pg_to_ascii DIVERGENCE enc={enc} src={bytes:02x?}: \
             C=(err {c_err}, {c_dest:02x?}) Rust=Ok({r_dest:02x?})"
        ),
        Err(e) => assert!(
            c_err == 1 && e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED,
            "pg_to_ascii DIVERGENCE enc={enc}: C err {c_err} vs Rust Err({:?} {})",
            e.sqlstate,
            e.message
        ),
    }

    // ascii_safe_strlcpy: C reads a NUL-terminated src; Rust reads a slice
    // with the same stop conditions. Feeding C the payload + trailing NUL
    // makes the two stop rules coincide exactly.
    let destsiz = (siz_sel % 33) as usize;
    let mut c_srcz = bytes.to_vec();
    c_srcz.push(0);
    let mut c_dest = vec![FILL; destsiz];
    unsafe { ascii_safe_strlcpy(c_dest.as_mut_ptr().cast(), c_srcz.as_ptr().cast(), destsiz) };
    let mut r_dest = vec![FILL; destsiz];
    adt_ascii::ascii_safe_strlcpy(&mut r_dest, bytes);
    assert!(
        c_dest == r_dest,
        "ascii_safe_strlcpy DIVERGENCE destsiz={destsiz} src={bytes:02x?}: \
         C={c_dest:02x?} Rust={r_dest:02x?}"
    );
}

// ---------------------------------------------------------------------------
// Family 3: keyword lookup. Word plane (hash + downcase + compare) and
// index plane (GetScanKeyword / keyword_text), plus the list invariants.
// ---------------------------------------------------------------------------

fn kwlookup_case(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let n = u16::from_le_bytes([payload[0], payload[1]]) as usize % 600;
    let word_all = &payload[2..];
    // C parses a NUL-terminated cstring: truncate the Rust slice at the
    // first NUL so both sides see the same word.
    let word = match word_all.iter().position(|&b| b == 0) {
        Some(i) => &word_all[..i],
        None => word_all,
    };
    let word = &word[..word.len().min(80)];

    let c_num = unsafe { pg_diff_scan_keywords_num() };
    let c_max = unsafe { pg_diff_scan_keywords_max_len() };
    assert!(
        c_num == keywords::ScanKeywords.num_keywords && c_max == keywords::ScanKeywords.max_kw_len,
        "ScanKeywordList meta DIVERGENCE: C=({c_num},{c_max}) Rust=({},{})",
        keywords::ScanKeywords.num_keywords,
        keywords::ScanKeywords.max_kw_len
    );

    let cs = CString::new(word).unwrap();
    let c_h = unsafe { pg_diff_scan_keyword_lookup(cs.as_ptr()) };
    let r_h = keywords::ScanKeywordLookup(word, &keywords::ScanKeywords);
    assert!(
        c_h == r_h,
        "ScanKeywordLookup DIVERGENCE word={:?}: C={c_h} Rust={r_h}",
        String::from_utf8_lossy(word)
    );

    // Index plane. C GetScanKeyword has no bounds check (caller contract);
    // only the in-range half is oracle-diffed — the out-of-range half is
    // the Rust-side None contract.
    let r_kw = keywords::GetScanKeyword(n, &keywords::ScanKeywords);
    let r_text = keywords::keyword_text(n);
    if (n as i32) < c_num {
        let c_kw = unsafe { pg_diff_get_scan_keyword(n as c_int) };
        let c_kw = unsafe { std::ffi::CStr::from_ptr(c_kw) }.to_bytes();
        assert!(
            r_kw == Some(c_kw),
            "GetScanKeyword DIVERGENCE n={n}: C={:?} Rust={r_kw:?}",
            String::from_utf8_lossy(c_kw)
        );
        assert!(
            r_text.map(str::as_bytes) == Some(c_kw),
            "keyword_text DIVERGENCE n={n}: C={:?} Rust={r_text:?}",
            String::from_utf8_lossy(c_kw)
        );
    } else {
        assert!(r_kw.is_none() && r_text.is_none(), "out-of-range n={n} must be None");
    }
}

// ---------------------------------------------------------------------------
// Family 4: the shipped fc_to_ascii_* SQL wrappers on a real Fcinfo frame.
// ---------------------------------------------------------------------------

/// Build a 4B-U text varlena image: [4-byte LE header][payload].
fn text_image(bytes: &[u8]) -> Vec<u8> {
    let total = bytes.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&(((total as u32) << 2).to_le_bytes()));
    img.extend_from_slice(bytes);
    img
}

/// Read back a 4B-U varlena result datum's payload.
unsafe fn result_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let word = u32::from_le_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
    let total = (word >> 2) as usize;
    std::slice::from_raw_parts(p.add(4), total - 4)
}

type FcFn = fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum>;

fn call_fc<const N: usize>(fc: FcFn, args: [Datum; N], mcx: mcx::Mcx<'_>) -> types_error::PgResult<Datum> {
    let mut f = LocalFcinfo::<N>::new(types_core::C_COLLATION_OID);
    for (slot, d) in f.args.iter_mut().zip(args) {
        *slot = NullableDatum::value(d);
    }
    // SAFETY: the context outlives the call (fn stack frame).
    unsafe { f.set_result_mcx(mcx) };
    fc(None, &mut f)
}

/// Oracle-check a wrapper result against the C kernel run with the same enc.
fn check_wrapper_result(
    who: &str,
    enc: i32,
    text: &[u8],
    res: types_error::PgResult<Datum>,
    // what an invalid-enc verdict means for this wrapper (None = wrapper
    // cannot produce one on this input path)
    invalid_enc: bool,
) {
    let mut c_src = text.to_vec();
    let mut c_dest = vec![FILL; text.len()];
    let (c_err, c_ok) = if invalid_enc {
        (-1, false) /* C kernel never consulted: wrapper gate fires first */
    } else {
        let c_end = unsafe { c_src.as_mut_ptr().add(c_src.len()) };
        unsafe { pg_diff_to_ascii(c_src.as_mut_ptr(), c_end, c_dest.as_mut_ptr(), enc) };
        let e = unsafe { pg_enc_tables_errcode_get() };
        (e, e == 0)
    };
    match res {
        Ok(d) => {
            let payload = unsafe { result_payload(d) };
            assert!(
                c_ok && payload == &c_dest[..],
                "{who} DIVERGENCE enc={enc} text={text:02x?}: \
                 C=(err {c_err}, {c_dest:02x?}) Rust=Ok({payload:02x?})"
            );
        }
        Err(e) => {
            if invalid_enc {
                assert!(
                    e.sqlstate == ERRCODE_UNDEFINED_OBJECT,
                    "{who} DIVERGENCE enc={enc}: expected undefined-object verdict, got ({:?} {})",
                    e.sqlstate,
                    e.message
                );
            } else {
                assert!(
                    c_err == 1 && e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED,
                    "{who} DIVERGENCE enc={enc}: C err {c_err} vs Rust Err({:?} {})",
                    e.sqlstate,
                    e.message
                );
            }
        }
    }
}

/// Name grid for fc_to_ascii_encname: resolvable names for all four
/// supported encodings, resolvable-but-unsupported names, and
/// unresolvable garbage (invalid-name error plane).
const NAME_GRID: [&str; 8] =
    ["latin1", "latin2", "latin9", "win1250", "utf8", "sql_ascii", "bogus_enc", ""];

/// Valid encodings for the session cell driven by fc_to_ascii_default.
const DB_ENC_GRID: [i32; 6] = [PG_LATIN1, PG_LATIN2, PG_LATIN9, PG_WIN1250, PG_UTF8, 0];

fn fc_wrappers_case(payload: &[u8]) {
    let (&wsel, payload) = match payload.split_first() {
        Some(x) => x,
        None => return,
    };
    let (&esel, text) = match payload.split_first() {
        Some(x) => x,
        None => return,
    };
    let text = &text[..text.len().min(100)];

    let cx = MemoryContext::new("enc_tables_fuzz");
    let img = text_image(text);
    let text_datum = Datum::from_usize(img.as_ptr() as usize);

    match wsel % 3 {
        0 => {
            // fc_to_ascii_enc: enc code straight from the wire, -4..=43
            // (covers negative, all 42 valid codes, and past-the-end).
            let enc = (esel as i32) % 48 - 4;
            let res = call_fc(
                adt_ascii::fc_to_ascii_enc,
                [text_datum, Datum::from_i32(enc)],
                cx.mcx(),
            );
            check_wrapper_result("fc_to_ascii_enc", enc, text, res, !pg_valid_encoding(enc));
        }
        1 => {
            // fc_to_ascii_encname
            let name = NAME_GRID[(esel % 8) as usize];
            let mut name_buf = [0u8; 64];
            name_buf[..name.len()].copy_from_slice(name.as_bytes());
            let name_datum = Datum::from_usize(name_buf.as_ptr() as usize);
            let enc = mbutils::pg_char_to_encoding(name);
            let res = call_fc(
                adt_ascii::fc_to_ascii_encname,
                [text_datum, name_datum],
                cx.mcx(),
            );
            check_wrapper_result("fc_to_ascii_encname", enc, text, res, enc < 0);
        }
        _ => {
            // fc_to_ascii_default over an explicitly-set session encoding
            let enc = DB_ENC_GRID[(esel % 6) as usize];
            mbutils::SetDatabaseEncoding(enc).expect("valid grid encoding");
            let res = call_fc(adt_ascii::fc_to_ascii_default, [text_datum], cx.mcx());
            check_wrapper_result("fc_to_ascii_default", enc, text, res, false);
        }
    }
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

pub fn enc_tables_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&fam, payload)) = data.split_first() else {
        return;
    };
    match fam % 5 {
        0 => b64_encode_case(payload),
        1 => b64_decode_case(payload),
        2 => to_ascii_case(payload),
        3 => kwlookup_case(payload),
        _ => fc_wrappers_case(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic seed grid: every family over edge shapes. This is the
    /// stable-toolchain smoke (drives the C link + comparators without
    /// cargo-fuzz) and the shape gen_seeds.sh banks for libFuzzer.
    #[test]
    fn enc_tables_corpus() {
        let _serial = crate::c_oracle_serial();
        // base64 encode/decode: sizes 0..=9, shrink 0..=8, byte classes
        for fam in [0u8, 1] {
            for shrink in 0..=8u8 {
                for src in [
                    &b""[..],
                    b"f",
                    b"fo",
                    b"foo",
                    b"foob",
                    b"fooba",
                    b"foobar",
                    b"Zg==",
                    b"Zm9v",
                    b"Zm=9",
                    b"Z g==",
                    b"====",
                    b"Zg==A",
                    b"\xff\x80\x00\x7f",
                    b"AAAABBBBCCCCDDDD",
                ] {
                    let mut d = vec![fam, shrink];
                    d.extend_from_slice(src);
                    enc_tables_diff(&d);
                }
            }
        }
        // to_ascii: all enc cells x byte sweep incl. 128/160 boundaries
        let sweep: Vec<u8> = (0u8..=255).collect();
        for enc_sel in 0..8u8 {
            for siz in [0u8, 1, 7, 32] {
                let mut d = vec![2, enc_sel, siz];
                d.extend_from_slice(&sweep);
                enc_tables_diff(&d);
                enc_tables_diff(&[2, enc_sel, siz, b'a', 0, b'b', b'\n', 0x9f, 0xa0, 0xff]);
            }
        }
        // keywords: every keyword index + word forms
        for n in 0..600u16 {
            let mut d = vec![3];
            d.extend_from_slice(&n.to_le_bytes());
            d.extend_from_slice(b"select");
            enc_tables_diff(&d);
        }
        for word in [
            &b"select"[..],
            b"SELECT",
            b"SeLeCt",
            b"selec",
            b"selectx",
            b"zone",
            b"abort",
            b"not_a_keyword",
            b"",
            b"a",
            b"characteristics",
            b"current_timestamp",
            b"with\0trailing",
            b"sel\xffect",
            b"verylongwordthatcannotpossiblybeakeyword",
        ] {
            let mut d = vec![3, 0, 0];
            d.extend_from_slice(word);
            enc_tables_diff(&d);
        }
        // fc wrappers: all wrapper x selector cells over a byte sweep
        for wsel in 0..3u8 {
            for esel in 0..=55u8 {
                let mut d = vec![4, wsel, esel];
                d.extend_from_slice(b"abc\x80\x9f\xa0\xc0\xe9\xf7\xff");
                enc_tables_diff(&d);
                enc_tables_diff(&[4, wsel, esel]);
            }
        }
    }

    /// CI replay rail: run every banked corpus unit through the driver.
    #[test]
    fn enc_tables_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/enc_tables_diff");
        let Ok(entries) = std::fs::read_dir(dir) else {
            return; /* corpus not banked yet */
        };
        let mut n = 0usize;
        for e in entries.flatten() {
            if let Ok(bytes) = std::fs::read(e.path()) {
                enc_tables_diff(&bytes);
                n += 1;
            }
        }
        eprintln!("replayed {n} corpus units");
    }
}
