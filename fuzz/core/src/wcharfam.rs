//! wcharfam_diff — composite dual-exec differential target for the
//! p1-laneah batch: common/wchar (per-encoding mblen/dsplen/verifychar/
//! verifystr tables, wchar<->mb converters, UTF-8 machinery, dispatchers)
//! and mb/mbutils (pure verifier/length/clip/increment wrappers + the
//! encnames lookups, with the DatabaseEncoding session cell env-mocked on
//! BOTH sides per exec — the laneg adt_ascii precedent).
//!
//! Oracle: verbatim vendored PostgreSQL 18.3 C (csrc/pg_wcharfam.c +
//! csrc/wcharfam/*, postgres-src 62d6c7d "Stamp 18.3"), in-process.
//! Comparison planes: value ints / whole output images, error-verdict,
//! and sqlstate (identical MAKE_SQLSTATE integer encodings on both
//! sides). Message text out of scope. Any mismatch panics = libFuzzer
//! divergence artifact.
//!
//! Documented harness contracts (plumbing, not carve-outs):
//!   - mblen/dsplen/cliplen inputs are padded with 4 trailing NUL bytes on
//!     BOTH sides: the C contract for mblen/dsplen is "full character
//!     present" (wchar.c reads lookahead bytes unchecked; the shipped Rust
//!     mirrors that and would index past a truncated slice exactly where C
//!     reads past the buffer). Truncation behavior is the *verifier*
//!     functions' plane, compared without padding.
//!   - C string walkers (pg_mbstrlen family) read to a NUL terminator; the
//!     Rust ports stop at the slice end. Feeding C `payload + NUL` and
//!     Rust `payload` makes the stop rules coincide (enc_tables precedent).
//!   - elog(ERROR) arms (check_encoding_conversion_args, invalid
//!     SetDatabaseEncoding) are internal-error class on both sides;
//!     verdict is compared, sqlstate pinned to XX000 on the C shim only
//!     (the Rust internal_error carries no explicit sqlstate).
//!
//! Input layout: [family][payload...], family = data[0] % 8:
//!   0 = encnames lookups
//!   1 = verifymbstr family (all planes incl errcode)
//!   2 = mblen/dsplen/verifymbchar/max_length dispatchers
//!   3 = db-encoding walkers (mbstrlen/cliplen/mblen_range; env-mock)
//!   4 = mb2wchar / wchar2mb converters (whole-image)
//!   5 = character incrementers (whole-image + verdict)
//!   6 = UTF-8 codepoint machinery + islegal
//!   7 = long-stream utf8 verifystr (SIMD fast path) + set_invalid +
//!       mblen_or_incomplete + conversion-args check

use std::os::raw::{c_char, c_int};

use wchar::{PG_ENCODING_BE_LAST, _PG_LAST_ENCODING_};

extern "C" {
    fn wfam_x_set_db_encoding(encoding: c_int);
    fn wfam_x_sqlstate(out: *mut c_char);
    fn wfam_x_verify_mbstr(
        encoding: c_int,
        mbstr: *const c_char,
        len: c_int,
        no_error: c_int,
        err: *mut c_int,
    ) -> c_int;
    fn wfam_x_verify_mbstr_len(
        encoding: c_int,
        mbstr: *const c_char,
        len: c_int,
        no_error: c_int,
        err: *mut c_int,
    ) -> c_int;
    fn wfam_x_verifymbstr_db(
        mbstr: *const c_char,
        len: c_int,
        no_error: c_int,
        err: *mut c_int,
    ) -> c_int;
    fn wfam_x_encoding_verifymbstr(encoding: c_int, mbstr: *const c_char, len: c_int) -> c_int;
    fn wfam_x_encoding_verifymbchar(encoding: c_int, mbstr: *const c_char, len: c_int) -> c_int;
    fn wfam_x_encoding_mblen(encoding: c_int, mbstr: *const c_char) -> c_int;
    fn wfam_x_encoding_mblen_bounded(encoding: c_int, mbstr: *const c_char) -> c_int;
    fn wfam_x_encoding_mblen_or_incomplete(
        encoding: c_int,
        mbstr: *const c_char,
        remaining: usize,
    ) -> c_int;
    fn wfam_x_encoding_dsplen(encoding: c_int, mbstr: *const c_char) -> c_int;
    fn wfam_x_encoding_max_length(encoding: c_int) -> c_int;
    fn wfam_x_encoding_set_invalid(encoding: c_int, dst: *mut c_char);
    fn wfam_x_utf8_islegal(source: *const u8, length: c_int) -> c_int;
    fn wfam_x_utf_mblen(s: *const u8) -> c_int;
    fn wfam_x_utf8_to_unicode(c: *const u8) -> u32;
    fn wfam_x_unicode_to_utf8(c: u32, utf8string: *mut u8);
    fn wfam_x_unicode_utf8len(c: u32) -> c_int;
    fn wfam_x_is_valid_unicode_codepoint(c: u32) -> c_int;
    fn wfam_x_is_utf16_surrogate_first(c: u32) -> c_int;
    fn wfam_x_is_utf16_surrogate_second(c: u32) -> c_int;
    fn wfam_x_surrogate_pair_to_codepoint(first: u32, second: u32) -> u32;
    fn wfam_x_mb2wchar_with_len(
        encoding: c_int,
        from: *const c_char,
        to: *mut u32,
        len: c_int,
    ) -> c_int;
    fn wfam_x_wchar2mb_with_len(
        encoding: c_int,
        from: *const u32,
        to: *mut c_char,
        len: c_int,
    ) -> c_int;
    fn wfam_x_mblen_db(mbstr: *const c_char) -> c_int;
    fn wfam_x_dsplen_db(mbstr: *const c_char) -> c_int;
    fn wfam_x_mblen_range_db(mbstr: *const c_char, end: *const c_char, err: *mut c_int) -> c_int;
    fn wfam_x_mblen_with_len_db(mbstr: *const c_char, limit: c_int, err: *mut c_int) -> c_int;
    fn wfam_x_mbstrlen_db(mbstr: *const c_char, err: *mut c_int) -> c_int;
    fn wfam_x_mbstrlen_with_len_db(mbstr: *const c_char, limit: c_int, err: *mut c_int) -> c_int;
    fn wfam_x_mbcliplen_db(mbstr: *const c_char, len: c_int, limit: c_int) -> c_int;
    fn wfam_x_encoding_mbcliplen(
        encoding: c_int,
        mbstr: *const c_char,
        len: c_int,
        limit: c_int,
    ) -> c_int;
    fn wfam_x_mbcharcliplen_db(
        mbstr: *const c_char,
        len: c_int,
        limit: c_int,
        err: *mut c_int,
    ) -> c_int;
    fn wfam_x_database_encoding_max_length_db() -> c_int;
    fn wfam_x_utf8_increment(charptr: *mut u8, length: c_int) -> c_int;
    fn wfam_x_eucjp_increment(charptr: *mut u8, length: c_int) -> c_int;
    fn wfam_x_generic_charinc_db(charptr: *mut u8, len: c_int) -> c_int;
    fn wfam_x_charinc_selector_db() -> c_int;
    fn wfam_x_check_encoding_conversion_args(
        src_encoding: c_int,
        dest_encoding: c_int,
        len: c_int,
        expected_src_encoding: c_int,
        expected_dest_encoding: c_int,
        err: *mut c_int,
    ) -> c_int;
    fn wfam_x_report_untranslatable_char(
        src_encoding: c_int,
        dest_encoding: c_int,
        mbstr: *const c_char,
        len: c_int,
        err: *mut c_int,
    ) -> c_int;
    fn wfam_x_char_to_encoding(name: *const c_char) -> c_int;
    fn wfam_x_encoding_to_char(encoding: c_int) -> *const c_char;
    fn wfam_x_valid_client_encoding(name: *const c_char) -> c_int;
    fn wfam_x_valid_server_encoding(name: *const c_char) -> c_int;
}

const N_ENC: i32 = _PG_LAST_ENCODING_; // 42
const N_BE: i32 = PG_ENCODING_BE_LAST + 1; // 35
const CAP: usize = 512;

/// C sqlstate capture as the same i32 encoding types_error::SqlState uses.
fn c_sqlstate() -> i32 {
    let mut buf = [0u8; 6];
    unsafe { wfam_x_sqlstate(buf.as_mut_ptr().cast()) };
    let b: [u8; 5] = core::array::from_fn(|i| buf[i]);
    let mut v = 0i32;
    for (i, ch) in b.iter().enumerate() {
        v += (((*ch as i32) - ('0' as i32)) & 0x3f) << (6 * i);
    }
    v
}

/// payload + 4 NUL pad (see header contract).
fn padded(bytes: &[u8]) -> Vec<u8> {
    let mut v = bytes.to_vec();
    v.extend_from_slice(&[0, 0, 0, 0]);
    v
}

fn set_db_encoding_both(enc_be: i32) {
    mbutils::SetDatabaseEncoding(enc_be).expect("valid BE encoding");
    unsafe { wfam_x_set_db_encoding(enc_be) };
}

// ---------------------------------------------------------------------------
// Family 0: encnames lookups.
// ---------------------------------------------------------------------------

fn encnames_case(payload: &[u8]) {
    let Some((&esel, name_all)) = payload.split_first() else {
        return;
    };
    // C parses a NUL-terminated cstring: truncate at the first NUL.
    let name = match name_all.iter().position(|&b| b == 0) {
        Some(i) => &name_all[..i],
        None => name_all,
    };
    let name = &name[..name.len().min(80)];
    let mut c_name = name.to_vec();
    c_name.push(0);

    let c_enc = unsafe { wfam_x_char_to_encoding(c_name.as_ptr().cast()) };
    let r_enc = mbutils::pg_char_to_encoding_bytes(name);
    assert!(
        c_enc == r_enc,
        "pg_char_to_encoding DIVERGENCE name={name:02x?}: C={c_enc} Rust={r_enc}"
    );

    if let Ok(s) = core::str::from_utf8(name) {
        let c_vc = unsafe { wfam_x_valid_client_encoding(c_name.as_ptr().cast()) };
        let r_vc = mbutils::pg_valid_client_encoding(s);
        assert!(c_vc == r_vc, "pg_valid_client_encoding DIVERGENCE {s:?}: C={c_vc} Rust={r_vc}");
        let c_vs = unsafe { wfam_x_valid_server_encoding(c_name.as_ptr().cast()) };
        let r_vs = mbutils::pg_valid_server_encoding(s);
        assert!(c_vs == r_vs, "pg_valid_server_encoding DIVERGENCE {s:?}: C={c_vs} Rust={r_vs}");
    }

    // encoding_to_char over a signed selector reaching both the valid range
    // and out-of-range verdicts on either side.
    let enc = esel as i8 as i32;
    cmp_encoding_to_char(enc);
}

pub fn cmp_encoding_to_char(enc: i32) {
    let c_name = unsafe { wfam_x_encoding_to_char(enc) };
    let c_name = unsafe { std::ffi::CStr::from_ptr(c_name) }.to_bytes();
    let r_name = mbutils::pg_encoding_to_char(enc);
    assert!(
        c_name == r_name.as_bytes(),
        "pg_encoding_to_char DIVERGENCE enc={enc}: C={:?} Rust={r_name:?}",
        String::from_utf8_lossy(c_name)
    );
}

// ---------------------------------------------------------------------------
// Family 1: string verifiers, all planes.
// ---------------------------------------------------------------------------

fn verify_case(payload: &[u8]) {
    let Some((&esel, bytes)) = payload.split_first() else {
        return;
    };
    let enc = (esel as i32) % N_ENC;
    let bytes = &bytes[..bytes.len().min(CAP)];
    let len = bytes.len() as i32;

    cmp_verifystr(enc, bytes);

    for no_error in [true, false] {
        // pg_verify_mbstr
        let mut c_err: c_int = 0;
        let c_ret = unsafe {
            wfam_x_verify_mbstr(enc, bytes.as_ptr().cast(), len, no_error as c_int, &mut c_err)
        };
        let r = mbutils::pg_verify_mbstr(enc, bytes, no_error);
        match r {
            Ok(ok) => assert!(
                c_err == 0 && (c_ret != 0) == ok,
                "pg_verify_mbstr DIVERGENCE enc={enc} noerr={no_error} {bytes:02x?}: \
                 C=({c_ret},{c_err}) Rust=Ok({ok})"
            ),
            Err(e) => assert!(
                c_err == 1 && e.sqlstate.0 == c_sqlstate(),
                "pg_verify_mbstr err DIVERGENCE enc={enc} {bytes:02x?}: \
                 C err={c_err} sqlstate={} Rust sqlstate={}",
                c_sqlstate(),
                e.sqlstate.0
            ),
        }

        // pg_verify_mbstr_len
        let mut c_err = 0;
        let c_ret = unsafe {
            wfam_x_verify_mbstr_len(enc, bytes.as_ptr().cast(), len, no_error as c_int, &mut c_err)
        };
        let r = mbutils::pg_verify_mbstr_len(enc, bytes, no_error);
        match r {
            Ok(n) => assert!(
                c_err == 0 && c_ret == n,
                "pg_verify_mbstr_len DIVERGENCE enc={enc} noerr={no_error} {bytes:02x?}: \
                 C=({c_ret},{c_err}) Rust=Ok({n})"
            ),
            Err(e) => assert!(
                c_err == 1 && e.sqlstate.0 == c_sqlstate(),
                "pg_verify_mbstr_len err DIVERGENCE enc={enc} {bytes:02x?}: \
                 C sqlstate={} Rust sqlstate={}",
                c_sqlstate(),
                e.sqlstate.0
            ),
        }
    }

    // db-encoding wrapper (BE encodings only), pg_verifymbstr
    let enc_be = (esel as i32) % N_BE;
    set_db_encoding_both(enc_be);
    let mut c_err = 0;
    let c_ret =
        unsafe { wfam_x_verifymbstr_db(bytes.as_ptr().cast(), len, 1, &mut c_err) };
    let r = mbutils::pg_verifymbstr(bytes, true);
    assert!(
        c_err == 0 && matches!(r, Ok(ok) if (c_ret != 0) == ok),
        "pg_verifymbstr DIVERGENCE enc={enc_be} {bytes:02x?}: C=({c_ret},{c_err}) Rust={r:?}"
    );
}

pub fn cmp_verifystr(enc: i32, bytes: &[u8]) {
    let c = unsafe { wfam_x_encoding_verifymbstr(enc, bytes.as_ptr().cast(), bytes.len() as c_int) };
    let r = wchar::pg_encoding_verifymbstr(enc, bytes);
    assert!(
        c == r,
        "pg_encoding_verifymbstr DIVERGENCE enc={enc} {bytes:02x?}: C={c} Rust={r}"
    );
}

pub fn cmp_verifychar(enc: i32, bytes: &[u8]) {
    assert!(!bytes.is_empty());
    let c = unsafe { wfam_x_encoding_verifymbchar(enc, bytes.as_ptr().cast(), bytes.len() as c_int) };
    let r = wchar::pg_encoding_verifymbchar(enc, bytes);
    assert!(
        c == r,
        "pg_encoding_verifymbchar DIVERGENCE enc={enc} {bytes:02x?}: C={c} Rust={r}"
    );
}

// ---------------------------------------------------------------------------
// Family 2: single-char dispatchers (padded plane, see header).
// ---------------------------------------------------------------------------

fn mblen_case(payload: &[u8]) {
    let Some((&esel, bytes)) = payload.split_first() else {
        return;
    };
    if bytes.is_empty() {
        return;
    }
    // full signed selector: the invalid-encoding fallback rows are in-plane
    let enc = esel as i8 as i32;
    let bytes = &bytes[..bytes.len().min(8)];
    cmp_mblen_dsplen(enc, bytes);
    if !bytes.is_empty() {
        cmp_verifychar((esel as i32) % N_ENC, bytes);
    }
    cmp_max_length((esel as i32) % N_ENC);
}

/// mblen/dsplen/mblen_bounded on a 4-NUL-padded buffer (both sides).
pub fn cmp_mblen_dsplen(enc: i32, bytes: &[u8]) {
    let p = padded(bytes);
    // table_index fallback: Rust routes invalid encodings to SQL_ASCII; the
    // C dispatchers assert validity (compiled out) and would index out of
    // bounds — pin the C call to the valid range and rule-check the Rust
    // fallback separately (proofs/utf8 eq_invalid_encoding_fallback owns it).
    if !(0..N_ENC).contains(&enc) {
        let r = wchar::pg_encoding_mblen(enc, &p);
        let r0 = wchar::pg_encoding_mblen(0, &p);
        assert!(r == r0, "invalid-enc mblen fallback skew enc={enc}");
        return;
    }
    let c = unsafe { wfam_x_encoding_mblen(enc, p.as_ptr().cast()) };
    let r = wchar::pg_encoding_mblen(enc, &p);
    assert!(c == r, "pg_encoding_mblen DIVERGENCE enc={enc} {bytes:02x?}: C={c} Rust={r}");

    let c = unsafe { wfam_x_encoding_mblen_bounded(enc, p.as_ptr().cast()) };
    let r = wchar::pg_encoding_mblen_bounded(enc, &p);
    assert!(
        c == r,
        "pg_encoding_mblen_bounded DIVERGENCE enc={enc} {bytes:02x?}: C={c} Rust={r}"
    );

    let c = unsafe { wfam_x_encoding_dsplen(enc, p.as_ptr().cast()) };
    let r = wchar::pg_encoding_dsplen(enc, &p);
    assert!(c == r, "pg_encoding_dsplen DIVERGENCE enc={enc} {bytes:02x?}: C={c} Rust={r}");

    // mblen_or_incomplete over the true (unpadded) remaining length
    let c = unsafe {
        wfam_x_encoding_mblen_or_incomplete(enc, p.as_ptr().cast(), bytes.len())
    };
    let r = wchar::pg_encoding_mblen_or_incomplete(enc, &bytes[..bytes.len().min(p.len())]);
    // C returns INT_MAX for incomplete; Rust i32::MAX
    assert!(
        c == r,
        "pg_encoding_mblen_or_incomplete DIVERGENCE enc={enc} {bytes:02x?}: C={c} Rust={r}"
    );
}

pub fn cmp_max_length(enc: i32) {
    let c = unsafe { wfam_x_encoding_max_length(enc) };
    let r = wchar::pg_encoding_max_length(enc);
    assert!(c == r, "pg_encoding_max_length DIVERGENCE enc={enc}: C={c} Rust={r}");
}

// ---------------------------------------------------------------------------
// Family 3: DatabaseEncoding walkers (env-mock both sides).
// ---------------------------------------------------------------------------

fn dbwalk_case(payload: &[u8]) {
    if payload.len() < 3 {
        return;
    }
    let enc_be = (payload[0] as i32) % N_BE;
    let len_sel = payload[1];
    let limit_sel = payload[2];
    let bytes = &payload[3..payload.len().min(3 + CAP)];
    set_db_encoding_both(enc_be);

    let c = unsafe { wfam_x_database_encoding_max_length_db() };
    let r = mbutils::pg_database_encoding_max_length();
    assert!(c == r, "pg_database_encoding_max_length DIVERGENCE enc={enc_be}: C={c} Rust={r}");

    // pg_mbstrlen / pg_mbstrlen_with_len: C walks to NUL / limit.
    let mut c_buf = bytes.to_vec();
    c_buf.extend_from_slice(&[0, 0, 0, 0]);
    let p = padded(bytes); // same bytes for Rust (stop rules coincide)

    let mut c_err = 0;
    let c_ret = unsafe { wfam_x_mbstrlen_db(c_buf.as_ptr().cast(), &mut c_err) };
    let r = mbutils::pg_mbstrlen(&p);
    cmp_result("pg_mbstrlen", enc_be, bytes, c_ret, c_err, &r);

    let mut c_err = 0;
    let c_ret = unsafe {
        wfam_x_mbstrlen_with_len_db(c_buf.as_ptr().cast(), p.len() as c_int, &mut c_err)
    };
    let r = mbutils::pg_mbstrlen_with_len(&p);
    cmp_result("pg_mbstrlen_with_len", enc_be, bytes, c_ret, c_err, &r);

    // UNPADDED plane (mutants-audit finding: the pad hid the loop-guard
    // boundary where the walk reaches the slice end exactly): limit = the
    // true payload length on both sides. BE-encoding mblen kernels read
    // only byte 0, so the unpadded Rust slice is in-contract.
    if !bytes.is_empty() {
        let mut c_err = 0;
        let c_ret = unsafe {
            wfam_x_mbstrlen_with_len_db(c_buf.as_ptr().cast(), bytes.len() as c_int, &mut c_err)
        };
        let r = mbutils::pg_mbstrlen_with_len(bytes);
        cmp_result("pg_mbstrlen_with_len(unpadded)", enc_be, bytes, c_ret, c_err, &r);
        let mut c_err = 0;
        let c_ret = unsafe { wfam_x_mbstrlen_db(c_buf.as_ptr().cast(), &mut c_err) };
        let r = mbutils::pg_mbstrlen(bytes);
        cmp_result("pg_mbstrlen(unpadded)", enc_be, bytes, c_ret, c_err, &r);
    }

    // clip family (value plane only; strings need not be valid)
    let len = (len_sel as i32) % (p.len() as i32 + 1);
    let limit = limit_sel as i8 as i32;
    let c_ret = unsafe { wfam_x_mbcliplen_db(c_buf.as_ptr().cast(), len, limit) };
    let r_ret = mbutils::pg_mbcliplen(&p, len, limit);
    assert!(
        c_ret == r_ret,
        "pg_mbcliplen DIVERGENCE enc={enc_be} len={len} limit={limit} {bytes:02x?}: \
         C={c_ret} Rust={r_ret}"
    );
    let enc_any = (len_sel as i32) % N_ENC;
    let c_ret = unsafe { wfam_x_encoding_mbcliplen(enc_any, c_buf.as_ptr().cast(), len, limit) };
    let r_ret = mbutils::pg_encoding_mbcliplen(enc_any, &p, len, limit);
    assert!(
        c_ret == r_ret,
        "pg_encoding_mbcliplen DIVERGENCE enc={enc_any} len={len} limit={limit}: \
         C={c_ret} Rust={r_ret}"
    );

    let mut c_err = 0;
    let c_ret = unsafe { wfam_x_mbcharcliplen_db(c_buf.as_ptr().cast(), len, limit, &mut c_err) };
    let r = mbutils::pg_mbcharcliplen(&p, len, limit);
    cmp_result("pg_mbcharcliplen", enc_be, bytes, c_ret, c_err, &r);

    // pg_mblen / pg_dsplen / pg_mblen_range / pg_mblen_with_len on the
    // leading character (padded plane for the unchecked pair).
    if !bytes.is_empty() {
        let c_ret = unsafe { wfam_x_mblen_db(p.as_ptr().cast()) };
        let r_ret = mbutils::pg_mblen(&p);
        assert!(c_ret == r_ret, "pg_mblen DIVERGENCE enc={enc_be}: C={c_ret} Rust={r_ret}");

        let c_ret = unsafe { wfam_x_dsplen_db(p.as_ptr().cast()) };
        let r_ret = mbutils::pg_dsplen(&p);
        assert!(c_ret == r_ret, "pg_dsplen DIVERGENCE enc={enc_be}: C={c_ret} Rust={r_ret}");

        // range-bounded: Rust slice end == C end pointer
        let mut c_err = 0;
        let c_ret = unsafe {
            wfam_x_mblen_range_db(
                c_buf.as_ptr().cast(),
                c_buf.as_ptr().add(bytes.len()).cast(),
                &mut c_err,
            )
        };
        let r = mbutils::pg_mblen_range(bytes);
        cmp_result("pg_mblen_range", enc_be, bytes, c_ret, c_err, &r);

        let limit = bytes.len() as i32;
        let mut c_err = 0;
        let c_ret = unsafe { wfam_x_mblen_with_len_db(p.as_ptr().cast(), limit, &mut c_err) };
        let r = mbutils::pg_mblen_with_len(&p, limit);
        cmp_result("pg_mblen_with_len", enc_be, bytes, c_ret, c_err, &r);
    }

    // incrementer selector identity
    let c_sel = unsafe { wfam_x_charinc_selector_db() };
    let r_f = mbutils::pg_database_encoding_character_incrementer();
    let r_sel = if std::ptr::fn_addr_eq(r_f, mbutils::pg_utf8_increment as mbutils::MbcharacterIncrementer) {
        1
    } else if std::ptr::fn_addr_eq(r_f, mbutils::pg_eucjp_increment as mbutils::MbcharacterIncrementer) {
        2
    } else {
        0
    };
    assert!(
        c_sel == r_sel,
        "charinc selector DIVERGENCE enc={enc_be}: C={c_sel} Rust={r_sel}"
    );
}

fn cmp_result(
    who: &str,
    enc: i32,
    bytes: &[u8],
    c_ret: c_int,
    c_err: c_int,
    r: &Result<i32, Box<types_error::PgError>>,
) {
    match r {
        Ok(n) => assert!(
            c_err == 0 && c_ret == *n,
            "{who} DIVERGENCE enc={enc} {bytes:02x?}: C=({c_ret},{c_err}) Rust=Ok({n})"
        ),
        Err(e) => assert!(
            c_err == 1 && e.sqlstate.0 == c_sqlstate(),
            "{who} err DIVERGENCE enc={enc} {bytes:02x?}: C err={c_err} sqlstate={} Rust {}",
            c_sqlstate(),
            e.sqlstate.0
        ),
    }
}

// ---------------------------------------------------------------------------
// Family 4: converters, whole-image plane.
// ---------------------------------------------------------------------------

fn convert_case(payload: &[u8]) {
    let Some((&esel, bytes)) = payload.split_first() else {
        return;
    };
    let enc = (esel as i32) % N_BE; // only BE encodings have converters
    let bytes = &bytes[..bytes.len().min(CAP)];

    // mb2wchar: buffers len+1, sentinel-prefilled
    const SENT: u32 = 0xAAAA_AAAA;
    let mut c_to = vec![SENT; bytes.len() + 1];
    let c_n = unsafe {
        wfam_x_mb2wchar_with_len(enc, bytes.as_ptr().cast(), c_to.as_mut_ptr(), bytes.len() as c_int)
    };
    let mut r_to = vec![SENT; bytes.len() + 1];
    let conv = wchar::pg_wchar_table[enc as usize].mb2wchar_with_len.unwrap();
    let r_n = conv(bytes, &mut r_to);
    assert!(
        c_n == r_n && c_to == r_to,
        "mb2wchar DIVERGENCE enc={enc} {bytes:02x?}: C=({c_n},{c_to:08x?}) Rust=({r_n},{r_to:08x?})"
    );

    // wchar2mb over the wchars mb2wchar just produced (in-contract inputs;
    // C sizes for the same worst case)
    let wchars = &r_to[..(r_n.max(0) as usize + 1)];
    let cap = wchars.len() * 4 + 1;
    let mut c_out = vec![0x5au8; cap];
    let c_n2 = unsafe {
        wfam_x_wchar2mb_with_len(
            enc,
            wchars.as_ptr(),
            c_out.as_mut_ptr().cast(),
            (wchars.len() - 1) as c_int,
        )
    };
    let mut r_out = vec![0x5au8; cap];
    let conv2 = wchar::pg_wchar_table[enc as usize].wchar2mb_with_len.unwrap();
    let r_n2 = conv2(&wchars[..wchars.len() - 1], &mut r_out);
    assert!(
        c_n2 == r_n2 && c_out == r_out,
        "wchar2mb DIVERGENCE enc={enc} {wchars:08x?}: C=({c_n2},{c_out:02x?}) Rust=({r_n2},{r_out:02x?})"
    );

    // the mbutils mcx wrappers over the same input
    let cx = mcx::MemoryContext::new("wcharfam_fuzz");
    let r_vec = mbutils::pg_encoding_mb2wchar_with_len(cx.mcx(), enc, bytes).unwrap();
    assert!(
        r_vec.len() == c_n.max(0) as usize && r_vec[..] == c_to[..r_vec.len()],
        "pg_encoding_mb2wchar_with_len wrapper DIVERGENCE enc={enc}"
    );
    let r_vec2 =
        mbutils::pg_encoding_wchar2mb_with_len(cx.mcx(), enc, &wchars[..wchars.len() - 1])
            .unwrap();
    assert!(
        r_vec2.len() == c_n2.max(0) as usize && r_vec2[..] == c_out[..r_vec2.len()],
        "pg_encoding_wchar2mb_with_len wrapper DIVERGENCE enc={enc}"
    );
}

// ---------------------------------------------------------------------------
// Family 5: incrementers, whole-image + verdict.
// ---------------------------------------------------------------------------

fn increment_case(payload: &[u8]) {
    let Some((&esel, bytes)) = payload.split_first() else {
        return;
    };
    if bytes.is_empty() || bytes.len() > 4 {
        return;
    }
    cmp_utf8_increment(bytes);
    if bytes.len() <= 3 {
        cmp_eucjp_increment(bytes);
    }
    let enc_be = (esel as i32) % N_BE;
    set_db_encoding_both(enc_be);
    cmp_generic_charinc(bytes);
}

pub fn cmp_utf8_increment(bytes: &[u8]) {
    let n = bytes.len();
    let mut c_buf = [0u8; 8];
    c_buf[..n].copy_from_slice(bytes);
    let c_ok = unsafe { wfam_x_utf8_increment(c_buf.as_mut_ptr(), n as c_int) };
    let mut r_buf = [0u8; 8];
    r_buf[..n].copy_from_slice(bytes);
    let r_ok = mbutils::pg_utf8_increment(&mut r_buf[..n]);
    assert!(
        (c_ok != 0) == r_ok && c_buf == r_buf,
        "pg_utf8_increment DIVERGENCE {bytes:02x?}: C=({c_ok},{c_buf:02x?}) Rust=({r_ok},{r_buf:02x?})"
    );
}

pub fn cmp_eucjp_increment(bytes: &[u8]) {
    let n = bytes.len();
    let mut c_buf = [0u8; 8];
    c_buf[..n].copy_from_slice(bytes);
    let c_ok = unsafe { wfam_x_eucjp_increment(c_buf.as_mut_ptr(), n as c_int) };
    let mut r_buf = [0u8; 8];
    r_buf[..n].copy_from_slice(bytes);
    let r_ok = mbutils::pg_eucjp_increment(&mut r_buf[..n]);
    assert!(
        (c_ok != 0) == r_ok && c_buf == r_buf,
        "pg_eucjp_increment DIVERGENCE {bytes:02x?}: C=({c_ok},{c_buf:02x?}) Rust=({r_ok},{r_buf:02x?})"
    );
}

/// Caller must have set the db encoding on both sides.
pub fn cmp_generic_charinc(bytes: &[u8]) {
    if bytes.is_empty() || bytes.len() > 8 {
        return;
    }
    let n = bytes.len();
    // C's verifiers bound their reads by the len argument; no pad needed.
    let mut c_buf = [0u8; 8];
    c_buf[..n].copy_from_slice(bytes);
    let c_ok = unsafe { wfam_x_generic_charinc_db(c_buf.as_mut_ptr(), n as c_int) };
    let mut r_buf = [0u8; 8];
    r_buf[..n].copy_from_slice(bytes);
    let r_ok = mbutils::pg_generic_charinc(&mut r_buf[..n]);
    assert!(
        (c_ok != 0) == r_ok && c_buf == r_buf,
        "pg_generic_charinc DIVERGENCE {bytes:02x?}: C=({c_ok},{c_buf:02x?}) Rust=({r_ok},{r_buf:02x?})"
    );
}

// ---------------------------------------------------------------------------
// Family 6: UTF-8 codepoint machinery.
// ---------------------------------------------------------------------------

fn unicode_case(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let cp = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let b4: [u8; 4] = payload[4..8].try_into().unwrap();
    cmp_codepoint(cp);
    cmp_utf8_bytes(&b4);
    let cp2 = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let c = unsafe { wfam_x_surrogate_pair_to_codepoint(cp, cp2) };
    let r = wchar::surrogate_pair_to_codepoint(cp, cp2);
    assert!(c == r, "surrogate_pair DIVERGENCE {cp:#x},{cp2:#x}: C={c:#x} Rust={r:#x}");
}

pub fn cmp_codepoint(cp: u32) {
    let c_len = unsafe { wfam_x_unicode_utf8len(cp) };
    let r_len = wchar::unicode_utf8len(cp);
    assert!(c_len == r_len, "unicode_utf8len DIVERGENCE {cp:#x}: C={c_len} Rust={r_len}");

    let mut c_buf = [0x5au8; 4];
    unsafe { wfam_x_unicode_to_utf8(cp, c_buf.as_mut_ptr()) };
    let mut r_buf = [0x5au8; 4];
    wchar::unicode_to_utf8(cp, &mut r_buf);
    assert!(
        c_buf == r_buf,
        "unicode_to_utf8 DIVERGENCE {cp:#x}: C={c_buf:02x?} Rust={r_buf:02x?}"
    );

    let c = unsafe { wfam_x_is_valid_unicode_codepoint(cp) } != 0;
    let r = wchar::is_valid_unicode_codepoint(cp);
    assert!(c == r, "is_valid_unicode_codepoint DIVERGENCE {cp:#x}");
    let c = unsafe { wfam_x_is_utf16_surrogate_first(cp) } != 0;
    let r = wchar::is_utf16_surrogate_first(cp);
    assert!(c == r, "is_utf16_surrogate_first DIVERGENCE {cp:#x}");
    let c = unsafe { wfam_x_is_utf16_surrogate_second(cp) } != 0;
    let r = wchar::is_utf16_surrogate_second(cp);
    assert!(c == r, "is_utf16_surrogate_second DIVERGENCE {cp:#x}");
}

pub fn cmp_utf8_bytes(b4: &[u8; 4]) {
    let c = unsafe { wfam_x_utf8_to_unicode(b4.as_ptr()) };
    let r = wchar::utf8_to_unicode(b4);
    assert!(c == r, "utf8_to_unicode DIVERGENCE {b4:02x?}: C={c:#x} Rust={r:#x}");

    let c = unsafe { wfam_x_utf_mblen(b4.as_ptr()) };
    let r = wchar::pg_utf_mblen(b4);
    assert!(c == r, "pg_utf_mblen DIVERGENCE {b4:02x?}: C={c} Rust={r}");

    for l in 1..=4 {
        let c = unsafe { wfam_x_utf8_islegal(b4.as_ptr(), l) } != 0;
        let r = wchar::pg_utf8_islegal(b4, l);
        assert!(c == r, "pg_utf8_islegal DIVERGENCE {b4:02x?} len={l}: C={c} Rust={r}");
    }
}

// ---------------------------------------------------------------------------
// Family 7: long-stream utf8 verifystr + odds and ends.
// ---------------------------------------------------------------------------

fn stream_case(payload: &[u8]) {
    if payload.len() < 3 {
        return;
    }
    let rep = (payload[0] % 8) as usize + 1;
    let bytes = &payload[1..payload.len().min(1 + CAP)];
    // repeat to reach the >=32B SIMD stride reliably
    let mut long: Vec<u8> = Vec::with_capacity(bytes.len() * rep);
    for _ in 0..rep {
        long.extend_from_slice(bytes);
    }
    cmp_verifystr(wchar::PG_UTF8, &long);
    // and the full encoding sweep on the raw payload
    for enc in 0..N_ENC {
        cmp_verifystr(enc, bytes);
    }

    // set_invalid over all multibyte encodings
    for enc in 0..N_ENC {
        if wchar::pg_encoding_max_length(enc) > 1 {
            cmp_set_invalid(enc);
        }
    }

    // conversion-args check (error plane; elog = internal class both sides)
    let a = payload[0] as i8 as i32;
    let b = payload[1] as i8 as i32;
    let l = payload[2] as i8 as i32;
    let mut c_err = 0;
    unsafe { wfam_x_check_encoding_conversion_args(a, b, l, -1, -1, &mut c_err) };
    let r = mbutils::check_encoding_conversion_args(a, b, l, -1, -1);
    assert!(
        (c_err == 1) == r.is_err(),
        "check_encoding_conversion_args verdict DIVERGENCE ({a},{b},{l}): C={c_err} Rust={r:?}"
    );

    // report_untranslatable_char (sqlstate plane) on valid encodings
    if !bytes.is_empty() {
        let src = (payload[0] as i32) % N_ENC;
        let dst = (payload[1] as i32) % N_ENC;
        let mut c_err = 0;
        unsafe {
            wfam_x_report_untranslatable_char(
                src,
                dst,
                bytes.as_ptr().cast(),
                bytes.len() as c_int,
                &mut c_err,
            )
        };
        let e = mbutils::report_untranslatable_char(src, dst, bytes);
        assert!(
            c_err == 1 && e.sqlstate.0 == c_sqlstate(),
            "report_untranslatable_char sqlstate DIVERGENCE ({src},{dst}): C={} Rust={}",
            c_sqlstate(),
            e.sqlstate.0
        );
    }
}

pub fn cmp_set_invalid(enc: i32) {
    let mut c_buf = [0x5au8; 2];
    unsafe { wfam_x_encoding_set_invalid(enc, c_buf.as_mut_ptr().cast()) };
    let mut r_buf = [0x5au8; 2];
    wchar::pg_encoding_set_invalid(enc, &mut r_buf);
    assert!(
        c_buf == r_buf,
        "pg_encoding_set_invalid DIVERGENCE enc={enc}: C={c_buf:02x?} Rust={r_buf:02x?}"
    );
}

// ---------------------------------------------------------------------------

pub fn wcharfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&fam, payload)) = data.split_first() else {
        return;
    };
    match fam % 8 {
        0 => encnames_case(payload),
        1 => verify_case(payload),
        2 => mblen_case(payload),
        3 => dbwalk_case(payload),
        4 => convert_case(payload),
        5 => increment_case(payload),
        6 => unicode_case(payload),
        _ => stream_case(payload),
    }
}

// ---------------------------------------------------------------------------
// Exhaustive-sweep entry points (a0 route driver: tests/wcharfam_exhaustive.rs)
// ---------------------------------------------------------------------------

pub fn set_db_encoding_pub(enc_be: i32) {
    set_db_encoding_both(enc_be);
}

pub fn cmp_char_to_encoding(name: &[u8]) {
    let name = match name.iter().position(|&b| b == 0) {
        Some(i) => &name[..i],
        None => name,
    };
    let mut c_name = name.to_vec();
    c_name.push(0);
    let c_enc = unsafe { wfam_x_char_to_encoding(c_name.as_ptr().cast()) };
    let r_enc = mbutils::pg_char_to_encoding_bytes(name);
    assert!(
        c_enc == r_enc,
        "pg_char_to_encoding DIVERGENCE name={name:02x?}: C={c_enc} Rust={r_enc}"
    );
    if let Ok(s) = core::str::from_utf8(name) {
        let c_vc = unsafe { wfam_x_valid_client_encoding(c_name.as_ptr().cast()) };
        assert!(c_vc == mbutils::pg_valid_client_encoding(s), "valid_client {s:?}");
        let c_vs = unsafe { wfam_x_valid_server_encoding(c_name.as_ptr().cast()) };
        assert!(c_vs == mbutils::pg_valid_server_encoding(s), "valid_server {s:?}");
    }
}

/// dsplen-only fast path (the utf8 4-byte 2^32 sweep).
pub fn cmp_dsplen_only(enc: i32, padded8: &[u8; 8]) {
    let c = unsafe { wfam_x_encoding_dsplen(enc, padded8.as_ptr().cast()) };
    let r = wchar::pg_encoding_dsplen(enc, padded8);
    assert!(c == r, "pg_encoding_dsplen DIVERGENCE enc={enc} {padded8:02x?}: C={c} Rust={r}");
}

/// mblen-only fast path over a pre-padded buffer.
pub fn cmp_mblen_only(enc: i32, padded8: &[u8; 8]) {
    let c = unsafe { wfam_x_encoding_mblen(enc, padded8.as_ptr().cast()) };
    let r = wchar::pg_encoding_mblen(enc, padded8);
    assert!(c == r, "pg_encoding_mblen DIVERGENCE enc={enc} {padded8:02x?}: C={c} Rust={r}");
}

pub fn cmp_check_args(a: i32, b: i32, l: i32, ea: i32, eb: i32) {
    let mut c_err: c_int = 0;
    unsafe { wfam_x_check_encoding_conversion_args(a, b, l, ea, eb, &mut c_err) };
    let r = mbutils::check_encoding_conversion_args(a, b, l, ea, eb);
    assert!(
        (c_err == 1) == r.is_err(),
        "check_encoding_conversion_args verdict DIVERGENCE ({a},{b},{l},{ea},{eb}): C={c_err} Rust={r:?}"
    );
}

pub fn cmp_untranslatable(src: i32, dst: i32, bytes: &[u8]) {
    let mut c_err: c_int = 0;
    unsafe {
        wfam_x_report_untranslatable_char(src, dst, bytes.as_ptr().cast(), bytes.len() as c_int, &mut c_err)
    };
    let e = mbutils::report_untranslatable_char(src, dst, bytes);
    assert!(
        c_err == 1 && e.sqlstate.0 == c_sqlstate(),
        "report_untranslatable_char sqlstate DIVERGENCE ({src},{dst})"
    );
}

pub fn cmp_surrogate_pair(first: u32, second: u32) {
    let c = unsafe { wfam_x_surrogate_pair_to_codepoint(first, second) };
    let r = wchar::surrogate_pair_to_codepoint(first, second);
    assert!(c == r, "surrogate_pair DIVERGENCE {first:#x},{second:#x}: C={c:#x} Rust={r:#x}");
}

/// fc_pg_encoding_max_length (oid 2319) on a real Fcinfo frame vs the C
/// wrapper rule: PG_VALID_ENCODING -> maxmblen (oracle value), else NULL.
/// C validity is oracle-derived from pg_encoding_to_char's gate (the same
/// PG_VALID_ENCODING macro), never re-derived in Rust.
pub fn cmp_fc_max_length(enc: i32, cx: &mcx::MemoryContext) {
    use datum::NullableDatum;
    use types_fmgr::LocalFcinfo;
    let mut f = LocalFcinfo::<1>::new(types_core::C_COLLATION_OID);
    f.args[0] = NullableDatum::value(datum::Datum::from_i32(enc));
    // SAFETY: the context outlives the call (fn stack frame).
    unsafe { f.set_result_mcx(cx.mcx()) };
    let res = mbutils::builtins::fc_pg_encoding_max_length(None, &mut f)
        .expect("fc_pg_encoding_max_length never errors");
    let c_name = unsafe { wfam_x_encoding_to_char(enc) };
    let c_valid = unsafe { *c_name != 0 };
    if c_valid {
        let c_val = unsafe { wfam_x_encoding_max_length(enc) };
        assert!(
            !f.isnull && res.as_i32() == c_val,
            "fc_pg_encoding_max_length DIVERGENCE enc={enc}: C={c_val} Rust=({},{})",
            f.isnull,
            res.as_i32()
        );
    } else {
        assert!(f.isnull, "fc_pg_encoding_max_length DIVERGENCE enc={enc}: C=NULL Rust=value");
    }
}

/// db-encoding mb2wchar/wchar2mb wrappers (caller sets encoding on both
/// sides) — whole-image vs the C dispatch.
pub fn cmp_db_mb2wchar_roundtrip(enc_be: i32, bytes: &[u8]) {
    set_db_encoding_both(enc_be);
    const SENT: u32 = 0xAAAA_AAAA;
    let mut c_to = vec![SENT; bytes.len() + 1];
    let c_n = unsafe {
        wfam_x_mb2wchar_with_len(enc_be, bytes.as_ptr().cast(), c_to.as_mut_ptr(), bytes.len() as c_int)
    };
    let cx = mcx::MemoryContext::new("wcharfam_dbconv");
    let r_vec = mbutils::pg_mb2wchar_with_len(cx.mcx(), bytes).unwrap();
    assert!(
        r_vec.len() == c_n.max(0) as usize && r_vec[..] == c_to[..r_vec.len()],
        "pg_mb2wchar_with_len(db) DIVERGENCE enc={enc_be} {bytes:02x?}"
    );
    let mut wch: Vec<u32> = r_vec.to_vec();
    wch.push(0);
    let cap = wch.len() * 4 + 1;
    let mut c_out = vec![0x5au8; cap];
    let c_n2 = unsafe {
        wfam_x_wchar2mb_with_len(enc_be, wch.as_ptr(), c_out.as_mut_ptr().cast(), (wch.len() - 1) as c_int)
    };
    let r_vec2 = mbutils::pg_wchar2mb_with_len(cx.mcx(), &wch[..wch.len() - 1]).unwrap();
    assert!(
        r_vec2.len() == c_n2.max(0) as usize && r_vec2[..] == c_out[..r_vec2.len()],
        "pg_wchar2mb_with_len(db) DIVERGENCE enc={enc_be}"
    );
}

/// validity predicates: rule-parity against the table constants (the C
/// macros are compile-time; the exhaustive i32 sweep of pg_encoding_to_char
/// oracles the same PG_VALID_ENCODING gate).
pub fn check_encoding_predicates(enc: i32) {
    use wchar::*;
    assert_eq!(pg_valid_encoding(enc), (0.._PG_LAST_ENCODING_).contains(&enc));
    assert_eq!(pg_valid_be_encoding(enc), (0..=PG_ENCODING_BE_LAST).contains(&enc));
    assert_eq!(pg_valid_fe_encoding(enc), pg_valid_encoding(enc));
    assert_eq!(
        pg_encoding_is_client_only(enc),
        enc > PG_ENCODING_BE_LAST && enc < _PG_LAST_ENCODING_
    );
}

/// pg_utf8_islegal out-of-contract lengths (C switch default arm).
pub fn cmp_islegal_len(b4: &[u8; 4], l: i32) {
    let c = unsafe { wfam_x_utf8_islegal(b4.as_ptr(), l) } != 0;
    let r = wchar::pg_utf8_islegal(b4, l);
    assert!(c == r, "pg_utf8_islegal DIVERGENCE {b4:02x?} len={l}: C={c} Rust={r}");
}

#[cfg(test)]
mod smoke {
    use super::*;

    /// Deterministic pseudo-random smoke over all families.
    /// Replay the COMMITTED corpus through the full dual-exec differential
    /// on stable (the banked corpus is the regression suite — any C/Rust
    /// divergence or harness panic fails this test per-commit).
    #[test]
    fn wcharfam_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/wcharfam_diff");
        let mut n = 0usize;
        for entry in std::fs::read_dir(dir).expect("committed corpus present") {
            let p = entry.unwrap().path();
            if p.is_file() {
                wcharfam_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n > 1000, "corpus unexpectedly small: {n} units");
    }

    #[test]
    fn wcharfam_diff_smoke() {
        let _serial = crate::c_oracle_serial();
        let mut x: u64 = 0x243f_6a88_85a3_08d3;
        let mut buf = Vec::new();
        for i in 0..60_000u32 {
            buf.clear();
            let n = (x % 64) as usize + 2;
            for _ in 0..n {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                buf.push((x >> 33) as u8);
            }
            buf[0] = (i % 8) as u8; // sweep families round-robin
            wcharfam_diff(&buf);
        }
        // targeted: long ascii + multibyte streams for the SIMD fast path
        let mut long = vec![7u8, 0];
        long.extend_from_slice("abcdefghij".repeat(20).as_bytes());
        wcharfam_diff(&long);
        let mut long = vec![7u8, 0];
        long.extend_from_slice("é漢字🎈".repeat(30).as_bytes());
        wcharfam_diff(&long);
    }
}
