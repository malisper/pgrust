//! stub:encoding — shared encoding static-table pin: the pg_enc universe
//! (id <-> official name, maxmblen, server-encoding boundary) pinned
//! IDENTICALLY on the Rust side (shipped wchar / mbutils crates) and the
//! C-oracle side (csrc/pg_stub_encoding.c, tables vendored verbatim from
//! 18.3 pg_wchar.h / encnames.c / wchar.c).
//!
//! A target that constructs encoding-dependent state calls `enc_from_byte`
//! to derive the SAME valid encoding id on both sides from one fuzz byte,
//! and relies on `assert_encoding_tables_pinned` (run by the committed test
//! and callable once per process by targets) to guarantee the id means the
//! same encoding everywhere. pg_conversion-style tables can extend this
//! module; the id/name/maxmblen pin is the substrate they all key off.
//!
//! CLAMPS (compared-input contract):
//!   - enc_from_byte : u8 % 42 (_PG_LAST_ENCODING_) — always a VALID
//!     encoding; the invalid-encoding error arms are target surface, not
//!     builder surface, so the builder never produces one.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int, CStr};

/// == C _PG_LAST_ENCODING_ (asserted against both sides' tables below).
pub const N_ENCODINGS: i32 = 42;

extern "C" {
    fn pg_stub_enc_count() -> c_int;
    fn pg_stub_enc_be_last() -> c_int;
    fn pg_stub_enc_name(enc: c_int) -> *const c_char;
    fn pg_stub_enc_enum_value(enc: c_int) -> c_int;
    fn pg_stub_enc_maxmblen(enc: c_int) -> c_int;
}

/// Derive a valid encoding id from one fuzz byte — the documented clamp,
/// identical on both sides because only the RESULT participates in any
/// wire either side consumes.
#[inline]
pub fn enc_from_byte(b: u8) -> i32 {
    (b as i32) % N_ENCODINGS
}

/// One side-by-side row (test/report helper).
#[derive(Debug, PartialEq, Eq)]
pub struct EncRow {
    pub enc: i32,
    pub name: String,
    pub maxmblen: i32,
    pub is_server_encoding: bool,
}

pub(crate) fn c_row(enc: i32) -> EncRow {
    // SAFETY: the C accessors bound-check and the strings are static tables.
    unsafe {
        let p = pg_stub_enc_name(enc);
        assert!(!p.is_null(), "C enc name table hole at {enc}");
        assert_eq!(pg_stub_enc_enum_value(enc), enc, "C enc2name self-index at {enc}");
        EncRow {
            enc,
            name: String::from_utf8_lossy(CStr::from_ptr(p).to_bytes()).into_owned(),
            maxmblen: pg_stub_enc_maxmblen(enc),
            is_server_encoding: enc <= pg_stub_enc_be_last(),
        }
    }
}

pub(crate) fn rust_row(enc: i32) -> EncRow {
    EncRow {
        enc,
        name: String::from(mbutils::pg_encoding_to_char(enc)),
        maxmblen: wchar::pg_encoding_max_length(enc),
        is_server_encoding: enc <= wchar::PG_ENCODING_BE_LAST,
    }
}

/// Compare the full pinned tables; panic with the first divergent row.
/// Committed-test + once-per-process callable for consumer targets.
pub fn assert_encoding_tables_pinned() {
    // SAFETY: scalar accessor.
    let c_count = unsafe { pg_stub_enc_count() };
    assert_eq!(c_count, N_ENCODINGS, "C encoding count");
    assert_eq!(wchar::_PG_LAST_ENCODING_, N_ENCODINGS, "Rust encoding count");
    for enc in 0..N_ENCODINGS {
        assert_eq!(rust_row(enc), c_row(enc), "encoding table divergence at enc {enc}");
    }
}

/// Both-side rows for external checks (used by the must-fail control).
pub fn table_rows() -> Vec<(EncRow, EncRow)> {
    (0..N_ENCODINGS).map(|e| (rust_row(e), c_row(e))).collect()
}
