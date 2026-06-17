//! Seam installation for the `backend-tsearch-parse` unit.
//!
//! [`init_seams`] installs every seam in [`backend_tsearch_parse_seams`] that
//! has a real provider in this tree. The remaining seams cross into subsystems
//! whose cores are not yet wired here (the dictionary / ts-config cache + fmgr
//! `lexize` dispatch, the `char2wchar` libc-locale wide path, and the generic
//! `TS_execute` engine over the seam's specialized `QueryItem` shape) and stay
//! at their loud-panic default until those owners land.
//!
//! The byte-path `p_iswhat` predicates (`is*`) are the process-global
//! `<ctype.h>` functions of the active `LC_CTYPE` locale — not PostgreSQL code
//! — so the real provider is `libc`, the blessed precedent shared with
//! `ts_locale.c`'s `t_is*` and `port_pgstrcasecmp::global_*`.
//!
//! The wide-path `p_iswhat` predicates (`isw*`) have no global-locale `<wctype.h>`
//! binding in the locked `libc` on this target, so they are ported 1:1 from the
//! C/POSIX `<wctype.h>` table here (ASCII classified by the standard rules, all
//! non-ASCII unclassified — exactly libc's C-locale behavior, the locale under
//! which `database_ctype_is_c` selects the `pg_wchar` path and this `wstr`/`isw*`
//! path is otherwise dormant). This is deterministic, not a fabricated stub.

use core::ffi::c_int;

// ---------------------------------------------------------------------------
// Wide `<wctype.h>` predicates (the `p_iswhat` wide path), C/POSIX locale.
// ---------------------------------------------------------------------------

/// C: `iswalnum((wint_t) wc)` — wide alphanumeric test, C/POSIX locale.
fn is_w_alnum(wc: u32) -> i32 {
    match char::from_u32(wc) {
        Some(c) if c.is_ascii() => c_int::from(c.is_ascii_alphanumeric()),
        _ => 0,
    }
}

/// C: `iswalpha((wint_t) wc)` — wide alphabetic test, C/POSIX locale.
fn is_w_alpha(wc: u32) -> i32 {
    match char::from_u32(wc) {
        Some(c) if c.is_ascii() => c_int::from(c.is_ascii_alphabetic()),
        _ => 0,
    }
}

/// C: `iswdigit((wint_t) wc)` — wide decimal-digit test, C/POSIX locale.
fn is_w_digit(wc: u32) -> i32 {
    match char::from_u32(wc) {
        Some(c) if c.is_ascii() => c_int::from(c.is_ascii_digit()),
        _ => 0,
    }
}

/// C: `iswspace((wint_t) wc)` — wide whitespace test, C/POSIX locale
/// (`' ' \t \n \v \f \r`).
fn is_w_space(wc: u32) -> i32 {
    match char::from_u32(wc) {
        Some(c) if c.is_ascii() => {
            c_int::from(matches!(wc as u8, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'))
        }
        _ => 0,
    }
}

/// C: `iswxdigit((wint_t) wc)` — wide hex-digit test, C/POSIX locale
/// (`0-9 A-F a-f`).
fn is_w_xdigit(wc: u32) -> i32 {
    match char::from_u32(wc) {
        Some(c) if c.is_ascii() => c_int::from(c.is_ascii_hexdigit()),
        _ => 0,
    }
}

/// Install the `backend-tsearch-parse` unit's seams. Single-threaded startup
/// install, before any seam is observed.
pub fn init_seams() {
    use backend_tsearch_parse_seams as s;

    // --- byte path: process-global <ctype.h> (the `p_iswhat` byte path) ------
    // The byte is already reduced to 0..=255 by the macro's `(unsigned char)`
    // cast on the call side, so `c as c_int` is exact and the c_int truth value
    // is returned unchanged.
    s::isalnum::set(|c| unsafe { libc::isalnum(c as libc::c_int) });
    s::isalpha::set(|c| unsafe { libc::isalpha(c as libc::c_int) });
    s::isdigit::set(|c| unsafe { libc::isdigit(c as libc::c_int) });
    s::isspace::set(|c| unsafe { libc::isspace(c as libc::c_int) });
    s::isxdigit::set(|c| unsafe { libc::isxdigit(c as libc::c_int) });

    // --- wide path: C/POSIX-faithful <wctype.h> (the `p_iswhat` wide path) ---
    s::iswalnum::set(is_w_alnum);
    s::iswalpha::set(is_w_alpha);
    s::iswdigit::set(is_w_digit);
    s::iswspace::set(is_w_space);
    s::iswxdigit::set(is_w_xdigit);

    // --- multibyte-encoding subsystem (utils/mb/{wchar,mbutils}.c) -----------
    // `pg_dsplen(s)` — display width of the leading character under the database
    // encoding. C is infallible (control/error cases return -1 inside the table
    // function); the provider mirrors that with a bare `i32`.
    s::pg_dsplen::set(backend_utils_mb_mbutils::pg_dsplen);
    // `pg_mblen_range(s, end)` — leading-char byte length bounded by the buffer
    // end; raises (SQLSTATE 22021) on a truncated/invalid sequence.
    s::pg_mblen_range::set(backend_utils_mb_mbutils::pg_mblen_range);
    // `pg_database_encoding_max_length()` / `GetDatabaseEncoding()`.
    s::pg_database_encoding_max_length::set(
        backend_utils_mb_mbutils::pg_database_encoding_max_length,
    );
    s::get_database_encoding::set(|| backend_utils_mb_mbutils::GetDatabaseEncoding() as i32);

    // --- pg_locale.c: database default-collation ctype ----------------------
    // `database_ctype_is_c()` selects the `pg_wchar` (C-locale) wide path over
    // the libc `char2wchar` path.
    s::database_ctype_is_c::set(backend_utils_adt_pg_locale::database_ctype_is_c);

    // The remaining seams stay at their loud-panic default until their owners
    // land: `char2wchar` / `pg_mb2wchar_with_len` (the libc-locale wide
    // conversion path, dormant while `database_ctype_is_c` is true and no
    // caller-buffer/locale-handle provider is wired); `config_lenmap` /
    // `config_dict_ids` / `dict_lexize` (the ts-config dictionary cache + fmgr
    // lexize dispatch, whose ts_cache catalog cores are unwired in production);
    // and `ts_execute_hl` / `ts_execute_locations_hl` (the generic TS_execute
    // engine, which exposes no pluggable-callback entry over this seam's
    // specialized `QueryItem` shape).
}
