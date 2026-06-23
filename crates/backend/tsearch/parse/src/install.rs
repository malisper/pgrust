//! Seam installation for the `backend-tsearch-parse` unit.
//!
//! [`init_seams`] installs every seam in [`parse_seams`] that
//! has a real provider in this tree. The remaining seams cross into subsystems
//! whose cores are not yet wired here (the dictionary / ts-config cache + fmgr
//! `lexize` dispatch, the `char2wchar` libc-locale wide path, and the generic
//! `TS_execute` engine over the seam's specialized `QueryItem` shape) and stay
//! at their loud-panic default until those owners land.
//!
//! The byte-path `p_iswhat` predicates (`is*`) are the process-global
//! `<ctype.h>` functions of the active `LC_CTYPE` locale — not PostgreSQL code
//! — so the real provider is `libc`, the blessed precedent shared with
//! `ts_locale.c`'s `t_is*` and `pgstrcasecmp::global_*`.
//!
//! The wide-path `p_iswhat` predicates (`isw*`) have no global-locale `<wctype.h>`
//! binding in the locked `libc` on this target, so they are ported 1:1 from the
//! C/POSIX `<wctype.h>` table here (ASCII classified by the standard rules, all
//! non-ASCII unclassified — exactly libc's C-locale behavior, the locale under
//! which `database_ctype_is_c` selects the `pg_wchar` path and this `wstr`/`isw*`
//! path is otherwise dormant). This is deterministic, not a fabricated stub.

use core::ffi::c_int;

use tsearchcmds_seams::LexDescr;
use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;

/// `F_PRSD_LEXTYPE` (`catalog/pg_proc.dat`): the default word parser's
/// `prsd_lextype` method OID.
const F_PRSD_LEXTYPE: Oid = 3721;

/// `getTokenTypes`'s `OidFunctionCall1(prs->lextypeOid, 0)` (tsearchcmds.c):
/// invoke the parser's `lextype` method, yielding its `LexDescr[]`. The only
/// built-in text-search parser is the default word parser, whose lextype method
/// is `prsd_lextype` (OID 3721); the full descriptor list, including the
/// trailing `lexid == 0` sentinel `getTokenTypes` stops at, is returned.
fn call_parser_lextype<'mcx>(
    mcx: Mcx<'mcx>,
    lextype_oid: Oid,
) -> types_error::PgResult<PgVec<'mcx, LexDescr>> {
    use utils_error::ereport;
    use types_error::ERROR;

    if lextype_oid != F_PRSD_LEXTYPE {
        return Err(ereport(ERROR)
            .errmsg(alloc::format!(
                "text search lextype method {lextype_oid} is not supported"
            ))
            .into_error());
    }

    let descrs = crate::wparser_def::prsd_lextype();
    let mut out: PgVec<'mcx, LexDescr> = mcx::vec_with_capacity_in(mcx, descrs.len())?;
    for (lexid, alias, _descr) in descrs {
        out.push(LexDescr { lexid, alias });
    }
    Ok(out)
}

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

/// C: `char2wchar(to, tolen, from, fromlen, locale)` (`pg_locale_libc.c`) — the
/// libc-locale wide path: `mbstowcs` the database-encoding string `from` into a
/// `wchar_t` array (without the trailing NUL). This seam is the default-locale
/// branch (`locale == 0` → `mbstowcs`), which is the only one the TS callers
/// reach (the nondefault-locale `mbstowcs_l` branch needs a `pg_locale_t`
/// handle the seam does not carry). On an invalid multibyte sequence
/// (`mbstowcs` returns `(size_t) -1`) the C code reports an
/// `ERRCODE_CHARACTER_NOT_IN_REPERTOIRE` error; we mirror that as an `Err`.
fn char2wchar(from: alloc::vec::Vec<u8>) -> types_error::PgResult<alloc::vec::Vec<u32>> {
    // The libc `mbstowcs` binding is not exposed by the `libc` crate on every
    // target, so declare it directly (the symbol is in the C standard library
    // this build already links). `(size_t) -1` signals a bad multibyte
    // sequence.
    extern "C" {
        fn mbstowcs(
            to: *mut libc::wchar_t,
            from: *const libc::c_char,
            n: libc::size_t,
        ) -> libc::size_t;
    }

    // mbstowcs requires a NUL-terminated source (C `pnstrdup(from, fromlen)`).
    let mut cstr: alloc::vec::Vec<u8> = from;
    cstr.push(0);

    // First pass: query the required wchar_t count (mbstowcs(NULL, str, 0)).
    let needed = unsafe { mbstowcs(core::ptr::null_mut(), cstr.as_ptr() as *const libc::c_char, 0) };
    if needed == usize::MAX {
        return Err(invalid_multibyte_error());
    }

    let mut to: alloc::vec::Vec<libc::wchar_t> = alloc::vec![0; needed + 1];
    let result =
        unsafe { mbstowcs(to.as_mut_ptr(), cstr.as_ptr() as *const libc::c_char, needed + 1) };
    if result == usize::MAX {
        return Err(invalid_multibyte_error());
    }

    to.truncate(result);
    Ok(to.into_iter().map(|w| w as u32).collect())
}

/// C: `pg_mb2wchar_with_len(from, to, len)` (`mbutils.c`) — the C-locale wide
/// path `prsd_start` takes when `database_ctype_is_c()`. Convert the
/// database-encoding bytes to `pg_wchar` code points (no trailing NUL),
/// charged to a scratch context (the result is copied into the owned `Vec`).
fn pg_mb2wchar_with_len(from: alloc::vec::Vec<u8>) -> types_error::PgResult<alloc::vec::Vec<u32>> {
    let ctx = mcx::MemoryContext::new("pg_mb2wchar_with_len");
    let mcx = ctx.mcx();
    let wide = mbutils::pg_mb2wchar_with_len(mcx, &from)?;
    Ok(wide.iter().map(|w| *w as u32).collect())
}

/// `ereport(ERROR, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, "invalid multibyte
/// character for locale")` — the C `char2wchar` bad-sequence path.
fn invalid_multibyte_error() -> types_error::PgError {
    use utils_error::ereport;
    use types_error::{error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERROR};
    ereport(ERROR)
        .errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
        .errmsg("invalid multibyte character for locale")
        .into_error()
}

/// Install the `backend-tsearch-parse` unit's seams. Single-threaded startup
/// install, before any seam is observed.
pub fn init_seams() {
    use parse_seams as s;

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
    s::pg_dsplen::set(mbutils::pg_dsplen);
    // `pg_mblen_range(s, end)` — leading-char byte length bounded by the buffer
    // end; raises (SQLSTATE 22021) on a truncated/invalid sequence.
    s::pg_mblen_range::set(mbutils::pg_mblen_range);
    // `pg_database_encoding_max_length()` / `GetDatabaseEncoding()`.
    s::pg_database_encoding_max_length::set(
        mbutils::pg_database_encoding_max_length,
    );
    s::get_database_encoding::set(|| mbutils::GetDatabaseEncoding() as i32);

    // --- pg_locale.c: database default-collation ctype ----------------------
    // `database_ctype_is_c()` selects the `pg_wchar` (C-locale) wide path over
    // the libc `char2wchar` path.
    s::database_ctype_is_c::set(pg_locale::database_ctype_is_c);

    // --- pg_locale_libc.c: libc-locale wide conversion ----------------------
    // `char2wchar` (the non-C-locale wide path `prsd_start` takes when
    // `!database_ctype_is_c()`), and reused by ts_locale's `t_isalpha` /
    // `t_isalnum` multibyte branch.
    s::char2wchar::set(char2wchar);

    // --- mbutils.c: C-locale `pg_wchar` wide path -----------------------------
    // `pg_mb2wchar_with_len` (the C-locale wide path `prsd_start` takes when
    // `database_ctype_is_c()` — reached under a `--no-locale` cluster).
    s::pg_mb2wchar_with_len::set(pg_mb2wchar_with_len);

    // getTokenTypes's lextype dispatch (OidFunctionCall1 of the parser's
    // lextype method) — the default word parser's prsd_lextype.
    tsearchcmds_seams::call_parser_lextype::set(call_parser_lextype);

    // The remaining seams stay at their loud-panic default until their owners
    // land: `ts_execute_hl` / `ts_execute_locations_hl` (the generic TS_execute
    // engine, which exposes no pluggable-callback entry over this seam's
    // specialized `QueryItem` shape).
    //
    // `config_lenmap` / `config_dict_ids` / `dict_lexize` (the ts-config
    // dictionary cache + lexize dispatch) are installed by the `to_tsany`
    // owner crate (`backend-tsearch-to-tsany`), which sits above both the
    // ts_cache and the dictionary crates.
}
