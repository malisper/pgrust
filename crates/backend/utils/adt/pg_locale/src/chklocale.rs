//! Port of `src/port/chklocale.c` — map a locale's `LC_CTYPE` to its implied
//! PostgreSQL backend encoding.
//!
//! `pg_get_encoding_from_locale(ctype, write_message)` builds an OS `locale_t`
//! for `ctype`, reads its `CODESET` via `nl_langinfo_l`, and looks the codeset
//! name up in [`ENCODING_MATCH_LIST`] (the verbatim `encoding_match_list` table).
//! C/POSIX maps to `PG_SQL_ASCII` (any encoding is acceptable); a bogus `ctype`
//! or unrecognized codeset returns `-1`.
//!
//! The WIN32 branch (`win32_get_codeset`/`pg_codepage_to_encoding`) is
//! Windows-only and not part of this (glibc/macOS) profile; the frontend
//! (`initdb`) `fprintf` variant of the warning is likewise not compiled. The
//! three backend call sites all pass `write_message = true`, so the seam takes
//! no flag and always emits the `ereport(WARNING)` form.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;

use alloc::format;
use core::ffi::c_char;

use ::utils_error::ereport;
use ::pgstrcasecmp::pg_strcasecmp;
use ::types_error::{ErrorLocation, PgResult, WARNING};
use ::types_wchar::encoding::{
    pg_enc, PG_BIG5, PG_EUC_CN, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK, PG_ISO_8859_5,
    PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_JOHAB, PG_KOI8R, PG_KOI8U, PG_LATIN1,
    PG_LATIN10, PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7, PG_LATIN8,
    PG_LATIN9, PG_SHIFT_JIS_2004, PG_SJIS, PG_SQL_ASCII, PG_UHC, PG_UTF8, PG_WIN1250, PG_WIN1251,
    PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255, PG_WIN1256, PG_WIN1257, PG_WIN1258, PG_WIN866,
    PG_WIN874,
};

// `nl_langinfo_l(item, locale)` (`<langinfo.h>`). POSIX-2008 defines it, but the
// `libc` crate only surfaces it on some Unix targets (linux/freebsd, not apple);
// declared here directly as OS FFI, like the `_l` family in `libc_provider.rs`.
extern "C" {
    fn nl_langinfo_l(item: libc::nl_item, locale: libc::locale_t) -> *mut c_char;
}

/// `struct encoding_match` (`chklocale.c:39`): a `(pg_enc, system codeset name)`
/// pair. The table is searched with `pg_strcasecmp`, so variant capitalizations
/// don't need their own entries.
struct EncodingMatch {
    pg_enc_code: pg_enc,
    system_enc_name: &'static str,
}

const fn m(pg_enc_code: pg_enc, system_enc_name: &'static str) -> EncodingMatch {
    EncodingMatch {
        pg_enc_code,
        system_enc_name,
    }
}

/// `encoding_match_list[]` (`chklocale.c:45-188`), verbatim. The C `{PG_SQL_ASCII,
/// NULL}` end marker is the slice end here. The CPnnn / Windows codepage spellings
/// are kept (they're harmless on Unix and let `initdb` recognize them).
static ENCODING_MATCH_LIST: &[EncodingMatch] = &[
    m(PG_EUC_JP, "EUC-JP"),
    m(PG_EUC_JP, "eucJP"),
    m(PG_EUC_JP, "IBM-eucJP"),
    m(PG_EUC_JP, "sdeckanji"),
    m(PG_EUC_JP, "CP20932"),
    m(PG_EUC_CN, "EUC-CN"),
    m(PG_EUC_CN, "eucCN"),
    m(PG_EUC_CN, "IBM-eucCN"),
    m(PG_EUC_CN, "GB2312"),
    m(PG_EUC_CN, "dechanzi"),
    m(PG_EUC_CN, "CP20936"),
    m(PG_EUC_KR, "EUC-KR"),
    m(PG_EUC_KR, "eucKR"),
    m(PG_EUC_KR, "IBM-eucKR"),
    m(PG_EUC_KR, "deckorean"),
    m(PG_EUC_KR, "5601"),
    m(PG_EUC_KR, "CP51949"),
    m(PG_EUC_TW, "EUC-TW"),
    m(PG_EUC_TW, "eucTW"),
    m(PG_EUC_TW, "IBM-eucTW"),
    m(PG_EUC_TW, "cns11643"),
    // No codepage for EUC-TW ?
    m(PG_UTF8, "UTF-8"),
    m(PG_UTF8, "utf8"),
    m(PG_UTF8, "CP65001"),
    m(PG_LATIN1, "ISO-8859-1"),
    m(PG_LATIN1, "ISO8859-1"),
    m(PG_LATIN1, "iso88591"),
    m(PG_LATIN1, "CP28591"),
    m(PG_LATIN2, "ISO-8859-2"),
    m(PG_LATIN2, "ISO8859-2"),
    m(PG_LATIN2, "iso88592"),
    m(PG_LATIN2, "CP28592"),
    m(PG_LATIN3, "ISO-8859-3"),
    m(PG_LATIN3, "ISO8859-3"),
    m(PG_LATIN3, "iso88593"),
    m(PG_LATIN3, "CP28593"),
    m(PG_LATIN4, "ISO-8859-4"),
    m(PG_LATIN4, "ISO8859-4"),
    m(PG_LATIN4, "iso88594"),
    m(PG_LATIN4, "CP28594"),
    m(PG_LATIN5, "ISO-8859-9"),
    m(PG_LATIN5, "ISO8859-9"),
    m(PG_LATIN5, "iso88599"),
    m(PG_LATIN5, "CP28599"),
    m(PG_LATIN6, "ISO-8859-10"),
    m(PG_LATIN6, "ISO8859-10"),
    m(PG_LATIN6, "iso885910"),
    m(PG_LATIN7, "ISO-8859-13"),
    m(PG_LATIN7, "ISO8859-13"),
    m(PG_LATIN7, "iso885913"),
    m(PG_LATIN8, "ISO-8859-14"),
    m(PG_LATIN8, "ISO8859-14"),
    m(PG_LATIN8, "iso885914"),
    m(PG_LATIN9, "ISO-8859-15"),
    m(PG_LATIN9, "ISO8859-15"),
    m(PG_LATIN9, "iso885915"),
    m(PG_LATIN9, "CP28605"),
    m(PG_LATIN10, "ISO-8859-16"),
    m(PG_LATIN10, "ISO8859-16"),
    m(PG_LATIN10, "iso885916"),
    m(PG_KOI8R, "KOI8-R"),
    m(PG_KOI8R, "CP20866"),
    m(PG_KOI8U, "KOI8-U"),
    m(PG_KOI8U, "CP21866"),
    m(PG_WIN866, "CP866"),
    m(PG_WIN874, "CP874"),
    m(PG_WIN1250, "CP1250"),
    m(PG_WIN1251, "CP1251"),
    m(PG_WIN1251, "ansi-1251"),
    m(PG_WIN1252, "CP1252"),
    m(PG_WIN1253, "CP1253"),
    m(PG_WIN1254, "CP1254"),
    m(PG_WIN1255, "CP1255"),
    m(PG_WIN1256, "CP1256"),
    m(PG_WIN1257, "CP1257"),
    m(PG_WIN1258, "CP1258"),
    m(PG_ISO_8859_5, "ISO-8859-5"),
    m(PG_ISO_8859_5, "ISO8859-5"),
    m(PG_ISO_8859_5, "iso88595"),
    m(PG_ISO_8859_5, "CP28595"),
    m(PG_ISO_8859_6, "ISO-8859-6"),
    m(PG_ISO_8859_6, "ISO8859-6"),
    m(PG_ISO_8859_6, "iso88596"),
    m(PG_ISO_8859_6, "CP28596"),
    m(PG_ISO_8859_7, "ISO-8859-7"),
    m(PG_ISO_8859_7, "ISO8859-7"),
    m(PG_ISO_8859_7, "iso88597"),
    m(PG_ISO_8859_7, "CP28597"),
    m(PG_ISO_8859_8, "ISO-8859-8"),
    m(PG_ISO_8859_8, "ISO8859-8"),
    m(PG_ISO_8859_8, "iso88598"),
    m(PG_ISO_8859_8, "CP28598"),
    m(PG_SJIS, "SJIS"),
    m(PG_SJIS, "PCK"),
    m(PG_SJIS, "CP932"),
    m(PG_SJIS, "SHIFT_JIS"),
    m(PG_BIG5, "BIG5"),
    m(PG_BIG5, "BIG5HKSCS"),
    m(PG_BIG5, "Big5-HKSCS"),
    m(PG_BIG5, "CP950"),
    m(PG_GBK, "GBK"),
    m(PG_GBK, "CP936"),
    m(PG_UHC, "UHC"),
    m(PG_UHC, "CP949"),
    m(PG_JOHAB, "JOHAB"),
    m(PG_JOHAB, "CP1361"),
    m(PG_GB18030, "GB18030"),
    m(PG_GB18030, "CP54936"),
    m(PG_SHIFT_JIS_2004, "SJIS_2004"),
    m(PG_SQL_ASCII, "US-ASCII"),
];

/// `pg_get_encoding_from_locale(ctype, write_message)` (`chklocale.c:300`): the
/// backend encoding implied by `ctype`'s `LC_CTYPE`, or `-1` if it can't be
/// determined.
///
/// The repo seam always passes `write_message = true` (the three backend call
/// sites), so the warning is unconditional here. Unlike the C entry point,
/// `ctype` is always supplied by the caller (the `ctype == NULL ->
/// setlocale(LC_CTYPE, NULL)` fallback is not reached through the seam).
pub fn pg_get_encoding_from_locale(ctype: &str) -> PgResult<i32> {
    // If locale is C or POSIX, we can allow all encodings.
    if ctype.eq_ignore_ascii_case("C") || ctype.eq_ignore_ascii_case("POSIX") {
        return Ok(PG_SQL_ASCII);
    }

    // newlocale(LC_CTYPE_MASK, ctype, (locale_t) 0).
    let cname = cstr(ctype);
    // SAFETY: cname is NUL-terminated; a null base requests a fresh locale.
    let loc =
        unsafe { libc::newlocale(libc::LC_CTYPE_MASK, cname.as_ptr() as *const c_char, core::ptr::null_mut()) };
    if loc.is_null() {
        return Ok(-1); // bogus ctype passed in?
    }

    // sys = nl_langinfo_l(CODESET, loc); copied out before freelocale.
    // SAFETY: loc is the owned locale just produced; CODESET is a valid nl_item.
    let sys_ptr = unsafe { nl_langinfo_l(libc::CODESET, loc) };
    let sys: Option<alloc::vec::Vec<u8>> = if sys_ptr.is_null() {
        None
    } else {
        // strdup: own the bytes before freelocale frees the locale's storage.
        // SAFETY: sys_ptr is a NUL-terminated string owned by loc.
        let bytes = unsafe { core::ffi::CStr::from_ptr(sys_ptr) }.to_bytes();
        Some(bytes.to_vec())
    };

    // SAFETY: loc is owned here; no further reads of its storage follow.
    unsafe { libc::freelocale(loc) };

    let Some(sys) = sys else {
        return Ok(-1); // out of memory; unlikely
    };

    // Check the table.
    for entry in ENCODING_MATCH_LIST {
        if pg_strcasecmp(&sys, entry.system_enc_name.as_bytes()) == 0 {
            return Ok(entry.pg_enc_code);
        }
    }

    // Special-case kluges for particular platforms go here.
    //
    // Current macOS has many locales that report an empty string for CODESET,
    // but they all seem to actually use UTF-8.
    #[cfg(target_os = "macos")]
    if sys.is_empty() {
        return Ok(PG_UTF8);
    }

    // We print a warning if we got a CODESET string but couldn't recognize it.
    // This means we need another entry in the table.
    let codeset = alloc::string::String::from_utf8_lossy(&sys);
    ereport(WARNING)
        .errmsg(format!(
            "could not determine encoding for locale \"{ctype}\": codeset is \"{codeset}\""
        ))
        .finish(ErrorLocation::new(
            "../src/port/chklocale.c",
            375,
            "pg_get_encoding_from_locale",
        ))?;

    Ok(-1)
}

/// Build a NUL-terminated byte vector for an FFI `*const c_char` argument.
fn cstr(s: &str) -> alloc::vec::Vec<u8> {
    let mut v = alloc::vec::Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}
