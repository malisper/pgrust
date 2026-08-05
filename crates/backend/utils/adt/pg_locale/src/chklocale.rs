//! chklocale.c: pg_get_encoding_from_locale — newlocale + nl_langinfo_l
//! CODESET probe mapped through encoding_match_list.

use core::ffi::CStr;

use libc::{c_char, locale_t};
use types_error::{PgResult, WARNING};
use wchar::{
    pg_enc, PG_BIG5, PG_EUC_CN, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK,
    PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_JOHAB, PG_KOI8R, PG_KOI8U,
    PG_LATIN1, PG_LATIN10, PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7,
    PG_LATIN8, PG_LATIN9, PG_SHIFT_JIS_2004, PG_SJIS, PG_SQL_ASCII, PG_UHC, PG_UTF8, PG_WIN1250,
    PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255, PG_WIN1256, PG_WIN1257,
    PG_WIN1258, PG_WIN866, PG_WIN874,
};

const ENCODING_MATCH_LIST: &[(pg_enc, &str)] = &[
    (PG_EUC_JP, "EUC-JP"),
    (PG_EUC_JP, "eucJP"),
    (PG_EUC_JP, "IBM-eucJP"),
    (PG_EUC_JP, "sdeckanji"),
    (PG_EUC_JP, "CP20932"),
    (PG_EUC_CN, "EUC-CN"),
    (PG_EUC_CN, "eucCN"),
    (PG_EUC_CN, "IBM-eucCN"),
    (PG_EUC_CN, "GB2312"),
    (PG_EUC_CN, "dechanzi"),
    (PG_EUC_CN, "CP20936"),
    (PG_EUC_KR, "EUC-KR"),
    (PG_EUC_KR, "eucKR"),
    (PG_EUC_KR, "IBM-eucKR"),
    (PG_EUC_KR, "deckorean"),
    (PG_EUC_KR, "5601"),
    (PG_EUC_KR, "CP51949"),
    (PG_EUC_TW, "EUC-TW"),
    (PG_EUC_TW, "eucTW"),
    (PG_EUC_TW, "IBM-eucTW"),
    (PG_EUC_TW, "cns11643"),
    (PG_UTF8, "UTF-8"),
    (PG_UTF8, "utf8"),
    (PG_UTF8, "CP65001"),
    (PG_LATIN1, "ISO-8859-1"),
    (PG_LATIN1, "ISO8859-1"),
    (PG_LATIN1, "iso88591"),
    (PG_LATIN1, "CP28591"),
    (PG_LATIN2, "ISO-8859-2"),
    (PG_LATIN2, "ISO8859-2"),
    (PG_LATIN2, "iso88592"),
    (PG_LATIN2, "CP28592"),
    (PG_LATIN3, "ISO-8859-3"),
    (PG_LATIN3, "ISO8859-3"),
    (PG_LATIN3, "iso88593"),
    (PG_LATIN3, "CP28593"),
    (PG_LATIN4, "ISO-8859-4"),
    (PG_LATIN4, "ISO8859-4"),
    (PG_LATIN4, "iso88594"),
    (PG_LATIN4, "CP28594"),
    (PG_LATIN5, "ISO-8859-9"),
    (PG_LATIN5, "ISO8859-9"),
    (PG_LATIN5, "iso88599"),
    (PG_LATIN5, "CP28599"),
    (PG_LATIN6, "ISO-8859-10"),
    (PG_LATIN6, "ISO8859-10"),
    (PG_LATIN6, "iso885910"),
    (PG_LATIN7, "ISO-8859-13"),
    (PG_LATIN7, "ISO8859-13"),
    (PG_LATIN7, "iso885913"),
    (PG_LATIN8, "ISO-8859-14"),
    (PG_LATIN8, "ISO8859-14"),
    (PG_LATIN8, "iso885914"),
    (PG_LATIN9, "ISO-8859-15"),
    (PG_LATIN9, "ISO8859-15"),
    (PG_LATIN9, "iso885915"),
    (PG_LATIN9, "CP28605"),
    (PG_LATIN10, "ISO-8859-16"),
    (PG_LATIN10, "ISO8859-16"),
    (PG_LATIN10, "iso885916"),
    (PG_KOI8R, "KOI8-R"),
    (PG_KOI8R, "CP20866"),
    (PG_KOI8U, "KOI8-U"),
    (PG_KOI8U, "CP21866"),
    (PG_WIN866, "CP866"),
    (PG_WIN874, "CP874"),
    (PG_WIN1250, "CP1250"),
    (PG_WIN1251, "CP1251"),
    (PG_WIN1251, "ansi-1251"),
    (PG_WIN1252, "CP1252"),
    (PG_WIN1253, "CP1253"),
    (PG_WIN1254, "CP1254"),
    (PG_WIN1255, "CP1255"),
    (PG_WIN1256, "CP1256"),
    (PG_WIN1257, "CP1257"),
    (PG_WIN1258, "CP1258"),
    (PG_ISO_8859_5, "ISO-8859-5"),
    (PG_ISO_8859_5, "ISO8859-5"),
    (PG_ISO_8859_5, "iso88595"),
    (PG_ISO_8859_5, "CP28595"),
    (PG_ISO_8859_6, "ISO-8859-6"),
    (PG_ISO_8859_6, "ISO8859-6"),
    (PG_ISO_8859_6, "iso88596"),
    (PG_ISO_8859_6, "CP28596"),
    (PG_ISO_8859_7, "ISO-8859-7"),
    (PG_ISO_8859_7, "ISO8859-7"),
    (PG_ISO_8859_7, "iso88597"),
    (PG_ISO_8859_7, "CP28597"),
    (PG_ISO_8859_8, "ISO-8859-8"),
    (PG_ISO_8859_8, "ISO8859-8"),
    (PG_ISO_8859_8, "iso88598"),
    (PG_ISO_8859_8, "CP28598"),
    (PG_SJIS, "SJIS"),
    (PG_SJIS, "PCK"),
    (PG_SJIS, "CP932"),
    (PG_SJIS, "SHIFT_JIS"),
    (PG_BIG5, "BIG5"),
    (PG_BIG5, "BIG5HKSCS"),
    (PG_BIG5, "Big5-HKSCS"),
    (PG_BIG5, "CP950"),
    (PG_GBK, "GBK"),
    (PG_GBK, "CP936"),
    (PG_UHC, "UHC"),
    (PG_UHC, "CP949"),
    (PG_JOHAB, "JOHAB"),
    (PG_JOHAB, "CP1361"),
    (PG_GB18030, "GB18030"),
    (PG_GB18030, "CP54936"),
    (PG_SHIFT_JIS_2004, "SJIS_2004"),
    (PG_SQL_ASCII, "US-ASCII"),
];

extern "C" {
    fn nl_langinfo_l(item: libc::nl_item, loc: locale_t) -> *mut c_char;
}

/// pg_get_encoding_from_locale(ctype, write_message) (chklocale.c:301).
/// `None` ctype means the active LC_CTYPE, per C's setlocale(LC_CTYPE, NULL).
pub fn pg_get_encoding_from_locale(ctype: Option<&str>, write_message: bool) -> PgResult<i32> {
    let active;
    let ctype = match ctype {
        Some(c) => c,
        None => {
            // This backend's permanent LC_CTYPE. After the global-locale
            // freeze, per-thread pg_perm_setlocale records replace C's
            // per-process global (our backends are threads); fall back to
            // reading the global, which is safe: it is only written during
            // single-threaded boot.
            active = match crate::setup::thread_locale(crate::lc::LC_CTYPE) {
                Some(v) => v,
                None => {
                    // SAFETY: setlocale(cat, NULL) only reads; result copied
                    // before any other locale call.
                    let p =
                        unsafe { libc::setlocale(crate::lc::LC_CTYPE, core::ptr::null()) };
                    if p.is_null() {
                        String::new()
                    } else {
                        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
                    }
                }
            };
            &active
        }
    };

    if ctype.eq_ignore_ascii_case("C") || ctype.eq_ignore_ascii_case("POSIX") {
        return Ok(PG_SQL_ASCII as i32);
    }

    let mut cbuf = Vec::with_capacity(ctype.len() + 1);
    cbuf.extend_from_slice(ctype.as_bytes());
    cbuf.push(0);
    // SAFETY: cbuf is NUL-terminated and outlives the call.
    let loc = unsafe {
        libc::newlocale(
            crate::lc::LC_CTYPE_MASK,
            cbuf.as_ptr() as *const c_char,
            core::ptr::null_mut(),
        )
    };
    if loc.is_null() {
        return Ok(-1); // bogus ctype passed in?
    }
    // SAFETY: loc is valid; nl_langinfo_l's result is copied before freelocale.
    let sys = unsafe {
        let p = nl_langinfo_l(libc::CODESET, loc);
        let s = if p.is_null() {
            None
        } else {
            Some(CStr::from_ptr(p).to_string_lossy().into_owned())
        };
        libc::freelocale(loc);
        s
    };
    let Some(sys) = sys else { return Ok(-1) };

    for (enc, name) in ENCODING_MATCH_LIST {
        if sys.eq_ignore_ascii_case(name) {
            return Ok(*enc as i32);
        }
    }

    // Current macOS reports an empty CODESET for many locales; they all
    // actually use UTF-8 (chklocale.c __darwin__ kluge).
    #[cfg(target_os = "macos")]
    if sys.is_empty() {
        return Ok(PG_UTF8 as i32);
    }

    if write_message {
        elog::ereport(WARNING)
            .errmsg(format!(
                "could not determine encoding for locale \"{ctype}\": codeset is \"{sys}\""
            ))
            .finish(types_error::ErrorLocation::new(
                "src/port/chklocale.c",
                377,
                "pg_get_encoding_from_locale",
            ))?;
    }

    Ok(-1)
}
