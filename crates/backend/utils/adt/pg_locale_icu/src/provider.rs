//! The live `#ifdef USE_ICU` body of `pg_locale_icu.c` (+ the ICU helpers
//! `pg_locale.c`/`collationcmds.c` own), bound to the system ICU.
//!
//! Bounded to the UTF-8 server encoding (see the crate docs): compare uses
//! `ucol_strcollUTF8` on raw bytes; sort keys convert UTF-8 → `UChar` via
//! `u_strFromUTF8` then `ucol_getSortKey`.

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::ffi::c_char;
use core::ptr;

use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::ffi::{self, *};

/// An owned ICU collator (`info.icu`): the `UCollator *` plus the locale string
/// the collator was opened with (C's `info.icu.locale`, used by case mapping —
/// not exercised by the bounded compare/sort-key path but kept for fidelity).
pub struct IcuLocale {
    ucol: *mut UCollator,
    /// `info.icu.locale` — the locale string the collator was opened with; the
    /// case-mapping path passes it to the `u_strTo*` functions.
    locale: String,
}

// SAFETY: a UCollator is touched only by the owning backend thread; the cache
// holds it for the backend lifetime and never shares it across threads.
unsafe impl Send for IcuLocale {}
unsafe impl Sync for IcuLocale {}

impl Drop for IcuLocale {
    fn drop(&mut self) {
        if !self.ucol.is_null() {
            // SAFETY: ucol was produced by ucol_open and is owned here.
            unsafe { ffi::ucol_close(self.ucol) };
        }
    }
}

/// Build a NUL-terminated byte vector for an FFI `*const c_char` argument.
fn cstr(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

/// `pg_ucol_open(loc_str)` (`pg_locale_icu.c:230`) — open a `UCollator` for the
/// locale string. The `< 55` / `< 54` legacy fixups are compiled out (modern
/// ICU does them inside `ucol_open`).
fn pg_ucol_open(loc_str: &str) -> PgResult<*mut UCollator> {
    let cloc = cstr(loc_str);
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: cloc is NUL-terminated; status is a valid out-pointer.
    let collator = unsafe { ffi::ucol_open(cloc.as_ptr() as *const c_char, &mut status) };
    if u_failure(status) {
        return Err(PgError::error(format!(
            "could not open collator for locale \"{loc_str}\": {}",
            error_name(status)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    Ok(collator)
}

/// `create_pg_locale_icu` collator construction = `make_icu_collator(iculocstr,
/// NULL)` = `pg_ucol_open(iculocstr)`. ICU custom-rules collations (non-NULL
/// `collicurules`) are out of the bounded scope.
pub fn make_icu_locale(iculocstr: &str) -> PgResult<IcuLocale> {
    let ucol = pg_ucol_open(iculocstr)?;
    Ok(IcuLocale {
        ucol,
        locale: iculocstr.to_string(),
    })
}

/// `strncoll_icu_utf8(arg1, arg2, locale)` (`pg_locale_icu.c:471`) —
/// `ucol_strcollUTF8` on the raw UTF-8 payloads. This is the bounded
/// (UTF-8-only) compare path.
pub fn strncoll_icu(locale: &IcuLocale, arg1: &[u8], arg2: &[u8]) -> PgResult<i32> {
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: arg1/arg2 are valid byte slices with concrete lengths; ucol is the
    // owned collator; status is a valid out-pointer.
    let result = unsafe {
        ffi::ucol_strcoll_utf8(
            locale.ucol,
            arg1.as_ptr() as *const c_char,
            arg1.len() as i32,
            arg2.as_ptr() as *const c_char,
            arg2.len() as i32,
            &mut status,
        )
    };
    if u_failure(status) {
        return Err(PgError::error(format!(
            "collation failed: {}",
            error_name(status)
        )));
    }
    Ok(result)
}

/// Convert a UTF-8 byte slice to a `UChar` (`u16`) vector via `u_strFromUTF8`.
/// Returns the NUL-free UChar buffer.
fn utf8_to_uchar(src: &[u8]) -> PgResult<Vec<UChar>> {
    // First pass: learn the length (dest capacity 0, NUL dest).
    let mut status: UErrorCode = U_ZERO_ERROR;
    let mut dest_len: i32 = 0;
    // SAFETY: NULL dest with capacity 0 is the documented preflight form.
    unsafe {
        ffi::u_str_from_utf8(
            ptr::null_mut(),
            0,
            &mut dest_len,
            src.as_ptr() as *const c_char,
            src.len() as i32,
            &mut status,
        );
    }
    if u_failure(status) && status != U_BUFFER_OVERFLOW_ERROR {
        return Err(PgError::error(format!(
            "u_strFromUTF8 failed: {}",
            error_name(status)
        )));
    }
    // Allocate dest_len + 1 (room for the NUL u_strFromUTF8 writes).
    let mut out: Vec<UChar> = vec![0u16; (dest_len as usize) + 1];
    status = U_ZERO_ERROR;
    let mut written: i32 = 0;
    // SAFETY: out has dest_len+1 elements; src is a valid byte slice.
    unsafe {
        ffi::u_str_from_utf8(
            out.as_mut_ptr(),
            (dest_len + 1) as i32,
            &mut written,
            src.as_ptr() as *const c_char,
            src.len() as i32,
            &mut status,
        );
    }
    if u_failure(status) {
        return Err(PgError::error(format!(
            "u_strFromUTF8 failed: {}",
            error_name(status)
        )));
    }
    out.truncate(written as usize);
    Ok(out)
}

/// `strnxfrm_icu(src, locale)` (`pg_locale_icu.c:496`) — the sort-key transform.
/// Bounded (UTF-8): convert UTF-8 → `UChar`, then `ucol_getSortKey`. Returns the
/// full sort-key blob *without* the trailing NUL (matching the C contract that
/// the returned length excludes the terminator).
pub fn strnxfrm_icu(locale: &IcuLocale, src: &[u8]) -> PgResult<Vec<u8>> {
    let uchar = utf8_to_uchar(src)?;
    let ulen = uchar.len() as i32;

    // First call with destsize 0 to learn the needed length (includes the NUL).
    // SAFETY: NULL dest with size 0 is the documented preflight form.
    let needed = unsafe {
        ffi::ucol_get_sort_key(locale.ucol, uchar.as_ptr(), ulen, ptr::null_mut(), 0)
    };
    if needed <= 0 {
        // ucol_getSortKey returns 0 only on internal error; result must be > 0.
        return Err(PgError::error("sort key generation failed"));
    }

    let mut out: Vec<u8> = vec![0u8; needed as usize];
    // SAFETY: out has `needed` bytes; uchar is a valid UChar buffer.
    let written = unsafe {
        ffi::ucol_get_sort_key(
            locale.ucol,
            uchar.as_ptr(),
            ulen,
            out.as_mut_ptr(),
            needed,
        )
    };
    // C: result_bsize--; (drop the counted NUL terminator).
    let len = (written.max(1) as usize).saturating_sub(1);
    out.truncate(len);
    Ok(out)
}

/// Convert a `UChar` slice back to a UTF-8 byte vector via `u_strToUTF8`.
fn uchar_to_utf8(src: &[UChar]) -> PgResult<Vec<u8>> {
    let mut status: UErrorCode = U_ZERO_ERROR;
    let mut dest_len: i32 = 0;
    // Preflight for the byte length.
    // SAFETY: NULL dest with capacity 0 is the documented preflight form.
    unsafe {
        ffi::u_str_to_utf8(
            ptr::null_mut(),
            0,
            &mut dest_len,
            src.as_ptr(),
            src.len() as i32,
            &mut status,
        );
    }
    if u_failure(status) && status != U_BUFFER_OVERFLOW_ERROR {
        return Err(PgError::error(format!(
            "u_strToUTF8 failed: {}",
            error_name(status)
        )));
    }
    let mut out: Vec<u8> = vec![0u8; (dest_len as usize) + 1];
    status = U_ZERO_ERROR;
    let mut written: i32 = 0;
    // SAFETY: out has dest_len+1 bytes; src is a valid UChar slice.
    unsafe {
        ffi::u_str_to_utf8(
            out.as_mut_ptr() as *mut c_char,
            (dest_len + 1) as i32,
            &mut written,
            src.as_ptr(),
            src.len() as i32,
            &mut status,
        );
    }
    if u_failure(status) {
        return Err(PgError::error(format!(
            "u_strToUTF8 failed: {}",
            error_name(status)
        )));
    }
    out.truncate(written as usize);
    Ok(out)
}

/// The signature of the ICU `u_strToLower`/`u_strToUpper` family (which take a
/// `locale` C string), as a Rust closure adapter.
type CaseFn = unsafe extern "C" fn(
    *mut UChar,
    i32,
    *const UChar,
    i32,
    *const c_char,
    *mut UErrorCode,
) -> i32;

/// `icu_convert_case(func, locale, src_uchar)` (`pg_locale_icu.c:660`): apply a
/// locale-taking ICU case function with the grow-on-`U_BUFFER_OVERFLOW_ERROR`
/// retry, returning the converted `UChar` vector.
fn icu_convert_case(func: CaseFn, locale_c: &[u8], src: &[UChar]) -> PgResult<Vec<UChar>> {
    let mut len_dest = src.len().max(1) as i32; // try first with same length
    let mut dest: Vec<UChar> = vec![0u16; len_dest as usize];
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: dest has len_dest elements; src valid; locale_c NUL-terminated.
    len_dest = unsafe {
        func(
            dest.as_mut_ptr(),
            len_dest,
            src.as_ptr(),
            src.len() as i32,
            locale_c.as_ptr() as *const c_char,
            &mut status,
        )
    };
    if status == U_BUFFER_OVERFLOW_ERROR {
        // Retry with the reported length.
        dest = vec![0u16; len_dest.max(0) as usize];
        status = U_ZERO_ERROR;
        // SAFETY: dest has len_dest elements now.
        len_dest = unsafe {
            func(
                dest.as_mut_ptr(),
                len_dest,
                src.as_ptr(),
                src.len() as i32,
                locale_c.as_ptr() as *const c_char,
                &mut status,
            )
        };
    }
    if u_failure(status) {
        return Err(PgError::error(format!(
            "case conversion failed: {}",
            error_name(status)
        )));
    }
    dest.truncate(len_dest.max(0) as usize);
    Ok(dest)
}

/// `strlower_icu` (`pg_locale_icu.c:383`) — bounded UTF-8: UTF-8 → UChar →
/// `u_strToLower` → UTF-8.
pub fn strlower_icu(locale: &IcuLocale, src: &[u8]) -> PgResult<Vec<u8>> {
    let uchar = utf8_to_uchar(src)?;
    let loc = cstr(&locale.locale);
    let conv = icu_convert_case(ffi::u_str_to_lower, &loc, &uchar)?;
    uchar_to_utf8(&conv)
}

/// `strupper_icu` (`pg_locale_icu.c:423`).
pub fn strupper_icu(locale: &IcuLocale, src: &[u8]) -> PgResult<Vec<u8>> {
    let uchar = utf8_to_uchar(src)?;
    let loc = cstr(&locale.locale);
    let conv = icu_convert_case(ffi::u_str_to_upper, &loc, &uchar)?;
    uchar_to_utf8(&conv)
}

/// `strtitle_icu` (`pg_locale_icu.c:403`) — `u_strToTitle` with a NULL break
/// iterator (`u_strToTitle_default_BI`).
pub fn strtitle_icu(locale: &IcuLocale, src: &[u8]) -> PgResult<Vec<u8>> {
    let uchar = utf8_to_uchar(src)?;
    let loc = cstr(&locale.locale);
    // u_strToTitle takes an extra titleIter arg; adapt to the CaseFn shape via a
    // local wrapper that passes NULL.
    let conv = icu_convert_case_title(&loc, &uchar)?;
    uchar_to_utf8(&conv)
}

/// `u_strToTitle(dest, cap, src, len, NULL, locale, status)` with the overflow
/// retry (the title fn has an extra `titleIter` parameter, so it can't share
/// [`icu_convert_case`]'s [`CaseFn`] signature).
fn icu_convert_case_title(locale_c: &[u8], src: &[UChar]) -> PgResult<Vec<UChar>> {
    let mut len_dest = src.len().max(1) as i32;
    let mut dest: Vec<UChar> = vec![0u16; len_dest as usize];
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: dest has len_dest elements; NULL titleIter is documented.
    len_dest = unsafe {
        ffi::u_str_to_title(
            dest.as_mut_ptr(),
            len_dest,
            src.as_ptr(),
            src.len() as i32,
            ptr::null_mut(),
            locale_c.as_ptr() as *const c_char,
            &mut status,
        )
    };
    if status == U_BUFFER_OVERFLOW_ERROR {
        dest = vec![0u16; len_dest.max(0) as usize];
        status = U_ZERO_ERROR;
        // SAFETY: dest resized to len_dest.
        len_dest = unsafe {
            ffi::u_str_to_title(
                dest.as_mut_ptr(),
                len_dest,
                src.as_ptr(),
                src.len() as i32,
                ptr::null_mut(),
                locale_c.as_ptr() as *const c_char,
                &mut status,
            )
        };
    }
    if u_failure(status) {
        return Err(PgError::error(format!(
            "case conversion failed: {}",
            error_name(status)
        )));
    }
    dest.truncate(len_dest.max(0) as usize);
    Ok(dest)
}

/// `strfold_icu` (`pg_locale_icu.c:443`) — `u_strFoldCase` with the Turkic
/// special-I option enabled for 'tr'/'az' (`u_strFoldCase_default`).
pub fn strfold_icu(locale: &IcuLocale, src: &[u8]) -> PgResult<Vec<u8>> {
    let uchar = utf8_to_uchar(src)?;

    // Determine the fold options: U_FOLD_CASE_EXCLUDE_SPECIAL_I for tr/az.
    let mut options = ffi::U_FOLD_CASE_DEFAULT;
    if let Ok(lang) = uloc_get_language_str(&locale.locale) {
        if lang == "tr" || lang == "az" {
            options = ffi::U_FOLD_CASE_EXCLUDE_SPECIAL_I;
        }
    }

    let mut len_dest = uchar.len().max(1) as i32;
    let mut dest: Vec<UChar> = vec![0u16; len_dest as usize];
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: dest has len_dest elements; uchar valid.
    len_dest = unsafe {
        ffi::u_str_fold_case(
            dest.as_mut_ptr(),
            len_dest,
            uchar.as_ptr(),
            uchar.len() as i32,
            options,
            &mut status,
        )
    };
    if status == U_BUFFER_OVERFLOW_ERROR {
        dest = vec![0u16; len_dest.max(0) as usize];
        status = U_ZERO_ERROR;
        // SAFETY: dest resized to len_dest.
        len_dest = unsafe {
            ffi::u_str_fold_case(
                dest.as_mut_ptr(),
                len_dest,
                uchar.as_ptr(),
                uchar.len() as i32,
                options,
                &mut status,
            )
        };
    }
    if u_failure(status) {
        return Err(PgError::error(format!(
            "case conversion failed: {}",
            error_name(status)
        )));
    }
    dest.truncate(len_dest.max(0) as usize);
    uchar_to_utf8(&dest)
}

/// `get_collation_actual_version_icu(collcollate)` (`pg_locale_icu.c:574`) —
/// open the collator, read `ucol_getVersion`, format with `u_versionToString`.
pub fn get_collation_actual_version_icu(collcollate: &str) -> PgResult<String> {
    let collator = pg_ucol_open(collcollate)?;
    let mut versioninfo: UVersionInfo = [0u8; U_MAX_VERSION_LENGTH];
    // SAFETY: collator is owned; versioninfo is a U_MAX_VERSION_LENGTH array.
    unsafe {
        ffi::ucol_get_version(collator, versioninfo.as_mut_ptr());
        ffi::ucol_close(collator);
    }
    let mut buf = [0i8; U_MAX_VERSION_STRING_LENGTH];
    // SAFETY: versioninfo is U_MAX_VERSION_LENGTH; buf is
    // U_MAX_VERSION_STRING_LENGTH (the documented minimum).
    unsafe {
        ffi::u_version_to_string(versioninfo.as_ptr(), buf.as_mut_ptr() as *mut c_char);
    }
    // buf is NUL-terminated.
    let bytes: &[u8] = unsafe { &*(buf.as_slice() as *const [i8] as *const [u8]) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// `uloc_getLanguage(loc_str)` into a fixed `ULOC_LANG_CAPACITY` buffer; returns
/// the language string, or `Err` carrying the ICU status text.
fn uloc_get_language_str(loc_str: &str) -> Result<String, (UErrorCode, String)> {
    let cloc = cstr(loc_str);
    let mut lang = [0i8; ULOC_LANG_CAPACITY];
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: cloc NUL-terminated; lang is ULOC_LANG_CAPACITY bytes.
    unsafe {
        ffi::uloc_get_language(
            cloc.as_ptr() as *const c_char,
            lang.as_mut_ptr() as *mut c_char,
            ULOC_LANG_CAPACITY as i32,
            &mut status,
        );
    }
    if u_failure(status) || status == U_STRING_NOT_TERMINATED_WARNING {
        return Err((status, error_name(status)));
    }
    let bytes: &[u8] = unsafe { &*(lang.as_slice() as *const [i8] as *const [u8]) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// A best-effort ICU locale-validation problem (`icu_validate_locale`,
/// `pg_locale.c:1608`). The first two are emitted at the configurable
/// `icu_validation_level` (default WARNING); the caller decides the elevel. The
/// language/unknown-language messages carry the "set icu_validation_level to
/// disabled" hint. The collator-open failure is a hard ERROR and surfaces as a
/// `PgError` directly (not here).
pub enum IcuValidateProblem {
    /// `could not get language from ICU locale "%s": %s`.
    CannotGetLanguage { loc: String, name: String },
    /// `ICU locale "%s" has unknown language "%s"`.
    UnknownLanguage { loc: String, lang: String },
}

/// `icu_validate_locale(loc_str)` (`pg_locale.c:1608`) — best-effort validation.
/// Returns `Ok(Some(problem))` for an elevel-gated problem (the caller emits at
/// `icu_validation_level`), `Ok(None)` on success. The final collator-open check
/// is a hard ERROR and returns `Err` regardless of elevel.
pub fn icu_validate_locale(loc_str: &str) -> PgResult<Option<IcuValidateProblem>> {
    // Validate that we can extract the language.
    let lang = match uloc_get_language_str(loc_str) {
        Ok(l) => l,
        Err((_, name)) => {
            return Ok(Some(IcuValidateProblem::CannotGetLanguage {
                loc: loc_str.to_string(),
                name,
            }));
        }
    };

    // Check for special language name (root locale).
    let mut found = lang.is_empty() || lang == "root" || lang == "und";

    // Search for matching language within ICU.
    // SAFETY: uloc_countAvailable takes no args.
    let count = unsafe { ffi::uloc_count_available() };
    let mut i = 0;
    while !found && i < count {
        // SAFETY: i is in [0, count); uloc_getAvailable returns a static string.
        let otherloc = unsafe { ffi::uloc_get_available(i) };
        if !otherloc.is_null() {
            let otherloc = unsafe { core::ffi::CStr::from_ptr(otherloc) };
            if let Ok(s) = otherloc.to_str() {
                if let Ok(otherlang) = uloc_get_language_str(s) {
                    if otherlang == lang {
                        found = true;
                    }
                }
            }
        }
        i += 1;
    }

    if !found {
        return Ok(Some(IcuValidateProblem::UnknownLanguage {
            loc: loc_str.to_string(),
            lang,
        }));
    }

    // Check that it can be opened (hard ERROR on failure, any elevel).
    let collator = pg_ucol_open(loc_str)?;
    // SAFETY: collator is owned.
    unsafe { ffi::ucol_close(collator) };
    Ok(None)
}

/// `icu_language_tag(loc_str, elevel)` (`pg_locale.c:1550`) — canonicalize an ICU
/// locale to a BCP-47 language tag via `uloc_toLanguageTag` (strict). Returns
/// `Ok(tag)` on success, or `Err(name)` carrying the ICU error name on a
/// conversion failure (the caller emits "could not convert locale name ..." at
/// the requested elevel and treats the failure as `NULL`).
pub fn icu_language_tag(loc_str: &str) -> Result<String, String> {
    let cloc = cstr(loc_str);
    let mut buflen: usize = 32;
    loop {
        let mut buf: Vec<u8> = vec![0u8; buflen];
        let mut status: UErrorCode = U_ZERO_ERROR;
        // SAFETY: cloc NUL-terminated; buf has buflen bytes; strict = 1.
        unsafe {
            ffi::uloc_to_language_tag(
                cloc.as_ptr() as *const c_char,
                buf.as_mut_ptr() as *mut c_char,
                buflen as i32,
                1, /* strict = true */
                &mut status,
            );
        }
        if (status == U_BUFFER_OVERFLOW_ERROR || status == U_STRING_NOT_TERMINATED_WARNING)
            && buflen < (1usize << 30)
        {
            buflen = (buflen * 2).min(1usize << 30);
            continue;
        }
        if u_failure(status) {
            return Err(error_name(status));
        }
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        return Ok(String::from_utf8_lossy(&buf[..end]).into_owned());
    }
}

/// `enumerate_icu_locales` (collationcmds.c:986) — the ICU root locale ("")
/// prepended to `uloc_getAvailable(0..uloc_countAvailable())` (the C loop starts
/// at `i = -1`, treating `-1` as the root locale).
pub fn enumerate_icu_locales() -> PgResult<Vec<String>> {
    let mut out = Vec::new();
    out.push(String::new()); /* i == -1: the root locale */
    // SAFETY: uloc_countAvailable takes no args.
    let count = unsafe { ffi::uloc_count_available() };
    for i in 0..count {
        // SAFETY: i in [0, count); returns a static NUL-terminated string.
        let p = unsafe { ffi::uloc_get_available(i) };
        if p.is_null() {
            continue;
        }
        let s = unsafe { core::ffi::CStr::from_ptr(p) };
        if let Ok(s) = s.to_str() {
            out.push(s.to_string());
        }
    }
    Ok(out)
}

/// `get_icu_locale_comment(localename)` (collationcmds.c:646) — the English
/// display name via `uloc_getDisplayName`, accepted only if all-ASCII (else
/// `None`, since template0 must be encoding-agnostic).
pub fn get_icu_locale_comment(localename: &str) -> PgResult<Option<String>> {
    let cloc = cstr(localename);
    let cen = cstr("en");
    let mut displayname = [0u16; 128];
    let mut status: UErrorCode = U_ZERO_ERROR;
    // SAFETY: cloc/cen NUL-terminated; displayname is 128 UChars.
    let len_uchar = unsafe {
        ffi::uloc_get_display_name(
            cloc.as_ptr() as *const c_char,
            cen.as_ptr() as *const c_char,
            displayname.as_mut_ptr(),
            displayname.len() as i32,
            &mut status,
        )
    };
    if u_failure(status) {
        return Ok(None); /* no good reason to raise an error */
    }
    let len = (len_uchar.max(0) as usize).min(displayname.len());
    let mut result = String::with_capacity(len);
    for &u in &displayname[..len] {
        if u > 127 {
            return Ok(None); /* non-ASCII comment: reject */
        }
        result.push(u as u8 as char);
    }
    Ok(Some(result))
}

/// `encoding not supported by ICU` — the error a non-UTF-8 server encoding hits
/// on the bounded compare/sort-key path (the `UConverter` path is not ported).
pub fn non_utf8_unsupported(what: &str) -> PgError {
    PgError::error(format!(
        "ICU collation {what} is only supported for UTF-8 encoding in this build"
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icu_compare_en_us_orders_case_then_letters() {
        // en-US ICU: "abc" < "abd"; case-insensitive secondary handled by locale.
        let loc = make_icu_locale("en-US").expect("open en-US collator");
        assert!(strncoll_icu(&loc, b"abc", b"abd").unwrap() < 0);
        assert!(strncoll_icu(&loc, b"abd", b"abc").unwrap() > 0);
        assert_eq!(strncoll_icu(&loc, b"abc", b"abc").unwrap(), 0);
    }

    #[test]
    fn icu_root_secondary_strength_is_case_insensitive() {
        // "@colStrength=secondary" on the root locale: 'a' == 'A' (equal at the
        // collation level; the deterministic tiebreak is applied by varstr_cmp,
        // not here).
        let loc = make_icu_locale("und-u-ks-level2").expect("open secondary collator");
        assert_eq!(strncoll_icu(&loc, b"abc", b"ABC").unwrap(), 0);
    }

    #[test]
    fn icu_sort_key_roundtrips_compare_order() {
        let loc = make_icu_locale("en-US").expect("open en-US collator");
        let ka = strnxfrm_icu(&loc, b"abc").unwrap();
        let kb = strnxfrm_icu(&loc, b"abd").unwrap();
        // Sort keys compare bytewise in the same order as strncoll.
        assert!(ka < kb);
        // Equal strings produce equal sort keys.
        let ka2 = strnxfrm_icu(&loc, b"abc").unwrap();
        assert_eq!(ka, ka2);
    }

    #[test]
    fn icu_case_mapping_lower_upper_fold() {
        let loc = make_icu_locale("en-US").expect("open en-US collator");
        assert_eq!(strlower_icu(&loc, "HIJ".as_bytes()).unwrap(), b"hij");
        assert_eq!(strupper_icu(&loc, "hij".as_bytes()).unwrap(), b"HIJ");
        // German sharp-s folds to "ss".
        assert_eq!(strfold_icu(&loc, "ß".as_bytes()).unwrap(), b"ss");
    }

    #[test]
    fn icu_turkish_lower_keeps_dotless_i() {
        // tr-TR lowercases 'I' to dotless 'ı' (U+0131).
        let loc = make_icu_locale("tr-TR").expect("open tr collator");
        assert_eq!(strlower_icu(&loc, "I".as_bytes()).unwrap(), "ı".as_bytes());
    }

    #[test]
    fn icu_language_tag_canonicalizes() {
        assert_eq!(icu_language_tag("en_US").unwrap(), "en-US");
        // An attribute-bearing locale canonicalizes to BCP-47 -u- form.
        assert_eq!(
            icu_language_tag("@colStrength=secondary").unwrap(),
            "und-u-ks-level2"
        );
        // A bogus attribute fails strict conversion.
        assert!(icu_language_tag("@colStrength=primary;nonsense=yes").is_err());
    }

    #[test]
    fn icu_validate_locale_flags_unknown_language() {
        // A known language validates (None = no problem).
        assert!(icu_validate_locale("en-US").unwrap().is_none());
        // An unknown language is flagged (elevel-gated by the caller).
        match icu_validate_locale("nonsense-nowhere").unwrap() {
            Some(IcuValidateProblem::UnknownLanguage { lang, .. }) => {
                assert_eq!(lang, "nonsense");
            }
            other => panic!("expected UnknownLanguage, got {:?}", other.is_some()),
        }
    }

    #[test]
    fn icu_actual_version_is_nonempty() {
        let v = get_collation_actual_version_icu("en-US").unwrap();
        assert!(!v.is_empty());
        // Version strings are dotted numerics, e.g. "153.128".
        assert!(v.chars().next().unwrap().is_ascii_digit());
    }

    #[test]
    fn enumerate_includes_root_and_locales() {
        let locales = enumerate_icu_locales().unwrap();
        // First entry is the root locale ("").
        assert_eq!(locales[0], "");
        // ICU ships many locales.
        assert!(locales.len() > 10);
    }
}
