//! dlopen/dlsym binding to the system libicu (i18n + common), resolved once
//! per process. No build-time ICU dependency: PGDG images ship version-
//! suffixed symbols (ucol_open_72), so each symbol is probed bare then with
//! the soname's major suffix, dfmgr-style.

#![allow(non_camel_case_types, non_snake_case, clippy::upper_case_acronyms)]

use core::ffi::{c_char, c_int, c_void};
use std::sync::OnceLock;

pub type UChar = u16;
pub type UErrorCode = i32;
pub type UBool = i8;

pub const U_ZERO_ERROR: UErrorCode = 0;
pub const U_BUFFER_OVERFLOW_ERROR: UErrorCode = 15;
pub const U_STRING_NOT_TERMINATED_WARNING: UErrorCode = -124;

#[inline]
pub fn U_FAILURE(status: UErrorCode) -> bool {
    status > U_ZERO_ERROR
}
#[inline]
pub fn U_SUCCESS(status: UErrorCode) -> bool {
    status <= U_ZERO_ERROR
}

pub enum UCollator {}
pub enum UConverter {}

// Stable public C struct (unicode/uiter.h): context + 5 int32 fields + 10 fn
// pointers ≈ 112 bytes on LP64; 256 is layout headroom, callee never reads
// past the real struct.
#[repr(C, align(8))]
pub struct UCharIterator(pub [u8; 256]);

impl UCharIterator {
    pub fn zeroed() -> UCharIterator {
        UCharIterator([0; 256])
    }
}

pub const UCOL_DEFAULT: c_int = -1;
pub const UCOL_DEFAULT_STRENGTH: c_int = 2;
pub const ULOC_LANG_CAPACITY: usize = 12;
pub const U_MAX_VERSION_STRING_LENGTH: usize = 20;
pub const U_FOLD_CASE_DEFAULT: u32 = 0;
pub const U_FOLD_CASE_EXCLUDE_SPECIAL_I: u32 = 1;

pub type ConvertFn = unsafe extern "C" fn(
    dest: *mut UChar,
    destCapacity: i32,
    src: *const UChar,
    srcLength: i32,
    locale: *const c_char,
    pErrorCode: *mut UErrorCode,
) -> i32;

pub struct IcuApi {
    pub major: i32,
    pub ucol_open:
        unsafe extern "C" fn(loc: *const c_char, status: *mut UErrorCode) -> *mut UCollator,
    pub ucol_close: unsafe extern "C" fn(coll: *mut UCollator),
    pub ucol_strcoll: unsafe extern "C" fn(
        coll: *const UCollator,
        s1: *const UChar,
        len1: i32,
        s2: *const UChar,
        len2: i32,
    ) -> c_int,
    pub ucol_strcollUTF8: unsafe extern "C" fn(
        coll: *const UCollator,
        s1: *const c_char,
        len1: i32,
        s2: *const c_char,
        len2: i32,
        status: *mut UErrorCode,
    ) -> c_int,
    pub ucol_getSortKey: unsafe extern "C" fn(
        coll: *const UCollator,
        src: *const UChar,
        srcLength: i32,
        result: *mut u8,
        resultLength: i32,
    ) -> i32,
    pub ucol_nextSortKeyPart: unsafe extern "C" fn(
        coll: *const UCollator,
        iter: *mut UCharIterator,
        state: *mut u32,
        dest: *mut u8,
        count: i32,
        status: *mut UErrorCode,
    ) -> i32,
    pub ucol_getVersion: unsafe extern "C" fn(coll: *const UCollator, info: *mut u8),
    pub ucol_getRules:
        unsafe extern "C" fn(coll: *const UCollator, length: *mut i32) -> *const UChar,
    pub ucol_openRules: unsafe extern "C" fn(
        rules: *const UChar,
        rulesLength: i32,
        normalizationMode: c_int,
        strength: c_int,
        parseError: *mut c_void,
        status: *mut UErrorCode,
    ) -> *mut UCollator,
    pub u_errorName: unsafe extern "C" fn(code: UErrorCode) -> *const c_char,
    pub u_versionToString: unsafe extern "C" fn(info: *const u8, versionString: *mut c_char),
    pub ucnv_open:
        unsafe extern "C" fn(name: *const c_char, status: *mut UErrorCode) -> *mut UConverter,
    pub ucnv_toUChars: unsafe extern "C" fn(
        cnv: *mut UConverter,
        dest: *mut UChar,
        destCapacity: i32,
        src: *const c_char,
        srcLength: i32,
        status: *mut UErrorCode,
    ) -> i32,
    pub ucnv_fromUChars: unsafe extern "C" fn(
        cnv: *mut UConverter,
        dest: *mut c_char,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        status: *mut UErrorCode,
    ) -> i32,
    pub u_strToLower: ConvertFn,
    pub u_strToUpper: ConvertFn,
    pub u_strToTitle: unsafe extern "C" fn(
        dest: *mut UChar,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        titleIter: *mut c_void,
        locale: *const c_char,
        pErrorCode: *mut UErrorCode,
    ) -> i32,
    pub u_strFoldCase: unsafe extern "C" fn(
        dest: *mut UChar,
        destCapacity: i32,
        src: *const UChar,
        srcLength: i32,
        options: u32,
        pErrorCode: *mut UErrorCode,
    ) -> i32,
    pub uloc_getLanguage: unsafe extern "C" fn(
        localeID: *const c_char,
        language: *mut c_char,
        languageCapacity: i32,
        err: *mut UErrorCode,
    ) -> i32,
    pub uloc_toLanguageTag: unsafe extern "C" fn(
        localeID: *const c_char,
        langtag: *mut c_char,
        langtagCapacity: i32,
        strict: UBool,
        err: *mut UErrorCode,
    ) -> i32,
    pub uloc_countAvailable: unsafe extern "C" fn() -> i32,
    pub uloc_getAvailable: unsafe extern "C" fn(n: i32) -> *const c_char,
    pub uiter_setUTF8:
        unsafe extern "C" fn(iter: *mut UCharIterator, s: *const c_char, length: i32),
    pub uiter_setString:
        unsafe extern "C" fn(iter: *mut UCharIterator, s: *const UChar, length: i32),
}

pub fn u_errorName_str(api: &IcuApi, status: UErrorCode) -> String {
    // SAFETY: u_errorName returns a static NUL-terminated string.
    unsafe {
        core::ffi::CStr::from_ptr((api.u_errorName)(status))
            .to_string_lossy()
            .into_owned()
    }
}

static ICU: OnceLock<Result<&'static IcuApi, String>> = OnceLock::new();

/// Panics on an unloadable/incomplete libicu: environment breakage, and a
/// stub would silently diverge from the C-with-ICU oracle.
pub fn icu() -> &'static IcuApi {
    match ICU.get_or_init(load) {
        Ok(api) => api,
        Err(e) => panic!("pg_locale_icu: {e}"),
    }
}

const MAJOR_MAX: i32 = 90;
const MAJOR_MIN: i32 = 50;

fn load() -> Result<&'static IcuApi, String> {
    let (handle, major_hint) = open_lib()?;
    let suffix = find_suffix(handle, major_hint)
        .ok_or_else(|| "libicui18n loaded but no ucol_open symbol found (probed bare and _50.._90 suffixes)".to_string())?;
    let api = resolve_all(handle, suffix)?;
    Ok(Box::leak(Box::new(api)))
}

fn open_lib() -> Result<(*mut c_void, Option<i32>), String> {
    for major in (MAJOR_MIN..=MAJOR_MAX).rev() {
        let name = format!("libicui18n.so.{major}\0");
        // SAFETY: name is NUL-terminated.
        let h = unsafe { libc::dlopen(name.as_ptr() as *const c_char, libc::RTLD_NOW) };
        if !h.is_null() {
            return Ok((h, Some(major)));
        }
    }
    // SAFETY: literal is NUL-terminated.
    let h = unsafe { libc::dlopen(c"libicui18n.so".as_ptr(), libc::RTLD_NOW) };
    if !h.is_null() {
        return Ok((h, None));
    }
    Err(format!(
        "could not dlopen libicui18n.so(.{MAJOR_MIN}..{MAJOR_MAX})"
    ))
}

fn sym(handle: *mut c_void, name: &str, suffix: i32) -> *mut c_void {
    let full = if suffix == 0 {
        format!("{name}\0")
    } else {
        format!("{name}_{suffix}\0")
    };
    // SAFETY: full is NUL-terminated; handle is a live dlopen handle.
    unsafe { libc::dlsym(handle, full.as_ptr() as *const c_char) }
}

// suffix 0 encodes unrenamed symbols (--disable-renaming builds).
fn find_suffix(handle: *mut c_void, major_hint: Option<i32>) -> Option<i32> {
    if !sym(handle, "ucol_open", 0).is_null() {
        return Some(0);
    }
    if let Some(m) = major_hint {
        if !sym(handle, "ucol_open", m).is_null() {
            return Some(m);
        }
    }
    (MAJOR_MIN..=MAJOR_MAX).rev().find(|&m| !sym(handle, "ucol_open", m).is_null())
}

fn resolve_all(handle: *mut c_void, suffix: i32) -> Result<IcuApi, String> {
    macro_rules! resolve {
        ($name:ident) => {{
            let p = sym(handle, stringify!($name), suffix);
            if p.is_null() {
                return Err(format!(
                    "libicu symbol {} (suffix {suffix}) not found",
                    stringify!($name)
                ));
            }
            // SAFETY: transmuting a non-null dlsym result to the C signature
            // declared for this ICU entry point (stable public C API).
            unsafe { core::mem::transmute(p) }
        }};
    }
    Ok(IcuApi {
        major: suffix,
        ucol_open: resolve!(ucol_open),
        ucol_close: resolve!(ucol_close),
        ucol_strcoll: resolve!(ucol_strcoll),
        ucol_strcollUTF8: resolve!(ucol_strcollUTF8),
        ucol_getSortKey: resolve!(ucol_getSortKey),
        ucol_nextSortKeyPart: resolve!(ucol_nextSortKeyPart),
        ucol_getVersion: resolve!(ucol_getVersion),
        ucol_getRules: resolve!(ucol_getRules),
        ucol_openRules: resolve!(ucol_openRules),
        u_errorName: resolve!(u_errorName),
        u_versionToString: resolve!(u_versionToString),
        ucnv_open: resolve!(ucnv_open),
        ucnv_toUChars: resolve!(ucnv_toUChars),
        ucnv_fromUChars: resolve!(ucnv_fromUChars),
        u_strToLower: resolve!(u_strToLower),
        u_strToUpper: resolve!(u_strToUpper),
        u_strToTitle: resolve!(u_strToTitle),
        u_strFoldCase: resolve!(u_strFoldCase),
        uloc_getLanguage: resolve!(uloc_getLanguage),
        uloc_toLanguageTag: resolve!(uloc_toLanguageTag),
        uloc_countAvailable: resolve!(uloc_countAvailable),
        uloc_getAvailable: resolve!(uloc_getAvailable),
        uiter_setUTF8: resolve!(uiter_setUTF8),
        uiter_setString: resolve!(uiter_setString),
    })
}
