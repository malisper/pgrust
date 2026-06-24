//! Raw ICU C API bindings used by the collation provider.
//!
//! These bind the SAME system ICU PostgreSQL's `--with-icu` binds — never a
//! pure-Rust reimplementation. ICU renames its exported symbols with a major
//! version suffix (`ucol_open_78`); `build.rs` discovers the major number and
//! exposes it as `PG_ICU_VERSION_MAJOR`, so the `#[link_name]` literals below
//! are built with `concat!`.
//!
//! Only the subset `pg_locale_icu.c` / `pg_locale.c` actually call is bound:
//! the collator open/close/compare/sort-key/version path and the `uloc_*`
//! locale helpers (validation, language-tag canonicalization, enumeration,
//! display name).

#![allow(non_camel_case_types)]

extern crate alloc;

use core::ffi::c_char;

/// `UChar` (`umachine.h`) — ICU's 16-bit code unit.
pub type UChar = u16;
/// `UErrorCode` (`utypes.h`) — `U_ZERO_ERROR == 0`; `> 0` is failure, `< 0` is a
/// warning. `U_STRING_NOT_TERMINATED_WARNING == -124`,
/// `U_BUFFER_OVERFLOW_ERROR == 15`.
pub type UErrorCode = i32;
/// `UBool` (`umachine.h`) — `int8_t` in modern ICU.
pub type UBool = i8;
/// `UCollationResult` (`ucol.h`) — `UCOL_LESS = -1`, `UCOL_EQUAL = 0`,
/// `UCOL_GREATER = 1`.
pub type UCollationResult = i32;
/// Opaque `UCollator`.
pub enum UCollator {}

/// `U_ZERO_ERROR`.
pub const U_ZERO_ERROR: UErrorCode = 0;
/// `U_BUFFER_OVERFLOW_ERROR`.
pub const U_BUFFER_OVERFLOW_ERROR: UErrorCode = 15;
/// `U_STRING_NOT_TERMINATED_WARNING`.
pub const U_STRING_NOT_TERMINATED_WARNING: UErrorCode = -124;
/// `U_MAX_VERSION_LENGTH` (`uversion.h`).
pub const U_MAX_VERSION_LENGTH: usize = 4;
/// `U_MAX_VERSION_STRING_LENGTH` (`uversion.h`).
pub const U_MAX_VERSION_STRING_LENGTH: usize = 20;
/// `ULOC_LANG_CAPACITY` (`uloc.h`).
pub const ULOC_LANG_CAPACITY: usize = 12;

/// `U_FAILURE(code)` — `code > U_ZERO_ERROR`.
#[inline]
#[must_use]
pub fn u_failure(code: UErrorCode) -> bool {
    code > U_ZERO_ERROR
}

/// `UVersionInfo` (`uversion.h`) — `uint8_t[U_MAX_VERSION_LENGTH]`.
pub type UVersionInfo = [u8; U_MAX_VERSION_LENGTH];

// The ICU C entry points, with the version-suffixed link names ICU exports.
extern "C" {
    #[link_name = concat!("ucol_open_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_open(loc: *const c_char, status: *mut UErrorCode) -> *mut UCollator;

    #[link_name = concat!("ucol_openRules_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_open_rules(
        rules: *const UChar,
        rules_length: i32,
        normalization_mode: i32,
        strength: i32,
        parse_error: *mut core::ffi::c_void,
        status: *mut UErrorCode,
    ) -> *mut UCollator;

    #[link_name = concat!("ucol_getRules_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_get_rules(coll: *const UCollator, length: *mut i32) -> *const UChar;

    #[link_name = concat!("ucol_close_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_close(coll: *mut UCollator);

    #[link_name = concat!("ucol_strcoll_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_strcoll(
        coll: *const UCollator,
        source: *const UChar,
        source_length: i32,
        target: *const UChar,
        target_length: i32,
    ) -> UCollationResult;

    #[link_name = concat!("ucol_strcollUTF8_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_strcoll_utf8(
        coll: *const UCollator,
        source: *const c_char,
        source_length: i32,
        target: *const c_char,
        target_length: i32,
        status: *mut UErrorCode,
    ) -> UCollationResult;

    #[link_name = concat!("ucol_getSortKey_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_get_sort_key(
        coll: *const UCollator,
        source: *const UChar,
        source_length: i32,
        result: *mut u8,
        result_length: i32,
    ) -> i32;

    #[link_name = concat!("ucol_getVersion_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucol_get_version(coll: *const UCollator, info: *mut u8);

    #[link_name = concat!("u_versionToString_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_version_to_string(version_array: *const u8, version_string: *mut c_char);

    #[link_name = concat!("u_errorName_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_error_name(code: UErrorCode) -> *const c_char;

    #[link_name = concat!("u_strFromUTF8_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_str_from_utf8(
        dest: *mut UChar,
        dest_capacity: i32,
        dest_length: *mut i32,
        src: *const c_char,
        src_length: i32,
        status: *mut UErrorCode,
    ) -> *mut UChar;

    #[link_name = concat!("uloc_getLanguage_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn uloc_get_language(
        locale_id: *const c_char,
        language: *mut c_char,
        language_capacity: i32,
        err: *mut UErrorCode,
    ) -> i32;

    #[link_name = concat!("uloc_toLanguageTag_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn uloc_to_language_tag(
        locale_id: *const c_char,
        langtag: *mut c_char,
        langtag_capacity: i32,
        strict: UBool,
        err: *mut UErrorCode,
    ) -> i32;

    #[link_name = concat!("uloc_countAvailable_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn uloc_count_available() -> i32;

    #[link_name = concat!("uloc_getAvailable_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn uloc_get_available(n: i32) -> *const c_char;

    #[link_name = concat!("uloc_getDisplayName_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn uloc_get_display_name(
        locale_id: *const c_char,
        in_locale_id: *const c_char,
        result: *mut UChar,
        max_result_size: i32,
        err: *mut UErrorCode,
    ) -> i32;

    #[link_name = concat!("u_strToUTF8_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_str_to_utf8(
        dest: *mut c_char,
        dest_capacity: i32,
        dest_length: *mut i32,
        src: *const UChar,
        src_length: i32,
        status: *mut UErrorCode,
    ) -> *mut c_char;

    #[link_name = concat!("u_strToLower_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_str_to_lower(
        dest: *mut UChar,
        dest_capacity: i32,
        src: *const UChar,
        src_length: i32,
        locale: *const c_char,
        p_error_code: *mut UErrorCode,
    ) -> i32;

    #[link_name = concat!("u_strToUpper_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_str_to_upper(
        dest: *mut UChar,
        dest_capacity: i32,
        src: *const UChar,
        src_length: i32,
        locale: *const c_char,
        p_error_code: *mut UErrorCode,
    ) -> i32;

    #[link_name = concat!("u_strToTitle_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_str_to_title(
        dest: *mut UChar,
        dest_capacity: i32,
        src: *const UChar,
        src_length: i32,
        title_iter: *mut core::ffi::c_void,
        locale: *const c_char,
        p_error_code: *mut UErrorCode,
    ) -> i32;

    #[link_name = concat!("u_strFoldCase_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_str_fold_case(
        dest: *mut UChar,
        dest_capacity: i32,
        src: *const UChar,
        src_length: i32,
        options: u32,
        p_error_code: *mut UErrorCode,
    ) -> i32;
}

extern "C" {
    #[link_name = concat!("u_strlen_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn u_strlen(s: *const UChar) -> i32;

    // The database-encoding <-> UChar converter (`ucnv`), used by the non-UTF-8
    // collation/sort-key path (`init_icu_converter` / `uchar_length` /
    // `uchar_convert`).
    #[link_name = concat!("ucnv_open_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucnv_open(converter_name: *const c_char, err: *mut UErrorCode) -> *mut UConverter;

    #[link_name = concat!("ucnv_close_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucnv_close(converter: *mut UConverter);

    #[link_name = concat!("ucnv_toUChars_", env!("PG_ICU_VERSION_MAJOR"))]
    pub fn ucnv_to_uchars(
        cnv: *mut UConverter,
        dest: *mut UChar,
        dest_capacity: i32,
        src: *const c_char,
        src_length: i32,
        p_error_code: *mut UErrorCode,
    ) -> i32;
}

/// Opaque `UConverter`.
pub enum UConverter {}

/// `U_FOLD_CASE_DEFAULT` (`uchar.h`).
pub const U_FOLD_CASE_DEFAULT: u32 = 0;
/// `U_FOLD_CASE_EXCLUDE_SPECIAL_I` (`uchar.h`).
pub const U_FOLD_CASE_EXCLUDE_SPECIAL_I: u32 = 1;
/// `UCOL_DEFAULT` (`ucol.h`).
pub const UCOL_DEFAULT: i32 = -1;
/// `UCOL_DEFAULT_STRENGTH` = `UCOL_TERTIARY` (`ucol.h`).
pub const UCOL_DEFAULT_STRENGTH: i32 = UCOL_DEFAULT;

/// `u_errorName(code)` as an owned Rust string (for error messages).
pub fn error_name(code: UErrorCode) -> alloc::string::String {
    // SAFETY: u_errorName returns a static NUL-terminated string for any code.
    unsafe {
        let p = u_error_name(code);
        core::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}
