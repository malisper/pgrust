//! Locale setup, GUC hooks, and `setlocale` wrappers (`pg_locale.c`).
//!
//! Holds the per-backend flags the GUC assign hooks and the
//! `cache_locale_time`/`PGLC_localeconv` machinery toggle (`database_ctype_is_c`,
//! `CurrentLocaleConvValid`, `CurrentLCTimeValid`), the `lc_*` GUC string values,
//! and `pg_perm_setlocale`/`check_locale` + the `check_locale_*`/`assign_locale_*`
//! hooks. The libc `setlocale`/`setenv` calls are OS FFI bound here; everything
//! PostgreSQL decides is ported in-crate.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;

use alloc::format;
use alloc::string::String;
use core::cell::Cell;
use core::ffi::c_char;

use backend_utils_error::{elog, ereport};
use mcx::{Mcx, PgString};
use types_error::{
    ErrorLocation, PgResult, DEBUG3, ERRCODE_INVALID_PARAMETER_VALUE, FATAL, WARNING,
};

use backend_utils_adt_pg_locale_env_seams as env;
use backend_utils_adt_pg_locale_seams::LcCategory;
use backend_utils_mb_mbutils_seams as mb;
use common_string::pg_is_ascii;

thread_local! {
    /// `bool database_ctype_is_c` — whether the database's LC_CTYPE is C/POSIX.
    static DATABASE_CTYPE_IS_C: Cell<bool> = const { Cell::new(false) };
    /// `static bool CurrentLocaleConvValid`.
    static CURRENT_LOCALE_CONV_VALID: Cell<bool> = const { Cell::new(false) };
    /// `static bool CurrentLCTimeValid`.
    static CURRENT_LC_TIME_VALID: Cell<bool> = const { Cell::new(false) };
}

// The `lc_*` GUC string values (C: `char *locale_monetary`/`_numeric`/`_time`,
// owned by GUC string-variable plumbing). The plumbing is not ported, so model
// them as per-backend strings initialized to the GUC `boot_val` (`"C"`).
thread_local! {
    static LOCALE_MONETARY: core::cell::RefCell<String> =
        core::cell::RefCell::new(String::from("C"));
    static LOCALE_NUMERIC: core::cell::RefCell<String> =
        core::cell::RefCell::new(String::from("C"));
    static LOCALE_TIME: core::cell::RefCell<String> = core::cell::RefCell::new(String::from("C"));
    /// `char *locale_messages` — the GUC `conf->variable` backing for
    /// `lc_messages` (boot_val ""). Unlike the monetary/numeric/time vars, C's
    /// assign hook only re-applies the OS locale and never reads this back, but
    /// the GUC engine still owns this storage as the canonical slot value.
    static LOCALE_MESSAGES: core::cell::RefCell<String> = core::cell::RefCell::new(String::new());
}

// `int icu_validation_level = WARNING` (pg_locale.c) — the GUC `conf->variable`
// backing for `icu_validation_level`, read directly by `icu_validate_locale`.
thread_local! {
    static ICU_VALIDATION_LEVEL: Cell<i32> = const { Cell::new(types_error::WARNING.0) };
}

/// `database_ctype_is_c` getter.
#[must_use]
pub fn database_ctype_is_c() -> bool {
    DATABASE_CTYPE_IS_C.with(Cell::get)
}

/// Set `database_ctype_is_c` (computed in postinit.c / `init_database_collation`
/// from `datctype`).
pub fn set_database_ctype_is_c(value: bool) {
    DATABASE_CTYPE_IS_C.with(|c| c.set(value));
}

pub(crate) fn current_locale_conv_valid() -> bool {
    CURRENT_LOCALE_CONV_VALID.with(Cell::get)
}
pub(crate) fn set_current_locale_conv_valid() {
    CURRENT_LOCALE_CONV_VALID.with(|c| c.set(true));
}
pub(crate) fn current_lc_time_valid() -> bool {
    CURRENT_LC_TIME_VALID.with(Cell::get)
}
pub(crate) fn set_current_lc_time_valid() {
    CURRENT_LC_TIME_VALID.with(|c| c.set(true));
}

pub(crate) fn locale_monetary() -> String {
    LOCALE_MONETARY.with(|s| s.borrow().clone())
}
pub(crate) fn locale_numeric() -> String {
    LOCALE_NUMERIC.with(|s| s.borrow().clone())
}
pub(crate) fn locale_time() -> String {
    LOCALE_TIME.with(|s| s.borrow().clone())
}
pub(crate) fn locale_messages() -> String {
    LOCALE_MESSAGES.with(|s| s.borrow().clone())
}

// GUC-slot setters mirroring C's `conf->variable` write: the GUC engine stores
// the new value into the owner-held backing before firing the assign hook.
pub(crate) fn set_locale_monetary(value: &str) {
    LOCALE_MONETARY.with(|s| *s.borrow_mut() = String::from(value));
}
pub(crate) fn set_locale_numeric(value: &str) {
    LOCALE_NUMERIC.with(|s| *s.borrow_mut() = String::from(value));
}
pub(crate) fn set_locale_time(value: &str) {
    LOCALE_TIME.with(|s| *s.borrow_mut() = String::from(value));
}
pub(crate) fn set_locale_messages(value: &str) {
    LOCALE_MESSAGES.with(|s| *s.borrow_mut() = String::from(value));
}

pub(crate) fn icu_validation_level() -> i32 {
    ICU_VALIDATION_LEVEL.with(Cell::get)
}
pub(crate) fn set_icu_validation_level(value: i32) {
    ICU_VALIDATION_LEVEL.with(|c| c.set(value));
}

/// Map an [`LcCategory`] to its libc `LC_*` category code.
fn lc_category_code(category: LcCategory) -> libc::c_int {
    match category {
        LcCategory::LcCollate => libc::LC_COLLATE,
        LcCategory::LcCtype => libc::LC_CTYPE,
        LcCategory::LcMessages => libc::LC_MESSAGES,
        LcCategory::LcMonetary => libc::LC_MONETARY,
        LcCategory::LcNumeric => libc::LC_NUMERIC,
        LcCategory::LcTime => libc::LC_TIME,
    }
}

/// Map an [`LcCategory`] to its `LC_*` environment-variable name (C: the
/// `switch (category)` in `pg_perm_setlocale`).
fn lc_category_envvar(category: LcCategory) -> &'static str {
    match category {
        LcCategory::LcCollate => "LC_COLLATE",
        LcCategory::LcCtype => "LC_CTYPE",
        LcCategory::LcMessages => "LC_MESSAGES",
        LcCategory::LcMonetary => "LC_MONETARY",
        LcCategory::LcNumeric => "LC_NUMERIC",
        LcCategory::LcTime => "LC_TIME",
    }
}

/// `setlocale(category, locale)` (libc): `locale == None` is the C NULL query.
/// Returns the (owned) canonical name, or `None` on failure.
fn setlocale(category: libc::c_int, locale: Option<&str>) -> Option<String> {
    let cbuf;
    let ptr = match locale {
        Some(s) => {
            cbuf = cstr(s);
            cbuf.as_ptr() as *const c_char
        }
        None => core::ptr::null(),
    };
    // SAFETY: ptr is either NUL-terminated or null; setlocale returns a static
    // buffer pointer (or null).
    let res = unsafe { libc::setlocale(category, ptr) };
    if res.is_null() {
        return None;
    }
    // SAFETY: res is a NUL-terminated static string from libc.
    let s = unsafe { core::ffi::CStr::from_ptr(res) };
    Some(String::from_utf8_lossy(s.to_bytes()).into_owned())
}

/// `setenv(name, value, 1)` (libc): returns `true` on success.
fn setenv(name: &str, value: &str) -> bool {
    let cname = cstr(name);
    let cval = cstr(value);
    // SAFETY: both args are NUL-terminated.
    let rc = unsafe { libc::setenv(cname.as_ptr() as *const c_char, cval.as_ptr() as *const c_char, 1) };
    rc == 0
}

fn cstr(s: &str) -> alloc::vec::Vec<u8> {
    let mut v = alloc::vec::Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

/// `pg_perm_setlocale(category, locale)` (`pg_locale.c:197`): wrap `setlocale`,
/// updating the message encoding on `LC_CTYPE` and setting the matching `LC_*`
/// env var on success. Returns the canonical name copied into `mcx`, or `None`
/// on failure. The WIN32 `IsoLocaleName` rewrite is compiled out (non-WIN32).
pub fn pg_perm_setlocale<'mcx>(
    mcx: Mcx<'mcx>,
    category: LcCategory,
    locale: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    let code = lc_category_code(category);

    // result = setlocale(category, locale);
    let result = match setlocale(code, Some(locale)) {
        Some(r) => r,
        // Fall out immediately on failure.
        None => return Ok(None),
    };

    // Under !ENABLE_NLS the message encoding equals the database encoding.
    if matches!(category, LcCategory::LcCtype) {
        env::set_message_encoding::call(mb::get_database_encoding::call());
    }

    // setenv(envvar, result, 1) != 0 -> return NULL.
    let envvar = lc_category_envvar(category);
    if !setenv(envvar, &result) {
        return Ok(None);
    }

    Ok(Some(PgString::from_str_in(&result, mcx)?))
}

/// `check_locale(category, locale, &canonname)` (`pg_locale.c:300`): is the
/// locale name valid for the category? Returns `(valid, canonical_name)`.
pub fn check_locale(category: libc::c_int, locale: &str) -> PgResult<(bool, Option<String>)> {
    check_locale_inner(category, locale, true)
}

/// `check_locale` with `canonname == NULL`: validity only.
fn check_locale_validate(category: libc::c_int, locale: &str) -> PgResult<bool> {
    Ok(check_locale_inner(category, locale, false)?.0)
}

fn check_locale_inner(
    category: libc::c_int,
    locale: &str,
    want_canonname: bool,
) -> PgResult<(bool, Option<String>)> {
    // Don't let Windows' non-ASCII locale names in.
    if !pg_is_ascii(locale) {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "locale name \"{locale}\" contains non-ASCII characters"
            ))
            .finish(ErrorLocation::new(
                "../src/backend/utils/adt/pg_locale.c",
                311,
                "check_locale",
            ))?;
        return Ok((false, None));
    }

    let mut canonname: Option<String> = None;

    // save = setlocale(category, NULL);
    let save = match setlocale(category, None) {
        Some(s) => s,
        None => return Ok((false, None)),
    };

    // res = setlocale(category, locale);
    let res = setlocale(category, Some(locale));

    if want_canonname {
        if let Some(ref r) = res {
            canonname = Some(r.clone());
        }
    }

    // Restore old value.
    if setlocale(category, Some(&save)).is_none() {
        let _ = elog(WARNING, format!("failed to restore old locale \"{save}\""));
    }

    // Don't let Windows' non-ASCII locale names out.
    if let Some(ref name) = canonname {
        if !pg_is_ascii(name) {
            ereport(WARNING)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "locale name \"{name}\" contains non-ASCII characters"
                ))
                .finish(ErrorLocation::new(
                    "../src/backend/utils/adt/pg_locale.c",
                    343,
                    "check_locale",
                ))?;
            return Ok((false, None));
        }
    }

    Ok((res.is_some(), canonname))
}

// GUC check/assign hooks. The assign hooks just reset the cache-valid flags so
// the next use re-caches; value = "" selects the postmaster's environment value.

/// `check_locale_monetary`.
pub fn check_locale_monetary(newval: &str) -> PgResult<bool> {
    check_locale_validate(libc::LC_MONETARY, newval)
}

/// `assign_locale_monetary`.
pub fn assign_locale_monetary(newval: &str) {
    LOCALE_MONETARY.with(|s| *s.borrow_mut() = String::from(newval));
    CURRENT_LOCALE_CONV_VALID.with(|c| c.set(false));
}

/// `check_locale_numeric`.
pub fn check_locale_numeric(newval: &str) -> PgResult<bool> {
    check_locale_validate(libc::LC_NUMERIC, newval)
}

/// `assign_locale_numeric`.
pub fn assign_locale_numeric(newval: &str) {
    LOCALE_NUMERIC.with(|s| *s.borrow_mut() = String::from(newval));
    CURRENT_LOCALE_CONV_VALID.with(|c| c.set(false));
}

/// `check_locale_time`.
pub fn check_locale_time(newval: &str) -> PgResult<bool> {
    check_locale_validate(libc::LC_TIME, newval)
}

/// `assign_locale_time`.
pub fn assign_locale_time(newval: &str) {
    LOCALE_TIME.with(|s| *s.borrow_mut() = String::from(newval));
    CURRENT_LC_TIME_VALID.with(|c| c.set(false));
}

/// `check_locale_messages` (`pg_locale.c:411`): LC_MESSAGES may be set globally.
/// `""` is accepted only for `PGC_S_DEFAULT`; on Windows accept blindly.
/// `is_default_source` models `source == PGC_S_DEFAULT`.
pub fn check_locale_messages(newval: &str, is_default_source: bool) -> PgResult<bool> {
    if newval.is_empty() {
        return Ok(is_default_source);
    }
    if cfg!(windows) {
        Ok(true)
    } else {
        check_locale_validate(libc::LC_MESSAGES, newval)
    }
}

/// `assign_locale_messages` (`pg_locale.c:434`): set LC_MESSAGES globally,
/// ignoring failure (C's `(void) pg_perm_setlocale(LC_MESSAGES, newval)`).
pub fn assign_locale_messages(newval: &str) {
    let ctx = mcx::MemoryContext::new("assign_locale_messages");
    let _ = pg_perm_setlocale(ctx.mcx(), LcCategory::LcMessages, newval);
}

/// FATAL "unrecognized LC category" — not reachable through the typed
/// [`LcCategory`] enum (every variant is recognized), kept to mirror C's
/// default arm.
#[allow(dead_code)]
fn unrecognized_lc_category(category: i32) -> PgResult<()> {
    ereport(FATAL)
        .errmsg_internal(format!("unrecognized LC category: {category}"))
        .finish(ErrorLocation::new(
            "../src/backend/utils/adt/pg_locale.c",
            279,
            "pg_perm_setlocale",
        ))?;
    let _ = DEBUG3;
    Ok(())
}
