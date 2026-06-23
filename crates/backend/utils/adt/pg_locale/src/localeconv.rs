//! `PGLC_localeconv()` and `cache_locale_time()` (`pg_locale.c:531-858`): the
//! `lconv`/`lc_time` formatting caches consulted by `to_char`/the money type.
//!
//! The repo seam carriers are the monetary/number subset ([`::types_cash::CashLconv`])
//! and the four localized day/month name arrays. The C-locale fast paths
//! (`lc_monetary`/`lc_numeric`/`lc_time` all C/POSIX) are bound here directly;
//! the non-C paths need `pg_localeconv_r` / `strftime_l` + the
//! `pg_get_encoding_from_locale` / `pg_any_to_server` encoding helpers, whose
//! owners are not yet ported. The non-C `lconv` snapshot crosses the
//! (infallible) `pglc_localeconv` seam, so it panics loudly until that owner
//! lands; the localized-name arrays return `None` for the C locale (DCH then
//! uses its built-in English names) and seam-and-panic the encoding step for a
//! non-C `LC_TIME`.

extern crate alloc;

use alloc::format;
use core::cell::RefCell;
use core::ffi::c_char;

use ::mcx::{Mcx, PgVec};
use ::types_cash::CashLconv;
use ::types_error::{PgError, PgResult};

use pg_locale_env_seams as env;

use crate::setup::{
    current_lc_time_valid, current_locale_conv_valid, locale_monetary, locale_numeric,
    locale_time, set_current_lc_time_valid, set_current_locale_conv_valid,
};

/// `MAX_L10N_DATA` (`pg_locale.c:66`).
pub const MAX_L10N_DATA: usize = 80;

thread_local! {
    /// The cached `CurrentLocaleConv` monetary/number subset.
    static CURRENT_LOCALE_CONV: RefCell<CashLconv> = RefCell::new(CashLconv::c_locale());

    /// `localized_abbrev_days[7]` / `localized_full_days[7]` /
    /// `localized_abbrev_months[12]` / `localized_full_months[12]`, each name as
    /// database-encoded NUL-free bytes. Empty (= C locale) by default.
    static LOCALIZED_ABBREV_DAYS: RefCell<alloc::vec::Vec<alloc::vec::Vec<u8>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
    static LOCALIZED_FULL_DAYS: RefCell<alloc::vec::Vec<alloc::vec::Vec<u8>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
    static LOCALIZED_ABBREV_MONTHS: RefCell<alloc::vec::Vec<alloc::vec::Vec<u8>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
    static LOCALIZED_FULL_MONTHS: RefCell<alloc::vec::Vec<alloc::vec::Vec<u8>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

/// Whether `locale` is the C/POSIX locale (empty also means the postmaster
/// environment, which during a C-locale boot is C).
fn is_c_locale(locale: &str) -> bool {
    locale == "C" || locale == "POSIX" || locale.is_empty()
}

/// `PGLC_localeconv()` (`pg_locale.c:531`): the POSIX `lconv` for the current
/// `lc_monetary`/`lc_numeric`. The repo seam returns the [`CashLconv`] subset and
/// is infallible. The C-locale fast path returns `CashLconv::c_locale()`; the
/// non-C path needs `pg_localeconv_r` + encoding conversion (unported owners), so
/// it panics loudly there (seam-and-panic).
pub fn pglc_localeconv() -> CashLconv {
    // Did we do it already?
    if current_locale_conv_valid() {
        return CURRENT_LOCALE_CONV.with(|c| c.borrow().clone());
    }

    let monetary = locale_monetary();
    let numeric = locale_numeric();

    if is_c_locale(&monetary) && is_c_locale(&numeric) {
        // libc localeconv() under the C locale is the canonical empty/CHAR_MAX
        // lconv; no conversion needed.
        let conv = CashLconv::c_locale();
        CURRENT_LOCALE_CONV.with(|c| *c.borrow_mut() = conv.clone());
        set_current_locale_conv_valid();
        return conv;
    }

    // The non-C lconv snapshot uses pg_localeconv_r() + db_encoding_convert(),
    // whose owners (port-batch pg_localeconv_r.c, mbutils/chklocale) are not yet
    // ported; mirror the C and fail loudly rather than fabricate a snapshot.
    panic!(
        "PGLC_localeconv for non-C LC_MONETARY=\"{monetary}\" / LC_NUMERIC=\"{numeric}\" \
         requires pg_localeconv_r (unported)"
    );
}

/// `cache_locale_time()` (`pg_locale.c:727`): refresh the localized day/month
/// name caches from `LC_TIME` if stale.
///
/// The C-locale fast path leaves the name arrays empty (the seam getters return
/// `None`, and DCH falls back to its built-in English names); the non-C path
/// drives `strftime_l` + `pg_get_encoding_from_locale`/`pg_any_to_server`
/// (unported encoding owners), seam-and-panicking there.
pub fn cache_locale_time() -> PgResult<()> {
    // Did we do this already?
    if current_lc_time_valid() {
        return Ok(());
    }

    let lc_time = locale_time();

    if is_c_locale(&lc_time) {
        // Under the C locale the seam getters report None (DCH uses its built-in
        // English names), so the caches stay empty.
        clear_localized();
        set_current_lc_time_valid();
        return Ok(());
    }

    // The non-C path runs strftime_l() into MAX_L10N_DATA buffers, then
    // pg_get_encoding_from_locale()/pg_any_to_server() to re-encode into the
    // database encoding. Those encoding owners are not ported.
    let encoding = env::pg_get_encoding_from_locale::call(&lc_time)?;
    let _ = encoding;
    Err(PgError::error(format!(
        "cache_locale_time for non-C LC_TIME=\"{lc_time}\" requires strftime_l + \
         pg_any_to_server (unported)"
    )))
}

fn clear_localized() {
    LOCALIZED_ABBREV_DAYS.with(|v| v.borrow_mut().clear());
    LOCALIZED_FULL_DAYS.with(|v| v.borrow_mut().clear());
    LOCALIZED_ABBREV_MONTHS.with(|v| v.borrow_mut().clear());
    LOCALIZED_FULL_MONTHS.with(|v| v.borrow_mut().clear());
}

/// Copy a cached name array into `mcx`, or `None` when empty (the C locale).
fn copy_names<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[alloc::vec::Vec<u8>],
) -> PgResult<Option<PgVec<'mcx, PgVec<'mcx, u8>>>> {
    if names.is_empty() {
        return Ok(None);
    }
    let mut out = ::mcx::vec_with_capacity_in::<PgVec<'mcx, u8>>(mcx, names.len())?;
    for n in names {
        out.push(::mcx::slice_in(mcx, n)?);
    }
    Ok(Some(out))
}

/// `localized_full_months[]`.
pub fn localized_full_months<'mcx>(mcx: Mcx<'mcx>) -> Option<PgVec<'mcx, PgVec<'mcx, u8>>> {
    LOCALIZED_FULL_MONTHS
        .with(|v| copy_names(mcx, &v.borrow()))
        .ok()
        .flatten()
}

/// `localized_abbrev_months[]`.
pub fn localized_abbrev_months<'mcx>(mcx: Mcx<'mcx>) -> Option<PgVec<'mcx, PgVec<'mcx, u8>>> {
    LOCALIZED_ABBREV_MONTHS
        .with(|v| copy_names(mcx, &v.borrow()))
        .ok()
        .flatten()
}

/// `localized_full_days[]`.
pub fn localized_full_days<'mcx>(mcx: Mcx<'mcx>) -> Option<PgVec<'mcx, PgVec<'mcx, u8>>> {
    LOCALIZED_FULL_DAYS
        .with(|v| copy_names(mcx, &v.borrow()))
        .ok()
        .flatten()
}

/// `localized_abbrev_days[]`.
pub fn localized_abbrev_days<'mcx>(mcx: Mcx<'mcx>) -> Option<PgVec<'mcx, PgVec<'mcx, u8>>> {
    LOCALIZED_ABBREV_DAYS
        .with(|v| copy_names(mcx, &v.borrow()))
        .ok()
        .flatten()
}

/// `strftime_l` OS binding (unused on the C-locale fast path; retained for the
/// non-C path the encoding owners gate). Kept to document the OS-FFI surface
/// `cache_locale_time` needs.
#[allow(dead_code)]
unsafe fn strftime_l(
    dst: *mut c_char,
    dstlen: usize,
    format: *const c_char,
    tm: *const libc::tm,
    locale: libc::locale_t,
) -> usize {
    libc::strftime_l(dst, dstlen, format, tm, locale)
}
