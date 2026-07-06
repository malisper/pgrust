//! cache_locale_time (pg_locale.c:728): localized day/month names for the
//! to_char/to_date TM prefix, rendered by strftime_l under lc_time and
//! converted from that locale's encoding to the database encoding.

use core::cell::{Cell, RefCell};
use std::rc::Rc;

use libc::{c_char, locale_t, size_t};
use mcx::Mcx;
use types_error::{PgError, PgResult};
use wchar::PG_SQL_ASCII;

// MAX_L10N_DATA (pg_locale.h): sufficient for every known locale.
const MAX_L10N_DATA: usize = 80;

extern "C" {
    fn strftime_l(
        s: *mut c_char,
        max: size_t,
        format: *const c_char,
        tm: *const libc::tm,
        loc: locale_t,
    ) -> size_t;
}

/// localized_abbrev_days/full_days/abbrev_months/full_months, in the database
/// encoding (not necessarily UTF-8, hence bytes).
pub struct LocalizedTimeNames {
    pub abbrev_days: [Vec<u8>; 7],
    pub full_days: [Vec<u8>; 7],
    pub abbrev_months: [Vec<u8>; 12],
    pub full_months: [Vec<u8>; 12],
}

thread_local! {
    static CURRENT_LC_TIME_VALID: Cell<bool> = const { Cell::new(false) };
    static LC_TIME_CACHE: RefCell<Option<Rc<LocalizedTimeNames>>> = const { RefCell::new(None) };
}

pub(crate) fn invalidate_lc_time() {
    CURRENT_LC_TIME_VALID.with(|v| v.set(false));
}

fn strftime_one(fmt: &[u8], tm: &libc::tm, loc: locale_t) -> Option<Vec<u8>> {
    debug_assert_eq!(*fmt.last().unwrap(), 0);
    let mut buf = [0u8; MAX_L10N_DATA];
    // SAFETY: fmt is NUL-terminated; at most MAX_L10N_DATA bytes written.
    let n = unsafe {
        strftime_l(
            buf.as_mut_ptr() as *mut c_char,
            MAX_L10N_DATA,
            fmt.as_ptr() as *const c_char,
            tm,
            loc,
        )
    };
    if n == 0 {
        return None;
    }
    Some(buf[..n].to_vec())
}

fn to_server(mcx: Mcx<'_>, src: Vec<u8>, encoding: i32) -> PgResult<Vec<u8>> {
    // cache_single_string: convert to the database encoding, or validate.
    match mbutils::pg_any_to_server(mcx, &src, encoding)? {
        Some(converted) => Ok(converted.to_vec()),
        None => Ok(src),
    }
}

pub fn cache_locale_time(mcx: Mcx<'_>) -> PgResult<Rc<LocalizedTimeNames>> {
    if CURRENT_LC_TIME_VALID.with(|v| v.get()) {
        return Ok(LC_TIME_CACHE.with(|c| c.borrow().clone()).expect("valid implies cached"));
    }

    let locale_time = crate::setup::locale_time_value();
    let loc = crate::libc_locale::newlocale_all(&locale_time)?;

    // Times close to current time as data for strftime().
    // SAFETY: time/gmtime_r on stack storage.
    let mut timeinfo: libc::tm = unsafe { core::mem::zeroed() };
    unsafe {
        let timenow = libc::time(core::ptr::null_mut());
        libc::gmtime_r(&timenow, &mut timeinfo);
    }

    let mut strftimefail = false;
    let mut render = |fmt: &[u8], tm: &libc::tm| -> Vec<u8> {
        strftime_one(fmt, tm, loc).unwrap_or_else(|| {
            strftimefail = true;
            Vec::new()
        })
    };

    let mut raw_abbrev_days: Vec<Vec<u8>> = Vec::with_capacity(7);
    let mut raw_full_days: Vec<Vec<u8>> = Vec::with_capacity(7);
    for i in 0..7 {
        timeinfo.tm_wday = i;
        raw_abbrev_days.push(render(b"%a\0", &timeinfo));
        raw_full_days.push(render(b"%A\0", &timeinfo));
    }

    let mut raw_abbrev_months: Vec<Vec<u8>> = Vec::with_capacity(12);
    let mut raw_full_months: Vec<Vec<u8>> = Vec::with_capacity(12);
    for i in 0..12 {
        timeinfo.tm_mon = i;
        timeinfo.tm_mday = 1; // make sure we don't have invalid date
        raw_abbrev_months.push(render(b"%b\0", &timeinfo));
        raw_full_months.push(render(b"%B\0", &timeinfo));
    }

    // SAFETY: loc came from newlocale and is not used past this point.
    unsafe { libc::freelocale(loc) };

    if strftimefail {
        return Err(PgError::error("strftime_l() failed").into());
    }

    // As in PGLC_localeconv, convert strftime output from the LC_TIME
    // encoding to the database encoding.
    let mut encoding = crate::chklocale::pg_get_encoding_from_locale(Some(&locale_time), true)?;
    if encoding < 0 {
        encoding = PG_SQL_ASCII as i32;
    }

    let conv = |v: Vec<Vec<u8>>| -> PgResult<Vec<Vec<u8>>> {
        v.into_iter().map(|s| to_server(mcx, s, encoding)).collect()
    };
    let names = Rc::new(LocalizedTimeNames {
        abbrev_days: conv(raw_abbrev_days)?.try_into().unwrap(),
        full_days: conv(raw_full_days)?.try_into().unwrap(),
        abbrev_months: conv(raw_abbrev_months)?.try_into().unwrap(),
        full_months: conv(raw_full_months)?.try_into().unwrap(),
    });

    LC_TIME_CACHE.with(|c| *c.borrow_mut() = Some(Rc::clone(&names)));
    CURRENT_LC_TIME_VALID.with(|v| v.set(true));
    Ok(names)
}
