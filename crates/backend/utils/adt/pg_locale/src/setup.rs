use core::cell::{Cell, RefCell};
use core::ffi::{c_char, c_int, CStr};

use mcx::{Mcx, PgString};
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, FATAL, WARNING};
use types_guc::GucSource;

use crate::loc;

thread_local! {
    static DATABASE_CTYPE_IS_C: Cell<bool> = const { Cell::new(false) };
    // GUC conf->variable backings (locale_monetary/_numeric/_time boot "C",
    // locale_messages boot "", icu_validation_level boot WARNING).
    static LOCALE_MONETARY: RefCell<String> = RefCell::new(String::from("C"));
    static LOCALE_NUMERIC: RefCell<String> = RefCell::new(String::from("C"));
    static LOCALE_TIME: RefCell<String> = RefCell::new(String::from("C"));
    static LOCALE_MESSAGES: RefCell<String> = const { RefCell::new(String::new()) };
    static ICU_VALIDATION_LEVEL: Cell<i32> = const { Cell::new(WARNING.0) };
}

pub(crate) fn monetary_and_numeric_are_c() -> bool {
    fn is_c(s: &str) -> bool {
        s == "C" || s == "POSIX"
    }
    LOCALE_MONETARY.with(|s| is_c(&s.borrow())) && LOCALE_NUMERIC.with(|s| is_c(&s.borrow()))
}

#[must_use]
pub fn database_ctype_is_c() -> bool {
    DATABASE_CTYPE_IS_C.with(Cell::get)
}

pub fn set_database_ctype_is_c(value: bool) {
    DATABASE_CTYPE_IS_C.with(|c| c.set(value));
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

fn setlocale(category: c_int, locale: Option<&str>) -> Option<String> {
    let cbuf;
    let ptr = match locale {
        Some(s) => {
            cbuf = cstr(s);
            cbuf.as_ptr() as *const c_char
        }
        None => core::ptr::null(),
    };
    // SAFETY: ptr is NUL-terminated or null; a non-null result is a static
    // NUL-terminated buffer.
    let res = unsafe { libc::setlocale(category, ptr) };
    if res.is_null() {
        return None;
    }
    // SAFETY: res is a NUL-terminated static string.
    let s = unsafe { CStr::from_ptr(res) };
    Some(String::from_utf8_lossy(s.to_bytes()).into_owned())
}

fn setenv(name: &str, value: &str) -> bool {
    let cname = cstr(name);
    let cval = cstr(value);
    // SAFETY: both arguments are NUL-terminated.
    unsafe { libc::setenv(cname.as_ptr() as *const c_char, cval.as_ptr() as *const c_char, 1) == 0 }
}

pub fn pg_perm_setlocale<'mcx>(
    mcx: Mcx<'mcx>,
    category: i32,
    locale: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    let Some(result) = setlocale(category, Some(locale)) else {
        return Ok(None);
    };

    if category == libc::LC_CTYPE {
        // !ENABLE_NLS: message encoding equals the database encoding.
        mbutils::SetMessageEncoding(mbutils::GetDatabaseEncoding());
    }

    let envvar = match category {
        libc::LC_COLLATE => "LC_COLLATE",
        libc::LC_CTYPE => "LC_CTYPE",
        libc::LC_MESSAGES => "LC_MESSAGES",
        libc::LC_MONETARY => "LC_MONETARY",
        libc::LC_NUMERIC => "LC_NUMERIC",
        libc::LC_TIME => "LC_TIME",
        _ => {
            elog::ereport(FATAL)
                .errmsg_internal(format!("unrecognized LC category: {category}"))
                .finish(loc(279, "pg_perm_setlocale"))?;
            return Ok(None);
        }
    };

    if !setenv(envvar, &result) {
        return Ok(None);
    }

    Ok(Some(PgString::from_str_in(&result, mcx)?))
}

pub fn check_locale(category: c_int, locale: &str) -> PgResult<(bool, Option<String>)> {
    check_locale_inner(category, locale, true)
}

fn check_locale_validate(category: c_int, locale: &str) -> PgResult<bool> {
    Ok(check_locale_inner(category, locale, false)?.0)
}

fn check_locale_inner(
    category: c_int,
    locale: &str,
    want_canonname: bool,
) -> PgResult<(bool, Option<String>)> {
    if !locale.is_ascii() {
        elog::ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "locale name \"{locale}\" contains non-ASCII characters"
            ))
            .finish(loc(311, "check_locale"))?;
        return Ok((false, None));
    }

    let Some(save) = setlocale(category, None) else {
        return Ok((false, None));
    };

    let res = setlocale(category, Some(locale));
    let canonname = if want_canonname { res.clone() } else { None };

    if setlocale(category, Some(&save)).is_none() {
        elog::ereport(WARNING)
            .errmsg_internal(format!("failed to restore old locale \"{save}\""))
            .finish(loc(330, "check_locale"))?;
    }

    if let Some(name) = canonname.as_deref() {
        if !name.is_ascii() {
            elog::ereport(WARNING)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "locale name \"{name}\" contains non-ASCII characters"
                ))
                .finish(loc(343, "check_locale"))?;
            return Ok((false, None));
        }
    }

    Ok((res.is_some(), canonname))
}

pub fn check_locale_monetary(newval: &str) -> PgResult<bool> {
    check_locale_validate(libc::LC_MONETARY, newval)
}

// Assign hooks reset C's CurrentLocaleConvValid/CurrentLCTimeValid; the flags
// land with their consumers (PGLC_localeconv / cache_locale_time, deferred).
pub fn assign_locale_monetary(newval: &str) {
    LOCALE_MONETARY.with(|s| *s.borrow_mut() = newval.to_owned());
}

pub fn check_locale_numeric(newval: &str) -> PgResult<bool> {
    check_locale_validate(libc::LC_NUMERIC, newval)
}

pub fn assign_locale_numeric(newval: &str) {
    LOCALE_NUMERIC.with(|s| *s.borrow_mut() = newval.to_owned());
}

pub fn check_locale_time(newval: &str) -> PgResult<bool> {
    check_locale_validate(libc::LC_TIME, newval)
}

pub fn assign_locale_time(newval: &str) {
    LOCALE_TIME.with(|s| *s.borrow_mut() = newval.to_owned());
}

// C: "" is accepted only when source == PGC_S_DEFAULT (can't verify the
// environment default before the GUC machinery is up).
pub fn check_locale_messages(newval: &str, is_default_source: bool) -> PgResult<bool> {
    if newval.is_empty() {
        return Ok(is_default_source);
    }
    check_locale_validate(libc::LC_MESSAGES, newval)
}

pub fn assign_locale_messages(newval: &str) {
    LOCALE_MESSAGES.with(|s| *s.borrow_mut() = newval.to_owned());
    // C: (void) pg_perm_setlocale(LC_MESSAGES, newval) — failure ignored.
    let ctx = mcx::MemoryContext::new("assign_locale_messages");
    let _ = pg_perm_setlocale(ctx.mcx(), libc::LC_MESSAGES, newval);
}

pub(crate) fn install_guc_hooks() {
    use guc_tables::{hooks, vars, GucVarAccessors};

    hooks::check_locale_monetary.install(|newval, _extra, _source| {
        check_locale_monetary(newval.as_deref().unwrap_or(""))
    });
    hooks::assign_locale_monetary.install(|newval, _extra| {
        assign_locale_monetary(newval.unwrap_or(""));
    });
    hooks::check_locale_numeric.install(|newval, _extra, _source| {
        check_locale_numeric(newval.as_deref().unwrap_or(""))
    });
    hooks::assign_locale_numeric.install(|newval, _extra| {
        assign_locale_numeric(newval.unwrap_or(""));
    });
    hooks::check_locale_time.install(|newval, _extra, _source| {
        check_locale_time(newval.as_deref().unwrap_or(""))
    });
    hooks::assign_locale_time.install(|newval, _extra| {
        assign_locale_time(newval.unwrap_or(""));
    });
    hooks::check_locale_messages.install(|newval, _extra, source| {
        check_locale_messages(
            newval.as_deref().unwrap_or(""),
            source == GucSource::PGC_S_DEFAULT,
        )
    });
    hooks::assign_locale_messages.install(|newval, _extra| {
        assign_locale_messages(newval.unwrap_or(""));
    });

    vars::locale_monetary.install(GucVarAccessors {
        get: || Some(LOCALE_MONETARY.with(|s| s.borrow().clone())),
        set: |v| LOCALE_MONETARY.with(|s| *s.borrow_mut() = v.unwrap_or_default()),
    });
    vars::locale_numeric.install(GucVarAccessors {
        get: || Some(LOCALE_NUMERIC.with(|s| s.borrow().clone())),
        set: |v| LOCALE_NUMERIC.with(|s| *s.borrow_mut() = v.unwrap_or_default()),
    });
    vars::locale_time.install(GucVarAccessors {
        get: || Some(LOCALE_TIME.with(|s| s.borrow().clone())),
        set: |v| LOCALE_TIME.with(|s| *s.borrow_mut() = v.unwrap_or_default()),
    });
    vars::locale_messages.install(GucVarAccessors {
        get: || Some(LOCALE_MESSAGES.with(|s| s.borrow().clone())),
        set: |v| LOCALE_MESSAGES.with(|s| *s.borrow_mut() = v.unwrap_or_default()),
    });
    vars::icu_validation_level.install(GucVarAccessors {
        get: || ICU_VALIDATION_LEVEL.with(Cell::get),
        set: |v| ICU_VALIDATION_LEVEL.with(|c| c.set(v)),
    });
}
