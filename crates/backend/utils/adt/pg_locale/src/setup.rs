use core::cell::{Cell, RefCell};
use core::ffi::{c_char, c_int, CStr};
use core::sync::atomic::{AtomicBool, Ordering};

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
    // This thread's permanent per-category locale values (pg_perm_setlocale
    // records here once the global locale is frozen). Mirrors C's per-PROCESS
    // global locale: C backends are processes, ours are threads, so the
    // per-backend state must be per-thread.
    static THREAD_LOCALES: RefCell<Vec<(c_int, String)>> = const { RefCell::new(Vec::new()) };
}

// Once true, the process-global libc locale is final and must never be
// modified again. setlocale() mutates process-global storage and is NOT
// thread-safe: a transition frees the previous locale data while other
// threads may be reading (or racing their own transitions) inside libc —
// on macOS this is a malloc-detected heap corruption and abort() (the
// diesel-parallel-suite SIGABRT: several backends concurrently ran the
// SET lc_messages check hook's setlocale save/set/restore dance).
// The postmaster flips this right before spawning its first child thread;
// after that, validation goes through newlocale() (private locale objects,
// thread-safe by design) and permanent assignments are per-thread records.
static GLOBAL_LOCALE_FROZEN: AtomicBool = AtomicBool::new(false);

/// Declare the process-global libc locale final. Called by the postmaster at
/// the last single-threaded instant, before the first child thread spawns.
pub fn freeze_global_locale() {
    GLOBAL_LOCALE_FROZEN.store(true, Ordering::Release);
}

fn global_locale_frozen() -> bool {
    GLOBAL_LOCALE_FROZEN.load(Ordering::Acquire)
}

fn record_thread_locale(category: c_int, value: &str) {
    THREAD_LOCALES.with(|l| {
        let mut l = l.borrow_mut();
        if let Some(slot) = l.iter_mut().find(|(c, _)| *c == category) {
            slot.1.clear();
            slot.1.push_str(value);
        } else {
            l.push((category, value.to_owned()));
        }
    });
}

/// This thread's recorded permanent locale for `category`, if
/// pg_perm_setlocale ran on this thread after the global freeze.
pub(crate) fn thread_locale(category: c_int) -> Option<String> {
    THREAD_LOCALES.with(|l| {
        l.borrow()
            .iter()
            .find(|(c, _)| *c == category)
            .map(|(_, v)| v.clone())
    })
}

pub(crate) fn monetary_and_numeric_are_c() -> bool {
    fn is_c(s: &str) -> bool {
        s == "C" || s == "POSIX"
    }
    LOCALE_MONETARY.with(|s| is_c(&s.borrow())) && LOCALE_NUMERIC.with(|s| is_c(&s.borrow()))
}

/// The session's (lc_monetary, lc_numeric) names — the two categories
/// PGLC_localeconv reads. Per-thread like every other locale record here,
/// because each backend is a thread and SET lc_monetary is session-scoped.
pub(crate) fn monetary_and_numeric_names() -> (String, String) {
    (
        LOCALE_MONETARY.with(|s| s.borrow().clone()),
        LOCALE_NUMERIC.with(|s| s.borrow().clone()),
    )
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

// Process-global setlocale. Boot-time (single-threaded) use only; the frozen
// paths below never call it.
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

fn category_mask(category: c_int) -> c_int {
    match category {
        crate::lc::LC_COLLATE => crate::lc::LC_COLLATE_MASK,
        crate::lc::LC_CTYPE => crate::lc::LC_CTYPE_MASK,
        crate::lc::LC_MESSAGES => crate::lc::LC_MESSAGES_MASK,
        crate::lc::LC_MONETARY => crate::lc::LC_MONETARY_MASK,
        crate::lc::LC_NUMERIC => crate::lc::LC_NUMERIC_MASK,
        crate::lc::LC_TIME => crate::lc::LC_TIME_MASK,
        _ => crate::lc::LC_ALL_MASK,
    }
}

fn category_envvar(category: c_int) -> Option<&'static str> {
    match category {
        crate::lc::LC_COLLATE => Some("LC_COLLATE"),
        crate::lc::LC_CTYPE => Some("LC_CTYPE"),
        crate::lc::LC_MESSAGES => Some("LC_MESSAGES"),
        crate::lc::LC_MONETARY => Some("LC_MONETARY"),
        crate::lc::LC_NUMERIC => Some("LC_NUMERIC"),
        crate::lc::LC_TIME => Some("LC_TIME"),
        _ => None,
    }
}

// The name setlocale(category, "") would resolve to, per the POSIX rules:
// LC_ALL, then LC_<category>, then LANG, else "C". Used for the canonical
// name of the "" locale in the thread-safe paths (which validate via
// newlocale and so never learn the resolved name from libc).
fn resolve_env_locale_name(category: c_int) -> String {
    let mut names: [&str; 3] = ["LC_ALL", "", "LANG"];
    names[1] = category_envvar(category).unwrap_or("");
    for name in names {
        if name.is_empty() {
            continue;
        }
        if let Ok(v) = std::env::var(name) {
            if !v.is_empty() {
                return v;
            }
        }
    }
    String::from("C")
}

// Thread-safe replacement for the C check_locale/pg_perm_setlocale
// setlocale probe: build a private locale object with newlocale (never
// touches the process-global locale), free it, and return the canonical
// name. newlocale accepts and rejects the same names as setlocale (verified
// on macOS and glibc), and "" resolves from the environment the same way.
// Returns None when the OS does not recognize the locale.
fn validate_locale_threadsafe(category: c_int, locale: &str) -> Option<String> {
    let cbuf = cstr(locale);
    // SAFETY: cbuf is NUL-terminated and outlives the call; a null base is
    // allowed (newlocale builds on the C locale).
    let loc = unsafe {
        libc::newlocale(
            category_mask(category),
            cbuf.as_ptr() as *const c_char,
            core::ptr::null_mut(),
        )
    };
    if loc.is_null() {
        return None;
    }
    // SAFETY: loc came from newlocale and is not used past this point.
    unsafe { libc::freelocale(loc) };
    Some(if locale.is_empty() {
        resolve_env_locale_name(category)
    } else {
        locale.to_owned()
    })
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
    let Some(envvar) = category_envvar(category) else {
        elog::ereport(FATAL)
            .errmsg_internal(format!("unrecognized LC category: {category}"))
            .finish(loc(279, "pg_perm_setlocale"))?;
        return Ok(None);
    };

    if global_locale_frozen() {
        // Threaded operation: never touch the process-global locale or the
        // process environment (both are shared across all backends and racy).
        // Validate with newlocale and record this backend's value per-thread
        // — the same per-backend semantics C gets from its per-process
        // global. With !ENABLE_NLS nothing reads the global LC_MESSAGES, and
        // collation/ctype behavior flows through locale_t providers, not the
        // global locale.
        let Some(result) = validate_locale_threadsafe(category, locale) else {
            return Ok(None);
        };
        record_thread_locale(category, &result);

        if category == crate::lc::LC_CTYPE {
            // !ENABLE_NLS: message encoding equals the database encoding.
            mbutils::SetMessageEncoding(mbutils::GetDatabaseEncoding());
        }

        return Ok(Some(PgString::from_str_in(&result, mcx)?));
    }

    let Some(result) = setlocale(category, Some(locale)) else {
        return Ok(None);
    };

    if category == crate::lc::LC_CTYPE {
        // !ENABLE_NLS: message encoding equals the database encoding.
        mbutils::SetMessageEncoding(mbutils::GetDatabaseEncoding());
    }

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

    // C probes with a setlocale save/set/restore dance — safe there because
    // every backend is its own process. Our backends are threads, and
    // concurrent setlocale transitions corrupt libc's process-global locale
    // storage (the diesel-parallel SIGABRT), so validate with a private
    // newlocale object instead. Accept/reject behavior and the canonical
    // name match setlocale on macOS and glibc.
    let res = validate_locale_threadsafe(category, locale);
    let canonname = if want_canonname { res.clone() } else { None };

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
    check_locale_validate(crate::lc::LC_MONETARY, newval)
}

// Assign hooks reset C's CurrentLocaleConvValid/CurrentLCTimeValid; the flags
// land with their consumers (PGLC_localeconv / cache_locale_time, deferred).
pub fn assign_locale_monetary(newval: &str) {
    LOCALE_MONETARY.with(|s| *s.borrow_mut() = newval.to_owned());
}

pub fn check_locale_numeric(newval: &str) -> PgResult<bool> {
    check_locale_validate(crate::lc::LC_NUMERIC, newval)
}

pub fn assign_locale_numeric(newval: &str) {
    LOCALE_NUMERIC.with(|s| *s.borrow_mut() = newval.to_owned());
}

pub fn check_locale_time(newval: &str) -> PgResult<bool> {
    check_locale_validate(crate::lc::LC_TIME, newval)
}

pub fn assign_locale_time(newval: &str) {
    LOCALE_TIME.with(|s| *s.borrow_mut() = newval.to_owned());
    // C: CurrentLCTimeValid = false.
    crate::locale_time::invalidate_lc_time();
}

pub(crate) fn locale_time_value() -> String {
    LOCALE_TIME.with(|s| s.borrow().clone())
}

// C: "" is accepted only when source == PGC_S_DEFAULT (can't verify the
// environment default before the GUC machinery is up).
pub fn check_locale_messages(newval: &str, is_default_source: bool) -> PgResult<bool> {
    if newval.is_empty() {
        return Ok(is_default_source);
    }
    check_locale_validate(crate::lc::LC_MESSAGES, newval)
}

pub fn assign_locale_messages(newval: &str) {
    LOCALE_MESSAGES.with(|s| *s.borrow_mut() = newval.to_owned());
    // C: (void) pg_perm_setlocale(LC_MESSAGES, newval) — failure ignored.
    let ctx = mcx::MemoryContext::new("assign_locale_messages");
    let _ = pg_perm_setlocale(ctx.mcx(), crate::lc::LC_MESSAGES, newval);
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
        set: |v| {
            LOCALE_TIME.with(|s| *s.borrow_mut() = v.unwrap_or_default());
            crate::locale_time::invalidate_lc_time();
        },
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
