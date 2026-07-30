//! pg_locale.c, C/builtin/libc provider arms: the pg_locale_t framework
//! (c_locale, default_locale, collation cache + MRU), init_database_collation,
//! pg_perm_setlocale/check_locale + the lc_* GUC hooks, collation versions,
//! the builtin-provider validators, the pg_strncoll/pg_strnxfrm collate
//! dispatch, and the pg_strlower/strtitle/strupper/strfold case dispatch
//! (builtin via unicode_case, libc via locale_t). PGLC_localeconv serves the
//! C-locale lconv (non-C lc_monetary/lc_numeric = loud, needs pg_localeconv_r).
//! Deferred loud: ICU, cache_locale_time.

#![allow(clippy::result_large_err)]

use core::cell::{Cell, RefCell};

use mcx::{Mcx, MemoryContext, PgHashMap, PgString};
use types_core::catalog::{C_COLLATION_OID, DEFAULT_COLLATION_OID};
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE, ErrorLocation, WARNING};

mod builtin_case;
mod chklocale;
mod icu;
mod lc;
mod locale_time;
mod icu_ffi;
mod lconv;
mod libc_locale;
mod setup;
#[cfg(test)]
mod tests;

pub use icu::{icu_language_tag, icu_validate_locale, icu_wc_isclass, icu_wc_tolower,
    icu_wc_toupper};
pub use chklocale::pg_get_encoding_from_locale;
pub use lconv::{pglc_localeconv, PgLconv, CHAR_MAX};
pub use locale_time::{cache_locale_time, LocalizedTimeNames};
pub use libc_locale::{pg_tolower, pg_toupper, WcClass};
pub use setup::{
    assign_locale_messages, assign_locale_monetary, assign_locale_numeric, assign_locale_time,
    check_locale, check_locale_messages, check_locale_monetary, check_locale_numeric,
    check_locale_time, database_ctype_is_c, freeze_global_locale, pg_perm_setlocale,
    set_database_ctype_is_c,
};

pub use pg_database_seams::{COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, COLLPROVIDER_LIBC};

const SRC: &str = "src/backend/utils/adt/pg_locale.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

/// pg_locale_struct. C's collate-methods vtable and info union collapse to
/// provider dispatch: builtin/libc live, ICU loud (only libc has non-C
/// collate here, so `lt` doubles as C's info.lt).
#[derive(Clone, Copy, Debug)]
pub struct PgLocale {
    pub provider: u8,
    pub deterministic: bool,
    pub collate_is_c: bool,
    pub ctype_is_c: bool,
    pub is_default: bool,
    pub builtin_locale: Option<&'static str>,
    pub builtin_casemap_full: bool,
    lt: libc_locale::LibcLocale,
    icu: icu::IcuLocale,
}

pub static C_LOCALE: PgLocale = PgLocale {
    provider: COLLPROVIDER_LIBC,
    deterministic: true,
    collate_is_c: true,
    ctype_is_c: true,
    is_default: false,
    builtin_locale: None,
    builtin_casemap_full: false,
    lt: libc_locale::LibcLocale::NONE,
    icu: icu::IcuLocale::NONE,
};

impl PgLocale {
    pub fn pg_strncoll(&self, arg1: &[u8], arg2: &[u8]) -> i32 {
        debug_assert!(!self.collate_is_c, "pg_strncoll on a collate_is_c locale");
        if self.provider == COLLPROVIDER_LIBC {
            libc_locale::strncoll_libc(arg1, arg2, self.lt)
        } else if self.provider == COLLPROVIDER_ICU {
            icu::strncoll(arg1, arg2, self.icu)
        } else {
            panic!(
                "pg_locale: pg_strncoll provider {} has no arm",
                self.provider as char
            );
        }
    }

    // strxfrm_is_safe: false for libc (no TRUST_STRXFRM — glibc strxfrm is
    // inconsistent with strcoll for many locales); true for ICU sort keys.
    pub fn pg_strxfrm_enabled(&self) -> bool {
        self.provider == COLLPROVIDER_ICU
    }

    pub fn pg_strnxfrm(&self, dest: &mut [u8], src: &[u8]) -> usize {
        debug_assert!(!self.collate_is_c, "pg_strnxfrm on a collate_is_c locale");
        if self.provider == COLLPROVIDER_LIBC {
            libc_locale::strnxfrm_libc(dest, src, self.lt)
        } else if self.provider == COLLPROVIDER_ICU {
            icu::strnxfrm(dest, src, self.icu)
        } else {
            panic!(
                "pg_locale: pg_strnxfrm provider {} has no arm",
                self.provider as char
            );
        }
    }

    // strnxfrm_prefix: NULL for libc, live for ICU.
    pub fn pg_strnxfrm_prefix_enabled(&self) -> bool {
        self.provider == COLLPROVIDER_ICU
    }

    pub fn pg_strnxfrm_prefix(&self, dest: &mut [u8], src: &[u8]) -> usize {
        debug_assert!(self.provider == COLLPROVIDER_ICU, "strnxfrm_prefix is ICU-only");
        icu::strnxfrm_prefix(dest, src, self.icu)
    }

    // SB_lower_char's tolower_l arm (like_match.c); SB encodings only.
    pub fn tolower_l(&self, c: u8) -> u8 {
        libc_locale::tolower_l_byte(c, self.lt)
    }

    // regc_pg_locale.c LIBC_WIDE/LIBC_1BYTE arms; the strategy dispatch lives
    // in regex_core::regex_locale. libc provider only.
    pub fn wc_isclass_wide(&self, c: u32, class: WcClass) -> bool {
        libc_locale::wc_isclass_wide(c, class, self.lt)
    }

    pub fn wc_isclass_1byte(&self, c: u32, class: WcClass) -> bool {
        c <= u8::MAX as u32 && libc_locale::wc_isclass_1byte(c as u8, class, self.lt)
    }

    pub fn wc_toupper_wide(&self, c: u32) -> u32 {
        libc_locale::wc_toupper_wide(c, self.lt)
    }

    pub fn wc_tolower_wide(&self, c: u32) -> u32 {
        libc_locale::wc_tolower_wide(c, self.lt)
    }

    pub fn wc_toupper_1byte(&self, c: u32) -> u32 {
        if c <= u8::MAX as u32 {
            libc_locale::wc_toupper_1byte(c as u8, self.lt)
        } else {
            c
        }
    }

    pub fn wc_tolower_1byte(&self, c: u32) -> u32 {
        if c <= u8::MAX as u32 {
            libc_locale::wc_tolower_1byte(c as u8, self.lt)
        } else {
            c
        }
    }

    // pattern_char_isalpha's isalpha_l arm (like_support.c:1849).
    pub fn isalpha_l(&self, c: u8) -> bool {
        libc_locale::wc_isclass_1byte(c, WcClass::Alpha, self.lt)
    }
}

pub fn pg_strlower<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if locale.provider == COLLPROVIDER_BUILTIN {
        Ok(builtin_case::strlower_builtin(dest, src, locale))
    } else if locale.provider == COLLPROVIDER_ICU {
        icu::str_case(icu::CASE_LOWER, dest, src, locale.icu)
    } else if locale.provider == COLLPROVIDER_LIBC {
        libc_locale::strlower_libc(mcx, dest, src, locale)
    } else {
        Err(support_error("pg_strlower", locale.provider))
    }
}

pub fn pg_strtitle<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if locale.provider == COLLPROVIDER_BUILTIN {
        Ok(builtin_case::strtitle_builtin(dest, src, locale))
    } else if locale.provider == COLLPROVIDER_ICU {
        icu::str_case(icu::CASE_TITLE, dest, src, locale.icu)
    } else if locale.provider == COLLPROVIDER_LIBC {
        libc_locale::strtitle_libc(mcx, dest, src, locale)
    } else {
        Err(support_error("pg_strtitle", locale.provider))
    }
}

pub fn pg_strupper<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if locale.provider == COLLPROVIDER_BUILTIN {
        Ok(builtin_case::strupper_builtin(dest, src, locale))
    } else if locale.provider == COLLPROVIDER_ICU {
        icu::str_case(icu::CASE_UPPER, dest, src, locale.icu)
    } else if locale.provider == COLLPROVIDER_LIBC {
        libc_locale::strupper_libc(mcx, dest, src, locale)
    } else {
        Err(support_error("pg_strupper", locale.provider))
    }
}

pub fn pg_strfold<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if locale.provider == COLLPROVIDER_BUILTIN {
        Ok(builtin_case::strfold_builtin(dest, src, locale))
    } else if locale.provider == COLLPROVIDER_ICU {
        icu::str_case(icu::CASE_FOLD, dest, src, locale.icu)
    } else if locale.provider == COLLPROVIDER_LIBC {
        // C: for libc, just use strlower.
        libc_locale::strlower_libc(mcx, dest, src, locale)
    } else {
        Err(support_error("pg_strfold", locale.provider))
    }
}

struct CollationCache {
    mcx: Mcx<'static>,
    map: PgHashMap<'static, Oid, &'static PgLocale>,
}

thread_local! {
    static DEFAULT_LOCALE: Cell<Option<&'static PgLocale>> = const { Cell::new(None) };
    // Database DEFAULT_LOCALE was built for (retention sanity check only).
    static DEFAULT_LOCALE_DB: Cell<Oid> = const { Cell::new(0) };
    static COLLATION_CACHE: RefCell<Option<CollationCache>> = const { RefCell::new(None) };
    static LAST_COLLATION_CACHE: Cell<Option<(Oid, &'static PgLocale)>> = const { Cell::new(None) };
}

// Test-only: regress-shaped C-locale default without a catalog.
pub fn set_default_locale_c_for_tests() {
    DEFAULT_LOCALE.with(|d| d.set(Some(&C_LOCALE)));
}

/// Non-panicking probe: init_database_collation has run (fail-closed gates
/// that would otherwise trip the DEFAULT_COLLATION expect in test/boot envs).
pub fn default_locale_installed() -> bool {
    DEFAULT_LOCALE.with(Cell::get).is_some()
}

#[track_caller]
#[cold]
fn cache_lookup_failed(collid: Oid) -> Box<PgError> {
    PgError::error(format!("cache lookup failed for collation {collid}")).into()
}

#[track_caller]
#[cold]
fn support_error(funcname: &str, provider: u8) -> Box<PgError> {
    PgError::error(format!(
        "unsupported collprovider for {funcname}: {}",
        provider as char
    ))
    .into()
}

pub fn pg_newlocale_from_collation(collid: Oid) -> PgResult<&'static PgLocale> {
    if collid == DEFAULT_COLLATION_OID {
        return Ok(DEFAULT_LOCALE
            .with(Cell::get)
            .expect("pg_locale: default_locale read before init_database_collation"));
    }
    if collid == C_COLLATION_OID {
        return Ok(&C_LOCALE);
    }
    if !OidIsValid(collid) {
        return Err(cache_lookup_failed(collid));
    }

    if let Some((oid, entry)) = LAST_COLLATION_CACHE.with(Cell::get) {
        if oid == collid {
            return Ok(entry);
        }
    }

    let entry = COLLATION_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let cache = slot.get_or_insert_with(|| {
            // C: CollationCacheContext, created lazily, never reset.
            let mcx = ::mcx::session_root("collation cache").mcx();
            // LIFO: empty the droppy TLS cache before its context is freed.
            ::mcx::register_session_cleanup(Box::new(|| {
                COLLATION_CACHE.with(|c| drop(c.borrow_mut().take()));
            }));
            CollationCache {
                mcx,
                map: PgHashMap::with_capacity_in(16, mcx),
            }
        });
        if let Some(entry) = cache.map.get(&collid) {
            return Ok(*entry);
        }
        let built = create_pg_locale(collid, cache.mcx)?;
        let built = mcx::alloc_leak_in(cache.mcx, built)?;
        cache.map.insert(collid, built);
        Ok::<&'static PgLocale, Box<PgError>>(built)
    })?;

    LAST_COLLATION_CACHE.with(|c| c.set(Some((collid, entry))));
    Ok(entry)
}

fn intern_str(cache_mcx: Mcx<'static>, s: &str) -> PgResult<&'static str> {
    let bytes = mcx::slice_borrow_in(cache_mcx, s.as_bytes())?;
    // SAFETY: bytes is a verbatim copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

fn create_pg_locale(collid: Oid, cache_mcx: Mcx<'static>) -> PgResult<PgLocale> {
    let ctx = MemoryContext::new("create_pg_locale");
    let row = syscache_seams::lookup_pg_collation_locale_row::call(ctx.mcx(), collid)?
        .ok_or_else(|| cache_lookup_failed(collid))?;

    let req = |v: &Option<PgString<'_>>, col: &str| -> PgResult<String> {
        v.as_ref().map(|s| s.as_str().to_owned()).ok_or_else(|| {
            PgError::error(format!("unexpected null {col} for collation {collid}")).into()
        })
    };

    let mut result = if row.collprovider == COLLPROVIDER_BUILTIN {
        create_pg_locale_builtin(cache_mcx, &req(&row.colllocale, "colllocale")?)?
    } else if row.collprovider == COLLPROVIDER_ICU {
        create_pg_locale_icu(
            cache_mcx,
            &req(&row.colllocale, "colllocale")?,
            row.collicurules.as_ref().map(|s| s.as_str()),
        )?
    } else if row.collprovider == COLLPROVIDER_LIBC {
        create_pg_locale_libc(
            &req(&row.collcollate, "collcollate")?,
            &req(&row.collctype, "collctype")?,
        )?
    } else {
        return Err(support_error("create_pg_locale", row.collprovider));
    };
    result.is_default = false;
    result.deterministic = row.collisdeterministic;
    // Non-ICU nondeterministic rows are unreachable via SQL (collationcmds.c:309).
    if !row.collisdeterministic && row.collprovider != COLLPROVIDER_ICU {
        panic!(
            "pg_locale: nondeterministic collation {collid} not ported \
             (hashtext/pattern-op arms, varstr equality via pg_strncoll)"
        );
    }

    if let Some(collversionstr) = row.collversion.as_ref() {
        let source = if row.collprovider == COLLPROVIDER_LIBC {
            req(&row.collcollate, "collcollate")?
        } else {
            req(&row.colllocale, "colllocale")?
        };
        let collname = String::from_utf8_lossy(row.collname.name_str()).into_owned();
        match collation_actual_version(row.collprovider, &source)? {
            None => {
                return Err(PgError::error(format!(
                    "collation \"{collname}\" has no actual version, but a version was recorded"
                ))
                .into());
            }
            Some(actual) if actual != collversionstr.as_str() => {
                warn_collation_version_mismatch(
                    &collname,
                    row.collnamespace,
                    collversionstr.as_str(),
                    &actual,
                )?;
            }
            Some(_) => {}
        }
    }

    Ok(result)
}

#[cold]
fn warn_collation_version_mismatch(
    collname: &str,
    collnamespace: Oid,
    collversionstr: &str,
    actual_versionstr: &str,
) -> PgResult<()> {
    let ctx = MemoryContext::new("collation version warning");
    let nspname = lsyscache::get_namespace_name(ctx.mcx(), collnamespace)?;
    let qualified = quote_qualified_identifier(nspname.as_ref().map(|s| s.as_str()), collname);
    elog::ereport(WARNING)
        .errmsg(format!("collation \"{collname}\" has version mismatch"))
        .errdetail(format!(
            "The collation in the database was created using version {collversionstr}, \
             but the operating system provides version {actual_versionstr}."
        ))
        .errhint(format!(
            "Rebuild all objects affected by this collation and run \
             ALTER COLLATION {qualified} REFRESH VERSION, \
             or build PostgreSQL with the right library version."
        ))
        .finish(loc(1134, "create_pg_locale"))
}

// quote_qualified_identifier (ruleutils.c) reduced to quote-when-not-plain,
// postinit precedent; the keyword-aware owner supersedes it.
fn quote_qualified_identifier(namespace: Option<&str>, ident: &str) -> String {
    match namespace {
        Some(n) => format!("{}.{}", quote_identifier(n), quote_identifier(ident)),
        None => quote_identifier(ident),
    }
}

fn quote_identifier(ident: &str) -> String {
    let plain = !ident.is_empty()
        && ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        ident.to_owned()
    } else {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

// create_pg_locale_builtin (pg_locale_builtin.c) create path, inlined here:
// the flag core needs no Unicode tables; the strlower/strupper/strfold
// workers stay with the builtin case-mapping unit.
fn create_pg_locale_builtin(cache_mcx: Mcx<'static>, locstr: &str) -> PgResult<PgLocale> {
    builtin_validate_locale(mbutils::GetDatabaseEncoding(), locstr)?;
    Ok(PgLocale {
        provider: COLLPROVIDER_BUILTIN,
        deterministic: true,
        collate_is_c: true,
        ctype_is_c: locstr == "C",
        is_default: false,
        builtin_locale: Some(intern_str(cache_mcx, locstr)?),
        builtin_casemap_full: locstr == "PG_UNICODE_FAST",
        lt: libc_locale::LibcLocale::NONE,
        icu: icu::IcuLocale::NONE,
    })
}

fn create_pg_locale_icu(
    cache_mcx: Mcx<'static>,
    iculocstr: &str,
    icurules: Option<&str>,
) -> PgResult<PgLocale> {
    Ok(PgLocale {
        provider: COLLPROVIDER_ICU,
        deterministic: true,
        collate_is_c: false,
        ctype_is_c: false,
        is_default: false,
        builtin_locale: None,
        builtin_casemap_full: false,
        lt: libc_locale::LibcLocale::NONE,
        icu: icu::create_icu_locale(cache_mcx, iculocstr, icurules)?,
    })
}

fn create_pg_locale_libc(collate: &str, ctype: &str) -> PgResult<PgLocale> {
    let lt = libc_locale::make_libc_collator(collate, ctype)?;
    Ok(PgLocale {
        provider: COLLPROVIDER_LIBC,
        deterministic: true,
        collate_is_c: collate == "C" || collate == "POSIX",
        ctype_is_c: ctype == "C" || ctype == "POSIX",
        is_default: false,
        builtin_locale: None,
        builtin_casemap_full: false,
        lt,
        icu: icu::IcuLocale::NONE,
    })
}

pub fn init_database_collation() -> PgResult<()> {
    // Retention (wretain): a retained pool thread reruns InitPostgres per
    // claim against the SAME database (dispatch pins it); the default locale
    // it built the first time is that database's, so keep it — C's semantics
    // are once-per-backend-lifetime anyway.
    if DEFAULT_LOCALE.with(Cell::get).is_some() {
        debug_assert_eq!(DEFAULT_LOCALE_DB.get(), init_small::globals::MyDatabaseId());
        return Ok(());
    }

    let dboid = init_small::globals::MyDatabaseId();
    DEFAULT_LOCALE_DB.set(dboid);
    let ctx = MemoryContext::new("init_database_collation");
    let row = pg_database_seams::search_database_syscache::call(ctx.mcx(), dboid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for database {dboid}")))?;

    // C: the default locale lives in TopMemoryContext; interned in the same
    // never-reset cache context here.
    let cache_mcx = default_locale_mcx();
    let mut result = if row.datlocprovider == COLLPROVIDER_BUILTIN {
        let locstr = row.datlocale.as_ref().ok_or_else(|| {
            PgError::error(format!("unexpected null datlocale for database {dboid}"))
        })?;
        create_pg_locale_builtin(cache_mcx, locstr.as_str())?
    } else if row.datlocprovider == COLLPROVIDER_ICU {
        let locstr = row.datlocale.as_ref().ok_or_else(|| {
            PgError::error(format!("unexpected null datlocale for database {dboid}"))
        })?;
        create_pg_locale_icu(
            cache_mcx,
            locstr.as_str(),
            row.daticurules.as_ref().map(|s| s.as_str()),
        )?
    } else if row.datlocprovider == COLLPROVIDER_LIBC {
        create_pg_locale_libc(row.datcollate.as_str(), row.datctype.as_str())?
    } else {
        return Err(support_error("init_database_collation", row.datlocprovider));
    };
    result.is_default = true;

    let entry = mcx::alloc_leak_in(cache_mcx, result)?;
    DEFAULT_LOCALE.with(|d| d.set(Some(entry)));
    Ok(())
}

fn default_locale_mcx() -> Mcx<'static> {
    COLLATION_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let cache = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("collation cache").mcx();
            // LIFO: empty the droppy TLS cache before its context is freed.
            ::mcx::register_session_cleanup(Box::new(|| {
                COLLATION_CACHE.with(|c| drop(c.borrow_mut().take()));
            }));
            CollationCache {
                mcx,
                map: PgHashMap::with_capacity_in(16, mcx),
            }
        });
        cache.mcx
    })
}

pub fn get_collation_actual_version(collprovider: u8, locale: &str) -> PgResult<Option<String>> {
    collation_actual_version(collprovider, locale)
}

fn collation_actual_version(collprovider: u8, locale: &str) -> PgResult<Option<String>> {
    if collprovider == COLLPROVIDER_BUILTIN {
        get_collation_actual_version_builtin(locale).map(|v| Some(v.to_owned()))
    } else if collprovider == COLLPROVIDER_LIBC {
        Ok(get_collation_actual_version_libc(locale))
    } else if collprovider == COLLPROVIDER_ICU {
        icu::get_collation_actual_version_icu(locale).map(Some)
    } else {
        Ok(None)
    }
}

// get_collation_actual_version_builtin (pg_locale_builtin.c).
fn get_collation_actual_version_builtin(collcollate: &str) -> PgResult<&'static str> {
    if collcollate == "C" || collcollate == "C.UTF-8" || collcollate == "PG_UNICODE_FAST" {
        Ok("1")
    } else {
        Err(invalid_builtin_locale(collcollate))
    }
}

// get_collation_actual_version_libc (pg_locale_libc.c): glibc reports its
// version; this platform set has no LC_VERSION_MASK/WIN32 arm.
fn get_collation_actual_version_libc(collcollate: &str) -> Option<String> {
    if collcollate.eq_ignore_ascii_case("C")
        || collcollate.len() >= 2 && collcollate.as_bytes()[..2].eq_ignore_ascii_case(b"C.")
        || collcollate.eq_ignore_ascii_case("POSIX")
    {
        return None;
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: gnu_get_libc_version returns a static NUL-terminated string.
        let v = unsafe { core::ffi::CStr::from_ptr(libc::gnu_get_libc_version()) };
        Some(String::from_utf8_lossy(v.to_bytes()).into_owned())
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    None
}

const PG_UTF8: i32 = 6;

#[track_caller]
#[cold]
fn invalid_builtin_locale(locale: &str) -> Box<PgError> {
    PgError::error(format!(
        "invalid locale name \"{locale}\" for builtin provider"
    ))
    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
    .into()
}

pub fn builtin_locale_encoding(locale: &str) -> PgResult<i32> {
    if locale == "C" {
        Ok(-1)
    } else if locale == "C.UTF-8" || locale == "PG_UNICODE_FAST" {
        Ok(PG_UTF8)
    } else {
        Err(invalid_builtin_locale(locale))
    }
}

pub fn builtin_validate_locale(encoding: i32, locale: &str) -> PgResult<&'static str> {
    let canonical_name = if locale == "C" {
        "C"
    } else if locale == "C.UTF-8" || locale == "C.UTF8" {
        "C.UTF-8"
    } else if locale == "PG_UNICODE_FAST" {
        "PG_UNICODE_FAST"
    } else {
        return Err(invalid_builtin_locale(locale));
    };

    let required_encoding = builtin_locale_encoding(canonical_name)?;
    if required_encoding >= 0 && encoding != required_encoding {
        return Err(PgError::error(format!(
            "encoding \"{}\" does not match locale \"{locale}\"",
            encoding_name(encoding)
        ))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
        .into());
    }
    Ok(canonical_name)
}

// pg_encoding_to_char rendering for the one mismatch message; the name-table
// owner is mb (only the database encoding reaches this in the create paths).
fn encoding_name(encoding: i32) -> String {
    if encoding == mbutils::GetDatabaseEncoding() {
        mbutils::GetDatabaseEncodingName().to_owned()
    } else {
        format!("encoding {encoding}")
    }
}

fn varstr_cmp_locale(collid: Oid, arg1: &[u8], arg2: &[u8]) -> PgResult<i32> {
    let locale = pg_newlocale_from_collation(collid)?;
    if locale.collate_is_c {
        return Ok(varlena::varstrfastcmp_c(arg1, arg2));
    }
    // C: cheap equality probe before the expensive collation compare.
    if arg1 == arg2 {
        return Ok(0);
    }
    let result = locale.pg_strncoll(arg1, arg2);
    if result == 0 && locale.deterministic {
        return Ok(varlena::varstrfastcmp_c(arg1, arg2));
    }
    Ok(result)
}

fn collation_is_deterministic(collid: Oid) -> PgResult<bool> {
    Ok(pg_newlocale_from_collation(collid)?.deterministic)
}

// hashtext/hashbpchar nondeterministic leg (hashfunc.c/varchar.c): hash the
// pg_strnxfrm sort key INCLUDING its NUL (C hashes bsize+1 bytes).
fn varstr_nondeterministic_hash(
    collid: Oid,
    data: &[u8],
    seed: Option<u64>,
) -> PgResult<Option<u64>> {
    let locale = pg_newlocale_from_collation(collid)?;
    if locale.deterministic {
        return Ok(None);
    }
    XFRM_SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        let bsize = locale.pg_strnxfrm(&mut [], data);
        buf.clear();
        buf.resize(bsize + 1, 0);
        let rsize = locale.pg_strnxfrm(&mut buf[..], data);
        if rsize > bsize {
            return Err(PgError::error("pg_strnxfrm() returned unexpected result").into());
        }
        buf[bsize] = 0;
        Ok(Some(match seed {
            None => hashfn::hash_bytes(&buf[..bsize + 1]) as u64,
            Some(seed) => hashfn::hash_bytes_extended(&buf[..bsize + 1], seed),
        }))
    })
}

thread_local! {
    static XFRM_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn get_collation_actual_version_seam<'mcx>(
    mcx: Mcx<'mcx>,
    collprovider: u8,
    locale: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    collation_actual_version(collprovider, locale)?
        .map(|v| PgString::from_str_in(&v, mcx))
        .transpose()
}

// icu_unicode_version (varlena.c): C returns the compile-time
// U_UNICODE_VERSION string under USE_ICU, else NULL. pgrust binds libicu at
// runtime, so the analog is the loaded library's u_getUnicodeVersion,
// rendered by ICU's own u_versionToString — the same "major.minor" shape as
// the header constant (trailing zero fields dropped, minimum two kept).
pub fn icu_unicode_version_str() -> Option<&'static str> {
    static VERSION: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    VERSION
        .get_or_init(|| {
            let api = icu_ffi::try_icu()?;
            let mut ver = [0u8; 4];
            let mut buf = [0u8; icu_ffi::U_MAX_VERSION_STRING_LENGTH];
            // SAFETY: ver is the 4-byte UVersionInfo both entry points take;
            // buf has the U_MAX_VERSION_STRING_LENGTH bytes the API requires.
            unsafe {
                (api.u_getUnicodeVersion)(ver.as_mut_ptr());
                (api.u_versionToString)(ver.as_ptr(), buf.as_mut_ptr() as *mut core::ffi::c_char);
            }
            let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            Some(String::from_utf8_lossy(&buf[..len]).into_owned())
        })
        .as_deref()
}

pub fn init_seams() {
    pg_locale_seams::varstr_cmp_locale::set(varstr_cmp_locale);
    pg_locale_seams::collation_is_deterministic::set(collation_is_deterministic);
    pg_locale_seams::pg_perm_setlocale::set(setup::pg_perm_setlocale);
    pg_locale_seams::set_database_ctype_is_c::set(setup::set_database_ctype_is_c);
    pg_locale_seams::init_database_collation::set(init_database_collation);
    pg_locale_seams::get_collation_actual_version::set(get_collation_actual_version_seam);
    pg_locale_seams::varstr_nondeterministic_hash::set(varstr_nondeterministic_hash);
    pg_locale_seams::icu_unicode_version::set(icu_unicode_version_str);

    setup::install_guc_hooks();
}
