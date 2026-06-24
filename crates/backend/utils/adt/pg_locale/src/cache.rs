//! The collation cache and locale singletons — the process-global state at the
//! top of `pg_locale.c` (`c_locale`, `default_locale`, the `collation_cache`
//! simplehash + MRU shortcut), and `create_pg_locale` / `pg_newlocale_from_collation`
//! / `init_database_collation`.
//!
//! ## Carrier model
//!
//! C hands callers a `pg_locale_t` = `pg_locale_struct *` pointing into the
//! never-reset `CollationCacheContext`; the struct carries the flag core, the
//! `collate` method vtable (non-NULL only for non-C libc/ICU), and the `info`
//! union (libc `locale_t`, builtin tables, ICU `UCollator`). The repo's
//! `::locale::PgLocaleStruct` is the flag core only — every consumer reads
//! it off the resolved locale, and the provider-specific operations re-key by
//! the collation OID (`pg_strncoll`/`pg_strxfrm`/`char_tolower`), with this crate
//! re-resolving the same cache entry. So the full entry lives here as
//! [`LocaleEntry`]: the flag core ([`LocaleEntry::view`]) the seams copy out, plus
//! the owned [`LocaleInfo`] that the collid-keyed operations dispatch on.
//!
//! The cache is per-backend C global state, so it is held in a `thread_local`
//! (one backend == one thread here). Entries are interned for the backend
//! lifetime (C's `CollationCacheContext` is never reset), so cached entries are
//! `&'static`-equivalent within the thread via a leaked `Box`.

extern crate alloc;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use core::cell::RefCell;

use ::utils_error::ereport;
use ::mcx::Mcx;
use ::types_core::primitive::{Oid, OidIsValid};
use ::types_error::{ErrorLocation, PgError, PgResult, WARNING};
use ::locale::{CollProvider, PgLocale, PgLocaleStruct};
use ::types_tuple::heaptuple::DEFAULT_COLLATION_OID;

use pg_locale_catalog_seams as catalog;

use crate::libc_provider::LibcLocale;

/// `C_COLLATION_OID` (`pg_collation.dat` oid 950) — the `C` collation, which
/// `pg_newlocale_from_collation` resolves without catalog access.
const C_COLLATION_OID: Oid = ::types_core::catalog::C_COLLATION_OID;

/// `COLLPROVIDER_*` codes as the `char`/`i8` the catalog row carries.
const COLLPROVIDER_BUILTIN: i8 = b'b' as i8;
const COLLPROVIDER_ICU: i8 = b'i' as i8;
const COLLPROVIDER_LIBC: i8 = b'c' as i8;

/// The owned provider-specific state — the C `pg_locale_struct.info` union plus
/// the `collate` vtable, expressed as a Rust enum so the resource lifetime is
/// type-tracked.
pub enum LocaleInfo {
    /// The bare C/POSIX locale (`c_locale`): no OS resource, `collate == NULL`.
    CLocale,
    /// A libc-provider locale: an owned `locale_t` (NULL for C/POSIX collate).
    Libc(LibcLocale),
    /// A builtin-provider locale (`pg_locale_builtin.c`): the builtin Unicode
    /// tables live in the builtin unit. Beyond the flag core this carries
    /// `info.builtin.casemap_full` (true only for `PG_UNICODE_FAST`), which the
    /// collid-keyed case-mapping and regex-predicate seams need; case-mapping
    /// itself dispatches to the builtin owner seam.
    Builtin {
        /// `info.builtin.casemap_full` — full Unicode case mapping.
        casemap_full: bool,
    },
    /// An ICU-provider locale (`pg_locale_icu.c`): the owned `UCollator` +
    /// locale string (`info.icu`). ICU always carries a `collate` vtable
    /// (`collate_is_c` is always false for ICU).
    Icu(pg_locale_icu::IcuLocale),
}

impl LocaleInfo {
    /// Whether this provider has a `collate` method vtable (C: `result->collate
    /// != NULL`). libc sets it iff its collation is not C/POSIX; the bare
    /// C-locale and builtin never do (they are `collate_is_c`); ICU always does.
    fn has_collate_methods(&self) -> bool {
        match self {
            LocaleInfo::Libc(l) => l.has_collate_methods(),
            LocaleInfo::Icu(_) => true,
            LocaleInfo::CLocale | LocaleInfo::Builtin { .. } => false,
        }
    }
}

/// A cached locale: the flag core the seams copy out plus the owned provider
/// state the collid-keyed operations dispatch on.
pub struct LocaleEntry {
    /// `*pg_locale_t` flag core (`provider`/`deterministic`/`collate_is_c`/
    /// `ctype_is_c`/`is_default`).
    pub view: PgLocaleStruct,
    /// Owned provider-specific state (the `info` union + `collate` discriminant).
    pub info: LocaleInfo,
}

impl LocaleEntry {
    /// The static C-locale entry (`c_locale`): `COLLPROVIDER_LIBC`,
    /// deterministic, `collate_is_c`, `ctype_is_c`, not default.
    fn c_locale() -> LocaleEntry {
        LocaleEntry {
            view: PgLocaleStruct {
                provider: CollProvider::Libc,
                deterministic: true,
                collate_is_c: true,
                ctype_is_c: true,
                is_default: false,
            },
            info: LocaleInfo::CLocale,
        }
    }
}

thread_local! {
    /// The static `c_locale` singleton, interned once per backend.
    static C_LOCALE: &'static LocaleEntry = Box::leak(Box::new(LocaleEntry::c_locale()));

    /// `static pg_locale_t default_locale` — the database default collation,
    /// `None` until `init_database_collation` runs.
    static DEFAULT_LOCALE: RefCell<Option<&'static LocaleEntry>> = const { RefCell::new(None) };

    /// `collation_cache` (the simplehash) + the `last_collation_cache_oid` /
    /// `last_collation_cache_locale` MRU shortcut.
    static COLLATION_CACHE: RefCell<BTreeMap<Oid, &'static LocaleEntry>> =
        const { RefCell::new(BTreeMap::new()) };
    static LAST_CACHE: RefCell<Option<(Oid, &'static LocaleEntry)>> = const { RefCell::new(None) };
}

/// The bare C-locale entry (`&c_locale`).
pub fn c_locale_entry() -> &'static LocaleEntry {
    C_LOCALE.with(|e| *e)
}

/// `default_locale`, if `init_database_collation` has run.
pub fn default_locale() -> Option<&'static LocaleEntry> {
    DEFAULT_LOCALE.with(|d| *d.borrow())
}

/// `create_pg_locale(collid, context)` (`pg_locale.c:1074`): look up the
/// `pg_collation` row, dispatch on the provider, set `is_default = false`,
/// assert the collate/collate_is_c invariant, then warn on a recorded-vs-actual
/// collation-version mismatch.
fn create_pg_locale(collid: Oid) -> PgResult<LocaleEntry> {
    let row = catalog::collation_locale_row::call(collid)?
        .ok_or_else(|| cache_lookup_failed_for_collation(collid))?;

    // C: dispatch on collform->collprovider.
    let mut entry = if row.provider == COLLPROVIDER_BUILTIN {
        create_pg_locale_builtin(collid)?
    } else if row.provider == COLLPROVIDER_ICU {
        // C: colllocale is NOT NULL for ICU collations (SysCacheGetAttrNotNull).
        let iculocstr = row.locale.as_deref().ok_or_else(|| {
            PgError::error(format!("null colllocale for ICU collation {collid}"))
        })?;
        create_pg_locale_icu(iculocstr, row.is_deterministic, row.icurules.as_deref())?
    } else if row.provider == COLLPROVIDER_LIBC {
        crate::libc_provider::create_pg_locale_libc(collid)?
    } else {
        // PGLOCALE_SUPPORT_ERROR: shouldn't happen.
        return Err(pglocale_support_error("create_pg_locale", row.provider));
    };

    // C: result->is_default = false.
    entry.view.is_default = false;

    // C: Assert((collate_is_c && collate == NULL) || (!collate_is_c && collate != NULL)).
    // Here the libc provider sets its `collate` methods iff !collate_is_c; the
    // entry's flag core must agree with whether it carries a collate vtable.
    debug_assert!(
        entry.view.collate_is_c != entry.info.has_collate_methods(),
        "collate_is_c XOR (collate methods present) invariant violated"
    );

    // C: version-mismatch handling, only when the catalog recorded a collversion.
    if let Some(ref collversionstr) = row.version {
        // C reads collcollate for libc, colllocale otherwise.
        let actual_source = if row.provider == COLLPROVIDER_LIBC {
            row.collate.as_deref().unwrap_or("")
        } else {
            row.locale.as_deref().unwrap_or("")
        };
        let actual = crate::version::get_collation_actual_version(row.provider, actual_source)?;
        match actual {
            None => return Err(collation_has_no_actual_version(&row.name)),
            Some(actual) => {
                if &actual != collversionstr {
                    warn_version_mismatch(&row, collversionstr, &actual)?;
                }
            }
        }
    }

    Ok(entry)
}

/// `pg_newlocale_from_collation(collid)` (`pg_locale.c:1196`): resolve a
/// collation OID to its cached entry, building it on first use. The returned
/// reference is stable for the backend lifetime (entries are interned).
pub fn resolve(collid: Oid) -> PgResult<&'static LocaleEntry> {
    // C: if (collid == DEFAULT_COLLATION_OID) return default_locale;
    if collid == DEFAULT_COLLATION_OID {
        return default_locale()
            .ok_or_else(|| PgError::error("default collation locale is not initialized"));
    }

    // C: some callers expect C_COLLATION_OID to succeed without catalog access.
    if collid == C_COLLATION_OID {
        return Ok(c_locale_entry());
    }

    // C: if (!OidIsValid(collid)) elog(ERROR, "cache lookup failed ...").
    if !OidIsValid(collid) {
        return Err(cache_lookup_failed_for_collation(collid));
    }

    // C: AssertCouldGetRelation(); MRU shortcut.
    if let Some(entry) = LAST_CACHE.with(|c| {
        c.borrow()
            .and_then(|(oid, e)| (oid == collid).then_some(e))
    }) {
        return Ok(entry);
    }

    // C: collation_cache_insert; build on a miss.
    let entry = COLLATION_CACHE.with(|cache| cache.borrow().get(&collid).copied());
    let entry = match entry {
        Some(e) => e,
        None => {
            let built: &'static LocaleEntry = Box::leak(Box::new(create_pg_locale(collid)?));
            COLLATION_CACHE.with(|cache| cache.borrow_mut().insert(collid, built));
            built
        }
    };

    LAST_CACHE.with(|c| *c.borrow_mut() = Some((collid, entry)));
    Ok(entry)
}

/// `pg_newlocale_from_collation` seam body: resolve the entry, then copy the
/// flag core ([`PgLocaleStruct`]) into `mcx` (the seam carrier).
pub fn pg_newlocale_from_collation<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<PgLocale<'mcx>> {
    let entry = resolve(collid)?;
    ::mcx::alloc_in(mcx, entry.view)
}

/// `init_database_collation()` (`pg_locale.c:1153`): build the database default
/// locale from `datlocprovider`, mark `is_default = true`, publish it as
/// `default_locale`, and record `database_ctype_is_c` from `datctype`.
pub fn init_database_collation() -> PgResult<()> {
    // C: Assert(default_locale == NULL).
    debug_assert!(
        default_locale().is_none(),
        "init_database_collation called twice"
    );

    // C: SearchSysCache1(DATABASEOID, MyDatabaseId); "cache lookup failed for
    // database" on an absent row.
    let row = catalog::database_locale_row::call()?
        .ok_or_else(|| cache_lookup_failed_for_database(catalog::my_database_id::call()))?;

    // C: dispatch on dbform->datlocprovider, building DEFAULT_COLLATION_OID.
    let mut entry = if row.provider == COLLPROVIDER_BUILTIN {
        create_pg_locale_builtin(DEFAULT_COLLATION_OID)?
    } else if row.provider == COLLPROVIDER_ICU {
        // C: the default database collation is always deterministic; datlocale
        // is NOT NULL for an ICU-provider database. `daticurules` is not yet
        // surfaced on DatabaseLocaleRow (no ICU-default cluster exercises it in
        // this profile), so custom default rules are not applied here.
        let iculocstr = row.locale.as_deref().ok_or_else(|| {
            PgError::error("null datlocale for ICU database default collation")
        })?;
        create_pg_locale_icu(iculocstr, true, None)?
    } else if row.provider == COLLPROVIDER_LIBC {
        crate::libc_provider::create_pg_locale_libc(DEFAULT_COLLATION_OID)?
    } else {
        return Err(pglocale_support_error("init_database_collation", row.provider));
    };

    // C: result->is_default = true.
    entry.view.is_default = true;

    let entry: &'static LocaleEntry = Box::leak(Box::new(entry));
    DEFAULT_LOCALE.with(|d| *d.borrow_mut() = Some(entry));

    // postinit.c computes database_ctype_is_c from datctype (C/POSIX compare),
    // independent of the provider; mirror that here (no postinit port yet).
    crate::setup::set_database_ctype_is_c(datctype_is_c(&row.ctype));

    Ok(())
}

/// C: `(strcmp(datctype,"C") == 0 || strcmp(datctype,"POSIX") == 0)`
/// (postinit.c CheckMyDatabase).
fn datctype_is_c(datctype: &str) -> bool {
    datctype == "C" || datctype == "POSIX"
}

/// `create_pg_locale_builtin(collid, context)` — builtin owner is not ported;
/// crosses to its seam (panics until it lands), then adapts the flag core into
/// a [`LocaleEntry`] with `info = Builtin` (builtin is always `collate_is_c`).
///
/// The builtin owner allocates its `pg_locale_struct` in `context`; here that is
/// a transient context (the flag core is `Copy`'d out, then the cached entry is
/// interned in the backend-lifetime `Box`, matching C's permanent cache).
fn create_pg_locale_builtin(collid: Oid) -> PgResult<LocaleEntry> {
    let ctx = ::mcx::MemoryContext::new("create_pg_locale_builtin");
    let (view, casemap_full) = {
        let result =
            pg_locale_builtin_seams::create_pg_locale_builtin::call(ctx.mcx(), collid)?;
        (*result.view, result.casemap_full)
    };
    Ok(LocaleEntry {
        view,
        info: LocaleInfo::Builtin { casemap_full },
    })
}

/// `create_pg_locale_icu(collid, context)` (`pg_locale_icu.c:142`, `USE_ICU`):
/// open the `UCollator` for `iculocstr` and assemble the locale entry. ICU is
/// always non-C for collate/ctype and carries a collate vtable. With the
/// `with-icu` feature off, the ICU provider crate has no live constructor and
/// this path is unreachable (an ICU collation row could only exist if seeded by
/// an ICU build; resolving it then errors through the seam).
#[cfg(feature = "icu")]
fn create_pg_locale_icu(
    iculocstr: &str,
    deterministic: bool,
    icurules: Option<&str>,
) -> PgResult<LocaleEntry> {
    if icurules.is_some() {
        // Custom ICU tailoring rules (`collicurules`) are out of the bounded
        // scope; opening with rules needs ucol_openRules + the UConverter path.
        return Err(PgError::error(
            "ICU collations with custom rules are not supported in this build",
        )
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }
    let result = pg_locale_icu::create_pg_locale_icu(iculocstr, deterministic)?;
    let view = PgLocaleStruct {
        provider: CollProvider::Icu,
        deterministic: result.deterministic,
        collate_is_c: false,
        ctype_is_c: false,
        is_default: false,
    };
    Ok(LocaleEntry {
        view,
        info: LocaleInfo::Icu(result.icu),
    })
}

/// `create_pg_locale_icu` with the ICU provider compiled out: an ICU collation
/// row resolved in a non-ICU build reports `FEATURE_NOT_SUPPORTED` (C's `#else`).
#[cfg(not(feature = "icu"))]
fn create_pg_locale_icu(
    _iculocstr: &str,
    _deterministic: bool,
    _icurules: Option<&str>,
) -> PgResult<LocaleEntry> {
    let ctx = ::mcx::MemoryContext::new("create_pg_locale_icu");
    pg_locale_icu::create_pg_locale_icu_seam(ctx.mcx(), C_COLLATION_OID).map(|_| unreachable!())
}

/// C: `elog(ERROR, "cache lookup failed for collation %u", collid)`.
fn cache_lookup_failed_for_collation(collid: Oid) -> PgError {
    PgError::error(format!("cache lookup failed for collation {collid}"))
}

/// C: `elog(ERROR, "cache lookup failed for database %u", MyDatabaseId)`.
fn cache_lookup_failed_for_database(dboid: Oid) -> PgError {
    PgError::error(format!("cache lookup failed for database {dboid}"))
}

/// C: `ereport(ERROR, errmsg("collation \"%s\" has no actual version, but a
/// version was recorded", ...))`.
fn collation_has_no_actual_version(collname: &str) -> PgError {
    PgError::error(format!(
        "collation \"{collname}\" has no actual version, but a version was recorded"
    ))
}

/// C: `PGLOCALE_SUPPORT_ERROR(provider)` -> `elog(ERROR, "unsupported
/// collprovider for %s: %c")`.
pub(crate) fn pglocale_support_error(funcname: &str, provider: i8) -> PgError {
    PgError::error(format!(
        "unsupported collprovider for {funcname}: {}",
        provider as u8 as char
    ))
}

/// C: `ereport(WARNING, "collation \"%s\" has version mismatch", errdetail(...),
/// errhint("... ALTER COLLATION %s REFRESH VERSION ..."))`.
fn warn_version_mismatch(
    row: &catalog::CollationLocaleRow,
    collversionstr: &str,
    actual_versionstr: &str,
) -> PgResult<()> {
    let collname = row.name.as_str();
    let nspname = catalog::get_namespace_name::call(row.namespace)?.unwrap_or_default();
    let qualified = quote_qualified_identifier(&nspname, collname);

    ereport(WARNING)
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
        .finish(ErrorLocation::new(
            "../src/backend/utils/adt/pg_locale.c",
            1132,
            "create_pg_locale",
        ))
}

/// `quote_qualified_identifier` (ruleutils.c): join namespace and name with a
/// dot, quoting any component that is not a bare lowercase identifier.
fn quote_qualified_identifier(namespace: &str, ident: &str) -> String {
    if namespace.is_empty() {
        quote_identifier(ident)
    } else {
        format!("{}.{}", quote_identifier(namespace), quote_identifier(ident))
    }
}

fn quote_identifier(ident: &str) -> String {
    let needs_quotes = ident.is_empty()
        || !ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        || !ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if needs_quotes {
        format!("\"{}\"", ident.replace('"', "\"\""))
    } else {
        String::from(ident)
    }
}
