#![no_std]
#![allow(non_snake_case)]

//! Port of `utils/adt/pg_locale_builtin.c` — the builtin collation/ctype
//! provider.
//!
//! The builtin provider is the one PostgreSQL locale provider that is entirely
//! self-contained: case mapping uses the in-tree Unicode tables
//! (`common/unicode_case.c` → [`unicode_case`]) and word boundaries use
//! the Unicode category tables (`common/unicode_category.c` →
//! [`unicode_category`]). The only genuinely-external input is the
//! database encoding (`GetDatabaseEncoding()`), reached through mbutils' seam.
//!
//! The flag core (`pg_locale_struct` minus its provider `info` union) is the
//! trimmed [`::locale::PgLocaleStruct`]; the `info.builtin.casemap_full`
//! flag travels alongside it in [`PgLocaleBuiltinResult`] so the permanent-cache
//! owner can store it in its `LocaleInfo::Builtin` arm. The case-mapping and
//! regex predicate seams are keyed by `collid` / `casemap_full`, re-resolving the
//! locale rather than threading the full `pg_locale_t` across the seam.

extern crate alloc;

use alloc::format;

use mcx::{Mcx, PgVec};
use ::types_core::primitive::{Oid, PgWChar};
use ::types_error::PgResult;
use locale::{CollProvider, PgLocaleStruct};
use ::types_tuple::heaptuple::DEFAULT_COLLATION_OID;

use unicode_case::{
    unicode_lowercase_simple, unicode_strfold, unicode_strlower, unicode_strtitle,
    unicode_strupper, unicode_uppercase_simple,
};
use unicode_category::{
    pg_u_isalnum, pg_u_isalpha, pg_u_isdigit, pg_u_isgraph, pg_u_islower, pg_u_isprint,
    pg_u_ispunct, pg_u_isspace, pg_u_isupper,
};

use collationcmds_seams as cc;
use pg_locale_builtin_seams::{self as seams, PgLocaleBuiltinResult};
use pg_locale_catalog_seams as catalog;
use ::pg_locale_seams::RegexWcClass;
use init_small_seams as init_small;
use mbutils_seams as mb;

/// `"PG_UNICODE_FAST"` — the full-Unicode-casemap builtin locale.
const PG_UNICODE_FAST_LOCALE: &str = "PG_UNICODE_FAST";
/// `"C"` — the C/POSIX builtin locale.
const C_LOCALE: &str = "C";

/// Resolve the locale name for a builtin-provider `collid`
/// (`create_pg_locale_builtin`, `pg_locale_builtin.c:122`): for
/// `DEFAULT_COLLATION_OID` it is the database's `datlocale`
/// (`SearchSysCache1(DATABASEOID, MyDatabaseId)`), otherwise the collation's
/// `colllocale` (`SearchSysCache1(COLLOID, collid)`).
fn builtin_locale_name(collid: Oid) -> PgResult<alloc::string::String> {
    if collid == DEFAULT_COLLATION_OID {
        let row = catalog::database_locale_row::call()?.ok_or_else(|| {
            let dbid = init_small::my_database_id::call();
            cache_lookup_failed_for_database(dbid)
        })?;
        // SysCacheGetAttrNotNull(DATABASEOID, Anum_pg_database_datlocale): a
        // builtin-provider database always records datlocale.
        row.locale.ok_or_else(attr_not_null)
    } else {
        let row = catalog::collation_locale_row::call(collid)?
            .ok_or_else(|| cache_lookup_failed_for_collation(collid))?;
        // SysCacheGetAttrNotNull(COLLOID, Anum_pg_collation_colllocale).
        row.locale.ok_or_else(attr_not_null)
    }
}

/// `create_pg_locale_builtin(collid, context)` (`pg_locale_builtin.c:122`):
/// look up the locale name, `builtin_validate_locale` it against the database
/// encoding, then assemble the flag core (`collate_is_c = true`,
/// `ctype_is_c = (locstr == "C")`, `casemap_full = (locstr ==
/// "PG_UNICODE_FAST")`, `deterministic = true`).
pub fn create_pg_locale_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    collid: Oid,
) -> PgResult<PgLocaleBuiltinResult<'mcx>> {
    let locstr = builtin_locale_name(collid)?;

    // builtin_validate_locale(GetDatabaseEncoding(), locstr) — pg_locale.c-owned,
    // installed by the pg_locale unit; canonicalizes + encoding-checks the name.
    cc::builtin_validate_locale::call(mcx, mb::get_database_encoding::call(), &locstr)?;

    let casemap_full = locstr == PG_UNICODE_FAST_LOCALE;
    let view = ::mcx::alloc_in(
        mcx,
        PgLocaleStruct {
            provider: CollProvider::Builtin,
            deterministic: true,
            collate_is_c: true,
            ctype_is_c: locstr == C_LOCALE,
            is_default: false,
        },
    )?;

    Ok(PgLocaleBuiltinResult { view, casemap_full })
}

/// `get_collation_actual_version_builtin(collcollate)`
/// (`pg_locale_builtin.c:169`): the static version `"1"` for the three supported
/// builtin locales, `Err(ERRCODE_WRONG_OBJECT_TYPE)` for any other name.
pub fn get_collation_actual_version_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    collcollate: &str,
) -> PgResult<PgVec<'mcx, u8>> {
    if collcollate == "C" || collcollate == "C.UTF-8" || collcollate == PG_UNICODE_FAST_LOCALE {
        ::mcx::slice_in(mcx, b"1".as_slice())
    } else {
        Err(invalid_locale_name(collcollate))
    }
}

/// Whether the builtin collation `collid` uses full Unicode case mapping
/// (`info.builtin.casemap_full`): true only for `PG_UNICODE_FAST`. Re-resolves
/// the locale name from the catalog (the collid-keyed `str*_builtin` family does
/// not carry the resolved `pg_locale_t`).
fn casemap_full_for(collid: Oid) -> PgResult<bool> {
    Ok(builtin_locale_name(collid)? == PG_UNICODE_FAST_LOCALE)
}

/// Decode `src` (a NUL-bounded UTF-8 `const char *`) to `&str`, mirroring C's
/// `srclen`-bounded input; the slice already bounds it.
fn src_str(src: &[u8]) -> PgResult<&str> {
    let bounded = src.split(|b| *b == 0).next().unwrap_or(src);
    core::str::from_utf8(bounded).map_err(|e| ::types_error::PgError::error(format!("{e}")))
}

/// `strlower_builtin` (`pg_locale_builtin.c:80`): `unicode_strlower(...,
/// casemap_full)`.
pub fn strlower_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let full = casemap_full_for(collid)?;
    unicode_strlower(mcx, src_str(src)?, full)
}

/// `strtitle_builtin` (`pg_locale_builtin.c:88`): `unicode_strtitle(...,
/// casemap_full, initcap_wbnext, ...)`. `initcap_wbnext` draws a word boundary
/// each time `pg_u_isalnum` changes; `posix` is `!casemap_full`
/// (`pg_locale_builtin.c:96`).
pub fn strtitle_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let full = casemap_full_for(collid)?;
    let s = src_str(src)?;
    let boundaries = word_boundaries(s, !full);
    let mut iter = boundaries.into_iter();
    unicode_strtitle(mcx, s, full, move || iter.next().unwrap_or(s.len()))
}

/// `strupper_builtin` (`pg_locale_builtin.c:106`): `unicode_strupper(...,
/// casemap_full)`.
pub fn strupper_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let full = casemap_full_for(collid)?;
    unicode_strupper(mcx, src_str(src)?, full)
}

/// `strfold_builtin` (`pg_locale_builtin.c:114`): `unicode_strfold(...,
/// casemap_full)`.
pub fn strfold_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let full = casemap_full_for(collid)?;
    unicode_strfold(mcx, src_str(src)?, full)
}

/// In-crate logic of `initcap_wbnext` (`pg_locale_builtin.c:52`): word
/// boundaries drawn each time `pg_u_isalnum` changes. Returns the byte offsets
/// the boundary iterator yields, with the final `src.len()` sentinel
/// `initcap_wbnext` returns when the cursor runs out. `posix` is `!casemap_full`.
fn word_boundaries(src: &str, posix: bool) -> alloc::vec::Vec<usize> {
    let mut boundaries = alloc::vec::Vec::new();
    let mut initialized = false;
    let mut prev_alnum = false;

    for (offset, ch) in src.char_indices() {
        let curr_alnum = pg_u_isalnum(ch as PgWChar, posix);
        if !initialized || curr_alnum != prev_alnum {
            boundaries.push(offset);
            initialized = true;
            prev_alnum = curr_alnum;
        }
    }
    boundaries.push(src.len());
    boundaries
}

/// `pg_wc_is<class>(c)` BUILTIN strategy (`regc_pg_locale.c:306..532`). The
/// digit/alnum/punct predicates take `posix` (= `!casemap_full`); the rest are
/// locale-independent.
pub fn regex_wc_isclass_builtin(class: RegexWcClass, c: PgWChar, posix: bool) -> bool {
    match class {
        RegexWcClass::Digit => pg_u_isdigit(c, posix),
        RegexWcClass::Alpha => pg_u_isalpha(c),
        RegexWcClass::Alnum => pg_u_isalnum(c, posix),
        RegexWcClass::Upper => pg_u_isupper(c),
        RegexWcClass::Lower => pg_u_islower(c),
        RegexWcClass::Graph => pg_u_isgraph(c),
        RegexWcClass::Print => pg_u_isprint(c),
        RegexWcClass::Punct => pg_u_ispunct(c, posix),
        RegexWcClass::Space => pg_u_isspace(c),
    }
}

/// `pg_wc_toupper(c)` BUILTIN strategy (`regc_pg_locale.c:558`):
/// `unicode_uppercase_simple(c)`.
pub fn regex_wc_toupper_builtin(c: PgWChar) -> PgWChar {
    unicode_uppercase_simple(c)
}

/// `pg_wc_tolower(c)` BUILTIN strategy (`regc_pg_locale.c:590`):
/// `unicode_lowercase_simple(c)`.
pub fn regex_wc_tolower_builtin(c: PgWChar) -> PgWChar {
    unicode_lowercase_simple(c)
}

/// C: `elog(ERROR, "cache lookup failed for collation %u", collid)`.
fn cache_lookup_failed_for_collation(collid: Oid) -> ::types_error::PgError {
    ::types_error::PgError::error(format!("cache lookup failed for collation {collid}"))
}

/// C: `elog(ERROR, "cache lookup failed for database %u", MyDatabaseId)`.
fn cache_lookup_failed_for_database(dboid: Oid) -> ::types_error::PgError {
    ::types_error::PgError::error(format!("cache lookup failed for database {dboid}"))
}

/// C: `SysCacheGetAttrNotNull` -> `elog(ERROR, "unexpected null value in system
/// cache %d column %d")` when the NOT-NULL `colllocale`/`datlocale` attribute is
/// NULL. Effectively unreachable: a builtin-provider row always records it.
fn attr_not_null() -> ::types_error::PgError {
    ::types_error::PgError::error("unexpected null value in system cache")
}

/// `ereport(ERROR, errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("invalid locale
/// name \"%s\" for builtin provider", ...))` (`pg_locale_builtin.c:186`).
fn invalid_locale_name(locale: &str) -> ::types_error::PgError {
    ::types_error::PgError::error(format!(
        "invalid locale name \"{locale}\" for builtin provider"
    ))
    .with_sqlstate(::types_error::ERRCODE_WRONG_OBJECT_TYPE)
}

/// Install every seam in `backend-utils-adt-pg-locale-builtin-seams`.
pub fn init_seams() {
    seams::create_pg_locale_builtin::set(create_pg_locale_builtin);
    seams::get_collation_actual_version_builtin::set(get_collation_actual_version_builtin);
    seams::strlower_builtin::set(strlower_builtin);
    seams::strtitle_builtin::set(strtitle_builtin);
    seams::strupper_builtin::set(strupper_builtin);
    seams::strfold_builtin::set(strfold_builtin);
    seams::regex_wc_isclass_builtin::set(regex_wc_isclass_builtin);
    seams::regex_wc_toupper_builtin::set(regex_wc_toupper_builtin);
    seams::regex_wc_tolower_builtin::set(regex_wc_tolower_builtin);
}
