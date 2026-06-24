//! Provider dispatch (`pg_locale.c:1270-1479`): the case-mapping family
//! (`pg_strlower`/`pg_strtitle`/`pg_strupper`/`pg_strfold`) reads the provider
//! discriminant, and the collation family (`pg_strcoll`/`pg_strncoll`/
//! `pg_strxfrm`/`pg_strnxfrm`/the prefix variants) indirects through the
//! `collate_methods` vtable (non-NULL only for non-C libc/ICU).
//!
//! Both families key by collation OID (the C `pg_locale_t` is re-resolved here
//! through the permanent cache): the case-mapping family resolves the entry,
//! dispatches on `entry.view.provider`, and hands the per-provider seam the same
//! `collid` so its owner re-resolves the `info` union it needs (the flag-core
//! [`PgLocaleStruct`] carries no `info`). The per-provider workers live in the
//! provider units: the libc collation primitives are bound in
//! [`crate::libc_provider`] (OS FFI), the libc case-mapping workers and the
//! builtin workers are in their (not-yet-ported) owner units, reached through
//! their seams.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;

use ::mcx::{Mcx, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::{PgError, PgResult};
use ::locale::{CollProvider, PgLocaleStruct};

use crate::cache::{resolve, LocaleEntry, LocaleInfo};

/// C: `PGLOCALE_SUPPORT_ERROR(provider)` -> `elog(ERROR, "unsupported
/// collprovider for %s: %c")`.
fn support_error(funcname: &str, provider: CollProvider) -> PgError {
    PgError::error(alloc::format!(
        "unsupported collprovider for {funcname}: {}",
        provider as u8 as char
    ))
}

// =============================================================================
// Flag-core accessors (collid-keyed seams used by the comparison families that
// carry no Mcx — varlena/hashfunc).
// =============================================================================

/// `pg_newlocale_from_collation(collid)->collate_is_c` (read off the resolved
/// locale by `varstr_cmp`).
pub fn collation_is_c(collid: Oid) -> PgResult<bool> {
    Ok(resolve(collid)?.view.collate_is_c)
}

/// `pg_newlocale_from_collation(collid)->deterministic`.
pub fn collation_is_deterministic(collid: Oid) -> PgResult<bool> {
    Ok(resolve(collid)?.view.deterministic)
}

/// `locale->deterministic` accessor over a resolved flag core (the
/// `pg_locale_deterministic` C helper).
#[must_use]
pub fn pg_locale_deterministic(locale: &PgLocaleStruct) -> bool {
    locale.deterministic
}

// =============================================================================
// Collation family (pg_locale.c:1347-1479) — collid-keyed seams.
// =============================================================================

/// `pg_strcoll(arg1, arg2, locale)` (`pg_locale.c:1352`) — `pg_strncoll` for
/// NUL-terminated inputs (the repo seam passes payload slices, equivalent to
/// the C `len == -1` legs).
pub fn pg_strcoll(collid: Oid, arg1: &[u8], arg2: &[u8]) -> PgResult<i32> {
    pg_strncoll(collid, arg1, arg2)
}

/// `pg_strncoll(arg1, len1, arg2, len2, locale)` (`pg_locale.c:1373`) — C:
/// `locale->collate->strncoll(...)`. Reachable only for `!collate_is_c` (a
/// `collate_is_c` locale has `collate == NULL`; callers take the memcmp fast
/// path first).
pub fn pg_strncoll(collid: Oid, arg1: &[u8], arg2: &[u8]) -> PgResult<i32> {
    let entry = resolve(collid)?;
    match &entry.info {
        LocaleInfo::Libc(l) if !entry.view.collate_is_c => {
            Ok(crate::libc_provider::strncoll_libc(arg1, arg2, l))
        }
        #[cfg(feature = "icu")]
        LocaleInfo::Icu(l) => icu_strncoll(l, arg1, arg2),
        _ => Err(support_error("pg_strncoll", entry.view.provider)),
    }
}

/// `strncoll_icu`/`strncoll_icu_utf8` dispatch: the ICU provider supports the
/// UTF-8 server encoding (the bounded scope); a non-UTF-8 encoding reports
/// `FEATURE_NOT_SUPPORTED`.
#[cfg(feature = "icu")]
fn icu_strncoll(l: &pg_locale_icu::IcuLocale, arg1: &[u8], arg2: &[u8]) -> PgResult<i32> {
    const PG_UTF8: i32 = 6;
    if mbutils_seams::get_database_encoding::call() != PG_UTF8 {
        return Err(pg_locale_icu::provider::non_utf8_unsupported("comparison"));
    }
    pg_locale_icu::provider::strncoll_icu(l, arg1, arg2)
}

/// `pg_strxfrm_enabled(locale)` (`pg_locale.c:1387`) —
/// `locale->collate->strxfrm_is_safe`. libc is conservatively `false` (the
/// non-`TRUST_STRXFRM` default); builtin/C have no collate vtable.
#[must_use]
pub fn pg_strxfrm_enabled(collid: Oid) -> bool {
    // collate_methods_libc.strxfrm_is_safe is `false` unless TRUST_STRXFRM, and
    // a collate_is_c / builtin locale has no collate vtable. ICU sets
    // `strxfrm_is_safe = true`.
    #[cfg(feature = "icu")]
    if let Ok(entry) = resolve(collid) {
        if matches!(entry.info, LocaleInfo::Icu(_)) {
            return true;
        }
    }
    let _ = collid;
    false
}

/// `pg_strxfrm(dest, src, destsize, locale)` (`pg_locale.c:1403`) — `pg_strnxfrm`
/// for a NUL-terminated input. The repo seam returns the full transformed blob.
pub fn pg_strxfrm<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let entry = resolve(collid)?;
    match &entry.info {
        LocaleInfo::Libc(l) if !entry.view.collate_is_c => {
            crate::libc_provider::strnxfrm_libc(mcx, src, l)
        }
        #[cfg(feature = "icu")]
        LocaleInfo::Icu(l) => {
            const PG_UTF8: i32 = 6;
            if mbutils_seams::get_database_encoding::call() != PG_UTF8 {
                return Err(pg_locale_icu::provider::non_utf8_unsupported("sort key"));
            }
            let blob = pg_locale_icu::provider::strnxfrm_icu(l, src)?;
            let mut out = ::mcx::vec_with_capacity_in::<u8>(mcx, blob.len())?;
            out.extend_from_slice(&blob);
            Ok(out)
        }
        _ => Err(support_error("pg_strnxfrm", entry.view.provider)),
    }
}

/// `pg_strxfrm_prefix_enabled(locale)` (`pg_locale.c:1439`) —
/// `locale->collate->strnxfrm_prefix != NULL`. libc never sets it; only ICU
/// provides a prefix transform.
#[must_use]
pub fn pg_strxfrm_prefix_enabled(_collid: Oid) -> bool {
    false
}

/// `pg_strxfrm_prefix(dest, src, destsize, locale)` (`pg_locale.c:1450`) —
/// `pg_strnxfrm_prefix`. libc (and builtin) have no `strnxfrm_prefix` method; C
/// would deref a NULL function pointer if a caller skipped
/// `pg_strxfrm_prefix_enabled`. Surface the support error instead.
pub fn pg_strxfrm_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    collid: Oid,
    _src: &[u8],
    _destsize: usize,
) -> PgResult<PgVec<'mcx, u8>> {
    let _ = mcx;
    let entry = resolve(collid)?;
    Err(support_error("pg_strnxfrm_prefix", entry.view.provider))
}

// =============================================================================
// Case mapping (pg_locale.c:1270-1345) — collid-keyed seams (symmetric with the
// comparison family): resolve the entry, dispatch on the resolved provider, and
// hand the per-provider seam the `collid` so its owner re-resolves the `info`
// union (libc `info.lt`, builtin `info.builtin.casemap_full`). The flag core
// [`PgLocaleStruct`] carries no `info`, so collid-keying is required here.
// =============================================================================

/// `pg_strlower(dst, dstsize, src, srclen, locale)` (`pg_locale.c:1271`).
pub fn pg_strlower<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let entry = resolve(collid)?;
    match entry.view.provider {
        CollProvider::Builtin => {
            pg_locale_builtin_seams::strlower_builtin::call(mcx, src, collid)
        }
        CollProvider::Libc => {
            pg_locale_libc_seams::strlower_libc::call(mcx, src, collid)
        }
        #[cfg(feature = "icu")]
        CollProvider::Icu => icu_case(mcx, &entry.info, src, pg_locale_icu::provider::strlower_icu),
        p => Err(support_error("pg_strlower", p)),
    }
}

/// Shared ICU case-mapping dispatch: the bounded UTF-8 path. Resolves the
/// `IcuLocale` from the entry, runs the given ICU case worker, and copies the
/// result into `mcx`. A non-UTF-8 server encoding reports
/// `FEATURE_NOT_SUPPORTED`.
#[cfg(feature = "icu")]
fn icu_case<'mcx>(
    mcx: Mcx<'mcx>,
    info: &LocaleInfo,
    src: &[u8],
    worker: fn(&pg_locale_icu::IcuLocale, &[u8]) -> PgResult<alloc::vec::Vec<u8>>,
) -> PgResult<PgVec<'mcx, u8>> {
    const PG_UTF8: i32 = 6;
    let LocaleInfo::Icu(l) = info else {
        return Err(support_error("pg_strlower", CollProvider::Icu));
    };
    if mbutils_seams::get_database_encoding::call() != PG_UTF8 {
        return Err(pg_locale_icu::provider::non_utf8_unsupported("case mapping"));
    }
    let blob = worker(l, src)?;
    let mut out = ::mcx::vec_with_capacity_in::<u8>(mcx, blob.len())?;
    out.extend_from_slice(&blob);
    Ok(out)
}

/// `pg_strtitle(...)` (`pg_locale.c:1290`).
pub fn pg_strtitle<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let entry = resolve(collid)?;
    match entry.view.provider {
        CollProvider::Builtin => {
            pg_locale_builtin_seams::strtitle_builtin::call(mcx, src, collid)
        }
        CollProvider::Libc => {
            pg_locale_libc_seams::strtitle_libc::call(mcx, src, collid)
        }
        #[cfg(feature = "icu")]
        CollProvider::Icu => icu_case(mcx, &entry.info, src, pg_locale_icu::provider::strtitle_icu),
        p => Err(support_error("pg_strtitle", p)),
    }
}

/// `pg_strupper(...)` (`pg_locale.c:1309`).
pub fn pg_strupper<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let entry = resolve(collid)?;
    match entry.view.provider {
        CollProvider::Builtin => {
            pg_locale_builtin_seams::strupper_builtin::call(mcx, src, collid)
        }
        CollProvider::Libc => {
            pg_locale_libc_seams::strupper_libc::call(mcx, src, collid)
        }
        #[cfg(feature = "icu")]
        CollProvider::Icu => icu_case(mcx, &entry.info, src, pg_locale_icu::provider::strupper_icu),
        p => Err(support_error("pg_strupper", p)),
    }
}

/// `pg_strfold(...)` (`pg_locale.c:1328`). For libc, C "just uses strlower"
/// (`pg_locale.c:1337-1339`); builtin has its own fold.
pub fn pg_strfold<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let entry = resolve(collid)?;
    match entry.view.provider {
        CollProvider::Builtin => {
            pg_locale_builtin_seams::strfold_builtin::call(mcx, src, collid)
        }
        CollProvider::Libc => {
            pg_locale_libc_seams::strlower_libc::call(mcx, src, collid)
        }
        #[cfg(feature = "icu")]
        CollProvider::Icu => icu_case(mcx, &entry.info, src, pg_locale_icu::provider::strfold_icu),
        p => Err(support_error("pg_strfold", p)),
    }
}

/// `tolower_l(c, locale->info.lt)` (`SB_lower_char` in like.c): single-byte
/// lower-case fold through the libc `locale_t` of the resolved (non-default,
/// non-C) libc collation. Re-keyed by `collid`.
pub fn char_tolower(c: u8, collid: Oid) -> u8 {
    let entry = match resolve(collid) {
        Ok(e) => e,
        // C reaches this only for a valid resolved libc collation; a failed
        // resolve is unreachable on the SB_lower_char third leg.
        Err(_) => return c,
    };
    match &entry.info {
        LocaleInfo::Libc(l) => crate::libc_provider::char_tolower_libc(c, l),
        // Not reached for builtin/C on SB_lower_char's libc-only leg.
        _ => c,
    }
}

/// Internal helper so `localeconv`/tests can read an entry's flag core view.
#[allow(dead_code)]
pub(crate) fn entry_view(entry: &LocaleEntry) -> &PgLocaleStruct {
    &entry.view
}
