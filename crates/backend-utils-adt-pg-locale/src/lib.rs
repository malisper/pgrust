//! Port of `src/backend/utils/adt/pg_locale.c` — the locale-machinery hub.
//!
//! `pg_locale.c` ties the three locale providers (libc / builtin / ICU) together
//! behind a uniform `pg_locale_t` and a provider-dispatch surface, plus the
//! collation cache, the `lconv`/`lc_time` formatting caches, the GUC locale
//! hooks, and the `setlocale` wrappers:
//!
//! * [`cache`] — `c_locale`, `default_locale`, the collation cache,
//!   `create_pg_locale`, `pg_newlocale_from_collation`, `init_database_collation`;
//! * [`dispatch`] — the provider-dispatch family (`pg_strcoll`/`pg_strlower`/…);
//! * [`libc_provider`] — the libc collation path bound to OS FFI (the
//!   `pg_locale_libc.c` collation primitives `make_libc_collator`/`strncoll_libc`/
//!   `strnxfrm_libc`/`get_collation_actual_version_libc`/`char_tolower`);
//! * [`version`] — `get_collation_actual_version`;
//! * [`setup`] — `pg_perm_setlocale`/`check_locale` + the GUC locale hooks;
//! * [`localeconv`] — `PGLC_localeconv`/`cache_locale_time`.
//!
//! ## Provider boundaries
//!
//! The builtin provider (`pg_locale_builtin.c`) is a separate, not-yet-ported
//! unit; its `create_pg_locale_builtin` / `strlower_builtin` / … cross through
//! `backend-utils-adt-pg-locale-builtin-seams` (panic until it lands). The ICU
//! provider is the merged ICU-disabled crate (returns `FEATURE_NOT_SUPPORTED`).
//! The libc *case-mapping* workers (`strlower_libc` …) belong to the libc unit
//! and cross through `backend-utils-adt-pg-locale-libc-seams`; the libc
//! *collation* primitives, which `pg_locale.c`'s permanent cache owns the
//! `locale_t` for, are bound here. The two syscache reads
//! (`COLLOID`/`DATABASEOID`) cross through
//! `backend-utils-adt-pg-locale-catalog-seams`, and the encoding helpers through
//! `backend-utils-adt-pg-locale-env-seams` (all unported owners).

#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;

use mcx::{Mcx, PgString};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE,
};

pub mod cache;
pub mod dispatch;
pub mod libc_provider;
pub mod localeconv;
pub mod regex_locale;
pub mod setup;
pub mod version;

pub use cache::{init_database_collation, pg_newlocale_from_collation};
pub use dispatch::{
    char_tolower, collation_is_c, collation_is_deterministic, pg_locale_deterministic, pg_strcoll,
    pg_strfold, pg_strlower, pg_strncoll, pg_strtitle, pg_strupper, pg_strxfrm, pg_strxfrm_enabled,
    pg_strxfrm_prefix, pg_strxfrm_prefix_enabled,
};
pub use localeconv::{
    cache_locale_time, localized_abbrev_days, localized_abbrev_months, localized_full_days,
    localized_full_months, pglc_localeconv,
};
pub use setup::{
    assign_locale_messages, assign_locale_monetary, assign_locale_numeric, assign_locale_time,
    check_locale, check_locale_messages, check_locale_monetary, check_locale_numeric,
    check_locale_time, database_ctype_is_c, pg_perm_setlocale, set_database_ctype_is_c,
};
pub use version::get_collation_actual_version;

const PG_UTF8: i32 = 6;

/// `builtin_locale_encoding(locale)` (`pg_locale.c:1485`): the required encoding
/// ID for the builtin locale, or `-1` if any encoding is valid.
pub fn builtin_locale_encoding(locale: &str) -> PgResult<i32> {
    if locale == "C" {
        Ok(-1)
    } else if locale == "C.UTF-8" {
        Ok(PG_UTF8)
    } else if locale == "PG_UNICODE_FAST" {
        Ok(PG_UTF8)
    } else {
        Err(PgError::error(format!(
            "invalid locale name \"{locale}\" for builtin provider"
        ))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE))
    }
}

/// `builtin_validate_locale(encoding, locale)` (`pg_locale.c:1509`): validate the
/// locale+encoding combination and return the canonical form, copied into `mcx`.
pub fn builtin_validate_locale<'mcx>(
    mcx: Mcx<'mcx>,
    encoding: i32,
    locale: &str,
) -> PgResult<PgString<'mcx>> {
    let canonical_name = if locale == "C" {
        Some("C")
    } else if locale == "C.UTF-8" || locale == "C.UTF8" {
        Some("C.UTF-8")
    } else if locale == "PG_UNICODE_FAST" {
        Some("PG_UNICODE_FAST")
    } else {
        None
    };

    let canonical_name = canonical_name.ok_or_else(|| {
        PgError::error(format!(
            "invalid locale name \"{locale}\" for builtin provider"
        ))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
    })?;

    let required_encoding = builtin_locale_encoding(canonical_name)?;
    if required_encoding >= 0 && encoding != required_encoding {
        return Err(PgError::error(format!(
            "encoding \"{}\" does not match locale \"{locale}\"",
            pg_encoding_to_char(encoding)
        ))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
    }

    PgString::from_str_in(canonical_name, mcx)
}

/// `pg_encoding_to_char(encoding)` (wchar.c) — minimal rendering for the
/// `builtin_validate_locale` error message (only PG_UTF8 reaches it in this
/// profile; the encoding-name table owner is mb).
fn pg_encoding_to_char(encoding: i32) -> alloc::string::String {
    if encoding == PG_UTF8 {
        alloc::string::String::from("UTF8")
    } else {
        format!("encoding {encoding}")
    }
}

/// `icu_language_tag(loc_str, elevel)` (`pg_locale.c:1549`): in the ICU-disabled
/// profile this is the `#else` arm — `ereport(ERROR, FEATURE_NOT_SUPPORTED)`.
pub fn icu_language_tag<'mcx>(_mcx: Mcx<'mcx>, _loc_str: &str) -> PgResult<PgString<'mcx>> {
    Err(PgError::error("ICU is not supported in this build")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `icu_validate_locale(loc_str)` (`pg_locale.c:1607`): in the ICU-disabled
/// profile, `ereport(ERROR, FEATURE_NOT_SUPPORTED)`.
pub fn icu_validate_locale(_loc_str: &str) -> PgResult<()> {
    Err(PgError::error("ICU is not supported in this build")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// Install every seam in `backend-utils-adt-pg-locale-seams`.
pub fn init_seams() {
    use backend_utils_adt_pg_locale_seams as seams;

    seams::pg_newlocale_from_collation::set(cache::pg_newlocale_from_collation);
    seams::init_database_collation::set(cache::init_database_collation);
    seams::collation_is_c::set(dispatch::collation_is_c);
    seams::collation_is_deterministic::set(dispatch::collation_is_deterministic);
    seams::pg_strcoll::set(dispatch::pg_strcoll);
    seams::pg_strncoll::set(dispatch::pg_strncoll);
    seams::pg_strxfrm::set(dispatch::pg_strxfrm);
    seams::pg_strxfrm_enabled::set(dispatch::pg_strxfrm_enabled);
    seams::pg_strxfrm_prefix::set(dispatch::pg_strxfrm_prefix);
    seams::pg_strxfrm_prefix_enabled::set(dispatch::pg_strxfrm_prefix_enabled);
    seams::pg_strlower::set(dispatch::pg_strlower);
    seams::pg_strupper::set(dispatch::pg_strupper);
    seams::pg_strtitle::set(dispatch::pg_strtitle);
    seams::pg_strfold::set(dispatch::pg_strfold);
    seams::char_tolower::set(dispatch::char_tolower);
    seams::get_collation_actual_version::set(version::get_collation_actual_version_seam);
    seams::pg_perm_setlocale::set(setup::pg_perm_setlocale);
    seams::set_database_ctype_is_c::set(setup::set_database_ctype_is_c);
    seams::pglc_localeconv::set(localeconv::pglc_localeconv);
    seams::cache_locale_time::set(localeconv::cache_locale_time);
    seams::localized_full_months::set(localeconv::localized_full_months);
    seams::localized_abbrev_months::set(localeconv::localized_abbrev_months);
    seams::localized_full_days::set(localeconv::localized_full_days);
    seams::localized_abbrev_days::set(localeconv::localized_abbrev_days);

    // The `pg_wc_*` probe seams (regc_pg_locale.c) reach the locale's provider
    // `info` union, which this crate's permanent cache owns; the regex engine
    // keeps only the C-strategy hard-wired. Install the non-C-strategy legs here
    // (libc bound to OS FFI, builtin/ICU delegated to their owners).
    seams::regex_wc_isclass::set(regex_locale::regex_wc_isclass);
    seams::regex_wc_toupper::set(regex_locale::regex_wc_toupper);
    seams::regex_wc_tolower::set(regex_locale::regex_wc_tolower);
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_core::catalog::C_COLLATION_OID;
    use types_locale::CollProvider;

    #[test]
    fn builtin_locale_encoding_matches_c() {
        assert_eq!(builtin_locale_encoding("C").unwrap(), -1);
        assert_eq!(builtin_locale_encoding("C.UTF-8").unwrap(), PG_UTF8);
        assert_eq!(builtin_locale_encoding("PG_UNICODE_FAST").unwrap(), PG_UTF8);
        let err = builtin_locale_encoding("en_US").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    }

    #[test]
    fn builtin_validate_locale_canonicalizes() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        assert_eq!(
            builtin_validate_locale(mcx, PG_UTF8, "C.UTF8").unwrap().as_str(),
            "C.UTF-8"
        );
        assert_eq!(
            builtin_validate_locale(mcx, -1, "C").unwrap().as_str(),
            "C"
        );
        // Encoding mismatch for a UTF8-required locale.
        let err = builtin_validate_locale(mcx, 0 /* PG_SQL_ASCII */, "C.UTF-8").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
        // Invalid locale.
        let err = builtin_validate_locale(mcx, PG_UTF8, "bogus").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    }

    #[test]
    fn icu_paths_report_feature_not_supported() {
        let ctx = mcx::MemoryContext::new("t");
        assert_eq!(
            icu_language_tag(ctx.mcx(), "en-US").unwrap_err().sqlstate(),
            ERRCODE_FEATURE_NOT_SUPPORTED
        );
        assert_eq!(
            icu_validate_locale("en-US").unwrap_err().sqlstate(),
            ERRCODE_FEATURE_NOT_SUPPORTED
        );
    }

    #[test]
    fn c_collation_resolves_without_catalog() {
        // C: pg_newlocale_from_collation(C_COLLATION_OID) returns &c_locale.
        let ctx = mcx::MemoryContext::new("t");
        let loc = pg_newlocale_from_collation(ctx.mcx(), C_COLLATION_OID).unwrap();
        assert!(loc.collate_is_c);
        assert!(loc.ctype_is_c);
        assert!(loc.deterministic);
        assert!(!loc.is_default);
        assert_eq!(loc.provider, CollProvider::Libc);

        // The collid-keyed flag accessors agree.
        assert!(collation_is_c(C_COLLATION_OID).unwrap());
        assert!(collation_is_deterministic(C_COLLATION_OID).unwrap());
    }

    #[test]
    fn invalid_oid_reports_cache_lookup_failed() {
        let err = collation_is_c(types_core::primitive::InvalidOid).unwrap_err();
        assert_eq!(err.message(), "cache lookup failed for collation 0");
    }

    #[test]
    fn c_locale_xfrm_dispatch_is_unsupported() {
        // A collate_is_c locale (the bare C locale) has no collate vtable: the
        // collation family surfaces the support error, not a null deref.
        let err = pg_strncoll(C_COLLATION_OID, b"a", b"b").unwrap_err();
        assert!(err
            .message()
            .starts_with("unsupported collprovider for pg_strncoll"));
        assert!(!pg_strxfrm_enabled(C_COLLATION_OID));
        assert!(!pg_strxfrm_prefix_enabled(C_COLLATION_OID));
    }

    #[test]
    fn pglc_localeconv_c_locale_is_canonical() {
        // lc_monetary / lc_numeric default to "C": the snapshot is the canonical
        // empty/CHAR_MAX lconv.
        let conv = pglc_localeconv();
        assert!(conv.decimal_point.is_empty());
        assert!(conv.currency_symbol.is_empty());
        assert_eq!(conv.frac_digits, types_cash::CashLconv::c_locale().frac_digits);
    }

    #[test]
    fn cache_locale_time_c_locale_gives_no_names() {
        // lc_time defaults to "C": cache_locale_time succeeds and the localized
        // name arrays are None (DCH uses its built-in English names).
        cache_locale_time().unwrap();
        let ctx = mcx::MemoryContext::new("t");
        assert!(localized_full_months(ctx.mcx()).is_none());
        assert!(localized_abbrev_days(ctx.mcx()).is_none());
    }

    #[test]
    fn database_ctype_flag_round_trips() {
        set_database_ctype_is_c(true);
        assert!(database_ctype_is_c());
        set_database_ctype_is_c(false);
        assert!(!database_ctype_is_c());
    }
}
