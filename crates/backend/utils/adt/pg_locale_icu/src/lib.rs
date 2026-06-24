//! Port of `src/backend/utils/adt/pg_locale_icu.c` — the ICU collation provider.
//!
//! Most of the file is wrapped in `#ifdef USE_ICU`: the collator construction
//! (`create_pg_locale_icu`/`make_icu_collator`/`pg_ucol_open`), the comparison
//! path (`strncoll_icu`/`strncoll_icu_utf8`), the sort-key path
//! (`strnxfrm_icu`), the actual-version reporter, and the locale helpers
//! (`icu_validate_locale`/`icu_language_tag`/`enumerate_icu_locales`/
//! `get_icu_locale_comment`) that `pg_locale.c` and `collationcmds.c` reach for.
//!
//! With the `with-icu` feature on (PostgreSQL's `--with-icu`), this binds the
//! system ICU (see [`ffi`]) and implements the live `#ifdef USE_ICU` branches.
//! With it off, the only compiled entry point is the `#else` of
//! `create_pg_locale_icu` (`pg_locale_icu.c:211-218`): the
//! `ERRCODE_FEATURE_NOT_SUPPORTED` "ICU is not supported in this build".
//!
//! ## Bounded scope (documented divergence)
//!
//! The compare/sort-key path is implemented for the **UTF-8** server encoding —
//! the encoding the ICU regression suite runs under and pgrust's primary
//! encoding. UTF-8 compare uses `ucol_strcollUTF8` on the raw bytes (C's
//! `strncoll_icu_utf8`); sort keys convert UTF-8 → `UChar` with `u_strFromUTF8`
//! and call `ucol_getSortKey`. A non-UTF-8 server encoding (which would need the
//! `UConverter` path, `ucnv_toUChars`) reports `FEATURE_NOT_SUPPORTED`. ICU
//! `colliculocale`-with-`collicurules` (custom tailoring rules) is likewise not
//! ported; a collation carrying ICU rules reports the unsupported error.

#![allow(clippy::result_large_err)]

extern crate alloc;

#[cfg(feature = "with-icu")]
pub mod ffi;

#[cfg(feature = "with-icu")]
pub mod provider;

use ::mcx::Mcx;
use ::types_core::primitive::Oid;
use ::types_error::{ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use ::locale::PgLocale;

#[cfg(feature = "with-icu")]
pub use provider::IcuLocale;

/// The provider state for an ICU collation that `pg_locale.c`'s cache holds in
/// its `LocaleInfo` union. Opaque to the cache; the compare/sort-key seams hand
/// it back here. With ICU off this is an empty (uninhabitable) carrier.
#[cfg(not(feature = "with-icu"))]
pub enum IcuLocale {}

/// The flag core + provider state `create_pg_locale_icu` produces, for
/// `pg_locale.c`'s cache to adapt into a `LocaleEntry` (mirrors how the builtin
/// provider returns a view + `casemap_full`).
pub struct IcuLocaleResult {
    /// `result->deterministic` (`collisdeterministic`, or `true` for the
    /// database default).
    pub deterministic: bool,
    /// The owned ICU collator + locale string (`info.icu`).
    pub icu: IcuLocale,
}

/// `create_pg_locale_icu(collid, context)` (`pg_locale_icu.c:142`).
///
/// In an ICU build this resolves the collation/database locale, opens a
/// `UCollator`, and returns the provider state. With ICU off the live code is
/// the `#else` arm (lines 211-218): `ERRCODE_FEATURE_NOT_SUPPORTED`.
#[cfg(feature = "with-icu")]
pub fn create_pg_locale_icu(
    iculocstr: &str,
    icurules: Option<&str>,
    deterministic: bool,
) -> PgResult<IcuLocaleResult> {
    let icu = provider::make_icu_locale(iculocstr, icurules)?;
    Ok(IcuLocaleResult { deterministic, icu })
}

/// `create_pg_locale_icu` seam body (the `pg_locale_icu_seams` shape) — only the
/// `#else` arm is reachable in this profile, so it always `Err`s. The cache
/// crate calls [`create_pg_locale_icu`] directly when the feature is on.
pub fn create_pg_locale_icu_seam<'mcx>(_mcx: Mcx<'mcx>, _collid: Oid) -> PgResult<PgLocale<'mcx>> {
    Err(PgError::new(ERROR, "ICU is not supported in this build")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_error_location(ErrorLocation::new(
            "../src/backend/utils/adt/pg_locale_icu.c",
            215,
            "create_pg_locale_icu",
        )))
}

/// Install every seam in `backend-utils-adt-pg-locale-icu-seams`.
pub fn init_seams() {
    pg_locale_icu_seams::create_pg_locale_icu::set(create_pg_locale_icu_seam);
}
