//! Port of `src/backend/utils/adt/pg_locale_icu.c`.
//!
//! Almost the entire file is wrapped in `#ifdef USE_ICU`: the collator
//! construction, the ICU `UConverter`/`UCollator` plumbing, the case-conversion
//! and sort-key routines, and the locale-attribute parser. The active migration
//! profile builds with **ICU disabled**, so the only function the compiled
//! object contributes is [`create_pg_locale_icu`], and within it only the
//! `#else` branch (`pg_locale_icu.c:211-218`) is live — the c2rust object that
//! replaces `utils_adt_pg_locale_icu.c.o` in a linking Postgres build confirms
//! this is the sole compiled entry point.
//!
//! The ICU branch and all `#ifdef USE_ICU` helpers therefore have no compiled
//! counterpart in this profile and are not ported; if/when ICU is enabled they
//! land with the ICU subsystem (`UCollator`/`UConverter`/`UChar` are external).

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_locale::PgLocale;

/// `create_pg_locale_icu(collid, context)` (`pg_locale_icu.c:142`).
///
/// In an ICU build this resolves the collation (or database default) locale,
/// opens a `UCollator`, and allocates a `pg_locale_t` in `context`. With ICU
/// disabled the live code is the `#else` arm (lines 211-218): a collation may
/// still have been created by an ICU-enabled build and referenced here, so the
/// function reports `ERRCODE_FEATURE_NOT_SUPPORTED` rather than silently
/// succeeding. It never returns a value in this profile.
///
/// `collid` and `mcx` (the C `MemoryContext context`) are part of the
/// PostgreSQL-shaped, allocation-capable entry point; the non-ICU branch never
/// inspects them.
pub fn create_pg_locale_icu<'mcx>(_mcx: Mcx<'mcx>, _collid: Oid) -> PgResult<PgLocale<'mcx>> {
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
    backend_utils_adt_pg_locale_icu_seams::create_pg_locale_icu::set(create_pg_locale_icu);
}
