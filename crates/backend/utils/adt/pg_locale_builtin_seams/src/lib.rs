//! Seam declarations for `backend-utils-adt-pg-locale-builtin`
//! (`utils/adt/pg_locale_builtin.c`).
//!
//! `pg_locale.c` calls into the builtin provider when resolving a
//! `COLLPROVIDER_BUILTIN` collation and when case-mapping under a builtin
//! locale. A direct dependency would cycle once the builtin unit lands, so the
//! calls cross here; until the builtin unit is ported these panic loudly. The
//! owning crate installs the implementations from its `init_seams()`.

use mcx::{Mcx, PgVec};
use ::types_core::primitive::{Oid, PgWChar};
use ::types_error::PgResult;
use ::locale::PgLocale;

pub use ::pg_locale_seams::RegexWcClass;

/// The builtin-provider locale `create_pg_locale_builtin` returns: the
/// provider-independent flag core (allocated in the caller's `mcx`) plus the
/// `info.builtin.casemap_full` flag the trimmed [`PgLocale`] flag core does not
/// carry. The consumer stores `casemap_full` in its `LocaleInfo::Builtin` arm,
/// where C keeps the `info` union; it is true only for `PG_UNICODE_FAST`
/// (`pg_locale_builtin.c:160`).
pub struct PgLocaleBuiltinResult<'mcx> {
    /// The flag core (`provider`/`deterministic`/`collate_is_c`/`ctype_is_c`),
    /// allocated in `mcx`.
    pub view: PgLocale<'mcx>,
    /// `info.builtin.casemap_full` — full Unicode case mapping (`PG_UNICODE_FAST`).
    pub casemap_full: bool,
}

seam_core::seam!(
    /// `create_pg_locale_builtin(collid, context)` (`pg_locale_builtin.c`):
    /// build the `pg_locale_t` flag core for a builtin-provider collation
    /// (`C` / `C.UTF-8` / `PG_UNICODE_FAST`), allocated in `mcx`, along with the
    /// `info.builtin.casemap_full` flag.
    pub fn create_pg_locale_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        collid: Oid,
    ) -> PgResult<PgLocaleBuiltinResult<'mcx>>
);

seam_core::seam!(
    /// `get_collation_actual_version_builtin(collcollate)`
    /// (`pg_locale_builtin.c`): the fixed builtin version string (`"1"`) for a
    /// supported builtin locale, copied into `mcx`; `Err` for an unknown name.
    pub fn get_collation_actual_version_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        collcollate: &str,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strlower_builtin(dst, dstsize, src, srclen, locale)`
    /// (`pg_locale_builtin.c:81`): lowercase `src` using the builtin Unicode
    /// case tables, dispatched on `info.builtin.casemap_full` of the locale
    /// resolved from `collid`. Returns the folded bytes (no trailing NUL), in
    /// `mcx`.
    pub fn strlower_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strtitle_builtin(...)` (`pg_locale_builtin.c:89`): titlecase `src` using
    /// the builtin Unicode case tables (resolved from `collid`). See
    /// [`strlower_builtin`].
    pub fn strtitle_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strupper_builtin(...)` (`pg_locale_builtin.c:107`): uppercase `src`
    /// using the builtin Unicode case tables (resolved from `collid`). See
    /// [`strlower_builtin`].
    pub fn strupper_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strfold_builtin(...)` (`pg_locale_builtin.c:115`): Unicode case-fold
    /// `src` using the builtin tables (resolved from `collid`). See
    /// [`strlower_builtin`].
    pub fn strfold_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `pg_wc_is*` BUILTIN strategy (`regc_pg_locale.c`): the builtin Unicode
    /// ctype predicate (`pg_u_isalpha`/…) for wide character `c`. The digit,
    /// alnum, and punct predicates take a `posix` argument (`pg_u_isdigit` etc.,
    /// `regc_pg_locale.c:307/361/505`), which C derives from the locale as
    /// `!info.builtin.casemap_full`; the consumer resolves it from the
    /// collation's `casemap_full` and passes it here.
    pub fn regex_wc_isclass_builtin(class: RegexWcClass, c: PgWChar, posix: bool) -> bool
);

seam_core::seam!(
    /// `pg_wc_toupper` BUILTIN strategy (`regc_pg_locale.c:558`):
    /// `unicode_uppercase_simple(c)`.
    pub fn regex_wc_toupper_builtin(c: PgWChar) -> PgWChar
);

seam_core::seam!(
    /// `pg_wc_tolower` BUILTIN strategy (`regc_pg_locale.c:590`):
    /// `unicode_lowercase_simple(c)`.
    pub fn regex_wc_tolower_builtin(c: PgWChar) -> PgWChar
);
