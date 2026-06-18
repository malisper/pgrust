//! Seam declarations for `backend-utils-adt-pg-locale-builtin`
//! (`utils/adt/pg_locale_builtin.c`).
//!
//! `pg_locale.c` calls into the builtin provider when resolving a
//! `COLLPROVIDER_BUILTIN` collation and when case-mapping under a builtin
//! locale. A direct dependency would cycle once the builtin unit lands, so the
//! calls cross here; until the builtin unit is ported these panic loudly. The
//! owning crate installs the implementations from its `init_seams()`.

use mcx::{Mcx, PgVec};
use types_core::primitive::{Oid, PgWChar};
use types_error::PgResult;
use types_locale::PgLocale;

pub use backend_utils_adt_pg_locale_seams::RegexWcClass;

seam_core::seam!(
    /// `create_pg_locale_builtin(collid, context)` (`pg_locale_builtin.c`):
    /// build the `pg_locale_t` flag core for a builtin-provider collation
    /// (`PG_C_UTF8` / `C.UTF-8` / `PG_UNICODE_FAST`), allocated in `mcx`.
    pub fn create_pg_locale_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        collid: Oid,
    ) -> PgResult<PgLocale<'mcx>>
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
    /// ctype predicate (`pg_u_isalpha`/…) for wide character `c`. Locale-
    /// independent (the builtin tables carry no per-collation state), so no
    /// collation/locale argument. Owner: the builtin unit (`unicode_*`).
    pub fn regex_wc_isclass_builtin(class: RegexWcClass, c: PgWChar) -> bool
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
