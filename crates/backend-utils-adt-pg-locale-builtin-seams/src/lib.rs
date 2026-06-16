//! Seam declarations for `backend-utils-adt-pg-locale-builtin`
//! (`utils/adt/pg_locale_builtin.c`).
//!
//! `pg_locale.c` calls into the builtin provider when resolving a
//! `COLLPROVIDER_BUILTIN` collation and when case-mapping under a builtin
//! locale. A direct dependency would cycle once the builtin unit lands, so the
//! calls cross here; until the builtin unit is ported these panic loudly. The
//! owning crate installs the implementations from its `init_seams()`.

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_locale::{PgLocale, PgLocaleStruct};

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
    /// (`pg_locale_builtin.c`): lowercase `src` using the builtin Unicode case
    /// tables for `locale`. Returns the folded bytes (no trailing NUL), in `mcx`.
    pub fn strlower_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        locale: &PgLocaleStruct,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strtitle_builtin(...)` (`pg_locale_builtin.c`): titlecase `src` using
    /// the builtin Unicode case tables. See [`strlower_builtin`].
    pub fn strtitle_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        locale: &PgLocaleStruct,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strupper_builtin(...)` (`pg_locale_builtin.c`): uppercase `src` using
    /// the builtin Unicode case tables. See [`strlower_builtin`].
    pub fn strupper_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        locale: &PgLocaleStruct,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strfold_builtin(...)` (`pg_locale_builtin.c`): Unicode case-fold `src`
    /// using the builtin tables. See [`strlower_builtin`].
    pub fn strfold_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        locale: &PgLocaleStruct,
    ) -> PgResult<PgVec<'mcx, u8>>
);
