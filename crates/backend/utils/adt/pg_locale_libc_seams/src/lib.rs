//! Case-mapping seam declarations for `backend-utils-adt-pg-locale-libc`
//! (`utils/adt/pg_locale_libc.c`).
//!
//! `pg_locale.c`'s `pg_strlower`/`pg_strtitle`/`pg_strupper` (and `pg_strfold`,
//! which for libc forwards to `strlower_libc`) dispatch into the libc provider
//! when the resolved locale is `COLLPROVIDER_LIBC`. The libc workers
//! (`strlower_libc`/…) need the locale's `info.lt` (`locale_t`) and the database
//! encoding — they belong to the libc unit, which is not yet ported, so the
//! calls cross here and panic until it lands.
//!
//! The flag-core [`PgLocaleStruct`] does not carry the libc `info.lt`
//! (`locale_t`), so the seams are keyed by collation OID: the libc owner
//! re-resolves `info.lt` from `collid` through `pg_locale.c`'s permanent cache,
//! exactly as the collation family (`pg_strncoll`/`char_tolower`) already does.
//! This makes the case family symmetric with the comparison family. For the
//! repo-faithful dispatch wrappers in `pg_locale.c` these are seam-and-panic
//! pending that owner.

use ::mcx::{Mcx, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `strlower_libc(dst, dstsize, src, srclen, locale)`
    /// (`pg_locale_libc.c:123`): lowercase `src` under the libc locale resolved
    /// from `collid` (single-byte `tolower_l`/`pg_tolower`, or the wchar
    /// `towlower_l` path for a multibyte database encoding). Returns the folded
    /// bytes (no trailing NUL), in `mcx`.
    pub fn strlower_libc<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strtitle_libc(...)` (`pg_locale_libc.c:133`): titlecase `src` under the
    /// libc locale resolved from `collid`. See [`strlower_libc`].
    pub fn strtitle_libc<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `strupper_libc(...)` (`pg_locale_libc.c:143`): uppercase `src` under the
    /// libc locale resolved from `collid`. See [`strlower_libc`].
    pub fn strupper_libc<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        collid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);
