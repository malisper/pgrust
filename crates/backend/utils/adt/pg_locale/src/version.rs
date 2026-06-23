//! `get_collation_actual_version()` (`pg_locale.c:1253`): the OS/library
//! collation version string for the given provider+locale, feeding the
//! version-mismatch WARNING in `create_pg_locale` and the
//! `pg_collation_actual_version` SQL function.

extern crate alloc;

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;

const COLLPROVIDER_BUILTIN: i8 = b'b' as i8;
const COLLPROVIDER_ICU: i8 = b'i' as i8;
const COLLPROVIDER_LIBC: i8 = b'c' as i8;

/// `get_collation_actual_version(collprovider, collcollate)` — internal form
/// returning the version bytes (used by `create_pg_locale`'s version check).
///
/// * `COLLPROVIDER_BUILTIN` -> `get_collation_actual_version_builtin` (the
///   builtin owner seam; static `"1"` for supported builtin locales);
/// * `COLLPROVIDER_ICU` -> compiled only under `USE_ICU`; without it the C call
///   falls through to NULL, and the ICU-disabled crate exposes no version
///   primitive, so this returns `None`;
/// * `COLLPROVIDER_LIBC` -> `get_collation_actual_version_libc` (glibc version,
///   `None` for C/POSIX or off-glibc);
/// * anything else -> `None` (C leaves `collversion` NULL).
pub fn get_collation_actual_version(
    collprovider: i8,
    collcollate: &str,
) -> PgResult<Option<alloc::string::String>> {
    let ctx = mcx::MemoryContext::new("get_collation_actual_version");
    let mcx = ctx.mcx();

    if collprovider == COLLPROVIDER_BUILTIN {
        let bytes = pg_locale_builtin_seams::get_collation_actual_version_builtin::call(
            mcx,
            collcollate,
        )?;
        return Ok(Some(bytes_to_string(&bytes)));
    }

    if collprovider == COLLPROVIDER_ICU {
        let _ = collcollate;
        return Ok(None);
    }

    if collprovider == COLLPROVIDER_LIBC {
        return Ok(
            crate::libc_provider::get_collation_actual_version_libc(mcx, collcollate)?
                .map(|bytes| bytes_to_string(&bytes)),
        );
    }

    Ok(None)
}

/// `get_collation_actual_version` seam body: the result copied into the caller's
/// `mcx` as a `PgString` (`None` for no version).
pub fn get_collation_actual_version_seam<'mcx>(
    mcx: Mcx<'mcx>,
    collprovider: i8,
    locale: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    match get_collation_actual_version(collprovider, locale)? {
        Some(s) => Ok(Some(PgString::from_str_in(&s, mcx)?)),
        None => Ok(None),
    }
}

fn bytes_to_string(bytes: &PgVec<'_, u8>) -> alloc::string::String {
    alloc::string::String::from_utf8_lossy(bytes).into_owned()
}
