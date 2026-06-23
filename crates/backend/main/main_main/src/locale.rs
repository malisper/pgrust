//! `init_locale` from `main.c`.

use ::pg_locale_seams::{pg_perm_setlocale, LcCategory};
use ::mcx::Mcx;
use ::types_error::{PgError, PgResult, FATAL};

/// `init_locale(categoryname, category, locale)` (main.c): make the initial
/// permanent setting for a locale category. If that fails (e.g. an invalid
/// `LC_foo` in the environment), fall back to locale `"C"`. If even that fails
/// (e.g. out of memory) the entire startup fails with an `elog(FATAL)`. On
/// return we are guaranteed to have a setting for the category.
pub fn init_locale<'mcx>(
    mcx: Mcx<'mcx>,
    categoryname: &str,
    category: LcCategory,
    locale: &str,
) -> PgResult<()> {
    if pg_perm_setlocale::call(mcx, category, locale)?.is_none()
        && pg_perm_setlocale::call(mcx, category, "C")?.is_none()
    {
        return Err(PgError::new(
            FATAL,
            format!("could not adopt \"{locale}\" locale nor C locale for {categoryname}"),
        ));
    }

    Ok(())
}
