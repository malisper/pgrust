//! Seam declarations for `utils/adt/pg_locale.c` — locale handling.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

/// The `int category` argument to `pg_perm_setlocale` (`locale.h` `LC_*`),
/// trimmed to the categories postinit passes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LcCategory {
    /// `LC_COLLATE`
    LcCollate,
    /// `LC_CTYPE`
    LcCtype,
}

seam_core::seam!(
    /// `pg_perm_setlocale(category, locale)` (pg_locale.c): set the process
    /// locale permanently for `category`. Returns the canonical locale name
    /// (copied into `mcx`), or `None` when `setlocale()` rejects the locale
    /// (the C NULL return). `Err` carries the OOM surface of the copy.
    pub fn pg_perm_setlocale<'mcx>(
        mcx: Mcx<'mcx>,
        category: LcCategory,
        locale: &str,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `database_ctype_is_c` (pg_locale.c global): set whether the database
    /// ctype is the C/POSIX locale.
    pub fn set_database_ctype_is_c(value: bool)
);

seam_core::seam!(
    /// `init_database_collation()` (pg_locale.c): initialize the default
    /// collation for the database. `Err` carries its catalog-read/`ereport`
    /// failure surface.
    pub fn init_database_collation() -> PgResult<()>
);

seam_core::seam!(
    /// `get_collation_actual_version(collprovider, collcollate)` (pg_locale.c):
    /// the OS-reported version of the collation for `locale` under provider
    /// `collprovider` (`COLLPROVIDER_*` as `char`), or `None` if the provider
    /// reports no version. Copied into `mcx`. `Err` carries the provider's own
    /// `ereport(ERROR)` surface.
    pub fn get_collation_actual_version<'mcx>(
        mcx: Mcx<'mcx>,
        collprovider: i8,
        locale: &str,
    ) -> PgResult<Option<PgString<'mcx>>>
);
