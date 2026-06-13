//! Seam declarations for `utils/adt/pg_locale.c` — locale handling.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::{Oid, PgWChar};
use types_error::PgResult;
use types_locale::PgLocale;

/// The `pg_wc_is*` probe family selector (`regc_pg_locale.c`). Identifies which
/// libc/ICU/builtin-Unicode ctype predicate the locale owner must evaluate for a
/// non-C-locale regex collation. The C-locale strategy is handled inside the
/// regex engine (hard-wired table) and never crosses this seam, and so is absent
/// here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegexWcClass {
    /// `pg_wc_isdigit`
    Digit,
    /// `pg_wc_isalpha`
    Alpha,
    /// `pg_wc_isalnum`
    Alnum,
    /// `pg_wc_isupper`
    Upper,
    /// `pg_wc_islower`
    Lower,
    /// `pg_wc_isgraph`
    Graph,
    /// `pg_wc_isprint`
    Print,
    /// `pg_wc_ispunct`
    Punct,
    /// `pg_wc_isspace`
    Space,
}

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

seam_core::seam!(
    /// `pg_newlocale_from_collation(Oid collid)` (pg_locale.c): resolve a
    /// collation OID to its `pg_locale_t`, building and caching the entry on
    /// first use. The regex engine calls this from `pg_set_regex_collation` for
    /// every non-`C_COLLATION_OID` collation to learn the locale's flag core
    /// (`provider`/`deterministic`/`ctype_is_c`/`is_default`) it needs to pick a
    /// classification strategy. `Err` carries its catalog-read / `ereport(ERROR)`
    /// failure surface (e.g. a dropped collation). The returned value is the flag
    /// core ([`PgLocaleStruct`]) copied into `mcx`; the provider-specific `info`
    /// union stays inside pg_locale.c's permanent cache, reached later by OID via
    /// the probe seams below. C returns a pointer into that permanent cache.
    pub fn pg_newlocale_from_collation<'mcx>(
        mcx: Mcx<'mcx>,
        collid: Oid,
    ) -> PgResult<PgLocale<'mcx>>
);

seam_core::seam!(
    /// `pg_newlocale_from_collation(collid)->collate_is_c` (pg_locale.c): whether
    /// the collation's `LC_COLLATE` is the C/POSIX locale. The C `varstr_cmp`
    /// reads this off the resolved `pg_locale_t`; the comparison family has no
    /// `Mcx` to materialize the full [`PgLocale`] handle, so this seam exposes the
    /// single flag keyed by OID against pg_locale.c's permanent cache. `Err`
    /// carries the catalog-read / `ereport(ERROR)` surface of resolving the
    /// collation (e.g. a dropped collation).
    pub fn collation_is_c(collid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `pg_newlocale_from_collation(collid)->deterministic` (pg_locale.c): whether
    /// the collation is deterministic (no equal-but-distinct byte sequences).
    /// Read off the resolved `pg_locale_t` by `texteq`/`textne`/`text_starts_with`
    /// /`btvarstrequalimage`; exposed as the single flag keyed by OID for the same
    /// reason as [`collation_is_c`]. `Err` carries the resolve failure surface.
    pub fn collation_is_deterministic(collid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `pg_strncoll(arg1, len1, arg2, len2, locale)` (pg_locale.c): collation-aware
    /// 3-way comparison of two byte runs under the (non-C) collation `collid`,
    /// returning the libc/ICU `strcoll`-style sign. The lengths are implicit in the
    /// slice lengths. C resolves `locale` from `collid` via the permanent cache;
    /// this seam takes the OID and lets pg_locale.c reach its own cache. `Err`
    /// carries the provider's `ereport(ERROR)` surface (e.g. encoding conversion
    /// failure) plus the collation-resolve surface.
    pub fn pg_strncoll(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `pg_wc_is*` (regc_pg_locale.c): evaluate one ctype predicate of the
    /// `class` family for wide character `c` under the active non-C-locale
    /// regex `collation`. The regex engine owns the strategy selection and the
    /// C-locale hard-wired table; this seam covers only the BUILTIN/LIBC/ICU
    /// paths, which reach into the locale's provider-specific `info` union (libc
    /// `locale_t`, builtin Unicode tables, ICU `uchar.h`) held in pg_locale.c's
    /// permanent cache (keyed by `collation`). Returns the C truth value as a
    /// `bool`.
    pub fn regex_wc_isclass(collation: Oid, class: RegexWcClass, c: PgWChar) -> bool
);

seam_core::seam!(
    /// `pg_wc_toupper` (regc_pg_locale.c): upper-case `c` under the active
    /// non-C-locale regex `collation` (BUILTIN/LIBC/ICU paths only). The regex
    /// engine handles the C-locale path and the LIBC `is_default` ASCII-forcing;
    /// everything else reaches the provider `info` union owned by pg_locale.c.
    pub fn regex_wc_toupper(collation: Oid, c: PgWChar) -> PgWChar
);

seam_core::seam!(
    /// `pg_wc_tolower` (regc_pg_locale.c): lower-case `c` under the active
    /// non-C-locale regex `collation` (BUILTIN/LIBC/ICU paths only). The regex
    /// engine handles the C-locale path and the LIBC `is_default` ASCII-forcing;
    /// everything else reaches the provider `info` union owned by pg_locale.c.
    pub fn regex_wc_tolower(collation: Oid, c: PgWChar) -> PgWChar
);
