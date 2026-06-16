//! Catalog-read seam declarations for `backend-utils-adt-pg-locale`
//! (`utils/adt/pg_locale.c`).
//!
//! `pg_locale.c` reads two syscache rows when building a `pg_locale_t`:
//!
//! * `SearchSysCache1(COLLOID, collid)` in `create_pg_locale` — to learn the
//!   provider, the locale/collate strings, the recorded `collversion`, and the
//!   collation name/namespace (for the version-mismatch WARNING);
//! * `SearchSysCache1(DATABASEOID, MyDatabaseId)` in `init_database_collation`
//!   and the libc default path — to learn the database `datlocprovider`,
//!   `datcollate`, and `datctype`.
//!
//! The syscache owner is not yet ported, so these cross here and panic until it
//! lands. The returned rows are owned (`String`/`Option<String>`); SQL NULL maps
//! to `None`. `provider` is the `char` `collprovider`/`datlocprovider` as `i8`
//! (`COLLPROVIDER_*`).

use types_core::primitive::Oid;
use types_error::PgResult;

extern crate alloc;
use alloc::string::String;

/// The `pg_collation` columns `create_pg_locale` consults
/// (`SearchSysCache1(COLLOID, ...)` + `SysCacheGetAttr(collversion/collcollate/
/// colllocale)` + `NameStr(collname)` + `collnamespace`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollationLocaleRow {
    /// `collprovider` (`COLLPROVIDER_*` as `char`).
    pub provider: i8,
    /// `collname` — the collation name (for the version-mismatch message).
    pub name: String,
    /// `collnamespace` — the collation's schema OID (for qualification).
    pub namespace: Oid,
    /// `collcollate` (libc LC_COLLATE), text or NULL.
    pub collate: Option<String>,
    /// `collctype` (libc LC_CTYPE), text or NULL.
    pub ctype: Option<String>,
    /// `colllocale` (builtin/ICU locale), text or NULL.
    pub locale: Option<String>,
    /// `collversion`, text or NULL.
    pub version: Option<String>,
}

/// The `pg_database` columns the libc default/`init_database_collation` paths
/// consult (`SearchSysCache1(DATABASEOID, MyDatabaseId)` +
/// `SysCacheGetAttrNotNull(datlocprovider/datcollate/datctype)`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseLocaleRow {
    /// `datlocprovider` (`COLLPROVIDER_*` as `char`).
    pub provider: i8,
    /// `datcollate` (libc LC_COLLATE for the database default).
    pub collate: String,
    /// `datctype` (libc LC_CTYPE for the database default).
    pub ctype: String,
}

seam_core::seam!(
    /// `SearchSysCache1(COLLOID, collid)` -> the locale-relevant `pg_collation`
    /// columns. `None` when the row is absent (the "cache lookup failed for
    /// collation" path). `Err` carries catalog-read failures.
    pub fn collation_locale_row(collid: Oid) -> PgResult<Option<CollationLocaleRow>>
);

seam_core::seam!(
    /// `SearchSysCache1(DATABASEOID, MyDatabaseId)` -> the locale-relevant
    /// `pg_database` columns for the current database. `None` when the row is
    /// absent (the "cache lookup failed for database" path). `Err` carries
    /// catalog-read failures.
    pub fn database_locale_row() -> PgResult<Option<DatabaseLocaleRow>>
);

seam_core::seam!(
    /// `MyDatabaseId` — the current database OID (for the "cache lookup failed
    /// for database %u" error message).
    pub fn my_database_id() -> Oid
);

seam_core::seam!(
    /// `get_namespace_name(nspid)` (namespace.c) — the schema name for the
    /// version-mismatch WARNING's `quote_qualified_identifier`. `None` when the
    /// namespace is absent.
    pub fn get_namespace_name(nspid: Oid) -> PgResult<Option<String>>
);
