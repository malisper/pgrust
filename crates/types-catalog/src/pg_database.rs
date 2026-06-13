//! `Form_pg_database` (`catalog/pg_database.h`), trimmed to the fields the
//! backend-startup path (postinit.c) reads.
//!
//! postinit.c reads the fixed-length columns via `GETSTRUCT` and the
//! variable-length locale columns via `SysCacheGetAttr*`/`TextDatumGetCString`.
//! The owning catalog-read seam decodes a `pg_database` tuple into this owned
//! form (the `heap_copytuple` analog — postinit copies the tuple before
//! releasing the buffer), so the consuming crate never touches the on-disk
//! datum layout.

use mcx::PgString;
use types_core::primitive::Oid;

/// `COLLPROVIDER_LIBC` (`pg_collation_d.h`) — the `char` value `'c'`.
pub const COLLPROVIDER_LIBC: i8 = b'c' as i8;

/// Decoded `pg_database` row (the columns postinit.c consumes).
///
/// Fixed-length columns mirror `FormData_pg_database`; the locale columns are
/// decoded varlena text (`None` for a SQL NULL, matching the
/// `SysCacheGetAttr`'s `isnull`).
pub struct FormPgDatabase<'mcx> {
    /// `oid` — the database's OID.
    pub oid: Oid,
    /// `datname` — the database name (`NameData`).
    pub datname: PgString<'mcx>,
    /// `encoding` — character encoding (`pg_enc` as `int32`).
    pub encoding: i32,
    /// `datlocprovider` — locale provider (`COLLPROVIDER_*`).
    pub datlocprovider: i8,
    /// `datallowconn` — new connections allowed?
    pub datallowconn: bool,
    /// `dathasloginevt` — database has login event triggers?
    pub dathasloginevt: bool,
    /// `datconnlimit` — max connections (negative = special, see DATCONNLIMIT_*).
    pub datconnlimit: i32,
    /// `dattablespace` — default tablespace OID.
    pub dattablespace: Oid,
    /// `datcollate` — LC_COLLATE setting (`BKI_FORCE_NOT_NULL`).
    pub datcollate: PgString<'mcx>,
    /// `datctype` — LC_CTYPE setting (`BKI_FORCE_NOT_NULL`).
    pub datctype: PgString<'mcx>,
    /// `datlocale` — ICU locale ID (nullable).
    pub datlocale: Option<PgString<'mcx>>,
    /// `datcollversion` — recorded collation version (nullable).
    pub datcollversion: Option<PgString<'mcx>>,
}
