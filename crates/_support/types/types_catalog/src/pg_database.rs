//! `Form_pg_database` (`catalog/pg_database.h`) — the decoded `pg_database`
//! row, field-for-field with `FormData_pg_database`.
//!
//! The fixed-length columns mirror the C struct; the variable-length locale /
//! ACL columns are decoded varlena (`None` for a SQL NULL, matching
//! `SysCacheGetAttr`'s `isnull`). The owning catalog crate
//! (`backend-catalog-pg-database`) decodes a `pg_database` tuple into this owned
//! form (the `heap_copytuple` analog — consumers copy the tuple before
//! releasing the buffer) and re-forms it for the catalog mutators, so a
//! consuming crate never touches the on-disk datum layout.

use ::mcx::PgString;
use ::types_core::primitive::Oid;
use ::types_core::TransactionId;

/// `COLLPROVIDER_LIBC` (`pg_collation_d.h`) — the `char` value `'c'`.
pub const COLLPROVIDER_LIBC: i8 = b'c' as i8;

// ---------------------------------------------------------------------------
// pg_database relation / index OIDs and column numbers (catalog/pg_database_d.h)
// ---------------------------------------------------------------------------

/// `DatabaseRelationId` — pg_database's relation OID (`pg_database.h` CATALOG).
pub const DatabaseRelationId: Oid = 1262;
/// `DatabaseNameIndexId` — `pg_database_datname_index` OID.
pub const DatabaseNameIndexId: Oid = 2671;
/// `DatabaseOidIndexId` — `pg_database_oid_index` OID.
pub const DatabaseOidIndexId: Oid = 2672;

/// `Template0DbOid` (`pg_database.h` `DECLARE_OID_DEFINING_MACRO`).
pub const Template0DbOid: Oid = 4;
/// `PostgresDbOid` (`pg_database.h` `DECLARE_OID_DEFINING_MACRO`).
pub const PostgresDbOid: Oid = 5;

/// `Anum_pg_database_oid` = 1.
pub const Anum_pg_database_oid: i32 = 1;
/// `Anum_pg_database_datname` = 2.
pub const Anum_pg_database_datname: i32 = 2;
/// `Anum_pg_database_datdba` = 3.
pub const Anum_pg_database_datdba: i32 = 3;
/// `Anum_pg_database_encoding` = 4.
pub const Anum_pg_database_encoding: i32 = 4;
/// `Anum_pg_database_datlocprovider` = 5.
pub const Anum_pg_database_datlocprovider: i32 = 5;
/// `Anum_pg_database_datistemplate` = 6.
pub const Anum_pg_database_datistemplate: i32 = 6;
/// `Anum_pg_database_datallowconn` = 7.
pub const Anum_pg_database_datallowconn: i32 = 7;
/// `Anum_pg_database_dathasloginevt` = 8.
pub const Anum_pg_database_dathasloginevt: i32 = 8;
/// `Anum_pg_database_datconnlimit` = 9.
pub const Anum_pg_database_datconnlimit: i32 = 9;
/// `Anum_pg_database_datfrozenxid` = 10.
pub const Anum_pg_database_datfrozenxid: i32 = 10;
/// `Anum_pg_database_datminmxid` = 11.
pub const Anum_pg_database_datminmxid: i32 = 11;
/// `Anum_pg_database_dattablespace` = 12.
pub const Anum_pg_database_dattablespace: i32 = 12;
/// `Anum_pg_database_datcollate` = 13.
pub const Anum_pg_database_datcollate: i32 = 13;
/// `Anum_pg_database_datctype` = 14.
pub const Anum_pg_database_datctype: i32 = 14;
/// `Anum_pg_database_datlocale` = 15.
pub const Anum_pg_database_datlocale: i32 = 15;
/// `Anum_pg_database_daticurules` = 16.
pub const Anum_pg_database_daticurules: i32 = 16;
/// `Anum_pg_database_datcollversion` = 17.
pub const Anum_pg_database_datcollversion: i32 = 17;
/// `Anum_pg_database_datacl` = 18.
pub const Anum_pg_database_datacl: i32 = 18;

/// `Natts_pg_database` = 18.
pub const Natts_pg_database: usize = 18;

// ---------------------------------------------------------------------------
// datconnlimit sentinels (catalog/pg_database.h)
// ---------------------------------------------------------------------------

/// `DATCONNLIMIT_UNLIMITED` (-1) — no limit on connections.
pub const DATCONNLIMIT_UNLIMITED: i32 = -1;
/// `DATCONNLIMIT_INVALID_DB` (-2) — database is being dropped / is invalid.
pub const DATCONNLIMIT_INVALID_DB: i32 = -2;

/// Decoded `pg_database` row, field-for-field with `FormData_pg_database`.
///
/// Fixed-length columns mirror the C struct; the variable-length locale /
/// ACL columns are decoded varlena (`None` for a SQL NULL). `datacl` crosses
/// as its raw external (on-disk, detoasted) varlena bytes — no consumer
/// decodes the `aclitem[]` array contents through this carrier (ACL rewrite
/// goes through `pg_database_aclmask` / `aclnewowner` on the held tuple), so
/// the bytes are kept opaque rather than modeling `Acl` here.
pub struct FormPgDatabase<'mcx> {
    /// `oid` — the database's OID.
    pub oid: Oid,
    /// `datname` — the database name (`NameData`).
    pub datname: PgString<'mcx>,
    /// `datdba` — owner of the database (`pg_authid` OID).
    pub datdba: Oid,
    /// `encoding` — character encoding (`pg_enc` as `int32`).
    pub encoding: i32,
    /// `datlocprovider` — locale provider (`COLLPROVIDER_*`).
    pub datlocprovider: i8,
    /// `datistemplate` — allowed as CREATE DATABASE template?
    pub datistemplate: bool,
    /// `datallowconn` — new connections allowed?
    pub datallowconn: bool,
    /// `dathasloginevt` — database has login event triggers?
    pub dathasloginevt: bool,
    /// `datconnlimit` — max connections (negative = special, see DATCONNLIMIT_*).
    pub datconnlimit: i32,
    /// `datfrozenxid` — all Xids < this are frozen in this DB.
    pub datfrozenxid: TransactionId,
    /// `datminmxid` — all multixacts in the DB are >= this.
    pub datminmxid: TransactionId,
    /// `dattablespace` — default tablespace OID.
    pub dattablespace: Oid,
    /// `datcollate` — LC_COLLATE setting (`BKI_FORCE_NOT_NULL`).
    pub datcollate: PgString<'mcx>,
    /// `datctype` — LC_CTYPE setting (`BKI_FORCE_NOT_NULL`).
    pub datctype: PgString<'mcx>,
    /// `datlocale` — ICU/builtin locale ID (nullable).
    pub datlocale: Option<PgString<'mcx>>,
    /// `daticurules` — ICU collation rules (nullable).
    pub daticurules: Option<PgString<'mcx>>,
    /// `datcollversion` — recorded collation version (nullable).
    pub datcollversion: Option<PgString<'mcx>>,
    /// `datacl` — access permissions: the raw detoasted `aclitem[]` varlena
    /// bytes, or `None` for a SQL NULL (the default for a freshly created DB).
    pub datacl: Option<::mcx::PgVec<'mcx, u8>>,
}

/// `NewDbRecord` — the column values for a freshly created `pg_database` row
/// (the `new_record[]` / `new_record_nulls[]` arrays createdb builds before
/// `heap_form_tuple`). The owner forms the tuple from this against the
/// `pg_database` descriptor. `datacl` is always null at create time, so it is
/// not represented.
pub struct NewDbRecord<'mcx> {
    /// `oid` — the chosen database OID (createdb allocates it before forming).
    pub oid: Oid,
    /// `datname` — the new database name.
    pub datname: PgString<'mcx>,
    /// `datdba` — owner OID.
    pub datdba: Oid,
    /// `encoding` — character encoding.
    pub encoding: i32,
    /// `datlocprovider` — locale provider.
    pub datlocprovider: i8,
    /// `datistemplate` — template flag.
    pub datistemplate: bool,
    /// `datallowconn` — connections-allowed flag.
    pub datallowconn: bool,
    /// `dathasloginevt` — login-event-trigger flag.
    pub dathasloginevt: bool,
    /// `datconnlimit` — connection limit.
    pub datconnlimit: i32,
    /// `datfrozenxid` — frozen Xid.
    pub datfrozenxid: TransactionId,
    /// `datminmxid` — min multixact.
    pub datminmxid: TransactionId,
    /// `dattablespace` — default tablespace OID.
    pub dattablespace: Oid,
    /// `datcollate` — LC_COLLATE.
    pub datcollate: PgString<'mcx>,
    /// `datctype` — LC_CTYPE.
    pub datctype: PgString<'mcx>,
    /// `datlocale` — locale ID (`None` => SQL NULL).
    pub datlocale: Option<PgString<'mcx>>,
    /// `daticurules` — ICU rules (`None` => SQL NULL).
    pub daticurules: Option<PgString<'mcx>>,
    /// `datcollversion` — collation version (`None` => SQL NULL).
    pub datcollversion: Option<PgString<'mcx>>,
}
