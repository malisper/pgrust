//! Search-key currency for `SearchSysCache*` / `SearchCatCache*`.
//!
//! In C every cache key is a bare `Datum`; pass-by-reference key types
//! (`name`, `cstring`, `text`) travel as pointers inside the word. The owned
//! model cannot smuggle pointers through `Datum`, so a key crosses as this
//! enum: by-value scalars keep the word, by-reference keys carry their bytes.

// Bare-word machine-word `Datum` (`types_datum::Datum`), aliased `ScalarWord`.
// A by-value system-cache search key is the bare machine word C passes as
// `Datum key1..key4` (`ObjectIdGetDatum`, `Int16GetDatum`, ...); it carries no
// deformed value (by-reference keys travel as `Str`/`Bytes` here), so it stays
// the audited bare word rather than the canonical `Datum<'mcx>` enum.
use types_datum::Datum as ScalarWord;
use mcx::PgVec;
use types_core::primitive::{AttrNumber, Oid as OidT};

/// Projection of the `pg_index` row (`catalog/pg_index.h`) the relcache's
/// `RelationInitIndexAccessInfo` consumes off `SearchSysCache1(INDEXRELID)`.
///
/// In C, relcache `heap_copytuple`s the whole `pg_index` tuple into
/// `rd_indextuple` and reads the variable-length `indkey`/`indcollation`/
/// `indclass`/`indoption` arrays out of it with `fastgetattr` (they sit after
/// the variable-width `indkey`, so the fixed `Form_pg_index` C struct can't
/// reach them). This projection carries the fixed `Form_pg_index` scalar
/// fields plus those four vararrays, copied into the caller's `mcx`.
#[derive(Debug)]
pub struct PgIndexInfo<'mcx> {
    pub indexrelid: OidT,
    pub indrelid: OidT,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    /// `int2vector indkey` — table column numbers of the index columns.
    pub indkey: PgVec<'mcx, AttrNumber>,
    /// `oidvector indcollation` — per key-column collation OIDs.
    pub indcollation: PgVec<'mcx, OidT>,
    /// `oidvector indclass` — per key-column opclass OIDs.
    pub indclass: PgVec<'mcx, OidT>,
    /// `int2vector indoption` — per key-column AM option flags.
    pub indoption: PgVec<'mcx, i16>,
}

/// One search key (`Datum key1..key4` of `SearchSysCache`).
#[derive(Clone, Copy, Debug)]
pub enum SysCacheKey<'a> {
    /// Pass-by-value key: the `Datum` word itself (`ObjectIdGetDatum`,
    /// `Int16GetDatum`, `CharGetDatum`, ...).
    Value(ScalarWord),
    /// NUL-free string key (`CStringGetDatum(name)` for `name`/`cstring` key
    /// columns, `CStringGetTextDatum` for `text` ones).
    Str(&'a str),
    /// Raw by-reference key payload for anything not covered by [`Str`]
    /// (verbatim on-disk bytes, including any varlena header).
    ///
    /// [`Str`]: SysCacheKey::Str
    Bytes(&'a [u8]),
}

impl SysCacheKey<'_> {
    /// The C `0` placeholder for an unused key slot.
    pub const UNUSED: SysCacheKey<'static> = SysCacheKey::Value(ScalarWord::null());
}

impl Default for SysCacheKey<'_> {
    fn default() -> Self {
        Self::UNUSED
    }
}

impl From<ScalarWord> for SysCacheKey<'static> {
    fn from(d: ScalarWord) -> Self {
        SysCacheKey::Value(d)
    }
}

/// Projection of the `pg_authid` row fields that role-identity consumers
/// (`miscinit.c` `has_rolreplication`/`InitializeSessionUserId`,
/// `superuser.c`) read off `SearchSysCache1(AUTHOID/AUTHNAME)` ->
/// `Form_pg_authid` (`catalog/pg_authid.h`). The role name is copied into the
/// caller's `mcx` (`pstrdup`/`NameStr`), so it carries `'mcx`.
#[derive(Debug)]
pub struct AuthIdRow<'mcx> {
    /// `oid` — the role's OID (`rform->oid`).
    pub oid: types_core::Oid,
    /// `rolname` (`NameStr(rform->rolname)`).
    pub rolname: mcx::PgString<'mcx>,
    /// `rolsuper` — has superuser privilege.
    pub rolsuper: bool,
    /// `rolcanlogin` — role can log in.
    pub rolcanlogin: bool,
    /// `rolreplication` — role has explicit REPLICATION privilege.
    pub rolreplication: bool,
    /// `rolbypassrls` — role bypasses row-level security
    /// (`has_bypassrls_privilege`, acl.c).
    pub rolbypassrls: bool,
    /// `rolconnlimit` — per-role connection limit (`-1` means no limit).
    pub rolconnlimit: i32,
}

/// Projection of one `pg_auth_members` row (`catalog/pg_auth_members.h`)
/// as read by `roles_is_member_of` (`utils/adt/acl.c`) off the
/// `SearchSysCacheList1(AUTHMEMMEMROLE, member)` catlist member tuples.
#[derive(Clone, Copy, Debug)]
pub struct AuthMembersRow {
    /// `roleid` (`Form_pg_auth_members->roleid`) — the role the member
    /// belongs to.
    pub roleid: types_core::Oid,
    /// `admin_option` — the grant carries WITH ADMIN OPTION.
    pub admin_option: bool,
    /// `inherit_option` — the grant is inherited (`WITH INHERIT TRUE`).
    pub inherit_option: bool,
    /// `set_option` — the grant permits `SET ROLE` (`WITH SET TRUE`).
    pub set_option: bool,
}

/// Projection of one `pg_foreign_data_wrapper` row
/// (`catalog/pg_foreign_data_wrapper.h`) as `GetForeignDataWrapperExtended`
/// (`foreign/foreign.c`) reads it off `SearchSysCache1(FOREIGNDATAWRAPPEROID)`
/// -> `Form_pg_foreign_data_wrapper`. The name is copied into the caller's
/// `mcx` (`pstrdup`/`NameStr`), so it carries `'mcx`. The `fdwoptions` text[]
/// column is *not* projected here — `foreign.c`'s callers (`foreigncmds.c`,
/// `nodeForeignscan.c`) read only these scalar fields off the descriptor.
#[derive(Debug)]
pub struct ForeignDataWrapperFormRow<'mcx> {
    /// `fdwname` — name of the FDW (`NameStr(fdwform->fdwname)`).
    pub fdwname: mcx::PgString<'mcx>,
    /// `fdwowner` — owning role OID.
    pub fdwowner: types_core::Oid,
    /// `fdwhandler` — OID of the handler function, or `InvalidOid`.
    pub fdwhandler: types_core::Oid,
    /// `fdwvalidator` — OID of the validator function, or `InvalidOid`.
    pub fdwvalidator: types_core::Oid,
}

/// Projection of one `pg_foreign_server` row (`catalog/pg_foreign_server.h`)
/// as `GetForeignServerExtended` (`foreign/foreign.c`) reads it off
/// `SearchSysCache1(FOREIGNSERVEROID)` -> `Form_pg_foreign_server`. The name
/// is copied into the caller's `mcx`. The `srvtype`/`srvversion`/`srvoptions`
/// columns are not projected — `foreign.c`'s callers read only these scalars.
#[derive(Debug)]
pub struct ForeignServerFormRow<'mcx> {
    /// `srvname` — name of the server (`NameStr(serverform->srvname)`).
    pub srvname: mcx::PgString<'mcx>,
    /// `srvowner` — owning role OID.
    pub srvowner: types_core::Oid,
    /// `srvfdw` — the server's foreign-data wrapper OID.
    pub srvfdw: types_core::Oid,
}

/* ---------------------------------------------------------------------------
 * ACL/owner catalog-row projections (the aclmask/aclcheck family in
 * catalog/aclchk.c). Each `*_owner_acl` projection returns the object's owner
 * plus the decoded `aclitem[]` ACL the C `aclmask()` consumes — `Some(items)`
 * for a present ACL, `None` for the SQL-null column (where aclchk builds the
 * hardwired default via `acldefault`). The C reads these off SearchSysCache1
 * + GETSTRUCT (owner) + SysCacheGetAttr (the `aclitem[]` column, detoasted via
 * `DatumGetAclP`). `aclmask` takes `&[AclItem]` in this port, so the ACL is
 * carried as its decoded item vector (the `Acl *` / `ArrayType` payload), not
 * an opaque byte blob.
 * ------------------------------------------------------------------------- */

use types_acl::AclItem;

/// `Form_pg_class` ACL/owner fields the table aclmask path reads
/// (`pg_class_aclmask_ext`, aclchk.c). `relowner` + `relkind` drive aclchk's
/// system-catalog-deny and `acldefault(OBJECT_SEQUENCE|OBJECT_TABLE, ...)`
/// branch; `relnamespace` lets aclchk compute `IsSystemClass` (via
/// catalog.c's `IsToastClass`/`IsCatalogRelationOid`). `acl` is the decoded
/// `relacl` (`None` = SQL null -> build default).
#[derive(Debug)]
pub struct ClassOwnerAcl<'mcx> {
    /// `relowner` (`Form_pg_class.relowner`).
    pub relowner: OidT,
    /// `relkind` (`Form_pg_class.relkind`).
    pub relkind: i8,
    /// `relnamespace` (`Form_pg_class.relnamespace`) — for `IsToastClass`.
    pub relnamespace: OidT,
    /// `relacl` decoded to its `aclitem[]` items, or `None` for SQL null.
    pub acl: Option<PgVec<'mcx, AclItem>>,
}

/// `Form_pg_namespace` ACL/owner fields (`pg_namespace_aclmask_ext`, aclchk.c).
#[derive(Debug)]
pub struct NamespaceOwnerAcl<'mcx> {
    /// `nspowner` (`Form_pg_namespace.nspowner`).
    pub nspowner: OidT,
    /// `nspacl` decoded to its `aclitem[]` items, or `None` for SQL null.
    pub acl: Option<PgVec<'mcx, AclItem>>,
}

/// `Form_pg_type` ACL/owner fields (`pg_type_aclmask_ext`, aclchk.c), after the
/// true-array-element and multirange redirects have been resolved (so `owner`
/// and `acl` are the *effective* type's). `typtype`/`typelem`/`typsubscript`
/// are not surfaced: the redirect is performed inside the projection so the
/// caller sees one resolved `(owner, acl)`.
#[derive(Debug)]
pub struct TypeOwnerAcl<'mcx> {
    /// `typowner` of the effective type (`Form_pg_type.typowner`).
    pub typowner: OidT,
    /// `typacl` of the effective type decoded to `aclitem[]`, or `None`.
    pub acl: Option<PgVec<'mcx, AclItem>>,
}

/// A generic catalog object's owner + ACL (`object_aclmask_ext`, aclchk.c),
/// projected off `SearchSysCache1(cacheid, objectid)` using the owner/acl
/// attribute numbers `get_object_attnum_owner`/`get_object_attnum_acl` resolve
/// for `classid`. `owner` is the `DatumGetObjectId(SysCacheGetAttrNotNull(...))`
/// result; `acl` is the decoded `aclitem[]` column, `None` for SQL null.
#[derive(Debug)]
pub struct ObjectOwnerAcl<'mcx> {
    /// The object's owning-role OID.
    pub owner: OidT,
    /// The object's ACL decoded to `aclitem[]`, or `None` for SQL null.
    pub acl: Option<PgVec<'mcx, AclItem>>,
}
