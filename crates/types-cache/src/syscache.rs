//! Search-key currency for `SearchSysCache*` / `SearchCatCache*`.
//!
//! In C every cache key is a bare `Datum`; pass-by-reference key types
//! (`name`, `cstring`, `text`) travel as pointers inside the word. The owned
//! model cannot smuggle pointers through `Datum`, so a key crosses as this
//! enum: by-value scalars keep the word, by-reference keys carry their bytes.

use types_datum::Datum;
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
    Value(Datum),
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
    pub const UNUSED: SysCacheKey<'static> = SysCacheKey::Value(Datum::null());
}

impl Default for SysCacheKey<'_> {
    fn default() -> Self {
        Self::UNUSED
    }
}

impl From<Datum> for SysCacheKey<'static> {
    fn from(d: Datum) -> Self {
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
