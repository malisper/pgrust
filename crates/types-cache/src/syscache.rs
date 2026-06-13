//! Search-key currency for `SearchSysCache*` / `SearchCatCache*`.
//!
//! In C every cache key is a bare `Datum`; pass-by-reference key types
//! (`name`, `cstring`, `text`) travel as pointers inside the word. The owned
//! model cannot smuggle pointers through `Datum`, so a key crosses as this
//! enum: by-value scalars keep the word, by-reference keys carry their bytes.

use types_datum::Datum;

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
