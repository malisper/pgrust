//! `pg_authid` / `pg_auth_members` catalog-row vocabulary consumed by
//! `commands/user.c` and produced by its catalog seams, plus the
//! `PasswordType` enum (`libpq/crypt.h`).
//!
//! Trimmed to the columns user.c reads/writes: the `*Form` views are the
//! `GETSTRUCT` projections (read by the syscache value seams and returned to
//! user.c by value — no opaque tuple handle, mirroring
//! `SearchSysCache`/`GETSTRUCT`/`ReleaseSysCache` collapsed into one
//! projection), `New*Record` the freshly-assembled tuples, and the `*Update`
//! structs the per-attribute `heap_modify_tuple` deltas.

#![allow(non_camel_case_types)]

use ::types_core::primitive::{Oid, TimestampTz};

/// `typedef enum PasswordType` (`libpq/crypt.h`). Values verified against PG
/// 18.3.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum PasswordType {
    /// `PASSWORD_TYPE_PLAINTEXT = 0`.
    Plaintext = 0,
    /// `PASSWORD_TYPE_MD5`.
    Md5 = 1,
    /// `PASSWORD_TYPE_SCRAM_SHA_256`.
    ScramSha256 = 2,
}

/// `STATUS_OK` (`c.h`): the success return of `plain_crypt_verify`.
pub const STATUS_OK: i32 = 0;

/// The fields of a `pg_authid` row user.c reads off the looked-up tuple
/// (`GETSTRUCT(tuple)` for the fixed columns, `heap_getattr` for the nullable
/// `rolpassword`/`rolvaliduntil`). The value seams project all of these in one
/// pass so user.c never holds a tuple handle.
#[derive(Clone, Debug, Default)]
pub struct AuthIdForm {
    /// `oid`.
    pub oid: Oid,
    /// `rolname`.
    pub rolname: String,
    /// `rolsuper`.
    pub rolsuper: bool,
    /// `rolinherit`.
    pub rolinherit: bool,
    /// `rolpassword` — `Some(text)` or `None` when the column is SQL NULL
    /// (read by `RenameRole` to decide whether to clear an MD5 hash).
    pub rolpassword: Option<String>,
    /// `rolvaliduntil` — `Some(ts)` or `None` when SQL NULL (read by
    /// `AlterRole` to preserve the existing expiry when re-encrypting).
    pub rolvaliduntil: Option<TimestampTz>,
}

/// The columns user.c assembles for a brand-new `pg_authid` tuple.
#[derive(Clone, Debug)]
pub struct NewAuthRecord {
    pub rolname: String,
    pub rolsuper: bool,
    pub rolinherit: bool,
    pub rolcreaterole: bool,
    pub rolcreatedb: bool,
    pub rolcanlogin: bool,
    pub rolreplication: bool,
    pub rolconnlimit: i32,
    /// `Some(hashed)` => the encrypted password text; `None` => SQL NULL.
    pub rolpassword: Option<String>,
    /// VALID UNTIL as a `timestamptz` value; `None` => SQL NULL.
    pub rolvaliduntil: Option<TimestampTz>,
    pub rolbypassrls: bool,
    /// The role OID.
    pub oid: Oid,
}

/// One per-attribute update applied to an existing `pg_authid` tuple.
#[derive(Clone, Debug, Default)]
pub struct AuthIdUpdate {
    pub rolsuper: Option<bool>,
    pub rolinherit: Option<bool>,
    pub rolcreaterole: Option<bool>,
    pub rolcreatedb: Option<bool>,
    pub rolcanlogin: Option<bool>,
    pub rolreplication: Option<bool>,
    pub rolconnlimit: Option<i32>,
    /// `Some(Some(hash))` => set password; `Some(None)` => NULL; `None` => unchanged.
    pub rolpassword: Option<Option<String>>,
    /// `Some(Some(ts))` => set valid-until; `Some(None)` => NULL; `None` => unchanged.
    pub rolvaliduntil: Option<Option<TimestampTz>>,
    pub rolbypassrls: Option<bool>,
}

/// The fields of a `pg_auth_members` row user.c reads via `GETSTRUCT(tuple)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuthMemForm {
    /// `oid`.
    pub oid: Oid,
    /// `roleid`.
    pub roleid: Oid,
    /// `member`.
    pub member: Oid,
    /// `grantor`.
    pub grantor: Oid,
    /// `admin_option`.
    pub admin_option: bool,
    /// `inherit_option`.
    pub inherit_option: bool,
    /// `set_option`.
    pub set_option: bool,
}

/// The columns user.c assembles for a brand-new `pg_auth_members` tuple.
#[derive(Clone, Copy, Debug)]
pub struct NewAuthMemRecord {
    /// The grant OID.
    pub oid: Oid,
    pub roleid: Oid,
    pub member: Oid,
    pub grantor: Oid,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

/// A per-option update applied to an existing `pg_auth_members` tuple.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuthMemUpdate {
    pub admin_option: Option<bool>,
    pub inherit_option: Option<bool>,
    pub set_option: Option<bool>,
}
