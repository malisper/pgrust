//! `AclMode` bits (`nodes/parsenodes.h`), `AclResult` (`utils/acl.h`), and the
//! ACL value vocabulary (`AclItem`, `RoleRecurseType`, `AclMaskHow`) consumed
//! by `utils/adt/acl.c`.

use ::types_core::Oid;

/// `AclMode` (`nodes/parsenodes.h`) — a `uint64` bitmask of privilege bits.
pub type AclMode = u64;

pub const ACL_INSERT: AclMode = 1 << 0;
pub const ACL_SELECT: AclMode = 1 << 1;
pub const ACL_UPDATE: AclMode = 1 << 2;
pub const ACL_DELETE: AclMode = 1 << 3;
pub const ACL_TRUNCATE: AclMode = 1 << 4;
pub const ACL_REFERENCES: AclMode = 1 << 5;
pub const ACL_TRIGGER: AclMode = 1 << 6;
pub const ACL_EXECUTE: AclMode = 1 << 7;
pub const ACL_USAGE: AclMode = 1 << 8;
pub const ACL_CREATE: AclMode = 1 << 9;
pub const ACL_CREATE_TEMP: AclMode = 1 << 10;
pub const ACL_CONNECT: AclMode = 1 << 11;
pub const ACL_SET: AclMode = 1 << 12;
pub const ACL_ALTER_SYSTEM: AclMode = 1 << 13;
pub const ACL_MAINTAIN: AclMode = 1 << 14;
pub const ACL_NO_RIGHTS: AclMode = 0;

/// `ACL_SELECT_FOR_UPDATE` (`nodes/parsenodes.h`) — privilege required for
/// SELECT FOR UPDATE/SHARE (an alias of `ACL_UPDATE`).
pub const ACL_SELECT_FOR_UPDATE: AclMode = ACL_UPDATE;

/// `ACL_ID_PUBLIC` (`utils/acl.h`) — placeholder grantee id for a PUBLIC item.
pub const ACL_ID_PUBLIC: Oid = 0;

/// `ACL_GRANT_OPTION_FOR(privs)` (`utils/acl.h`) — shift privilege bits into
/// the grant-option half of an `AclMode`:
/// `(((AclMode) (privs) & 0xFFFFFFFF) << 32)`.
pub const fn ACL_GRANT_OPTION_FOR(privs: AclMode) -> AclMode {
    (privs & 0xFFFF_FFFF) << 32
}

/// `ACLITEM_ALL_PRIV_BITS` (`utils/acl.h`) — `(AclMode) 0xFFFFFFFF`.
pub const ACLITEM_ALL_PRIV_BITS: AclMode = 0xFFFF_FFFF;
/// `ACLITEM_ALL_GOPTION_BITS` (`utils/acl.h`) — `(AclMode) 0xFFFFFFFF << 32`.
pub const ACLITEM_ALL_GOPTION_BITS: AclMode = 0xFFFF_FFFF << 32;

/// `N_ACL_RIGHTS` (`nodes/parsenodes.h`) — 1 plus the last `1<<x` privilege bit.
pub const N_ACL_RIGHTS: u32 = 15;

/// `ACL_ALL_RIGHTS_STR` (`utils/acl.h`) — the privilege characters, in bit order.
pub const ACL_ALL_RIGHTS_STR: &[u8; 15] = b"arwdDxtXUCTcsAm";

// `ACL_*_CHR` (`utils/acl.h`) — the external privilege character for each bit.
pub const ACL_INSERT_CHR: u8 = b'a';
pub const ACL_SELECT_CHR: u8 = b'r';
pub const ACL_UPDATE_CHR: u8 = b'w';
pub const ACL_DELETE_CHR: u8 = b'd';
pub const ACL_TRUNCATE_CHR: u8 = b'D';
pub const ACL_REFERENCES_CHR: u8 = b'x';
pub const ACL_TRIGGER_CHR: u8 = b't';
pub const ACL_EXECUTE_CHR: u8 = b'X';
pub const ACL_USAGE_CHR: u8 = b'U';
pub const ACL_CREATE_CHR: u8 = b'C';
pub const ACL_CREATE_TEMP_CHR: u8 = b'T';
pub const ACL_CONNECT_CHR: u8 = b'c';
pub const ACL_SET_CHR: u8 = b's';
pub const ACL_ALTER_SYSTEM_CHR: u8 = b'A';
pub const ACL_MAINTAIN_CHR: u8 = b'm';

/// `ACLITEM_GET_PRIVS(item)` (`utils/acl.h`) — the lower 32 privilege bits.
#[inline]
pub fn aclitem_get_privs(item: AclItem) -> AclMode {
    item.ai_privs & 0xFFFF_FFFF
}

/// `ACLITEM_GET_GOPTIONS(item)` (`utils/acl.h`) — the upper 32 grant-option bits.
#[inline]
pub fn aclitem_get_goptions(item: AclItem) -> AclMode {
    (item.ai_privs >> 32) & 0xFFFF_FFFF
}

/// `ACLITEM_GET_RIGHTS(item)` (`utils/acl.h`) — the combined rights field.
#[inline]
pub fn aclitem_get_rights(item: AclItem) -> AclMode {
    item.ai_privs
}

/// `ACLITEM_SET_PRIVS_GOPTIONS(item, privs, goptions)` (`utils/acl.h`).
#[inline]
pub fn aclitem_set_privs_goptions(item: &mut AclItem, privs: AclMode, goptions: AclMode) {
    item.ai_privs = (privs & 0xFFFF_FFFF) | ((goptions & 0xFFFF_FFFF) << 32);
}

/// `ACL_GRANT_OPTION_FOR(privs)` (`utils/acl.h`) —
/// `(((AclMode) (privs) & 0xFFFFFFFF) << 32)`: the grant-option bits
/// corresponding to the given privilege bits.
#[inline]
pub const fn acl_grant_option_for(privs: AclMode) -> AclMode {
    (privs & 0xFFFF_FFFF) << 32
}

/// `AclItem` (`utils/acl.h`). Must be a fixed 16-byte layout on every platform
/// (the size is hardcoded in `pg_type.h`); the upper 32 bits of `ai_privs` are
/// the grant-option bits, the lower 32 the privilege bits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct AclItem {
    /// `ai_grantee` — ID that this item grants privs to.
    pub ai_grantee: Oid,
    /// `ai_grantor` — grantor of privs.
    pub ai_grantor: Oid,
    /// `ai_privs` — privilege bits (lower 32) and grant-option bits (upper 32).
    pub ai_privs: AclMode,
}

/// `RoleRecurseType` (`utils/adt/acl.c`) — selects which grant edges
/// `roles_is_member_of()` follows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum RoleRecurseType {
    /// `ROLERECURSE_MEMBERS` — recurse unconditionally.
    RolerecurseMembers = 0,
    /// `ROLERECURSE_PRIVS` — recurse through inheritable grants.
    RolerecursePrivs = 1,
    /// `ROLERECURSE_SETROLE` — recurse through grants with `set_option`.
    RolerecurseSetrole = 2,
}

pub use RoleRecurseType::{
    RolerecurseMembers as ROLERECURSE_MEMBERS, RolerecursePrivs as ROLERECURSE_PRIVS,
    RolerecurseSetrole as ROLERECURSE_SETROLE,
};

/// `AclMaskHow` (`utils/acl.h`) — how `aclmask()` should compute its result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum AclMaskHow {
    /// `ACLMASK_ALL` — normal case: compute all bits.
    AclmaskAll = 0,
    /// `ACLMASK_ANY` — return when result is known nonzero.
    AclmaskAny = 1,
}

pub use AclMaskHow::{AclmaskAll as ACLMASK_ALL, AclmaskAny as ACLMASK_ANY};

/// `AclResult` (`utils/acl.h`) — outcome of an ACL check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum AclResult {
    /// `ACLCHECK_OK`
    AclcheckOk = 0,
    /// `ACLCHECK_NO_PRIV`
    AclcheckNoPriv = 1,
    /// `ACLCHECK_NOT_OWNER`
    AclcheckNotOwner = 2,
}

pub use AclResult::{
    AclcheckNoPriv as ACLCHECK_NO_PRIV, AclcheckNotOwner as ACLCHECK_NOT_OWNER,
    AclcheckOk as ACLCHECK_OK,
};

/// `CheckEnableRlsResult` (`utils/rls.h`) — outcome of `check_enable_rls`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum CheckEnableRlsResult {
    /// `RLS_NONE` — RLS is not enabled for this query.
    RlsNone = 0,
    /// `RLS_NONE_ENV` — RLS disabled now, but could enable if env changes.
    RlsNoneEnv = 1,
    /// `RLS_ENABLED` — RLS applies; row-security quals must be added.
    RlsEnabled = 2,
}

pub use CheckEnableRlsResult::{
    RlsEnabled as RLS_ENABLED, RlsNone as RLS_NONE, RlsNoneEnv as RLS_NONE_ENV,
};
