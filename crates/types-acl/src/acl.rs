//! `AclMode` bits (`nodes/parsenodes.h`) and `AclResult` (`utils/acl.h`).

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
