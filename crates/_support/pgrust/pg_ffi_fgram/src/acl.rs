//! ABI vocabulary for the `aclitem` type (PostgreSQL `src/include/utils/acl.h`
//! and the `AclMode` privilege bits from `src/include/nodes/parsenodes.h`).
//!
//! `AclItem` is an on-disk / `Datum`-carried fixed-size struct: its size is
//! hardcoded in the `pg_type.h` entry for `aclitem`, so it MUST keep the exact
//! C layout on every platform.  `Acl` itself is just an `ArrayType` of
//! `AclItem` (a standard one-dimensional, no-nulls PostgreSQL array), so it is
//! represented through the existing array machinery rather than a dedicated
//! struct here.

use crate::Oid;

/// `typedef uint64 AclMode;` -- a bitmask of privilege bits
/// (`src/include/nodes/parsenodes.h`).  The upper 32 bits are grant-option
/// bits; the lower 32 bits are the actual privileges.
pub type AclMode = u64;

/// `AclItem` -- one access-control list entry.
///
/// Note: must be the same size on all platforms, because the size is hardcoded
/// in the `pg_type.h` entry for `aclitem`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct AclItem {
    /// ID that this item grants privileges to.
    pub ai_grantee: Oid,
    /// Grantor of privileges.
    pub ai_grantor: Oid,
    /// Privilege bits (upper 32 = grant options, lower 32 = privileges).
    pub ai_privs: AclMode,
}

/// Placeholder for id in a PUBLIC acl item (`ACL_ID_PUBLIC`).
pub const ACL_ID_PUBLIC: Oid = 0;

// --- Individual privilege bits (parsenodes.h) ------------------------------

pub const ACL_INSERT: AclMode = 1 << 0; // for relations
pub const ACL_SELECT: AclMode = 1 << 1;
pub const ACL_UPDATE: AclMode = 1 << 2;
pub const ACL_DELETE: AclMode = 1 << 3;
pub const ACL_TRUNCATE: AclMode = 1 << 4;
pub const ACL_REFERENCES: AclMode = 1 << 5;
pub const ACL_TRIGGER: AclMode = 1 << 6;
pub const ACL_EXECUTE: AclMode = 1 << 7; // for functions
pub const ACL_USAGE: AclMode = 1 << 8; // for various object types
pub const ACL_CREATE: AclMode = 1 << 9; // for namespaces and databases
pub const ACL_CREATE_TEMP: AclMode = 1 << 10; // for databases
pub const ACL_CONNECT: AclMode = 1 << 11; // for databases
pub const ACL_SET: AclMode = 1 << 12; // for configuration parameters
pub const ACL_ALTER_SYSTEM: AclMode = 1 << 13; // for configuration parameters
pub const ACL_MAINTAIN: AclMode = 1 << 14; // for relations
pub const N_ACL_RIGHTS: u32 = 15; // 1 plus the last 1<<x

pub const ACL_NO_RIGHTS: AclMode = 0;

/// `ACLITEM_ALL_PRIV_BITS` -- all privilege bits set (lower 32 bits).
pub const ACLITEM_ALL_PRIV_BITS: AclMode = 0xFFFF_FFFF;
/// `ACLITEM_ALL_GOPTION_BITS` -- all grant-option bits set (upper 32 bits).
pub const ACLITEM_ALL_GOPTION_BITS: AclMode = 0xFFFF_FFFF << 32;

/// `ACLITEM_GET_PRIVS(item)` -- the privilege bits of an `AclItem`.
#[inline]
pub const fn aclitem_get_privs(item: AclItem) -> AclMode {
    item.ai_privs & 0xFFFF_FFFF
}

/// `ACLITEM_GET_GOPTIONS(item)` -- the grant-option bits of an `AclItem`.
#[inline]
pub const fn aclitem_get_goptions(item: AclItem) -> AclMode {
    (item.ai_privs >> 32) & 0xFFFF_FFFF
}

/// `ACLITEM_GET_RIGHTS(item)` -- the combined rights field of an `AclItem`.
#[inline]
pub const fn aclitem_get_rights(item: AclItem) -> AclMode {
    item.ai_privs
}

/// `ACL_GRANT_OPTION_FOR(privs)` -- shift privilege bits into grant-option
/// position.
#[inline]
pub const fn acl_grant_option_for(privs: AclMode) -> AclMode {
    (privs & 0xFFFF_FFFF) << 32
}

/// `ACL_OPTION_TO_PRIVS(privs)` -- shift grant-option bits down into privilege
/// position.
#[inline]
pub const fn acl_option_to_privs(privs: AclMode) -> AclMode {
    (privs >> 32) & 0xFFFF_FFFF
}

/// `ACLITEM_SET_PRIVS_GOPTIONS(item, privs, goptions)` -- set both halves of
/// the rights field of an `AclItem` (acl.h).
#[inline]
pub fn aclitem_set_privs_goptions(item: &mut AclItem, privs: AclMode, goptions: AclMode) {
    item.ai_privs = (privs & 0xFFFF_FFFF) | ((goptions & 0xFFFF_FFFF) << 32);
}

/// `ACLITEM_SET_RIGHTS(item, rights)` -- set the combined rights field (acl.h).
#[inline]
pub fn aclitem_set_rights(item: &mut AclItem, rights: AclMode) {
    item.ai_privs = rights & (ACLITEM_ALL_PRIV_BITS | ACLITEM_ALL_GOPTION_BITS);
}

// --- Privilege string characters (acl.h) -----------------------------------

pub const ACL_INSERT_CHR: char = 'a'; // formerly known as "append"
pub const ACL_SELECT_CHR: char = 'r'; // formerly known as "read"
pub const ACL_UPDATE_CHR: char = 'w'; // formerly known as "write"
pub const ACL_DELETE_CHR: char = 'd';
pub const ACL_TRUNCATE_CHR: char = 'D'; // super-delete, as it were
pub const ACL_REFERENCES_CHR: char = 'x';
pub const ACL_TRIGGER_CHR: char = 't';
pub const ACL_EXECUTE_CHR: char = 'X';
pub const ACL_USAGE_CHR: char = 'U';
pub const ACL_CREATE_CHR: char = 'C';
pub const ACL_CREATE_TEMP_CHR: char = 'T';
pub const ACL_CONNECT_CHR: char = 'c';
pub const ACL_SET_CHR: char = 's';
pub const ACL_ALTER_SYSTEM_CHR: char = 'A';
pub const ACL_MAINTAIN_CHR: char = 'm';

/// `ACL_ALL_RIGHTS_STR` -- order corresponds to the bit positions 0..N_ACL_RIGHTS.
pub const ACL_ALL_RIGHTS_STR: &str = "arwdDxtXUCTcsAm";

// --- Per-object-type "all rights" masks (acl.h) -----------------------------

pub const ACL_ALL_RIGHTS_COLUMN: AclMode = ACL_INSERT | ACL_SELECT | ACL_UPDATE | ACL_REFERENCES;
pub const ACL_ALL_RIGHTS_RELATION: AclMode = ACL_INSERT
    | ACL_SELECT
    | ACL_UPDATE
    | ACL_DELETE
    | ACL_TRUNCATE
    | ACL_REFERENCES
    | ACL_TRIGGER
    | ACL_MAINTAIN;
pub const ACL_ALL_RIGHTS_SEQUENCE: AclMode = ACL_USAGE | ACL_SELECT | ACL_UPDATE;
pub const ACL_ALL_RIGHTS_DATABASE: AclMode = ACL_CREATE | ACL_CREATE_TEMP | ACL_CONNECT;
pub const ACL_ALL_RIGHTS_FDW: AclMode = ACL_USAGE;
pub const ACL_ALL_RIGHTS_FOREIGN_SERVER: AclMode = ACL_USAGE;
pub const ACL_ALL_RIGHTS_FUNCTION: AclMode = ACL_EXECUTE;
pub const ACL_ALL_RIGHTS_LANGUAGE: AclMode = ACL_USAGE;
pub const ACL_ALL_RIGHTS_LARGEOBJECT: AclMode = ACL_SELECT | ACL_UPDATE;
pub const ACL_ALL_RIGHTS_PARAMETER_ACL: AclMode = ACL_SET | ACL_ALTER_SYSTEM;
pub const ACL_ALL_RIGHTS_SCHEMA: AclMode = ACL_USAGE | ACL_CREATE;
pub const ACL_ALL_RIGHTS_TABLESPACE: AclMode = ACL_CREATE;
pub const ACL_ALL_RIGHTS_TYPE: AclMode = ACL_USAGE;

// --- Modification type codes for aclupdate (acl.h) --------------------------

pub const ACL_MODECHG_ADD: i32 = 1;
pub const ACL_MODECHG_DEL: i32 = 2;
pub const ACL_MODECHG_EQL: i32 = 3;

/// `AclMaskHow` -- the "how" argument to `aclmask` (acl.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum AclMaskHow {
    /// normal case: compute all bits
    AclmaskAll = 0,
    /// return when result is known nonzero
    AclmaskAny = 1,
}
pub use AclMaskHow::{AclmaskAll as ACLMASK_ALL, AclmaskAny as ACLMASK_ANY};

/// `AclResult` -- result codes for ACL checks (acl.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum AclResult {
    /// no error
    AclcheckOk = 0,
    /// permission denied
    AclcheckNoPriv = 1,
    /// must be owner of object
    AclcheckNotOwner = 2,
}
pub use AclResult::{
    AclcheckNoPriv as ACLCHECK_NO_PRIV, AclcheckNotOwner as ACLCHECK_NOT_OWNER,
    AclcheckOk as ACLCHECK_OK,
};

/// `aclitem` type OID (`pg_type.dat`: oid => '1033').
pub const ACLITEMOID: Oid = 1033;

/// `pg_database_owner` role OID (`pg_authid.dat`: ROLE_PG_DATABASE_OWNER).
pub const ROLE_PG_DATABASE_OWNER: Oid = 6171;

// The catalog relation OIDs used as `object_aclcheck` classids
// (`AUTH_ID_RELATION_ID`, `DATABASE_RELATION_ID`, `NAMESPACE_RELATION_ID`,
// `PROCEDURE_RELATION_ID`, `RELATION_RELATION_ID`, `TYPE_RELATION_ID`, and the
// foreign/language/tablespace ones) live in `crate::catalog`.

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn aclitem_layout_matches_postgres() {
        assert_eq!(size_of::<AclItem>(), 16);
        assert_eq!(align_of::<AclItem>(), 8);
        assert_eq!(offset_of!(AclItem, ai_grantee), 0);
        assert_eq!(offset_of!(AclItem, ai_grantor), 4);
        assert_eq!(offset_of!(AclItem, ai_privs), 8);
    }

    #[test]
    fn privilege_bit_helpers_match_c_macros() {
        let item = AclItem {
            ai_grantee: 10,
            ai_grantor: 20,
            ai_privs: acl_grant_option_for(ACL_SELECT) | ACL_INSERT | ACL_SELECT,
        };
        assert_eq!(aclitem_get_privs(item), ACL_INSERT | ACL_SELECT);
        assert_eq!(aclitem_get_goptions(item), ACL_SELECT);
        assert_eq!(aclitem_get_rights(item), item.ai_privs);
        assert_eq!(
            acl_option_to_privs(acl_grant_option_for(ACL_SELECT)),
            ACL_SELECT
        );
    }
}
