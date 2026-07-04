#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::Oid;
use types_error::PgResult;

pub mod builtins;
mod io;
mod membership;
mod ops;
pub mod varlena;
#[cfg(test)]
mod tests;

pub use io::{aclitemin, aclitemout, aclparse, ACL_ALL_RIGHTS_STR};
pub use membership::{
    get_language_oid, get_role_oid, get_role_oid_or_public, has_privs_of_role, initialize_acl,
    is_admin_of_role, is_member_of_role, is_member_of_role_nosuper, member_can_set_role,
    select_best_admin, RoleMembershipCacheCallback,
};
pub(crate) use membership::RoleRecurseType;
pub use ops::{
    aclconcat, aclcontains, aclcopy, aclequal, aclitem_comparator, aclitem_match, aclitemsort,
    aclmask_direct, aclmembers, aclmerge, aclnewowner, aclupdate, convert_any_priv_string,
    select_best_grantor, PrivMapEntry, ACL_MODECHG_ADD, ACL_MODECHG_DEL, ACL_MODECHG_EQL,
    DROP_CASCADE, DROP_RESTRICT,
};

pub const ACLITEMOID: u32 = 1033;

pub const ACL_INSERT: u64 = 1 << 0;
pub const ACL_SELECT: u64 = 1 << 1;
pub const ACL_UPDATE: u64 = 1 << 2;
pub const ACL_DELETE: u64 = 1 << 3;
pub const ACL_TRUNCATE: u64 = 1 << 4;
pub const ACL_REFERENCES: u64 = 1 << 5;
pub const ACL_TRIGGER: u64 = 1 << 6;
pub const ACL_EXECUTE: u64 = 1 << 7;
pub const ACL_USAGE: u64 = 1 << 8;
pub const ACL_CREATE: u64 = 1 << 9;
pub const ACL_CREATE_TEMP: u64 = 1 << 10;
pub const ACL_CONNECT: u64 = 1 << 11;
pub const ACL_SET: u64 = 1 << 12;
pub const ACL_ALTER_SYSTEM: u64 = 1 << 13;
pub const ACL_MAINTAIN: u64 = 1 << 14;
pub const N_ACL_RIGHTS: u32 = 15;
pub const ACL_NO_RIGHTS: u64 = 0;

pub const ACL_ID_PUBLIC: Oid = 0;
pub const ACLITEM_ALL_PRIV_BITS: u64 = 0xFFFF_FFFF;
pub const ACLITEM_ALL_GOPTION_BITS: u64 = 0xFFFF_FFFF << 32;

pub const ACL_ALL_RIGHTS_RELATION: u64 = ACL_INSERT
    | ACL_SELECT
    | ACL_UPDATE
    | ACL_DELETE
    | ACL_TRUNCATE
    | ACL_REFERENCES
    | ACL_TRIGGER
    | ACL_MAINTAIN;
pub const ACL_ALL_RIGHTS_COLUMN: u64 = ACL_INSERT | ACL_SELECT | ACL_UPDATE | ACL_REFERENCES;
pub const ACL_ALL_RIGHTS_SEQUENCE: u64 = ACL_USAGE | ACL_SELECT | ACL_UPDATE;
pub const ACL_ALL_RIGHTS_DATABASE: u64 = ACL_CREATE | ACL_CREATE_TEMP | ACL_CONNECT;
pub const ACL_ALL_RIGHTS_FDW: u64 = ACL_USAGE;
pub const ACL_ALL_RIGHTS_FOREIGN_SERVER: u64 = ACL_USAGE;
pub const ACL_ALL_RIGHTS_FUNCTION: u64 = ACL_EXECUTE;
pub const ACL_ALL_RIGHTS_LANGUAGE: u64 = ACL_USAGE;
pub const ACL_ALL_RIGHTS_LARGEOBJECT: u64 = ACL_SELECT | ACL_UPDATE;
pub const ACL_ALL_RIGHTS_PARAMETER_ACL: u64 = ACL_SET | ACL_ALTER_SYSTEM;
pub const ACL_ALL_RIGHTS_SCHEMA: u64 = ACL_USAGE | ACL_CREATE;
pub const ACL_ALL_RIGHTS_TABLESPACE: u64 = ACL_CREATE;
pub const ACL_ALL_RIGHTS_TYPE: u64 = ACL_USAGE;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AclItem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

const _: () = assert!(core::mem::size_of::<AclItem>() == 16);

#[inline]
pub const fn aclitem_get_privs(item: &AclItem) -> u64 {
    item.ai_privs & 0xFFFF_FFFF
}

#[inline]
pub const fn aclitem_get_goptions(item: &AclItem) -> u64 {
    (item.ai_privs >> 32) & 0xFFFF_FFFF
}

#[inline]
pub const fn aclitem_get_rights(item: &AclItem) -> u64 {
    item.ai_privs
}

#[inline]
pub fn aclitem_set_rights(item: &mut AclItem, rights: u64) {
    item.ai_privs = rights;
}

#[inline]
pub fn aclitem_set_privs_goptions(item: &mut AclItem, privs: u64, goptions: u64) {
    item.ai_privs = (privs & 0xFFFF_FFFF) | ((goptions & 0xFFFF_FFFF) << 32);
}

#[inline]
pub const fn acl_grant_option_for(privs: u64) -> u64 {
    (privs & 0xFFFF_FFFF) << 32
}

#[inline]
pub const fn acl_option_to_privs(privs: u64) -> u64 {
    (privs >> 32) & 0xFFFF_FFFF
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclObjectType {
    Column,
    Table,
    Sequence,
    Database,
    Function,
    Language,
    LargeObject,
    Schema,
    Tablespace,
    Fdw,
    ForeignServer,
    Domain,
    Type,
    ParameterAcl,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclMaskHow {
    AclmaskAll,
    AclmaskAny,
}

pub struct DefaultAcl {
    items: [AclItem; 2],
    n: usize,
}

impl DefaultAcl {
    pub fn as_slice(&self) -> &[AclItem] {
        &self.items[..self.n]
    }
}

pub fn acldefault(objtype: AclObjectType, owner_id: Oid) -> DefaultAcl {
    use AclObjectType::*;
    let (world_default, owner_default) = match objtype {
        Column => (ACL_NO_RIGHTS, ACL_NO_RIGHTS),
        Table => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_RELATION),
        Sequence => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_SEQUENCE),
        Database => (ACL_CREATE_TEMP | ACL_CONNECT, ACL_ALL_RIGHTS_DATABASE),
        Function => (ACL_EXECUTE, ACL_ALL_RIGHTS_FUNCTION),
        Language => (ACL_USAGE, ACL_ALL_RIGHTS_LANGUAGE),
        LargeObject => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_LARGEOBJECT),
        Schema => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_SCHEMA),
        Tablespace => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_TABLESPACE),
        Fdw => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_FDW),
        ForeignServer => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_FOREIGN_SERVER),
        Domain | Type => (ACL_USAGE, ACL_ALL_RIGHTS_TYPE),
        ParameterAcl => (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_PARAMETER_ACL),
    };

    let mut acl = DefaultAcl {
        items: [AclItem { ai_grantee: 0, ai_grantor: 0, ai_privs: 0 }; 2],
        n: 0,
    };
    if world_default != ACL_NO_RIGHTS {
        acl.items[acl.n] = AclItem {
            ai_grantee: ACL_ID_PUBLIC,
            ai_grantor: owner_id,
            ai_privs: world_default,
        };
        acl.n += 1;
    }
    // Owner shows all ordinary privileges but no grant options: owner grant
    // options are special-cased wherever grant options are tested.
    if owner_default != ACL_NO_RIGHTS {
        acl.items[acl.n] = AclItem {
            ai_grantee: owner_id,
            ai_grantor: owner_id,
            ai_privs: owner_default,
        };
        acl.n += 1;
    }
    acl
}

pub fn aclmask(
    acl: &[AclItem],
    roleid: Oid,
    owner_id: Oid,
    mask: u64,
    how: AclMaskHow,
) -> PgResult<u64> {
    if mask == 0 {
        return Ok(0);
    }

    let done = |result: u64| match how {
        AclMaskHow::AclmaskAll => result == mask,
        AclMaskHow::AclmaskAny => result != 0,
    };

    let mut result = 0;

    // Owner always implicitly has all grant options.
    if mask & ACLITEM_ALL_GOPTION_BITS != 0 && has_privs_of_role(roleid, owner_id)? {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if done(result) {
            return Ok(result);
        }
    }

    for item in acl {
        if item.ai_grantee == ACL_ID_PUBLIC || item.ai_grantee == roleid {
            result |= item.ai_privs & mask;
            if done(result) {
                return Ok(result);
            }
        }
    }

    // Second pass for indirect grants, so the expensive membership test runs
    // only for entries still granting privileges of interest.
    let mut remaining = mask & !result;
    for item in acl {
        if item.ai_grantee == ACL_ID_PUBLIC || item.ai_grantee == roleid {
            continue;
        }
        if item.ai_privs & remaining != 0 && has_privs_of_role(roleid, item.ai_grantee)? {
            result |= item.ai_privs & mask;
            if done(result) {
                return Ok(result);
            }
            remaining = mask & !result;
        }
    }

    Ok(result)
}

pub fn init_seams() {
    acl_seams::initialize_acl::set(initialize_acl);
    acl_seams::has_privs_of_role::set(has_privs_of_role);
    acl_seams::member_can_set_role::set(member_can_set_role);
    acl_seams::is_member_of_role_nosuper::set(is_member_of_role_nosuper);
    acl_seams::get_role_oid::set(get_role_oid);
}
