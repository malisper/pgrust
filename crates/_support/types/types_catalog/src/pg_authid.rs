//! `pg_authid` / `pg_auth_members` catalog column vocabulary
//! (`catalog/pg_authid_d.h`, `catalog/pg_auth_members_d.h`).
//!
//! Attribute numbers and relation/index OIDs consumed by the `commands/user.c`
//! catalog-write value layer (`backend-catalog-indexing`).

#![allow(non_upper_case_globals)]

use ::types_core::primitive::Oid;

/* ---- pg_authid (`pg_authid_d.h`) ---- */

/// `AuthIdRelationId` — `pg_authid`.
pub const AuthIdRelationId: Oid = 1260;
/// `AuthIdRolnameIndexId` — `pg_authid_rolname_index`.
pub const AuthIdRolnameIndexId: Oid = 2676;
/// `AuthIdOidIndexId` — `pg_authid_oid_index`.
pub const AuthIdOidIndexId: Oid = 2677;

pub const Anum_pg_authid_oid: i16 = 1;
pub const Anum_pg_authid_rolname: i16 = 2;
pub const Anum_pg_authid_rolsuper: i16 = 3;
pub const Anum_pg_authid_rolinherit: i16 = 4;
pub const Anum_pg_authid_rolcreaterole: i16 = 5;
pub const Anum_pg_authid_rolcreatedb: i16 = 6;
pub const Anum_pg_authid_rolcanlogin: i16 = 7;
pub const Anum_pg_authid_rolreplication: i16 = 8;
pub const Anum_pg_authid_rolbypassrls: i16 = 9;
pub const Anum_pg_authid_rolconnlimit: i16 = 10;
pub const Anum_pg_authid_rolpassword: i16 = 11;
pub const Anum_pg_authid_rolvaliduntil: i16 = 12;
pub const Natts_pg_authid: usize = 12;

/* ---- pg_auth_members (`pg_auth_members_d.h`) ---- */

/// `AuthMemRelationId` — `pg_auth_members`.
pub const AuthMemRelationId: Oid = 1261;
/// `AuthMemOidIndexId` — `pg_auth_members_oid_index`.
pub const AuthMemOidIndexId: Oid = 6303;
/// `AuthMemRoleMemIndexId` — `pg_auth_members_role_member_index`.
pub const AuthMemRoleMemIndexId: Oid = 2694;
/// `AuthMemMemRoleIndexId` — `pg_auth_members_member_role_index`.
pub const AuthMemMemRoleIndexId: Oid = 2695;
/// `AuthMemGrantorIndexId` — `pg_auth_members_grantor_index`.
pub const AuthMemGrantorIndexId: Oid = 6302;

pub const Anum_pg_auth_members_oid: i16 = 1;
pub const Anum_pg_auth_members_roleid: i16 = 2;
pub const Anum_pg_auth_members_member: i16 = 3;
pub const Anum_pg_auth_members_grantor: i16 = 4;
pub const Anum_pg_auth_members_admin_option: i16 = 5;
pub const Anum_pg_auth_members_inherit_option: i16 = 6;
pub const Anum_pg_auth_members_set_option: i16 = 7;
pub const Natts_pg_auth_members: usize = 7;
