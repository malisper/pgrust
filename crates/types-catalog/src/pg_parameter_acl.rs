//! `pg_parameter_acl` catalog vocabulary (`catalog/pg_parameter_acl.h` /
//! `pg_parameter_acl_d.h`) — relation / index OIDs, column numbers, and the
//! attribute count.
//!
//! The owning catalog crate (`backend-catalog-pg-parameter-acl`) forms a
//! `pg_parameter_acl` tuple from these column numbers against the relation
//! descriptor (`ParameterAclCreate`), so consumers never touch the on-disk
//! datum layout. Field order is verified field-for-field against
//! `FormData_pg_parameter_acl` in `pg_parameter_acl.h`:
//!   oid,
//!   [CATALOG_VARLEN] parname (text, FORCE_NOT_NULL), paracl (aclitem[],
//!   `_null_` default).

use types_core::primitive::Oid;

// ---------------------------------------------------------------------------
// Relation / index OIDs (catalog/pg_parameter_acl_d.h).
// ---------------------------------------------------------------------------

/// `ParameterAclRelationId` — `pg_parameter_acl`'s relation OID
/// (`CATALOG(pg_parameter_acl,6243,ParameterAclRelationId)`).
pub const ParameterAclRelationId: Oid = 6243;

/// `ParameterAclOidIndexId` — `pg_parameter_acl_oid_index` OID (unique on
/// `oid`).
pub const ParameterAclOidIndexId: Oid = 6247;

/// `ParameterAclParnameIndexId` — `pg_parameter_acl_parname_index` OID (unique
/// on `parname`).
pub const ParameterAclParnameIndexId: Oid = 6246;

// ---------------------------------------------------------------------------
// Column numbers (catalog/pg_parameter_acl_d.h `Anum_pg_parameter_acl_*`).
// ---------------------------------------------------------------------------

/// `Anum_pg_parameter_acl_oid` = 1.
pub const Anum_pg_parameter_acl_oid: i32 = 1;
/// `Anum_pg_parameter_acl_parname` = 2 (text, FORCE_NOT_NULL).
pub const Anum_pg_parameter_acl_parname: i32 = 2;
/// `Anum_pg_parameter_acl_paracl` = 3 (aclitem[], `_null_` default).
pub const Anum_pg_parameter_acl_paracl: i32 = 3;

/// `Natts_pg_parameter_acl` = 3.
pub const Natts_pg_parameter_acl: usize = 3;
