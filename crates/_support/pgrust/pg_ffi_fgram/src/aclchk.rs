//! ABI vocabulary for `backend/catalog/aclchk.c` — the GRANT/REVOKE executor
//! and the access-control check families.
//!
//! These `#[repr(C)]` structs / enums / constants cross the boundary between
//! the rewritten `backend-catalog-aclchk` crate and the rest of the backend.
//! They mirror the C definitions in
//!   * `src/include/nodes/parsenodes.h`  (`GrantStmt`, `GrantTargetType`,
//!     `AccessPriv`, `AlterDefaultPrivilegesStmt`)
//!   * `src/include/utils/aclchk_internal.h` (`InternalGrant`)
//!   * the catalog `pg_*_d.h` headers (the `XxxRelationId` OIDs aclchk.c names)
//!
//! The privilege-bit constants (`ACL_*`), the `AclItem`/`AclMode` value types,
//! `AclMaskHow`, `AclResult`, and the per-object `ACL_ALL_RIGHTS_*` masks all
//! already live in [`crate::acl`]; this module re-exports the ones aclchk.c
//! consumes (so the whole aclchk ABI is nameable from one place) and adds only
//! the parse-node / InternalGrant structs and the catalog OIDs aclchk.c spells
//! in the `XxxRelationId` form.
//!
//! `Acl` itself is a `ArrayType` of `AclItem` (a one-dimensional, no-nulls
//! PostgreSQL array); the C code carries it as `Acl *`, so we model it as a
//! pointer to [`crate::array::ArrayType`].

use crate::array::ArrayType;
use crate::commands_parsenodes::RoleSpec;
use crate::list::List;

// Re-export `ObjectType` so the whole aclchk parse-node ABI is nameable from
// this one module (it is the GRANT/REVOKE object kind throughout aclchk.c).
pub use crate::commands_parsenodes::ObjectType;
use crate::types::NodeTag;
use crate::{AclMode, DropBehavior, Oid};

// Re-export the ACL value-layer vocabulary aclchk.c uses, so callers can name
// the whole aclchk ABI from this one module.
pub use crate::acl::{
    acl_grant_option_for, acl_option_to_privs, aclitem_get_goptions, aclitem_get_privs,
    aclitem_get_rights, aclitem_set_privs_goptions, aclitem_set_rights, AclItem, AclMaskHow,
    AclResult, ACLCHECK_NOT_OWNER, ACLCHECK_NO_PRIV, ACLCHECK_OK, ACLITEM_ALL_GOPTION_BITS,
    ACLITEM_ALL_PRIV_BITS, ACLMASK_ALL, ACLMASK_ANY, ACL_ALL_RIGHTS_COLUMN,
    ACL_ALL_RIGHTS_DATABASE, ACL_ALL_RIGHTS_FDW, ACL_ALL_RIGHTS_FOREIGN_SERVER,
    ACL_ALL_RIGHTS_FUNCTION, ACL_ALL_RIGHTS_LANGUAGE, ACL_ALL_RIGHTS_LARGEOBJECT,
    ACL_ALL_RIGHTS_PARAMETER_ACL, ACL_ALL_RIGHTS_RELATION, ACL_ALL_RIGHTS_SCHEMA,
    ACL_ALL_RIGHTS_SEQUENCE, ACL_ALL_RIGHTS_STR, ACL_ALL_RIGHTS_TABLESPACE, ACL_ALL_RIGHTS_TYPE,
    ACL_ALTER_SYSTEM, ACL_ALTER_SYSTEM_CHR, ACL_CONNECT, ACL_CONNECT_CHR, ACL_CREATE,
    ACL_CREATE_CHR, ACL_CREATE_TEMP, ACL_CREATE_TEMP_CHR, ACL_DELETE, ACL_DELETE_CHR, ACL_EXECUTE,
    ACL_EXECUTE_CHR, ACL_ID_PUBLIC, ACL_INSERT, ACL_INSERT_CHR, ACL_MAINTAIN, ACL_MAINTAIN_CHR,
    ACL_MODECHG_ADD, ACL_MODECHG_DEL, ACL_MODECHG_EQL, ACL_NO_RIGHTS, ACL_REFERENCES,
    ACL_REFERENCES_CHR, ACL_SELECT, ACL_SELECT_CHR, ACL_SET, ACL_SET_CHR, ACL_TRIGGER,
    ACL_TRIGGER_CHR, ACL_TRUNCATE, ACL_TRUNCATE_CHR, ACL_UPDATE, ACL_UPDATE_CHR, ACL_USAGE,
    ACL_USAGE_CHR, N_ACL_RIGHTS,
};

/// `Acl` — an access-control list: a `ArrayType` of [`AclItem`].  aclchk.c
/// carries it as `Acl *`; here it is a pointer to the standard one-dimensional
/// PostgreSQL array struct.
pub type Acl = ArrayType;

// ---------------------------------------------------------------------------
// GRANT/REVOKE parse nodes (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef enum GrantTargetType` (parsenodes.h) — type of the grant target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub enum GrantTargetType {
    /// grant on specific named object(s)
    AclTargetObject = 0,
    /// grant on all objects in given schema(s)
    AclTargetAllInSchema = 1,
    /// ALTER DEFAULT PRIVILEGES
    AclTargetDefaults = 2,
}
pub use GrantTargetType::{
    AclTargetAllInSchema as ACL_TARGET_ALL_IN_SCHEMA, AclTargetDefaults as ACL_TARGET_DEFAULTS,
    AclTargetObject as ACL_TARGET_OBJECT,
};

/// `typedef struct GrantStmt` (parsenodes.h) — a GRANT or REVOKE statement.
#[repr(C)]
pub struct GrantStmt {
    pub type_: NodeTag,
    /// true = GRANT, false = REVOKE
    pub is_grant: bool,
    /// type of the grant target
    pub targtype: GrantTargetType,
    /// kind of object being operated on
    pub objtype: ObjectType,
    /// list of RangeVar / ObjectWithArgs nodes, or plain names (String values)
    pub objects: *mut List,
    /// list of AccessPriv nodes (NIL denotes ALL PRIVILEGES)
    pub privileges: *mut List,
    /// list of RoleSpec nodes
    pub grantees: *mut List,
    /// grant or revoke grant option
    pub grant_option: bool,
    pub grantor: *mut RoleSpec,
    /// drop behavior (for REVOKE)
    pub behavior: DropBehavior,
}

/// `typedef struct AccessPriv` (parsenodes.h) — an access privilege, with an
/// optional list of column names.  `priv_name == NULL` denotes ALL PRIVILEGES
/// (only used with a column list); `cols == NIL` denotes "all columns".
#[repr(C)]
pub struct AccessPriv {
    pub type_: NodeTag,
    /// string name of privilege
    pub priv_name: *mut core::ffi::c_char,
    /// list of String
    pub cols: *mut List,
}

/// `typedef struct AlterDefaultPrivilegesStmt` (parsenodes.h).
#[repr(C)]
pub struct AlterDefaultPrivilegesStmt {
    pub type_: NodeTag,
    /// list of DefElem
    pub options: *mut List,
    /// GRANT/REVOKE action (with objects=NIL)
    pub action: *mut GrantStmt,
}

// ---------------------------------------------------------------------------
// InternalGrant (utils/aclchk_internal.h)
// ---------------------------------------------------------------------------

/// `typedef struct { ... } InternalGrant` (aclchk_internal.h) — the internal
/// form of a GRANT/REVOKE: object/grantee names turned into Oids, the privilege
/// list reduced to an `AclMode` bitmask.
///
/// If `privileges` is `ACL_NO_RIGHTS` and `all_privs` is true, the GRANT code
/// fills `privileges` with the right `ACL_ALL_RIGHTS_*` for the object type
/// (NB: this mutates the struct).
#[repr(C)]
pub struct InternalGrant {
    pub is_grant: bool,
    pub objtype: ObjectType,
    pub objects: *mut List,
    pub all_privs: bool,
    pub privileges: AclMode,
    /// untransformed `AccessPriv` nodes for column privileges (OBJECT_TABLE only)
    pub col_privs: *mut List,
    pub grantees: *mut List,
    pub grant_option: bool,
    pub behavior: DropBehavior,
}

// ---------------------------------------------------------------------------
// Catalog relation OIDs aclchk.c spells in the `XxxRelationId` form.
//
// The canonical `XXX_RELATION_ID` constants live in `crate::catalog`; these
// aliases keep aclchk.c's own spelling so the port reads 1:1 against the C.
// Values are from the generated catalog `pg_*_d.h` headers.
// ---------------------------------------------------------------------------

/// `RelationRelationId` — `pg_class` (`pg_class_d.h`).
pub const RelationRelationId: Oid = crate::catalog::RELATION_RELATION_ID;
/// `AttributeRelationId` — `pg_attribute` (`pg_attribute_d.h`).
pub const AttributeRelationId: Oid = crate::catalog::ATTRIBUTE_RELATION_ID;
/// `DatabaseRelationId` — `pg_database` (`pg_database_d.h`).
pub const DatabaseRelationId: Oid = crate::catalog::DATABASE_RELATION_ID;
/// `TypeRelationId` — `pg_type` (`pg_type_d.h`).
pub const TypeRelationId: Oid = crate::catalog::TYPE_RELATION_ID;
/// `NamespaceRelationId` — `pg_namespace` (`pg_namespace_d.h`).
pub const NamespaceRelationId: Oid = crate::catalog::NAMESPACE_RELATION_ID;
/// `LanguageRelationId` — `pg_language` (`pg_language_d.h`).
pub const LanguageRelationId: Oid = crate::catalog::LANGUAGE_RELATION_ID;
/// `TableSpaceRelationId` — `pg_tablespace` (`pg_tablespace_d.h`).
pub const TableSpaceRelationId: Oid = crate::catalog::TABLE_SPACE_RELATION_ID;
/// `ForeignDataWrapperRelationId` — `pg_foreign_data_wrapper`.
pub const ForeignDataWrapperRelationId: Oid = crate::catalog::FOREIGN_DATA_WRAPPER_RELATION_ID;
/// `ForeignServerRelationId` — `pg_foreign_server` (`pg_foreign_server_d.h`).
pub const ForeignServerRelationId: Oid = crate::catalog::FOREIGN_SERVER_RELATION_ID;
/// `AuthIdRelationId` — `pg_authid` (`pg_authid_d.h`).
pub const AuthIdRelationId: Oid = crate::catalog::AUTH_ID_RELATION_ID;
/// `AuthMemRelationId` — `pg_auth_members` (`pg_auth_members_d.h`).
pub const AuthMemRelationId: Oid = crate::catalog::AUTH_MEM_RELATION_ID;
/// `AuthIdRolnameIndexId` — `pg_authid_rolname_index` (`pg_authid.h:58`).
pub const AuthIdRolnameIndexId: Oid = crate::catalog::AUTH_ID_ROLNAME_INDEX_ID;
/// `AuthIdOidIndexId` — `pg_authid_oid_index` (`pg_authid.h:59`).
pub const AuthIdOidIndexId: Oid = crate::catalog::AUTH_ID_OID_INDEX_ID;
/// `AuthMemOidIndexId` — `pg_auth_members_oid_index` (`pg_auth_members.h:48`).
pub const AuthMemOidIndexId: Oid = crate::catalog::AUTH_MEM_OID_INDEX_ID;
/// `AuthMemRoleMemIndexId` — `pg_auth_members_role_member_index`
/// (`pg_auth_members.h:49`).
pub const AuthMemRoleMemIndexId: Oid = crate::catalog::AUTH_MEM_ROLE_MEM_INDEX_ID;
/// `AuthMemMemRoleIndexId` — `pg_auth_members_member_role_index`
/// (`pg_auth_members.h:50`).
pub const AuthMemMemRoleIndexId: Oid = crate::catalog::AUTH_MEM_MEM_ROLE_INDEX_ID;
/// `ParameterAclRelationId` — `pg_parameter_acl` (`pg_parameter_acl_d.h`).
pub const ParameterAclRelationId: Oid = crate::catalog::PARAMETER_ACL_RELATION_ID;
/// `DefaultAclRelationId` — `pg_default_acl` (`pg_default_acl_d.h`).
pub const DefaultAclRelationId: Oid = crate::catalog::DEFAULT_ACL_RELATION_ID;
/// `LargeObjectRelationId` — `pg_largeobject` (`pg_largeobject_d.h`).
pub const LargeObjectRelationId: Oid = crate::catalog::LARGE_OBJECT_RELATION_ID;

/// `InitPrivsRelationId` — `pg_init_privs` (`pg_init_privs_d.h:23`).
pub const InitPrivsRelationId: Oid = 3394;
/// `LargeObjectMetadataRelationId` — `pg_largeobject_metadata`
/// (`pg_largeobject_metadata_d.h:23`).
pub const LargeObjectMetadataRelationId: Oid = 2995;

// ---------------------------------------------------------------------------
// pg_init_privs privtype codes (pg_init_privs.h)
// ---------------------------------------------------------------------------

/// `INITPRIVS_INITDB` — set during initdb.
pub const INITPRIVS_INITDB: i8 = b'i' as i8;
/// `INITPRIVS_EXTENSION` — set during CREATE EXTENSION.
pub const INITPRIVS_EXTENSION: i8 = b'e' as i8;
