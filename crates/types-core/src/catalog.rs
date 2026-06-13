//! Bootstrap catalog OIDs (`pg_*.h` `DECLARE_*`/`CATALOG` fixed OIDs).
//!
//! Populated incrementally; only the items ports currently consume are
//! present. Values verified against the PG 18.3 catalog headers.

use crate::primitive::Oid;

/// `NamespaceRelationId` (`catalog/pg_namespace.h`) — pg_namespace's OID.
pub const NAMESPACE_RELATION_ID: Oid = 2615;
/// `RelationRelationId` (`catalog/pg_class.h`) — pg_class's OID.
pub const RELATION_RELATION_ID: Oid = 1259;
/// `DatabaseRelationId` (`catalog/pg_database.h`) — pg_database's OID.
pub const DATABASE_RELATION_ID: Oid = 1262;
/// `ProcedureRelationId` (`catalog/pg_proc.h`) — pg_proc's OID.
pub const PROCEDURE_RELATION_ID: Oid = 1255;
/// `TypeRelationId` (`catalog/pg_type.h`) — pg_type's OID.
pub const TYPE_RELATION_ID: Oid = 1247;
/// `LanguageRelationId` (`catalog/pg_language.h`) — pg_language's OID.
pub const LANGUAGE_RELATION_ID: Oid = 2612;
/// `ForeignServerRelationId` (`catalog/pg_foreign_server.h`) — pg_foreign_server's OID.
pub const FOREIGN_SERVER_RELATION_ID: Oid = 1417;
/// `ForeignDataWrapperRelationId` (`catalog/pg_foreign_data_wrapper.h`) — pg_foreign_data_wrapper's OID.
pub const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = 2328;
/// `TableSpaceRelationId` (`catalog/pg_tablespace.h`) — pg_tablespace's OID.
pub const TABLE_SPACE_RELATION_ID: Oid = 1213;
/// `AuthIdRelationId` (`catalog/pg_authid.h`) — pg_authid's OID.
pub const AUTH_ID_RELATION_ID: Oid = 1260;
/// `AuthIdOidIndexId` (`catalog/pg_authid.h`) — pg_authid_oid_index's OID.
pub const AUTH_ID_OID_INDEX_ID: Oid = 2677;
/// `AuthMemRelationId` (`catalog/pg_auth_members.h`) — pg_auth_members's OID.
pub const AUTH_MEM_RELATION_ID: Oid = 1261;
/// `AuthMemOidIndexId` (`catalog/pg_auth_members.h`) — pg_auth_members_oid_index's OID.
pub const AUTH_MEM_OID_INDEX_ID: Oid = 6303;
/// `AttributeRelationId` (`catalog/pg_attribute.h`) — pg_attribute's OID.
pub const ATTRIBUTE_RELATION_ID: Oid = 1249;
/// `IndexRelationId` (`catalog/pg_index.h`) — pg_index's OID.
pub const INDEX_RELATION_ID: Oid = 2610;
/// `ConstraintRelationId` (`catalog/pg_constraint.h`) — pg_constraint's OID.
pub const CONSTRAINT_RELATION_ID: Oid = 2606;

/// `PG_CATALOG_NAMESPACE` (`catalog/pg_namespace_d.h`) — OID of the
/// `pg_catalog` namespace.
pub const PG_CATALOG_NAMESPACE: Oid = 11;
/// `PG_TOAST_NAMESPACE` (`catalog/pg_namespace_d.h`) — OID of the `pg_toast`
/// namespace.
pub const PG_TOAST_NAMESPACE: Oid = 99;
/// `BOOTSTRAP_SUPERUSERID` (`catalog/pg_authid_d.h`).
pub const BOOTSTRAP_SUPERUSERID: Oid = 10;
/// `ROLE_PG_DATABASE_OWNER` (`catalog/pg_authid.dat`) — the `pg_database_owner`
/// predefined role.
pub const ROLE_PG_DATABASE_OWNER: Oid = 6171;

/// `FirstGenbkiObjectId` (`access/transam.h`) — first OID assignable by
/// genbki.pl; the initdb OID generator starts here.
pub const FirstGenbkiObjectId: Oid = 10000;
/// `FirstUnpinnedObjectId` (`access/transam.h`) — first OID that is not pinned;
/// `StopGeneratingPinnedObjectIds` forces the counter up to this value.
pub const FirstUnpinnedObjectId: Oid = 12000;
/// `FirstNormalObjectId` (`access/transam.h`) — first OID assignable to
/// user-created objects; OIDs below this belong to built-in system objects.
pub const FirstNormalObjectId: Oid = 16384;

/// `OIDOID` (`catalog/pg_type_d.h`) — the OID of the `oid` type.
pub const OIDOID: Oid = 26;
/// `BOOLOID` (`catalog/pg_type_d.h`) — the OID of the `bool` type.
pub const BOOLOID: Oid = 16;
/// `INT8OID` (`catalog/pg_type_d.h`) — the OID of the `int8` (bigint) type.
pub const INT8OID: Oid = 20;
/// `INT4OID` (`catalog/pg_type_d.h`) — the OID of the `int4` (integer) type.
pub const INT4OID: Oid = 23;
/// `VOIDOID` (`catalog/pg_type_d.h`) — the OID of the `void` pseudo-type.
pub const VOIDOID: Oid = 2278;
/// `INTERNALOID` (`catalog/pg_type_d.h`) — the OID of the `internal`
/// pseudo-type.
pub const INTERNALOID: Oid = 2281;

/// `BTREE_AM_OID` (`catalog/pg_am_d.h`) — the OID of the btree access method.
pub const BTREE_AM_OID: Oid = 403;

/// `C_COLLATION_OID` (`pg_collation.dat` oid 950) — the `C` collation. The
/// `ScanKeyInit` shorthand always stamps this into `sk_collation` (correct
/// for all collation-aware catalog columns, ignored for the rest).
pub const C_COLLATION_OID: Oid = 950;

/// `RELPERSISTENCE_PERMANENT` (`catalog/pg_class.h`) — regular table.
pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
/// `RELPERSISTENCE_UNLOGGED` (`catalog/pg_class.h`) — unlogged permanent table.
pub const RELPERSISTENCE_UNLOGGED: u8 = b'u';
/// `RELPERSISTENCE_TEMP` (`catalog/pg_class.h`) — temporary table.
pub const RELPERSISTENCE_TEMP: u8 = b't';
