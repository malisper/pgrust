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

/// `OIDOID` (`catalog/pg_type_d.h`) — the OID of the `oid` type.
pub const OIDOID: Oid = 26;

/// `RELPERSISTENCE_PERMANENT` (`catalog/pg_class.h`) — regular table.
pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
/// `RELPERSISTENCE_UNLOGGED` (`catalog/pg_class.h`) — unlogged permanent table.
pub const RELPERSISTENCE_UNLOGGED: u8 = b'u';
/// `RELPERSISTENCE_TEMP` (`catalog/pg_class.h`) — temporary table.
pub const RELPERSISTENCE_TEMP: u8 = b't';
