//! Genbki-assigned catalog relation OIDs (`catalog/pg_*_d.h`), trimmed to the
//! rows the current ports consume.

use types_core::primitive::Oid;

/// `RelationRelationId` — `pg_class` (`pg_class_d.h`).
pub const RELATION_RELATION_ID: Oid = 1259;
/// `TypeRelationId` — `pg_type` (`pg_type_d.h`).
pub const TYPE_RELATION_ID: Oid = 1247;
/// `ConstraintRelationId` — `pg_constraint` (`pg_constraint_d.h`).
pub const CONSTRAINT_RELATION_ID: Oid = 2606;
/// `ExtensionRelationId` — `pg_extension` (`pg_extension_d.h`).
pub const EXTENSION_RELATION_ID: Oid = 3079;

/// `PG_CATALOG_NAMESPACE` — the `pg_catalog` schema's OID
/// (`pg_namespace_d.h`).
pub const PG_CATALOG_NAMESPACE: Oid = 11;

/// `RELKIND_SEQUENCE` (`catalog/pg_class.h`) — `pg_class.relkind` for a
/// sequence object.
pub const RELKIND_SEQUENCE: u8 = b'S';
