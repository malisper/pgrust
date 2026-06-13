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
/// `DatabaseRelationId` — `pg_database` (`pg_database_d.h`).
pub const DATABASE_RELATION_ID: Oid = 1262;
/// `AuthIdRelationId` — `pg_authid` (`pg_authid_d.h`).
pub const AUTH_ID_RELATION_ID: Oid = 1260;
/// `DbRoleSettingRelationId` — `pg_db_role_setting` (`pg_db_role_setting_d.h`).
pub const DB_ROLE_SETTING_RELATION_ID: Oid = 2964;
/// `DatabaseNameIndexId` — `pg_database_datname_index` (`pg_database_d.h`).
pub const DATABASE_NAME_INDEX_ID: Oid = 2671;
/// `DatabaseOidIndexId` — `pg_database_oid_index` (`pg_database_d.h`).
pub const DATABASE_OID_INDEX_ID: Oid = 2672;

/// `Template1DbOid` — the `template1` database (`pg_database_d.h`).
pub const TEMPLATE1_DB_OID: Oid = 1;
/// `DEFAULTTABLESPACE_OID` — the `pg_default` tablespace (`pg_tablespace_d.h`).
pub const DEFAULTTABLESPACE_OID: Oid = 1663;
/// `ROLE_PG_USE_RESERVED_CONNECTIONS` (`pg_authid_d.h`).
pub const ROLE_PG_USE_RESERVED_CONNECTIONS: Oid = 4550;

/// `PG_CATALOG_NAMESPACE` — the `pg_catalog` schema's OID
/// (`pg_namespace_d.h`).
pub const PG_CATALOG_NAMESPACE: Oid = 11;

/// `RELKIND_SEQUENCE` (`catalog/pg_class.h`) — `pg_class.relkind` for a
/// sequence object.
pub const RELKIND_SEQUENCE: u8 = b'S';
