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
/// `GLOBALTABLESPACE_OID` — the `pg_global` tablespace (`pg_tablespace_d.h`).
pub const GLOBALTABLESPACE_OID: Oid = 1664;
/// `ROLE_PG_USE_RESERVED_CONNECTIONS` (`pg_authid_d.h`).
pub const ROLE_PG_USE_RESERVED_CONNECTIONS: Oid = 4550;

/// `AccessMethodRelationId` — `pg_am` (`pg_am_d.h`).
pub const ACCESS_METHOD_RELATION_ID: Oid = 2601;
/// `AccessMethodOperatorRelationId` — `pg_amop` (`pg_amop_d.h`).
pub const ACCESS_METHOD_OPERATOR_RELATION_ID: Oid = 2602;
/// `AccessMethodProcedureRelationId` — `pg_amproc` (`pg_amproc_d.h`).
pub const ACCESS_METHOD_PROCEDURE_RELATION_ID: Oid = 2603;

/// `PG_CATALOG_NAMESPACE` — the `pg_catalog` schema's OID
/// (`pg_namespace_d.h`).
pub const PG_CATALOG_NAMESPACE: Oid = 11;

/// `RELKIND_SEQUENCE` (`catalog/pg_class.h`) — `pg_class.relkind` for a
/// sequence object.
pub const RELKIND_SEQUENCE: u8 = b'S';

/* Catalog relation OIDs consumed by the pg_shdepend port
 * (`catalog/pg_*_d.h`). */

/// `AuthMemRelationId` — `pg_auth_members` (`pg_auth_members_d.h`).
pub const AUTH_MEM_RELATION_ID: Oid = 1261;
/// `TableSpaceRelationId` — `pg_tablespace` (`pg_tablespace_d.h`).
pub const TABLE_SPACE_RELATION_ID: Oid = 1213;
/// `NamespaceRelationId` — `pg_namespace` (`pg_namespace_d.h`).
pub const NAMESPACE_RELATION_ID: Oid = 2615;
/// `DefaultAclRelationId` — `pg_default_acl` (`pg_default_acl_d.h`).
pub const DEFAULT_ACL_RELATION_ID: Oid = 826;
/// `UserMappingRelationId` — `pg_user_mapping` (`pg_user_mapping_d.h`).
pub const USER_MAPPING_RELATION_ID: Oid = 1418;
/// `ForeignServerRelationId` — `pg_foreign_server` (`pg_foreign_server_d.h`).
pub const FOREIGN_SERVER_RELATION_ID: Oid = 1417;
/// `ForeignDataWrapperRelationId` — `pg_foreign_data_wrapper`
/// (`pg_foreign_data_wrapper_d.h`).
pub const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = 2328;
/// `EventTriggerRelationId` — `pg_event_trigger` (`pg_event_trigger_d.h`).
pub const EVENT_TRIGGER_RELATION_ID: Oid = 3466;
/// `PublicationRelationId` — `pg_publication` (`pg_publication_d.h`).
pub const PUBLICATION_RELATION_ID: Oid = 6104;
/// `SubscriptionRelationId` — `pg_subscription` (`pg_subscription_d.h`).
pub const SUBSCRIPTION_RELATION_ID: Oid = 6100;
/// `CollationRelationId` — `pg_collation` (`pg_collation_d.h`).
pub const COLLATION_RELATION_ID: Oid = 3456;
/// `ConversionRelationId` — `pg_conversion` (`pg_conversion_d.h`).
pub const CONVERSION_RELATION_ID: Oid = 2607;
/// `OperatorRelationId` — `pg_operator` (`pg_operator_d.h`).
pub const OPERATOR_RELATION_ID: Oid = 2617;
/// `ProcedureRelationId` — `pg_proc` (`pg_proc_d.h`).
pub const PROCEDURE_RELATION_ID: Oid = 1255;
/// `LanguageRelationId` — `pg_language` (`pg_language_d.h`).
pub const LANGUAGE_RELATION_ID: Oid = 2612;
/// `LargeObjectRelationId` — `pg_largeobject` (`pg_largeobject_d.h`).
pub const LARGE_OBJECT_RELATION_ID: Oid = 2613;
/// `OperatorFamilyRelationId` — `pg_opfamily` (`pg_opfamily_d.h`).
pub const OPERATOR_FAMILY_RELATION_ID: Oid = 2753;
/// `OperatorClassRelationId` — `pg_opclass` (`pg_opclass_d.h`).
pub const OPERATOR_CLASS_RELATION_ID: Oid = 2616;
/// `StatisticExtRelationId` — `pg_statistic_ext` (`pg_statistic_ext_d.h`).
pub const STATISTIC_EXT_RELATION_ID: Oid = 3381;
/// `TSConfigRelationId` — `pg_ts_config` (`pg_ts_config_d.h`).
pub const TS_CONFIG_RELATION_ID: Oid = 3602;
/// `TSDictionaryRelationId` — `pg_ts_dict` (`pg_ts_dict_d.h`).
pub const TS_DICTIONARY_RELATION_ID: Oid = 3600;

/* ===========================================================================
 * Shared-relation classification set consumed by `catalog/catalog.c`
 * (`IsSharedRelation`, `IsCatalogTextUniqueIndexOid`, `IsPinnedObject`). The
 * shared catalogs, their indexes, and their TOAST tables/indexes. Verified
 * against `catalog/pg_*_d.h`.
 * ========================================================================= */

/// `FirstUnpinnedObjectId` (`access/transam.h`) — OIDs `< FirstUnpinnedObjectId`
/// are pinned (assigned during bootstrap); the OID generator skips this range
/// on wraparound.
pub const FIRST_UNPINNED_OBJECT_ID: Oid = 12000;

/// `GLOBALTABLESPACE_OID` — the `pg_global` tablespace (`pg_tablespace_d.h`).
pub const GLOBALTABLESPACE_OID: Oid = 1664;

/// `PG_PUBLIC_NAMESPACE` — the `public` schema's OID (`pg_namespace_d.h`).
pub const PG_PUBLIC_NAMESPACE: Oid = 2200;

/// `ParameterAclRelationId` — `pg_parameter_acl` (`pg_parameter_acl_d.h`).
pub const PARAMETER_ACL_RELATION_ID: Oid = 6243;
/// `ReplicationOriginRelationId` — `pg_replication_origin`
/// (`pg_replication_origin_d.h`).
pub const REPLICATION_ORIGIN_RELATION_ID: Oid = 6000;
/// `SharedDescriptionRelationId` — `pg_shdescription` (`pg_shdescription_d.h`).
pub const SHARED_DESCRIPTION_RELATION_ID: Oid = 2396;
/// `SharedSecLabelRelationId` — `pg_shseclabel` (`pg_shseclabel_d.h`).
pub const SHARED_SEC_LABEL_RELATION_ID: Oid = 3592;

/* Shared-catalog indexes. */
/// `AuthIdRolnameIndexId` — `pg_authid_rolname_index` (`pg_authid_d.h`).
pub const AUTH_ID_ROLNAME_INDEX_ID: Oid = 2676;
/// `AuthMemMemRoleIndexId` — `pg_auth_members_member_role_index`.
pub const AUTH_MEM_MEM_ROLE_INDEX_ID: Oid = 2695;
/// `AuthMemRoleMemIndexId` — `pg_auth_members_role_member_index`.
pub const AUTH_MEM_ROLE_MEM_INDEX_ID: Oid = 2694;
/// `AuthMemOidIndexId` — `pg_auth_members_oid_index`.
pub const AUTH_MEM_OID_INDEX_ID: Oid = 6303;
/// `AuthMemGrantorIndexId` — `pg_auth_members_grantor_index`.
pub const AUTH_MEM_GRANTOR_INDEX_ID: Oid = 6302;
/// `DbRoleSettingDatidRolidIndexId` — `pg_db_role_setting_databaseid_rol_index`.
pub const DB_ROLE_SETTING_DATID_ROLID_INDEX_ID: Oid = 2965;
/// `ParameterAclOidIndexId` — `pg_parameter_acl_oid_index`.
pub const PARAMETER_ACL_OID_INDEX_ID: Oid = 6247;
/// `ParameterAclParnameIndexId` — `pg_parameter_acl_parname_index` (text key).
pub const PARAMETER_ACL_PARNAME_INDEX_ID: Oid = 6246;
/// `ReplicationOriginIdentIndex` — `pg_replication_origin_roiident_index`.
pub const REPLICATION_ORIGIN_IDENT_INDEX: Oid = 6001;
/// `ReplicationOriginNameIndex` — `pg_replication_origin_roname_index` (text).
pub const REPLICATION_ORIGIN_NAME_INDEX: Oid = 6002;
/// `SecLabelObjectIndexId` — `pg_seclabel_object_index` (text key).
pub const SEC_LABEL_OBJECT_INDEX_ID: Oid = 3597;
/// `SharedDependDependerIndexId` — `pg_shdepend_depender_index`.
pub const SHARED_DEPEND_DEPENDER_INDEX_ID: Oid = 1232;
/// `SharedDependReferenceIndexId` — `pg_shdepend_reference_index`.
pub const SHARED_DEPEND_REFERENCE_INDEX_ID: Oid = 1233;
/// `SharedDescriptionObjIndexId` — `pg_shdescription_o_c_index`.
pub const SHARED_DESCRIPTION_OBJ_INDEX_ID: Oid = 2397;
/// `SharedSecLabelObjectIndexId` — `pg_shseclabel_object_index` (text key).
pub const SHARED_SEC_LABEL_OBJECT_INDEX_ID: Oid = 3593;
/// `SubscriptionNameIndexId` — `pg_subscription_subname_index` (text key).
pub const SUBSCRIPTION_NAME_INDEX_ID: Oid = 6115;
/// `SubscriptionObjectIndexId` — `pg_subscription_oid_index`.
pub const SUBSCRIPTION_OBJECT_INDEX_ID: Oid = 6114;
/// `TablespaceNameIndexId` — `pg_tablespace_spcname_index`.
pub const TABLESPACE_NAME_INDEX_ID: Oid = 2698;
/// `TablespaceOidIndexId` — `pg_tablespace_oid_index`.
pub const TABLESPACE_OID_INDEX_ID: Oid = 2697;
/// `ClassOidIndexId` — `pg_class_oid_index` (`pg_class_d.h`).
pub const CLASS_OID_INDEX_ID: Oid = 2662;
/// `Anum_pg_class_oid` — the `oid` column of pg_class (`pg_class_d.h`).
pub const ANUM_PG_CLASS_OID: types_core::primitive::AttrNumber = 1;

/* Shared-catalog TOAST tables and their indexes (`pg_*_d.h`). */
/// `PgDatabaseToastTable`.
pub const PG_DATABASE_TOAST_TABLE: Oid = 4177;
/// `PgDatabaseToastIndex`.
pub const PG_DATABASE_TOAST_INDEX: Oid = 4178;
/// `PgDbRoleSettingToastTable`.
pub const PG_DB_ROLE_SETTING_TOAST_TABLE: Oid = 2966;
/// `PgDbRoleSettingToastIndex`.
pub const PG_DB_ROLE_SETTING_TOAST_INDEX: Oid = 2967;
/// `PgParameterAclToastTable`.
pub const PG_PARAMETER_ACL_TOAST_TABLE: Oid = 6244;
/// `PgParameterAclToastIndex`.
pub const PG_PARAMETER_ACL_TOAST_INDEX: Oid = 6245;
/// `PgShdescriptionToastTable`.
pub const PG_SHDESCRIPTION_TOAST_TABLE: Oid = 2846;
/// `PgShdescriptionToastIndex`.
pub const PG_SHDESCRIPTION_TOAST_INDEX: Oid = 2847;
/// `PgShseclabelToastTable`.
pub const PG_SHSECLABEL_TOAST_TABLE: Oid = 4060;
/// `PgShseclabelToastIndex`.
pub const PG_SHSECLABEL_TOAST_INDEX: Oid = 4061;
/// `PgSubscriptionToastTable`.
pub const PG_SUBSCRIPTION_TOAST_TABLE: Oid = 4183;
/// `PgSubscriptionToastIndex`.
pub const PG_SUBSCRIPTION_TOAST_INDEX: Oid = 4184;
/// `PgTablespaceToastTable`.
pub const PG_TABLESPACE_TOAST_TABLE: Oid = 4185;
/// `PgTablespaceToastIndex`.
pub const PG_TABLESPACE_TOAST_INDEX: Oid = 4186;
