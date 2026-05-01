use crate::include::catalog::{
    BOOL_BTREE_OPCLASS_OID, BootstrapCatalogKind, CHAR_BTREE_OPCLASS_OID, INT2_BTREE_OPCLASS_OID,
    INT4_BTREE_OPCLASS_OID, NAME_BTREE_OPCLASS_OID, OID_BTREE_OPCLASS_OID,
    OIDVECTOR_BTREE_OPCLASS_OID, TEXT_BTREE_OPCLASS_OID,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogIndexDescriptor {
    pub relation_oid: u32,
    pub relation_name: &'static str,
    pub heap_kind: BootstrapCatalogKind,
    pub unique: bool,
    pub key_attnums: &'static [i16],
    pub opclass_oids: &'static [u32],
}

const PG_NAMESPACE_NSPNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_NAMESPACE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_CLASS_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_CLASS_RELNAME_NSP_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_CLASS_TBLSPC_RELFILENODE_INDEX_KEYS: [i16; 2] = [7, 8];
const PG_ATTRIBUTE_RELID_ATTNAM_INDEX_KEYS: [i16; 2] = [1, 2];
const PG_ATTRIBUTE_RELID_ATTNUM_INDEX_KEYS: [i16; 2] = [1, 5];
const PG_ATTRDEF_ADRELID_ADNUM_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_ATTRDEF_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TYPE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TYPE_TYPNAME_NSP_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_PROC_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_PROC_PRONAME_ARGS_NSP_INDEX_KEYS: [i16; 3] = [2, 20, 3];
const PG_PROC_PROLANG_PROSRC_INDEX_KEYS: [i16; 2] = [5, 25];
const PG_DEFAULT_ACL_ROLE_NSP_OBJ_INDEX_KEYS: [i16; 3] = [2, 3, 4];
const PG_DEFAULT_ACL_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_AGGREGATE_FNOID_INDEX_KEYS: [i16; 1] = [1];
const PG_LANGUAGE_NAME_INDEX_KEYS: [i16; 1] = [2];
const PG_LANGUAGE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TS_DICT_DICTNAME_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_TS_DICT_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TS_PARSER_PRSNAME_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_TS_PARSER_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TS_CONFIG_CFGNAME_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_TS_CONFIG_MAP_INDEX_KEYS: [i16; 3] = [1, 2, 3];
const PG_TS_CONFIG_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TS_TEMPLATE_TMPLNAME_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_TS_TEMPLATE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_OPERATOR_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_OPERATOR_OPRNAME_L_R_N_INDEX_KEYS: [i16; 4] = [2, 8, 9, 3];
const PG_DATABASE_DATNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_DATABASE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_EXTENSION_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_EXTENSION_NAME_INDEX_KEYS: [i16; 1] = [2];
const PG_AUTHID_ROLNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_AUTHID_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_AUTH_MEMBERS_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_AUTH_MEMBERS_ROLE_MEMBER_INDEX_KEYS: [i16; 3] = [2, 3, 4];
const PG_AUTH_MEMBERS_MEMBER_ROLE_INDEX_KEYS: [i16; 3] = [3, 2, 4];
const PG_AUTH_MEMBERS_GRANTOR_INDEX_KEYS: [i16; 1] = [4];
const PG_CAST_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_CAST_SOURCE_TARGET_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_CONVERSION_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_CONSTRAINT_CONNAME_NSP_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_KEYS: [i16; 3] = [9, 10, 2];
const PG_CONSTRAINT_CONTYPID_INDEX_KEYS: [i16; 1] = [10];
const PG_CONSTRAINT_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_CONSTRAINT_CONPARENTID_INDEX_KEYS: [i16; 1] = [12];
const PG_DEPEND_DEPENDER_INDEX_KEYS: [i16; 3] = [1, 2, 3];
const PG_DEPEND_REFERENCE_INDEX_KEYS: [i16; 3] = [4, 5, 6];
const PG_DESCRIPTION_O_C_O_INDEX_KEYS: [i16; 3] = [1, 2, 3];
const PG_FOREIGN_DATA_WRAPPER_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_FOREIGN_DATA_WRAPPER_NAME_INDEX_KEYS: [i16; 1] = [2];
const PG_FOREIGN_SERVER_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_FOREIGN_SERVER_NAME_INDEX_KEYS: [i16; 1] = [2];
const PG_USER_MAPPING_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_USER_MAPPING_USER_SERVER_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_FOREIGN_TABLE_RELID_INDEX_KEYS: [i16; 1] = [1];
const PG_INDEX_INDRELID_INDEX_KEYS: [i16; 1] = [2];
const PG_INDEX_INDEXRELID_INDEX_KEYS: [i16; 1] = [1];
const PG_INHERITS_RELID_SEQNO_INDEX_KEYS: [i16; 2] = [1, 3];
const PG_INHERITS_PARENT_INDEX_KEYS: [i16; 1] = [2];
const PG_LARGEOBJECT_LOID_PN_INDEX_KEYS: [i16; 2] = [1, 2];
const PG_LARGEOBJECT_METADATA_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_PARTITIONED_TABLE_PARTRELID_INDEX_KEYS: [i16; 1] = [1];
const PG_REWRITE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_REWRITE_REL_RULENAME_INDEX_KEYS: [i16; 2] = [3, 2];
const PG_TRIGGER_RELID_NAME_INDEX_KEYS: [i16; 2] = [2, 4];
const PG_TRIGGER_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_EVENT_TRIGGER_EVTNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_EVENT_TRIGGER_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_PUBLICATION_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_PUBLICATION_PUBNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_PUBLICATION_REL_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_PUBLICATION_REL_PRRELID_PRPUBID_INDEX_KEYS: [i16; 2] = [3, 2];
const PG_PUBLICATION_REL_PRPUBID_INDEX_KEYS: [i16; 1] = [2];
const PG_PUBLICATION_NAMESPACE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_PUBLICATION_NAMESPACE_PNNSPID_PNPUBID_INDEX_KEYS: [i16; 2] = [3, 2];
const PG_REPLICATION_ORIGIN_ROIIDENT_INDEX_KEYS: [i16; 1] = [1];
const PG_REPLICATION_ORIGIN_RONAME_INDEX_KEYS: [i16; 1] = [2];
const PG_POLICY_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_POLICY_POLRELID_POLNAME_INDEX_KEYS: [i16; 2] = [3, 2];
const PG_AM_NAME_INDEX_KEYS: [i16; 1] = [2];
const PG_AM_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_AMOP_FAM_STRAT_INDEX_KEYS: [i16; 5] = [2, 3, 4, 5, 6];
const PG_AMOP_OPR_FAM_INDEX_KEYS: [i16; 3] = [7, 6, 2];
const PG_AMOP_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_AMPROC_FAM_PROC_INDEX_KEYS: [i16; 4] = [2, 3, 4, 5];
const PG_AMPROC_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TRANSFORM_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TRANSFORM_TYPE_LANG_INDEX_KEYS: [i16; 2] = [2, 3];
const PG_OPCLASS_AM_NAME_NSP_INDEX_KEYS: [i16; 3] = [2, 3, 4];
const PG_OPCLASS_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_OPFAMILY_AM_NAME_NSP_INDEX_KEYS: [i16; 3] = [2, 3, 4];
const PG_OPFAMILY_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_COLLATION_NAME_ENC_NSP_INDEX_KEYS: [i16; 3] = [2, 7, 3];
const PG_COLLATION_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TABLESPACE_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_TABLESPACE_SPCNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_SHDESCRIPTION_O_C_INDEX_KEYS: [i16; 2] = [1, 2];
const PG_SEQUENCE_SEQRELID_INDEX_KEYS: [i16; 1] = [1];
const PG_SUBSCRIPTION_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_SUBSCRIPTION_SUBNAME_INDEX_KEYS: [i16; 2] = [2, 4];
const PG_PARAMETER_ACL_PARNAME_INDEX_KEYS: [i16; 1] = [2];
const PG_PARAMETER_ACL_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_STATISTIC_RELID_ATTNUM_INH_INDEX_KEYS: [i16; 3] = [1, 2, 3];
const PG_STATISTIC_EXT_RELID_INDEX_KEYS: [i16; 1] = [2];
const PG_STATISTIC_EXT_OID_INDEX_KEYS: [i16; 1] = [1];
const PG_STATISTIC_EXT_NAME_NSP_INDEX_KEYS: [i16; 2] = [3, 4];
const PG_STATISTIC_EXT_DATA_STXOID_INH_INDEX_KEYS: [i16; 2] = [1, 2];

const OID_OPCLASS_1: [u32; 1] = [OID_BTREE_OPCLASS_OID];
const NAME_OPCLASS_1: [u32; 1] = [NAME_BTREE_OPCLASS_OID];
const TEXT_OPCLASS_1: [u32; 1] = [TEXT_BTREE_OPCLASS_OID];
const OID_OPCLASS_2: [u32; 2] = [OID_BTREE_OPCLASS_OID, OID_BTREE_OPCLASS_OID];
const OID_INT4_OPCLASS_2: [u32; 2] = [OID_BTREE_OPCLASS_OID, INT4_BTREE_OPCLASS_OID];
const OID_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
];
const OID_OID_CHAR_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    CHAR_BTREE_OPCLASS_OID,
];
const NAME_OID_OPCLASS_2: [u32; 2] = [NAME_BTREE_OPCLASS_OID, OID_BTREE_OPCLASS_OID];
const OID_NAME_OPCLASS_2: [u32; 2] = [OID_BTREE_OPCLASS_OID, NAME_BTREE_OPCLASS_OID];
const OID_NAME_OID_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    NAME_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
];
const NAME_OIDVECTOR_OID_OPCLASS_3: [u32; 3] = [
    NAME_BTREE_OPCLASS_OID,
    OIDVECTOR_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
];
const OID_TEXT_OPCLASS_2: [u32; 2] = [OID_BTREE_OPCLASS_OID, TEXT_BTREE_OPCLASS_OID];
const NAME_OID_OID_OID_OPCLASS_4: [u32; 4] = [
    NAME_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
];
const OID_INT2_OPCLASS_2: [u32; 2] = [OID_BTREE_OPCLASS_OID, INT2_BTREE_OPCLASS_OID];
const NAME_INT4_OID_OPCLASS_3: [u32; 3] = [
    NAME_BTREE_OPCLASS_OID,
    INT4_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
];
const OID_CHAR_OID_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    CHAR_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
];
const OID_OID_NAME_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    NAME_BTREE_OPCLASS_OID,
];
const OID_OID_INT4_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    INT4_BTREE_OPCLASS_OID,
];
const OID_INT4_INT4_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    INT4_BTREE_OPCLASS_OID,
    INT4_BTREE_OPCLASS_OID,
];
const OID_OID_OID_INT2_OPCLASS_4: [u32; 4] = [
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    INT2_BTREE_OPCLASS_OID,
];
const OID_BOOL_OPCLASS_2: [u32; 2] = [OID_BTREE_OPCLASS_OID, BOOL_BTREE_OPCLASS_OID];
const OID_OID_OID_INT2_CHAR_OPCLASS_5: [u32; 5] = [
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    OID_BTREE_OPCLASS_OID,
    INT2_BTREE_OPCLASS_OID,
    CHAR_BTREE_OPCLASS_OID,
];
const OID_INT2_BOOL_OPCLASS_3: [u32; 3] = [
    OID_BTREE_OPCLASS_OID,
    INT2_BTREE_OPCLASS_OID,
    BOOL_BTREE_OPCLASS_OID,
];

pub const SYSTEM_CATALOG_INDEXES: [CatalogIndexDescriptor; 110] = [
    CatalogIndexDescriptor {
        relation_oid: 2684,
        relation_name: "pg_namespace_nspname_index",
        heap_kind: BootstrapCatalogKind::PgNamespace,
        unique: true,
        key_attnums: &PG_NAMESPACE_NSPNAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2685,
        relation_name: "pg_namespace_oid_index",
        heap_kind: BootstrapCatalogKind::PgNamespace,
        unique: true,
        key_attnums: &PG_NAMESPACE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2662,
        relation_name: "pg_class_oid_index",
        heap_kind: BootstrapCatalogKind::PgClass,
        unique: true,
        key_attnums: &PG_CLASS_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2663,
        relation_name: "pg_class_relname_nsp_index",
        heap_kind: BootstrapCatalogKind::PgClass,
        unique: true,
        key_attnums: &PG_CLASS_RELNAME_NSP_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3455,
        relation_name: "pg_class_tblspc_relfilenode_index",
        heap_kind: BootstrapCatalogKind::PgClass,
        unique: false,
        key_attnums: &PG_CLASS_TBLSPC_RELFILENODE_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2658,
        relation_name: "pg_attribute_relid_attnam_index",
        heap_kind: BootstrapCatalogKind::PgAttribute,
        unique: true,
        key_attnums: &PG_ATTRIBUTE_RELID_ATTNAM_INDEX_KEYS,
        opclass_oids: &OID_NAME_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2659,
        relation_name: "pg_attribute_relid_attnum_index",
        heap_kind: BootstrapCatalogKind::PgAttribute,
        unique: true,
        key_attnums: &PG_ATTRIBUTE_RELID_ATTNUM_INDEX_KEYS,
        opclass_oids: &OID_INT2_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2656,
        relation_name: "pg_attrdef_adrelid_adnum_index",
        heap_kind: BootstrapCatalogKind::PgAttrdef,
        unique: true,
        key_attnums: &PG_ATTRDEF_ADRELID_ADNUM_INDEX_KEYS,
        opclass_oids: &OID_INT2_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2657,
        relation_name: "pg_attrdef_oid_index",
        heap_kind: BootstrapCatalogKind::PgAttrdef,
        unique: true,
        key_attnums: &PG_ATTRDEF_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2703,
        relation_name: "pg_type_oid_index",
        heap_kind: BootstrapCatalogKind::PgType,
        unique: true,
        key_attnums: &PG_TYPE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2704,
        relation_name: "pg_type_typname_nsp_index",
        heap_kind: BootstrapCatalogKind::PgType,
        unique: true,
        key_attnums: &PG_TYPE_TYPNAME_NSP_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2690,
        relation_name: "pg_proc_oid_index",
        heap_kind: BootstrapCatalogKind::PgProc,
        unique: true,
        key_attnums: &PG_PROC_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2691,
        relation_name: "pg_proc_proname_args_nsp_index",
        heap_kind: BootstrapCatalogKind::PgProc,
        unique: true,
        key_attnums: &PG_PROC_PRONAME_ARGS_NSP_INDEX_KEYS,
        opclass_oids: &NAME_OIDVECTOR_OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 15000,
        // :HACK: pgrust's executor still handles the opr_sanity pg_proc
        // self-join expensively. This pgrust-only catalog index matches the
        // prolang constant plus prosrc join key until planner/runtime joins
        // catch up. Keep this below FirstNormalObjectId so bootstrap indexes
        // do not advance user-object OID allocation.
        relation_name: "pg_proc_prolang_prosrc_index",
        heap_kind: BootstrapCatalogKind::PgProc,
        unique: false,
        key_attnums: &PG_PROC_PROLANG_PROSRC_INDEX_KEYS,
        opclass_oids: &OID_TEXT_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 827,
        relation_name: "pg_default_acl_role_nsp_obj_index",
        heap_kind: BootstrapCatalogKind::PgDefaultAcl,
        unique: true,
        key_attnums: &PG_DEFAULT_ACL_ROLE_NSP_OBJ_INDEX_KEYS,
        opclass_oids: &OID_OID_CHAR_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 828,
        relation_name: "pg_default_acl_oid_index",
        heap_kind: BootstrapCatalogKind::PgDefaultAcl,
        unique: true,
        key_attnums: &PG_DEFAULT_ACL_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2650,
        relation_name: "pg_aggregate_fnoid_index",
        heap_kind: BootstrapCatalogKind::PgAggregate,
        unique: true,
        key_attnums: &PG_AGGREGATE_FNOID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2681,
        relation_name: "pg_language_name_index",
        heap_kind: BootstrapCatalogKind::PgLanguage,
        unique: true,
        key_attnums: &PG_LANGUAGE_NAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2682,
        relation_name: "pg_language_oid_index",
        heap_kind: BootstrapCatalogKind::PgLanguage,
        unique: true,
        key_attnums: &PG_LANGUAGE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3604,
        relation_name: "pg_ts_dict_dictname_index",
        heap_kind: BootstrapCatalogKind::PgTsDict,
        unique: true,
        key_attnums: &PG_TS_DICT_DICTNAME_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3605,
        relation_name: "pg_ts_dict_oid_index",
        heap_kind: BootstrapCatalogKind::PgTsDict,
        unique: true,
        key_attnums: &PG_TS_DICT_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3606,
        relation_name: "pg_ts_parser_prsname_index",
        heap_kind: BootstrapCatalogKind::PgTsParser,
        unique: true,
        key_attnums: &PG_TS_PARSER_PRSNAME_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3607,
        relation_name: "pg_ts_parser_oid_index",
        heap_kind: BootstrapCatalogKind::PgTsParser,
        unique: true,
        key_attnums: &PG_TS_PARSER_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3608,
        relation_name: "pg_ts_config_cfgname_index",
        heap_kind: BootstrapCatalogKind::PgTsConfig,
        unique: true,
        key_attnums: &PG_TS_CONFIG_CFGNAME_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3609,
        relation_name: "pg_ts_config_map_index",
        heap_kind: BootstrapCatalogKind::PgTsConfigMap,
        unique: true,
        key_attnums: &PG_TS_CONFIG_MAP_INDEX_KEYS,
        opclass_oids: &OID_INT4_INT4_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 3712,
        relation_name: "pg_ts_config_oid_index",
        heap_kind: BootstrapCatalogKind::PgTsConfig,
        unique: true,
        key_attnums: &PG_TS_CONFIG_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3766,
        relation_name: "pg_ts_template_tmplname_index",
        heap_kind: BootstrapCatalogKind::PgTsTemplate,
        unique: true,
        key_attnums: &PG_TS_TEMPLATE_TMPLNAME_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3767,
        relation_name: "pg_ts_template_oid_index",
        heap_kind: BootstrapCatalogKind::PgTsTemplate,
        unique: true,
        key_attnums: &PG_TS_TEMPLATE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2688,
        relation_name: "pg_operator_oid_index",
        heap_kind: BootstrapCatalogKind::PgOperator,
        unique: true,
        key_attnums: &PG_OPERATOR_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2689,
        relation_name: "pg_operator_oprname_l_r_n_index",
        heap_kind: BootstrapCatalogKind::PgOperator,
        unique: true,
        key_attnums: &PG_OPERATOR_OPRNAME_L_R_N_INDEX_KEYS,
        opclass_oids: &NAME_OID_OID_OID_OPCLASS_4,
    },
    CatalogIndexDescriptor {
        relation_oid: 2671,
        relation_name: "pg_database_datname_index",
        heap_kind: BootstrapCatalogKind::PgDatabase,
        unique: true,
        key_attnums: &PG_DATABASE_DATNAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2672,
        relation_name: "pg_database_oid_index",
        heap_kind: BootstrapCatalogKind::PgDatabase,
        unique: true,
        key_attnums: &PG_DATABASE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3080,
        relation_name: "pg_extension_oid_index",
        heap_kind: BootstrapCatalogKind::PgExtension,
        unique: true,
        key_attnums: &PG_EXTENSION_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3081,
        relation_name: "pg_extension_name_index",
        heap_kind: BootstrapCatalogKind::PgExtension,
        unique: true,
        key_attnums: &PG_EXTENSION_NAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2676,
        relation_name: "pg_authid_rolname_index",
        heap_kind: BootstrapCatalogKind::PgAuthId,
        unique: true,
        key_attnums: &PG_AUTHID_ROLNAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2677,
        relation_name: "pg_authid_oid_index",
        heap_kind: BootstrapCatalogKind::PgAuthId,
        unique: true,
        key_attnums: &PG_AUTHID_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6303,
        relation_name: "pg_auth_members_oid_index",
        heap_kind: BootstrapCatalogKind::PgAuthMembers,
        unique: true,
        key_attnums: &PG_AUTH_MEMBERS_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2694,
        relation_name: "pg_auth_members_role_member_index",
        heap_kind: BootstrapCatalogKind::PgAuthMembers,
        unique: true,
        key_attnums: &PG_AUTH_MEMBERS_ROLE_MEMBER_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2695,
        relation_name: "pg_auth_members_member_role_index",
        heap_kind: BootstrapCatalogKind::PgAuthMembers,
        unique: true,
        key_attnums: &PG_AUTH_MEMBERS_MEMBER_ROLE_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 6302,
        relation_name: "pg_auth_members_grantor_index",
        heap_kind: BootstrapCatalogKind::PgAuthMembers,
        unique: false,
        key_attnums: &PG_AUTH_MEMBERS_GRANTOR_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2660,
        relation_name: "pg_cast_oid_index",
        heap_kind: BootstrapCatalogKind::PgCast,
        unique: true,
        key_attnums: &PG_CAST_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2661,
        relation_name: "pg_cast_source_target_index",
        heap_kind: BootstrapCatalogKind::PgCast,
        unique: true,
        key_attnums: &PG_CAST_SOURCE_TARGET_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2670,
        relation_name: "pg_conversion_oid_index",
        heap_kind: BootstrapCatalogKind::PgConversion,
        unique: true,
        key_attnums: &PG_CONVERSION_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2664,
        relation_name: "pg_constraint_conname_nsp_index",
        heap_kind: BootstrapCatalogKind::PgConstraint,
        unique: false,
        key_attnums: &PG_CONSTRAINT_CONNAME_NSP_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2665,
        relation_name: "pg_constraint_conrelid_contypid_conname_index",
        heap_kind: BootstrapCatalogKind::PgConstraint,
        unique: true,
        key_attnums: &PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_KEYS,
        opclass_oids: &OID_OID_NAME_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2666,
        relation_name: "pg_constraint_contypid_index",
        heap_kind: BootstrapCatalogKind::PgConstraint,
        unique: false,
        key_attnums: &PG_CONSTRAINT_CONTYPID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2667,
        relation_name: "pg_constraint_oid_index",
        heap_kind: BootstrapCatalogKind::PgConstraint,
        unique: true,
        key_attnums: &PG_CONSTRAINT_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2579,
        relation_name: "pg_constraint_conparentid_index",
        heap_kind: BootstrapCatalogKind::PgConstraint,
        unique: false,
        key_attnums: &PG_CONSTRAINT_CONPARENTID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2673,
        relation_name: "pg_depend_depender_index",
        heap_kind: BootstrapCatalogKind::PgDepend,
        unique: false,
        key_attnums: &PG_DEPEND_DEPENDER_INDEX_KEYS,
        opclass_oids: &OID_OID_INT4_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2674,
        relation_name: "pg_depend_reference_index",
        heap_kind: BootstrapCatalogKind::PgDepend,
        unique: false,
        key_attnums: &PG_DEPEND_REFERENCE_INDEX_KEYS,
        opclass_oids: &OID_OID_INT4_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2675,
        relation_name: "pg_description_o_c_o_index",
        heap_kind: BootstrapCatalogKind::PgDescription,
        unique: true,
        key_attnums: &PG_DESCRIPTION_O_C_O_INDEX_KEYS,
        opclass_oids: &OID_OID_INT4_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 112,
        relation_name: "pg_foreign_data_wrapper_oid_index",
        heap_kind: BootstrapCatalogKind::PgForeignDataWrapper,
        unique: true,
        key_attnums: &PG_FOREIGN_DATA_WRAPPER_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 548,
        relation_name: "pg_foreign_data_wrapper_name_index",
        heap_kind: BootstrapCatalogKind::PgForeignDataWrapper,
        unique: true,
        key_attnums: &PG_FOREIGN_DATA_WRAPPER_NAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 113,
        relation_name: "pg_foreign_server_oid_index",
        heap_kind: BootstrapCatalogKind::PgForeignServer,
        unique: true,
        key_attnums: &PG_FOREIGN_SERVER_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 549,
        relation_name: "pg_foreign_server_name_index",
        heap_kind: BootstrapCatalogKind::PgForeignServer,
        unique: true,
        key_attnums: &PG_FOREIGN_SERVER_NAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 174,
        relation_name: "pg_user_mapping_oid_index",
        heap_kind: BootstrapCatalogKind::PgUserMapping,
        unique: true,
        key_attnums: &PG_USER_MAPPING_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 175,
        relation_name: "pg_user_mapping_user_server_index",
        heap_kind: BootstrapCatalogKind::PgUserMapping,
        unique: true,
        key_attnums: &PG_USER_MAPPING_USER_SERVER_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3119,
        relation_name: "pg_foreign_table_relid_index",
        heap_kind: BootstrapCatalogKind::PgForeignTable,
        unique: true,
        key_attnums: &PG_FOREIGN_TABLE_RELID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2678,
        relation_name: "pg_index_indrelid_index",
        heap_kind: BootstrapCatalogKind::PgIndex,
        unique: false,
        key_attnums: &PG_INDEX_INDRELID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2679,
        relation_name: "pg_index_indexrelid_index",
        heap_kind: BootstrapCatalogKind::PgIndex,
        unique: true,
        key_attnums: &PG_INDEX_INDEXRELID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2680,
        relation_name: "pg_inherits_relid_seqno_index",
        heap_kind: BootstrapCatalogKind::PgInherits,
        unique: true,
        key_attnums: &PG_INHERITS_RELID_SEQNO_INDEX_KEYS,
        opclass_oids: &OID_INT4_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2683,
        relation_name: "pg_largeobject_loid_pn_index",
        heap_kind: BootstrapCatalogKind::PgLargeobject,
        unique: true,
        key_attnums: &PG_LARGEOBJECT_LOID_PN_INDEX_KEYS,
        opclass_oids: &OID_INT4_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2996,
        relation_name: "pg_largeobject_metadata_oid_index",
        heap_kind: BootstrapCatalogKind::PgLargeobjectMetadata,
        unique: true,
        key_attnums: &PG_LARGEOBJECT_METADATA_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2187,
        relation_name: "pg_inherits_parent_index",
        heap_kind: BootstrapCatalogKind::PgInherits,
        unique: false,
        key_attnums: &PG_INHERITS_PARENT_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3351,
        relation_name: "pg_partitioned_table_partrelid_index",
        heap_kind: BootstrapCatalogKind::PgPartitionedTable,
        unique: true,
        key_attnums: &PG_PARTITIONED_TABLE_PARTRELID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 5002,
        relation_name: "pg_sequence_seqrelid_index",
        heap_kind: BootstrapCatalogKind::PgSequence,
        unique: true,
        key_attnums: &PG_SEQUENCE_SEQRELID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2692,
        relation_name: "pg_rewrite_oid_index",
        heap_kind: BootstrapCatalogKind::PgRewrite,
        unique: true,
        key_attnums: &PG_REWRITE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2693,
        relation_name: "pg_rewrite_rel_rulename_index",
        heap_kind: BootstrapCatalogKind::PgRewrite,
        unique: true,
        key_attnums: &PG_REWRITE_REL_RULENAME_INDEX_KEYS,
        opclass_oids: &OID_NAME_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 6001,
        relation_name: "pg_replication_origin_roiident_index",
        heap_kind: BootstrapCatalogKind::PgReplicationOrigin,
        unique: true,
        key_attnums: &PG_REPLICATION_ORIGIN_ROIIDENT_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6002,
        relation_name: "pg_replication_origin_roname_index",
        heap_kind: BootstrapCatalogKind::PgReplicationOrigin,
        unique: true,
        key_attnums: &PG_REPLICATION_ORIGIN_RONAME_INDEX_KEYS,
        opclass_oids: &TEXT_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2701,
        relation_name: "pg_trigger_tgrelid_tgname_index",
        heap_kind: BootstrapCatalogKind::PgTrigger,
        unique: true,
        key_attnums: &PG_TRIGGER_RELID_NAME_INDEX_KEYS,
        opclass_oids: &OID_NAME_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2702,
        relation_name: "pg_trigger_oid_index",
        heap_kind: BootstrapCatalogKind::PgTrigger,
        unique: true,
        key_attnums: &PG_TRIGGER_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3467,
        relation_name: "pg_event_trigger_evtname_index",
        heap_kind: BootstrapCatalogKind::PgEventTrigger,
        unique: true,
        key_attnums: &PG_EVENT_TRIGGER_EVTNAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3468,
        relation_name: "pg_event_trigger_oid_index",
        heap_kind: BootstrapCatalogKind::PgEventTrigger,
        unique: true,
        key_attnums: &PG_EVENT_TRIGGER_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6110,
        relation_name: "pg_publication_oid_index",
        heap_kind: BootstrapCatalogKind::PgPublication,
        unique: true,
        key_attnums: &PG_PUBLICATION_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6111,
        relation_name: "pg_publication_pubname_index",
        heap_kind: BootstrapCatalogKind::PgPublication,
        unique: true,
        key_attnums: &PG_PUBLICATION_PUBNAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6112,
        relation_name: "pg_publication_rel_oid_index",
        heap_kind: BootstrapCatalogKind::PgPublicationRel,
        unique: true,
        key_attnums: &PG_PUBLICATION_REL_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6113,
        relation_name: "pg_publication_rel_prrelid_prpubid_index",
        heap_kind: BootstrapCatalogKind::PgPublicationRel,
        unique: true,
        key_attnums: &PG_PUBLICATION_REL_PRRELID_PRPUBID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 6116,
        relation_name: "pg_publication_rel_prpubid_index",
        heap_kind: BootstrapCatalogKind::PgPublicationRel,
        unique: false,
        key_attnums: &PG_PUBLICATION_REL_PRPUBID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6238,
        relation_name: "pg_publication_namespace_oid_index",
        heap_kind: BootstrapCatalogKind::PgPublicationNamespace,
        unique: true,
        key_attnums: &PG_PUBLICATION_NAMESPACE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6239,
        relation_name: "pg_publication_namespace_pnnspid_pnpubid_index",
        heap_kind: BootstrapCatalogKind::PgPublicationNamespace,
        unique: true,
        key_attnums: &PG_PUBLICATION_NAMESPACE_PNNSPID_PNPUBID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 6114,
        relation_name: "pg_subscription_oid_index",
        heap_kind: BootstrapCatalogKind::PgSubscription,
        unique: true,
        key_attnums: &PG_SUBSCRIPTION_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6115,
        relation_name: "pg_subscription_subname_index",
        heap_kind: BootstrapCatalogKind::PgSubscription,
        unique: true,
        key_attnums: &PG_SUBSCRIPTION_SUBNAME_INDEX_KEYS,
        opclass_oids: &OID_NAME_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 6246,
        relation_name: "pg_parameter_acl_parname_index",
        heap_kind: BootstrapCatalogKind::PgParameterAcl,
        unique: true,
        key_attnums: &PG_PARAMETER_ACL_PARNAME_INDEX_KEYS,
        opclass_oids: &TEXT_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 6247,
        relation_name: "pg_parameter_acl_oid_index",
        heap_kind: BootstrapCatalogKind::PgParameterAcl,
        unique: true,
        key_attnums: &PG_PARAMETER_ACL_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3257,
        relation_name: "pg_policy_oid_index",
        heap_kind: BootstrapCatalogKind::PgPolicy,
        unique: true,
        key_attnums: &PG_POLICY_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3258,
        relation_name: "pg_policy_polrelid_polname_index",
        heap_kind: BootstrapCatalogKind::PgPolicy,
        unique: true,
        key_attnums: &PG_POLICY_POLRELID_POLNAME_INDEX_KEYS,
        opclass_oids: &OID_NAME_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2651,
        relation_name: "pg_am_name_index",
        heap_kind: BootstrapCatalogKind::PgAm,
        unique: true,
        key_attnums: &PG_AM_NAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2652,
        relation_name: "pg_am_oid_index",
        heap_kind: BootstrapCatalogKind::PgAm,
        unique: true,
        key_attnums: &PG_AM_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2653,
        relation_name: "pg_amop_fam_strat_index",
        heap_kind: BootstrapCatalogKind::PgAmop,
        unique: true,
        key_attnums: &PG_AMOP_FAM_STRAT_INDEX_KEYS,
        opclass_oids: &OID_OID_OID_INT2_CHAR_OPCLASS_5,
    },
    CatalogIndexDescriptor {
        relation_oid: 2654,
        relation_name: "pg_amop_opr_fam_index",
        heap_kind: BootstrapCatalogKind::PgAmop,
        unique: true,
        key_attnums: &PG_AMOP_OPR_FAM_INDEX_KEYS,
        opclass_oids: &OID_CHAR_OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2756,
        relation_name: "pg_amop_oid_index",
        heap_kind: BootstrapCatalogKind::PgAmop,
        unique: true,
        key_attnums: &PG_AMOP_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2655,
        relation_name: "pg_amproc_fam_proc_index",
        heap_kind: BootstrapCatalogKind::PgAmproc,
        unique: true,
        key_attnums: &PG_AMPROC_FAM_PROC_INDEX_KEYS,
        opclass_oids: &OID_OID_OID_INT2_OPCLASS_4,
    },
    CatalogIndexDescriptor {
        relation_oid: 2757,
        relation_name: "pg_amproc_oid_index",
        heap_kind: BootstrapCatalogKind::PgAmproc,
        unique: true,
        key_attnums: &PG_AMPROC_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3574,
        relation_name: "pg_transform_oid_index",
        heap_kind: BootstrapCatalogKind::PgTransform,
        unique: true,
        key_attnums: &PG_TRANSFORM_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3575,
        relation_name: "pg_transform_type_lang_index",
        heap_kind: BootstrapCatalogKind::PgTransform,
        unique: true,
        key_attnums: &PG_TRANSFORM_TYPE_LANG_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2686,
        relation_name: "pg_opclass_am_name_nsp_index",
        heap_kind: BootstrapCatalogKind::PgOpclass,
        unique: true,
        key_attnums: &PG_OPCLASS_AM_NAME_NSP_INDEX_KEYS,
        opclass_oids: &OID_NAME_OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2687,
        relation_name: "pg_opclass_oid_index",
        heap_kind: BootstrapCatalogKind::PgOpclass,
        unique: true,
        key_attnums: &PG_OPCLASS_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2754,
        relation_name: "pg_opfamily_am_name_nsp_index",
        heap_kind: BootstrapCatalogKind::PgOpfamily,
        unique: true,
        key_attnums: &PG_OPFAMILY_AM_NAME_NSP_INDEX_KEYS,
        opclass_oids: &OID_NAME_OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 2755,
        relation_name: "pg_opfamily_oid_index",
        heap_kind: BootstrapCatalogKind::PgOpfamily,
        unique: true,
        key_attnums: &PG_OPFAMILY_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3164,
        relation_name: "pg_collation_name_enc_nsp_index",
        heap_kind: BootstrapCatalogKind::PgCollation,
        unique: true,
        key_attnums: &PG_COLLATION_NAME_ENC_NSP_INDEX_KEYS,
        opclass_oids: &NAME_INT4_OID_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 3085,
        relation_name: "pg_collation_oid_index",
        heap_kind: BootstrapCatalogKind::PgCollation,
        unique: true,
        key_attnums: &PG_COLLATION_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2697,
        relation_name: "pg_tablespace_oid_index",
        heap_kind: BootstrapCatalogKind::PgTablespace,
        unique: true,
        key_attnums: &PG_TABLESPACE_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2698,
        relation_name: "pg_tablespace_spcname_index",
        heap_kind: BootstrapCatalogKind::PgTablespace,
        unique: true,
        key_attnums: &PG_TABLESPACE_SPCNAME_INDEX_KEYS,
        opclass_oids: &NAME_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 2397,
        relation_name: "pg_shdescription_o_c_index",
        heap_kind: BootstrapCatalogKind::PgShdescription,
        unique: true,
        key_attnums: &PG_SHDESCRIPTION_O_C_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 2696,
        relation_name: "pg_statistic_relid_att_inh_index",
        heap_kind: BootstrapCatalogKind::PgStatistic,
        unique: true,
        key_attnums: &PG_STATISTIC_RELID_ATTNUM_INH_INDEX_KEYS,
        opclass_oids: &OID_INT2_BOOL_OPCLASS_3,
    },
    CatalogIndexDescriptor {
        relation_oid: 3379,
        relation_name: "pg_statistic_ext_relid_index",
        heap_kind: BootstrapCatalogKind::PgStatisticExt,
        unique: false,
        key_attnums: &PG_STATISTIC_EXT_RELID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3380,
        relation_name: "pg_statistic_ext_oid_index",
        heap_kind: BootstrapCatalogKind::PgStatisticExt,
        unique: true,
        key_attnums: &PG_STATISTIC_EXT_OID_INDEX_KEYS,
        opclass_oids: &OID_OPCLASS_1,
    },
    CatalogIndexDescriptor {
        relation_oid: 3997,
        relation_name: "pg_statistic_ext_name_index",
        heap_kind: BootstrapCatalogKind::PgStatisticExt,
        unique: true,
        key_attnums: &PG_STATISTIC_EXT_NAME_NSP_INDEX_KEYS,
        opclass_oids: &NAME_OID_OPCLASS_2,
    },
    CatalogIndexDescriptor {
        relation_oid: 3433,
        relation_name: "pg_statistic_ext_data_stxoid_inh_index",
        heap_kind: BootstrapCatalogKind::PgStatisticExtData,
        unique: true,
        key_attnums: &PG_STATISTIC_EXT_DATA_STXOID_INH_INDEX_KEYS,
        opclass_oids: &OID_BOOL_OPCLASS_2,
    },
];

pub fn system_catalog_indexes() -> &'static [CatalogIndexDescriptor] {
    &SYSTEM_CATALOG_INDEXES
}

pub fn system_catalog_indexes_for_heap(
    heap_kind: BootstrapCatalogKind,
) -> impl Iterator<Item = &'static CatalogIndexDescriptor> {
    SYSTEM_CATALOG_INDEXES
        .iter()
        .filter(move |descriptor| descriptor.heap_kind == heap_kind)
}

pub fn system_catalog_index_by_oid(relation_oid: u32) -> Option<&'static CatalogIndexDescriptor> {
    SYSTEM_CATALOG_INDEXES
        .iter()
        .find(|descriptor| descriptor.relation_oid == relation_oid)
}

pub fn system_catalog_index_is_primary(descriptor: &CatalogIndexDescriptor) -> bool {
    matches!(
        descriptor.relation_name,
        "pg_aggregate_fnoid_index"
            | "pg_am_oid_index"
            | "pg_amop_oid_index"
            | "pg_amproc_oid_index"
            | "pg_attrdef_oid_index"
            | "pg_attribute_relid_attnum_index"
            | "pg_auth_members_oid_index"
            | "pg_authid_oid_index"
            | "pg_cast_oid_index"
            | "pg_class_oid_index"
            | "pg_collation_oid_index"
            | "pg_constraint_oid_index"
            | "pg_conversion_oid_index"
            | "pg_database_oid_index"
            | "pg_default_acl_oid_index"
            | "pg_description_o_c_o_index"
            | "pg_event_trigger_oid_index"
            | "pg_extension_oid_index"
            | "pg_foreign_data_wrapper_oid_index"
            | "pg_foreign_server_oid_index"
            | "pg_foreign_table_relid_index"
            | "pg_index_indexrelid_index"
            | "pg_inherits_relid_seqno_index"
            | "pg_largeobject_loid_pn_index"
            | "pg_language_oid_index"
            | "pg_largeobject_metadata_oid_index"
            | "pg_namespace_oid_index"
            | "pg_opclass_oid_index"
            | "pg_operator_oid_index"
            | "pg_opfamily_oid_index"
            | "pg_parameter_acl_oid_index"
            | "pg_partitioned_table_partrelid_index"
            | "pg_policy_oid_index"
            | "pg_proc_oid_index"
            | "pg_publication_namespace_oid_index"
            | "pg_publication_oid_index"
            | "pg_publication_rel_oid_index"
            | "pg_replication_origin_roiident_index"
            | "pg_rewrite_oid_index"
            | "pg_sequence_seqrelid_index"
            | "pg_shdescription_o_c_index"
            | "pg_statistic_ext_data_stxoid_inh_index"
            | "pg_statistic_ext_oid_index"
            | "pg_statistic_relid_att_inh_index"
            | "pg_subscription_oid_index"
            | "pg_tablespace_oid_index"
            | "pg_transform_oid_index"
            | "pg_trigger_oid_index"
            | "pg_ts_config_map_index"
            | "pg_ts_config_oid_index"
            | "pg_ts_dict_oid_index"
            | "pg_ts_parser_oid_index"
            | "pg_ts_template_oid_index"
            | "pg_type_oid_index"
            | "pg_user_mapping_oid_index"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_catalog_indexes_include_pg_aggregate_fnoid_index() {
        let descriptor = system_catalog_index_by_oid(2650).expect("pg_aggregate_fnoid_index");
        assert_eq!(descriptor.relation_name, "pg_aggregate_fnoid_index");
        assert_eq!(descriptor.heap_kind, BootstrapCatalogKind::PgAggregate);
        assert!(descriptor.unique);
    }
}
