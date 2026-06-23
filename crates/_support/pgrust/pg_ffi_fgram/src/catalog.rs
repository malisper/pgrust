use core::ffi::c_char;

use crate::types::{Oid, RelFileNumber, TransactionId};
use crate::{AttrNumber, NameData, RegProcedure};

pub const RELATION_RELATION_ID: Oid = 1259;
pub const ATTRIBUTE_RELATION_ID: Oid = 1249;
pub const INDEX_RELATION_ID: Oid = 2610;
pub const CONSTRAINT_RELATION_ID: Oid = 2606;
pub const NAMESPACE_RELATION_ID: Oid = 2615;
pub const PROCEDURE_RELATION_ID: Oid = 1255;
/// `ProcedureRelationId` — the PostgreSQL spelling of `PROCEDURE_RELATION_ID`
/// (`pg_proc.h`); used by the command crates' `ObjectAddressSet`.
pub const ProcedureRelationId: Oid = 1255;

// ---------------------------------------------------------------------------
// `XxxRelationId` PostgreSQL-spelling catalog-OID aliases used by the command
// crates' `ObjectAddressSet`/`SearchSysCache`/dependency calls (tablecmds.c et
// al.).  Each aliases the canonical `XXX_RELATION_ID` constant above; values
// are from the generated catalog `pg_*_d.h` headers.  (`ProcedureRelationId`
// is defined just above.)
// ---------------------------------------------------------------------------

/// `RelationRelationId` — `pg_class` (`pg_class_d.h`).
pub const RelationRelationId: Oid = RELATION_RELATION_ID;
/// `AttributeRelationId` — `pg_attribute` (`pg_attribute_d.h`).
pub const AttributeRelationId: Oid = ATTRIBUTE_RELATION_ID;
/// `ConstraintRelationId` — `pg_constraint` (`pg_constraint_d.h`).
pub const ConstraintRelationId: Oid = CONSTRAINT_RELATION_ID;
/// `CollationRelationId` — `pg_collation` (`pg_collation_d.h`).
pub const CollationRelationId: Oid = COLLATION_RELATION_ID;
/// `TypeRelationId` — `pg_type` (`pg_type_d.h`).
pub const TypeRelationId: Oid = TYPE_RELATION_ID;
/// `NamespaceRelationId` — `pg_namespace` (`pg_namespace_d.h`).
pub const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;
/// `TableSpaceRelationId` — `pg_tablespace` (`pg_tablespace_d.h`).
pub const TableSpaceRelationId: Oid = TABLE_SPACE_RELATION_ID;
/// `LargeObjectRelationId` — `pg_largeobject` (`pg_largeobject_d.h`).
pub const LargeObjectRelationId: Oid = LARGE_OBJECT_RELATION_ID;
/// `AccessMethodRelationId` — `pg_am` (`pg_am_d.h`).
pub const AccessMethodRelationId: Oid = ACCESS_METHOD_RELATION_ID;
/// `AttrDefaultRelationId` — `pg_attrdef` (`pg_attrdef_d.h`).
pub const AttrDefaultRelationId: Oid = ATTR_DEFAULT_RELATION_ID;
/// `DependRelationId` — `pg_depend` (`pg_depend_d.h`).
pub const DependRelationId: Oid = crate::catalog_dependency::DEPEND_RELATION_ID;
/// `InheritsRelationId` — `pg_inherits` (`pg_inherits_d.h`).
pub const InheritsRelationId: Oid = crate::catalog_dependency::INHERITS_RELATION_ID;
/// `PolicyRelationId` — `pg_policy` (`pg_policy_d.h`).
pub const PolicyRelationId: Oid = POLICY_RELATION_ID;
/// `PublicationRelRelationId` — `pg_publication_rel` (`pg_publication_rel_d.h`).
pub const PublicationRelRelationId: Oid = PUBLICATION_REL_RELATION_ID;
/// `RewriteRelationId` — `pg_rewrite` (`pg_rewrite_d.h`).
pub const RewriteRelationId: Oid = REWRITE_RELATION_ID;
/// `StatisticExtRelationId` — `pg_statistic_ext` (`pg_statistic_ext_d.h`).
pub const StatisticExtRelationId: Oid = STATISTIC_EXT_RELATION_ID;
/// `TriggerRelationId` — `pg_trigger` (`pg_trigger_d.h`).
pub const TriggerRelationId: Oid = TRIGGER_RELATION_ID;
/// `SubscriptionRelationId` — `pg_subscription` (`pg_subscription_d.h`); used by
/// the pg_upgrade_support.c port.
pub const SubscriptionRelationId: Oid = SUBSCRIPTION_RELATION_ID;
/// `ReplicationOriginRelationId` — `pg_replication_origin`
/// (`pg_replication_origin_d.h`); used by the pg_upgrade_support.c port.
pub const ReplicationOriginRelationId: Oid = REPLICATION_ORIGIN_RELATION_ID;
pub const DATABASE_RELATION_ID: Oid = 1262;
pub const LARGE_OBJECT_RELATION_ID: Oid = 2613;
/// `LargeObjectMetadataRelationId` — `pg_largeobject_metadata`
/// (`pg_largeobject_metadata_d.h:23`).
pub const LARGE_OBJECT_METADATA_RELATION_ID: Oid = 2995;
/// `LargeObjectLOidPNIndexId` — `pg_largeobject_loid_pn_index`
/// (`pg_largeobject_d.h:24`): unique btree on `(loid, pageno)`.
pub const LARGE_OBJECT_LOID_PN_INDEX_ID: Oid = 2683;
/// `LargeObjectMetadataOidIndexId` — `pg_largeobject_metadata_oid_index`
/// (`pg_largeobject_metadata_d.h:24`): unique btree on `oid`.
pub const LARGE_OBJECT_METADATA_OID_INDEX_ID: Oid = 2996;
pub const TYPE_RELATION_ID: Oid = 1247;
pub const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = 2328;
pub const FOREIGN_SERVER_RELATION_ID: Oid = 1417;
pub const LANGUAGE_RELATION_ID: Oid = 2612;
pub const TABLE_SPACE_RELATION_ID: Oid = 1213;

pub const FIRST_UNPINNED_OBJECT_ID: Oid = 12_000;
pub const FIRST_GENBKI_OBJECT_ID: Oid = 10_000;
pub const FIRST_NORMAL_OBJECT_ID: Oid = 16_384;
pub const BOOTSTRAP_SUPERUSERID: Oid = 10;

pub const PG_CATALOG_NAMESPACE: Oid = 11;
pub const PG_TOAST_NAMESPACE: Oid = 99;
pub const PG_PUBLIC_NAMESPACE: Oid = 2_200;

pub const CLASS_OID_INDEX_ID: Oid = 2662;
pub const ANUM_PG_CLASS_OID: AttrNumber = 1;

/// `NamespaceOidIndexId` — `pg_namespace_oid_index` (pg_namespace_d.h:25).
pub const NAMESPACE_OID_INDEX_ID: Oid = 2685;

pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
pub const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
pub const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

pub const AUTH_ID_RELATION_ID: Oid = 1260;
pub const AUTH_MEM_RELATION_ID: Oid = 1261;
pub const DB_ROLE_SETTING_RELATION_ID: Oid = 2964;
pub const PARAMETER_ACL_RELATION_ID: Oid = 6243;
pub const REPLICATION_ORIGIN_RELATION_ID: Oid = 6000;
pub const SHARED_DEPEND_RELATION_ID: Oid = 1214;
pub const SHARED_DESCRIPTION_RELATION_ID: Oid = 2396;
pub const SHARED_SEC_LABEL_RELATION_ID: Oid = 3592;
pub const SUBSCRIPTION_RELATION_ID: Oid = 6100;
pub const TABLESPACE_RELATION_ID: Oid = 1213;

// Catalog relation OIDs referenced by backend-catalog-heap (heap.c).  Values
// from the generated *_d.h headers.  (INHERITS_RELATION_ID is in
// catalog_dependency.rs; COLLATION/OPERATOR_CLASS/ACCESS_METHOD are defined
// later in this file.)
pub const STATISTIC_RELATION_ID: Oid = 2619;
pub const FOREIGN_TABLE_RELATION_ID: Oid = 3118;
pub const PARTITIONED_RELATION_ID: Oid = 3350;
pub const SUBSCRIPTION_REL_RELATION_ID: Oid = 6102;

pub const AUTH_ID_ROLNAME_INDEX_ID: Oid = 2676;
pub const AUTH_ID_OID_INDEX_ID: Oid = 2677;
pub const AUTH_MEM_MEM_ROLE_INDEX_ID: Oid = 2695;
pub const AUTH_MEM_ROLE_MEM_INDEX_ID: Oid = 2694;
pub const AUTH_MEM_OID_INDEX_ID: Oid = 6303;
pub const AUTH_MEM_GRANTOR_INDEX_ID: Oid = 6302;
pub const DATABASE_NAME_INDEX_ID: Oid = 2671;
pub const DATABASE_OID_INDEX_ID: Oid = 2672;
pub const DB_ROLE_SETTING_DATID_ROLID_INDEX_ID: Oid = 2965;
pub const PARAMETER_ACL_OID_INDEX_ID: Oid = 6247;
pub const PARAMETER_ACL_PARNAME_INDEX_ID: Oid = 6246;
pub const REPLICATION_ORIGIN_IDENT_INDEX: Oid = 6001;
pub const REPLICATION_ORIGIN_NAME_INDEX: Oid = 6002;
pub const SHARED_DEPEND_DEPENDER_INDEX_ID: Oid = 1232;
pub const SHARED_DEPEND_REFERENCE_INDEX_ID: Oid = 1233;
pub const SHARED_DESCRIPTION_OBJ_INDEX_ID: Oid = 2397;
pub const SEC_LABEL_OBJECT_INDEX_ID: Oid = 3597;
pub const SHARED_SEC_LABEL_OBJECT_INDEX_ID: Oid = 3593;
pub const SUBSCRIPTION_NAME_INDEX_ID: Oid = 6115;
pub const SUBSCRIPTION_OBJECT_INDEX_ID: Oid = 6114;
pub const TABLESPACE_NAME_INDEX_ID: Oid = 2698;
pub const TABLESPACE_OID_INDEX_ID: Oid = 2697;

pub const PG_DATABASE_TOAST_TABLE: Oid = 4177;
pub const PG_DATABASE_TOAST_INDEX: Oid = 4178;
pub const PG_DB_ROLE_SETTING_TOAST_TABLE: Oid = 2966;
pub const PG_DB_ROLE_SETTING_TOAST_INDEX: Oid = 2967;
pub const PG_PARAMETER_ACL_TOAST_TABLE: Oid = 6244;
pub const PG_PARAMETER_ACL_TOAST_INDEX: Oid = 6245;
pub const PG_SHDESCRIPTION_TOAST_TABLE: Oid = 2846;
pub const PG_SHDESCRIPTION_TOAST_INDEX: Oid = 2847;
pub const PG_SHSECLABEL_TOAST_TABLE: Oid = 4060;
pub const PG_SHSECLABEL_TOAST_INDEX: Oid = 4061;
pub const PG_SUBSCRIPTION_TOAST_TABLE: Oid = 4183;
pub const PG_SUBSCRIPTION_TOAST_INDEX: Oid = 4184;
pub const PG_TABLESPACE_TOAST_TABLE: Oid = 4185;
pub const PG_TABLESPACE_TOAST_INDEX: Oid = 4186;

pub const GLOBALTABLESPACE_OID: Oid = 1664;
pub const INVALID_REL_FILE_NUMBER: RelFileNumber = crate::InvalidOid;

// Additional catalog relation OIDs used by the catalog-foundation crates
// (`backend-catalog-{dependency,pg-depend,pg-shdepend,objectaccess}`). Each
// matches the `CATALOG(<rel>,<oid>,<Id>)` macro in the corresponding
// `catalog/pg_*.h` header of PostgreSQL 18.3.

/// `ExtensionRelationId` — `pg_extension` (`pg_extension.h`).
pub const EXTENSION_RELATION_ID: Oid = 3079;
/// `AttrDefaultRelationId` — `pg_attrdef` (`pg_attrdef.h`).
pub const ATTR_DEFAULT_RELATION_ID: Oid = 2604;
/// `OperatorRelationId` — `pg_operator` (`pg_operator.h`).
pub const OPERATOR_RELATION_ID: Oid = 2617;
/// `OID_RANGE_INTERSECT_RANGE_OP` — the `range * range` intersect operator
/// (`pg_operator.dat`), used by `FindFKPeriodOpers` for temporal FKs.
pub const OID_RANGE_INTERSECT_RANGE_OP: Oid = 3900;
/// `OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP` — the `multirange * multirange`
/// intersect operator (`pg_operator.dat`).
pub const OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP: Oid = 4394;
/// `RewriteRelationId` — `pg_rewrite` (`pg_rewrite.h`).
pub const REWRITE_RELATION_ID: Oid = 2618;
/// `TriggerRelationId` — `pg_trigger` (`pg_trigger.h`).
pub const TRIGGER_RELATION_ID: Oid = 2620;
/// `StatisticExtRelationId` — `pg_statistic_ext` (`pg_statistic_ext.h`).
pub const STATISTIC_EXT_RELATION_ID: Oid = 3381;
/// `TSConfigRelationId` — `pg_ts_config` (`pg_ts_config.h`).
pub const TS_CONFIG_RELATION_ID: Oid = 3602;
/// `PolicyRelationId` — `pg_policy` (`pg_policy.h`).
pub const POLICY_RELATION_ID: Oid = 3256;
/// `PublicationNamespaceRelationId` — `pg_publication_namespace`.
pub const PUBLICATION_NAMESPACE_RELATION_ID: Oid = 6237;
/// `PublicationRelRelationId` — `pg_publication_rel` (`pg_publication_rel.h`).
pub const PUBLICATION_REL_RELATION_ID: Oid = 6106;
/// `PublicationRelationId` — `pg_publication` (`pg_publication.h`).
pub const PUBLICATION_RELATION_ID: Oid = 6104;
/// `CastRelationId` — `pg_cast` (`pg_cast.h`).
pub const CAST_RELATION_ID: Oid = 2605;
/// `CollationRelationId` — `pg_collation` (`pg_collation_d.h:23`).
pub const COLLATION_RELATION_ID: Oid = 3456;
/// `ConversionRelationId` — `pg_conversion` (`pg_conversion.h`).
pub const CONVERSION_RELATION_ID: Oid = 2607;
/// `OperatorClassRelationId` — `pg_opclass` (`pg_opclass_d.h:23`).
pub const OPERATOR_CLASS_RELATION_ID: Oid = 2616;
/// `OperatorFamilyRelationId` — `pg_opfamily` (`pg_opfamily.h`).
pub const OPERATOR_FAMILY_RELATION_ID: Oid = 2753;
/// `AccessMethodRelationId` — `pg_am` (`pg_am_d.h:23`).
pub const ACCESS_METHOD_RELATION_ID: Oid = 2601;
/// `AccessMethodOperatorRelationId` — `pg_amop` (`pg_amop.h`).
pub const ACCESS_METHOD_OPERATOR_RELATION_ID: Oid = 2602;
/// `AccessMethodProcedureRelationId` — `pg_amproc` (`pg_amproc.h`).
pub const ACCESS_METHOD_PROCEDURE_RELATION_ID: Oid = 2603;
/// `TSParserRelationId` — `pg_ts_parser` (`pg_ts_parser.h`).
pub const TS_PARSER_RELATION_ID: Oid = 3601;
/// `TSDictionaryRelationId` — `pg_ts_dict` (`pg_ts_dict.h`).
pub const TS_DICTIONARY_RELATION_ID: Oid = 3600;
/// `TSTemplateRelationId` — `pg_ts_template` (`pg_ts_template.h`).
pub const TS_TEMPLATE_RELATION_ID: Oid = 3764;
/// `TSConfigMapRelationId` — `pg_ts_config_map` (`pg_ts_config_map.h`).
pub const TS_CONFIG_MAP_RELATION_ID: Oid = 3603;
/// `UserMappingRelationId` — `pg_user_mapping` (`pg_user_mapping.h`).
pub const USER_MAPPING_RELATION_ID: Oid = 1418;
/// `DefaultAclRelationId` — `pg_default_acl` (`pg_default_acl.h`).
pub const DEFAULT_ACL_RELATION_ID: Oid = 826;
/// `EventTriggerRelationId` — `pg_event_trigger` (`pg_event_trigger.h`).
pub const EVENT_TRIGGER_RELATION_ID: Oid = 3466;
/// `TransformRelationId` — `pg_transform` (`pg_transform.h`).
pub const TRANSFORM_RELATION_ID: Oid = 3576;

/// `DEFAULTTABLESPACE_OID` — `pg_default` tablespace (`pg_tablespace.dat`).
pub const DEFAULTTABLESPACE_OID: Oid = 1663;

// `reg*` pseudo-type OIDs (`catalog/pg_type.dat`).  `dependency.c`'s
// `find_expr_references_walker` switches on these when a `Const` holds a
// registered-object OID, to record the implied dependency on the referenced
// catalog object.
/// `REGPROCOID` — `regproc`.
pub const REGPROCOID: Oid = 24;
/// `REGPROCEDUREOID` — `regprocedure`.
pub const REGPROCEDUREOID: Oid = 2202;
/// `REGOPEROID` — `regoper`.
pub const REGOPEROID: Oid = 2203;
/// `REGOPERATOROID` — `regoperator`.
pub const REGOPERATOROID: Oid = 2204;
/// `REGCLASSOID` — `regclass`.
pub const REGCLASSOID: Oid = 2205;
/// `REGCOLLATIONOID` — `regcollation`.
pub const REGCOLLATIONOID: Oid = 4191;
/// `REGTYPEOID` — `regtype`.
pub const REGTYPEOID: Oid = 2206;
/// `REGROLEOID` — `regrole`.
pub const REGROLEOID: Oid = 4096;
/// `REGNAMESPACEOID` — `regnamespace`.
pub const REGNAMESPACEOID: Oid = 4089;
/// `REGCONFIGOID` — `regconfig`.
pub const REGCONFIGOID: Oid = 3734;
/// `REGDICTIONARYOID` — `regdictionary`.
pub const REGDICTIONARYOID: Oid = 3769;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelKind(u8);

impl RelKind {
    pub const RELATION: Self = Self(b'r');
    pub const INDEX: Self = Self(b'i');
    pub const SEQUENCE: Self = Self(b'S');
    pub const TOASTVALUE: Self = Self(b't');
    pub const VIEW: Self = Self(b'v');
    pub const MATVIEW: Self = Self(b'm');
    pub const COMPOSITE_TYPE: Self = Self(b'c');
    pub const FOREIGN_TABLE: Self = Self(b'f');
    pub const PARTITIONED_TABLE: Self = Self(b'p');
    pub const PARTITIONED_INDEX: Self = Self(b'I');

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn as_byte(self) -> u8 {
        self.0
    }
}

impl From<u8> for RelKind {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl From<RelKind> for u8 {
    fn from(value: RelKind) -> Self {
        value.as_byte()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_class {
    pub oid: Oid,
    pub relname: NameData,
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relowner: Oid,
    pub relam: Oid,
    pub relfilenode: Oid,
    pub reltablespace: Oid,
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub reltoastrelid: Oid,
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: c_char,
    pub relkind: c_char,
    pub relnatts: i16,
    pub relchecks: i16,
    pub relhasrules: bool,
    pub relhastriggers: bool,
    pub relhassubclass: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relispopulated: bool,
    pub relreplident: c_char,
    pub relispartition: bool,
    pub relrewrite: Oid,
    pub relfrozenxid: TransactionId,
    pub relminmxid: TransactionId,
}

pub type Form_pg_class = *mut FormData_pg_class;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct int2vector {
    pub vl_len_: i32,
    pub ndim: i32,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: i32,
    pub lbound1: i32,
    pub values: [i16; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_index {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    pub indkey: int2vector,
}

pub type Form_pg_index = *mut FormData_pg_index;

/// `CONSTRAINT_FOREIGN` (`catalog/pg_constraint.h`) — `contype` code for a
/// foreign-key constraint.
pub const CONSTRAINT_FOREIGN: c_char = b'f' as c_char;

/// Fixed-length prefix of a `pg_constraint` catalog tuple
/// (`catalog/pg_constraint.h`: `FormData_pg_constraint`), through `conrelid`.
///
/// `inval.c`'s `CacheInvalidateHeapTupleCommon` reads only `contype` and
/// `conrelid` via `GETSTRUCT`; the remaining fixed columns are present so the
/// `#[repr(C)]` offsets of those two fields match PostgreSQL exactly.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_constraint {
    pub oid: Oid,
    pub conname: NameData,
    pub connamespace: Oid,
    pub contype: c_char,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: Oid,
    pub contypid: Oid,
    pub conindid: Oid,
    pub conparentid: Oid,
    pub confrelid: Oid,
    pub confupdtype: c_char,
    pub confdeltype: c_char,
    pub confmatchtype: c_char,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
}

pub type Form_pg_constraint = *mut FormData_pg_constraint;

/// `pg_enum` relation OID (`catalog/pg_enum.h`: `CATALOG(pg_enum,3501,...)`).
pub const ENUM_RELATION_ID: Oid = 3501;
/// `EnumOidIndexId` — `pg_enum_oid_index` (`pg_enum.h:46`).
pub const ENUM_OID_INDEX_ID: Oid = 3502;
/// `EnumTypIdLabelIndexId` — `pg_enum_typid_label_index` (`pg_enum.h:47`).
pub const ENUM_TYP_ID_LABEL_INDEX_ID: Oid = 3503;
/// `EnumTypIdSortOrderIndexId` — `pg_enum_typid_sortorder_index` (`pg_enum.h:48`),
/// the index `enum.c`'s `enum_endpoint` / `enum_range_internal` scan in order.
pub const ENUM_TYP_ID_SORT_ORDER_INDEX_ID: Oid = 3534;

/// `Anum_pg_enum_oid` (`pg_enum_d.h`).
pub const ANUM_PG_ENUM_OID: AttrNumber = 1;
/// `Anum_pg_enum_enumtypid` (`pg_enum_d.h`) — the scan-key attribute used by
/// `enum_endpoint` / `enum_range_internal`.
pub const ANUM_PG_ENUM_ENUMTYPID: AttrNumber = 2;
/// `Anum_pg_enum_enumsortorder` (`pg_enum_d.h`).
pub const ANUM_PG_ENUM_ENUMSORTORDER: AttrNumber = 3;
/// `Anum_pg_enum_enumlabel` (`pg_enum_d.h`).
pub const ANUM_PG_ENUM_ENUMLABEL: AttrNumber = 4;

/// On-disk layout of a `pg_enum` catalog tuple
/// (`catalog/pg_enum.h`: `FormData_pg_enum`).  Exact `#[repr(C)]` ABI match —
/// `GETSTRUCT` casts a heap tuple's data to this; the comparison/I/O functions in
/// `enum.c` read `oid`, `enumtypid` and `enumlabel` from it.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_enum {
    /// `oid` — the OID stored in user tables for this enum value.
    pub oid: Oid,
    /// `enumtypid` — OID of the owning enum type.
    pub enumtypid: Oid,
    /// `enumsortorder` — `float4` sort position of this enum value.
    pub enumsortorder: f32,
    /// `enumlabel` — text representation of the enum value.
    pub enumlabel: NameData,
}

pub type Form_pg_enum = *mut FormData_pg_enum;

/// `Natts_pg_enum` (`pg_enum_d.h`) — number of attributes in `pg_enum`.
pub const NATTS_PG_ENUM: usize = 4;

/// `MAX_CATALOG_MULTI_INSERT_BYTES` (`catalog/indexing.h`) — the target byte
/// budget per `CatalogTuplesMultiInsertWithInfo` batch.  `pg_enum.c`'s
/// `EnumValuesCreate` divides this by `sizeof(FormData_pg_enum)` to size its
/// slot array.
pub const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

/* ===========================================================================
 * Relation / index / TOAST OIDs for the catalog-insert helper crates
 * (`backend-catalog-pg-{cast,range,conversion,collation,attrdef,enum,
 * aggregate,operator,type,constraint}`).  Each value matches the
 * `CATALOG(<rel>,<oid>,<Id>)` / `DECLARE_*INDEX*(...,<oid>,<Id>,...)` /
 * `DECLARE_TOAST(<rel>,<oid>,<index>)` macros in the corresponding
 * `catalog/pg_*.h` header of PostgreSQL 18.3.
 * ========================================================================= */

/// `TypeRelationId` — `pg_type` (`pg_type.h:36`).
pub const TYPE_RELATION_ID_CATALOG: Oid = 1247;
/// `TypeRelation_Rowtype_Id` — `BKI_ROWTYPE_OID(71,...)` (`pg_type.h:36`).
pub const TYPE_RELATION_ROWTYPE_ID: Oid = 71;
/// `TypeOidIndexId` — `pg_type_oid_index` (`pg_type.h:265`).
pub const TYPE_OID_INDEX_ID: Oid = 2703;
/// `TypeNameNspIndexId` — `pg_type_typname_nsp_index` (`pg_type.h:266`).
pub const TYPE_NAME_NSP_INDEX_ID: Oid = 2704;
/// `pg_type` TOAST table OID (`DECLARE_TOAST(pg_type, 4171, 4172)`).
pub const PG_TYPE_TOAST_TABLE: Oid = 4171;
/// `pg_type` TOAST index OID.
pub const PG_TYPE_TOAST_INDEX: Oid = 4172;

/// `ConstraintNameNspIndexId` — `pg_constraint_conname_nsp_index` (`pg_constraint.h:179`).
pub const CONSTRAINT_NAME_NSP_INDEX_ID: Oid = 2664;
/// `ConstraintRelidTypidNameIndexId` — `pg_constraint_conrelid_contypid_conname_index`.
pub const CONSTRAINT_RELID_TYPID_NAME_INDEX_ID: Oid = 2665;
/// `ConstraintTypidIndexId` — `pg_constraint_contypid_index`.
pub const CONSTRAINT_TYPID_INDEX_ID: Oid = 2666;
/// `ConstraintOidIndexId` — `pg_constraint_oid_index`.
pub const CONSTRAINT_OID_INDEX_ID: Oid = 2667;
/// `ConstraintParentIndexId` — `pg_constraint_conparentid_index`.
pub const CONSTRAINT_PARENT_INDEX_ID: Oid = 2579;
/// `pg_constraint` TOAST table OID (`DECLARE_TOAST(pg_constraint, 2832, 2833)`).
pub const PG_CONSTRAINT_TOAST_TABLE: Oid = 2832;
/// `pg_constraint` TOAST index OID.
pub const PG_CONSTRAINT_TOAST_INDEX: Oid = 2833;

/// `OperatorOidIndexId` — `pg_operator_oid_index` (`pg_operator.h:85`).
pub const OPERATOR_OID_INDEX_ID: Oid = 2688;
/// `OperatorNameNspIndexId` — `pg_operator_oprname_l_r_n_index` (`pg_operator.h:86`).
pub const OPERATOR_NAME_NSP_INDEX_ID: Oid = 2689;

/// `AggregateRelationId` — `pg_aggregate` (`pg_aggregate.h:32`).
pub const AGGREGATE_RELATION_ID: Oid = 2600;
/// `AggregateFnoidIndexId` — `pg_aggregate_fnoid_index` (`pg_aggregate.h:113`).
pub const AGGREGATE_FNOID_INDEX_ID: Oid = 2650;
/// `pg_aggregate` TOAST table OID (`DECLARE_TOAST(pg_aggregate, 4159, 4160)`).
pub const PG_AGGREGATE_TOAST_TABLE: Oid = 4159;
/// `pg_aggregate` TOAST index OID.
pub const PG_AGGREGATE_TOAST_INDEX: Oid = 4160;

/// `CastOidIndexId` — `pg_cast_oid_index` (`pg_cast.h:59`).
pub const CAST_OID_INDEX_ID: Oid = 2660;
/// `CastSourceTargetIndexId` — `pg_cast_source_target_index` (`pg_cast.h:60`).
pub const CAST_SOURCE_TARGET_INDEX_ID: Oid = 2661;

/// `RangeRelationId` — `pg_range` (`pg_range.h:29`).
pub const RANGE_RELATION_ID: Oid = 3541;
/// `RangeTypidIndexId` — `pg_range_rngtypid_index` (`pg_range.h:60`).
pub const RANGE_TYPID_INDEX_ID: Oid = 3542;
/// `RangeMultirangeTypidIndexId` — `pg_range_rngmultitypid_index` (`pg_range.h:61`).
pub const RANGE_MULTIRANGE_TYPID_INDEX_ID: Oid = 2228;

/// `ConversionDefaultIndexId` — `pg_conversion_default_index` (`pg_conversion.h:63`).
pub const CONVERSION_DEFAULT_INDEX_ID: Oid = 2668;
/// `ConversionNameNspIndexId` — `pg_conversion_name_nsp_index` (`pg_conversion.h:64`).
pub const CONVERSION_NAME_NSP_INDEX_ID: Oid = 2669;
/// `ConversionOidIndexId` — `pg_conversion_oid_index` (`pg_conversion.h:65`).
pub const CONVERSION_OID_INDEX_ID: Oid = 2670;

/// `CollationNameEncNspIndexId` — `pg_collation_name_enc_nsp_index` (`pg_collation.h:62`).
pub const COLLATION_NAME_ENC_NSP_INDEX_ID: Oid = 3164;
/// `CollationOidIndexId` — `pg_collation_oid_index` (`pg_collation.h:63`).
pub const COLLATION_OID_INDEX_ID: Oid = 3085;
/// `pg_collation` TOAST table OID (`DECLARE_TOAST(pg_collation, 6175, 6176)`).
pub const PG_COLLATION_TOAST_TABLE: Oid = 6175;
/// `pg_collation` TOAST index OID.
pub const PG_COLLATION_TOAST_INDEX: Oid = 6176;

/// `AttrDefaultIndexId` — `pg_attrdef_adrelid_adnum_index` (`pg_attrdef.h:53`).
pub const ATTR_DEFAULT_INDEX_ID: Oid = 2656;
/// `AttrDefaultOidIndexId` — `pg_attrdef_oid_index` (`pg_attrdef.h:54`).
pub const ATTR_DEFAULT_OID_INDEX_ID: Oid = 2657;
/// `pg_attrdef` TOAST table OID (`DECLARE_TOAST(pg_attrdef, 2830, 2831)`).
pub const PG_ATTRDEF_TOAST_TABLE: Oid = 2830;
/// `pg_attrdef` TOAST index OID.
pub const PG_ATTRDEF_TOAST_INDEX: Oid = 2831;

/* ===========================================================================
 * pg_type — `FormData_pg_type` (catalog/pg_type.h:36).
 * ========================================================================= */

// `typtype` codes (`TYPTYPE_*`, pg_type.h).
/// `TYPTYPE_BASE` — base type (ordinary scalar type).
/// `TYPTYPE_COMPOSITE` — composite (e.g. table's rowtype).
/// `TYPTYPE_DOMAIN` — domain over another type.
/// `TYPTYPE_ENUM` — enumerated type.
/// `TYPTYPE_MULTIRANGE` — multirange type.
/// `TYPTYPE_PSEUDO` — pseudo-type.
/// `TYPTYPE_RANGE` — range type.

// `typcategory` codes (`TYPCATEGORY_*`, pg_type.h).
/// `TYPCATEGORY_INVALID` — not an allowed category (`'\0'`).
pub const TYPCATEGORY_INVALID: c_char = 0;
/// `TYPCATEGORY_ARRAY`.
pub const TYPCATEGORY_ARRAY: c_char = b'A' as c_char;
/// `TYPCATEGORY_BOOLEAN`.
pub const TYPCATEGORY_BOOLEAN: c_char = b'B' as c_char;
/// `TYPCATEGORY_COMPOSITE`.
pub const TYPCATEGORY_COMPOSITE: c_char = b'C' as c_char;
/// `TYPCATEGORY_DATETIME`.
pub const TYPCATEGORY_DATETIME: c_char = b'D' as c_char;
/// `TYPCATEGORY_ENUM`.
pub const TYPCATEGORY_ENUM: c_char = b'E' as c_char;
/// `TYPCATEGORY_GEOMETRIC`.
pub const TYPCATEGORY_GEOMETRIC: c_char = b'G' as c_char;
/// `TYPCATEGORY_NETWORK` (think INET).
pub const TYPCATEGORY_NETWORK: c_char = b'I' as c_char;
/// `TYPCATEGORY_NUMERIC`.
pub const TYPCATEGORY_NUMERIC: c_char = b'N' as c_char;
/// `TYPCATEGORY_PSEUDOTYPE`.
pub const TYPCATEGORY_PSEUDOTYPE: c_char = b'P' as c_char;
/// `TYPCATEGORY_RANGE`.
pub const TYPCATEGORY_RANGE: c_char = b'R' as c_char;
/// `TYPCATEGORY_STRING`.
pub const TYPCATEGORY_STRING: c_char = b'S' as c_char;
/// `TYPCATEGORY_TIMESPAN`.
pub const TYPCATEGORY_TIMESPAN: c_char = b'T' as c_char;
/// `TYPCATEGORY_USER`.
pub const TYPCATEGORY_USER: c_char = b'U' as c_char;
/// `TYPCATEGORY_BITSTRING` ("varbit").
pub const TYPCATEGORY_BITSTRING: c_char = b'V' as c_char;
/// `TYPCATEGORY_UNKNOWN`.
pub const TYPCATEGORY_UNKNOWN: c_char = b'X' as c_char;
/// `TYPCATEGORY_INTERNAL`.
pub const TYPCATEGORY_INTERNAL: c_char = b'Z' as c_char;

// `typalign` codes (`TYPALIGN_*`) are defined in `crate::heaptuple` and
// re-exported at the crate root; `typstorage` codes (`TYPSTORAGE_*`) likewise.

// `Anum_pg_type_*` attribute numbers (1-based field positions, pg_type_d.h).
pub const ANUM_PG_TYPE_OID: AttrNumber = 1;
pub const ANUM_PG_TYPE_TYPNAME: AttrNumber = 2;
pub const ANUM_PG_TYPE_TYPNAMESPACE: AttrNumber = 3;
pub const ANUM_PG_TYPE_TYPOWNER: AttrNumber = 4;
pub const ANUM_PG_TYPE_TYPLEN: AttrNumber = 5;
pub const ANUM_PG_TYPE_TYPBYVAL: AttrNumber = 6;
pub const ANUM_PG_TYPE_TYPTYPE: AttrNumber = 7;
pub const ANUM_PG_TYPE_TYPCATEGORY: AttrNumber = 8;
pub const ANUM_PG_TYPE_TYPISPREFERRED: AttrNumber = 9;
pub const ANUM_PG_TYPE_TYPISDEFINED: AttrNumber = 10;
pub const ANUM_PG_TYPE_TYPDELIM: AttrNumber = 11;
pub const ANUM_PG_TYPE_TYPRELID: AttrNumber = 12;
pub const ANUM_PG_TYPE_TYPSUBSCRIPT: AttrNumber = 13;
pub const ANUM_PG_TYPE_TYPELEM: AttrNumber = 14;
pub const ANUM_PG_TYPE_TYPARRAY: AttrNumber = 15;
pub const ANUM_PG_TYPE_TYPINPUT: AttrNumber = 16;
pub const ANUM_PG_TYPE_TYPOUTPUT: AttrNumber = 17;
pub const ANUM_PG_TYPE_TYPRECEIVE: AttrNumber = 18;
pub const ANUM_PG_TYPE_TYPSEND: AttrNumber = 19;
pub const ANUM_PG_TYPE_TYPMODIN: AttrNumber = 20;
pub const ANUM_PG_TYPE_TYPMODOUT: AttrNumber = 21;
pub const ANUM_PG_TYPE_TYPANALYZE: AttrNumber = 22;
pub const ANUM_PG_TYPE_TYPALIGN: AttrNumber = 23;
pub const ANUM_PG_TYPE_TYPSTORAGE: AttrNumber = 24;
pub const ANUM_PG_TYPE_TYPNOTNULL: AttrNumber = 25;
pub const ANUM_PG_TYPE_TYPBASETYPE: AttrNumber = 26;
pub const ANUM_PG_TYPE_TYPTYPMOD: AttrNumber = 27;
pub const ANUM_PG_TYPE_TYPNDIMS: AttrNumber = 28;
pub const ANUM_PG_TYPE_TYPCOLLATION: AttrNumber = 29;
pub const ANUM_PG_TYPE_TYPDEFAULTBIN: AttrNumber = 30;
pub const ANUM_PG_TYPE_TYPDEFAULT: AttrNumber = 31;
pub const ANUM_PG_TYPE_TYPACL: AttrNumber = 32;
/// `Natts_pg_type` — number of attributes.
pub const NATTS_PG_TYPE: usize = 32;

/// On-disk fixed-length prefix of a `pg_type` catalog tuple
/// (`catalog/pg_type.h`: `FormData_pg_type`), through `typcollation` — the last
/// fixed-length (non-`CATALOG_VARLEN`) column.  `GETSTRUCT` casts a heap
/// tuple's data to this; `#[repr(C)]` field order/offsets match PostgreSQL.
/// The trailing variable-length columns (`typdefaultbin`, `typdefault`,
/// `typacl`) are not part of the fixed struct and are accessed via the tuple
/// descriptor, exactly as in C.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_type {
    pub oid: Oid,
    pub typname: NameData,
    pub typnamespace: Oid,
    pub typowner: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: c_char,
    pub typcategory: c_char,
    pub typispreferred: bool,
    pub typisdefined: bool,
    pub typdelim: c_char,
    pub typrelid: Oid,
    pub typsubscript: RegProcedure,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typinput: RegProcedure,
    pub typoutput: RegProcedure,
    pub typreceive: RegProcedure,
    pub typsend: RegProcedure,
    pub typmodin: RegProcedure,
    pub typmodout: RegProcedure,
    pub typanalyze: RegProcedure,
    pub typalign: c_char,
    pub typstorage: c_char,
    pub typnotnull: bool,
    pub typbasetype: Oid,
    pub typtypmod: i32,
    pub typndims: i32,
    pub typcollation: Oid,
}

pub type Form_pg_type = *mut FormData_pg_type;

/* ===========================================================================
 * pg_operator — `FormData_pg_operator` (catalog/pg_operator.h:31).
 * ========================================================================= */

/// `oprkind` = `'l'` — prefix (left-unary) operator.
pub const OPRKIND_PREFIX: c_char = b'l' as c_char;
/// `oprkind` = `'b'` — infix (binary) operator.
pub const OPRKIND_INFIX: c_char = b'b' as c_char;

pub const ANUM_PG_OPERATOR_OID: AttrNumber = 1;
pub const ANUM_PG_OPERATOR_OPRNAME: AttrNumber = 2;
pub const ANUM_PG_OPERATOR_OPRNAMESPACE: AttrNumber = 3;
pub const ANUM_PG_OPERATOR_OPROWNER: AttrNumber = 4;
pub const ANUM_PG_OPERATOR_OPRKIND: AttrNumber = 5;
pub const ANUM_PG_OPERATOR_OPRCANMERGE: AttrNumber = 6;
pub const ANUM_PG_OPERATOR_OPRCANHASH: AttrNumber = 7;
pub const ANUM_PG_OPERATOR_OPRLEFT: AttrNumber = 8;
pub const ANUM_PG_OPERATOR_OPRRIGHT: AttrNumber = 9;
pub const ANUM_PG_OPERATOR_OPRRESULT: AttrNumber = 10;
pub const ANUM_PG_OPERATOR_OPRCOM: AttrNumber = 11;
pub const ANUM_PG_OPERATOR_OPRNEGATE: AttrNumber = 12;
pub const ANUM_PG_OPERATOR_OPRCODE: AttrNumber = 13;
pub const ANUM_PG_OPERATOR_OPRREST: AttrNumber = 14;
pub const ANUM_PG_OPERATOR_OPRJOIN: AttrNumber = 15;
/// `Natts_pg_operator`.
pub const NATTS_PG_OPERATOR: usize = 15;

/// On-disk layout of a `pg_operator` catalog tuple
/// (`catalog/pg_operator.h`: `FormData_pg_operator`).  All columns are
/// fixed-length, so this is the complete row.  `#[repr(C)]` ABI match.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_operator {
    pub oid: Oid,
    pub oprname: NameData,
    pub oprnamespace: Oid,
    pub oprowner: Oid,
    pub oprkind: c_char,
    pub oprcanmerge: bool,
    pub oprcanhash: bool,
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprresult: Oid,
    pub oprcom: Oid,
    pub oprnegate: Oid,
    pub oprcode: RegProcedure,
    pub oprrest: RegProcedure,
    pub oprjoin: RegProcedure,
}

pub type Form_pg_operator = *mut FormData_pg_operator;

/* ===========================================================================
 * pg_aggregate — `FormData_pg_aggregate` (catalog/pg_aggregate.h:32).
 * ========================================================================= */

// `aggkind` codes (`AGGKIND_*`, pg_aggregate.h).
/// `AGGKIND_NORMAL` — ordinary aggregate.
/// `AGGKIND_ORDERED_SET` — ordered-set aggregate.
/// `AGGKIND_HYPOTHETICAL` — hypothetical-set aggregate.

// `aggfinalmodify` / `aggmfinalmodify` codes (`AGGMODIFY_*`, pg_aggregate.h).
/// `AGGMODIFY_READ_ONLY`.
/// `AGGMODIFY_SHAREABLE`.
/// `AGGMODIFY_READ_WRITE`.

pub const ANUM_PG_AGGREGATE_AGGFNOID: AttrNumber = 1;
pub const ANUM_PG_AGGREGATE_AGGKIND: AttrNumber = 2;
pub const ANUM_PG_AGGREGATE_AGGNUMDIRECTARGS: AttrNumber = 3;
pub const ANUM_PG_AGGREGATE_AGGTRANSFN: AttrNumber = 4;
pub const ANUM_PG_AGGREGATE_AGGFINALFN: AttrNumber = 5;
pub const ANUM_PG_AGGREGATE_AGGCOMBINEFN: AttrNumber = 6;
pub const ANUM_PG_AGGREGATE_AGGSERIALFN: AttrNumber = 7;
pub const ANUM_PG_AGGREGATE_AGGDESERIALFN: AttrNumber = 8;
pub const ANUM_PG_AGGREGATE_AGGMTRANSFN: AttrNumber = 9;
pub const ANUM_PG_AGGREGATE_AGGMINVTRANSFN: AttrNumber = 10;
pub const ANUM_PG_AGGREGATE_AGGMFINALFN: AttrNumber = 11;
pub const ANUM_PG_AGGREGATE_AGGFINALEXTRA: AttrNumber = 12;
pub const ANUM_PG_AGGREGATE_AGGMFINALEXTRA: AttrNumber = 13;
pub const ANUM_PG_AGGREGATE_AGGFINALMODIFY: AttrNumber = 14;
pub const ANUM_PG_AGGREGATE_AGGMFINALMODIFY: AttrNumber = 15;
pub const ANUM_PG_AGGREGATE_AGGSORTOP: AttrNumber = 16;
pub const ANUM_PG_AGGREGATE_AGGTRANSTYPE: AttrNumber = 17;
pub const ANUM_PG_AGGREGATE_AGGTRANSSPACE: AttrNumber = 18;
pub const ANUM_PG_AGGREGATE_AGGMTRANSTYPE: AttrNumber = 19;
pub const ANUM_PG_AGGREGATE_AGGMTRANSSPACE: AttrNumber = 20;
pub const ANUM_PG_AGGREGATE_AGGINITVAL: AttrNumber = 21;
pub const ANUM_PG_AGGREGATE_AGGMINITVAL: AttrNumber = 22;
/// `Natts_pg_aggregate`.
pub const NATTS_PG_AGGREGATE: usize = 22;

/// On-disk fixed-length prefix of a `pg_aggregate` catalog tuple
/// (`catalog/pg_aggregate.h`: `FormData_pg_aggregate`), through
/// `aggmtransspace` — the last fixed-length column.  `agginitval` /
/// `aggminitval` are `CATALOG_VARLEN` text and accessed via the descriptor.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_aggregate {
    pub aggfnoid: RegProcedure,
    pub aggkind: c_char,
    pub aggnumdirectargs: i16,
    pub aggtransfn: RegProcedure,
    pub aggfinalfn: RegProcedure,
    pub aggcombinefn: RegProcedure,
    pub aggserialfn: RegProcedure,
    pub aggdeserialfn: RegProcedure,
    pub aggmtransfn: RegProcedure,
    pub aggminvtransfn: RegProcedure,
    pub aggmfinalfn: RegProcedure,
    pub aggfinalextra: bool,
    pub aggmfinalextra: bool,
    pub aggfinalmodify: c_char,
    pub aggmfinalmodify: c_char,
    pub aggsortop: Oid,
    pub aggtranstype: Oid,
    pub aggtransspace: i32,
    pub aggmtranstype: Oid,
    pub aggmtransspace: i32,
}

pub type Form_pg_aggregate = *mut FormData_pg_aggregate;

/* ===========================================================================
 * pg_cast — `FormData_pg_cast` (catalog/pg_cast.h:32).
 * ========================================================================= */

// `castcontext` codes (`CoercionCodes`, pg_cast.h) — stored as `char`.
/// `COERCION_CODE_IMPLICIT` — coercion in context of expression.
pub const COERCION_CODE_IMPLICIT: c_char = b'i' as c_char;
/// `COERCION_CODE_ASSIGNMENT` — coercion in context of assignment.
pub const COERCION_CODE_ASSIGNMENT: c_char = b'a' as c_char;
/// `COERCION_CODE_EXPLICIT` — explicit cast operation.
pub const COERCION_CODE_EXPLICIT: c_char = b'e' as c_char;

// `castmethod` codes (`CoercionMethod`, pg_cast.h) — stored as `char`.
/// `COERCION_METHOD_FUNCTION` — use a function.
pub const COERCION_METHOD_FUNCTION: c_char = b'f' as c_char;
/// `COERCION_METHOD_BINARY` — types are binary-compatible.
pub const COERCION_METHOD_BINARY: c_char = b'b' as c_char;
/// `COERCION_METHOD_INOUT` — use input/output functions.
pub const COERCION_METHOD_INOUT: c_char = b'i' as c_char;

// `CoercionContext` (`nodes/primnodes.h`) is defined canonically in
// `commands_parsenodes` (`COERCION_IMPLICIT`/`COERCION_ASSIGNMENT`/
// `COERCION_PLPGSQL`/`COERCION_EXPLICIT`, repr(i32), discriminants 0-3) and
// re-exported from the crate root; do not redefine it here.

// `pg_parameter_acl` attribute numbers / column count
// (`catalog/pg_parameter_acl_d.h`, generated from `pg_parameter_acl.h`).
pub const ANUM_PG_PARAMETER_ACL_OID: AttrNumber = 1;
pub const ANUM_PG_PARAMETER_ACL_PARNAME: AttrNumber = 2;
pub const ANUM_PG_PARAMETER_ACL_PARACL: AttrNumber = 3;
/// `Natts_pg_parameter_acl`.
pub const NATTS_PG_PARAMETER_ACL: usize = 3;

pub const ANUM_PG_CAST_OID: AttrNumber = 1;
pub const ANUM_PG_CAST_CASTSOURCE: AttrNumber = 2;
pub const ANUM_PG_CAST_CASTTARGET: AttrNumber = 3;
pub const ANUM_PG_CAST_CASTFUNC: AttrNumber = 4;
pub const ANUM_PG_CAST_CASTCONTEXT: AttrNumber = 5;
pub const ANUM_PG_CAST_CASTMETHOD: AttrNumber = 6;
/// `Natts_pg_cast`.
pub const NATTS_PG_CAST: usize = 6;

/// On-disk layout of a `pg_cast` catalog tuple
/// (`catalog/pg_cast.h`: `FormData_pg_cast`).  All columns are fixed-length.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_cast {
    pub oid: Oid,
    pub castsource: Oid,
    pub casttarget: Oid,
    pub castfunc: Oid,
    pub castcontext: c_char,
    pub castmethod: c_char,
}

pub type Form_pg_cast = *mut FormData_pg_cast;

/* ===========================================================================
 * pg_range — `FormData_pg_range` (catalog/pg_range.h:29).
 * ========================================================================= */

pub const ANUM_PG_RANGE_RNGTYPID: AttrNumber = 1;
pub const ANUM_PG_RANGE_RNGSUBTYPE: AttrNumber = 2;
pub const ANUM_PG_RANGE_RNGMULTITYPID: AttrNumber = 3;
pub const ANUM_PG_RANGE_RNGCOLLATION: AttrNumber = 4;
pub const ANUM_PG_RANGE_RNGSUBOPC: AttrNumber = 5;
pub const ANUM_PG_RANGE_RNGCANONICAL: AttrNumber = 6;
pub const ANUM_PG_RANGE_RNGSUBDIFF: AttrNumber = 7;
/// `Natts_pg_range`.
pub const NATTS_PG_RANGE: usize = 7;

/// On-disk layout of a `pg_range` catalog tuple
/// (`catalog/pg_range.h`: `FormData_pg_range`).  All columns are fixed-length.
/// Note `pg_range` has no `oid` column (it is keyed on `rngtypid`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_range {
    pub rngtypid: Oid,
    pub rngsubtype: Oid,
    pub rngmultitypid: Oid,
    pub rngcollation: Oid,
    pub rngsubopc: Oid,
    pub rngcanonical: RegProcedure,
    pub rngsubdiff: RegProcedure,
}

pub type Form_pg_range = *mut FormData_pg_range;

/* ===========================================================================
 * pg_conversion — `FormData_pg_conversion` (catalog/pg_conversion.h:29).
 * ========================================================================= */

pub const ANUM_PG_CONVERSION_OID: AttrNumber = 1;
pub const ANUM_PG_CONVERSION_CONNAME: AttrNumber = 2;
pub const ANUM_PG_CONVERSION_CONNAMESPACE: AttrNumber = 3;
pub const ANUM_PG_CONVERSION_CONOWNER: AttrNumber = 4;
pub const ANUM_PG_CONVERSION_CONFORENCODING: AttrNumber = 5;
pub const ANUM_PG_CONVERSION_CONTOENCODING: AttrNumber = 6;
pub const ANUM_PG_CONVERSION_CONPROC: AttrNumber = 7;
pub const ANUM_PG_CONVERSION_CONDEFAULT: AttrNumber = 8;
/// `Natts_pg_conversion`.
pub const NATTS_PG_CONVERSION: usize = 8;

/// On-disk layout of a `pg_conversion` catalog tuple
/// (`catalog/pg_conversion.h`: `FormData_pg_conversion`).  All columns are
/// fixed-length.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_conversion {
    pub oid: Oid,
    pub conname: NameData,
    pub connamespace: Oid,
    pub conowner: Oid,
    pub conforencoding: i32,
    pub contoencoding: i32,
    pub conproc: RegProcedure,
    pub condefault: bool,
}

pub type Form_pg_conversion = *mut FormData_pg_conversion;

/* ===========================================================================
 * pg_collation — `FormData_pg_collation` (catalog/pg_collation.h:29).
 * ========================================================================= */

// `collprovider` codes (`COLLPROVIDER_*`, pg_collation.h) are defined in
// `crate::locale` and re-exported at the crate root.

pub const ANUM_PG_COLLATION_OID: AttrNumber = 1;
pub const ANUM_PG_COLLATION_COLLNAME: AttrNumber = 2;
pub const ANUM_PG_COLLATION_COLLNAMESPACE: AttrNumber = 3;
pub const ANUM_PG_COLLATION_COLLOWNER: AttrNumber = 4;
pub const ANUM_PG_COLLATION_COLLPROVIDER: AttrNumber = 5;
pub const ANUM_PG_COLLATION_COLLISDETERMINISTIC: AttrNumber = 6;
pub const ANUM_PG_COLLATION_COLLENCODING: AttrNumber = 7;
pub const ANUM_PG_COLLATION_COLLCOLLATE: AttrNumber = 8;
pub const ANUM_PG_COLLATION_COLLCTYPE: AttrNumber = 9;
pub const ANUM_PG_COLLATION_COLLLOCALE: AttrNumber = 10;
pub const ANUM_PG_COLLATION_COLLICURULES: AttrNumber = 11;
pub const ANUM_PG_COLLATION_COLLVERSION: AttrNumber = 12;
/// `Natts_pg_collation`.
pub const NATTS_PG_COLLATION: usize = 12;

/// On-disk fixed-length prefix of a `pg_collation` catalog tuple
/// (`catalog/pg_collation.h`: `FormData_pg_collation`), through `collencoding`
/// — the last non-`CATALOG_VARLEN` column.  The text columns
/// (`collcollate`/`collctype`/`colllocale`/`collicurules`/`collversion`) are
/// accessed via the descriptor.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_collation {
    pub oid: Oid,
    pub collname: NameData,
    pub collnamespace: Oid,
    pub collowner: Oid,
    pub collprovider: c_char,
    pub collisdeterministic: bool,
    pub collencoding: i32,
}

pub type Form_pg_collation = *mut FormData_pg_collation;

/* ===========================================================================
 * pg_attrdef — `FormData_pg_attrdef` (catalog/pg_attrdef.h:30).
 * ========================================================================= */

pub const ANUM_PG_ATTRDEF_OID: AttrNumber = 1;
pub const ANUM_PG_ATTRDEF_ADRELID: AttrNumber = 2;
pub const ANUM_PG_ATTRDEF_ADNUM: AttrNumber = 3;
pub const ANUM_PG_ATTRDEF_ADBIN: AttrNumber = 4;
/// `Natts_pg_attrdef`.
pub const NATTS_PG_ATTRDEF: usize = 4;

/// On-disk fixed-length prefix of a `pg_attrdef` catalog tuple
/// (`catalog/pg_attrdef.h`: `FormData_pg_attrdef`), through `adnum` — the last
/// non-`CATALOG_VARLEN` column.  `adbin` (`pg_node_tree`) is accessed via the
/// descriptor.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_attrdef {
    pub oid: Oid,
    pub adrelid: Oid,
    pub adnum: i16,
}

pub type Form_pg_attrdef = *mut FormData_pg_attrdef;

/* ===========================================================================
 * pg_largeobject — `FormData_pg_largeobject` (catalog/pg_largeobject.h:31).
 * ========================================================================= */

/// `Anum_pg_largeobject_loid` (`pg_largeobject_d.h:26`).
pub const ANUM_PG_LARGEOBJECT_LOID: AttrNumber = 1;
/// `Anum_pg_largeobject_pageno` (`pg_largeobject_d.h:27`).
pub const ANUM_PG_LARGEOBJECT_PAGENO: AttrNumber = 2;
/// `Anum_pg_largeobject_data` (`pg_largeobject_d.h:28`).
pub const ANUM_PG_LARGEOBJECT_DATA: AttrNumber = 3;
/// `Natts_pg_largeobject` (`pg_largeobject_d.h:30`).
pub const NATTS_PG_LARGEOBJECT: usize = 3;

/// On-disk fixed-length prefix of a `pg_largeobject` catalog tuple
/// (`catalog/pg_largeobject.h`: `FormData_pg_largeobject`), through `pageno` —
/// the last non-varlena column.  `data` (`bytea`) is accessed via the
/// descriptor (`inv_api.c` does direct `heap_getattr`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_largeobject {
    pub loid: Oid,
    pub pageno: i32,
}

pub type Form_pg_largeobject = *mut FormData_pg_largeobject;

/* ===========================================================================
 * pg_largeobject_metadata — `FormData_pg_largeobject_metadata`
 * (catalog/pg_largeobject_metadata.h:31).
 * ========================================================================= */

/// `Anum_pg_largeobject_metadata_oid` (`pg_largeobject_metadata_d.h:26`).
pub const ANUM_PG_LARGEOBJECT_METADATA_OID: AttrNumber = 1;
/// `Anum_pg_largeobject_metadata_lomowner` (`pg_largeobject_metadata_d.h:27`).
pub const ANUM_PG_LARGEOBJECT_METADATA_LOMOWNER: AttrNumber = 2;
/// `Anum_pg_largeobject_metadata_lomacl` (`pg_largeobject_metadata_d.h:28`).
pub const ANUM_PG_LARGEOBJECT_METADATA_LOMACL: AttrNumber = 3;
/// `Natts_pg_largeobject_metadata` (`pg_largeobject_metadata_d.h:30`).
pub const NATTS_PG_LARGEOBJECT_METADATA: usize = 3;

/// On-disk fixed-length prefix of a `pg_largeobject_metadata` catalog tuple
/// (`catalog/pg_largeobject_metadata.h`: `FormData_pg_largeobject_metadata`),
/// through `lomowner` — the last non-`CATALOG_VARLEN` column.  `lomacl`
/// (`aclitem[]`) is accessed via the descriptor.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_largeobject_metadata {
    pub oid: Oid,
    pub lomowner: Oid,
}

pub type Form_pg_largeobject_metadata = *mut FormData_pg_largeobject_metadata;

/* ===========================================================================
 * pg_constraint — `FormData_pg_constraint` (catalog/pg_constraint.h:31).
 *
 * The fixed-length prefix `FormData_pg_constraint` (oid..conrelid) is declared
 * above for `inval.c`; here we add the `contype` codes, the `ConstraintCategory`
 * lookup enum, the attribute numbers, and a complete fixed-length form
 * (`FormData_pg_constraint_full`) for the catalog-insert helper crate, which
 * needs every fixed column through `conperiod`.
 * ========================================================================= */

// `contype` codes (`CONSTRAINT_*`, pg_constraint.h).  `CONSTRAINT_FOREIGN`
// (`'f'`) is declared above.
/// `CONSTRAINT_CHECK`.
pub const CONSTRAINT_CHECK: c_char = b'c' as c_char;
/// `CONSTRAINT_NOTNULL`.
pub const CONSTRAINT_NOTNULL: c_char = b'n' as c_char;
/// `CONSTRAINT_PRIMARY`.
pub const CONSTRAINT_PRIMARY: c_char = b'p' as c_char;
/// `CONSTRAINT_UNIQUE`.
pub const CONSTRAINT_UNIQUE: c_char = b'u' as c_char;
/// `CONSTRAINT_TRIGGER`.
pub const CONSTRAINT_TRIGGER: c_char = b't' as c_char;
/// `CONSTRAINT_EXCLUSION`.
pub const CONSTRAINT_EXCLUSION: c_char = b'x' as c_char;

/// `ConstraintCategory` (`catalog/pg_constraint.h`) — identifies a constraint
/// type for lookup purposes.  `repr(i32)` matches the C enum.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConstraintCategory {
    /// `CONSTRAINT_RELATION`.
    Relation = 0,
    /// `CONSTRAINT_DOMAIN`.
    Domain = 1,
    /// `CONSTRAINT_ASSERTION` — for future expansion.
    Assertion = 2,
}

pub const ANUM_PG_CONSTRAINT_OID: AttrNumber = 1;
pub const ANUM_PG_CONSTRAINT_CONNAME: AttrNumber = 2;
pub const ANUM_PG_CONSTRAINT_CONNAMESPACE: AttrNumber = 3;
pub const ANUM_PG_CONSTRAINT_CONTYPE: AttrNumber = 4;
pub const ANUM_PG_CONSTRAINT_CONDEFERRABLE: AttrNumber = 5;
pub const ANUM_PG_CONSTRAINT_CONDEFERRED: AttrNumber = 6;
pub const ANUM_PG_CONSTRAINT_CONENFORCED: AttrNumber = 7;
pub const ANUM_PG_CONSTRAINT_CONVALIDATED: AttrNumber = 8;
pub const ANUM_PG_CONSTRAINT_CONRELID: AttrNumber = 9;
pub const ANUM_PG_CONSTRAINT_CONTYPID: AttrNumber = 10;
pub const ANUM_PG_CONSTRAINT_CONINDID: AttrNumber = 11;
pub const ANUM_PG_CONSTRAINT_CONPARENTID: AttrNumber = 12;
pub const ANUM_PG_CONSTRAINT_CONFRELID: AttrNumber = 13;
pub const ANUM_PG_CONSTRAINT_CONFUPDTYPE: AttrNumber = 14;
pub const ANUM_PG_CONSTRAINT_CONFDELTYPE: AttrNumber = 15;
pub const ANUM_PG_CONSTRAINT_CONFMATCHTYPE: AttrNumber = 16;
pub const ANUM_PG_CONSTRAINT_CONISLOCAL: AttrNumber = 17;
pub const ANUM_PG_CONSTRAINT_CONINHCOUNT: AttrNumber = 18;
pub const ANUM_PG_CONSTRAINT_CONNOINHERIT: AttrNumber = 19;
pub const ANUM_PG_CONSTRAINT_CONPERIOD: AttrNumber = 20;
pub const ANUM_PG_CONSTRAINT_CONKEY: AttrNumber = 21;
pub const ANUM_PG_CONSTRAINT_CONFKEY: AttrNumber = 22;
pub const ANUM_PG_CONSTRAINT_CONPFEQOP: AttrNumber = 23;
pub const ANUM_PG_CONSTRAINT_CONPPEQOP: AttrNumber = 24;
pub const ANUM_PG_CONSTRAINT_CONFFEQOP: AttrNumber = 25;
pub const ANUM_PG_CONSTRAINT_CONFDELSETCOLS: AttrNumber = 26;
pub const ANUM_PG_CONSTRAINT_CONEXCLOP: AttrNumber = 27;
pub const ANUM_PG_CONSTRAINT_CONBIN: AttrNumber = 28;
/// `Natts_pg_constraint`.
pub const NATTS_PG_CONSTRAINT: usize = 28;

/// Complete fixed-length form of a `pg_constraint` catalog tuple
/// (`catalog/pg_constraint.h`: `FormData_pg_constraint`), through `conperiod`
/// — the last non-`CATALOG_VARLEN` column.  The shorter
/// [`FormData_pg_constraint`] prefix above is retained for `inval.c`; this
/// `_full` form carries every fixed column the catalog-insert helper reads.
/// The variable-length array columns (`conkey`, `confkey`, `conpfeqop`,
/// `conppeqop`, `conffeqop`, `confdelsetcols`, `conexclop`, `conbin`) are
/// accessed via the tuple descriptor exactly as in C.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_constraint_full {
    pub oid: Oid,
    pub conname: NameData,
    pub connamespace: Oid,
    pub contype: c_char,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: Oid,
    pub contypid: Oid,
    pub conindid: Oid,
    pub conparentid: Oid,
    pub confrelid: Oid,
    pub confupdtype: c_char,
    pub confdeltype: c_char,
    pub confmatchtype: c_char,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
}

/* ===========================================================================
 * pg_foreign_data_wrapper — `FormData_pg_foreign_data_wrapper`
 * (catalog/pg_foreign_data_wrapper.h).  The fixed-length prefix
 * (`GETSTRUCT` result) ends before the `CATALOG_VARLEN` `fdwacl`/`fdwoptions`
 * columns.
 * ========================================================================= */

pub const Anum_pg_foreign_data_wrapper_oid: AttrNumber = 1;
pub const Anum_pg_foreign_data_wrapper_fdwname: AttrNumber = 2;
pub const Anum_pg_foreign_data_wrapper_fdwowner: AttrNumber = 3;
pub const Anum_pg_foreign_data_wrapper_fdwhandler: AttrNumber = 4;
pub const Anum_pg_foreign_data_wrapper_fdwvalidator: AttrNumber = 5;
pub const Anum_pg_foreign_data_wrapper_fdwacl: AttrNumber = 6;
pub const Anum_pg_foreign_data_wrapper_fdwoptions: AttrNumber = 7;
/// `Natts_pg_foreign_data_wrapper`.
pub const Natts_pg_foreign_data_wrapper: usize = 7;

/// On-disk fixed-length prefix of a `pg_foreign_data_wrapper` tuple
/// (`GETSTRUCT`).  The `fdwacl`/`fdwoptions` varlena columns follow.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_foreign_data_wrapper {
    pub oid: Oid,
    pub fdwname: NameData,
    pub fdwowner: Oid,
    pub fdwhandler: Oid,
    pub fdwvalidator: Oid,
}

pub type Form_pg_foreign_data_wrapper = *mut FormData_pg_foreign_data_wrapper;

/* ===========================================================================
 * pg_foreign_server — `FormData_pg_foreign_server`
 * (catalog/pg_foreign_server.h).
 * ========================================================================= */

pub const Anum_pg_foreign_server_oid: AttrNumber = 1;
pub const Anum_pg_foreign_server_srvname: AttrNumber = 2;
pub const Anum_pg_foreign_server_srvowner: AttrNumber = 3;
pub const Anum_pg_foreign_server_srvfdw: AttrNumber = 4;
pub const Anum_pg_foreign_server_srvtype: AttrNumber = 5;
pub const Anum_pg_foreign_server_srvversion: AttrNumber = 6;
pub const Anum_pg_foreign_server_srvacl: AttrNumber = 7;
pub const Anum_pg_foreign_server_srvoptions: AttrNumber = 8;
/// `Natts_pg_foreign_server`.
pub const Natts_pg_foreign_server: usize = 8;

/// On-disk fixed-length prefix of a `pg_foreign_server` tuple (`GETSTRUCT`).
/// The `srvtype`/`srvversion`/`srvacl`/`srvoptions` varlena columns follow.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_foreign_server {
    pub oid: Oid,
    pub srvname: NameData,
    pub srvowner: Oid,
    pub srvfdw: Oid,
}

pub type Form_pg_foreign_server = *mut FormData_pg_foreign_server;

/* ===========================================================================
 * pg_foreign_table — `FormData_pg_foreign_table` (catalog/pg_foreign_table.h).
 * ========================================================================= */

pub const Anum_pg_foreign_table_ftrelid: AttrNumber = 1;
pub const Anum_pg_foreign_table_ftserver: AttrNumber = 2;
pub const Anum_pg_foreign_table_ftoptions: AttrNumber = 3;
/// `Natts_pg_foreign_table`.
pub const Natts_pg_foreign_table: usize = 3;

/// On-disk fixed-length prefix of a `pg_foreign_table` tuple (`GETSTRUCT`).
/// The `ftoptions` varlena column follows.  Note `pg_foreign_table` has no
/// `oid` column (it is keyed on `ftrelid`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_foreign_table {
    pub ftrelid: Oid,
    pub ftserver: Oid,
}

pub type Form_pg_foreign_table = *mut FormData_pg_foreign_table;

/* ===========================================================================
 * pg_user_mapping — `FormData_pg_user_mapping` (catalog/pg_user_mapping.h).
 * ========================================================================= */

pub const Anum_pg_user_mapping_oid: AttrNumber = 1;
pub const Anum_pg_user_mapping_umuser: AttrNumber = 2;
pub const Anum_pg_user_mapping_umserver: AttrNumber = 3;
pub const Anum_pg_user_mapping_umoptions: AttrNumber = 4;
/// `Natts_pg_user_mapping`.
pub const Natts_pg_user_mapping: usize = 4;

/// On-disk fixed-length prefix of a `pg_user_mapping` tuple (`GETSTRUCT`).
/// The `umoptions` varlena column follows.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_user_mapping {
    pub oid: Oid,
    pub umuser: Oid,
    pub umserver: Oid,
}

pub type Form_pg_user_mapping = *mut FormData_pg_user_mapping;

// ---------------------------------------------------------------------------
// pg_db_role_setting (catalog/pg_db_role_setting_d.h) — per-database /
// per-role configuration settings.  Shared, nailed catalog.
// ---------------------------------------------------------------------------

/// `DbRoleSettingRelationId` — `pg_db_role_setting` (`pg_db_role_setting_d.h`).
pub const DbRoleSettingRelationId: Oid = 2964;
/// `DbRoleSettingDatidRolidIndexId` — the `(setdatabase, setrole)` unique index.
pub const DbRoleSettingDatidRolidIndexId: Oid = 2965;
/// `PgDbRoleSettingToastTable` — the catalog's TOAST table.
pub const PgDbRoleSettingToastTable: Oid = 2966;
/// `PgDbRoleSettingToastIndex` — the catalog's TOAST index.
pub const PgDbRoleSettingToastIndex: Oid = 2967;

/// `Anum_pg_db_role_setting_setdatabase` (`pg_db_role_setting_d.h`).
pub const Anum_pg_db_role_setting_setdatabase: AttrNumber = 1;
/// `Anum_pg_db_role_setting_setrole` (`pg_db_role_setting_d.h`).
pub const Anum_pg_db_role_setting_setrole: AttrNumber = 2;
/// `Anum_pg_db_role_setting_setconfig` (`pg_db_role_setting_d.h`).
pub const Anum_pg_db_role_setting_setconfig: AttrNumber = 3;
/// `Natts_pg_db_role_setting` (`pg_db_role_setting_d.h`).
pub const Natts_pg_db_role_setting: usize = 3;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn pg_class_prefix_layout_matches_postgres() {
        assert_eq!(align_of::<FormData_pg_class>(), 4);
        assert_eq!(offset_of!(FormData_pg_class, oid), 0);
        assert_eq!(offset_of!(FormData_pg_class, relname), 4);
        assert_eq!(offset_of!(FormData_pg_class, relnamespace), 68);
        assert_eq!(offset_of!(FormData_pg_class, relminmxid), 140);
        assert_eq!(size_of::<FormData_pg_class>(), 144);
    }

    #[test]
    fn pg_index_prefix_layout_matches_postgres() {
        assert_eq!(offset_of!(FormData_pg_index, indrelid), 4);
        assert_eq!(offset_of!(FormData_pg_index, indnkeyatts), 10);
        assert_eq!(offset_of!(FormData_pg_index, indkey), 24);
    }

    #[test]
    fn pg_constraint_prefix_layout_matches_postgres() {
        // FormData_pg_constraint fixed prefix:
        //   oid(4) + conname(NameData=64) + connamespace(4) + contype(char,1)
        //   + condeferrable + condeferred + conenforced + convalidated (4 bools)
        //   + 3 pad + conrelid(Oid,4).
        assert_eq!(align_of::<FormData_pg_constraint>(), 4);
        assert_eq!(offset_of!(FormData_pg_constraint, oid), 0);
        assert_eq!(offset_of!(FormData_pg_constraint, conname), 4);
        assert_eq!(offset_of!(FormData_pg_constraint, connamespace), 68);
        assert_eq!(offset_of!(FormData_pg_constraint, contype), 72);
        assert_eq!(offset_of!(FormData_pg_constraint, conrelid), 80);
    }

    #[test]
    fn pg_enum_layout_matches_postgres() {
        // FormData_pg_enum: oid(4) + enumtypid(4) + enumsortorder(float4, 4)
        // + enumlabel(NameData, NAMEDATALEN=64) = 76 bytes, 4-byte aligned.
        assert_eq!(align_of::<FormData_pg_enum>(), 4);
        assert_eq!(offset_of!(FormData_pg_enum, oid), 0);
        assert_eq!(offset_of!(FormData_pg_enum, enumtypid), 4);
        assert_eq!(offset_of!(FormData_pg_enum, enumsortorder), 8);
        assert_eq!(offset_of!(FormData_pg_enum, enumlabel), 12);
        assert_eq!(size_of::<FormData_pg_enum>(), 76);
    }

    #[test]
    fn pg_type_fixed_prefix_layout_matches_postgres() {
        // oid(4) + typname(64) + typnamespace(4) + typowner(4) + typlen(int2,2)
        // + typbyval(bool,1) + typtype(char,1) + typcategory(char,1)
        // + typispreferred(bool,1) + typisdefined(bool,1) + typdelim(char,1)
        // + 1 pad + typrelid(Oid,4) + ... + typcollation(Oid,4).
        assert_eq!(align_of::<FormData_pg_type>(), 4);
        assert_eq!(offset_of!(FormData_pg_type, oid), 0);
        assert_eq!(offset_of!(FormData_pg_type, typname), 4);
        assert_eq!(offset_of!(FormData_pg_type, typnamespace), 68);
        assert_eq!(offset_of!(FormData_pg_type, typowner), 72);
        assert_eq!(offset_of!(FormData_pg_type, typlen), 76);
        assert_eq!(offset_of!(FormData_pg_type, typbyval), 78);
        assert_eq!(offset_of!(FormData_pg_type, typtype), 79);
        assert_eq!(offset_of!(FormData_pg_type, typcategory), 80);
        assert_eq!(offset_of!(FormData_pg_type, typdelim), 83);
        assert_eq!(offset_of!(FormData_pg_type, typrelid), 84);
        assert_eq!(offset_of!(FormData_pg_type, typalign), 128);
        assert_eq!(offset_of!(FormData_pg_type, typstorage), 129);
        assert_eq!(offset_of!(FormData_pg_type, typnotnull), 130);
        assert_eq!(offset_of!(FormData_pg_type, typbasetype), 132);
        assert_eq!(offset_of!(FormData_pg_type, typcollation), 144);
        assert_eq!(size_of::<FormData_pg_type>(), 148);
    }

    #[test]
    fn pg_operator_layout_matches_postgres() {
        // oid(4) + oprname(64) + oprnamespace(4) + oprowner(4) + oprkind(char,1)
        // + oprcanmerge(bool,1) + oprcanhash(bool,1) + 1 pad + oprleft(Oid,4)
        // + oprright + oprresult + oprcom + oprnegate + oprcode + oprrest + oprjoin.
        assert_eq!(align_of::<FormData_pg_operator>(), 4);
        assert_eq!(offset_of!(FormData_pg_operator, oid), 0);
        assert_eq!(offset_of!(FormData_pg_operator, oprname), 4);
        assert_eq!(offset_of!(FormData_pg_operator, oprnamespace), 68);
        assert_eq!(offset_of!(FormData_pg_operator, oprkind), 76);
        assert_eq!(offset_of!(FormData_pg_operator, oprleft), 80);
        assert_eq!(offset_of!(FormData_pg_operator, oprcode), 100);
        assert_eq!(offset_of!(FormData_pg_operator, oprrest), 104);
        assert_eq!(offset_of!(FormData_pg_operator, oprjoin), 108);
        assert_eq!(size_of::<FormData_pg_operator>(), 112);
    }

    #[test]
    fn pg_aggregate_fixed_prefix_layout_matches_postgres() {
        // aggfnoid(4) + aggkind(char,1) + 1 pad + aggnumdirectargs(int2,2)
        // + aggtransfn(4) ... 9 regprocs, 2 bools, 2 chars, then 4-byte cols.
        assert_eq!(align_of::<FormData_pg_aggregate>(), 4);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggfnoid), 0);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggkind), 4);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggnumdirectargs), 6);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggtransfn), 8);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggfinalextra), 40);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggmfinalextra), 41);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggfinalmodify), 42);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggmfinalmodify), 43);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggsortop), 44);
        assert_eq!(offset_of!(FormData_pg_aggregate, aggmtransspace), 60);
        assert_eq!(size_of::<FormData_pg_aggregate>(), 64);
    }

    #[test]
    fn pg_cast_layout_matches_postgres() {
        // oid(4) + castsource(4) + casttarget(4) + castfunc(4)
        // + castcontext(char,1) + castmethod(char,1) + 2 pad.
        assert_eq!(align_of::<FormData_pg_cast>(), 4);
        assert_eq!(offset_of!(FormData_pg_cast, oid), 0);
        assert_eq!(offset_of!(FormData_pg_cast, castsource), 4);
        assert_eq!(offset_of!(FormData_pg_cast, casttarget), 8);
        assert_eq!(offset_of!(FormData_pg_cast, castfunc), 12);
        assert_eq!(offset_of!(FormData_pg_cast, castcontext), 16);
        assert_eq!(offset_of!(FormData_pg_cast, castmethod), 17);
        assert_eq!(size_of::<FormData_pg_cast>(), 20);
    }

    #[test]
    fn pg_range_layout_matches_postgres() {
        // 5 Oid + 2 regproc, all 4-byte = 28 bytes.  No oid column.
        assert_eq!(align_of::<FormData_pg_range>(), 4);
        assert_eq!(offset_of!(FormData_pg_range, rngtypid), 0);
        assert_eq!(offset_of!(FormData_pg_range, rngsubdiff), 24);
        assert_eq!(size_of::<FormData_pg_range>(), 28);
    }

    #[test]
    fn pg_conversion_layout_matches_postgres() {
        // oid(4) + conname(64) + connamespace(4) + conowner(4)
        // + conforencoding(int4,4) + contoencoding(int4,4) + conproc(4)
        // + condefault(bool,1) + 3 pad.
        assert_eq!(align_of::<FormData_pg_conversion>(), 4);
        assert_eq!(offset_of!(FormData_pg_conversion, oid), 0);
        assert_eq!(offset_of!(FormData_pg_conversion, conname), 4);
        assert_eq!(offset_of!(FormData_pg_conversion, connamespace), 68);
        assert_eq!(offset_of!(FormData_pg_conversion, conproc), 84);
        assert_eq!(offset_of!(FormData_pg_conversion, condefault), 88);
        assert_eq!(size_of::<FormData_pg_conversion>(), 92);
    }

    #[test]
    fn pg_collation_fixed_prefix_layout_matches_postgres() {
        // oid(4) + collname(64) + collnamespace(4) + collowner(4)
        // + collprovider(char,1) + collisdeterministic(bool,1) + 2 pad
        // + collencoding(int4,4).
        assert_eq!(align_of::<FormData_pg_collation>(), 4);
        assert_eq!(offset_of!(FormData_pg_collation, oid), 0);
        assert_eq!(offset_of!(FormData_pg_collation, collname), 4);
        assert_eq!(offset_of!(FormData_pg_collation, collnamespace), 68);
        assert_eq!(offset_of!(FormData_pg_collation, collprovider), 76);
        assert_eq!(offset_of!(FormData_pg_collation, collisdeterministic), 77);
        assert_eq!(offset_of!(FormData_pg_collation, collencoding), 80);
        assert_eq!(size_of::<FormData_pg_collation>(), 84);
    }

    #[test]
    fn pg_attrdef_fixed_prefix_layout_matches_postgres() {
        // oid(4) + adrelid(4) + adnum(int2,2).
        assert_eq!(align_of::<FormData_pg_attrdef>(), 4);
        assert_eq!(offset_of!(FormData_pg_attrdef, oid), 0);
        assert_eq!(offset_of!(FormData_pg_attrdef, adrelid), 4);
        assert_eq!(offset_of!(FormData_pg_attrdef, adnum), 8);
    }

    #[test]
    fn pg_constraint_full_fixed_prefix_layout_matches_postgres() {
        // oid(4) + conname(64) + connamespace(4) + contype(char,1)
        // + 4 bools + 3 pad + conrelid(Oid,4) + contypid + conindid
        // + conparentid + confrelid + confupdtype(char,1) + confdeltype(char,1)
        // + confmatchtype(char,1) + conislocal(bool,1) + coninhcount(int2,2)
        // + connoinherit(bool,1) + conperiod(bool,1).
        assert_eq!(align_of::<FormData_pg_constraint_full>(), 4);
        assert_eq!(offset_of!(FormData_pg_constraint_full, oid), 0);
        assert_eq!(offset_of!(FormData_pg_constraint_full, conname), 4);
        assert_eq!(offset_of!(FormData_pg_constraint_full, connamespace), 68);
        assert_eq!(offset_of!(FormData_pg_constraint_full, contype), 72);
        assert_eq!(offset_of!(FormData_pg_constraint_full, conrelid), 80);
        assert_eq!(offset_of!(FormData_pg_constraint_full, contypid), 84);
        assert_eq!(offset_of!(FormData_pg_constraint_full, confrelid), 96);
        assert_eq!(offset_of!(FormData_pg_constraint_full, confupdtype), 100);
        assert_eq!(offset_of!(FormData_pg_constraint_full, conislocal), 103);
        assert_eq!(offset_of!(FormData_pg_constraint_full, coninhcount), 104);
        assert_eq!(offset_of!(FormData_pg_constraint_full, connoinherit), 106);
        assert_eq!(offset_of!(FormData_pg_constraint_full, conperiod), 107);
        assert_eq!(size_of::<FormData_pg_constraint_full>(), 108);
    }
}
