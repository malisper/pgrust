pub const PG_CATALOG_NAMESPACE_OID: u32 = 11;
pub const PG_TOAST_NAMESPACE_OID: u32 = 99;
pub const PUBLIC_NAMESPACE_OID: u32 = 2200;
pub const INFORMATION_SCHEMA_NAMESPACE_OID: u32 = 13244;

pub const PG_TYPE_RELATION_OID: u32 = 1247;
pub const PG_DATABASE_ROWTYPE_OID: u32 = 1248;
pub const PG_ATTRIBUTE_RELATION_OID: u32 = 1249;
pub const PG_PROC_RELATION_OID: u32 = 1255;
pub const PG_DEFAULT_ACL_RELATION_OID: u32 = 826;
pub const PG_TS_DICT_RELATION_OID: u32 = 3600;
pub const PG_TS_PARSER_RELATION_OID: u32 = 3601;
pub const PG_TS_CONFIG_RELATION_OID: u32 = 3602;
pub const PG_TS_CONFIG_MAP_RELATION_OID: u32 = 3603;
pub const PG_PROC_ROWTYPE_OID: u32 = 81;
pub const PG_CLASS_RELATION_OID: u32 = 1259;
pub const PG_AUTHID_RELATION_OID: u32 = 1260;
pub const PG_AUTH_MEMBERS_RELATION_OID: u32 = 1261;
pub const PG_DATABASE_RELATION_OID: u32 = 1262;
pub const PG_EXTENSION_RELATION_OID: u32 = 3079;
pub const PG_EVENT_TRIGGER_RELATION_OID: u32 = 3466;
pub const PG_COLLATION_RELATION_OID: u32 = 3456;
pub const PG_LARGEOBJECT_RELATION_OID: u32 = 2613;
pub const PG_LARGEOBJECT_METADATA_RELATION_OID: u32 = 2995;
pub const PG_TABLESPACE_RELATION_OID: u32 = 1213;
pub const PG_SHDEPEND_RELATION_OID: u32 = 1214;
pub const PG_SHDESCRIPTION_RELATION_OID: u32 = 2396;
pub const PG_REPLICATION_ORIGIN_RELATION_OID: u32 = 6000;
pub const PG_AM_RELATION_OID: u32 = 2601;
pub const PG_AMOP_RELATION_OID: u32 = 2602;
pub const PG_AMPROC_RELATION_OID: u32 = 2603;
pub const PG_TRANSFORM_RELATION_OID: u32 = 3576;
pub const PG_ATTRDEF_RELATION_OID: u32 = 2604;
pub const PG_CAST_RELATION_OID: u32 = 2605;
pub const PG_CONSTRAINT_RELATION_OID: u32 = 2606;
pub const PG_CONVERSION_RELATION_OID: u32 = 2607;
pub const PG_AGGREGATE_RELATION_OID: u32 = 2600;
pub const PG_DEPEND_RELATION_OID: u32 = 2608;
pub const PG_DESCRIPTION_RELATION_OID: u32 = 2609;
pub const PG_FOREIGN_DATA_WRAPPER_RELATION_OID: u32 = 2328;
pub const PG_FOREIGN_SERVER_RELATION_OID: u32 = 1417;
pub const PG_USER_MAPPING_RELATION_OID: u32 = 1418;
pub const PG_FOREIGN_TABLE_RELATION_OID: u32 = 3118;
pub const PG_INDEX_RELATION_OID: u32 = 2610;
pub const PG_INHERITS_RELATION_OID: u32 = 2611;
pub const PG_PARTITIONED_TABLE_RELATION_OID: u32 = 3350;
pub const PG_REWRITE_RELATION_OID: u32 = 2618;
pub const PG_STATISTIC_RELATION_OID: u32 = 2619;
pub const PG_STATISTIC_EXT_RELATION_OID: u32 = 3381;
pub const PG_STATISTIC_EXT_DATA_RELATION_OID: u32 = 3429;
pub const PG_TRIGGER_RELATION_OID: u32 = 2620;
pub const PG_POLICY_RELATION_OID: u32 = 3256;
pub const PG_LANGUAGE_RELATION_OID: u32 = 2612;
pub const PG_NAMESPACE_RELATION_OID: u32 = 2615;
pub const PG_OPCLASS_RELATION_OID: u32 = 2616;
pub const PG_OPERATOR_RELATION_OID: u32 = 2617;
pub const PG_SEQUENCE_RELATION_OID: u32 = 2224;
pub const PG_OPFAMILY_RELATION_OID: u32 = 2753;
pub const PG_PUBLICATION_RELATION_OID: u32 = 6104;
pub const PG_PUBLICATION_REL_RELATION_OID: u32 = 6106;
pub const PG_PUBLICATION_NAMESPACE_RELATION_OID: u32 = 6237;
pub const PG_SUBSCRIPTION_RELATION_OID: u32 = 6100;
pub const PG_PARAMETER_ACL_RELATION_OID: u32 = 6243;
pub const PG_TS_TEMPLATE_RELATION_OID: u32 = 3764;

pub const PG_NAMESPACE_ROWTYPE_OID: u32 = 0;
pub const PG_TYPE_ROWTYPE_OID: u32 = 71;
pub const PG_ATTRIBUTE_ROWTYPE_OID: u32 = 75;
pub const PG_CLASS_ROWTYPE_OID: u32 = 83;
pub const PG_AM_ROWTYPE_OID: u32 = 0;
pub const PG_ATTRDEF_ROWTYPE_OID: u32 = 0;
pub const PG_DEPEND_ROWTYPE_OID: u32 = 0;
pub const PG_INDEX_ROWTYPE_OID: u32 = 0;
pub const PG_INHERITS_ROWTYPE_OID: u32 = 0;
pub const PG_REWRITE_ROWTYPE_OID: u32 = 0;
pub const PG_STATISTIC_ROWTYPE_OID: u32 = 10029;
pub const PG_STATISTIC_EXT_ROWTYPE_OID: u32 = 10031;
pub const PG_STATISTIC_EXT_DATA_ROWTYPE_OID: u32 = 10033;
pub const PG_TRIGGER_ROWTYPE_OID: u32 = 0;
pub const PG_EVENT_TRIGGER_ROWTYPE_OID: u32 = 0;
pub const PG_PUBLICATION_ROWTYPE_OID: u32 = 0;
pub const PG_PUBLICATION_REL_ROWTYPE_OID: u32 = 0;
pub const PG_PUBLICATION_NAMESPACE_ROWTYPE_OID: u32 = 0;
pub const PG_PARTITIONED_TABLE_ROWTYPE_OID: u32 = 0;
pub const PG_TYPE_ARRAY_TYPE_OID: u32 = 210;
pub const PG_ATTRIBUTE_ARRAY_TYPE_OID: u32 = 270;
pub const PG_PROC_ARRAY_TYPE_OID: u32 = 272;
pub const PG_CLASS_ARRAY_TYPE_OID: u32 = 273;
pub const PG_DATABASE_ARRAY_TYPE_OID: u32 = 10026;
pub const INFORMATION_SCHEMA_CARDINAL_NUMBER_TYPE_OID: u32 = 13245;
pub const INFORMATION_SCHEMA_CARDINAL_NUMBER_ARRAY_TYPE_OID: u32 = 13246;
pub const INFORMATION_SCHEMA_CHARACTER_DATA_TYPE_OID: u32 = 13247;
pub const INFORMATION_SCHEMA_CHARACTER_DATA_ARRAY_TYPE_OID: u32 = 13248;
pub const INFORMATION_SCHEMA_SQL_IDENTIFIER_TYPE_OID: u32 = 13249;
pub const INFORMATION_SCHEMA_SQL_IDENTIFIER_ARRAY_TYPE_OID: u32 = 13250;
pub const INFORMATION_SCHEMA_TIME_STAMP_TYPE_OID: u32 = 13251;
pub const INFORMATION_SCHEMA_TIME_STAMP_ARRAY_TYPE_OID: u32 = 13252;
pub const INFORMATION_SCHEMA_YES_OR_NO_TYPE_OID: u32 = 13253;
pub const INFORMATION_SCHEMA_YES_OR_NO_ARRAY_TYPE_OID: u32 = 13254;

pub const BOOL_TYPE_OID: u32 = 16;
pub const BYTEA_TYPE_OID: u32 = 17;
pub const UUID_TYPE_OID: u32 = 2950;
pub const INTERNAL_CHAR_TYPE_OID: u32 = 18;
pub const NAME_TYPE_OID: u32 = 19;
pub const BIT_TYPE_OID: u32 = 1560;
pub const VARBIT_TYPE_OID: u32 = 1562;
pub const BOOL_ARRAY_TYPE_OID: u32 = 1000;
pub const BYTEA_ARRAY_TYPE_OID: u32 = 1001;
pub const UUID_ARRAY_TYPE_OID: u32 = 2951;
pub const INTERNAL_CHAR_ARRAY_TYPE_OID: u32 = 1002;
pub const NAME_ARRAY_TYPE_OID: u32 = 1003;
pub const BIT_ARRAY_TYPE_OID: u32 = 1561;
pub const VARBIT_ARRAY_TYPE_OID: u32 = 1563;
pub const INT8_TYPE_OID: u32 = 20;
pub const INT2_TYPE_OID: u32 = 21;
pub const INT2VECTOR_TYPE_OID: u32 = 22;
pub const INT2VECTOR_ARRAY_TYPE_OID: u32 = 1006;
pub const INT4_TYPE_OID: u32 = 23;
pub const INT2_ARRAY_TYPE_OID: u32 = 1005;
pub const INT4_ARRAY_TYPE_OID: u32 = 1007;
pub const TEXT_TYPE_OID: u32 = 25;
pub const REFCURSOR_TYPE_OID: u32 = 1790;
pub const REFCURSOR_ARRAY_TYPE_OID: u32 = 2201;
pub const OID_TYPE_OID: u32 = 26;
pub const REGPROC_TYPE_OID: u32 = 24;
pub const REGOPERATOR_TYPE_OID: u32 = 2204;
pub const REGOPER_TYPE_OID: u32 = 2203;
pub const REGCLASS_TYPE_OID: u32 = 2205;
pub const REGCOLLATION_TYPE_OID: u32 = 4191;
pub const TID_TYPE_OID: u32 = 27;
pub const XID_TYPE_OID: u32 = 28;
pub const CID_TYPE_OID: u32 = 29;
pub const TXID_SNAPSHOT_TYPE_OID: u32 = 2970;
pub const OIDVECTOR_TYPE_OID: u32 = 30;
pub const OIDVECTOR_ARRAY_TYPE_OID: u32 = 1013;
pub const REGTYPE_TYPE_OID: u32 = 2206;
pub const REGROLE_TYPE_OID: u32 = 4096;
pub const REGNAMESPACE_TYPE_OID: u32 = 4089;
pub const REGPROCEDURE_TYPE_OID: u32 = 2202;
pub const REGPROC_ARRAY_TYPE_OID: u32 = 1008;
pub const REGOPER_ARRAY_TYPE_OID: u32 = 2208;
pub const REGOPERATOR_ARRAY_TYPE_OID: u32 = 2209;
pub const REGCLASS_ARRAY_TYPE_OID: u32 = 2210;
pub const REGTYPE_ARRAY_TYPE_OID: u32 = 2211;
pub const REGROLE_ARRAY_TYPE_OID: u32 = 4097;
pub const REGCOLLATION_ARRAY_TYPE_OID: u32 = 4192;
pub const REGNAMESPACE_ARRAY_TYPE_OID: u32 = 4090;
pub const REGPROCEDURE_ARRAY_TYPE_OID: u32 = 2207;
pub const ANYOID: u32 = 2276;
pub const UNKNOWN_TYPE_OID: u32 = 705;
pub const ANYELEMENTOID: u32 = 2283;
pub const ANYNONARRAYOID: u32 = 2776;
pub const ANYARRAYOID: u32 = 2277;
pub const CSTRING_TYPE_OID: u32 = 2275;
pub const CSTRING_ARRAY_TYPE_OID: u32 = 1263;
pub const VOID_TYPE_OID: u32 = 2278;
pub const TRIGGER_TYPE_OID: u32 = 2279;
pub const EVENT_TRIGGER_TYPE_OID: u32 = 3838;
pub const INTERNAL_TYPE_OID: u32 = 2281;
pub const FDW_HANDLER_TYPE_OID: u32 = 3115;
pub const INDEX_AM_HANDLER_TYPE_OID: u32 = 325;
pub const TABLE_AM_HANDLER_TYPE_OID: u32 = 269;
pub const RECORD_TYPE_OID: u32 = 2249;
pub const RECORD_ARRAY_TYPE_OID: u32 = 2287;
pub const PG_STATISTIC_ARRAY_TYPE_OID: u32 = 10028;
pub const PG_STATISTIC_EXT_ARRAY_TYPE_OID: u32 = 10030;
pub const PG_STATISTIC_EXT_DATA_ARRAY_TYPE_OID: u32 = 10032;
pub const TEXT_ARRAY_TYPE_OID: u32 = 1009;
pub const TID_ARRAY_TYPE_OID: u32 = 1010;
pub const XID_ARRAY_TYPE_OID: u32 = 1011;
pub const CID_ARRAY_TYPE_OID: u32 = 1012;
pub const TXID_SNAPSHOT_ARRAY_TYPE_OID: u32 = 2949;
pub const BPCHAR_ARRAY_TYPE_OID: u32 = 1014;
pub const VARCHAR_ARRAY_TYPE_OID: u32 = 1015;
pub const INT8_ARRAY_TYPE_OID: u32 = 1016;
pub const FLOAT4_TYPE_OID: u32 = 700;
pub const FLOAT8_TYPE_OID: u32 = 701;
pub const MONEY_TYPE_OID: u32 = 790;
pub const MONEY_ARRAY_TYPE_OID: u32 = 791;
pub const MACADDR8_TYPE_OID: u32 = 774;
pub const MACADDR8_ARRAY_TYPE_OID: u32 = 775;
pub const MACADDR_TYPE_OID: u32 = 829;
pub const MACADDR_ARRAY_TYPE_OID: u32 = 1040;
pub const CIDR_TYPE_OID: u32 = 650;
pub const CIDR_ARRAY_TYPE_OID: u32 = 651;
pub const INET_TYPE_OID: u32 = 869;
pub const INET_ARRAY_TYPE_OID: u32 = 1041;
pub const POINT_TYPE_OID: u32 = 600;
pub const POINT_ARRAY_TYPE_OID: u32 = 1017;
pub const LSEG_TYPE_OID: u32 = 601;
pub const LSEG_ARRAY_TYPE_OID: u32 = 1018;
pub const PATH_TYPE_OID: u32 = 602;
pub const PATH_ARRAY_TYPE_OID: u32 = 1019;
pub const BOX_TYPE_OID: u32 = 603;
pub const BOX_ARRAY_TYPE_OID: u32 = 1020;
pub const POLYGON_TYPE_OID: u32 = 604;
pub const POLYGON_ARRAY_TYPE_OID: u32 = 1027;
pub const LINE_TYPE_OID: u32 = 628;
pub const LINE_ARRAY_TYPE_OID: u32 = 629;
pub const CIRCLE_TYPE_OID: u32 = 718;
pub const CIRCLE_ARRAY_TYPE_OID: u32 = 719;
pub const FLOAT4_ARRAY_TYPE_OID: u32 = 1021;
pub const FLOAT8_ARRAY_TYPE_OID: u32 = 1022;
pub const VARCHAR_TYPE_OID: u32 = 1043;
pub const BPCHAR_TYPE_OID: u32 = 1042;
pub const ACLITEM_TYPE_OID: u32 = 1033;
pub const ACLITEM_ARRAY_TYPE_OID: u32 = 1034;
pub const DATE_TYPE_OID: u32 = 1082;
pub const DATE_ARRAY_TYPE_OID: u32 = 1182;
pub const TIME_TYPE_OID: u32 = 1083;
pub const TIME_ARRAY_TYPE_OID: u32 = 1183;
pub const TIMESTAMP_TYPE_OID: u32 = 1114;
pub const TIMESTAMP_ARRAY_TYPE_OID: u32 = 1115;
pub const TIMESTAMPTZ_TYPE_OID: u32 = 1184;
pub const TIMESTAMPTZ_ARRAY_TYPE_OID: u32 = 1185;
pub const INTERVAL_TYPE_OID: u32 = 1186;
pub const INTERVAL_ARRAY_TYPE_OID: u32 = 1187;
pub const TIMETZ_TYPE_OID: u32 = 1266;
pub const TIMETZ_ARRAY_TYPE_OID: u32 = 1270;
pub const NUMERIC_TYPE_OID: u32 = 1700;
pub const NUMERIC_ARRAY_TYPE_OID: u32 = 1231;
pub const INT4RANGE_TYPE_OID: u32 = 3904;
pub const INT4RANGE_ARRAY_TYPE_OID: u32 = 3905;
pub const NUMRANGE_TYPE_OID: u32 = 3906;
pub const NUMRANGE_ARRAY_TYPE_OID: u32 = 3907;
pub const TSRANGE_TYPE_OID: u32 = 3908;
pub const TSRANGE_ARRAY_TYPE_OID: u32 = 3909;
pub const TSTZRANGE_TYPE_OID: u32 = 3910;
pub const TSTZRANGE_ARRAY_TYPE_OID: u32 = 3911;
pub const DATERANGE_TYPE_OID: u32 = 3912;
pub const DATERANGE_ARRAY_TYPE_OID: u32 = 3913;
pub const INT8RANGE_TYPE_OID: u32 = 3926;
pub const INT8RANGE_ARRAY_TYPE_OID: u32 = 3927;
pub const ANYRANGEOID: u32 = 3831;
pub const ANYENUMOID: u32 = 3500;
pub const INT4MULTIRANGE_TYPE_OID: u32 = 4451;
pub const NUMMULTIRANGE_TYPE_OID: u32 = 4532;
pub const TSMULTIRANGE_TYPE_OID: u32 = 4533;
pub const TSTZMULTIRANGE_TYPE_OID: u32 = 4534;
pub const DATEMULTIRANGE_TYPE_OID: u32 = 4535;
pub const INT8MULTIRANGE_TYPE_OID: u32 = 4536;
pub const ANYMULTIRANGEOID: u32 = 4537;
pub const ANYCOMPATIBLEOID: u32 = 5077;
pub const ANYCOMPATIBLEARRAYOID: u32 = 5078;
pub const ANYCOMPATIBLENONARRAYOID: u32 = 5079;
pub const ANYCOMPATIBLERANGEOID: u32 = 5080;
pub const ANYCOMPATIBLEMULTIRANGEOID: u32 = 4538;
pub const INT4MULTIRANGE_ARRAY_TYPE_OID: u32 = 6150;
pub const NUMMULTIRANGE_ARRAY_TYPE_OID: u32 = 6151;
pub const TSMULTIRANGE_ARRAY_TYPE_OID: u32 = 6152;
pub const TSTZMULTIRANGE_ARRAY_TYPE_OID: u32 = 6153;
pub const DATEMULTIRANGE_ARRAY_TYPE_OID: u32 = 6155;
pub const INT8MULTIRANGE_ARRAY_TYPE_OID: u32 = 6157;
// :HACK: These are PostgreSQL regression-test compatibility range types, not
// in-core PostgreSQL bootstrap types. Keep their OIDs outside the bootstrap
// range so type_sanity does not treat them as core catalog coverage.
pub const ARRAYRANGE_TYPE_OID: u32 = 60_000;
pub const ARRAYRANGE_ARRAY_TYPE_OID: u32 = 60_001;
pub const ARRAYMULTIRANGE_TYPE_OID: u32 = 60_002;
pub const ARRAYMULTIRANGE_ARRAY_TYPE_OID: u32 = 60_003;
pub const VARBITRANGE_TYPE_OID: u32 = 60_004;
pub const VARBITRANGE_ARRAY_TYPE_OID: u32 = 60_005;
pub const VARBITMULTIRANGE_TYPE_OID: u32 = 60_006;
pub const VARBITMULTIRANGE_ARRAY_TYPE_OID: u32 = 60_007;
pub const JSON_TYPE_OID: u32 = 114;
pub const JSON_ARRAY_TYPE_OID: u32 = 199;
pub const XML_TYPE_OID: u32 = 142;
pub const XML_ARRAY_TYPE_OID: u32 = 143;
pub const PG_NODE_TREE_TYPE_OID: u32 = 194;
pub const PG_SNAPSHOT_TYPE_OID: u32 = 5038;
pub const PG_SNAPSHOT_ARRAY_TYPE_OID: u32 = 5039;
pub const XID8_ARRAY_TYPE_OID: u32 = 271;
pub const XID8_TYPE_OID: u32 = 5069;
pub const PG_NDISTINCT_TYPE_OID: u32 = 3361;
pub const PG_DEPENDENCIES_TYPE_OID: u32 = 3402;
pub const PG_BRIN_BLOOM_SUMMARY_TYPE_OID: u32 = 4600;
pub const PG_BRIN_MINMAX_MULTI_SUMMARY_TYPE_OID: u32 = 4601;
pub const PG_MCV_LIST_TYPE_OID: u32 = 5017;
pub const OID_ARRAY_TYPE_OID: u32 = 1028;
pub const JSONB_TYPE_OID: u32 = 3802;
pub const JSONB_ARRAY_TYPE_OID: u32 = 3807;
pub const JSONPATH_TYPE_OID: u32 = 4072;
pub const JSONPATH_ARRAY_TYPE_OID: u32 = 4073;
pub const TSVECTOR_TYPE_OID: u32 = 3614;
pub const TSVECTOR_ARRAY_TYPE_OID: u32 = 3643;
pub const GTSVECTOR_TYPE_OID: u32 = 3642;
pub const GTSVECTOR_ARRAY_TYPE_OID: u32 = 3644;
pub const TSQUERY_TYPE_OID: u32 = 3615;
pub const TSQUERY_ARRAY_TYPE_OID: u32 = 3645;
pub const REGCONFIG_TYPE_OID: u32 = 3734;
pub const REGCONFIG_ARRAY_TYPE_OID: u32 = 3735;
pub const REGDICTIONARY_TYPE_OID: u32 = 3769;
pub const REGDICTIONARY_ARRAY_TYPE_OID: u32 = 3770;
pub const PG_LSN_TYPE_OID: u32 = 3220;
pub const PG_LSN_ARRAY_TYPE_OID: u32 = 3221;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapCatalogRelation {
    pub oid: u32,
    pub name: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BootstrapCatalogKind {
    PgNamespace,
    PgClass,
    PgAttribute,
    PgType,
    PgProc,
    PgDefaultAcl,
    PgTsParser,
    PgTsTemplate,
    PgTsDict,
    PgTsConfig,
    PgTsConfigMap,
    PgLanguage,
    PgOperator,
    PgDatabase,
    PgExtension,
    PgEventTrigger,
    PgAuthId,
    PgAuthMembers,
    PgCollation,
    PgLargeobject,
    PgLargeobjectMetadata,
    PgTablespace,
    PgShdepend,
    PgShdescription,
    PgReplicationOrigin,
    PgAm,
    PgAmop,
    PgAmproc,
    PgTransform,
    PgAttrdef,
    PgCast,
    PgConstraint,
    PgConversion,
    PgDepend,
    PgDescription,
    PgForeignDataWrapper,
    PgForeignServer,
    PgUserMapping,
    PgForeignTable,
    PgIndex,
    PgInherits,
    PgPartitionedTable,
    PgRewrite,
    PgSequence,
    PgStatistic,
    PgStatisticExt,
    PgStatisticExtData,
    PgTrigger,
    PgPolicy,
    PgPublication,
    PgPublicationRel,
    PgPublicationNamespace,
    PgSubscription,
    PgParameterAcl,
    PgOpclass,
    PgOpfamily,
    PgAggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogScope {
    Shared,
    Database(u32),
}

impl BootstrapCatalogKind {
    pub const fn relation_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_RELATION_OID,
            Self::PgClass => PG_CLASS_RELATION_OID,
            Self::PgAttribute => PG_ATTRIBUTE_RELATION_OID,
            Self::PgType => PG_TYPE_RELATION_OID,
            Self::PgProc => PG_PROC_RELATION_OID,
            Self::PgDefaultAcl => PG_DEFAULT_ACL_RELATION_OID,
            Self::PgTsParser => PG_TS_PARSER_RELATION_OID,
            Self::PgTsTemplate => PG_TS_TEMPLATE_RELATION_OID,
            Self::PgTsDict => PG_TS_DICT_RELATION_OID,
            Self::PgTsConfig => PG_TS_CONFIG_RELATION_OID,
            Self::PgTsConfigMap => PG_TS_CONFIG_MAP_RELATION_OID,
            Self::PgLanguage => PG_LANGUAGE_RELATION_OID,
            Self::PgOperator => PG_OPERATOR_RELATION_OID,
            Self::PgDatabase => PG_DATABASE_RELATION_OID,
            Self::PgExtension => PG_EXTENSION_RELATION_OID,
            Self::PgEventTrigger => PG_EVENT_TRIGGER_RELATION_OID,
            Self::PgAuthId => PG_AUTHID_RELATION_OID,
            Self::PgAuthMembers => PG_AUTH_MEMBERS_RELATION_OID,
            Self::PgCollation => PG_COLLATION_RELATION_OID,
            Self::PgLargeobject => PG_LARGEOBJECT_RELATION_OID,
            Self::PgLargeobjectMetadata => PG_LARGEOBJECT_METADATA_RELATION_OID,
            Self::PgTablespace => PG_TABLESPACE_RELATION_OID,
            Self::PgShdepend => PG_SHDEPEND_RELATION_OID,
            Self::PgShdescription => PG_SHDESCRIPTION_RELATION_OID,
            Self::PgReplicationOrigin => PG_REPLICATION_ORIGIN_RELATION_OID,
            Self::PgAm => PG_AM_RELATION_OID,
            Self::PgAmop => PG_AMOP_RELATION_OID,
            Self::PgAmproc => PG_AMPROC_RELATION_OID,
            Self::PgTransform => PG_TRANSFORM_RELATION_OID,
            Self::PgAttrdef => PG_ATTRDEF_RELATION_OID,
            Self::PgCast => PG_CAST_RELATION_OID,
            Self::PgConstraint => PG_CONSTRAINT_RELATION_OID,
            Self::PgConversion => PG_CONVERSION_RELATION_OID,
            Self::PgAggregate => PG_AGGREGATE_RELATION_OID,
            Self::PgDepend => PG_DEPEND_RELATION_OID,
            Self::PgDescription => PG_DESCRIPTION_RELATION_OID,
            Self::PgForeignDataWrapper => PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
            Self::PgForeignServer => PG_FOREIGN_SERVER_RELATION_OID,
            Self::PgUserMapping => PG_USER_MAPPING_RELATION_OID,
            Self::PgForeignTable => PG_FOREIGN_TABLE_RELATION_OID,
            Self::PgIndex => PG_INDEX_RELATION_OID,
            Self::PgInherits => PG_INHERITS_RELATION_OID,
            Self::PgPartitionedTable => PG_PARTITIONED_TABLE_RELATION_OID,
            Self::PgRewrite => PG_REWRITE_RELATION_OID,
            Self::PgSequence => PG_SEQUENCE_RELATION_OID,
            Self::PgStatistic => PG_STATISTIC_RELATION_OID,
            Self::PgStatisticExt => PG_STATISTIC_EXT_RELATION_OID,
            Self::PgStatisticExtData => PG_STATISTIC_EXT_DATA_RELATION_OID,
            Self::PgTrigger => PG_TRIGGER_RELATION_OID,
            Self::PgPolicy => PG_POLICY_RELATION_OID,
            Self::PgPublication => PG_PUBLICATION_RELATION_OID,
            Self::PgPublicationRel => PG_PUBLICATION_REL_RELATION_OID,
            Self::PgPublicationNamespace => PG_PUBLICATION_NAMESPACE_RELATION_OID,
            Self::PgSubscription => PG_SUBSCRIPTION_RELATION_OID,
            Self::PgParameterAcl => PG_PARAMETER_ACL_RELATION_OID,
            Self::PgOpclass => PG_OPCLASS_RELATION_OID,
            Self::PgOpfamily => PG_OPFAMILY_RELATION_OID,
        }
    }

    pub const fn relation_name(self) -> &'static str {
        match self {
            Self::PgNamespace => "pg_namespace",
            Self::PgClass => "pg_class",
            Self::PgAttribute => "pg_attribute",
            Self::PgType => "pg_type",
            Self::PgProc => "pg_proc",
            Self::PgDefaultAcl => "pg_default_acl",
            Self::PgTsParser => "pg_ts_parser",
            Self::PgTsTemplate => "pg_ts_template",
            Self::PgTsDict => "pg_ts_dict",
            Self::PgTsConfig => "pg_ts_config",
            Self::PgTsConfigMap => "pg_ts_config_map",
            Self::PgLanguage => "pg_language",
            Self::PgOperator => "pg_operator",
            Self::PgDatabase => "pg_database",
            Self::PgExtension => "pg_extension",
            Self::PgEventTrigger => "pg_event_trigger",
            Self::PgAuthId => "pg_authid",
            Self::PgAuthMembers => "pg_auth_members",
            Self::PgCollation => "pg_collation",
            Self::PgLargeobject => "pg_largeobject",
            Self::PgLargeobjectMetadata => "pg_largeobject_metadata",
            Self::PgTablespace => "pg_tablespace",
            Self::PgShdepend => "pg_shdepend",
            Self::PgShdescription => "pg_shdescription",
            Self::PgReplicationOrigin => "pg_replication_origin",
            Self::PgAm => "pg_am",
            Self::PgAmop => "pg_amop",
            Self::PgAmproc => "pg_amproc",
            Self::PgTransform => "pg_transform",
            Self::PgAttrdef => "pg_attrdef",
            Self::PgCast => "pg_cast",
            Self::PgConstraint => "pg_constraint",
            Self::PgConversion => "pg_conversion",
            Self::PgAggregate => "pg_aggregate",
            Self::PgDepend => "pg_depend",
            Self::PgDescription => "pg_description",
            Self::PgForeignDataWrapper => "pg_foreign_data_wrapper",
            Self::PgForeignServer => "pg_foreign_server",
            Self::PgUserMapping => "pg_user_mapping",
            Self::PgForeignTable => "pg_foreign_table",
            Self::PgIndex => "pg_index",
            Self::PgInherits => "pg_inherits",
            Self::PgPartitionedTable => "pg_partitioned_table",
            Self::PgRewrite => "pg_rewrite",
            Self::PgSequence => "pg_sequence",
            Self::PgStatistic => "pg_statistic",
            Self::PgStatisticExt => "pg_statistic_ext",
            Self::PgStatisticExtData => "pg_statistic_ext_data",
            Self::PgTrigger => "pg_trigger",
            Self::PgPolicy => "pg_policy",
            Self::PgPublication => "pg_publication",
            Self::PgPublicationRel => "pg_publication_rel",
            Self::PgPublicationNamespace => "pg_publication_namespace",
            Self::PgSubscription => "pg_subscription",
            Self::PgParameterAcl => "pg_parameter_acl",
            Self::PgOpclass => "pg_opclass",
            Self::PgOpfamily => "pg_opfamily",
        }
    }

    pub const fn row_type_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_ROWTYPE_OID,
            Self::PgClass => PG_CLASS_ROWTYPE_OID,
            Self::PgAttribute => PG_ATTRIBUTE_ROWTYPE_OID,
            Self::PgType => PG_TYPE_ROWTYPE_OID,
            Self::PgProc => PG_PROC_ROWTYPE_OID,
            Self::PgDefaultAcl => 0,
            Self::PgTsParser => 0,
            Self::PgTsTemplate => 0,
            Self::PgTsDict => 0,
            Self::PgTsConfig => 0,
            Self::PgTsConfigMap => 0,
            Self::PgLanguage => 0,
            Self::PgOperator => 0,
            Self::PgDatabase => PG_DATABASE_ROWTYPE_OID,
            Self::PgExtension => 0,
            Self::PgEventTrigger => PG_EVENT_TRIGGER_ROWTYPE_OID,
            Self::PgAuthId => 0,
            Self::PgAuthMembers => 0,
            Self::PgCollation => 0,
            Self::PgLargeobject => 0,
            Self::PgLargeobjectMetadata => 0,
            Self::PgTablespace => 0,
            Self::PgShdepend => 0,
            Self::PgShdescription => 0,
            Self::PgReplicationOrigin => 0,
            Self::PgAm => PG_AM_ROWTYPE_OID,
            Self::PgAmop => 0,
            Self::PgAmproc => 0,
            Self::PgTransform => 0,
            Self::PgAttrdef => PG_ATTRDEF_ROWTYPE_OID,
            Self::PgCast => 0,
            Self::PgConstraint => 0,
            Self::PgConversion => 0,
            Self::PgAggregate => 0,
            Self::PgDepend => PG_DEPEND_ROWTYPE_OID,
            Self::PgDescription => 0,
            Self::PgForeignDataWrapper => 0,
            Self::PgForeignServer => 0,
            Self::PgUserMapping => 0,
            Self::PgForeignTable => 0,
            Self::PgIndex => PG_INDEX_ROWTYPE_OID,
            Self::PgInherits => PG_INHERITS_ROWTYPE_OID,
            Self::PgPartitionedTable => PG_PARTITIONED_TABLE_ROWTYPE_OID,
            Self::PgRewrite => PG_REWRITE_ROWTYPE_OID,
            Self::PgSequence => 0,
            Self::PgStatistic => PG_STATISTIC_ROWTYPE_OID,
            Self::PgStatisticExt => PG_STATISTIC_EXT_ROWTYPE_OID,
            Self::PgStatisticExtData => PG_STATISTIC_EXT_DATA_ROWTYPE_OID,
            Self::PgTrigger => PG_TRIGGER_ROWTYPE_OID,
            Self::PgPolicy => 0,
            Self::PgPublication => PG_PUBLICATION_ROWTYPE_OID,
            Self::PgPublicationRel => PG_PUBLICATION_REL_ROWTYPE_OID,
            Self::PgPublicationNamespace => PG_PUBLICATION_NAMESPACE_ROWTYPE_OID,
            Self::PgSubscription => 0,
            Self::PgParameterAcl => 0,
            Self::PgOpclass => 0,
            Self::PgOpfamily => 0,
        }
    }

    pub const fn array_type_oid(self) -> u32 {
        match self {
            Self::PgType => PG_TYPE_ARRAY_TYPE_OID,
            Self::PgProc => PG_PROC_ARRAY_TYPE_OID,
            Self::PgAttribute => PG_ATTRIBUTE_ARRAY_TYPE_OID,
            Self::PgClass => PG_CLASS_ARRAY_TYPE_OID,
            Self::PgDatabase => PG_DATABASE_ARRAY_TYPE_OID,
            Self::PgStatistic => PG_STATISTIC_ARRAY_TYPE_OID,
            Self::PgStatisticExt => PG_STATISTIC_EXT_ARRAY_TYPE_OID,
            Self::PgStatisticExtData => PG_STATISTIC_EXT_DATA_ARRAY_TYPE_OID,
            _ => 0,
        }
    }

    pub const fn scope(self) -> CatalogScope {
        match self {
            Self::PgDatabase
            | Self::PgAuthId
            | Self::PgAuthMembers
            | Self::PgLargeobjectMetadata
            | Self::PgTablespace
            | Self::PgShdepend
            | Self::PgShdescription
            | Self::PgReplicationOrigin
            | Self::PgSubscription => CatalogScope::Shared,
            _ => CatalogScope::Database(0),
        }
    }

    pub const fn toast_relation_oid(self) -> u32 {
        match self {
            Self::PgAggregate => 4159,
            Self::PgAttrdef => 2830,
            Self::PgCollation => 6175,
            Self::PgConstraint => 2832,
            Self::PgDatabase => 4177,
            Self::PgDescription => 2834,
            Self::PgForeignDataWrapper => 4149,
            Self::PgForeignServer => 4151,
            Self::PgUserMapping => 4173,
            Self::PgForeignTable => 4153,
            Self::PgIndex => 6351,
            Self::PgLanguage => 4157,
            Self::PgNamespace => 4163,
            Self::PgPartitionedTable => 4165,
            Self::PgPolicy => 4167,
            Self::PgProc => 2836,
            Self::PgPublicationRel => 6228,
            Self::PgRewrite => 2838,
            Self::PgStatistic => 2840,
            Self::PgStatisticExt => 3439,
            Self::PgStatisticExtData => 3430,
            Self::PgTablespace => 4185,
            Self::PgTrigger => 2336,
            Self::PgEventTrigger => 4145,
            Self::PgTsDict => 4169,
            Self::PgType => 4171,
            _ => 0,
        }
    }
}

pub const CORE_BOOTSTRAP_KINDS: [BootstrapCatalogKind; 57] = [
    BootstrapCatalogKind::PgNamespace,
    BootstrapCatalogKind::PgType,
    BootstrapCatalogKind::PgProc,
    BootstrapCatalogKind::PgTsParser,
    BootstrapCatalogKind::PgTsTemplate,
    BootstrapCatalogKind::PgTsDict,
    BootstrapCatalogKind::PgTsConfig,
    BootstrapCatalogKind::PgTsConfigMap,
    BootstrapCatalogKind::PgLanguage,
    BootstrapCatalogKind::PgOperator,
    BootstrapCatalogKind::PgOpfamily,
    BootstrapCatalogKind::PgOpclass,
    BootstrapCatalogKind::PgAmop,
    BootstrapCatalogKind::PgAmproc,
    BootstrapCatalogKind::PgAttribute,
    BootstrapCatalogKind::PgClass,
    BootstrapCatalogKind::PgAuthId,
    BootstrapCatalogKind::PgAuthMembers,
    BootstrapCatalogKind::PgCollation,
    BootstrapCatalogKind::PgLargeobject,
    BootstrapCatalogKind::PgLargeobjectMetadata,
    BootstrapCatalogKind::PgDatabase,
    BootstrapCatalogKind::PgTablespace,
    BootstrapCatalogKind::PgShdepend,
    BootstrapCatalogKind::PgShdescription,
    BootstrapCatalogKind::PgReplicationOrigin,
    BootstrapCatalogKind::PgAm,
    BootstrapCatalogKind::PgAttrdef,
    BootstrapCatalogKind::PgCast,
    BootstrapCatalogKind::PgConstraint,
    BootstrapCatalogKind::PgConversion,
    BootstrapCatalogKind::PgDepend,
    BootstrapCatalogKind::PgDescription,
    BootstrapCatalogKind::PgForeignDataWrapper,
    BootstrapCatalogKind::PgForeignServer,
    BootstrapCatalogKind::PgUserMapping,
    BootstrapCatalogKind::PgForeignTable,
    BootstrapCatalogKind::PgIndex,
    BootstrapCatalogKind::PgInherits,
    BootstrapCatalogKind::PgPartitionedTable,
    BootstrapCatalogKind::PgRewrite,
    BootstrapCatalogKind::PgSequence,
    BootstrapCatalogKind::PgStatistic,
    BootstrapCatalogKind::PgStatisticExt,
    BootstrapCatalogKind::PgStatisticExtData,
    BootstrapCatalogKind::PgTrigger,
    BootstrapCatalogKind::PgPolicy,
    BootstrapCatalogKind::PgPublication,
    BootstrapCatalogKind::PgPublicationRel,
    BootstrapCatalogKind::PgPublicationNamespace,
    BootstrapCatalogKind::PgAggregate,
    BootstrapCatalogKind::PgDefaultAcl,
    BootstrapCatalogKind::PgExtension,
    BootstrapCatalogKind::PgEventTrigger,
    BootstrapCatalogKind::PgTransform,
    BootstrapCatalogKind::PgSubscription,
    BootstrapCatalogKind::PgParameterAcl,
];

pub const fn bootstrap_catalog_kinds() -> [BootstrapCatalogKind; 57] {
    CORE_BOOTSTRAP_KINDS
}

pub fn bootstrap_relation_desc(kind: BootstrapCatalogKind) -> RelationDesc {
    match kind {
        BootstrapCatalogKind::PgNamespace => pg_namespace_desc(),
        BootstrapCatalogKind::PgClass => pg_class_desc(),
        BootstrapCatalogKind::PgAttribute => pg_attribute_desc(),
        BootstrapCatalogKind::PgType => pg_type_desc(),
        BootstrapCatalogKind::PgProc => pg_proc_desc(),
        BootstrapCatalogKind::PgDefaultAcl => pg_default_acl_desc(),
        BootstrapCatalogKind::PgTsParser => pg_ts_parser_desc(),
        BootstrapCatalogKind::PgTsTemplate => pg_ts_template_desc(),
        BootstrapCatalogKind::PgTsDict => pg_ts_dict_desc(),
        BootstrapCatalogKind::PgTsConfig => pg_ts_config_desc(),
        BootstrapCatalogKind::PgTsConfigMap => pg_ts_config_map_desc(),
        BootstrapCatalogKind::PgLanguage => pg_language_desc(),
        BootstrapCatalogKind::PgOperator => pg_operator_desc(),
        BootstrapCatalogKind::PgDatabase => pg_database_desc(),
        BootstrapCatalogKind::PgExtension => pg_extension_desc(),
        BootstrapCatalogKind::PgEventTrigger => pg_event_trigger_desc(),
        BootstrapCatalogKind::PgAuthId => pg_authid_desc(),
        BootstrapCatalogKind::PgAuthMembers => pg_auth_members_desc(),
        BootstrapCatalogKind::PgCollation => pg_collation_desc(),
        BootstrapCatalogKind::PgLargeobject => pg_largeobject_desc(),
        BootstrapCatalogKind::PgLargeobjectMetadata => pg_largeobject_metadata_desc(),
        BootstrapCatalogKind::PgTablespace => pg_tablespace_desc(),
        BootstrapCatalogKind::PgShdepend => pg_shdepend_desc(),
        BootstrapCatalogKind::PgShdescription => {
            crate::include::catalog::pg_shdescription::pg_shdescription_desc()
        }
        BootstrapCatalogKind::PgReplicationOrigin => pg_replication_origin_desc(),
        BootstrapCatalogKind::PgAm => pg_am_desc(),
        BootstrapCatalogKind::PgAmop => pg_amop_desc(),
        BootstrapCatalogKind::PgAmproc => pg_amproc_desc(),
        BootstrapCatalogKind::PgTransform => pg_transform_desc(),
        BootstrapCatalogKind::PgAttrdef => pg_attrdef_desc(),
        BootstrapCatalogKind::PgCast => pg_cast_desc(),
        BootstrapCatalogKind::PgConstraint => pg_constraint_desc(),
        BootstrapCatalogKind::PgConversion => pg_conversion_desc(),
        BootstrapCatalogKind::PgAggregate => pg_aggregate_desc(),
        BootstrapCatalogKind::PgDepend => pg_depend_desc(),
        BootstrapCatalogKind::PgDescription => pg_description_desc(),
        BootstrapCatalogKind::PgForeignDataWrapper => pg_foreign_data_wrapper_desc(),
        BootstrapCatalogKind::PgForeignServer => pg_foreign_server_desc(),
        BootstrapCatalogKind::PgUserMapping => pg_user_mapping_desc(),
        BootstrapCatalogKind::PgForeignTable => pg_foreign_table_desc(),
        BootstrapCatalogKind::PgIndex => pg_index_desc(),
        BootstrapCatalogKind::PgInherits => pg_inherits_desc(),
        BootstrapCatalogKind::PgPartitionedTable => pg_partitioned_table_desc(),
        BootstrapCatalogKind::PgRewrite => pg_rewrite_desc(),
        BootstrapCatalogKind::PgSequence => pg_sequence_desc(),
        BootstrapCatalogKind::PgStatistic => pg_statistic_desc(),
        BootstrapCatalogKind::PgStatisticExt => pg_statistic_ext_desc(),
        BootstrapCatalogKind::PgStatisticExtData => pg_statistic_ext_data_desc(),
        BootstrapCatalogKind::PgTrigger => pg_trigger_desc(),
        BootstrapCatalogKind::PgPolicy => pg_policy_desc(),
        BootstrapCatalogKind::PgPublication => pg_publication_desc(),
        BootstrapCatalogKind::PgPublicationRel => pg_publication_rel_desc(),
        BootstrapCatalogKind::PgPublicationNamespace => pg_publication_namespace_desc(),
        BootstrapCatalogKind::PgSubscription => pg_subscription_desc(),
        BootstrapCatalogKind::PgParameterAcl => pg_parameter_acl_desc(),
        BootstrapCatalogKind::PgOpclass => pg_opclass_desc(),
        BootstrapCatalogKind::PgOpfamily => pg_opfamily_desc(),
    }
}

pub const fn bootstrap_namespace_oid() -> u32 {
    PG_CATALOG_NAMESPACE_OID
}

pub const CORE_BOOTSTRAP_RELATIONS: [BootstrapCatalogRelation; 57] = [
    BootstrapCatalogRelation {
        oid: PG_NAMESPACE_RELATION_OID,
        name: "pg_namespace",
    },
    BootstrapCatalogRelation {
        oid: PG_TYPE_RELATION_OID,
        name: "pg_type",
    },
    BootstrapCatalogRelation {
        oid: PG_PROC_RELATION_OID,
        name: "pg_proc",
    },
    BootstrapCatalogRelation {
        oid: PG_TS_PARSER_RELATION_OID,
        name: "pg_ts_parser",
    },
    BootstrapCatalogRelation {
        oid: PG_TS_TEMPLATE_RELATION_OID,
        name: "pg_ts_template",
    },
    BootstrapCatalogRelation {
        oid: PG_TS_DICT_RELATION_OID,
        name: "pg_ts_dict",
    },
    BootstrapCatalogRelation {
        oid: PG_TS_CONFIG_RELATION_OID,
        name: "pg_ts_config",
    },
    BootstrapCatalogRelation {
        oid: PG_TS_CONFIG_MAP_RELATION_OID,
        name: "pg_ts_config_map",
    },
    BootstrapCatalogRelation {
        oid: PG_LANGUAGE_RELATION_OID,
        name: "pg_language",
    },
    BootstrapCatalogRelation {
        oid: PG_OPERATOR_RELATION_OID,
        name: "pg_operator",
    },
    BootstrapCatalogRelation {
        oid: PG_OPFAMILY_RELATION_OID,
        name: "pg_opfamily",
    },
    BootstrapCatalogRelation {
        oid: PG_OPCLASS_RELATION_OID,
        name: "pg_opclass",
    },
    BootstrapCatalogRelation {
        oid: PG_AMOP_RELATION_OID,
        name: "pg_amop",
    },
    BootstrapCatalogRelation {
        oid: PG_AMPROC_RELATION_OID,
        name: "pg_amproc",
    },
    BootstrapCatalogRelation {
        oid: PG_ATTRIBUTE_RELATION_OID,
        name: "pg_attribute",
    },
    BootstrapCatalogRelation {
        oid: PG_CLASS_RELATION_OID,
        name: "pg_class",
    },
    BootstrapCatalogRelation {
        oid: PG_AUTHID_RELATION_OID,
        name: "pg_authid",
    },
    BootstrapCatalogRelation {
        oid: PG_AUTH_MEMBERS_RELATION_OID,
        name: "pg_auth_members",
    },
    BootstrapCatalogRelation {
        oid: PG_COLLATION_RELATION_OID,
        name: "pg_collation",
    },
    BootstrapCatalogRelation {
        oid: PG_LARGEOBJECT_RELATION_OID,
        name: "pg_largeobject",
    },
    BootstrapCatalogRelation {
        oid: PG_LARGEOBJECT_METADATA_RELATION_OID,
        name: "pg_largeobject_metadata",
    },
    BootstrapCatalogRelation {
        oid: PG_DATABASE_RELATION_OID,
        name: "pg_database",
    },
    BootstrapCatalogRelation {
        oid: PG_TABLESPACE_RELATION_OID,
        name: "pg_tablespace",
    },
    BootstrapCatalogRelation {
        oid: PG_SHDEPEND_RELATION_OID,
        name: "pg_shdepend",
    },
    BootstrapCatalogRelation {
        oid: PG_SHDESCRIPTION_RELATION_OID,
        name: "pg_shdescription",
    },
    BootstrapCatalogRelation {
        oid: PG_REPLICATION_ORIGIN_RELATION_OID,
        name: "pg_replication_origin",
    },
    BootstrapCatalogRelation {
        oid: PG_AM_RELATION_OID,
        name: "pg_am",
    },
    BootstrapCatalogRelation {
        oid: PG_ATTRDEF_RELATION_OID,
        name: "pg_attrdef",
    },
    BootstrapCatalogRelation {
        oid: PG_CAST_RELATION_OID,
        name: "pg_cast",
    },
    BootstrapCatalogRelation {
        oid: PG_CONSTRAINT_RELATION_OID,
        name: "pg_constraint",
    },
    BootstrapCatalogRelation {
        oid: PG_CONVERSION_RELATION_OID,
        name: "pg_conversion",
    },
    BootstrapCatalogRelation {
        oid: PG_DEPEND_RELATION_OID,
        name: "pg_depend",
    },
    BootstrapCatalogRelation {
        oid: PG_DESCRIPTION_RELATION_OID,
        name: "pg_description",
    },
    BootstrapCatalogRelation {
        oid: PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
        name: "pg_foreign_data_wrapper",
    },
    BootstrapCatalogRelation {
        oid: PG_FOREIGN_SERVER_RELATION_OID,
        name: "pg_foreign_server",
    },
    BootstrapCatalogRelation {
        oid: PG_USER_MAPPING_RELATION_OID,
        name: "pg_user_mapping",
    },
    BootstrapCatalogRelation {
        oid: PG_FOREIGN_TABLE_RELATION_OID,
        name: "pg_foreign_table",
    },
    BootstrapCatalogRelation {
        oid: PG_INDEX_RELATION_OID,
        name: "pg_index",
    },
    BootstrapCatalogRelation {
        oid: PG_INHERITS_RELATION_OID,
        name: "pg_inherits",
    },
    BootstrapCatalogRelation {
        oid: PG_PARTITIONED_TABLE_RELATION_OID,
        name: "pg_partitioned_table",
    },
    BootstrapCatalogRelation {
        oid: PG_REWRITE_RELATION_OID,
        name: "pg_rewrite",
    },
    BootstrapCatalogRelation {
        oid: PG_SEQUENCE_RELATION_OID,
        name: "pg_sequence",
    },
    BootstrapCatalogRelation {
        oid: PG_STATISTIC_RELATION_OID,
        name: "pg_statistic",
    },
    BootstrapCatalogRelation {
        oid: PG_STATISTIC_EXT_RELATION_OID,
        name: "pg_statistic_ext",
    },
    BootstrapCatalogRelation {
        oid: PG_STATISTIC_EXT_DATA_RELATION_OID,
        name: "pg_statistic_ext_data",
    },
    BootstrapCatalogRelation {
        oid: PG_TRIGGER_RELATION_OID,
        name: "pg_trigger",
    },
    BootstrapCatalogRelation {
        oid: PG_POLICY_RELATION_OID,
        name: "pg_policy",
    },
    BootstrapCatalogRelation {
        oid: PG_PUBLICATION_RELATION_OID,
        name: "pg_publication",
    },
    BootstrapCatalogRelation {
        oid: PG_PUBLICATION_REL_RELATION_OID,
        name: "pg_publication_rel",
    },
    BootstrapCatalogRelation {
        oid: PG_PUBLICATION_NAMESPACE_RELATION_OID,
        name: "pg_publication_namespace",
    },
    BootstrapCatalogRelation {
        oid: PG_AGGREGATE_RELATION_OID,
        name: "pg_aggregate",
    },
    BootstrapCatalogRelation {
        oid: PG_DEFAULT_ACL_RELATION_OID,
        name: "pg_default_acl",
    },
    BootstrapCatalogRelation {
        oid: PG_EXTENSION_RELATION_OID,
        name: "pg_extension",
    },
    BootstrapCatalogRelation {
        oid: PG_EVENT_TRIGGER_RELATION_OID,
        name: "pg_event_trigger",
    },
    BootstrapCatalogRelation {
        oid: PG_TRANSFORM_RELATION_OID,
        name: "pg_transform",
    },
    BootstrapCatalogRelation {
        oid: PG_SUBSCRIPTION_RELATION_OID,
        name: "pg_subscription",
    },
    BootstrapCatalogRelation {
        oid: PG_PARAMETER_ACL_RELATION_OID,
        name: "pg_parameter_acl",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_bootstrap_relations_match_expected_oids() {
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[0].oid, PG_NAMESPACE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[1].oid, PG_TYPE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[2].oid, PG_PROC_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[3].oid, PG_TS_PARSER_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[4].oid, PG_TS_TEMPLATE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[5].oid, PG_TS_DICT_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[6].oid, PG_TS_CONFIG_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[7].oid,
            PG_TS_CONFIG_MAP_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[8].oid, PG_LANGUAGE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[9].oid, PG_OPERATOR_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[10].oid, PG_OPFAMILY_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[11].oid, PG_OPCLASS_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[12].oid, PG_AMOP_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[13].oid, PG_AMPROC_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[14].oid, PG_ATTRIBUTE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[15].oid, PG_CLASS_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[16].oid, PG_AUTHID_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[17].oid,
            PG_AUTH_MEMBERS_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[18].oid, PG_COLLATION_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[19].oid,
            PG_LARGEOBJECT_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[20].oid,
            PG_LARGEOBJECT_METADATA_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[21].oid, PG_DATABASE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[22].oid, PG_TABLESPACE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[23].oid, PG_SHDEPEND_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[24].oid,
            PG_SHDESCRIPTION_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[25].oid,
            PG_REPLICATION_ORIGIN_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[26].oid, PG_AM_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[27].oid, PG_ATTRDEF_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[28].oid, PG_CAST_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[29].oid, PG_CONSTRAINT_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[30].oid, PG_CONVERSION_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[31].oid, PG_DEPEND_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[32].oid,
            PG_DESCRIPTION_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[33].oid,
            PG_FOREIGN_DATA_WRAPPER_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[34].oid,
            PG_FOREIGN_SERVER_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[35].oid,
            PG_USER_MAPPING_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[36].oid,
            PG_FOREIGN_TABLE_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[37].oid, PG_INDEX_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[38].oid, PG_INHERITS_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[39].oid,
            PG_PARTITIONED_TABLE_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[40].oid, PG_REWRITE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[41].oid, PG_SEQUENCE_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[42].oid, PG_STATISTIC_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[43].oid,
            PG_STATISTIC_EXT_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[44].oid,
            PG_STATISTIC_EXT_DATA_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[45].oid, PG_TRIGGER_RELATION_OID);
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[46].oid, PG_POLICY_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[47].oid,
            PG_PUBLICATION_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[48].oid,
            PG_PUBLICATION_REL_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[49].oid,
            PG_PUBLICATION_NAMESPACE_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[50].oid, PG_AGGREGATE_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[51].oid,
            PG_DEFAULT_ACL_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[52].oid, PG_EXTENSION_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[53].oid,
            PG_EVENT_TRIGGER_RELATION_OID
        );
        assert_eq!(CORE_BOOTSTRAP_RELATIONS[54].oid, PG_TRANSFORM_RELATION_OID);
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[55].oid,
            PG_SUBSCRIPTION_RELATION_OID
        );
        assert_eq!(
            CORE_BOOTSTRAP_RELATIONS[56].oid,
            PG_PARAMETER_ACL_RELATION_OID
        );
    }

    #[test]
    fn core_bootstrap_relation_names_are_stable() {
        let names: Vec<_> = CORE_BOOTSTRAP_RELATIONS
            .iter()
            .map(|rel| rel.name)
            .collect();
        assert_eq!(
            names,
            vec![
                "pg_namespace",
                "pg_type",
                "pg_proc",
                "pg_ts_parser",
                "pg_ts_template",
                "pg_ts_dict",
                "pg_ts_config",
                "pg_ts_config_map",
                "pg_language",
                "pg_operator",
                "pg_opfamily",
                "pg_opclass",
                "pg_amop",
                "pg_amproc",
                "pg_attribute",
                "pg_class",
                "pg_authid",
                "pg_auth_members",
                "pg_collation",
                "pg_largeobject",
                "pg_largeobject_metadata",
                "pg_database",
                "pg_tablespace",
                "pg_shdepend",
                "pg_shdescription",
                "pg_replication_origin",
                "pg_am",
                "pg_attrdef",
                "pg_cast",
                "pg_constraint",
                "pg_conversion",
                "pg_depend",
                "pg_description",
                "pg_foreign_data_wrapper",
                "pg_foreign_server",
                "pg_user_mapping",
                "pg_foreign_table",
                "pg_index",
                "pg_inherits",
                "pg_partitioned_table",
                "pg_rewrite",
                "pg_sequence",
                "pg_statistic",
                "pg_statistic_ext",
                "pg_statistic_ext_data",
                "pg_trigger",
                "pg_policy",
                "pg_publication",
                "pg_publication_rel",
                "pg_publication_namespace",
                "pg_aggregate",
                "pg_default_acl",
                "pg_extension",
                "pg_event_trigger",
                "pg_transform",
                "pg_subscription",
                "pg_parameter_acl",
            ]
        );
    }

    #[test]
    fn core_bootstrap_kinds_match_relation_list() {
        let pairs: Vec<_> = CORE_BOOTSTRAP_KINDS
            .iter()
            .map(|kind| (kind.relation_oid(), kind.relation_name()))
            .collect();
        let shared: Vec<_> = CORE_BOOTSTRAP_RELATIONS
            .iter()
            .map(|rel| (rel.oid, rel.name))
            .collect();
        assert_eq!(pairs, shared);
    }
}
use super::{
    pg_aggregate_desc, pg_am_desc, pg_amop_desc, pg_amproc_desc, pg_attrdef_desc,
    pg_attribute_desc, pg_auth_members_desc, pg_authid_desc, pg_cast_desc, pg_class_desc,
    pg_collation_desc, pg_constraint_desc, pg_conversion_desc, pg_database_desc,
    pg_default_acl_desc, pg_depend_desc, pg_description_desc, pg_event_trigger_desc,
    pg_extension_desc, pg_foreign_data_wrapper_desc, pg_foreign_server_desc, pg_foreign_table_desc,
    pg_index_desc, pg_inherits_desc, pg_language_desc, pg_largeobject_desc,
    pg_largeobject_metadata_desc, pg_namespace_desc, pg_opclass_desc, pg_operator_desc,
    pg_opfamily_desc, pg_parameter_acl_desc, pg_partitioned_table_desc, pg_policy_desc,
    pg_proc_desc, pg_publication_desc, pg_publication_namespace_desc, pg_publication_rel_desc,
    pg_replication_origin_desc, pg_rewrite_desc, pg_sequence_desc, pg_shdepend_desc,
    pg_statistic_desc, pg_statistic_ext_data_desc, pg_statistic_ext_desc, pg_subscription_desc,
    pg_tablespace_desc, pg_transform_desc, pg_trigger_desc, pg_ts_config_desc,
    pg_ts_config_map_desc, pg_ts_dict_desc, pg_ts_parser_desc, pg_ts_template_desc, pg_type_desc,
    pg_user_mapping_desc,
};
use crate::backend::executor::RelationDesc;
