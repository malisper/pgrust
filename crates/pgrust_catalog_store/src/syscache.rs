use crate::catalog::CatalogError;
use crate::rowcodec::{
    namespace_row_from_values, pg_aggregate_row_from_values, pg_am_row_from_values,
    pg_amop_row_from_values, pg_amproc_row_from_values, pg_attrdef_row_from_values,
    pg_attribute_row_from_values, pg_auth_members_row_from_values, pg_authid_row_from_values,
    pg_cast_row_from_values, pg_class_row_from_values, pg_collation_row_from_values,
    pg_constraint_row_from_values, pg_depend_row_from_values, pg_description_row_from_values,
    pg_event_trigger_row_from_values, pg_foreign_server_row_from_values, pg_index_row_from_values,
    pg_inherits_row_from_values, pg_language_row_from_values, pg_opclass_row_from_values,
    pg_operator_row_from_values, pg_opfamily_row_from_values, pg_partitioned_table_row_from_values,
    pg_policy_row_from_values, pg_proc_row_from_values, pg_publication_namespace_row_from_values,
    pg_publication_rel_row_from_values, pg_publication_row_from_values, pg_rewrite_row_from_values,
    pg_shdepend_row_from_values, pg_statistic_ext_data_row_from_values,
    pg_statistic_ext_row_from_values, pg_statistic_row_from_values, pg_trigger_row_from_values,
    pg_type_row_from_values,
};
use crate::rows::PhysicalCatalogRows;
use pgrust_catalog_data::{
    BootstrapCatalogKind, PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgDependRow, PgDescriptionRow, PgEventTriggerRow, PgForeignServerRow,
    PgIndexRow, PgInheritsRow, PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow,
    PgOpfamilyRow, PgPartitionedTableRow, PgPolicyRow, PgProcRow, PgPublicationNamespaceRow,
    PgPublicationRelRow, PgPublicationRow, PgRewriteRow, PgShdependRow, PgStatisticExtDataRow,
    PgStatisticExtRow, PgStatisticRow, PgTriggerRow, PgTypeRow, bootstrap_composite_type_rows,
    builtin_type_rows, system_catalog_index_by_oid,
};
use pgrust_nodes::{ScanKeyData, Value};

pub const BT_EQUAL_STRATEGY_NUMBER: u16 = 3;

pub const PG_ATTRIBUTE_RELID_ATTNAM_INDEX_OID: u32 = 2658;
pub const PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID: u32 = 2659;
pub const PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID: u32 = 2656;
pub const PG_ATTRDEF_OID_INDEX_OID: u32 = 2657;
pub const PG_AGGREGATE_FNOID_INDEX_OID: u32 = 2650;
pub const PG_AM_NAME_INDEX_OID: u32 = 2651;
pub const PG_AM_OID_INDEX_OID: u32 = 2652;
pub const PG_AMOP_FAM_STRAT_INDEX_OID: u32 = 2653;
pub const PG_AMPROC_FAM_PROC_INDEX_OID: u32 = 2655;
pub const PG_AUTHID_ROLNAME_INDEX_OID: u32 = 2676;
pub const PG_AUTHID_OID_INDEX_OID: u32 = 2677;
pub const PG_AUTH_MEMBERS_OID_INDEX_OID: u32 = 6303;
pub const PG_AUTH_MEMBERS_ROLE_MEMBER_INDEX_OID: u32 = 2694;
pub const PG_AUTH_MEMBERS_MEMBER_ROLE_INDEX_OID: u32 = 2695;
pub const PG_AUTH_MEMBERS_GRANTOR_INDEX_OID: u32 = 6302;
pub const PG_SHDEPEND_DEPENDER_INDEX_OID: u32 = 1232;
pub const PG_SHDEPEND_REFERENCE_INDEX_OID: u32 = 1233;
pub const PG_CAST_OID_INDEX_OID: u32 = 2660;
pub const PG_CAST_SOURCE_TARGET_INDEX_OID: u32 = 2661;
pub const PG_CLASS_OID_INDEX_OID: u32 = 2662;
pub const PG_CLASS_RELNAME_NSP_INDEX_OID: u32 = 2663;
pub const PG_COLLATION_OID_INDEX_OID: u32 = 3085;
pub const PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_OID: u32 = 2665;
pub const PG_CONSTRAINT_OID_INDEX_OID: u32 = 2667;
pub const PG_DEPEND_DEPENDER_INDEX_OID: u32 = 2673;
pub const PG_DEPEND_REFERENCE_INDEX_OID: u32 = 2674;
pub const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;
pub const PG_FOREIGN_SERVER_OID_INDEX_OID: u32 = 113;
pub const PG_FOREIGN_SERVER_NAME_INDEX_OID: u32 = 549;
pub const PG_INDEX_INDRELID_INDEX_OID: u32 = 2678;
pub const PG_INDEX_INDEXRELID_INDEX_OID: u32 = 2679;
pub const PG_INHERITS_RELID_SEQNO_INDEX_OID: u32 = 2680;
pub const PG_INHERITS_PARENT_INDEX_OID: u32 = 2187;
pub const PG_LANGUAGE_NAME_INDEX_OID: u32 = 2681;
pub const PG_LANGUAGE_OID_INDEX_OID: u32 = 2682;
pub const PG_NAMESPACE_NSPNAME_INDEX_OID: u32 = 2684;
pub const PG_NAMESPACE_OID_INDEX_OID: u32 = 2685;
pub const PG_OPCLASS_AM_NAME_NSP_INDEX_OID: u32 = 2686;
pub const PG_OPCLASS_OID_INDEX_OID: u32 = 2687;
pub const PG_OPFAMILY_OID_INDEX_OID: u32 = 2755;
pub const PG_OPERATOR_OID_INDEX_OID: u32 = 2688;
pub const PG_OPERATOR_OPRNAME_L_R_N_INDEX_OID: u32 = 2689;
pub const PG_PARTITIONED_TABLE_PARTRELID_INDEX_OID: u32 = 3351;
pub const PG_POLICY_OID_INDEX_OID: u32 = 3257;
pub const PG_POLICY_POLRELID_POLNAME_INDEX_OID: u32 = 3258;
pub const PG_PROC_OID_INDEX_OID: u32 = 2690;
pub const PG_PROC_PRONAME_ARGS_NSP_INDEX_OID: u32 = 2691;
pub const PG_PUBLICATION_OID_INDEX_OID: u32 = 6110;
pub const PG_PUBLICATION_PUBNAME_INDEX_OID: u32 = 6111;
pub const PG_PUBLICATION_REL_OID_INDEX_OID: u32 = 6112;
pub const PG_PUBLICATION_REL_PRRELID_PRPUBID_INDEX_OID: u32 = 6113;
pub const PG_PUBLICATION_REL_PRPUBID_INDEX_OID: u32 = 6116;
pub const PG_PUBLICATION_NAMESPACE_OID_INDEX_OID: u32 = 6238;
pub const PG_PUBLICATION_NAMESPACE_PNNSPID_PNPUBID_INDEX_OID: u32 = 6239;
pub const PG_REWRITE_OID_INDEX_OID: u32 = 2692;
pub const PG_REWRITE_REL_RULENAME_INDEX_OID: u32 = 2693;
pub const PG_STATISTIC_RELID_ATT_INH_INDEX_OID: u32 = 2696;
pub const PG_STATISTIC_EXT_RELID_INDEX_OID: u32 = 3379;
pub const PG_STATISTIC_EXT_OID_INDEX_OID: u32 = 3380;
pub const PG_STATISTIC_EXT_NAME_INDEX_OID: u32 = 3997;
pub const PG_STATISTIC_EXT_DATA_STXOID_INH_INDEX_OID: u32 = 3433;
pub const PG_TRIGGER_RELID_NAME_INDEX_OID: u32 = 2701;
pub const PG_TRIGGER_OID_INDEX_OID: u32 = 2702;
pub const PG_EVENT_TRIGGER_EVTNAME_INDEX_OID: u32 = 3467;
pub const PG_EVENT_TRIGGER_OID_INDEX_OID: u32 = 3468;
pub const PG_TYPE_OID_INDEX_OID: u32 = 2703;
pub const PG_TYPE_TYPNAME_NSP_INDEX_OID: u32 = 2704;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum SysCacheId {
    // PostgreSQL syscache name: AGGFNOID.
    AggFnoid,
    // PostgreSQL syscache name: AMNAME.
    AmName,
    // PostgreSQL syscache name: AMOID.
    AmOid,
    // PostgreSQL syscache name: AMPROCNUM.
    AmprocNum,
    // PostgreSQL syscache name: AMOPSTRATEGY.
    AmopStrategy,
    // PostgreSQL syscache name: ATTNAME.
    AttrName,
    // PostgreSQL syscache name: ATTNUM.
    AttrNum,
    // PostgreSQL syscache name: AUTHNAME.
    AuthIdRolname,
    // PostgreSQL syscache name: AUTHOID.
    AuthIdOid,
    // PostgreSQL systable scan index: AuthMemOidIndexId.
    AuthMembersOid,
    // PostgreSQL systable scan index: AuthMemRoleMemIndexId.
    AuthMembersRoleMember,
    // PostgreSQL systable scan index: AuthMemMemRoleIndexId.
    AuthMembersMemberRole,
    // PostgreSQL systable scan index: AuthMemGrantorIndexId.
    AuthMembersGrantor,
    // PostgreSQL systable scan index: SharedDependDependerIndexId.
    ShdependDepender,
    // PostgreSQL systable scan index: SharedDependReferenceIndexId.
    ShdependReference,
    // PostgreSQL syscache name: FOREIGNSERVEROID.
    ForeignServerOid,
    // PostgreSQL syscache name: FOREIGNSERVERNAME.
    ForeignServerName,
    // PostgreSQL systable scan index: AttrDefaultIndexId.
    AttrDefault,
    // PostgreSQL systable scan index: AttrDefaultOidIndexId.
    AttrDefaultOid,
    // PostgreSQL systable scan index: CastOidIndexId.
    CastOid,
    // PostgreSQL syscache name: CASTSOURCETARGET.
    CastSourceTarget,
    // PostgreSQL syscache name: COLLOID.
    CollOid,
    // PostgreSQL syscache name: CONSTROID.
    ConstraintOid,
    // PostgreSQL-like relation constraint lookup over pg_constraint_conrelid_*.
    ConstraintRelId,
    // PostgreSQL systable scan index: DependDependerIndexId.
    DependDepender,
    // PostgreSQL systable scan index: DependReferenceIndexId.
    DependReference,
    // PostgreSQL systable scan index: DescriptionObjIndexId.
    DescriptionObj,
    // PostgreSQL syscache name: INDEXRELID.
    IndexRelId,
    // PostgreSQL-like heap index lookup over pg_index_indrelid_index.
    IndexIndRelId,
    // PostgreSQL systable scan index: InheritsRelidSeqnoIndexId.
    InheritsRelIdSeqNo,
    // PostgreSQL systable scan index: InheritsParentIndexId.
    InheritsParent,
    // PostgreSQL syscache name: LANGNAME.
    LangName,
    // PostgreSQL syscache name: LANGOID.
    LangOid,
    // PostgreSQL syscache name: NAMESPACENAME.
    NamespaceName,
    // PostgreSQL syscache name: NAMESPACEOID.
    NamespaceOid,
    // PostgreSQL syscache name: CLAOID.
    OpclassOid,
    // PostgreSQL syscache name: CLAAMNAMENSP.
    ClaAmNameNsp,
    // PostgreSQL syscache name: OPFAMILYOID.
    OpfamilyOid,
    // PostgreSQL syscache name: OPEROID.
    OperOid,
    // PostgreSQL syscache name: OPERNAMENSP.
    OperNameNsp,
    // PostgreSQL syscache name: PARTRELID.
    PartRelId,
    // PostgreSQL systable scan index: PolicyOidIndexId.
    PolicyOid,
    // PostgreSQL systable scan index: PolicyPolrelidPolnameIndexId.
    PolicyPolrelidPolname,
    // PostgreSQL syscache name: PROCOID.
    ProcOid,
    // PostgreSQL syscache name: PROCNAMEARGSNSP.
    ProcNameArgsNsp,
    // PostgreSQL syscache name: PUBLICATIONOID.
    PublicationOid,
    // PostgreSQL syscache name: PUBLICATIONNAME.
    PublicationName,
    // PostgreSQL syscache name: PUBLICATIONREL.
    PublicationRel,
    // PostgreSQL syscache name: PUBLICATIONRELMAP.
    PublicationRelMap,
    // PostgreSQL systable scan index: PublicationRelPrpubidIndexId.
    PublicationRelPrpubid,
    // PostgreSQL syscache name: PUBLICATIONNAMESPACE.
    PublicationNamespace,
    // PostgreSQL syscache name: PUBLICATIONNAMESPACEMAP.
    PublicationNamespaceMap,
    // PostgreSQL syscache name: RELOID.
    RelOid,
    // PostgreSQL syscache name: RELNAMENSP.
    RelNameNsp,
    // PostgreSQL systable scan index: RewriteOidIndexId.
    RewriteOid,
    // PostgreSQL syscache name: RULERELNAME.
    RuleRelName,
    // PostgreSQL systable scan index: TriggerRelidNameIndexId.
    TriggerRelidName,
    // PostgreSQL systable scan index: TriggerOidIndexId.
    TriggerOid,
    // PostgreSQL syscache name: EVENTTRIGGERNAME.
    EventTriggerName,
    // PostgreSQL syscache name: EVENTTRIGGEROID.
    EventTriggerOid,
    // PostgreSQL syscache name: STATRELATTINH.
    StatRelAttInh,
    // PostgreSQL syscache name: STATEXTOID.
    StatExtOid,
    // PostgreSQL syscache name: STATEXTNAMENSP.
    StatExtNameNsp,
    // PostgreSQL systable scan index: StatisticExtRelidIndexId.
    StatisticExtRelId,
    // PostgreSQL systable scan index: StatisticExtDataStxoidInhIndexId.
    StatisticExtDataStxoidInh,
    // PostgreSQL syscache name: TYPEOID.
    TypeOid,
    // PostgreSQL syscache name: TYPENAMENSP.
    TypeNameNsp,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum SysCacheKeyPart {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Text(String),
    InternalChar(u8),
    Bool(bool),
    Array(Vec<SysCacheKeyPart>),
    Null,
}

impl SysCacheKeyPart {
    pub fn from_value(value: &Value) -> Option<Self> {
        match value.to_owned_value() {
            Value::Int16(value) => Some(Self::Int16(value)),
            Value::Int32(value) => Some(Self::Int32(value)),
            Value::Int64(value) => Some(Self::Int64(value)),
            Value::Text(value) => Some(Self::Text(value.to_string())),
            Value::InternalChar(value) => Some(Self::InternalChar(value)),
            Value::Bool(value) => Some(Self::Bool(value)),
            Value::Array(values) => values
                .iter()
                .map(Self::from_value)
                .collect::<Option<Vec<_>>>()
                .map(Self::Array),
            Value::PgArray(array) => array
                .elements
                .iter()
                .map(Self::from_value)
                .collect::<Option<Vec<_>>>()
                .map(Self::Array),
            Value::Null => Some(Self::Null),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SysCacheInvalidationKey {
    pub cache_id: SysCacheId,
    pub keys: Vec<SysCacheKeyPart>,
}

impl SysCacheInvalidationKey {
    pub fn new(cache_id: SysCacheId, keys: Vec<SysCacheKeyPart>) -> Self {
        Self { cache_id, keys }
    }

    pub fn from_values(cache_id: SysCacheId, keys: &[Value]) -> Option<Self> {
        keys.iter()
            .map(SysCacheKeyPart::from_value)
            .collect::<Option<Vec<_>>>()
            .map(|keys| Self { cache_id, keys })
    }

    pub fn matches_prefix(&self, prefix: &Self) -> bool {
        self.cache_id == prefix.cache_id
            && prefix.keys.len() <= self.keys.len()
            && self.keys[..prefix.keys.len()] == prefix.keys
    }
}

impl SysCacheId {
    pub const AGGFNOID: Self = Self::AggFnoid;
    pub const AMNAME: Self = Self::AmName;
    pub const AMOID: Self = Self::AmOid;
    pub const AMOPSTRATEGY: Self = Self::AmopStrategy;
    pub const AMPROCNUM: Self = Self::AmprocNum;
    pub const ATTNAME: Self = Self::AttrName;
    pub const ATTNUM: Self = Self::AttrNum;
    pub const AUTHNAME: Self = Self::AuthIdRolname;
    pub const AUTHOID: Self = Self::AuthIdOid;
    pub const AUTHMEMOID: Self = Self::AuthMembersOid;
    pub const AUTHMEMROLEMEM: Self = Self::AuthMembersRoleMember;
    pub const AUTHMEMMEMROLE: Self = Self::AuthMembersMemberRole;
    pub const AUTHMEMGRANTOR: Self = Self::AuthMembersGrantor;
    pub const SHDEPENDDEPENDER: Self = Self::ShdependDepender;
    pub const SHDEPENDREFERENCE: Self = Self::ShdependReference;
    pub const FOREIGNSERVEROID: Self = Self::ForeignServerOid;
    pub const FOREIGNSERVERNAME: Self = Self::ForeignServerName;
    pub const ATTRDEFAULT: Self = Self::AttrDefault;
    pub const ATTRDEFOID: Self = Self::AttrDefaultOid;
    pub const CASTOID: Self = Self::CastOid;
    pub const CASTSOURCETARGET: Self = Self::CastSourceTarget;
    pub const COLLOID: Self = Self::CollOid;
    pub const CONSTROID: Self = Self::ConstraintOid;
    pub const CONSTRAINTRELID: Self = Self::ConstraintRelId;
    pub const DEPENDDEPENDER: Self = Self::DependDepender;
    pub const DEPENDREFERENCE: Self = Self::DependReference;
    pub const DESCRIPTIONOBJ: Self = Self::DescriptionObj;
    pub const INDEXRELID: Self = Self::IndexRelId;
    pub const INDEXINDRELID: Self = Self::IndexIndRelId;
    pub const INHRELIDSEQNO: Self = Self::InheritsRelIdSeqNo;
    pub const INHPARENT: Self = Self::InheritsParent;
    pub const LANGNAME: Self = Self::LangName;
    pub const LANGOID: Self = Self::LangOid;
    pub const NAMESPACENAME: Self = Self::NamespaceName;
    pub const NAMESPACEOID: Self = Self::NamespaceOid;
    pub const CLAAMNAMENSP: Self = Self::ClaAmNameNsp;
    pub const CLAOID: Self = Self::OpclassOid;
    pub const OPFAMILYOID: Self = Self::OpfamilyOid;
    pub const OPEROID: Self = Self::OperOid;
    pub const OPERNAMENSP: Self = Self::OperNameNsp;
    pub const PARTRELID: Self = Self::PartRelId;
    pub const POLICYOID: Self = Self::PolicyOid;
    pub const POLICYPOLRELIDPOLNAME: Self = Self::PolicyPolrelidPolname;
    pub const PROCOID: Self = Self::ProcOid;
    pub const PROCNAMEARGSNSP: Self = Self::ProcNameArgsNsp;
    pub const PUBLICATIONOID: Self = Self::PublicationOid;
    pub const PUBLICATIONNAME: Self = Self::PublicationName;
    pub const PUBLICATIONREL: Self = Self::PublicationRel;
    pub const PUBLICATIONRELMAP: Self = Self::PublicationRelMap;
    pub const PUBLICATIONRELPRPUBID: Self = Self::PublicationRelPrpubid;
    pub const PUBLICATIONNAMESPACE: Self = Self::PublicationNamespace;
    pub const PUBLICATIONNAMESPACEMAP: Self = Self::PublicationNamespaceMap;
    pub const RELOID: Self = Self::RelOid;
    pub const RELNAMENSP: Self = Self::RelNameNsp;
    pub const REWRITEOID: Self = Self::RewriteOid;
    pub const RULERELNAME: Self = Self::RuleRelName;
    pub const STATRELATTINH: Self = Self::StatRelAttInh;
    pub const STATEXTOID: Self = Self::StatExtOid;
    pub const STATEXTNAMENSP: Self = Self::StatExtNameNsp;
    pub const STATEXTRELID: Self = Self::StatisticExtRelId;
    pub const STATEXTDATASTXOID: Self = Self::StatisticExtDataStxoidInh;
    pub const TRIGGERRELIDNAME: Self = Self::TriggerRelidName;
    pub const TRIGGEROID: Self = Self::TriggerOid;
    pub const TYPEOID: Self = Self::TypeOid;
    pub const TYPENAMENSP: Self = Self::TypeNameNsp;

    pub fn index_oid(self) -> u32 {
        match self {
            Self::AggFnoid => PG_AGGREGATE_FNOID_INDEX_OID,
            Self::AmName => PG_AM_NAME_INDEX_OID,
            Self::AmOid => PG_AM_OID_INDEX_OID,
            Self::AmopStrategy => PG_AMOP_FAM_STRAT_INDEX_OID,
            Self::AmprocNum => PG_AMPROC_FAM_PROC_INDEX_OID,
            Self::AuthIdRolname => PG_AUTHID_ROLNAME_INDEX_OID,
            Self::AuthIdOid => PG_AUTHID_OID_INDEX_OID,
            Self::AuthMembersOid => PG_AUTH_MEMBERS_OID_INDEX_OID,
            Self::AuthMembersRoleMember => PG_AUTH_MEMBERS_ROLE_MEMBER_INDEX_OID,
            Self::AuthMembersMemberRole => PG_AUTH_MEMBERS_MEMBER_ROLE_INDEX_OID,
            Self::AuthMembersGrantor => PG_AUTH_MEMBERS_GRANTOR_INDEX_OID,
            Self::ShdependDepender => PG_SHDEPEND_DEPENDER_INDEX_OID,
            Self::ShdependReference => PG_SHDEPEND_REFERENCE_INDEX_OID,
            Self::ForeignServerOid => PG_FOREIGN_SERVER_OID_INDEX_OID,
            Self::ForeignServerName => PG_FOREIGN_SERVER_NAME_INDEX_OID,
            Self::AttrName => PG_ATTRIBUTE_RELID_ATTNAM_INDEX_OID,
            Self::AttrNum => PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID,
            Self::AttrDefault => PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID,
            Self::AttrDefaultOid => PG_ATTRDEF_OID_INDEX_OID,
            Self::CastOid => PG_CAST_OID_INDEX_OID,
            Self::CastSourceTarget => PG_CAST_SOURCE_TARGET_INDEX_OID,
            Self::CollOid => PG_COLLATION_OID_INDEX_OID,
            Self::ConstraintOid => PG_CONSTRAINT_OID_INDEX_OID,
            Self::ConstraintRelId => PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_OID,
            Self::DependDepender => PG_DEPEND_DEPENDER_INDEX_OID,
            Self::DependReference => PG_DEPEND_REFERENCE_INDEX_OID,
            Self::DescriptionObj => PG_DESCRIPTION_O_C_O_INDEX_OID,
            Self::IndexRelId => PG_INDEX_INDEXRELID_INDEX_OID,
            Self::IndexIndRelId => PG_INDEX_INDRELID_INDEX_OID,
            Self::InheritsRelIdSeqNo => PG_INHERITS_RELID_SEQNO_INDEX_OID,
            Self::InheritsParent => PG_INHERITS_PARENT_INDEX_OID,
            Self::LangName => PG_LANGUAGE_NAME_INDEX_OID,
            Self::LangOid => PG_LANGUAGE_OID_INDEX_OID,
            Self::NamespaceName => PG_NAMESPACE_NSPNAME_INDEX_OID,
            Self::NamespaceOid => PG_NAMESPACE_OID_INDEX_OID,
            Self::ClaAmNameNsp => PG_OPCLASS_AM_NAME_NSP_INDEX_OID,
            Self::OpclassOid => PG_OPCLASS_OID_INDEX_OID,
            Self::OpfamilyOid => PG_OPFAMILY_OID_INDEX_OID,
            Self::OperOid => PG_OPERATOR_OID_INDEX_OID,
            Self::OperNameNsp => PG_OPERATOR_OPRNAME_L_R_N_INDEX_OID,
            Self::PartRelId => PG_PARTITIONED_TABLE_PARTRELID_INDEX_OID,
            Self::PolicyOid => PG_POLICY_OID_INDEX_OID,
            Self::PolicyPolrelidPolname => PG_POLICY_POLRELID_POLNAME_INDEX_OID,
            Self::ProcOid => PG_PROC_OID_INDEX_OID,
            Self::ProcNameArgsNsp => PG_PROC_PRONAME_ARGS_NSP_INDEX_OID,
            Self::PublicationOid => PG_PUBLICATION_OID_INDEX_OID,
            Self::PublicationName => PG_PUBLICATION_PUBNAME_INDEX_OID,
            Self::PublicationRel => PG_PUBLICATION_REL_OID_INDEX_OID,
            Self::PublicationRelMap => PG_PUBLICATION_REL_PRRELID_PRPUBID_INDEX_OID,
            Self::PublicationRelPrpubid => PG_PUBLICATION_REL_PRPUBID_INDEX_OID,
            Self::PublicationNamespace => PG_PUBLICATION_NAMESPACE_OID_INDEX_OID,
            Self::PublicationNamespaceMap => PG_PUBLICATION_NAMESPACE_PNNSPID_PNPUBID_INDEX_OID,
            Self::RelOid => PG_CLASS_OID_INDEX_OID,
            Self::RelNameNsp => PG_CLASS_RELNAME_NSP_INDEX_OID,
            Self::RewriteOid => PG_REWRITE_OID_INDEX_OID,
            Self::RuleRelName => PG_REWRITE_REL_RULENAME_INDEX_OID,
            Self::StatRelAttInh => PG_STATISTIC_RELID_ATT_INH_INDEX_OID,
            Self::StatExtOid => PG_STATISTIC_EXT_OID_INDEX_OID,
            Self::StatExtNameNsp => PG_STATISTIC_EXT_NAME_INDEX_OID,
            Self::StatisticExtRelId => PG_STATISTIC_EXT_RELID_INDEX_OID,
            Self::StatisticExtDataStxoidInh => PG_STATISTIC_EXT_DATA_STXOID_INH_INDEX_OID,
            Self::TriggerRelidName => PG_TRIGGER_RELID_NAME_INDEX_OID,
            Self::TriggerOid => PG_TRIGGER_OID_INDEX_OID,
            Self::EventTriggerName => PG_EVENT_TRIGGER_EVTNAME_INDEX_OID,
            Self::EventTriggerOid => PG_EVENT_TRIGGER_OID_INDEX_OID,
            Self::TypeOid => PG_TYPE_OID_INDEX_OID,
            Self::TypeNameNsp => PG_TYPE_TYPNAME_NSP_INDEX_OID,
        }
    }

    pub fn expected_keys(self) -> usize {
        match self {
            Self::AggFnoid
            | Self::AmName
            | Self::AmOid
            | Self::AuthIdRolname
            | Self::AuthIdOid
            | Self::AuthMembersOid
            | Self::AuthMembersGrantor
            | Self::CastOid
            | Self::CollOid
            | Self::LangName
            | Self::LangOid
            | Self::NamespaceName
            | Self::NamespaceOid
            | Self::OpclassOid
            | Self::OpfamilyOid
            | Self::OperOid
            | Self::PolicyOid
            | Self::ProcOid
            | Self::PublicationOid
            | Self::PublicationName
            | Self::PublicationRel
            | Self::PublicationRelPrpubid
            | Self::PublicationNamespace
            | Self::StatExtOid
            | Self::StatisticExtRelId
            | Self::AttrDefaultOid
            | Self::ConstraintOid
            | Self::ConstraintRelId
            | Self::IndexRelId
            | Self::IndexIndRelId
            | Self::InheritsParent
            | Self::PartRelId
            | Self::RelOid
            | Self::RewriteOid
            | Self::TriggerOid
            | Self::EventTriggerName
            | Self::EventTriggerOid
            | Self::ForeignServerOid
            | Self::ForeignServerName
            | Self::TypeOid => 1,
            Self::AttrDefault
            | Self::AttrName
            | Self::AttrNum
            | Self::CastSourceTarget
            | Self::InheritsRelIdSeqNo
            | Self::ShdependReference
            | Self::PolicyPolrelidPolname
            | Self::PublicationRelMap
            | Self::PublicationNamespaceMap
            | Self::StatExtNameNsp
            | Self::StatisticExtDataStxoidInh
            | Self::TypeNameNsp
            | Self::RelNameNsp
            | Self::RuleRelName
            | Self::TriggerRelidName => 2,
            Self::DependDepender
            | Self::DependReference
            | Self::DescriptionObj
            | Self::ProcNameArgsNsp
            | Self::StatRelAttInh => 3,
            Self::AuthMembersRoleMember | Self::AuthMembersMemberRole => 3,
            Self::ClaAmNameNsp => 3,
            Self::ShdependDepender | Self::AmprocNum | Self::OperNameNsp => 4,
            Self::AmopStrategy => 5,
        }
    }

    pub fn catalog_kind(self) -> Option<BootstrapCatalogKind> {
        system_catalog_index_by_oid(self.index_oid()).map(|descriptor| descriptor.heap_kind)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SysCacheTuple {
    Aggregate(PgAggregateRow),
    Am(PgAmRow),
    Amop(PgAmopRow),
    Amproc(PgAmprocRow),
    Attrdef(PgAttrdefRow),
    Attribute(PgAttributeRow),
    AuthId(PgAuthIdRow),
    AuthMembers(PgAuthMembersRow),
    Cast(PgCastRow),
    Class(PgClassRow),
    Collation(PgCollationRow),
    Constraint(PgConstraintRow),
    Depend(PgDependRow),
    Description(PgDescriptionRow),
    ForeignServer(PgForeignServerRow),
    Index(PgIndexRow),
    Inherits(PgInheritsRow),
    Language(PgLanguageRow),
    Namespace(PgNamespaceRow),
    Opclass(PgOpclassRow),
    Opfamily(PgOpfamilyRow),
    Operator(PgOperatorRow),
    PartitionedTable(PgPartitionedTableRow),
    Policy(PgPolicyRow),
    Proc(PgProcRow),
    Publication(PgPublicationRow),
    PublicationRel(PgPublicationRelRow),
    PublicationNamespace(PgPublicationNamespaceRow),
    Rewrite(PgRewriteRow),
    Shdepend(PgShdependRow),
    Statistic(PgStatisticRow),
    StatisticExt(PgStatisticExtRow),
    StatisticExtData(PgStatisticExtDataRow),
    Trigger(PgTriggerRow),
    EventTrigger(PgEventTriggerRow),
    Type(PgTypeRow),
}

impl SysCacheTuple {
    pub fn oid(&self) -> Option<u32> {
        match self {
            Self::Aggregate(row) => Some(row.aggfnoid),
            Self::Am(row) => Some(row.oid),
            Self::Amop(row) => Some(row.oid),
            Self::Amproc(row) => Some(row.oid),
            Self::Attrdef(row) => Some(row.oid),
            Self::Attribute(_) => None,
            Self::AuthId(row) => Some(row.oid),
            Self::AuthMembers(row) => Some(row.oid),
            Self::Cast(row) => Some(row.oid),
            Self::Class(row) => Some(row.oid),
            Self::Collation(row) => Some(row.oid),
            Self::Constraint(row) => Some(row.oid),
            Self::Depend(_) => None,
            Self::Description(_) => None,
            Self::ForeignServer(row) => Some(row.oid),
            Self::Index(row) => Some(row.indexrelid),
            Self::Inherits(_) => None,
            Self::Language(row) => Some(row.oid),
            Self::Namespace(row) => Some(row.oid),
            Self::Opclass(row) => Some(row.oid),
            Self::Opfamily(row) => Some(row.oid),
            Self::Operator(row) => Some(row.oid),
            Self::PartitionedTable(row) => Some(row.partrelid),
            Self::Policy(row) => Some(row.oid),
            Self::Proc(row) => Some(row.oid),
            Self::Publication(row) => Some(row.oid),
            Self::PublicationRel(row) => Some(row.oid),
            Self::PublicationNamespace(row) => Some(row.oid),
            Self::Rewrite(row) => Some(row.oid),
            Self::Shdepend(_) => None,
            Self::Statistic(_) => None,
            Self::StatisticExt(row) => Some(row.oid),
            Self::StatisticExtData(row) => Some(row.stxoid),
            Self::Trigger(row) => Some(row.oid),
            Self::EventTrigger(row) => Some(row.oid),
            Self::Type(row) => Some(row.oid),
        }
    }
}

fn oid_part(oid: u32) -> SysCacheKeyPart {
    SysCacheKeyPart::Int64(i64::from(oid))
}

fn int16_part(value: i16) -> SysCacheKeyPart {
    SysCacheKeyPart::Int16(value)
}

fn int32_part(value: i32) -> SysCacheKeyPart {
    SysCacheKeyPart::Int32(value)
}

fn bool_part(value: bool) -> SysCacheKeyPart {
    SysCacheKeyPart::Bool(value)
}

fn name_part(value: &str) -> SysCacheKeyPart {
    SysCacheKeyPart::Text(value.to_string())
}

fn oid_vector_part(value: &str) -> SysCacheKeyPart {
    SysCacheKeyPart::Array(
        value
            .split_ascii_whitespace()
            .filter_map(|part| part.parse::<i32>().ok())
            .map(SysCacheKeyPart::Int32)
            .collect(),
    )
}

fn key(cache_id: SysCacheId, keys: Vec<SysCacheKeyPart>) -> SysCacheInvalidationKey {
    SysCacheInvalidationKey::new(cache_id, keys)
}

pub fn syscache_invalidation_keys_for_tuple(tuple: &SysCacheTuple) -> Vec<SysCacheInvalidationKey> {
    match tuple {
        SysCacheTuple::Aggregate(row) => {
            vec![key(SysCacheId::AggFnoid, vec![oid_part(row.aggfnoid)])]
        }
        SysCacheTuple::Am(row) => vec![
            key(SysCacheId::AmName, vec![name_part(&row.amname)]),
            key(SysCacheId::AmOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::Amop(row) => vec![key(
            SysCacheId::AmopStrategy,
            vec![
                oid_part(row.amopfamily),
                oid_part(row.amoplefttype),
                oid_part(row.amoprighttype),
                int16_part(row.amopstrategy),
                SysCacheKeyPart::InternalChar(row.amoppurpose as u8),
            ],
        )],
        SysCacheTuple::Amproc(row) => vec![key(
            SysCacheId::AmprocNum,
            vec![
                oid_part(row.amprocfamily),
                oid_part(row.amproclefttype),
                oid_part(row.amprocrighttype),
                int16_part(row.amprocnum),
            ],
        )],
        SysCacheTuple::Attrdef(row) => vec![
            key(
                SysCacheId::AttrDefault,
                vec![oid_part(row.adrelid), int16_part(row.adnum)],
            ),
            key(SysCacheId::AttrDefaultOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::Attribute(row) => vec![
            key(
                SysCacheId::AttrName,
                vec![oid_part(row.attrelid), name_part(&row.attname)],
            ),
            key(
                SysCacheId::AttrNum,
                vec![oid_part(row.attrelid), int16_part(row.attnum)],
            ),
        ],
        SysCacheTuple::AuthId(row) => vec![
            key(SysCacheId::AuthIdRolname, vec![name_part(&row.rolname)]),
            key(SysCacheId::AuthIdOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::AuthMembers(row) => vec![
            key(SysCacheId::AuthMembersOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::AuthMembersRoleMember,
                vec![
                    oid_part(row.roleid),
                    oid_part(row.member),
                    oid_part(row.grantor),
                ],
            ),
            key(
                SysCacheId::AuthMembersMemberRole,
                vec![
                    oid_part(row.member),
                    oid_part(row.roleid),
                    oid_part(row.grantor),
                ],
            ),
            key(SysCacheId::AuthMembersGrantor, vec![oid_part(row.grantor)]),
        ],
        SysCacheTuple::Shdepend(row) => vec![
            key(
                SysCacheId::ShdependDepender,
                vec![
                    oid_part(row.dbid),
                    oid_part(row.classid),
                    oid_part(row.objid),
                    int32_part(row.objsubid),
                ],
            ),
            key(
                SysCacheId::ShdependReference,
                vec![oid_part(row.refclassid), oid_part(row.refobjid)],
            ),
        ],
        SysCacheTuple::Cast(row) => vec![
            key(SysCacheId::CastOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::CastSourceTarget,
                vec![oid_part(row.castsource), oid_part(row.casttarget)],
            ),
        ],
        SysCacheTuple::Class(row) => vec![
            key(SysCacheId::RelOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::RelNameNsp,
                vec![name_part(&row.relname), oid_part(row.relnamespace)],
            ),
        ],
        SysCacheTuple::Collation(row) => vec![key(SysCacheId::CollOid, vec![oid_part(row.oid)])],
        SysCacheTuple::Constraint(row) => vec![
            key(SysCacheId::ConstraintOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::ConstraintRelId,
                vec![
                    oid_part(row.conrelid),
                    oid_part(row.contypid),
                    name_part(&row.conname),
                ],
            ),
        ],
        SysCacheTuple::Depend(row) => vec![
            key(
                SysCacheId::DependDepender,
                vec![
                    oid_part(row.classid),
                    oid_part(row.objid),
                    int32_part(row.objsubid),
                ],
            ),
            key(
                SysCacheId::DependReference,
                vec![
                    oid_part(row.refclassid),
                    oid_part(row.refobjid),
                    int32_part(row.refobjsubid),
                ],
            ),
        ],
        SysCacheTuple::Description(row) => vec![key(
            SysCacheId::DescriptionObj,
            vec![
                oid_part(row.objoid),
                oid_part(row.classoid),
                int32_part(row.objsubid),
            ],
        )],
        SysCacheTuple::ForeignServer(row) => vec![
            key(SysCacheId::ForeignServerOid, vec![oid_part(row.oid)]),
            key(SysCacheId::ForeignServerName, vec![name_part(&row.srvname)]),
        ],
        SysCacheTuple::Index(row) => vec![
            key(SysCacheId::IndexRelId, vec![oid_part(row.indexrelid)]),
            key(SysCacheId::IndexIndRelId, vec![oid_part(row.indrelid)]),
        ],
        SysCacheTuple::Inherits(row) => vec![
            key(
                SysCacheId::InheritsRelIdSeqNo,
                vec![oid_part(row.inhrelid), int32_part(row.inhseqno)],
            ),
            key(SysCacheId::InheritsParent, vec![oid_part(row.inhparent)]),
        ],
        SysCacheTuple::Language(row) => vec![
            key(SysCacheId::LangName, vec![name_part(&row.lanname)]),
            key(SysCacheId::LangOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::Namespace(row) => vec![
            key(SysCacheId::NamespaceName, vec![name_part(&row.nspname)]),
            key(SysCacheId::NamespaceOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::Opclass(row) => vec![
            key(SysCacheId::OpclassOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::ClaAmNameNsp,
                vec![
                    oid_part(row.opcmethod),
                    name_part(&row.opcname),
                    oid_part(row.opcnamespace),
                ],
            ),
        ],
        SysCacheTuple::Opfamily(row) => vec![key(SysCacheId::OpfamilyOid, vec![oid_part(row.oid)])],
        SysCacheTuple::Operator(row) => vec![
            key(SysCacheId::OperOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::OperNameNsp,
                vec![
                    name_part(&row.oprname),
                    oid_part(row.oprleft),
                    oid_part(row.oprright),
                    oid_part(row.oprnamespace),
                ],
            ),
        ],
        SysCacheTuple::PartitionedTable(row) => {
            vec![key(SysCacheId::PartRelId, vec![oid_part(row.partrelid)])]
        }
        SysCacheTuple::Policy(row) => vec![
            key(SysCacheId::PolicyOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::PolicyPolrelidPolname,
                vec![oid_part(row.polrelid), name_part(&row.polname)],
            ),
        ],
        SysCacheTuple::Proc(row) => vec![
            key(SysCacheId::ProcOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::ProcNameArgsNsp,
                vec![
                    name_part(&row.proname),
                    oid_vector_part(&row.proargtypes),
                    oid_part(row.pronamespace),
                ],
            ),
        ],
        SysCacheTuple::Publication(row) => vec![
            key(SysCacheId::PublicationOid, vec![oid_part(row.oid)]),
            key(SysCacheId::PublicationName, vec![name_part(&row.pubname)]),
        ],
        SysCacheTuple::PublicationRel(row) => vec![
            key(SysCacheId::PublicationRel, vec![oid_part(row.oid)]),
            key(
                SysCacheId::PublicationRelMap,
                vec![oid_part(row.prrelid), oid_part(row.prpubid)],
            ),
            key(
                SysCacheId::PublicationRelPrpubid,
                vec![oid_part(row.prpubid)],
            ),
        ],
        SysCacheTuple::PublicationNamespace(row) => vec![
            key(SysCacheId::PublicationNamespace, vec![oid_part(row.oid)]),
            key(
                SysCacheId::PublicationNamespaceMap,
                vec![oid_part(row.pnnspid), oid_part(row.pnpubid)],
            ),
        ],
        SysCacheTuple::Rewrite(row) => vec![
            key(SysCacheId::RewriteOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::RuleRelName,
                vec![oid_part(row.ev_class), name_part(&row.rulename)],
            ),
        ],
        SysCacheTuple::Statistic(row) => vec![key(
            SysCacheId::StatRelAttInh,
            vec![
                oid_part(row.starelid),
                int16_part(row.staattnum),
                bool_part(row.stainherit),
            ],
        )],
        SysCacheTuple::StatisticExt(row) => vec![
            key(SysCacheId::StatExtOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::StatExtNameNsp,
                vec![name_part(&row.stxname), oid_part(row.stxnamespace)],
            ),
            key(SysCacheId::StatisticExtRelId, vec![oid_part(row.stxrelid)]),
        ],
        SysCacheTuple::StatisticExtData(row) => vec![key(
            SysCacheId::StatisticExtDataStxoidInh,
            vec![oid_part(row.stxoid), bool_part(row.stxdinherit)],
        )],
        SysCacheTuple::Trigger(row) => vec![
            key(
                SysCacheId::TriggerRelidName,
                vec![oid_part(row.tgrelid), name_part(&row.tgname)],
            ),
            key(SysCacheId::TriggerOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::EventTrigger(row) => vec![
            key(SysCacheId::EventTriggerName, vec![name_part(&row.evtname)]),
            key(SysCacheId::EventTriggerOid, vec![oid_part(row.oid)]),
        ],
        SysCacheTuple::Type(row) => vec![
            key(SysCacheId::TypeOid, vec![oid_part(row.oid)]),
            key(
                SysCacheId::TypeNameNsp,
                vec![name_part(&row.typname), oid_part(row.typnamespace)],
            ),
        ],
    }
}

pub fn relcache_oids_for_tuple(tuple: &SysCacheTuple) -> Vec<u32> {
    match tuple {
        SysCacheTuple::Class(row) => vec![row.oid],
        SysCacheTuple::Attribute(row) => vec![row.attrelid],
        SysCacheTuple::Attrdef(row) => vec![row.adrelid],
        SysCacheTuple::Index(row) => vec![row.indexrelid, row.indrelid],
        SysCacheTuple::Inherits(row) => vec![row.inhrelid, row.inhparent],
        SysCacheTuple::Constraint(row) => [row.conrelid, row.confrelid, row.conindid]
            .into_iter()
            .filter(|oid| *oid != 0)
            .collect(),
        SysCacheTuple::PartitionedTable(row) => vec![row.partrelid],
        SysCacheTuple::Rewrite(row) => vec![row.ev_class],
        SysCacheTuple::Trigger(row) => [row.tgrelid, row.tgconstrrelid, row.tgconstrindid]
            .into_iter()
            .filter(|oid| *oid != 0)
            .collect(),
        SysCacheTuple::Policy(row) => vec![row.polrelid],
        SysCacheTuple::Statistic(row) => vec![row.starelid],
        SysCacheTuple::StatisticExt(row) => vec![row.stxrelid],
        SysCacheTuple::PublicationRel(row) => vec![row.prrelid],
        SysCacheTuple::Shdepend(_) => Vec::new(),
        _ => Vec::new(),
    }
}

fn extend_tuple_invalidations(
    tuple: SysCacheTuple,
    keys: &mut Vec<SysCacheInvalidationKey>,
    relcache_oids: &mut Vec<u32>,
) {
    for key in syscache_invalidation_keys_for_tuple(&tuple) {
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    for oid in relcache_oids_for_tuple(&tuple) {
        if !relcache_oids.contains(&oid) {
            relcache_oids.push(oid);
        }
    }
}

pub fn catalog_row_invalidations_for_rows(
    rows: &PhysicalCatalogRows,
) -> (Vec<SysCacheInvalidationKey>, Vec<u32>) {
    let mut keys = Vec::new();
    let mut relcache_oids = Vec::new();
    for row in &rows.aggregates {
        extend_tuple_invalidations(
            SysCacheTuple::Aggregate(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.ams {
        extend_tuple_invalidations(
            SysCacheTuple::Am(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.amops {
        extend_tuple_invalidations(
            SysCacheTuple::Amop(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.amprocs {
        extend_tuple_invalidations(
            SysCacheTuple::Amproc(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.attrdefs {
        extend_tuple_invalidations(
            SysCacheTuple::Attrdef(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.attributes {
        extend_tuple_invalidations(
            SysCacheTuple::Attribute(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.authids {
        extend_tuple_invalidations(
            SysCacheTuple::AuthId(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.auth_members {
        extend_tuple_invalidations(
            SysCacheTuple::AuthMembers(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.shdepends {
        extend_tuple_invalidations(
            SysCacheTuple::Shdepend(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.casts {
        extend_tuple_invalidations(
            SysCacheTuple::Cast(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.classes {
        extend_tuple_invalidations(
            SysCacheTuple::Class(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.collations {
        extend_tuple_invalidations(
            SysCacheTuple::Collation(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.constraints {
        extend_tuple_invalidations(
            SysCacheTuple::Constraint(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.depends {
        extend_tuple_invalidations(
            SysCacheTuple::Depend(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.descriptions {
        extend_tuple_invalidations(
            SysCacheTuple::Description(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.foreign_servers {
        extend_tuple_invalidations(
            SysCacheTuple::ForeignServer(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.indexes {
        extend_tuple_invalidations(
            SysCacheTuple::Index(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.inherits {
        extend_tuple_invalidations(
            SysCacheTuple::Inherits(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.languages {
        extend_tuple_invalidations(
            SysCacheTuple::Language(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.namespaces {
        extend_tuple_invalidations(
            SysCacheTuple::Namespace(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.opclasses {
        extend_tuple_invalidations(
            SysCacheTuple::Opclass(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.opfamilies {
        extend_tuple_invalidations(
            SysCacheTuple::Opfamily(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.operators {
        extend_tuple_invalidations(
            SysCacheTuple::Operator(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.partitioned_tables {
        extend_tuple_invalidations(
            SysCacheTuple::PartitionedTable(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.policies {
        extend_tuple_invalidations(
            SysCacheTuple::Policy(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.procs {
        extend_tuple_invalidations(
            SysCacheTuple::Proc(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.publications {
        extend_tuple_invalidations(
            SysCacheTuple::Publication(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.publication_rels {
        extend_tuple_invalidations(
            SysCacheTuple::PublicationRel(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.publication_namespaces {
        extend_tuple_invalidations(
            SysCacheTuple::PublicationNamespace(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.rewrites {
        extend_tuple_invalidations(
            SysCacheTuple::Rewrite(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.statistics {
        extend_tuple_invalidations(
            SysCacheTuple::Statistic(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.statistics_ext {
        extend_tuple_invalidations(
            SysCacheTuple::StatisticExt(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.statistics_ext_data {
        extend_tuple_invalidations(
            SysCacheTuple::StatisticExtData(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.triggers {
        extend_tuple_invalidations(
            SysCacheTuple::Trigger(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.event_triggers {
        extend_tuple_invalidations(
            SysCacheTuple::EventTrigger(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    for row in &rows.types {
        extend_tuple_invalidations(
            SysCacheTuple::Type(row.clone()),
            &mut keys,
            &mut relcache_oids,
        );
    }
    (keys, relcache_oids)
}

pub fn oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

pub fn equality_scan_keys(keys: &[Value]) -> Vec<ScanKeyData> {
    keys.iter()
        .enumerate()
        .map(|(index, value)| ScanKeyData {
            attribute_number: index.saturating_add(1) as i16,
            strategy: BT_EQUAL_STRATEGY_NUMBER,
            argument: value.to_owned_value(),
        })
        .collect()
}

pub fn bootstrap_sys_cache_tuple(cache_id: SysCacheId, keys: &[Value]) -> Option<SysCacheTuple> {
    match (cache_id, keys) {
        (SysCacheId::TypeOid, [key]) => {
            let oid = match key {
                Value::Int32(value) => u32::try_from(*value).ok()?,
                Value::Int64(value) => u32::try_from(*value).ok()?,
                _ => return None,
            };
            builtin_type_rows()
                .into_iter()
                .chain(bootstrap_composite_type_rows())
                .find(|row| row.oid == oid)
                .map(SysCacheTuple::Type)
        }
        (SysCacheId::TypeNameNsp, [Value::Text(name), namespace_key]) => {
            let namespace_oid = match namespace_key {
                Value::Int32(value) => u32::try_from(*value).ok()?,
                Value::Int64(value) => u32::try_from(*value).ok()?,
                _ => return None,
            };
            builtin_type_rows()
                .into_iter()
                .chain(bootstrap_composite_type_rows())
                .find(|row| {
                    row.typnamespace == namespace_oid
                        && row.typname.eq_ignore_ascii_case(name.as_str())
                })
                .map(SysCacheTuple::Type)
        }
        _ => None,
    }
}

pub fn extra_type_sys_cache_tuples(
    type_rows: &[PgTypeRow],
    cache_id: SysCacheId,
    keys: &[Value],
) -> Vec<SysCacheTuple> {
    match (cache_id, keys) {
        (SysCacheId::TypeOid, [key]) => {
            let oid = match key {
                Value::Int32(value) => u32::try_from(*value).ok(),
                Value::Int64(value) => u32::try_from(*value).ok(),
                _ => None,
            };
            oid.into_iter()
                .flat_map(|oid| type_rows.iter().filter(move |row| row.oid == oid).cloned())
                .map(SysCacheTuple::Type)
                .collect()
        }
        (SysCacheId::TypeNameNsp, [Value::Text(name), namespace_key]) => {
            let namespace_oid = match namespace_key {
                Value::Int32(value) => u32::try_from(*value).ok(),
                Value::Int64(value) => u32::try_from(*value).ok(),
                _ => None,
            };
            namespace_oid
                .into_iter()
                .flat_map(|namespace_oid| {
                    type_rows
                        .iter()
                        .filter(move |row| {
                            row.typnamespace == namespace_oid
                                && row.typname.eq_ignore_ascii_case(name.as_str())
                        })
                        .cloned()
                })
                .map(SysCacheTuple::Type)
                .collect()
        }
        _ => Vec::new(),
    }
}

pub fn sys_cache_tuple_from_values(
    cache_id: SysCacheId,
    values: Vec<Value>,
) -> Result<SysCacheTuple, CatalogError> {
    match cache_id {
        SysCacheId::AggFnoid => pg_aggregate_row_from_values(values).map(SysCacheTuple::Aggregate),
        SysCacheId::AmName | SysCacheId::AmOid => {
            pg_am_row_from_values(values).map(SysCacheTuple::Am)
        }
        SysCacheId::AmopStrategy => pg_amop_row_from_values(values).map(SysCacheTuple::Amop),
        SysCacheId::AmprocNum => pg_amproc_row_from_values(values).map(SysCacheTuple::Amproc),
        SysCacheId::AttrDefault | SysCacheId::AttrDefaultOid => {
            pg_attrdef_row_from_values(values).map(SysCacheTuple::Attrdef)
        }
        SysCacheId::AttrName | SysCacheId::AttrNum => {
            pg_attribute_row_from_values(values).map(SysCacheTuple::Attribute)
        }
        SysCacheId::AuthIdRolname | SysCacheId::AuthIdOid => {
            pg_authid_row_from_values(values).map(SysCacheTuple::AuthId)
        }
        SysCacheId::AuthMembersOid
        | SysCacheId::AuthMembersRoleMember
        | SysCacheId::AuthMembersMemberRole
        | SysCacheId::AuthMembersGrantor => {
            pg_auth_members_row_from_values(values).map(SysCacheTuple::AuthMembers)
        }
        SysCacheId::ShdependDepender | SysCacheId::ShdependReference => {
            pg_shdepend_row_from_values(values).map(SysCacheTuple::Shdepend)
        }
        SysCacheId::CastOid | SysCacheId::CastSourceTarget => {
            pg_cast_row_from_values(values).map(SysCacheTuple::Cast)
        }
        SysCacheId::CollOid => pg_collation_row_from_values(values).map(SysCacheTuple::Collation),
        SysCacheId::ConstraintOid | SysCacheId::ConstraintRelId => {
            pg_constraint_row_from_values(values).map(SysCacheTuple::Constraint)
        }
        SysCacheId::DependDepender | SysCacheId::DependReference => {
            pg_depend_row_from_values(values).map(SysCacheTuple::Depend)
        }
        SysCacheId::DescriptionObj => {
            pg_description_row_from_values(values).map(SysCacheTuple::Description)
        }
        SysCacheId::ForeignServerOid | SysCacheId::ForeignServerName => {
            pg_foreign_server_row_from_values(values).map(SysCacheTuple::ForeignServer)
        }
        SysCacheId::IndexRelId | SysCacheId::IndexIndRelId => {
            pg_index_row_from_values(values).map(SysCacheTuple::Index)
        }
        SysCacheId::InheritsRelIdSeqNo | SysCacheId::InheritsParent => {
            pg_inherits_row_from_values(values).map(SysCacheTuple::Inherits)
        }
        SysCacheId::LangName | SysCacheId::LangOid => {
            pg_language_row_from_values(values).map(SysCacheTuple::Language)
        }
        SysCacheId::NamespaceName | SysCacheId::NamespaceOid => {
            namespace_row_from_values(values).map(SysCacheTuple::Namespace)
        }
        SysCacheId::ClaAmNameNsp | SysCacheId::OpclassOid => {
            pg_opclass_row_from_values(values).map(SysCacheTuple::Opclass)
        }
        SysCacheId::OpfamilyOid => pg_opfamily_row_from_values(values).map(SysCacheTuple::Opfamily),
        SysCacheId::OperOid | SysCacheId::OperNameNsp => {
            pg_operator_row_from_values(values).map(SysCacheTuple::Operator)
        }
        SysCacheId::PartRelId => {
            pg_partitioned_table_row_from_values(values).map(SysCacheTuple::PartitionedTable)
        }
        SysCacheId::PolicyOid | SysCacheId::PolicyPolrelidPolname => {
            pg_policy_row_from_values(values).map(SysCacheTuple::Policy)
        }
        SysCacheId::ProcOid | SysCacheId::ProcNameArgsNsp => {
            pg_proc_row_from_values(values).map(SysCacheTuple::Proc)
        }
        SysCacheId::PublicationOid | SysCacheId::PublicationName => {
            pg_publication_row_from_values(values).map(SysCacheTuple::Publication)
        }
        SysCacheId::PublicationRel
        | SysCacheId::PublicationRelMap
        | SysCacheId::PublicationRelPrpubid => {
            pg_publication_rel_row_from_values(values).map(SysCacheTuple::PublicationRel)
        }
        SysCacheId::PublicationNamespace | SysCacheId::PublicationNamespaceMap => {
            pg_publication_namespace_row_from_values(values)
                .map(SysCacheTuple::PublicationNamespace)
        }
        SysCacheId::RelOid | SysCacheId::RelNameNsp => {
            pg_class_row_from_values(values).map(SysCacheTuple::Class)
        }
        SysCacheId::RewriteOid | SysCacheId::RuleRelName => {
            pg_rewrite_row_from_values(values).map(SysCacheTuple::Rewrite)
        }
        SysCacheId::StatRelAttInh => {
            pg_statistic_row_from_values(values).map(SysCacheTuple::Statistic)
        }
        SysCacheId::StatExtOid | SysCacheId::StatExtNameNsp | SysCacheId::StatisticExtRelId => {
            pg_statistic_ext_row_from_values(values).map(SysCacheTuple::StatisticExt)
        }
        SysCacheId::StatisticExtDataStxoidInh => {
            pg_statistic_ext_data_row_from_values(values).map(SysCacheTuple::StatisticExtData)
        }
        SysCacheId::TriggerRelidName | SysCacheId::TriggerOid => {
            pg_trigger_row_from_values(values).map(SysCacheTuple::Trigger)
        }
        SysCacheId::EventTriggerName | SysCacheId::EventTriggerOid => {
            pg_event_trigger_row_from_values(values).map(SysCacheTuple::EventTrigger)
        }
        SysCacheId::TypeOid | SysCacheId::TypeNameNsp => {
            pg_type_row_from_values(values).map(SysCacheTuple::Type)
        }
    }
}
