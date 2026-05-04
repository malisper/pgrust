use crate::catalog::CatalogError;
use crate::rowcodec::{
    namespace_row_from_values, pg_aggregate_row_from_values, pg_am_row_from_values,
    pg_amop_row_from_values, pg_amproc_row_from_values, pg_attrdef_row_from_values,
    pg_attribute_row_from_values, pg_auth_members_row_from_values, pg_authid_row_from_values,
    pg_cast_row_from_values, pg_class_row_from_values, pg_collation_row_from_values,
    pg_constraint_row_from_values, pg_depend_row_from_values, pg_description_row_from_values,
    pg_event_trigger_row_from_values, pg_index_row_from_values, pg_inherits_row_from_values,
    pg_language_row_from_values, pg_opclass_row_from_values, pg_operator_row_from_values,
    pg_opfamily_row_from_values, pg_partitioned_table_row_from_values, pg_policy_row_from_values,
    pg_proc_row_from_values, pg_publication_namespace_row_from_values,
    pg_publication_rel_row_from_values, pg_publication_row_from_values, pg_rewrite_row_from_values,
    pg_statistic_ext_data_row_from_values, pg_statistic_ext_row_from_values,
    pg_statistic_row_from_values, pg_trigger_row_from_values, pg_type_row_from_values,
};
use pgrust_catalog_data::{
    BootstrapCatalogKind, PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgDependRow, PgDescriptionRow, PgEventTriggerRow, PgIndexRow, PgInheritsRow,
    PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow,
    PgPartitionedTableRow, PgPolicyRow, PgProcRow, PgPublicationNamespaceRow, PgPublicationRelRow,
    PgPublicationRow, PgRewriteRow, PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow,
    PgTriggerRow, PgTypeRow, bootstrap_composite_type_rows, builtin_type_rows,
    system_catalog_index_by_oid,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
            | Self::TypeOid => 1,
            Self::AttrDefault
            | Self::AttrName
            | Self::AttrNum
            | Self::CastSourceTarget
            | Self::InheritsRelIdSeqNo
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
            Self::AmprocNum | Self::OperNameNsp => 4,
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
            Self::Statistic(_) => None,
            Self::StatisticExt(row) => Some(row.oid),
            Self::StatisticExtData(row) => Some(row.stxoid),
            Self::Trigger(row) => Some(row.oid),
            Self::EventTrigger(row) => Some(row.oid),
            Self::Type(row) => Some(row.oid),
        }
    }
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
