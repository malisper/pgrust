use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible_in_db;
use crate::backend::catalog::rowcodec::{
    namespace_row_from_values, pg_aggregate_row_from_values, pg_am_row_from_values,
    pg_amop_row_from_values, pg_amproc_row_from_values, pg_attrdef_row_from_values,
    pg_attribute_row_from_values, pg_auth_members_row_from_values, pg_authid_row_from_values,
    pg_class_row_from_values, pg_collation_row_from_values, pg_constraint_row_from_values,
    pg_depend_row_from_values, pg_description_row_from_values, pg_index_row_from_values,
    pg_inherits_row_from_values, pg_language_row_from_values, pg_opclass_row_from_values,
    pg_operator_row_from_values, pg_opfamily_row_from_values, pg_partitioned_table_row_from_values,
    pg_policy_row_from_values, pg_proc_row_from_values, pg_publication_namespace_row_from_values,
    pg_publication_rel_row_from_values, pg_publication_row_from_values, pg_rewrite_row_from_values,
    pg_statistic_ext_data_row_from_values, pg_statistic_ext_row_from_values,
    pg_statistic_row_from_values, pg_trigger_row_from_values, pg_type_row_from_values,
};
use crate::backend::catalog::store::{CatalogStore, CatalogWriteContext};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::lsyscache::dynamic_type_rows_for_search_path;
use crate::backend::utils::cache::relcache::{
    IndexAmOpEntry, IndexAmProcEntry, IndexRelCacheEntry, RelCache, RelCacheEntry,
    relation_locator_for_class_row,
};
use crate::backend::utils::time::snapmgr::{Snapshot, get_catalog_snapshot};
use crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    PG_CONSTRAINT_RELATION_OID, PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgClassRow, PgCollationRow, PgConstraintRow,
    PgDependRow, PgDescriptionRow, PgIndexRow, PgInheritsRow, PgLanguageRow, PgNamespaceRow,
    PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgPartitionedTableRow, PgPolicyRow, PgProcRow,
    PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow, PgRewriteRow,
    PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow, PgTriggerRow, PgTypeRow,
    bootstrap_composite_type_rows, builtin_type_rows, system_catalog_index_by_oid,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::SqlType;
use crate::pgrust::database::Database;

const PG_ATTRIBUTE_RELID_ATTNAM_INDEX_OID: u32 = 2658;
const PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID: u32 = 2659;
const PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID: u32 = 2656;
const PG_ATTRDEF_OID_INDEX_OID: u32 = 2657;
const PG_AGGREGATE_FNOID_INDEX_OID: u32 = 2650;
const PG_AM_NAME_INDEX_OID: u32 = 2651;
const PG_AM_OID_INDEX_OID: u32 = 2652;
const PG_AMOP_FAM_STRAT_INDEX_OID: u32 = 2653;
const PG_AMPROC_FAM_PROC_INDEX_OID: u32 = 2655;
const PG_AUTHID_ROLNAME_INDEX_OID: u32 = 2676;
const PG_AUTHID_OID_INDEX_OID: u32 = 2677;
const PG_AUTH_MEMBERS_OID_INDEX_OID: u32 = 6303;
const PG_AUTH_MEMBERS_ROLE_MEMBER_INDEX_OID: u32 = 2694;
const PG_AUTH_MEMBERS_MEMBER_ROLE_INDEX_OID: u32 = 2695;
const PG_AUTH_MEMBERS_GRANTOR_INDEX_OID: u32 = 6302;
const PG_CLASS_OID_INDEX_OID: u32 = 2662;
const PG_CLASS_RELNAME_NSP_INDEX_OID: u32 = 2663;
const PG_COLLATION_OID_INDEX_OID: u32 = 3085;
const PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_OID: u32 = 2665;
const PG_CONSTRAINT_OID_INDEX_OID: u32 = 2667;
const PG_DEPEND_DEPENDER_INDEX_OID: u32 = 2673;
const PG_DEPEND_REFERENCE_INDEX_OID: u32 = 2674;
const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;
const PG_INDEX_INDRELID_INDEX_OID: u32 = 2678;
const PG_INDEX_INDEXRELID_INDEX_OID: u32 = 2679;
const PG_INHERITS_RELID_SEQNO_INDEX_OID: u32 = 2680;
const PG_INHERITS_PARENT_INDEX_OID: u32 = 2187;
const PG_LANGUAGE_NAME_INDEX_OID: u32 = 2681;
const PG_LANGUAGE_OID_INDEX_OID: u32 = 2682;
const PG_NAMESPACE_NSPNAME_INDEX_OID: u32 = 2684;
const PG_NAMESPACE_OID_INDEX_OID: u32 = 2685;
const PG_OPCLASS_AM_NAME_NSP_INDEX_OID: u32 = 2686;
const PG_OPCLASS_OID_INDEX_OID: u32 = 2687;
const PG_OPFAMILY_OID_INDEX_OID: u32 = 2755;
const PG_OPERATOR_OID_INDEX_OID: u32 = 2688;
const PG_OPERATOR_OPRNAME_L_R_N_INDEX_OID: u32 = 2689;
const PG_PARTITIONED_TABLE_PARTRELID_INDEX_OID: u32 = 3351;
const PG_POLICY_OID_INDEX_OID: u32 = 3257;
const PG_POLICY_POLRELID_POLNAME_INDEX_OID: u32 = 3258;
const PG_PROC_OID_INDEX_OID: u32 = 2690;
const PG_PROC_PRONAME_ARGS_NSP_INDEX_OID: u32 = 2691;
const PG_PUBLICATION_OID_INDEX_OID: u32 = 6110;
const PG_PUBLICATION_PUBNAME_INDEX_OID: u32 = 6111;
const PG_PUBLICATION_REL_OID_INDEX_OID: u32 = 6112;
const PG_PUBLICATION_REL_PRRELID_PRPUBID_INDEX_OID: u32 = 6113;
const PG_PUBLICATION_REL_PRPUBID_INDEX_OID: u32 = 6116;
const PG_PUBLICATION_NAMESPACE_OID_INDEX_OID: u32 = 6238;
const PG_PUBLICATION_NAMESPACE_PNNSPID_PNPUBID_INDEX_OID: u32 = 6239;
const PG_REWRITE_OID_INDEX_OID: u32 = 2692;
const PG_REWRITE_REL_RULENAME_INDEX_OID: u32 = 2693;
const PG_STATISTIC_RELID_ATT_INH_INDEX_OID: u32 = 2696;
const PG_STATISTIC_EXT_RELID_INDEX_OID: u32 = 3379;
const PG_STATISTIC_EXT_OID_INDEX_OID: u32 = 3380;
const PG_STATISTIC_EXT_NAME_INDEX_OID: u32 = 3997;
const PG_STATISTIC_EXT_DATA_STXOID_INH_INDEX_OID: u32 = 3433;
const PG_TRIGGER_RELID_NAME_INDEX_OID: u32 = 2701;
const PG_TRIGGER_OID_INDEX_OID: u32 = 2702;
const PG_TYPE_OID_INDEX_OID: u32 = 2703;
const PG_TYPE_TYPNAME_NSP_INDEX_OID: u32 = 2704;

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
    fn index_oid(self) -> u32 {
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
            Self::TypeOid => PG_TYPE_OID_INDEX_OID,
            Self::TypeNameNsp => PG_TYPE_TYPNAME_NSP_INDEX_OID,
        }
    }

    fn expected_keys(self) -> usize {
        match self {
            Self::AggFnoid
            | Self::AmName
            | Self::AmOid
            | Self::AuthIdRolname
            | Self::AuthIdOid
            | Self::AuthMembersOid
            | Self::AuthMembersGrantor
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
            | Self::TypeOid => 1,
            Self::AttrDefault
            | Self::AttrName
            | Self::AttrNum
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

    fn catalog_kind(self) -> Option<crate::include::catalog::BootstrapCatalogKind> {
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
    Type(PgTypeRow),
}

fn oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn equality_scan_keys(keys: &[Value]) -> Vec<ScanKeyData> {
    keys.iter()
        .enumerate()
        .map(|(index, value)| ScanKeyData {
            attribute_number: index.saturating_add(1) as i16,
            strategy: BT_EQUAL_STRATEGY_NUMBER,
            argument: value.to_owned_value(),
        })
        .collect()
}

fn bootstrap_sys_cache_tuple(cache_id: SysCacheId, keys: &[Value]) -> Option<SysCacheTuple> {
    let SysCacheId::TypeOid = cache_id else {
        return None;
    };
    let [key] = keys else {
        return None;
    };
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

fn extra_type_sys_cache_tuples(
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

fn sys_cache_tuple_from_values(
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
        SysCacheId::TypeOid | SysCacheId::TypeNameNsp => {
            pg_type_row_from_values(values).map(SysCacheTuple::Type)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ResolvedIndexSupportMetadata {
    opfamily_oids: Vec<u32>,
    opcintype_oids: Vec<u32>,
    opckeytype_oids: Vec<u32>,
    amop_entries: Vec<Vec<IndexAmOpEntry>>,
    amproc_entries: Vec<Vec<IndexAmProcEntry>>,
}

fn resolve_index_support_metadata<OpclassByOid, AmopRows, AmprocRows, OperatorByOid>(
    indclass: &[u32],
    mut opclass_by_oid: OpclassByOid,
    mut amop_rows_for_family: AmopRows,
    mut amproc_rows_for_family: AmprocRows,
    mut operator_by_oid: OperatorByOid,
) -> Result<ResolvedIndexSupportMetadata, CatalogError>
where
    OpclassByOid: FnMut(u32) -> Result<Option<PgOpclassRow>, CatalogError>,
    AmopRows: FnMut(u32) -> Result<Vec<PgAmopRow>, CatalogError>,
    AmprocRows: FnMut(u32) -> Result<Vec<PgAmprocRow>, CatalogError>,
    OperatorByOid: FnMut(u32) -> Result<Option<PgOperatorRow>, CatalogError>,
{
    let mut resolved_opclasses = Vec::new();
    for oid in indclass {
        if let Some(row) = opclass_by_oid(*oid)? {
            resolved_opclasses.push(row);
        }
    }
    let opfamily_oids = resolved_opclasses
        .iter()
        .map(|row| row.opcfamily)
        .collect::<Vec<_>>();
    let opcintype_oids = resolved_opclasses
        .iter()
        .map(|row| row.opcintype)
        .collect::<Vec<_>>();
    let opckeytype_oids = resolved_opclasses
        .iter()
        .map(|row| row.opckeytype)
        .collect::<Vec<_>>();

    let mut operator_cache = BTreeMap::<u32, Option<PgOperatorRow>>::new();
    let mut amop_entries = Vec::with_capacity(opfamily_oids.len());
    for family_oid in &opfamily_oids {
        let mut entries = Vec::new();
        for row in amop_rows_for_family(*family_oid)? {
            if !operator_cache.contains_key(&row.amopopr) {
                operator_cache.insert(row.amopopr, operator_by_oid(row.amopopr)?);
            }
            entries.push(IndexAmOpEntry {
                strategy: row.amopstrategy,
                purpose: row.amoppurpose,
                lefttype: row.amoplefttype,
                righttype: row.amoprighttype,
                operator_oid: row.amopopr,
                operator_proc_oid: operator_cache
                    .get(&row.amopopr)
                    .and_then(|row| row.as_ref())
                    .map(|row| row.oprcode)
                    .unwrap_or(0),
                sortfamily_oid: row.amopsortfamily,
            });
        }
        amop_entries.push(entries);
    }

    let mut amproc_entries = Vec::with_capacity(opfamily_oids.len());
    for family_oid in &opfamily_oids {
        amproc_entries.push(
            amproc_rows_for_family(*family_oid)?
                .into_iter()
                .map(|row| IndexAmProcEntry {
                    procnum: row.amprocnum,
                    lefttype: row.amproclefttype,
                    righttype: row.amprocrighttype,
                    proc_oid: row.amproc,
                })
                .collect(),
        );
    }

    Ok(ResolvedIndexSupportMetadata {
        opfamily_oids,
        opcintype_oids,
        opckeytype_oids,
        amop_entries,
        amproc_entries,
    })
}

fn index_relcache_entry_from_index_row(
    class_row: &PgClassRow,
    index: PgIndexRow,
    am_handler_oid: Option<u32>,
    support: ResolvedIndexSupportMetadata,
) -> IndexRelCacheEntry {
    IndexRelCacheEntry {
        indexrelid: index.indexrelid,
        indrelid: index.indrelid,
        indnatts: index.indnatts,
        indnkeyatts: index.indnkeyatts,
        indisunique: index.indisunique,
        indnullsnotdistinct: index.indnullsnotdistinct,
        indisprimary: index.indisprimary,
        indisexclusion: index.indisexclusion,
        indimmediate: index.indimmediate,
        indisclustered: index.indisclustered,
        indisvalid: index.indisvalid,
        indcheckxmin: index.indcheckxmin,
        indisready: index.indisready,
        indislive: index.indislive,
        indisreplident: index.indisreplident,
        am_oid: class_row.relam,
        am_handler_oid,
        indkey: index.indkey,
        indclass: index.indclass,
        indcollation: index.indcollation,
        indoption: index.indoption,
        opfamily_oids: support.opfamily_oids,
        opcintype_oids: support.opcintype_oids,
        opckeytype_oids: support.opckeytype_oids,
        amop_entries: support.amop_entries,
        amproc_entries: support.amproc_entries,
        indexprs: index.indexprs,
        indpred: index.indpred,
        rd_indexprs: None,
        rd_indpred: None,
        brin_options: None,
        gin_options: None,
        hash_options: None,
    }
}

fn merge_catcaches(shared: CatCache, local: CatCache) -> CatCache {
    CatCache::from_rows(
        local.namespace_rows(),
        local.class_rows(),
        local.attribute_rows(),
        local.attrdef_rows(),
        local.depend_rows(),
        local.inherit_rows(),
        local.index_rows(),
        local.rewrite_rows(),
        local.trigger_rows(),
        local.policy_rows(),
        local.publication_rows(),
        local.publication_rel_rows(),
        local.publication_namespace_rows(),
        local.statistic_ext_rows(),
        local.statistic_ext_data_rows(),
        local.am_rows(),
        local.amop_rows(),
        local.amproc_rows(),
        shared.authid_rows(),
        shared.auth_members_rows(),
        local.language_rows(),
        local.ts_parser_rows(),
        local.ts_template_rows(),
        local.ts_dict_rows(),
        local.ts_config_rows(),
        local.ts_config_map_rows(),
        local.constraint_rows(),
        local.operator_rows(),
        local.opclass_rows(),
        local.opfamily_rows(),
        local.partitioned_table_rows(),
        local.proc_rows(),
        local.aggregate_rows(),
        local.cast_rows(),
        local.collation_rows(),
        local.foreign_data_wrapper_rows(),
        shared.database_rows(),
        shared.tablespace_rows(),
        local.statistic_rows(),
        local.type_rows(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCacheContext {
    Autocommit,
    Transaction { xid: TransactionId, cid: CommandId },
}

impl From<Option<(TransactionId, CommandId)>> for BackendCacheContext {
    fn from(txn_ctx: Option<(TransactionId, CommandId)>) -> Self {
        match txn_ctx {
            Some((xid, cid)) => Self::Transaction { xid, cid },
            None => Self::Autocommit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SysCacheKeyPart {
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
    fn from_value(value: &Value) -> Option<Self> {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SysCacheQueryKey {
    cache_id: SysCacheId,
    keys: Vec<SysCacheKeyPart>,
}

impl SysCacheQueryKey {
    fn new(cache_id: SysCacheId, keys: &[Value]) -> Option<Self> {
        keys.iter()
            .map(SysCacheKeyPart::from_value)
            .collect::<Option<Vec<_>>>()
            .map(|keys| Self { cache_id, keys })
    }

    fn invalidated_by(&self, invalidation: &CatalogInvalidation) -> bool {
        if invalidation.full_reset {
            return true;
        }
        if invalidation.touched_catalogs.is_empty() {
            return !invalidation.is_empty();
        }
        self.cache_id
            .catalog_kind()
            .is_none_or(|kind| invalidation.touched_catalogs.contains(&kind))
    }
}

#[derive(Debug, Clone, Copy)]
enum SysCacheLookupMode {
    Exact,
    List,
}

#[derive(Debug, Default, Clone)]
struct BackendSysCacheMaps {
    exact: HashMap<SysCacheQueryKey, Vec<SysCacheTuple>>,
    list: HashMap<SysCacheQueryKey, Vec<SysCacheTuple>>,
}

impl BackendSysCacheMaps {
    fn get(&self, mode: SysCacheLookupMode, key: &SysCacheQueryKey) -> Option<Vec<SysCacheTuple>> {
        match mode {
            SysCacheLookupMode::Exact => self.exact.get(key),
            SysCacheLookupMode::List => self.list.get(key),
        }
        .cloned()
    }

    fn insert(
        &mut self,
        mode: SysCacheLookupMode,
        key: SysCacheQueryKey,
        value: Vec<SysCacheTuple>,
    ) {
        match mode {
            SysCacheLookupMode::Exact => self.exact.insert(key, value),
            SysCacheLookupMode::List => self.list.insert(key, value),
        };
    }

    fn invalidate(&mut self, invalidation: &CatalogInvalidation) {
        self.exact
            .retain(|key, _| !key.invalidated_by(invalidation));
        self.list.retain(|key, _| !key.invalidated_by(invalidation));
    }

    fn clear(&mut self) {
        self.exact.clear();
        self.list.clear();
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct BackendSysCache {
    autocommit: BackendSysCacheMaps,
    transaction: BackendSysCacheMaps,
    transaction_ctx: Option<BackendCacheContext>,
}

impl BackendSysCache {
    fn maps_mut(&mut self, cache_ctx: BackendCacheContext) -> &mut BackendSysCacheMaps {
        match cache_ctx {
            BackendCacheContext::Autocommit => &mut self.autocommit,
            BackendCacheContext::Transaction { .. } => {
                if self.transaction_ctx != Some(cache_ctx) {
                    self.transaction.clear();
                    self.transaction_ctx = Some(cache_ctx);
                }
                &mut self.transaction
            }
        }
    }

    fn get(
        &mut self,
        cache_ctx: BackendCacheContext,
        mode: SysCacheLookupMode,
        key: &SysCacheQueryKey,
    ) -> Option<Vec<SysCacheTuple>> {
        self.maps_mut(cache_ctx).get(mode, key)
    }

    fn insert(
        &mut self,
        cache_ctx: BackendCacheContext,
        mode: SysCacheLookupMode,
        key: SysCacheQueryKey,
        value: Vec<SysCacheTuple>,
    ) {
        self.maps_mut(cache_ctx).insert(mode, key, value);
    }

    pub(crate) fn invalidate(&mut self, invalidation: &CatalogInvalidation) {
        if invalidation.full_reset {
            *self = Self::default();
            return;
        }
        self.autocommit.invalidate(invalidation);
        self.transaction.invalidate(invalidation);
    }
}

#[derive(Debug, Default, Clone)]
pub struct BackendCacheState {
    pub catalog_snapshot: Option<Snapshot>,
    pub catalog_snapshot_ctx: Option<BackendCacheContext>,
    pub transaction_snapshot_override: Option<(TransactionId, Snapshot)>,
    pub catcache: Option<CatCache>,
    pub relcache: Option<RelCache>,
    pub cache_ctx: Option<BackendCacheContext>,
    pub(crate) syscache: BackendSysCache,
    pub pending_invalidations: Vec<CatalogInvalidation>,
}

pub fn invalidate_backend_cache_state(db: &Database, client_id: ClientId) {
    db.backend_cache_states.write().remove(&client_id);
}

impl CatalogStore {
    pub(crate) fn search_sys_cache(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        keys: Vec<Value>,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        if keys.len() != cache_id.expected_keys() {
            return Err(CatalogError::Corrupt("syscache key count mismatch"));
        }

        if let Some(tuple) = bootstrap_sys_cache_tuple(cache_id, &keys) {
            return Ok(vec![tuple]);
        }

        let extra_tuples = extra_type_sys_cache_tuples(self.extra_type_rows(), cache_id, &keys);
        if !extra_tuples.is_empty() {
            return Ok(extra_tuples);
        }

        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let rows = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            cache_id.index_oid(),
            equality_scan_keys(&keys),
        )?;

        rows.into_iter()
            .map(|values| sys_cache_tuple_from_values(cache_id, values))
            .collect()
    }

    pub(crate) fn search_sys_cache1(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache(ctx, cache_id, vec![key1])
    }

    pub(crate) fn search_sys_cache2(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
        key2: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache(ctx, cache_id, vec![key1, key2])
    }

    pub(crate) fn search_sys_cache_list1(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache_list(ctx, cache_id, vec![key1])
    }

    pub(crate) fn search_sys_cache_list2(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
        key2: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache_list(ctx, cache_id, vec![key1, key2])
    }

    fn search_sys_cache_list(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        keys: Vec<Value>,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        if keys.is_empty() || keys.len() > cache_id.expected_keys() {
            return Err(CatalogError::Corrupt("syscache list key count mismatch"));
        }

        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let rows = probe_system_catalog_rows_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            cache_id.index_oid(),
            equality_scan_keys(&keys),
        )?;

        rows.into_iter()
            .map(|values| sys_cache_tuple_from_values(cache_id, values))
            .collect()
    }

    pub(crate) fn get_relname_relid(
        &self,
        ctx: &CatalogWriteContext,
        relname: &str,
        relnamespace: u32,
    ) -> Result<Option<u32>, CatalogError> {
        self.search_sys_cache2(
            ctx,
            SysCacheId::RelNameNsp,
            Value::Text(relname.to_ascii_lowercase().into()),
            oid_key(relnamespace),
        )
        .map(|tuples| {
            tuples.into_iter().find_map(|tuple| match tuple {
                SysCacheTuple::Class(row) => Some(row.oid),
                _ => None,
            })
        })
    }

    pub(crate) fn relation_id_get_relation(
        &self,
        ctx: &CatalogWriteContext,
        relation_oid: u32,
    ) -> Result<Option<RelCacheEntry>, CatalogError> {
        self.relation_id_get_relation_with_extra_type_rows(ctx, relation_oid, &[])
    }

    pub(crate) fn relation_id_get_relation_with_extra_type_rows(
        &self,
        ctx: &CatalogWriteContext,
        relation_oid: u32,
        extra_type_rows: &[PgTypeRow],
    ) -> Result<Option<RelCacheEntry>, CatalogError> {
        let Some(class_row) = self
            .search_sys_cache1(ctx, SysCacheId::RelOid, oid_key(relation_oid))?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Class(row) => Some(row),
                _ => None,
            })
        else {
            return Ok(None);
        };

        let mut attributes = self
            .search_sys_cache_list1(ctx, SysCacheId::AttrNum, oid_key(relation_oid))?
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Attribute(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();
        attributes.sort_by_key(|row| row.attnum);

        let attrdefs = self
            .search_sys_cache_list1(ctx, SysCacheId::AttrDefault, oid_key(relation_oid))?
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Attrdef(row) => Some((row.adnum, row)),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let constraints = self
            .search_sys_cache_list1(ctx, SysCacheId::ConstraintRelId, oid_key(relation_oid))?
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Constraint(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();
        let not_null_constraints = constraints
            .iter()
            .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_NOTNULL)
            .filter_map(|row| {
                let attnum = *row.conkey.as_ref()?.first()?;
                Some((attnum, row))
            })
            .collect::<BTreeMap<_, _>>();
        let primary_constraint_oids = constraints
            .iter()
            .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        let mut pk_owned_not_null = BTreeSet::new();
        for primary_constraint_oid in primary_constraint_oids {
            pk_owned_not_null.extend(
                self.search_sys_cache_list2(
                    ctx,
                    SysCacheId::DependReference,
                    oid_key(PG_CONSTRAINT_RELATION_OID),
                    oid_key(primary_constraint_oid),
                )?
                .into_iter()
                .filter_map(|tuple| match tuple {
                    SysCacheTuple::Depend(row) if row.classid == PG_CONSTRAINT_RELATION_OID => {
                        Some(row.objid)
                    }
                    _ => None,
                }),
            );
        }

        let extra_types_by_oid = extra_type_rows
            .iter()
            .map(|row| (row.oid, row.sql_type))
            .collect::<BTreeMap<_, _>>();
        let mut columns = Vec::with_capacity(attributes.len());
        for attr in attributes {
            let sql_type = self
                .search_sys_cache1(ctx, SysCacheId::TypeOid, oid_key(attr.atttypid))?
                .into_iter()
                .find_map(|tuple| match tuple {
                    SysCacheTuple::Type(row) => Some(row.sql_type),
                    _ => None,
                })
                .or_else(|| extra_types_by_oid.get(&attr.atttypid).copied())
                .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
            let mut desc = column_desc(
                attr.attname,
                SqlType {
                    typmod: attr.atttypmod,
                    ..sql_type
                },
                !attr.attnotnull,
            );
            desc.storage.attlen = attr.attlen;
            desc.storage.attalign = attr.attalign;
            desc.storage.attstorage = attr.attstorage;
            desc.storage.attcompression = attr.attcompression;
            desc.attstattarget = attr.attstattarget;
            desc.attinhcount = attr.attinhcount;
            desc.attislocal = attr.attislocal;
            desc.generated =
                crate::include::nodes::parsenodes::ColumnGeneratedKind::from_catalog_char(
                    attr.attgenerated,
                );
            desc.dropped = attr.attisdropped;
            if let Some(constraint) = not_null_constraints.get(&attr.attnum) {
                desc.not_null_constraint_oid = Some(constraint.oid);
                desc.not_null_constraint_name = Some(constraint.conname.clone());
                desc.not_null_constraint_validated = constraint.convalidated;
                desc.not_null_constraint_is_local = constraint.conislocal;
                desc.not_null_constraint_inhcount = constraint.coninhcount;
                desc.not_null_constraint_no_inherit = constraint.connoinherit;
                desc.not_null_primary_key_owned = pk_owned_not_null.contains(&constraint.oid);
            }
            if let Some(attrdef) = attrdefs.get(&attr.attnum) {
                desc.attrdef_oid = Some(attrdef.oid);
                desc.default_expr = Some(attrdef.adbin.clone());
                desc.default_sequence_oid =
                    crate::pgrust::database::default_sequence_oid_from_default_expr(&attrdef.adbin);
                desc.missing_default_value = None;
            }
            columns.push(desc);
        }

        let array_type_oid = if class_row.reltype == 0 {
            0
        } else {
            self.search_sys_cache1(ctx, SysCacheId::TypeOid, oid_key(class_row.reltype))?
                .into_iter()
                .find_map(|tuple| match tuple {
                    SysCacheTuple::Type(row) => Some(row.typarray),
                    _ => None,
                })
                .unwrap_or(0)
        };
        let index_row = matches!(class_row.relkind, 'i' | 'I')
            .then(|| {
                self.search_sys_cache1(ctx, SysCacheId::IndexRelId, oid_key(relation_oid))?
                    .into_iter()
                    .find_map(|tuple| match tuple {
                        SysCacheTuple::Index(row) => Some(row),
                        _ => None,
                    })
                    .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))
            })
            .transpose()?;
        let partitioned_table = matches!(class_row.relkind, 'p')
            .then(|| self.search_sys_cache1(ctx, SysCacheId::PartRelId, oid_key(relation_oid)))
            .transpose()?
            .into_iter()
            .flatten()
            .find_map(|tuple| match tuple {
                SysCacheTuple::PartitionedTable(row) => Some(row),
                _ => None,
            });
        let index = index_row
            .map(|index| {
                let support = resolve_index_support_metadata(
                    &index.indclass,
                    |opclass_oid| {
                        Ok(self
                            .search_sys_cache1(ctx, SysCacheId::OpclassOid, oid_key(opclass_oid))?
                            .into_iter()
                            .find_map(|tuple| match tuple {
                                SysCacheTuple::Opclass(row) => Some(row),
                                _ => None,
                            }))
                    },
                    |family_oid| {
                        Ok(self
                            .search_sys_cache_list1(
                                ctx,
                                SysCacheId::AmopStrategy,
                                oid_key(family_oid),
                            )?
                            .into_iter()
                            .filter_map(|tuple| match tuple {
                                SysCacheTuple::Amop(row) => Some(row),
                                _ => None,
                            })
                            .collect())
                    },
                    |family_oid| {
                        Ok(self
                            .search_sys_cache_list1(
                                ctx,
                                SysCacheId::AmprocNum,
                                oid_key(family_oid),
                            )?
                            .into_iter()
                            .filter_map(|tuple| match tuple {
                                SysCacheTuple::Amproc(row) => Some(row),
                                _ => None,
                            })
                            .collect())
                    },
                    |operator_oid| {
                        Ok(self
                            .search_sys_cache1(ctx, SysCacheId::OperOid, oid_key(operator_oid))?
                            .into_iter()
                            .find_map(|tuple| match tuple {
                                SysCacheTuple::Operator(row) => Some(row),
                                _ => None,
                            }))
                    },
                )?;
                let am_handler_oid = self
                    .search_sys_cache1(ctx, SysCacheId::AmOid, oid_key(class_row.relam))?
                    .into_iter()
                    .find_map(|tuple| match tuple {
                        SysCacheTuple::Am(row) => Some(row.amhandler),
                        _ => None,
                    });
                Ok(index_relcache_entry_from_index_row(
                    &class_row,
                    index,
                    am_handler_oid,
                    support,
                ))
            })
            .transpose()?;

        Ok(Some(RelCacheEntry {
            rel: relation_locator_for_class_row(
                class_row.oid,
                class_row.relfilenode,
                self.scope_db_oid(),
            ),
            relation_oid: class_row.oid,
            namespace_oid: class_row.relnamespace,
            owner_oid: class_row.relowner,
            of_type_oid: class_row.reloftype,
            row_type_oid: class_row.reltype,
            array_type_oid,
            reltoastrelid: class_row.reltoastrelid,
            relhasindex: class_row.relhasindex,
            relpersistence: class_row.relpersistence,
            relkind: class_row.relkind,
            relispopulated: class_row.relispopulated,
            relispartition: class_row.relispartition,
            relpartbound: class_row.relpartbound,
            relhastriggers: class_row.relhastriggers,
            relrowsecurity: class_row.relrowsecurity,
            relforcerowsecurity: class_row.relforcerowsecurity,
            desc: crate::backend::executor::RelationDesc { columns },
            partitioned_table,
            index,
        }))
    }
}

pub(crate) fn relation_id_get_relation_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Result<Option<RelCacheEntry>, CatalogError> {
    let Some(class_row) = search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::RelOid,
        oid_key(relation_oid),
    )?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Class(row) => Some(row),
        _ => None,
    }) else {
        return Ok(None);
    };

    let mut attributes = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AttrNum,
        oid_key(relation_oid),
    )?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::Attribute(row) => Some(row),
        _ => None,
    })
    .collect::<Vec<_>>();
    attributes.sort_by_key(|row| row.attnum);

    let attrdefs = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AttrDefault,
        oid_key(relation_oid),
    )?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::Attrdef(row) => Some((row.adnum, row)),
        _ => None,
    })
    .collect::<BTreeMap<_, _>>();
    let constraints = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ConstraintRelId,
        oid_key(relation_oid),
    )?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::Constraint(row) => Some(row),
        _ => None,
    })
    .collect::<Vec<_>>();
    let not_null_constraints = constraints
        .iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_NOTNULL)
        .filter_map(|row| {
            let attnum = *row.conkey.as_ref()?.first()?;
            Some((attnum, row))
        })
        .collect::<BTreeMap<_, _>>();
    let primary_constraint_oids = constraints
        .iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    let mut pk_owned_not_null = BTreeSet::new();
    for primary_constraint_oid in primary_constraint_oids {
        pk_owned_not_null.extend(
            search_sys_cache_list2_db(
                db,
                client_id,
                txn_ctx,
                SysCacheId::DependReference,
                oid_key(PG_CONSTRAINT_RELATION_OID),
                oid_key(primary_constraint_oid),
            )?
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Depend(row) if row.classid == PG_CONSTRAINT_RELATION_OID => {
                    Some(row.objid)
                }
                _ => None,
            }),
        );
    }

    let search_path = db.effective_search_path(client_id, None);
    let mut dynamic_type_rows = db.domain_type_rows_for_search_path(&search_path);
    dynamic_type_rows.extend(db.enum_type_rows_for_search_path(&search_path));
    dynamic_type_rows.extend(db.range_type_rows_for_search_path(&search_path));
    let dynamic_types_by_oid = dynamic_type_rows
        .iter()
        .map(|row| (row.oid, row.sql_type))
        .collect::<BTreeMap<_, _>>();

    let mut columns = Vec::with_capacity(attributes.len());
    for attr in attributes {
        let sql_type = search_sys_cache1_db(
            db,
            client_id,
            txn_ctx,
            SysCacheId::TypeOid,
            oid_key(attr.atttypid),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row.sql_type),
            _ => None,
        })
        .or_else(|| dynamic_types_by_oid.get(&attr.atttypid).copied())
        .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
        let mut desc = column_desc(
            attr.attname,
            SqlType {
                typmod: attr.atttypmod,
                ..sql_type
            },
            !attr.attnotnull,
        );
        desc.storage.attlen = attr.attlen;
        desc.storage.attalign = attr.attalign;
        desc.storage.attstorage = attr.attstorage;
        desc.storage.attcompression = attr.attcompression;
        desc.attstattarget = attr.attstattarget;
        desc.attinhcount = attr.attinhcount;
        desc.attislocal = attr.attislocal;
        desc.generated = crate::include::nodes::parsenodes::ColumnGeneratedKind::from_catalog_char(
            attr.attgenerated,
        );
        desc.dropped = attr.attisdropped;
        if let Some(constraint) = not_null_constraints.get(&attr.attnum) {
            desc.not_null_constraint_oid = Some(constraint.oid);
            desc.not_null_constraint_name = Some(constraint.conname.clone());
            desc.not_null_constraint_validated = constraint.convalidated;
            desc.not_null_constraint_is_local = constraint.conislocal;
            desc.not_null_constraint_inhcount = constraint.coninhcount;
            desc.not_null_constraint_no_inherit = constraint.connoinherit;
            desc.not_null_primary_key_owned = pk_owned_not_null.contains(&constraint.oid);
        }
        if let Some(attrdef) = attrdefs.get(&attr.attnum) {
            desc.attrdef_oid = Some(attrdef.oid);
            desc.default_expr = Some(attrdef.adbin.clone());
            desc.default_sequence_oid =
                crate::pgrust::database::default_sequence_oid_from_default_expr(&attrdef.adbin);
            desc.missing_default_value = None;
        }
        columns.push(desc);
    }

    let array_type_oid = if class_row.reltype == 0 {
        0
    } else {
        search_sys_cache1_db(
            db,
            client_id,
            txn_ctx,
            SysCacheId::TypeOid,
            oid_key(class_row.reltype),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row.typarray),
            _ => None,
        })
        .unwrap_or(0)
    };
    let index_row = matches!(class_row.relkind, 'i' | 'I')
        .then(|| {
            search_sys_cache1_db(
                db,
                client_id,
                txn_ctx,
                SysCacheId::IndexRelId,
                oid_key(relation_oid),
            )?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Index(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))
        })
        .transpose()?;
    let partitioned_table = matches!(class_row.relkind, 'p')
        .then(|| {
            search_sys_cache1_db(
                db,
                client_id,
                txn_ctx,
                SysCacheId::PartRelId,
                oid_key(relation_oid),
            )
        })
        .transpose()?
        .into_iter()
        .flatten()
        .find_map(|tuple| match tuple {
            SysCacheTuple::PartitionedTable(row) => Some(row),
            _ => None,
        });
    let index = index_row
        .map(|index| {
            let support = resolve_index_support_metadata(
                &index.indclass,
                |opclass_oid| {
                    Ok(search_sys_cache1_db(
                        db,
                        client_id,
                        txn_ctx,
                        SysCacheId::OpclassOid,
                        oid_key(opclass_oid),
                    )?
                    .into_iter()
                    .find_map(|tuple| match tuple {
                        SysCacheTuple::Opclass(row) => Some(row),
                        _ => None,
                    }))
                },
                |family_oid| {
                    Ok(search_sys_cache_list1_db(
                        db,
                        client_id,
                        txn_ctx,
                        SysCacheId::AmopStrategy,
                        oid_key(family_oid),
                    )?
                    .into_iter()
                    .filter_map(|tuple| match tuple {
                        SysCacheTuple::Amop(row) => Some(row),
                        _ => None,
                    })
                    .collect())
                },
                |family_oid| {
                    Ok(search_sys_cache_list1_db(
                        db,
                        client_id,
                        txn_ctx,
                        SysCacheId::AmprocNum,
                        oid_key(family_oid),
                    )?
                    .into_iter()
                    .filter_map(|tuple| match tuple {
                        SysCacheTuple::Amproc(row) => Some(row),
                        _ => None,
                    })
                    .collect())
                },
                |operator_oid| {
                    Ok(search_sys_cache1_db(
                        db,
                        client_id,
                        txn_ctx,
                        SysCacheId::OperOid,
                        oid_key(operator_oid),
                    )?
                    .into_iter()
                    .find_map(|tuple| match tuple {
                        SysCacheTuple::Operator(row) => Some(row),
                        _ => None,
                    }))
                },
            )?;
            let am_handler_oid = search_sys_cache1_db(
                db,
                client_id,
                txn_ctx,
                SysCacheId::AmOid,
                oid_key(class_row.relam),
            )?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Am(row) => Some(row.amhandler),
                _ => None,
            });
            Ok(index_relcache_entry_from_index_row(
                &class_row,
                index,
                am_handler_oid,
                support,
            ))
        })
        .transpose()?;

    Ok(Some(RelCacheEntry {
        rel: relation_locator_for_class_row(class_row.oid, class_row.relfilenode, db.database_oid),
        relation_oid: class_row.oid,
        namespace_oid: class_row.relnamespace,
        owner_oid: class_row.relowner,
        of_type_oid: class_row.reloftype,
        row_type_oid: class_row.reltype,
        array_type_oid,
        reltoastrelid: class_row.reltoastrelid,
        relhasindex: class_row.relhasindex,
        relpersistence: class_row.relpersistence,
        relkind: class_row.relkind,
        relispopulated: class_row.relispopulated,
        relispartition: class_row.relispartition,
        relpartbound: class_row.relpartbound,
        relhastriggers: class_row.relhastriggers,
        relrowsecurity: class_row.relrowsecurity,
        relforcerowsecurity: class_row.relforcerowsecurity,
        desc: crate::backend::executor::RelationDesc { columns },
        partitioned_table,
        index,
    }))
}

fn backend_syscache_get(
    db: &Database,
    client_id: ClientId,
    cache_ctx: BackendCacheContext,
    mode: SysCacheLookupMode,
    key: &SysCacheQueryKey,
) -> Option<Vec<SysCacheTuple>> {
    db.backend_cache_states
        .write()
        .entry(client_id)
        .or_default()
        .syscache
        .get(cache_ctx, mode, key)
}

fn backend_syscache_insert(
    db: &Database,
    client_id: ClientId,
    cache_ctx: BackendCacheContext,
    mode: SysCacheLookupMode,
    key: SysCacheQueryKey,
    value: Vec<SysCacheTuple>,
) {
    db.backend_cache_states
        .write()
        .entry(client_id)
        .or_default()
        .syscache
        .insert(cache_ctx, mode, key, value);
}

pub(crate) fn search_sys_cache_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    if keys.len() != cache_id.expected_keys() {
        return Err(CatalogError::Corrupt("syscache key count mismatch"));
    }

    if let Some(tuple) = bootstrap_sys_cache_tuple(cache_id, &keys) {
        return Ok(vec![tuple]);
    }

    let cache_key = SysCacheQueryKey::new(cache_id, &keys);
    let cache_ctx = BackendCacheContext::from(txn_ctx);
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    if let Some(key) = cache_key.as_ref()
        && let Some(cached) =
            backend_syscache_get(db, client_id, cache_ctx, SysCacheLookupMode::Exact, key)
    {
        return Ok(cached);
    }

    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    let rows = probe_system_catalog_rows_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        cache_id.index_oid(),
        equality_scan_keys(&keys),
    )?;

    let tuples = rows
        .into_iter()
        .map(|values| sys_cache_tuple_from_values(cache_id, values))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(key) = cache_key {
        backend_syscache_insert(
            db,
            client_id,
            cache_ctx,
            SysCacheLookupMode::Exact,
            key,
            tuples.clone(),
        );
    }
    Ok(tuples)
}

pub(crate) fn search_sys_cache1_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_db(db, client_id, txn_ctx, cache_id, vec![key1])
}

pub(crate) fn search_sys_cache2_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_db(db, client_id, txn_ctx, cache_id, vec![key1, key2])
}

pub(crate) fn search_sys_cache_list1_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_list_db(db, client_id, txn_ctx, cache_id, vec![key1])
}

pub(crate) fn search_sys_cache_list2_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_list_db(db, client_id, txn_ctx, cache_id, vec![key1, key2])
}

pub(crate) fn search_sys_cache_list3_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
    key3: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_list_db(db, client_id, txn_ctx, cache_id, vec![key1, key2, key3])
}

fn search_sys_cache_list_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    if keys.is_empty() || keys.len() > cache_id.expected_keys() {
        return Err(CatalogError::Corrupt("syscache list key count mismatch"));
    }

    let cache_key = SysCacheQueryKey::new(cache_id, &keys);
    let cache_ctx = BackendCacheContext::from(txn_ctx);
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    if let Some(key) = cache_key.as_ref()
        && let Some(cached) =
            backend_syscache_get(db, client_id, cache_ctx, SysCacheLookupMode::List, key)
    {
        return Ok(cached);
    }

    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    let rows = probe_system_catalog_rows_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        cache_id.index_oid(),
        equality_scan_keys(&keys),
    )?;

    let tuples = rows
        .into_iter()
        .map(|values| sys_cache_tuple_from_values(cache_id, values))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(key) = cache_key {
        backend_syscache_insert(
            db,
            client_id,
            cache_ctx,
            SysCacheLookupMode::List,
            key,
            tuples.clone(),
        );
    }
    Ok(tuples)
}

pub fn backend_catcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<CatCache, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }

    let cache_ctx = BackendCacheContext::from(txn_ctx);
    if let Some(cache) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.cache_ctx == Some(cache_ctx))
        .and_then(|state| state.catcache.clone())
    {
        return Ok(cache);
    }

    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    let mut cache = {
        let txns = db.txns.read();
        let shared = db
            .shared_catalog
            .read()
            .catcache_with_snapshot(&db.pool, &txns, &snapshot, client_id)?;
        let local = db
            .catalog
            .read()
            .catcache_with_snapshot(&db.pool, &txns, &snapshot, client_id)?;
        merge_catcaches(shared, local)
    };
    let search_path = db.effective_search_path(client_id, None);
    cache.extend_type_rows(db.domain_type_rows_for_search_path(&search_path));
    cache.extend_type_rows(db.enum_type_rows_for_search_path(&search_path));
    cache.extend_type_rows(db.range_type_rows_for_search_path(&search_path));

    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.cache_ctx = Some(cache_ctx);
    state.catcache = Some(cache.clone());
    state.relcache = None;
    Ok(cache)
}

pub fn backend_relcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<RelCache, CatalogError> {
    let cache_ctx = BackendCacheContext::from(txn_ctx);
    if let Some(cache) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.cache_ctx == Some(cache_ctx))
        .and_then(|state| state.relcache.clone())
    {
        return Ok(cache);
    }

    let search_path = db.effective_search_path(client_id, None);
    let dynamic_type_rows = dynamic_type_rows_for_search_path(db, &search_path);
    let relcache = RelCache::from_catcache_in_db_with_extra_type_rows(
        &backend_catcache(db, client_id, txn_ctx)?,
        db.database_oid,
        &dynamic_type_rows,
    )?;
    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.cache_ctx = Some(cache_ctx);
    state.relcache = Some(relcache.clone());
    Ok(relcache)
}

pub fn drain_pending_invalidations(db: &Database, client_id: ClientId) -> Vec<CatalogInvalidation> {
    db.backend_cache_states
        .write()
        .entry(client_id)
        .or_default()
        .pending_invalidations
        .drain(..)
        .collect()
}

pub fn ensure_namespace_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgNamespaceRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.namespace_rows())
        .unwrap_or_default()
}

pub fn ensure_class_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgClassRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.class_rows())
        .unwrap_or_default()
}

pub fn ensure_constraint_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgConstraintRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.constraint_rows())
        .unwrap_or_default()
}

pub fn ensure_depend_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgDependRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.depend_rows())
        .unwrap_or_default()
}

pub fn ensure_inherit_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgInheritsRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.inherit_rows())
        .unwrap_or_default()
}

pub fn ensure_rewrite_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgRewriteRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.rewrite_rows())
        .unwrap_or_default()
}

pub fn ensure_statistic_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgStatisticRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.statistic_rows())
        .unwrap_or_default()
}

pub fn ensure_attribute_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAttributeRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.attribute_rows())
        .unwrap_or_default()
}

pub fn ensure_attrdef_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAttrdefRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.attrdef_rows())
        .unwrap_or_default()
}

pub fn ensure_type_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgTypeRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.type_rows())
        .unwrap_or_default()
}

pub fn ensure_index_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgIndexRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.index_rows())
        .unwrap_or_default()
}

pub fn ensure_am_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.am_rows())
        .unwrap_or_default()
}

pub fn ensure_amop_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmopRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.amop_rows())
        .unwrap_or_default()
}

pub fn ensure_amproc_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmprocRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.amproc_rows())
        .unwrap_or_default()
}

pub fn ensure_opclass_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgOpclassRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.opclass_rows())
        .unwrap_or_default()
}

pub fn ensure_opfamily_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgOpfamilyRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.opfamily_rows())
        .unwrap_or_default()
}

pub fn ensure_collation_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgCollationRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.collation_rows())
        .unwrap_or_default()
}

pub fn ensure_proc_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgProcRow> {
    backend_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.proc_rows())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::BootstrapCatalogKind;

    #[test]
    fn backend_syscache_caches_empty_exact_and_list_results() {
        let mut cache = BackendSysCache::default();
        let exact_key = SysCacheQueryKey::new(SysCacheId::RelOid, &[oid_key(42)]).unwrap();
        let list_key = SysCacheQueryKey::new(SysCacheId::AttrNum, &[oid_key(42)]).unwrap();

        cache.insert(
            BackendCacheContext::Autocommit,
            SysCacheLookupMode::Exact,
            exact_key.clone(),
            Vec::new(),
        );
        cache.insert(
            BackendCacheContext::Autocommit,
            SysCacheLookupMode::List,
            list_key.clone(),
            Vec::new(),
        );

        assert_eq!(
            cache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::Exact,
                &exact_key
            ),
            Some(Vec::new())
        );
        assert_eq!(
            cache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::List,
                &list_key
            ),
            Some(Vec::new())
        );
    }

    #[test]
    fn backend_syscache_invalidates_by_catalog_kind() {
        let mut cache = BackendSysCache::default();
        let rel_key = SysCacheQueryKey::new(SysCacheId::RelOid, &[oid_key(42)]).unwrap();
        let type_key = SysCacheQueryKey::new(
            SysCacheId::TypeNameNsp,
            &[Value::Text("t".into()), oid_key(11)],
        )
        .unwrap();

        cache.insert(
            BackendCacheContext::Autocommit,
            SysCacheLookupMode::Exact,
            rel_key.clone(),
            Vec::new(),
        );
        cache.insert(
            BackendCacheContext::Autocommit,
            SysCacheLookupMode::Exact,
            type_key.clone(),
            Vec::new(),
        );

        let mut invalidation = CatalogInvalidation::default();
        invalidation
            .touched_catalogs
            .insert(BootstrapCatalogKind::PgClass);
        cache.invalidate(&invalidation);

        assert_eq!(
            cache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::Exact,
                &rel_key
            ),
            None
        );
        assert_eq!(
            cache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::Exact,
                &type_key
            ),
            Some(Vec::new())
        );
    }

    #[test]
    fn backend_syscache_keeps_only_current_transaction_context() {
        let mut cache = BackendSysCache::default();
        let key = SysCacheQueryKey::new(SysCacheId::RelOid, &[oid_key(42)]).unwrap();
        let first = BackendCacheContext::Transaction { xid: 1, cid: 1 };
        let second = BackendCacheContext::Transaction { xid: 1, cid: 2 };

        cache.insert(first, SysCacheLookupMode::Exact, key.clone(), Vec::new());
        assert_eq!(
            cache.get(first, SysCacheLookupMode::Exact, &key),
            Some(Vec::new())
        );
        assert_eq!(cache.get(second, SysCacheLookupMode::Exact, &key), None);
    }

    #[test]
    fn syscache_query_key_skips_unsupported_value_shapes() {
        assert!(SysCacheQueryKey::new(SysCacheId::TypeOid, &[Value::Float64(1.0)]).is_none());
    }
}
