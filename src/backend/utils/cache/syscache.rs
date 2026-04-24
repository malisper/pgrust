use std::collections::{BTreeMap, BTreeSet};

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible_in_db;
use crate::backend::catalog::rowcodec::{
    pg_attrdef_row_from_values, pg_attribute_row_from_values, pg_class_row_from_values,
    pg_constraint_row_from_values, pg_depend_row_from_values, pg_index_row_from_values,
    pg_inherits_row_from_values, pg_partitioned_table_row_from_values, pg_rewrite_row_from_values,
    pg_trigger_row_from_values, pg_type_row_from_values,
};
use crate::backend::catalog::store::{CatalogStore, CatalogWriteContext};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::relcache::{
    IndexRelCacheEntry, RelCache, RelCacheEntry, relation_locator_for_class_row,
};
use crate::backend::utils::time::snapmgr::{Snapshot, get_catalog_snapshot};
use crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    PG_CONSTRAINT_RELATION_OID, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow,
    PgClassRow, PgCollationRow, PgConstraintRow, PgDependRow, PgIndexRow, PgInheritsRow,
    PgNamespaceRow, PgOpclassRow, PgOpfamilyRow, PgPartitionedTableRow, PgProcRow, PgRewriteRow,
    PgStatisticRow, PgTriggerRow, PgTypeRow, bootstrap_composite_type_rows, builtin_type_rows,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::SqlType;
use crate::pgrust::database::Database;

const PG_ATTRIBUTE_RELID_ATTNAM_INDEX_OID: u32 = 2658;
const PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID: u32 = 2659;
const PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID: u32 = 2656;
const PG_ATTRDEF_OID_INDEX_OID: u32 = 2657;
const PG_CLASS_OID_INDEX_OID: u32 = 2662;
const PG_CLASS_RELNAME_NSP_INDEX_OID: u32 = 2663;
const PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_OID: u32 = 2665;
const PG_CONSTRAINT_OID_INDEX_OID: u32 = 2667;
const PG_DEPEND_DEPENDER_INDEX_OID: u32 = 2673;
const PG_DEPEND_REFERENCE_INDEX_OID: u32 = 2674;
const PG_INDEX_INDRELID_INDEX_OID: u32 = 2678;
const PG_INDEX_INDEXRELID_INDEX_OID: u32 = 2679;
const PG_INHERITS_RELID_SEQNO_INDEX_OID: u32 = 2680;
const PG_INHERITS_PARENT_INDEX_OID: u32 = 2187;
const PG_PARTITIONED_TABLE_PARTRELID_INDEX_OID: u32 = 3351;
const PG_REWRITE_OID_INDEX_OID: u32 = 2692;
const PG_REWRITE_REL_RULENAME_INDEX_OID: u32 = 2693;
const PG_TRIGGER_RELID_NAME_INDEX_OID: u32 = 2701;
const PG_TRIGGER_OID_INDEX_OID: u32 = 2702;
const PG_TYPE_OID_INDEX_OID: u32 = 2703;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SysCacheId {
    // PostgreSQL syscache name: ATTNAME.
    AttrName,
    // PostgreSQL syscache name: ATTNUM.
    AttrNum,
    // PostgreSQL systable scan index: AttrDefaultIndexId.
    AttrDefault,
    // PostgreSQL systable scan index: AttrDefaultOidIndexId.
    AttrDefaultOid,
    // PostgreSQL syscache name: CONSTROID.
    ConstraintOid,
    // PostgreSQL-like relation constraint lookup over pg_constraint_conrelid_*.
    ConstraintRelId,
    // PostgreSQL systable scan index: DependDependerIndexId.
    DependDepender,
    // PostgreSQL systable scan index: DependReferenceIndexId.
    DependReference,
    // PostgreSQL syscache name: INDEXRELID.
    IndexRelId,
    // PostgreSQL-like heap index lookup over pg_index_indrelid_index.
    IndexIndRelId,
    // PostgreSQL systable scan index: InheritsRelidSeqnoIndexId.
    InheritsRelIdSeqNo,
    // PostgreSQL systable scan index: InheritsParentIndexId.
    InheritsParent,
    // PostgreSQL syscache name: PARTRELID.
    PartRelId,
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
    // PostgreSQL syscache name: TYPEOID.
    TypeOid,
}

impl SysCacheId {
    fn index_oid(self) -> u32 {
        match self {
            Self::AttrName => PG_ATTRIBUTE_RELID_ATTNAM_INDEX_OID,
            Self::AttrNum => PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID,
            Self::AttrDefault => PG_ATTRDEF_ADRELID_ADNUM_INDEX_OID,
            Self::AttrDefaultOid => PG_ATTRDEF_OID_INDEX_OID,
            Self::ConstraintOid => PG_CONSTRAINT_OID_INDEX_OID,
            Self::ConstraintRelId => PG_CONSTRAINT_CONRELID_CONTYPID_CONNAME_INDEX_OID,
            Self::DependDepender => PG_DEPEND_DEPENDER_INDEX_OID,
            Self::DependReference => PG_DEPEND_REFERENCE_INDEX_OID,
            Self::IndexRelId => PG_INDEX_INDEXRELID_INDEX_OID,
            Self::IndexIndRelId => PG_INDEX_INDRELID_INDEX_OID,
            Self::InheritsRelIdSeqNo => PG_INHERITS_RELID_SEQNO_INDEX_OID,
            Self::InheritsParent => PG_INHERITS_PARENT_INDEX_OID,
            Self::PartRelId => PG_PARTITIONED_TABLE_PARTRELID_INDEX_OID,
            Self::RelOid => PG_CLASS_OID_INDEX_OID,
            Self::RelNameNsp => PG_CLASS_RELNAME_NSP_INDEX_OID,
            Self::RewriteOid => PG_REWRITE_OID_INDEX_OID,
            Self::RuleRelName => PG_REWRITE_REL_RULENAME_INDEX_OID,
            Self::TriggerRelidName => PG_TRIGGER_RELID_NAME_INDEX_OID,
            Self::TriggerOid => PG_TRIGGER_OID_INDEX_OID,
            Self::TypeOid => PG_TYPE_OID_INDEX_OID,
        }
    }

    fn expected_keys(self) -> usize {
        match self {
            Self::AttrDefaultOid
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
            | Self::RelNameNsp
            | Self::RuleRelName
            | Self::TriggerRelidName => 2,
            Self::DependDepender | Self::DependReference => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SysCacheTuple {
    Attrdef(PgAttrdefRow),
    Attribute(PgAttributeRow),
    Class(PgClassRow),
    Constraint(PgConstraintRow),
    Depend(PgDependRow),
    Index(PgIndexRow),
    Inherits(PgInheritsRow),
    PartitionedTable(PgPartitionedTableRow),
    Rewrite(PgRewriteRow),
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

fn sys_cache_tuple_from_values(
    cache_id: SysCacheId,
    values: Vec<Value>,
) -> Result<SysCacheTuple, CatalogError> {
    match cache_id {
        SysCacheId::AttrDefault | SysCacheId::AttrDefaultOid => {
            pg_attrdef_row_from_values(values).map(SysCacheTuple::Attrdef)
        }
        SysCacheId::AttrName | SysCacheId::AttrNum => {
            pg_attribute_row_from_values(values).map(SysCacheTuple::Attribute)
        }
        SysCacheId::ConstraintOid | SysCacheId::ConstraintRelId => {
            pg_constraint_row_from_values(values).map(SysCacheTuple::Constraint)
        }
        SysCacheId::DependDepender | SysCacheId::DependReference => {
            pg_depend_row_from_values(values).map(SysCacheTuple::Depend)
        }
        SysCacheId::IndexRelId | SysCacheId::IndexIndRelId => {
            pg_index_row_from_values(values).map(SysCacheTuple::Index)
        }
        SysCacheId::InheritsRelIdSeqNo | SysCacheId::InheritsParent => {
            pg_inherits_row_from_values(values).map(SysCacheTuple::Inherits)
        }
        SysCacheId::PartRelId => {
            pg_partitioned_table_row_from_values(values).map(SysCacheTuple::PartitionedTable)
        }
        SysCacheId::RelOid | SysCacheId::RelNameNsp => {
            pg_class_row_from_values(values).map(SysCacheTuple::Class)
        }
        SysCacheId::RewriteOid | SysCacheId::RuleRelName => {
            pg_rewrite_row_from_values(values).map(SysCacheTuple::Rewrite)
        }
        SysCacheId::TriggerRelidName | SysCacheId::TriggerOid => {
            pg_trigger_row_from_values(values).map(SysCacheTuple::Trigger)
        }
        SysCacheId::TypeOid => pg_type_row_from_values(values).map(SysCacheTuple::Type),
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

#[derive(Debug, Default, Clone)]
pub struct BackendCacheState {
    pub catalog_snapshot: Option<Snapshot>,
    pub catalog_snapshot_ctx: Option<BackendCacheContext>,
    pub catcache: Option<CatCache>,
    pub relcache: Option<RelCache>,
    pub cache_ctx: Option<BackendCacheContext>,
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

        let mut columns = Vec::with_capacity(attributes.len());
        for attr in attributes {
            let sql_type = self
                .search_sys_cache1(ctx, SysCacheId::TypeOid, oid_key(attr.atttypid))?
                .into_iter()
                .find_map(|tuple| match tuple {
                    SysCacheTuple::Type(row) => Some(row.sql_type),
                    _ => None,
                })
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

        Ok(Some(RelCacheEntry {
            rel: relation_locator_for_class_row(
                class_row.oid,
                class_row.relfilenode,
                self.scope_db_oid(),
            ),
            relation_oid: class_row.oid,
            namespace_oid: class_row.relnamespace,
            owner_oid: class_row.relowner,
            row_type_oid: class_row.reltype,
            array_type_oid,
            reltoastrelid: class_row.reltoastrelid,
            relpersistence: class_row.relpersistence,
            relkind: class_row.relkind,
            relispartition: class_row.relispartition,
            relpartbound: class_row.relpartbound,
            relhastriggers: class_row.relhastriggers,
            relrowsecurity: class_row.relrowsecurity,
            relforcerowsecurity: class_row.relforcerowsecurity,
            desc: crate::backend::executor::RelationDesc { columns },
            partitioned_table,
            index: index_row.map(|index| IndexRelCacheEntry {
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
                am_handler_oid: None,
                indkey: index.indkey,
                indclass: index.indclass,
                indcollation: index.indcollation,
                indoption: index.indoption,
                opfamily_oids: Vec::new(),
                opcintype_oids: Vec::new(),
                opckeytype_oids: Vec::new(),
                amop_entries: Vec::new(),
                amproc_entries: Vec::new(),
                indexprs: index.indexprs,
                indpred: index.indpred,
                brin_options: None,
            }),
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

    Ok(Some(RelCacheEntry {
        rel: relation_locator_for_class_row(class_row.oid, class_row.relfilenode, db.database_oid),
        relation_oid: class_row.oid,
        namespace_oid: class_row.relnamespace,
        owner_oid: class_row.relowner,
        row_type_oid: class_row.reltype,
        array_type_oid,
        reltoastrelid: class_row.reltoastrelid,
        relpersistence: class_row.relpersistence,
        relkind: class_row.relkind,
        relispartition: class_row.relispartition,
        relpartbound: class_row.relpartbound,
        relhastriggers: class_row.relhastriggers,
        relrowsecurity: class_row.relrowsecurity,
        relforcerowsecurity: class_row.relforcerowsecurity,
        desc: crate::backend::executor::RelationDesc { columns },
        partitioned_table,
        index: index_row.map(|index| IndexRelCacheEntry {
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
            am_handler_oid: None,
            indkey: index.indkey,
            indclass: index.indclass,
            indcollation: index.indcollation,
            indoption: index.indoption,
            opfamily_oids: Vec::new(),
            opcintype_oids: Vec::new(),
            opckeytype_oids: Vec::new(),
            amop_entries: Vec::new(),
            amproc_entries: Vec::new(),
            indexprs: index.indexprs,
            indpred: index.indpred,
            brin_options: None,
        }),
    }))
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

    rows.into_iter()
        .map(|values| sys_cache_tuple_from_values(cache_id, values))
        .collect()
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

    rows.into_iter()
        .map(|values| sys_cache_tuple_from_values(cache_id, values))
        .collect()
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
    let cache = {
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

    let relcache =
        RelCache::from_catcache_in_db(&backend_catcache(db, client_id, txn_ctx)?, db.database_oid)?;
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
