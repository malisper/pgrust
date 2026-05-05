use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

use crate::backend::access::transam::xact::{CommandId, TransactionId, TransactionManager};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::indexing::{
    CatalogScannedTuple, CatalogTupleIdentity, probe_system_catalog_rows_visible_in_db,
    probe_system_catalog_tuples_visible_in_db,
};
use crate::backend::catalog::rowcodec::{
    pg_auth_members_row_from_values, pg_authid_row_from_values,
    pg_publication_namespace_row_from_values, pg_type_row_from_values,
};
use crate::backend::catalog::store::{CatalogStore, CatalogWriteContext};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::evtcache::EventTriggerCache;
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::relcache::{
    IndexAmOpEntry, IndexAmProcEntry, IndexRelCacheEntry, RelCacheEntry,
    relation_locator_for_class_row,
};
use crate::backend::utils::time::snapmgr::{Snapshot, get_catalog_snapshot};
use crate::include::access::htup::HeapTuple;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    PG_AUTHID_RELATION_OID, PG_CONSTRAINT_RELATION_OID, PgAmRow, PgAmopRow, PgAmprocRow,
    PgAttrdefRow, PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow,
    PgCollationRow, PgConstraintRow, PgDependRow, PgEventTriggerRow, PgIndexRow, PgInheritsRow,
    PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgPolicyRow,
    PgProcRow, PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow, PgRewriteRow,
    PgShdependRow, PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow, PgTriggerRow,
    PgTypeRow, bootstrap_composite_type_rows, builtin_type_rows,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind};
use crate::pgrust::database::Database;
use crate::{BufferPool, ClientId};

pub use pgrust_catalog_store::syscache::*;

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
        indclass_options: crate::backend::catalog::index_opclass_options_from_reloptions(
            class_row.reloptions.as_deref(),
        ),
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
        btree_options: None,
        brin_options: None,
        gist_options: None,
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
        local.sequence_rows(),
        local.trigger_rows(),
        local.event_trigger_rows(),
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
        local.conversion_rows(),
        local.collation_rows(),
        local.foreign_data_wrapper_rows(),
        local.foreign_server_rows(),
        local.foreign_table_rows(),
        local.user_mapping_rows(),
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

fn relation_cache_context(cache_ctx: BackendCacheContext) -> BackendCacheContext {
    match cache_ctx {
        BackendCacheContext::Autocommit => BackendCacheContext::Autocommit,
        BackendCacheContext::Transaction { xid, .. } => {
            BackendCacheContext::Transaction { xid, cid: 0 }
        }
    }
}

fn shared_catcache_context(cache_ctx: BackendCacheContext) -> BackendCacheContext {
    match cache_ctx {
        BackendCacheContext::Autocommit => BackendCacheContext::Autocommit,
        BackendCacheContext::Transaction { xid, .. } => {
            BackendCacheContext::Transaction { xid, cid: 0 }
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

    fn as_invalidation_key(&self) -> SysCacheInvalidationKey {
        SysCacheInvalidationKey::new(self.cache_id, self.keys.clone())
    }

    fn invalidated_by(&self, invalidation: &CatalogInvalidation, mode: SysCacheLookupMode) -> bool {
        if invalidation.full_reset {
            return true;
        }
        let query_key = self.as_invalidation_key();
        if invalidation.syscache_keys.iter().any(|key| match mode {
            SysCacheLookupMode::Exact => key == &query_key,
            SysCacheLookupMode::List => key.matches_prefix(&query_key),
        }) {
            return true;
        }
        self.cache_id
            .catalog_kind()
            .is_some_and(|kind| invalidation.syscache_flush_catalogs.contains(&kind))
    }
}

#[derive(Debug, Clone, Copy)]
enum SysCacheLookupMode {
    Exact,
    List,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedSysCacheTuple {
    pub(crate) identity: Option<CatalogTupleIdentity>,
    pub(crate) heap_tuple: Option<HeapTuple>,
    decoded: Arc<OnceLock<SysCacheTuple>>,
}

impl PartialEq for CachedSysCacheTuple {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.heap_tuple == other.heap_tuple
            && self.decoded() == other.decoded()
    }
}

impl Eq for CachedSysCacheTuple {}

impl CachedSysCacheTuple {
    fn from_decoded(tuple: SysCacheTuple) -> Self {
        let decoded = OnceLock::new();
        let _ = decoded.set(tuple);
        Self {
            identity: None,
            heap_tuple: None,
            decoded: Arc::new(decoded),
        }
    }

    fn from_scanned(scanned: CatalogScannedTuple) -> Self {
        Self {
            identity: Some(scanned.identity),
            heap_tuple: Some(scanned.tuple),
            decoded: Arc::new(OnceLock::new()),
        }
    }

    fn decoded(&self) -> Option<SysCacheTuple> {
        self.decoded.get().cloned()
    }

    pub(crate) fn decode_with_context(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
    ) -> Result<SysCacheTuple, CatalogError> {
        let snapshot = ctx.snapshot_for_command()?;
        decode_cached_syscache_entry(
            self,
            cache_id,
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
        )
    }
}

#[derive(Debug, Default, Clone)]
struct BackendSysCacheMaps {
    exact: HashMap<SysCacheQueryKey, Vec<CachedSysCacheTuple>>,
    list: HashMap<SysCacheQueryKey, Vec<CachedSysCacheTuple>>,
}

impl BackendSysCacheMaps {
    fn get(
        &self,
        mode: SysCacheLookupMode,
        key: &SysCacheQueryKey,
    ) -> Option<Vec<CachedSysCacheTuple>> {
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
        value: Vec<CachedSysCacheTuple>,
    ) {
        match mode {
            SysCacheLookupMode::Exact => self.exact.insert(key, value),
            SysCacheLookupMode::List => self.list.insert(key, value),
        };
    }

    fn invalidate(&mut self, invalidation: &CatalogInvalidation) {
        self.exact
            .retain(|key, _| !key.invalidated_by(invalidation, SysCacheLookupMode::Exact));
        self.list
            .retain(|key, _| !key.invalidated_by(invalidation, SysCacheLookupMode::List));
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
    transaction_xid: Option<TransactionId>,
}

impl BackendSysCache {
    fn maps_mut(&mut self, cache_ctx: BackendCacheContext) -> &mut BackendSysCacheMaps {
        match cache_ctx {
            BackendCacheContext::Autocommit => &mut self.autocommit,
            BackendCacheContext::Transaction { xid, .. } => {
                if self.transaction_xid != Some(xid) {
                    self.transaction.clear();
                    self.transaction_xid = Some(xid);
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
    ) -> Option<Vec<CachedSysCacheTuple>> {
        self.maps_mut(cache_ctx).get(mode, key)
    }

    fn insert(
        &mut self,
        cache_ctx: BackendCacheContext,
        mode: SysCacheLookupMode,
        key: SysCacheQueryKey,
        value: Vec<CachedSysCacheTuple>,
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
    pub shared_catcache: Option<CatCache>,
    pub shared_catcache_ctx: Option<BackendCacheContext>,
    pub catcache: Option<CatCache>,
    pub catcache_ctx: Option<BackendCacheContext>,
    pub event_trigger_cache: Option<EventTriggerCache>,
    pub event_trigger_cache_ctx: Option<BackendCacheContext>,
    pub relation_cache: HashMap<u32, RelCacheEntry>,
    pub relation_cache_ctx: Option<BackendCacheContext>,
    pub(crate) syscache: BackendSysCache,
    pub pending_invalidations: Vec<CatalogInvalidation>,
}

fn decode_cached_syscache_entry(
    entry: &CachedSysCacheTuple,
    cache_id: SysCacheId,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
    client_id: ClientId,
) -> Result<SysCacheTuple, CatalogError> {
    if let Some(decoded) = entry.decoded() {
        return Ok(decoded);
    }

    let identity = entry.identity.as_ref().ok_or(CatalogError::Corrupt(
        "syscache tuple has no catalog identity",
    ))?;
    let heap_tuple = entry
        .heap_tuple
        .as_ref()
        .ok_or(CatalogError::Corrupt("syscache tuple has no heap tuple"))?;
    let heap_desc = crate::include::catalog::bootstrap_relation_desc(identity.kind);
    let txns_guard = txns.read();
    let values = crate::backend::catalog::rowcodec::decode_catalog_tuple_values_with_toast(
        pool,
        &txns_guard,
        snapshot,
        client_id,
        identity.kind,
        identity.db_oid,
        &heap_desc,
        heap_tuple,
    )?;
    let decoded = sys_cache_tuple_from_values(cache_id, values)?;
    let _ = entry.decoded.set(decoded);
    entry
        .decoded()
        .ok_or(CatalogError::Corrupt("syscache tuple decode cache failed"))
}

fn decode_cached_syscache_entries(
    entries: Vec<CachedSysCacheTuple>,
    cache_id: SysCacheId,
    pool: &BufferPool<SmgrStorageBackend>,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
    client_id: ClientId,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    entries
        .iter()
        .map(|entry| decode_cached_syscache_entry(entry, cache_id, pool, txns, snapshot, client_id))
        .collect()
}

pub fn invalidate_backend_cache_state(db: &Database, client_id: ClientId) {
    db.backend_cache_states.write().remove(&client_id);
}

impl CatalogStore {
    pub(crate) fn search_sys_cache_entries(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        keys: Vec<Value>,
    ) -> Result<Vec<CachedSysCacheTuple>, CatalogError> {
        if keys.len() != cache_id.expected_keys() {
            return Err(CatalogError::Corrupt("syscache key count mismatch"));
        }

        if let Some(tuple) = bootstrap_sys_cache_tuple(cache_id, &keys) {
            return Ok(vec![CachedSysCacheTuple::from_decoded(tuple)]);
        }

        let extra_tuples = extra_type_sys_cache_tuples(self.extra_type_rows(), cache_id, &keys);
        if !extra_tuples.is_empty() {
            return Ok(extra_tuples
                .into_iter()
                .map(CachedSysCacheTuple::from_decoded)
                .collect());
        }

        let snapshot = ctx.snapshot_for_command()?;
        let tuples = probe_system_catalog_tuples_visible_in_db(
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
            self.scope_db_oid(),
            cache_id.index_oid(),
            equality_scan_keys(&keys),
        )?;

        Ok(tuples
            .into_iter()
            .map(CachedSysCacheTuple::from_scanned)
            .collect())
    }

    pub(crate) fn search_sys_cache(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        keys: Vec<Value>,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        let entries = self.search_sys_cache_entries(ctx, cache_id, keys)?;
        let snapshot = ctx.snapshot_for_command()?;
        decode_cached_syscache_entries(
            entries,
            cache_id,
            &ctx.pool,
            &ctx.txns,
            &snapshot,
            ctx.client_id,
        )
    }

    #[allow(non_snake_case)]
    pub(crate) fn SearchSysCache(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        keys: Vec<Value>,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache(ctx, cache_id, keys)
    }

    pub(crate) fn search_sys_cache1(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache(ctx, cache_id, vec![key1])
    }

    #[allow(non_snake_case)]
    pub(crate) fn SearchSysCache1(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.SearchSysCache(ctx, cache_id, vec![key1])
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

    #[allow(non_snake_case)]
    pub(crate) fn SearchSysCache2(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
        key2: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.SearchSysCache(ctx, cache_id, vec![key1, key2])
    }

    pub(crate) fn search_sys_cache_list1(
        &self,
        ctx: &CatalogWriteContext,
        cache_id: SysCacheId,
        key1: Value,
    ) -> Result<Vec<SysCacheTuple>, CatalogError> {
        self.search_sys_cache_list(ctx, cache_id, vec![key1])
    }

    #[allow(non_snake_case)]
    pub(crate) fn SearchSysCacheList1(
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

    #[allow(non_snake_case)]
    pub(crate) fn SearchSysCacheList2(
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

        let snapshot = ctx.snapshot_for_command()?;
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

    #[allow(non_snake_case)]
    pub(crate) fn RelationIdGetRelation(
        &self,
        ctx: &CatalogWriteContext,
        relation_oid: u32,
    ) -> Result<Option<RelCacheEntry>, CatalogError> {
        self.relation_id_get_relation(ctx, relation_oid)
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
            let fdw_options = attr.attfdwoptions.clone();
            let sql_type = self
                .search_sys_cache1(ctx, SysCacheId::TypeOid, oid_key(attr.atttypid))?
                .into_iter()
                .find_map(|tuple| match tuple {
                    SysCacheTuple::Type(row) => Some(row.sql_type),
                    _ => None,
                })
                .or_else(|| extra_types_by_oid.get(&attr.atttypid).copied())
                .or_else(|| attr.attisdropped.then_some(SqlType::new(SqlTypeKind::Int4)))
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
            desc.attstattarget = attr.attstattarget.unwrap_or(-1);
            desc.attinhcount = attr.attinhcount;
            desc.attislocal = attr.attislocal;
            desc.attacl = attr.attacl.clone();
            desc.collation_oid = attr.attcollation;
            desc.fdw_options = fdw_options;
            desc.identity =
                crate::include::nodes::parsenodes::ColumnIdentityKind::from_catalog_char(
                    attr.attidentity,
                );
            desc.generated =
                crate::include::nodes::parsenodes::ColumnGeneratedKind::from_catalog_char(
                    attr.attgenerated,
                );
            desc.dropped = attr.attisdropped;
            desc.missing_default_value = attr
                .attmissingval
                .as_ref()
                .and_then(|values| values.first().cloned())
                .map(|value| {
                    crate::backend::catalog::catalog::missing_default_value_from_attmissingval(
                        value,
                        desc.sql_type,
                    )
                });
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

        let mut entry = RelCacheEntry {
            rel: relation_locator_for_class_row(
                class_row.oid,
                class_row.relfilenode,
                class_row.reltablespace,
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
            partition_spec: None,
            index,
        };
        entry.partition_spec = lower_relcache_partition_spec(&entry);
        Ok(Some(entry))
    }
}

fn lower_relcache_partition_spec(
    entry: &RelCacheEntry,
) -> Option<crate::backend::parser::LoweredPartitionSpec> {
    entry.partitioned_table.as_ref()?;
    let relation = crate::backend::parser::BoundRelation {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: None,
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        of_type_oid: entry.of_type_oid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relispopulated: entry.relispopulated,
        relispartition: entry.relispartition,
        relpartbound: entry.relpartbound.clone(),
        desc: entry.desc.clone(),
        partitioned_table: entry.partitioned_table.clone(),
        partition_spec: None,
    };
    crate::backend::parser::lower_relation_partition_spec_uncached(&relation).ok()
}

pub(crate) fn relation_id_get_relation_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Result<Option<RelCacheEntry>, CatalogError> {
    let cache_ctx = BackendCacheContext::from(txn_ctx);
    let relation_cache_ctx = relation_cache_context(cache_ctx);
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    if let Some(entry) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.relation_cache_ctx == Some(relation_cache_ctx))
        .and_then(|state| state.relation_cache.get(&relation_oid).cloned())
    {
        return Ok(Some(entry));
    }

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
    if db.other_session_temp_namespace_oid(client_id, class_row.relnamespace) {
        return Ok(None);
    }

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
        let fdw_options = attr.attfdwoptions.clone();
        let type_row = search_sys_cache1_db(
            db,
            client_id,
            txn_ctx,
            SysCacheId::TypeOid,
            oid_key(attr.atttypid),
        )?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row),
            _ => None,
        });
        let sql_type = if let Some(type_row) = type_row {
            let mut sql_type = type_row.sql_type;
            if sql_type.is_array
                && matches!(sql_type.kind, SqlTypeKind::Record)
                && type_row.typelem != 0
                && let Some(element_type) = search_sys_cache1_db(
                    db,
                    client_id,
                    txn_ctx,
                    SysCacheId::TypeOid,
                    oid_key(type_row.typelem),
                )?
                .into_iter()
                .find_map(|tuple| match tuple {
                    SysCacheTuple::Type(row)
                        if matches!(row.sql_type.kind, SqlTypeKind::Composite) =>
                    {
                        Some(row.sql_type)
                    }
                    _ => None,
                })
            {
                sql_type = SqlType::array_of(element_type);
            }
            sql_type
        } else {
            dynamic_types_by_oid
                .get(&attr.atttypid)
                .copied()
                .or_else(|| attr.attisdropped.then_some(SqlType::new(SqlTypeKind::Int4)))
                .ok_or(CatalogError::Corrupt("unknown atttypid"))?
        };
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
        desc.attstattarget = attr.attstattarget.unwrap_or(-1);
        desc.attinhcount = attr.attinhcount;
        desc.attislocal = attr.attislocal;
        desc.attacl = attr.attacl.clone();
        desc.collation_oid = attr.attcollation;
        desc.fdw_options = fdw_options;
        desc.identity = crate::include::nodes::parsenodes::ColumnIdentityKind::from_catalog_char(
            attr.attidentity,
        );
        desc.generated = crate::include::nodes::parsenodes::ColumnGeneratedKind::from_catalog_char(
            attr.attgenerated,
        );
        desc.dropped = attr.attisdropped;
        desc.missing_default_value = attr
            .attmissingval
            .as_ref()
            .and_then(|values| values.first().cloned())
            .map(|value| {
                crate::backend::catalog::catalog::missing_default_value_from_attmissingval(
                    value,
                    desc.sql_type,
                )
            });
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

    let mut entry = RelCacheEntry {
        rel: relation_locator_for_class_row(
            class_row.oid,
            class_row.relfilenode,
            class_row.reltablespace,
            db.database_oid,
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
        partition_spec: None,
        index,
    };
    entry.partition_spec = lower_relcache_partition_spec(&entry);

    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    if state.relation_cache_ctx != Some(relation_cache_ctx) {
        state.relation_cache.clear();
        state.relation_cache_ctx = Some(relation_cache_ctx);
    }
    state.relation_cache.insert(relation_oid, entry.clone());

    Ok(Some(entry))
}

#[allow(non_snake_case)]
pub(crate) fn RelationIdGetRelation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Result<Option<RelCacheEntry>, CatalogError> {
    relation_id_get_relation_db(db, client_id, txn_ctx, relation_oid)
}

fn backend_syscache_get(
    db: &Database,
    client_id: ClientId,
    cache_ctx: BackendCacheContext,
    mode: SysCacheLookupMode,
    key: &SysCacheQueryKey,
) -> Option<Vec<CachedSysCacheTuple>> {
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
    value: Vec<CachedSysCacheTuple>,
) {
    db.backend_cache_states
        .write()
        .entry(client_id)
        .or_default()
        .syscache
        .insert(cache_ctx, mode, key, value);
}

pub(crate) fn search_sys_cache_entries_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<CachedSysCacheTuple>, CatalogError> {
    if keys.len() != cache_id.expected_keys() {
        return Err(CatalogError::Corrupt("syscache key count mismatch"));
    }

    if let Some(tuple) = bootstrap_sys_cache_tuple(cache_id, &keys) {
        return Ok(vec![CachedSysCacheTuple::from_decoded(tuple)]);
    }

    if matches!(cache_id, SysCacheId::TypeOid | SysCacheId::TypeNameNsp) {
        let search_path = db.effective_search_path(client_id, None);
        let dynamic_type_rows = db.dynamic_type_rows_for_search_path(&search_path);
        let extra_tuples = extra_type_sys_cache_tuples(&dynamic_type_rows, cache_id, &keys);
        if !extra_tuples.is_empty() {
            return Ok(extra_tuples
                .into_iter()
                .map(CachedSysCacheTuple::from_decoded)
                .collect());
        }
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
    let tuples = probe_system_catalog_tuples_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        cache_id.index_oid(),
        equality_scan_keys(&keys),
    )?;

    let entries = tuples
        .into_iter()
        .map(CachedSysCacheTuple::from_scanned)
        .collect::<Vec<_>>();
    if let Some(key) = cache_key {
        backend_syscache_insert(
            db,
            client_id,
            cache_ctx,
            SysCacheLookupMode::Exact,
            key,
            entries.clone(),
        );
    }
    Ok(entries)
}

pub(crate) fn search_sys_cache_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    let entries = search_sys_cache_entries_db(db, client_id, txn_ctx, cache_id, keys)?;
    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    let tuples = decode_cached_syscache_entries(
        entries, cache_id, &db.pool, &db.txns, &snapshot, client_id,
    )?;
    Ok(tuples)
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_db(db, client_id, txn_ctx, cache_id, keys)
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheEntries(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<CachedSysCacheTuple>, CatalogError> {
    search_sys_cache_entries_db(db, client_id, txn_ctx, cache_id, keys)
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheCopy1(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
) -> Result<Option<CachedSysCacheTuple>, CatalogError> {
    let mut tuples = SearchSysCacheEntries(db, client_id, txn_ctx, cache_id, vec![key1])?;
    Ok(tuples.pop())
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

#[allow(non_snake_case)]
pub(crate) fn SearchSysCache1(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCache(db, client_id, txn_ctx, cache_id, vec![key1])
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

#[allow(non_snake_case)]
pub(crate) fn SearchSysCache2(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCache(db, client_id, txn_ctx, cache_id, vec![key1, key2])
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCache3(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
    key3: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCache(db, client_id, txn_ctx, cache_id, vec![key1, key2, key3])
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCache4(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
    key3: Value,
    key4: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCache(
        db,
        client_id,
        txn_ctx,
        cache_id,
        vec![key1, key2, key3, key4],
    )
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheExists(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<bool, CatalogError> {
    SearchSysCache(db, client_id, txn_ctx, cache_id, keys).map(|tuples| !tuples.is_empty())
}

#[allow(non_snake_case)]
pub(crate) fn GetSysCacheOid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Option<u32>, CatalogError> {
    SearchSysCache(db, client_id, txn_ctx, cache_id, keys)
        .map(|tuples| tuples.into_iter().find_map(|tuple| tuple.oid()))
}

#[allow(non_snake_case)]
pub(crate) fn SysCacheGetAttr(tuple: &SysCacheTuple, attribute_name: &str) -> Option<Value> {
    let attr = attribute_name.to_ascii_lowercase();
    if attr == "oid" {
        return tuple.oid().map(|oid| Value::Int64(i64::from(oid)));
    }

    match (tuple, attr.as_str()) {
        (SysCacheTuple::Attribute(row), "attname") => Some(Value::Text(row.attname.clone().into())),
        (SysCacheTuple::Attribute(row), "attnum") => Some(Value::Int16(row.attnum)),
        (SysCacheTuple::Class(row), "relname") => Some(Value::Text(row.relname.clone().into())),
        (SysCacheTuple::Class(row), "relnamespace") => {
            Some(Value::Int64(i64::from(row.relnamespace)))
        }
        (SysCacheTuple::Index(row), "indexrelid") => Some(Value::Int64(i64::from(row.indexrelid))),
        (SysCacheTuple::Index(row), "indrelid") => Some(Value::Int64(i64::from(row.indrelid))),
        (SysCacheTuple::Namespace(row), "nspname") => Some(Value::Text(row.nspname.clone().into())),
        (SysCacheTuple::Opclass(row), "opcname") => Some(Value::Text(row.opcname.clone().into())),
        (SysCacheTuple::Operator(row), "oprname") => Some(Value::Text(row.oprname.clone().into())),
        (SysCacheTuple::Proc(row), "proname") => Some(Value::Text(row.proname.clone().into())),
        (SysCacheTuple::Type(row), "typname") => Some(Value::Text(row.typname.clone().into())),
        _ => None,
    }
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

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheList(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    search_sys_cache_list_db(db, client_id, txn_ctx, cache_id, keys)
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheListEntries(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<CachedSysCacheTuple>, CatalogError> {
    search_sys_cache_list_entries_db(db, client_id, txn_ctx, cache_id, keys)
}

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheList1(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCacheList(db, client_id, txn_ctx, cache_id, vec![key1])
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

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheList2(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCacheList(db, client_id, txn_ctx, cache_id, vec![key1, key2])
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

#[allow(non_snake_case)]
pub(crate) fn SearchSysCacheList3(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    key1: Value,
    key2: Value,
    key3: Value,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    SearchSysCacheList(db, client_id, txn_ctx, cache_id, vec![key1, key2, key3])
}

pub(crate) fn shared_dependencies_for_referenced_role(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    role_oid: u32,
) -> Result<Vec<PgShdependRow>, CatalogError> {
    SearchSysCacheList2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::SHDEPENDREFERENCE,
        oid_key(PG_AUTHID_RELATION_OID),
        oid_key(role_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Shdepend(row) => Some(row),
                _ => None,
            })
            .collect()
    })
}

pub(crate) fn shared_dependencies_for_object(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    db_oid: u32,
    class_oid: u32,
    object_oid: u32,
    object_subid: i32,
) -> Result<Vec<PgShdependRow>, CatalogError> {
    SearchSysCacheList(
        db,
        client_id,
        txn_ctx,
        SysCacheId::SHDEPENDDEPENDER,
        vec![
            oid_key(db_oid),
            oid_key(class_oid),
            oid_key(object_oid),
            Value::Int32(object_subid),
        ],
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Shdepend(row) => Some(row),
                _ => None,
            })
            .collect()
    })
}

fn search_sys_cache_list_entries_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<CachedSysCacheTuple>, CatalogError> {
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
    let tuples = probe_system_catalog_tuples_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        cache_id.index_oid(),
        equality_scan_keys(&keys),
    )?;

    let entries = tuples
        .into_iter()
        .map(CachedSysCacheTuple::from_scanned)
        .collect::<Vec<_>>();
    if let Some(key) = cache_key {
        backend_syscache_insert(
            db,
            client_id,
            cache_ctx,
            SysCacheLookupMode::List,
            key,
            entries.clone(),
        );
    }
    Ok(entries)
}

fn search_sys_cache_list_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
    keys: Vec<Value>,
) -> Result<Vec<SysCacheTuple>, CatalogError> {
    let entries = search_sys_cache_list_entries_db(db, client_id, txn_ctx, cache_id, keys)?;
    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    let tuples = decode_cached_syscache_entries(
        entries, cache_id, &db.pool, &db.txns, &snapshot, client_id,
    )?;
    Ok(tuples)
}

pub(crate) fn scan_authid_rows_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<Vec<PgAuthIdRow>, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    probe_system_catalog_rows_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        PG_AUTHID_OID_INDEX_OID,
        Vec::new(),
    )?
    .into_iter()
    .map(pg_authid_row_from_values)
    .collect()
}

pub(crate) fn scan_auth_members_rows_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<Vec<PgAuthMembersRow>, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    probe_system_catalog_rows_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        PG_AUTH_MEMBERS_OID_INDEX_OID,
        Vec::new(),
    )?
    .into_iter()
    .map(pg_auth_members_row_from_values)
    .collect()
}

pub(crate) fn scan_publication_namespace_rows_by_publication_db(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    publication_oid: u32,
) -> Result<Vec<PgPublicationNamespaceRow>, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    probe_system_catalog_rows_visible_in_db(
        &db.pool,
        &db.txns,
        &snapshot,
        client_id,
        db.database_oid,
        PG_PUBLICATION_NAMESPACE_PNNSPID_PNPUBID_INDEX_OID,
        vec![ScanKeyData {
            attribute_number: 2,
            strategy: BT_EQUAL_STRATEGY_NUMBER,
            argument: oid_key(publication_oid),
        }],
    )?
    .into_iter()
    .map(pg_publication_namespace_row_from_values)
    .collect()
}

fn load_backend_catcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<CatCache, CatalogError> {
    #[cfg(test)]
    db.record_backend_catcache_load_for_tests();

    // :HACK: broad CatCache materialization is a compatibility escape hatch for
    // catalog views/tests and legacy callers. Hot planner/executor paths should
    // use SearchSysCache* or RelationIdGetRelation instead, matching
    // PostgreSQL's keyed syscache/relcache model.
    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Io("catalog snapshot failed".into()))?;
    let cache_ctx = BackendCacheContext::from(txn_ctx);
    let shared_cache_ctx = shared_catcache_context(cache_ctx);
    let shared = if let Some(cache) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.shared_catcache_ctx == Some(shared_cache_ctx))
        .and_then(|state| state.shared_catcache.clone())
    {
        cache
    } else {
        let txns = db.txns.read();
        let shared = db
            .shared_catalog
            .read()
            .catcache_with_snapshot(&db.pool, &txns, &snapshot, client_id)?;
        drop(txns);

        let mut states = db.backend_cache_states.write();
        let state = states.entry(client_id).or_default();
        state.shared_catcache_ctx = Some(shared_cache_ctx);
        state.shared_catcache = Some(shared.clone());
        shared
    };
    let mut cache = {
        let txns = db.txns.read();
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
    Ok(cache)
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
        .filter(|state| state.catcache_ctx == Some(cache_ctx))
        .and_then(|state| state.catcache.clone())
    {
        return Ok(cache);
    }

    let cache = load_backend_catcache(db, client_id, txn_ctx)?;

    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.catcache_ctx = Some(cache_ctx);
    state.catcache = Some(cache.clone());
    Ok(cache)
}

pub(crate) fn with_backend_catcache<T>(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    f: impl FnOnce(&CatCache) -> T,
) -> Result<T, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }

    let cache_ctx = BackendCacheContext::from(txn_ctx);
    {
        let states = db.backend_cache_states.read();
        if let Some(cache) = states
            .get(&client_id)
            .filter(|state| state.catcache_ctx == Some(cache_ctx))
            .and_then(|state| state.catcache.as_ref())
        {
            return Ok(f(cache));
        }
    }

    let cache = load_backend_catcache(db, client_id, txn_ctx)?;
    let result = f(&cache);
    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.catcache_ctx = Some(cache_ctx);
    state.catcache = Some(cache);
    Ok(result)
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

fn scan_syscache_rows_without_catcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    cache_id: SysCacheId,
) -> Vec<SysCacheTuple> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    get_catalog_snapshot(db, client_id, txn_ctx, None)
        .and_then(|snapshot| {
            probe_system_catalog_rows_visible_in_db(
                &db.pool,
                &db.txns,
                &snapshot,
                client_id,
                db.database_oid,
                cache_id.index_oid(),
                Vec::new(),
            )
            .ok()
        })
        .unwrap_or_default()
        .into_iter()
        .filter_map(|values| sys_cache_tuple_from_values(cache_id, values).ok())
        .collect()
}

pub fn ensure_namespace_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgNamespaceRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::NAMESPACEOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Namespace(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_class_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgClassRow> {
    scan_class_rows_without_catcache(db, client_id, txn_ctx)
}

pub fn scan_class_rows_without_catcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgClassRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::RELOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Class(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_constraint_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgConstraintRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::CONSTROID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Constraint(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_depend_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgDependRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::DEPENDDEPENDER)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Depend(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_inherit_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgInheritsRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::INHRELIDSEQNO)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Inherits(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_rewrite_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgRewriteRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::REWRITEOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Rewrite(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_statistic_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgStatisticRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::STATRELATTINH)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Statistic(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_attribute_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAttributeRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::ATTNUM)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Attribute(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_attrdef_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAttrdefRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::ATTRDEFOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Attrdef(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_type_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgTypeRow> {
    let mut rows_by_oid = BTreeMap::new();
    for row in builtin_type_rows()
        .into_iter()
        .chain(bootstrap_composite_type_rows())
    {
        rows_by_oid.insert(row.oid, row);
    }

    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }
    if let Some(snapshot) = get_catalog_snapshot(db, client_id, txn_ctx, None)
        && let Ok(rows) = probe_system_catalog_rows_visible_in_db(
            &db.pool,
            &db.txns,
            &snapshot,
            client_id,
            db.database_oid,
            PG_TYPE_OID_INDEX_OID,
            Vec::new(),
        )
    {
        for row in rows
            .into_iter()
            .filter_map(|values| pg_type_row_from_values(values).map_err(|_| ()).ok())
        {
            rows_by_oid.insert(row.oid, row);
        }
    }

    rows_by_oid.into_values().collect()
}

pub fn ensure_index_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgIndexRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::INDEXRELID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Index(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_am_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::AMOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Am(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_amop_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmopRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::AMOPSTRATEGY)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Amop(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_amproc_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmprocRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::AMPROCNUM)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Amproc(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_opclass_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgOpclassRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::CLAOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Opclass(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_opfamily_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgOpfamilyRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::OPFAMILYOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Opfamily(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_collation_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgCollationRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::COLLOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Collation(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_proc_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgProcRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::PROCOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Proc(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_language_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgLanguageRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::LANGOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Language(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_operator_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgOperatorRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::OPEROID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Operator(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_cast_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgCastRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::CASTOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Cast(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_trigger_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgTriggerRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::TRIGGEROID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Trigger(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_event_trigger_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgEventTriggerRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::EventTriggerOid)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::EventTrigger(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_policy_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgPolicyRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::POLICYOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Policy(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_publication_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgPublicationRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::PUBLICATIONOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Publication(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_publication_rel_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgPublicationRelRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::PUBLICATIONREL)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::PublicationRel(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_publication_namespace_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgPublicationNamespaceRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::PUBLICATIONNAMESPACE)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::PublicationNamespace(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_statistic_ext_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgStatisticExtRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::STATEXTOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::StatisticExt(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn ensure_statistic_ext_data_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgStatisticExtDataRow> {
    scan_syscache_rows_without_catcache(db, client_id, txn_ctx, SysCacheId::STATEXTDATASTXOID)
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::StatisticExtData(row) => Some(row),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{
        BOOTSTRAP_SUPERUSER_OID, BootstrapCatalogKind, PUBLIC_NAMESPACE_OID, PgDatabaseRow,
    };

    #[test]
    fn postgres_syscache_names_map_to_catalog_indexes() {
        assert_eq!(SysCacheId::RELOID, SysCacheId::RelOid);
        assert_eq!(SysCacheId::RELOID.index_oid(), PG_CLASS_OID_INDEX_OID);
        assert_eq!(
            SysCacheId::RELNAMENSP.index_oid(),
            PG_CLASS_RELNAME_NSP_INDEX_OID
        );
        assert_eq!(
            SysCacheId::ATTNUM.index_oid(),
            PG_ATTRIBUTE_RELID_ATTNUM_INDEX_OID
        );
        assert_eq!(
            SysCacheId::INDEXRELID.index_oid(),
            PG_INDEX_INDEXRELID_INDEX_OID
        );
        assert_eq!(SysCacheId::TYPEOID.index_oid(), PG_TYPE_OID_INDEX_OID);
        assert_eq!(
            SysCacheId::CONSTROID.index_oid(),
            PG_CONSTRAINT_OID_INDEX_OID
        );
        assert_eq!(
            SysCacheId::DATABASEOID.index_oid(),
            PG_DATABASE_OID_INDEX_OID
        );
        assert_eq!(
            SysCacheId::TABLESPACEOID.index_oid(),
            PG_TABLESPACE_OID_INDEX_OID
        );
        assert_eq!(
            SysCacheId::TABLESPACENAME.index_oid(),
            PG_TABLESPACE_SPCNAME_INDEX_OID
        );
    }

    #[test]
    fn backend_syscache_caches_empty_exact_and_list_results() {
        let mut cache = BackendSysCache::default();
        let exact_key = SysCacheQueryKey::new(SysCacheId::RELOID, &[oid_key(42)]).unwrap();
        let list_key = SysCacheQueryKey::new(SysCacheId::ATTNUM, &[oid_key(42)]).unwrap();

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
        let rel_key = SysCacheQueryKey::new(SysCacheId::RELOID, &[oid_key(42)]).unwrap();
        let type_key = SysCacheQueryKey::new(
            SysCacheId::TYPENAMENSP,
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
            .syscache_flush_catalogs
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
    fn backend_syscache_invalidates_exact_negative_entry_by_key() {
        let mut cache = BackendSysCache::default();
        let rel_key = SysCacheQueryKey::new(
            SysCacheId::RELNAMENSP,
            &[
                Value::Text("created_later".into()),
                oid_key(PUBLIC_NAMESPACE_OID),
            ],
        )
        .unwrap();
        let other_key = SysCacheQueryKey::new(
            SysCacheId::RELNAMENSP,
            &[
                Value::Text("unrelated".into()),
                oid_key(PUBLIC_NAMESPACE_OID),
            ],
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
            other_key.clone(),
            Vec::new(),
        );

        let mut invalidation = CatalogInvalidation::default();
        invalidation
            .syscache_keys
            .insert(rel_key.as_invalidation_key());
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
                &other_key
            ),
            Some(Vec::new())
        );
    }

    #[test]
    fn backend_syscache_invalidates_list_prefix_by_full_key() {
        let mut cache = BackendSysCache::default();
        let rel42_attrs = SysCacheQueryKey::new(SysCacheId::ATTNUM, &[oid_key(42)]).unwrap();
        let rel43_attrs = SysCacheQueryKey::new(SysCacheId::ATTNUM, &[oid_key(43)]).unwrap();
        let full_attnum =
            SysCacheQueryKey::new(SysCacheId::ATTNUM, &[oid_key(42), Value::Int16(1)]).unwrap();

        cache.insert(
            BackendCacheContext::Autocommit,
            SysCacheLookupMode::List,
            rel42_attrs.clone(),
            Vec::new(),
        );
        cache.insert(
            BackendCacheContext::Autocommit,
            SysCacheLookupMode::List,
            rel43_attrs.clone(),
            Vec::new(),
        );

        let mut invalidation = CatalogInvalidation::default();
        invalidation
            .syscache_keys
            .insert(full_attnum.as_invalidation_key());
        cache.invalidate(&invalidation);

        assert_eq!(
            cache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::List,
                &rel42_attrs
            ),
            None
        );
        assert_eq!(
            cache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::List,
                &rel43_attrs
            ),
            Some(Vec::new())
        );
    }

    #[test]
    fn backend_syscache_reuses_transaction_entries_across_command_ids() {
        let mut cache = BackendSysCache::default();
        let key = SysCacheQueryKey::new(SysCacheId::RELOID, &[oid_key(42)]).unwrap();
        let first = BackendCacheContext::Transaction { xid: 1, cid: 1 };
        let second = BackendCacheContext::Transaction { xid: 1, cid: 2 };
        let other_xid = BackendCacheContext::Transaction { xid: 2, cid: 1 };

        cache.insert(first, SysCacheLookupMode::Exact, key.clone(), Vec::new());
        assert_eq!(
            cache.get(first, SysCacheLookupMode::Exact, &key),
            Some(Vec::new())
        );
        assert_eq!(
            cache.get(second, SysCacheLookupMode::Exact, &key),
            Some(Vec::new())
        );
        assert_eq!(cache.get(other_xid, SysCacheLookupMode::Exact, &key), None);
    }

    #[test]
    fn syscache_query_key_skips_unsupported_value_shapes() {
        assert!(SysCacheQueryKey::new(SysCacheId::TYPEOID, &[Value::Float64(1.0)]).is_none());
    }

    #[test]
    fn relation_lookup_uses_keyed_syscache_and_one_relcache_entry() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "lazy_relation");
        let db = Database::open(&base, 16).unwrap();
        db.execute(1, "create table syscache_items (id int4)")
            .unwrap();

        let client_id = 77;
        let rel_key = SysCacheQueryKey::new(
            SysCacheId::RELNAMENSP,
            &[
                Value::Text("syscache_items".into()),
                oid_key(PUBLIC_NAMESPACE_OID),
            ],
        )
        .unwrap();
        let rows = SearchSysCache2(
            &db,
            client_id,
            None,
            SysCacheId::RELNAMENSP,
            Value::Text("syscache_items".into()),
            oid_key(PUBLIC_NAMESPACE_OID),
        )
        .unwrap();
        let relation_oid = rows
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Class(row) => Some(row.oid),
                _ => None,
            })
            .unwrap();
        let class_row = class_row_for_cache_test(&db, client_id, relation_oid);

        {
            let mut states = db.backend_cache_states.write();
            let state = states.get_mut(&client_id).unwrap();
            let cached = state
                .syscache
                .get(
                    BackendCacheContext::Autocommit,
                    SysCacheLookupMode::Exact,
                    &rel_key,
                )
                .unwrap();
            assert_eq!(cached.len(), 1);
            assert!(cached[0].identity.is_some());
            assert_eq!(cached[0].decoded(), Some(SysCacheTuple::Class(class_row)));
            assert!(state.catcache.is_none());
            assert!(state.relation_cache.is_empty());
        }

        let entry = RelationIdGetRelation(&db, client_id, None, relation_oid)
            .unwrap()
            .unwrap();
        assert_eq!(entry.relation_oid, relation_oid);

        let mut states = db.backend_cache_states.write();
        let state = states.get_mut(&client_id).unwrap();
        assert!(state.catcache.is_none());
        assert_eq!(state.relation_cache.len(), 1);
        assert!(state.relation_cache.contains_key(&relation_oid));
    }

    #[test]
    fn proc_name_lookup_uses_syscache_list_without_catcache() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "proc_name");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 78;
        let proc_key =
            SysCacheQueryKey::new(SysCacheId::PROCNAMEARGSNSP, &[Value::Text("abs".into())])
                .unwrap();

        let rows = SearchSysCacheList1(
            &db,
            client_id,
            None,
            SysCacheId::PROCNAMEARGSNSP,
            Value::Text("abs".into()),
        )
        .unwrap();
        assert!(
            rows.iter().any(|tuple| matches!(
                tuple,
                SysCacheTuple::Proc(row) if row.proname.eq_ignore_ascii_case("abs")
            )),
            "pg_proc rows should be found through the PROCNAMEARGSNSP prefix list"
        );

        let mut states = db.backend_cache_states.write();
        let state = states.get_mut(&client_id).unwrap();
        let cached = state
            .syscache
            .get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::List,
                &proc_key,
            )
            .unwrap();
        let cached_rows = cached
            .iter()
            .filter_map(CachedSysCacheTuple::decoded)
            .collect::<Vec<_>>();
        assert_eq!(cached_rows, rows);
        assert!(cached.iter().all(|entry| entry.identity.is_some()));
        assert!(state.catcache.is_none());
        assert!(state.relation_cache.is_empty());
    }

    #[test]
    fn database_oid_lookup_uses_keyed_syscache_without_catcache() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "database_oid");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 81;
        let key =
            SysCacheQueryKey::new(SysCacheId::DATABASEOID, &[oid_key(db.database_oid)]).unwrap();

        let rows = SearchSysCache1(
            &db,
            client_id,
            None,
            SysCacheId::DATABASEOID,
            oid_key(db.database_oid),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            &rows[0],
            SysCacheTuple::Database(row)
                if row.oid == db.database_oid && row.datdba == BOOTSTRAP_SUPERUSER_OID
        ));

        let mut states = db.backend_cache_states.write();
        let state = states.get_mut(&client_id).unwrap();
        let cached = state
            .syscache
            .get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::Exact,
                &key,
            )
            .unwrap();
        let cached_rows = cached
            .iter()
            .filter_map(CachedSysCacheTuple::decoded)
            .collect::<Vec<_>>();
        assert_eq!(cached_rows, rows);
        assert!(cached.iter().all(|entry| entry.identity.is_some()));
        assert!(state.catcache.is_none());
    }

    #[test]
    fn database_oid_lookup_caches_negative_entry_without_catcache() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "database_negative");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 82;
        let missing_oid = db.shared_catalog.read().next_oid().saturating_add(10_000);
        let key = SysCacheQueryKey::new(SysCacheId::DATABASEOID, &[oid_key(missing_oid)]).unwrap();

        assert!(
            SearchSysCache1(
                &db,
                client_id,
                None,
                SysCacheId::DATABASEOID,
                oid_key(missing_oid),
            )
            .unwrap()
            .is_empty()
        );

        let mut states = db.backend_cache_states.write();
        let state = states.get_mut(&client_id).unwrap();
        assert_eq!(
            state.syscache.get(
                BackendCacheContext::Autocommit,
                SysCacheLookupMode::Exact,
                &key
            ),
            Some(Vec::new())
        );
        assert!(state.catcache.is_none());
    }

    #[test]
    fn create_database_invalidates_negative_database_oid_entry() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "database_create");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 83;
        let new_database_oid = db.shared_catalog.read().next_oid();

        assert!(
            SearchSysCache1(
                &db,
                client_id,
                None,
                SysCacheId::DATABASEOID,
                oid_key(new_database_oid),
            )
            .unwrap()
            .is_empty()
        );

        db.execute(client_id, "create database syscache_created_db")
            .unwrap();

        assert!(
            SearchSysCache1(
                &db,
                client_id,
                None,
                SysCacheId::DATABASEOID,
                oid_key(new_database_oid),
            )
            .unwrap()
            .into_iter()
            .any(|tuple| matches!(tuple, SysCacheTuple::Database(row)
                    if row.oid == new_database_oid && row.datname == "syscache_created_db"))
        );
    }

    #[test]
    fn alter_database_owner_invalidates_database_oid_entry() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "database_update");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 84;
        let original = database_row_for_cache_test(&db, client_id, db.database_oid);

        db.execute(client_id, "create role database_cache_owner")
            .unwrap();
        let new_owner_oid = SearchSysCache1(
            &db,
            client_id,
            None,
            SysCacheId::AUTHNAME,
            Value::Text("database_cache_owner".into()),
        )
        .unwrap()
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::AuthId(row) => Some(row.oid),
            _ => None,
        })
        .unwrap();

        db.execute(
            client_id,
            &format!(
                "alter database {} owner to database_cache_owner",
                db.database_name
            ),
        )
        .unwrap();

        let updated = database_row_for_cache_test(&db, client_id, db.database_oid);
        assert_eq!(updated.datdba, new_owner_oid);
        assert_ne!(updated.datdba, original.datdba);
    }

    #[test]
    fn create_table_invalidates_negative_relname_syscache_entry() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "create_rel");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 79;
        let name = Value::Text("created_later".into());

        assert!(
            SearchSysCache2(
                &db,
                client_id,
                None,
                SysCacheId::RELNAMENSP,
                name.clone(),
                oid_key(PUBLIC_NAMESPACE_OID),
            )
            .unwrap()
            .is_empty()
        );

        db.execute(client_id, "create table created_later (id int4)")
            .unwrap();

        assert!(
            SearchSysCache2(
                &db,
                client_id,
                None,
                SysCacheId::RELNAMENSP,
                name,
                oid_key(PUBLIC_NAMESPACE_OID),
            )
            .unwrap()
            .into_iter()
            .any(
                |tuple| matches!(tuple, SysCacheTuple::Class(row) if row.relname == "created_later")
            )
        );
    }

    #[test]
    fn create_index_invalidates_heap_relcache_entry() {
        let base = crate::pgrust::test_support::scratch_temp_dir("syscache", "index_relcache");
        let db = Database::open(&base, 16).unwrap();
        let client_id = 80;

        db.execute(client_id, "create table relcache_indexed (id int4)")
            .unwrap();
        let relation_oid = SearchSysCache2(
            &db,
            client_id,
            None,
            SysCacheId::RELNAMENSP,
            Value::Text("relcache_indexed".into()),
            oid_key(PUBLIC_NAMESPACE_OID),
        )
        .unwrap()
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Class(row) => Some(row.oid),
            _ => None,
        })
        .unwrap();

        let before = RelationIdGetRelation(&db, client_id, None, relation_oid)
            .unwrap()
            .unwrap();
        assert!(!before.relhasindex);

        db.execute(
            client_id,
            "create index relcache_indexed_id_idx on relcache_indexed (id)",
        )
        .unwrap();

        let after = RelationIdGetRelation(&db, client_id, None, relation_oid)
            .unwrap()
            .unwrap();
        assert!(after.relhasindex);
    }

    fn class_row_for_cache_test(
        db: &Database,
        client_id: ClientId,
        relation_oid: u32,
    ) -> PgClassRow {
        SearchSysCache1(
            db,
            client_id,
            None,
            SysCacheId::RELOID,
            oid_key(relation_oid),
        )
        .unwrap()
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Class(row) => Some(row),
            _ => None,
        })
        .unwrap()
    }

    fn database_row_for_cache_test(
        db: &Database,
        client_id: ClientId,
        database_oid: u32,
    ) -> PgDatabaseRow {
        SearchSysCache1(
            db,
            client_id,
            None,
            SysCacheId::DATABASEOID,
            oid_key(database_oid),
        )
        .unwrap()
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Database(row) => Some(row),
            _ => None,
        })
        .unwrap()
    }
}
