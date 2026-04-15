use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogError;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::inval::CatalogInvalidation;
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::time::snapmgr::{Snapshot, get_catalog_snapshot};
use crate::include::catalog::{
    PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgDependRow, PgIndexRow, PgInheritsRow, PgNamespaceRow, PgOpclassRow,
    PgOpfamilyRow, PgRewriteRow, PgStatisticRow, PgTypeRow,
};
use crate::pgrust::database::Database;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCacheContext {
    Autocommit,
    Transaction {
        xid: TransactionId,
        cid: CommandId,
    },
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
        let store = db.catalog.read();
        let txns = db.txns.read();
        store.catcache_with_snapshot(&db.pool, &txns, &snapshot, client_id)?
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

    let relcache = RelCache::from_catcache(&backend_catcache(db, client_id, txn_ctx)?)?;
    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.cache_ctx = Some(cache_ctx);
    state.relcache = Some(relcache.clone());
    Ok(relcache)
}

pub fn drain_pending_invalidations(
    db: &Database,
    client_id: ClientId,
) -> Vec<CatalogInvalidation> {
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
