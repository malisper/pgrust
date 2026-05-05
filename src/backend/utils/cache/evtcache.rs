use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogError;
use crate::backend::utils::cache::syscache::BackendCacheContext;
use crate::backend::utils::time::snapmgr::get_catalog_snapshot;
use crate::pgrust::database::Database;

pub use pgrust_commands::event_trigger::EventTriggerCache;

pub fn event_trigger_cache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<EventTriggerCache, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }

    let cache_ctx = BackendCacheContext::from(txn_ctx);
    if let Some(cache) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.event_trigger_cache_ctx == Some(cache_ctx))
        .and_then(|state| state.event_trigger_cache.clone())
    {
        return Ok(cache);
    }

    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Corrupt("missing catalog snapshot"))?;
    let rows = {
        let txns = db.txns.read();
        db.catalog
            .read()
            .event_trigger_rows_with_snapshot(&db.pool, &txns, &snapshot, client_id)?
    };
    let cache = EventTriggerCache::from_rows(rows);

    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.event_trigger_cache_ctx = Some(cache_ctx);
    state.event_trigger_cache = Some(cache.clone());
    Ok(cache)
}
