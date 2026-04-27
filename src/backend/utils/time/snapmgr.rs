use std::collections::BTreeSet;

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, INVALID_TRANSACTION_ID, TransactionId};
use crate::backend::utils::cache::syscache::BackendCacheContext;
use crate::include::catalog::BootstrapCatalogKind;
use crate::pgrust::database::Database;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub current_xid: TransactionId,
    pub current_cid: CommandId,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub(crate) in_progress: BTreeSet<TransactionId>,
}

impl Snapshot {
    pub fn bootstrap() -> Self {
        Self {
            current_xid: INVALID_TRANSACTION_ID,
            current_cid: CommandId::MAX,
            xmin: 1,
            xmax: 1,
            in_progress: BTreeSet::new(),
        }
    }

    pub fn transaction_active_in_snapshot(&self, xid: TransactionId) -> bool {
        xid != INVALID_TRANSACTION_ID
            && xid != self.current_xid
            && xid >= self.xmin
            && xid < self.xmax
            && self.in_progress.contains(&xid)
    }
}

pub fn relation_has_syscache(relation_oid: u32) -> bool {
    matches!(
        relation_oid,
        oid if oid == BootstrapCatalogKind::PgNamespace.relation_oid()
            || oid == BootstrapCatalogKind::PgClass.relation_oid()
            || oid == BootstrapCatalogKind::PgAttribute.relation_oid()
            || oid == BootstrapCatalogKind::PgAttrdef.relation_oid()
            || oid == BootstrapCatalogKind::PgType.relation_oid()
            || oid == BootstrapCatalogKind::PgProc.relation_oid()
            || oid == BootstrapCatalogKind::PgLanguage.relation_oid()
            || oid == BootstrapCatalogKind::PgIndex.relation_oid()
            || oid == BootstrapCatalogKind::PgAm.relation_oid()
            || oid == BootstrapCatalogKind::PgConstraint.relation_oid()
            || oid == BootstrapCatalogKind::PgOpclass.relation_oid()
            || oid == BootstrapCatalogKind::PgOpfamily.relation_oid()
            || oid == BootstrapCatalogKind::PgCollation.relation_oid()
    )
}

pub fn relation_invalidates_snapshots_only(_relation_oid: u32) -> bool {
    false
}

pub fn invalidate_catalog_snapshot(db: &Database, client_id: ClientId) {
    if let Some(state) = db.backend_cache_states.write().get_mut(&client_id) {
        state.catalog_snapshot = None;
        state.catalog_snapshot_ctx = None;
    }
}

pub fn set_transaction_snapshot_override(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    snapshot: Snapshot,
) {
    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.transaction_snapshot_override = Some((xid, snapshot));
    state.catalog_snapshot = None;
    state.catalog_snapshot_ctx = None;
    state.catcache = None;
    state.catcache_ctx = None;
    state.relation_cache.clear();
}

pub fn clear_transaction_snapshot_override(db: &Database, client_id: ClientId) {
    if let Some(state) = db.backend_cache_states.write().get_mut(&client_id) {
        state.transaction_snapshot_override = None;
        state.catalog_snapshot = None;
        state.catalog_snapshot_ctx = None;
        state.catcache = None;
        state.catcache_ctx = None;
        state.relation_cache.clear();
    }
}

pub fn get_catalog_snapshot(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: Option<u32>,
) -> Option<Snapshot> {
    let snapshot_ctx = BackendCacheContext::from(txn_ctx);
    let reusable_snapshot = relation_oid
        .is_none_or(|oid| relation_has_syscache(oid) || relation_invalidates_snapshots_only(oid));

    if !reusable_snapshot {
        invalidate_catalog_snapshot(db, client_id);
    }

    if let Some(snapshot) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.catalog_snapshot_ctx == Some(snapshot_ctx))
        .and_then(|state| state.catalog_snapshot.clone())
    {
        return reusable_snapshot.then_some(snapshot);
    }

    let snapshot = if let Some((xid, cid)) = txn_ctx {
        let override_snapshot = db
            .backend_cache_states
            .read()
            .get(&client_id)
            .and_then(|state| state.transaction_snapshot_override.clone())
            .filter(|(override_xid, _)| *override_xid == xid)
            .map(|(_, mut snapshot)| {
                snapshot.current_xid = xid;
                snapshot.current_cid = cid;
                snapshot
            });
        if override_snapshot.is_some() {
            override_snapshot
        } else {
            let txns = db.txns.read();
            txns.snapshot_for_command(xid, cid).ok()
        }
    } else {
        let txns = db.txns.read();
        txns.snapshot(INVALID_TRANSACTION_ID).ok()
    }?;

    if reusable_snapshot {
        let mut states = db.backend_cache_states.write();
        let state = states.entry(client_id).or_default();
        state.catalog_snapshot = Some(snapshot.clone());
        state.catalog_snapshot_ctx = Some(snapshot_ctx);
    }
    Some(snapshot)
}
