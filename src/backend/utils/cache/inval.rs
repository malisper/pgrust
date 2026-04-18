use std::collections::BTreeSet;

use crate::ClientId;
use crate::backend::catalog::bootstrap::bootstrap_catalog_entry;
use crate::backend::catalog::indexing::vacuum_system_catalog_indexes_for_kinds_in_db;
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::storage::smgr::StorageManager;
use crate::backend::utils::cache::syscache::{BackendCacheState, drain_pending_invalidations};
use crate::include::catalog::BootstrapCatalogKind;
use crate::pgrust::database::Database;

#[derive(Debug, Clone, Default)]
pub struct CatalogInvalidation {
    pub touched_catalogs: BTreeSet<BootstrapCatalogKind>,
    pub relation_oids: BTreeSet<u32>,
    pub namespace_oids: BTreeSet<u32>,
    pub type_oids: BTreeSet<u32>,
    pub full_reset: bool,
}

impl CatalogInvalidation {
    pub fn is_empty(&self) -> bool {
        !self.full_reset
            && self.touched_catalogs.is_empty()
            && self.relation_oids.is_empty()
            && self.namespace_oids.is_empty()
            && self.type_oids.is_empty()
    }
}

pub fn catalog_invalidation_from_effect(effect: &CatalogMutationEffect) -> CatalogInvalidation {
    CatalogInvalidation {
        touched_catalogs: effect.touched_catalogs.iter().copied().collect(),
        relation_oids: effect.relation_oids.iter().copied().collect(),
        namespace_oids: effect.namespace_oids.iter().copied().collect(),
        type_oids: effect.type_oids.iter().copied().collect(),
        full_reset: effect.full_reset,
    }
}

pub fn apply_backend_cache_invalidation(
    db: &Database,
    client_id: ClientId,
    invalidation: &CatalogInvalidation,
) {
    if invalidation.is_empty() {
        return;
    }

    let mut states = db.backend_cache_states.write();
    let Some(state) = states.get_mut(&client_id) else {
        return;
    };

    if invalidation.full_reset {
        *state = BackendCacheState::default();
        return;
    }

    state.catcache = None;
    state.relcache = None;
    state.cache_ctx = None;
    state.catalog_snapshot = None;
    state.catalog_snapshot_ctx = None;
}

fn queue_backend_cache_invalidation(
    db: &Database,
    source_client_id: Option<ClientId>,
    invalidation: &CatalogInvalidation,
) {
    if invalidation.is_empty() {
        return;
    }
    let client_ids = db
        .backend_cache_states
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for client_id in client_ids {
        if Some(client_id) == source_client_id {
            continue;
        }
        db.backend_cache_states
            .write()
            .entry(client_id)
            .or_default()
            .pending_invalidations
            .push(invalidation.clone());
    }
}

pub fn publish_committed_catalog_invalidation(
    db: &Database,
    source_client_id: ClientId,
    invalidation: &CatalogInvalidation,
) {
    if invalidation.is_empty() {
        return;
    }
    apply_backend_cache_invalidation(db, source_client_id, invalidation);
    queue_backend_cache_invalidation(db, Some(source_client_id), invalidation);
}

pub fn accept_invalidation_messages(db: &Database, client_id: ClientId) {
    for invalidation in drain_pending_invalidations(db, client_id) {
        apply_backend_cache_invalidation(db, client_id, &invalidation);
    }
}

pub fn finalize_command_end_local_catalog_invalidations(
    db: &Database,
    client_id: ClientId,
    invalidations: &[CatalogInvalidation],
) {
    for invalidation in invalidations {
        apply_backend_cache_invalidation(db, client_id, invalidation);
    }
}

pub fn finalize_committed_catalog_effects(
    db: &Database,
    source_client_id: ClientId,
    effects: &[CatalogMutationEffect],
    invalidations: &[CatalogInvalidation],
) {
    let mut touched_catalogs = Vec::new();
    for effect in effects {
        for &kind in &effect.touched_catalogs {
            if !touched_catalogs.contains(&kind) {
                touched_catalogs.push(kind);
            }
        }
    }
    for kind in touched_catalogs {
        let rel = bootstrap_catalog_entry(kind).rel;
        let nblocks = db
            .pool
            .with_storage_mut(|s| {
                s.smgr
                    .nblocks(rel, crate::backend::storage::smgr::ForkNumber::Main)
            })
            .unwrap_or(0);
        for block in 0..nblocks {
            let _ = crate::backend::access::heap::heapam::heap_flush(&db.pool, 0, rel, block);
        }
    }
    let touched_catalogs = effects
        .iter()
        .flat_map(|effect| effect.touched_catalogs.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let _ = vacuum_system_catalog_indexes_for_kinds_in_db(
        &db.pool,
        &db.txns,
        db.database_oid,
        &touched_catalogs,
    );
    for effect in effects {
        for rel in &effect.dropped_rels {
            let _ = db.pool.invalidate_relation(*rel);
            db.pool
                .with_storage_mut(|s| s.smgr.unlink(*rel, None, false));
        }
    }
    // PostgreSQL invalidates catcache/relcache entries at commit and reloads
    // them lazily on the next lookup. Avoid rebuilding the shared catalog
    // snapshot here; some readers already resolve visible catalog state on
    // demand, and eager refresh introduces lock-order and hot-path costs.
    for invalidation in invalidations {
        publish_committed_catalog_invalidation(db, source_client_id, invalidation);
    }
}

pub fn finalize_aborted_local_catalog_invalidations(
    db: &Database,
    client_id: ClientId,
    prior_invalidations: &[CatalogInvalidation],
    current_invalidations: &[CatalogInvalidation],
) {
    for invalidation in prior_invalidations {
        apply_backend_cache_invalidation(db, client_id, invalidation);
    }
    for invalidation in current_invalidations {
        apply_backend_cache_invalidation(db, client_id, invalidation);
    }
}
