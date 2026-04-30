use std::collections::BTreeSet;

use crate::ClientId;
use crate::backend::catalog::bootstrap::bootstrap_catalog_entry;
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

    db.invalidate_plpgsql_function_cache(client_id);

    let mut states = db.backend_cache_states.write();
    let Some(state) = states.get_mut(&client_id) else {
        return;
    };

    if invalidation.full_reset {
        *state = BackendCacheState::default();
        return;
    }

    state.syscache.invalidate(invalidation);
    if invalidation
        .touched_catalogs
        .contains(&BootstrapCatalogKind::PgEventTrigger)
        || (invalidation.touched_catalogs.is_empty()
            && invalidation
                .relation_oids
                .contains(&BootstrapCatalogKind::PgEventTrigger.relation_oid()))
    {
        state.event_trigger_cache = None;
        state.event_trigger_cache_ctx = None;
    }
    state.catcache = None;
    state.catcache_ctx = None;
    let touches_shared_catalog = invalidation.touched_catalogs.is_empty()
        || invalidation
            .touched_catalogs
            .iter()
            .any(|kind| matches!(kind.scope(), crate::include::catalog::CatalogScope::Shared));
    if touches_shared_catalog {
        state.shared_catcache = None;
        state.shared_catcache_ctx = None;
    }
    if invalidation.relation_oids.is_empty() {
        state.relation_cache.clear();
        state.relation_cache_ctx = None;
    } else {
        for relation_oid in &invalidation.relation_oids {
            state.relation_cache.remove(relation_oid);
        }
    }
    state.catalog_snapshot = None;
    state.catalog_snapshot_ctx = None;
}

#[allow(non_snake_case)]
pub fn InvalidateSystemCaches(db: &Database, client_id: ClientId) {
    let invalidation = CatalogInvalidation {
        full_reset: true,
        ..CatalogInvalidation::default()
    };
    apply_backend_cache_invalidation(db, client_id, &invalidation);
}

#[allow(non_snake_case)]
pub fn CacheInvalidateRelcache(db: &Database, client_id: ClientId, relation_oid: u32) {
    let mut invalidation = CatalogInvalidation::default();
    invalidation.relation_oids.insert(relation_oid);
    apply_backend_cache_invalidation(db, client_id, &invalidation);
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

#[allow(non_snake_case)]
pub fn AcceptInvalidationMessages(db: &Database, client_id: ClientId) {
    accept_invalidation_messages(db, client_id);
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
    for effect in effects {
        for rel in &effect.dropped_rels {
            let _ = db.pool.invalidate_relation(*rel);
            db.pool
                .with_storage_mut(|s| s.smgr.unlink(*rel, None, false));
        }
    }
    // PostgreSQL invalidates catcache/relcache entries at commit and reloads
    // them lazily on the next lookup. It also leaves dead catalog index tuples
    // behind for a later VACUUM once the visibility horizon advances, rather
    // than running a special post-commit cleanup pass here.
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
