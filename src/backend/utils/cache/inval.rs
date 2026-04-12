use std::collections::BTreeSet;

use crate::backend::catalog::bootstrap::bootstrap_catalog_entry;
use crate::backend::catalog::store::CatalogMutationEffect;
use crate::backend::storage::smgr::StorageManager;
use crate::backend::utils::cache::syscache::SessionCatalogState;
use crate::include::catalog::BootstrapCatalogKind;
use crate::pgrust::database::Database;
use crate::ClientId;

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

pub fn apply_session_catalog_invalidation(
    db: &Database,
    client_id: ClientId,
    invalidation: &CatalogInvalidation,
) {
    if invalidation.is_empty() {
        return;
    }

    let mut states = db.session_catalog_states.write();
    let Some(state) = states.get_mut(&client_id) else {
        return;
    };

    if invalidation.full_reset {
        *state = SessionCatalogState::default();
        return;
    }

    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgNamespace))
    {
        state.namespace_rows = None;
        state.catalog_snapshot = None;
    }
    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgClass))
    {
        state.class_rows = None;
        state.catalog_snapshot = None;
    }
    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgAttribute))
    {
        state.attribute_rows = None;
        state.catalog_snapshot = None;
    }
    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgAttrdef))
    {
        state.attrdef_rows = None;
        state.catalog_snapshot = None;
    }
    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgType))
    {
        state.type_rows = None;
        state.catalog_snapshot = None;
    }
    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgIndex))
    {
        state.index_rows = None;
        state.catalog_snapshot = None;
    }
    if invalidation
        .touched_catalogs
        .iter()
        .any(|kind| matches!(kind, BootstrapCatalogKind::PgAm))
    {
        state.am_rows = None;
        state.catalog_snapshot = None;
    }

    for oid in &invalidation.relation_oids {
        state.relation_entries_by_oid.remove(oid);
    }

    if !invalidation.namespace_oids.is_empty() {
        state.namespace_rows = None;
        state.class_rows = None;
        state.relation_entries_by_oid.clear();
        state.catalog_snapshot = None;
    }
    if !invalidation.type_oids.is_empty() {
        state.type_rows = None;
        state.relation_entries_by_oid.clear();
        state.catalog_snapshot = None;
    }
}

pub fn publish_session_catalog_invalidation(
    db: &Database,
    source_client_id: Option<ClientId>,
    invalidation: &CatalogInvalidation,
) {
    if invalidation.is_empty() {
        return;
    }
    if invalidation.full_reset {
        db.session_catalog_states.write().clear();
        return;
    }
    let client_ids = db
        .session_catalog_states
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for client_id in client_ids {
        if Some(client_id) == source_client_id {
            continue;
        }
        apply_session_catalog_invalidation(db, client_id, invalidation);
    }
}

pub fn finalize_command_end_local_catalog_invalidations(
    db: &Database,
    client_id: ClientId,
    invalidations: &[CatalogInvalidation],
) {
    for invalidation in invalidations {
        apply_session_catalog_invalidation(db, client_id, invalidation);
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
            .with_storage_mut(|s| s.smgr.nblocks(rel, crate::backend::storage::smgr::ForkNumber::Main))
            .unwrap_or(0);
        for block in 0..nblocks {
            let _ = crate::backend::access::heap::heapam::heap_flush(&db.pool, 0, rel, block);
        }
    }
    for effect in effects {
        for rel in &effect.dropped_rels {
            let _ = db.pool.invalidate_relation(*rel);
            db.pool.with_storage_mut(|s| s.smgr.unlink(*rel, None, false));
        }
    }
    for invalidation in invalidations {
        publish_session_catalog_invalidation(db, Some(source_client_id), invalidation);
    }
}

pub fn finalize_aborted_local_catalog_invalidations(
    db: &Database,
    client_id: ClientId,
    prior_invalidations: &[CatalogInvalidation],
    current_invalidations: &[CatalogInvalidation],
) {
    for invalidation in prior_invalidations {
        apply_session_catalog_invalidation(db, client_id, invalidation);
    }
    for invalidation in current_invalidations {
        apply_session_catalog_invalidation(db, client_id, invalidation);
    }
}
