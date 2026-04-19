use super::*;

impl Database {
    pub(crate) fn catalog_invalidation_from_effect(
        effect: &CatalogMutationEffect,
    ) -> CatalogInvalidation {
        catalog_invalidation_from_effect(effect)
    }

    pub(crate) fn finalize_command_end_local_catalog_invalidations(
        &self,
        client_id: ClientId,
        invalidations: &[CatalogInvalidation],
    ) {
        finalize_command_end_local_catalog_invalidations(self, client_id, invalidations);
    }

    pub(super) fn apply_catalog_mutation_effect_immediate(
        &self,
        effect: &CatalogMutationEffect,
    ) -> Result<(), ExecError> {
        for rel in &effect.created_rels {
            self.pool
                .with_storage_mut(|s| {
                    let _ = s.smgr.open(*rel);
                    s.smgr
                        .create(*rel, crate::backend::storage::smgr::ForkNumber::Main, true)
                })
                .map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(e))
                })?;
        }
        Ok(())
    }

    pub(crate) fn finalize_committed_catalog_effects(
        &self,
        source_client_id: ClientId,
        effects: &[CatalogMutationEffect],
        invalidations: &[CatalogInvalidation],
    ) {
        finalize_committed_catalog_effects(self, source_client_id, effects, invalidations);
    }

    pub(crate) fn finalize_aborted_catalog_effects(&self, effects: &[CatalogMutationEffect]) {
        for effect in effects {
            for rel in &effect.created_rels {
                let _ = self.pool.invalidate_relation(*rel);
                self.pool
                    .with_storage_mut(|s| s.smgr.unlink(*rel, None, false));
            }
        }
    }

    pub(crate) fn finalize_aborted_local_catalog_invalidations(
        &self,
        client_id: ClientId,
        prior_invalidations: &[CatalogInvalidation],
        current_invalidations: &[CatalogInvalidation],
    ) {
        finalize_aborted_local_catalog_invalidations(
            self,
            client_id,
            prior_invalidations,
            current_invalidations,
        );
    }

    pub(crate) fn finalize_committed_temp_effects(
        &self,
        _client_id: ClientId,
        _effects: &[TempMutationEffect],
    ) {
    }

    pub(crate) fn finalize_committed_sequence_effects(
        &self,
        effects: &[SequenceMutationEffect],
    ) -> Result<(), ExecError> {
        self.sequences.finalize_committed_effects(effects)
    }

    pub(crate) fn finalize_aborted_temp_effects(
        &self,
        client_id: ClientId,
        effects: &[TempMutationEffect],
    ) {
        let mut namespaces = self.temp_relations.write();
        for effect in effects.iter().rev() {
            match effect {
                TempMutationEffect::Create {
                    name,
                    namespace_created,
                    ..
                } => {
                    if *namespace_created {
                        namespaces.remove(&client_id);
                        continue;
                    }
                    if let Some(namespace) = namespaces.get_mut(&client_id) {
                        namespace.tables.remove(name);
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
                TempMutationEffect::Drop {
                    name,
                    entry,
                    on_commit,
                } => {
                    if let Some(namespace) = namespaces.get_mut(&client_id) {
                        namespace.tables.insert(
                            name.clone(),
                            TempCatalogEntry {
                                entry: entry.clone(),
                                on_commit: *on_commit,
                            },
                        );
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
                TempMutationEffect::Rename { old_name, new_name } => {
                    if let Some(namespace) = namespaces.get_mut(&client_id)
                        && let Some(entry) = namespace.tables.remove(new_name)
                    {
                        namespace.tables.insert(old_name.clone(), entry);
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
            }
        }
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
    }

    pub(crate) fn finalize_aborted_sequence_effects(&self, effects: &[SequenceMutationEffect]) {
        self.sequences.finalize_aborted_effects(effects);
    }

    pub(super) fn finish_txn(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
        catalog_effects: &[CatalogMutationEffect],
        temp_effects: &[TempMutationEffect],
        sequence_effects: &[SequenceMutationEffect],
    ) -> Result<StatementResult, ExecError> {
        match result {
            Ok(r) => {
                let _checkpoint_guard = self.checkpoint_commit_guard();
                self.pool.write_wal_commit(xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                self.pool.flush_wal().map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                self.txns.write().commit(xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(e))
                })?;
                // :HACK: `CatalogStore::catalog_snapshot()` currently rebuilds durable
                // visibility from a fresh on-disk transaction-status reader, so make the
                // just-committed xid visible there immediately instead of waiting for drop
                // or checkpoint-time CLOG flush.
                self.txns.write().flush_clog().map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(e))
                })?;
                let invalidations = catalog_effects
                    .iter()
                    .map(Self::catalog_invalidation_from_effect)
                    .filter(|invalidation| !invalidation.is_empty())
                    .collect::<Vec<_>>();
                self.finalize_command_end_local_catalog_invalidations(client_id, &invalidations);
                self.finalize_committed_catalog_effects(client_id, catalog_effects, &invalidations);
                self.finalize_committed_temp_effects(client_id, temp_effects);
                self.finalize_committed_sequence_effects(sequence_effects)?;
                self.apply_temp_on_commit(client_id)?;
                self.txn_waiter.notify();
                Ok(r)
            }
            Err(e) => {
                let _ = self.txns.write().abort(xid);
                let invalidations = catalog_effects
                    .iter()
                    .map(Self::catalog_invalidation_from_effect)
                    .filter(|invalidation| !invalidation.is_empty())
                    .collect::<Vec<_>>();
                self.finalize_aborted_local_catalog_invalidations(client_id, &[], &invalidations);
                self.finalize_aborted_catalog_effects(catalog_effects);
                self.finalize_aborted_temp_effects(client_id, temp_effects);
                self.finalize_aborted_sequence_effects(sequence_effects);
                self.txn_waiter.notify();
                Err(e)
            }
        }
    }
}

pub(super) struct AutoCommitGuard<'a> {
    txns: &'a Arc<RwLock<TransactionManager>>,
    txn_waiter: &'a TransactionWaiter,
    xid: TransactionId,
    committed: bool,
}

impl<'a> AutoCommitGuard<'a> {
    pub(super) fn new(
        txns: &'a Arc<RwLock<TransactionManager>>,
        txn_waiter: &'a TransactionWaiter,
        xid: TransactionId,
    ) -> Self {
        Self {
            txns,
            txn_waiter,
            xid,
            committed: false,
        }
    }

    pub(super) fn disarm(mut self) {
        self.committed = true;
    }
}

impl Drop for AutoCommitGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.txns.write().abort(self.xid);
            self.txn_waiter.notify();
        }
    }
}
