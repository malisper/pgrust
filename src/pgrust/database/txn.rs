use super::*;

impl Database {
    pub(crate) fn catalog_store_snapshot(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<crate::backend::catalog::store::CatalogStoreSnapshot, ExecError> {
        let ctx = txn_ctx.map(|(xid, cid)| CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        });
        self.catalog
            .read()
            .snapshot_for_command(ctx.as_ref())
            .map_err(map_catalog_error)
    }

    pub(crate) fn restore_catalog_store_snapshot(
        &self,
        snapshot: crate::backend::catalog::store::CatalogStoreSnapshot,
    ) {
        self.catalog.write().restore_snapshot(snapshot);
    }

    pub(crate) fn restore_catalog_store_snapshot_for_savepoint(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        snapshot: crate::backend::catalog::store::CatalogStoreSnapshot,
        aborted_effects: &[CatalogMutationEffect],
    ) -> Result<CatalogMutationEffect, ExecError> {
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        self.catalog
            .write()
            .restore_snapshot_for_savepoint_rollback(snapshot, aborted_effects, &ctx)
            .map_err(map_catalog_error)
    }

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
        let invalidation = catalog_invalidation_from_effect(effect);
        if !invalidation.is_empty() {
            let client_ids = self
                .backend_cache_states
                .read()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            for client_id in client_ids {
                crate::backend::utils::cache::inval::apply_backend_cache_invalidation(
                    self,
                    client_id,
                    &invalidation,
                );
            }
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
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        for effect in effects.iter().rev() {
            match effect {
                TempMutationEffect::Create {
                    name,
                    namespace_created,
                    ..
                } => {
                    if *namespace_created {
                        namespaces.remove(&temp_backend_id);
                        continue;
                    }
                    if let Some(namespace) = namespaces.get_mut(&temp_backend_id) {
                        namespace.tables.remove(name);
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
                TempMutationEffect::Drop {
                    name,
                    entry,
                    on_commit,
                } => {
                    if let Some(namespace) = namespaces.get_mut(&temp_backend_id) {
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
                    if let Some(namespace) = namespaces.get_mut(&temp_backend_id)
                        && let Some(entry) = namespace.tables.remove(new_name)
                    {
                        namespace.tables.insert(old_name.clone(), entry);
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
                TempMutationEffect::ReplaceRel {
                    relation_oid,
                    old_rel,
                    ..
                } => {
                    if let Some(namespace) = namespaces.get_mut(&temp_backend_id)
                        && let Some(entry) = namespace
                            .tables
                            .values_mut()
                            .find(|entry| entry.entry.relation_oid == *relation_oid)
                    {
                        entry.entry.rel = *old_rel;
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
                self.advisory_locks
                    .unlock_all_transaction(client_id, u64::from(xid));
                self.row_locks
                    .unlock_all_transaction(client_id, u64::from(xid));
                self.commit_enum_labels_created_by(xid);
                self.txn_waiter.unregister_holder(xid);
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
                self.advisory_locks
                    .unlock_all_transaction(client_id, u64::from(xid));
                self.row_locks
                    .unlock_all_transaction(client_id, u64::from(xid));
                self.txn_waiter.unregister_holder(xid);
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

    pub(super) fn new_for_client(
        txns: &'a Arc<RwLock<TransactionManager>>,
        txn_waiter: &'a TransactionWaiter,
        xid: TransactionId,
        client_id: crate::ClientId,
    ) -> Self {
        txn_waiter.register_holder(xid, client_id);
        Self::new(txns, txn_waiter, xid)
    }

    pub(super) fn disarm(mut self) {
        self.committed = true;
    }
}

impl Drop for AutoCommitGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.txns.write().abort(self.xid);
            self.txn_waiter.unregister_holder(self.xid);
            self.txn_waiter.notify();
        }
    }
}
