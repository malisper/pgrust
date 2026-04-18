use super::*;

fn normalize_temp_lookup_name(table_name: &str) -> String {
    table_name
        .strip_prefix("pg_temp.")
        .unwrap_or(table_name)
        .to_ascii_lowercase()
}

impl Database {
    #[cfg(test)]
    pub(crate) fn temp_entry(
        &self,
        client_id: ClientId,
        table_name: &str,
    ) -> Option<RelCacheEntry> {
        let normalized = normalize_temp_lookup_name(table_name);
        self.temp_relations
            .read()
            .get(&client_id)
            .and_then(|ns| ns.tables.get(&normalized).map(|entry| entry.entry.clone()))
    }

    pub(super) fn ensure_temp_namespace(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<TempNamespace, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        if let Some(namespace) = self.owned_temp_namespace(client_id) {
            return Ok(namespace);
        }

        let namespace = TempNamespace {
            oid: Self::temp_namespace_oid(client_id),
            name: Self::temp_namespace_name(client_id),
            owner_oid: self.auth_state(client_id).current_user_oid(),
            toast_oid: Self::temp_toast_namespace_oid(client_id),
            toast_name: Self::temp_toast_namespace_name(client_id),
            tables: BTreeMap::new(),
            generation: 0,
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .create_namespace_mvcc(namespace.oid, &namespace.name, namespace.owner_oid, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        let effect = self
            .catalog
            .write()
            .create_namespace_mvcc(
                namespace.toast_oid,
                &namespace.toast_name,
                namespace.owner_oid,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        {
            let mut namespaces = self.temp_relations.write();
            namespaces.insert(client_id, namespace.clone());
        }
        temp_effects.push(TempMutationEffect::Create {
            name: namespace.name.clone(),
            entry: RelCacheEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: crate::include::catalog::BootstrapCatalogKind::PgNamespace
                        .relation_oid(),
                },
                relation_oid: namespace.oid,
                namespace_oid: namespace.oid,
                owner_oid: namespace.owner_oid,
                row_type_oid: 0,
                array_type_oid: 0,
                reltoastrelid: 0,
                relpersistence: 't',
                relkind: 'n',
                relhastriggers: false,
                desc: crate::backend::executor::RelationDesc {
                    columns: Vec::new(),
                },
                index: None,
            },
            on_commit: OnCommitAction::PreserveRows,
            namespace_created: true,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(namespace)
    }

    pub(super) fn create_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        table_name: String,
        desc: crate::backend::executor::RelationDesc,
        on_commit: OnCommitAction,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<CreatedTempRelation, ExecError> {
        self.create_temp_relation_with_relkind_in_transaction(
            client_id,
            table_name,
            desc,
            on_commit,
            xid,
            cid,
            'r',
            catalog_effects,
            temp_effects,
        )
    }

    pub(super) fn create_temp_relation_with_relkind_in_transaction(
        &self,
        client_id: ClientId,
        table_name: String,
        desc: crate::backend::executor::RelationDesc,
        on_commit: OnCommitAction,
        xid: TransactionId,
        cid: CommandId,
        relkind: char,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<CreatedTempRelation, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let normalized = normalize_temp_lookup_name(&table_name);
        let namespace =
            self.ensure_temp_namespace(client_id, xid, cid, catalog_effects, temp_effects)?;
        if namespace.tables.contains_key(&normalized) {
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(normalized)));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let (created, effect) = if relkind == 'r' {
            self.catalog
                .write()
                .create_table_mvcc_with_options(
                    format!("{}.{}", namespace.name, normalized),
                    desc,
                    namespace.oid,
                    Self::temp_db_oid(client_id),
                    't',
                    namespace.toast_oid,
                    &namespace.toast_name,
                    self.auth_state(client_id).current_user_oid(),
                    &ctx,
                )
                .map_err(map_catalog_error)?
        } else {
            let (entry, effect) = self
                .catalog
                .write()
                .create_relation_mvcc_with_relkind(
                    format!("{}.{}", namespace.name, normalized),
                    desc,
                    namespace.oid,
                    Self::temp_db_oid(client_id),
                    't',
                    relkind,
                    self.auth_state(client_id).current_user_oid(),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            (
                crate::backend::catalog::store::CreateTableResult { entry, toast: None },
                effect,
            )
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let rel_entry = RelCacheEntry {
            rel: created.entry.rel,
            relation_oid: created.entry.relation_oid,
            namespace_oid: created.entry.namespace_oid,
            owner_oid: created.entry.owner_oid,
            row_type_oid: created.entry.row_type_oid,
            array_type_oid: created.entry.array_type_oid,
            reltoastrelid: created.entry.reltoastrelid,
            relpersistence: created.entry.relpersistence,
            relkind: created.entry.relkind,
            relhastriggers: created.entry.relhastriggers,
            desc: created.entry.desc.clone(),
            index: None,
        };
        {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&client_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.tables.insert(
                normalized.clone(),
                TempCatalogEntry {
                    entry: rel_entry.clone(),
                    on_commit,
                },
            );
            namespace.generation = namespace.generation.saturating_add(1);
        }
        temp_effects.push(TempMutationEffect::Create {
            name: normalized,
            entry: rel_entry.clone(),
            on_commit,
            namespace_created: false,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(CreatedTempRelation {
            entry: rel_entry,
            toast: created.toast,
        })
    }

    pub(crate) fn drop_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        table_name: &str,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let normalized = normalize_temp_lookup_name(table_name);
        let removed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&client_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            let removed = namespace.tables.remove(&normalized).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.generation = namespace.generation.saturating_add(1);
            removed
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .drop_relation_by_oid_mvcc(removed.entry.relation_oid, &ctx)
            .map_err(map_catalog_error)?
            .1;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        temp_effects.push(TempMutationEffect::Drop {
            name: normalized,
            entry: removed.entry.clone(),
            on_commit: removed.on_commit,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(removed.entry)
    }

    pub(crate) fn rename_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        new_table_name: &str,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let normalized_new = normalize_temp_lookup_name(new_table_name);
        let old_name = {
            let namespaces = self.temp_relations.read();
            let namespace = namespaces.get(&client_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized_new.clone()))
            })?;
            namespace
                .tables
                .iter()
                .find_map(|(name, entry)| {
                    (entry.entry.relation_oid == relation_oid).then(|| name.clone())
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
                })?
        };

        if old_name != normalized_new {
            let namespaces = self.temp_relations.read();
            let namespace = namespaces
                .get(&client_id)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(old_name.clone())))?;
            if namespace.tables.contains_key(&normalized_new) {
                return Err(ExecError::Parse(ParseError::TableAlreadyExists(
                    normalized_new.clone(),
                )));
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .rename_relation_mvcc(relation_oid, &normalized_new, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);

        let renamed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces
                .get_mut(&client_id)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(old_name.clone())))?;
            let entry = namespace
                .tables
                .remove(&old_name)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(old_name.clone())))?;
            let rel_entry = entry.entry.clone();
            namespace.tables.insert(normalized_new.clone(), entry);
            namespace.generation = namespace.generation.saturating_add(1);
            rel_entry
        };
        temp_effects.push(TempMutationEffect::Rename {
            old_name,
            new_name: normalized_new,
        });
        self.invalidate_backend_cache_state(client_id);
        Ok(renamed)
    }

    pub(crate) fn apply_temp_on_commit(&self, client_id: ClientId) -> Result<(), ExecError> {
        let mut to_delete = Vec::new();
        let mut to_drop = Vec::new();
        {
            let namespaces = self.temp_relations.read();
            if let Some(namespace) = namespaces.get(&client_id) {
                for (name, entry) in &namespace.tables {
                    match entry.on_commit {
                        OnCommitAction::PreserveRows => {}
                        OnCommitAction::DeleteRows => to_delete.push(entry.entry.rel),
                        OnCommitAction::Drop => to_drop.push(name.clone()),
                    }
                }
            }
        }

        for rel in to_delete {
            let _ = self.pool.invalidate_relation(rel);
            self.pool
                .with_storage_mut(|s| {
                    s.smgr
                        .truncate(rel, crate::backend::storage::smgr::ForkNumber::Main, 0)
                })
                .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
        }

        for name in to_drop {
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            let result = self.drop_temp_relation_in_transaction(
                client_id,
                &name,
                xid,
                0,
                &mut catalog_effects,
                &mut temp_effects,
            );
            let result = self.finish_txn(
                client_id,
                xid,
                result.map(|_| StatementResult::AffectedRows(0)),
                &catalog_effects,
                &temp_effects,
                &[],
            );
            guard.disarm();
            let _ = result?;
        }
        Ok(())
    }

    pub(crate) fn cleanup_client_temp_relations(&self, client_id: ClientId) {
        let Some(namespace) = self.owned_temp_namespace(client_id) else {
            return;
        };
        for name in namespace.tables.keys().cloned().collect::<Vec<_>>() {
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            let result = self.drop_temp_relation_in_transaction(
                client_id,
                &name,
                xid,
                0,
                &mut catalog_effects,
                &mut temp_effects,
            );
            let _ = self.finish_txn(
                client_id,
                xid,
                result.map(|_| StatementResult::AffectedRows(0)),
                &catalog_effects,
                &temp_effects,
                &[],
            );
            guard.disarm();
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let mut effects = Vec::new();
        if let Ok(effect) = self.catalog.write().drop_namespace_mvcc(
            namespace.oid,
            &namespace.name,
            namespace.owner_oid,
            &ctx,
        ) {
            effects.push(effect);
        }
        if let Ok(effect) = self.catalog.write().drop_namespace_mvcc(
            namespace.toast_oid,
            &namespace.toast_name,
            namespace.owner_oid,
            &ctx,
        ) {
            effects.push(effect);
        }
        if !effects.is_empty() {
            let _ = self.finish_txn(
                client_id,
                xid,
                Ok(StatementResult::AffectedRows(0)),
                &effects,
                &[],
                &[],
            );
            guard.disarm();
        }
        self.temp_relations.write().remove(&client_id);
        self.invalidate_backend_cache_state(client_id);
    }
}
