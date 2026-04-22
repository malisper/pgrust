use super::*;

fn normalize_temp_lookup_name(table_name: &str) -> String {
    table_name
        .strip_prefix("pg_temp.")
        .unwrap_or(table_name)
        .to_ascii_lowercase()
}

fn temp_relation_drop_order(
    relation_oids: Vec<u32>,
    inherit_rows: &[crate::include::catalog::PgInheritsRow],
) -> Vec<u32> {
    fn visit_relation(
        relation_oid: u32,
        children_by_parent: &std::collections::BTreeMap<u32, Vec<u32>>,
        seen: &mut std::collections::BTreeSet<u32>,
        ordered: &mut Vec<u32>,
    ) {
        if !seen.insert(relation_oid) {
            return;
        }
        if let Some(children) = children_by_parent.get(&relation_oid) {
            for child_oid in children {
                visit_relation(*child_oid, children_by_parent, seen, ordered);
            }
        }
        ordered.push(relation_oid);
    }

    let mut relation_oids = relation_oids;
    relation_oids.sort_unstable();
    let relation_set = relation_oids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut children_by_parent = std::collections::BTreeMap::<u32, Vec<u32>>::new();
    for row in inherit_rows {
        if relation_set.contains(&row.inhparent) && relation_set.contains(&row.inhrelid) {
            children_by_parent
                .entry(row.inhparent)
                .or_default()
                .push(row.inhrelid);
        }
    }
    for children in children_by_parent.values_mut() {
        children.sort_unstable();
    }

    let mut ordered = Vec::with_capacity(relation_oids.len());
    let mut seen = std::collections::BTreeSet::new();
    for relation_oid in relation_oids {
        visit_relation(relation_oid, &children_by_parent, &mut seen, &mut ordered);
    }
    ordered
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
            .get(&self.temp_backend_id(client_id))
            .and_then(|ns| ns.tables.get(&normalized).map(|entry| entry.entry.clone()))
    }

    pub(super) fn temp_entry_on_commit(
        &self,
        client_id: ClientId,
        relation_oid: u32,
    ) -> Option<OnCommitAction> {
        self.temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .and_then(|ns| {
                ns.tables
                    .values()
                    .find(|entry| entry.entry.relation_oid == relation_oid)
                    .map(|entry| entry.on_commit)
            })
    }

    pub(super) fn install_temp_entry(
        &self,
        client_id: ClientId,
        table_name: &str,
        entry: RelCacheEntry,
        on_commit: OnCommitAction,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(table_name);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces
            .get_mut(&temp_backend_id)
            .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone())))?;
        namespace
            .tables
            .insert(normalized, TempCatalogEntry { entry, on_commit });
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn replace_temp_entry_desc(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        desc: crate::backend::executor::RelationDesc,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })?;
        let entry = namespace
            .tables
            .values_mut()
            .find(|entry| entry.entry.relation_oid == relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        entry.entry.desc = desc;
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn replace_temp_entry_partition_metadata(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        relkind: char,
        relispartition: bool,
        relpartbound: Option<String>,
        partitioned_table: Option<crate::include::catalog::PgPartitionedTableRow>,
    ) -> Result<(), ExecError> {
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut namespaces = self.temp_relations.write();
        let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })?;
        let entry = namespace
            .tables
            .values_mut()
            .find(|entry| entry.entry.relation_oid == relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
            })?;
        entry.entry.relkind = relkind;
        entry.entry.relispartition = relispartition;
        entry.entry.relpartbound = relpartbound;
        entry.entry.partitioned_table = partitioned_table;
        namespace.generation = namespace.generation.saturating_add(1);
        drop(namespaces);
        self.invalidate_backend_cache_state(client_id);
        Ok(())
    }

    pub(super) fn temp_relation_name_for_oid(
        &self,
        client_id: ClientId,
        relation_oid: u32,
    ) -> Option<String> {
        self.temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .and_then(|ns| {
                ns.tables.iter().find_map(|(name, entry)| {
                    (entry.entry.relation_oid == relation_oid).then(|| name.clone())
                })
            })
    }

    fn cleanup_stale_temp_relations_in_transaction(
        &self,
        client_id: ClientId,
        temp_backend_id: TempBackendId,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let namespace_oids = [
            Self::temp_namespace_oid(temp_backend_id),
            Self::temp_toast_namespace_oid(temp_backend_id),
        ];
        for include_indexes in [false, true] {
            let catcache = self
                .txn_backend_catcache(client_id, xid, *cid)
                .map_err(map_catalog_error)?;
            let relation_oids = temp_relation_drop_order(
                catcache
                    .class_rows()
                    .into_iter()
                    .filter(|row| {
                        row.relpersistence == 't'
                            && namespace_oids.contains(&row.relnamespace)
                            && (include_indexes == (row.relkind == 'i'))
                    })
                    .map(|row| row.oid)
                    .collect::<Vec<_>>(),
                &catcache.inherit_rows(),
            );
            for relation_oid in relation_oids {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: *cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: self.interrupt_state(client_id),
                };
                let effect = self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(relation_oid, &ctx)
                    .map_err(map_catalog_error)?
                    .1;
                catalog_effects.push(effect);
                *cid = (*cid).saturating_add(1);
            }
        }
        Ok(())
    }

    pub(super) fn ensure_temp_namespace(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<TempNamespace, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let temp_backend_id = self.temp_backend_id(client_id);
        if let Some(namespace) = self.owned_temp_namespace(client_id) {
            return Ok(namespace);
        }

        let temp_oid = Self::temp_namespace_oid(temp_backend_id);
        let temp_name = Self::temp_namespace_name(temp_backend_id);
        let temp_toast_oid = Self::temp_toast_namespace_oid(temp_backend_id);
        let temp_toast_name = Self::temp_toast_namespace_name(temp_backend_id);
        let catcache = self
            .txn_backend_catcache(client_id, xid, *cid)
            .map_err(map_catalog_error)?;
        let existing_namespace = catcache.namespace_by_oid(temp_oid).cloned();
        let existing_toast_namespace = catcache.namespace_by_oid(temp_toast_oid).cloned();

        if existing_namespace.is_some() || existing_toast_namespace.is_some() {
            self.cleanup_stale_temp_relations_in_transaction(
                client_id,
                temp_backend_id,
                xid,
                cid,
                catalog_effects,
            )?;
        }

        let namespace = TempNamespace {
            oid: temp_oid,
            name: temp_name,
            owner_oid: existing_namespace
                .as_ref()
                .or(existing_toast_namespace.as_ref())
                .map(|row| row.nspowner)
                .unwrap_or_else(|| self.auth_state(client_id).current_user_oid()),
            toast_oid: temp_toast_oid,
            toast_name: temp_toast_name,
            tables: BTreeMap::new(),
            generation: 0,
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: *cid,
            client_id,
            waiter: None,
            interrupts,
        };
        if existing_namespace.is_none() {
            let effect = self
                .catalog
                .write()
                .create_namespace_mvcc(namespace.oid, &namespace.name, namespace.owner_oid, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        if existing_toast_namespace.is_none() {
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
        }
        {
            let mut namespaces = self.temp_relations.write();
            namespaces.insert(temp_backend_id, namespace.clone());
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
                relispartition: false,
                relpartbound: None,
                relrowsecurity: false,
                relforcerowsecurity: false,
                desc: crate::backend::executor::RelationDesc {
                    columns: Vec::new(),
                },
                partitioned_table: None,
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
        mut cid: CommandId,
        relkind: char,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<CreatedTempRelation, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(&table_name);
        let namespace =
            self.ensure_temp_namespace(client_id, xid, &mut cid, catalog_effects, temp_effects)?;
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
                    Self::temp_db_oid(temp_backend_id),
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
                    Self::temp_db_oid(temp_backend_id),
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
            relispartition: created.entry.relispartition,
            relpartbound: created.entry.relpartbound.clone(),
            relrowsecurity: created.entry.relrowsecurity,
            relforcerowsecurity: created.entry.relforcerowsecurity,
            desc: created.entry.desc.clone(),
            partitioned_table: created.entry.partitioned_table.clone(),
            index: None,
        };
        {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
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
        let temp_backend_id = self.temp_backend_id(client_id);
        let normalized = normalize_temp_lookup_name(table_name);
        let removed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&temp_backend_id).ok_or_else(|| {
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
        let temp_backend_id = self.temp_backend_id(client_id);
        let old_name = {
            let namespaces = self.temp_relations.read();
            let namespace = namespaces.get(&temp_backend_id).ok_or_else(|| {
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
                .get(&temp_backend_id)
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
                .get_mut(&temp_backend_id)
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
        let temp_backend_id = self.temp_backend_id(client_id);
        let mut to_delete = Vec::new();
        let mut to_drop = Vec::new();
        {
            let namespaces = self.temp_relations.read();
            if let Some(namespace) = namespaces.get(&temp_backend_id) {
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
                        .truncate(rel, crate::backend::storage::smgr::ForkNumber::Main, 0)?;
                    if s.smgr.exists(
                        rel,
                        crate::backend::storage::smgr::ForkNumber::VisibilityMap,
                    ) {
                        s.smgr.truncate(
                            rel,
                            crate::backend::storage::smgr::ForkNumber::VisibilityMap,
                            0,
                        )?;
                    }
                    Ok(())
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
        let temp_backend_id = self.temp_backend_id(client_id);
        let Some(_namespace) = self.owned_temp_namespace(client_id) else {
            self.temp_relations.write().remove(&temp_backend_id);
            return;
        };
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut cid = 0;
        let mut effects = Vec::new();
        let result = self.cleanup_stale_temp_relations_in_transaction(
            client_id,
            temp_backend_id,
            xid,
            &mut cid,
            &mut effects,
        );
        let _ = self.finish_txn(
            client_id,
            xid,
            result.map(|_| StatementResult::AffectedRows(0)),
            &effects,
            &[],
            &[],
        );
        guard.disarm();
        self.temp_relations.write().remove(&temp_backend_id);
        self.invalidate_backend_cache_state(client_id);
    }
}
