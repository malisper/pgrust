use super::super::*;

impl Database {
    pub(crate) fn execute_drop_domain_stmt_with_search_path(
        &self,
        _client_id: ClientId,
        drop_stmt: &DropDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, _, _) =
            self.normalize_domain_name_for_create(&drop_stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        if domains.remove(&normalized).is_none() {
            if drop_stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                drop_stmt.domain_name.clone(),
            )));
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::DropTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .table_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for table_name in &drop_stmt.table_names {
            let maybe_entry = catalog.lookup_any_relation(table_name);
            if maybe_entry
                .as_ref()
                .is_some_and(|entry| entry.relpersistence == 't')
            {
                match self.drop_temp_relation_in_transaction(
                    client_id,
                    table_name,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                ) {
                    Ok(_) => dropped += 1,
                    Err(_) if drop_stmt.if_exists => {}
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                }
                continue;
            }

            let relation_oid = match maybe_entry.as_ref() {
                Some(entry) if entry.relkind == 'r' => entry.relation_oid,
                Some(_) => {
                    result = Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: table_name.clone(),
                        expected: "table",
                    }));
                    break;
                }
                None if drop_stmt.if_exists => continue,
                None => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        table_name.clone(),
                    )));
                    break;
                }
            };
            if let Err(err) = reject_relation_with_dependent_views(
                self,
                client_id,
                Some((xid, cid)),
                relation_oid,
                "DROP TABLE on relation without dependent views",
            ) {
                result = Err(err);
                break;
            }
            let mut catalog_guard = self.catalog.write();
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            match catalog_guard.drop_relation_by_oid_mvcc(relation_oid, &ctx) {
                Ok((entries, effect)) => {
                    drop(catalog_guard);
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    let _ = entries;
                    dropped += 1;
                }
                Err(CatalogError::UnknownTable(_)) if drop_stmt.if_exists => {}
                Err(CatalogError::UnknownTable(_)) => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        table_name.clone(),
                    )));
                    break;
                }
                Err(other) => {
                    result = Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "droppable table",
                        actual: format!("{other:?}"),
                    }));
                    break;
                }
            }
        }

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        if result.is_ok() {
            Ok(StatementResult::AffectedRows(dropped))
        } else {
            result
        }
    }

    pub(crate) fn execute_drop_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .view_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for view_name in &drop_stmt.view_names {
            let maybe_entry = catalog.lookup_any_relation(view_name);
            let relation_oid = match maybe_entry.as_ref() {
                Some(entry) if entry.relkind == 'v' => entry.relation_oid,
                Some(_) => {
                    result = Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: view_name.clone(),
                        expected: "view",
                    }));
                    break;
                }
                None if drop_stmt.if_exists => continue,
                None => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        view_name.clone(),
                    )));
                    break;
                }
            };
            if let Err(err) = reject_relation_with_dependent_views(
                self,
                client_id,
                Some((xid, cid)),
                relation_oid,
                "DROP VIEW on relation without dependent views",
            ) {
                result = Err(err);
                break;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            match self
                .catalog
                .write()
                .drop_view_by_oid_mvcc(relation_oid, &ctx)
            {
                Ok((_entry, effect)) => {
                    catalog_effects.push(effect);
                    dropped += 1;
                }
                Err(CatalogError::UnknownTable(_)) if drop_stmt.if_exists => {}
                Err(CatalogError::UnknownTable(_)) => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        view_name.clone(),
                    )));
                    break;
                }
                Err(other) => {
                    result = Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "droppable view",
                        actual: format!("{other:?}"),
                    }));
                    break;
                }
            }
        }

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        if result.is_ok() {
            Ok(StatementResult::AffectedRows(dropped))
        } else {
            result
        }
    }
}
