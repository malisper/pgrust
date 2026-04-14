use super::super::*;

impl Database {
    pub(crate) fn execute_create_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_table_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateViewStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_view_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (table_name, persistence) =
            self.normalize_create_table_stmt_with_search_path(create_stmt, configured_search_path)?;
        let lowered = lower_create_table(create_stmt)?;
        let desc = lowered.relation_desc;
        match persistence {
            TablePersistence::Permanent => {
                let mut catalog_guard = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let result =
                    catalog_guard.create_table_mvcc(table_name.clone(), desc.clone(), &ctx);
                match result {
                    Err(CatalogError::TableAlreadyExists(name)) if create_stmt.if_not_exists => {
                        Ok(StatementResult::AffectedRows(0))
                    }
                    Err(err) => Err(map_catalog_error(err)),
                    Ok((created, effect)) => {
                        drop(catalog_guard);
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        let relation = crate::backend::parser::BoundRelation {
                            rel: created.entry.rel,
                            relation_oid: created.entry.relation_oid,
                            namespace_oid: created.entry.namespace_oid,
                            relpersistence: created.entry.relpersistence,
                            relkind: created.entry.relkind,
                            toast: None,
                            desc: created.entry.desc.clone(),
                        };
                        for (index, action) in lowered.constraint_actions.iter().enumerate() {
                            let action_cid = cid
                                .saturating_add(1)
                                .saturating_add((index as u32).saturating_mul(3));
                            let index_name = self.choose_index_backed_constraint_name(
                                client_id,
                                xid,
                                action_cid,
                                relation.namespace_oid,
                                &table_name,
                                &action.columns,
                                action.primary,
                            )?;
                            let index_columns = action
                                .columns
                                .iter()
                                .cloned()
                                .map(crate::backend::parser::IndexColumnDef::from)
                                .collect::<Vec<_>>();
                            let build_options = self.resolve_simple_btree_build_options(
                                client_id,
                                Some((xid, action_cid)),
                                &relation,
                                &index_columns,
                            )?;
                            let index_entry = self.build_simple_btree_index_in_transaction(
                                client_id,
                                &relation,
                                &index_name,
                                &index_columns,
                                true,
                                action.primary,
                                xid,
                                action_cid,
                                build_options.0,
                                build_options.1,
                                &build_options.2,
                                65_536,
                                catalog_effects,
                            )?;
                            let constraint_ctx = CatalogWriteContext {
                                pool: self.pool.clone(),
                                txns: self.txns.clone(),
                                xid,
                                cid: action_cid.saturating_add(2),
                                client_id,
                                waiter: None,
                                interrupts: Arc::clone(&interrupts),
                            };
                            let constraint_effect = self
                                .catalog
                                .write()
                                .create_index_backed_constraint_mvcc(
                                    relation.relation_oid,
                                    index_entry.relation_oid,
                                    index_name,
                                    if action.primary {
                                        crate::include::catalog::CONSTRAINT_PRIMARY
                                    } else {
                                        crate::include::catalog::CONSTRAINT_UNIQUE
                                    },
                                    &constraint_ctx,
                                )
                                .map_err(map_catalog_error)?;
                            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
                            catalog_effects.push(constraint_effect);
                        }
                        Ok(StatementResult::AffectedRows(0))
                    }
                }
            }
            TablePersistence::Temporary => {
                if !lowered.constraint_actions.is_empty() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "permanent table for PRIMARY KEY/UNIQUE constraints",
                        actual: "temporary table".into(),
                    }));
                }
                let _ = self.create_temp_relation_in_transaction(
                    client_id,
                    table_name,
                    desc,
                    create_stmt.on_commit,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_create_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let view_name =
            self.normalize_create_view_stmt_with_search_path(create_stmt, configured_search_path)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let plan = crate::backend::parser::pg_plan_query(&create_stmt.query, &catalog)?.plan_tree;
        let desc = crate::backend::executor::RelationDesc {
            columns: plan
                .column_names()
                .into_iter()
                .zip(plan.columns())
                .map(|(name, column)| column_desc(name, column.sql_type, true))
                .collect(),
        };
        let mut referenced_relation_oids = std::collections::BTreeSet::new();
        collect_direct_relation_oids_from_select(
            &create_stmt.query,
            &catalog,
            &mut Vec::new(),
            &mut referenced_relation_oids,
        );
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let (_entry, effect) = self
            .catalog
            .write()
            .create_view_mvcc(
                view_name.clone(),
                desc,
                namespace_oid_for_relation_name(&view_name),
                create_stmt.query_sql.clone(),
                &referenced_relation_oids.into_iter().collect::<Vec<_>>(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_table_as_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (table_name, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(create_stmt, configured_search_path)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let planned_stmt = crate::backend::parser::pg_plan_query(&create_stmt.query, &catalog)?;
        let mut rels = std::collections::BTreeSet::new();
        collect_rels_from_planned_stmt(&planned_stmt, &mut rels);

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            interrupts: Arc::clone(&interrupts),
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
            subplans: Vec::new(),
            timed: false,
        };
        let query_result = execute_readonly_statement(
            Statement::Select(create_stmt.query.clone()),
            &catalog,
            &mut ctx,
        );
        let StatementResult::Query {
            columns,
            column_names,
            rows,
        } = query_result?
        else {
            unreachable!("ctas query should return rows");
        };

        let desc = crate::backend::executor::RelationDesc {
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let name = create_stmt
                        .column_names
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| column_names[index].clone());
                    column_desc(name, column.sql_type, true)
                })
                .collect(),
        };

        let (rel, toast, toast_index) = match persistence {
            TablePersistence::Permanent => {
                let stmt = CreateTableStatement {
                    schema_name: None,
                    table_name: table_name.clone(),
                    persistence,
                    on_commit: create_stmt.on_commit,
                    elements: desc
                        .columns
                        .iter()
                        .map(|column| {
                            crate::backend::parser::CreateTableElement::Column(
                                crate::backend::parser::ColumnDef {
                                    name: column.name.clone(),
                                    ty: column.sql_type,
                                    nullable: true,
                                    default_expr: None,
                                    primary_key: false,
                                    unique: false,
                                },
                            )
                        })
                        .collect(),
                    if_not_exists: create_stmt.if_not_exists,
                };
                let mut catalog_guard = self.catalog.write();
                let write_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let (created, effect) = catalog_guard
                    .create_table_mvcc(table_name.clone(), create_relation_desc(&stmt)?, &write_ctx)
                    .map_err(map_catalog_error)?;
                drop(catalog_guard);
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let (toast, toast_index) = toast_bindings_from_create_result(&created);
                (created.entry.rel, toast, toast_index)
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc.clone(),
                    create_stmt.on_commit,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                )?;
                let (toast, toast_index) = toast_bindings_from_temp_relation(&created);
                (created.entry.rel, toast, toast_index)
            }
        };
        if rows.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut insert_ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            interrupts,
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
            subplans: Vec::new(),
            timed: false,
        };
        let inserted = crate::backend::commands::tablecmds::execute_insert_values(
            rel,
            toast,
            toast_index.as_ref(),
            &desc,
            &[],
            &rows,
            &mut insert_ctx,
            xid,
            cid,
        )?;
        Ok(StatementResult::AffectedRows(inserted))
    }

    pub(crate) fn execute_create_table_as_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: Option<TransactionId>,
        cid: u32,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if let Some(xid) = xid {
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            return self.execute_create_table_as_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                &mut catalog_effects,
                &mut temp_effects,
            );
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_table_as_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            cid,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
        guard.disarm();
        result
    }
}
