use super::*;
use std::collections::BTreeSet;

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

    pub(crate) fn execute_comment_on_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &comment_stmt.table_name)?;
        self.table_locks
            .lock_table(relation.rel, TableLockMode::AccessExclusive, client_id);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_table_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_add_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        self.table_locks
            .lock_table(relation.rel, TableLockMode::AccessExclusive, client_id);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_analyze_stmt_with_search_path(
        &self,
        client_id: ClientId,
        analyze_stmt: &AnalyzeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let relation_names = analyze_stmt
            .targets
            .iter()
            .map(|target| target.table_name.clone())
            .collect::<Vec<_>>();
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let rels = relation_names
            .iter()
            .map(|name| lookup_heap_relation_for_ddl(&catalog, name))
            .collect::<Result<Vec<_>, _>>()?;
        for rel in &rels {
            self.table_locks
                .lock_table(rel.rel, TableLockMode::AccessExclusive, client_id);
        }

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_analyze_stmt_in_transaction_with_search_path(
            client_id,
            analyze_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        for rel in &rels {
            self.table_locks.unlock_table(rel.rel, client_id);
        }
        result
    }

    pub(crate) fn execute_analyze_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        analyze_stmt: &AnalyzeStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: cid,
            timed: false,
            outer_rows: Vec::new(),
        };
        let analyzed = collect_analyze_stats(&analyze_stmt.targets, &catalog, &mut ctx)?;
        drop(ctx);

        let write_ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
        };
        let mut store = self.catalog.write();
        for result in analyzed {
            let effect = store
                .set_relation_analyze_stats_mvcc(
                    result.relation_oid,
                    result.relpages,
                    result.reltuples,
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
            let effect = store
                .replace_relation_statistics_mvcc(
                    result.relation_oid,
                    result.statistics,
                    &write_ctx,
                )
                .map_err(ExecError::from)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &comment_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON TABLE",
                actual: "temporary table".into(),
            }));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_add_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE ADD COLUMN",
                actual: "temporary table".into(),
            }));
        }
        reject_relation_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            "ALTER TABLE on relation without dependent views",
        )?;
        let column = validate_alter_table_add_column(&relation.desc, &alter_stmt.column)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let effect = self
            .catalog
            .write()
            .alter_table_add_column_mvcc(relation.relation_oid, column, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
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
        let view_name =
            self.normalize_create_view_stmt_with_search_path(create_stmt, configured_search_path)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let plan = build_plan(&create_stmt.query, &catalog)?;
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

    fn resolve_simple_btree_build_options(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation: &crate::backend::parser::BoundRelation,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> Result<(u32, u32, CatalogIndexBuildOptions), ExecError> {
        let access_method = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self, client_id, txn_ctx, "btree",
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            })
        })?;
        if !access_method.amname.eq_ignore_ascii_case("btree") {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            }));
        }

        let type_rows =
            crate::backend::utils::cache::syscache::ensure_type_rows(self, client_id, txn_ctx);
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
        for column in columns {
            let bound_column = relation
                .desc
                .columns
                .iter()
                .find(|desc| desc.name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.name.clone())))?;
            let type_oid = type_rows
                .iter()
                .find(|row| row.sql_type == bound_column.sql_type)
                .map(|row| row.oid)
                .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(column.name.clone())))?;
            let opclass = crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
                self,
                client_id,
                txn_ctx,
                access_method.oid,
                type_oid,
            )
            .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(column.name.clone())))?;
            indclass.push(opclass.oid);
            indcollation.push(0);
            let mut option = 0i16;
            if column.descending {
                option |= 0x0001;
            }
            if column.nulls_first.unwrap_or(false) {
                option |= 0x0002;
            }
            indoption.push(option);
        }

        Ok((
            access_method.oid,
            access_method.amhandler,
            CatalogIndexBuildOptions {
                am_oid: access_method.oid,
                indclass,
                indcollation,
                indoption,
            },
        ))
    }

    fn build_simple_btree_index_in_transaction(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        index_name: &str,
        columns: &[crate::backend::parser::IndexColumnDef],
        unique: bool,
        primary: bool,
        xid: TransactionId,
        cid: CommandId,
        access_method_oid: u32,
        access_method_handler: u32,
        build_options: &CatalogIndexBuildOptions,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::catalog::CatalogEntry, ExecError> {
        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let (index_entry, effect) = catalog_guard
            .create_index_for_relation_mvcc_with_options(
                index_name.to_string(),
                relation.relation_oid,
                unique,
                primary,
                columns,
                build_options,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        drop(catalog_guard);

        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        let snapshot = self
            .txns
            .read()
            .snapshot_for_command(xid, cid)
            .map_err(|_| ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index build snapshot",
                actual: "snapshot creation failed".into(),
            }))?;
        let index_meta =
            index_entry
                .index_meta
                .clone()
                .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index metadata",
                    actual: "missing index metadata".into(),
                }))?;
        let build_ctx = crate::include::access::amapi::IndexBuildContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            client_id,
            snapshot,
            heap_relation: relation.rel,
            heap_desc: relation.desc.clone(),
            index_relation: index_entry.rel,
            index_name: index_name.to_string(),
            index_desc: index_entry.desc.clone(),
            index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry {
                indrelid: index_meta.indrelid,
                indnatts: index_meta.indkey.len() as i16,
                indnkeyatts: index_meta.indkey.len() as i16,
                indisunique: index_meta.indisunique,
                indnullsnotdistinct: false,
                indisprimary: index_meta.indisprimary,
                indisexclusion: false,
                indimmediate: false,
                indisclustered: false,
                indisvalid: index_meta.indisvalid,
                indcheckxmin: false,
                indisready: index_meta.indisready,
                indislive: index_meta.indislive,
                indisreplident: false,
                am_oid: access_method_oid,
                am_handler_oid: Some(access_method_handler),
                indkey: index_meta.indkey.clone(),
                indclass: index_meta.indclass.clone(),
                indcollation: index_meta.indcollation.clone(),
                indoption: index_meta.indoption.clone(),
                opfamily_oids: Vec::new(),
                opcintype_oids: Vec::new(),
                indexprs: index_meta.indexprs.clone(),
                indpred: index_meta.indpred.clone(),
            },
            maintenance_work_mem_kb,
        };
        crate::backend::access::index::indexam::index_build_stub(&build_ctx, access_method_oid)
            .map_err(|err| match err {
                CatalogError::UniqueViolation(constraint) => {
                    ExecError::UniqueViolation { constraint }
                }
                _ => ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index access method build",
                    actual: "index build failed".into(),
                }),
            })?;

        let mut catalog_guard = self.catalog.write();
        let readiness_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: None,
        };
        let ready_effect = catalog_guard
            .set_index_ready_valid_mvcc(index_entry.relation_oid, true, true, &readiness_ctx)
            .map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index catalog readiness update",
                    actual: "index readiness update failed".into(),
                })
            })?;
        drop(catalog_guard);

        self.apply_catalog_mutation_effect_immediate(&ready_effect)?;
        catalog_effects.push(ready_effect);
        Ok(index_entry)
    }

    fn choose_index_backed_constraint_name(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        namespace_oid: u32,
        table_name: &str,
        columns: &[String],
        primary: bool,
    ) -> Result<String, ExecError> {
        let base = if primary {
            format!("{table_name}_pkey")
        } else {
            format!("{table_name}_{}_key", columns.join("_"))
        };
        let snapshot = self
            .txns
            .read()
            .snapshot_for_command(xid, cid)
            .map_err(|_| ExecError::Parse(ParseError::UnexpectedToken {
                expected: "constraint name lookup snapshot",
                actual: "snapshot creation failed".into(),
            }))?;
        let catalog = self.catalog.read();
        let txns = self.txns.read();
        let existing = crate::backend::catalog::loader::load_visible_class_rows(
            catalog.base_dir(),
            &self.pool,
            &txns,
            &snapshot,
            client_id,
        )
        .map_err(map_catalog_error)?
        .into_iter()
        .filter(|row| row.relnamespace == namespace_oid)
        .map(|row| row.relname.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
        if !existing.contains(&base.to_ascii_lowercase()) {
            return Ok(base);
        }
        for suffix in 1.. {
            let candidate = format!("{base}{suffix}");
            if !existing.contains(&candidate.to_ascii_lowercase()) {
                return Ok(candidate);
            }
        }
        unreachable!("numeric suffix search should always find a free index name")
    }

    pub(crate) fn execute_create_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_index_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            maintenance_work_mem_kb,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let entry = catalog
            .lookup_any_relation(&create_stmt.table_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(
                    create_stmt.table_name.clone(),
                ))
            })?;

        if entry.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for CREATE INDEX",
                actual: "temporary table".into(),
            }));
        }
        if entry.relkind != 'r' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: create_stmt.table_name.clone(),
                expected: "table",
            }));
        }
        if create_stmt
            .using_method
            .as_deref()
            .is_some_and(|method| !method.eq_ignore_ascii_case("btree"))
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            }));
        }
        if !create_stmt.include_columns.is_empty()
            || !create_stmt.options.is_empty()
            || create_stmt.predicate.is_some()
            || create_stmt
                .columns
                .iter()
                .any(|column| column.descending || column.nulls_first.is_some())
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "simple btree column index",
                actual: "unsupported CREATE INDEX feature".into(),
            }));
        }
        let (access_method_oid, access_method_handler, build_options) =
            self.resolve_simple_btree_build_options(
                client_id,
                Some((xid, cid)),
                &entry,
                &create_stmt.columns,
            )?;
        self.build_simple_btree_index_in_transaction(
            client_id,
            &entry,
            &create_stmt.index_name,
            &create_stmt.columns,
            create_stmt.unique,
            false,
            xid,
            cid,
            access_method_oid,
            access_method_handler,
            &build_options,
            maintenance_work_mem_kb,
            catalog_effects,
        )?;
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
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .table_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        for rel in &rels {
            self.table_locks
                .lock_table(*rel, TableLockMode::AccessExclusive, client_id);
        }

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
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .view_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        for rel in &rels {
            self.table_locks
                .lock_table(*rel, TableLockMode::AccessExclusive, client_id);
        }

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
            };
            match self.catalog.write().drop_view_by_oid_mvcc(relation_oid, &ctx) {
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
        let (table_name, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(create_stmt, configured_search_path)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let plan = build_plan(&create_stmt.query, &catalog)?;
        let mut rels = std::collections::BTreeSet::new();
        collect_rels_from_plan(&plan, &mut rels);

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
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
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
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

    pub fn execute(&self, client_id: ClientId, sql: &str) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path(client_id, sql, None)
    }

    pub(crate) fn execute_with_search_path(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let stmt = self.plan_cache.get_statement(sql)?;
        self.execute_statement_with_search_path(client_id, stmt, configured_search_path)
    }

    pub(crate) fn execute_statement_with_search_path(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::commands::tablecmds::{
            execute_delete_with_waiter, execute_insert, execute_truncate_table,
            execute_update_with_waiter, execute_vacuum,
        };

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Analyze(ref analyze_stmt) => {
                self.execute_analyze_stmt_with_search_path(
                    client_id,
                    analyze_stmt,
                    configured_search_path,
                )
            }
            Statement::CreateIndex(ref create_stmt) => {
                self.execute_create_index_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    65_536,
                )
            }
            Statement::AlterTableAddColumn(ref alter_stmt) => self
                .execute_alter_table_add_column_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::Show(_)
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CopyFrom(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY handled by session layer",
                actual: "COPY".into(),
            })),
            Statement::CommentOnTable(ref comment_stmt) => self
                .execute_comment_on_table_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let visible_catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let (plan_or_stmt, rels) = {
                    let mut rels = std::collections::BTreeSet::new();
                    match &stmt {
                        Statement::Select(select) => {
                            let plan =
                                crate::backend::parser::build_plan(select, &visible_catalog)?;
                            collect_rels_from_plan(&plan, &mut rels);
                        }
                        Statement::Values(_) => {}
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                let plan =
                                    crate::backend::parser::build_plan(select, &visible_catalog)?;
                                collect_rels_from_plan(&plan, &mut rels);
                            }
                        }
                        _ => unreachable!(),
                    }
                    (stmt, rels.into_iter().collect::<Vec<_>>())
                };

                lock_relations(&self.table_locks, client_id, &rels);

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_readonly_statement(plan_or_stmt, &visible_catalog, &mut ctx);
                drop(ctx);

                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_insert(insert_stmt, &catalog)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_insert(bound, &mut ctx, xid, 0);
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_update(update_stmt, &catalog)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_update_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    0,
                    Some((&self.txns, &self.txn_waiter)),
                );
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_delete_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter)),
                );
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }
            Statement::CreateTable(ref create_stmt) => {
                self.execute_create_table_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                )
            }
            Statement::CreateView(ref create_stmt) => self.execute_create_view_stmt_with_search_path(
                client_id,
                create_stmt,
                configured_search_path,
            ),
            Statement::CreateTableAs(ref create_stmt) => {
                self.execute_create_table_as_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    None,
                    0,
                    configured_search_path,
                )
            }
            Statement::DropTable(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_table_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result =
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
                guard.disarm();
                result
            }
            Statement::DropView(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let result = self.execute_drop_view_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
                guard.disarm();
                result
            }
            Statement::TruncateTable(ref truncate_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let rels = truncate_stmt
                    .table_names
                    .iter()
                    .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                    .collect::<Vec<_>>();
                for rel in &rels {
                    self.table_locks
                        .lock_table(*rel, TableLockMode::AccessExclusive, client_id);
                }

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_truncate_table(
                    truncate_stmt.clone(),
                    &catalog,
                    &mut ctx,
                    INVALID_TRANSACTION_ID,
                );
                drop(ctx);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }
            Statement::Vacuum(ref vacuum_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                execute_vacuum(vacuum_stmt.clone(), &catalog)
            }
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub fn execute_streaming(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<SelectGuard<'_>, ExecError> {
        self.execute_streaming_with_search_path(client_id, select_stmt, txn_ctx, None)
    }

    pub(crate) fn execute_streaming_with_search_path(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
    ) -> Result<SelectGuard<'_>, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::executor::executor_start;

        let (plan, rels) = {
            let visible_catalog =
                self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
            let plan = build_plan(select_stmt, &visible_catalog)?;
            let mut rels = std::collections::BTreeSet::new();
            collect_rels_from_plan(&plan, &mut rels);
            (plan, rels.into_iter().collect::<Vec<_>>())
        };

        lock_relations(&self.table_locks, client_id, &rels);

        let (snapshot, command_id) = match txn_ctx {
            Some((xid, cid)) => (self.txns.read().snapshot_for_command(xid, cid)?, cid),
            None => (self.txns.read().snapshot(INVALID_TRANSACTION_ID)?, 0),
        };
        let columns = plan.columns();
        let column_names = plan.column_names();
        let state = executor_start(plan);
        let ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: command_id,
            outer_rows: Vec::new(),
            timed: false,
        };

        Ok(SelectGuard {
            state,
            ctx,
            columns,
            column_names,
            rels,
            table_locks: &self.table_locks,
            client_id,
        })
    }
}
