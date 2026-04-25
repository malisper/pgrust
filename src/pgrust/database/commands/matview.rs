use super::super::*;
use crate::backend::access::heap::heapam::HeapError;
use crate::backend::commands::tablecmds::{execute_insert_values, reinitialize_index_relation};
use crate::backend::rewrite::load_view_return_select;
use crate::backend::storage::smgr::{ForkNumber, StorageManager};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::nodes::parsenodes::{
    DropMaterializedViewStatement, RefreshMaterializedViewStatement, TableAsObjectType,
};
use crate::include::nodes::primnodes::QueryColumn;
use std::collections::HashMap;

impl Database {
    pub(crate) fn execute_create_materialized_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if create_stmt.object_type != TableAsObjectType::MaterializedView {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "CREATE MATERIALIZED VIEW statement",
                actual: "CREATE TABLE AS".into(),
            }));
        }
        if create_stmt.persistence != TablePersistence::Permanent {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "temporary materialized views are not supported".into(),
            )));
        }

        let interrupts = self.interrupt_state(client_id);
        let (matview_name, namespace_oid, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        if persistence != TablePersistence::Permanent {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "temporary materialized views are not supported".into(),
            )));
        }

        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        if catalog
            .lookup_any_relation(&matview_name)
            .is_some_and(|relation| relation.namespace_oid == namespace_oid)
        {
            if create_stmt.if_not_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(
                matview_name,
            )));
        }

        let query_sql = create_stmt.query_sql.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "stored materialized view SELECT",
                actual: "missing SELECT text".into(),
            })
        })?;
        let planned_stmt = crate::backend::parser::pg_plan_query(&create_stmt.query, &catalog)?;
        let columns = planned_stmt.columns();
        let column_names = planned_stmt.column_names();
        validate_matview_column_names(create_stmt, columns.len())?;
        let desc = matview_relation_desc(create_stmt, &columns, &column_names);

        let rows = if create_stmt.skip_data {
            Vec::new()
        } else {
            execute_matview_select_rows(
                self,
                client_id,
                xid,
                cid,
                Arc::clone(&interrupts),
                &catalog,
                Statement::Select(create_stmt.query.clone()),
                false,
            )?
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let (created, create_effect) = self
            .catalog
            .write()
            .create_materialized_view_mvcc_with_options(
                matview_name.clone(),
                desc.clone(),
                namespace_oid,
                self.database_oid,
                'p',
                crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
                self.auth_state(client_id).current_user_oid(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&create_effect)?;
        catalog_effects.push(create_effect);
        let (toast, toast_index) = toast_bindings_from_create_result(&created);

        let mut referenced_relation_oids = std::collections::BTreeSet::new();
        collect_direct_relation_oids_from_select(
            &create_stmt.query,
            &catalog,
            &mut Vec::new(),
            &mut referenced_relation_oids,
        );
        let rule_ctx = CatalogWriteContext {
            cid: cid.saturating_add(1),
            ..ctx
        };
        let rule_effect = self
            .catalog
            .write()
            .create_rule_mvcc_with_owner_dependency(
                created.entry.relation_oid,
                "_RETURN",
                '1',
                true,
                String::new(),
                query_sql,
                &referenced_relation_oids.into_iter().collect::<Vec<_>>(),
                crate::backend::catalog::store::RuleOwnerDependency::Internal,
                &rule_ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(rule_effect);

        if !rows.is_empty() {
            let mut insert_ctx = self.matview_executor_context(
                client_id,
                xid,
                cid,
                Arc::clone(&interrupts),
                catalog.materialize_visible_catalog(),
                true,
            )?;
            execute_insert_values(
                &matview_name,
                created.entry.relation_oid,
                created.entry.rel,
                toast,
                toast_index.as_ref(),
                &desc,
                &crate::backend::parser::BoundRelationConstraints::default(),
                &[],
                &[],
                &rows,
                &mut insert_ctx,
                xid,
                cid,
            )?;
        }

        if create_stmt.skip_data {
            let populated_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(2),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .set_matview_populated_mvcc(created.entry.relation_oid, false, &populated_ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }

        Ok(StatementResult::AffectedRows(rows.len()))
    }

    pub(crate) fn execute_refresh_materialized_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        refresh_stmt: &RefreshMaterializedViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if refresh_stmt.concurrently {
            return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported".into(),
            )));
        }

        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match catalog.lookup_any_relation(&refresh_stmt.relation_name) {
            Some(relation) if relation.relkind == 'm' => relation,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: refresh_stmt.relation_name.clone(),
                    expected: "materialized view",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    refresh_stmt.relation_name.clone(),
                )));
            }
        };
        ensure_relation_owner(self, client_id, &relation, &refresh_stmt.relation_name)?;
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &[relation.rel],
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let result = (|| {
            let select = load_view_return_select(relation.relation_oid, None, &catalog, &[])?;
            let rows = if refresh_stmt.skip_data {
                Vec::new()
            } else {
                execute_matview_select_rows(
                    self,
                    client_id,
                    xid,
                    cid,
                    Arc::clone(&interrupts),
                    &catalog,
                    Statement::Select(select),
                    false,
                )?
            };
            truncate_matview_storage(self, client_id, xid, cid, &relation, &catalog)?;
            if !rows.is_empty() {
                insert_matview_rows(
                    self,
                    client_id,
                    xid,
                    cid,
                    Arc::clone(&interrupts),
                    &relation,
                    &catalog,
                    &rows,
                )?;
            }
            let populated_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(1),
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .set_matview_populated_mvcc(
                    relation.relation_oid,
                    !refresh_stmt.skip_data,
                    &populated_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            // REFRESH uses one command id for heap replacement and the next
            // command id for the relispopulated catalog update. Pad the effect
            // list so the session's following statement sees that catalog row.
            catalog_effects.push(CatalogMutationEffect::default());
            Ok(StatementResult::AffectedRows(rows.len()))
        })();

        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_refresh_materialized_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        refresh_stmt: &RefreshMaterializedViewStatement,
        xid: Option<TransactionId>,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if let Some(xid) = xid {
            let mut catalog_effects = Vec::new();
            return self.execute_refresh_materialized_view_stmt_in_transaction_with_search_path(
                client_id,
                refresh_stmt,
                xid,
                cid,
                configured_search_path,
                &mut catalog_effects,
            );
        }

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_refresh_materialized_view_stmt_in_transaction_with_search_path(
            client_id,
            refresh_stmt,
            xid,
            cid,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_materialized_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropMaterializedViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_drop_relation_stmt_in_transaction_with_search_path(
            client_id,
            &drop_stmt.view_names,
            drop_stmt.if_exists,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            None,
            'm',
            "materialized view",
        )
    }

    pub(crate) fn execute_drop_materialized_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropMaterializedViewStatement,
        xid: Option<TransactionId>,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if let Some(xid) = xid {
            let mut catalog_effects = Vec::new();
            return self.execute_drop_materialized_view_stmt_in_transaction_with_search_path(
                client_id,
                drop_stmt,
                xid,
                cid,
                configured_search_path,
                &mut catalog_effects,
            );
        }

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_materialized_view_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            cid,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn matview_executor_context(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        interrupts: Arc<InterruptState>,
        catalog: Option<VisibleCatalog>,
        allow_side_effects: bool,
    ) -> Result<ExecutorContext, ExecError> {
        Ok(ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: std::collections::HashMap::new(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects,
            pending_async_notifications: Vec::new(),
            catalog,
            compiled_functions: HashMap::new(),
            cte_tables: HashMap::new(),
            cte_producers: HashMap::new(),
            recursive_worktables: HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        })
    }
}

fn validate_matview_column_names(
    create_stmt: &CreateTableAsStatement,
    query_column_count: usize,
) -> Result<(), ExecError> {
    if create_stmt.column_names.len() > query_column_count {
        return Err(ExecError::Parse(ParseError::DetailedError {
            message: "too many column names were specified".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        }));
    }
    Ok(())
}

fn matview_relation_desc(
    create_stmt: &CreateTableAsStatement,
    columns: &[QueryColumn],
    column_names: &[String],
) -> crate::backend::executor::RelationDesc {
    crate::backend::executor::RelationDesc {
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
    }
}

fn execute_matview_select_rows(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    catalog: &dyn CatalogLookup,
    stmt: Statement,
    allow_side_effects: bool,
) -> Result<Vec<Vec<Value>>, ExecError> {
    let mut ctx = db.matview_executor_context(
        client_id,
        xid,
        cid,
        interrupts,
        catalog.materialize_visible_catalog(),
        allow_side_effects,
    )?;
    let StatementResult::Query { rows, .. } = execute_readonly_statement(stmt, catalog, &mut ctx)?
    else {
        unreachable!("materialized view query should return rows");
    };
    Ok(rows)
}

fn truncate_matview_storage(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    let interrupts = db.interrupt_state(client_id);
    let mut ctx = db.matview_executor_context(
        client_id,
        xid,
        cid,
        interrupts,
        catalog.materialize_visible_catalog(),
        true,
    )?;
    let indexes = catalog.index_relations_for_heap(relation.relation_oid);
    let _ = ctx.pool.invalidate_relation(relation.rel);
    ctx.pool
        .with_storage_mut(|s| {
            s.smgr.truncate(relation.rel, ForkNumber::Main, 0)?;
            if s.smgr.exists(relation.rel, ForkNumber::VisibilityMap) {
                s.smgr
                    .truncate(relation.rel, ForkNumber::VisibilityMap, 0)?;
            }
            Ok(())
        })
        .map_err(HeapError::Storage)?;
    for index in indexes
        .iter()
        .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
    {
        reinitialize_index_relation(index, &mut ctx, xid)?;
    }
    ctx.session_stats
        .write()
        .note_relation_truncate(relation.relation_oid);
    Ok(())
}

fn insert_matview_rows(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
    rows: &[Vec<Value>],
) -> Result<usize, ExecError> {
    let indexes = catalog.index_relations_for_heap(relation.relation_oid);
    let toast_index = relation.toast.and_then(|toast| {
        catalog
            .index_relations_for_heap(toast.relation_oid)
            .into_iter()
            .next()
    });
    let mut ctx = db.matview_executor_context(
        client_id,
        xid,
        cid,
        interrupts,
        catalog.materialize_visible_catalog(),
        true,
    )?;
    let relation_name = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    execute_insert_values(
        &relation_name,
        relation.relation_oid,
        relation.rel,
        relation.toast,
        toast_index.as_ref(),
        &relation.desc,
        &crate::backend::parser::BoundRelationConstraints::default(),
        &[],
        &indexes,
        rows,
        &mut ctx,
        xid,
        cid,
    )
}
