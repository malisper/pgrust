use super::super::*;
use super::create::describe_select_query_without_planning;
use super::tablespace::resolve_relation_tablespace_oid;
use crate::backend::access::heap::heapam::HeapError;
use crate::backend::commands::tablecmds::{execute_insert_values, reinitialize_index_relation};
use crate::backend::parser::{BoundIndexRelation, BoundRelation};
use crate::backend::rewrite::load_view_return_select;
use crate::backend::storage::smgr::{ForkNumber, StorageManager};
use crate::include::nodes::parsenodes::{
    AlterMaterializedViewSetAccessMethodStatement, CreateTableAsQuery,
    DropMaterializedViewStatement, RefreshMaterializedViewStatement, TableAsObjectType,
};
use crate::include::nodes::primnodes::QueryColumn;
use std::collections::{HashMap, HashSet};

impl Database {
    pub(crate) fn execute_alter_materialized_view_set_access_method_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterMaterializedViewSetAccessMethodStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_materialized_view_set_access_method_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_materialized_view_set_access_method_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterMaterializedViewSetAccessMethodStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match catalog.lookup_any_relation(&alter_stmt.relation_name) {
            Some(relation) if relation.relkind == 'm' => relation,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: alter_stmt.relation_name.clone(),
                    expected: "materialized view",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    alter_stmt.relation_name.clone(),
                )));
            }
        };
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.relation_name)?;
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &[relation.rel],
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;
        let mut ctx = self.matview_executor_context(
            client_id,
            xid,
            cid,
            Arc::clone(&interrupts),
            Some(crate::backend::executor::executor_catalog(catalog.clone())),
            true,
        )?;
        let result = self
            .fire_table_rewrite_event_in_executor_context(
                &mut ctx,
                "ALTER MATERIALIZED VIEW",
                relation.relation_oid,
                8,
            )
            .map(|_| StatementResult::AffectedRows(0));
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_create_materialized_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        gucs: Option<&HashMap<String, String>>,
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
                crate::backend::utils::misc::notices::push_notice(format!(
                    "relation \"{}\" already exists, skipping",
                    relation_notice_name(&matview_name)
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "relation \"{}\" already exists",
                    relation_notice_name(&matview_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P07",
            });
        }

        let query_sql = create_stmt.query_sql.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "stored materialized view SELECT",
                actual: "missing SELECT text".into(),
            })
        })?;
        let select_query = match &create_stmt.query {
            CreateTableAsQuery::Select(query) => query,
            CreateTableAsQuery::Execute(execute) => {
                return Err(ExecError::Parse(ParseError::DetailedError {
                    message: format!("prepared statement \"{}\" does not exist", execute.name),
                    detail: None,
                    hint: None,
                    sqlstate: "26000",
                }));
            }
        };
        let (columns, column_names) = if create_stmt.skip_data {
            describe_select_query_without_planning(select_query, &catalog)?
        } else {
            let planned_stmt = crate::backend::parser::pg_plan_query(select_query, &catalog)?;
            (planned_stmt.columns(), planned_stmt.column_names())
        };
        validate_matview_column_names(create_stmt, columns.len())?;
        let desc = matview_relation_desc(create_stmt, &columns, &column_names);

        let (rows, create_cid) = if create_stmt.skip_data {
            (Vec::new(), cid)
        } else {
            let select_result = execute_matview_select_rows(
                self,
                client_id,
                xid,
                cid,
                Arc::clone(&interrupts),
                &catalog,
                Statement::Select(select_query.clone()),
                false,
            )?;
            let create_cid = select_result.next_command_id.max(cid);
            catalog_effects.extend(select_result.catalog_effects);
            (select_result.rows, create_cid)
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: create_cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let relation_tablespace_oid = resolve_relation_tablespace_oid(
            self,
            client_id,
            Some((xid, create_cid)),
            create_stmt.tablespace.as_deref(),
            gucs,
        )?;
        let (mut created, create_effect) = self
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
                !create_stmt.skip_data,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&create_effect)?;
        catalog_effects.push(create_effect);
        if relation_tablespace_oid != created.entry.rel.spc_oid {
            let set_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: create_cid.saturating_add(1),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .set_relation_tablespace_mvcc(
                    created.entry.relation_oid,
                    relation_tablespace_oid,
                    false,
                    &set_ctx,
                )
                .map_err(map_catalog_error)?;
            created.entry.rel = effect.created_rels.first().copied().unwrap_or_else(|| {
                let mut rel = created.entry.rel;
                rel.spc_oid = relation_tablespace_oid;
                rel
            });
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        let (toast, toast_index) = toast_bindings_from_create_result(&created);

        let mut referenced_relation_oids = std::collections::BTreeSet::new();
        collect_direct_relation_oids_from_select(
            select_query,
            &catalog,
            &mut Vec::new(),
            &mut referenced_relation_oids,
        );
        let rule_ctx = CatalogWriteContext {
            cid: create_cid.saturating_add(1),
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
                &[],
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
                Some(crate::backend::executor::executor_catalog(catalog.clone())),
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
                None,
                &mut insert_ctx,
                xid,
                create_cid,
            )?;
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
        validate_concurrent_matview_refresh(refresh_stmt, &relation, &catalog)?;
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &[relation.rel],
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let result = (|| {
            let select = load_view_return_select(relation.relation_oid, None, &catalog, &[])?;
            let (rows, refresh_cid) = if refresh_stmt.skip_data {
                (Vec::new(), cid)
            } else {
                let select_result = execute_matview_select_rows(
                    self,
                    client_id,
                    xid,
                    cid,
                    Arc::clone(&interrupts),
                    &catalog,
                    Statement::Select(select),
                    false,
                )?;
                let refresh_cid = select_result.next_command_id.max(cid);
                catalog_effects.extend(select_result.catalog_effects);
                (select_result.rows, refresh_cid)
            };
            let refresh_catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, refresh_cid)),
                configured_search_path,
            );
            let refresh_relation = refresh_catalog
                .relation_by_oid(relation.relation_oid)
                .unwrap_or_else(|| relation.clone());
            validate_concurrent_matview_unique_index_after_query(
                refresh_stmt,
                &refresh_relation,
                &refresh_catalog,
            )?;
            validate_refresh_matview_rows(
                refresh_stmt,
                &refresh_relation,
                &refresh_catalog,
                &rows,
            )?;
            truncate_matview_storage(
                self,
                client_id,
                xid,
                refresh_cid,
                &refresh_relation,
                &refresh_catalog,
            )?;
            if !rows.is_empty() {
                insert_matview_rows(
                    self,
                    client_id,
                    xid,
                    refresh_cid,
                    Arc::clone(&interrupts),
                    &refresh_relation,
                    &refresh_catalog,
                    &rows,
                )?;
            }
            let populated_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: refresh_cid.saturating_add(1),
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
            drop_stmt.cascade,
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

    pub(crate) fn matview_executor_context(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        interrupts: Arc<InterruptState>,
        catalog: Option<crate::backend::executor::ExecutorCatalog>,
        allow_side_effects: bool,
    ) -> Result<ExecutorContext, ExecError> {
        Ok(ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: super::maintenance_safe_gucs(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
            write_xid_override: None,
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
            random_state: crate::backend::executor::PgPrngState::shared(),
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            active_grouping_refs: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
            catalog,
            scalar_function_cache: std::collections::HashMap::new(),
            srf_rows_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: HashMap::new(),
            cte_producers: HashMap::new(),
            recursive_worktables: HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        })
    }
}

fn validate_concurrent_matview_refresh(
    refresh_stmt: &RefreshMaterializedViewStatement,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if !refresh_stmt.concurrently {
        return Ok(());
    }
    if !relation.relispopulated {
        return Err(ExecError::DetailedError {
            message: "CONCURRENTLY cannot be used when the materialized view is not populated"
                .into(),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    if refresh_stmt.skip_data {
        return Err(ExecError::DetailedError {
            message: "CONCURRENTLY and WITH NO DATA options cannot be used together".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    if catalog
        .index_relations_for_heap(relation.relation_oid)
        .iter()
        .any(is_usable_unique_index_for_concurrent_refresh)
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "cannot refresh materialized view \"{}\" concurrently",
            qualified_matview_name(catalog, relation)
        ),
        detail: None,
        hint: Some(
            "Create a unique index with no WHERE clause on one or more columns of the materialized view."
                .into(),
        ),
        sqlstate: "55000",
    })
}

fn is_usable_unique_index_for_concurrent_refresh(index: &BoundIndexRelation) -> bool {
    let meta = &index.index_meta;
    meta.indisunique
        && meta.indimmediate
        && meta.indisvalid
        && meta.indisready
        && meta.indislive
        && meta.indpred.is_none()
        && meta.indexprs.is_none()
        && !meta.indkey.is_empty()
        && meta.indkey.iter().all(|attnum| *attnum > 0)
}

fn validate_concurrent_matview_unique_index_after_query(
    refresh_stmt: &RefreshMaterializedViewStatement,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if !refresh_stmt.concurrently {
        return Ok(());
    }
    if catalog
        .index_relations_for_heap(relation.relation_oid)
        .iter()
        .any(is_usable_unique_index_for_concurrent_refresh)
    {
        return Ok(());
    }
    let relname = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    Err(ExecError::DetailedError {
        message: format!(
            "could not find suitable unique index on materialized view \"{}\"",
            relname
        ),
        detail: None,
        hint: None,
        sqlstate: "55000",
    })
}

fn validate_refresh_matview_rows(
    refresh_stmt: &RefreshMaterializedViewStatement,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
    rows: &[Vec<Value>],
) -> Result<(), ExecError> {
    if refresh_stmt.concurrently {
        if let Some(row) = first_duplicate_full_row_without_nulls(rows) {
            let relname = catalog
                .class_row_by_oid(relation.relation_oid)
                .map(|row| row.relname)
                .unwrap_or_else(|| relation.relation_oid.to_string());
            return Err(ExecError::DetailedError {
                message: format!(
                    "new data for materialized view \"{}\" contains duplicate rows without any null columns",
                    relname
                ),
                detail: Some(format!("Row: ({})", format_matview_row(row, false))),
                hint: None,
                sqlstate: "21000",
            });
        }
        return Ok(());
    }

    if let Some((index_name, columns, values)) =
        first_duplicate_unique_index_key(relation, catalog, rows)
    {
        return Err(ExecError::DetailedError {
            message: format!("could not create unique index \"{}\"", index_name),
            detail: Some(format!(
                "Key ({})=({}) is duplicated.",
                columns.join(", "),
                format_matview_row(&values, true)
            )),
            hint: None,
            sqlstate: "23505",
        });
    }
    Ok(())
}

fn first_duplicate_full_row_without_nulls(rows: &[Vec<Value>]) -> Option<&[Value]> {
    let mut seen = HashSet::new();
    for row in rows {
        if row.iter().any(|value| matches!(value, Value::Null)) {
            continue;
        }
        if !seen.insert(row.clone()) {
            return Some(row);
        }
    }
    None
}

fn first_duplicate_unique_index_key(
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
    rows: &[Vec<Value>],
) -> Option<(String, Vec<String>, Vec<Value>)> {
    for index in catalog.index_relations_for_heap(relation.relation_oid) {
        let meta = &index.index_meta;
        if !meta.indisunique
            || !meta.indisvalid
            || !meta.indisready
            || meta.indexprs.is_some()
            || meta.indpred.is_some()
        {
            continue;
        }
        let key_attnums = meta
            .indkey
            .iter()
            .take(usize::try_from(meta.indnkeyatts.max(0)).ok()?)
            .copied()
            .collect::<Vec<_>>();
        if key_attnums.is_empty() || key_attnums.iter().any(|attnum| *attnum <= 0) {
            continue;
        }
        let mut key_indexes = Vec::with_capacity(key_attnums.len());
        let mut key_columns = Vec::with_capacity(key_attnums.len());
        for attnum in key_attnums {
            let index = usize::try_from(attnum - 1).ok()?;
            let column = relation.desc.columns.get(index)?;
            key_indexes.push(index);
            key_columns.push(column.name.clone());
        }

        let mut seen = HashSet::new();
        for row in rows {
            let key = key_indexes
                .iter()
                .filter_map(|index| row.get(*index).cloned())
                .collect::<Vec<_>>();
            if key.len() != key_indexes.len() {
                continue;
            }
            if !meta.indnullsnotdistinct && key.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            if !seen.insert(key.clone()) {
                return Some((index.name, key_columns, key));
            }
        }
    }
    None
}

fn format_matview_row(values: &[Value], include_spaces: bool) -> String {
    let separator = if include_spaces { ", " } else { "," };
    values
        .iter()
        .map(format_matview_value)
        .collect::<Vec<_>>()
        .join(separator)
}

fn format_matview_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Xid8(value) => value.to_string(),
        Value::Money(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Numeric(value) => value.render(),
        Value::Text(value) => value.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Bool(true) => "t".into(),
        Value::Bool(false) => "f".into(),
        _ => format!("{value:?}"),
    }
}

fn qualified_matview_name(catalog: &dyn CatalogLookup, relation: &BoundRelation) -> String {
    let relname = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    let nspname = catalog
        .namespace_row_by_oid(relation.namespace_oid)
        .map(|row| row.nspname)
        .unwrap_or_else(|| "public".into());
    format!("{nspname}.{relname}")
}

fn relation_notice_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name).trim_matches('"')
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

struct MatviewSelectResult {
    rows: Vec<Vec<Value>>,
    catalog_effects: Vec<CatalogMutationEffect>,
    next_command_id: CommandId,
}

fn execute_matview_select_rows(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    catalog: &crate::backend::utils::cache::lsyscache::LazyCatalogLookup,
    stmt: Statement,
    allow_side_effects: bool,
) -> Result<MatviewSelectResult, ExecError> {
    let mut ctx = db.matview_executor_context(
        client_id,
        xid,
        cid,
        interrupts,
        Some(crate::backend::executor::executor_catalog(catalog.clone())),
        allow_side_effects,
    )?;
    let StatementResult::Query { rows, .. } = execute_readonly_statement(stmt, catalog, &mut ctx)?
    else {
        unreachable!("materialized view query should return rows");
    };
    Ok(MatviewSelectResult {
        rows,
        catalog_effects: ctx.catalog_effects,
        next_command_id: ctx.next_command_id,
    })
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
    let mut ctx = db.matview_executor_context(client_id, xid, cid, interrupts, None, true)?;
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
    let mut ctx = db.matview_executor_context(client_id, xid, cid, interrupts, None, true)?;
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
        None,
        &mut ctx,
        xid,
        cid,
    )
}
