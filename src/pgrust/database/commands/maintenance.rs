use super::super::*;
use super::constraint::{find_constraint_row, validate_check_rows, validate_not_null_rows};
use super::create::{aggregate_signature_arg_oids, resolve_aggregate_proc_rows};
use crate::backend::access::heap::heapam::heap_update_with_waiter;
use crate::backend::commands::tablecmds::{collect_matching_rows_heap, maintain_indexes_for_row};
use crate::backend::executor::value_io::{coerce_assignment_value, tuple_from_values};
use crate::backend::executor::{ExecutorContext, RelationDesc};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::{PG_CATALOG_NAMESPACE_OID, relkind_is_analyzable};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    CommentOnAggregateStatement, CommentOnIndexStatement, MaintenanceTarget,
};
use crate::pgrust::database::ddl::{
    lookup_analyzable_relation_for_ddl, lookup_heap_relation_for_alter_table,
    lookup_heap_relation_for_ddl, lookup_index_relation_for_alter_index,
};
use std::collections::BTreeSet;

struct AddColumnTarget {
    relation: crate::backend::parser::BoundRelation,
    column: crate::backend::executor::ColumnDesc,
    new_desc: RelationDesc,
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn rewrite_heap_rows_for_added_serial_column(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    new_desc: &RelationDesc,
    indexes: &[crate::backend::parser::BoundIndexRelation],
    sequence_oid: u32,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let target_rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
    let new_column = new_desc
        .columns
        .last()
        .expect("serial add-column rewrite requires appended column");
    for (tid, mut values) in target_rows {
        ctx.check_for_interrupts()?;
        let next = db
            .sequences
            .allocate_value(sequence_oid, relation.relpersistence != 't')?;
        values.push(coerce_assignment_value(
            &Value::Int64(next),
            new_column.sql_type,
        )?);
        let replacement = tuple_from_values(new_desc, &values)?;
        let new_tid = heap_update_with_waiter(
            &*ctx.pool,
            ctx.client_id,
            relation.rel,
            &ctx.txns,
            xid,
            cid,
            tid,
            &replacement,
            None,
        )?;
        maintain_indexes_for_row(relation.rel, new_desc, indexes, &values, new_tid, ctx)?;
    }
    Ok(())
}

fn current_database_owner_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
) -> Result<u32, ExecError> {
    db.backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .database_rows()
        .into_iter()
        .find(|row| row.oid == db.database_oid)
        .map(|row| row.datdba)
        .ok_or_else(|| ExecError::DetailedError {
            message: "current database does not exist".into(),
            detail: None,
            hint: None,
            sqlstate: "3D000",
        })
}

fn collect_catalog_analyze_targets(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    analyze_stmt: &AnalyzeStatement,
) -> Result<Vec<MaintenanceTarget>, ExecError> {
    if !analyze_stmt.targets.is_empty() {
        return Ok(analyze_stmt.targets.clone());
    }

    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let is_superuser = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper);
    let database_owner_oid = current_database_owner_oid(db, client_id, txn_ctx)?;
    let class_rows = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .class_rows();

    let mut targets = Vec::new();
    for class in class_rows {
        if !relkind_is_analyzable(class.relkind) {
            continue;
        }
        if db.other_session_temp_namespace_oid(client_id, class.relnamespace) {
            continue;
        }
        if !is_superuser
            && auth.current_user_oid() != database_owner_oid
            && !auth.has_effective_membership(class.relowner, &auth_catalog)
        {
            continue;
        }
        let Some(table_name) = crate::backend::utils::cache::lsyscache::relation_display_name(
            db,
            client_id,
            txn_ctx,
            configured_search_path,
            class.oid,
        ) else {
            continue;
        };
        targets.push(MaintenanceTarget {
            table_name,
            columns: Vec::new(),
            only: false,
        });
    }
    Ok(targets)
}

fn relation_name_for_add_column_notice(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn collect_add_column_targets(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    base_column: &crate::backend::executor::ColumnDesc,
    only: bool,
) -> Result<Vec<AddColumnTarget>, ExecError> {
    let target_relation_oids = if only {
        vec![relation.relation_oid]
    } else {
        catalog.find_all_inheritors(relation.relation_oid)
    };
    let target_relation_oids = target_relation_oids.into_iter().collect::<BTreeSet<_>>();
    let mut targets = Vec::with_capacity(target_relation_oids.len());

    for relation_oid in &target_relation_oids {
        let target_relation = if *relation_oid == relation.relation_oid {
            relation.clone()
        } else {
            catalog
                .lookup_relation_by_oid(*relation_oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownTable(relation_oid.to_string()))
                })?
        };
        if target_relation.desc.columns.iter().any(|existing| {
            !existing.dropped && existing.name.eq_ignore_ascii_case(&base_column.name)
        }) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "new column name",
                actual: format!("column already exists: {}", base_column.name),
            }));
        }
        let direct_parent_count = if *relation_oid == relation.relation_oid {
            0
        } else {
            catalog
                .inheritance_parents(*relation_oid)
                .into_iter()
                .filter(|parent| target_relation_oids.contains(&parent.inhparent))
                .count()
        };
        let mut column = base_column.clone();
        if direct_parent_count > 0 {
            column.attinhcount = direct_parent_count as i16;
            column.attislocal = false;
            if direct_parent_count > 1 {
                push_notice(format!(
                    "merging definition of column \"{}\" for child \"{}\"",
                    column.name,
                    relation_name_for_add_column_notice(catalog, target_relation.relation_oid)
                ));
            }
        }
        let mut new_desc = target_relation.desc.clone();
        new_desc.columns.push(column.clone());
        targets.push(AddColumnTarget {
            relation: target_relation,
            column,
            new_desc,
        });
    }

    Ok(targets)
}

impl Database {
    pub(crate) fn effective_analyze_targets_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
        analyze_stmt: &AnalyzeStatement,
    ) -> Result<Vec<MaintenanceTarget>, ExecError> {
        collect_catalog_analyze_targets(
            self,
            client_id,
            txn_ctx,
            configured_search_path,
            analyze_stmt,
        )
    }

    pub(crate) fn execute_comment_on_domain_stmt_with_search_path(
        &self,
        _client_id: ClientId,
        comment_stmt: &CommentOnDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, _, _) = self
            .normalize_domain_name_for_create(&comment_stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&normalized) else {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                comment_stmt.domain_name.clone(),
            )));
        };
        domain.comment = comment_stmt.comment.clone();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_aggregate_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnAggregateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_aggregate_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnIndexStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation =
            lookup_index_relation_for_alter_index(&catalog, &comment_stmt.index_name, false)?
                .expect("index lookup without if_exists should return relation or error");
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_index_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_comment_on_aggregate_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnAggregateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let arg_oids = aggregate_signature_arg_oids(&catalog, &comment_stmt.signature)
            .map_err(ExecError::Parse)?;
        let schema_oid = match &comment_stmt.schema_name {
            Some(schema_name) => Some(
                self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?,
            ),
            None => None,
        };
        let matches = resolve_aggregate_proc_rows(
            &catalog,
            &comment_stmt.aggregate_name,
            schema_oid,
            &arg_oids,
        );
        let proc_row = match matches.as_slice() {
            [(row, _agg)] => row.clone(),
            [] => {
                return Err(ExecError::DetailedError {
                    message: format!("aggregate {} does not exist", comment_stmt.aggregate_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                });
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "aggregate name {} is ambiguous",
                        comment_stmt.aggregate_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42725",
                });
            }
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_proc_mvcc(proc_row.oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation =
            lookup_index_relation_for_alter_index(&catalog, &comment_stmt.index_name, false)?
                .expect("index lookup without if_exists should return relation or error");
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.index_name)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = match catalog.lookup_any_relation(&comment_stmt.table_name) {
            Some(entry) if entry.relkind == 'r' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: comment_stmt.table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(ExecError::DetailedError {
                    message: format!("relation \"{}\" does not exist", comment_stmt.table_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                });
            }
        };
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
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
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_comment_on_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &comment_stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_constraint_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
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
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
            &mut sequence_effects,
        );
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            &catalog_effects,
            &temp_effects,
            &sequence_effects,
        );
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
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let targets = self.effective_analyze_targets_with_search_path(
            client_id,
            None,
            configured_search_path,
            analyze_stmt,
        )?;
        let relation_names = targets
            .iter()
            .map(|target| target.table_name.clone())
            .collect::<Vec<_>>();
        let rels = relation_names
            .iter()
            .map(|name| lookup_analyzable_relation_for_ddl(&catalog, name))
            .collect::<Result<Vec<_>, _>>()?;
        let rel_locs = rels.iter().map(|rel| rel.rel).collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rel_locs,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_analyze_stmt_in_transaction_with_search_path(
            client_id,
            &targets,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for rel in rel_locs {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_analyze_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        targets: &[MaintenanceTarget],
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            statement_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            timed: false,
            allow_side_effects: false,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            pending_async_notifications: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
        };
        let analyzed = collect_analyze_stats(targets, &catalog, &mut ctx)?;
        drop(ctx);

        let write_ctx = CatalogWriteContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: Arc::clone(&interrupts),
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
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &comment_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON TABLE",
                actual: "temporary table".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.table_name)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &comment_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON CONSTRAINT",
                actual: "temporary table".into(),
            }));
        }
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for COMMENT ON CONSTRAINT",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &comment_stmt.table_name)?;
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        let row = find_constraint_row(&rows, &comment_stmt.constraint_name).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing table constraint",
                actual: format!(
                    "constraint \"{}\" for table \"{}\" does not exist",
                    comment_stmt.constraint_name,
                    relation_basename(&comment_stmt.table_name).to_ascii_lowercase()
                ),
            })
        })?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .comment_constraint_mvcc(row.oid, comment_stmt.comment.as_deref(), &ctx)
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
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        if relation.relpersistence != 't' {
            reject_inheritance_tree_ddl(
                &catalog,
                relation.relation_oid,
                "ALTER TABLE ADD COLUMN on inheritance tree members is not supported yet",
            )?;
        }
        reject_relation_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            "ALTER TABLE on relation without dependent views",
        )?;
        let table_name = relation_basename(&alter_stmt.table_name).to_string();
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let plan = validate_alter_table_add_column(
            &table_name,
            &relation.desc,
            &alter_stmt.column,
            &existing_constraints,
            &catalog,
        )?;
        let crate::pgrust::database::ddl::AlterTableAddColumnPlan {
            mut column,
            owned_sequence,
            not_null_action,
            check_actions,
        } = plan;
        if let Some(serial_column) = owned_sequence.as_ref() {
            let mut used_names = std::collections::BTreeSet::new();
            let created = self.create_owned_sequence_for_serial_column(
                client_id,
                &alter_stmt.table_name,
                relation.namespace_oid,
                if relation.relpersistence == 't' {
                    TablePersistence::Temporary
                } else {
                    TablePersistence::Permanent
                },
                serial_column,
                xid,
                cid,
                &mut used_names,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?;
            column.default_expr = Some(format_nextval_default_oid(
                created.sequence_oid,
                serial_column.sql_type,
            ));
            column.default_sequence_oid = Some(created.sequence_oid);
        }
        let targets = collect_add_column_targets(&catalog, &relation, &column, alter_stmt.only)?;
        let indexes = targets
            .iter()
            .map(|target| {
                (
                    target.relation.relation_oid,
                    catalog.index_relations_for_heap(target.relation.relation_oid),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        if let Some(sequence_oid) = column.default_sequence_oid {
            let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&self.pool),
                txns: self.txns.clone(),
                txn_waiter: Some(self.txn_waiter.clone()),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                async_notify_runtime: Some(self.async_notify_runtime.clone()),
                advisory_locks: Arc::clone(&self.advisory_locks),
                checkpoint_stats: self.checkpoint_stats_snapshot(),
                datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(
                ),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                snapshot,
                client_id,
                current_database_name: self.current_database_name(),
                session_user_oid: self.auth_state(client_id).session_user_oid(),
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                active_role_oid: self.auth_state(client_id).active_role_oid(),
                statement_lock_scope_id: None,
                next_command_id: cid,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
                timed: false,
                allow_side_effects: false,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                pending_async_notifications: Vec::new(),
                catalog: catalog.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
            };
            for target in &targets {
                rewrite_heap_rows_for_added_serial_column(
                    self,
                    &target.relation,
                    &target.new_desc,
                    indexes
                        .get(&target.relation.relation_oid)
                        .expect("indexes for add-column target"),
                    sequence_oid,
                    &mut ctx,
                    xid,
                    cid,
                )?;
            }
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        for target in &targets {
            let effect = self
                .catalog
                .write()
                .alter_table_add_column_mvcc(
                    target.relation.relation_oid,
                    target.column.clone(),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            if target.relation.relpersistence == 't' {
                self.replace_temp_entry_desc(
                    client_id,
                    target.relation.relation_oid,
                    target.new_desc.clone(),
                )?;
            }
        }
        for target in targets {
            let mut target_desc = target.new_desc.clone();
            let mut target_relation = target.relation.clone();
            target_relation.desc = target_desc.clone();
            let target_name =
                relation_name_for_add_column_notice(&catalog, target.relation.relation_oid);
            let new_column_index = target_desc
                .columns
                .len()
                .checked_sub(1)
                .expect("add-column target has appended column");

            if let Some(action) = not_null_action.as_ref() {
                if !action.not_valid {
                    validate_not_null_rows(
                        self,
                        &target_relation,
                        &target_name,
                        new_column_index,
                        &action.constraint_name,
                        &catalog,
                        client_id,
                        xid,
                        cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                }
                let set_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: cid
                        .saturating_add(1)
                        .saturating_add(catalog_effects.len() as u32),
                    client_id,
                    waiter: None,
                    interrupts: std::sync::Arc::clone(&interrupts),
                };
                let (constraint_oid, effect) = self
                    .catalog
                    .write()
                    .set_column_not_null_mvcc(
                        target.relation.relation_oid,
                        &action.column,
                        action.constraint_name.clone(),
                        !action.not_valid,
                        action.no_inherit,
                        false,
                        &set_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let column = target_desc
                    .columns
                    .get_mut(new_column_index)
                    .expect("new column present in target desc");
                column.storage.nullable = false;
                column.not_null_constraint_oid = Some(constraint_oid);
                column.not_null_constraint_name = Some(action.constraint_name.clone());
                column.not_null_constraint_validated = !action.not_valid;
                column.not_null_constraint_no_inherit = action.no_inherit;
                column.not_null_primary_key_owned = false;
                target_relation.desc = target_desc.clone();
            }

            for action in &check_actions {
                crate::backend::parser::bind_check_constraint_expr(
                    &action.expr_sql,
                    Some(&target_name),
                    &target_relation.desc,
                    &catalog,
                )
                .map_err(ExecError::Parse)?;
                if !action.not_valid {
                    validate_check_rows(
                        self,
                        &target_relation,
                        &target_name,
                        &action.constraint_name,
                        &action.expr_sql,
                        &catalog,
                        client_id,
                        xid,
                        cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                }
                let constraint_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: cid
                        .saturating_add(1)
                        .saturating_add(catalog_effects.len() as u32),
                    client_id,
                    waiter: None,
                    interrupts: std::sync::Arc::clone(&interrupts),
                };
                let effect = self
                    .catalog
                    .write()
                    .create_check_constraint_mvcc(
                        target.relation.relation_oid,
                        action.constraint_name.clone(),
                        !action.not_valid,
                        action.no_inherit,
                        action.expr_sql.clone(),
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }

            if target.relation.relpersistence == 't' {
                self.replace_temp_entry_desc(client_id, target.relation.relation_oid, target_desc)?;
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
