use super::super::*;
use crate::backend::access::heap::heapam::heap_update_with_waiter;
use crate::backend::commands::tablecmds::{collect_matching_rows_heap, maintain_indexes_for_row};
use crate::backend::executor::value_io::{coerce_assignment_value, tuple_from_values};
use crate::backend::executor::{ExecutorContext, RelationDesc};
use crate::include::catalog::relkind_is_analyzable;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::MaintenanceTarget;
use crate::pgrust::database::ddl::lookup_analyzable_relation_for_ddl;

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

    pub(crate) fn execute_comment_on_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
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

    pub(crate) fn execute_alter_table_add_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
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
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            client_id,
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            next_command_id: cid,
            timed: false,
            allow_side_effects: false,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
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
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE ADD COLUMN",
                actual: "temporary table".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        reject_inheritance_tree_ddl(
            &catalog,
            relation.relation_oid,
            "ALTER TABLE ADD COLUMN on inheritance tree members is not supported yet",
        )?;
        reject_relation_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            "ALTER TABLE on relation without dependent views",
        )?;
        let plan = validate_alter_table_add_column(&relation.desc, &alter_stmt.column, &catalog)?;
        let mut column = plan.column;
        if let Some(serial_column) = plan.owned_sequence.as_ref() {
            let mut used_names = std::collections::BTreeSet::new();
            let created = self.create_owned_sequence_for_serial_column(
                client_id,
                &alter_stmt.table_name,
                relation.namespace_oid,
                TablePersistence::Permanent,
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
        let mut new_desc = relation.desc.clone();
        new_desc.columns.push(column.clone());
        let indexes = catalog.index_relations_for_heap(relation.relation_oid);
        if let Some(sequence_oid) = column.default_sequence_oid {
            let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&self.pool),
                txns: self.txns.clone(),
                txn_waiter: Some(self.txn_waiter.clone()),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                checkpoint_stats: self.checkpoint_stats_snapshot(),
                datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(
                ),
                interrupts: Arc::clone(&interrupts),
                stats: Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                snapshot,
                client_id,
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                next_command_id: cid,
                timed: false,
                allow_side_effects: false,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                catalog: catalog.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
            };
            rewrite_heap_rows_for_added_serial_column(
                self,
                &relation,
                &new_desc,
                &indexes,
                sequence_oid,
                &mut ctx,
                xid,
                cid,
            )?;
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
            .alter_table_add_column_mvcc(relation.relation_oid, column, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
