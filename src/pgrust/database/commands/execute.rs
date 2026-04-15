use super::super::*;

impl Database {
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
        let interrupts = self.interrupt_state(client_id);

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Analyze(ref analyze_stmt) => self.execute_analyze_stmt_with_search_path(
                client_id,
                analyze_stmt,
                configured_search_path,
            ),
            Statement::CreateIndex(ref create_stmt) => self
                .execute_create_index_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    65_536,
                ),
            Statement::AlterTableRename(ref rename_stmt) => self
                .execute_alter_table_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRenameColumn(ref rename_stmt) => self
                .execute_alter_table_rename_column_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddColumn(ref alter_stmt) => self
                .execute_alter_table_add_column_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDropColumn(ref drop_stmt) => self
                .execute_alter_table_drop_column_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnType(ref alter_stmt) => self
                .execute_alter_table_alter_column_type_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::Show(_)
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::Unsupported(ref unsupported_stmt) => {
                Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                    "{}: {}",
                    unsupported_stmt.feature, unsupported_stmt.sql
                ))))
            }
            Statement::CopyFrom(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "COPY handled by session layer",
                actual: "COPY".into(),
            })),
            Statement::CreateFunction(ref create_stmt) => self
                .execute_create_function_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnTable(ref comment_stmt) => self
                .execute_comment_on_table_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnDomain(ref comment_stmt) => self
                .execute_comment_on_domain_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let visible_catalog =
                    self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let (plan_or_stmt, rels) = {
                    let mut rels = std::collections::BTreeSet::new();
                    match &stmt {
                        Statement::Select(select) => {
                            let planned_stmt =
                                crate::backend::parser::pg_plan_query(select, &visible_catalog)?;
                            collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                        }
                        Statement::Values(_) => {}
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                let planned_stmt = crate::backend::parser::pg_plan_query(
                                    select,
                                    &visible_catalog,
                                )?;
                                collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                            }
                        }
                        _ => unreachable!(),
                    }
                    (stmt, rels.into_iter().collect::<Vec<_>>())
                };

                lock_relations_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    catalog: visible_catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
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
                self.table_locks.lock_table_interruptible(
                    rel,
                    TableLockMode::RowExclusive,
                    client_id,
                    interrupts.as_ref(),
                )?;

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                };
                let result = execute_insert(bound, &catalog, &mut ctx, xid, 0);
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
                self.table_locks.lock_table_interruptible(
                    rel,
                    TableLockMode::RowExclusive,
                    client_id,
                    interrupts.as_ref(),
                )?;

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                };
                let result = execute_update_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    0,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
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
                self.table_locks.lock_table_interruptible(
                    rel,
                    TableLockMode::RowExclusive,
                    client_id,
                    interrupts.as_ref(),
                )?;

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                };
                let result = execute_delete_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                );
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }
            Statement::CreateTable(ref create_stmt) => self
                .execute_create_table_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateDomain(ref create_stmt) => self
                .execute_create_domain_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateView(ref create_stmt) => self
                .execute_create_view_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTableAs(ref create_stmt) => self
                .execute_create_table_as_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    None,
                    0,
                    configured_search_path,
                ),
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
            Statement::DropDomain(ref drop_stmt) => self
                .execute_drop_domain_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
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
                lock_tables_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    TableLockMode::AccessExclusive,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
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

        let visible_catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let visible_catalog_snapshot = visible_catalog.materialize_visible_catalog();
        let (query_desc, rels) = {
            let query_desc = crate::backend::executor::create_query_desc(
                crate::backend::parser::pg_plan_query(select_stmt, &visible_catalog)?,
                None,
            );
            let mut rels = std::collections::BTreeSet::new();
            collect_rels_from_planned_stmt(&query_desc.planned_stmt, &mut rels);
            (query_desc, rels.into_iter().collect::<Vec<_>>())
        };

        let interrupts = self.interrupt_state(client_id);
        lock_relations_interruptible(&self.table_locks, client_id, &rels, interrupts.as_ref())?;

        let (snapshot, command_id) = match txn_ctx {
            Some((xid, cid)) => (self.txns.read().snapshot_for_command(xid, cid)?, cid),
            None => (self.txns.read().snapshot(INVALID_TRANSACTION_ID)?, 0),
        };
        let columns = query_desc.columns();
        let column_names = query_desc.column_names();
        let state = executor_start(query_desc.planned_stmt.plan_tree);
        let ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            interrupts,
            snapshot,
            client_id,
            next_command_id: command_id,
            outer_rows: Vec::new(),
            subplans: query_desc.planned_stmt.subplans,
            timed: false,
            catalog: visible_catalog_snapshot,
            compiled_functions: std::collections::HashMap::new(),
        };

        Ok(SelectGuard {
            state,
            ctx,
            columns,
            column_names,
            rels,
            table_locks: &self.table_locks,
            client_id,
            interrupt_guard: None,
        })
    }
}
