use super::super::*;
use crate::backend::executor::execute_planned_stmt;

impl Database {
    pub(crate) fn execute_truncate_table_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::TruncateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut rewrite_oids = Vec::new();

        for table_name in &stmt.table_names {
            let entry = match catalog.lookup_any_relation(table_name) {
                Some(entry) if entry.relkind == 'r' => entry,
                Some(_) => {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: table_name.clone(),
                        expected: "table",
                    }));
                }
                None => {
                    return Err(ExecError::Parse(ParseError::UnknownTable(
                        table_name.clone(),
                    )));
                }
            };
            if catalog.has_subclass(entry.relation_oid) {
                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                    "TRUNCATE on inherited parents is not supported yet".into(),
                )));
            }

            if !rewrite_oids.contains(&entry.relation_oid) {
                rewrite_oids.push(entry.relation_oid);
            }
            for index in catalog.index_relations_for_heap(entry.relation_oid) {
                if !rewrite_oids.contains(&index.relation_oid) {
                    rewrite_oids.push(index.relation_oid);
                }
            }
            if let Some(toast) = entry.toast {
                if !rewrite_oids.contains(&toast.relation_oid) {
                    rewrite_oids.push(toast.relation_oid);
                }
                for index in catalog.index_relations_for_heap(toast.relation_oid) {
                    if !rewrite_oids.contains(&index.relation_oid) {
                        rewrite_oids.push(index.relation_oid);
                    }
                }
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .rewrite_relation_storage_mvcc(&rewrite_oids, &ctx)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
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
        let interrupts = self.interrupt_state(client_id);

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Checkpoint(_) => {
                let auth = self.auth_state(client_id);
                let auth_catalog = self.auth_catalog(client_id, None).map_err(|err| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "authorization catalog",
                        actual: format!("{err:?}"),
                    })
                })?;
                if !auth.has_effective_membership(
                    crate::include::catalog::PG_CHECKPOINT_OID,
                    &auth_catalog,
                ) {
                    return Err(ExecError::DetailedError {
                        message: "permission denied to execute CHECKPOINT command".into(),
                        detail: Some(
                            "Only roles with privileges of the \"pg_checkpoint\" role may execute this command."
                                .into(),
                        ),
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                self.request_checkpoint(crate::backend::access::transam::CheckpointRequestFlags::sql())?;
                Ok(StatementResult::AffectedRows(0))
            }
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
            Statement::AlterTableOwner(ref alter_stmt) => self
                .execute_alter_table_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRename(ref rename_stmt) => self
                .execute_alter_table_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewOwner(ref alter_stmt) => self
                .execute_alter_view_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
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
            Statement::AlterTableAddConstraint(ref alter_stmt) => self
                .execute_alter_table_add_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDropConstraint(ref alter_stmt) => self
                .execute_alter_table_drop_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetNotNull(ref alter_stmt) => self
                .execute_alter_table_set_not_null_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDropNotNull(ref alter_stmt) => self
                .execute_alter_table_drop_not_null_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableValidateConstraint(ref alter_stmt) => self
                .execute_alter_table_validate_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::Show(_)
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CreateRole(ref create_stmt) => {
                self.execute_create_role_stmt(client_id, create_stmt, None)
            }
            Statement::CreateDatabase(ref create_stmt) => {
                self.execute_create_database_stmt(client_id, create_stmt)
            }
            Statement::AlterRole(ref alter_stmt) => {
                self.execute_alter_role_stmt(client_id, alter_stmt)
            }
            Statement::DropRole(ref drop_stmt) => self.execute_drop_role_stmt(client_id, drop_stmt),
            Statement::DropDatabase(ref drop_stmt) => {
                self.execute_drop_database_stmt(client_id, drop_stmt)
            }
            Statement::GrantObject(ref grant_stmt) => self
                .execute_grant_object_stmt_with_search_path(
                    client_id,
                    grant_stmt,
                    configured_search_path,
                ),
            Statement::RevokeObject(ref revoke_stmt) => self
                .execute_revoke_object_stmt_with_search_path(
                    client_id,
                    revoke_stmt,
                    configured_search_path,
                ),
            Statement::GrantRoleMembership(ref grant_stmt) => {
                self.execute_grant_role_membership_stmt(client_id, grant_stmt)
            }
            Statement::RevokeRoleMembership(ref revoke_stmt) => {
                self.execute_revoke_role_membership_stmt(client_id, revoke_stmt)
            }
            Statement::ReassignOwned(ref reassign_stmt) => {
                self.execute_reassign_owned_stmt(client_id, reassign_stmt)
            }
            Statement::CommentOnRole(ref comment_stmt) => {
                self.execute_comment_on_role_stmt(client_id, comment_stmt)
            }
            Statement::SetSessionAuthorization(ref set_stmt) => {
                self.execute_set_session_authorization_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetSessionAuthorization(ref reset_stmt) => {
                self.execute_reset_session_authorization_stmt(client_id, reset_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
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
            Statement::CreateSchema(ref create_stmt) => self
                .execute_create_schema_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTablespace(ref create_stmt) => {
                self.execute_create_tablespace_stmt(client_id, create_stmt)
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => self
                .execute_alter_schema_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateSequence(ref create_stmt) => self
                .execute_create_sequence_stmt_with_search_path(
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
                let (stmt, planned_select, rels) = {
                    let mut rels = std::collections::BTreeSet::new();
                    let mut planned_select = None;
                    match &stmt {
                        Statement::Select(select) => {
                            let planned_stmt =
                                crate::backend::parser::pg_plan_query(select, &visible_catalog)?;
                            collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                            planned_select = Some(planned_stmt);
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
                    (stmt, planned_select, rels.into_iter().collect::<Vec<_>>())
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
                    sequences: Some(self.sequences.clone()),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config:
                        crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    catalog: visible_catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                let result = match planned_select {
                    Some(planned_stmt) => execute_planned_stmt(planned_stmt, &mut ctx),
                    None => execute_readonly_statement(stmt, &visible_catalog, &mut ctx),
                };
                drop(ctx);

                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_insert(insert_stmt, &catalog)?;
                let lock_requests = insert_foreign_key_lock_requests(&bound);
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config:
                        crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                let result = execute_insert(bound, &catalog, &mut ctx, xid, 0);
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[], &[]);
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_update(update_stmt, &catalog)?;
                let lock_requests = update_foreign_key_lock_requests(&bound);
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config:
                        crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
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
                let result = self.finish_txn(client_id, xid, result, &[], &[], &[]);
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let lock_requests = delete_foreign_key_lock_requests(&bound);
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config:
                        crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                let result = execute_delete_with_waiter(
                    bound,
                    &catalog,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                );
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[], &[]);
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
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
            Statement::CreateType(ref create_stmt) => self
                .execute_create_type_stmt_with_search_path(
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
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
                guard.disarm();
                result
            }
            Statement::DropIndex(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let result = self.execute_drop_index_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::DropDomain(ref drop_stmt) => self.execute_drop_domain_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropType(ref drop_stmt) => self.execute_drop_type_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropSequence(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let mut sequence_effects = Vec::new();
                let result = self.execute_drop_sequence_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
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
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::DropSchema(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let result = self.execute_drop_schema_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::AlterSequence(ref alter_stmt) => self
                .execute_alter_sequence_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceOwner(ref alter_stmt) => self
                .execute_alter_sequence_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceRename(ref rename_stmt) => self
                .execute_alter_sequence_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::TruncateTable(ref truncate_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let relations = truncate_stmt
                    .table_names
                    .iter()
                    .filter_map(|name| catalog.lookup_any_relation(name))
                    .collect::<Vec<_>>();
                for relation in &relations {
                    reject_relation_with_referencing_foreign_keys(
                        &catalog,
                        relation.relation_oid,
                        "TRUNCATE on table without referencing foreign keys",
                    )?;
                }
                let rels = relations
                    .iter()
                    .map(|relation| relation.rel)
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
                    sequences: Some(self.sequences.clone()),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config:
                        crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
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
                let rels = vacuum_stmt
                    .targets
                    .iter()
                    .filter_map(|target| {
                        catalog
                            .lookup_relation(&target.table_name)
                            .map(|entry| entry.rel)
                    })
                    .collect::<Vec<_>>();
                lock_tables_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    TableLockMode::ShareUpdateExclusive,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    interrupts: Arc::clone(&interrupts),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    datetime_config:
                        crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: false,
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                };
                let result = execute_vacuum(vacuum_stmt.clone(), &catalog, &mut ctx);
                drop(ctx);
                unlock_relations(&self.table_locks, client_id, &rels);
                result
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
            sequences: Some(self.sequences.clone()),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts,
            snapshot,
            client_id,
            next_command_id: command_id,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: query_desc.planned_stmt.subplans,
            timed: false,
            allow_side_effects: true,
            catalog: visible_catalog_snapshot,
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
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
