use super::super::*;
use crate::backend::executor::execute_planned_stmt;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;

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
        let mut truncated_relation_oids = Vec::new();

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
            if !truncated_relation_oids.contains(&entry.relation_oid) {
                truncated_relation_oids.push(entry.relation_oid);
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
        {
            let stats_state = self.session_stats_state(client_id);
            let mut stats_state = stats_state.write();
            for relation_oid in truncated_relation_oids {
                stats_state.note_relation_truncate(relation_oid);
            }
        }
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub fn execute(&self, client_id: ClientId, sql: &str) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path_and_datetime_config(
            client_id,
            sql,
            None,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_with_search_path(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path_and_datetime_config(
            client_id,
            sql,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<StatementResult, ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            let stmt = self.plan_cache.get_statement(sql)?;
            self.execute_statement_with_search_path_and_datetime_config(
                client_id,
                stmt,
                configured_search_path,
                datetime_config,
            )
        })
    }

    pub(crate) fn execute_statement_with_search_path(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_and_datetime_config(
            client_id,
            stmt,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_statement_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<StatementResult, ExecError> {
        let statement_lock_scope_id = Some(self.allocate_statement_lock_scope_id());
        let stats_state = self.session_stats_state(client_id);
        stats_state.write().begin_top_level_xact();
        let advisory_locks = std::sync::Arc::clone(&self.advisory_locks);
        let result = self.execute_statement_with_search_path_inner(
            client_id,
            stmt,
            statement_lock_scope_id,
            configured_search_path,
            datetime_config,
        );
        if let Some(scope_id) = statement_lock_scope_id {
            advisory_locks.unlock_all_statement(client_id, scope_id);
        }
        match &result {
            Ok(_) => stats_state.write().commit_top_level_xact(&self.stats),
            Err(_) => stats_state.write().rollback_top_level_xact(),
        }
        result
    }

    fn finish_txn_with_async_notifications(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
        catalog_effects: &[CatalogMutationEffect],
        temp_effects: &[TempMutationEffect],
        sequence_effects: &[SequenceMutationEffect],
        pending_async_notifications: Vec<PendingNotification>,
    ) -> Result<StatementResult, ExecError> {
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            catalog_effects,
            temp_effects,
            sequence_effects,
        );
        if result.is_ok() {
            self.async_notify_runtime
                .publish(client_id, &pending_async_notifications);
        }
        result
    }

    fn execute_notify_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::NotifyStatement,
    ) -> Result<StatementResult, ExecError> {
        let mut pending_async_notifications = Vec::new();
        queue_pending_notification(
            &mut pending_async_notifications,
            &stmt.channel,
            stmt.payload.as_deref().unwrap_or(""),
        )?;
        self.async_notify_runtime
            .publish(client_id, &pending_async_notifications);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_listen_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::ListenStatement,
    ) -> StatementResult {
        self.async_notify_runtime.listen(client_id, &stmt.channel);
        StatementResult::AffectedRows(0)
    }

    fn execute_unlisten_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::UnlistenStatement,
    ) -> StatementResult {
        self.async_notify_runtime
            .unlisten(client_id, stmt.channel.as_deref());
        StatementResult::AffectedRows(0)
    }

    fn execute_statement_with_search_path_inner(
        &self,
        client_id: ClientId,
        stmt: Statement,
        statement_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
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
                self.request_checkpoint(
                    crate::backend::access::transam::CheckpointRequestFlags::sql(),
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Notify(ref notify_stmt) => self.execute_notify_stmt(client_id, notify_stmt),
            Statement::Listen(ref listen_stmt) => {
                Ok(self.execute_listen_stmt(client_id, listen_stmt))
            }
            Statement::Unlisten(ref unlisten_stmt) => {
                Ok(self.execute_unlisten_stmt(client_id, unlisten_stmt))
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
            Statement::CreateStatistics(ref create_stmt) => self
                .execute_create_statistics_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
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
            Statement::AlterIndexRename(ref rename_stmt) => self
                .execute_alter_index_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => self
                .execute_alter_index_alter_column_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
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
            Statement::AlterTableAlterColumnDefault(ref alter_stmt) => self
                .execute_alter_table_alter_column_default_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnCompression(ref alter_stmt) => self
                .execute_alter_table_alter_column_compression_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnStorage(ref alter_stmt) => self
                .execute_alter_table_alter_column_storage_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnOptions(ref alter_stmt) => self
                .execute_alter_table_alter_column_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => self
                .execute_alter_table_alter_column_statistics_stmt_with_search_path(
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
            Statement::AlterTableAlterConstraint(ref alter_stmt) => self
                .execute_alter_table_alter_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRenameConstraint(ref alter_stmt) => self
                .execute_alter_table_rename_constraint_stmt_with_search_path(
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
            Statement::AlterTableInherit(ref alter_stmt) => self
                .execute_alter_table_inherit_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableNoInherit(ref alter_stmt) => self
                .execute_alter_table_no_inherit_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetRowSecurity(ref alter_stmt) => self
                .execute_alter_table_set_row_security_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterPolicy(ref alter_stmt) => self
                .execute_alter_policy_stmt_with_search_path(
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
            Statement::DropOwned(ref drop_stmt) => {
                self.execute_drop_owned_stmt(client_id, drop_stmt)
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
            Statement::SetRole(ref set_stmt) => {
                self.execute_set_role_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetRole(ref reset_stmt) => {
                self.execute_reset_role_stmt(client_id, reset_stmt)?;
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
            Statement::CreateOperatorClass(ref create_stmt) => self
                .execute_create_operator_class_stmt_with_search_path(
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
                self.execute_create_tablespace_stmt(client_id, create_stmt, false)
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => self
                .execute_alter_schema_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterPublication(ref alter_stmt) => self
                .execute_alter_publication_stmt_with_search_path(
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
            Statement::Merge(ref merge_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = crate::backend::parser::plan_merge(merge_stmt, &catalog)?;
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                };
                let result = crate::backend::commands::tablecmds::execute_merge(
                    bound, &catalog, &mut ctx, xid, 0,
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                result
            }
            Statement::CommentOnTable(ref comment_stmt) => self
                .execute_comment_on_table_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnConstraint(ref comment_stmt) => self
                .execute_comment_on_constraint_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnRule(ref comment_stmt) => self
                .execute_comment_on_rule_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnTrigger(ref comment_stmt) => self
                .execute_comment_on_trigger_stmt_with_search_path(
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
            Statement::CommentOnConversion(ref comment_stmt) => self
                .execute_comment_on_conversion_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnForeignDataWrapper(ref comment_stmt) => {
                self.execute_comment_on_foreign_data_wrapper_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnPublication(ref comment_stmt) => self
                .execute_comment_on_publication_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CreateForeignDataWrapper(ref create_stmt) => self
                .execute_create_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterForeignDataWrapper(ref alter_stmt) => self
                .execute_alter_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterForeignDataWrapperOwner(ref alter_stmt) => {
                self.execute_alter_foreign_data_wrapper_owner_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignDataWrapperRename(ref alter_stmt) => {
                self.execute_alter_foreign_data_wrapper_rename_stmt(client_id, alter_stmt)
            }
            Statement::DropForeignDataWrapper(ref drop_stmt) => {
                self.execute_drop_foreign_data_wrapper_stmt(client_id, drop_stmt)
            }
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

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: visible_catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                };
                let result = match planned_select {
                    Some(planned_stmt) => execute_planned_stmt(planned_stmt, &mut ctx),
                    None => execute_readonly_statement(stmt, &visible_catalog, &mut ctx),
                };
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();

                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_insert(insert_stmt, &catalog)?;
                let prepared = super::rules::prepare_bound_insert_for_execution(bound, &catalog)?;
                let lock_requests = merge_table_lock_requests(
                    &insert_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                };
                let result = super::rules::execute_bound_insert_with_rules(
                    prepared.stmt,
                    &catalog,
                    &mut ctx,
                    xid,
                    0,
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_update(update_stmt, &catalog)?;
                let prepared = super::rules::prepare_bound_update_for_execution(bound, &catalog)?;
                let lock_requests = merge_table_lock_requests(
                    &update_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                };
                let result = super::rules::execute_bound_update_with_rules(
                    prepared.stmt,
                    &catalog,
                    &mut ctx,
                    xid,
                    0,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let prepared = super::rules::prepare_bound_delete_for_execution(bound, &catalog)?;
                let lock_requests = merge_table_lock_requests(
                    &delete_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                };
                let result = super::rules::execute_bound_delete_with_rules(
                    prepared.stmt,
                    &catalog,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
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
            Statement::CreateConversion(ref create_stmt) => self
                .execute_create_conversion_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreatePublication(ref create_stmt) => self
                .execute_create_publication_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTrigger(ref create_stmt) => self
                .execute_create_trigger_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreatePolicy(ref create_stmt) => self
                .execute_create_policy_stmt_with_search_path(
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
            Statement::CreateRule(ref create_stmt) => self
                .execute_create_rule_stmt_with_search_path(
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
            Statement::DropFunction(ref drop_stmt) => self
                .execute_drop_function_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropConversion(ref drop_stmt) => self
                .execute_drop_conversion_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropPublication(ref drop_stmt) => self
                .execute_drop_publication_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropTrigger(ref drop_stmt) => self
                .execute_drop_trigger_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropPolicy(ref drop_stmt) => self.execute_drop_policy_stmt_with_search_path(
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
            Statement::DropRule(ref drop_stmt) => self.execute_drop_rule_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
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
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
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
                    large_objects: Some(self.large_objects.clone()),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    statement_lock_scope_id,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    datetime_config: datetime_config.clone(),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: false,
                    pending_async_notifications: Vec::new(),
                    catalog: catalog.materialize_visible_catalog(),
                    compiled_functions: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
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
        self.execute_streaming_with_search_path_and_datetime_config(
            client_id,
            select_stmt,
            txn_ctx,
            None,
            None,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_search_path(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
    ) -> Result<SelectGuard<'_>, ExecError> {
        self.execute_streaming_with_search_path_and_datetime_config(
            client_id,
            select_stmt,
            txn_ctx,
            None,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
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
            large_objects: Some(self.large_objects.clone()),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: datetime_config.clone(),
            interrupts,
            stats: std::sync::Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            statement_lock_scope_id,
            next_command_id: command_id,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: query_desc.planned_stmt.subplans,
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog: visible_catalog_snapshot,
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
        };

        Ok(SelectGuard {
            state,
            ctx,
            columns,
            column_names,
            rels,
            table_locks: &self.table_locks,
            client_id,
            advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
            statement_lock_scope_id,
            interrupt_guard: None,
        })
    }
}
