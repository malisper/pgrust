use super::super::*;
use crate::pgrust::database::ddl::lookup_heap_relation_for_alter_table;

fn action_cid(base_cid: CommandId, action_index: usize) -> CommandId {
    // Reserve wide CID gaps so later subcommands can observe earlier catalog
    // mutations even when an individual helper uses a handful of internal CIDs.
    base_cid.saturating_add((action_index as u32).saturating_mul(1024))
}

fn make_add_column_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    column: crate::backend::parser::ColumnDef,
) -> crate::backend::parser::AlterTableAddColumnStatement {
    crate::backend::parser::AlterTableAddColumnStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        column,
    }
}

fn make_add_constraint_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    constraint: crate::backend::parser::TableConstraint,
) -> crate::backend::parser::AlterTableAddConstraintStatement {
    crate::backend::parser::AlterTableAddConstraintStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        constraint,
    }
}

fn make_drop_column_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    column_name: String,
) -> crate::backend::parser::AlterTableDropColumnStatement {
    crate::backend::parser::AlterTableDropColumnStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        column_name,
    }
}

fn make_drop_constraint_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    constraint_name: String,
) -> crate::backend::parser::AlterTableDropConstraintStatement {
    crate::backend::parser::AlterTableDropConstraintStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        constraint_name,
    }
}

fn make_alter_constraint_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    constraint_name: String,
    deferrable: Option<bool>,
    initially_deferred: Option<bool>,
) -> crate::backend::parser::AlterTableAlterConstraintStatement {
    crate::backend::parser::AlterTableAlterConstraintStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        constraint_name,
        deferrable,
        initially_deferred,
    }
}

fn make_rename_constraint_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    constraint_name: String,
    new_constraint_name: String,
) -> crate::backend::parser::AlterTableRenameConstraintStatement {
    crate::backend::parser::AlterTableRenameConstraintStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        constraint_name,
        new_constraint_name,
    }
}

fn make_alter_column_type_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    column_name: String,
    ty: crate::backend::parser::RawTypeName,
    using_expr: Option<crate::backend::parser::SqlExpr>,
) -> crate::backend::parser::AlterTableAlterColumnTypeStatement {
    crate::backend::parser::AlterTableAlterColumnTypeStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        column_name,
        ty,
        using_expr,
    }
}

fn make_set_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    options: Vec<crate::backend::parser::RelOption>,
) -> crate::backend::parser::AlterTableSetStatement {
    crate::backend::parser::AlterTableSetStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        options,
    }
}

fn make_set_not_null_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    column_name: String,
) -> crate::backend::parser::AlterTableSetNotNullStatement {
    crate::backend::parser::AlterTableSetNotNullStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        column_name,
    }
}

fn make_drop_not_null_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    column_name: String,
) -> crate::backend::parser::AlterTableDropNotNullStatement {
    crate::backend::parser::AlterTableDropNotNullStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        column_name,
    }
}

fn make_validate_constraint_stmt(
    stmt: &crate::backend::parser::AlterTableStatement,
    constraint_name: String,
) -> crate::backend::parser::AlterTableValidateConstraintStatement {
    crate::backend::parser::AlterTableValidateConstraintStatement {
        if_exists: stmt.if_exists,
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        constraint_name,
    }
}

impl Database {
    pub(crate) fn execute_alter_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableStatement,
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
        let result = self.execute_alter_table_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            relation.rel,
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

    pub(crate) fn execute_alter_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableStatement,
        primary_rel: RelFileLocator,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);

        for (index, action) in alter_stmt.actions.iter().enumerate() {
            let action_cid = action_cid(cid, index);
            match action.clone() {
                crate::backend::parser::AlterTableAction::AddColumn(column) => {
                    let action_stmt = make_add_column_stmt(alter_stmt, column);
                    self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                        temp_effects,
                        sequence_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::AddConstraint(constraint) => {
                    let action_stmt = make_add_constraint_stmt(alter_stmt, constraint);
                    let catalog = self.lazy_catalog_lookup(
                        client_id,
                        Some((xid, action_cid)),
                        configured_search_path,
                    );
                    let relation = lookup_heap_relation_for_alter_table(
                        &catalog,
                        &alter_stmt.table_name,
                        alter_stmt.if_exists,
                    )?
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                    let lock_requests =
                        alter_table_add_constraint_lock_requests(&relation, &action_stmt, &catalog)?;
                    crate::backend::storage::lmgr::lock_table_requests_interruptible(
                        &self.table_locks,
                        client_id,
                        &lock_requests,
                        interrupts.as_ref(),
                    )?;
                    let locked_rels = table_lock_relations(&lock_requests)
                        .into_iter()
                        .filter(|rel| *rel != primary_rel)
                        .collect::<Vec<_>>();
                    let result =
                        self.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
                            client_id,
                            &action_stmt,
                            xid,
                            action_cid,
                            configured_search_path,
                            catalog_effects,
                        );
                    unlock_relations(&self.table_locks, client_id, &locked_rels);
                    result?;
                }
                crate::backend::parser::AlterTableAction::DropColumn { column_name } => {
                    let action_stmt = make_drop_column_stmt(alter_stmt, column_name);
                    self.execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::DropConstraint { constraint_name } => {
                    let action_stmt = make_drop_constraint_stmt(alter_stmt, constraint_name);
                    self.execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::AlterConstraint {
                    constraint_name,
                    deferrable,
                    initially_deferred,
                } => {
                    let action_stmt = make_alter_constraint_stmt(
                        alter_stmt,
                        constraint_name,
                        deferrable,
                        initially_deferred,
                    );
                    self.execute_alter_table_alter_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::RenameConstraint {
                    constraint_name,
                    new_constraint_name,
                } => {
                    let action_stmt =
                        make_rename_constraint_stmt(alter_stmt, constraint_name, new_constraint_name);
                    self.execute_alter_table_rename_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::AlterColumnType {
                    column_name,
                    ty,
                    using_expr,
                } => {
                    let action_stmt =
                        make_alter_column_type_stmt(alter_stmt, column_name, ty, using_expr);
                    self.execute_alter_table_alter_column_type_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::Set { options } => {
                    let _ = make_set_stmt(alter_stmt, options);
                }
                crate::backend::parser::AlterTableAction::SetNotNull { column_name } => {
                    let action_stmt = make_set_not_null_stmt(alter_stmt, column_name);
                    self.execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::DropNotNull { column_name } => {
                    let action_stmt = make_drop_not_null_stmt(alter_stmt, column_name);
                    self.execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
                        client_id,
                        &action_stmt,
                        xid,
                        action_cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                crate::backend::parser::AlterTableAction::ValidateConstraint { constraint_name } => {
                    let action_stmt = make_validate_constraint_stmt(alter_stmt, constraint_name);
                    let catalog = self.lazy_catalog_lookup(
                        client_id,
                        Some((xid, action_cid)),
                        configured_search_path,
                    );
                    let relation = lookup_heap_relation_for_alter_table(
                        &catalog,
                        &alter_stmt.table_name,
                        alter_stmt.if_exists,
                    )?
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::TableDoesNotExist(
                            alter_stmt.table_name.clone(),
                        ))
                    })?;
                    let lock_requests = alter_table_validate_constraint_lock_requests(
                        &relation,
                        &action_stmt,
                        &catalog,
                    )?;
                    crate::backend::storage::lmgr::lock_table_requests_interruptible(
                        &self.table_locks,
                        client_id,
                        &lock_requests,
                        interrupts.as_ref(),
                    )?;
                    let locked_rels = table_lock_relations(&lock_requests)
                        .into_iter()
                        .filter(|rel| *rel != primary_rel)
                        .collect::<Vec<_>>();
                    let result = self
                        .execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
                            client_id,
                            &action_stmt,
                            xid,
                            action_cid,
                            configured_search_path,
                            catalog_effects,
                        );
                    unlock_relations(&self.table_locks, client_id, &locked_rels);
                    result?;
                }
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }
}
