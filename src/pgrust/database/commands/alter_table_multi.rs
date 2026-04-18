use super::super::*;
use crate::include::nodes::parsenodes::{AlterTableMultiAction, AlterTableMultiStatement};

fn alter_table_multi_action_lock_requests(
    catalog: &dyn CatalogLookup,
    action: &AlterTableMultiAction,
) -> Result<Vec<(RelFileLocator, TableLockMode)>, ExecError> {
    match action {
        AlterTableMultiAction::Set(_) => Ok(Vec::new()),
        AlterTableMultiAction::AddConstraint(stmt) => {
            let relation = catalog
                .lookup_any_relation(&stmt.table_name)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(stmt.table_name.clone()))
                })?;
            alter_table_add_constraint_lock_requests(&relation, stmt, catalog)
        }
        AlterTableMultiAction::ValidateConstraint(stmt) => {
            let relation = catalog
                .lookup_any_relation(&stmt.table_name)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(stmt.table_name.clone()))
                })?;
            alter_table_validate_constraint_lock_requests(&relation, stmt, catalog)
        }
        _ => {
            let table_name = action.table_name();
            let relation = catalog.lookup_any_relation(table_name).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(table_name.into()))
            })?;
            Ok(vec![(relation.rel, TableLockMode::AccessExclusive)])
        }
    }
}

impl Database {
    pub(crate) fn execute_alter_table_multi_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableMultiStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let mut held_locks = Vec::new();
        let result = self.execute_alter_table_multi_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
            &mut sequence_effects,
            &mut held_locks,
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
        for rel in held_locks {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_multi_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableMultiStatement,
        xid: TransactionId,
        starting_cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
        held_locks: &mut Vec<RelFileLocator>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut cid = starting_cid;
        for action in &alter_stmt.actions {
            let catalog =
                self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
            for (rel, mode) in alter_table_multi_action_lock_requests(&catalog, action)? {
                if held_locks.iter().any(|existing| *existing == rel) {
                    continue;
                }
                self.table_locks.lock_table_interruptible(
                    rel,
                    mode,
                    client_id,
                    interrupts.as_ref(),
                )?;
                held_locks.push(rel);
            }
            match action {
                AlterTableMultiAction::Set(_) => {}
                AlterTableMultiAction::AddColumn(stmt) => {
                    self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                        temp_effects,
                        sequence_effects,
                    )?;
                }
                AlterTableMultiAction::AddConstraint(stmt) => {
                    self.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                AlterTableMultiAction::DropColumn(stmt) => {
                    self.execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                AlterTableMultiAction::DropConstraint(stmt) => {
                    self.execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                AlterTableMultiAction::AlterColumnType(stmt) => {
                    self.execute_alter_table_alter_column_type_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                AlterTableMultiAction::SetNotNull(stmt) => {
                    self.execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                AlterTableMultiAction::DropNotNull(stmt) => {
                    self.execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                AlterTableMultiAction::ValidateConstraint(stmt) => {
                    self.execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
            }
            cid = cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
