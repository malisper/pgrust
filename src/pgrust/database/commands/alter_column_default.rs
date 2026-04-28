use super::super::*;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table,
    validate_alter_table_alter_column_default, validate_alter_table_alter_column_expression,
};

impl Database {
    pub(crate) fn execute_alter_table_alter_column_default_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnDefaultStatement,
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
        let result = self
            .execute_alter_table_alter_column_default_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
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

    pub(crate) fn execute_alter_table_alter_column_default_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnDefaultStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
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
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN DEFAULT",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let plan = validate_alter_table_alter_column_default(
            &catalog,
            &relation.desc,
            &alter_stmt.column_name,
            alter_stmt.default_expr.as_ref(),
            alter_stmt.default_expr_sql.as_deref(),
        )?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let default_expr_sql = plan.default_expr_sql.clone();
        let default_sequence_oid = plan.default_sequence_oid;
        let effect = self
            .catalog
            .write()
            .alter_table_set_column_default_mvcc(
                relation.relation_oid,
                &plan.column_name,
                default_expr_sql.clone(),
                default_sequence_oid,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        if relation.relpersistence == 't' {
            let mut temp_desc = relation.desc.clone();
            let column = temp_desc
                .columns
                .iter_mut()
                .find(|column| column.name.eq_ignore_ascii_case(&plan.column_name))
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownColumn(plan.column_name.clone()))
                })?;
            column.default_expr = default_expr_sql;
            column.default_sequence_oid = default_sequence_oid;
            if column.default_expr.is_none() {
                column.attrdef_oid = None;
                column.missing_default_value = None;
            }
            self.replace_temp_entry_desc(client_id, relation.relation_oid, temp_desc)?;
        }
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_alter_column_expression_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnExpressionStatement,
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
        let result = self
            .execute_alter_table_alter_column_expression_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
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

    pub(crate) fn execute_alter_table_alter_column_expression_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnExpressionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
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
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN EXPRESSION",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let plan = validate_alter_table_alter_column_expression(
            &catalog,
            relation.relation_oid,
            relation.namespace_oid,
            &relation.desc,
            &alter_stmt.column_name,
            &alter_stmt.action,
        )?;
        if plan.noop {
            return Ok(StatementResult::AffectedRows(0));
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
        let default_expr_sql = plan.default_expr_sql.clone();
        let generated = plan.generated;
        let effect = self
            .catalog
            .write()
            .alter_table_set_column_generation_mvcc(
                relation.relation_oid,
                &plan.column_name,
                default_expr_sql.clone(),
                generated,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        if relation.relpersistence == 't' {
            let mut temp_desc = relation.desc.clone();
            let column = temp_desc
                .columns
                .iter_mut()
                .find(|column| column.name.eq_ignore_ascii_case(&plan.column_name))
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownColumn(plan.column_name.clone()))
                })?;
            column.default_expr = default_expr_sql;
            column.default_sequence_oid = None;
            column.generated = generated;
            if column.default_expr.is_none() {
                column.attrdef_oid = None;
                column.missing_default_value = None;
            }
            self.replace_temp_entry_desc(client_id, relation.relation_oid, temp_desc)?;
        }
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
