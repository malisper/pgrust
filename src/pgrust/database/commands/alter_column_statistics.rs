use super::super::*;
use super::alter_table_work_queue::build_alter_table_work_queue;
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table,
    lookup_index_relation_for_alter_index, validate_alter_index_alter_column_statistics,
    validate_alter_table_alter_column_statistics,
};

impl Database {
    pub(crate) fn execute_alter_table_alter_column_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnStatisticsStatement,
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
            .execute_alter_table_alter_column_statistics_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_table_alter_column_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnStatisticsStatement,
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
                expected: "user table for ALTER TABLE ALTER COLUMN SET STATISTICS",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let work_queue = build_alter_table_work_queue(&catalog, &relation, alter_stmt.only)?;
        let mut warning = None;
        for item in work_queue {
            let (column_name, statistics_target) = validate_alter_table_alter_column_statistics(
                &item.relation.desc,
                &alter_stmt.column_name,
                alter_stmt.statistics_target,
            )?;
            warning = warning.or(statistics_target.warning);
            let effect = self
                .catalog
                .write()
                .alter_table_set_column_statistics_mvcc(
                    item.relation.relation_oid,
                    &column_name,
                    statistics_target.value,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        if let Some(warning) = warning {
            push_warning(warning);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_index_alter_column_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterIndexAlterColumnStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_index_relation_for_alter_index(
            &catalog,
            &alter_stmt.index_name,
            alter_stmt.if_exists,
        )?
        else {
            push_relation_missing_notice(&alter_stmt.index_name);
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
            .execute_alter_index_alter_column_statistics_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_index_alter_column_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterIndexAlterColumnStatisticsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_index_relation_for_alter_index(
            &catalog,
            &alter_stmt.index_name,
            alter_stmt.if_exists,
        )?
        else {
            push_relation_missing_notice(&alter_stmt.index_name);
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.index_name)?;
        let described = self
            .describe_relation_by_oid(client_id, Some((xid, cid)), relation.relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(alter_stmt.index_name.clone()))
            })?;
        let display_name = self
            .relation_display_name(
                client_id,
                Some((xid, cid)),
                configured_search_path,
                relation.relation_oid,
            )
            .unwrap_or_else(|| alter_stmt.index_name.clone());
        let (column_name, statistics_target) = validate_alter_index_alter_column_statistics(
            &described,
            &display_name,
            alter_stmt.column_number,
            alter_stmt.statistics_target,
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
        let effect = self
            .catalog
            .write()
            .alter_index_set_column_statistics_mvcc(
                relation.relation_oid,
                &column_name,
                statistics_target.value,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        if let Some(warning) = statistics_target.warning {
            push_warning(warning);
        }
        Ok(StatementResult::AffectedRows(0))
    }
}

fn push_relation_missing_notice(name: &str) {
    push_notice(format!(r#"relation "{name}" does not exist, skipping"#));
}
