use super::super::*;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    is_system_column_name, lookup_heap_relation_for_alter_table,
    reject_column_with_trigger_dependencies,
};

impl Database {
    pub(crate) fn execute_alter_table_drop_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &AlterTableDropColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        self.table_locks
            .lock_table(relation.rel, TableLockMode::AccessExclusive, client_id);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
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

    pub(crate) fn execute_alter_table_drop_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &AlterTableDropColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE DROP COLUMN",
                actual: "system catalog".into(),
            }));
        }
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE DROP COLUMN",
                actual: "temporary table".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &drop_stmt.table_name)?;
        reject_inheritance_tree_ddl(
            &catalog,
            relation.relation_oid,
            "ALTER TABLE DROP COLUMN on inheritance tree members is not supported yet",
        )?;
        if is_system_column_name(&drop_stmt.column_name) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user column name for DROP COLUMN",
                actual: drop_stmt.column_name.clone(),
            }));
        }
        let column_index = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(&drop_stmt.column_name))
                    .then_some(index)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownColumn(drop_stmt.column_name.clone()))
            })?;
        reject_relation_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            "ALTER TABLE DROP COLUMN on relation without dependent views",
        )?;
        reject_column_with_foreign_key_dependencies(
            &catalog,
            relation.relation_oid,
            &relation.desc.columns[column_index].name,
            (column_index + 1) as i16,
            "ALTER TABLE DROP COLUMN on column without foreign key dependencies",
        )?;
        reject_column_with_trigger_dependencies(
            &catalog,
            relation.relation_oid,
            &relation.desc.columns[column_index].name,
            (column_index + 1) as i16,
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
            .alter_table_drop_column_mvcc(relation.relation_oid, &drop_stmt.column_name, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
