use super::super::*;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    any_dependent_view_references_column, dependent_view_rewrites_for_relation,
    is_system_column_name, lookup_heap_relation_for_alter_table,
    reject_column_referenced_by_generated_columns, reject_column_with_trigger_dependencies,
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
        reject_column_referenced_by_generated_columns(
            &catalog,
            &relation.desc,
            column_index,
            "drop",
        )?;
        let dependent_views = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
        )?;
        if any_dependent_view_references_column(
            &dependent_views,
            relation.relation_oid,
            (column_index + 1) as i16,
        ) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER TABLE DROP COLUMN on column without dependent views",
                actual: format!(
                    "cannot ALTER TABLE DROP COLUMN on relation because a dependent view uses column {}",
                    relation.desc.columns[column_index].name
                ),
            }));
        }
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
        let target_attnum = (column_index + 1) as i16;
        let dependent_indexes = catalog
            .index_relations_for_heap(relation.relation_oid)
            .into_iter()
            .filter(|index| index.index_meta.indkey.contains(&target_attnum))
            .collect::<Vec<_>>();
        let mut next_cid = cid;
        self.drop_statistics_for_column_in_transaction(
            client_id,
            relation.relation_oid,
            target_attnum,
            xid,
            &mut next_cid,
            catalog_effects,
        )?;
        for index in dependent_indexes {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .drop_relation_entry_by_oid_mvcc(index.relation_oid, &ctx)
                .map_err(|err| match err {
                    CatalogError::UnknownTable(_) => ExecError::Parse(
                        ParseError::TableDoesNotExist(index.relation_oid.to_string()),
                    ),
                    other => ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "dependent index for DROP COLUMN",
                        actual: format!("{other:?}"),
                    }),
                })?
                .1;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: next_cid,
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
