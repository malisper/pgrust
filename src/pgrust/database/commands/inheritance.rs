use std::collections::BTreeMap;
use std::sync::Arc;

use super::super::*;
use crate::backend::storage::lmgr::{TableLockMode, lock_table_requests_interruptible};
use crate::include::nodes::parsenodes::AlterTableNoInheritStatement;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table, lookup_heap_relation_for_ddl,
};

fn add_lock_request(
    requests: &mut BTreeMap<RelFileLocator, TableLockMode>,
    rel: RelFileLocator,
    mode: TableLockMode,
) {
    requests
        .entry(rel)
        .and_modify(|existing| *existing = existing.strongest(mode))
        .or_insert(mode);
}

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

impl Database {
    pub(crate) fn execute_alter_table_no_inherit_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableNoInheritStatement,
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
        let parent = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.parent_name)?;

        let mut requests = BTreeMap::new();
        add_lock_request(&mut requests, relation.rel, TableLockMode::AccessExclusive);
        add_lock_request(&mut requests, parent.rel, TableLockMode::AccessShare);
        let requests = requests.into_iter().collect::<Vec<_>>();
        lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &requests,
            interrupts.as_ref(),
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_no_inherit_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for (rel, _) in requests {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_no_inherit_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableNoInheritStatement,
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
        let parent = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.parent_name)?;
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;

        if !catalog
            .inheritance_parents(relation.relation_oid)
            .into_iter()
            .any(|row| row.inhparent == parent.relation_oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "relation \"{}\" is not a parent of relation \"{}\"",
                    relation_name_for_oid(&catalog, parent.relation_oid),
                    relation_name_for_oid(&catalog, relation.relation_oid),
                ),
                detail: None,
                hint: None,
                sqlstate: "42P01",
            });
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let (new_child_entry, effect) = self
            .catalog
            .write()
            .drop_relation_inheritance_parent_mvcc(relation.relation_oid, parent.relation_oid, &ctx)
            .map_err(map_catalog_error)?;
        if relation.relpersistence == 't' {
            self.replace_temp_entry_desc(client_id, relation.relation_oid, new_child_entry.desc)?;
        }
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
