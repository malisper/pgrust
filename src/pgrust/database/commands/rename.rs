use super::super::*;

fn normalize_rename_target_name(name: &str) -> Result<String, ExecError> {
    if name.contains('.') {
        return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
            name.to_string(),
        )));
    }
    Ok(name.to_ascii_lowercase())
}

impl Database {
    pub(crate) fn execute_alter_table_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &rename_stmt.table_name)?;
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
        let result = self.execute_alter_table_rename_stmt_in_transaction_with_search_path(
            client_id,
            rename_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &rename_stmt.table_name)?;
        let new_table_name = normalize_rename_target_name(&rename_stmt.new_table_name)?;

        if relation.relpersistence != 't' {
            reject_relation_with_dependent_views(
                self,
                client_id,
                Some((xid, cid)),
                relation.relation_oid,
                "ALTER TABLE RENAME on relation without dependent views",
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
                .rename_relation_mvcc(relation.relation_oid, &new_table_name, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        } else {
            let _ = self.rename_temp_relation_in_transaction(
                client_id,
                relation.relation_oid,
                &new_table_name,
                xid,
                cid,
                catalog_effects,
                temp_effects,
            )?;
        }

        Ok(StatementResult::AffectedRows(0))
    }
}
