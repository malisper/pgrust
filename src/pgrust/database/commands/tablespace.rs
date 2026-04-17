use super::super::*;
use crate::include::nodes::parsenodes::CreateTablespaceStatement;

impl Database {
    pub(crate) fn execute_create_tablespace_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateTablespaceStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result =
            self.execute_create_tablespace_stmt_in_transaction(client_id, stmt, xid, 0, &mut catalog_effects);
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_tablespace_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &CreateTablespaceStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache
            .tablespace_rows()
            .into_iter()
            .any(|row| row.spcname.eq_ignore_ascii_case(&stmt.tablespace_name))
        {
            return Err(ExecError::DetailedError {
                message: format!("tablespace \"{}\" already exists", stmt.tablespace_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let owner_oid = self.auth_state(client_id).current_user_oid();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let (_oid, effect) = self
            .catalog
            .write()
            .create_tablespace_mvcc(&stmt.tablespace_name, owner_oid, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let _ = &stmt.location;
        Ok(StatementResult::AffectedRows(0))
    }
}
