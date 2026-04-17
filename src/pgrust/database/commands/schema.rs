use super::super::*;
use crate::backend::commands::schemacmds::{
    CreateSchemaResolution, resolve_create_schema_stmt,
};
use crate::include::catalog::CURRENT_DATABASE_OID;

fn current_database_owner_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
) -> Result<u32, ExecError> {
    db.backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .database_rows()
        .into_iter()
        .find(|row| row.oid == CURRENT_DATABASE_OID)
        .map(|row| row.datdba)
        .ok_or_else(|| ExecError::DetailedError {
            message: "current database does not exist".into(),
            detail: None,
            hint: None,
            sqlstate: "3D000",
        })
}

impl Database {
    pub(crate) fn execute_create_schema_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateSchemaStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_schema_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let namespace_rows = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?
            .namespace_rows();
        let database_owner_oid = current_database_owner_oid(self, client_id, Some((xid, cid)))?;
        let has_database_create_privilege =
            self.user_has_database_create_privilege(&auth, &auth_catalog);
        let resolved = resolve_create_schema_stmt(
            stmt,
            &auth,
            &auth_catalog,
            database_owner_oid,
            has_database_create_privilege,
            &namespace_rows,
        )?;
        let CreateSchemaResolution::Create(resolved) = resolved else {
            return Ok(StatementResult::AffectedRows(0));
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .create_namespace_mvcc(0, &resolved.schema_name, resolved.owner_oid, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
