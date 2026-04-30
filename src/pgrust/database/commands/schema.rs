use super::super::*;
use crate::backend::commands::schemacmds::{
    CreateSchemaResolution, resolve_create_schema_stmt, transform_create_schema_stmt_elements,
};

const DEFAULT_CREATE_SCHEMA_MAINTENANCE_WORK_MEM_KB: usize = 65_536;

fn current_database_owner_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
) -> Result<u32, ExecError> {
    db.backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .database_rows()
        .into_iter()
        .find(|row| row.oid == db.database_oid)
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
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_schema_stmt_with_search_path_and_maintenance_work_mem(
            client_id,
            stmt,
            configured_search_path,
            DEFAULT_CREATE_SCHEMA_MAINTENANCE_WORK_MEM_KB,
        )
    }

    pub(crate) fn execute_create_schema_stmt_with_search_path_and_maintenance_work_mem(
        &self,
        client_id: ClientId,
        stmt: &CreateSchemaStatement,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self
            .execute_create_schema_stmt_in_transaction_with_search_path_and_maintenance_work_mem(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                maintenance_work_mem_kb,
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
        result
    }

    pub(crate) fn execute_create_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_schema_stmt_in_transaction_with_search_path_and_maintenance_work_mem(
            client_id,
            stmt,
            xid,
            cid,
            configured_search_path,
            DEFAULT_CREATE_SCHEMA_MAINTENANCE_WORK_MEM_KB,
            catalog_effects,
            temp_effects,
            sequence_effects,
        )
    }

    pub(crate) fn execute_create_schema_stmt_in_transaction_with_search_path_and_maintenance_work_mem(
        &self,
        client_id: ClientId,
        stmt: &CreateSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
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
        let resolved = match resolved {
            CreateSchemaResolution::Create(resolved) => resolved,
            CreateSchemaResolution::SkipExisting(schema_name) => {
                crate::backend::utils::misc::notices::push_notice(format!(
                    "schema \"{schema_name}\" already exists, skipping"
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
        };
        let elements =
            transform_create_schema_stmt_elements(&stmt.elements, &resolved.schema_name)?;

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
        self.invalidate_backend_cache_state(client_id);

        if !elements.is_empty() {
            let mut schema_search_path = Vec::with_capacity(
                configured_search_path.map_or(1, |path| path.len().saturating_add(1)),
            );
            schema_search_path.push(resolved.schema_name.clone());
            if let Some(configured_search_path) = configured_search_path {
                schema_search_path.extend(
                    configured_search_path
                        .iter()
                        .filter(|schema| !schema.eq_ignore_ascii_case(&resolved.schema_name))
                        .cloned(),
                );
            }
            for (index, element) in elements.iter().enumerate() {
                let element_cid = cid.saturating_add(1 + index as u32 * 10);
                match element {
                    Statement::CreateSequence(create_stmt) => {
                        self.execute_create_sequence_stmt_in_transaction_with_search_path(
                            client_id,
                            create_stmt,
                            xid,
                            element_cid,
                            Some(&schema_search_path),
                            catalog_effects,
                            temp_effects,
                            sequence_effects,
                        )?;
                    }
                    Statement::CreateTable(create_stmt) => {
                        self.execute_create_table_stmt_in_transaction_with_search_path_and_gucs(
                            client_id,
                            create_stmt,
                            xid,
                            element_cid,
                            Some(&schema_search_path),
                            None,
                            catalog_effects,
                            temp_effects,
                            sequence_effects,
                        )?;
                    }
                    Statement::CreateView(create_stmt) => {
                        let mut create_stmt = create_stmt.clone();
                        if create_stmt.schema_name.is_none() {
                            create_stmt.schema_name = Some(resolved.schema_name.clone());
                        }
                        self.execute_create_view_stmt_in_transaction_with_search_path(
                            client_id,
                            &create_stmt,
                            xid,
                            element_cid,
                            Some(&schema_search_path),
                            catalog_effects,
                            temp_effects,
                        )?;
                    }
                    Statement::CreateIndex(create_stmt) => {
                        self.execute_create_index_stmt_in_transaction_with_search_path(
                            client_id,
                            create_stmt,
                            xid,
                            element_cid,
                            Some(&schema_search_path),
                            None,
                            maintenance_work_mem_kb,
                            catalog_effects,
                        )?;
                    }
                    Statement::CreateTrigger(create_stmt) => {
                        self.execute_create_trigger_stmt_in_transaction_with_search_path(
                            client_id,
                            create_stmt,
                            xid,
                            element_cid,
                            Some(&schema_search_path),
                            catalog_effects,
                        )?;
                    }
                    Statement::GrantObject(grant_stmt) => {
                        self.execute_grant_object_stmt_in_transaction_with_search_path(
                            client_id,
                            grant_stmt,
                            xid,
                            element_cid,
                            Some(&schema_search_path),
                            catalog_effects,
                        )?;
                    }
                    _ => {
                        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                            "CREATE SCHEMA elements other than CREATE SEQUENCE, CREATE TABLE, CREATE VIEW, CREATE INDEX, CREATE TRIGGER, or GRANT".into(),
                        )));
                    }
                }
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
