use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::role_management_error;
use crate::backend::parser::{
    AlterRelationOwnerStatement, AlterSchemaOwnerStatement, BoundRelation,
};
use crate::pgrust::database::ddl::relation_kind_name;

fn lookup_relation_for_owner_change(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
    expected_relkind: char,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(relation_name) {
        Some(entry) if entry.relkind == expected_relkind => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: relation_name.to_string(),
            expected: relation_kind_name(expected_relkind),
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            relation_name.to_string(),
        ))),
    }
}

impl Database {
    pub(crate) fn execute_alter_schema_owner_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterSchemaOwnerStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_schema_owner_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_owner_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationOwnerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_owner_stmt_with_search_path(
            client_id,
            alter_stmt,
            configured_search_path,
            'r',
            "ALTER TABLE OWNER TO",
        )
    }

    pub(crate) fn execute_alter_view_owner_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationOwnerStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_owner_stmt_with_search_path(
            client_id,
            alter_stmt,
            configured_search_path,
            'v',
            "ALTER VIEW OWNER TO",
        )
    }

    pub(crate) fn execute_alter_table_owner_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationOwnerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_owner_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            'r',
            "ALTER TABLE OWNER TO",
        )
    }

    pub(crate) fn execute_alter_view_owner_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationOwnerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_owner_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            'v',
            "ALTER VIEW OWNER TO",
        )
    }

    pub(crate) fn execute_alter_schema_owner_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterSchemaOwnerStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let schema = catcache
            .namespace_by_name(&alter_stmt.schema_name)
            .cloned()
            .filter(|row| !self.other_session_temp_namespace_oid(client_id, row.oid))
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{}\" does not exist", alter_stmt.schema_name),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        let auth_catalog = self.auth_catalog(client_id, None).map_err(|err| {
            ExecError::Parse(role_management_error(format!(
                "authorization catalog unavailable: {err:?}"
            )))
        })?;
        if !auth.has_effective_membership(schema.nspowner, &auth_catalog) {
            return Err(ExecError::DetailedError {
                message: format!("must be owner of schema {}", alter_stmt.schema_name),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
        let new_owner = find_role_by_name(auth_catalog.roles(), &alter_stmt.new_owner)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    alter_stmt.new_owner
                )))
            })?;
        ensure_can_set_role(self, client_id, new_owner.oid, &new_owner.rolname)?;

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
            .alter_namespace_owner_mvcc(schema.oid, new_owner.oid, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_alter_relation_owner_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationOwnerStatement,
        configured_search_path: Option<&[String]>,
        expected_relkind: char,
        clause: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_relation_for_owner_change(
            &catalog,
            &alter_stmt.relation_name,
            expected_relkind,
        )?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_relation_owner_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            expected_relkind,
            clause,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    fn execute_alter_relation_owner_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationOwnerStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        expected_relkind: char,
        clause: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_relation_for_owner_change(
            &catalog,
            &alter_stmt.relation_name,
            expected_relkind,
        )?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: clause,
                actual: "temporary relation".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.relation_name)?;

        let auth_catalog = self.auth_catalog(client_id, None).map_err(|err| {
            ExecError::Parse(role_management_error(format!(
                "authorization catalog unavailable: {err:?}"
            )))
        })?;
        let new_owner = find_role_by_name(auth_catalog.roles(), &alter_stmt.new_owner)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    alter_stmt.new_owner
                )))
            })?;
        ensure_can_set_role(self, client_id, new_owner.oid, &new_owner.rolname)?;

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
            .alter_relation_owner_mvcc(relation.relation_oid, new_owner.oid, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
