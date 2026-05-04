use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterTableNotOfStatement, AlterTableOfStatement, BoundRelation, CatalogLookup,
};
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table, map_catalog_error,
};
use pgrust_commands::typed_table::TypedTableError;

fn typed_table_error_to_exec(error: TypedTableError) -> ExecError {
    match error {
        TypedTableError::Parse(error) => ExecError::Parse(error),
        TypedTableError::Detailed {
            message,
            detail,
            hint,
            sqlstate,
        } => ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
    }
}

impl Database {
    pub(crate) fn execute_alter_table_of_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableOfStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) =
            lookup_heap_relation_for_alter_table(&catalog, &stmt.table_name, stmt.if_exists)?
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
        let result = self.execute_alter_table_of_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
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

    pub(crate) fn execute_alter_table_of_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableOfStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) =
            lookup_heap_relation_for_alter_table(&catalog, &stmt.table_name, stmt.if_exists)?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        reject_alter_table_of_target(&catalog, &relation, "ALTER TABLE OF")?;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        let (type_row, type_relation) =
            resolve_standalone_composite_type(&catalog, &stmt.type_name)?;
        validate_typed_table_compatibility(&relation, &type_relation)?;

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
            .alter_relation_of_type_mvcc(relation.relation_oid, type_row.oid, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_not_of_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableNotOfStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) =
            lookup_heap_relation_for_alter_table(&catalog, &stmt.table_name, stmt.if_exists)?
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
        let result = self.execute_alter_table_not_of_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
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

    pub(crate) fn execute_alter_table_not_of_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableNotOfStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) =
            lookup_heap_relation_for_alter_table(&catalog, &stmt.table_name, stmt.if_exists)?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        if relation.of_type_oid == 0 {
            return Err(ExecError::DetailedError {
                message: format!("table \"{}\" is not a typed table", stmt.table_name),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        if relation.relpersistence == 't' {
            return Err(ExecError::DetailedError {
                message: "ALTER TABLE NOT OF is not supported for temporary tables".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
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
        let effect = self
            .catalog
            .write()
            .alter_relation_of_type_mvcc(relation.relation_oid, 0, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

pub(crate) fn resolve_standalone_composite_type(
    catalog: &dyn CatalogLookup,
    type_name: &str,
) -> Result<(crate::include::catalog::PgTypeRow, BoundRelation), ExecError> {
    pgrust_commands::typed_table::resolve_standalone_composite_type(catalog, type_name)
        .map_err(typed_table_error_to_exec)
}

pub(crate) fn reject_typed_table_ddl(
    relation: &BoundRelation,
    operation: &str,
) -> Result<(), ExecError> {
    pgrust_commands::typed_table::reject_typed_table_ddl(relation, operation)
        .map_err(typed_table_error_to_exec)
}

fn reject_alter_table_of_target(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    operation: &str,
) -> Result<(), ExecError> {
    pgrust_commands::typed_table::reject_alter_table_of_target(catalog, relation, operation)
        .map_err(typed_table_error_to_exec)
}

fn validate_typed_table_compatibility(
    relation: &BoundRelation,
    type_relation: &BoundRelation,
) -> Result<(), ExecError> {
    pgrust_commands::typed_table::validate_typed_table_compatibility(relation, type_relation)
        .map_err(typed_table_error_to_exec)
}
