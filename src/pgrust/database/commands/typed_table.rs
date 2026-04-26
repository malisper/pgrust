use super::super::*;
use crate::backend::executor::{ColumnDesc, RelationDesc, StatementResult};
use crate::backend::parser::{
    AlterTableNotOfStatement, AlterTableOfStatement, BoundRelation, CatalogLookup, ParseError,
    SqlTypeKind,
};
use crate::include::catalog::{PG_CATALOG_NAMESPACE_OID, PgTypeRow};
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table, map_catalog_error,
};

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
) -> Result<(PgTypeRow, BoundRelation), ExecError> {
    let type_row = catalog
        .type_by_name(type_name)
        .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(type_name.to_string())))?;
    if matches!(type_row.sql_type.kind, SqlTypeKind::Shell) {
        return Err(ExecError::DetailedError {
            message: format!("type \"{}\" is only a shell", type_row.typname),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    if !matches!(type_row.sql_type.kind, SqlTypeKind::Composite) || type_row.typrelid == 0 {
        return Err(ExecError::DetailedError {
            message: format!("type {} is not a composite type", type_row.typname),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    let class_row = catalog.class_row_by_oid(type_row.typrelid).ok_or_else(|| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "composite type relation",
            actual: format!("missing relation oid {}", type_row.typrelid),
        })
    })?;
    if class_row.relkind != 'c' {
        return Err(ExecError::DetailedError {
            message: format!("type {} is the row type of another table", type_row.typname),
            detail: Some(
                "A typed table must use a stand-alone composite type created with CREATE TYPE."
                    .into(),
            ),
            hint: None,
            sqlstate: "42809",
        });
    }
    let relation = catalog
        .lookup_relation_by_oid(type_row.typrelid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "composite type relation",
                actual: format!("missing relation oid {}", type_row.typrelid),
            })
        })?;
    Ok((type_row, relation))
}

pub(crate) fn reject_typed_table_ddl(
    relation: &BoundRelation,
    operation: &str,
) -> Result<(), ExecError> {
    if relation.of_type_oid != 0 {
        return Err(ExecError::DetailedError {
            message: format!("cannot {operation} typed table"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

fn reject_alter_table_of_target(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    operation: &str,
) -> Result<(), ExecError> {
    if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user table for typed table operation",
            actual: "system catalog".into(),
        }));
    }
    if relation.relpersistence == 't' {
        return Err(ExecError::DetailedError {
            message: format!("{operation} is not supported for temporary tables"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if !catalog
        .inheritance_parents(relation.relation_oid)
        .is_empty()
        || catalog.has_subclass(relation.relation_oid)
    {
        return Err(ExecError::DetailedError {
            message: "cannot change typed-table status of inherited table".into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

fn validate_typed_table_compatibility(
    relation: &BoundRelation,
    type_relation: &BoundRelation,
) -> Result<(), ExecError> {
    let table_columns = visible_columns(&relation.desc);
    let type_columns = visible_columns(&type_relation.desc);
    if table_columns.len() != type_columns.len() {
        return Err(typed_table_mismatch_error(
            "table has a different number of columns",
        ));
    }
    for (table_column, type_column) in table_columns.into_iter().zip(type_columns) {
        if !table_column.name.eq_ignore_ascii_case(&type_column.name) {
            return Err(typed_table_mismatch_error(&format!(
                "column \"{}\" has a different name",
                table_column.name
            )));
        }
        if table_column.sql_type != type_column.sql_type {
            return Err(typed_table_mismatch_error(&format!(
                "column \"{}\" has a different type",
                table_column.name
            )));
        }
        if table_column.collation_oid != type_column.collation_oid {
            return Err(typed_table_mismatch_error(&format!(
                "column \"{}\" has a different collation",
                table_column.name
            )));
        }
    }
    Ok(())
}

fn visible_columns(desc: &RelationDesc) -> Vec<&ColumnDesc> {
    desc.columns
        .iter()
        .filter(|column| !column.dropped)
        .collect()
}

fn typed_table_mismatch_error(detail: &str) -> ExecError {
    ExecError::DetailedError {
        message: "table is not compatible with composite type".into(),
        detail: Some(detail.to_string()),
        hint: None,
        sqlstate: "42809",
    }
}
