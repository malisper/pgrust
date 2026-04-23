use super::super::*;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    lookup_heap_relation_for_alter_table, lookup_index_or_partitioned_index_for_alter_index_rename,
    lookup_table_or_partitioned_table_for_alter_table, relation_kind_name,
    validate_alter_table_rename_column,
};

fn normalize_rename_target_name(name: &str) -> Result<String, ExecError> {
    if name.contains('.') {
        return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
            name.to_string(),
        )));
    }
    Ok(name.to_ascii_lowercase())
}

fn push_relation_missing_notice(name: &str) {
    push_notice(format!(r#"relation "{name}" does not exist, skipping"#));
}

fn lookup_relation_for_rename(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
    if_exists: bool,
    expected_relkind: char,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(relation_name) {
        Some(relation) if relation.relkind == expected_relkind => Ok(Some(relation)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: relation_name.to_string(),
            expected: relation_kind_name(expected_relkind),
        })),
        None if if_exists => Ok(None),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            relation_name.to_string(),
        ))),
    }
}

impl Database {
    pub(crate) fn execute_alter_view_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_relation_for_rename(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
            'v',
        )?
        else {
            push_relation_missing_notice(&rename_stmt.table_name);
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
        let result = self.execute_alter_view_rename_stmt_in_transaction_with_search_path(
            client_id,
            rename_stmt,
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

    pub(crate) fn execute_alter_view_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_relation_for_rename(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
            'v',
        )?
        else {
            push_relation_missing_notice(&rename_stmt.table_name);
            return Ok(StatementResult::AffectedRows(0));
        };
        let new_name = normalize_rename_target_name(&rename_stmt.new_table_name)?;
        let visible_type_rows = catalog.type_rows();
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
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
            .rename_relation_mvcc(relation.relation_oid, &new_name, &visible_type_rows, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_index_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_index_or_partitioned_index_for_alter_index_rename(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
        )?
        else {
            push_relation_missing_notice(&rename_stmt.table_name);
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
        let result = self.execute_alter_index_rename_stmt_in_transaction_with_search_path(
            client_id,
            rename_stmt,
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

    pub(crate) fn execute_alter_index_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_index_or_partitioned_index_for_alter_index_rename(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
        )?
        else {
            push_relation_missing_notice(&rename_stmt.table_name);
            return Ok(StatementResult::AffectedRows(0));
        };
        let new_name = normalize_rename_target_name(&rename_stmt.new_table_name)?;
        let visible_type_rows = catalog.type_rows();
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
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
            .rename_relation_mvcc(relation.relation_oid, &new_name, &visible_type_rows, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
        )?
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
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
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
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let new_table_name = normalize_rename_target_name(&rename_stmt.new_table_name)?;
        let visible_type_rows = catalog.type_rows();
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;

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
                .rename_relation_mvcc(
                    relation.relation_oid,
                    &new_table_name,
                    &visible_type_rows,
                    &ctx,
                )
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

    pub(crate) fn execute_alter_table_rename_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
        )?
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
        let result = self.execute_alter_table_rename_column_stmt_in_transaction_with_search_path(
            client_id,
            rename_stmt,
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

    pub(crate) fn execute_alter_table_rename_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &rename_stmt.table_name,
            rename_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE RENAME COLUMN",
                actual: "system catalog".into(),
            }));
        }
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE RENAME COLUMN",
                actual: "temporary table".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
        reject_inheritance_tree_ddl(
            &catalog,
            relation.relation_oid,
            "ALTER TABLE RENAME COLUMN on inheritance tree members is not supported yet",
        )?;
        let new_column_name = validate_alter_table_rename_column(
            &relation.desc,
            &rename_stmt.column_name,
            &rename_stmt.new_column_name,
        )?;
        reject_relation_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
            "ALTER TABLE RENAME COLUMN on relation without dependent views",
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
            .alter_table_rename_column_mvcc(
                relation.relation_oid,
                &rename_stmt.column_name,
                &new_column_name,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
