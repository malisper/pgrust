use super::super::*;
use super::alter_table_work_queue::{build_alter_table_work_queue, has_inheritance_children};
use super::typed_table::reject_typed_table_ddl;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::include::nodes::parsenodes::AlterRelationSetSchemaStatement;
use crate::pgrust::database::ddl::{
    dependent_view_rewrites_for_relation, lookup_heap_relation_for_alter_table,
    lookup_index_or_partitioned_index_for_alter_index_rename,
    lookup_table_or_partitioned_table_for_alter_table, relation_kind_name, rewrite_dependent_views,
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

fn collect_rename_column_targets(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    column_name: &str,
    new_column_name: &str,
    only: bool,
) -> Result<Vec<BoundRelation>, ExecError> {
    if only && has_inheritance_children(catalog, relation.relation_oid) {
        return Err(ExecError::DetailedError {
            message: format!(
                "inherited column \"{column_name}\" must be renamed in child tables too"
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }

    let work_queue = build_alter_table_work_queue(catalog, relation, only)?;
    let mut targets = Vec::with_capacity(work_queue.len());

    for item in work_queue {
        let target = item.relation;
        let Some(column) = target
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        else {
            return Err(ExecError::Parse(ParseError::UnknownColumn(
                column_name.to_string(),
            )));
        };
        if column.attinhcount > item.expected_parents {
            return Err(ExecError::DetailedError {
                message: format!("cannot rename inherited column \"{column_name}\""),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        let relation_name = relation_name_for_error(catalog, &target);
        validate_alter_table_rename_column(
            &target.desc,
            &relation_name,
            column_name,
            new_column_name,
        )?;
        targets.push(target);
    }

    Ok(targets)
}

fn relation_name_for_error(catalog: &dyn CatalogLookup, relation: &BoundRelation) -> String {
    catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string())
}

fn map_relation_rename_error(err: crate::backend::catalog::catalog::CatalogError) -> ExecError {
    match err {
        crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(name) => {
            ExecError::DetailedError {
                message: format!("relation \"{name}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42P07",
            }
        }
        other => map_catalog_error(other),
    }
}

fn lookup_relation_for_rename(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
    if_exists: bool,
    expected_relkind: char,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(relation_name) {
        Some(relation)
            if relation.relkind == expected_relkind
                || (expected_relkind == 'r' && relation.relkind == 'f') =>
        {
            Ok(Some(relation))
        }
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

fn normalize_schema_name(name: &str) -> String {
    name.to_ascii_lowercase()
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
        let dependent_views = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
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
            .rename_relation_mvcc(relation.relation_oid, &new_name, &visible_type_rows, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        rewrite_dependent_views(
            self,
            client_id,
            Some((xid, cid.saturating_add(10))),
            &dependent_views,
            xid,
            cid.saturating_add(10),
            catalog_effects,
        )?;
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
        let current_relation_name = relation_name_for_error(&catalog, &relation);
        if current_relation_name.eq_ignore_ascii_case(&new_table_name) {
            return Err(ExecError::DetailedError {
                message: format!("relation \"{current_relation_name}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42P07",
            });
        }
        let visible_type_rows = catalog.type_rows();
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
        let dependent_views = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
        )?;

        if relation.relpersistence != 't' {
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
                .map_err(map_relation_rename_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            rewrite_dependent_views(
                self,
                client_id,
                Some((xid, cid.saturating_add(10))),
                &dependent_views,
                xid,
                cid.saturating_add(10),
                catalog_effects,
            )?;
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
        let lock_queue = build_alter_table_work_queue(&catalog, &relation, rename_stmt.only)?;
        let lock_requests = lock_queue
            .iter()
            .map(|item| (item.relation.rel, TableLockMode::AccessExclusive))
            .collect::<Vec<_>>();
        crate::backend::storage::lmgr::lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &lock_requests,
            interrupts.as_ref(),
        )?;
        let locked_rels =
            crate::pgrust::database::foreign_keys::table_lock_relations(&lock_requests);
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
        for rel in locked_rels {
            self.table_locks.unlock_table(rel, client_id);
        }
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
        reject_typed_table_ddl(&relation, "rename column of")?;
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
        let new_column_name = normalize_rename_target_name(&rename_stmt.new_column_name)?;
        let targets = collect_rename_column_targets(
            &catalog,
            &relation,
            &rename_stmt.column_name,
            &new_column_name,
            rename_stmt.only,
        )?;
        let mut dependent_views = Vec::new();
        for target in &targets {
            dependent_views.extend(dependent_view_rewrites_for_relation(
                self,
                client_id,
                Some((xid, cid)),
                target.relation_oid,
            )?);
        }
        dependent_views.sort_by_key(|view| view.relation_oid);
        dependent_views.dedup_by_key(|view| view.relation_oid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        for target in targets {
            let effect = self
                .catalog
                .write()
                .alter_table_rename_column_mvcc(
                    target.relation_oid,
                    &rename_stmt.column_name,
                    &new_column_name,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        rewrite_dependent_views(
            self,
            client_id,
            Some((xid, cid.saturating_add(10))),
            &dependent_views,
            xid,
            cid.saturating_add(10),
            catalog_effects,
        )?;
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_view_rename_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterTableRenameColumnStatement,
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
        let result = self.execute_alter_view_rename_column_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_view_rename_column_stmt_in_transaction_with_search_path(
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
        ensure_relation_owner(self, client_id, &relation, &rename_stmt.table_name)?;
        let new_column_name = validate_alter_table_rename_column(
            &relation.desc,
            &relation_name_for_error(&catalog, &relation),
            &rename_stmt.column_name,
            &rename_stmt.new_column_name,
        )?;
        let dependent_views = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
        )?;
        let mut new_desc = relation.desc.clone();
        let column_index = new_desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(&rename_stmt.column_name))
                    .then_some(index)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownColumn(rename_stmt.column_name.clone()))
            })?;
        new_desc.columns[column_index].name = new_column_name.clone();
        new_desc.columns[column_index].storage.name = new_column_name.clone();
        let reloptions = catalog
            .class_row_by_oid(relation.relation_oid)
            .and_then(|row| row.reloptions.clone());
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
            .alter_view_relation_desc_mvcc(relation.relation_oid, new_desc, reloptions, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        rewrite_dependent_views(
            self,
            client_id,
            Some((xid, cid.saturating_add(10))),
            &dependent_views,
            xid,
            cid.saturating_add(10),
            catalog_effects,
        )?;
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_set_schema_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_set_schema_stmt_with_search_path(
            client_id,
            alter_stmt,
            configured_search_path,
            'r',
        )
    }

    pub(crate) fn execute_alter_view_set_schema_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_set_schema_stmt_with_search_path(
            client_id,
            alter_stmt,
            configured_search_path,
            'v',
        )
    }

    pub(crate) fn execute_alter_materialized_view_set_schema_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_set_schema_stmt_with_search_path(
            client_id,
            alter_stmt,
            configured_search_path,
            'm',
        )
    }

    fn execute_alter_relation_set_schema_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        configured_search_path: Option<&[String]>,
        relkind: char,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_relation_for_rename(
            &catalog,
            &alter_stmt.relation_name,
            alter_stmt.if_exists,
            relkind,
        )?
        else {
            push_relation_missing_notice(&alter_stmt.relation_name);
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
        let result = self.execute_alter_relation_set_schema_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            relkind,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_set_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_set_schema_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            cid,
            configured_search_path,
            'r',
            catalog_effects,
            temp_effects,
        )
    }

    pub(crate) fn execute_alter_view_set_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_set_schema_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            cid,
            configured_search_path,
            'v',
            catalog_effects,
            temp_effects,
        )
    }

    pub(crate) fn execute_alter_materialized_view_set_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_relation_set_schema_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            cid,
            configured_search_path,
            'm',
            catalog_effects,
            temp_effects,
        )
    }

    fn execute_alter_relation_set_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterRelationSetSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        relkind: char,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        _temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_relation_for_rename(
            &catalog,
            &alter_stmt.relation_name,
            alter_stmt.if_exists,
            relkind,
        )?
        else {
            push_relation_missing_notice(&alter_stmt.relation_name);
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.relation_name)?;
        let target_schema = normalize_schema_name(&alter_stmt.schema_name);
        let namespace_oid = self
            .visible_namespace_oid_by_name(client_id, Some((xid, cid)), &target_schema)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{}\" does not exist", alter_stmt.schema_name),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        let dependent_views = dependent_view_rewrites_for_relation(
            self,
            client_id,
            Some((xid, cid)),
            relation.relation_oid,
        )?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "ALTER {} SET SCHEMA for temporary relations",
                relation_kind_name(relkind)
            ))));
        }
        let visible_type_rows = catalog.type_rows();
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
            .move_relation_to_namespace_mvcc(
                relation.relation_oid,
                namespace_oid,
                &visible_type_rows,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        rewrite_dependent_views(
            self,
            client_id,
            Some((xid, cid.saturating_add(10))),
            &dependent_views,
            xid,
            cid.saturating_add(10),
            catalog_effects,
        )?;
        Ok(StatementResult::AffectedRows(0))
    }
}
