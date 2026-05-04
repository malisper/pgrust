use super::super::*;
use super::foreign_data_wrapper::alter_option_map;
use super::reloptions::{reset_reloptions, split_table_reloption_resets};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table,
    validate_alter_table_alter_column_options,
};
use pgrust_commands::reloptions::{reset_view_reloptions, set_view_reloptions};

fn reloption_error_to_exec(err: pgrust_commands::reloptions::RelOptionError) -> ExecError {
    match err {
        pgrust_commands::reloptions::RelOptionError::Parse(err) => ExecError::Parse(err),
        pgrust_commands::reloptions::RelOptionError::Detailed {
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
    pub(crate) fn execute_alter_view_set_options_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableSetStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
        if relation.relkind != 'v' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: alter_stmt.table_name.clone(),
                expected: "view",
            }));
        }
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_view_set_options_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
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

    pub(crate) fn execute_alter_view_set_options_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableSetStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
        if relation.relkind != 'v' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: alter_stmt.table_name.clone(),
                expected: "view",
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let current = catalog
            .class_row_by_oid(relation.relation_oid)
            .and_then(|row| row.reloptions);
        let reloptions =
            set_view_reloptions(current, &alter_stmt.options).map_err(reloption_error_to_exec)?;
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
            .alter_relation_reloptions_mvcc(relation.relation_oid, reloptions, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_view_reset_options_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableResetStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
        if relation.relkind != 'v' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: alter_stmt.table_name.clone(),
                expected: "view",
            }));
        }
        let lock_tag = crate::pgrust::database::relation_lock_tag(&relation);
        self.table_locks.lock_table_interruptible(
            lock_tag,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_view_reset_options_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(lock_tag, client_id);
        result
    }

    pub(crate) fn execute_alter_view_reset_options_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableResetStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.table_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.table_name.clone(),
            )));
        };
        if relation.relkind != 'v' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: alter_stmt.table_name.clone(),
                expected: "view",
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let current = catalog
            .class_row_by_oid(relation.relation_oid)
            .and_then(|row| row.reloptions);
        let reloptions =
            reset_view_reloptions(current, &alter_stmt.options).map_err(reloption_error_to_exec)?;
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
            .alter_relation_reloptions_mvcc(relation.relation_oid, reloptions, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_reset_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableResetStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = match catalog.lookup_any_relation(&alter_stmt.table_name) {
            Some(relation) if matches!(relation.relkind, 'r' | 'p' | 'f' | 'v') => relation,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: alter_stmt.table_name.clone(),
                    expected: "table",
                }));
            }
            None if alter_stmt.if_exists => {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    alter_stmt.table_name.clone(),
                )));
            }
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
        let result = self.execute_alter_table_reset_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
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

    pub(crate) fn execute_alter_table_reset_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableResetStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match catalog.lookup_any_relation(&alter_stmt.table_name) {
            Some(relation) if matches!(relation.relkind, 'r' | 'p' | 'f' | 'v') => relation,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: alter_stmt.table_name.clone(),
                    expected: "table",
                }));
            }
            None if alter_stmt.if_exists => {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    alter_stmt.table_name.clone(),
                )));
            }
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE RESET options",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        if relation.relkind == 'v' {
            let current = catalog
                .class_row_by_oid(relation.relation_oid)
                .and_then(|row| row.reloptions);
            let reloptions = reset_view_reloptions(current, &alter_stmt.options)
                .map_err(reloption_error_to_exec)?;
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
                .alter_relation_reloptions_mvcc(relation.relation_oid, reloptions, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            return Ok(StatementResult::AffectedRows(0));
        }
        let resets = split_table_reloption_resets(&alter_stmt.options)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        if !resets.heap.is_empty() {
            let current = catalog
                .class_row_by_oid(relation.relation_oid)
                .and_then(|row| row.reloptions);
            let reloptions = reset_reloptions(current, &resets.heap);
            let effect = self
                .catalog
                .write()
                .alter_relation_reloptions_mvcc(relation.relation_oid, reloptions, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        if !resets.toast.is_empty()
            && let Some(toast) = relation.toast.as_ref()
        {
            let current = catalog
                .class_row_by_oid(toast.relation_oid)
                .and_then(|row| row.reloptions);
            let reloptions = reset_reloptions(current, &resets.toast);
            let effect = self
                .catalog
                .write()
                .alter_relation_reloptions_mvcc(toast.relation_oid, reloptions, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_alter_column_options_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnOptionsStatement,
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
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_table_alter_column_options_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
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

    pub(crate) fn execute_alter_table_alter_column_options_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnOptionsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN SET/RESET options",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let column_name =
            validate_alter_table_alter_column_options(&relation.desc, &alter_stmt.column_name)?;
        if relation.relkind == 'f' {
            let crate::backend::parser::AlterColumnOptionsAction::Fdw(options) = &alter_stmt.action
            else {
                return Ok(StatementResult::AffectedRows(0));
            };
            let existing = self
                .backend_catcache(client_id, Some((xid, cid)))
                .map_err(map_catalog_error)?
                .attributes_by_relid(relation.relation_oid)
                .and_then(|rows| {
                    rows.iter()
                        .find(|row| {
                            !row.attisdropped && row.attname.eq_ignore_ascii_case(&column_name)
                        })
                        .and_then(|row| row.attfdwoptions.clone())
                })
                .or_else(|| {
                    relation
                        .desc
                        .columns
                        .iter()
                        .find(|column| {
                            !column.dropped && column.name.eq_ignore_ascii_case(&column_name)
                        })
                        .and_then(|column| column.fdw_options.clone())
                });
            let fdw_options = alter_option_map(existing, options)?;
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
                .alter_table_set_column_fdw_options_mvcc(
                    relation.relation_oid,
                    &column_name,
                    fdw_options,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            _catalog_effects.push(effect);
            return Ok(StatementResult::AffectedRows(0));
        }

        // :HACK: PostgreSQL stores column-level SET/RESET options in pg_attribute.attoptions.
        // pgrust does not model attoptions yet, so for now we validate and accept the syntax
        // without persisting option values.
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_index_alter_column_options_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterIndexAlterColumnOptionsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&alter_stmt.index_name) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    alter_stmt.index_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                alter_stmt.index_name.clone(),
            )));
        };
        if !matches!(relation.relkind, 'i' | 'I') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: alter_stmt.index_name.clone(),
                expected: "index",
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.index_name)?;
        let display_name = self
            .relation_display_name(
                client_id,
                None,
                configured_search_path,
                relation.relation_oid,
            )
            .unwrap_or_else(|| alter_stmt.index_name.clone());
        let action = match alter_stmt.action {
            crate::backend::parser::AlterColumnOptionsAction::Set(_) => "ALTER COLUMN ... SET",
            crate::backend::parser::AlterColumnOptionsAction::Reset(_) => "ALTER COLUMN ... RESET",
            crate::backend::parser::AlterColumnOptionsAction::Fdw(_) => "ALTER COLUMN ... OPTIONS",
        };
        let detail = if relation.relkind == 'I' {
            "This operation is not supported for partitioned indexes."
        } else {
            "This operation is not supported for indexes."
        };
        Err(ExecError::DetailedError {
            message: format!(
                "ALTER action {action} cannot be performed on relation \"{display_name}\""
            ),
            detail: Some(detail.into()),
            hint: None,
            sqlstate: "42809",
        })
    }
}
