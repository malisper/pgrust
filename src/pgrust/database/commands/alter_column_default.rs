use super::super::*;
use super::alter_column_type::{
    apply_rewritten_rows_for_alter_column_type, plan_rewritten_rows_for_alter_column_type,
    rebuild_relation_indexes_for_alter_column_type,
    reject_row_type_dependents_for_column_type_change, validate_unique_indexes_for_rewritten_rows,
};
use super::alter_table_work_queue::{
    build_alter_table_work_queue, has_inheritance_children, relation_name_for_alter_error,
};
use crate::backend::parser::{bind_generated_expr, bind_relation_constraints};
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::include::nodes::parsenodes::ColumnGeneratedKind;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, validate_alter_table_alter_column_default,
    validate_alter_table_alter_column_expression,
};

fn lookup_relation_for_alter_column_default(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<crate::backend::parser::BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'f' | 'v') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None if if_exists => {
            crate::backend::utils::misc::notices::push_notice(format!(
                r#"relation "{name}" does not exist, skipping"#
            ));
            Ok(None)
        }
        None => Err(ExecError::Parse(ParseError::UnknownTable(name.to_string()))),
    }
}

fn rewrite_stored_generated_column_rows<C>(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    new_desc: &crate::backend::executor::RelationDesc,
    column_index: usize,
    catalog: &C,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError>
where
    C: CatalogLookup + Clone + 'static,
{
    if relation.relkind != 'r' {
        return Ok(());
    }
    if new_desc.columns[column_index].generated != Some(ColumnGeneratedKind::Stored) {
        return Ok(());
    }
    let Some(rewrite_expr) =
        bind_generated_expr(new_desc, column_index, catalog).map_err(ExecError::Parse)?
    else {
        return Ok(());
    };
    let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
    let mut ctx = super::constraint::ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        &datetime_config,
        interrupts,
    )?;
    ctx.catalog = Some(crate::backend::executor::executor_catalog(catalog.clone()));
    let rewritten_rows = plan_rewritten_rows_for_alter_column_type(
        relation,
        new_desc,
        column_index,
        &rewrite_expr,
        &mut ctx,
    )?;
    let relation_constraints = bind_relation_constraints(
        Some(relation_name),
        relation.relation_oid,
        new_desc,
        catalog,
    )
    .map_err(ExecError::Parse)?;
    for row in &rewritten_rows {
        crate::backend::executor::enforce_relation_constraints(
            relation_name,
            new_desc,
            &relation_constraints,
            &row.values,
            &mut ctx,
        )
        .map_err(map_stored_generated_rewrite_constraint_error)?;
    }
    let indexes = catalog.index_relations_for_heap(relation.relation_oid);
    validate_unique_indexes_for_rewritten_rows(new_desc, &indexes, &rewritten_rows, &mut ctx)?;
    let rewritten_rows = apply_rewritten_rows_for_alter_column_type(
        relation,
        new_desc,
        &rewritten_rows,
        &mut ctx,
        xid,
        cid,
    )
    .map_err(map_stored_generated_rewrite_constraint_error)?;
    rebuild_relation_indexes_for_alter_column_type(
        relation,
        new_desc,
        &indexes,
        &rewritten_rows,
        &mut ctx,
        xid,
    )?;
    Ok(())
}

fn map_stored_generated_rewrite_constraint_error(err: ExecError) -> ExecError {
    match err {
        ExecError::CheckViolation {
            relation,
            constraint,
            ..
        } => ExecError::DetailedError {
            message: format!(
                "check constraint \"{constraint}\" of relation \"{relation}\" is violated by some row"
            ),
            detail: None,
            hint: None,
            sqlstate: "23514",
        },
        other => other,
    }
}

impl Database {
    pub(crate) fn execute_alter_table_alter_column_default_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnDefaultStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_relation_for_alter_column_default(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
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
        let result = self
            .execute_alter_table_alter_column_default_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_table_alter_column_default_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnDefaultStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_relation_for_alter_column_default(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN DEFAULT",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        let work_queue = build_alter_table_work_queue(&catalog, &relation, alter_stmt.only)?;
        for item in work_queue {
            let relation_name = relation_name_for_alter_error(&catalog, item.relation.relation_oid);
            let plan = validate_alter_table_alter_column_default(
                &catalog,
                &item.relation.desc,
                &relation_name,
                &alter_stmt.column_name,
                alter_stmt.default_expr.as_ref(),
                alter_stmt.default_expr_sql.as_deref(),
            )?;
            let default_expr_sql = plan.default_expr_sql.clone();
            let default_sequence_oid = plan.default_sequence_oid;
            let effect = self
                .catalog
                .write()
                .alter_table_set_column_default_mvcc(
                    item.relation.relation_oid,
                    &plan.column_name,
                    default_expr_sql.clone(),
                    default_sequence_oid,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            if item.relation.relpersistence == 't' {
                let mut temp_desc = item.relation.desc.clone();
                let column = temp_desc
                    .columns
                    .iter_mut()
                    .find(|column| column.name.eq_ignore_ascii_case(&plan.column_name))
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(plan.column_name.clone()))
                    })?;
                column.default_expr = default_expr_sql;
                column.default_sequence_oid = default_sequence_oid;
                if column.default_expr.is_none() {
                    column.attrdef_oid = None;
                    column.missing_default_value = None;
                }
                self.replace_temp_entry_desc(client_id, item.relation.relation_oid, temp_desc)?;
            }
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_alter_column_expression_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnExpressionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_relation_for_alter_column_default(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
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
        let result = self
            .execute_alter_table_alter_column_expression_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_table_alter_column_expression_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnExpressionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_relation_for_alter_column_default(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN EXPRESSION",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        if matches!(
            &alter_stmt.action,
            crate::backend::parser::AlterColumnExpressionAction::Drop { .. }
        ) && alter_stmt.only
            && has_inheritance_children(&catalog, relation.relation_oid)
            && relation.desc.columns.iter().any(|column| {
                !column.dropped
                    && column.name.eq_ignore_ascii_case(&alter_stmt.column_name)
                    && column.generated.is_some()
            })
        {
            return Err(ExecError::DetailedError {
                message: "ALTER TABLE / DROP EXPRESSION must be applied to child tables too".into(),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        let work_queue = build_alter_table_work_queue(&catalog, &relation, alter_stmt.only)?;
        for item in work_queue {
            let plan = validate_alter_table_alter_column_expression(
                &catalog,
                item.relation.relation_oid,
                item.relation.namespace_oid,
                &item.relation.desc,
                &alter_stmt.column_name,
                &alter_stmt.action,
            )?;
            let column_index = item
                .relation
                .desc
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(&plan.column_name))
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownColumn(plan.column_name.clone()))
                })?;
            if matches!(
                &alter_stmt.action,
                crate::backend::parser::AlterColumnExpressionAction::Drop { .. }
            ) && (item.relation.desc.columns[column_index].attinhcount > item.expected_parents
                || (!item.recursing
                    && catalog
                        .inheritance_parents(item.relation.relation_oid)
                        .into_iter()
                        .any(|parent| !parent.inhdetachpending)))
            {
                return Err(ExecError::DetailedError {
                    message: "cannot drop generation expression from inherited column".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P16",
                });
            }
            if matches!(
                &alter_stmt.action,
                crate::backend::parser::AlterColumnExpressionAction::Set { .. }
            ) && item.relation.relkind != 'p'
                && !item.relation.relispartition
            {
                reject_row_type_dependents_for_column_type_change(&catalog, &item.relation)?;
            }
            let force_inherited_drop = matches!(
                &alter_stmt.action,
                crate::backend::parser::AlterColumnExpressionAction::Drop { .. }
            ) && item.recursing
                && plan.noop
                && item.relation.desc.columns[column_index]
                    .default_expr
                    .is_some();
            if plan.noop && !force_inherited_drop {
                continue;
            }

            let default_expr_sql = if force_inherited_drop {
                None
            } else {
                plan.default_expr_sql.clone()
            };
            let generated = if force_inherited_drop {
                None
            } else {
                plan.generated
            };
            let mut new_desc = item.relation.desc.clone();
            {
                let column = &mut new_desc.columns[column_index];
                column.default_expr = default_expr_sql.clone();
                column.default_sequence_oid = None;
                column.generated = generated;
                if column.default_expr.is_none() {
                    column.attrdef_oid = None;
                    column.missing_default_value = None;
                }
            }
            let relation_name = relation_name_for_alter_error(&catalog, item.relation.relation_oid);
            rewrite_stored_generated_column_rows(
                self,
                &item.relation,
                &relation_name,
                &new_desc,
                column_index,
                &catalog,
                client_id,
                xid,
                cid,
                std::sync::Arc::clone(&interrupts),
            )?;
            let effect = self
                .catalog
                .write()
                .alter_table_set_column_generation_mvcc(
                    item.relation.relation_oid,
                    &plan.column_name,
                    default_expr_sql.clone(),
                    generated,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            if item.relation.relpersistence == 't' {
                let mut temp_desc = item.relation.desc.clone();
                let column = temp_desc
                    .columns
                    .iter_mut()
                    .find(|column| column.name.eq_ignore_ascii_case(&plan.column_name))
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(plan.column_name.clone()))
                    })?;
                column.default_expr = default_expr_sql;
                column.default_sequence_oid = None;
                column.generated = generated;
                if column.default_expr.is_none() {
                    column.attrdef_oid = None;
                    column.missing_default_value = None;
                }
                self.replace_temp_entry_desc(client_id, item.relation.relation_oid, temp_desc)?;
            }
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
