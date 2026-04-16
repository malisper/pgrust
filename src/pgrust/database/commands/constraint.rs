use super::super::*;
use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::{ExecutorContext, eval_expr};
use crate::backend::parser::BoundCheckConstraint;
use crate::include::catalog::{
    CONSTRAINT_CHECK, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE,
    PG_CATALOG_NAMESPACE_OID, PgConstraintRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::pgrust::database::ddl::is_system_column_name;

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn choose_available_constraint_name(
    base: &str,
    used_names: &mut std::collections::BTreeSet<String>,
) -> String {
    if used_names.insert(base.to_ascii_lowercase()) {
        return base.to_string();
    }
    for suffix in 1.. {
        let candidate = format!("{base}{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    unreachable!("constraint name suffix space exhausted")
}

fn ddl_executor_context(
    db: &Database,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<ExecutorContext, ExecError> {
    let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    Ok(ExecutorContext {
        pool: std::sync::Arc::clone(&db.pool),
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts,
        snapshot,
        client_id,
        next_command_id: cid,
        outer_rows: Vec::new(),
        outer_system_bindings: Vec::new(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        catalog: catalog.materialize_visible_catalog(),
        compiled_functions: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
    })
}

fn validate_not_null_rows(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    column_index: usize,
    constraint_name: &str,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    let mut ctx = ddl_executor_context(db, catalog, client_id, xid, cid, interrupts)?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    let column_name = relation.desc.columns[column_index].name.clone();
    for (_, values) in rows {
        if matches!(values.get(column_index), Some(Value::Null) | None) {
            return Err(ExecError::NotNullViolation {
                relation: relation_name.to_string(),
                column: column_name,
                constraint: constraint_name.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_check_rows(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    constraint_name: &str,
    expr_sql: &str,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    let expr = crate::backend::parser::bind_check_constraint_expr(
        expr_sql,
        Some(relation_name),
        &relation.desc,
        catalog,
    )
    .map_err(ExecError::Parse)?;
    let check = BoundCheckConstraint {
        constraint_name: constraint_name.to_string(),
        expr,
    };
    let mut ctx = ddl_executor_context(db, catalog, client_id, xid, cid, interrupts)?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    for (_, values) in rows {
        let mut slot = TupleSlot::virtual_row(values);
        match eval_expr(&check.expr, &mut slot, &mut ctx)? {
            Value::Null | Value::Bool(true) => {}
            Value::Bool(false) => {
                return Err(ExecError::CheckViolation {
                    relation: relation_name.to_string(),
                    constraint: check.constraint_name.clone(),
                });
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: "CHECK constraint expression must return boolean".into(),
                    detail: Some(format!(
                        "constraint \"{}\" on relation \"{}\" produced a non-boolean value",
                        check.constraint_name, relation_name
                    )),
                    hint: None,
                    sqlstate: "42804",
                });
            }
        }
    }
    Ok(())
}

fn attnums_from_constraint(row: &PgConstraintRow) -> Result<Vec<i16>, ExecError> {
    row.conkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "constraint columns",
                actual: format!("missing conkey for constraint {}", row.conname),
            })
        })
}

fn column_index_for_attnum(
    relation: &crate::backend::parser::BoundRelation,
    attnum: i16,
) -> Result<usize, ExecError> {
    let index = usize::try_from(attnum.saturating_sub(1)).map_err(|_| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column attnum",
            actual: format!("invalid attnum {attnum}"),
        })
    })?;
    relation
        .desc
        .columns
        .get(index)
        .filter(|column| !column.dropped)
        .map(|_| index)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user column attnum",
                actual: format!("invalid attnum {attnum}"),
            })
        })
}

fn find_constraint_row<'a>(rows: &'a [PgConstraintRow], name: &str) -> Option<&'a PgConstraintRow> {
    rows.iter()
        .find(|row| row.conname.eq_ignore_ascii_case(name))
}

fn ensure_constraint_relation(
    db: &Database,
    client_id: ClientId,
    relation: &crate::backend::parser::BoundRelation,
    table_name: &str,
) -> Result<(), ExecError> {
    if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user table for ALTER TABLE constraint operations",
            actual: "system catalog".into(),
        }));
    }
    if relation.relpersistence == 't' {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "permanent table for ALTER TABLE constraint operations",
            actual: "temporary table".into(),
        }));
    }
    ensure_relation_owner(db, client_id, relation, table_name)
}

fn primary_constraint_for_attnum<'a>(
    rows: &'a [PgConstraintRow],
    attnum: i16,
) -> Option<&'a PgConstraintRow> {
    rows.iter().find(|row| {
        row.contype == CONSTRAINT_PRIMARY
            && row
                .conkey
                .as_ref()
                .is_some_and(|keys| keys.contains(&attnum))
    })
}

impl Database {
    pub(crate) fn execute_alter_table_add_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAddConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAddConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        ensure_constraint_relation(self, client_id, &relation, &alter_stmt.table_name)?;
        let table_name = relation_basename(&alter_stmt.table_name).to_string();
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let normalized = crate::backend::parser::normalize_alter_table_add_constraint(
            &table_name,
            &relation.desc,
            &existing_constraints,
            &alter_stmt.constraint,
        )
        .map_err(ExecError::Parse)?;

        match normalized {
            crate::backend::parser::NormalizedAlterTableConstraint::NotNull(action) => {
                let column_index = relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, column)| {
                        (!column.dropped && column.name.eq_ignore_ascii_case(&action.column))
                            .then_some(index)
                    })
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(action.column.clone()))
                    })?;
                if !action.not_valid {
                    validate_not_null_rows(
                        self,
                        &relation,
                        &table_name,
                        column_index,
                        &action.constraint_name,
                        &catalog,
                        client_id,
                        xid,
                        cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
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
                    .set_column_not_null_mvcc(
                        relation.relation_oid,
                        &action.column,
                        action.constraint_name,
                        !action.not_valid,
                        false,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                let (_constraint_oid, effect) = effect;
                catalog_effects.push(effect);
            }
            crate::backend::parser::NormalizedAlterTableConstraint::Check(action) => {
                crate::backend::parser::bind_check_constraint_expr(
                    &action.expr_sql,
                    Some(&table_name),
                    &relation.desc,
                    &catalog,
                )
                .map_err(ExecError::Parse)?;
                if !action.not_valid {
                    validate_check_rows(
                        self,
                        &relation,
                        &table_name,
                        &action.constraint_name,
                        &action.expr_sql,
                        &catalog,
                        client_id,
                        xid,
                        cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
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
                    .create_check_constraint_mvcc(
                        relation.relation_oid,
                        action.constraint_name,
                        !action.not_valid,
                        action.expr_sql,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
            }
            crate::backend::parser::NormalizedAlterTableConstraint::IndexBacked(action) => {
                let mut primary_key_owned_not_null_oids = Vec::new();
                if action.primary {
                    let mut used_names = existing_constraints
                        .iter()
                        .map(|row| row.conname.to_ascii_lowercase())
                        .collect::<std::collections::BTreeSet<_>>();
                    for column_name in &action.columns {
                        let column_index = relation
                            .desc
                            .columns
                            .iter()
                            .enumerate()
                            .find_map(|(index, column)| {
                                (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                                    .then_some(index)
                            })
                            .ok_or_else(|| {
                                ExecError::Parse(ParseError::UnknownColumn(column_name.clone()))
                            })?;
                        if relation.desc.columns[column_index].storage.nullable {
                            let not_null_name = choose_available_constraint_name(
                                &format!("{table_name}_{column_name}_not_null"),
                                &mut used_names,
                            );
                            validate_not_null_rows(
                                self,
                                &relation,
                                &table_name,
                                column_index,
                                &not_null_name,
                                &catalog,
                                client_id,
                                xid,
                                cid,
                                std::sync::Arc::clone(&interrupts),
                            )?;
                            let set_ctx = CatalogWriteContext {
                                pool: self.pool.clone(),
                                txns: self.txns.clone(),
                                xid,
                                cid: cid.saturating_add(catalog_effects.len() as u32),
                                client_id,
                                waiter: None,
                                interrupts: std::sync::Arc::clone(&interrupts),
                            };
                            let effect = self
                                .catalog
                                .write()
                                .set_column_not_null_mvcc(
                                    relation.relation_oid,
                                    column_name,
                                    not_null_name.clone(),
                                    true,
                                    true,
                                    &set_ctx,
                                )
                                .map_err(map_catalog_error)?;
                            let (not_null_oid, effect) = effect;
                            primary_key_owned_not_null_oids.push(not_null_oid);
                            catalog_effects.push(effect);
                        }
                    }
                }

                let index_cid = cid
                    .saturating_add(1)
                    .saturating_add(catalog_effects.len() as u32);
                let constraint_name = action
                    .constraint_name
                    .clone()
                    .expect("normalized key constraint name");
                let index_name = self.choose_available_relation_name(
                    client_id,
                    xid,
                    index_cid,
                    relation.namespace_oid,
                    &constraint_name,
                )?;
                let index_columns = action
                    .columns
                    .iter()
                    .cloned()
                    .map(crate::backend::parser::IndexColumnDef::from)
                    .collect::<Vec<_>>();
                let build_options = self.resolve_simple_btree_build_options(
                    client_id,
                    Some((xid, index_cid)),
                    &relation,
                    &index_columns,
                )?;
                let index_entry = self.build_simple_btree_index_in_transaction(
                    client_id,
                    &relation,
                    &index_name,
                    &index_columns,
                    true,
                    action.primary,
                    xid,
                    index_cid,
                    build_options.0,
                    build_options.1,
                    &build_options.2,
                    65_536,
                    catalog_effects,
                )?;
                let constraint_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: index_cid.saturating_add(2),
                    client_id,
                    waiter: None,
                    interrupts,
                };
                let effect = self
                    .catalog
                    .write()
                    .create_index_backed_constraint_mvcc(
                        relation.relation_oid,
                        index_entry.relation_oid,
                        constraint_name,
                        if action.primary {
                            CONSTRAINT_PRIMARY
                        } else {
                            CONSTRAINT_UNIQUE
                        },
                        &primary_key_owned_not_null_oids,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_drop_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::AlterTableDropConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &drop_stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::AlterTableDropConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &drop_stmt.table_name)?;
        ensure_constraint_relation(self, client_id, &relation, &drop_stmt.table_name)?;
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        let row = find_constraint_row(&rows, &drop_stmt.constraint_name)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing table constraint",
                    actual: format!(
                        "constraint \"{}\" does not exist",
                        drop_stmt.constraint_name
                    ),
                })
            })?;

        match row.contype {
            CONSTRAINT_CHECK => {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts,
                };
                let (_removed, effect) = self
                    .catalog
                    .write()
                    .drop_relation_constraint_mvcc(
                        relation.relation_oid,
                        &drop_stmt.constraint_name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
            }
            CONSTRAINT_NOTNULL => {
                let attnum = *attnums_from_constraint(&row)?
                    .first()
                    .expect("not null attnum");
                if let Some(primary) = primary_constraint_for_attnum(&rows, attnum) {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "droppable NOT NULL constraint",
                        actual: format!(
                            "column is required by PRIMARY KEY constraint \"{}\"",
                            primary.conname
                        ),
                    }));
                }
                let column_index = column_index_for_attnum(&relation, attnum)?;
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
                    .drop_column_not_null_mvcc(
                        relation.relation_oid,
                        &relation.desc.columns[column_index].name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
            }
            CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE => {
                let mut next_cid = cid;
                if row.contype == CONSTRAINT_PRIMARY {
                    let pk_owned_not_null_oids =
                        crate::backend::utils::cache::syscache::ensure_depend_rows(
                            self,
                            client_id,
                            Some((xid, cid)),
                        )
                        .into_iter()
                        .filter(|depend| {
                            depend.classid == crate::include::catalog::PG_CONSTRAINT_RELATION_OID
                                && depend.refclassid
                                    == crate::include::catalog::PG_CONSTRAINT_RELATION_OID
                                && depend.refobjid == row.oid
                                && depend.deptype == crate::include::catalog::DEPENDENCY_INTERNAL
                        })
                        .map(|depend| depend.objid)
                        .collect::<std::collections::BTreeSet<_>>();
                    for attnum in attnums_from_constraint(&row)? {
                        let column_index = column_index_for_attnum(&relation, attnum)?;
                        let not_null_row = rows.iter().find(|constraint| {
                            constraint.contype == CONSTRAINT_NOTNULL
                                && pk_owned_not_null_oids.contains(&constraint.oid)
                                && constraint
                                    .conkey
                                    .as_ref()
                                    .is_some_and(|keys| keys.contains(&attnum))
                        });
                        if not_null_row.is_some() {
                            let ctx = CatalogWriteContext {
                                pool: self.pool.clone(),
                                txns: self.txns.clone(),
                                xid,
                                cid: next_cid,
                                client_id,
                                waiter: None,
                                interrupts: std::sync::Arc::clone(&interrupts),
                            };
                            let effect = self
                                .catalog
                                .write()
                                .drop_column_not_null_mvcc(
                                    relation.relation_oid,
                                    &relation.desc.columns[column_index].name,
                                    &ctx,
                                )
                                .map_err(map_catalog_error)?;
                            catalog_effects.push(effect);
                            next_cid = next_cid.saturating_add(1);
                        }
                    }
                }
                let constraint_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: std::sync::Arc::clone(&interrupts),
                };
                let (removed, effect) = self
                    .catalog
                    .write()
                    .drop_relation_constraint_mvcc(
                        relation.relation_oid,
                        &drop_stmt.constraint_name,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                if removed.conindid != 0 {
                    let drop_index_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: next_cid.saturating_add(1),
                        client_id,
                        waiter: None,
                        interrupts,
                    };
                    let (_entry, effect) = self
                        .catalog
                        .write()
                        .drop_relation_entry_by_oid_mvcc(removed.conindid, &drop_index_ctx)
                        .map_err(map_catalog_error)?;
                    catalog_effects.push(effect);
                }
            }
            _ => {
                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                    "ALTER TABLE DROP CONSTRAINT".into(),
                )));
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_set_not_null_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableSetNotNullStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableSetNotNullStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        ensure_constraint_relation(self, client_id, &relation, &alter_stmt.table_name)?;
        if is_system_column_name(&alter_stmt.column_name) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user column name for SET NOT NULL",
                actual: alter_stmt.column_name.clone(),
            }));
        }
        let column_index = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(&alter_stmt.column_name))
                    .then_some(index)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownColumn(alter_stmt.column_name.clone()))
            })?;
        if !relation.desc.columns[column_index].storage.nullable {
            return Ok(StatementResult::AffectedRows(0));
        }
        let used_names = catalog
            .constraint_rows_for_relation(relation.relation_oid)
            .into_iter()
            .collect::<Vec<_>>();
        let constraint_name = crate::backend::parser::generated_not_null_constraint_name(
            relation_basename(&alter_stmt.table_name),
            &relation.desc.columns[column_index].name,
            &used_names,
        );
        validate_not_null_rows(
            self,
            &relation,
            relation_basename(&alter_stmt.table_name),
            column_index,
            &constraint_name,
            &catalog,
            client_id,
            xid,
            cid,
            std::sync::Arc::clone(&interrupts),
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
            .set_column_not_null_mvcc(
                relation.relation_oid,
                &relation.desc.columns[column_index].name,
                constraint_name,
                true,
                false,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        let (_constraint_oid, effect) = effect;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_drop_not_null_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableDropNotNullStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableDropNotNullStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        ensure_constraint_relation(self, client_id, &relation, &alter_stmt.table_name)?;
        if is_system_column_name(&alter_stmt.column_name) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user column name for DROP NOT NULL",
                actual: alter_stmt.column_name.clone(),
            }));
        }
        let column_index = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(&alter_stmt.column_name))
                    .then_some(index)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownColumn(alter_stmt.column_name.clone()))
            })?;
        if relation.desc.columns[column_index].storage.nullable {
            return Ok(StatementResult::AffectedRows(0));
        }
        let attnum = (column_index + 1) as i16;
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        if let Some(primary) = primary_constraint_for_attnum(&existing_constraints, attnum) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "droppable NOT NULL column",
                actual: format!(
                    "column is required by PRIMARY KEY constraint \"{}\"",
                    primary.conname
                ),
            }));
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
            .drop_column_not_null_mvcc(
                relation.relation_oid,
                &relation.desc.columns[column_index].name,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_validate_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableValidateConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
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
            .execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableValidateConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = lookup_heap_relation_for_ddl(&catalog, &alter_stmt.table_name)?;
        ensure_constraint_relation(self, client_id, &relation, &alter_stmt.table_name)?;
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        let row = find_constraint_row(&rows, &alter_stmt.constraint_name)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing table constraint",
                    actual: format!(
                        "constraint \"{}\" does not exist",
                        alter_stmt.constraint_name
                    ),
                })
            })?;
        if row.convalidated {
            return Ok(StatementResult::AffectedRows(0));
        }

        match row.contype {
            CONSTRAINT_NOTNULL => {
                let attnum = *attnums_from_constraint(&row)?
                    .first()
                    .expect("not null attnum");
                let column_index = column_index_for_attnum(&relation, attnum)?;
                validate_not_null_rows(
                    self,
                    &relation,
                    relation_basename(&alter_stmt.table_name),
                    column_index,
                    &row.conname,
                    &catalog,
                    client_id,
                    xid,
                    cid,
                    std::sync::Arc::clone(&interrupts),
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
                    .validate_not_null_constraint_mvcc(
                        relation.relation_oid,
                        &alter_stmt.constraint_name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
            }
            CONSTRAINT_CHECK => {
                let expr_sql = row.conbin.clone().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "stored CHECK constraint expression",
                        actual: format!("missing expression for constraint {}", row.conname),
                    })
                })?;
                validate_check_rows(
                    self,
                    &relation,
                    relation_basename(&alter_stmt.table_name),
                    &row.conname,
                    &expr_sql,
                    &catalog,
                    client_id,
                    xid,
                    cid,
                    std::sync::Arc::clone(&interrupts),
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
                    .validate_check_constraint_mvcc(
                        relation.relation_oid,
                        &alter_stmt.constraint_name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
            }
            _ => {}
        }

        Ok(StatementResult::AffectedRows(0))
    }
}
