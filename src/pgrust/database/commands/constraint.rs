use super::super::*;
use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::value_io::format_failing_row_detail;
use crate::backend::executor::{ExecutorContext, eval_expr};
use crate::backend::parser::{
    BoundCheckConstraint, BoundExclusionConstraint, BoundForeignKeyConstraint,
    BoundTemporalConstraint, ForeignKeyConstraintAction,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::{
    CONSTRAINT_CHECK, CONSTRAINT_EXCLUSION, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
    CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, DEPENDENCY_NORMAL, PG_CATALOG_NAMESPACE_OID,
    PG_CONSTRAINT_RELATION_OID, PG_REWRITE_RELATION_OID, PgConstraintRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::parsenodes::{ForeignKeyAction, ForeignKeyMatchType};
use crate::pgrust::database::ddl::{
    is_system_column_name, lookup_heap_relation_for_alter_table,
    lookup_table_or_partitioned_table_for_alter_table,
};

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

fn check_constraint_exprs_match(row: &PgConstraintRow, expr_sql: &str) -> bool {
    row.conbin
        .as_deref()
        .is_some_and(|conbin| conbin.trim().eq_ignore_ascii_case(expr_sql.trim()))
}

fn reject_constraint_with_dependent_views(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    catalog: &dyn CatalogLookup,
    relation_name: &str,
    constraint: &PgConstraintRow,
) -> Result<(), ExecError> {
    let depend_rows =
        crate::backend::utils::cache::syscache::ensure_depend_rows(db, client_id, txn_ctx);
    let rewrite_rows =
        crate::backend::utils::cache::syscache::ensure_rewrite_rows(db, client_id, txn_ctx);
    let Some((_, rewrite)) = depend_rows
        .iter()
        .filter(|depend| {
            depend.classid == PG_REWRITE_RELATION_OID
                && depend.refclassid == PG_CONSTRAINT_RELATION_OID
                && depend.refobjid == constraint.oid
                && depend.deptype == DEPENDENCY_NORMAL
        })
        .filter_map(|depend| {
            rewrite_rows
                .iter()
                .find(|rewrite| rewrite.oid == depend.objid)
                .map(|rewrite| (depend, rewrite))
        })
        .find(|(_, rewrite)| {
            catalog
                .class_row_by_oid(rewrite.ev_class)
                .is_some_and(|class| class.relkind == 'v')
        })
    else {
        return Ok(());
    };
    let view_name = catalog
        .class_row_by_oid(rewrite.ev_class)
        .map(|class| class.relname)
        .unwrap_or_else(|| rewrite.ev_class.to_string());
    let relation_name = relation_basename(relation_name);
    Err(ExecError::DetailedError {
        message: format!(
            "cannot drop constraint {} on table {} because other objects depend on it",
            constraint.conname, relation_name
        ),
        detail: Some(format!(
            "view {} depends on constraint {} on table {}",
            view_name, constraint.conname, relation_name
        )),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
}

fn ddl_executor_context(
    db: &Database,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<ExecutorContext, ExecError> {
    let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    Ok(ExecutorContext {
        pool: std::sync::Arc::clone(&db.pool),
        data_dir: None,
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        lock_status_provider: Some(std::sync::Arc::new(db.clone())),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        stats_import_runtime: None,
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: std::sync::Arc::clone(&db.advisory_locks),
        row_locks: std::sync::Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: datetime_config.clone(),
        statement_timestamp_usecs:
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: std::collections::HashMap::new(),
        interrupts,
        stats: std::sync::Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot,
        transaction_state: None,
        client_id,
        current_database_name: db.current_database_name(),
        session_user_oid: db.auth_state(client_id).session_user_oid(),
        current_user_oid: db.auth_state(client_id).current_user_oid(),
        active_role_oid: db.auth_state(client_id).active_role_oid(),
        session_replication_role: db.session_replication_role(client_id),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: cid,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        random_state: crate::backend::executor::PgPrngState::shared(),
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        pending_async_notifications: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: Some(db.clone()),
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        catalog: catalog.materialize_visible_catalog(),
        plpgsql_function_cache: db.plpgsql_function_cache(client_id),
        pinned_cte_tables: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    })
}

pub(super) fn validate_not_null_rows(
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
    if relation.relkind == 'f' {
        return Ok(());
    }
    let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
    let mut ctx = ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        &datetime_config,
        interrupts,
    )?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    let column_name = relation.desc.columns[column_index].name.clone();
    for (_, values) in rows {
        if matches!(values.get(column_index), Some(Value::Null) | None) {
            return Err(ExecError::NotNullViolation {
                relation: relation_name.to_string(),
                column: column_name,
                constraint: constraint_name.to_string(),
                detail: Some(format_failing_row_detail(&values, &ctx.datetime_config)),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_check_rows(
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
    if relation.relkind == 'f' {
        return Ok(());
    }
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
        enforced: true,
    };
    let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
    let mut ctx = ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        &datetime_config,
        interrupts,
    )?;
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

fn bound_temporal_constraint_from_action(
    relation: &crate::backend::parser::BoundRelation,
    action: &crate::backend::parser::IndexBackedConstraintAction,
) -> Result<BoundTemporalConstraint, ExecError> {
    let period_column = action.without_overlaps.as_deref().ok_or_else(|| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "WITHOUT OVERLAPS column",
            actual: "missing WITHOUT OVERLAPS column".into(),
        })
    })?;
    let mut column_names = Vec::with_capacity(action.columns.len());
    let mut column_indexes = Vec::with_capacity(action.columns.len());
    for column_name in &action.columns {
        let index = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
            })
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))?;
        column_names.push(relation.desc.columns[index].name.clone());
        column_indexes.push(index);
    }
    let period_column_index = column_names
        .iter()
        .position(|column| column.eq_ignore_ascii_case(period_column))
        .and_then(|index| column_indexes.get(index).copied())
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(period_column.to_string())))?;
    Ok(BoundTemporalConstraint {
        constraint_oid: 0,
        constraint_name: action
            .constraint_name
            .clone()
            .expect("normalized key constraint name"),
        column_names,
        column_indexes,
        period_column_index,
        primary: action.primary,
        enforced: true,
    })
}

fn bound_exclusion_constraint_from_action(
    relation: &crate::backend::parser::BoundRelation,
    action: &crate::backend::parser::IndexBackedConstraintAction,
    operator_oids: Vec<u32>,
    catalog: &dyn CatalogLookup,
) -> Result<BoundExclusionConstraint, ExecError> {
    let mut column_names = Vec::with_capacity(action.columns.len());
    let mut column_indexes = Vec::with_capacity(action.columns.len());
    for column_name in &action.columns {
        let index = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
            })
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))?;
        column_names.push(relation.desc.columns[index].name.clone());
        column_indexes.push(index);
    }
    if column_indexes.len() != operator_oids.len() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "one exclusion operator per key column",
            actual: format!(
                "{} columns and {} operators",
                column_indexes.len(),
                operator_oids.len()
            ),
        }));
    }
    let operator_proc_oids = operator_oids
        .iter()
        .map(|operator_oid| {
            catalog
                .operator_by_oid(*operator_oid)
                .map(|operator| operator.oprcode)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "exclusion constraint operator",
                        actual: format!("unknown operator oid {operator_oid}"),
                    })
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BoundExclusionConstraint {
        constraint_oid: 0,
        constraint_name: action
            .constraint_name
            .clone()
            .expect("normalized exclusion constraint name"),
        column_names,
        column_indexes,
        operator_oids,
        operator_proc_oids,
        enforced: true,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_temporal_constraint_rows(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    constraint: &BoundTemporalConstraint,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    let mut ctx = ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        datetime_config,
        interrupts,
    )?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    crate::backend::commands::tablecmds::validate_temporal_constraint_existing_rows(
        relation_name,
        &relation.desc,
        constraint,
        &rows,
        &mut ctx,
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_exclusion_constraint_rows(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    constraint: &BoundExclusionConstraint,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
    let mut ctx = ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        &datetime_config,
        interrupts,
    )?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    crate::backend::commands::tablecmds::validate_exclusion_constraint_existing_rows(
        relation_name,
        &relation.desc,
        constraint,
        &rows,
        &mut ctx,
    )
}

fn foreign_key_action_code(action: ForeignKeyAction) -> char {
    match action {
        ForeignKeyAction::NoAction => 'a',
        ForeignKeyAction::Restrict => 'r',
        ForeignKeyAction::Cascade => 'c',
        ForeignKeyAction::SetNull => 'n',
        ForeignKeyAction::SetDefault => 'd',
    }
}

fn foreign_key_match_code(match_type: ForeignKeyMatchType) -> char {
    match match_type {
        ForeignKeyMatchType::Simple => 's',
        ForeignKeyMatchType::Full => 'f',
        ForeignKeyMatchType::Partial => 'p',
    }
}

fn column_attnums_for_names(
    desc: &crate::backend::executor::RelationDesc,
    columns: &[String],
) -> Result<Vec<i16>, ExecError> {
    columns
        .iter()
        .map(|column_name| {
            desc.columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some(index as i16 + 1)
                })
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))
        })
        .collect()
}

fn validate_foreign_key_rows(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    action: &ForeignKeyConstraintAction,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    let referenced_relation = catalog
        .lookup_relation_by_oid(action.referenced_relation_oid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnknownTable(action.referenced_table.clone()))
        })?;
    let referenced_index = catalog
        .index_relations_for_heap(referenced_relation.relation_oid)
        .into_iter()
        .find(|index| index.relation_oid == action.referenced_index_oid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "referenced foreign-key index",
                actual: format!("missing referenced index {}", action.referenced_index_oid),
            })
        })?;
    let constraint = BoundForeignKeyConstraint {
        constraint_oid: 0,
        constraint_name: action.constraint_name.clone(),
        relation_name: relation_name.to_string(),
        column_names: action.columns.clone(),
        column_indexes: action
            .columns
            .iter()
            .map(|column_name| {
                relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, column)| {
                        (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                            .then_some(index)
                    })
                    .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))
            })
            .collect::<Result<Vec<_>, _>>()?,
        period_column_index: action
            .period
            .as_ref()
            .map(|period_column| {
                relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, column)| {
                        (!column.dropped && column.name.eq_ignore_ascii_case(period_column))
                            .then_some(index)
                    })
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(period_column.clone()))
                    })
            })
            .transpose()?,
        match_type: action.match_type,
        referenced_relation_name: action.referenced_table.clone(),
        referenced_relation_oid: referenced_relation.relation_oid,
        referenced_rel: referenced_relation.rel,
        referenced_toast: referenced_relation.toast,
        referenced_desc: referenced_relation.desc.clone(),
        referenced_column_indexes: action
            .referenced_columns
            .iter()
            .map(|column_name| {
                referenced_relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, column)| {
                        (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                            .then_some(index)
                    })
                    .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))
            })
            .collect::<Result<Vec<_>, _>>()?,
        referenced_period_column_index: action
            .referenced_period
            .as_ref()
            .map(|period_column| {
                referenced_relation
                    .desc
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(index, column)| {
                        (!column.dropped && column.name.eq_ignore_ascii_case(period_column))
                            .then_some(index)
                    })
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(period_column.clone()))
                    })
            })
            .transpose()?,
        referenced_index,
        deferrable: false,
        initially_deferred: false,
        enforced: true,
    };
    let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
    let mut ctx = ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        &datetime_config,
        interrupts,
    )?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    for (_, values) in rows {
        crate::backend::executor::enforce_outbound_foreign_keys(
            relation_name,
            std::slice::from_ref(&constraint),
            None,
            &values,
            &mut ctx,
        )?;
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

pub(crate) fn find_constraint_row<'a>(
    rows: &'a [PgConstraintRow],
    name: &str,
) -> Option<&'a PgConstraintRow> {
    rows.iter()
        .find(|row| row.conname.eq_ignore_ascii_case(name))
}

fn normalize_constraint_rename_target_name(name: &str) -> Result<String, ExecError> {
    if name.contains('.') {
        return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
            name.to_string(),
        )));
    }
    Ok(name.to_ascii_lowercase())
}

fn resolve_alter_constraint_deferrability(
    row: &PgConstraintRow,
    alter_stmt: &crate::backend::parser::AlterTableAlterConstraintStatement,
) -> Result<(bool, bool, bool), ExecError> {
    let deferrable = alter_stmt.deferrable.unwrap_or(row.condeferrable);
    let initially_deferred =
        if alter_stmt.deferrable == Some(false) && alter_stmt.initially_deferred.is_none() {
            false
        } else {
            alter_stmt.initially_deferred.unwrap_or(row.condeferred)
        };
    let enforced = alter_stmt.enforced.unwrap_or(row.conenforced);
    if !deferrable && initially_deferred {
        return Err(ExecError::DetailedError {
            message: format!("constraint \"{}\" is not deferrable", row.conname),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    Ok((deferrable, initially_deferred, enforced))
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
    pub(crate) fn execute_alter_table_alter_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
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
            .execute_alter_table_alter_constraint_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_table_alter_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
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
        if row.contype != CONSTRAINT_FOREIGN {
            return Err(ExecError::DetailedError {
                message: format!(
                    "constraint \"{}\" of relation \"{}\" is not a foreign key constraint",
                    alter_stmt.constraint_name,
                    relation_basename(&alter_stmt.table_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        let (deferrable, initially_deferred, enforced) =
            resolve_alter_constraint_deferrability(&row, alter_stmt)?;
        let validating_enable = alter_stmt.enforced == Some(true) && !row.convalidated;
        if row.condeferrable == deferrable
            && row.condeferred == initially_deferred
            && row.conenforced == enforced
            && !validating_enable
        {
            return Ok(StatementResult::AffectedRows(0));
        }
        if enforced && (!row.conenforced || validating_enable) {
            let constraints = crate::backend::parser::bind_relation_constraints(
                Some(relation_basename(&alter_stmt.table_name)),
                relation.relation_oid,
                &relation.desc,
                &catalog,
            )
            .map_err(ExecError::Parse)?;
            let constraint = constraints
                .foreign_keys
                .iter()
                .find(|constraint| {
                    constraint
                        .constraint_name
                        .eq_ignore_ascii_case(&alter_stmt.constraint_name)
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "bound foreign key constraint",
                        actual: format!(
                            "missing foreign key binding for {}",
                            alter_stmt.constraint_name
                        ),
                    })
                })?;
            let validation_action = ForeignKeyConstraintAction {
                constraint_name: constraint.constraint_name.clone(),
                columns: constraint.column_names.clone(),
                period: constraint
                    .period_column_index
                    .map(|index| relation.desc.columns[index].name.clone()),
                referenced_table: constraint.referenced_relation_name.clone(),
                referenced_relation_oid: constraint.referenced_relation_oid,
                referenced_index_oid: constraint.referenced_index.relation_oid,
                self_referential: false,
                referenced_columns: constraint
                    .referenced_column_indexes
                    .iter()
                    .map(|&index| constraint.referenced_desc.columns[index].name.clone())
                    .collect(),
                referenced_period: constraint
                    .referenced_period_column_index
                    .map(|index| constraint.referenced_desc.columns[index].name.clone()),
                match_type: constraint.match_type,
                on_delete: ForeignKeyAction::NoAction,
                on_delete_set_columns: None,
                on_update: ForeignKeyAction::NoAction,
                deferrable: constraint.deferrable,
                initially_deferred: constraint.initially_deferred,
                not_valid: false,
                enforced: true,
            };
            validate_foreign_key_rows(
                self,
                &relation,
                relation_basename(&alter_stmt.table_name),
                &validation_action,
                &catalog,
                client_id,
                xid,
                cid,
                std::sync::Arc::clone(&interrupts),
            )?;
        }
        let validated = if !enforced {
            false
        } else if !row.conenforced || validating_enable {
            true
        } else {
            row.convalidated
        };
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
            .alter_foreign_key_constraint_attributes_mvcc(
                relation.relation_oid,
                &alter_stmt.constraint_name,
                deferrable,
                initially_deferred,
                enforced,
                validated,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_rename_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableRenameConstraintStatement,
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
            .execute_alter_table_rename_constraint_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_table_rename_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableRenameConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_constraint_relation(self, client_id, &relation, &alter_stmt.table_name)?;
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        find_constraint_row(&rows, &alter_stmt.constraint_name).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing table constraint",
                actual: format!(
                    "constraint \"{}\" does not exist",
                    alter_stmt.constraint_name
                ),
            })
        })?;
        let new_constraint_name =
            normalize_constraint_rename_target_name(&alter_stmt.new_constraint_name)?;
        if find_constraint_row(&rows, &new_constraint_name).is_some() {
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(
                new_constraint_name,
            )));
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
            .rename_relation_constraint_mvcc(
                relation.relation_oid,
                &alter_stmt.constraint_name,
                &alter_stmt.new_constraint_name,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_add_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAddConstraintStatement,
        configured_search_path: Option<&[String]>,
        datetime_config: Option<&crate::backend::utils::misc::guc_datetime::DateTimeConfig>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let lock_requests =
            alter_table_add_constraint_lock_requests(&relation, alter_stmt, &catalog)?;
        crate::backend::storage::lmgr::lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &lock_requests,
            interrupts.as_ref(),
        )?;
        let locked_rels = table_lock_relations(&lock_requests);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            datetime_config,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        unlock_relations(&self.table_locks, client_id, &locked_rels);
        result
    }

    pub(crate) fn execute_alter_table_add_constraint_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAddConstraintStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        datetime_config: Option<&crate::backend::utils::misc::guc_datetime::DateTimeConfig>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let default_datetime_config =
            crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
        let datetime_config = datetime_config.unwrap_or(&default_datetime_config);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_constraint_relation(self, client_id, &relation, &alter_stmt.table_name)?;
        let table_name = relation_basename(&alter_stmt.table_name).to_string();
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let normalized = crate::backend::parser::normalize_alter_table_add_constraint(
            &table_name,
            relation.relation_oid,
            relation.relpersistence,
            &relation.desc,
            &existing_constraints,
            &alter_stmt.constraint,
            &catalog,
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
                        action.no_inherit,
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
                let (parent_constraint, effect) = self
                    .catalog
                    .write()
                    .create_check_constraint_mvcc_with_row(
                        relation.relation_oid,
                        action.constraint_name.clone(),
                        action.enforced,
                        action.enforced && !action.not_valid,
                        action.no_inherit,
                        action.expr_sql.clone(),
                        action.parent_constraint_oid.unwrap_or(0),
                        action.is_local,
                        action.inhcount,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                if !alter_stmt.only && !action.no_inherit {
                    self.propagate_check_constraint_to_inheritors(
                        client_id,
                        xid,
                        cid.saturating_add(1),
                        &relation,
                        &action,
                        parent_constraint.oid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
            }
            crate::backend::parser::NormalizedAlterTableConstraint::IndexBacked(action) => {
                if relation.relkind == 'p' || relation.relispartition {
                    let _ = self.install_partitioned_index_backed_constraints_in_transaction(
                        client_id,
                        xid,
                        cid.saturating_add(1),
                        &relation,
                        &[action],
                        configured_search_path,
                        catalog_effects,
                    )?;
                    return Ok(StatementResult::AffectedRows(0));
                }
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
                                    false,
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

                if let Some(existing_index_name) = action.existing_index_name.as_deref() {
                    let constraint_name = action
                        .constraint_name
                        .clone()
                        .expect("normalized key constraint name");
                    let existing_index = catalog
                        .index_relations_for_heap(relation.relation_oid)
                        .into_iter()
                        .find(|index| index.name.eq_ignore_ascii_case(existing_index_name))
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnknownTable(
                                existing_index_name.to_string(),
                            ))
                        })?;
                    let mut index_entry = super::index::catalog_entry_from_bound_index_relation(
                        &existing_index,
                        relation.namespace_oid,
                        relation.owner_oid,
                        relation.relpersistence,
                    );
                    if !existing_index.name.eq_ignore_ascii_case(&constraint_name) {
                        push_notice(format!(
                            "ALTER TABLE / ADD CONSTRAINT USING INDEX will rename index \"{}\" to \"{}\"",
                            existing_index.name, constraint_name
                        ));
                        let rename_ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: cid
                                .saturating_add(1)
                                .saturating_add(catalog_effects.len() as u32),
                            client_id,
                            waiter: None,
                            interrupts: std::sync::Arc::clone(&interrupts),
                        };
                        let visible_type_rows = catalog.type_rows();
                        let rename_effect = self
                            .catalog
                            .write()
                            .rename_relation_mvcc(
                                existing_index.relation_oid,
                                &constraint_name,
                                &visible_type_rows,
                                &rename_ctx,
                            )
                            .map_err(map_catalog_error)?;
                        self.apply_catalog_mutation_effect_immediate(&rename_effect)?;
                        catalog_effects.push(rename_effect);
                    }
                    let old_index_entry = index_entry.clone();
                    if let Some(index_meta) = index_entry.index_meta.as_mut() {
                        index_meta.indisprimary = action.primary;
                        index_meta.indisunique = true;
                    }
                    let table_entry = super::index::catalog_entry_from_bound_relation(&relation);
                    let index_flags_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: cid
                            .saturating_add(1)
                            .saturating_add(catalog_effects.len() as u32),
                        client_id,
                        waiter: None,
                        interrupts: std::sync::Arc::clone(&interrupts),
                    };
                    let index_flags_effect = self
                        .catalog
                        .write()
                        .set_index_entry_constraint_flags_mvcc(
                            &old_index_entry,
                            action.primary,
                            true,
                            &index_flags_ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&index_flags_effect)?;
                    catalog_effects.push(index_flags_effect);
                    let constraint_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: cid
                            .saturating_add(1)
                            .saturating_add(catalog_effects.len() as u32),
                        client_id,
                        waiter: None,
                        interrupts,
                    };
                    let effect = self
                        .catalog
                        .write()
                        .create_index_backed_constraint_for_entries_mvcc_with_period(
                            &table_entry,
                            &index_entry,
                            constraint_name,
                            if action.primary {
                                CONSTRAINT_PRIMARY
                            } else {
                                CONSTRAINT_UNIQUE
                            },
                            &primary_key_owned_not_null_oids,
                            false,
                            None,
                            action.deferrable,
                            action.initially_deferred,
                            &constraint_ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    return Ok(StatementResult::AffectedRows(0));
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
                let mut storage_columns = index_columns.clone();
                storage_columns.extend(
                    action
                        .include_columns
                        .iter()
                        .cloned()
                        .map(crate::backend::parser::IndexColumnDef::from),
                );
                let exclusion_operator_oids = if action.exclusion {
                    Some(self.exclusion_constraint_operator_oids_for_desc(
                        &relation.desc,
                        &action.columns,
                        &action.exclusion_operators,
                        &catalog,
                    )?)
                } else {
                    None
                };
                if action.without_overlaps.is_some() {
                    let temporal = bound_temporal_constraint_from_action(&relation, &action)?;
                    validate_temporal_constraint_rows(
                        self,
                        &relation,
                        &table_name,
                        &temporal,
                        &catalog,
                        client_id,
                        xid,
                        index_cid,
                        datetime_config,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                }
                if let Some(operator_oids) = exclusion_operator_oids.clone() {
                    let exclusion = bound_exclusion_constraint_from_action(
                        &relation,
                        &action,
                        operator_oids,
                        &catalog,
                    )?;
                    validate_exclusion_constraint_rows(
                        self,
                        &relation,
                        &table_name,
                        &exclusion,
                        &catalog,
                        client_id,
                        xid,
                        index_cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                }
                let (access_method_oid, access_method_handler, build_options) = if action.exclusion
                {
                    self.resolve_simple_index_build_options(
                        client_id,
                        Some((xid, index_cid)),
                        action.access_method.as_deref().unwrap_or("gist"),
                        &relation,
                        &index_columns,
                        &[],
                    )?
                } else if action.without_overlaps.is_some() {
                    self.resolve_temporal_index_build_options(
                        client_id,
                        Some((xid, index_cid)),
                        &relation,
                        &index_columns,
                    )?
                } else {
                    self.resolve_simple_index_build_options(
                        client_id,
                        Some((xid, index_cid)),
                        "btree",
                        &relation,
                        &index_columns,
                        &[],
                    )?
                };
                let build_options = crate::backend::catalog::CatalogIndexBuildOptions {
                    indimmediate: !action.deferrable,
                    indisexclusion: action.exclusion || build_options.indisexclusion,
                    ..build_options
                };
                let index_entry = self.build_simple_index_in_transaction(
                    client_id,
                    &relation,
                    &index_name,
                    catalog.materialize_visible_catalog(),
                    &storage_columns,
                    None,
                    !action.exclusion,
                    action.primary,
                    action.nulls_not_distinct,
                    xid,
                    index_cid,
                    access_method_oid,
                    access_method_handler,
                    &build_options,
                    65_536,
                    false,
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
                let conexclop = if let Some(operator_oids) = exclusion_operator_oids {
                    Some(operator_oids)
                } else if action.without_overlaps.is_some() {
                    Some(self.temporal_constraint_operator_oids_for_desc(
                        &relation.desc,
                        &action.columns,
                        action.without_overlaps.as_deref(),
                        &catalog,
                    )?)
                } else {
                    None
                };
                let table_entry = super::index::catalog_entry_from_bound_relation(&relation);
                let effect = self
                    .catalog
                    .write()
                    .create_index_backed_constraint_for_entries_mvcc_with_period(
                        &table_entry,
                        &index_entry,
                        constraint_name,
                        if action.exclusion {
                            CONSTRAINT_EXCLUSION
                        } else if action.primary {
                            CONSTRAINT_PRIMARY
                        } else if action.exclusion {
                            crate::include::catalog::CONSTRAINT_EXCLUSION
                        } else {
                            CONSTRAINT_UNIQUE
                        },
                        &primary_key_owned_not_null_oids,
                        action.without_overlaps.is_some(),
                        conexclop,
                        action.deferrable,
                        action.initially_deferred,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            }
            crate::backend::parser::NormalizedAlterTableConstraint::ForeignKey(action) => {
                if !action.not_valid {
                    if action.enforced {
                        validate_foreign_key_rows(
                            self,
                            &relation,
                            &table_name,
                            &action,
                            &catalog,
                            client_id,
                            xid,
                            cid,
                            std::sync::Arc::clone(&interrupts),
                        )?;
                    }
                }
                let referenced_relation = catalog
                    .lookup_relation_by_oid(action.referenced_relation_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(action.referenced_table.clone()))
                    })?;
                let local_attnums = column_attnums_for_names(&relation.desc, &action.columns)?;
                let referenced_attnums = column_attnums_for_names(
                    &referenced_relation.desc,
                    &action.referenced_columns,
                )?;
                let delete_set_attnums = action
                    .on_delete_set_columns
                    .as_deref()
                    .map(|columns| column_attnums_for_names(&relation.desc, columns))
                    .transpose()?;
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts,
                };
                let (constraint_row, effect) = self
                    .catalog
                    .write()
                    .create_foreign_key_constraint_mvcc(
                        relation.relation_oid,
                        action.constraint_name,
                        action.deferrable,
                        action.initially_deferred,
                        action.enforced,
                        action.enforced && !action.not_valid,
                        &local_attnums,
                        action.referenced_relation_oid,
                        action.referenced_index_oid,
                        &referenced_attnums,
                        foreign_key_action_code(action.on_update),
                        foreign_key_action_code(action.on_delete),
                        foreign_key_match_code(action.match_type),
                        delete_set_attnums.as_deref(),
                        action.period.is_some(),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                self.create_foreign_key_triggers_in_transaction(
                    client_id,
                    xid,
                    cid.saturating_add(1),
                    &constraint_row,
                    catalog_effects,
                )?;
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    #[allow(clippy::too_many_arguments)]
    fn propagate_check_constraint_to_inheritors(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_relation: &crate::backend::parser::BoundRelation,
        action: &crate::backend::parser::CheckConstraintAction,
        parent_constraint_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut children = catalog.inheritance_children(parent_relation.relation_oid);
        children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
        for child in children {
            let child_relation =
                catalog
                    .lookup_relation_by_oid(child.inhrelid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(child.inhrelid.to_string()))
                    })?;
            let child_name = relation_basename(
                &catalog
                    .class_row_by_oid(child_relation.relation_oid)
                    .map(|row| row.relname)
                    .unwrap_or_else(|| child_relation.relation_oid.to_string()),
            )
            .to_string();
            crate::backend::parser::bind_check_constraint_expr(
                &action.expr_sql,
                Some(&child_name),
                &child_relation.desc,
                &catalog,
            )
            .map_err(ExecError::Parse)?;
            if !action.not_valid {
                validate_check_rows(
                    self,
                    &child_relation,
                    &child_name,
                    &action.constraint_name,
                    &action.expr_sql,
                    &catalog,
                    client_id,
                    xid,
                    cid,
                    self.interrupt_state(client_id),
                )?;
            }

            let existing = catalog
                .constraint_rows_for_relation(child_relation.relation_oid)
                .into_iter()
                .find(|row| {
                    row.contype == CONSTRAINT_CHECK
                        && row.conname.eq_ignore_ascii_case(&action.constraint_name)
                });
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let child_constraint_oid = if let Some(existing) = existing {
                if !check_constraint_exprs_match(&existing, &action.expr_sql) {
                    return Err(ExecError::Parse(ParseError::InvalidTableDefinition(
                        format!(
                            "constraint \"{}\" conflicts with inherited constraint",
                            action.constraint_name
                        ),
                    )));
                }
                let effect = self
                    .catalog
                    .write()
                    .update_check_constraint_inheritance_mvcc(
                        child_relation.relation_oid,
                        existing.oid,
                        parent_constraint_oid,
                        existing.conislocal,
                        existing.coninhcount.saturating_add(1),
                        false,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                existing.oid
            } else {
                let (child_constraint, effect) = self
                    .catalog
                    .write()
                    .create_check_constraint_mvcc_with_row(
                        child_relation.relation_oid,
                        action.constraint_name.clone(),
                        action.enforced,
                        action.enforced && !action.not_valid,
                        false,
                        action.expr_sql.clone(),
                        parent_constraint_oid,
                        false,
                        1,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                child_constraint.oid
            };
            self.propagate_check_constraint_to_inheritors(
                client_id,
                xid,
                cid.saturating_add(1),
                &child_relation,
                action,
                child_constraint_oid,
                configured_search_path,
                catalog_effects,
            )?;
        }
        Ok(())
    }

    pub(crate) fn execute_alter_table_drop_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::AlterTableDropConstraintStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
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
        let result = self.execute_alter_table_drop_constraint_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
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
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &drop_stmt.table_name,
            drop_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_constraint_relation(self, client_id, &relation, &drop_stmt.table_name)?;
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        let row = match find_constraint_row(&rows, &drop_stmt.constraint_name).cloned() {
            Some(row) => row,
            None if drop_stmt.missing_ok => {
                push_notice(format!(
                    "constraint \"{}\" of relation \"{}\" does not exist, skipping",
                    drop_stmt.constraint_name,
                    relation_basename(&drop_stmt.table_name)
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing table constraint",
                    actual: format!(
                        "constraint \"{}\" does not exist",
                        drop_stmt.constraint_name
                    ),
                }));
            }
        };
        if drop_stmt.cascade {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "ALTER TABLE DROP CONSTRAINT CASCADE".into(),
            )));
        }
        reject_constraint_with_dependent_views(
            self,
            client_id,
            Some((xid, cid)),
            &catalog,
            &drop_stmt.table_name,
            &row,
        )?;

        match row.contype {
            CONSTRAINT_CHECK | CONSTRAINT_FOREIGN => {
                if row.contype == CONSTRAINT_FOREIGN {
                    self.drop_foreign_key_triggers_in_transaction(
                        client_id,
                        xid,
                        cid,
                        &row,
                        &catalog,
                        catalog_effects,
                    )?;
                }
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: cid.saturating_add(catalog_effects.len() as u32),
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
                if row.conparentid != 0 || row.coninhcount > 0 {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot drop inherited constraint \"{}\" of relation \"{}\"",
                            row.conname,
                            relation_basename(&drop_stmt.table_name),
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                if row.conindid != 0 {
                    reject_index_with_referencing_foreign_keys(
                        &catalog,
                        row.conindid,
                        "ALTER TABLE DROP CONSTRAINT on unreferenced key",
                    )?;
                }
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
        let result = self.execute_alter_table_set_not_null_stmt_in_transaction_with_search_path(
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
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
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
            if relation.desc.columns[column_index].not_null_constraint_no_inherit {
                let constraint_name = relation.desc.columns[column_index]
                    .not_null_constraint_name
                    .as_deref()
                    .unwrap_or(&alter_stmt.column_name);
                return Err(ExecError::Parse(ParseError::InvalidTableDefinition(
                    format!(
                        "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                        constraint_name,
                        relation_basename(&alter_stmt.table_name),
                    ),
                )));
            }
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
        let result = self.execute_alter_table_drop_not_null_stmt_in_transaction_with_search_path(
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
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
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
        if relation.desc.columns[column_index].identity.is_some() {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" is an identity column",
                    relation.desc.columns[column_index].name, alter_stmt.table_name
                )
                .into(),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }
        let attnum = (column_index + 1) as i16;
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        if let Some(primary) = primary_constraint_for_attnum(&existing_constraints, attnum) {
            if primary.conperiod {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "droppable NOT NULL column",
                    actual: format!(
                        "column \"{}\" is in a primary key",
                        relation.desc.columns[column_index].name
                    ),
                }));
            }
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
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let lock_requests =
            alter_table_validate_constraint_lock_requests(&relation, alter_stmt, &catalog)?;
        crate::backend::storage::lmgr::lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &lock_requests,
            interrupts.as_ref(),
        )?;
        let locked_rels = table_lock_relations(&lock_requests);
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
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        unlock_relations(&self.table_locks, client_id, &locked_rels);
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
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
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
        if !row.conenforced {
            return Err(ExecError::DetailedError {
                message: "cannot validate NOT ENFORCED constraint".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
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
            CONSTRAINT_FOREIGN => {
                let constraints = crate::backend::parser::bind_relation_constraints(
                    Some(relation_basename(&alter_stmt.table_name)),
                    relation.relation_oid,
                    &relation.desc,
                    &catalog,
                )
                .map_err(ExecError::Parse)?;
                let constraint = constraints
                    .foreign_keys
                    .into_iter()
                    .find(|constraint| {
                        constraint
                            .constraint_name
                            .eq_ignore_ascii_case(&row.conname)
                    })
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "bound foreign key constraint",
                            actual: format!("missing foreign key binding for {}", row.conname),
                        })
                    })?;
                let mut ctx = ddl_executor_context(
                    self,
                    &catalog,
                    client_id,
                    xid,
                    cid,
                    &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                    std::sync::Arc::clone(&interrupts),
                )?;
                let rows = collect_matching_rows_heap(
                    relation.rel,
                    &relation.desc,
                    relation.toast,
                    None,
                    &mut ctx,
                )?;
                for (_, values) in rows {
                    crate::backend::executor::enforce_outbound_foreign_keys(
                        relation_basename(&alter_stmt.table_name),
                        std::slice::from_ref(&constraint),
                        None,
                        &values,
                        &mut ctx,
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
                    .validate_foreign_key_constraint_mvcc(
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
